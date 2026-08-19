use std::sync::Arc;

use cc_lb_clock::SystemClock;
use cc_lb_storage_api::{
    BackendKind, CacheKeepaliveEnqueueState, CacheKeepaliveHitRefreshRequest,
    CacheKeepaliveReplaceRequest, CacheKeepaliveSessionStatus, CacheKeepaliveSessionStore,
    CacheKeepaliveTerminalReason, CacheTtl, MetaStore, cache_keepalive_job_key,
};
use tempfile::TempDir;
use uuid::Uuid;

use crate::{SqliteStorage, open_sqlite};

async fn storage() -> (TempDir, SqliteStorage) {
    let dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!("sqlite://{}", dir.path().join("keepalive.sqlite").display());
    let storage = open_sqlite(&database_url, Arc::new(SystemClock))
        .await
        .expect("open sqlite");
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("initialize sqlite");
    (dir, storage)
}

fn replace_request(payload: &[u8], now: u64) -> CacheKeepaliveReplaceRequest {
    CacheKeepaliveReplaceRequest {
        session_key_hash: "session-hash".to_owned(),
        principal_id: "principal".to_owned(),
        accounting_key_id: None,
        upstream_id: Uuid::from_u128(7),
        cache_anchor_at_unix_secs: now,
        ttl: CacheTtl::Ttl5m,
        run_at_unix_secs: now + 270,
        expires_at_unix_secs: now + 300,
        encrypted_payload: payload.to_vec(),
        display_reason: "agent-in-turn".to_owned(),
        config_snapshot: cc_lb_storage_api::CacheKeepaliveConfigSnapshot {
            refresh_lead_time_5m_secs: 30,
            refresh_lead_time_1h_secs: 300,
            max_refreshes_per_session: 12,
            max_total_duration_secs: 14_400,
            snapshot_max_bytes: 524_288,
        },
        now_unix_secs: now,
    }
}

#[tokio::test]
async fn replace_from_real_request_bumps_generation_and_resets_counters() {
    let (_dir, storage) = storage().await;

    let first = storage
        .replace_from_real_request(&replace_request(b"ciphertext-one", 100))
        .await
        .expect("insert first session");
    let second = storage
        .replace_from_real_request(&replace_request(b"ciphertext-two", 200))
        .await
        .expect("replace session");

    assert_eq!(first.generation, 1);
    assert_eq!(second.generation, 2);
    assert_eq!(second.refresh_count, 0);
    assert_eq!(second.first_scheduled_at_unix_secs, 200);
    assert_eq!(second.cache_anchor_at_unix_secs, 200);
    assert_eq!(second.run_at_unix_secs, 470);
    assert_eq!(
        second.current_job_key,
        cache_keepalive_job_key("session-hash", 2)
    );
    assert_eq!(second.encrypted_payload, b"ciphertext-two");
}

#[tokio::test]
async fn replace_roundtrips_optional_accounting_key_id() {
    let (_dir, storage) = storage().await;
    let with_key = CacheKeepaliveReplaceRequest {
        accounting_key_id: Some("key-live-123".to_owned()),
        ..replace_request(b"ciphertext-with-key", 100)
    };
    let mut without_key = replace_request(b"ciphertext-without-key", 110);
    without_key.session_key_hash = "session-without-key".to_owned();

    let with_key = storage
        .replace_from_real_request(&with_key)
        .await
        .expect("insert keyed session");
    let without_key = storage
        .replace_from_real_request(&without_key)
        .await
        .expect("insert unkeyed session");

    assert_eq!(with_key.accounting_key_id.as_deref(), Some("key-live-123"));
    assert_eq!(without_key.accounting_key_id, None);
}

#[tokio::test]
async fn concurrent_claim_allows_exactly_one_enqueued_generation_winner() {
    let (_dir, storage) = storage().await;
    let storage = Arc::new(storage);
    let record = storage
        .replace_from_real_request(&replace_request(b"ciphertext", 100))
        .await
        .expect("insert session");
    assert!(
        storage
            .mark_cache_keepalive_enqueued("session-hash", record.generation, 101)
            .await
            .expect("mark enqueued")
    );
    let barrier = Arc::new(tokio::sync::Barrier::new(2));

    let first = tokio::spawn({
        let storage = Arc::clone(&storage);
        let barrier = Arc::clone(&barrier);
        async move {
            barrier.wait().await;
            storage
                .claim_cache_keepalive_turn("session-hash", record.generation, 200)
                .await
        }
    });
    let second = tokio::spawn({
        let storage = Arc::clone(&storage);
        let barrier = Arc::clone(&barrier);
        async move {
            barrier.wait().await;
            storage
                .claim_cache_keepalive_turn("session-hash", record.generation, 200)
                .await
        }
    });

    let (first, second) = tokio::join!(first, second);
    let first = first
        .expect("first task joins")
        .expect("first claim succeeds");
    let second = second
        .expect("second task joins")
        .expect("second claim succeeds");

    assert_eq!(u8::from(first) + u8::from(second), 1);
    let claimed = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load claimed session")
        .expect("session exists");
    assert_eq!(claimed.enqueue_state, CacheKeepaliveEnqueueState::Running);
    assert_eq!(claimed.running_since_unix_secs, Some(200));
}

#[tokio::test]
async fn update_payload_only_mutates_matching_pending_generation() {
    let (_dir, storage) = storage().await;
    let record = storage
        .replace_from_real_request(&replace_request(b"pending-placeholder", 100))
        .await
        .expect("insert session");

    assert!(
        !storage
            .update_cache_keepalive_payload("session-hash", record.generation + 1, b"stale", 101)
            .await
            .expect("stale payload no-op")
    );
    assert!(
        storage
            .update_cache_keepalive_payload("session-hash", record.generation, b"ciphertext", 102)
            .await
            .expect("fresh payload update")
    );

    let updated = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("row exists");
    assert_eq!(updated.encrypted_payload, b"ciphertext");
    assert_eq!(updated.enqueue_state, CacheKeepaliveEnqueueState::Pending);

    assert!(
        storage
            .mark_cache_keepalive_enqueued("session-hash", record.generation, 103)
            .await
            .expect("mark enqueued")
    );
    assert!(
        !storage
            .update_cache_keepalive_payload("session-hash", record.generation, b"late", 104)
            .await
            .expect("enqueued payload no-op")
    );
}

#[tokio::test]
async fn cache_hit_reschedule_preserves_duration_anchor_and_rejects_stale_generation() {
    let (_dir, storage) = storage().await;
    let original = storage
        .replace_from_real_request(&replace_request(b"ciphertext-one", 100))
        .await
        .expect("insert first session");

    let stale = storage
        .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
            session_key_hash: "session-hash".to_owned(),
            generation: original.generation + 1,
            cache_anchor_at_unix_secs: 150,
            run_at_unix_secs: 420,
            expires_at_unix_secs: 450,
            encrypted_payload: None,
            now_unix_secs: 151,
        })
        .await
        .expect("stale update is a no-op");
    assert!(stale.is_none());

    let updated = storage
        .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
            session_key_hash: "session-hash".to_owned(),
            generation: original.generation,
            cache_anchor_at_unix_secs: 150,
            run_at_unix_secs: 420,
            expires_at_unix_secs: 450,
            encrypted_payload: None,
            now_unix_secs: 151,
        })
        .await
        .expect("fresh update")
        .expect("row updated");

    assert_eq!(updated.generation, 2);
    assert_eq!(updated.refresh_count, 1);
    assert_eq!(updated.first_scheduled_at_unix_secs, 100);
    assert_eq!(updated.cache_anchor_at_unix_secs, 150);
    assert_eq!(updated.run_at_unix_secs, 420);
    assert_eq!(updated.encrypted_payload, b"ciphertext-one");
    let migrated = storage
        .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
            session_key_hash: "session-hash".to_owned(),
            generation: updated.generation,
            cache_anchor_at_unix_secs: 151,
            run_at_unix_secs: 421,
            expires_at_unix_secs: 451,
            encrypted_payload: Some(b"compressed-ciphertext".to_vec()),
            now_unix_secs: 152,
        })
        .await
        .expect("fresh payload migration")
        .expect("row migrated");
    assert_eq!(migrated.generation, 3);
    assert_eq!(migrated.refresh_count, 2);
    assert_eq!(migrated.encrypted_payload, b"compressed-ciphertext");
}

#[tokio::test]
async fn conditional_enqueue_terminal_and_purge_are_generation_safe() {
    let (_dir, storage) = storage().await;
    let record = storage
        .replace_from_real_request(&replace_request(b"ciphertext", 100))
        .await
        .expect("insert session");

    assert!(
        storage
            .mark_cache_keepalive_enqueued("session-hash", record.generation, 101)
            .await
            .expect("mark enqueued")
    );
    assert!(
        !storage
            .mark_cache_keepalive_terminal(
                "session-hash",
                record.generation + 1,
                CacheKeepaliveTerminalReason::CacheMiss,
                102,
            )
            .await
            .expect("stale terminal no-op")
    );
    assert!(
        storage
            .mark_cache_keepalive_terminal(
                "session-hash",
                record.generation,
                CacheKeepaliveTerminalReason::CacheMiss,
                102,
            )
            .await
            .expect("fresh terminal")
    );
    let terminal = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load terminal session")
        .expect("terminal session exists");
    assert!(terminal.encrypted_payload.is_empty());
    let check = storage
        .check_cache_keepalive_generation("session-hash")
        .await
        .expect("check")
        .expect("row exists");
    assert_eq!(check.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(check.enqueue_state, CacheKeepaliveEnqueueState::Enqueued);

    let removed = storage
        .purge_cache_keepalive_expired(401)
        .await
        .expect("purge expired");
    assert_eq!(removed, 1);
    assert!(
        storage
            .get_cache_keepalive_session("session-hash")
            .await
            .expect("get after purge")
            .is_none()
    );
}

#[tokio::test]
async fn mark_latest_terminal_only_mutates_active_latest_session() {
    let (_dir, storage) = storage().await;
    let first = storage
        .replace_from_real_request(&replace_request(b"ciphertext-one", 100))
        .await
        .expect("insert first session");
    let second = storage
        .replace_from_real_request(&replace_request(b"ciphertext-two", 110))
        .await
        .expect("replace session");

    assert_eq!(first.generation, 1);
    assert_eq!(second.generation, 2);
    assert!(
        storage
            .mark_latest_cache_keepalive_terminal(
                "session-hash",
                CacheKeepaliveTerminalReason::Cancelled,
                120,
            )
            .await
            .expect("mark latest terminal")
    );
    assert!(
        !storage
            .mark_latest_cache_keepalive_terminal(
                "session-hash",
                CacheKeepaliveTerminalReason::CacheMiss,
                121,
            )
            .await
            .expect("terminal row no-op")
    );

    let record = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("row exists");
    assert_eq!(record.generation, 2);
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::Cancelled)
    );
    assert!(record.encrypted_payload.is_empty());
}

#[tokio::test]
async fn purge_stale_pending_keeps_enqueued_work() {
    let (_dir, storage) = storage().await;
    let pending = storage
        .replace_from_real_request(&replace_request(b"pending", 100))
        .await
        .expect("insert pending session");
    let mut enqueued_request = replace_request(b"enqueued", 110);
    enqueued_request.session_key_hash = "old-enqueued".to_owned();
    let enqueued = storage
        .replace_from_real_request(&enqueued_request)
        .await
        .expect("insert enqueued session");
    assert!(
        storage
            .mark_cache_keepalive_enqueued("old-enqueued", enqueued.generation, 111)
            .await
            .expect("mark enqueued")
    );

    let removed = storage
        .purge_cache_keepalive_stale_pending(112)
        .await
        .expect("purge stale pending");

    assert_eq!(pending.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert_eq!(removed, 1);
    assert!(
        storage
            .get_cache_keepalive_session("session-hash")
            .await
            .expect("load pending after purge")
            .is_none()
    );
    assert!(
        storage
            .get_cache_keepalive_session("old-enqueued")
            .await
            .expect("load enqueued after purge")
            .is_some()
    );
}
