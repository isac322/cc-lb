use std::sync::Arc;

use cc_lb_clock::SystemClock;
use cc_lb_storage_api::{
    CacheKeepaliveEnqueueState, CacheKeepaliveHitRefreshRequest, CacheKeepaliveReplaceRequest,
    CacheKeepaliveSessionStatus, CacheKeepaliveSessionStore, CacheKeepaliveTerminalReason,
    CacheTtl, MetaStore, cache_keepalive_job_key,
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
    storage.initialize().await.expect("initialize sqlite");
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
async fn ticketed_completions_use_stable_logical_anchor_and_reject_slow_older_responses() {
    let (_dir, storage) = storage().await;
    let original = storage
        .replace_from_real_request(&replace_request(b"original-prefix", 100))
        .await
        .expect("initial real completion");
    let replacement_ticket = storage
        .allocate_cache_keepalive_completion_ticket("session-hash")
        .await
        .expect("replacement ticket");
    let refreshed = storage
        .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
            session_key_hash: "session-hash".to_owned(),
            generation: original.generation,
            cache_anchor_at_unix_secs: 371,
            run_at_unix_secs: 641,
            expires_at_unix_secs: 671,
            encrypted_payload: None,
            now_unix_secs: 371,
        })
        .await
        .expect("synthetic refresh")
        .expect("refreshed generation");
    assert_eq!(refreshed.first_scheduled_at_unix_secs, 100);
    let active = storage
        .replace_from_real_request_fenced(
            &replace_request(b"new-real-prefix", 370),
            Some(replacement_ticket),
        )
        .await
        .expect("new real completion after synthetic refresh")
        .expect("real logical anchor is newer despite older sliding anchor");
    assert_eq!(active.first_scheduled_at_unix_secs, 370);
    assert_eq!(active.cache_anchor_at_unix_secs, 370);
    assert_eq!(active.generation, refreshed.generation + 1);

    let cancel_ticket = storage
        .allocate_cache_keepalive_completion_ticket("session-hash")
        .await
        .expect("later-observed cancellation ticket");
    assert!(
        !storage
            .cancel_cache_keepalive_fenced(
                "session-hash",
                Some(cancel_ticket),
                369,
                CacheKeepaliveTerminalReason::Cancelled,
                400,
            )
            .await
            .expect("slow older cancellation")
    );
    assert_eq!(
        storage
            .get_meta_value(&cc_lb_storage_api::cache_keepalive_order_meta_key("session-hash"))
            .await
            .expect("rejected cancellation must roll back applied order"),
        Some("2:1".to_owned())
    );
    assert_eq!(
        storage
            .get_cache_keepalive_session("session-hash")
            .await
            .expect("load protected real generation")
            .expect("protected real generation exists"),
        active
    );
    assert!(
        storage
            .cancel_cache_keepalive_fenced(
                "session-hash",
                Some(cancel_ticket),
                372,
                CacheKeepaliveTerminalReason::Cancelled,
                400,
            )
            .await
            .expect("retry same ticket at a valid logical boundary")
    );
    let terminal = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load logical cancellation fence")
        .expect("logical cancellation fence exists");
    assert_eq!(terminal.first_scheduled_at_unix_secs, 372);
    assert_eq!(terminal.cache_anchor_at_unix_secs, 370);

    let newer_ticket = storage
        .allocate_cache_keepalive_completion_ticket("session-hash")
        .await
        .expect("later-observed replacement ticket");
    assert!(
        storage
            .replace_from_real_request_fenced(
                &replace_request(b"slow-old-prefix", 371),
                Some(newer_ticket),
            )
            .await
            .expect("slow older replacement")
            .is_none()
    );
    assert_eq!(
        storage
            .get_meta_value(&cc_lb_storage_api::cache_keepalive_order_meta_key("session-hash"))
            .await
            .expect("rejected replacement must roll back applied order"),
        Some("3:2".to_owned())
    );
    let replacement = storage
        .replace_from_real_request_fenced(
            &replace_request(b"new-post-compaction-prefix", 373),
            Some(newer_ticket),
        )
        .await
        .expect("retry same replacement ticket")
        .expect("newer logical anchor reactivates terminal state");
    assert_eq!(replacement.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(replacement.first_scheduled_at_unix_secs, 373);
    assert_eq!(replacement.generation, terminal.generation + 1);
    assert_eq!(replacement.encrypted_payload, b"new-post-compaction-prefix");
    assert_eq!(
        storage
            .get_meta_value(&cc_lb_storage_api::cache_keepalive_order_meta_key("session-hash"))
            .await
            .expect("successful retry commits applied order"),
        Some("3:3".to_owned())
    );
}

#[tokio::test]
async fn ticketed_cancel_retires_generation_after_synthetic_refresh() {
    let (_dir, storage) = storage().await;
    let original = storage
        .replace_from_real_request(&replace_request(b"old-prefix", 100))
        .await
        .expect("initial real generation");
    let ticket = storage
        .allocate_cache_keepalive_completion_ticket("session-hash")
        .await
        .expect("compaction completion ticket");
    assert_eq!(ticket, 1);
    let refreshed = storage
        .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
            session_key_hash: "session-hash".to_owned(),
            generation: original.generation,
            cache_anchor_at_unix_secs: 371,
            run_at_unix_secs: 641,
            expires_at_unix_secs: 671,
            encrypted_payload: None,
            now_unix_secs: 371,
        })
        .await
        .expect("synthetic refresh")
        .expect("refreshed generation");

    assert!(
        storage
            .cancel_cache_keepalive_fenced(
                "session-hash",
                Some(ticket),
                370,
                CacheKeepaliveTerminalReason::Cancelled,
                400,
            )
            .await
            .expect("ticketed compaction cancellation")
    );
    let terminal = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load terminal generation")
        .expect("terminal generation exists");
    assert_eq!(terminal.generation, refreshed.generation);
    assert_eq!(terminal.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(terminal.cache_anchor_at_unix_secs, 371);
    assert_eq!(terminal.first_scheduled_at_unix_secs, 370);
    assert_eq!(terminal.updated_at_unix_secs, 400);
    assert!(terminal.encrypted_payload.is_empty());
    assert_eq!(
        storage
            .get_meta_value(&cc_lb_storage_api::cache_keepalive_order_meta_key("session-hash"))
            .await
            .expect("load completion order"),
        Some("1:1".to_owned())
    );
    assert!(
        !storage
            .cancel_cache_keepalive_fenced(
                "session-hash",
                Some(ticket),
                401,
                CacheKeepaliveTerminalReason::Cancelled,
                401,
            )
            .await
            .expect("already applied ticket is stale")
    );
}

#[tokio::test]
async fn newer_ticket_replaces_terminal_generation_before_terminal_write_timestamp() {
    let (_dir, storage) = storage().await;
    let original = storage
        .replace_from_real_request(&replace_request(b"old-prefix", 100))
        .await
        .expect("initial real generation");
    let cancelled_ticket = storage
        .allocate_cache_keepalive_completion_ticket("session-hash")
        .await
        .expect("compaction completion ticket");
    assert!(
        storage
            .cancel_cache_keepalive_fenced(
                "session-hash",
                Some(cancelled_ticket),
                100,
                CacheKeepaliveTerminalReason::Cancelled,
                103,
            )
            .await
            .expect("compaction cancellation")
    );
    let newer_ticket = storage
        .allocate_cache_keepalive_completion_ticket("session-hash")
        .await
        .expect("post-compaction completion ticket");
    assert_eq!(newer_ticket, cancelled_ticket + 1);
    let active = storage
        .replace_from_real_request_fenced(
            &replace_request(b"post-compaction-prefix", 102),
            Some(newer_ticket),
        )
        .await
        .expect("newer real completion")
        .expect("newer ticket supersedes terminal write timestamp");
    assert_eq!(active.generation, original.generation + 1);
    assert_eq!(active.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(active.cache_anchor_at_unix_secs, 102);
    assert_eq!(active.encrypted_payload, b"post-compaction-prefix");
    assert_eq!(
        storage
            .get_meta_value(&cc_lb_storage_api::cache_keepalive_order_meta_key("session-hash"))
            .await
            .expect("load completion order"),
        Some("2:2".to_owned())
    );
    assert!(
        storage
            .replace_from_real_request_fenced(
                &replace_request(b"obsolete-completion-prefix", 200),
                Some(cancelled_ticket),
            )
            .await
            .expect("older applied completion ticket")
            .is_none()
    );
    assert_eq!(
        storage
            .get_cache_keepalive_session("session-hash")
            .await
            .expect("load post-compaction generation")
            .expect("post-compaction generation exists"),
        active
    );
}

#[tokio::test]
async fn completion_tickets_preserve_active_anchor_and_legacy_timestamp_guards() {
    let (_dir, storage) = storage().await;
    let original = storage
        .replace_from_real_request(&replace_request(b"newer-active-prefix", 371))
        .await
        .expect("initial active generation");
    let ticket = storage
        .allocate_cache_keepalive_completion_ticket("session-hash")
        .await
        .expect("real completion ticket");
    for expected_ticket in [None, Some(ticket)] {
        assert!(
            storage
                .replace_from_real_request_fenced(
                    &replace_request(b"older-prefix", 370),
                    expected_ticket,
                )
                .await
                .expect("stale active anchor")
                .is_none()
        );
    }
    assert!(
        !storage
            .cancel_cache_keepalive_fenced(
                "session-hash",
                None,
                370,
                CacheKeepaliveTerminalReason::Cancelled,
                400,
            )
            .await
            .expect("legacy stale cancellation")
    );
    assert_eq!(
        storage
            .get_cache_keepalive_session("session-hash")
            .await
            .expect("load active generation")
            .expect("active generation exists"),
        original
    );
    assert_eq!(
        storage
            .get_meta_value(&cc_lb_storage_api::cache_keepalive_order_meta_key("session-hash"))
            .await
            .expect("rejected replacement preserves completion order"),
        Some("1:0".to_owned())
    );
    assert!(
        storage
            .cancel_cache_keepalive_fenced(
                "session-hash",
                None,
                371,
                CacheKeepaliveTerminalReason::Cancelled,
                400,
            )
            .await
            .expect("legacy current-anchor cancellation")
    );
    assert!(
        storage
            .replace_from_real_request_fenced(&replace_request(b"stale-terminal-prefix", 399), None)
            .await
            .expect("legacy terminal timestamp guard")
            .is_none()
    );
    assert!(
        storage
            .replace_from_real_request_fenced(
                &replace_request(b"older-logical-prefix", 370),
                Some(ticket),
            )
            .await
            .expect("ticket cannot bypass logical terminal fence")
            .is_none()
    );
    let replacement = storage
        .replace_from_real_request_fenced(
            &replace_request(b"logically-newer-prefix", 371),
            Some(ticket),
        )
        .await
        .expect("ticketed terminal replacement")
        .expect("same ticket can replace at the logical boundary");
    assert_eq!(replacement.generation, original.generation + 1);
    assert_eq!(replacement.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(replacement.encrypted_payload, b"logically-newer-prefix");
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
                None,
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
                None,
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
    assert_eq!(check.enqueue_state, CacheKeepaliveEnqueueState::Pending);

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
async fn terminal_error_is_truncated_and_resets_running_enqueue_state() {
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
        storage
            .claim_cache_keepalive_turn("session-hash", record.generation, 102)
            .await
            .expect("claim turn")
    );

    let error = "é".repeat(300);
    assert!(
        storage
            .mark_cache_keepalive_terminal(
                "session-hash",
                record.generation,
                CacheKeepaliveTerminalReason::DispatchError,
                Some(&error),
                103,
            )
            .await
            .expect("mark terminal")
    );

    let terminal = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load terminal session")
        .expect("terminal session exists");
    let persisted_error = terminal.error.as_deref().expect("terminal error exists");
    assert_eq!(persisted_error.len(), 512);
    assert_eq!(persisted_error.chars().count(), 256);
    assert!(persisted_error.is_char_boundary(persisted_error.len()));
    assert_eq!(terminal.enqueue_state, CacheKeepaliveEnqueueState::Pending);
}

#[tokio::test]
async fn fenced_terminal_paths_reset_enqueue_state_and_reschedule_clears_error() {
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
        storage
            .claim_cache_keepalive_turn("session-hash", record.generation, 102)
            .await
            .expect("claim turn")
    );
    assert!(
        storage
            .cancel_cache_keepalive_fenced(
                "session-hash",
                None,
                100,
                CacheKeepaliveTerminalReason::Cancelled,
                103,
            )
            .await
            .expect("cancel session")
    );
    let cancelled = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load cancelled session")
        .expect("cancelled session exists");
    assert_eq!(cancelled.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert_eq!(cancelled.error, None);

    let active = storage
        .replace_from_real_request(&replace_request(b"ciphertext-two", 110))
        .await
        .expect("replace session");
    assert!(
        storage
            .mark_cache_keepalive_enqueued("session-hash", active.generation, 111)
            .await
            .expect("mark replacement enqueued")
    );
    assert!(
        storage
            .claim_cache_keepalive_turn("session-hash", active.generation, 112)
            .await
            .expect("claim replacement turn")
    );
    assert!(
        storage
            .mark_latest_cache_keepalive_terminal(
                "session-hash",
                CacheKeepaliveTerminalReason::Cancelled,
                113,
            )
            .await
            .expect("mark latest terminal")
    );
    let latest = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load latest session")
        .expect("latest session exists");
    assert_eq!(latest.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert_eq!(latest.error, None);

    let active = storage
        .replace_from_real_request(&replace_request(b"ciphertext-three", 120))
        .await
        .expect("replace for reschedule");
    sqlx::query(
        "UPDATE cache_keepalive_sessions SET error = ? WHERE session_key_hash = ? AND generation = ?",
    )
    .bind("stale error")
    .bind(&active.session_key_hash)
    .bind(i64::try_from(active.generation).expect("generation fits i64"))
    .execute(storage.pool())
    .await
    .expect("seed active error");
    let refreshed = storage
        .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
            session_key_hash: active.session_key_hash,
            generation: active.generation,
            cache_anchor_at_unix_secs: 121,
            run_at_unix_secs: 391,
            expires_at_unix_secs: 421,
            encrypted_payload: None,
            now_unix_secs: 121,
        })
        .await
        .expect("reschedule after cache hit")
        .expect("rescheduled session exists");
    assert_eq!(refreshed.error, None);
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
