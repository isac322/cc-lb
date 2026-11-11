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
        cycle_id: Uuid::nil(),
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
                None,
                400,
            )
            .await
            .expect("slow older cancellation")
    );
    assert_eq!(
        storage
            .get_meta_value(&cc_lb_storage_api::cache_keepalive_order_meta_key(
                "session-hash"
            ))
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
                None,
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
            .get_meta_value(&cc_lb_storage_api::cache_keepalive_order_meta_key(
                "session-hash"
            ))
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
            .get_meta_value(&cc_lb_storage_api::cache_keepalive_order_meta_key(
                "session-hash"
            ))
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
                None,
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
            .get_meta_value(&cc_lb_storage_api::cache_keepalive_order_meta_key(
                "session-hash"
            ))
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
                None,
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
                None,
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
            .get_meta_value(&cc_lb_storage_api::cache_keepalive_order_meta_key(
                "session-hash"
            ))
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
                None,
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
            .get_meta_value(&cc_lb_storage_api::cache_keepalive_order_meta_key(
                "session-hash"
            ))
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
                None,
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
async fn pending_claim_requires_payload_and_mark_cannot_overwrite_running() {
    let (_dir, storage) = storage().await;
    let record = storage
        .replace_from_real_request(&replace_request(b"", 100))
        .await
        .expect("insert pending placeholder");

    assert!(
        !storage
            .claim_cache_keepalive_turn("session-hash", record.generation, 101)
            .await
            .expect("reject empty pending payload")
    );
    assert!(
        storage
            .update_cache_keepalive_payload("session-hash", record.generation, b"ciphertext", 102)
            .await
            .expect("fill pending payload")
    );
    assert!(
        storage
            .claim_cache_keepalive_turn("session-hash", record.generation, 103)
            .await
            .expect("claim published pending payload")
    );
    let claimed = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load running session")
        .expect("running session exists");
    assert_eq!(claimed.enqueue_state, CacheKeepaliveEnqueueState::Running);
    assert_eq!(claimed.running_since_unix_secs, Some(103));
    assert!(
        !storage
            .mark_cache_keepalive_enqueued("session-hash", record.generation, 104)
            .await
            .expect("running mark is a no-op")
    );
    let unchanged = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("reload running session")
        .expect("running session remains");
    assert_eq!(unchanged.enqueue_state, CacheKeepaliveEnqueueState::Running);
    assert_eq!(unchanged.running_since_unix_secs, Some(103));
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
    assert_eq!(removed, 0);
    assert!(
        storage
            .get_cache_keepalive_session("session-hash")
            .await
            .expect("get after retained purge")
            .is_some()
    );
    let removed = storage
        .purge_cache_keepalive_expired(102 + 7 * 24 * 60 * 60 + 1)
        .await
        .expect("purge retained session");
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
                None,
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
async fn recover_stale_running_requires_active_running_strictly_old_lease_and_scrubs_payload() {
    let (_dir, storage) = storage().await;
    let cutoff = 200;
    let now = 500;
    let mut before = Vec::new();

    for (key, status, enqueue_state, lease) in [
        ("stale-running", "active", "running", Some(199_i64)),
        ("exact-cutoff", "active", "running", Some(200)),
        ("fresh-running", "active", "running", Some(201)),
        ("null-lease", "active", "running", None),
        ("pending", "active", "pending", Some(199)),
        ("enqueued", "active", "enqueued", Some(199)),
        ("terminal", "terminal", "running", Some(199)),
    ] {
        let mut request = replace_request(key.as_bytes(), 100);
        request.session_key_hash = key.to_owned();
        request.accounting_key_id = Some(format!("accounting-{key}"));
        storage
            .replace_from_real_request(&request)
            .await
            .expect("insert session");
        request.now_unix_secs = 101;
        storage
            .replace_from_real_request(&request)
            .await
            .expect("replace session to exercise a noninitial generation");

        // Legacy state combinations isolate each predicate, including a missing lease
        // and old leases on rows that are not active/running.
        sqlx::query(
            "UPDATE cache_keepalive_sessions
             SET status = ?, enqueue_state = ?, running_since_unix_secs = ?, error = ?
             WHERE session_key_hash = ?",
        )
        .bind(status)
        .bind(enqueue_state)
        .bind(lease)
        .bind("previous error")
        .bind(key)
        .execute(storage.pool())
        .await
        .expect("seed recovery state");
        before.push(
            storage
                .get_cache_keepalive_session(key)
                .await
                .expect("load seeded session")
                .expect("seeded session exists"),
        );
    }

    let affected = storage
        .recover_cache_keepalive_stale_running(cutoff, now)
        .await
        .expect("recover stale running sessions");
    assert_eq!(affected, 1);

    let mut expected = before.remove(0);
    let recovered = storage
        .get_cache_keepalive_session(&expected.session_key_hash)
        .await
        .expect("load recovered session")
        .expect("recovered session remains persisted");
    assert_eq!(recovered.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        recovered.terminal_reason,
        Some(CacheKeepaliveTerminalReason::Stale)
    );
    assert_eq!(recovered.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert_eq!(
        recovered.error.as_deref(),
        Some("keepalive renewal timed out while running")
    );
    assert_eq!(recovered.running_since_unix_secs, None);
    assert!(recovered.encrypted_payload.is_empty());
    assert_eq!(recovered.updated_at_unix_secs, now);
    assert_eq!(recovered.generation, expected.generation);
    assert_eq!(recovered.current_job_key, expected.current_job_key);

    expected.status = CacheKeepaliveSessionStatus::Terminal;
    expected.terminal_reason = Some(CacheKeepaliveTerminalReason::Stale);
    expected.enqueue_state = CacheKeepaliveEnqueueState::Pending;
    expected.error = Some("keepalive renewal timed out while running".to_owned());
    expected.running_since_unix_secs = None;
    expected.encrypted_payload.clear();
    expected.updated_at_unix_secs = now;
    assert_eq!(recovered, expected, "unrelated session fields must survive");

    for control in before {
        let persisted = storage
            .get_cache_keepalive_session(&control.session_key_hash)
            .await
            .expect("load noncandidate")
            .expect("noncandidate remains persisted");
        assert_eq!(
            persisted, control,
            "noncandidates must retain their payload, identity, and all other fields"
        );
    }
    assert_eq!(
        storage
            .recover_cache_keepalive_stale_running(cutoff, now + 1)
            .await
            .expect("recovery is idempotent"),
        0
    );
}

#[tokio::test]
async fn recover_stale_running_rejects_overflow_without_mutation() {
    let (_dir, storage) = storage().await;
    let record = storage
        .replace_from_real_request(&replace_request(b"overflow", 100))
        .await
        .expect("insert running session");
    assert!(
        storage
            .mark_cache_keepalive_enqueued("session-hash", record.generation, 110)
            .await
            .expect("mark enqueued")
    );
    assert!(
        storage
            .claim_cache_keepalive_turn("session-hash", record.generation, 150)
            .await
            .expect("claim running turn")
    );
    let before = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session before overflow checks")
        .expect("running session exists");

    for (cutoff, now) in [(200, u64::MAX), (u64::MAX, 500)] {
        assert!(
            storage
                .recover_cache_keepalive_stale_running(cutoff, now)
                .await
                .is_err(),
            "out-of-range recovery timestamp must be rejected"
        );
        assert_eq!(
            storage
                .get_cache_keepalive_session("session-hash")
                .await
                .expect("load session after overflow rejection")
                .expect("running session remains"),
            before,
            "overflow rejection must not mutate persisted state"
        );
    }
}

#[tokio::test]
async fn purge_expired_uses_a_strict_cutoff_boundary() {
    let (_dir, storage) = storage().await;
    for (key, now) in [
        ("expired-before-cutoff", 99),
        ("exact-cutoff-expiry", 100),
        ("future-expiry", 101),
    ] {
        let mut request = replace_request(key.as_bytes(), now);
        request.session_key_hash = key.to_owned();
        storage
            .replace_from_real_request(&request)
            .await
            .expect("insert expiry boundary session");
    }

    assert_eq!(
        storage
            .purge_cache_keepalive_expired(400)
            .await
            .expect("purge before cutoff"),
        1
    );
    assert!(
        storage
            .get_cache_keepalive_session("expired-before-cutoff")
            .await
            .expect("load expired row")
            .is_none()
    );
    assert!(
        storage
            .get_cache_keepalive_session("exact-cutoff-expiry")
            .await
            .expect("load exact boundary row")
            .is_some(),
        "expiry equal to cutoff must not be purged"
    );
    assert!(
        storage
            .get_cache_keepalive_session("future-expiry")
            .await
            .expect("load future row")
            .is_some()
    );
    assert_eq!(
        storage
            .purge_cache_keepalive_expired(401)
            .await
            .expect("purge exact boundary after advancing cutoff"),
        1
    );
    assert!(
        storage
            .get_cache_keepalive_session("exact-cutoff-expiry")
            .await
            .expect("load exact row after advancing cutoff")
            .is_none()
    );
    assert!(
        storage
            .get_cache_keepalive_session("future-expiry")
            .await
            .expect("load future row after purge")
            .is_some()
    );
}

#[tokio::test]
async fn purge_stale_pending_uses_strict_cutoff_and_state_predicates() {
    let (_dir, storage) = storage().await;
    for key in [
        "stale-pending-boundary",
        "exact-pending-boundary",
        "running-old",
        "terminal-old",
        "enqueued-old",
    ] {
        let mut request = replace_request(key.as_bytes(), 100);
        request.session_key_hash = key.to_owned();
        storage
            .replace_from_real_request(&request)
            .await
            .expect("insert pending predicate session");
    }
    for (key, status, enqueue_state, lease) in [
        ("stale-pending-boundary", "active", "pending", None),
        ("exact-pending-boundary", "active", "pending", None),
        ("running-old", "active", "running", Some(199_i64)),
        ("terminal-old", "terminal", "pending", None),
        ("enqueued-old", "active", "enqueued", None),
    ] {
        sqlx::query(
            "UPDATE cache_keepalive_sessions
             SET status = ?, enqueue_state = ?, running_since_unix_secs = ?, updated_at = ?
             WHERE session_key_hash = ?",
        )
        .bind(status)
        .bind(enqueue_state)
        .bind(lease)
        .bind(if key == "exact-pending-boundary" {
            200_i64
        } else {
            199_i64
        })
        .bind(key)
        .execute(storage.pool())
        .await
        .expect("seed pending predicate state");
    }

    assert_eq!(
        storage
            .purge_cache_keepalive_stale_pending(200)
            .await
            .expect("purge stale pending"),
        1
    );
    assert!(
        storage
            .get_cache_keepalive_session("stale-pending-boundary")
            .await
            .expect("load stale pending")
            .is_none()
    );
    for (key, expected_status, expected_enqueue_state) in [
        (
            "exact-pending-boundary",
            CacheKeepaliveSessionStatus::Active,
            CacheKeepaliveEnqueueState::Pending,
        ),
        (
            "running-old",
            CacheKeepaliveSessionStatus::Active,
            CacheKeepaliveEnqueueState::Running,
        ),
        (
            "terminal-old",
            CacheKeepaliveSessionStatus::Terminal,
            CacheKeepaliveEnqueueState::Pending,
        ),
        (
            "enqueued-old",
            CacheKeepaliveSessionStatus::Active,
            CacheKeepaliveEnqueueState::Enqueued,
        ),
    ] {
        let persisted = storage
            .get_cache_keepalive_session(key)
            .await
            .expect("load excluded pending predicate state")
            .expect("excluded state remains persisted");
        assert_eq!(persisted.status, expected_status);
        assert_eq!(persisted.enqueue_state, expected_enqueue_state);
    }
}

#[tokio::test]
async fn concurrent_recovery_counts_once_and_replacement_reschedule_preserve_latest_active_payload()
{
    let (_dir, storage) = storage().await;
    let storage = Arc::new(storage);
    let running = storage
        .replace_from_real_request(&replace_request(b"concurrent-running", 100))
        .await
        .expect("insert concurrent recovery session");
    assert!(
        storage
            .mark_cache_keepalive_enqueued("session-hash", running.generation, 110)
            .await
            .expect("mark concurrent session enqueued")
    );
    assert!(
        storage
            .claim_cache_keepalive_turn("session-hash", running.generation, 150)
            .await
            .expect("claim concurrent session")
    );
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let first = tokio::spawn({
        let storage = Arc::clone(&storage);
        let barrier = Arc::clone(&barrier);
        async move {
            barrier.wait().await;
            storage
                .recover_cache_keepalive_stale_running(200, 500)
                .await
        }
    });
    let second = tokio::spawn({
        let storage = Arc::clone(&storage);
        let barrier = Arc::clone(&barrier);
        async move {
            barrier.wait().await;
            storage
                .recover_cache_keepalive_stale_running(200, 500)
                .await
        }
    });
    let (first, second) = tokio::try_join!(first, second).expect("concurrent recovery tasks");
    let first = first.expect("first recovery");
    let second = second.expect("second recovery");
    assert_eq!(first + second, 1);
    let recovered = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load recovered concurrent session")
        .expect("recovered concurrent session exists");
    assert_eq!(recovered.status, CacheKeepaliveSessionStatus::Terminal);
    assert!(recovered.encrypted_payload.is_empty());

    let latest_key = "latest-before-recovery";
    let mut previous_request = replace_request(b"previous-generation", 100);
    previous_request.session_key_hash = latest_key.to_owned();
    let previous = storage
        .replace_from_real_request(&previous_request)
        .await
        .expect("insert previous active generation");
    assert!(
        storage
            .mark_cache_keepalive_enqueued(latest_key, previous.generation, 110)
            .await
            .expect("mark previous generation enqueued")
    );
    assert!(
        storage
            .claim_cache_keepalive_turn(latest_key, previous.generation, 150)
            .await
            .expect("claim previous generation")
    );
    let mut latest_request = replace_request(b"latest-before-recovery", 600);
    latest_request.session_key_hash = latest_key.to_owned();
    let latest = storage
        .replace_from_real_request(&latest_request)
        .await
        .expect("replace with latest active generation");
    let refreshed = storage
        .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
            session_key_hash: latest_key.to_owned(),
            generation: latest.generation,
            cache_anchor_at_unix_secs: 650,
            run_at_unix_secs: 920,
            expires_at_unix_secs: 950,
            encrypted_payload: Some(b"latest-rescheduled-payload".to_vec()),
            now_unix_secs: 650,
        })
        .await
        .expect("reschedule latest active generation")
        .expect("latest active generation remains");
    assert_eq!(refreshed.generation, latest.generation + 1);
    assert_eq!(refreshed.encrypted_payload, b"latest-rescheduled-payload");
    assert_eq!(refreshed.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(refreshed.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert_eq!(
        storage
            .recover_cache_keepalive_stale_running(700, 800)
            .await
            .expect("recover after latest replacement"),
        0
    );
    assert_eq!(
        storage
            .get_cache_keepalive_session(latest_key)
            .await
            .expect("load latest after recovery")
            .expect("latest session exists"),
        refreshed
    );
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

#[tokio::test]
async fn cancellation_causes_roundtrip_and_replacement_clears_them() {
    let (_dir, storage) = storage().await;
    for (index, reason) in [
        None,
        Some("new_request"),
        Some("user_turn_detected"),
        Some("snapshot_too_large"),
        Some("cached_prefix_too_small"),
        Some("zero_token_renewal_unsupported"),
    ]
    .into_iter()
    .enumerate()
    {
        let now = 100 + index as u64 * 10;
        let active = storage
            .replace_from_real_request(&replace_request(b"prefix", now))
            .await
            .expect("replace");
        assert_eq!(active.cancel_reason, None);
        assert!(
            storage
                .cancel_cache_keepalive_fenced(
                    "session-hash",
                    None,
                    now,
                    CacheKeepaliveTerminalReason::Cancelled,
                    reason,
                    now + 1
                )
                .await
                .expect("cancel")
        );
        let terminal = storage
            .get_cache_keepalive_session("session-hash")
            .await
            .expect("get")
            .expect("session");
        assert_eq!(terminal.cancel_reason.as_deref(), reason);
        assert_eq!(
            terminal.terminal_reason,
            Some(CacheKeepaliveTerminalReason::Cancelled)
        );
        assert!(
            !storage
                .cancel_cache_keepalive_fenced(
                    "session-hash",
                    None,
                    now,
                    CacheKeepaliveTerminalReason::Cancelled,
                    Some("new_request"),
                    now + 2
                )
                .await
                .expect("duplicate cancellation")
        );
        assert_eq!(
            storage
                .get_cache_keepalive_session("session-hash")
                .await
                .expect("get")
                .expect("session")
                .cancel_reason
                .as_deref(),
            reason,
            "a stale cancellation cannot change the cause"
        );
    }
}
