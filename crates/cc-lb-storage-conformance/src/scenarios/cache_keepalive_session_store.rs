use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::{
    CacheKeepaliveEnqueueState, CacheKeepaliveHitRefreshRequest, CacheKeepaliveReplaceRequest,
    CacheKeepaliveSessionStatus, CacheKeepaliveSessionStore, CacheKeepaliveTerminalReason,
    CacheTtl, cache_keepalive_job_key,
};
use tokio::sync::Barrier;
use uuid::Uuid;

use crate::harness::{ConformanceBackend, with_conformance_fixture};

pub async fn run_all<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    replace_from_real_request_bumps_generation_and_resets_counters(Arc::clone(&backend)).await?;
    replace_roundtrips_optional_accounting_key_id(Arc::clone(&backend)).await?;
    update_payload_only_mutates_matching_pending_generation(Arc::clone(&backend)).await?;
    cache_hit_reschedule_preserves_duration_anchor_and_rejects_stale_generation(Arc::clone(
        &backend,
    ))
    .await?;
    conditional_enqueue_terminal_and_purge_are_generation_safe(Arc::clone(&backend)).await?;
    mark_latest_terminal_only_mutates_active_latest_session(Arc::clone(&backend)).await?;
    purge_stale_pending_keeps_enqueued_work(Arc::clone(&backend)).await?;
    concurrent_claim_allows_exactly_one_enqueued_generation_winner(backend).await
}

pub async fn replace_from_real_request_bumps_generation_and_resets_counters<B>(
    backend: Arc<B>,
) -> Result<()>
where
    B: ConformanceBackend,
{
    with_conformance_fixture(backend, |storage| async move {
        let first = storage
            .replace_from_real_request(&replace_request("replace-session", b"ciphertext-one", 100))
            .await?;
        let second = storage
            .replace_from_real_request(&replace_request("replace-session", b"ciphertext-two", 200))
            .await?;

        ensure!(first.generation == 1, "first generation must be 1");
        ensure!(second.generation == 2, "replacement generation must be 2");
        ensure!(
            second.refresh_count == 0,
            "replacement must reset refresh_count"
        );
        ensure!(
            second.first_scheduled_at_unix_secs == 200,
            "replacement must reset the duration anchor"
        );
        ensure!(
            second.cache_anchor_at_unix_secs == 200,
            "replacement must store the new cache anchor"
        );
        ensure!(
            second.run_at_unix_secs == 470,
            "replacement must store the new run time"
        );
        ensure!(
            second.current_job_key == cache_keepalive_job_key("replace-session", 2),
            "replacement job key must use the new generation"
        );
        ensure!(
            second.encrypted_payload == b"ciphertext-two",
            "replacement must store the new encrypted payload"
        );
        Ok(())
    })
    .await
}

pub async fn replace_roundtrips_optional_accounting_key_id<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    with_conformance_fixture(backend, |storage| async move {
        let with_key = CacheKeepaliveReplaceRequest {
            accounting_key_id: Some("key-live-123".to_owned()),
            ..replace_request("keyed-session", b"ciphertext-with-key", 100)
        };
        let without_key = replace_request("unkeyed-session", b"ciphertext-without-key", 110);

        let with_key = storage.replace_from_real_request(&with_key).await?;
        let without_key = storage.replace_from_real_request(&without_key).await?;

        ensure!(
            with_key.accounting_key_id.as_deref() == Some("key-live-123"),
            "accounting key id must round-trip when present"
        );
        ensure!(
            without_key.accounting_key_id.is_none(),
            "accounting key id must remain absent"
        );

        for session_key_hash in ["keyed-session", "unkeyed-session"] {
            ensure!(
                storage
                    .mark_latest_cache_keepalive_terminal(
                        session_key_hash,
                        CacheKeepaliveTerminalReason::Cancelled,
                        120,
                    )
                    .await?,
                "active optional-accounting session {session_key_hash} must terminalize"
            );
        }
        Ok(())
    })
    .await
}

pub async fn update_payload_only_mutates_matching_pending_generation<B>(
    backend: Arc<B>,
) -> Result<()>
where
    B: ConformanceBackend,
{
    with_conformance_fixture(backend, |storage| async move {
        let record = storage
            .replace_from_real_request(&replace_request(
                "payload-session",
                b"pending-placeholder",
                100,
            ))
            .await?;

        ensure!(
            !storage
                .update_cache_keepalive_payload(
                    "payload-session",
                    record.generation + 1,
                    b"stale",
                    101,
                )
                .await?,
            "stale generation payload update must be a no-op"
        );
        ensure!(
            storage
                .update_cache_keepalive_payload(
                    "payload-session",
                    record.generation,
                    b"ciphertext",
                    102,
                )
                .await?,
            "matching pending generation must accept a payload update"
        );

        let updated = storage
            .get_cache_keepalive_session("payload-session")
            .await?
            .expect("payload session must exist");
        ensure!(
            updated.encrypted_payload == b"ciphertext",
            "fresh payload must be stored exactly"
        );
        ensure!(
            updated.enqueue_state == CacheKeepaliveEnqueueState::Pending,
            "payload update must leave the session pending"
        );

        ensure!(
            storage
                .mark_cache_keepalive_enqueued("payload-session", record.generation, 103)
                .await?,
            "pending session must enqueue"
        );
        ensure!(
            !storage
                .update_cache_keepalive_payload("payload-session", record.generation, b"late", 104,)
                .await?,
            "enqueued session payload update must be a no-op"
        );
        Ok(())
    })
    .await
}

pub async fn cache_hit_reschedule_preserves_duration_anchor_and_rejects_stale_generation<B>(
    backend: Arc<B>,
) -> Result<()>
where
    B: ConformanceBackend,
{
    with_conformance_fixture(backend, |storage| async move {
        let original = storage
            .replace_from_real_request(&replace_request("hit-session", b"ciphertext-one", 100))
            .await?;

        let stale = storage
            .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
                session_key_hash: "hit-session".to_owned(),
                generation: original.generation + 1,
                cache_anchor_at_unix_secs: 150,
                run_at_unix_secs: 420,
                expires_at_unix_secs: 450,
                encrypted_payload: None,
                now_unix_secs: 151,
            })
            .await?;
        ensure!(stale.is_none(), "stale reschedule must be a no-op");

        let updated = storage
            .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
                session_key_hash: "hit-session".to_owned(),
                generation: original.generation,
                cache_anchor_at_unix_secs: 150,
                run_at_unix_secs: 420,
                expires_at_unix_secs: 450,
                encrypted_payload: None,
                now_unix_secs: 151,
            })
            .await?
            .expect("matching active session must reschedule");

        ensure!(updated.generation == 2, "reschedule must bump generation");
        ensure!(
            updated.refresh_count == 1,
            "reschedule must increment refresh_count"
        );
        ensure!(
            updated.first_scheduled_at_unix_secs == 100,
            "reschedule must preserve first_scheduled_at"
        );
        ensure!(
            updated.cache_anchor_at_unix_secs == 150,
            "reschedule must store the new cache anchor"
        );
        ensure!(
            updated.run_at_unix_secs == 420,
            "reschedule must store the new run time"
        );
        ensure!(
            updated.encrypted_payload == b"ciphertext-one",
            "reschedule without payload must preserve the encrypted payload"
        );

        let migrated = storage
            .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
                session_key_hash: "hit-session".to_owned(),
                generation: updated.generation,
                cache_anchor_at_unix_secs: 151,
                run_at_unix_secs: 421,
                expires_at_unix_secs: 451,
                encrypted_payload: Some(b"compressed-ciphertext".to_vec()),
                now_unix_secs: 152,
            })
            .await?
            .expect("matching active session must migrate payload");
        ensure!(
            migrated.generation == 3,
            "payload migration must bump generation"
        );
        ensure!(
            migrated.refresh_count == 2,
            "payload migration must increment refresh_count"
        );
        ensure!(
            migrated.encrypted_payload == b"compressed-ciphertext",
            "payload migration must replace encrypted bytes exactly"
        );
        Ok(())
    })
    .await
}

pub async fn conditional_enqueue_terminal_and_purge_are_generation_safe<B>(
    backend: Arc<B>,
) -> Result<()>
where
    B: ConformanceBackend,
{
    with_conformance_fixture(backend, |storage| async move {
        let record = storage
            .replace_from_real_request(&replace_request("terminal-session", b"ciphertext", 100))
            .await?;

        ensure!(
            storage
                .mark_cache_keepalive_enqueued("terminal-session", record.generation, 101)
                .await?,
            "matching pending generation must enqueue"
        );
        ensure!(
            !storage
                .mark_cache_keepalive_terminal(
                    "terminal-session",
                    record.generation + 1,
                    CacheKeepaliveTerminalReason::CacheMiss,
                    102,
                )
                .await?,
            "stale generation terminal transition must be a no-op"
        );
        ensure!(
            storage
                .mark_cache_keepalive_terminal(
                    "terminal-session",
                    record.generation,
                    CacheKeepaliveTerminalReason::CacheMiss,
                    102,
                )
                .await?,
            "matching active generation must terminalize"
        );

        let terminal = storage
            .get_cache_keepalive_session("terminal-session")
            .await?
            .expect("terminal session must exist");
        ensure!(
            terminal.encrypted_payload.is_empty(),
            "terminal transition must clear encrypted payload"
        );
        let check = storage
            .check_cache_keepalive_generation("terminal-session")
            .await?
            .expect("terminal generation check must exist");
        ensure!(
            check.status == CacheKeepaliveSessionStatus::Terminal,
            "generation check must expose terminal status"
        );
        ensure!(
            check.enqueue_state == CacheKeepaliveEnqueueState::Enqueued,
            "terminal transition must preserve enqueue state"
        );

        let removed = storage.purge_cache_keepalive_expired(401).await?;
        ensure!(
            removed == 1,
            "expired purge must remove exactly one session"
        );
        ensure!(
            storage
                .get_cache_keepalive_session("terminal-session")
                .await?
                .is_none(),
            "expired purged session must be absent"
        );
        Ok(())
    })
    .await
}

pub async fn mark_latest_terminal_only_mutates_active_latest_session<B>(
    backend: Arc<B>,
) -> Result<()>
where
    B: ConformanceBackend,
{
    with_conformance_fixture(backend, |storage| async move {
        let first = storage
            .replace_from_real_request(&replace_request("latest-terminal", b"ciphertext-one", 100))
            .await?;
        let second = storage
            .replace_from_real_request(&replace_request("latest-terminal", b"ciphertext-two", 110))
            .await?;

        ensure!(first.generation == 1, "first generation must be 1");
        ensure!(second.generation == 2, "replacement generation must be 2");
        ensure!(
            storage
                .mark_latest_cache_keepalive_terminal(
                    "latest-terminal",
                    CacheKeepaliveTerminalReason::Cancelled,
                    120,
                )
                .await?,
            "latest active session must terminalize"
        );
        ensure!(
            !storage
                .mark_latest_cache_keepalive_terminal(
                    "latest-terminal",
                    CacheKeepaliveTerminalReason::CacheMiss,
                    121,
                )
                .await?,
            "already terminal latest session must be a no-op"
        );

        let record = storage
            .get_cache_keepalive_session("latest-terminal")
            .await?
            .expect("latest terminal session must exist");
        ensure!(record.generation == 2, "latest generation must remain 2");
        ensure!(
            record.status == CacheKeepaliveSessionStatus::Terminal,
            "latest session must be terminal"
        );
        ensure!(
            record.terminal_reason == Some(CacheKeepaliveTerminalReason::Cancelled),
            "first terminal reason must be preserved"
        );
        ensure!(
            record.encrypted_payload.is_empty(),
            "terminal session payload must be cleared"
        );
        Ok(())
    })
    .await
}

pub async fn purge_stale_pending_keeps_enqueued_work<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    with_conformance_fixture(backend, |storage| async move {
        let pending = storage
            .replace_from_real_request(&replace_request("stale-pending", b"pending", 100))
            .await?;
        let enqueued = storage
            .replace_from_real_request(&replace_request("old-enqueued", b"enqueued", 110))
            .await?;
        ensure!(
            storage
                .mark_cache_keepalive_enqueued("old-enqueued", enqueued.generation, 111)
                .await?,
            "second session must enqueue"
        );

        let removed = storage.purge_cache_keepalive_stale_pending(112).await?;

        ensure!(
            pending.enqueue_state == CacheKeepaliveEnqueueState::Pending,
            "seeded stale session must start pending"
        );
        ensure!(
            removed == 1,
            "stale pending purge must remove exactly one session"
        );
        ensure!(
            storage
                .get_cache_keepalive_session("stale-pending")
                .await?
                .is_none(),
            "stale pending session must be removed"
        );
        ensure!(
            storage
                .get_cache_keepalive_session("old-enqueued")
                .await?
                .is_some(),
            "enqueued work must survive stale pending purge"
        );
        Ok(())
    })
    .await
}

pub async fn concurrent_claim_allows_exactly_one_enqueued_generation_winner<B>(
    backend: Arc<B>,
) -> Result<()>
where
    B: ConformanceBackend,
{
    with_conformance_fixture(backend, |storage| async move {
        let record = storage
            .replace_from_real_request(&replace_request("concurrent-claim-cas", b"ciphertext", 100))
            .await?;
        ensure!(
            storage
                .mark_cache_keepalive_enqueued("concurrent-claim-cas", record.generation, 101,)
                .await?,
            "matching pending generation must enqueue before claim"
        );

        let barrier = Arc::new(Barrier::new(2));
        let first = tokio::spawn({
            let storage = Arc::clone(&storage);
            let barrier = Arc::clone(&barrier);
            async move {
                barrier.wait().await;
                storage
                    .claim_cache_keepalive_turn("concurrent-claim-cas", record.generation, 200)
                    .await
            }
        });
        let second = tokio::spawn({
            let storage = Arc::clone(&storage);
            let barrier = Arc::clone(&barrier);
            async move {
                barrier.wait().await;
                storage
                    .claim_cache_keepalive_turn("concurrent-claim-cas", record.generation, 200)
                    .await
            }
        });

        let (first, second) = tokio::join!(first, second);
        let first = first??;
        let second = second??;
        ensure!(
            u8::from(first) + u8::from(second) == 1,
            "exactly one concurrent claim must win"
        );

        let claimed = storage
            .get_cache_keepalive_session("concurrent-claim-cas")
            .await?
            .expect("claimed session must exist");
        ensure!(
            claimed.enqueue_state == CacheKeepaliveEnqueueState::Running,
            "winning claim must move session to running"
        );
        ensure!(
            claimed.running_since_unix_secs == Some(200),
            "winning claim must record running_since"
        );
        Ok(())
    })
    .await
}

fn replace_request(
    session_key_hash: &str,
    payload: &[u8],
    now: u64,
) -> CacheKeepaliveReplaceRequest {
    CacheKeepaliveReplaceRequest {
        session_key_hash: session_key_hash.to_owned(),
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
