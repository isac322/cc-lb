use std::{
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use cc_lb_clock::SystemClock;
use cc_lb_storage_api::{
    CacheKeepaliveConfigSnapshot, CacheKeepaliveEnqueueState, CacheKeepaliveHitRefreshRequest,
    CacheKeepaliveReplaceRequest, CacheKeepaliveSessionRecord, CacheKeepaliveSessionStatus,
    CacheKeepaliveSessionStore, CacheKeepaliveTerminalReason, CacheTtl, MetaStore,
    cache_keepalive_job_key,
};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use tempfile::TempDir;
use tokio::sync::Notify;
use uuid::Uuid;

use crate::SqliteStorage;

#[derive(Default)]
struct AfterReleaseGate {
    armed: AtomicBool,
    entered: Notify,
    release: Notify,
}

impl AfterReleaseGate {
    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    fn release(&self) {
        self.release.notify_one();
    }

    async fn wait_until_entered(&self) -> bool {
        tokio::time::timeout(Duration::from_secs(15), self.entered.notified())
            .await
            .is_ok()
    }
}

async fn storages() -> (TempDir, SqliteStorage, SqliteStorage, Arc<AfterReleaseGate>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!("sqlite://{}", dir.path().join("keepalive.sqlite").display());
    let options = SqliteConnectOptions::from_str(&database_url)
        .expect("sqlite options")
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .foreign_keys(true)
        .busy_timeout(Duration::from_secs(5));
    let gate = Arc::new(AfterReleaseGate::default());
    let gate_for_pool = Arc::clone(&gate);
    let pool_a = SqlitePoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        .after_release(move |connection, _metadata| {
            let gate = Arc::clone(&gate_for_pool);
            Box::pin(async move {
                if !gate.armed.load(Ordering::SeqCst) {
                    return Ok(true);
                }
                let generation = sqlx::query_scalar::<_, i64>(
                    "SELECT generation FROM cache_keepalive_sessions WHERE session_key_hash = 'session-hash'",
                )
                .fetch_optional(&mut *connection)
                .await?;
                if generation == Some(2) && gate.armed.swap(false, Ordering::SeqCst) {
                    gate.entered.notify_one();
                    gate.release.notified().await;
                }
                Ok(true)
            })
        })
        .connect_with(options.clone())
        .await
        .expect("open adapter A pool");
    let pool_b = SqlitePoolOptions::new()
        .max_connections(4)
        .min_connections(1)
        .connect_with(options)
        .await
        .expect("open adapter B pool");
    let storage_a = SqliteStorage::new(pool_a, Arc::new(SystemClock));
    storage_a.initialize().await.expect("initialize sqlite");
    let storage_b = SqliteStorage::new(pool_b, Arc::new(SystemClock));
    (dir, storage_a, storage_b, gate)
}

fn replacement(
    key: &str,
    payload: &[u8],
    now: u64,
    principal_id: &str,
    upstream_id: u128,
    accounting_key_id: Option<&str>,
    refresh_lead_time_5m_secs: u32,
) -> CacheKeepaliveReplaceRequest {
    CacheKeepaliveReplaceRequest {
        session_key_hash: key.to_owned(),
        principal_id: principal_id.to_owned(),
        accounting_key_id: accounting_key_id.map(str::to_owned),
        upstream_id: Uuid::from_u128(upstream_id),
        cycle_id: Uuid::nil(),
        cache_anchor_at_unix_secs: now,
        ttl: CacheTtl::Ttl5m,
        run_at_unix_secs: now + 270,
        expires_at_unix_secs: now + 300,
        encrypted_payload: payload.to_vec(),
        display_reason: format!("display-{principal_id}"),
        config_snapshot: CacheKeepaliveConfigSnapshot {
            refresh_lead_time_5m_secs,
            refresh_lead_time_1h_secs: refresh_lead_time_5m_secs * 10,
            max_refreshes_per_session: 12,
            max_total_duration_secs: 14_400,
            snapshot_max_bytes: 524_288,
        },
        now_unix_secs: now,
    }
}

fn seed_request() -> CacheKeepaliveReplaceRequest {
    replacement(
        "session-hash",
        b"seed-ciphertext",
        100,
        "principal-seed",
        7,
        Some("accounting-seed"),
        31,
    )
}

fn a_request() -> CacheKeepaliveReplaceRequest {
    replacement(
        "session-hash",
        b"",
        150,
        "principal-a",
        8,
        Some("accounting-a"),
        32,
    )
}

fn b_request() -> CacheKeepaliveReplaceRequest {
    replacement(
        "session-hash",
        b"",
        200,
        "principal-b",
        9,
        Some("accounting-b"),
        33,
    )
}

fn hit_request(
    generation: u64,
    encrypted_payload: Option<Vec<u8>>,
) -> CacheKeepaliveHitRefreshRequest {
    CacheKeepaliveHitRefreshRequest {
        session_key_hash: "session-hash".to_owned(),
        generation,
        cache_anchor_at_unix_secs: 150,
        run_at_unix_secs: 420,
        expires_at_unix_secs: 450,
        encrypted_payload,
        now_unix_secs: 151,
    }
}

fn assert_identity(record: &CacheKeepaliveSessionRecord, request: &CacheKeepaliveReplaceRequest) {
    assert_eq!(record.session_key_hash, request.session_key_hash);
    assert_eq!(record.principal_id, request.principal_id);
    assert_eq!(record.accounting_key_id, request.accounting_key_id);
    assert_eq!(record.upstream_id, request.upstream_id);
    assert_eq!(record.ttl, request.ttl);
    assert_eq!(record.display_reason, request.display_reason);
    assert_eq!(
        record.config_snapshot.as_ref(),
        Some(&request.config_snapshot)
    );
    assert_eq!(
        record.first_scheduled_at_unix_secs,
        request.cache_anchor_at_unix_secs
    );
    assert_eq!(
        record.cache_anchor_at_unix_secs,
        request.cache_anchor_at_unix_secs
    );
    assert_eq!(record.run_at_unix_secs, request.run_at_unix_secs);
    assert_eq!(record.expires_at_unix_secs, request.expires_at_unix_secs);
    // Payload is checked separately because the owner may publish ciphertext after the snapshot.
}
fn assert_hit_snapshot(
    record: &CacheKeepaliveSessionRecord,
    seed: &CacheKeepaliveSessionRecord,
    expected_payload: &[u8],
) {
    assert_eq!(record.generation, 2);
    assert_eq!(record.refresh_count, 1);
    assert_eq!(record.principal_id, seed.principal_id);
    assert_eq!(record.session_key_hash, seed.session_key_hash);
    assert_eq!(record.ttl, seed.ttl);
    assert_eq!(record.display_reason, seed.display_reason);
    assert_eq!(record.accounting_key_id, seed.accounting_key_id);
    assert_eq!(record.upstream_id, seed.upstream_id);
    assert_eq!(record.config_snapshot, seed.config_snapshot);
    assert_eq!(
        record.first_scheduled_at_unix_secs,
        seed.first_scheduled_at_unix_secs
    );
    assert_eq!(record.cache_anchor_at_unix_secs, 150);
    assert_eq!(record.run_at_unix_secs, 420);
    assert_eq!(record.expires_at_unix_secs, 450);
    assert_eq!(
        record.current_job_key,
        cache_keepalive_job_key("session-hash", 2)
    );
    assert_eq!(record.encrypted_payload, expected_payload);
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(record.enqueue_state, CacheKeepaliveEnqueueState::Pending);
}

async fn run_hit_supersession_case(encrypted_payload: Option<Vec<u8>>) {
    let (_dir, storage_a, storage_b, gate) = storages().await;
    let seed = storage_b
        .replace_from_real_request(&seed_request())
        .await
        .expect("seed real request");
    let expected_payload = encrypted_payload
        .clone()
        .unwrap_or_else(|| seed.encrypted_payload.clone());
    let generation = seed.generation;
    gate.arm();
    let task_storage = storage_a.clone();
    let task = tokio::spawn(async move {
        task_storage
            .reschedule_after_cache_hit(&hit_request(generation, encrypted_payload))
            .await
    });
    if !gate.wait_until_entered().await {
        gate.release();
        task.abort();
        panic!("pool gate did not observe generation 2 after A's committed write");
    }

    let committed_result = storage_b.get_cache_keepalive_session("session-hash").await;
    let b_result = storage_b.replace_from_real_request(&b_request()).await;
    gate.release();
    let committed = committed_result
        .expect("read committed hit row")
        .expect("committed hit row exists");
    assert_eq!(committed.generation, 2);
    assert_eq!(committed.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(committed.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert_eq!(committed.refresh_count, 1);
    assert_eq!(committed.first_scheduled_at_unix_secs, 100);
    assert_eq!(committed.cache_anchor_at_unix_secs, 150);
    assert_eq!(committed.run_at_unix_secs, 420);
    assert_eq!(committed.expires_at_unix_secs, 450);
    assert_eq!(
        committed.current_job_key,
        cache_keepalive_job_key("session-hash", 2)
    );
    assert_hit_snapshot(&committed, &seed, &expected_payload);
    let b = b_result.expect("B replacement");
    let a = tokio::time::timeout(Duration::from_secs(15), task)
        .await
        .expect("A task did not finish after gate release")
        .expect("A task joins")
        .expect("A hit reschedule")
        .expect("A returns its row");

    assert_eq!(a, committed);
    assert_eq!(b.generation, 3);
    assert_eq!(
        b.current_job_key,
        cache_keepalive_job_key("session-hash", 3)
    );
    assert_identity(&b, &b_request());
    assert_eq!(b.encrypted_payload, b_request().encrypted_payload);
    assert!(
        !storage_b
            .mark_cache_keepalive_enqueued("session-hash", a.generation, 202)
            .await
            .expect("stale enqueue")
    );
    assert!(
        !storage_b
            .mark_cache_keepalive_terminal(
                "session-hash",
                a.generation,
                CacheKeepaliveTerminalReason::CacheMiss,
                None,
                203,
            )
            .await
            .expect("stale terminal")
    );
    assert!(
        storage_b
            .update_cache_keepalive_payload("session-hash", b.generation, b"B-ciphertext", 204)
            .await
            .expect("B payload update")
    );
    let current = storage_b
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("read current row")
        .expect("current row exists");
    assert_eq!(current.generation, b.generation);
    assert_eq!(current.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert_eq!(
        current.current_job_key,
        cache_keepalive_job_key("session-hash", 3)
    );
    assert_eq!(current.encrypted_payload, b"B-ciphertext");
    assert_identity(&current, &b_request());
}

#[tokio::test]
async fn hit_reschedule_keeps_result_owned_when_superseded() {
    run_hit_supersession_case(None).await;
    run_hit_supersession_case(Some(b"hit-rewrite-ciphertext".to_vec())).await;
}

async fn run_hit_cancellation_case(encrypted_payload: Option<Vec<u8>>) {
    let (_dir, storage_a, storage_b, gate) = storages().await;
    let seed = storage_b
        .replace_from_real_request(&seed_request())
        .await
        .expect("seed real request");
    let expected_payload = encrypted_payload
        .clone()
        .unwrap_or_else(|| seed.encrypted_payload.clone());
    let generation = seed.generation;
    gate.arm();
    let task_storage = storage_a.clone();
    let task = tokio::spawn(async move {
        task_storage
            .reschedule_after_cache_hit(&hit_request(generation, encrypted_payload))
            .await
    });
    if !gate.wait_until_entered().await {
        gate.release();
        task.abort();
        panic!("pool gate did not observe generation 2 after A's committed write");
    }

    let committed_result = storage_b.get_cache_keepalive_session("session-hash").await;
    let cancel_anchor = match &committed_result {
        Ok(Some(record)) => Some(record.cache_anchor_at_unix_secs),
        _ => None,
    };
    let cancelled_result = match cancel_anchor {
        Some(anchor) => Some(
            storage_b
                .cancel_cache_keepalive_fenced(
                    "session-hash",
                    None,
                    anchor,
                    CacheKeepaliveTerminalReason::Cancelled,
                    None,
                    152,
                )
                .await,
        ),
        None => None,
    };
    gate.release();
    let committed = committed_result
        .expect("read committed hit row")
        .expect("committed hit row exists");
    let cancelled = cancelled_result
        .expect("cancellation setup ran")
        .expect("same-generation cancellation");
    assert_hit_snapshot(&committed, &seed, &expected_payload);
    let a = tokio::time::timeout(Duration::from_secs(15), task)
        .await
        .expect("A task did not finish after gate release")
        .expect("A task joins")
        .expect("A hit reschedule")
        .expect("A returns its row");
    assert_eq!(a, committed);
    assert!(cancelled);
    assert_eq!(a.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(a.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert!(
        !storage_b
            .mark_cache_keepalive_enqueued("session-hash", a.generation, 153)
            .await
            .expect("stale enqueue after cancellation")
    );
    assert!(
        !storage_b
            .mark_cache_keepalive_terminal(
                "session-hash",
                a.generation,
                CacheKeepaliveTerminalReason::CacheMiss,
                None,
                154,
            )
            .await
            .expect("stale terminal after cancellation")
    );
    assert!(
        !storage_b
            .update_cache_keepalive_payload("session-hash", a.generation, b"late", 155)
            .await
            .expect("stale payload after cancellation")
    );
    let current = storage_b
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("read cancelled row")
        .expect("cancelled row exists");
    assert_eq!(current.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(current.generation, a.generation);
    assert_eq!(
        current.terminal_reason,
        Some(CacheKeepaliveTerminalReason::Cancelled)
    );
    assert!(current.encrypted_payload.is_empty());
}

#[tokio::test]
async fn hit_reschedule_keeps_result_owned_when_cancelled() {
    run_hit_cancellation_case(None).await;
    run_hit_cancellation_case(Some(b"hit-rewrite-ciphertext".to_vec())).await;
}

async fn run_real_replacement_case(ticketed: bool) {
    let (_dir, storage_a, storage_b, gate) = storages().await;
    let seed = storage_b
        .replace_from_real_request(&seed_request())
        .await
        .expect("seed real request");
    assert_eq!(seed.generation, 1);
    let a_input = a_request();
    let b_input = b_request();
    let a_ticket = if ticketed {
        Some(
            storage_b
                .allocate_cache_keepalive_completion_ticket("session-hash")
                .await
                .expect("A completion ticket"),
        )
    } else {
        None
    };
    let b_ticket = if ticketed {
        Some(
            storage_b
                .allocate_cache_keepalive_completion_ticket("session-hash")
                .await
                .expect("B completion ticket"),
        )
    } else {
        None
    };
    gate.arm();
    let task_storage = storage_a.clone();
    let task_input = a_input.clone();
    let task = tokio::spawn(async move {
        match a_ticket {
            Some(ticket) => {
                task_storage
                    .replace_from_real_request_fenced(&task_input, Some(ticket))
                    .await
            }
            None => task_storage
                .replace_from_real_request(&task_input)
                .await
                .map(Some),
        }
    });
    if !gate.wait_until_entered().await {
        gate.release();
        task.abort();
        panic!("pool gate did not observe generation 2 after A's committed write");
    }

    let committed_result = storage_b.get_cache_keepalive_session("session-hash").await;
    let b_result = match b_ticket {
        Some(ticket) => {
            storage_b
                .replace_from_real_request_fenced(&b_input, Some(ticket))
                .await
        }
        None => storage_b
            .replace_from_real_request(&b_input)
            .await
            .map(Some),
    };
    gate.release();
    let committed = committed_result
        .expect("read committed replacement row")
        .expect("committed replacement row exists");
    let b = b_result.expect("B replacement").expect("B replacement row");
    assert_eq!(committed.generation, 2);
    assert_eq!(
        committed.current_job_key,
        cache_keepalive_job_key("session-hash", 2)
    );
    assert_identity(&committed, &a_input);
    assert_eq!(committed.encrypted_payload, a_input.encrypted_payload);
    let a = tokio::time::timeout(Duration::from_secs(15), task)
        .await
        .expect("A task did not finish after gate release")
        .expect("A task joins")
        .expect("A replacement")
        .expect("A returns its row");

    assert_eq!(a, committed);
    assert_eq!(
        a.current_job_key,
        cache_keepalive_job_key("session-hash", 2)
    );
    assert_eq!(b.generation, 3);
    assert_identity(&b, &b_input);
    assert_eq!(b.encrypted_payload, b_input.encrypted_payload);
    assert!(
        !storage_b
            .update_cache_keepalive_payload("session-hash", a.generation, b"A-ciphertext", 201)
            .await
            .expect("stale A payload")
    );
    assert!(
        !storage_b
            .mark_cache_keepalive_enqueued("session-hash", a.generation, 202)
            .await
            .expect("stale A enqueue")
    );
    assert!(
        !storage_b
            .mark_cache_keepalive_terminal(
                "session-hash",
                a.generation,
                CacheKeepaliveTerminalReason::DispatchError,
                None,
                203,
            )
            .await
            .expect("stale A terminal")
    );
    assert!(
        storage_b
            .update_cache_keepalive_payload("session-hash", b.generation, b"B-ciphertext", 204)
            .await
            .expect("B payload update")
    );
    let current = storage_b
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("read current row")
        .expect("current row exists");
    assert_eq!(current.generation, b.generation);
    assert_eq!(current.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert_eq!(current.encrypted_payload, b"B-ciphertext");
    assert_eq!(
        current.current_job_key,
        cache_keepalive_job_key("session-hash", 3)
    );
    assert_identity(&current, &b_input);
}

#[tokio::test]
async fn real_replacement_keeps_result_owned_when_superseded() {
    run_real_replacement_case(true).await;
    run_real_replacement_case(false).await;
}

async fn run_hit_negative_cases(encrypted_payload: Option<Vec<u8>>) {
    let (_dir, storage, _other, _gate) = storages().await;
    assert!(
        storage
            .reschedule_after_cache_hit(&hit_request(1, encrypted_payload.clone()))
            .await
            .expect("missing row no-op")
            .is_none()
    );
    assert!(
        storage
            .get_cache_keepalive_session("session-hash")
            .await
            .expect("read missing row")
            .is_none()
    );

    let seed = storage
        .replace_from_real_request(&seed_request())
        .await
        .expect("seed real request");
    let before_stale = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("read before stale")
        .expect("row before stale");
    assert!(
        storage
            .reschedule_after_cache_hit(&hit_request(
                seed.generation + 1,
                encrypted_payload.clone()
            ))
            .await
            .expect("stale row no-op")
            .is_none()
    );
    let after_stale = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("read after stale")
        .expect("row after stale");
    assert_eq!(after_stale, before_stale);

    assert!(
        storage
            .mark_cache_keepalive_terminal(
                "session-hash",
                seed.generation,
                CacheKeepaliveTerminalReason::CacheMiss,
                None,
                102,
            )
            .await
            .expect("terminalize row")
    );
    let before_terminal = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("read before terminal branch")
        .expect("terminal row");
    assert!(
        storage
            .reschedule_after_cache_hit(&hit_request(seed.generation, encrypted_payload))
            .await
            .expect("terminal row no-op")
            .is_none()
    );
    let after_terminal = storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("read after terminal branch")
        .expect("terminal row remains");
    assert_eq!(after_terminal, before_terminal);
}

#[tokio::test]
async fn hit_reschedule_missing_stale_and_terminal_are_noops_for_both_payload_branches() {
    run_hit_negative_cases(None).await;
    run_hit_negative_cases(Some(b"hit-rewrite-ciphertext".to_vec())).await;
}
