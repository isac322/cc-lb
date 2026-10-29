use super::*;

use bytes::Bytes;
use cc_lb_clock::SystemClock;
use cc_lb_config::{SchedulerConfig, StorageConfig};
use cc_lb_engine::cache_keepalive::{
    CacheKeepaliveCancelRequest, CacheKeepaliveNotTrackedRequest, RequestSnapshot, ScheduleParams,
};
use cc_lb_scheduler::error::SchedulerError;
use cc_lb_scheduler::worker::{Filter, TaskStatus};
use cc_lb_storage_api::{
    CacheKeepaliveSessionFilter, CacheKeepaliveSessionListQuery, CacheKeepaliveSessionReadStore,
    CacheKeepaliveSessionStatus, CacheTtl, MetaStore,
};
use http::{HeaderMap, Method};
use tempfile::TempDir;
use url::Url;
use uuid::Uuid;

#[tokio::test]
async fn enqueue_persists_session_payload_and_apalis_job() {
    let fixture = Fixture::new().await;
    let backend = crate::scheduler_factory::open_scheduler_storage(
        &StorageConfig::Sqlite {
            path: fixture.sqlite_path.clone(),
        },
        &SchedulerConfig::default(),
        Arc::new(SystemClock),
    )
    .await
    .expect("open scheduler storage");
    let enqueuer = ServerCacheKeepaliveEnqueuer::new(ServerCacheKeepaliveEnqueuerDeps {
        storage: fixture.storage.clone(),
        pusher: Arc::new(backend.backend.clone()),
        aead: fixture.aead.clone(),
        clock: Arc::new(SystemClock),
    });

    enqueuer
        .enqueue_cache_keepalive(enqueue_request())
        .await
        .expect("enqueue cache keepalive");

    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load cache keepalive session")
        .expect("session exists");
    assert_eq!(record.generation, 1);
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(record.accounting_key_id.as_deref(), Some("key-live-123"));
    assert!(!record.encrypted_payload.is_empty());
    assert!(!String::from_utf8_lossy(&record.encrypted_payload).contains("cached prompt"));

    let jobs = backend
        .backend
        .list_keepalive_tasks(&Filter {
            status: Some(TaskStatus::Pending),
            page: 1,
            page_size: Some(10),
        })
        .await
        .expect("list cache keepalive jobs");
    assert_eq!(jobs.len(), 1);
    let job = &jobs[0];
    assert_eq!(
        job.idempotency_key.as_deref(),
        Some(record.current_job_key.as_str())
    );
    assert_eq!(job.max_attempts, 1);
    assert_eq!(job.run_at_unix_secs, record.run_at_unix_secs);
}

#[tokio::test]
async fn enqueue_failure_terminalizes_committed_pending_session() {
    let fixture = Fixture::new().await;
    let enqueuer = ServerCacheKeepaliveEnqueuer::new(ServerCacheKeepaliveEnqueuerDeps {
        storage: fixture.storage.clone(),
        pusher: Arc::new(FailingPusher),
        aead: fixture.aead.clone(),
        clock: Arc::new(SystemClock),
    });

    let result = enqueuer.enqueue_cache_keepalive(enqueue_request()).await;

    assert!(result.is_err());
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load cache keepalive session")
        .expect("session exists");
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::DispatchError)
    );
}

#[tokio::test]
async fn cancel_terminalizes_active_session_without_deleting_pending_job() {
    let fixture = Fixture::new().await;
    let backend = crate::scheduler_factory::open_scheduler_storage(
        &StorageConfig::Sqlite {
            path: fixture.sqlite_path.clone(),
        },
        &SchedulerConfig::default(),
        Arc::new(SystemClock),
    )
    .await
    .expect("open scheduler storage");
    let enqueuer = ServerCacheKeepaliveEnqueuer::new(ServerCacheKeepaliveEnqueuerDeps {
        storage: fixture.storage.clone(),
        pusher: Arc::new(backend.backend.clone()),
        aead: fixture.aead.clone(),
        clock: Arc::new(SystemClock),
    });
    enqueuer
        .enqueue_cache_keepalive(enqueue_request())
        .await
        .expect("enqueue cache keepalive");

    let observed = enqueuer
        .observe_session_generation("session-hash")
        .await
        .expect("observe generation");
    enqueuer
        .cancel_cache_keepalive(CacheKeepaliveCancelRequest {
            session_key_hash: "session-hash".to_owned(),
            reason: cc_lb_engine::cache_keepalive::CancelReason::UserTurnDetected,
            expected_generation: observed,
            cache_anchor_age: std::time::Duration::ZERO,
        })
        .await
        .expect("cancel cache keepalive");

    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load cache keepalive session")
        .expect("session exists");
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::Cancelled)
    );
    let jobs = backend
        .backend
        .list_keepalive_tasks(&Filter {
            status: Some(TaskStatus::Pending),
            page: 1,
            page_size: Some(10),
        })
        .await
        .expect("list cache keepalive jobs");
    assert_eq!(jobs.len(), 1);
}

#[tokio::test]
async fn not_tracked_writes_a_decision_projection_without_a_renewal_turn() {
    let fixture = Fixture::new().await;
    let enqueuer = ServerCacheKeepaliveEnqueuer::new(ServerCacheKeepaliveEnqueuerDeps {
        storage: fixture.storage.clone(),
        pusher: Arc::new(FailingPusher),
        aead: fixture.aead.clone(),
        clock: Arc::new(SystemClock),
    });
    let request = enqueue_request();

    assert!(
        enqueuer
            .enqueue_cache_keepalive(request.clone())
            .await
            .is_err()
    );

    enqueuer
        .record_cache_keepalive_not_tracked(CacheKeepaliveNotTrackedRequest {
            session_key_hash: request.session_key_hash,
            principal_id: request.principal_id,
            upstream_id: Uuid::from_u128(42),
            ttl: CacheTtl::Ttl5m,
            config_snapshot: request.config_snapshot,
            reason: "user turn (stop_reason=end_turn)".to_owned(),
        })
        .await
        .expect("write not-tracked projection");

    let page = fixture
        .storage
        .list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
            principal_id: "principal".to_owned(),
            horizon_start_ms: None,
            filter: CacheKeepaliveSessionFilter::NotTracked,
            cursor: None,
            limit: 10,
        })
        .await
        .expect("read not-tracked projection");
    assert_eq!(page.rows.len(), 1);
    assert!(page.rows[0].is_decision());
    assert_eq!(page.rows[0].reason, "user turn (stop_reason=end_turn)");
    let turns = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM cache_keepalive_turns")
        .fetch_one(fixture.storage.pool())
        .await
        .expect("count renewal turns");
    assert_eq!(turns, 0);
    let request_events = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM request_events_v1")
        .fetch_one(fixture.storage.pool())
        .await
        .expect("count request events");
    assert_eq!(request_events, 0);
    if let Ok(qa_database_path) = std::env::var("CC_LB_CACHE_KEEPALIVE_QA_DATABASE") {
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(fixture.storage.pool())
            .await
            .expect("checkpoint QA database");
        std::fs::copy(&fixture.sqlite_path, &qa_database_path).expect("copy QA database");
        eprintln!("cache keepalive QA database: {qa_database_path}");
    }
}

struct Fixture {
    _dir: TempDir,
    sqlite_path: std::path::PathBuf,
    storage: Arc<cc_lb_storage_sqlite::SqliteStorage>,
    aead: Arc<AeadService>,
}

impl Fixture {
    async fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let sqlite_path = dir.path().join("keepalive.sqlite");
        let database_url = format!("sqlite://{}", sqlite_path.display());
        let storage = Arc::new(
            cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(SystemClock))
                .await
                .expect("open sqlite"),
        );
        storage.initialize().await.expect("initialize sqlite");
        Self {
            _dir: dir,
            sqlite_path,
            storage,
            aead: Arc::new(AeadService::from_master_key([7; 32])),
        }
    }
}

struct FailingPusher;

#[async_trait]
impl CacheKeepaliveTaskPusher for FailingPusher {
    async fn push_cache_keepalive_task(
        &self,
        _task: SchedulerPushTask<AdaptiveJob>,
    ) -> SchedulerResult<()> {
        Err(SchedulerError::Job(
            "forced apalis enqueue failure".to_owned(),
        ))
    }
}

fn enqueue_request() -> CacheKeepaliveEnqueueRequest {
    CacheKeepaliveEnqueueRequest {
        session_key_hash: "session-hash".to_owned(),
        principal_id: "principal".to_owned(),
        accounting_key_id: Some("key-live-123".to_owned()),
        cache_anchor_age: std::time::Duration::from_secs(30),
        expected_generation: None,
        params: ScheduleParams {
            delay: std::time::Duration::from_secs(240),
            max_refreshes: 3,
            max_total_duration_secs: 600,
        },
        display_reason: "agent-in-turn — first renewal in 4m".to_owned(),
        config_snapshot: cc_lb_storage_api::CacheKeepaliveConfigSnapshot {
            refresh_lead_time_5m_secs: 30,
            refresh_lead_time_1h_secs: 300,
            max_refreshes_per_session: 3,
            max_total_duration_secs: 600,
            snapshot_max_bytes: 524_288,
        },
        snapshot: RequestSnapshot::capture(
            Url::parse("http://upstream.local/v1/messages").expect("url parses"),
            Method::POST,
            HeaderMap::new(),
            Bytes::from_static(
                br#"{"model":"claude-test","max_tokens":32,"system":[{"type":"text","text":"cached prompt","cache_control":{"type":"ephemeral"}}],"messages":[{"role":"user","content":"hi"}]}"#,
            ),
            Uuid::from_u128(42),
            CacheTtl::Ttl5m,
            524_288,
        )
        .expect("snapshot captures"),
    }
}

#[tokio::test]
async fn multi_replica_stale_enqueue_and_cancel_do_not_overwrite_newer_session_state() {
    let fixture = Fixture::new().await;
    let backend = crate::scheduler_factory::open_scheduler_storage(
        &StorageConfig::Sqlite {
            path: fixture.sqlite_path.clone(),
        },
        &SchedulerConfig::default(),
        Arc::new(SystemClock),
    )
    .await
    .expect("open scheduler storage");
    let replica_a = ServerCacheKeepaliveEnqueuer::new(ServerCacheKeepaliveEnqueuerDeps {
        storage: fixture.storage.clone(),
        pusher: Arc::new(backend.backend.clone()),
        aead: fixture.aead.clone(),
        clock: Arc::new(SystemClock),
    });
    let replica_b = ServerCacheKeepaliveEnqueuer::new(ServerCacheKeepaliveEnqueuerDeps {
        storage: fixture.storage.clone(),
        pusher: Arc::new(backend.backend.clone()),
        aead: fixture.aead.clone(),
        clock: Arc::new(SystemClock),
    });

    replica_a
        .enqueue_cache_keepalive(enqueue_request())
        .await
        .expect("initial enqueue");

    let replica_a_observed = replica_a
        .observe_session_generation("session-hash")
        .await
        .expect("replica a observes active generation 1");
    let replica_b_observed = replica_b
        .observe_session_generation("session-hash")
        .await
        .expect("replica b observes active generation 1");

    replica_b
        .cancel_cache_keepalive(CacheKeepaliveCancelRequest {
            session_key_hash: "session-hash".to_owned(),
            reason: cc_lb_engine::cache_keepalive::CancelReason::UserTurnDetected,
            expected_generation: replica_b_observed,
            cache_anchor_age: std::time::Duration::ZERO,
        })
        .await
        .expect("replica b cancels");

    let mut stale_enqueue = enqueue_request();
    stale_enqueue.cache_anchor_age = std::time::Duration::ZERO;
    stale_enqueue.expected_generation = replica_a_observed;
    replica_a
        .enqueue_cache_keepalive(stale_enqueue)
        .await
        .expect("stale enqueue with pre-cancel token is ignored");
    let after_stale_enqueue = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(after_stale_enqueue.generation, 1);
    assert_eq!(
        after_stale_enqueue.status,
        CacheKeepaliveSessionStatus::Terminal
    );

    let fresh_observed = replica_b
        .observe_session_generation("session-hash")
        .await
        .expect("replica b observes terminal generation 1");
    let mut fresh_enqueue = enqueue_request();
    fresh_enqueue.cache_anchor_age = std::time::Duration::ZERO;
    fresh_enqueue.expected_generation = fresh_observed;
    replica_b
        .enqueue_cache_keepalive(fresh_enqueue)
        .await
        .expect("fresh enqueue advances to generation 2");

    replica_a
        .cancel_cache_keepalive(CacheKeepaliveCancelRequest {
            session_key_hash: "session-hash".to_owned(),
            reason: cc_lb_engine::cache_keepalive::CancelReason::CachedPrefixTooSmall,
            expected_generation: replica_a_observed,
            cache_anchor_age: std::time::Duration::ZERO,
        })
        .await
        .expect("stale cancel is ignored");
    let after_stale_cancel = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(after_stale_cancel.generation, 2);
    assert_eq!(
        after_stale_cancel.status,
        CacheKeepaliveSessionStatus::Active
    );

    let replica_a = Arc::new(replica_a);
    let replica_b = Arc::new(replica_b);
    let start_barrier = Arc::new(tokio::sync::Barrier::new(2));
    let cancel_done_barrier = Arc::new(tokio::sync::Barrier::new(2));
    let a_task = {
        let replica_a = Arc::clone(&replica_a);
        let start_barrier = Arc::clone(&start_barrier);
        let cancel_done_barrier = Arc::clone(&cancel_done_barrier);
        tokio::spawn(async move {
            let observed = replica_a
                .observe_session_generation("session-hash")
                .await
                .expect("observe active generation 2");
            start_barrier.wait().await;
            cancel_done_barrier.wait().await;
            let mut req = enqueue_request();
            req.cache_anchor_age = std::time::Duration::ZERO;
            req.expected_generation = observed;
            replica_a
                .enqueue_cache_keepalive(req)
                .await
                .expect("fenced enqueue after barrier");
        })
    };
    let b_task = {
        let replica_b = Arc::clone(&replica_b);
        let start_barrier = Arc::clone(&start_barrier);
        let cancel_done_barrier = Arc::clone(&cancel_done_barrier);
        tokio::spawn(async move {
            let observed = replica_b
                .observe_session_generation("session-hash")
                .await
                .expect("observe active generation 2");
            start_barrier.wait().await;
            replica_b
                .cancel_cache_keepalive(CacheKeepaliveCancelRequest {
                    session_key_hash: "session-hash".to_owned(),
                    reason: cc_lb_engine::cache_keepalive::CancelReason::UserTurnDetected,
                    expected_generation: observed,
                    cache_anchor_age: std::time::Duration::ZERO,
                })
                .await
                .expect("barrier cancel");
            cancel_done_barrier.wait().await;
        })
    };
    a_task.await.expect("replica a task completes");
    b_task.await.expect("replica b task completes");
    let after_barrier_race = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(after_barrier_race.generation, 2);
    assert_eq!(
        after_barrier_race.status,
        CacheKeepaliveSessionStatus::Terminal
    );

    let inv_start_barrier = Arc::new(tokio::sync::Barrier::new(2));
    let enqueue_done_barrier = Arc::new(tokio::sync::Barrier::new(2));
    let older_enqueue_ticket = replica_a
        .observe_session_generation("session-hash")
        .await
        .expect("older enqueue allocates ticket first");
    let newer_cancel_ticket = replica_b
        .observe_session_generation("session-hash")
        .await
        .expect("newer cancel allocates ticket second");
    let a_inv_task = {
        let replica_a = Arc::clone(&replica_a);
        let inv_start_barrier = Arc::clone(&inv_start_barrier);
        let enqueue_done_barrier = Arc::clone(&enqueue_done_barrier);
        tokio::spawn(async move {
            inv_start_barrier.wait().await;
            let mut req = enqueue_request();
            req.cache_anchor_age = std::time::Duration::ZERO;
            req.expected_generation = older_enqueue_ticket;
            replica_a
                .enqueue_cache_keepalive(req)
                .await
                .expect("older enqueue commits first");
            enqueue_done_barrier.wait().await;
        })
    };
    let b_inv_task = {
        let replica_b = Arc::clone(&replica_b);
        let inv_start_barrier = Arc::clone(&inv_start_barrier);
        let enqueue_done_barrier = Arc::clone(&enqueue_done_barrier);
        tokio::spawn(async move {
            inv_start_barrier.wait().await;
            enqueue_done_barrier.wait().await;
            replica_b
                .cancel_cache_keepalive(CacheKeepaliveCancelRequest {
                    session_key_hash: "session-hash".to_owned(),
                    reason: cc_lb_engine::cache_keepalive::CancelReason::UserTurnDetected,
                    expected_generation: newer_cancel_ticket,
                    cache_anchor_age: std::time::Duration::ZERO,
                })
                .await
                .expect("newer cancel runs second and cancels generation 3");
        })
    };
    a_inv_task.await.expect("older enqueue task completes");
    b_inv_task.await.expect("newer cancel task completes");
    let after_inverse_race = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(after_inverse_race.generation, 3);
    assert_eq!(
        after_inverse_race.status,
        CacheKeepaliveSessionStatus::Terminal
    );
}
