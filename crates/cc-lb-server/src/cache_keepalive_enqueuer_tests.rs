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

    enqueuer
        .cancel_cache_keepalive(CacheKeepaliveCancelRequest {
            session_key_hash: "session-hash".to_owned(),
            reason: cc_lb_engine::cache_keepalive::CancelReason::UserTurnDetected,
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
