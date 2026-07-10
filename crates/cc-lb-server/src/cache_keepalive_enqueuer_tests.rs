use super::*;

use bytes::Bytes;
use cc_lb_config::{SchedulerConfig, StorageConfig};
use cc_lb_engine::SystemClock;
use cc_lb_engine::cache_keepalive::{CacheKeepaliveCancelRequest, RequestSnapshot, ScheduleParams};
use cc_lb_scheduler::error::SchedulerError;
use cc_lb_scheduler::worker::{Filter, TaskStatus};
use cc_lb_storage_api::{BackendKind, CacheKeepaliveSessionStatus, CacheTtl, MetaStore};
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
    assert!(!record.encrypted_payload.is_empty());
    assert!(!String::from_utf8_lossy(&record.encrypted_payload).contains("cached prompt"));

    let jobs = backend
        .backend
        .list_adaptive_tasks(&Filter {
            status: Some(TaskStatus::Pending),
            page: 1,
            page_size: Some(10),
        })
        .await
        .expect("list adaptive jobs");
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
        .list_adaptive_tasks(&Filter {
            status: Some(TaskStatus::Pending),
            page: 1,
            page_size: Some(10),
        })
        .await
        .expect("list adaptive jobs");
    assert_eq!(jobs.len(), 1);
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
        storage
            .initialize(BackendKind::Sqlite)
            .await
            .expect("initialize sqlite");
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
        cache_anchor_age: std::time::Duration::from_secs(30),
        params: ScheduleParams {
            delay: std::time::Duration::from_secs(240),
            max_refreshes: 3,
            max_total_duration_secs: 600,
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
