#![allow(non_snake_case)]

use super::*;

use std::sync::Mutex;

use bytes::Bytes;
use cc_lb_aead::AeadError;
use cc_lb_engine::cache_keepalive::{
    CacheKeepaliveCancelRequest, CacheKeepaliveNotTrackedRequest, PersistedRequestSnapshot,
    RequestSnapshot, ScheduleParams,
};
use cc_lb_scheduler::error::SchedulerError;
use cc_lb_storage_api::{
    CacheKeepaliveSessionFilter, CacheKeepaliveSessionListQuery, CacheKeepaliveSessionReadStore,
    CacheKeepaliveSessionStatus, CacheTtl, RequestEventStore,
};
use cc_lb_testkit::{InMemoryStorage, fixed_clock};
use http::{HeaderMap, Method};
use url::Url;
use uuid::Uuid;

#[tokio::test]
async fn t2__cache_keepalive_enqueuer__persists_session_payload_and_apalis_job() {
    let fixture = Fixture::new();
    let pusher = Arc::new(RecordingPusher::default());
    let enqueuer = fixture.enqueuer(pusher.clone());

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

    let job = pusher.take_only();
    assert_eq!(
        job.idempotency_key.as_deref(),
        Some(record.current_job_key.as_str())
    );
    assert_eq!(job.max_attempts, Some(1));
    assert_eq!(job.run_at_unix_secs, Some(record.run_at_unix_secs));
}

#[tokio::test]
async fn t2__cache_keepalive_enqueuer__aead_encryption_failure_marks_terminal() {
    let fixture = Fixture::new();
    let pusher = Arc::new(RecordingPusher::default());
    let enqueuer =
        fixture.enqueuer_with_payload_encryptor(pusher.clone(), Arc::new(FailingAeadService));

    let error = enqueuer
        .enqueue_cache_keepalive(enqueue_request())
        .await
        .expect_err("AEAD encryption failure must fail enqueue");

    assert_eq!(
        error.0,
        "cache keepalive payload encryption failed: AEAD encryption failed"
    );
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load cache keepalive session")
        .expect("committed session exists");
    assert_eq!(record.generation, 1);
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::DispatchError)
    );
    assert!(record.encrypted_payload.is_empty());
    assert_eq!(pusher.len(), 0);
}

#[tokio::test]
async fn t2__cache_keepalive_enqueuer__push_failure_terminalizes_committed_pending_session() {
    let fixture = Fixture::new();
    let enqueuer = fixture.enqueuer(Arc::new(FailingPusher));

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
async fn t2__cache_keepalive_enqueuer__cancel_terminalizes_leased_session_without_deleting_pending_job()
 {
    let fixture = Fixture::new();
    let pusher = Arc::new(RecordingPusher::default());
    let enqueuer = fixture.enqueuer(pusher.clone());
    enqueuer
        .enqueue_cache_keepalive(enqueue_request())
        .await
        .expect("enqueue cache keepalive");
    assert!(
        CacheKeepaliveSessionStore::claim_cache_keepalive_turn(
            fixture.storage.as_ref(),
            "session-hash",
            1,
            1_700_000_000,
        )
        .await
        .expect("claim cache keepalive lease")
    );

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
    assert_eq!(record.running_since_unix_secs, None);
    assert!(record.encrypted_payload.is_empty());
    assert_eq!(pusher.len(), 1);
}

#[tokio::test]
async fn t2__cache_keepalive_enqueuer__not_tracked_writes_a_decision_projection_without_a_renewal_turn()
 {
    let fixture = Fixture::new();
    let enqueuer = fixture.enqueuer(Arc::new(FailingPusher));
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
    let turns = fixture
        .storage
        .list_cache_keepalive_turns("principal", "session-hash")
        .await
        .expect("count renewal turns");
    assert_eq!(turns.len(), 0);
    let request_events = fixture
        .storage
        .query_request_events(0, u64::MAX, usize::MAX)
        .await
        .expect("count request events");
    assert_eq!(request_events.len(), 0);
}

struct Fixture {
    storage: Arc<InMemoryStorage>,
    aead: Arc<AeadService>,
    clock: cc_lb_engine::ClockHandle,
}

impl Fixture {
    fn new() -> Self {
        let clock = fixed_clock(1_700_000_000);
        Self {
            storage: Arc::new(InMemoryStorage::with_clock(clock.clone())),
            aead: Arc::new(AeadService::from_master_key([7; 32])),
            clock,
        }
    }

    fn enqueuer(&self, pusher: Arc<dyn CacheKeepaliveTaskPusher>) -> ServerCacheKeepaliveEnqueuer {
        self.enqueuer_with_payload_encryptor(pusher, Arc::new(FixedPayloadEncryptor))
    }

    fn enqueuer_with_payload_encryptor(
        &self,
        pusher: Arc<dyn CacheKeepaliveTaskPusher>,
        payload_encryptor: Arc<dyn CacheKeepalivePayloadEncryptor>,
    ) -> ServerCacheKeepaliveEnqueuer {
        ServerCacheKeepaliveEnqueuer::new(ServerCacheKeepaliveEnqueuerDeps {
            storage: self.storage.clone(),
            pusher,
            aead: self.aead.clone(),
            clock: self.clock.clone(),
        })
        .with_payload_encryptor(payload_encryptor)
        .with_decision_id_generator(Arc::new(FixedDecisionIdGenerator))
    }
}

struct FixedDecisionIdGenerator;

impl CacheKeepaliveDecisionIdGenerator for FixedDecisionIdGenerator {
    fn next_id(&self) -> String {
        "not-tracked:test-decision".to_owned()
    }
}

struct FixedPayloadEncryptor;

impl CacheKeepalivePayloadEncryptor for FixedPayloadEncryptor {
    fn encrypt(
        &self,
        _principal_id: &str,
        _session_key_hash: &str,
        _upstream_id: Uuid,
        _payload_generation: u64,
        _snapshot: &PersistedRequestSnapshot,
    ) -> Result<Vec<u8>, CacheKeepalivePayloadError> {
        Ok(vec![0xA5; 32])
    }
}

struct FailingAeadService;

impl CacheKeepalivePayloadEncryptor for FailingAeadService {
    fn encrypt(
        &self,
        _principal_id: &str,
        _session_key_hash: &str,
        _upstream_id: Uuid,
        _payload_generation: u64,
        _snapshot: &PersistedRequestSnapshot,
    ) -> Result<Vec<u8>, CacheKeepalivePayloadError> {
        Err(CacheKeepalivePayloadError::Aead(
            AeadError::EncryptionFailed,
        ))
    }
}

#[derive(Default)]
struct RecordingPusher {
    tasks: Mutex<Vec<SchedulerPushTask<AdaptiveJob>>>,
}

impl RecordingPusher {
    fn len(&self) -> usize {
        self.tasks.lock().expect("recording pusher lock").len()
    }

    fn take_only(&self) -> SchedulerPushTask<AdaptiveJob> {
        let mut tasks = self.tasks.lock().expect("recording pusher lock");
        assert_eq!(tasks.len(), 1);
        tasks.pop().expect("one recorded cache keepalive task")
    }
}

#[async_trait]
impl CacheKeepaliveTaskPusher for RecordingPusher {
    async fn push_cache_keepalive_task(
        &self,
        task: SchedulerPushTask<AdaptiveJob>,
    ) -> SchedulerResult<()> {
        self.tasks.lock().expect("recording pusher lock").push(task);
        Ok(())
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
