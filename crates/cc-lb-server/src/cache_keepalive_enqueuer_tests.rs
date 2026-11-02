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
    CacheKeepaliveEnqueueState, CacheKeepaliveSessionFilter, CacheKeepaliveSessionListQuery,
    CacheKeepaliveSessionReadStore, CacheKeepaliveSessionStatus, CacheTtl, MetaStore,
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
    assert_eq!(record.enqueue_state, CacheKeepaliveEnqueueState::Pending);
    assert!(record.running_since_unix_secs.is_none());
    assert!(
        record
            .error
            .as_deref()
            .is_some_and(|error| error.contains("forced apalis enqueue failure"))
    );
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

#[tokio::test]
async fn shape_injected_forced_tool_choice_is_durably_not_tracked_without_session_or_job() {
    use cc_lb_control::api_keys::builtin_authn::BuiltinAuthn;
    use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
    use cc_lb_control::api_keys::principal_view::{
        DialectCache, PrincipalView, RouterPipelineCache, ShapePluginCache,
    };
    use cc_lb_control::{DynamicViewBuilder, DynamicViewHolder};
    use cc_lb_storage_api::{
        PrincipalCreate, PrincipalKind, PrincipalStore, UpstreamCreate, UpstreamKind, UpstreamStore,
    };

    let fixture = Fixture::new().await;
    let principal = PrincipalStore::create(
        fixture.storage.as_ref(),
        PrincipalCreate {
            name: "shape-keepalive".to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models: Vec::new(),
            allowed_upstreams: Vec::new(),
            default_limits: Vec::new(),
            cache_keepalive: Some(cc_lb_storage_api::CacheKeepaliveConfig {
                enabled: true,
                refresh_lead_time_5m_secs: 30,
                refresh_lead_time_1h_secs: 300,
                max_refreshes_per_session: 3,
                max_total_duration_secs: 600,
                snapshot_max_bytes: 524_288,
                min_cache_tokens: 10_000,
                classifier: Default::default(),
            }),
        },
        1,
    )
    .await
    .expect("create principal");
    let keys = Arc::new(KeyStore::new(fixture.storage.clone()));
    let (_, secret) = keys
        .create(
            &principal.name,
            CreateParams {
                label: "shape-regression".to_owned(),
                description: None,
                expires_at_unix_secs: None,
                limit_overrides: Vec::new(),
            },
        )
        .await
        .expect("create key");
    let upstream = UpstreamStore::create(
        fixture.storage.as_ref(),
        UpstreamCreate {
            id: Uuid::new_v4(),
            name: "shape-upstream".to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: Some(Url::parse("http://shape.local/").expect("URL")),
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        },
    )
    .await
    .expect("create upstream");
    // Use the same explicit UpstreamDialect seam as response_transform_paths:
    // the native hook and Wasmtime adapter both return a ShapedRequest body.
    let principal_view = Arc::new(PrincipalView::from_db(
        std::slice::from_ref(&principal),
        std::collections::HashMap::from([(
            principal.name.clone(),
            (
                Some(Arc::new(RouterPipelineCache::empty(
                    cc_lb_domain::TerminalStrategy::FirstPick,
                ))),
                DialectCache::Explicit(ShapePluginCache {
                    dialect: Arc::new(ForcedToolChoiceShape),
                }),
            ),
        )]),
    ));
    let backend = crate::scheduler_factory::open_scheduler_storage(
        &StorageConfig::Sqlite {
            path: fixture.sqlite_path.clone(),
        },
        &SchedulerConfig::default(),
        Arc::new(SystemClock),
    )
    .await
    .expect("open scheduler");
    let lifecycle = cc_lb_engine::Lifecycle::new_with_dynamic_view(
        Arc::new(BuiltinAuthn::new(keys, Arc::new(SystemClock))),
        Arc::new(DynamicViewHolder::new(
            DynamicViewBuilder::new(0)
                .principal_view(principal_view)
                .upstream_records(vec![upstream])
                .signer_factory(Arc::new(ShapeTestSigner))
                .build(),
        )),
        Arc::new(ShapeTestDispatch),
        cc_lb_engine::LifecycleConfig::default(),
        Arc::new(SystemClock),
    )
    .with_cache_keepalive_enqueuer(Arc::new(ServerCacheKeepaliveEnqueuer::new(
        ServerCacheKeepaliveEnqueuerDeps {
            storage: fixture.storage.clone(),
            pusher: Arc::new(backend.backend.clone()),
            aead: fixture.aead.clone(),
            clock: Arc::new(SystemClock),
        },
    )));
    let request = http::Request::builder()
        .method(Method::POST)
        .uri("/v1/messages")
        .header("x-api-key", secret.expose())
        .header("x-session-id", "shape-session")
        .body(Bytes::from_static(br#"{"model":"claude-test","max_tokens":32,"tool_choice":{"type":"auto"},"system":[{"type":"text","text":"cached","cache_control":{"type":"ephemeral"}}],"messages":[{"role":"user","content":"hello"}]}"#))
        .expect("request");
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("authenticate");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("live response");
    assert_eq!(response.status(), http::StatusCode::OK);
    http_body_util::BodyExt::collect(response.into_body())
        .await
        .expect("response body");
    let decisions = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let (decisions, sessions) = sqlx::query_as::<_, (i64, i64)>(
                "SELECT (SELECT COUNT(*) FROM cache_keepalive_decisions WHERE decision = 'not_tracked' AND reason = 'zero-token renewal unsupported: forced tool_choice'), (SELECT COUNT(*) FROM cache_keepalive_sessions)",
            ).fetch_one(fixture.storage.pool()).await.expect("decision boundary");
            if decisions > 0 || sessions > 0 {
                break decisions;
            }
            tokio::task::yield_now().await;
        }
    }).await.expect("keepalive decision");
    assert_eq!(
        decisions, 1,
        "shaped tool choice must exclude the live request"
    );
    let sessions = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM cache_keepalive_sessions")
        .fetch_one(fixture.storage.pool())
        .await
        .expect("sessions");
    assert_eq!(sessions, 0);
    let jobs = backend
        .backend
        .list_keepalive_tasks(&Filter {
            status: None,
            page: 1,
            page_size: Some(10),
        })
        .await
        .expect("jobs");
    assert!(jobs.is_empty());
}

struct ForcedToolChoiceShape;

impl cc_lb_upstream::UpstreamDialect for ForcedToolChoiceShape {
    fn shape(
        &self,
        context: &cc_lb_upstream::DialectShapeContext,
        _upstream: &cc_lb_domain::Upstream,
        _principal: &cc_lb_domain::Principal,
        builder: &mut cc_lb_upstream::ShapedRequestBuilder,
    ) -> Result<cc_lb_upstream::ShapedRequest, cc_lb_upstream::DialectError> {
        let mut value: serde_json::Value =
            sonic_rs::from_slice(&context.body_bytes).expect("live JSON");
        assert_eq!(value["tool_choice"]["type"], "auto");
        value["tool_choice"] = serde_json::json!({"type": "any"});
        Ok(builder.shaped_request(
            Url::parse("http://shape.local/v1/messages").expect("shape URL"),
            context.method.clone(),
            context.downstream_headers.clone(),
            Bytes::from(serde_json::to_vec(&value).expect("shaped JSON")),
        ))
    }
}

struct ShapeTestSigner;

impl cc_lb_engine::ApiKeyAwareSignerFactory for ShapeTestSigner {
    fn with_router_choice(&self, _name: String) -> Arc<dyn cc_lb_upstream::SignerFactory> {
        Arc::new(Self)
    }
}

#[async_trait]
impl cc_lb_upstream::SignerFactory for ShapeTestSigner {
    async fn build(
        &self,
        _upstream: &cc_lb_domain::Upstream,
    ) -> Result<Arc<dyn cc_lb_upstream::Signer>, cc_lb_upstream::SignerError> {
        Ok(Arc::new(Self))
    }
}

#[async_trait]
impl cc_lb_upstream::Signer for ShapeTestSigner {
    async fn sign(
        &self,
        shaped: cc_lb_upstream::ShapedRequest,
        capability: &mut cc_lb_upstream::SigningCapability,
    ) -> Result<cc_lb_upstream::SignedRequest, cc_lb_upstream::SignerError> {
        Ok(cc_lb_upstream::SignedRequest::from_shaped(
            shaped, capability,
        ))
    }

    async fn on_unauthorized(
        &self,
        _error: &cc_lb_upstream::UpstreamError,
    ) -> cc_lb_upstream::RetryDecision {
        cc_lb_upstream::RetryDecision::Fail
    }
}

struct ShapeTestDispatch;

#[async_trait]
impl cc_lb_engine::UpstreamDispatch for ShapeTestDispatch {
    async fn dispatch(
        &self,
        request: cc_lb_upstream::SignedRequest,
    ) -> Result<http::Response<axum::body::Body>, cc_lb_engine::DispatchError> {
        let value: serde_json::Value =
            sonic_rs::from_slice(request.body()).expect("dispatched JSON");
        assert_eq!(value["tool_choice"]["type"], "any");
        Ok(http::Response::builder()
            .header("content-type", "application/json")
            .body(axum::body::Body::from(
                serde_json::json!({
                    "type": "message",
                    "content": [{"type": "tool_use", "id": "tool1", "name": "Bash", "input": {}}],
                    "stop_reason": "tool_use",
                    "usage": {"cache_read_input_tokens": 12_000, "output_tokens": 2}
                })
                .to_string(),
            ))
            .expect("response"))
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
    let stale_enqueue_ticket = replica_a
        .observe_session_generation("session-hash")
        .await
        .expect("replica a observes active generation 2");
    let cancel_ticket = replica_b
        .observe_session_generation("session-hash")
        .await
        .expect("replica b observes active generation 2");
    let a_task = {
        let replica_a = Arc::clone(&replica_a);
        let start_barrier = Arc::clone(&start_barrier);
        let cancel_done_barrier = Arc::clone(&cancel_done_barrier);
        tokio::spawn(async move {
            start_barrier.wait().await;
            cancel_done_barrier.wait().await;
            let mut req = enqueue_request();
            req.cache_anchor_age = std::time::Duration::ZERO;
            req.expected_generation = stale_enqueue_ticket;
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
            start_barrier.wait().await;
            replica_b
                .cancel_cache_keepalive(CacheKeepaliveCancelRequest {
                    session_key_hash: "session-hash".to_owned(),
                    reason: cc_lb_engine::cache_keepalive::CancelReason::UserTurnDetected,
                    expected_generation: cancel_ticket,
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
