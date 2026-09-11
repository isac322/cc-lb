#[path = "support/data.rs"]
mod data;
#[path = "support/fakes.rs"]
mod fakes;

use super::*;

use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use ::http::{HeaderMap, HeaderValue, Method};
use bytes::Bytes;
use cc_lb_config::{SchedulerConfig, StorageConfig};
use cc_lb_control::RequestEventBus;
use cc_lb_control::api_keys::limit_engine::LimitEngine;
use cc_lb_engine::DynamicViewBuilder;
use cc_lb_engine::api_keys::principal_view::PrincipalView;
use cc_lb_engine::cache_keepalive::{
    AnthropicKeepaliveDispatcher, CacheKeepaliveEnqueueRequest, KeepaliveDispatcher,
    RequestSnapshot, ScheduleParams,
};
use cc_lb_runtime_wasmtime::HotEngineConfig;
use cc_lb_scheduler::error::Result as SchedulerResult;
use cc_lb_scheduler::jobs::cache_keepalive::CacheKeepaliveJob;
use cc_lb_scheduler::worker::{AdaptiveJob, SchedulerPushTask};
use cc_lb_storage_api::upstream::{UpstreamCreate, UpstreamKind};
use cc_lb_storage_api::{
    BackendKind, CacheKeepaliveSessionStore, CacheTtl, ManagedKeyStore, MetaStore, PrincipalStore,
    UpstreamStore,
};
use cc_lb_testkit::{InMemoryStorage, fixed_clock};
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::cache_keepalive_enqueuer::{
    CacheKeepaliveTaskPusher, ServerCacheKeepaliveEnqueuer, ServerCacheKeepaliveEnqueuerDeps,
};
use crate::dynamic_view_builder::Stores;

pub(super) use fakes::{FailingPusher, RecordingHttp};

use self::data::{principal_record_with_id, principal_with_keepalive};
use self::fakes::{NoRouteRouter, RecordingSignerFactory};

pub(super) fn empty_dynamic_view() -> Arc<DynamicViewHolder> {
    Arc::new(DynamicViewHolder::new(
        DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(RecordingSignerFactory {
                calls: Arc::new(Mutex::new(Vec::new())),
            }))
            .global_router(Arc::new(NoRouteRouter))
            .global_observability_hooks(vec![Arc::new(crate::builtins::NoopObservabilityHook)])
            .principal_view(Arc::new(PrincipalView::from_db(
                &[],
                std::collections::HashMap::new(),
            )))
            .build(),
    ))
}

struct TestDataDir {
    path: PathBuf,
}

impl TestDataDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "cc-lb-scheduler-dispatch-test-{}-{}",
            std::process::id(),
            Uuid::new_v4()
        ));
        std::fs::create_dir(&path).expect("create scheduler dispatch test data dir");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDataDir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.path).expect("remove scheduler dispatch test data dir");
    }
}

struct PendingPayloadReplacement {
    storage: Arc<dyn Storage>,
    generation: u64,
    encrypted_payload: Vec<u8>,
    now_unix_secs: u64,
}

#[derive(Default)]
struct RecordingPusher {
    tasks: Mutex<Vec<SchedulerPushTask<AdaptiveJob>>>,
    pending_payload_replacement: Mutex<Option<PendingPayloadReplacement>>,
}

#[async_trait]
impl CacheKeepaliveTaskPusher for RecordingPusher {
    async fn push_cache_keepalive_task(
        &self,
        task: SchedulerPushTask<AdaptiveJob>,
    ) -> SchedulerResult<()> {
        let replacement = self
            .pending_payload_replacement
            .lock()
            .expect("pending payload replacement lock")
            .take();
        if let (
            Some(PendingPayloadReplacement {
                storage,
                generation,
                encrypted_payload,
                now_unix_secs,
            }),
            AdaptiveJob::CacheKeepalive(job),
        ) = (replacement, &task.args)
            && job.generation == generation
        {
            CacheKeepaliveSessionStore::update_cache_keepalive_payload(
                storage.as_ref(),
                &job.session_key_hash,
                generation,
                &encrypted_payload,
                now_unix_secs,
            )
            .await
            .expect("replace pending cache keepalive payload");
        }
        self.tasks.lock().expect("recording pusher lock").push(task);
        Ok(())
    }
}

pub(super) struct Fixture {
    pub(super) storage: Arc<dyn Storage>,
    pub(super) managed_store: Arc<dyn ManagedKeyStore>,
    sqlite_storage: Option<Arc<cc_lb_storage_sqlite::SqliteStorage>>,
    pub(super) backend: crate::scheduler_factory::OpenedScheduler,
    pusher: Arc<RecordingPusher>,
    clock: cc_lb_engine::ClockHandle,
    aead: Arc<AeadService>,
    dynamic_view: Arc<DynamicViewHolder>,
    pub(super) http: Arc<RecordingHttp>,
    pub(super) signer_calls: Arc<Mutex<Vec<String>>>,
    pub(super) upstream_id: Uuid,
    event_bus: Arc<cc_lb_control::InMemoryBus>,
    data_dir: PathBuf,
    _dir: TestDataDir,
}

impl Fixture {
    pub(super) async fn new() -> Self {
        Self::new_with_upstream_kind(UpstreamKind::AnthropicOauth).await
    }

    pub(super) async fn new_with_upstream_kind(upstream_kind: UpstreamKind) -> Self {
        let clock = fixed_clock(1_700_000_000);
        let storage = Arc::new(InMemoryStorage::with_clock(clock.clone()));
        let storage_dyn: Arc<dyn Storage> = storage.clone();
        let managed_store: Arc<dyn ManagedKeyStore> = storage;
        Self::build(
            TestDataDir::new(),
            storage_dyn,
            managed_store,
            None,
            upstream_kind,
            clock,
        )
        .await
    }

    pub(super) async fn new_sqlite() -> Self {
        let dir = TestDataDir::new();
        let clock = fixed_clock(1_700_000_000);
        let sqlite_path = dir.path().join("cc-lb.sqlite");
        let database_url = format!("sqlite://{}", sqlite_path.display());
        let storage = Arc::new(
            cc_lb_storage_sqlite::open_sqlite(&database_url, clock.clone())
                .await
                .expect("open sqlite"),
        );
        storage
            .initialize(BackendKind::Sqlite)
            .await
            .expect("initialize sqlite");
        let storage_dyn: Arc<dyn Storage> = storage.clone();
        let managed_store: Arc<dyn ManagedKeyStore> = storage.clone();
        Self::build(
            dir,
            storage_dyn,
            managed_store,
            Some(storage),
            UpstreamKind::AnthropicOauth,
            clock,
        )
        .await
    }

    async fn build(
        dir: TestDataDir,
        storage: Arc<dyn Storage>,
        managed_store: Arc<dyn ManagedKeyStore>,
        sqlite_storage: Option<Arc<cc_lb_storage_sqlite::SqliteStorage>>,
        upstream_kind: UpstreamKind,
        clock: cc_lb_engine::ClockHandle,
    ) -> Self {
        let upstream = UpstreamStore::create(
            storage.as_ref(),
            UpstreamCreate {
                name: "fake-upstream".to_owned(),
                kind: upstream_kind,
                base_url: Some(Url::parse("http://fake-upstream.local/").expect("base URL parses")),
                api_key_ciphertext: None,
                oauth_token_generation: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
            },
        )
        .await
        .expect("create upstream");
        PrincipalStore::create(storage.as_ref(), principal_with_keepalive(), 1)
            .await
            .expect("create principal");
        let backend = crate::scheduler_factory::open_scheduler_storage(
            &StorageConfig::Sqlite {
                path: dir.path().join("scheduler.sqlite"),
            },
            &SchedulerConfig::default(),
            clock.clone(),
        )
        .await
        .expect("open scheduler storage");
        let signer_calls = Arc::new(Mutex::new(Vec::new()));
        let dynamic_view = Arc::new(DynamicViewHolder::new(
            DynamicViewBuilder::new(0)
                .signer_factory(Arc::new(RecordingSignerFactory {
                    calls: Arc::clone(&signer_calls),
                }))
                .global_router(Arc::new(NoRouteRouter))
                .global_observability_hooks(vec![Arc::new(crate::builtins::NoopObservabilityHook)])
                .principal_view(Arc::new(PrincipalView::from_db(
                    &[principal_record_with_id("principal")],
                    std::collections::HashMap::new(),
                )))
                .upstream_records(vec![upstream.clone()])
                .build(),
        ));
        let event_bus = Arc::new(cc_lb_control::InMemoryBus::new());
        let pusher = Arc::new(RecordingPusher::default());
        Self {
            data_dir: dir.path().to_path_buf(),
            _dir: dir,
            storage,
            managed_store,
            sqlite_storage,
            backend,
            pusher,
            clock,
            aead: Arc::new(AeadService::from_master_key([9; 32])),
            dynamic_view,
            http: Arc::new(RecordingHttp::default()),
            signer_calls,
            upstream_id: upstream.id,
            event_bus,
        }
    }

    pub(super) fn sqlite_pool(&self) -> &sqlx::SqlitePool {
        self.sqlite_storage
            .as_ref()
            .expect("sqlite policy test uses sqlite storage")
            .pool()
    }

    pub(super) fn encrypt_generation_payload(&self, generation: u64, plaintext: &[u8]) -> Vec<u8> {
        self.aead
            .encrypt(
                plaintext,
                &crate::scheduler_dispatch::cache_keepalive_payload_aad(
                    "principal",
                    "session-hash",
                    self.upstream_id,
                    generation,
                ),
            )
            .expect("encrypt test payload")
    }

    pub(super) fn revoke_principal(&self) {
        let current = self.dynamic_view.load();
        let mut disabled = principal_record_with_id("principal");
        disabled.enabled = false;
        let view = DynamicViewBuilder::from_view(&current)
            .principal_view(Arc::new(PrincipalView::from_db(
                &[disabled],
                std::collections::HashMap::new(),
            )))
            .build();
        self.dynamic_view.store(view);
    }

    pub(super) fn disable_cache_keepalive_on_next_http_dispatch(&self) {
        let dynamic_view = Arc::clone(&self.dynamic_view);
        self.http.run_on_next_dispatch(Arc::new(move || {
            let current = dynamic_view.load();
            let mut disabled = principal_record_with_id("principal");
            disabled
                .cache_keepalive
                .as_mut()
                .expect("fixture principal has cache keepalive")
                .enabled = false;
            let view = DynamicViewBuilder::from_view(&current)
                .principal_view(Arc::new(PrincipalView::from_db(
                    &[disabled],
                    std::collections::HashMap::new(),
                )))
                .build();
            dynamic_view.store(view);
        }));
    }

    pub(super) fn pusher(&self) -> Arc<dyn CacheKeepaliveTaskPusher> {
        self.pusher.clone()
    }

    pub(super) fn enqueuer(
        &self,
        pusher: Arc<dyn CacheKeepaliveTaskPusher>,
    ) -> ServerCacheKeepaliveEnqueuer {
        ServerCacheKeepaliveEnqueuer::new(ServerCacheKeepaliveEnqueuerDeps {
            storage: self.storage.clone(),
            pusher,
            aead: self.aead.clone(),
            clock: self.clock.clone(),
        })
    }

    pub(super) fn replace_payload_during_next_enqueue(
        &self,
        generation: u64,
        encrypted_payload: Vec<u8>,
    ) {
        *self
            .pusher
            .pending_payload_replacement
            .lock()
            .expect("pending payload replacement lock") = Some(PendingPayloadReplacement {
            storage: self.storage.clone(),
            generation,
            encrypted_payload,
            now_unix_secs: cc_lb_engine::clock::unix_secs(self.clock.now()),
        });
    }

    pub(super) fn event_bus(&self) -> Arc<cc_lb_control::InMemoryBus> {
        self.event_bus.clone()
    }

    pub(super) fn dynamic_view(&self) -> Arc<DynamicViewHolder> {
        self.dynamic_view.clone()
    }

    pub(super) fn dispatch(&self, pusher: Arc<dyn CacheKeepaliveTaskPusher>) -> SchedulerDispatch {
        self.dispatch_with_limit_engine(
            pusher,
            cc_lb_control::api_keys::limit_engine::LimitEngine::new(
                Arc::new(cc_lb_control::api_keys::concurrent_guard::KeyConcurrencyManager::new()),
                self.clock.clone(),
            ),
        )
    }

    pub(super) fn dispatch_with_config(
        &self,
        pusher: Arc<dyn CacheKeepaliveTaskPusher>,
        config: Config,
    ) -> SchedulerDispatch {
        let keepalive_dispatcher = Arc::new(
            AnthropicKeepaliveDispatcher::new(
                self.dynamic_view.clone(),
                self.storage.clone(),
                self.http.clone(),
            )
            .with_timeout(Duration::from_secs(1)),
        );
        self.dispatch_with_config_limit_engine_and_keepalive_dispatcher(
            pusher,
            cc_lb_control::api_keys::limit_engine::LimitEngine::new(
                Arc::new(cc_lb_control::api_keys::concurrent_guard::KeyConcurrencyManager::new()),
                self.clock.clone(),
            ),
            keepalive_dispatcher,
            config,
        )
    }

    pub(super) fn dispatch_with_limit_engine(
        &self,
        pusher: Arc<dyn CacheKeepaliveTaskPusher>,
        limit_engine: Arc<LimitEngine>,
    ) -> SchedulerDispatch {
        let keepalive_dispatcher = Arc::new(
            AnthropicKeepaliveDispatcher::new(
                self.dynamic_view.clone(),
                self.storage.clone(),
                self.http.clone(),
            )
            .with_timeout(Duration::from_secs(1)),
        );
        self.dispatch_with_limit_engine_and_keepalive_dispatcher(
            pusher,
            limit_engine,
            keepalive_dispatcher,
        )
    }

    pub(super) fn dispatch_with_limit_engine_and_keepalive_dispatcher(
        &self,
        pusher: Arc<dyn CacheKeepaliveTaskPusher>,
        limit_engine: Arc<LimitEngine>,
        keepalive_dispatcher: Arc<dyn KeepaliveDispatcher>,
    ) -> SchedulerDispatch {
        self.dispatch_with_config_limit_engine_and_keepalive_dispatcher(
            pusher,
            limit_engine,
            keepalive_dispatcher,
            Config::default(),
        )
    }

    fn dispatch_with_config_limit_engine_and_keepalive_dispatcher(
        &self,
        pusher: Arc<dyn CacheKeepaliveTaskPusher>,
        limit_engine: Arc<LimitEngine>,
        keepalive_dispatcher: Arc<dyn KeepaliveDispatcher>,
        config: Config,
    ) -> SchedulerDispatch {
        let stores = Arc::new(Stores {
            upstreams: self.storage.clone(),
            principals: self.storage.clone(),
            plugin_registry: self.storage.clone(),
            upstream_rate_limits: self.storage.clone(),
            upstream_subscription_quotas: self.storage.clone(),
            upstream_subscription_metadata: self.storage.clone(),
            organization_metadata: self.storage.clone(),
            plan_tiers: self.storage.clone(),
            prompt_cache_observations: self.storage.clone(),
            anthropic_compatibility_kv: self.storage.clone(),
            audit: Some(self.storage.clone()),
        });
        SchedulerDispatch::new(SchedulerDispatchDeps {
            backend: self.backend.backend.clone(),
            cache_keepalive_pusher: pusher,
            config,
            storage: self.storage.clone(),
            stores,
            aead: self.aead.clone(),
            oauth_cfg: Arc::new(AnthropicOAuthConfig::default()),
            runtime: Arc::new(
                WasmtimeRuntime::new(HotEngineConfig::default()).expect("wasmtime runtime"),
            ),
            data_dir: self.data_dir.clone(),
            lazy_refresher: None,
            subscription_quota_sink: cc_lb_engine::SubscriptionQuotaSink::new().0,
            subscription_quota_cache: Arc::new(SubscriptionQuotaCache::new()),
            cancel: CancellationToken::new(),
            replica_id: None,
            price_catalog: cc_lb_pricing::global_catalog().clone(),
            key_store: Arc::new(cc_lb_control::api_keys::key_store::KeyStore::new(
                self.managed_store.clone(),
            )),
            limit_engine,
            dynamic_view: self.dynamic_view.clone(),
            keepalive_dispatcher,
            event_bus: self.event_bus.clone() as Arc<dyn RequestEventBus>,
            clock: self.clock.clone(),
        })
    }

    pub(super) fn enqueue_request(&self) -> CacheKeepaliveEnqueueRequest {
        let mut headers = HeaderMap::new();
        headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
        CacheKeepaliveEnqueueRequest {
            session_key_hash: "session-hash".to_owned(),
            principal_id: "principal".to_owned(),
            accounting_key_id: None,
            cache_anchor_age: Duration::ZERO,
            params: ScheduleParams {
                delay: Duration::from_secs(1),
                max_refreshes: 3,
                max_total_duration_secs: 600,
            },
            display_reason: "agent-in-turn — first renewal in 1s".to_owned(),
            config_snapshot: cc_lb_storage_api::CacheKeepaliveConfigSnapshot {
                refresh_lead_time_5m_secs: 30,
                refresh_lead_time_1h_secs: 300,
                max_refreshes_per_session: 3,
                max_total_duration_secs: 600,
                snapshot_max_bytes: 524_288,
            },
            snapshot: RequestSnapshot::capture(
                Url::parse("https://api.anthropic.com/v1/messages").expect("url parses"),
                Method::POST,
                headers,
                Bytes::from_static(
                    br#"{"model":"claude-test","max_tokens":32,"stream":true,"system":[{"type":"text","text":"cached prompt","cache_control":{"type":"ephemeral"}}],"messages":[{"role":"user","content":"hi"}]}"#,
                ),
                self.upstream_id,
                CacheTtl::Ttl5m,
                524_288,
            )
            .expect("snapshot captures"),
        }
    }

    pub(super) async fn pending_keepalive_job(&self, generation: u64) -> CacheKeepaliveJob {
        self.pusher
            .tasks
            .lock()
            .expect("recording pusher lock")
            .iter()
            .find_map(|task| match &task.args {
                AdaptiveJob::CacheKeepalive(job) if job.generation == generation => {
                    Some(job.clone())
                }
                AdaptiveJob::Warmup(_)
                | AdaptiveJob::OAuthRefresh(_)
                | AdaptiveJob::MetadataRefresh(_)
                | AdaptiveJob::CacheKeepalive(_) => None,
            })
            .expect("pending cache keepalive job exists")
    }
}
