#[path = "support/data.rs"]
mod data;
#[path = "support/fakes.rs"]
mod fakes;

use super::*;

use std::sync::Mutex;
use std::time::Duration;

use ::http::{HeaderMap, HeaderValue, Method};
use bytes::Bytes;
use cc_lb_config::{SchedulerConfig, StorageConfig};
use cc_lb_engine::api_keys::principal_view::PrincipalView;
use cc_lb_engine::cache_keepalive::{
    AnthropicKeepaliveDispatcher, CacheKeepaliveEnqueueRequest, RequestSnapshot, ScheduleParams,
};
use cc_lb_engine::{DynamicViewBuilder, SystemClock};
use cc_lb_runtime_wasmtime::HotEngineConfig;
use cc_lb_scheduler::jobs::cache_keepalive::CacheKeepaliveJob;
use cc_lb_scheduler::worker::{AdaptiveJob, Filter, TaskStatus};
use cc_lb_storage_api::upstream::{UpstreamCreate, UpstreamKind};
use cc_lb_storage_api::{BackendKind, CacheTtl, MetaStore, PrincipalStore, UpstreamStore};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::cache_keepalive_enqueuer::{
    CacheKeepaliveTaskPusher, ServerCacheKeepaliveEnqueuer, ServerCacheKeepaliveEnqueuerDeps,
};
use crate::dynamic_view_builder::Stores;

pub(super) use fakes::{FailingPusher, RecordingHttp};

use self::data::{principal_record_with_id, principal_with_keepalive};
use self::fakes::{NoRouteRouter, RecordingSignerFactory};

pub(super) struct Fixture {
    _dir: TempDir,
    pub(super) storage: Arc<cc_lb_storage_sqlite::SqliteStorage>,
    storage_dyn: Arc<dyn Storage>,
    pub(super) backend: crate::scheduler_factory::OpenedScheduler,
    aead: Arc<AeadService>,
    dynamic_view: Arc<DynamicViewHolder>,
    pub(super) http: Arc<RecordingHttp>,
    pub(super) signer_calls: Arc<Mutex<Vec<String>>>,
    upstream_id: Uuid,
    data_dir: std::path::PathBuf,
}

impl Fixture {
    pub(super) async fn new() -> Self {
        Self::new_with_upstream_kind(UpstreamKind::AnthropicOauth).await
    }

    pub(super) async fn new_with_upstream_kind(upstream_kind: UpstreamKind) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let sqlite_path = dir.path().join("cc-lb.sqlite");
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
                path: sqlite_path.clone(),
            },
            &SchedulerConfig::default(),
            Arc::new(SystemClock),
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
        let storage_dyn: Arc<dyn Storage> = storage.clone();
        Self {
            data_dir: dir.path().to_path_buf(),
            _dir: dir,
            storage,
            storage_dyn,
            backend,
            aead: Arc::new(AeadService::from_master_key([9; 32])),
            dynamic_view,
            http: Arc::new(RecordingHttp::default()),
            signer_calls,
            upstream_id: upstream.id,
        }
    }

    pub(super) async fn replace_encrypted_payload(&self, payload: Vec<u8>) {
        sqlx::query(
            "UPDATE cache_keepalive_sessions SET encrypted_payload = ? WHERE session_key_hash = ?",
        )
        .bind(payload)
        .bind("session-hash")
        .execute(self.storage.pool())
        .await
        .expect("replace encrypted payload");
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

    pub(super) async fn expire_session(&self) {
        sqlx::query(
            "UPDATE cache_keepalive_sessions SET expires_at = 1 WHERE session_key_hash = ?",
        )
        .bind("session-hash")
        .execute(self.storage.pool())
        .await
        .expect("expire session");
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

    pub(super) fn enqueuer(
        &self,
        pusher: Arc<dyn CacheKeepaliveTaskPusher>,
    ) -> ServerCacheKeepaliveEnqueuer {
        ServerCacheKeepaliveEnqueuer::new(ServerCacheKeepaliveEnqueuerDeps {
            storage: self.storage_dyn.clone(),
            pusher,
            aead: self.aead.clone(),
            clock: Arc::new(SystemClock),
        })
    }

    pub(super) fn dispatch(&self, pusher: Arc<dyn CacheKeepaliveTaskPusher>) -> SchedulerDispatch {
        let stores = Arc::new(Stores {
            upstreams: self.storage_dyn.clone(),
            principals: self.storage_dyn.clone(),
            plugin_registry: self.storage_dyn.clone(),
            upstream_rate_limits: self.storage_dyn.clone(),
            upstream_subscription_quotas: self.storage_dyn.clone(),
            upstream_subscription_metadata: self.storage_dyn.clone(),
            organization_metadata: self.storage_dyn.clone(),
            plan_tiers: self.storage_dyn.clone(),
            prompt_cache_observations: self.storage_dyn.clone(),
            anthropic_compatibility_kv: self.storage_dyn.clone(),
            audit: Some(self.storage_dyn.clone()),
        });
        let keepalive_dispatcher = Arc::new(
            AnthropicKeepaliveDispatcher::new(
                self.dynamic_view.clone(),
                self.storage_dyn.clone(),
                self.http.clone(),
            )
            .with_timeout(Duration::from_secs(1)),
        );
        SchedulerDispatch::new(SchedulerDispatchDeps {
            backend: self.backend.backend.clone(),
            cache_keepalive_pusher: pusher,
            config: Config::default(),
            storage: self.storage_dyn.clone(),
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
            dynamic_view: self.dynamic_view.clone(),
            keepalive_dispatcher,
            clock: Arc::new(SystemClock),
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
        let tasks = self
            .backend
            .backend
            .list_keepalive_tasks(&Filter {
                status: Some(TaskStatus::Pending),
                page: 1,
                page_size: Some(20),
            })
            .await
            .expect("list cache keepalive tasks");
        tasks
            .into_iter()
            .find_map(|task| match task.args {
                AdaptiveJob::CacheKeepalive(job) if job.generation == generation => Some(job),
                AdaptiveJob::Warmup(_)
                | AdaptiveJob::OAuthRefresh(_)
                | AdaptiveJob::MetadataRefresh(_)
                | AdaptiveJob::CacheKeepalive(_) => None,
            })
            .expect("pending cache keepalive job exists")
    }
}
