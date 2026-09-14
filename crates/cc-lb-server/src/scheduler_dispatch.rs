mod cache_keepalive;
mod cron;
mod http;
mod oauth;
mod outcomes;
mod storage;
mod time;
mod usage;
mod warmup;
mod watchdog;

use std::sync::Arc;

use cc_lb_aead::AeadService;
use cc_lb_config::{AnthropicOAuthConfig, Config};
use cc_lb_control::RequestEventBus;
use cc_lb_control::api_keys::key_store::KeyStore;
use cc_lb_control::api_keys::limit_engine::LimitEngine;
use cc_lb_engine::DynamicViewHolder;
use cc_lb_engine::cache_keepalive::KeepaliveDispatcher;
use cc_lb_engine::clock::ClockHandle;
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use cc_lb_scheduler::error::Result as SchedulerResult;
use cc_lb_scheduler::jobs::metadata_refresh::{
    CoreMetadataRefreshRunner, MetadataRefreshJobHandler,
};
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::worker::{AdaptiveJob, SchedulerBackend, SchedulerCtx};
use cc_lb_storage_api::Storage;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::cache_keepalive_enqueuer::CacheKeepaliveTaskPusher;
use crate::dynamic_view_builder::Stores;
use crate::refresh::LazyRefresher;
use crate::scheduler_dispatch::http::{JsonHttpClient, json_http_client};
use crate::scheduler_dispatch::outcomes::metadata_outcome;
use crate::subscription_quota_cache::SubscriptionQuotaCache;

pub(crate) struct SchedulerDispatchDeps<B = SchedulerBackend> {
    pub backend: B,
    pub cache_keepalive_pusher: Arc<dyn CacheKeepaliveTaskPusher>,
    pub config: Config,
    pub storage: Arc<dyn Storage>,
    pub stores: Arc<Stores>,
    pub aead: Arc<AeadService>,
    pub oauth_cfg: Arc<AnthropicOAuthConfig>,
    pub runtime: Arc<WasmtimeRuntime>,
    pub data_dir: std::path::PathBuf,
    pub lazy_refresher: Option<Arc<LazyRefresher>>,
    pub subscription_quota_sink: cc_lb_engine::SubscriptionQuotaSink,
    pub subscription_quota_cache: Arc<SubscriptionQuotaCache>,
    pub cancel: CancellationToken,
    pub replica_id: Option<Uuid>,
    pub price_catalog: Arc<cc_lb_pricing::PriceCatalog>,
    pub key_store: Arc<KeyStore>,
    pub limit_engine: Arc<LimitEngine>,
    pub dynamic_view: Arc<DynamicViewHolder>,
    pub keepalive_dispatcher: Arc<dyn KeepaliveDispatcher>,
    pub event_bus: Arc<dyn RequestEventBus>,
    pub clock: ClockHandle,
}

#[derive(Clone)]
pub(super) struct SchedulerDispatch {
    pub(super) backend: Arc<dyn cron::SchedulerDispatchBackend>,
    pub(super) cache_keepalive_pusher: Arc<dyn CacheKeepaliveTaskPusher>,
    pub(super) config: Arc<Config>,
    pub(super) storage: Arc<dyn Storage>,
    pub(super) stores: Arc<Stores>,
    pub(super) aead: Arc<AeadService>,
    pub(super) oauth_cfg: Arc<AnthropicOAuthConfig>,
    pub(super) runtime: Arc<WasmtimeRuntime>,
    pub(super) data_dir: Arc<std::path::PathBuf>,
    pub(super) lazy_refresher: Option<Arc<LazyRefresher>>,
    pub(super) subscription_quota_sink: cc_lb_engine::SubscriptionQuotaSink,
    pub(super) subscription_quota_cache: Arc<SubscriptionQuotaCache>,
    pub(super) cancel: CancellationToken,
    pub(super) replica_id: Option<Uuid>,
    pub(super) price_catalog: Arc<cc_lb_pricing::PriceCatalog>,
    pub(super) key_store: Arc<KeyStore>,
    pub(super) limit_engine: Arc<LimitEngine>,
    pub(super) http: JsonHttpClient,
    pub(super) dynamic_view: Arc<DynamicViewHolder>,
    pub(super) keepalive_dispatcher: Arc<dyn KeepaliveDispatcher>,
    pub(super) event_bus: Arc<dyn RequestEventBus>,
    pub(super) clock: ClockHandle,
}

pub(crate) fn build_scheduler_ctx(deps: SchedulerDispatchDeps) -> SchedulerCtx {
    let dispatch = SchedulerDispatch::new(deps);
    SchedulerCtx::new(
        dispatch.config.scheduler.clone(),
        dispatch.clone().adaptive_dispatch(),
        dispatch.clone().cron_dispatch(),
        dispatch.clock.clone(),
    )
}

impl SchedulerDispatch {
    fn new<B>(deps: SchedulerDispatchDeps<B>) -> Self
    where
        B: cron::SchedulerDispatchBackend + 'static,
    {
        Self {
            backend: Arc::new(deps.backend),
            cache_keepalive_pusher: deps.cache_keepalive_pusher,
            config: Arc::new(deps.config),
            storage: deps.storage,
            stores: deps.stores,
            aead: deps.aead,
            oauth_cfg: deps.oauth_cfg,
            runtime: deps.runtime,
            data_dir: Arc::new(deps.data_dir),
            lazy_refresher: deps.lazy_refresher,
            subscription_quota_sink: deps.subscription_quota_sink,
            subscription_quota_cache: deps.subscription_quota_cache,
            cancel: deps.cancel,
            replica_id: deps.replica_id,
            price_catalog: deps.price_catalog,
            key_store: deps.key_store,
            limit_engine: deps.limit_engine,
            http: json_http_client(),
            dynamic_view: deps.dynamic_view,
            keepalive_dispatcher: deps.keepalive_dispatcher,
            event_bus: deps.event_bus,
            clock: deps.clock,
        }
    }

    fn adaptive_dispatch(self) -> cc_lb_scheduler::worker::AdaptiveDispatchFn {
        Arc::new(move |job| {
            let dispatch = self.clone();
            Box::pin(async move { dispatch.dispatch_entity(job).await })
        })
    }

    fn cron_dispatch(self) -> cc_lb_scheduler::worker::CronDispatchFn {
        Arc::new(move |job| {
            let dispatch = self.clone();
            Box::pin(async move { dispatch.dispatch_singleton(job).await })
        })
    }

    async fn dispatch_entity(&self, job: AdaptiveJob) -> SchedulerResult<JobOutcome> {
        match job {
            AdaptiveJob::Warmup(job) => self.dispatch_warmup(job).await,
            AdaptiveJob::OAuthRefresh(job) => self.dispatch_oauth_refresh(job).await,
            AdaptiveJob::MetadataRefresh(job) => {
                let runner = CoreMetadataRefreshRunner::new(
                    self.storage.clone(),
                    self.aead.clone(),
                    self.cancel.clone(),
                    self.clock.clone(),
                );
                metadata_outcome(MetadataRefreshJobHandler::new(runner).handle(job).await?)
            }
            AdaptiveJob::CacheKeepalive(job) => self.dispatch_cache_keepalive(job).await,
        }
    }
}

pub(crate) fn cache_keepalive_payload_aad(
    principal_id: &str,
    session_key_hash: &str,
    upstream_id: uuid::Uuid,
    generation: u64,
) -> Vec<u8> {
    format!(
        "cache_keepalive_snapshot:v1:{principal_id}:{session_key_hash}:{upstream_id}:{generation}"
    )
    .into_bytes()
}

#[cfg(test)]
#[path = "scheduler_dispatch_tests.rs"]
mod tests;
