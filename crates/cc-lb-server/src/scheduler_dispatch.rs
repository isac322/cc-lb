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
use cc_lb_runtime_extism::ExtismRuntime;
use cc_lb_scheduler::error::Result as SchedulerResult;
use cc_lb_scheduler::jobs::metadata_refresh::{
    CoreMetadataRefreshRunner, MetadataRefreshJobHandler,
};
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::worker::{AdaptiveJob, SchedulerBackend, SchedulerCtx};
use cc_lb_storage_api::Storage;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::dynamic_view_builder::Stores;
use crate::refresh::LazyRefresher;
use crate::scheduler_dispatch::http::{JsonHttpClient, json_http_client};
use crate::scheduler_dispatch::outcomes::metadata_outcome;
use crate::subscription_quota_cache::SubscriptionQuotaCache;

pub(crate) struct SchedulerDispatchDeps {
    pub backend: SchedulerBackend,
    pub config: Config,
    pub storage: Arc<dyn Storage>,
    pub stores: Arc<Stores>,
    pub aead: Arc<AeadService>,
    pub oauth_cfg: Arc<AnthropicOAuthConfig>,
    pub runtime: Arc<ExtismRuntime>,
    pub data_dir: std::path::PathBuf,
    pub lazy_refresher: Option<Arc<LazyRefresher>>,
    pub subscription_quota_sink: cc_lb_core::SubscriptionQuotaSink,
    pub subscription_quota_cache: Arc<SubscriptionQuotaCache>,
    pub cancel: CancellationToken,
    pub replica_id: Option<Uuid>,
    pub price_catalog: Arc<cc_lb_pricing::PriceCatalog>,
}

#[derive(Clone)]
pub(super) struct SchedulerDispatch {
    pub(super) backend: SchedulerBackend,
    pub(super) config: Arc<Config>,
    pub(super) storage: Arc<dyn Storage>,
    pub(super) stores: Arc<Stores>,
    pub(super) aead: Arc<AeadService>,
    pub(super) oauth_cfg: Arc<AnthropicOAuthConfig>,
    pub(super) runtime: Arc<ExtismRuntime>,
    pub(super) data_dir: Arc<std::path::PathBuf>,
    pub(super) lazy_refresher: Option<Arc<LazyRefresher>>,
    pub(super) subscription_quota_sink: cc_lb_core::SubscriptionQuotaSink,
    pub(super) subscription_quota_cache: Arc<SubscriptionQuotaCache>,
    pub(super) cancel: CancellationToken,
    pub(super) replica_id: Option<Uuid>,
    pub(super) price_catalog: Arc<cc_lb_pricing::PriceCatalog>,
    pub(super) http: JsonHttpClient,
}

pub(crate) fn build_scheduler_ctx(deps: SchedulerDispatchDeps) -> SchedulerCtx {
    let dispatch = SchedulerDispatch::new(deps);
    SchedulerCtx::new(
        dispatch.config.scheduler.clone(),
        dispatch.clone().adaptive_dispatch(),
        dispatch.clone().cron_dispatch(),
    )
}

impl SchedulerDispatch {
    fn new(deps: SchedulerDispatchDeps) -> Self {
        Self {
            backend: deps.backend,
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
            http: json_http_client(),
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
                );
                metadata_outcome(MetadataRefreshJobHandler::new(runner).handle(job).await?)
            }
        }
    }
}
