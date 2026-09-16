pub mod audit;
pub mod auth;
pub mod cache_keepalive_view;
pub mod dashboard;
pub mod dashboard_routes;
pub mod events;
pub mod events_detail_route;
pub mod events_routes;
pub mod internal_partials;
pub mod management;
mod oauth_pkce;
pub mod ports;
pub mod principals;
mod response_cache;
pub mod routes;
pub mod scheduler;
pub mod settings;
mod static_assets;
pub mod subscription_quotas;
pub mod v1;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use axum::http::{HeaderMap, StatusCode};
use cc_lb_aead::AeadService;
use cc_lb_config::Config;
use cc_lb_signer_anthropic_oauth::LazyRefreshHandle;

use cc_lb_clock::ClockHandle;
use cc_lb_control::RequestEventBus;
use cc_lb_control::{
    DynamicView, DynamicViewHolder, MetadataHookHandle,
    api_keys::{key_store::KeyStore, limit_engine::LimitEngine},
};
use cc_lb_domain::ReplicaIdentity;
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use cc_lb_storage_api::{Storage, UpstreamRecord};

use crate::ports::{
    RetainedPartialPort, RoutePreviewPort, SubscriptionQuotaIngestionPort, WarmupPort,
};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum WarmupDialectDispatchErrorKind {
    Transient,
    Permanent,
}

#[derive(Debug, Clone)]
pub struct WarmupDialectDispatchError {
    pub kind: WarmupDialectDispatchErrorKind,
    pub detail: String,
}

impl WarmupDialectDispatchError {
    pub fn new(kind: WarmupDialectDispatchErrorKind, detail: impl Into<String>) -> Self {
        Self {
            kind,
            detail: detail.into(),
        }
    }
}

#[derive(Debug)]
pub struct WarmupDialectDispatchOutcome {
    pub status: StatusCode,
    pub headers: HeaderMap,
}

#[async_trait]
pub trait WarmupDialectDispatcher: Send + Sync {
    async fn dispatch_warmup_with_dialect(
        &self,
        runtime: &Arc<WasmtimeRuntime>,
        data_dir: &Path,
        upstream: &UpstreamRecord,
    ) -> Result<WarmupDialectDispatchOutcome, WarmupDialectDispatchError>;
}

#[async_trait]
pub trait DynamicViewRebinder: Send + Sync {
    async fn rebuild_dynamic_view(
        &self,
        current_generation: u64,
    ) -> anyhow::Result<Arc<DynamicView>>;
}

#[derive(Clone, Default)]
pub struct AdminPorts {
    pub route_preview: Option<Arc<dyn RoutePreviewPort>>,
    pub warmup: Option<Arc<dyn WarmupPort>>,
    pub retained_partials: Option<Arc<dyn RetainedPartialPort>>,
    pub subscription_quota_ingestion: Option<Arc<dyn SubscriptionQuotaIngestionPort>>,
    pub replica_identity: Option<ReplicaIdentity>,
}

#[derive(Clone, Debug, Default)]
pub struct StartupConfigOverrides {
    pub runtime_data_dir: Option<PathBuf>,
}

#[derive(Clone)]
pub struct AdminState {
    pub config_path: Option<PathBuf>,
    pub startup_config_overrides: StartupConfigOverrides,
    pub storage: Option<Arc<dyn Storage>>,
    pub key_store: Option<Arc<KeyStore>>,
    pub aead: Arc<AeadService>,
    pub limit_engine: Arc<LimitEngine>,
    pub lifecycle: Option<AdminPorts>,
    pub subscription_metadata_hook: Option<MetadataHookHandle>,
    pub lazy_refresher: Option<Arc<dyn LazyRefreshHandle>>,
    pub runtime: Option<Arc<WasmtimeRuntime>>,
    pub data_dir: Option<PathBuf>,
    pub warmup_dialect_dispatcher: Option<Arc<dyn WarmupDialectDispatcher>>,
    pub dynamic_view: Arc<DynamicViewHolder>,
    pub config: Arc<Config>,
    pub dynamic_view_rebinder: Option<Arc<dyn DynamicViewRebinder>>,
    pub scheduler: Option<cc_lb_scheduler::admin::SchedulerAdminHandle>,
    pub admin_auth: Arc<auth::AdminAuthenticator>,
    pub start_time: std::time::Instant,
    pub event_bus: Option<Arc<dyn RequestEventBus>>,
    pub storage_tail: tokio::sync::broadcast::Sender<events::StorageTailUpdate>,
    pub clock: ClockHandle,
}

pub fn router(state: AdminState) -> Router {
    routes::build_router(state)
}
