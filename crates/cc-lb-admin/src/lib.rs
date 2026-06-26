pub mod auth;
mod credential_crypto;
pub mod credentials;
pub mod dashboard;
pub mod dashboard_routes;
pub mod events;
pub mod events_routes;
pub mod management;
mod oauth_pkce;
pub mod principals;
pub mod routes;
pub mod scheduler;
pub mod settings;
mod static_assets;
pub mod status;
pub mod subscription_quotas;
pub mod v1;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use axum::http::{HeaderMap, StatusCode};
use cc_lb_aead::AeadService;
use cc_lb_config::{Config, RestartRequiredField};
use cc_lb_signer_anthropic_oauth::LazyRefreshHandle;
use serde::Serialize;

use cc_lb_core::{
    AuditWriterSink, DynamicView, DynamicViewHolder, Lifecycle, MetadataHookHandle,
    RequestEventBus,
    api_keys::{key_store::KeyStore, limit_engine::LimitEngine},
};
use cc_lb_runtime_extism::ExtismRuntime;
use cc_lb_storage_api::{Storage, UpstreamRecord};

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
        runtime: &ExtismRuntime,
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

#[derive(Clone)]
pub struct AdminState {
    pub storage: Option<Arc<dyn Storage>>,
    pub key_store: Option<Arc<KeyStore>>,
    pub aead: Arc<AeadService>,
    pub limit_engine: Arc<LimitEngine>,
    pub lifecycle: Option<Arc<Lifecycle>>,
    pub subscription_metadata_hook: Option<MetadataHookHandle>,
    pub lazy_refresher: Option<Arc<dyn LazyRefreshHandle>>,
    pub runtime: Option<Arc<ExtismRuntime>>,
    pub data_dir: Option<PathBuf>,
    pub warmup_dialect_dispatcher: Option<Arc<dyn WarmupDialectDispatcher>>,
    pub audit_sink: Option<Arc<AuditWriterSink>>,
    pub dynamic_view: Arc<DynamicViewHolder>,
    pub config: Arc<dyn CurrentConfig>,
    pub scheduler: Option<cc_lb_scheduler::admin::SchedulerAdminHandle>,
    pub admin_token: Option<String>,
    pub start_time: std::time::Instant,
    pub event_bus: Option<Arc<dyn RequestEventBus>>,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigDraftError {
    #[error("config draft/apply is unavailable for this server configuration")]
    Unavailable,
    #[error("no config draft is pending")]
    MissingDraft,
    #[error("invalid draft config: {0}")]
    Invalid(String),
}

#[derive(Clone, Debug, Serialize)]
pub struct LastReloadStatus {
    pub timestamp_unix_secs: u64,
    pub outcome: ReloadOutcome,
    pub config_path: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub enum ReloadOutcome {
    Success,
    Failure {
        reason: String,
        principal: Option<String>,
        plugin: Option<String>,
    },
}

pub trait CurrentConfig: Send + Sync {
    fn current_config(&self) -> Arc<Config>;

    fn restart_required_changes(&self) -> Vec<RestartRequiredField> {
        Vec::new()
    }

    fn last_reload_status(&self) -> Option<LastReloadStatus> {
        None
    }

    fn put_draft_config(&self, _config: Config) -> Result<(), ConfigDraftError> {
        Err(ConfigDraftError::Unavailable)
    }

    fn apply_draft_config(&self) -> Result<Arc<Config>, ConfigDraftError> {
        Err(ConfigDraftError::Unavailable)
    }

    fn dynamic_view_rebinder(&self) -> Option<Arc<dyn DynamicViewRebinder>> {
        None
    }
}

pub trait ConfigReloader: Send + Sync {
    fn reload_now(&self) -> Result<(), String>;
}

impl CurrentConfig for Config {
    fn current_config(&self) -> Arc<Config> {
        Arc::new(self.clone())
    }
}

pub fn router(state: AdminState) -> Router {
    routes::build_router(state)
}
