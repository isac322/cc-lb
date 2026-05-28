pub mod auth;
mod credential_crypto;
pub mod management;
pub mod oauth;
mod oauth_pkce;
pub mod principals;
pub mod routes;
pub mod settings;
pub mod status;

use arc_swap::ArcSwap;
use std::sync::Arc;

use axum::Router;
use cc_lb_aead::AeadService;
use cc_lb_config::Config;
use serde::Serialize;

use cc_lb_core::{
    AuditWriterSink, Lifecycle,
    api_keys::{limit_engine::LimitEngine, principal_view::PrincipalView},
};
use cc_lb_storage_redb::Storage;

#[derive(Clone)]
pub struct AdminState {
    pub storage: Option<Arc<Storage>>,
    pub aead: Arc<AeadService>,
    pub limit_engine: Arc<LimitEngine>,
    pub lifecycle: Option<Arc<Lifecycle>>,
    pub audit_sink: Option<Arc<AuditWriterSink>>,
    pub principal_view: Arc<ArcSwap<PrincipalView>>,
    pub config: Arc<dyn CurrentConfig>,
    pub admin_token: Option<String>,
    pub start_time: std::time::Instant,
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

    fn last_reload_status(&self) -> Option<LastReloadStatus> {
        None
    }

    fn put_draft_config(&self, _config: Config) -> Result<(), ConfigDraftError> {
        Err(ConfigDraftError::Unavailable)
    }

    fn apply_draft_config(&self) -> Result<Arc<Config>, ConfigDraftError> {
        Err(ConfigDraftError::Unavailable)
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
