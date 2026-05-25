pub mod auth;
mod oauth_pkce;
mod credential_crypto;
pub mod management;
pub mod oauth;
pub mod principals;
pub mod routes;

use arc_swap::ArcSwap;
use std::sync::Arc;

use axum::Router;
use cc_lb_aead::AeadService;
use cc_lb_config::Config;
use cc_lb_core::{
    api_keys::{limit_engine::LimitEngine, principal_view::PrincipalView},
    AuditWriterSink, Lifecycle,
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

pub trait CurrentConfig: Send + Sync {
    fn current_config(&self) -> Arc<Config>;

    fn put_draft_config(&self, _config: Config) -> Result<(), ConfigDraftError> {
        Err(ConfigDraftError::Unavailable)
    }

    fn apply_draft_config(&self) -> Result<Arc<Config>, ConfigDraftError> {
        Err(ConfigDraftError::Unavailable)
    }
}

impl CurrentConfig for Config {
    fn current_config(&self) -> Arc<Config> {
        Arc::new(self.clone())
    }
}

pub fn router(state: AdminState) -> Router {
    routes::build_router(state)
}
