pub mod auth;
mod credential_crypto;
pub mod management;
mod oauth_pkce;
pub mod principals;
pub mod routes;
pub mod settings;
pub mod status;
pub mod v1;

use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use cc_lb_aead::AeadService;
use cc_lb_config::{Config, RestartRequiredField};
use serde::Serialize;

use cc_lb_core::{
    AuditWriterSink, DynamicView, DynamicViewHolder, Lifecycle,
    api_keys::{key_store::KeyStore, limit_engine::LimitEngine},
};
use cc_lb_storage_api::Storage;

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
    pub audit_sink: Option<Arc<AuditWriterSink>>,
    pub dynamic_view: Arc<DynamicViewHolder>,
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
