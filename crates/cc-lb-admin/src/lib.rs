pub mod auth;
pub mod dashboard;
pub mod events;
pub mod management;
pub mod oauth;
mod principals;
pub mod routes;
pub mod settings;
pub mod status;

use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use cc_lb_config::Config;
use cc_lb_core::{
    BreakerRegistry, BulkheadRegistry, DashboardBroadcaster, DrainController, Lifecycle,
    QuotaManager,
};
use cc_lb_storage_redb::Storage;

#[derive(Clone, Debug)]
pub struct PluginRuntimeSlotStatus {
    pub loaded: bool,
    pub disabled: bool,
    pub failure_count: u64,
    pub last_error: Option<String>,
}

pub trait PluginRuntimeStatus: Send + Sync {
    fn plugin_status(&self, plugin_name: &str) -> Option<PluginRuntimeSlotStatus>;
}

#[derive(Clone)]
pub struct AdminState {
    pub storage: Option<Arc<Storage>>,
    pub quota_manager: Option<Arc<QuotaManager>>,
    pub lifecycle: Option<Arc<Lifecycle>>,
    pub breaker_registry: Option<Arc<BreakerRegistry>>,
    pub drain_controller: Option<DrainController>,
    pub bulkhead_registry: Option<Arc<BulkheadRegistry>>,
    pub plugin_runtime_status: Option<Arc<dyn PluginRuntimeStatus>>,
    pub dashboard_broadcaster: Arc<DashboardBroadcaster>,
    pub config: Arc<dyn CurrentConfig>,
    pub config_path: Option<PathBuf>,
    pub config_watcher: Option<Arc<dyn ConfigReloader>>,
    pub config_started_at_unix_secs: u64,
    pub admin_token: Option<String>,
    pub start_time: std::time::Instant,
}

pub trait CurrentConfig: Send + Sync {
    fn current_config(&self) -> Arc<Config>;
}

impl CurrentConfig for Config {
    fn current_config(&self) -> Arc<Config> {
        Arc::new(self.clone())
    }
}

pub trait ConfigReloader: Send + Sync {
    fn reload_now(&self) -> Result<(), String>;
}

pub fn router(state: AdminState) -> Router {
    routes::build_router(state)
}
