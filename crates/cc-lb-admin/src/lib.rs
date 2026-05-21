pub mod auth;
pub mod oauth;
pub mod routes;

use std::sync::Arc;

use axum::Router;
use cc_lb_config::Config;
use cc_lb_core::{Lifecycle, QuotaManager};
use cc_lb_storage_redb::Storage;

#[derive(Clone)]
pub struct AdminState {
    pub storage: Option<Arc<Storage>>,
    pub quota_manager: Option<Arc<QuotaManager>>,
    pub lifecycle: Option<Arc<Lifecycle>>,
    pub config: Arc<dyn CurrentConfig>,
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

pub fn router(state: AdminState) -> Router {
    routes::build_router(state)
}
