pub mod auth;
pub mod management;
pub mod oauth;
pub mod principals;
pub mod routes;

use arc_swap::ArcSwap;
use std::sync::Arc;

use axum::Router;
use cc_lb_config::Config;
use cc_lb_core::{
    api_keys::{limit_engine::LimitEngine, principal_view::PrincipalView},
    Lifecycle,
};
use cc_lb_storage_redb::Storage;

#[derive(Clone)]
pub struct AdminState {
    pub storage: Option<Arc<Storage>>,
    pub limit_engine: Arc<LimitEngine>,
    pub lifecycle: Option<Arc<Lifecycle>>,
    pub principal_view: Arc<ArcSwap<PrincipalView>>,
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
