use std::sync::Arc;

use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post};

use crate::oauth::{authorize, refresh_history, token};

mod config;
#[cfg(any(debug_assertions, feature = "debug-endpoints"))]
mod debug;
mod handlers;
mod helpers;
mod state;

pub use config::{AppConfig, MessageScript, RecordedMessageRequest, ScriptedMessageResponse};
pub(crate) use state::AppState;
pub use state::with_fixture_headers;

pub fn app(config: AppConfig) -> Router {
    let max_body = config.files_cap_bytes;
    let state = Arc::new(AppState::new(config));

    let router = Router::new()
        .route("/v1/messages", post(handlers::messages))
        .route("/v1/messages/count_tokens", post(handlers::count_tokens))
        .route("/oauth/authorize", get(authorize))
        .route("/oauth/token", post(token))
        .route("/v1/oauth/token", post(token))
        .route("/__refresh_history", get(refresh_history))
        .route("/__last_request", get(handlers::last_request));

    #[cfg(any(debug_assertions, feature = "debug-endpoints"))]
    let router = router
        .route("/__inject_cache_stats", post(debug::inject_cache_stats))
        .route(
            "/__set_default_cache_stats",
            post(debug::set_default_cache_stats),
        );

    router
        .route("/v1/models", get(handlers::list_models))
        .route("/v1/models/{id}", get(handlers::get_model))
        .route(
            "/v1/files",
            get(handlers::list_files).post(handlers::create_file),
        )
        .route(
            "/v1/files/{id}",
            get(handlers::get_file).delete(handlers::delete_file),
        )
        .fallback(handlers::not_found)
        .with_state(state)
        .layer(DefaultBodyLimit::max(max_body))
}
