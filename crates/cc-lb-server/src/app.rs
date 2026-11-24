use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(feature = "admin")]
use cc_lb_admin::AdminState;
use cc_lb_runtime_sync::admin_http::{RequestIdState, request_id_middleware};
#[cfg(feature = "admin")]
use cc_lb_runtime_sync::admin_http::{
    compression_layer as admin_compression_layer, with_browser_security_headers,
};

use async_trait::async_trait;
use axum::Extension;
use axum::Json;
use axum::Router;
use axum::body::Body;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{OriginalUri, State};
use axum::http::header::{
    CONNECTION, CONTENT_LENGTH, HOST, HeaderValue, RETRY_AFTER, SEC_WEBSOCKET_KEY,
    SEC_WEBSOCKET_PROTOCOL, SEC_WEBSOCKET_VERSION, UPGRADE,
};
use axum::http::{HeaderMap, HeaderName, Request, Response, StatusCode, Uri};
use axum::middleware::{self, Next};
use axum::routing::{any, get, post};
use bytes::{Bytes, BytesMut};
use cc_lb_aead::AeadService;
#[cfg(all(test, feature = "admin"))]
use cc_lb_clock::ClockHandle;
use cc_lb_control::DynamicView;
use cc_lb_engine::realtime_websocket::{
    RealtimeStreamMetrics, RealtimeUsage, RealtimeWebSocketKind, RealtimeWebSocketSession,
    RealtimeWebSocketTarget, RealtimeWebSocketTermination, gemini_setup_model,
};
use cc_lb_engine::responses_websocket::{
    ResponsesWebSocketGateway, WebSocketDispatch, WebSocketError,
};
use cc_lb_engine::{
    DrainController, HopByHopStripLayer, ProviderScope, UpstreamDispatch, anthropic_error_response,
    provider_scope_error_response,
};
use cc_lb_storage_api::UpstreamRecord;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_upstream::{SignedRequest, scrub_query_api_key};
use futures_util::{FutureExt, SinkExt, StreamExt};
use http_body_util::{BodyExt, LengthLimitError, Limited};
use serde::Serialize;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::{
    Message as UpstreamMessage, client::IntoClientRequest, protocol::WebSocketConfig,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, client_async_tls_with_config, connect_async_with_config,
};
use tokio_util::sync::CancellationToken;
use tower::ServiceBuilder;
use tracing::Instrument as _;

use crate::build_meta::BuildMeta;
use crate::state_machine::ServerState;

#[cfg(feature = "admin")]
mod admin_wiring;
#[path = "app/body.rs"]
mod body;
#[path = "app/bootstrap.rs"]
mod bootstrap;
#[path = "input_tokens_ws.rs"]
mod input_tokens_ws;
#[path = "app/proxy_router.rs"]
mod proxy_router;
#[path = "app_realtime_websocket.rs"]
mod realtime_websocket;
#[path = "app/responses_request_body.rs"]
mod responses_request_body;
#[path = "app/responses_websocket.rs"]
mod responses_websocket;
#[cfg(all(test, feature = "admin"))]
#[path = "app/tests.rs"]
mod tests;

use body::*;
use bootstrap::*;
use proxy_router::*;
use realtime_websocket::{
    codex_backend_root_handler, gemini_live_websocket_handler, openai_live_call_websocket_handler,
    openai_live_websocket_handler, openai_realtime_websocket_handler,
};
use responses_websocket::*;

#[cfg(feature = "admin")]
pub use admin_wiring::{
    App, build_app, build_app_for_testing, build_app_with_path, build_app_with_storage, run_serve,
};
#[cfg(all(feature = "admin", feature = "storage-postgres"))]
pub use admin_wiring::{build_app_for_testing_postgres, seed_app_testing_storage};
pub use bootstrap::{
    BuildError, ProxyApp, ProxyRole, ProxyServeOptions, ServeError, build_proxy_app,
    build_proxy_app_with_path, build_proxy_app_with_storage, open_storage, resolve_data_dir,
    run_proxy_serve,
};

#[cfg(feature = "admin")]
fn admin_router(admin_state: AdminState) -> Router {
    let admin_router = cc_lb_admin::router(admin_state);
    let admin_router = cc_lb_admin::routes::with_request_tracing(admin_router)
        // Admin surface only — proxy_router stays uncompressed to keep SSE
        // bodies streaming and skip CPU on the hot data plane. ETagged
        // static assets skip dynamic compression to keep strong ETags valid.
        // Defaults (gzip 6, brotli 4) are deliberate; avoid Best/level 11.
        .layer(admin_compression_layer())
        // Reuse the existing request-id seam on the separate admin listener so
        // admin spans and responses carry the same correlation identifier.
        .layer(middleware::from_fn_with_state(
            RequestIdState::counter("req_admin_"),
            request_id_middleware,
        ));

    with_browser_security_headers(admin_router)
}
