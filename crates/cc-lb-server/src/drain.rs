pub use cc_lb_engine::DrainController;

use axum::body::Body;
use axum::extract::State;
use axum::http::header::RETRY_AFTER;
use axum::http::{Request, Response, StatusCode};
use axum::middleware::Next;

/// Drain gate for the proxy listener.
///
/// In-flight accounting stays with `cc_lb_engine::proxy_drain_middleware`.
/// `lifecycle_middleware` runs earlier in the proxy stack, so when the gate
/// rejects with the engine's `503 + retry-after` draining response we attach
/// the terminal classification as a response extension and the lifecycle
/// middleware finalizes the request-log row on the way out.
pub async fn proxy_drain_middleware(
    State(controller): State<DrainController>,
    request: Request<Body>,
    next: Next,
) -> Response<Body> {
    let mut response =
        cc_lb_engine::proxy_drain_middleware(State(controller.clone()), request, next).await;
    if controller.is_draining()
        && response.status() == StatusCode::SERVICE_UNAVAILABLE
        && response.headers().contains_key(RETRY_AFTER)
    {
        response
            .extensions_mut()
            .insert(cc_lb_engine::TerminalClassification::DRAIN_REJECTED);
    }
    response
}
