pub use cc_lb_engine::DrainController;

use axum::body::Body;
use axum::extract::State;
use axum::http::header::RETRY_AFTER;
use axum::http::{Request, Response, StatusCode};
use axum::middleware::Next;

/// Drain gate for the proxy listener.
///
/// In-flight accounting stays with `cc_lb_engine::proxy_drain_middleware`.
/// `lifecycle_middleware` runs earlier in the proxy stack, so the request
/// already carries a `LifecycleContext` in its extensions; when the gate
/// rejects with the engine's `503 + retry-after` draining response, record the
/// rejection so the request still produces a request-log row instead of
/// vanishing.
pub async fn proxy_drain_middleware(
    State(controller): State<DrainController>,
    request: Request<Body>,
    next: Next,
) -> Response<Body> {
    let observer = request
        .extensions()
        .get::<cc_lb_engine::LifecycleContext>()
        .cloned();
    let response =
        cc_lb_engine::proxy_drain_middleware(State(controller.clone()), request, next).await;
    if let Some(observer) = observer
        && controller.is_draining()
        && response.status() == StatusCode::SERVICE_UNAVAILABLE
        && response.headers().contains_key(RETRY_AFTER)
    {
        observer.record_drain_rejection();
    }
    response
}
