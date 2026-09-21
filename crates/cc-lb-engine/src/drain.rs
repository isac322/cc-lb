use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::header::{HeaderValue, RETRY_AFTER};
use axum::http::{Request, Response, StatusCode};
use axum::middleware::Next;
use bytes::Bytes;
use http_body::Frame;
use tokio::sync::Notify;

use crate::terminal_observer::TerminalClassification;

#[derive(Clone)]
pub struct DrainController {
    inner: Arc<DrainState>,
}

struct DrainState {
    drain_started: AtomicBool,
    force_marked: AtomicBool,
    in_flight: AtomicUsize,
    force_closed_total: AtomicU64,
    drained: Notify,
}

impl Default for DrainController {
    fn default() -> Self {
        Self::new()
    }
}

impl DrainController {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DrainState {
                drain_started: AtomicBool::new(false),
                force_marked: AtomicBool::new(false),
                in_flight: AtomicUsize::new(0),
                force_closed_total: AtomicU64::new(0),
                drained: Notify::new(),
            }),
        }
    }

    pub fn trigger(&self) {
        self.set_draining(true);
    }

    pub fn is_draining(&self) -> bool {
        self.inner.drain_started.load(Ordering::Acquire)
    }

    pub fn in_flight(&self) -> usize {
        self.inner.in_flight.load(Ordering::Acquire)
    }

    pub fn force_closed_total(&self) -> u64 {
        self.inner.force_closed_total.load(Ordering::Acquire)
    }

    pub async fn await_drained(&self, deadline: Duration) -> bool {
        if self.in_flight() == 0 {
            return false;
        }

        tokio::time::timeout(deadline, self.wait_until_drained())
            .await
            .is_err()
    }

    pub fn mark_force_closed(&self) -> usize {
        if self.inner.force_marked.swap(true, Ordering::AcqRel) {
            return 0;
        }

        let remaining = self.in_flight();
        if remaining == 0 {
            return 0;
        }

        let increment = u64::try_from(remaining).unwrap_or(u64::MAX);
        self.inner
            .force_closed_total
            .fetch_add(increment, Ordering::AcqRel);
        metrics::counter!("cc_lb_drain_force_closed_total").increment(increment);
        self.inner.drained.notify_waiters();
        remaining
    }

    pub fn set_draining(&self, draining: bool) {
        self.inner.drain_started.store(draining, Ordering::Release);
        metrics::gauge!("cc_lb_drain_in_progress").set(if draining { 1.0 } else { 0.0 });
        if !draining {
            self.inner.force_marked.store(false, Ordering::Release);
        }
        self.inner.drained.notify_waiters();
    }

    fn try_track(&self) -> Option<InFlightGuard> {
        if self.is_draining() {
            return None;
        }

        self.inner.in_flight.fetch_add(1, Ordering::AcqRel);
        if self.is_draining() {
            self.release_one();
            return None;
        }

        Some(InFlightGuard {
            controller: self.clone(),
        })
    }

    async fn wait_until_drained(&self) {
        loop {
            if self.in_flight() == 0 {
                return;
            }
            self.inner.drained.notified().await;
        }
    }

    fn release_one(&self) {
        if self.inner.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.drained.notify_waiters();
        }
    }
}

pub async fn proxy_drain_middleware(
    State(controller): State<DrainController>,
    request: Request<Body>,
    next: Next,
) -> Response<Body> {
    let Some(guard) = controller.try_track() else {
        return draining_response();
    };

    let response = next.run(request).await;
    // The request stays in flight until its response body is fully relayed or
    // dropped, so `await_drained` covers streaming bodies — not just headers.
    response.map(|body| guard_body(body, guard))
}

fn draining_response() -> Response<Body> {
    let mut response = Response::new(Body::from("draining"));
    *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
    response
        .headers_mut()
        .insert(RETRY_AFTER, HeaderValue::from_static("60"));
    response
        .extensions_mut()
        .insert(TerminalClassification::DRAIN_REJECTED);
    response
}

/// Wraps `body` so `guard` is released only when the body is exhausted or
/// dropped (client disconnect). Frames pass through unchanged and the inner
/// body's `size_hint`/`is_end_stream` are preserved, so a buffered response
/// keeps its `content-length` instead of falling back to chunked framing.
fn guard_body(body: Body, guard: InFlightGuard) -> Body {
    Body::new(GuardedBody {
        inner: body,
        guard: Some(guard),
    })
}

struct GuardedBody {
    inner: Body,
    guard: Option<InFlightGuard>,
}

impl http_body::Body for GuardedBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        // `axum::body::Body` is `Unpin`, so no pin projection is needed.
        let poll = Pin::new(&mut self.inner).poll_frame(cx);
        if matches!(poll, Poll::Ready(None)) {
            // End of stream: release in-flight now instead of waiting for the
            // response to be dropped.
            self.guard.take();
        }
        poll
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

struct InFlightGuard {
    controller: DrainController,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.controller.release_one();
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::time::Duration;

    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::middleware;
    use axum::response::Response;
    use axum::routing::get;
    use bytes::Bytes;
    use tokio::sync::Notify;
    use tower::ServiceExt;

    use super::{DrainController, guard_body, proxy_drain_middleware};

    #[tokio::test]
    async fn response_body_stream_holds_in_flight_until_dropped() {
        let controller = DrainController::new();
        let app = Router::new()
            .route("/", get(streaming_response))
            .route_layer(middleware::from_fn_with_state(
                controller.clone(),
                proxy_drain_middleware,
            ));

        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        // The guard lives in the response body: headers alone do not release it.
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(controller.in_flight(), 1);
        assert!(controller.await_drained(Duration::from_millis(1)).await);

        drop(response);
        assert_eq!(controller.in_flight(), 0);
    }

    #[tokio::test]
    async fn request_processing_still_holds_in_flight() {
        let controller = DrainController::new();
        let entered = std::sync::Arc::new(Notify::new());
        let release = std::sync::Arc::new(Notify::new());
        let route_entered = entered.clone();
        let route_release = release.clone();
        let app = Router::new()
            .route(
                "/",
                get(move || {
                    let entered = route_entered.clone();
                    let release = route_release.clone();
                    async move {
                        entered.notify_one();
                        release.notified().await;
                        "ok"
                    }
                }),
            )
            .route_layer(middleware::from_fn_with_state(
                controller.clone(),
                proxy_drain_middleware,
            ));

        let request = tokio::spawn(async move {
            app.oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
                .await
                .unwrap()
        });

        entered.notified().await;
        assert_eq!(controller.in_flight(), 1);

        controller.trigger();
        assert!(controller.await_drained(Duration::from_millis(1)).await);

        release.notify_one();
        let response = request.await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        drop(response);
        assert_eq!(controller.in_flight(), 0);
    }

    #[test]
    fn guard_body_preserves_size_hint() {
        // A buffered body has an exact size hint; losing it makes hyper fall
        // back to chunked framing while the stale `content-length` header
        // survives, corrupting the payload for clients.
        let controller = DrainController::new();
        let guard = controller.try_track().unwrap();
        let inner = Body::from("buffered");
        let expected = http_body::Body::size_hint(&inner).exact();
        assert!(expected.is_some());

        let wrapped = guard_body(inner, guard);

        assert_eq!(http_body::Body::size_hint(&wrapped).exact(), expected);
    }

    async fn streaming_response() -> Response {
        let stream = async_stream::stream! {
            yield Ok::<Bytes, Infallible>(Bytes::from_static(b"data: hello\n\n"));
            std::future::pending::<()>().await;
        };
        Response::new(Body::from_stream(stream))
    }
}
