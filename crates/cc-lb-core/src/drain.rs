use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::header::{HeaderValue, RETRY_AFTER};
use axum::http::{Request, Response, StatusCode};
use axum::middleware::Next;
use tokio::sync::Notify;

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
    drop(guard);
    response
}

fn draining_response() -> Response<Body> {
    let mut response = Response::new(Body::from("draining"));
    *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
    response
        .headers_mut()
        .insert(RETRY_AFTER, HeaderValue::from_static("60"));
    response
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

    use super::{DrainController, proxy_drain_middleware};

    #[tokio::test]
    async fn response_body_stream_does_not_hold_in_flight() {
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

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(controller.in_flight(), 0);
        assert!(!controller.await_drained(Duration::from_millis(1)).await);

        drop(response);
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
        assert_eq!(controller.in_flight(), 0);
    }

    async fn streaming_response() -> Response {
        let stream = async_stream::stream! {
            yield Ok::<Bytes, Infallible>(Bytes::from_static(b"data: hello\n\n"));
            std::future::pending::<()>().await;
        };
        Response::new(Body::from_stream(stream))
    }
}
