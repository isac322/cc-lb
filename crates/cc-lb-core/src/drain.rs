use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::header::{HeaderValue, RETRY_AFTER};
use axum::http::{Request, Response, StatusCode};
use axum::middleware::Next;
use bytes::Bytes;
use http_body_util::BodyExt;
use tokio::sync::{watch, Notify};

#[derive(Clone)]
pub struct DrainController {
    inner: Arc<DrainState>,
}

struct DrainState {
    drain_started: AtomicBool,
    force_marked: AtomicBool,
    in_flight: AtomicUsize,
    force_closed_total: AtomicU64,
    force_close: watch::Sender<bool>,
    drained: Notify,
}

impl Default for DrainController {
    fn default() -> Self {
        Self::new()
    }
}

impl DrainController {
    pub fn new() -> Self {
        let (force_close, _) = watch::channel(false);
        Self {
            inner: Arc::new(DrainState {
                drain_started: AtomicBool::new(false),
                force_marked: AtomicBool::new(false),
                in_flight: AtomicUsize::new(0),
                force_closed_total: AtomicU64::new(0),
                force_close,
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
        let _ = self.inner.force_close.send(true);
        self.inner.drained.notify_waiters();
        remaining
    }

    pub fn set_draining(&self, draining: bool) {
        self.inner.drain_started.store(draining, Ordering::Release);
        metrics::gauge!("cc_lb_drain_in_progress").set(if draining { 1.0 } else { 0.0 });
        if !draining {
            self.inner.force_marked.store(false, Ordering::Release);
            let _ = self.inner.force_close.send(false);
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

    fn force_close_receiver(&self) -> watch::Receiver<bool> {
        self.inner.force_close.subscribe()
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
    guard_response(response, guard)
}

fn draining_response() -> Response<Body> {
    let mut response = Response::new(Body::from("draining"));
    *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
    response
        .headers_mut()
        .insert(RETRY_AFTER, HeaderValue::from_static("60"));
    response
}

fn guard_response(response: Response<Body>, guard: InFlightGuard) -> Response<Body> {
    let (parts, mut body) = response.into_parts();
    let mut force_close = guard.controller.force_close_receiver();
    let stream = async_stream::stream! {
        let _guard = guard;
        loop {
            if *force_close.borrow() {
                break;
            }

            tokio::select! {
                changed = force_close.changed() => {
                    if changed.is_err() || *force_close.borrow() {
                        break;
                    }
                }
                frame = body.frame() => {
                    let Some(frame) = frame else {
                        break;
                    };
                    match frame {
                        Ok(frame) => {
                            if let Ok(data) = frame.into_data() {
                                yield Ok::<Bytes, axum::Error>(data);
                            }
                        }
                        Err(source) => {
                            yield Err::<Bytes, axum::Error>(source);
                            break;
                        }
                    }
                }
            }
        }
    };
    Response::from_parts(parts, Body::from_stream(stream))
}

struct InFlightGuard {
    controller: DrainController,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.controller.release_one();
    }
}
