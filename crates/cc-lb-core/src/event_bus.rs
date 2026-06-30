//! Cross-component bus for `RequestEvent` lifecycle updates.
//!
//! Every payload is a [`RequestEventUpdate`] tagged with
//! [`RequestEventPhase::Partial`] (mid-flight snapshot for live UI updates) or
//! [`RequestEventPhase::Final`] (one-shot terminal snapshot that the durable
//! consumer persists to `request_events_v1`).
//!
//! ## Two consumer roles
//!
//! - **Durable consumer** (at most one per bus): the DB writer task. Receives
//!   updates via [`InMemoryBus::attach_writer`] — a dedicated bounded mpsc so
//!   the writer's pace cannot be affected by slow ephemeral consumers.
//! - **Ephemeral consumers** (many): admin SSE subscribers powering the live
//!   dashboard. Obtained via [`RequestEventBus::subscribe`] and backed by
//!   `tokio::sync::broadcast`. Slow consumers receive `RecvError::Lagged(n)`
//!   and emit a `resync_required` UI signal instead of back-pressuring the
//!   producer.
//!
//! ## Publish semantics
//!
//! [`RequestEventBus::publish`] is **synchronous and non-blocking**. The proxy
//! hot path and the `TerminalObserver` `Drop` fallback both call it without
//! `.await`. Failures (no receivers, writer mpsc full) increment metrics and
//! `tracing::warn!` but never block the producer.

use std::sync::{Arc, Mutex};

use cc_lb_storage_api::RequestEvent;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc};

/// Default capacity for the broadcast channel powering admin SSE subscribers.
pub const DEFAULT_BROADCAST_CAPACITY: usize = 1024;

/// Default capacity for the durable DB-writer mpsc channel.
///
/// At typical 30 RPS this absorbs ~136 seconds of burst before drop-newest
/// engages.
pub const DEFAULT_WRITER_CAPACITY: usize = 4096;

/// Errors surfaced by [`RequestEventBus`] implementations.
#[derive(Debug, thiserror::Error)]
pub enum BusError {
    #[error("event bus closed")]
    Closed,
    #[error("event bus backend failure: {0}")]
    Backend(String),
}

/// Phase tag for `RequestEvent` updates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RequestEventPhase {
    /// Mid-flight snapshot for live UI updates. **Never persisted.**
    Partial,
    /// One-shot terminal snapshot. Durable consumer persists this to DB.
    Final,
}

impl RequestEventPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Partial => "partial",
            Self::Final => "final",
        }
    }
}

/// Wire envelope carried by [`RequestEventBus::publish`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestEventUpdate {
    pub phase: RequestEventPhase,
    pub event: RequestEvent,
}

impl RequestEventUpdate {
    pub fn partial(event: RequestEvent) -> Self {
        Self {
            phase: RequestEventPhase::Partial,
            event,
        }
    }

    /// Construct a `Final` update.
    ///
    /// Trailing underscore avoids the `final` reserved keyword.
    pub fn final_(event: RequestEvent) -> Self {
        Self {
            phase: RequestEventPhase::Final,
            event,
        }
    }

    pub fn is_final(&self) -> bool {
        matches!(self.phase, RequestEventPhase::Final)
    }
}

/// Receiver side of [`RequestEventBus::subscribe`] (ephemeral consumers).
pub enum BusReceiver {
    InMemory(broadcast::Receiver<RequestEventUpdate>),
    Remote(mpsc::Receiver<RequestEventUpdate>),
}

/// Transport-agnostic event sink used by `Lifecycle` (producer), the DB writer
/// task (durable consumer via [`InMemoryBus::attach_writer`]), and the admin
/// SSE handler (ephemeral consumer via [`subscribe`](RequestEventBus::subscribe)).
pub trait RequestEventBus: Send + Sync + 'static {
    /// Publish an event update. **Synchronous, non-blocking.**
    ///
    /// Hot-path contract: never block on slow consumers. Failures are
    /// best-effort logged + metered by the impl.
    fn publish(&self, update: RequestEventUpdate);

    /// Subscribe an ephemeral consumer (live dashboard SSE).
    ///
    /// Slow consumers may observe `Lagged(n)`.
    fn subscribe(&self) -> BusReceiver;
}

/// Default single-process implementation.
///
/// Holds a broadcast channel for ephemeral SSE subscribers and an optional
/// mpsc channel for the durable DB writer. `publish` synchronously fans out
/// to both.
#[derive(Clone)]
pub struct InMemoryBus {
    inner: Arc<InMemoryBusInner>,
}

struct InMemoryBusInner {
    broadcast_tx: broadcast::Sender<RequestEventUpdate>,
    writer_tx: Mutex<Option<mpsc::Sender<RequestEventUpdate>>>,
}

impl InMemoryBus {
    /// Construct with default broadcast capacity.
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_BROADCAST_CAPACITY)
    }

    /// Construct with an explicit broadcast channel capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        let bounded_capacity = capacity.max(1);
        let (broadcast_tx, _) = broadcast::channel(bounded_capacity);
        Self {
            inner: Arc::new(InMemoryBusInner {
                broadcast_tx,
                writer_tx: Mutex::new(None),
            }),
        }
    }

    /// Attach the durable DB-writer consumer.
    ///
    /// Returns the receiver to be moved into the writer task. Only one writer
    /// may be attached; subsequent calls replace the previous sender (the
    /// previous receiver still sees its already-buffered items but no new
    /// publishes).
    pub fn attach_writer(&self, capacity: usize) -> mpsc::Receiver<RequestEventUpdate> {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        let mut guard = self
            .inner
            .writer_tx
            .lock()
            .expect("event bus writer mutex poisoned");
        *guard = Some(tx);
        rx
    }
}

impl Default for InMemoryBus {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestEventBus for InMemoryBus {
    fn publish(&self, update: RequestEventUpdate) {
        // Broadcast to ephemeral SSE consumers (best-effort, drop-oldest on
        // lag with `Lagged(n)` signal). `SendError` is returned only when
        // there are no live receivers — fine, swallow.
        let _ = self.inner.broadcast_tx.send(update.clone());

        // Try-send to durable writer (drop-newest on full + warn metric).
        // Lock held briefly (just to clone the Sender handle).
        let writer_tx = {
            let guard = self
                .inner
                .writer_tx
                .lock()
                .expect("event bus writer mutex poisoned");
            guard.clone()
        };
        if let Some(tx) = writer_tx {
            match tx.try_send(update) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(dropped)) => {
                    cc_lb_observability::increment_dropped_events_by(
                        "request_event_writer_full",
                        1,
                    );
                    tracing::warn!(
                        request_id = %dropped.event.request_id,
                        phase = dropped.phase.as_str(),
                        "request event writer mpsc full; dropping event (DB row may be missing)",
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    // Writer task exited; expected during shutdown.
                    tracing::debug!("request event writer mpsc closed");
                }
            }
        }
    }

    fn subscribe(&self) -> BusReceiver {
        BusReceiver::InMemory(self.inner.broadcast_tx.subscribe())
    }
}

/// Convenience: wrap an [`InMemoryBus`] in an `Arc<dyn RequestEventBus>`.
pub fn new_in_memory_bus() -> Arc<dyn RequestEventBus> {
    Arc::new(InMemoryBus::new())
}

/// Record a `sse_lagged` event in observability metrics.
///
/// Kept at this path for backward compatibility with code that imported it
/// from the (now removed) `dashboard_broadcaster` module.
pub fn record_dashboard_sse_lagged(skipped: u64) {
    cc_lb_observability::increment_dropped_events_by("sse_lagged", skipped);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_event(request_id: &str) -> RequestEvent {
        RequestEvent {
            ts: 1_700_000_000,
            request_id: request_id.to_string(),
            status: 200,
            duration_ms: 42,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn publish_fans_out_to_broadcast_and_writer() {
        let bus = InMemoryBus::new();
        let BusReceiver::InMemory(mut rx_sse) = bus.subscribe() else {
            panic!("InMemoryBus should yield InMemory receiver");
        };
        let mut rx_writer = bus.attach_writer(8);

        bus.publish(RequestEventUpdate::partial(sample_event("req-1")));
        bus.publish(RequestEventUpdate::final_(sample_event("req-2")));

        let sse_a = rx_sse.recv().await.expect("sse partial");
        let sse_b = rx_sse.recv().await.expect("sse final");
        assert_eq!(sse_a.phase, RequestEventPhase::Partial);
        assert_eq!(sse_a.event.request_id, "req-1");
        assert_eq!(sse_b.phase, RequestEventPhase::Final);
        assert_eq!(sse_b.event.request_id, "req-2");

        let wrt_a = rx_writer.recv().await.expect("writer partial");
        let wrt_b = rx_writer.recv().await.expect("writer final");
        assert_eq!(wrt_a.event.request_id, "req-1");
        assert_eq!(wrt_b.event.request_id, "req-2");
    }

    #[tokio::test]
    async fn publish_with_no_subscribers_is_noop() {
        let bus = InMemoryBus::new();
        bus.publish(RequestEventUpdate::final_(sample_event("orphan")));
    }

    #[tokio::test]
    async fn writer_full_drops_newest_silently() {
        let bus = InMemoryBus::new();
        let _rx = bus.attach_writer(1);
        bus.publish(RequestEventUpdate::final_(sample_event("a")));
        // Second publish observes mpsc-full and increments the dropped metric.
        bus.publish(RequestEventUpdate::final_(sample_event("b")));
    }

    #[tokio::test]
    async fn sse_subscriber_receives_lagged_when_falling_behind() {
        let bus = InMemoryBus::with_capacity(2);
        let BusReceiver::InMemory(mut rx) = bus.subscribe() else {
            panic!("InMemoryBus should yield InMemory receiver");
        };
        bus.publish(RequestEventUpdate::final_(sample_event("a")));
        bus.publish(RequestEventUpdate::final_(sample_event("b")));
        bus.publish(RequestEventUpdate::final_(sample_event("c")));

        let err = rx.recv().await.expect_err("expected Lagged");
        match err {
            broadcast::error::RecvError::Lagged(skipped) => {
                assert!(skipped >= 1, "expected at least one skipped, got {skipped}");
            }
            other => panic!("expected Lagged, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn lag_helper_increments_dropped_event_counter() {
        let before = cc_lb_observability::dropped_events_total();
        record_dashboard_sse_lagged(3);
        assert_eq!(cc_lb_observability::dropped_events_total() - before, 3);
    }
}
