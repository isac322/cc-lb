//! Cross-component bus for freshly-finalized `RequestEvent`s.
//!
//! The bus is the live-tail backbone for the admin dashboard. `Lifecycle`
//! publishes each `RequestEvent` immediately after persisting it to storage,
//! and the admin SSE handler subscribes for true push semantics (no DB
//! polling).
//!
//! ## Backends
//!
//! - [`InMemoryBus`] (default): single-process `tokio::sync::broadcast`. Used
//!   for SQLite deployments and single-replica Postgres deployments. Cheap and
//!   adds no operational dependencies.
//! - `PostgresBus` (future): Postgres `LISTEN`/`NOTIFY` for multi-instance
//!   Postgres deployments. The trait shape leaves room for it without forcing
//!   producers/consumers to change.
//! - `RedisBus` (future): Redis Streams for shops that prefer Redis. Same
//!   trait — no producer/consumer code changes when added.
//!
//! ## Why a trait
//!
//! `Lifecycle` and the admin SSE handler should not care which transport
//! actually carries the event between processes. The trait pins the contract
//! (`publish` + `subscribe`) so the wiring is decoupled from the backend.

use std::sync::Arc;

use async_trait::async_trait;
use cc_lb_storage_api::RequestEvent;
use tokio::sync::{broadcast, mpsc};

use crate::dashboard_broadcaster::DashboardBroadcaster;

/// Errors surfaced by [`RequestEventBus`] implementations.
///
/// Publish is best-effort by design (the proxy hot path must not block on the
/// bus), so producers typically log-and-continue on `BusError`. Subscribers
/// translate `Lagged` into a UI `resync_required` signal.
#[derive(Debug, thiserror::Error)]
pub enum BusError {
    /// Bus has been closed (no live receivers, transport torn down).
    #[error("event bus closed")]
    Closed,
    /// Backend-specific transient failure (e.g. Postgres `NOTIFY` failed).
    #[error("event bus backend failure: {0}")]
    Backend(String),
}

/// Receiver side of [`RequestEventBus::subscribe`].
///
/// Different backends yield different concrete receiver types. The enum lets
/// the SSE handler `match` once instead of paying for a `Box<dyn Stream>`
/// per-event indirection.
pub enum BusReceiver {
    /// Same-process broadcast receiver. Backed by `tokio::sync::broadcast`,
    /// so slow consumers get `RecvError::Lagged(n)` instead of memory growth.
    InMemory(broadcast::Receiver<RequestEvent>),
    /// Multi-process receiver (e.g. Postgres `LISTEN`/`NOTIFY`). Backed by an
    /// `mpsc` channel fed by a background listener task that drains the
    /// remote queue + same-node mirror.
    Remote(mpsc::Receiver<RequestEvent>),
}

/// Transport-agnostic event fan-out used by `Lifecycle` (producer) and the
/// admin SSE handler (consumer).
///
/// The trait is intentionally narrow: callers only need to `publish` and
/// `subscribe`. Heavy lifting (queue sizing, drop-oldest, listener tasks,
/// node-id filtering, `NOTIFY` payload encoding) lives behind the impl.
#[async_trait]
pub trait RequestEventBus: Send + Sync + 'static {
    /// Publish a freshly-finalized `RequestEvent`.
    ///
    /// **Hot path contract**: implementations must return quickly and must
    /// not block on slow consumers. Failures (no receivers, transport down)
    /// are best-effort logged by the impl — the producer treats publish as
    /// fire-and-forget.
    async fn publish(&self, event: RequestEvent);

    /// Subscribe to the live event stream.
    ///
    /// Same-process events are delivered with sub-millisecond latency.
    /// Cross-process events (when the impl is multi-instance aware) arrive
    /// after a short transport round trip.
    fn subscribe(&self) -> BusReceiver;
}

/// Default single-process implementation.
///
/// Wraps the existing [`DashboardBroadcaster`] (`tokio::sync::broadcast`).
/// Slow consumers receive `RecvError::Lagged(n)` so the SSE handler can emit
/// a `resync_required` frame instead of silently dropping events.
#[derive(Clone, Debug)]
pub struct InMemoryBus {
    broadcaster: DashboardBroadcaster,
}

impl InMemoryBus {
    /// Construct with the default broadcast capacity (`1024`).
    pub fn new() -> Self {
        Self {
            broadcaster: DashboardBroadcaster::new(),
        }
    }

    /// Construct with an explicit broadcast channel capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            broadcaster: DashboardBroadcaster::with_capacity(capacity),
        }
    }
}

impl Default for InMemoryBus {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RequestEventBus for InMemoryBus {
    async fn publish(&self, event: RequestEvent) {
        // `broadcast::Sender::send` returns the count of receivers (Ok) or
        // a `SendError` only when there are no live receivers. Either way the
        // proxy hot path treats publish as fire-and-forget.
        self.broadcaster.publish(event);
    }

    fn subscribe(&self) -> BusReceiver {
        BusReceiver::InMemory(self.broadcaster.subscribe())
    }
}

/// Convenience: wrap an [`InMemoryBus`] in an `Arc<dyn RequestEventBus>` so
/// callers can pass it straight to [`super::Lifecycle::with_event_bus`].
pub fn new_in_memory_bus() -> Arc<dyn RequestEventBus> {
    Arc::new(InMemoryBus::new())
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
    async fn in_memory_bus_smoke_publishes_to_all_subscribers() {
        let bus = InMemoryBus::new();
        let BusReceiver::InMemory(mut rx1) = bus.subscribe() else {
            panic!("InMemoryBus should yield InMemory receiver");
        };
        let BusReceiver::InMemory(mut rx2) = bus.subscribe() else {
            panic!("InMemoryBus should yield InMemory receiver");
        };

        bus.publish(sample_event("req-1")).await;
        bus.publish(sample_event("req-2")).await;

        let got1a = rx1.recv().await.expect("rx1 receives req-1");
        let got1b = rx1.recv().await.expect("rx1 receives req-2");
        let got2a = rx2.recv().await.expect("rx2 receives req-1");
        let got2b = rx2.recv().await.expect("rx2 receives req-2");

        assert_eq!(got1a.request_id, "req-1");
        assert_eq!(got1b.request_id, "req-2");
        assert_eq!(got2a.request_id, "req-1");
        assert_eq!(got2b.request_id, "req-2");
    }

    #[tokio::test]
    async fn in_memory_bus_drops_oldest_when_consumer_lags() {
        // Capacity 2 means once 3 events are published without consumption the
        // subscriber will see Lagged(1).
        let bus = InMemoryBus::with_capacity(2);
        let BusReceiver::InMemory(mut rx) = bus.subscribe() else {
            panic!("InMemoryBus should yield InMemory receiver");
        };

        bus.publish(sample_event("a")).await;
        bus.publish(sample_event("b")).await;
        bus.publish(sample_event("c")).await;

        // First recv should report Lagged because we missed 1 event.
        let err = rx.recv().await.expect_err("expected Lagged signal");
        match err {
            broadcast::error::RecvError::Lagged(skipped) => {
                assert!(
                    skipped >= 1,
                    "expected at least one skipped event, got {skipped}"
                );
            }
            other => panic!("expected Lagged, got {other:?}"),
        }
        // Next two events should still be deliverable.
        let next1 = rx.recv().await.expect("next event after lag");
        let next2 = rx.recv().await.expect("next event after lag");
        assert!(matches!(next1.request_id.as_str(), "b" | "c"));
        assert!(matches!(next2.request_id.as_str(), "b" | "c"));
    }

    #[tokio::test]
    async fn publish_with_no_subscribers_is_noop() {
        let bus = InMemoryBus::new();
        // Should not panic, hang, or error.
        bus.publish(sample_event("orphan")).await;
    }
}
