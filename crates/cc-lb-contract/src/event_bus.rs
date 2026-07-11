//! Transport wire types for request-event bus consumers and producers.

use tokio::sync::{broadcast, mpsc};

use cc_lb_request_log::RequestEventUpdate;

use crate::LifecycleEvent;

/// Default capacity for the lifecycle-event broadcast channel.
///
/// The lifecycle stream fires up to ~10 events per request, so this absorbs
/// bursts of ~200 in-flight requests before slow ephemeral consumers observe
/// `Lagged(n)`.
pub const DEFAULT_LIFECYCLE_BROADCAST_CAPACITY: usize = 2048;

/// Receiver side of [`RequestEventBus::subscribe`] for ephemeral consumers.
#[derive(Debug)]
pub enum BusReceiver {
    InMemory(broadcast::Receiver<RequestEventUpdate>),
    Remote(mpsc::Receiver<RequestEventUpdate>),
}

/// Receiver side of [`RequestEventBus::subscribe_lifecycle`].
///
/// `None` is returned by trait implementations that do not publish lifecycle
/// events, such as test doubles that only exercise the `RequestEvent` path.
#[derive(Debug)]
pub enum LifecycleBusReceiver {
    None,
    InMemory(broadcast::Receiver<LifecycleEvent>),
}

/// Transport-agnostic event sink used by lifecycle producers and admin SSE consumers.
pub trait RequestEventBus: Send + Sync + 'static {
    /// Publish an event update. Synchronous and non-blocking.
    fn publish(&self, update: RequestEventUpdate);

    /// Subscribe an ephemeral consumer. Slow consumers may observe `Lagged(n)`.
    fn subscribe(&self) -> BusReceiver;

    fn publish_lifecycle(&self, event: LifecycleEvent);

    fn subscribe_lifecycle(&self) -> LifecycleBusReceiver;
}
