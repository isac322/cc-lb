//! Cross-component bus for `RequestEvent` lifecycle updates delivered to
//! admin SSE subscribers.
//!
//! Every payload is a [`RequestEventUpdate`] tagged with
//! [`RequestEventPhase::Partial`] (mid-flight snapshot for live UI updates) or
//! [`RequestEventPhase::Final`] (one-shot terminal snapshot). The
//! `lifecycle_event_assembler` publishes finalized request rows on this bus
//! so the admin dashboard receives the same row that was persisted.
//!
//! Subscribers obtained via [`RequestEventBus::subscribe`] are backed by
//! `tokio::sync::broadcast`. Slow consumers receive `RecvError::Lagged(n)` and
//! emit a `resync_required` UI signal instead of back-pressuring the producer.
//!
//! [`RequestEventBus::publish`] is synchronous and non-blocking; a
//! `SendError` from no live receivers is silently swallowed.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arc_swap::{ArcSwap, ArcSwapOption};
use cc_lb_lifecycle::LifecycleEvent;
#[cfg(test)]
use cc_lb_request_log::{RequestEvent, RequestEventPartial};
pub use cc_lb_request_log::{RequestEventPhase, RequestEventUpdate};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

/// Default capacity for the lifecycle-event broadcast channel.
///
/// The lifecycle stream fires up to ~10 events per request, so this absorbs
/// bursts of ~200 in-flight requests before slow ephemeral consumers observe
/// `Lagged(n)`.
pub const DEFAULT_LIFECYCLE_BROADCAST_CAPACITY: usize = 2048;

/// Transport-agnostic event sink used by lifecycle producers and admin SSE consumers.
pub trait RequestEventBus: Send + Sync + 'static {
    /// Publish an event update. Synchronous and non-blocking.
    fn publish(&self, update: RequestEventUpdate);

    /// Subscribe an ephemeral consumer. Slow consumers may observe `Lagged(n)`.
    fn subscribe(&self) -> broadcast::Receiver<RequestEventUpdate>;

    fn publish_lifecycle(&self, event: LifecycleEvent);

    fn subscribe_lifecycle(&self) -> broadcast::Receiver<LifecycleEvent>;

    /// Detached overflow tasks currently parked on a full
    /// lifecycle-assembler channel. The assembler polls this during its
    /// shutdown drain so parked row-bearing events are delivered before the
    /// receiver is dropped. Buses without an assembler overflow path return
    /// 0; bus wrappers should delegate to their inner bus.
    fn assembler_overflow_in_flight(&self) -> usize {
        0
    }
}

/// Default capacity for the broadcast channel powering admin SSE subscribers.
pub const DEFAULT_BROADCAST_CAPACITY: usize = 4096;

/// Default capacity for the durable lifecycle-writer mpsc channel wired by
/// `cc_lb_engine::spawn_lifecycle_event_logger`.
pub const DEFAULT_LIFECYCLE_WRITER_CAPACITY: usize = 4096;
pub const DEFAULT_LIFECYCLE_ASSEMBLER_CAPACITY: usize = 4096;

/// Upper bound on detached overflow tasks retrying a saturated
/// lifecycle-assembler send. Row-bearing events must not be dropped on
/// `TrySendError::Full`, so [`InMemoryBus`] parks them in a spawned task that
/// awaits channel capacity; this cap bounds that fallback's task growth.
const MAX_ASSEMBLER_OVERFLOW_TASKS: usize = 1024;
pub const DEFAULT_LIFECYCLE_PRICING_CAPACITY: usize = 4096;
pub const DEFAULT_LIFECYCLE_LIMIT_RECONCILE_CAPACITY: usize = 4096;
pub const DEFAULT_LIFECYCLE_RATE_LIMIT_HEADER_CAPACITY: usize = 4096;
pub const DEFAULT_LIFECYCLE_SUBSCRIPTION_QUOTA_CAPACITY: usize = 4096;
pub const DEFAULT_LIFECYCLE_LIMIT_REJECTION_AUDIT_CAPACITY: usize = 4096;
pub const DEFAULT_LIFECYCLE_API_KEY_METRICS_CAPACITY: usize = 4096;
pub const DEFAULT_LIFECYCLE_CACHE_HIT_MISS_CAPACITY: usize = 4096;
pub const DEFAULT_LIFECYCLE_ROUTING_TIER_CAPACITY: usize = 4096;

/// Default single-process implementation.
///
/// Holds a broadcast channel for ephemeral SSE subscribers and an optional
/// mpsc channel for the durable DB writer. `publish` synchronously fans out
/// to both.
#[derive(Clone)]
pub struct InMemoryBus {
    inner: Arc<InMemoryBusInner>,
}

#[derive(Clone, Default)]
struct LifecycleSubscribers {
    writer_tx: Option<mpsc::Sender<LifecycleEvent>>,
    assembler_tx: Option<mpsc::Sender<LifecycleEvent>>,
    pricing_tx: Option<mpsc::Sender<LifecycleEvent>>,
    limit_reconcile_tx: Option<mpsc::Sender<LifecycleEvent>>,
    rate_limit_header_tx: Option<mpsc::Sender<LifecycleEvent>>,
    subscription_quota_tx: Option<mpsc::Sender<LifecycleEvent>>,
    limit_rejection_audit_tx: Option<mpsc::Sender<LifecycleEvent>>,
    api_key_metrics_tx: Option<mpsc::Sender<LifecycleEvent>>,
    cache_hit_miss_tx: Option<mpsc::Sender<LifecycleEvent>>,
    routing_tier_tx: Option<mpsc::Sender<LifecycleEvent>>,
}

struct InMemoryBusInner {
    broadcast_tx: broadcast::Sender<RequestEventUpdate>,
    lifecycle_broadcast_tx: broadcast::Sender<LifecycleEvent>,
    subscribers: ArcSwap<LifecycleSubscribers>,
    /// Serializes `attach_lifecycle_*` copy-on-write updates; never acquired on `publish_lifecycle`.
    attach_lock: Mutex<()>,
    /// Optional single-channel ingest queue draining into a background fanout worker.
    dispatcher_tx: ArcSwapOption<mpsc::Sender<LifecycleEvent>>,
    /// Detached overflow tasks currently parked on a full assembler channel. Bounded
    /// by [`MAX_ASSEMBLER_OVERFLOW_TASKS`].
    assembler_overflow_in_flight: AtomicUsize,
    /// Detached overflow tasks currently parked on a full dispatcher channel. Bounded
    /// by [`MAX_ASSEMBLER_OVERFLOW_TASKS`].
    dispatcher_overflow_in_flight: AtomicUsize,
}

/// Handle to the background fanout dispatcher spawned by [`InMemoryBus::spawn_lifecycle_dispatcher`].
pub struct LifecycleDispatcherHandle {
    inner: Arc<InMemoryBusInner>,
    sender: mpsc::Sender<LifecycleEvent>,
    task: JoinHandle<()>,
}

impl LifecycleDispatcherHandle {
    /// Detach the single-channel dispatcher, drain all queued and overflowed
    /// events through the subscriber channels, and await completion.
    pub async fn shutdown(self) {
        self.inner.dispatcher_tx.store(None);
        drop(self.sender);
        while self
            .inner
            .dispatcher_overflow_in_flight
            .load(Ordering::Acquire)
            > 0
        {
            tokio::task::yield_now().await;
        }
        let _ = self.task.await;
    }
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
        let (lifecycle_broadcast_tx, _) = broadcast::channel(DEFAULT_LIFECYCLE_BROADCAST_CAPACITY);
        Self {
            inner: Arc::new(InMemoryBusInner {
                broadcast_tx,
                lifecycle_broadcast_tx,
                subscribers: ArcSwap::from_pointee(LifecycleSubscribers::default()),
                attach_lock: Mutex::new(()),
                dispatcher_tx: ArcSwapOption::empty(),
                assembler_overflow_in_flight: AtomicUsize::new(0),
                dispatcher_overflow_in_flight: AtomicUsize::new(0),
            }),
        }
    }

    /// Spawn a background worker that receives `LifecycleEvent`s from a single
    /// lock-free ingest MPSC channel and fans them out off the request thread.
    pub fn spawn_lifecycle_dispatcher(&self, capacity: usize) -> LifecycleDispatcherHandle {
        let (tx, mut rx) = mpsc::channel(capacity.max(1));
        self.inner.dispatcher_tx.store(Some(Arc::new(tx.clone())));
        let inner = Arc::clone(&self.inner);
        let task = tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                fanout_lifecycle_event(&inner, event);
            }
        });
        LifecycleDispatcherHandle {
            inner: Arc::clone(&self.inner),
            sender: tx,
            task,
        }
    }

    fn update_subscriber(
        &self,
        capacity: usize,
        assign: impl FnOnce(&mut LifecycleSubscribers, mpsc::Sender<LifecycleEvent>),
    ) -> mpsc::Receiver<LifecycleEvent> {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        let _guard = self
            .inner
            .attach_lock
            .lock()
            .expect("event bus subscriber attach mutex poisoned");
        let mut next = (**self.inner.subscribers.load()).clone();
        assign(&mut next, tx);
        self.inner.subscribers.store(Arc::new(next));
        rx
    }

    /// Attach the durable lifecycle-writer consumer (see
    /// `cc_lb_engine::spawn_lifecycle_event_logger`). Only one may be attached;
    /// subsequent calls replace the previous sender.
    pub fn attach_lifecycle_writer(&self, capacity: usize) -> mpsc::Receiver<LifecycleEvent> {
        self.update_subscriber(capacity, |subs, tx| subs.writer_tx = Some(tx))
    }

    pub fn attach_lifecycle_assembler(&self, capacity: usize) -> mpsc::Receiver<LifecycleEvent> {
        self.update_subscriber(capacity, |subs, tx| subs.assembler_tx = Some(tx))
    }

    pub fn attach_lifecycle_pricing(&self, capacity: usize) -> mpsc::Receiver<LifecycleEvent> {
        self.update_subscriber(capacity, |subs, tx| subs.pricing_tx = Some(tx))
    }

    pub fn attach_lifecycle_limit_reconcile(
        &self,
        capacity: usize,
    ) -> mpsc::Receiver<LifecycleEvent> {
        self.update_subscriber(capacity, |subs, tx| subs.limit_reconcile_tx = Some(tx))
    }

    pub fn attach_lifecycle_rate_limit_header(
        &self,
        capacity: usize,
    ) -> mpsc::Receiver<LifecycleEvent> {
        self.update_subscriber(capacity, |subs, tx| subs.rate_limit_header_tx = Some(tx))
    }

    pub fn attach_lifecycle_subscription_quota(
        &self,
        capacity: usize,
    ) -> mpsc::Receiver<LifecycleEvent> {
        self.update_subscriber(capacity, |subs, tx| subs.subscription_quota_tx = Some(tx))
    }

    pub fn attach_lifecycle_limit_rejection_audit(
        &self,
        capacity: usize,
    ) -> mpsc::Receiver<LifecycleEvent> {
        self.update_subscriber(capacity, |subs, tx| {
            subs.limit_rejection_audit_tx = Some(tx)
        })
    }

    pub fn attach_lifecycle_api_key_metrics(
        &self,
        capacity: usize,
    ) -> mpsc::Receiver<LifecycleEvent> {
        self.update_subscriber(capacity, |subs, tx| subs.api_key_metrics_tx = Some(tx))
    }

    pub fn attach_lifecycle_cache_hit_miss(
        &self,
        capacity: usize,
    ) -> mpsc::Receiver<LifecycleEvent> {
        self.update_subscriber(capacity, |subs, tx| subs.cache_hit_miss_tx = Some(tx))
    }

    pub fn attach_lifecycle_routing_tier(&self, capacity: usize) -> mpsc::Receiver<LifecycleEvent> {
        self.update_subscriber(capacity, |subs, tx| subs.routing_tier_tx = Some(tx))
    }

    /// Whether this event carries request-log meaning for the durable
    /// assembler: it creates the row (`RequestStarted`), finalizes it
    /// (`RequestTerminated`), or records an abnormal outcome
    /// (`RequestLogUpstreamErrorObserved`, `UpstreamStreamDiagnosticsObserved`,
    /// `StreamCompleted`, and authoritative final usage). These must not be
    /// dropped when the assembler channel is full.
    fn carries_request_row(event: &LifecycleEvent) -> bool {
        matches!(
            event,
            LifecycleEvent::RequestStarted { .. }
                | LifecycleEvent::RequestTerminated { .. }
                | LifecycleEvent::RequestLogUpstreamErrorObserved { .. }
                | LifecycleEvent::UpstreamStreamDiagnosticsObserved { .. }
                | LifecycleEvent::StreamCompleted { .. }
                | LifecycleEvent::UpstreamQuotaFrameObserved { .. }
                | LifecycleEvent::RequestFundingObserved { .. }
                | LifecycleEvent::UsageObserved {
                    source: cc_lb_lifecycle::UsageSource::Final,
                    ..
                }
        )
    }

    /// Detached overflow tasks currently parked on a full assembler or
    /// dispatcher channel. Read by the assembler's shutdown drain; see
    /// [`RequestEventBus::assembler_overflow_in_flight`].
    pub fn assembler_overflow_in_flight(&self) -> usize {
        self.inner
            .assembler_overflow_in_flight
            .load(Ordering::Relaxed)
            + self
                .inner
                .dispatcher_overflow_in_flight
                .load(Ordering::Relaxed)
    }
}

fn send_to_assembler(
    inner: &Arc<InMemoryBusInner>,
    subs: &LifecycleSubscribers,
    event: LifecycleEvent,
) {
    let Some(tx) = subs.assembler_tx.clone() else {
        return;
    };
    match tx.try_send(event) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(dropped)) => {
            if InMemoryBus::carries_request_row(&dropped) {
                spawn_channel_overflow(
                    Arc::clone(inner),
                    &inner.assembler_overflow_in_flight,
                    tx,
                    dropped,
                    "lifecycle_assembler_full",
                    "lifecycle assembler",
                );
            } else {
                record_dropped_events_by("lifecycle_assembler_full", 1);
                tracing::error!(
                    kind = dropped.kind(),
                    event_id = %dropped.event_id(),
                    "lifecycle assembler mpsc full; dropping event (request row may be missing)",
                );
            }
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            tracing::debug!("lifecycle assembler mpsc closed");
        }
    }
}

fn spawn_channel_overflow(
    inner: Arc<InMemoryBusInner>,
    counter: &AtomicUsize,
    tx: mpsc::Sender<LifecycleEvent>,
    event: LifecycleEvent,
    drop_reason: &'static str,
    channel_label: &'static str,
) {
    let in_flight = counter.fetch_add(1, Ordering::Relaxed);
    if in_flight >= MAX_ASSEMBLER_OVERFLOW_TASKS {
        counter.fetch_sub(1, Ordering::Relaxed);
        record_dropped_events_by(drop_reason, 1);
        tracing::error!(
            kind = event.kind(),
            event_id = %event.event_id(),
            in_flight,
            channel_label,
            "overflow task cap reached; dropping row-bearing event (request row may be missing)",
        );
        return;
    }
    if tokio::runtime::Handle::try_current().is_err() {
        counter.fetch_sub(1, Ordering::Relaxed);
        record_dropped_events_by(drop_reason, 1);
        tracing::error!(
            kind = event.kind(),
            event_id = %event.event_id(),
            channel_label,
            "mpsc full outside a tokio runtime; dropping row-bearing event (request row may be missing)",
        );
        return;
    }
    let is_dispatcher = std::ptr::eq(counter, &inner.dispatcher_overflow_in_flight);
    tokio::spawn(async move {
        if let Err(send_error) = tx.send(event).await {
            let event = send_error.0;
            tracing::error!(
                kind = event.kind(),
                event_id = %event.event_id(),
                channel_label,
                "mpsc closed while overflow task waited; request row may be missing",
            );
        }
        if is_dispatcher {
            inner
                .dispatcher_overflow_in_flight
                .fetch_sub(1, Ordering::Release);
        } else {
            inner
                .assembler_overflow_in_flight
                .fetch_sub(1, Ordering::Release);
        }
    });
}

fn try_send_activity(
    slot: Option<&mpsc::Sender<LifecycleEvent>>,
    drop_reason: &'static str,
    event: &LifecycleEvent,
) {
    let Some(tx) = slot else {
        return;
    };
    match tx.try_send(event.clone()) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(dropped)) => {
            record_dropped_events_by(drop_reason, 1);
            tracing::debug!(
                kind = dropped.kind(),
                event_id = %dropped.event_id(),
                drop_reason,
                "lifecycle subscriber mpsc full; dropping activity heartbeat",
            );
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {}
    }
}

fn try_send_subscriber(
    slot: Option<&mpsc::Sender<LifecycleEvent>>,
    drop_reason: &'static str,
    full_msg: &'static str,
    closed_msg: &'static str,
    event: &LifecycleEvent,
) {
    let Some(tx) = slot else {
        return;
    };
    match tx.try_send(event.clone()) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(dropped)) => {
            record_dropped_events_by(drop_reason, 1);
            tracing::warn!(
                kind = dropped.kind(),
                event_id = %dropped.event_id(),
                "{full_msg}",
            );
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            tracing::debug!("{closed_msg}");
        }
    }
}

fn fanout_lifecycle_event(inner: &Arc<InMemoryBusInner>, event: LifecycleEvent) {
    let subs = inner.subscribers.load();
    if matches!(&event, LifecycleEvent::RequestActivity { .. }) {
        // Advisory heartbeat: keep broadcast listeners and stateful
        // subscribers warm without spawning assembler overflow tasks or
        // waking the stateless limit-rejection audit subscriber.
        if inner.lifecycle_broadcast_tx.receiver_count() > 0 {
            let _ = inner.lifecycle_broadcast_tx.send(event.clone());
        }
        let targets = [
            (
                subs.writer_tx.as_ref(),
                "lifecycle_writer_activity_dropped",
            ),
            (
                subs.assembler_tx.as_ref(),
                "lifecycle_assembler_activity_dropped",
            ),
            (
                subs.pricing_tx.as_ref(),
                "lifecycle_pricing_activity_dropped",
            ),
            (
                subs.limit_reconcile_tx.as_ref(),
                "lifecycle_limit_reconcile_activity_dropped",
            ),
            (
                subs.rate_limit_header_tx.as_ref(),
                "lifecycle_rate_limit_header_activity_dropped",
            ),
            (
                subs.subscription_quota_tx.as_ref(),
                "lifecycle_subscription_quota_activity_dropped",
            ),
            (
                subs.api_key_metrics_tx.as_ref(),
                "lifecycle_api_key_metrics_activity_dropped",
            ),
            (
                subs.cache_hit_miss_tx.as_ref(),
                "lifecycle_cache_hit_miss_activity_dropped",
            ),
            (
                subs.routing_tier_tx.as_ref(),
                "lifecycle_routing_tier_activity_dropped",
            ),
        ];
        for (slot, drop_reason) in targets {
            try_send_activity(slot, drop_reason, &event);
        }
        return;
    }
    if matches!(&event, LifecycleEvent::UpstreamQuotaFrameObserved { .. }) {
        // Account-only frames feed only the quota subscriber. A request
        // identity is required before the assembler may create a row.
        let request_scoped = matches!(
            &event,
            LifecycleEvent::UpstreamQuotaFrameObserved { event_id, .. }
                if !event_id.is_empty()
        );
        if request_scoped {
            send_to_assembler(inner, &subs, event.clone());
        }
        if let Some(tx) = subs.subscription_quota_tx.as_ref()
            && tx.try_send(event).is_err()
        {
            record_dropped_events_by("lifecycle_subscription_quota_full", 1);
        }
        return;
    }
    if matches!(&event, LifecycleEvent::RequestFundingObserved { .. }) {
        send_to_assembler(inner, &subs, event);
        return;
    }
    if matches!(
        &event,
        LifecycleEvent::RequestLogUpstreamErrorObserved { .. }
            | LifecycleEvent::UpstreamStreamDiagnosticsObserved { .. }
    ) {
        let retry_after = match &event {
            LifecycleEvent::RequestLogUpstreamErrorObserved {
                event_id,
                retry_after: Some(retry_after),
                ..
            } => Some((event_id.clone(), retry_after.clone())),
            _ => None,
        };
        // Row-bearing event routed only to the durable assembler.
        send_to_assembler(inner, &subs, event);
        if let Some((event_id, retry_after)) = retry_after {
            let sanitized = LifecycleEvent::RequestLogUpstreamErrorObserved {
                event_id,
                error_type: String::new(),
                error_message: String::new(),
                upstream_error_code: None,
                retry_after: Some(retry_after),
            };
            if inner.lifecycle_broadcast_tx.receiver_count() > 0 {
                let _ = inner.lifecycle_broadcast_tx.send(sanitized.clone());
            }
            if let Some(tx) = subs.rate_limit_header_tx.as_ref() {
                let _ = tx.try_send(sanitized);
            }
        }
        return;
    }
    // Advisory broadcast copy for live dashboards; the durable request
    // row does not depend on it, so a dropped send is acceptable.
    if inner.lifecycle_broadcast_tx.receiver_count() > 0 {
        let _ = inner.lifecycle_broadcast_tx.send(event.clone());
    }

    send_to_assembler(inner, &subs, event.clone());
    let routes = [
        (
            subs.writer_tx.as_ref(),
            "lifecycle_writer_full",
            "lifecycle writer mpsc full; dropping event (subscriber metric may lag)",
            "lifecycle writer mpsc closed",
        ),
        (
            subs.pricing_tx.as_ref(),
            "lifecycle_pricing_full",
            "lifecycle pricing subscriber mpsc full; dropping event (pricing may be missing)",
            "lifecycle pricing subscriber mpsc closed",
        ),
        (
            subs.limit_reconcile_tx.as_ref(),
            "lifecycle_limit_reconcile_full",
            "lifecycle limit reconcile subscriber mpsc full; dropping event (reservation may not reconcile)",
            "lifecycle limit reconcile subscriber mpsc closed",
        ),
        (
            subs.rate_limit_header_tx.as_ref(),
            "lifecycle_rate_limit_header_full",
            "lifecycle rate limit header subscriber mpsc full; dropping event (upstream rate limit observation may be missing)",
            "lifecycle rate limit header subscriber mpsc closed",
        ),
        (
            subs.subscription_quota_tx.as_ref(),
            "lifecycle_subscription_quota_full",
            "lifecycle subscription quota subscriber mpsc full; dropping event (subscription quota observation may be missing)",
            "lifecycle subscription quota subscriber mpsc closed",
        ),
        (
            subs.limit_rejection_audit_tx.as_ref(),
            "lifecycle_limit_rejection_audit_full",
            "lifecycle limit rejection audit subscriber mpsc full; dropping event (audit row may be missing)",
            "lifecycle limit rejection audit subscriber mpsc closed",
        ),
        (
            subs.api_key_metrics_tx.as_ref(),
            "lifecycle_api_key_metrics_full",
            "lifecycle api-key metrics subscriber mpsc full; dropping event (metric samples may be missing)",
            "lifecycle api-key metrics subscriber mpsc closed",
        ),
        (
            subs.cache_hit_miss_tx.as_ref(),
            "lifecycle_cache_hit_miss_full",
            "lifecycle cache hit/miss subscriber mpsc full; dropping event (hit/miss metric may be missing)",
            "lifecycle cache hit/miss subscriber mpsc closed",
        ),
        (
            subs.routing_tier_tx.as_ref(),
            "lifecycle_routing_tier_full",
            "lifecycle routing tier subscriber mpsc full; dropping event (tier metric may be missing)",
            "lifecycle routing tier subscriber mpsc closed",
        ),
    ];
    for (slot, drop_reason, full_msg, closed_msg) in routes {
        try_send_subscriber(slot, drop_reason, full_msg, closed_msg, &event);
    }
}

impl Default for InMemoryBus {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestEventBus for InMemoryBus {
    fn publish(&self, update: RequestEventUpdate) {
        // Advisory: this broadcast feeds live admin SSE dashboards only; the
        // durable request row does not depend on it, so a full channel or an
        // absent subscriber may drop the update.
        let _ = self.inner.broadcast_tx.send(update);
    }

    fn subscribe(&self) -> broadcast::Receiver<RequestEventUpdate> {
        self.inner.broadcast_tx.subscribe()
    }

    fn publish_lifecycle(&self, event: LifecycleEvent) {
        if let Some(dispatcher_tx) = self.inner.dispatcher_tx.load_full() {
            match dispatcher_tx.try_send(event) {
                Ok(()) => return,
                Err(mpsc::error::TrySendError::Full(dropped)) => {
                    if Self::carries_request_row(&dropped) {
                        spawn_channel_overflow(
                            Arc::clone(&self.inner),
                            &self.inner.dispatcher_overflow_in_flight,
                            (*dispatcher_tx).clone(),
                            dropped,
                            "lifecycle_dispatcher_full",
                            "lifecycle dispatcher",
                        );
                    } else {
                        record_dropped_events_by("lifecycle_dispatcher_full", 1);
                        tracing::debug!(
                            kind = dropped.kind(),
                            event_id = %dropped.event_id(),
                            "lifecycle dispatcher mpsc full; dropping non-row event",
                        );
                    }
                    return;
                }
                Err(mpsc::error::TrySendError::Closed(returned)) => {
                    fanout_lifecycle_event(&self.inner, returned);
                    return;
                }
            }
        }
        fanout_lifecycle_event(&self.inner, event);
    }

    fn subscribe_lifecycle(&self) -> broadcast::Receiver<LifecycleEvent> {
        self.inner.lifecycle_broadcast_tx.subscribe()
    }

    fn assembler_overflow_in_flight(&self) -> usize {
        InMemoryBus::assembler_overflow_in_flight(self)
    }
}

/// Convenience: wrap an [`InMemoryBus`] in an `Arc<dyn RequestEventBus>`.
pub fn new_in_memory_bus() -> Arc<dyn RequestEventBus> {
    Arc::new(InMemoryBus::new())
}

fn record_dropped_events_by(reason: &'static str, count: u64) {
    metrics::counter!("cc_lb_dropped_events_total", "reason" => reason.to_owned()).increment(count);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_event(request_id: &str) -> RequestEvent {
        RequestEvent {
            ts: 1_700_000_000,
            request_id: request_id.to_string(),
            event_id: Some(format!("event-{request_id}")),
            status: 200,
            duration_ms: 42,
            ..Default::default()
        }
    }

    fn sample_partial(request_id: &str) -> RequestEventPartial {
        RequestEventPartial {
            event_id: format!("event-{request_id}"),
            request_id: request_id.to_owned(),
            ts: 1_700_000_000,
            ts_ms: 1_700_000_000_000,
            last_update_ms: 1_700_000_000_000,
            elapsed_ms: 0,
            stream: false,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn publish_delivers_to_sse_subscribers() {
        let bus = InMemoryBus::new();
        let mut rx_sse = bus.subscribe();

        bus.publish(RequestEventUpdate::partial(sample_partial("req-1")));
        bus.publish(RequestEventUpdate::final_(sample_event("req-2"), 2));

        let sse_a = rx_sse.recv().await.expect("sse partial");
        let sse_b = rx_sse.recv().await.expect("sse final");
        assert_eq!(sse_a.phase(), RequestEventPhase::Partial);
        assert_eq!(sse_a.event_id(), "event-req-1");
        assert_eq!(sse_b.phase(), RequestEventPhase::Final);
        assert_eq!(sse_b.event_id(), "event-req-2");
    }

    #[tokio::test]
    async fn publish_with_no_subscribers_is_noop() {
        let bus = InMemoryBus::new();
        bus.publish(RequestEventUpdate::final_(sample_event("orphan"), 1));
    }

    #[tokio::test]
    async fn sse_subscriber_receives_lagged_when_falling_behind() {
        let bus = InMemoryBus::with_capacity(2);
        let mut rx = bus.subscribe();
        bus.publish(RequestEventUpdate::final_(sample_event("a"), 1));
        bus.publish(RequestEventUpdate::final_(sample_event("b"), 2));
        bus.publish(RequestEventUpdate::final_(sample_event("c"), 3));

        let err = rx.recv().await.expect_err("expected Lagged");
        match err {
            broadcast::error::RecvError::Lagged(skipped) => {
                assert!(skipped >= 1, "expected at least one skipped, got {skipped}");
            }
            other => panic!("expected Lagged, got {other:?}"),
        }
    }

    fn sample_lifecycle_event(request_id: &str) -> LifecycleEvent {
        LifecycleEvent::RequestStarted {
            event_id: format!("evt-{request_id}"),
            request_id: request_id.to_owned(),
            ts_ms: 1_700_000_000_000,
            stream: false,
            source_kind: None,
            source_ref_id: None,
            event_kind: None,
            protocol: None,
        }
    }

    #[tokio::test]
    async fn publish_lifecycle_fans_out_to_broadcast_and_writer() {
        let bus = InMemoryBus::new();
        let mut rx_sse = bus.subscribe_lifecycle();
        let mut rx_writer = bus.attach_lifecycle_writer(8);

        bus.publish_lifecycle(sample_lifecycle_event("req-1"));
        bus.publish_lifecycle(sample_lifecycle_event("req-2"));

        let sse_a = rx_sse.recv().await.expect("sse a");
        let sse_b = rx_sse.recv().await.expect("sse b");
        assert_eq!(sse_a.event_id(), "evt-req-1");
        assert_eq!(sse_b.event_id(), "evt-req-2");

        let wrt_a = rx_writer.recv().await.expect("writer a");
        let wrt_b = rx_writer.recv().await.expect("writer b");
        assert_eq!(wrt_a.event_id(), "evt-req-1");
        assert_eq!(wrt_b.event_id(), "evt-req-2");
    }

    #[tokio::test]
    async fn request_log_upstream_error_routes_only_to_assembler() {
        let bus = InMemoryBus::new();
        let mut broadcast_rx = bus.subscribe_lifecycle();
        let mut assembler_rx = bus.attach_lifecycle_assembler(1);
        let mut writer_rx = bus.attach_lifecycle_writer(1);
        let event = LifecycleEvent::RequestLogUpstreamErrorObserved {
            event_id: "evt-private".into(),
            error_type: "rate_limit_error".into(),
            error_message: "bounded".into(),
            upstream_error_code: Some("rate_limit_code".into()),
            retry_after: None,
        };

        bus.publish_lifecycle(event.clone());

        assert_eq!(assembler_rx.try_recv().expect("assembler event"), event);
        assert!(broadcast_rx.try_recv().is_err());
        assert!(writer_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn request_log_retry_after_broadcast_is_sanitized() {
        let bus = InMemoryBus::new();
        let mut broadcast_rx = bus.subscribe_lifecycle();
        let mut assembler_rx = bus.attach_lifecycle_assembler(1);
        let mut rate_limit_rx = bus.attach_lifecycle_rate_limit_header(1);
        let event = LifecycleEvent::RequestLogUpstreamErrorObserved {
            event_id: "evt-retry".into(),
            error_type: "rate_limit_error".into(),
            error_message: "private".into(),
            upstream_error_code: Some("rate_limit_code".into()),
            retry_after: Some("30".into()),
        };

        bus.publish_lifecycle(event.clone());

        let expected_sanitized = LifecycleEvent::RequestLogUpstreamErrorObserved {
            event_id: "evt-retry".into(),
            error_type: String::new(),
            error_message: String::new(),
            upstream_error_code: None,
            retry_after: Some("30".into()),
        };
        assert_eq!(assembler_rx.try_recv().expect("assembler event"), event);
        assert_eq!(
            broadcast_rx.try_recv().expect("broadcast signal"),
            expected_sanitized
        );
        assert_eq!(
            rate_limit_rx.try_recv().expect("rate limit header signal"),
            expected_sanitized
        );
    }

    #[tokio::test]
    async fn upstream_stream_diagnostics_routes_only_to_assembler() {
        let bus = InMemoryBus::new();
        let mut broadcast_rx = bus.subscribe_lifecycle();
        let mut assembler_rx = bus.attach_lifecycle_assembler(1);
        let mut writer_rx = bus.attach_lifecycle_writer(1);
        let event = LifecycleEvent::UpstreamStreamDiagnosticsObserved {
            event_id: "evt-private".into(),
            diagnostics: cc_lb_lifecycle::UpstreamStreamDiagnostics {
                request_id: Some("req_upstream".into()),
                body_bytes: 10,
                ..Default::default()
            },
        };

        bus.publish_lifecycle(event.clone());

        assert_eq!(assembler_rx.try_recv().expect("assembler event"), event);
        assert!(broadcast_rx.try_recv().is_err());
        assert!(writer_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn publish_lifecycle_with_no_subscribers_is_noop() {
        let bus = InMemoryBus::new();
        bus.publish_lifecycle(sample_lifecycle_event("orphan"));
    }

    #[tokio::test]
    async fn publish_lifecycle_writer_full_drops_newest() {
        let bus = InMemoryBus::new();
        let _rx = bus.attach_lifecycle_writer(1);
        bus.publish_lifecycle(sample_lifecycle_event("a"));
        bus.publish_lifecycle(sample_lifecycle_event("b"));
    }

    #[tokio::test]
    async fn request_activity_fans_out_without_overflow_or_rejection_audit() {
        let bus = InMemoryBus::new();
        let mut broadcast_rx = bus.subscribe_lifecycle();
        let mut writer_rx = bus.attach_lifecycle_writer(1);
        let mut assembler_rx = bus.attach_lifecycle_assembler(1);
        let mut pricing_rx = bus.attach_lifecycle_pricing(1);
        let mut reconcile_rx = bus.attach_lifecycle_limit_reconcile(1);
        let mut rate_limit_rx = bus.attach_lifecycle_rate_limit_header(1);
        let mut quota_rx = bus.attach_lifecycle_subscription_quota(1);
        let mut api_metrics_rx = bus.attach_lifecycle_api_key_metrics(1);
        let mut cache_hit_rx = bus.attach_lifecycle_cache_hit_miss(1);
        let mut routing_tier_rx = bus.attach_lifecycle_routing_tier(1);
        let mut audit_rx = bus.attach_lifecycle_limit_rejection_audit(1);

        let first = LifecycleEvent::RequestActivity {
            event_id: "evt-1".into(),
        };
        bus.publish_lifecycle(first.clone());

        assert_eq!(broadcast_rx.try_recv().expect("broadcast"), first);
        assert_eq!(writer_rx.try_recv().expect("writer"), first);
        assert_eq!(assembler_rx.try_recv().expect("assembler"), first);
        assert_eq!(pricing_rx.try_recv().expect("pricing"), first);
        assert_eq!(reconcile_rx.try_recv().expect("reconcile"), first);
        assert_eq!(rate_limit_rx.try_recv().expect("rate_limit"), first);
        assert_eq!(quota_rx.try_recv().expect("quota"), first);
        assert_eq!(api_metrics_rx.try_recv().expect("api_metrics"), first);
        assert_eq!(cache_hit_rx.try_recv().expect("cache_hit"), first);
        assert_eq!(routing_tier_rx.try_recv().expect("routing_tier"), first);
        assert!(
            audit_rx.try_recv().is_err(),
            "audit must skip RequestActivity"
        );

        // Fill assembler channel with a non-activity row, then send RequestActivity:
        // must drop cleanly without spawning an overflow task.
        bus.publish_lifecycle(sample_lifecycle_event("fill"));
        assert_eq!(bus.assembler_overflow_in_flight(), 0);
        bus.publish_lifecycle(LifecycleEvent::RequestActivity {
            event_id: "evt-drop".into(),
        });
        assert_eq!(
            bus.assembler_overflow_in_flight(),
            0,
            "full assembler channel must drop RequestActivity without spawning overflow tasks"
        );
    }

    #[tokio::test]
    async fn publish_lifecycle_arc_swap_delivers_across_all_ten_slots() {
        let bus = InMemoryBus::new();
        let mut writer_rx = bus.attach_lifecycle_writer(4);
        let mut assembler_rx = bus.attach_lifecycle_assembler(4);
        let mut pricing_rx = bus.attach_lifecycle_pricing(4);
        let mut reconcile_rx = bus.attach_lifecycle_limit_reconcile(4);
        let mut rate_limit_rx = bus.attach_lifecycle_rate_limit_header(4);
        let mut quota_rx = bus.attach_lifecycle_subscription_quota(4);
        let mut rejection_rx = bus.attach_lifecycle_limit_rejection_audit(4);
        let mut api_metrics_rx = bus.attach_lifecycle_api_key_metrics(4);
        let mut cache_hit_rx = bus.attach_lifecycle_cache_hit_miss(4);
        let mut routing_tier_rx = bus.attach_lifecycle_routing_tier(4);

        let event = sample_lifecycle_event("all-10");
        bus.publish_lifecycle(event.clone());

        for rx in [
            &mut writer_rx,
            &mut assembler_rx,
            &mut pricing_rx,
            &mut reconcile_rx,
            &mut rate_limit_rx,
            &mut quota_rx,
            &mut rejection_rx,
            &mut api_metrics_rx,
            &mut cache_hit_rx,
            &mut routing_tier_rx,
        ] {
            assert_eq!(rx.try_recv().expect("all 10 slots receive event"), event);
        }
    }

    #[tokio::test]
    async fn lifecycle_dispatcher_fans_out_single_channel_and_drains_on_shutdown() {
        let bus = InMemoryBus::new();
        let mut assembler_rx = bus.attach_lifecycle_assembler(8);
        let mut writer_rx = bus.attach_lifecycle_writer(8);
        let dispatcher = bus.spawn_lifecycle_dispatcher(2);

        for idx in 0..6 {
            bus.publish_lifecycle(sample_lifecycle_event(&format!("disp-{idx}")));
        }

        dispatcher.shutdown().await;

        for idx in 0..6 {
            let expected = format!("evt-disp-{idx}");
            assert_eq!(
                assembler_rx.try_recv().expect("assembler drained").event_id(),
                &expected
            );
            assert_eq!(
                writer_rx.try_recv().expect("writer drained").event_id(),
                &expected
            );
        }
    }
}
