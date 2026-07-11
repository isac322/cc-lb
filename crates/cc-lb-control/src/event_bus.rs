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

use std::sync::{Arc, Mutex};

use cc_lb_contract::LifecycleEvent;
pub use cc_lb_contract::{
    BusReceiver, DEFAULT_LIFECYCLE_BROADCAST_CAPACITY, LifecycleBusReceiver, RequestEventBus,
};
#[cfg(test)]
use cc_lb_request_log::{RequestEvent, RequestEventPartial};
pub use cc_lb_request_log::{RequestEventPhase, RequestEventUpdate};
use tokio::sync::{broadcast, mpsc};

/// Default capacity for the broadcast channel powering admin SSE subscribers.
pub const DEFAULT_BROADCAST_CAPACITY: usize = 4096;

/// Default capacity for the durable lifecycle-writer mpsc channel used by
/// [`LifecycleEventLogger`](crate::lifecycle_event_logger::LifecycleEventLogger).
pub const DEFAULT_LIFECYCLE_WRITER_CAPACITY: usize = 4096;
pub const DEFAULT_LIFECYCLE_ASSEMBLER_CAPACITY: usize = 4096;
pub const DEFAULT_LIFECYCLE_HOOK_ADAPTER_CAPACITY: usize = 4096;
pub const DEFAULT_LIFECYCLE_PRICING_CAPACITY: usize = 4096;
pub const DEFAULT_LIFECYCLE_LIMIT_RECONCILE_CAPACITY: usize = 4096;
pub const DEFAULT_LIFECYCLE_CACHE_OBS_CAPACITY: usize = 4096;
pub const DEFAULT_LIFECYCLE_RATE_LIMIT_HEADER_CAPACITY: usize = 4096;
pub const DEFAULT_LIFECYCLE_SUBSCRIPTION_QUOTA_CAPACITY: usize = 4096;
pub const DEFAULT_LIFECYCLE_LIMIT_REJECTION_AUDIT_CAPACITY: usize = 4096;
pub const DEFAULT_LIFECYCLE_API_KEY_METRICS_CAPACITY: usize = 4096;
pub const DEFAULT_LIFECYCLE_CACHE_HIT_MISS_CAPACITY: usize = 4096;
pub const DEFAULT_LIFECYCLE_ROUTING_TIER_CAPACITY: usize = 4096;
pub const DEFAULT_LIFECYCLE_PROMPT_CACHE_DRIFT_CAPACITY: usize = 4096;
pub const DEFAULT_LIFECYCLE_PROMPT_CACHE_OBSERVATION_CAPACITY: usize = 4096;

/// Errors surfaced by [`RequestEventBus`] implementations.
#[derive(Debug, thiserror::Error)]
pub enum BusError {
    #[error("event bus closed")]
    Closed,
    #[error("event bus backend failure: {0}")]
    Backend(String),
}

#[async_trait::async_trait]
pub trait EventFanout: Send + Sync {
    async fn publish_partial(&self, update: RequestEventUpdate) -> Result<(), BusError>;

    fn subscribe(&self) -> BusReceiver;
}

#[derive(Clone)]
pub struct InMemoryFanout {
    bus: InMemoryBus,
}

impl InMemoryFanout {
    pub fn new(capacity: usize) -> Self {
        Self {
            bus: InMemoryBus::with_capacity(capacity),
        }
    }

    pub fn bus(&self) -> InMemoryBus {
        self.bus.clone()
    }
}

#[async_trait::async_trait]
impl EventFanout for InMemoryFanout {
    async fn publish_partial(&self, update: RequestEventUpdate) -> Result<(), BusError> {
        self.bus.publish(update);
        Ok(())
    }

    fn subscribe(&self) -> BusReceiver {
        RequestEventBus::subscribe(&self.bus)
    }
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
    lifecycle_broadcast_tx: broadcast::Sender<LifecycleEvent>,
    lifecycle_writer_tx: Mutex<Option<mpsc::Sender<LifecycleEvent>>>,
    lifecycle_assembler_tx: Mutex<Option<mpsc::Sender<LifecycleEvent>>>,
    lifecycle_hook_adapter_tx: Mutex<Option<mpsc::Sender<LifecycleEvent>>>,
    lifecycle_pricing_tx: Mutex<Option<mpsc::Sender<LifecycleEvent>>>,
    lifecycle_limit_reconcile_tx: Mutex<Option<mpsc::Sender<LifecycleEvent>>>,
    lifecycle_cache_obs_tx: Mutex<Option<mpsc::Sender<LifecycleEvent>>>,
    lifecycle_rate_limit_header_tx: Mutex<Option<mpsc::Sender<LifecycleEvent>>>,
    lifecycle_subscription_quota_tx: Mutex<Option<mpsc::Sender<LifecycleEvent>>>,
    lifecycle_limit_rejection_audit_tx: Mutex<Option<mpsc::Sender<LifecycleEvent>>>,
    lifecycle_api_key_metrics_tx: Mutex<Option<mpsc::Sender<LifecycleEvent>>>,
    lifecycle_cache_hit_miss_tx: Mutex<Option<mpsc::Sender<LifecycleEvent>>>,
    lifecycle_routing_tier_tx: Mutex<Option<mpsc::Sender<LifecycleEvent>>>,
    lifecycle_prompt_cache_drift_tx: Mutex<Option<mpsc::Sender<LifecycleEvent>>>,
    lifecycle_prompt_cache_observation_tx: Mutex<Option<mpsc::Sender<LifecycleEvent>>>,
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
                lifecycle_writer_tx: Mutex::new(None),
                lifecycle_assembler_tx: Mutex::new(None),
                lifecycle_hook_adapter_tx: Mutex::new(None),
                lifecycle_pricing_tx: Mutex::new(None),
                lifecycle_limit_reconcile_tx: Mutex::new(None),
                lifecycle_cache_obs_tx: Mutex::new(None),
                lifecycle_rate_limit_header_tx: Mutex::new(None),
                lifecycle_subscription_quota_tx: Mutex::new(None),
                lifecycle_limit_rejection_audit_tx: Mutex::new(None),
                lifecycle_api_key_metrics_tx: Mutex::new(None),
                lifecycle_cache_hit_miss_tx: Mutex::new(None),
                lifecycle_routing_tier_tx: Mutex::new(None),
                lifecycle_prompt_cache_drift_tx: Mutex::new(None),
                lifecycle_prompt_cache_observation_tx: Mutex::new(None),
            }),
        }
    }

    /// Attach the durable lifecycle-writer consumer
    /// (`LifecycleEventLogger`). Only one may be attached; subsequent calls
    /// replace the previous sender.
    pub fn attach_lifecycle_writer(&self, capacity: usize) -> mpsc::Receiver<LifecycleEvent> {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        let mut guard = self
            .inner
            .lifecycle_writer_tx
            .lock()
            .expect("event bus lifecycle writer mutex poisoned");
        *guard = Some(tx);
        rx
    }

    pub fn attach_lifecycle_assembler(&self, capacity: usize) -> mpsc::Receiver<LifecycleEvent> {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        let mut guard = self
            .inner
            .lifecycle_assembler_tx
            .lock()
            .expect("event bus lifecycle assembler mutex poisoned");
        *guard = Some(tx);
        rx
    }

    pub fn attach_lifecycle_hook_adapter(&self, capacity: usize) -> mpsc::Receiver<LifecycleEvent> {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        let mut guard = self
            .inner
            .lifecycle_hook_adapter_tx
            .lock()
            .expect("event bus lifecycle hook adapter mutex poisoned");
        *guard = Some(tx);
        rx
    }

    pub fn attach_lifecycle_pricing(&self, capacity: usize) -> mpsc::Receiver<LifecycleEvent> {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        let mut guard = self
            .inner
            .lifecycle_pricing_tx
            .lock()
            .expect("event bus lifecycle pricing mutex poisoned");
        *guard = Some(tx);
        rx
    }

    pub fn attach_lifecycle_limit_reconcile(
        &self,
        capacity: usize,
    ) -> mpsc::Receiver<LifecycleEvent> {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        let mut guard = self
            .inner
            .lifecycle_limit_reconcile_tx
            .lock()
            .expect("event bus lifecycle limit reconcile mutex poisoned");
        *guard = Some(tx);
        rx
    }

    pub fn attach_lifecycle_cache_observation(
        &self,
        capacity: usize,
    ) -> mpsc::Receiver<LifecycleEvent> {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        let mut guard = self
            .inner
            .lifecycle_cache_obs_tx
            .lock()
            .expect("event bus lifecycle cache observation mutex poisoned");
        *guard = Some(tx);
        rx
    }

    pub fn attach_lifecycle_rate_limit_header(
        &self,
        capacity: usize,
    ) -> mpsc::Receiver<LifecycleEvent> {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        let mut guard = self
            .inner
            .lifecycle_rate_limit_header_tx
            .lock()
            .expect("event bus lifecycle rate limit header mutex poisoned");
        *guard = Some(tx);
        rx
    }

    pub fn attach_lifecycle_subscription_quota(
        &self,
        capacity: usize,
    ) -> mpsc::Receiver<LifecycleEvent> {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        let mut guard = self
            .inner
            .lifecycle_subscription_quota_tx
            .lock()
            .expect("event bus lifecycle subscription quota mutex poisoned");
        *guard = Some(tx);
        rx
    }

    pub fn attach_lifecycle_limit_rejection_audit(
        &self,
        capacity: usize,
    ) -> mpsc::Receiver<LifecycleEvent> {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        let mut guard = self
            .inner
            .lifecycle_limit_rejection_audit_tx
            .lock()
            .expect("event bus lifecycle limit rejection audit mutex poisoned");
        *guard = Some(tx);
        rx
    }

    pub fn attach_lifecycle_api_key_metrics(
        &self,
        capacity: usize,
    ) -> mpsc::Receiver<LifecycleEvent> {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        let mut guard = self
            .inner
            .lifecycle_api_key_metrics_tx
            .lock()
            .expect("event bus lifecycle api-key metrics mutex poisoned");
        *guard = Some(tx);
        rx
    }

    pub fn attach_lifecycle_cache_hit_miss(
        &self,
        capacity: usize,
    ) -> mpsc::Receiver<LifecycleEvent> {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        let mut guard = self
            .inner
            .lifecycle_cache_hit_miss_tx
            .lock()
            .expect("event bus lifecycle cache hit/miss mutex poisoned");
        *guard = Some(tx);
        rx
    }

    pub fn attach_lifecycle_routing_tier(&self, capacity: usize) -> mpsc::Receiver<LifecycleEvent> {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        let mut guard = self
            .inner
            .lifecycle_routing_tier_tx
            .lock()
            .expect("event bus lifecycle routing tier mutex poisoned");
        *guard = Some(tx);
        rx
    }

    pub fn attach_lifecycle_prompt_cache_drift(
        &self,
        capacity: usize,
    ) -> mpsc::Receiver<LifecycleEvent> {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        let mut guard = self
            .inner
            .lifecycle_prompt_cache_drift_tx
            .lock()
            .expect("event bus lifecycle prompt cache drift mutex poisoned");
        *guard = Some(tx);
        rx
    }

    pub fn attach_lifecycle_prompt_cache_observation(
        &self,
        capacity: usize,
    ) -> mpsc::Receiver<LifecycleEvent> {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        let mut guard = self
            .inner
            .lifecycle_prompt_cache_observation_tx
            .lock()
            .expect("event bus lifecycle prompt cache observation mutex poisoned");
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
        let _ = self.inner.broadcast_tx.send(update);
    }

    fn subscribe(&self) -> BusReceiver {
        BusReceiver::InMemory(self.inner.broadcast_tx.subscribe())
    }

    fn publish_lifecycle(&self, event: LifecycleEvent) {
        if matches!(
            &event,
            LifecycleEvent::RequestLogUpstreamErrorObserved { .. }
        ) {
            let assembler_tx = {
                let guard = match self.inner.lifecycle_assembler_tx.lock() {
                    Ok(guard) => guard,
                    Err(poisoned) => poisoned.into_inner(),
                };
                guard.clone()
            };
            if let Some(tx) = assembler_tx {
                match tx.try_send(event) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(dropped)) => {
                        record_dropped_events_by("lifecycle_assembler_full", 1);
                        tracing::warn!(
                            kind = dropped.kind(),
                            event_id = %dropped.event_id(),
                            "lifecycle assembler mpsc full; dropping event (request row may be missing)",
                        );
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        tracing::debug!("lifecycle assembler mpsc closed");
                    }
                }
            }
            return;
        }
        let _ = self.inner.lifecycle_broadcast_tx.send(event.clone());

        let writer_tx = {
            let guard = self
                .inner
                .lifecycle_writer_tx
                .lock()
                .expect("event bus lifecycle writer mutex poisoned");
            guard.clone()
        };
        let assembler_tx = {
            let guard = self
                .inner
                .lifecycle_assembler_tx
                .lock()
                .expect("event bus lifecycle assembler mutex poisoned");
            guard.clone()
        };
        let hook_adapter_tx = {
            let guard = self
                .inner
                .lifecycle_hook_adapter_tx
                .lock()
                .expect("event bus lifecycle hook adapter mutex poisoned");
            guard.clone()
        };
        let pricing_tx = {
            let guard = self
                .inner
                .lifecycle_pricing_tx
                .lock()
                .expect("event bus lifecycle pricing mutex poisoned");
            guard.clone()
        };
        let limit_reconcile_tx = {
            let guard = self
                .inner
                .lifecycle_limit_reconcile_tx
                .lock()
                .expect("event bus lifecycle limit reconcile mutex poisoned");
            guard.clone()
        };
        let cache_obs_tx = {
            let guard = self
                .inner
                .lifecycle_cache_obs_tx
                .lock()
                .expect("event bus lifecycle cache obs mutex poisoned");
            guard.clone()
        };
        let rate_limit_header_tx = {
            let guard = self
                .inner
                .lifecycle_rate_limit_header_tx
                .lock()
                .expect("event bus lifecycle rate limit header mutex poisoned");
            guard.clone()
        };
        let subscription_quota_tx = {
            let guard = self
                .inner
                .lifecycle_subscription_quota_tx
                .lock()
                .expect("event bus lifecycle subscription quota mutex poisoned");
            guard.clone()
        };
        let limit_rejection_audit_tx = {
            let guard = self
                .inner
                .lifecycle_limit_rejection_audit_tx
                .lock()
                .expect("event bus lifecycle limit rejection audit mutex poisoned");
            guard.clone()
        };
        let api_key_metrics_tx = {
            let guard = self
                .inner
                .lifecycle_api_key_metrics_tx
                .lock()
                .expect("event bus lifecycle api-key metrics mutex poisoned");
            guard.clone()
        };
        let cache_hit_miss_tx = {
            let guard = self
                .inner
                .lifecycle_cache_hit_miss_tx
                .lock()
                .expect("event bus lifecycle cache hit/miss mutex poisoned");
            guard.clone()
        };
        let routing_tier_tx = {
            let guard = self
                .inner
                .lifecycle_routing_tier_tx
                .lock()
                .expect("event bus lifecycle routing tier mutex poisoned");
            guard.clone()
        };
        let prompt_cache_drift_tx = {
            let guard = self
                .inner
                .lifecycle_prompt_cache_drift_tx
                .lock()
                .expect("event bus lifecycle prompt cache drift mutex poisoned");
            guard.clone()
        };
        let prompt_cache_observation_tx = {
            let guard = self
                .inner
                .lifecycle_prompt_cache_observation_tx
                .lock()
                .expect("event bus lifecycle prompt cache observation mutex poisoned");
            guard.clone()
        };
        if let Some(tx) = writer_tx {
            match tx.try_send(event.clone()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(dropped)) => {
                    record_dropped_events_by("lifecycle_writer_full", 1);
                    tracing::warn!(
                        kind = dropped.kind(),
                        event_id = %dropped.event_id(),
                        "lifecycle writer mpsc full; dropping event (subscriber metric may lag)",
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    tracing::debug!("lifecycle writer mpsc closed");
                }
            }
        }
        if let Some(tx) = assembler_tx {
            match tx.try_send(event.clone()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(dropped)) => {
                    record_dropped_events_by("lifecycle_assembler_full", 1);
                    tracing::warn!(
                        kind = dropped.kind(),
                        event_id = %dropped.event_id(),
                        "lifecycle assembler mpsc full; dropping event (request row may be missing)",
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    tracing::debug!("lifecycle assembler mpsc closed");
                }
            }
        }
        if let Some(tx) = hook_adapter_tx {
            match tx.try_send(event.clone()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(dropped)) => {
                    record_dropped_events_by("lifecycle_hook_adapter_full", 1);
                    tracing::warn!(
                        kind = dropped.kind(),
                        event_id = %dropped.event_id(),
                        "lifecycle hook adapter mpsc full; dropping event (hook fire may be missing)",
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    tracing::debug!("lifecycle hook adapter mpsc closed");
                }
            }
        }
        if let Some(tx) = pricing_tx {
            match tx.try_send(event.clone()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(dropped)) => {
                    record_dropped_events_by("lifecycle_pricing_full", 1);
                    tracing::warn!(
                        kind = dropped.kind(),
                        event_id = %dropped.event_id(),
                        "lifecycle pricing subscriber mpsc full; dropping event (pricing may be missing)",
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    tracing::debug!("lifecycle pricing subscriber mpsc closed");
                }
            }
        }
        if let Some(tx) = limit_reconcile_tx {
            match tx.try_send(event.clone()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(dropped)) => {
                    record_dropped_events_by("lifecycle_limit_reconcile_full", 1);
                    tracing::warn!(
                        kind = dropped.kind(),
                        event_id = %dropped.event_id(),
                        "lifecycle limit reconcile subscriber mpsc full; dropping event (reservation may not reconcile)",
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    tracing::debug!("lifecycle limit reconcile subscriber mpsc closed");
                }
            }
        }
        if let Some(tx) = cache_obs_tx {
            match tx.try_send(event.clone()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(dropped)) => {
                    record_dropped_events_by("lifecycle_cache_obs_full", 1);
                    tracing::warn!(
                        kind = dropped.kind(),
                        event_id = %dropped.event_id(),
                        "lifecycle cache observation subscriber mpsc full; dropping event (prompt cache observations may be missing)",
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    tracing::debug!("lifecycle cache observation subscriber mpsc closed");
                }
            }
        }
        if let Some(tx) = rate_limit_header_tx {
            match tx.try_send(event.clone()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(dropped)) => {
                    record_dropped_events_by("lifecycle_rate_limit_header_full", 1);
                    tracing::warn!(
                        kind = dropped.kind(),
                        event_id = %dropped.event_id(),
                        "lifecycle rate limit header subscriber mpsc full; dropping event (upstream rate limit observation may be missing)",
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    tracing::debug!("lifecycle rate limit header subscriber mpsc closed");
                }
            }
        }
        if let Some(tx) = subscription_quota_tx {
            match tx.try_send(event.clone()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(dropped)) => {
                    record_dropped_events_by("lifecycle_subscription_quota_full", 1);
                    tracing::warn!(
                        kind = dropped.kind(),
                        event_id = %dropped.event_id(),
                        "lifecycle subscription quota subscriber mpsc full; dropping event (subscription quota observation may be missing)",
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    tracing::debug!("lifecycle subscription quota subscriber mpsc closed");
                }
            }
        }
        if let Some(tx) = limit_rejection_audit_tx {
            match tx.try_send(event.clone()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(dropped)) => {
                    record_dropped_events_by("lifecycle_limit_rejection_audit_full", 1);
                    tracing::warn!(
                        kind = dropped.kind(),
                        event_id = %dropped.event_id(),
                        "lifecycle limit rejection audit subscriber mpsc full; dropping event (audit row may be missing)",
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    tracing::debug!("lifecycle limit rejection audit subscriber mpsc closed");
                }
            }
        }
        if let Some(tx) = api_key_metrics_tx {
            match tx.try_send(event.clone()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(dropped)) => {
                    record_dropped_events_by("lifecycle_api_key_metrics_full", 1);
                    tracing::warn!(
                        kind = dropped.kind(),
                        event_id = %dropped.event_id(),
                        "lifecycle api-key metrics subscriber mpsc full; dropping event (metric samples may be missing)",
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    tracing::debug!("lifecycle api-key metrics subscriber mpsc closed");
                }
            }
        }
        if let Some(tx) = cache_hit_miss_tx {
            match tx.try_send(event.clone()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(dropped)) => {
                    record_dropped_events_by("lifecycle_cache_hit_miss_full", 1);
                    tracing::warn!(
                        kind = dropped.kind(),
                        event_id = %dropped.event_id(),
                        "lifecycle cache hit/miss subscriber mpsc full; dropping event (hit/miss metric may be missing)",
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    tracing::debug!("lifecycle cache hit/miss subscriber mpsc closed");
                }
            }
        }
        if let Some(tx) = routing_tier_tx {
            match tx.try_send(event.clone()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(dropped)) => {
                    record_dropped_events_by("lifecycle_routing_tier_full", 1);
                    tracing::warn!(
                        kind = dropped.kind(),
                        event_id = %dropped.event_id(),
                        "lifecycle routing tier subscriber mpsc full; dropping event (tier metric may be missing)",
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    tracing::debug!("lifecycle routing tier subscriber mpsc closed");
                }
            }
        }
        if let LifecycleEvent::PromptCacheObservationsProduced { .. } = &event
            && let Some(tx) = prompt_cache_observation_tx
        {
            match tx.try_send(event.clone()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(dropped)) => {
                    record_dropped_events_by("lifecycle_prompt_cache_observation_full", 1);
                    tracing::warn!(
                        kind = dropped.kind(),
                        event_id = %dropped.event_id(),
                        "lifecycle prompt cache observation subscriber mpsc full; dropping event (prompt cache observation may be missing)",
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    tracing::debug!("lifecycle prompt cache observation subscriber mpsc closed");
                }
            }
        }
        if let Some(tx) = prompt_cache_drift_tx {
            match tx.try_send(event) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(dropped)) => {
                    record_dropped_events_by("lifecycle_prompt_cache_drift_full", 1);
                    tracing::warn!(
                        kind = dropped.kind(),
                        event_id = %dropped.event_id(),
                        "lifecycle prompt cache drift subscriber mpsc full; dropping event (drift histogram may be missing)",
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    tracing::debug!("lifecycle prompt cache drift subscriber mpsc closed");
                }
            }
        }
    }

    fn subscribe_lifecycle(&self) -> LifecycleBusReceiver {
        LifecycleBusReceiver::InMemory(self.inner.lifecycle_broadcast_tx.subscribe())
    }
}

/// Convenience: wrap an [`InMemoryBus`] in an `Arc<dyn RequestEventBus>`.
pub fn new_in_memory_bus() -> Arc<dyn RequestEventBus> {
    Arc::new(InMemoryBus::new())
}

/// Record a `sse_lagged` event in dropped-event metrics.
///
/// Kept at this path for backward compatibility with code that imported it
/// from the (now removed) `dashboard_broadcaster` module.
pub fn record_dashboard_sse_lagged(skipped: u64) {
    record_dropped_events_by("sse_lagged", skipped);
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
        let BusReceiver::InMemory(mut rx_sse) = bus.subscribe() else {
            panic!("InMemoryBus should yield InMemory receiver");
        };

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
        let BusReceiver::InMemory(mut rx) = bus.subscribe() else {
            panic!("InMemoryBus should yield InMemory receiver");
        };
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

    #[tokio::test]
    async fn lag_helper_increments_dropped_event_counter() {
        record_dashboard_sse_lagged(3);
    }

    fn sample_lifecycle_event(request_id: &str) -> LifecycleEvent {
        LifecycleEvent::RequestStarted {
            event_id: format!("evt-{request_id}"),
            request_id: request_id.to_owned(),
            ts_ms: 1_700_000_000_000,
            stream: false,
        }
    }

    #[tokio::test]
    async fn publish_lifecycle_fans_out_to_broadcast_and_writer() {
        let bus = InMemoryBus::new();
        let LifecycleBusReceiver::InMemory(mut rx_sse) = bus.subscribe_lifecycle() else {
            panic!("InMemoryBus should yield InMemory lifecycle receiver");
        };
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
        let LifecycleBusReceiver::InMemory(mut broadcast_rx) = bus.subscribe_lifecycle() else {
            panic!("expected in-memory lifecycle receiver");
        };
        let mut assembler_rx = bus.attach_lifecycle_assembler(1);
        let mut hook_rx = bus.attach_lifecycle_hook_adapter(1);
        let mut writer_rx = bus.attach_lifecycle_writer(1);
        let event = LifecycleEvent::RequestLogUpstreamErrorObserved {
            event_id: "evt-private".into(),
            error_type: "rate_limit_error".into(),
            error_message: "bounded".into(),
        };

        bus.publish_lifecycle(event.clone());

        assert_eq!(assembler_rx.try_recv().expect("assembler event"), event);
        assert!(broadcast_rx.try_recv().is_err());
        assert!(hook_rx.try_recv().is_err());
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
}
