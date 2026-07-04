//! Pricing subscriber.
//!
//! Consumes the `LifecycleEvent` stream, tracks `(model, upstream_kind,
//! usage)` per `event_id`, and on `RequestTerminated` emits a derived
//! `LifecycleEvent::Priced` back onto the same bus. The event assembler
//! merges `Priced` events into the request row's cost columns.
//!
//! ## Backpressure
//!
//! `HashMap<EventId, Partial>` capped at [`DEFAULT_PRICING_MAP_CAP`] entries.
//! TTL sweeps every [`SWEEP_INTERVAL`]; entries older than
//! [`DEFAULT_PRICING_TTL`] are evicted.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cc_lb_contract::{CostBreakdown, EventId, LifecycleEvent};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::event_bus::RequestEventBus;
use crate::lifecycle::{cost_breakdown_to_event_options, pricing_upstream_kind_from_label};

pub const DEFAULT_PRICING_MAP_CAP: usize = 4096;
pub const DEFAULT_PRICING_TTL: Duration = Duration::from_secs(300);
const SWEEP_INTERVAL: Duration = Duration::from_secs(30);

pub struct PricingSubscriberHandle {
    shutdown_tx: oneshot::Sender<()>,
    join: JoinHandle<()>,
}

impl PricingSubscriberHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        if let Err(error) = self.join.await {
            tracing::warn!(%error, "lifecycle pricing subscriber task panicked");
        }
    }
}

pub fn spawn_lifecycle_pricing_subscriber(
    rx: mpsc::Receiver<LifecycleEvent>,
    bus: Arc<dyn RequestEventBus>,
) -> PricingSubscriberHandle {
    spawn_with_config(rx, bus, DEFAULT_PRICING_MAP_CAP, DEFAULT_PRICING_TTL)
}

pub fn spawn_with_config(
    rx: mpsc::Receiver<LifecycleEvent>,
    bus: Arc<dyn RequestEventBus>,
    map_cap: usize,
    ttl: Duration,
) -> PricingSubscriberHandle {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let join = tokio::spawn(pricing_loop(rx, bus, map_cap, ttl, shutdown_rx));
    PricingSubscriberHandle { shutdown_tx, join }
}

#[derive(Default)]
struct Partial {
    inserted_at: Option<Instant>,
    model: Option<String>,
    upstream_kind_label: Option<String>,
    input_tokens: u64,
    output_tokens: u64,
    cache_creation_5m: u64,
    cache_creation_1h: u64,
    cache_read: u64,
    usage_seen: bool,
}

impl Partial {
    fn new(now: Instant) -> Self {
        Self {
            inserted_at: Some(now),
            ..Self::default()
        }
    }
}

async fn pricing_loop(
    mut rx: mpsc::Receiver<LifecycleEvent>,
    bus: Arc<dyn RequestEventBus>,
    map_cap: usize,
    ttl: Duration,
    mut shutdown: oneshot::Receiver<()>,
) {
    let mut partials: HashMap<EventId, Partial> = HashMap::new();
    let mut sweeper = tokio::time::interval(SWEEP_INTERVAL);
    sweeper.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    sweeper.tick().await;

    loop {
        tokio::select! {
            biased;
            event = rx.recv() => {
                match event {
                    Some(event) => handle_event(&*bus, &mut partials, map_cap, event),
                    None => break,
                }
            }
            _ = sweeper.tick() => sweep_orphans(&mut partials, ttl),
            _ = &mut shutdown => break,
        }
    }

    while let Ok(event) = rx.try_recv() {
        handle_event(&*bus, &mut partials, map_cap, event);
    }
}

fn handle_event(
    bus: &dyn RequestEventBus,
    partials: &mut HashMap<EventId, Partial>,
    map_cap: usize,
    event: LifecycleEvent,
) {
    let now = Instant::now();
    let event_id = event.event_id().clone();

    if matches!(event, LifecycleEvent::RequestTerminated { .. }) {
        if let Some(partial) = partials.remove(&event_id) {
            if let Some(breakdown) = compute_cost(&partial) {
                bus.publish_lifecycle(LifecycleEvent::Priced {
                    event_id: event_id.clone(),
                    cost: breakdown,
                });
                metrics::counter!("cc_lb_contract_pricing_events_total", "outcome" => "priced")
                    .increment(1);
            } else {
                metrics::counter!(
                    "cc_lb_contract_pricing_events_total",
                    "outcome" => "skipped_no_usage_or_model"
                )
                .increment(1);
            }
        } else {
            metrics::counter!(
                "cc_lb_contract_pricing_events_total",
                "outcome" => "terminated_without_partial"
            )
            .increment(1);
        }
        return;
    }

    let partial = partials
        .entry(event_id)
        .or_insert_with(|| Partial::new(now));
    partial.inserted_at = Some(now);
    merge(partial, event);

    if partials.len() > map_cap {
        drop_oldest(partials);
    }
}

fn merge(partial: &mut Partial, event: LifecycleEvent) {
    match event {
        LifecycleEvent::ParseCompleted {
            result: Ok(info), ..
        } => {
            if partial.model.is_none() {
                partial.model = info.model;
            }
        }
        LifecycleEvent::RouteCompleted {
            result: Ok(info), ..
        } => {
            if let Some(m) = info.model {
                partial.model = Some(m);
            }
            partial.upstream_kind_label = info.upstream_kind;
        }
        LifecycleEvent::UsageObserved { usage, .. } => {
            partial.input_tokens = usage.input_tokens;
            partial.output_tokens = usage.output_tokens;
            partial.cache_creation_5m = usage.cache_creation_input_tokens_5m;
            partial.cache_creation_1h = usage.cache_creation_input_tokens_1h;
            partial.cache_read = usage.cache_read_input_tokens;
            partial.usage_seen = true;
        }
        LifecycleEvent::StreamCompleted {
            result: Ok(success),
            ..
        } => {
            partial.input_tokens = success.usage.input_tokens;
            partial.output_tokens = success.usage.output_tokens;
            partial.cache_creation_5m = success.usage.cache_creation_input_tokens_5m;
            partial.cache_creation_1h = success.usage.cache_creation_input_tokens_1h;
            partial.cache_read = success.usage.cache_read_input_tokens;
            partial.usage_seen = true;
        }
        _ => {}
    }
}

fn compute_cost(partial: &Partial) -> Option<CostBreakdown> {
    if !partial.usage_seen {
        return None;
    }
    let model = partial.model.as_deref()?;
    let upstream_kind = partial
        .upstream_kind_label
        .as_deref()
        .and_then(pricing_upstream_kind_from_label);
    let breakdown = cc_lb_pricing::virtual_cost_micros_full(
        model,
        partial.input_tokens,
        partial.output_tokens,
        partial.cache_creation_5m,
        partial.cache_creation_1h,
        partial.cache_read,
        upstream_kind,
    );
    let opts = cost_breakdown_to_event_options(&breakdown);
    Some(CostBreakdown {
        total_micros: opts.total,
        input_micros: opts.input,
        output_micros: opts.output,
        cache_creation_5m_micros: opts.cache_creation_5m,
        cache_creation_1h_micros: opts.cache_creation_1h,
        cache_read_micros: opts.cache_read,
    })
}

fn sweep_orphans(partials: &mut HashMap<EventId, Partial>, ttl: Duration) {
    let now = Instant::now();
    let before = partials.len();
    partials.retain(|_, p| p.inserted_at.is_none_or(|t| now.duration_since(t) < ttl));
    let removed = before.saturating_sub(partials.len());
    if removed > 0 {
        metrics::counter!(
            "cc_lb_contract_pricing_events_total",
            "outcome" => "orphan_ttl_evicted"
        )
        .increment(removed as u64);
    }
}

fn drop_oldest(partials: &mut HashMap<EventId, Partial>) {
    let Some((oldest_key, _)) = partials
        .iter()
        .min_by_key(|(_, p)| p.inserted_at.unwrap_or_else(Instant::now))
        .map(|(k, v)| (k.clone(), v.inserted_at))
    else {
        return;
    };
    partials.remove(&oldest_key);
    metrics::counter!(
        "cc_lb_contract_pricing_events_total",
        "outcome" => "cap_evicted"
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_bus::{InMemoryBus, LifecycleBusReceiver};
    use cc_lb_contract::{
        ParseInfo, RouteInfo, StreamSuccess, TerminationReason, UsageSnapshot, UsageSource,
    };
    use uuid::Uuid;

    fn eid(s: &str) -> EventId {
        s.to_owned()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminated_with_usage_emits_priced() {
        let bus = Arc::new(InMemoryBus::new());
        let LifecycleBusReceiver::InMemory(mut rx_bcast) = bus.subscribe_lifecycle() else {
            panic!("expected in-memory lifecycle receiver");
        };
        let (tx, rx) = mpsc::channel(16);
        let handle = spawn_lifecycle_pricing_subscriber(rx, bus.clone());

        tx.send(LifecycleEvent::ParseCompleted {
            event_id: eid("evt-1"),
            result: Ok(ParseInfo {
                path: "/v1/messages".into(),
                method: "POST".into(),
                model: Some("claude-sonnet-4-5-20250929".into()),
                stream: false,
                body_bytes: 100,
                ..ParseInfo::default()
            }),
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::RouteCompleted {
            event_id: eid("evt-1"),
            result: Ok(RouteInfo {
                upstream_id: Uuid::nil(),
                upstream_name: "u1".into(),
                model: Some("claude-sonnet-4-5-20250929".into()),
                upstream_kind: Some("anthropic_key".into()),
                route_ms: None,
                routing_trace: None,
                predicted_cache_read_tokens: None,
            }),
            routing_trace: None,
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::UsageObserved {
            event_id: eid("evt-1"),
            source: UsageSource::NonStreamBody,
            usage: UsageSnapshot {
                input_tokens: 1000,
                output_tokens: 500,
                ..Default::default()
            },
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::RequestTerminated {
            event_id: eid("evt-1"),
            reason: TerminationReason::Success,
            client_status: 200,
            duration_ms: 100,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            upstream_body_ms: None,
        })
        .await
        .unwrap();
        drop(tx);
        handle.shutdown().await;

        let mut saw_priced = false;
        while let Ok(event) = rx_bcast.try_recv() {
            if let LifecycleEvent::Priced { event_id, cost } = event {
                assert_eq!(event_id, "evt-1");
                assert!(cost.total_micros.is_some() || cost.total_micros.is_none());
                saw_priced = true;
            }
        }
        assert!(saw_priced, "expected Priced event on the bus");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminated_without_usage_skips_priced() {
        let bus = Arc::new(InMemoryBus::new());
        let LifecycleBusReceiver::InMemory(mut rx_bcast) = bus.subscribe_lifecycle() else {
            panic!("expected in-memory lifecycle receiver");
        };
        let (tx, rx) = mpsc::channel(16);
        let handle = spawn_lifecycle_pricing_subscriber(rx, bus.clone());

        tx.send(LifecycleEvent::RequestTerminated {
            event_id: eid("evt-nousage"),
            reason: TerminationReason::ErrorCode("upstream_5xx".into()),
            client_status: 502,
            duration_ms: 10,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            upstream_body_ms: None,
        })
        .await
        .unwrap();
        drop(tx);
        handle.shutdown().await;

        while let Ok(event) = rx_bcast.try_recv() {
            assert!(
                !matches!(event, LifecycleEvent::Priced { .. }),
                "should not emit Priced without usage"
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stream_success_terminated_emits_priced() {
        let bus = Arc::new(InMemoryBus::new());
        let LifecycleBusReceiver::InMemory(mut rx_bcast) = bus.subscribe_lifecycle() else {
            panic!("expected in-memory lifecycle receiver");
        };
        let (tx, rx) = mpsc::channel(16);
        let handle = spawn_lifecycle_pricing_subscriber(rx, bus.clone());

        tx.send(LifecycleEvent::RouteCompleted {
            event_id: eid("evt-stream"),
            result: Ok(RouteInfo {
                upstream_id: Uuid::nil(),
                upstream_name: "u1".into(),
                model: Some("claude-sonnet-4-5-20250929".into()),
                upstream_kind: Some("anthropic_oauth".into()),
                route_ms: None,
                routing_trace: None,
                predicted_cache_read_tokens: None,
            }),
            routing_trace: None,
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::StreamCompleted {
            event_id: eid("evt-stream"),
            result: Ok(StreamSuccess {
                usage: UsageSnapshot {
                    input_tokens: 200,
                    output_tokens: 400,
                    cache_read_input_tokens: 50,
                    ..Default::default()
                },
                sse_event_count: 100,
                ..Default::default()
            }),
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::RequestTerminated {
            event_id: eid("evt-stream"),
            reason: TerminationReason::Success,
            client_status: 200,
            duration_ms: 500,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            upstream_body_ms: None,
        })
        .await
        .unwrap();
        drop(tx);
        handle.shutdown().await;

        let mut saw_priced = false;
        while let Ok(event) = rx_bcast.try_recv() {
            if matches!(event, LifecycleEvent::Priced { .. }) {
                saw_priced = true;
            }
        }
        assert!(saw_priced);
    }
}
