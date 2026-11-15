//! API-key metrics subscriber.
//!
//! Owns `record_api_key_request_metric` and `record_api_key_usage_metrics`
//! emissions off the `LifecycleEvent` stream. Consumes `RequestStarted`,
//! `UsageObserved` / `StreamCompleted`, `RequestTerminated`, and `Priced` per
//! `event_id`. Requests/tokens emit at termination; cost joins the pricing result.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cc_lb_control::RequestEventBus;
use cc_lb_lifecycle::{EventId, LifecycleEvent};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

pub const DEFAULT_API_KEY_METRICS_MAP_CAP: usize = 4096;
pub const DEFAULT_API_KEY_METRICS_TTL: Duration = Duration::from_secs(300);
const SWEEP_INTERVAL: Duration = Duration::from_secs(30);

pub struct ApiKeyMetricsSubscriberHandle {
    shutdown_tx: oneshot::Sender<()>,
    join: JoinHandle<()>,
}

impl ApiKeyMetricsSubscriberHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        if let Err(error) = self.join.await {
            tracing::warn!(%error, "lifecycle api-key metrics subscriber task panicked");
        }
    }
}

pub fn spawn_lifecycle_api_key_metrics_subscriber(
    rx: mpsc::Receiver<LifecycleEvent>,
    _bus: Arc<dyn RequestEventBus>,
) -> ApiKeyMetricsSubscriberHandle {
    spawn_with_config(
        rx,
        DEFAULT_API_KEY_METRICS_MAP_CAP,
        DEFAULT_API_KEY_METRICS_TTL,
    )
}

pub fn spawn_with_config(
    rx: mpsc::Receiver<LifecycleEvent>,
    map_cap: usize,
    ttl: Duration,
) -> ApiKeyMetricsSubscriberHandle {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let join = tokio::spawn(subscriber_loop(rx, map_cap, ttl, shutdown_rx));
    ApiKeyMetricsSubscriberHandle { shutdown_tx, join }
}

#[derive(Default)]
struct Partial {
    inserted_at: Option<Instant>,
    key_id: Option<String>,
    principal_id: Option<String>,
    model: Option<String>,
    upstream_name: Option<String>,
    upstream_kind: Option<String>,
    input_tokens: u64,
    output_tokens: u64,
    cache_creation_input_tokens: u64,
    cache_read_input_tokens: u64,
    usage_seen: bool,
    cost_micros: Option<u64>,
    priced_seen: bool,
    terminated: bool,
}

impl Partial {
    fn new(now: Instant) -> Self {
        Self {
            inserted_at: Some(now),
            ..Self::default()
        }
    }
}

async fn subscriber_loop(
    mut rx: mpsc::Receiver<LifecycleEvent>,
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
                    Some(event) => handle_event(&mut partials, map_cap, event),
                    None => break,
                }
            }
            _ = sweeper.tick() => sweep_orphans(&mut partials, ttl),
            _ = &mut shutdown => break,
        }
    }

    while let Ok(event) = rx.try_recv() {
        handle_event(&mut partials, map_cap, event);
    }
    for partial in partials.values().filter(|partial| partial.terminated) {
        record_eviction(partial, "shutdown");
    }
}

fn handle_event(partials: &mut HashMap<EventId, Partial>, map_cap: usize, event: LifecycleEvent) {
    let now = Instant::now();
    let event_id = event.event_id().clone();

    if let LifecycleEvent::RequestTerminated { client_status, .. } = &event {
        let status = *client_status;
        if let Some(partial) = partials.get_mut(&event_id) {
            if partial.terminated {
                return;
            }
            emit_metrics(partial, status);
            partial.terminated = true;
            partial.inserted_at = Some(now);
            if partial.priced_seen
                || !partial.usage_seen
                || partial.model.is_none()
                || partial.key_id.is_none()
            {
                emit_cost_metrics(partial);
                partials.remove(&event_id);
            }
        } else {
            metrics::counter!(
                "cc_lb_lifecycle_api_key_metrics_events_total",
                "outcome" => "terminated_without_partial"
            )
            .increment(1);
        }
        return;
    }

    if matches!(event, LifecycleEvent::Priced { .. }) {
        // A price without attribution cannot contribute, including a duplicate
        // arriving after completion or an evicted join.
        if let Some(partial) = partials.get_mut(&event_id) {
            merge(partial, event);
            if partial.terminated {
                emit_cost_metrics(partial);
                partials.remove(&event_id);
            }
        }
        return;
    }

    let partial = partials
        .entry(event_id)
        .or_insert_with(|| Partial::new(now));
    if partial.terminated {
        return;
    }
    partial.inserted_at = Some(now);
    merge(partial, event);

    if partials.len() > map_cap {
        drop_oldest(partials);
    }
}

fn merge(partial: &mut Partial, event: LifecycleEvent) {
    match event {
        LifecycleEvent::AuthCompleted {
            result: Ok(info), ..
        } => {
            partial.principal_id = Some(info.principal_id);
            partial.key_id = info.key_id;
        }
        LifecycleEvent::AuthCompleted {
            result:
                Err(cc_lb_lifecycle::AuthFailure::AuthenticationFailed {
                    reason: Some(reason),
                    ..
                }),
            ..
        } => {
            metrics::counter!(
                "cclb_key_auth_failures_total",
                "reason" => reason
            )
            .increment(1);
        }
        LifecycleEvent::AuthCompleted { result: Err(_), .. } => {}
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
            partial.upstream_name = Some(info.upstream_name);
            partial.upstream_kind = info.upstream_kind;
            if let Some(model) = info.model {
                partial.model = Some(model);
            }
        }
        LifecycleEvent::UsageObserved { usage, .. } => {
            partial.input_tokens = usage.input_tokens;
            partial.output_tokens = usage.output_tokens;
            partial.cache_creation_input_tokens = usage.cache_creation_input_tokens;
            partial.cache_read_input_tokens = usage.cache_read_input_tokens;
            partial.usage_seen = true;
        }
        LifecycleEvent::StreamCompleted {
            result: Ok(success),
            ..
        } => {
            if !success.usage_present {
                return;
            }
            partial.input_tokens = success.usage.input_tokens;
            partial.output_tokens = success.usage.output_tokens;
            partial.cache_creation_input_tokens = success.usage.cache_creation_input_tokens;
            partial.cache_read_input_tokens = success.usage.cache_read_input_tokens;
            partial.usage_seen = true;
        }
        LifecycleEvent::Priced { cost, .. } => {
            partial.priced_seen = true;
            partial.cost_micros = cost.total_micros.map(|total| total.max(0) as u64);
        }
        _ => {}
    }
}

fn emit_metrics(partial: &Partial, status: u16) {
    let Some(key_id) = partial.key_id.as_deref() else {
        metrics::counter!(
            "cc_lb_lifecycle_api_key_metrics_events_total",
            "outcome" => "skipped_no_key_id"
        )
        .increment(1);
        return;
    };
    let principal_id = partial.principal_id.as_deref().unwrap_or("");
    let model = partial.model.as_deref().unwrap_or("unknown");
    let upstream_name = partial.upstream_name.as_deref().unwrap_or("unknown");
    let upstream_kind = partial.upstream_kind.as_deref().unwrap_or("unknown");

    metrics::counter!(
        "cclb_api_key_requests_total",
        "key_id" => key_id.to_owned(),
        "principal_id" => principal_id.to_owned(),
        "model" => model.to_owned(),
        "upstream_kind" => upstream_kind.to_owned(),
        "status" => status.to_string()
    )
    .increment(1);

    if !partial.usage_seen {
        return;
    }
    increment_token_metric(key_id, "input", partial.input_tokens);
    increment_token_metric(key_id, "output", partial.output_tokens);
    increment_token_metric(
        key_id,
        "cache_creation",
        partial.cache_creation_input_tokens,
    );
    increment_token_metric(key_id, "cache_read", partial.cache_read_input_tokens);

    metrics::counter!(
        "cc_lb_tokens_total",
        "principal" => principal_id.to_owned(),
        "upstream" => upstream_name.to_owned(),
        "model" => model.to_owned(),
        "direction" => "input"
    )
    .increment(partial.input_tokens);
    metrics::counter!(
        "cc_lb_tokens_total",
        "principal" => principal_id.to_owned(),
        "upstream" => upstream_name.to_owned(),
        "model" => model.to_owned(),
        "direction" => "output"
    )
    .increment(partial.output_tokens);
}

fn emit_cost_metrics(partial: &Partial) {
    let (Some(key_id), Some(cost_micros)) = (partial.key_id.as_deref(), partial.cost_micros) else {
        return;
    };
    if !partial.usage_seen {
        return;
    }
    if cost_micros > 0 {
        metrics::counter!(
            "cclb_api_key_cost_usd_micro_total",
            "key_id" => key_id.to_owned()
        )
        .increment(cost_micros);
    }
    let principal_id = partial.principal_id.as_deref().unwrap_or("");
    let model = partial.model.as_deref().unwrap_or("unknown");
    let upstream_name = partial.upstream_name.as_deref().unwrap_or("unknown");
    metrics::counter!(
        "cc_lb_virtual_cost_usd_total",
        "principal" => principal_id.to_owned(),
        "upstream" => upstream_name.to_owned(),
        "model" => model.to_owned()
    )
    .increment(cost_micros);
}

fn increment_token_metric(key_id: &str, kind: &'static str, value: u64) {
    if value == 0 {
        return;
    }
    metrics::counter!(
        "cclb_api_key_tokens_total",
        "key_id" => key_id.to_owned(),
        "kind" => kind
    )
    .increment(value);
}

fn sweep_orphans(partials: &mut HashMap<EventId, Partial>, ttl: Duration) {
    let now = Instant::now();
    partials.retain(|_, partial| {
        let keep = partial
            .inserted_at
            .is_none_or(|t| now.duration_since(t) < ttl);
        if !keep {
            record_eviction(partial, "orphan_ttl_evicted");
        }
        keep
    });
}

fn drop_oldest(partials: &mut HashMap<EventId, Partial>) {
    let Some(oldest_key) = partials
        .iter()
        // Completed requests have already contributed request/token metrics.
        // Prefer losing their unresolved cost over an entire in-flight request.
        .min_by_key(|(_, p)| (!p.terminated, p.inserted_at))
        .map(|(k, _)| k.clone())
    else {
        return;
    };
    if let Some(partial) = partials.remove(&oldest_key) {
        record_eviction(&partial, "cap_evicted");
    }
}

fn record_eviction(partial: &Partial, orphan_outcome: &'static str) {
    metrics::counter!(
        "cc_lb_lifecycle_api_key_metrics_events_total",
        "outcome" => if partial.terminated { "cost_unresolved" } else { orphan_outcome }
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;
    use cc_lb_lifecycle::{AuthInfo, ParseInfo, TerminationReason, UsageSnapshot, UsageSource};
    use cc_lb_request_log::CostBreakdown;

    fn eid(s: &str) -> EventId {
        s.to_owned()
    }

    pub(super) fn terminated(event_id: &str, status: u16) -> LifecycleEvent {
        LifecycleEvent::RequestTerminated {
            event_id: eid(event_id),
            reason: TerminationReason::Success,
            client_status: status,
            duration_ms: 42,
            first_body_chunk_ms: None,
            first_content_delta_ms: None,
            dns_ms: None,
            connect_ms: None,
            connection_reused: None,
            internal_errors: Vec::new(),
            proxy_setup_ms: None,
            request_body_read_ms: None,
            request_body_bytes: None,
            finalize_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            upstream_body_ms: None,
            event_kind: None,
            funding_ambiguous: false,
        }
    }

    fn bus() -> Arc<dyn RequestEventBus> {
        Arc::new(cc_lb_control::event_bus::InMemoryBus::new())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminated_after_key_and_usage_sequence_emits_metrics() {
        let (tx, rx) = mpsc::channel(16);
        let handle = spawn_lifecycle_api_key_metrics_subscriber(rx, bus());

        tx.send(LifecycleEvent::AuthCompleted {
            event_id: eid("api-key-a"),
            result: Ok(AuthInfo {
                principal_id: "principal-a".into(),
                key_id: Some("key-a".into()),
                principal_kind: Some("machine".into()),
                auth_ms: Some(3),
            }),
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::ParseCompleted {
            event_id: eid("api-key-a"),
            result: Ok(ParseInfo {
                model: Some("claude-sonnet-4".into()),
                ..Default::default()
            }),
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::UsageObserved {
            event_id: eid("api-key-a"),
            usage: UsageSnapshot {
                input_tokens: 10,
                output_tokens: 20,
                cache_creation_input_tokens: 3,
                cache_read_input_tokens: 4,
                ..Default::default()
            },
            source: UsageSource::NonStreamBody,
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::Priced {
            event_id: eid("api-key-a"),
            cost: CostBreakdown {
                total_micros: Some(77),
                ..Default::default()
            },
        })
        .await
        .unwrap();
        tx.send(terminated("api-key-a", 200)).await.unwrap();
        drop(tx);
        handle.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminated_without_partial_shuts_down_cleanly() {
        let (tx, rx) = mpsc::channel(16);
        let handle = spawn_lifecycle_api_key_metrics_subscriber(rx, bus());

        tx.send(terminated("api-key-b", 499)).await.unwrap();
        drop(tx);
        handle.shutdown().await;
    }
}

#[cfg(test)]
#[path = "lifecycle_api_key_metrics_subscriber_tests.rs"]
mod join_tests;
