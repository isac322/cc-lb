//! Phase-5 api-key metrics subscriber.
//!
//! Reproduces the inline handler metric emissions
//! (`record_api_key_request_metric` + `record_api_key_usage_metrics`) off
//! the LifecycleEvent stream. Legacy inline path remains authoritative in
//! Phase 5; Phase 6d deletes it and flips this subscriber's default.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cc_lb_lifecycle::{EventId, LifecycleEvent};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::event_bus::RequestEventBus;

pub const DEFAULT_API_KEY_METRICS_MAP_CAP: usize = 4096;
pub const DEFAULT_API_KEY_METRICS_TTL: Duration = Duration::from_secs(300);
const SWEEP_INTERVAL: Duration = Duration::from_secs(30);

// Currently the only Upstream variant is AnthropicDirect, whose
// audit-name is "anthropic_direct" (see lifecycle::audit_upstream_name).
// The subscriber uses this static value to keep metric-label parity with
// the inline path. When a second Upstream variant is added, both the
// inline path and this subscriber must be updated in lock-step.
const UPSTREAM_AUDIT_NAME: &str = "anthropic_direct";

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
    input_tokens: u64,
    output_tokens: u64,
    cache_creation_input_tokens: u64,
    cache_read_input_tokens: u64,
    usage_seen: bool,
    cost_micros: Option<u64>,
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
}

fn handle_event(partials: &mut HashMap<EventId, Partial>, map_cap: usize, event: LifecycleEvent) {
    let now = Instant::now();
    let event_id = event.event_id().clone();

    if let LifecycleEvent::RequestTerminated { client_status, .. } = &event {
        let status = *client_status;
        if let Some(partial) = partials.remove(&event_id) {
            emit_metrics(&partial, status);
        } else {
            metrics::counter!(
                "cc_lb_lifecycle_api_key_metrics_events_total",
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
        LifecycleEvent::AuthCompleted {
            result: Ok(info), ..
        } => {
            partial.principal_id = Some(info.principal_id);
            partial.key_id = info.key_id;
        }
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
            partial.input_tokens = success.usage.input_tokens;
            partial.output_tokens = success.usage.output_tokens;
            partial.cache_creation_input_tokens = success.usage.cache_creation_input_tokens;
            partial.cache_read_input_tokens = success.usage.cache_read_input_tokens;
            partial.usage_seen = true;
        }
        LifecycleEvent::Priced { cost, .. } => {
            if let Some(total) = cost.total_micros {
                partial.cost_micros = Some(total.max(0) as u64);
            }
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

    metrics::counter!(
        "cclb_api_key_requests_total",
        "key_id" => key_id.to_owned(),
        "principal_id" => principal_id.to_owned(),
        "model" => model.to_owned(),
        "upstream_kind" => UPSTREAM_AUDIT_NAME,
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

    let cost_micros = partial.cost_micros.unwrap_or(0);
    if cost_micros > 0 {
        metrics::counter!(
            "cclb_api_key_cost_usd_micro_total",
            "key_id" => key_id.to_owned()
        )
        .increment(cost_micros);
    }

    metrics::counter!(
        "cc_lb_tokens_total",
        "principal" => principal_id.to_owned(),
        "upstream" => UPSTREAM_AUDIT_NAME,
        "model" => model.to_owned(),
        "direction" => "input"
    )
    .increment(partial.input_tokens);
    metrics::counter!(
        "cc_lb_tokens_total",
        "principal" => principal_id.to_owned(),
        "upstream" => UPSTREAM_AUDIT_NAME,
        "model" => model.to_owned(),
        "direction" => "output"
    )
    .increment(partial.output_tokens);
    metrics::counter!(
        "cc_lb_virtual_cost_usd_total",
        "principal" => principal_id.to_owned(),
        "upstream" => UPSTREAM_AUDIT_NAME,
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
    let before = partials.len();
    partials.retain(|_, p| p.inserted_at.is_none_or(|t| now.duration_since(t) < ttl));
    let removed = before.saturating_sub(partials.len());
    if removed > 0 {
        metrics::counter!(
            "cc_lb_lifecycle_api_key_metrics_events_total",
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
        "cc_lb_lifecycle_api_key_metrics_events_total",
        "outcome" => "cap_evicted"
    )
    .increment(1);
}
