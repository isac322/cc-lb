//! Cache hit/miss subscriber.
//!
//! Owns `inc_cache_hit` / `inc_cache_miss` counter emissions off the
//! `LifecycleEvent` stream. Consumes `RequestStarted`, `UpstreamAttempt`,
//! `UsageObserved` / `StreamCompleted`, and `RequestTerminated` per
//! `event_id`; fires on termination when cache tokens indicate a hit.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cc_lb_contract::{EngineMetricsHook, EventId, LifecycleEvent};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::model_resolution::canonical_model_id;

pub const DEFAULT_CACHE_HIT_MISS_MAP_CAP: usize = 4096;
pub const DEFAULT_CACHE_HIT_MISS_TTL: Duration = Duration::from_secs(300);
const SWEEP_INTERVAL: Duration = Duration::from_secs(30);

pub struct CacheHitMissSubscriberHandle {
    shutdown_tx: oneshot::Sender<()>,
    join: JoinHandle<()>,
}

impl CacheHitMissSubscriberHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        if let Err(error) = self.join.await {
            tracing::warn!(%error, "lifecycle cache hit/miss subscriber task panicked");
        }
    }
}

pub fn spawn_lifecycle_cache_hit_miss_subscriber(
    rx: mpsc::Receiver<LifecycleEvent>,
    metrics: Arc<dyn EngineMetricsHook>,
) -> CacheHitMissSubscriberHandle {
    spawn_with_config(
        rx,
        metrics,
        DEFAULT_CACHE_HIT_MISS_MAP_CAP,
        DEFAULT_CACHE_HIT_MISS_TTL,
    )
}

pub fn spawn_with_config(
    rx: mpsc::Receiver<LifecycleEvent>,
    metrics: Arc<dyn EngineMetricsHook>,
    map_cap: usize,
    ttl: Duration,
) -> CacheHitMissSubscriberHandle {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let join = tokio::spawn(subscriber_loop(rx, metrics, map_cap, ttl, shutdown_rx));
    CacheHitMissSubscriberHandle { shutdown_tx, join }
}

#[derive(Default)]
struct Partial {
    inserted_at: Option<Instant>,
    model: Option<String>,
    upstream_name: Option<String>,
    has_cache_breakpoints: bool,
    cache_read_input_tokens: u64,
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

async fn subscriber_loop(
    mut rx: mpsc::Receiver<LifecycleEvent>,
    metrics: Arc<dyn EngineMetricsHook>,
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
                        Some(event) => handle_event(metrics.as_ref(), &mut partials, map_cap, event),
                        None => break,
                    }
                }
            _ = sweeper.tick() => sweep_orphans(&mut partials, ttl),
            _ = &mut shutdown => break,
        }
    }

    while let Ok(event) = rx.try_recv() {
        handle_event(metrics.as_ref(), &mut partials, map_cap, event);
    }
}

fn handle_event(
    metrics: &dyn EngineMetricsHook,
    partials: &mut HashMap<EventId, Partial>,
    map_cap: usize,
    event: LifecycleEvent,
) {
    let now = Instant::now();
    let event_id = event.event_id().clone();

    if let LifecycleEvent::RequestTerminated { client_status, .. } = &event {
        let status = *client_status;
        if let Some(partial) = partials.remove(&event_id) {
            emit_metric(metrics, &partial, status);
        } else {
            metrics::counter!(
                "cc_lb_contract_cache_hit_miss_events_total",
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
            partial.has_cache_breakpoints = !info.cache_breakpoints.is_empty();
        }
        LifecycleEvent::RouteCompleted {
            result: Ok(info), ..
        } => {
            if let Some(m) = info.model {
                partial.model = Some(m);
            }
            partial.upstream_name = Some(info.upstream_name);
        }
        LifecycleEvent::UsageObserved { usage, .. } => {
            partial.cache_read_input_tokens = usage.cache_read_input_tokens;
            partial.usage_seen = true;
        }
        LifecycleEvent::StreamCompleted {
            result: Ok(success),
            ..
        } => {
            partial.cache_read_input_tokens = success.usage.cache_read_input_tokens;
            partial.usage_seen = true;
        }
        _ => {}
    }
}

fn emit_metric(metrics_hook: &dyn EngineMetricsHook, partial: &Partial, status: u16) {
    if status != 200 {
        return;
    }
    if !partial.has_cache_breakpoints {
        return;
    }
    if !partial.usage_seen {
        metrics::counter!(
            "cc_lb_contract_cache_hit_miss_events_total",
            "outcome" => "skipped_no_usage"
        )
        .increment(1);
        return;
    }
    let upstream = partial.upstream_name.as_deref().unwrap_or("unknown");
    let raw_model = partial.model.as_deref().unwrap_or("");
    let model = canonical_model_id(raw_model);
    if partial.cache_read_input_tokens > 0 {
        metrics_hook.record_cache_hit(upstream, model);
    } else {
        metrics_hook.record_cache_miss(upstream, model);
    }
    metrics::counter!(
        "cc_lb_contract_cache_hit_miss_events_total",
        "outcome" => "observed"
    )
    .increment(1);
}

fn sweep_orphans(partials: &mut HashMap<EventId, Partial>, ttl: Duration) {
    let now = Instant::now();
    let before = partials.len();
    partials.retain(|_, p| p.inserted_at.is_none_or(|t| now.duration_since(t) < ttl));
    let removed = before.saturating_sub(partials.len());
    if removed > 0 {
        metrics::counter!(
            "cc_lb_contract_cache_hit_miss_events_total",
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
        "cc_lb_contract_cache_hit_miss_events_total",
        "outcome" => "cap_evicted"
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;
    use cc_lb_contract::{
        EngineMetricsHook, NoopMetricsHook, ParseInfo, RequestCacheBreakpoint,
        RequestCacheBreakpointSource, RouteInfo, TerminationReason, UsageSnapshot, UsageSource,
    };
    use uuid::Uuid;

    fn eid(s: &str) -> EventId {
        s.to_owned()
    }

    fn cache_breakpoint() -> RequestCacheBreakpoint {
        RequestCacheBreakpoint {
            block_index: 0,
            source: RequestCacheBreakpointSource::Message,
            path: "/messages/0/content/0".into(),
            message_index: Some(0),
            ttl: Some("5m".into()),
            prefix_hash: "prefix-a".into(),
            prefix_token_count: 1_200,
            lookback_prefixes: Vec::new(),
            token_estimate_source: None,
        }
    }

    fn noop_metrics() -> Arc<dyn EngineMetricsHook> {
        Arc::new(NoopMetricsHook)
    }

    fn parse_completed(event_id: &str) -> LifecycleEvent {
        LifecycleEvent::ParseCompleted {
            event_id: eid(event_id),
            result: Ok(ParseInfo {
                model: Some("claude-sonnet-4".into()),
                cache_breakpoints: vec![cache_breakpoint()],
                ..Default::default()
            }),
        }
    }

    fn route_completed(event_id: &str) -> LifecycleEvent {
        LifecycleEvent::RouteCompleted {
            event_id: eid(event_id),
            result: Ok(RouteInfo {
                upstream_id: Uuid::from_u128(3),
                upstream_name: "upstream-a".into(),
                model: Some("claude-sonnet-4".into()),
                upstream_kind: None,
                route_ms: None,
                routing_trace: None,
                predicted_cache_read_tokens: Some(100),
                matched_v3_cache_key: None,
                breakpoint_content_block_index: None,
                matched_content_block_index: None,
                lookback_distance: None,
                predicted_cache_creation_tokens_5m: None,
                predicted_cache_creation_tokens_1h: None,
                token_estimate_source: None,
                cache_value_micros: None,
                formula_winner_upstream_id: None,
                kept_upstream_id: None,
                quota_urgency_5h: None,
                quota_urgency_7d: None,
                quota_urgency_combined: None,
                quota_weight_factor: None,
                quota_cache_multiplier: None,
                quota_warning_multiplier: None,
                quota_effective_weight: None,
                quota_uniform_fallback: None,
                wrh_key_source: None,
                lineage_would_have_predicted_read_tokens: None,
                lineage_would_have_picked_upstream_id: None,
            }),
            routing_trace: None,
        }
    }

    fn terminated(event_id: &str) -> LifecycleEvent {
        LifecycleEvent::RequestTerminated {
            event_id: eid(event_id),
            reason: TerminationReason::Success,
            client_status: 200,
            duration_ms: 12,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            upstream_body_ms: None,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminated_after_usage_sequence_records_cache_hit_metric() {
        let (tx, rx) = mpsc::channel(16);
        let handle = spawn_lifecycle_cache_hit_miss_subscriber(rx, noop_metrics());

        tx.send(parse_completed("cache-a")).await.unwrap();
        tx.send(route_completed("cache-a")).await.unwrap();
        tx.send(LifecycleEvent::UsageObserved {
            event_id: eid("cache-a"),
            usage: UsageSnapshot {
                cache_read_input_tokens: 32,
                ..Default::default()
            },
            source: UsageSource::NonStreamBody,
        })
        .await
        .unwrap();
        tx.send(terminated("cache-a")).await.unwrap();
        drop(tx);
        handle.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminated_without_usage_shuts_down_cleanly() {
        let (tx, rx) = mpsc::channel(16);
        let handle = spawn_lifecycle_cache_hit_miss_subscriber(rx, noop_metrics());

        tx.send(parse_completed("cache-b")).await.unwrap();
        tx.send(route_completed("cache-b")).await.unwrap();
        tx.send(terminated("cache-b")).await.unwrap();
        drop(tx);
        handle.shutdown().await;
    }
}
