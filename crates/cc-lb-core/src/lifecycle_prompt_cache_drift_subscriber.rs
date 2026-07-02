//! Phase-5 prompt cache drift subscriber.
//!
//! Reproduces the inline handler emissions related to the prompt-cache
//! shadow-write pipeline that ARE observable from the LifecycleEvent
//! stream:
//!   * `observe_prompt_cache_token_drift` (cc_lb_cache_token_drift
//!     histogram) - non-stream and stream call sites
//!   * `inc_cache_observation_dropped(STATUS_4XX)` for the handler-side
//!     4xx branch (lifecycle.rs:1668)
//!
//! Not reproduced (these fire from inside the prompt-cache observation
//! pipeline, not observable from lifecycle events, and remain inline for
//! Phase 6f):
//!   * `inc_cache_observation_dropped` calls inside
//!     `prompt_cache_observation_context` (context-build failures)
//!   * `inc_cache_observation_dropped` inside
//!     `record_prompt_cache_observations_for_response_status`

use std::collections::HashMap;
use std::time::{Duration, Instant};

use cc_lb_lifecycle::{EventId, LifecycleEvent};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::model_resolution::canonical_model_id;

pub const DEFAULT_PROMPT_CACHE_DRIFT_MAP_CAP: usize = 4096;
pub const DEFAULT_PROMPT_CACHE_DRIFT_TTL: Duration = Duration::from_secs(300);
const SWEEP_INTERVAL: Duration = Duration::from_secs(30);

pub struct PromptCacheDriftSubscriberHandle {
    shutdown_tx: oneshot::Sender<()>,
    join: JoinHandle<()>,
}

impl PromptCacheDriftSubscriberHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        if let Err(error) = self.join.await {
            tracing::warn!(%error, "lifecycle prompt cache drift subscriber task panicked");
        }
    }
}

pub fn spawn_lifecycle_prompt_cache_drift_subscriber(
    rx: mpsc::Receiver<LifecycleEvent>,
    prompt_cache_shadow_enabled: bool,
) -> PromptCacheDriftSubscriberHandle {
    spawn_with_config(
        rx,
        prompt_cache_shadow_enabled,
        DEFAULT_PROMPT_CACHE_DRIFT_MAP_CAP,
        DEFAULT_PROMPT_CACHE_DRIFT_TTL,
    )
}

pub fn spawn_with_config(
    rx: mpsc::Receiver<LifecycleEvent>,
    prompt_cache_shadow_enabled: bool,
    map_cap: usize,
    ttl: Duration,
) -> PromptCacheDriftSubscriberHandle {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let join = tokio::spawn(subscriber_loop(
        rx,
        prompt_cache_shadow_enabled,
        map_cap,
        ttl,
        shutdown_rx,
    ));
    PromptCacheDriftSubscriberHandle { shutdown_tx, join }
}

#[derive(Default)]
struct Partial {
    inserted_at: Option<Instant>,
    upstream_id: Option<Uuid>,
    model: Option<String>,
    predicted_cache_read_tokens: Option<u32>,
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
    prompt_cache_shadow_enabled: bool,
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
                    Some(event) => handle_event(&mut partials, prompt_cache_shadow_enabled, map_cap, event),
                    None => break,
                }
            }
            _ = sweeper.tick() => sweep_orphans(&mut partials, ttl),
            _ = &mut shutdown => break,
        }
    }

    while let Ok(event) = rx.try_recv() {
        handle_event(&mut partials, prompt_cache_shadow_enabled, map_cap, event);
    }
}

fn handle_event(
    partials: &mut HashMap<EventId, Partial>,
    prompt_cache_shadow_enabled: bool,
    map_cap: usize,
    event: LifecycleEvent,
) {
    let now = Instant::now();
    let event_id = event.event_id().clone();

    if let LifecycleEvent::RequestTerminated { client_status, .. } = &event {
        let status = *client_status;
        if let Some(partial) = partials.remove(&event_id) {
            emit_metrics(&partial, status, prompt_cache_shadow_enabled);
        } else {
            metrics::counter!(
                "cc_lb_lifecycle_prompt_cache_drift_events_total",
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
            partial.upstream_id = Some(info.upstream_id);
            if let Some(m) = info.model {
                partial.model = Some(m);
            }
            partial.predicted_cache_read_tokens = info.predicted_cache_read_tokens;
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

fn emit_metrics(partial: &Partial, status: u16, prompt_cache_shadow_enabled: bool) {
    if !prompt_cache_shadow_enabled {
        return;
    }
    let has_context = partial.has_cache_breakpoints && partial.upstream_id.is_some();
    if (400..500).contains(&status) && has_context {
        cc_lb_observability::inc_cache_observation_dropped(
            cc_lb_observability::cache_observation_dropped_reason::STATUS_4XX,
        );
    }
    if status != 200 || !partial.usage_seen || !has_context {
        return;
    }
    let Some(upstream_id) = partial.upstream_id else {
        return;
    };
    let raw_model = partial.model.as_deref().unwrap_or("");
    let canonical = canonical_model_id(raw_model);
    let predicted = i64::from(partial.predicted_cache_read_tokens.unwrap_or(0));
    let actual = i64::try_from(partial.cache_read_input_tokens).unwrap_or(i64::MAX);
    let drift = actual
        .saturating_sub(predicted)
        .clamp(i64::from(i32::MIN), i64::from(i32::MAX)) as i32;
    metrics::histogram!(
        "cc_lb_cache_token_drift",
        "upstream" => upstream_id.to_string(),
        "model" => canonical.to_owned()
    )
    .record(f64::from(drift));
    metrics::counter!(
        "cc_lb_lifecycle_prompt_cache_drift_events_total",
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
            "cc_lb_lifecycle_prompt_cache_drift_events_total",
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
        "cc_lb_lifecycle_prompt_cache_drift_events_total",
        "outcome" => "cap_evicted"
    )
    .increment(1);
}
