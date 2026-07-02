//! Phase-5 cache hit/miss subscriber.
//!
//! Reproduces the inline handler cache hit/miss counter emissions
//! (`inc_cache_hit` / `inc_cache_miss`) off the LifecycleEvent stream.
//! Legacy inline path remains authoritative in Phase 5; Phase 6e deletes
//! it and flips this subscriber's default.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use cc_lb_lifecycle::{EventId, LifecycleEvent};
use cc_lb_observability::{inc_cache_hit, inc_cache_miss};
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
) -> CacheHitMissSubscriberHandle {
    spawn_with_config(
        rx,
        DEFAULT_CACHE_HIT_MISS_MAP_CAP,
        DEFAULT_CACHE_HIT_MISS_TTL,
    )
}

pub fn spawn_with_config(
    rx: mpsc::Receiver<LifecycleEvent>,
    map_cap: usize,
    ttl: Duration,
) -> CacheHitMissSubscriberHandle {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let join = tokio::spawn(subscriber_loop(rx, map_cap, ttl, shutdown_rx));
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
            emit_metric(&partial, status);
        } else {
            metrics::counter!(
                "cc_lb_lifecycle_cache_hit_miss_events_total",
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

fn emit_metric(partial: &Partial, status: u16) {
    if status != 200 {
        return;
    }
    if !partial.has_cache_breakpoints {
        return;
    }
    if !partial.usage_seen {
        metrics::counter!(
            "cc_lb_lifecycle_cache_hit_miss_events_total",
            "outcome" => "skipped_no_usage"
        )
        .increment(1);
        return;
    }
    let upstream = partial.upstream_name.as_deref().unwrap_or("unknown");
    let raw_model = partial.model.as_deref().unwrap_or("");
    let model = canonical_model_id(raw_model);
    if partial.cache_read_input_tokens > 0 {
        inc_cache_hit(upstream, model);
    } else {
        inc_cache_miss(upstream, model);
    }
    metrics::counter!(
        "cc_lb_lifecycle_cache_hit_miss_events_total",
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
            "cc_lb_lifecycle_cache_hit_miss_events_total",
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
        "cc_lb_lifecycle_cache_hit_miss_events_total",
        "outcome" => "cap_evicted"
    )
    .increment(1);
}
