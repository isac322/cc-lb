use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cc_lb_domain::SubscriptionTier;
use cc_lb_lifecycle::{EventId, LifecycleEvent};
use cc_lb_observability::EngineMetricsHook;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

pub const DEFAULT_ROUTING_TIER_MAP_CAP: usize = 4096;
pub const DEFAULT_ROUTING_TIER_TTL: Duration = cc_lb_lifecycle::DEFAULT_LIFECYCLE_IDLE_TTL;
pub const DEFAULT_ROUTING_TIER_MAX_TTL: Duration = cc_lb_lifecycle::DEFAULT_LIFECYCLE_MAX_TTL;
const SWEEP_INTERVAL: Duration = Duration::from_secs(30);

pub struct RoutingTierSubscriberHandle {
    shutdown_tx: oneshot::Sender<()>,
    join: JoinHandle<()>,
}

impl RoutingTierSubscriberHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        if let Err(error) = self.join.await {
            tracing::warn!(%error, "lifecycle routing tier subscriber task panicked");
        }
    }
}

#[cfg(test)]
fn spawn_lifecycle_routing_tier_subscriber(
    rx: mpsc::Receiver<LifecycleEvent>,
    metrics: Arc<dyn EngineMetricsHook>,
) -> RoutingTierSubscriberHandle {
    spawn_with_config(
        rx,
        metrics,
        DEFAULT_ROUTING_TIER_MAP_CAP,
        DEFAULT_ROUTING_TIER_TTL,
    )
}

pub fn spawn_with_config(
    rx: mpsc::Receiver<LifecycleEvent>,
    metrics: Arc<dyn EngineMetricsHook>,
    map_cap: usize,
    ttl: Duration,
) -> RoutingTierSubscriberHandle {
    spawn_with_ttls(
        rx,
        metrics,
        map_cap,
        ttl,
        ttl.max(DEFAULT_ROUTING_TIER_MAX_TTL),
    )
}

pub fn spawn_with_ttls(
    rx: mpsc::Receiver<LifecycleEvent>,
    metrics: Arc<dyn EngineMetricsHook>,
    map_cap: usize,
    idle_ttl: Duration,
    max_ttl: Duration,
) -> RoutingTierSubscriberHandle {
    let max_ttl = max_ttl.max(idle_ttl);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let join = tokio::spawn(subscriber_loop(
        rx,
        metrics,
        map_cap,
        idle_ttl,
        max_ttl,
        shutdown_rx,
    ));
    RoutingTierSubscriberHandle { shutdown_tx, join }
}

#[derive(Default)]
struct Partial {
    created_at: Option<Instant>,
    last_activity_at: Option<Instant>,
    principal_id: Option<String>,
}

impl Partial {
    fn new(now: Instant) -> Self {
        Self {
            created_at: Some(now),
            last_activity_at: Some(now),
            ..Self::default()
        }
    }

    fn touch(&mut self, now: Instant) {
        self.last_activity_at = Some(now);
    }
}

async fn subscriber_loop(
    mut rx: mpsc::Receiver<LifecycleEvent>,
    metrics: Arc<dyn EngineMetricsHook>,
    map_cap: usize,
    idle_ttl: Duration,
    max_ttl: Duration,
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
            _ = sweeper.tick() => sweep_orphans(&mut partials, idle_ttl, max_ttl),
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
    match event {
        LifecycleEvent::RequestActivity { event_id } => {
            if let Some(partial) = partials.get_mut(&event_id) {
                partial.touch(now);
            }
        }
        LifecycleEvent::AuthenticationCompleted {
            event_id,
            principal_id,
            ..
        } => {
            let partial = partials
                .entry(event_id)
                .or_insert_with(|| Partial::new(now));
            partial.touch(now);
            partial.principal_id = Some(principal_id);
            if partials.len() > map_cap {
                drop_oldest(partials);
            }
        }
        LifecycleEvent::RouteCompleted {
            event_id,
            result: Ok(info),
            routing_trace,
        } => {
            // Keep the partial until `RequestTerminated`: a failover re-emits
            // `RouteCompleted` for the next upstream and must keep the
            // principal label.
            let principal_id = partials
                .get(&event_id)
                .and_then(|p| p.principal_id.as_deref());
            if let Some(trace) = routing_trace.as_ref().or(info.routing_trace.as_ref()) {
                for stage in &trace.stages {
                    if let Some(subscription_preference) = &stage.subscription_preference {
                        emit_metric(
                            metrics,
                            principal_id,
                            &info.upstream_name,
                            tier_to_label(subscription_preference.chosen_tier),
                        );
                    }
                }
            }
        }
        LifecycleEvent::RouteCompleted { event_id, .. }
        | LifecycleEvent::RequestTerminated { event_id, .. } => {
            partials.remove(&event_id);
        }
        _ => {}
    }
}

fn emit_metric(
    metrics: &dyn EngineMetricsHook,
    principal_id: Option<&str>,
    upstream_name: &str,
    tier: &'static str,
) {
    let Some(principal_id) = principal_id else {
        metrics::counter!(
            "cc_lb_lifecycle_routing_tier_events_total",
            "outcome" => "missing_principal_id"
        )
        .increment(1);
        return;
    };

    metrics.record_routing_tier_selection(tier, upstream_name, principal_id);
    metrics::counter!(
        "cc_lb_lifecycle_routing_tier_events_total",
        "outcome" => "emitted"
    )
    .increment(1);
}

fn tier_to_label(chosen_tier: SubscriptionTier) -> &'static str {
    match chosen_tier {
        SubscriptionTier::KnownBase => "known_base",
        SubscriptionTier::PartialBase => "partial_base",
        SubscriptionTier::Overage => "overage",
        SubscriptionTier::UnknownProbe => "unknown_probe",
    }
}

fn sweep_orphans(partials: &mut HashMap<EventId, Partial>, idle_ttl: Duration, max_ttl: Duration) {
    let effective_max_ttl = max_ttl.max(idle_ttl);
    let now = Instant::now();
    let before = partials.len();
    partials.retain(|_, p| {
        let idle_live = p
            .last_activity_at
            .is_none_or(|t| now.duration_since(t) < idle_ttl);
        let max_live = p
            .created_at
            .is_none_or(|t| now.duration_since(t) < effective_max_ttl);
        idle_live && max_live
    });
    let removed = before.saturating_sub(partials.len());
    if removed > 0 {
        metrics::counter!(
            "cc_lb_lifecycle_routing_tier_events_total",
            "outcome" => "orphan_ttl_evicted"
        )
        .increment(removed as u64);
    }
}

fn drop_oldest(partials: &mut HashMap<EventId, Partial>) {
    let Some((oldest_key, _)) = partials
        .iter()
        .min_by_key(|(_, p)| p.last_activity_at.unwrap_or_else(Instant::now))
        .map(|(k, v)| (k.clone(), v.last_activity_at))
    else {
        return;
    };
    partials.remove(&oldest_key);
    metrics::counter!(
        "cc_lb_lifecycle_routing_tier_events_total",
        "outcome" => "cap_evicted"
    )
    .increment(1);
}

#[cfg(test)]
fn partials_len(partials: &HashMap<EventId, Partial>) -> usize {
    partials.len()
}

#[cfg(test)]
#[path = "lifecycle_routing_tier_subscriber_tests.rs"]
mod tests;
