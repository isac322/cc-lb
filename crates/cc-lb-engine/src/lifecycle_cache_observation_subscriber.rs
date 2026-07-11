use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cc_lb_contract::{EventId, LifecycleEvent, RequestEventBus, UsageSnapshot};
use cc_lb_request_log::RequestCacheState;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

pub const DEFAULT_CACHE_OBS_MAP_CAP: usize = 4096;
pub const DEFAULT_CACHE_OBS_TTL: Duration = Duration::from_secs(300);
const SWEEP_INTERVAL: Duration = Duration::from_secs(30);

pub struct CacheObservationSubscriberHandle {
    shutdown_tx: oneshot::Sender<()>,
    join: JoinHandle<()>,
}

impl CacheObservationSubscriberHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        if let Err(error) = self.join.await {
            tracing::warn!(%error, "lifecycle cache observation subscriber task panicked");
        }
    }
}

pub fn spawn_lifecycle_cache_observation_subscriber(
    rx: mpsc::Receiver<LifecycleEvent>,
    bus: Arc<dyn RequestEventBus>,
) -> CacheObservationSubscriberHandle {
    spawn_with_config(rx, bus, DEFAULT_CACHE_OBS_MAP_CAP, DEFAULT_CACHE_OBS_TTL)
}

pub fn spawn_with_config(
    rx: mpsc::Receiver<LifecycleEvent>,
    bus: Arc<dyn RequestEventBus>,
    map_cap: usize,
    ttl: Duration,
) -> CacheObservationSubscriberHandle {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let join = tokio::spawn(subscriber_loop(rx, bus, map_cap, ttl, shutdown_rx));
    CacheObservationSubscriberHandle { shutdown_tx, join }
}

#[derive(Default)]
struct Partial {
    inserted_at: Option<Instant>,
    cache_control_block_count: Option<u64>,
    usage: UsageSnapshot,
    usage_seen: bool,
}

impl Partial {
    fn new(now: Instant) -> Self {
        Self {
            inserted_at: Some(now),
            ..Self::default()
        }
    }

    fn touch(&mut self, now: Instant) {
        self.inserted_at = Some(now);
    }
}

async fn subscriber_loop(
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

    if let LifecycleEvent::RequestTerminated { .. } = &event {
        if let Some(partial) = partials.remove(&event_id) {
            let cache_state = derive_cache_state(&partial);
            bus.publish_lifecycle(LifecycleEvent::CacheObserved {
                event_id: event_id.clone(),
                cache_state,
            });
            metrics::counter!(
                "cc_lb_contract_cache_obs_subscriber_rows_total",
                "outcome" => "published"
            )
            .increment(1);
        } else {
            metrics::counter!(
                "cc_lb_contract_cache_obs_subscriber_rows_total",
                "outcome" => "terminated_without_partial"
            )
            .increment(1);
        }
        return;
    }

    let partial = partials
        .entry(event_id)
        .or_insert_with(|| Partial::new(now));
    partial.touch(now);
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
            partial.cache_control_block_count = info.cache_control_block_count;
        }
        LifecycleEvent::UsageObserved { usage, .. } => {
            partial.usage = usage;
            partial.usage_seen = true;
        }
        LifecycleEvent::StreamCompleted {
            result: Ok(success),
            ..
        } => {
            partial.usage = success.usage;
            partial.usage_seen = true;
        }
        _ => {}
    }
}

fn derive_cache_state(partial: &Partial) -> RequestCacheState {
    let cache_read = partial.usage.cache_read_input_tokens > 0;
    let cache_write = partial.usage.cache_creation_input_tokens > 0;
    let has_breakpoints = partial.cache_control_block_count.unwrap_or(0) > 0;
    let usage_present = partial.usage_seen;
    match (cache_read, cache_write, has_breakpoints, usage_present) {
        (true, true, _, _) => RequestCacheState::Refresh,
        (true, false, _, _) => RequestCacheState::Hit,
        (false, true, _, _) => RequestCacheState::Write,
        (false, false, true, _) => RequestCacheState::Miss,
        (false, false, false, true) => RequestCacheState::None,
        _ => RequestCacheState::Unknown,
    }
}

fn sweep_orphans(partials: &mut HashMap<EventId, Partial>, ttl: Duration) {
    let now = Instant::now();
    let before = partials.len();
    partials.retain(|_, p| p.inserted_at.is_none_or(|t| now.duration_since(t) < ttl));
    let removed = before.saturating_sub(partials.len());
    if removed > 0 {
        metrics::counter!(
            "cc_lb_contract_cache_obs_subscriber_rows_total",
            "outcome" => "orphan_ttl_evicted"
        )
        .increment(removed as u64);
    }
}

fn drop_oldest(partials: &mut HashMap<EventId, Partial>) {
    let Some(oldest_key) = partials
        .iter()
        .min_by_key(|(_, p)| p.inserted_at.unwrap_or_else(Instant::now))
        .map(|(k, _)| k.clone())
    else {
        return;
    };
    partials.remove(&oldest_key);
    metrics::counter!(
        "cc_lb_contract_cache_obs_subscriber_rows_total",
        "outcome" => "cap_evicted"
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_bus::InMemoryBus;
    use cc_lb_contract::{LifecycleBusReceiver, ParseInfo, TerminationReason};

    fn eid(s: &str) -> EventId {
        s.to_owned()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn hit_state_published_when_only_cache_read_tokens() {
        let bus = Arc::new(InMemoryBus::new());
        let (tx, rx) = mpsc::channel(16);
        let LifecycleBusReceiver::InMemory(mut sub_rx) = bus.subscribe_lifecycle() else {
            panic!("expected InMemory receiver");
        };
        let handle = spawn_lifecycle_cache_observation_subscriber(rx, bus.clone());

        tx.send(LifecycleEvent::ParseCompleted {
            event_id: eid("evt-hit"),
            result: Ok(ParseInfo {
                path: "/v1/messages".into(),
                method: "messages".into(),
                model: Some("m".into()),
                stream: false,
                body_bytes: 0,
                cache_control_block_count: Some(1),
                cache_breakpoints: Vec::new(),
                cache_prefix_hash: None,
                ..ParseInfo::default()
            }),
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::UsageObserved {
            event_id: eid("evt-hit"),
            usage: UsageSnapshot {
                cache_read_input_tokens: 100,
                ..UsageSnapshot::default()
            },
            source: cc_lb_contract::UsageSource::NonStreamBody,
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::RequestTerminated {
            event_id: eid("evt-hit"),
            reason: TerminationReason::Success,
            client_status: 200,
            duration_ms: 1,
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

        let mut observed = None;
        while let Ok(event) = sub_rx.try_recv() {
            if let LifecycleEvent::CacheObserved { cache_state, .. } = event {
                observed = Some(cache_state);
            }
        }
        assert_eq!(observed, Some(RequestCacheState::Hit));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn write_state_when_only_creation_tokens() {
        let bus = Arc::new(InMemoryBus::new());
        let (tx, rx) = mpsc::channel(16);
        let LifecycleBusReceiver::InMemory(mut sub_rx) = bus.subscribe_lifecycle() else {
            panic!("expected InMemory receiver");
        };
        let handle = spawn_lifecycle_cache_observation_subscriber(rx, bus.clone());

        tx.send(LifecycleEvent::UsageObserved {
            event_id: eid("evt-write"),
            usage: UsageSnapshot {
                cache_creation_input_tokens: 50,
                ..UsageSnapshot::default()
            },
            source: cc_lb_contract::UsageSource::NonStreamBody,
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::RequestTerminated {
            event_id: eid("evt-write"),
            reason: TerminationReason::Success,
            client_status: 200,
            duration_ms: 1,
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

        let mut observed = None;
        while let Ok(event) = sub_rx.try_recv() {
            if let LifecycleEvent::CacheObserved { cache_state, .. } = event {
                observed = Some(cache_state);
            }
        }
        assert_eq!(observed, Some(RequestCacheState::Write));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminated_without_partial_publishes_unknown() {
        let bus = Arc::new(InMemoryBus::new());
        let (tx, rx) = mpsc::channel(16);
        let LifecycleBusReceiver::InMemory(mut sub_rx) = bus.subscribe_lifecycle() else {
            panic!("expected InMemory receiver");
        };
        let handle = spawn_lifecycle_cache_observation_subscriber(rx, bus.clone());

        tx.send(LifecycleEvent::RequestTerminated {
            event_id: eid("orphan"),
            reason: TerminationReason::Dropped,
            client_status: 499,
            duration_ms: 1,
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

        let mut observed_any = false;
        while let Ok(event) = sub_rx.try_recv() {
            if matches!(event, LifecycleEvent::CacheObserved { .. }) {
                observed_any = true;
            }
        }
        assert!(!observed_any);
    }
}
