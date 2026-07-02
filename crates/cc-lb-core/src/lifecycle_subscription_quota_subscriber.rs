//! Subscription quota subscriber.
//!
//! Consumes `LifecycleEvent::UpstreamAttempt` (to correlate `upstream_id`
//! with the event) and `LifecycleEvent::UpstreamResponseStarted` (to read
//! Anthropic unified-quota headers). Reconstructs a `HeaderMap`, parses
//! observations via `observe_subscription_quota_headers`, upserts them
//! into the shared `SubscriptionQuotaCacheLike`, and enqueues durable
//! records into `SubscriptionQuotaSink` for the writer task.
//!
//! This subscriber owns the main request path. `Lifecycle::
//! record_subscription_quota_observations` is retained solely for admin
//! fire-now warmup and is not called on the proxy request path.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use cc_lb_lifecycle::{EventId, HeaderSnapshot, LifecycleEvent};
use http::header::{HeaderMap, HeaderName, HeaderValue};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::lifecycle::{SubscriptionQuotaCacheLike, observe_subscription_quota_headers};
use crate::subscription_quota_events::SubscriptionQuotaSink;

pub const DEFAULT_SUBSCRIPTION_QUOTA_MAP_CAP: usize = 4096;
pub const DEFAULT_SUBSCRIPTION_QUOTA_TTL: Duration = Duration::from_secs(300);
const SWEEP_INTERVAL: Duration = Duration::from_secs(30);

pub struct SubscriptionQuotaSubscriberHandle {
    shutdown_tx: oneshot::Sender<()>,
    join: JoinHandle<()>,
}

impl SubscriptionQuotaSubscriberHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        if let Err(error) = self.join.await {
            tracing::warn!(%error, "lifecycle subscription quota subscriber task panicked");
        }
    }
}

pub fn spawn_lifecycle_subscription_quota_subscriber(
    rx: mpsc::Receiver<LifecycleEvent>,
    cache: Option<Arc<dyn SubscriptionQuotaCacheLike>>,
    sink: Option<SubscriptionQuotaSink>,
) -> SubscriptionQuotaSubscriberHandle {
    spawn_with_config(
        rx,
        cache,
        sink,
        DEFAULT_SUBSCRIPTION_QUOTA_MAP_CAP,
        DEFAULT_SUBSCRIPTION_QUOTA_TTL,
    )
}

pub fn spawn_with_config(
    rx: mpsc::Receiver<LifecycleEvent>,
    cache: Option<Arc<dyn SubscriptionQuotaCacheLike>>,
    sink: Option<SubscriptionQuotaSink>,
    map_cap: usize,
    ttl: Duration,
) -> SubscriptionQuotaSubscriberHandle {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let join = tokio::spawn(subscriber_loop(rx, cache, sink, map_cap, ttl, shutdown_rx));
    SubscriptionQuotaSubscriberHandle { shutdown_tx, join }
}

#[derive(Default)]
struct Partial {
    inserted_at: Option<Instant>,
    upstream_id: Option<Uuid>,
}

impl Partial {
    fn new(now: Instant) -> Self {
        Self {
            inserted_at: Some(now),
            upstream_id: None,
        }
    }
}

async fn subscriber_loop(
    mut rx: mpsc::Receiver<LifecycleEvent>,
    cache: Option<Arc<dyn SubscriptionQuotaCacheLike>>,
    sink: Option<SubscriptionQuotaSink>,
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
                    Some(event) => handle_event(cache.as_deref(), sink.as_ref(), &mut partials, map_cap, event),
                    None => break,
                }
            }
            _ = sweeper.tick() => sweep_orphans(&mut partials, ttl),
            _ = &mut shutdown => break,
        }
    }

    while let Ok(event) = rx.try_recv() {
        handle_event(
            cache.as_deref(),
            sink.as_ref(),
            &mut partials,
            map_cap,
            event,
        );
    }
}

fn handle_event(
    cache: Option<&dyn SubscriptionQuotaCacheLike>,
    sink: Option<&SubscriptionQuotaSink>,
    partials: &mut HashMap<EventId, Partial>,
    map_cap: usize,
    event: LifecycleEvent,
) {
    let now = Instant::now();
    let event_id = event.event_id().clone();

    match event {
        LifecycleEvent::UpstreamAttempt { upstream_id, .. } => {
            let partial = partials
                .entry(event_id)
                .or_insert_with(|| Partial::new(now));
            partial.upstream_id = Some(upstream_id);
            partial.inserted_at = Some(now);
        }
        LifecycleEvent::UpstreamResponseStarted {
            status, headers, ..
        } => {
            let is_observable_status = (200..300).contains(&status) || status == 429;
            if !is_observable_status {
                return;
            }
            if cache.is_none() && sink.is_none() {
                return;
            }
            let Some(partial) = partials.get(&event_id) else {
                metrics::counter!(
                    "cc_lb_lifecycle_subscription_quota_events_total",
                    "outcome" => "response_without_attempt"
                )
                .increment(1);
                return;
            };
            let Some(upstream_id) = partial.upstream_id else {
                metrics::counter!(
                    "cc_lb_lifecycle_subscription_quota_events_total",
                    "outcome" => "response_without_upstream_id"
                )
                .increment(1);
                return;
            };
            apply_observations(cache, sink, upstream_id, &headers);
        }
        LifecycleEvent::RequestTerminated { .. } => {
            partials.remove(&event_id);
        }
        _ => {}
    }

    if partials.len() > map_cap {
        drop_oldest(partials);
    }
}

fn apply_observations(
    cache: Option<&dyn SubscriptionQuotaCacheLike>,
    sink: Option<&SubscriptionQuotaSink>,
    upstream_id: Uuid,
    snapshot: &HeaderSnapshot,
) {
    let observed_at_unix_millis = system_time_unix_millis(SystemTime::now());
    let header_map = header_map_from_snapshot(snapshot);
    let records =
        observe_subscription_quota_headers(&header_map, upstream_id, observed_at_unix_millis);
    if records.is_empty() {
        metrics::counter!(
            "cc_lb_lifecycle_subscription_quota_events_total",
            "outcome" => "no_headers"
        )
        .increment(1);
        return;
    }

    for record in records {
        if let Some(cache) = cache {
            cache.upsert_observation(&record);
        }
        if let Some(sink) = sink {
            let _ = sink.enqueue(record);
        }
    }

    metrics::counter!(
        "cc_lb_lifecycle_subscription_quota_events_total",
        "outcome" => "observed"
    )
    .increment(1);
}

fn header_map_from_snapshot(snapshot: &HeaderSnapshot) -> HeaderMap {
    let mut map = HeaderMap::new();
    for (name, value) in &snapshot.anthropic_headers {
        let Ok(header_name) = HeaderName::from_bytes(name.as_bytes()) else {
            continue;
        };
        let Ok(header_value) = HeaderValue::from_str(value) else {
            continue;
        };
        map.insert(header_name, header_value);
    }
    if let Some(retry_after) = snapshot.retry_after.as_deref()
        && let Ok(value) = HeaderValue::from_str(retry_after)
    {
        map.insert(http::header::RETRY_AFTER, value);
    }
    map
}

fn system_time_unix_millis(t: SystemTime) -> u64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn sweep_orphans(partials: &mut HashMap<EventId, Partial>, ttl: Duration) {
    let now = Instant::now();
    let before = partials.len();
    partials.retain(|_, p| p.inserted_at.is_none_or(|t| now.duration_since(t) < ttl));
    let removed = before.saturating_sub(partials.len());
    if removed > 0 {
        metrics::counter!(
            "cc_lb_lifecycle_subscription_quota_events_total",
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
        "cc_lb_lifecycle_subscription_quota_events_total",
        "outcome" => "cap_evicted"
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;
    use cc_lb_plugin_api::SubscriptionQuotaCandidateSnapshot;
    use cc_lb_storage_api::{
        SubscriptionQuotaObservationRecord, SubscriptionQuotaStatus, SubscriptionQuotaWindow,
    };
    use std::collections::BTreeMap;
    use std::sync::Mutex as StdMutex;

    #[derive(Default)]
    struct RecordingSubscriptionQuotaCache {
        records: StdMutex<Vec<SubscriptionQuotaObservationRecord>>,
    }

    impl SubscriptionQuotaCacheLike for RecordingSubscriptionQuotaCache {
        fn upsert_observation(&self, record: &SubscriptionQuotaObservationRecord) {
            self.records.lock().unwrap().push(record.clone());
        }

        fn snapshot_for_upstream(
            &self,
            _upstream_id: Uuid,
            _now_unix_millis: u64,
            _max_staleness_secs: u64,
        ) -> Vec<SubscriptionQuotaCandidateSnapshot> {
            Vec::new()
        }
    }

    fn eid(s: &str) -> EventId {
        s.to_owned()
    }

    fn response_started(event_id: &str, headers: HeaderSnapshot) -> LifecycleEvent {
        LifecycleEvent::UpstreamResponseStarted {
            event_id: eid(event_id),
            status: 200,
            headers,
            bulkhead_wait_ms: None,
            dns_ms: None,
            connect_ms: None,
            connection_reused: None,
            shape_ms: None,
            sign_ms: None,
            upstream_ttfb_ms: None,
        }
    }

    fn quota_headers() -> HeaderSnapshot {
        HeaderSnapshot {
            anthropic_headers: BTreeMap::from([
                (
                    "anthropic-ratelimit-unified-5h-utilization".to_owned(),
                    "0.10".to_owned(),
                ),
                (
                    "anthropic-ratelimit-unified-5h-status".to_owned(),
                    "allowed_warning".to_owned(),
                ),
                (
                    "anthropic-ratelimit-unified-5h-reset".to_owned(),
                    "1800000001".to_owned(),
                ),
            ]),
            ..Default::default()
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn response_after_attempt_updates_cache_and_sink() {
        let (tx, rx) = mpsc::channel(16);
        let upstream_id = Uuid::from_u128(2);
        let cache = Arc::new(RecordingSubscriptionQuotaCache::default());
        let subscriber_cache: Arc<dyn SubscriptionQuotaCacheLike> = cache.clone();
        let (sink, mut sink_rx) = SubscriptionQuotaSink::with_capacity(16);
        let handle =
            spawn_lifecycle_subscription_quota_subscriber(rx, Some(subscriber_cache), Some(sink));

        tx.send(LifecycleEvent::UpstreamAttempt {
            event_id: eid("quota-a"),
            attempt_num: 1,
            upstream_id,
        })
        .await
        .unwrap();
        tx.send(response_started("quota-a", quota_headers()))
            .await
            .unwrap();
        drop(tx);
        handle.shutdown().await;

        let records = cache.records.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].upstream_id, upstream_id);
        assert_eq!(records[0].window, SubscriptionQuotaWindow::FiveHour);
        assert_eq!(records[0].utilization, Some(0.10));
        assert_eq!(
            records[0].status,
            Some(SubscriptionQuotaStatus::AllowedWarning)
        );
        drop(records);

        let sink_record = sink_rx.try_recv().unwrap();
        assert_eq!(sink_record.upstream_id, upstream_id);
        assert_eq!(sink_record.window, SubscriptionQuotaWindow::FiveHour);
        assert!(sink_rx.try_recv().is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn response_without_attempt_is_ignored() {
        let (tx, rx) = mpsc::channel(16);
        let cache = Arc::new(RecordingSubscriptionQuotaCache::default());
        let subscriber_cache: Arc<dyn SubscriptionQuotaCacheLike> = cache.clone();
        let (sink, mut sink_rx) = SubscriptionQuotaSink::with_capacity(16);
        let handle =
            spawn_lifecycle_subscription_quota_subscriber(rx, Some(subscriber_cache), Some(sink));

        tx.send(response_started("quota-b", quota_headers()))
            .await
            .unwrap();
        drop(tx);
        handle.shutdown().await;

        assert!(cache.records.lock().unwrap().is_empty());
        assert!(sink_rx.try_recv().is_err());
    }
}
