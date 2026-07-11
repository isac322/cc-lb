//! Upstream rate-limit header subscriber.
//!
//! Consumes `LifecycleEvent::UpstreamAttempt` to correlate `upstream_id`
//! with the event, then `LifecycleEvent::UpstreamResponseStarted` to read
//! `anthropic-ratelimit-*` headers. Reconstructs a `HeaderMap`, parses
//! observations via `observe_rate_limits`, applies them to the shared
//! `UpstreamRateLimitCache`, and enqueues durable records into
//! `UpstreamRateLimitSink` for the writer task.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use cc_lb_lifecycle::{EventId, LifecycleEvent};
use cc_lb_request_log::HeaderSnapshot;
use http::header::{HeaderMap, HeaderName, HeaderValue};
use parking_lot::RwLock;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::lifecycle::observe_rate_limits;
use crate::upstream_rate_limit_events::UpstreamRateLimitSink;
use cc_lb_control::dynamic_view::UpstreamRateLimitCache;

pub const DEFAULT_RATE_LIMIT_HEADER_MAP_CAP: usize = 4096;
pub const DEFAULT_RATE_LIMIT_HEADER_TTL: Duration = Duration::from_secs(300);
const SWEEP_INTERVAL: Duration = Duration::from_secs(30);

pub struct RateLimitHeaderSubscriberHandle {
    shutdown_tx: oneshot::Sender<()>,
    join: JoinHandle<()>,
}

impl RateLimitHeaderSubscriberHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        if let Err(error) = self.join.await {
            tracing::warn!(%error, "lifecycle rate-limit header subscriber task panicked");
        }
    }
}

pub fn spawn_lifecycle_rate_limit_header_subscriber(
    rx: mpsc::Receiver<LifecycleEvent>,
    cache: Arc<RwLock<UpstreamRateLimitCache>>,
    sink: Option<UpstreamRateLimitSink>,
) -> RateLimitHeaderSubscriberHandle {
    spawn_with_config(
        rx,
        cache,
        sink,
        DEFAULT_RATE_LIMIT_HEADER_MAP_CAP,
        DEFAULT_RATE_LIMIT_HEADER_TTL,
    )
}

pub fn spawn_with_config(
    rx: mpsc::Receiver<LifecycleEvent>,
    cache: Arc<RwLock<UpstreamRateLimitCache>>,
    sink: Option<UpstreamRateLimitSink>,
    map_cap: usize,
    ttl: Duration,
) -> RateLimitHeaderSubscriberHandle {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let join = tokio::spawn(subscriber_loop(rx, cache, sink, map_cap, ttl, shutdown_rx));
    RateLimitHeaderSubscriberHandle { shutdown_tx, join }
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
    cache: Arc<RwLock<UpstreamRateLimitCache>>,
    sink: Option<UpstreamRateLimitSink>,
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
                    Some(event) => handle_event(&cache, sink.as_ref(), &mut partials, map_cap, event),
                    None => break,
                }
            }
            _ = sweeper.tick() => sweep_orphans(&mut partials, ttl),
            _ = &mut shutdown => break,
        }
    }

    while let Ok(event) = rx.try_recv() {
        handle_event(&cache, sink.as_ref(), &mut partials, map_cap, event);
    }
}

fn handle_event(
    cache: &Arc<RwLock<UpstreamRateLimitCache>>,
    sink: Option<&UpstreamRateLimitSink>,
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
            let Some(partial) = partials.get(&event_id) else {
                metrics::counter!(
                    "cc_lb_contract_rate_limit_header_events_total",
                    "outcome" => "response_without_attempt"
                )
                .increment(1);
                return;
            };
            let Some(upstream_id) = partial.upstream_id else {
                metrics::counter!(
                    "cc_lb_contract_rate_limit_header_events_total",
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
    cache: &Arc<RwLock<UpstreamRateLimitCache>>,
    sink: Option<&UpstreamRateLimitSink>,
    upstream_id: Uuid,
    snapshot: &HeaderSnapshot,
) {
    let observed_at = system_time_unix_secs(SystemTime::now());
    let header_map = header_map_from_snapshot(snapshot);
    let records = observe_rate_limits(&header_map, upstream_id, observed_at);
    if records.is_empty() {
        metrics::counter!(
            "cc_lb_contract_rate_limit_header_events_total",
            "outcome" => "no_headers"
        )
        .increment(1);
        return;
    }

    {
        let mut guard = cache.write();
        for record in records.iter().cloned() {
            guard.upsert_record(record);
        }
        guard.updated_at_unix_secs = observed_at;
    }

    if let Some(sink) = sink {
        for record in records {
            let _ = sink.enqueue(record);
        }
    }

    metrics::counter!(
        "cc_lb_contract_rate_limit_header_events_total",
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

fn system_time_unix_secs(t: SystemTime) -> u64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn sweep_orphans(partials: &mut HashMap<EventId, Partial>, ttl: Duration) {
    let now = Instant::now();
    let before = partials.len();
    partials.retain(|_, p| p.inserted_at.is_none_or(|t| now.duration_since(t) < ttl));
    let removed = before.saturating_sub(partials.len());
    if removed > 0 {
        metrics::counter!(
            "cc_lb_contract_rate_limit_header_events_total",
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
        "cc_lb_contract_rate_limit_header_events_total",
        "outcome" => "cap_evicted"
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;
    use cc_lb_domain::RateLimitKind;
    use std::collections::BTreeMap;

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

    fn rate_limit_headers() -> HeaderSnapshot {
        HeaderSnapshot {
            anthropic_headers: BTreeMap::from([
                (
                    "anthropic-ratelimit-requests-limit".to_owned(),
                    "1000".to_owned(),
                ),
                (
                    "anthropic-ratelimit-requests-remaining".to_owned(),
                    "997".to_owned(),
                ),
                (
                    "anthropic-ratelimit-requests-reset".to_owned(),
                    "2026-05-20T00:00:01Z".to_owned(),
                ),
                (
                    "anthropic-ratelimit-tokens-limit".to_owned(),
                    "100000".to_owned(),
                ),
                (
                    "anthropic-ratelimit-tokens-remaining".to_owned(),
                    "99990".to_owned(),
                ),
                (
                    "anthropic-ratelimit-tokens-reset".to_owned(),
                    "2026-05-20T00:00:02Z".to_owned(),
                ),
            ]),
            ..Default::default()
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn response_after_attempt_updates_cache_and_sink() {
        let (tx, rx) = mpsc::channel(16);
        let upstream_id = Uuid::from_u128(1);
        let cache = Arc::new(RwLock::new(UpstreamRateLimitCache::default()));
        let (sink, mut sink_rx) = UpstreamRateLimitSink::with_capacity(16);
        let handle = spawn_lifecycle_rate_limit_header_subscriber(rx, cache.clone(), Some(sink));

        tx.send(LifecycleEvent::UpstreamAttempt {
            event_id: eid("rate-a"),
            attempt_num: 1,
            upstream_id,
        })
        .await
        .unwrap();
        tx.send(response_started("rate-a", rate_limit_headers()))
            .await
            .unwrap();
        drop(tx);
        handle.shutdown().await;

        let cache_guard = cache.read();
        let snapshots = cache_guard.snapshots.get(&upstream_id).unwrap();
        assert_eq!(snapshots.len(), 2);
        assert!(snapshots.iter().any(|snapshot| {
            snapshot.kind == RateLimitKind::Requests
                && snapshot.limit == Some(1000)
                && snapshot.remaining == Some(997)
        }));
        assert!(snapshots.iter().any(|snapshot| {
            snapshot.kind == RateLimitKind::Tokens
                && snapshot.limit == Some(100000)
                && snapshot.remaining == Some(99990)
        }));
        drop(cache_guard);

        let mut sink_records = Vec::new();
        while let Ok(record) = sink_rx.try_recv() {
            sink_records.push(record);
        }
        assert_eq!(sink_records.len(), 2);
        assert!(
            sink_records
                .iter()
                .all(|record| record.upstream_id == upstream_id)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn response_without_attempt_is_ignored() {
        let (tx, rx) = mpsc::channel(16);
        let cache = Arc::new(RwLock::new(UpstreamRateLimitCache::default()));
        let (sink, mut sink_rx) = UpstreamRateLimitSink::with_capacity(16);
        let handle = spawn_lifecycle_rate_limit_header_subscriber(rx, cache.clone(), Some(sink));

        tx.send(response_started("rate-b", rate_limit_headers()))
            .await
            .unwrap();
        drop(tx);
        handle.shutdown().await;

        assert!(cache.read().snapshots.is_empty());
        assert!(sink_rx.try_recv().is_err());
    }
}
