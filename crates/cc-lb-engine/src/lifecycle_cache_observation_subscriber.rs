use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cc_lb_control::RequestEventBus;
use cc_lb_lifecycle::{EventId, LifecycleEvent, UsageSnapshot};
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
    native_cache: Option<cc_lb_request_log::RequestNativeCacheObservation>,
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
                "cc_lb_lifecycle_cache_obs_subscriber_rows_total",
                "outcome" => "published"
            )
            .increment(1);
        } else {
            metrics::counter!(
                "cc_lb_lifecycle_cache_obs_subscriber_rows_total",
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
            partial.native_cache = info.native_cache;
        }
        LifecycleEvent::UsageObserved { usage, .. } => {
            apply_native_usage(&mut partial.native_cache, &usage);
            partial.usage = usage;
            partial.usage_seen = true;
        }
        LifecycleEvent::StreamCompleted {
            result: Ok(success),
            ..
        } => {
            if !success.usage_present {
                return;
            }
            apply_native_usage(&mut partial.native_cache, &success.usage);
            partial.usage = success.usage;
            partial.usage_seen = true;
        }
        _ => {}
    }
}

fn apply_native_usage(
    native: &mut Option<cc_lb_request_log::RequestNativeCacheObservation>,
    usage: &UsageSnapshot,
) {
    let Some(native) = native.as_mut() else {
        return;
    };
    let (evidence, cached_tokens, write_tokens) = crate::provider_cache::project_usage(
        native,
        usage.cache_read_evidence.unwrap_or_default(),
        usage.cache_read_input_tokens,
        usage.cache_write_evidence.unwrap_or_default(),
        usage.cache_write_input_tokens,
    );
    native.evidence = evidence;
    native.cached_tokens = cached_tokens;
    native.write_tokens = write_tokens;
}

fn derive_cache_state(partial: &Partial) -> RequestCacheState {
    if let Some(native) = partial.native_cache.as_ref() {
        if native.mode == "unknown"
            || native.evidence_source.as_deref() == Some("unsupported_model_or_api")
        {
            return RequestCacheState::Unknown;
        }
        if native.provider == "gemini"
            && native.mode == "explicit"
            && native.evidence != cc_lb_request_log::RequestCacheEvidence::Positive
        {
            return RequestCacheState::Unknown;
        }
        return match (native.evidence, native.write_tokens.unwrap_or_default() > 0) {
            (cc_lb_request_log::RequestCacheEvidence::Positive, true) => RequestCacheState::Refresh,
            (cc_lb_request_log::RequestCacheEvidence::Positive, false) => RequestCacheState::Hit,
            (cc_lb_request_log::RequestCacheEvidence::Zero, true) => RequestCacheState::Write,
            (cc_lb_request_log::RequestCacheEvidence::Zero, false) => RequestCacheState::Miss,
            (cc_lb_request_log::RequestCacheEvidence::Invalid, _)
            | (cc_lb_request_log::RequestCacheEvidence::Unreported, _) => {
                RequestCacheState::Unknown
            }
        };
    }
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
            "cc_lb_lifecycle_cache_obs_subscriber_rows_total",
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
        "cc_lb_lifecycle_cache_obs_subscriber_rows_total",
        "outcome" => "cap_evicted"
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cc_lb_control::{RequestEventBus, event_bus::InMemoryBus};
    use cc_lb_lifecycle::{
        EventId, LifecycleEvent, ParseInfo, StreamSuccess, TerminationReason, UsageSnapshot,
    };
    use cc_lb_request_log::RequestCacheState;
    use tokio::sync::mpsc;

    use super::spawn_lifecycle_cache_observation_subscriber;

    fn eid(s: &str) -> EventId {
        s.to_owned()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn hit_state_published_when_only_cache_read_tokens() {
        let bus = Arc::new(InMemoryBus::new());
        let (tx, rx) = mpsc::channel(16);
        let mut sub_rx = bus.subscribe_lifecycle();
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
            source: cc_lb_lifecycle::UsageSource::NonStreamBody,
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::StreamCompleted {
            event_id: eid("evt-hit"),
            result: Ok(StreamSuccess {
                usage: UsageSnapshot::default(),
                usage_present: false,
                ..Default::default()
            }),
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::RequestTerminated {
            event_id: eid("evt-hit"),
            reason: TerminationReason::Success,
            client_status: 200,
            duration_ms: 1,
            first_body_chunk_ms: None,
            first_content_delta_ms: None,
            dns_ms: None,
            connect_ms: None,
            connection_reused: None,
            internal_errors: Vec::new(),
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            request_body_read_ms: None,
            request_body_bytes: None,
            finalize_ms: None,
            upstream_body_ms: None,
            event_kind: None,
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
        let mut sub_rx = bus.subscribe_lifecycle();
        let handle = spawn_lifecycle_cache_observation_subscriber(rx, bus.clone());

        tx.send(LifecycleEvent::UsageObserved {
            event_id: eid("evt-write"),
            usage: UsageSnapshot {
                cache_creation_input_tokens: 50,
                ..UsageSnapshot::default()
            },
            source: cc_lb_lifecycle::UsageSource::NonStreamBody,
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::RequestTerminated {
            event_id: eid("evt-write"),
            reason: TerminationReason::Success,
            client_status: 200,
            duration_ms: 1,
            first_body_chunk_ms: None,
            first_content_delta_ms: None,
            dns_ms: None,
            connect_ms: None,
            connection_reused: None,
            internal_errors: Vec::new(),
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            request_body_read_ms: None,
            request_body_bytes: None,
            finalize_ms: None,
            upstream_body_ms: None,
            event_kind: None,
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
        let mut sub_rx = bus.subscribe_lifecycle();
        let handle = spawn_lifecycle_cache_observation_subscriber(rx, bus.clone());

        tx.send(LifecycleEvent::RequestTerminated {
            event_id: eid("orphan"),
            reason: TerminationReason::Dropped,
            client_status: 499,
            duration_ms: 1,
            first_body_chunk_ms: None,
            first_content_delta_ms: None,
            dns_ms: None,
            connect_ms: None,
            connection_reused: None,
            internal_errors: Vec::new(),
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            request_body_read_ms: None,
            request_body_bytes: None,
            finalize_ms: None,
            upstream_body_ms: None,
            event_kind: None,
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
    fn native_parse(event_id: &str, provider: &str, mode: &str) -> LifecycleEvent {
        LifecycleEvent::ParseCompleted {
            event_id: eid(event_id),
            result: Ok(ParseInfo {
                native_cache: Some(cc_lb_request_log::RequestNativeCacheObservation {
                    provider: provider.into(),
                    mode: mode.into(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        }
    }

    fn native_usage(
        event_id: &str,
        read: u64,
        read_evidence: &str,
        write: Option<u64>,
        write_evidence: &str,
    ) -> LifecycleEvent {
        let mut value = serde_json::to_value(UsageSnapshot {
            cache_read_input_tokens: read,
            cache_creation_input_tokens: 50,
            cache_write_input_tokens: write,
            ..Default::default()
        })
        .expect("usage snapshot serializes");
        value["cache_read_evidence"] = serde_json::json!(read_evidence);
        value["cache_write_evidence"] = serde_json::json!(write_evidence);
        let usage = serde_json::from_value(value).expect("usage snapshot deserializes");
        LifecycleEvent::UsageObserved {
            event_id: eid(event_id),
            usage,
            source: cc_lb_lifecycle::UsageSource::NonStreamBody,
        }
    }

    fn terminated(event_id: &str) -> LifecycleEvent {
        LifecycleEvent::RequestTerminated {
            event_id: eid(event_id),
            reason: TerminationReason::Success,
            client_status: 200,
            duration_ms: 1,
            first_body_chunk_ms: None,
            first_content_delta_ms: None,
            dns_ms: None,
            connect_ms: None,
            connection_reused: None,
            internal_errors: Vec::new(),
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            request_body_read_ms: None,
            request_body_bytes: None,
            finalize_ms: None,
            upstream_body_ms: None,
            event_kind: None,
        }
    }

    async fn observe_native_state(
        event_id: &str,
        provider: &str,
        mode: &str,
        read: u64,
        read_evidence: &str,
        write: Option<u64>,
        write_evidence: &str,
    ) -> RequestCacheState {
        let bus = Arc::new(InMemoryBus::new());
        let (tx, rx) = mpsc::channel(16);
        let mut sub_rx = bus.subscribe_lifecycle();
        let handle = spawn_lifecycle_cache_observation_subscriber(rx, bus.clone());
        tx.send(native_parse(event_id, provider, mode))
            .await
            .expect("subscriber is running");
        tx.send(native_usage(
            event_id,
            read,
            read_evidence,
            write,
            write_evidence,
        ))
        .await
        .expect("subscriber is running");
        tx.send(terminated(event_id))
            .await
            .expect("subscriber is running");
        drop(tx);
        handle.shutdown().await;

        while let Ok(event) = sub_rx.try_recv() {
            if let LifecycleEvent::CacheObserved { cache_state, .. } = event {
                return cache_state;
            }
        }
        panic!("native cache observation was not published");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn native_cache_write_states_cover_write_refresh_healthy_miss_and_invalid_unknown() {
        for (name, read, read_evidence, write, write_evidence, expected) in [
            (
                "native-write",
                0,
                "zero",
                Some(300),
                "positive",
                RequestCacheState::Write,
            ),
            (
                "native-refresh",
                100,
                "positive",
                Some(300),
                "positive",
                RequestCacheState::Refresh,
            ),
            (
                "native-healthy-miss",
                0,
                "zero",
                Some(0),
                "zero",
                RequestCacheState::Miss,
            ),
            (
                "native-invalid",
                0,
                "zero",
                None,
                "invalid",
                RequestCacheState::Unknown,
            ),
            (
                "native-absent-write",
                0,
                "zero",
                None,
                "unreported",
                RequestCacheState::Miss,
            ),
            (
                "native-invalid-positive-read",
                100,
                "positive",
                None,
                "invalid",
                RequestCacheState::Unknown,
            ),
        ] {
            assert_eq!(
                observe_native_state(
                    name,
                    "openai",
                    "implicit",
                    read,
                    read_evidence,
                    write,
                    write_evidence,
                )
                .await,
                expected,
                "{name}"
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn native_cache_unsupported_gemini_is_unknown_even_with_positive_write() {
        assert_eq!(
            observe_native_state(
                "native-unsupported-gemini",
                "gemini",
                "explicit",
                0,
                "zero",
                Some(300),
                "positive",
            )
            .await,
            RequestCacheState::Unknown
        );
    }

    #[test]
    fn native_cache_unsupported_source_and_unknown_mode_publish_unknown() {
        for (mode, source) in [
            ("implicit", Some("unsupported_model_or_api")),
            ("unknown", None),
        ] {
            let bus = InMemoryBus::new();
            let mut events = bus.subscribe_lifecycle();
            let mut partials = std::collections::HashMap::new();
            let parse = LifecycleEvent::ParseCompleted {
                event_id: eid("unsupported"),
                result: Ok(ParseInfo {
                    native_cache: Some(cc_lb_request_log::RequestNativeCacheObservation {
                        provider: "gemini".into(),
                        mode: mode.into(),
                        evidence_source: source.map(str::to_owned),
                        evidence: cc_lb_request_log::RequestCacheEvidence::Zero,
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            };
            for event in [
                parse,
                native_usage("unsupported", 0, "zero", Some(300), "positive"),
                terminated("unsupported"),
            ] {
                super::handle_event(&bus, &mut partials, 16, event);
            }
            assert!(matches!(
                events.try_recv(),
                Ok(LifecycleEvent::CacheObserved {
                    cache_state: RequestCacheState::Unknown,
                    ..
                })
            ));
        }
    }

    #[test]
    fn gemini_explicit_zero_evidence_is_unknown() {
        let partial = super::Partial {
            native_cache: Some(cc_lb_request_log::RequestNativeCacheObservation {
                provider: "gemini".into(),
                mode: "explicit".into(),
                evidence: cc_lb_request_log::RequestCacheEvidence::Zero,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            super::derive_cache_state(&partial),
            RequestCacheState::Unknown
        );
    }
}
