//! Cache hit/miss subscriber.
//!
//! Owns `inc_cache_hit` / `inc_cache_miss` and prompt-cache prediction
//! outcome counter emissions off the `LifecycleEvent` stream. Consumes
//! `RequestStarted`, `UpstreamAttempt`, `UsageObserved` / `StreamCompleted`,
//! and `RequestTerminated` per `event_id`; fires on termination when cache
//! tokens indicate a hit.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cc_lb_lifecycle::{EventId, LifecycleEvent};
use cc_lb_observability::EngineMetricsHook;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::model_resolution::canonical_model_id;

pub const DEFAULT_CACHE_HIT_MISS_MAP_CAP: usize = 4096;
pub const DEFAULT_CACHE_HIT_MISS_TTL: Duration = Duration::from_secs(300);
const SWEEP_INTERVAL: Duration = Duration::from_secs(30);
const CACHE_PREDICTION_METRIC: &str = "cc_lb_cache_prediction_total";
const PREDICTED_HIT_ACTUAL_HIT: &str = "predicted_hit_actual_hit";
const UNPREDICTED_HIT: &str = "unpredicted_hit";
const PREDICTED_MISS_ACTUAL_MISS: &str = "predicted_miss_actual_miss";
const PREDICTED_HIT_ACTUAL_MISS: &str = "predicted_hit_actual_miss";

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
    predicted_cache_read_tokens: Option<u32>,
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
            if let Some(cache_breakpoints) = info.cache_breakpoints {
                partial.has_cache_breakpoints = !cache_breakpoints.is_empty();
            }
            partial.upstream_name = Some(info.upstream_name);
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
            if !success.usage_present {
                return;
            }
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
            "cc_lb_lifecycle_cache_hit_miss_events_total",
            "outcome" => "skipped_no_usage"
        )
        .increment(1);
        return;
    }
    let upstream = partial.upstream_name.as_deref().unwrap_or("unknown");
    let raw_model = partial.model.as_deref().unwrap_or("");
    let model = canonical_model_id(raw_model);
    let actual_hit = partial.cache_read_input_tokens > 0;
    if actual_hit {
        metrics_hook.record_cache_hit(upstream, model);
    } else {
        metrics_hook.record_cache_miss(upstream, model);
    }
    let predicted_hit = partial
        .predicted_cache_read_tokens
        .is_some_and(|tokens| tokens > 0);
    let outcome = match (predicted_hit, actual_hit) {
        (true, true) => PREDICTED_HIT_ACTUAL_HIT,
        (false, true) => UNPREDICTED_HIT,
        (false, false) => PREDICTED_MISS_ACTUAL_MISS,
        (true, false) => PREDICTED_HIT_ACTUAL_MISS,
    };
    metrics::counter!(CACHE_PREDICTION_METRIC, "outcome" => outcome).increment(1);
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

#[cfg(test)]
mod tests {
    use super::*;
    use cc_lb_lifecycle::{
        ParseFailure, ParseInfo, RouteFailure, RouteInfo, StreamError, StreamSuccess,
        TerminationReason, UsageSnapshot, UsageSource,
    };
    use cc_lb_observability::MetricsCrateHook;
    use cc_lb_request_log::{RequestCacheBreakpoint, RequestCacheBreakpointSource};
    use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
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

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    const OUTCOMES: [&str; 4] = [
        "predicted_hit_actual_hit",
        "unpredicted_hit",
        "predicted_miss_actual_miss",
        "predicted_hit_actual_miss",
    ];

    fn parse_completed_with_breakpoints(
        event_id: &str,
        cache_breakpoints: Vec<RequestCacheBreakpoint>,
    ) -> LifecycleEvent {
        LifecycleEvent::ParseCompleted {
            event_id: eid(event_id),
            result: Ok(ParseInfo {
                model: Some("claude-sonnet-4".into()),
                cache_breakpoints,
                ..Default::default()
            }),
        }
    }

    fn parse_completed(event_id: &str) -> LifecycleEvent {
        parse_completed_with_breakpoints(event_id, vec![cache_breakpoint()])
    }

    fn parse_completed_without_breakpoints(event_id: &str) -> LifecycleEvent {
        parse_completed_with_breakpoints(event_id, Vec::new())
    }

    fn route_completed(
        event_id: &str,
        prediction: Result<Option<u32>, RouteFailure>,
    ) -> LifecycleEvent {
        route_completed_with_breakpoints(event_id, prediction, None)
    }

    fn route_completed_with_breakpoints(
        event_id: &str,
        prediction: Result<Option<u32>, RouteFailure>,
        cache_breakpoints: Option<Vec<RequestCacheBreakpoint>>,
    ) -> LifecycleEvent {
        LifecycleEvent::RouteCompleted {
            event_id: eid(event_id),
            result: prediction.map(|predicted_cache_read_tokens| RouteInfo {
                cache_breakpoints,
                cache_prefix_hash: None,
                upstream_id: Uuid::from_u128(3),
                upstream_name: "upstream-a".into(),
                model: Some("claude-sonnet-4".into()),
                upstream_kind: None,
                route_ms: None,
                routing_trace: None,
                predicted_cache_read_tokens,
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
                quota_warning_multiplier: None,
            }),
            routing_trace: None,
        }
    }

    fn terminated(event_id: &str, client_status: u16, reason: TerminationReason) -> LifecycleEvent {
        LifecycleEvent::RequestTerminated {
            event_id: eid(event_id),
            reason,
            client_status,
            duration_ms: 12,
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
        }
    }

    fn usage(event_id: &str, read: u64, creation: u64) -> LifecycleEvent {
        LifecycleEvent::UsageObserved {
            event_id: eid(event_id),
            usage: UsageSnapshot {
                cache_read_input_tokens: read,
                cache_creation_input_tokens: creation,
                ..Default::default()
            },
            source: UsageSource::NonStreamBody,
        }
    }

    fn completed(event_id: &str, read: Result<u64, ()>) -> LifecycleEvent {
        completed_with_presence(event_id, read, true)
    }

    fn completed_without_usage(event_id: &str, read: u64) -> LifecycleEvent {
        completed_with_presence(event_id, Ok(read), false)
    }

    fn completed_with_presence(
        event_id: &str,
        read: Result<u64, ()>,
        usage_present: bool,
    ) -> LifecycleEvent {
        LifecycleEvent::StreamCompleted {
            event_id: eid(event_id),
            result: read
                .map(|cache_read_input_tokens| StreamSuccess {
                    usage: UsageSnapshot {
                        cache_read_input_tokens,
                        ..Default::default()
                    },
                    usage_present,
                    ..Default::default()
                })
                .map_err(|()| StreamError {
                    error_type: "upstream_error".into(),
                    error_message: "stream failed after usage".into(),
                    ..Default::default()
                }),
        }
    }

    async fn send(
        tx: &mpsc::Sender<LifecycleEvent>,
        events: impl IntoIterator<Item = LifecycleEvent>,
    ) -> TestResult {
        for event in events {
            tx.send(event).await?;
        }
        Ok(())
    }

    async fn render_events(events: Vec<LifecycleEvent>) -> TestResult<String> {
        let recorder = PrometheusBuilder::new().build_recorder();
        let prometheus = recorder.handle();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);
        let (tx, rx) = mpsc::channel(16);
        let subscriber = spawn_lifecycle_cache_hit_miss_subscriber(rx, Arc::new(MetricsCrateHook));
        send(&tx, events).await?;
        drop(tx);
        subscriber.shutdown().await;
        Ok(prometheus.render())
    }

    fn counter_total(rendered: &str, name: &str) -> TestResult<u64> {
        let labeled = format!("{name}{{");
        let unlabeled = format!("{name} ");
        rendered
            .lines()
            .filter(|line| line.starts_with(&labeled) || line.starts_with(&unlabeled))
            .try_fold(0, |total, line| {
                let (_, value) = line.rsplit_once(' ').ok_or("counter has no value")?;
                Ok(total + value.parse::<u64>()?)
            })
    }

    fn assert_counts(
        context: &str,
        rendered: &str,
        outcomes: &[(&str, u64)],
        hits: u64,
        misses: u64,
    ) -> TestResult {
        // Healthy controls must pass on the pre-feature implementation as well.
        assert_eq!(
            counter_total(rendered, "cc_lb_cache_hit_total")?,
            hits,
            "{context}: actual hits\n{rendered}"
        );
        assert_eq!(
            counter_total(rendered, "cc_lb_cache_miss_total")?,
            misses,
            "{context}: actual misses\n{rendered}"
        );
        for line in rendered
            .lines()
            .filter(|line| line.starts_with("cc_lb_cache_prediction_total"))
        {
            let (series, _) = line.rsplit_once(' ').ok_or("counter has no value")?;
            assert!(
                OUTCOMES.iter().any(|outcome| {
                    series == format!("cc_lb_cache_prediction_total{{outcome=\"{outcome}\"}}")
                }),
                "{context}: unexpected labels or outcome: {line}"
            );
        }
        for outcome in OUTCOMES {
            let expected = outcomes
                .iter()
                .find_map(|(label, value)| (*label == outcome).then_some(*value))
                .unwrap_or_default();
            let series = format!("cc_lb_cache_prediction_total{{outcome=\"{outcome}\"}}");
            let actual = rendered
                .lines()
                .find_map(|line| line.strip_prefix(&format!("{series} ")))
                .map(str::parse::<u64>)
                .transpose()?
                .unwrap_or_default();
            assert_eq!(actual, expected, "{context}: {series}\n{rendered}");
            if expected > 0 {
                assert!(
                    rendered
                        .lines()
                        .any(|line| line == format!("{series} {expected}")),
                    "{context}: missing outcome-only numeric sample\n{rendered}"
                );
            }
        }
        Ok(())
    }

    async fn checkpoint(
        tx: &mpsc::Sender<LifecycleEvent>,
        prometheus: &PrometheusHandle,
        sequence: u64,
    ) -> TestResult {
        // A real terminal event without a partial provides an observable FIFO barrier.
        tx.send(terminated("__barrier", 200, TerminationReason::Success))
            .await?;
        let barrier = format!(
            "cc_lb_lifecycle_cache_hit_miss_events_total{{outcome=\"terminated_without_partial\"}} {sequence}"
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if prometheus.render().lines().any(|line| line == barrier) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cache_prediction_outcomes_follow_truth_table() -> TestResult {
        for (prediction, read, outcome) in [
            (Some(Some(100)), 32, "predicted_hit_actual_hit"),
            (Some(Some(100)), 0, "predicted_hit_actual_miss"),
            (Some(Some(0)), 32, "unpredicted_hit"),
            (Some(Some(0)), 0, "predicted_miss_actual_miss"),
            (Some(None), 32, "unpredicted_hit"),
            (Some(None), 0, "predicted_miss_actual_miss"),
            (None, 32, "unpredicted_hit"),
            (None, 0, "predicted_miss_actual_miss"),
        ] {
            let mut events = vec![parse_completed("truth")];
            if let Some(prediction) = prediction {
                events.push(route_completed("truth", Ok(prediction)));
            }
            events.extend([
                usage("truth", read, 0),
                terminated("truth", 200, TerminationReason::Success),
            ]);
            let rendered = render_events(events).await?;
            assert_counts(
                &format!("prediction={prediction:?}, read={read}"),
                &rendered,
                &[(outcome, 1)],
                u64::from(read > 0),
                u64::from(read == 0),
            )?;
        }
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cache_prediction_preserves_status_breakpoint_and_usage_gates() -> TestResult {
        for (name, status, reason, parse, observed, expected) in [
            (
                "success",
                200,
                TerminationReason::Success,
                Some(true),
                true,
                true,
            ),
            (
                "created",
                201,
                TerminationReason::Success,
                Some(true),
                true,
                false,
            ),
            (
                "error status",
                500,
                TerminationReason::Success,
                Some(true),
                true,
                false,
            ),
            (
                "dropped with 200",
                200,
                TerminationReason::Dropped,
                Some(true),
                true,
                true,
            ),
            (
                "error reason with 200",
                200,
                TerminationReason::ErrorCode("upstream_error".into()),
                Some(true),
                true,
                true,
            ),
            (
                "no breakpoints",
                200,
                TerminationReason::Success,
                Some(false),
                true,
                false,
            ),
            (
                "parse failed",
                200,
                TerminationReason::Success,
                None,
                true,
                false,
            ),
            (
                "no usage",
                200,
                TerminationReason::Success,
                Some(true),
                false,
                false,
            ),
        ] {
            let parse = LifecycleEvent::ParseCompleted {
                event_id: eid(name),
                result: match parse {
                    Some(has_breakpoints) => Ok(ParseInfo {
                        cache_breakpoints: if has_breakpoints {
                            vec![cache_breakpoint()]
                        } else {
                            Vec::new()
                        },
                        ..Default::default()
                    }),
                    None => Err(ParseFailure::InvalidJson),
                },
            };
            let mut events = vec![parse, route_completed(name, Ok(Some(100)))];
            if observed {
                events.push(usage(name, 0, 0));
            }
            events.push(terminated(name, status, reason));
            assert_counts(
                name,
                &render_events(events).await?,
                &[("predicted_hit_actual_miss", u64::from(expected))],
                0,
                u64::from(expected),
            )?;
        }
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cache_prediction_uses_latest_successful_route_including_clears() -> TestResult {
        let failed = Err(RouteFailure::RouteNoUpstreamAfterFilter);
        for (name, routes, outcome) in [
            (
                "positive to zero",
                vec![Ok(Some(100)), Ok(Some(0))],
                "unpredicted_hit",
            ),
            (
                "positive to none",
                vec![Ok(Some(100)), Ok(None)],
                "unpredicted_hit",
            ),
            (
                "zero to positive",
                vec![Ok(Some(0)), Ok(Some(100))],
                "predicted_hit_actual_hit",
            ),
            (
                "none to positive",
                vec![Ok(None), Ok(Some(100))],
                "predicted_hit_actual_hit",
            ),
            ("failed only", vec![failed.clone()], "unpredicted_hit"),
            (
                "success then failure",
                vec![Ok(Some(100)), failed.clone()],
                "predicted_hit_actual_hit",
            ),
            (
                "failure then success",
                vec![failed.clone(), Ok(Some(100))],
                "predicted_hit_actual_hit",
            ),
            (
                "clear then failure",
                vec![Ok(Some(100)), Ok(None), failed],
                "unpredicted_hit",
            ),
        ] {
            let mut events = vec![parse_completed(name)];
            events.extend(routes.into_iter().map(|route| route_completed(name, route)));
            events.extend([
                usage(name, 10, 0),
                terminated(name, 200, TerminationReason::Success),
            ]);
            assert_counts(name, &render_events(events).await?, &[(outcome, 1)], 1, 0)?;
        }
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cache_prediction_uses_latest_usage_snapshot_and_preserves_usage_on_error() -> TestResult
    {
        for (name, snapshots, hit, seen) in [
            (
                "usage hit then stream miss",
                vec![usage("usage", 10, 0), completed("usage", Ok(0))],
                false,
                true,
            ),
            (
                "usage hit then absent stream",
                vec![usage("usage", 10, 0), completed_without_usage("usage", 0)],
                true,
                true,
            ),
            (
                "absent stream only",
                vec![completed_without_usage("usage", 10)],
                false,
                false,
            ),
            (
                "usage miss then stream hit",
                vec![usage("usage", 0, 0), completed("usage", Ok(10))],
                true,
                true,
            ),
            (
                "stream hit then usage miss",
                vec![completed("usage", Ok(10)), usage("usage", 0, 0)],
                false,
                true,
            ),
            (
                "stream miss then usage hit",
                vec![completed("usage", Ok(0)), usage("usage", 10, 0)],
                true,
                true,
            ),
            (
                "usage hit then failed stream",
                vec![usage("usage", 10, 0), completed("usage", Err(()))],
                true,
                true,
            ),
            (
                "usage miss then failed stream",
                vec![usage("usage", 0, 0), completed("usage", Err(()))],
                false,
                true,
            ),
            (
                "failed stream only",
                vec![completed("usage", Err(()))],
                false,
                false,
            ),
        ] {
            let mut events = vec![
                parse_completed("usage"),
                route_completed("usage", Ok(Some(100))),
            ];
            events.extend(snapshots);
            events.push(terminated("usage", 200, TerminationReason::Success));
            let outcome = if hit {
                "predicted_hit_actual_hit"
            } else {
                "predicted_hit_actual_miss"
            };
            assert_counts(
                name,
                &render_events(events).await?,
                &[(outcome, u64::from(seen))],
                u64::from(seen && hit),
                u64::from(seen && !hit),
            )?;
        }
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cache_prediction_keeps_interleaved_requests_and_creation_tokens_isolated() -> TestResult
    {
        let rendered = render_events(vec![
            parse_completed("mixed"),
            parse_completed("creation"),
            route_completed("creation", Ok(None)),
            parse_completed("unexpected"),
            route_completed("mixed", Ok(Some(100))),
            parse_completed("miss"),
            usage("creation", 0, 80),
            route_completed("miss", Ok(Some(100))),
            usage("unexpected", 20, 0),
            usage("mixed", 20, 80),
            terminated("creation", 200, TerminationReason::Success),
            usage("miss", 0, 0),
            terminated("mixed", 200, TerminationReason::Success),
            terminated("miss", 200, TerminationReason::Success),
            terminated("unexpected", 200, TerminationReason::Success),
        ])
        .await?;
        assert_counts(
            "interleaved requests",
            &rendered,
            &OUTCOMES.map(|outcome| (outcome, 1)),
            2,
            2,
        )
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cache_prediction_emits_only_at_termination_and_ignores_late_events_alone() -> TestResult
    {
        let recorder = PrometheusBuilder::new().build_recorder();
        let prometheus = recorder.handle();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);
        let (tx, rx) = mpsc::channel(16);
        let subscriber = spawn_lifecycle_cache_hit_miss_subscriber(rx, Arc::new(MetricsCrateHook));
        send(
            &tx,
            [
                parse_completed("terminal"),
                route_completed("terminal", Ok(Some(100))),
                usage("terminal", 10, 0),
            ],
        )
        .await?;
        checkpoint(&tx, &prometheus, 1).await?;
        assert_counts("before termination", &prometheus.render(), &[], 0, 0)?;
        send(
            &tx,
            [
                terminated("terminal", 200, TerminationReason::Success),
                terminated("terminal", 200, TerminationReason::Success),
            ],
        )
        .await?;
        checkpoint(&tx, &prometheus, 3).await?;
        assert_counts(
            "duplicate termination",
            &prometheus.render(),
            &[("predicted_hit_actual_hit", 1)],
            1,
            0,
        )?;
        send(
            &tx,
            [
                usage("terminal", 0, 100),
                completed("terminal", Ok(100)),
                route_completed("terminal", Ok(None)),
            ],
        )
        .await?;
        drop(tx);
        subscriber.shutdown().await;
        assert_counts(
            "late events without another termination",
            &prometheus.render(),
            &[("predicted_hit_actual_hit", 1)],
            1,
            0,
        )
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cache_prediction_does_not_emit_for_cap_evicted_partial() -> TestResult {
        let recorder = PrometheusBuilder::new().build_recorder();
        let prometheus = recorder.handle();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);
        let (tx, rx) = mpsc::channel(16);
        let subscriber =
            spawn_with_config(rx, Arc::new(MetricsCrateHook), 0, Duration::from_secs(300));
        send(
            &tx,
            [
                parse_completed("evicted"),
                route_completed("evicted", Ok(Some(100))),
                usage("evicted", 100, 0),
                terminated("evicted", 200, TerminationReason::Success),
            ],
        )
        .await?;
        drop(tx);
        subscriber.shutdown().await;
        let rendered = prometheus.render();
        assert!(
            rendered.lines().any(|line| {
                line == "cc_lb_lifecycle_cache_hit_miss_events_total{outcome=\"cap_evicted\"} 3"
            }),
            "each partial must be evicted by the zero-cap subscriber\n{rendered}"
        );
        assert_counts("cap eviction before healthy control", &rendered, &[], 0, 0)?;

        let (tx, rx) = mpsc::channel(16);
        let subscriber = spawn_lifecycle_cache_hit_miss_subscriber(rx, Arc::new(MetricsCrateHook));
        send(
            &tx,
            [
                parse_completed("retained"),
                route_completed("retained", Ok(Some(100))),
                usage("retained", 0, 0),
                // The normal subscriber remains a positive recorder control.
                terminated("retained", 200, TerminationReason::Success),
            ],
        )
        .await?;
        drop(tx);
        subscriber.shutdown().await;
        let rendered = prometheus.render();
        assert!(
            rendered.lines().any(|line| {
                line == "cc_lb_lifecycle_cache_hit_miss_events_total{outcome=\"cap_evicted\"} 3"
            }),
            "the normal subscriber must not add cap evictions\n{rendered}"
        );
        assert_counts(
            "cap eviction",
            &rendered,
            &[("predicted_hit_actual_miss", 1)],
            0,
            1,
        )
    }
    #[tokio::test(flavor = "current_thread")]
    async fn cache_prediction_uses_literal_route_breakpoints_when_parse_is_empty() -> TestResult {
        for (name, read, outcome, hits, misses) in [
            ("literal route hit", 32, "predicted_hit_actual_hit", 1, 0),
            ("literal route miss", 0, "predicted_hit_actual_miss", 0, 1),
        ] {
            let rendered = render_events(vec![
                parse_completed_without_breakpoints(name),
                route_completed_with_breakpoints(
                    name,
                    Ok(Some(100)),
                    Some(vec![cache_breakpoint()]),
                ),
                usage(name, read, 0),
                terminated(name, 200, TerminationReason::Success),
            ])
            .await?;
            assert_counts(name, &rendered, &[(outcome, 1)], hits, misses)?;
        }
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cache_prediction_uses_latest_route_breakpoint_state() -> TestResult {
        let rendered = render_events(vec![
            parse_completed("latest-route-empty"),
            route_completed_with_breakpoints(
                "latest-route-empty",
                Ok(Some(100)),
                Some(vec![cache_breakpoint()]),
            ),
            route_completed_with_breakpoints("latest-route-empty", Ok(Some(100)), Some(Vec::new())),
            usage("latest-route-empty", 32, 0),
            terminated("latest-route-empty", 200, TerminationReason::Success),
        ])
        .await?;
        assert_counts("latest route empty", &rendered, &[], 0, 0)?;

        let rendered = render_events(vec![
            parse_completed("route-none-fallback"),
            route_completed_with_breakpoints("route-none-fallback", Ok(Some(100)), None),
            usage("route-none-fallback", 32, 0),
            terminated("route-none-fallback", 200, TerminationReason::Success),
        ])
        .await?;
        assert_counts(
            "route none parse fallback",
            &rendered,
            &[("predicted_hit_actual_hit", 1)],
            1,
            0,
        )?;
        Ok(())
    }
}
