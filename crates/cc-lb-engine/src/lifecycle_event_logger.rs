//! Lifecycle-event metrics subscriber.
//!
//! Counts every `LifecycleEvent` and keeps a bounded, event-id keyed timing
//! aggregate until the terminal event arrives. Terminal metrics observe parent
//! stages once, including disjoint retry overhead, and expose fixed-vocabulary
//! diagnostic I/O stages without adding overlapping children to accounting.
//!
//! ## Shutdown protocol
//!
//! Signal, drain, await. Late lifecycle events after shutdown are lost;
//! this is acceptable because the assembler already persisted the row.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use cc_lb_lifecycle::{
    LifecycleEvent, LimitDecisionKind, RequestIoTimings, RouteFailure, TerminationReason,
};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

const MAX_ACTIVE_REQUEST_TIMINGS: usize = 16_384;
const UNACCOUNTED_BUDGET_MS: u64 = 10;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum SourceKind {
    Proxy,
    Renewal,
    #[default]
    Unknown,
}

impl SourceKind {
    fn from_event(value: Option<&str>) -> Self {
        match value {
            Some("proxy") => Self::Proxy,
            Some("renewal") => Self::Renewal,
            _ => Self::Unknown,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Proxy => "proxy",
            Self::Renewal => "renewal",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RequestOutcome {
    Success,
    ClientCancelled,
    Error,
    Timeout,
}

impl RequestOutcome {
    fn from_terminal(reason: &TerminationReason, client_status: u16) -> Self {
        if client_status == 499 {
            Self::ClientCancelled
        } else if matches!(client_status, 408 | 504) {
            Self::Timeout
        } else if reason == &TerminationReason::Success {
            Self::Success
        } else {
            Self::Error
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::ClientCancelled => "client_cancelled",
            Self::Error => "error",
            Self::Timeout => "timeout",
        }
    }
}

#[derive(Debug, Default)]
struct RequestTiming {
    source_kind: SourceKind,
    request_body_read_ms: Option<u64>,
    request_body_bytes: Option<u64>,
    io_timings: RequestIoTimings,
    proxy_setup_ms: Option<u64>,
    shape_ms: Option<u64>,
    sign_ms: Option<u64>,
    upstream_ttfb_ms: Option<u64>,
    bulkhead_wait_ms: Option<u64>,
    dns_ms: Option<u64>,
    connect_ms: Option<u64>,
    first_body_chunk_ms: Option<u64>,
    first_content_delta_ms: Option<u64>,
    stream_total_ms: Option<u64>,
    upstream_body_ms: Option<u64>,
    finalize_ms: Option<u64>,
    max_attempt_num: Option<u32>,
    route_decision: Option<&'static str>,
    limit_decision: Option<&'static str>,
}

impl RequestTiming {
    fn response_body_ms(&self, outcome: RequestOutcome) -> Option<u64> {
        if outcome == RequestOutcome::ClientCancelled {
            self.upstream_body_ms
        } else {
            self.stream_total_ms.or(self.upstream_body_ms)
        }
    }

    fn proxy_accounted_ms(&self, response_body_ms: Option<u64>) -> u64 {
        [
            self.request_body_read_ms,
            self.proxy_setup_ms,
            self.shape_ms,
            self.sign_ms,
            self.upstream_ttfb_ms,
            response_body_ms,
            self.finalize_ms,
            retry_overhead_ms_for_accounting(self.io_timings.retry_overhead_ms),
        ]
        .into_iter()
        .flatten()
        .fold(0_u64, u64::saturating_add)
    }

    fn has_complete_proxy_timing(&self, response_body_ms: Option<u64>) -> bool {
        self.request_body_read_ms.is_some()
            && self.proxy_setup_ms.is_some()
            && self.shape_ms.is_some()
            && self.sign_ms.is_some()
            && self.upstream_ttfb_ms.is_some()
            && response_body_ms.is_some()
            && self.finalize_ms.is_some()
    }
}

fn retry_overhead_ms_for_accounting(retry_overhead_ms: Option<f64>) -> Option<u64> {
    retry_overhead_ms
        .filter(|value| value.is_finite() && *value >= 0.0)
        .map(|value| value as u64)
}

struct ActiveRequestTiming {
    sequence: u64,
    timing: RequestTiming,
}

#[derive(Default)]
struct RequestTimingAggregator {
    active: HashMap<Arc<str>, ActiveRequestTiming>,
    active_order: BTreeMap<u64, Arc<str>>,
    next_sequence: u64,
}

impl RequestTimingAggregator {
    fn observe(&mut self, event: &LifecycleEvent) {
        match event {
            LifecycleEvent::RequestStarted {
                event_id,
                source_kind,
                ..
            } => {
                let source_kind = SourceKind::from_event(source_kind.as_deref());
                if let Some(active) = self.active.get_mut(event_id.as_str()) {
                    active.timing.source_kind = source_kind;
                    return;
                }
                if self.active.len() >= MAX_ACTIVE_REQUEST_TIMINGS {
                    self.evict_oldest();
                }

                let sequence = self.next_sequence;
                self.next_sequence = self
                    .next_sequence
                    .checked_add(1)
                    .expect("request timing sequence exhausted");
                let event_id = Arc::<str>::from(event_id.as_str());
                self.active_order.insert(sequence, Arc::clone(&event_id));
                self.active.insert(
                    event_id,
                    ActiveRequestTiming {
                        sequence,
                        timing: RequestTiming {
                            source_kind,
                            ..RequestTiming::default()
                        },
                    },
                );
            }
            LifecycleEvent::RouteCompleted {
                event_id, result, ..
            } => {
                let Some(active) = self.active.get_mut(event_id.as_str()) else {
                    return;
                };
                active.timing.route_decision = Some(match result {
                    Ok(_) => "success",
                    Err(RouteFailure::RouterPipelineUnavailable) => "router_pipeline_unavailable",
                    Err(RouteFailure::RouteNoUpstreamAfterFilter) => {
                        "route_no_upstream_after_filter"
                    }
                    Err(RouteFailure::RouteNotConfigured) => "route_not_configured",
                    Err(_) => "unknown",
                });
            }
            LifecycleEvent::LimitDecision {
                event_id, decision, ..
            } => {
                let Some(active) = self.active.get_mut(event_id.as_str()) else {
                    return;
                };
                active.timing.limit_decision = Some(match decision {
                    LimitDecisionKind::Reserved { .. } => "reserved",
                    LimitDecisionKind::Rejected { .. } => "rejected",
                    _ => "unknown",
                });
            }
            LifecycleEvent::UpstreamAttempt {
                event_id,
                attempt_num,
                ..
            } => {
                let Some(active) = self.active.get_mut(event_id.as_str()) else {
                    return;
                };
                active.timing.max_attempt_num = Some(
                    active
                        .timing
                        .max_attempt_num
                        .unwrap_or_default()
                        .max(*attempt_num),
                );
                active.timing.shape_ms = None;
                active.timing.sign_ms = None;
                active.timing.upstream_ttfb_ms = None;
                active.timing.bulkhead_wait_ms = None;
                active.timing.dns_ms = None;
                active.timing.connect_ms = None;
            }
            LifecycleEvent::UpstreamResponseStarted {
                event_id,
                bulkhead_wait_ms,
                dns_ms,
                connect_ms,
                shape_ms,
                sign_ms,
                upstream_ttfb_ms,
                ..
            } => {
                let Some(active) = self.active.get_mut(event_id.as_str()) else {
                    return;
                };
                active.timing.bulkhead_wait_ms = *bulkhead_wait_ms;
                active.timing.dns_ms = *dns_ms;
                active.timing.connect_ms = *connect_ms;
                active.timing.shape_ms = *shape_ms;
                active.timing.sign_ms = *sign_ms;
                active.timing.upstream_ttfb_ms = *upstream_ttfb_ms;
            }
            LifecycleEvent::StreamCompleted { event_id, result } => {
                let Some(active) = self.active.get_mut(event_id.as_str()) else {
                    return;
                };
                if let Ok(success) = result {
                    active.timing.stream_total_ms = success.stream_total_ms;
                    active.timing.first_content_delta_ms = success.stream_first_content_delta_ms;
                }
            }
            LifecycleEvent::RequestTerminated {
                event_id,
                reason,
                client_status,
                duration_ms,
                request_body_read_ms,
                request_body_bytes,
                proxy_setup_ms,
                upstream_body_ms,
                first_content_delta_ms,
                first_body_chunk_ms,
                finalize_ms,
                dns_ms,
                connect_ms,
                io_timings,
                ..
            } => {
                let Some(active) = self.active.remove(event_id.as_str()) else {
                    return;
                };
                self.active_order.remove(&active.sequence);

                let mut timing = active.timing;
                timing.request_body_read_ms = *request_body_read_ms;
                timing.request_body_bytes = *request_body_bytes;
                timing.proxy_setup_ms = *proxy_setup_ms;
                timing.upstream_body_ms = *upstream_body_ms;
                timing.first_content_delta_ms =
                    (*first_content_delta_ms).or(timing.first_content_delta_ms);
                timing.first_body_chunk_ms = *first_body_chunk_ms;
                timing.finalize_ms = *finalize_ms;
                timing.dns_ms = (*dns_ms).or(timing.dns_ms);
                timing.connect_ms = (*connect_ms).or(timing.connect_ms);
                timing.io_timings = *io_timings;
                emit_terminal_metrics(
                    &timing,
                    RequestOutcome::from_terminal(reason, *client_status),
                    *client_status,
                    *duration_ms,
                );
            }
            _ => {}
        }
    }

    fn evict_oldest(&mut self) {
        let Some((_, event_id)) = self.active_order.pop_first() else {
            return;
        };
        if self.active.remove(event_id.as_ref()).is_some() {
            metrics::counter!("cc_lb_request_timing_aggregator_evictions_total").increment(1);
        }
    }

    fn clear(&mut self) {
        self.active.clear();
        self.active_order.clear();
    }
}
fn emit_terminal_metrics(
    timing: &RequestTiming,
    outcome: RequestOutcome,
    client_status: u16,
    duration_ms: u64,
) {
    let source_kind = timing.source_kind.as_str();
    let outcome_label = outcome.as_str();
    let status_class = client_status_class(client_status);

    metrics::counter!(
        "cc_lb_requests_completed_total",
        "source_kind" => source_kind,
        "terminal_outcome" => outcome_label,
        "client_status_class" => status_class
    )
    .increment(1);
    metrics::histogram!(
        "cc_lb_request_completion_duration_seconds",
        "source_kind" => source_kind,
        "terminal_outcome" => outcome_label
    )
    .record(milliseconds_to_seconds(duration_ms));

    if let Some(outcome) = timing.route_decision {
        metrics::counter!(
            "cc_lb_request_decisions_total",
            "stage" => "route",
            "outcome" => outcome
        )
        .increment(1);
    }
    if let Some(outcome) = timing.limit_decision {
        metrics::counter!(
            "cc_lb_request_decisions_total",
            "stage" => "limit",
            "outcome" => outcome
        )
        .increment(1);
    }
    metrics::histogram!(
        "cc_lb_request_retry_attempts",
        "source_kind" => source_kind,
        "terminal_outcome" => outcome_label
    )
    .record(timing.max_attempt_num.map_or(0.0, |max_attempt_num| {
        max_attempt_num.saturating_sub(1) as f64
    }));

    if timing.source_kind == SourceKind::Renewal {
        record_stage(source_kind, outcome_label, "renewal_cycle", duration_ms);
        return;
    }

    if let Some(body_size) = timing.request_body_bytes {
        metrics::histogram!(
            "cc_lb_request_body_size_bytes",
            "source_kind" => source_kind,
            "outcome" => outcome_label
        )
        .record(body_size as f64);
    }

    let response_body_ms = timing.response_body_ms(outcome);
    record_optional_stage(
        source_kind,
        outcome_label,
        "request_body_read",
        timing.request_body_read_ms,
    );
    record_optional_stage(
        source_kind,
        outcome_label,
        "proxy_setup",
        timing.proxy_setup_ms,
    );
    record_optional_stage(source_kind, outcome_label, "shape", timing.shape_ms);
    record_optional_stage(source_kind, outcome_label, "sign", timing.sign_ms);
    record_optional_stage(
        source_kind,
        outcome_label,
        "upstream_ttfb",
        timing.upstream_ttfb_ms,
    );
    record_optional_stage(
        source_kind,
        outcome_label,
        "bulkhead_wait",
        timing.bulkhead_wait_ms,
    );
    record_optional_stage(source_kind, outcome_label, "dns", timing.dns_ms);
    record_optional_stage(source_kind, outcome_label, "connect", timing.connect_ms);
    record_optional_stage(
        source_kind,
        outcome_label,
        "first_body_chunk",
        timing.first_body_chunk_ms,
    );
    record_optional_stage(
        source_kind,
        outcome_label,
        "first_content_delta",
        timing.first_content_delta_ms,
    );
    record_optional_stage(
        source_kind,
        outcome_label,
        "response_body",
        response_body_ms,
    );
    record_optional_stage(source_kind, outcome_label, "finalize", timing.finalize_ms);
    record_optional_precise_stage(
        source_kind,
        outcome_label,
        "request_body_first_chunk_marker",
        timing.io_timings.request_body_first_chunk_ms,
    );
    record_optional_precise_stage(
        source_kind,
        outcome_label,
        "request_body_receive_marker",
        timing.io_timings.request_body_receive_ms,
    );
    record_optional_precise_stage(
        source_kind,
        outcome_label,
        "request_body_wait_mixed",
        timing.io_timings.request_body_wait_ms,
    );
    record_optional_precise_stage(
        source_kind,
        outcome_label,
        "request_body_process",
        timing.io_timings.request_body_process_ms,
    );
    record_optional_precise_stage(
        source_kind,
        outcome_label,
        "response_body_wait_mixed",
        timing.io_timings.response_body_wait_ms,
    );
    record_optional_precise_stage(
        source_kind,
        outcome_label,
        "response_body_process",
        timing.io_timings.response_body_process_ms,
    );
    record_optional_precise_stage(
        source_kind,
        outcome_label,
        "downstream_poll_gap_mixed",
        timing.io_timings.response_body_downstream_poll_gap_ms,
    );
    record_optional_precise_stage(
        source_kind,
        outcome_label,
        "retry_overhead_mixed",
        timing.io_timings.retry_overhead_ms,
    );

    if timing.source_kind != SourceKind::Proxy
        || !timing.has_complete_proxy_timing(response_body_ms)
    {
        return;
    }

    let unaccounted_ms = duration_ms.saturating_sub(timing.proxy_accounted_ms(response_body_ms));
    metrics::histogram!(
        "cc_lb_request_unaccounted_duration_seconds",
        "source_kind" => source_kind,
        "outcome" => outcome_label
    )
    .record(milliseconds_to_seconds(unaccounted_ms));

    if outcome == RequestOutcome::Success && unaccounted_ms > UNACCOUNTED_BUDGET_MS {
        metrics::counter!(
            "cc_lb_request_unaccounted_over_budget_total",
            "source_kind" => source_kind,
            "outcome" => outcome_label
        )
        .increment(1);
    }
}

fn client_status_class(status: u16) -> &'static str {
    match status {
        100..=199 => "1xx",
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "other",
    }
}

fn record_optional_stage(
    source_kind: &'static str,
    outcome: &'static str,
    stage: &'static str,
    duration_ms: Option<u64>,
) {
    if let Some(duration_ms) = duration_ms {
        record_stage(source_kind, outcome, stage, duration_ms);
    }
}
fn record_optional_precise_stage(
    source_kind: &'static str,
    outcome: &'static str,
    stage: &'static str,
    duration_ms: Option<f64>,
) {
    let Some(duration_ms) = duration_ms.filter(|value| value.is_finite() && *value >= 0.0) else {
        return;
    };
    metrics::histogram!(
        "cc_lb_request_stage_duration_seconds",
        "source_kind" => source_kind,
        "stage" => stage,
        "outcome" => outcome
    )
    .record(duration_ms / 1_000.0);
}

fn record_stage(
    source_kind: &'static str,
    outcome: &'static str,
    stage: &'static str,
    duration_ms: u64,
) {
    metrics::histogram!(
        "cc_lb_request_stage_duration_seconds",
        "source_kind" => source_kind,
        "stage" => stage,
        "outcome" => outcome
    )
    .record(milliseconds_to_seconds(duration_ms));
}

fn milliseconds_to_seconds(duration_ms: u64) -> f64 {
    duration_ms as f64 / 1_000.0
}
/// Handle to a spawned lifecycle-event-logger task.
pub struct LifecycleEventLoggerHandle {
    shutdown_tx: oneshot::Sender<()>,
    join: JoinHandle<()>,
}

impl LifecycleEventLoggerHandle {
    /// Signal the logger to drain and exit.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        if let Err(error) = self.join.await {
            tracing::warn!(%error, "lifecycle event logger task panicked");
        }
    }
}

/// Spawn the subscriber that counts lifecycle events and aggregates request timings.
///
/// `rx` is obtained from
/// [`InMemoryBus::attach_lifecycle_writer`](cc_lb_control::event_bus::InMemoryBus::attach_lifecycle_writer).
pub fn spawn_lifecycle_event_logger(
    rx: mpsc::Receiver<LifecycleEvent>,
) -> LifecycleEventLoggerHandle {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let join = tokio::spawn(logger_loop(rx, shutdown_rx));
    LifecycleEventLoggerHandle { shutdown_tx, join }
}

async fn logger_loop(mut rx: mpsc::Receiver<LifecycleEvent>, mut shutdown: oneshot::Receiver<()>) {
    let mut timings = RequestTimingAggregator::default();
    loop {
        tokio::select! {
            biased;
            event = rx.recv() => {
                match event {
                    Some(event) => record(&event, &mut timings),
                    None => break,
                }
            }
            _ = &mut shutdown => break,
        }
    }
    while let Ok(event) = rx.try_recv() {
        record(&event, &mut timings);
    }
    timings.clear();
}

fn record(event: &LifecycleEvent, timings: &mut RequestTimingAggregator) {
    metrics::counter!("cc_lb_lifecycle_events_total", "kind" => event.kind()).increment(1);
    timings.observe(event);
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use cc_lb_lifecycle::{StreamSuccess, TerminationReason};
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    use super::*;

    #[derive(Debug, PartialEq)]
    enum MetricValue {
        Counter(u64),
        Gauge(f64),
        Histogram(Vec<f64>),
    }

    #[derive(Debug, PartialEq)]
    struct MetricSample {
        name: String,
        labels: BTreeMap<String, String>,
        value: MetricValue,
    }

    #[derive(Clone, Copy, Default)]
    struct TerminalTiming {
        duration_ms: u64,
        request_body_read_ms: Option<u64>,
        request_body_bytes: Option<u64>,
        proxy_setup_ms: Option<u64>,
        upstream_body_ms: Option<u64>,
        first_content_delta_ms: Option<u64>,
        finalize_ms: Option<u64>,
        io_timings: RequestIoTimings,
    }

    fn sample_event(request_id: &str) -> LifecycleEvent {
        started(&format!("evt-{request_id}"), None)
    }

    fn started(event_id: &str, source_kind: Option<&str>) -> LifecycleEvent {
        LifecycleEvent::RequestStarted {
            event_id: event_id.to_owned(),
            request_id: format!("request-{event_id}"),
            ts_ms: 0,
            stream: false,
            source_kind: source_kind.map(str::to_owned),
            source_ref_id: None,
            event_kind: None,
        }
    }

    fn upstream_started(
        event_id: &str,
        shape_ms: Option<u64>,
        sign_ms: Option<u64>,
        upstream_ttfb_ms: Option<u64>,
    ) -> LifecycleEvent {
        LifecycleEvent::UpstreamResponseStarted {
            event_id: event_id.to_owned(),
            status: 200,
            headers: Default::default(),
            bulkhead_wait_ms: None,
            dns_ms: None,
            connect_ms: None,
            connection_reused: None,
            shape_ms,
            sign_ms,
            upstream_ttfb_ms,
        }
    }
    fn upstream_started_with_attempt_stages(
        event_id: &str,
        bulkhead_wait_ms: Option<u64>,
        dns_ms: Option<u64>,
        connect_ms: Option<u64>,
        upstream_ttfb_ms: Option<u64>,
    ) -> LifecycleEvent {
        LifecycleEvent::UpstreamResponseStarted {
            event_id: event_id.to_owned(),
            status: 200,
            headers: Default::default(),
            bulkhead_wait_ms,
            dns_ms,
            connect_ms,
            connection_reused: None,
            shape_ms: None,
            sign_ms: None,
            upstream_ttfb_ms,
        }
    }

    fn stream_completed_with_first_delta(
        event_id: &str,
        stream_total_ms: u64,
        first_content_delta_ms: u64,
    ) -> LifecycleEvent {
        LifecycleEvent::StreamCompleted {
            event_id: event_id.to_owned(),
            result: Ok(StreamSuccess {
                stream_total_ms: Some(stream_total_ms),
                stream_first_content_delta_ms: Some(first_content_delta_ms),
                ..Default::default()
            }),
        }
    }

    fn upstream_attempt(event_id: &str, attempt_num: u32) -> LifecycleEvent {
        LifecycleEvent::UpstreamAttempt {
            event_id: event_id.to_owned(),
            attempt_num,
            upstream_id: uuid::Uuid::nil(),
        }
    }

    fn stream_completed(event_id: &str, stream_total_ms: u64) -> LifecycleEvent {
        LifecycleEvent::StreamCompleted {
            event_id: event_id.to_owned(),
            result: Ok(StreamSuccess {
                stream_total_ms: Some(stream_total_ms),
                ..Default::default()
            }),
        }
    }

    fn terminated(
        event_id: &str,
        reason: TerminationReason,
        client_status: u16,
        timing: TerminalTiming,
    ) -> LifecycleEvent {
        LifecycleEvent::RequestTerminated {
            event_id: event_id.to_owned(),
            reason,
            client_status,
            duration_ms: timing.duration_ms,
            request_body_read_ms: timing.request_body_read_ms,
            request_body_bytes: timing.request_body_bytes,
            proxy_setup_ms: timing.proxy_setup_ms,
            setup_timings: Default::default(),
            io_timings: timing.io_timings,
            upstream_body_ms: timing.upstream_body_ms,
            first_content_delta_ms: timing.first_content_delta_ms,
            first_body_chunk_ms: None,
            finalize_ms: timing.finalize_ms,
            dns_ms: None,
            connect_ms: None,
            connection_reused: None,
            internal_errors: Vec::new(),
            event_kind: None,
        }
    }

    fn complete_proxy_events(
        event_id: &str,
        reason: TerminationReason,
        client_status: u16,
        duration_ms: u64,
    ) -> Vec<LifecycleEvent> {
        vec![
            started(event_id, Some("proxy")),
            upstream_started(event_id, Some(5), Some(6), Some(7)),
            stream_completed(event_id, 8),
            terminated(
                event_id,
                reason,
                client_status,
                TerminalTiming {
                    duration_ms,
                    request_body_read_ms: Some(3),
                    request_body_bytes: Some(1_024),
                    proxy_setup_ms: Some(4),
                    upstream_body_ms: Some(8),
                    first_content_delta_ms: None,
                    finalize_ms: Some(9),
                    io_timings: RequestIoTimings::default(),
                },
            ),
        ]
    }

    fn capture_metrics(
        events: impl IntoIterator<Item = LifecycleEvent>,
    ) -> (RequestTimingAggregator, Vec<MetricSample>) {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let mut timings = RequestTimingAggregator::default();
        metrics::with_local_recorder(&recorder, || {
            for event in events {
                record(&event, &mut timings);
            }
        });

        let samples = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .map(|(key, _, _, value)| {
                let labels = key
                    .key()
                    .labels()
                    .map(|label| (label.key().to_owned(), label.value().to_owned()))
                    .collect();
                let value = match value {
                    DebugValue::Counter(value) => MetricValue::Counter(value),
                    DebugValue::Gauge(value) => MetricValue::Gauge(value.into_inner()),
                    DebugValue::Histogram(values) => MetricValue::Histogram(
                        values.into_iter().map(|value| value.into_inner()).collect(),
                    ),
                };
                MetricSample {
                    name: key.key().name().to_owned(),
                    labels,
                    value,
                }
            })
            .collect();
        (timings, samples)
    }

    fn named<'a>(samples: &'a [MetricSample], name: &str) -> Vec<&'a MetricSample> {
        samples
            .iter()
            .filter(|sample| sample.name == name)
            .collect()
    }

    fn assert_labels(sample: &MetricSample, expected: &[(&str, &str)]) {
        assert_eq!(
            sample.labels.len(),
            expected.len(),
            "unexpected labels on {sample:?}"
        );
        for (key, value) in expected {
            assert_eq!(
                sample.labels.get(*key).map(String::as_str),
                Some(*value),
                "unexpected {key} label on {sample:?}"
            );
        }
    }

    fn assert_histogram(sample: &MetricSample, expected_ms: &[u64]) {
        let MetricValue::Histogram(values) = &sample.value else {
            panic!("expected histogram sample, got {sample:?}");
        };
        let expected = expected_ms
            .iter()
            .copied()
            .map(milliseconds_to_seconds)
            .collect::<Vec<_>>();
        assert_eq!(values, &expected);
    }

    #[tokio::test]
    async fn logger_drains_remaining_events_on_shutdown() {
        let (tx, rx) = mpsc::channel::<LifecycleEvent>(8);
        let handle = spawn_lifecycle_event_logger(rx);

        for i in 0..4 {
            tx.send(sample_event(&format!("req-{i}")))
                .await
                .expect("send");
        }
        drop(tx);
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn logger_exits_when_channel_closes() {
        let (tx, rx) = mpsc::channel::<LifecycleEvent>(8);
        let handle = spawn_lifecycle_event_logger(rx);
        drop(tx);
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn logger_increments_metrics_per_event() {
        let (tx, rx) = mpsc::channel::<LifecycleEvent>(8);
        let handle = spawn_lifecycle_event_logger(rx);

        tx.send(sample_event("metric-test")).await.expect("send");
        drop(tx);
        handle.shutdown().await;
    }

    #[test]
    fn capacity_evicts_oldest_active_and_preserves_newer_terminal_metrics() {
        let mut events = (0..MAX_ACTIVE_REQUEST_TIMINGS)
            .map(|index| started(&format!("active-{index}"), Some("proxy")))
            .collect::<Vec<_>>();
        events.push(started("overflow", Some("proxy")));
        events.extend(
            complete_proxy_events("active-1", TerminationReason::Success, 200, 50)
                .into_iter()
                .skip(1),
        );

        let (timings, samples) = capture_metrics(events);

        assert!(
            !timings.active.contains_key("active-0"),
            "the oldest active timing must be evicted"
        );
        assert!(
            timings.active.contains_key("overflow"),
            "the newly started timing must remain active"
        );
        assert!(
            !timings.active.contains_key("active-1"),
            "a newer terminal timing must be cleaned normally"
        );
        assert_eq!(timings.active.len(), MAX_ACTIVE_REQUEST_TIMINGS - 1);
        assert_eq!(timings.active_order.len(), timings.active.len());

        let evictions = named(&samples, "cc_lb_request_timing_aggregator_evictions_total");
        assert_eq!(evictions.len(), 1);
        assert_labels(evictions[0], &[]);
        assert_eq!(evictions[0].value, MetricValue::Counter(1));

        let unaccounted = named(&samples, "cc_lb_request_unaccounted_duration_seconds");
        assert_eq!(unaccounted.len(), 1);
        assert_labels(
            unaccounted[0],
            &[("source_kind", "proxy"), ("outcome", "success")],
        );
        assert_histogram(unaccounted[0], &[8]);
    }

    #[test]
    fn terminal_cleanup_keeps_active_order_bounded() {
        let mut timings = RequestTimingAggregator::default();
        let mut max_ordering_entries = 0;

        for index in 0..=MAX_ACTIVE_REQUEST_TIMINGS {
            let event_id = format!("completed-{index}");
            timings.observe(&started(&event_id, None));
            max_ordering_entries = max_ordering_entries.max(timings.active_order.len());
            timings.observe(&terminated(
                &event_id,
                TerminationReason::Success,
                200,
                TerminalTiming::default(),
            ));
            assert_eq!(timings.active_order.len(), timings.active.len());
        }

        assert_eq!(max_ordering_entries, 1);
        assert!(timings.active.is_empty());
        assert!(timings.active_order.is_empty());
    }

    #[test]
    fn normal_proxy_records_each_parent_stage_once_and_ignores_duplicate_terminal() {
        let event_id = "normal-proxy";
        let terminal = terminated(
            event_id,
            TerminationReason::Success,
            200,
            TerminalTiming {
                duration_ms: 50,
                request_body_read_ms: Some(3),
                request_body_bytes: Some(1_024),
                proxy_setup_ms: Some(4),
                upstream_body_ms: Some(8),
                first_content_delta_ms: None,
                finalize_ms: Some(9),
                io_timings: RequestIoTimings::default(),
            },
        );
        let (timings, samples) = capture_metrics([
            started(event_id, Some("proxy")),
            upstream_started(event_id, Some(5), Some(6), Some(7)),
            stream_completed(event_id, 8),
            terminal.clone(),
            terminal,
        ]);

        assert!(
            timings.active.is_empty(),
            "terminal must clean request state"
        );
        let stage_samples = named(&samples, "cc_lb_request_stage_duration_seconds");
        let expected_stages = [
            ("request_body_read", 3),
            ("proxy_setup", 4),
            ("shape", 5),
            ("sign", 6),
            ("upstream_ttfb", 7),
            ("response_body", 8),
            ("finalize", 9),
        ];
        assert_eq!(stage_samples.len(), expected_stages.len());
        for (stage, expected_ms) in expected_stages {
            let matching = stage_samples
                .iter()
                .copied()
                .filter(|sample| sample.labels.get("stage").map(String::as_str) == Some(stage))
                .collect::<Vec<_>>();
            assert_eq!(matching.len(), 1, "{stage} must be observed exactly once");
            assert_labels(
                matching[0],
                &[
                    ("source_kind", "proxy"),
                    ("stage", stage),
                    ("outcome", "success"),
                ],
            );
            assert_histogram(matching[0], &[expected_ms]);
        }

        let body_size = named(&samples, "cc_lb_request_body_size_bytes");
        assert_eq!(body_size.len(), 1);
        assert_labels(
            body_size[0],
            &[("source_kind", "proxy"), ("outcome", "success")],
        );
        assert_eq!(body_size[0].value, MetricValue::Histogram(vec![1_024.0]));

        let unaccounted = named(&samples, "cc_lb_request_unaccounted_duration_seconds");
        assert_eq!(unaccounted.len(), 1);
        assert_labels(
            unaccounted[0],
            &[("source_kind", "proxy"), ("outcome", "success")],
        );
        assert_histogram(unaccounted[0], &[8]);
        assert!(
            named(&samples, "cc_lb_request_unaccounted_over_budget_total").is_empty(),
            "an 8ms residual must remain within budget"
        );
    }
    #[test]
    fn terminal_first_content_delta_is_recorded_for_non_success_outcomes() {
        let (_, samples) = capture_metrics([
            started("cancel-delta", Some("proxy")),
            terminated(
                "cancel-delta",
                TerminationReason::Dropped,
                499,
                TerminalTiming {
                    duration_ms: 17,
                    first_content_delta_ms: Some(17),
                    ..Default::default()
                },
            ),
            started("error-delta", Some("proxy")),
            terminated(
                "error-delta",
                TerminationReason::ErrorCode("upstream_error".to_owned()),
                500,
                TerminalTiming {
                    duration_ms: 19,
                    first_content_delta_ms: Some(19),
                    ..Default::default()
                },
            ),
        ]);

        let deltas = named(&samples, "cc_lb_request_stage_duration_seconds")
            .into_iter()
            .filter(|sample| {
                sample.labels.get("stage").map(String::as_str) == Some("first_content_delta")
            })
            .collect::<Vec<_>>();
        assert_eq!(deltas.len(), 2);
        for (outcome, expected_ms) in [("client_cancelled", 17), ("error", 19)] {
            let sample = deltas
                .iter()
                .find(|sample| sample.labels.get("outcome").map(String::as_str) == Some(outcome))
                .unwrap_or_else(|| panic!("missing first-content sample for {outcome}"));
            assert_histogram(sample, &[expected_ms]);
        }
    }
    #[test]
    fn io_children_stay_excluded_while_retry_parent_closes_residual() {
        let event_id = "io-timing-stages";
        let mut io_timings = RequestIoTimings {
            request_body_first_chunk_ms: Some(0.125),
            request_body_receive_ms: Some(2.5),
            request_body_wait_ms: Some(7.75),
            request_body_process_ms: Some(0.0),
            request_body_chunk_count: Some(0),
            response_body_wait_ms: Some(18.25),
            response_body_process_ms: Some(1.5),
            response_body_downstream_poll_gap_ms: Some(4.125),
            retry_overhead_ms: None,
        };
        let parent_timing = RequestTiming {
            request_body_read_ms: Some(3),
            proxy_setup_ms: Some(4),
            shape_ms: Some(5),
            sign_ms: Some(6),
            upstream_ttfb_ms: Some(7),
            upstream_body_ms: Some(8),
            finalize_ms: Some(9),
            io_timings,
            ..RequestTiming::default()
        };
        assert_eq!(parent_timing.proxy_accounted_ms(Some(8)), 42);

        io_timings.retry_overhead_ms = Some(8.625);
        let retry_timing = RequestTiming {
            io_timings,
            ..parent_timing
        };
        assert_eq!(retry_timing.proxy_accounted_ms(Some(8)), 50);

        let (_, samples) = capture_metrics([
            started(event_id, Some("proxy")),
            upstream_started(event_id, Some(5), Some(6), Some(7)),
            stream_completed(event_id, 8),
            terminated(
                event_id,
                TerminationReason::Success,
                200,
                TerminalTiming {
                    duration_ms: 50,
                    request_body_read_ms: Some(3),
                    request_body_bytes: Some(1_024),
                    proxy_setup_ms: Some(4),
                    upstream_body_ms: Some(8),
                    first_content_delta_ms: None,
                    finalize_ms: Some(9),
                    io_timings,
                },
            ),
        ]);

        let stage_samples = named(&samples, "cc_lb_request_stage_duration_seconds");
        assert_eq!(stage_samples.len(), 15);
        let expected = [
            ("request_body_first_chunk_marker", 0.125),
            ("request_body_receive_marker", 2.5),
            ("request_body_wait_mixed", 7.75),
            ("request_body_process", 0.0),
            ("response_body_wait_mixed", 18.25),
            ("response_body_process", 1.5),
            ("downstream_poll_gap_mixed", 4.125),
            ("retry_overhead_mixed", 8.625),
        ];
        for (stage, expected_ms) in expected {
            let matching = stage_samples
                .iter()
                .copied()
                .filter(|sample| sample.labels.get("stage").map(String::as_str) == Some(stage))
                .collect::<Vec<_>>();
            assert_eq!(matching.len(), 1, "{stage} must be observed exactly once");
            assert_labels(
                matching[0],
                &[
                    ("source_kind", "proxy"),
                    ("stage", stage),
                    ("outcome", "success"),
                ],
            );
            assert_eq!(
                matching[0].value,
                MetricValue::Histogram(vec![expected_ms / 1_000.0])
            );
        }

        let unaccounted = named(&samples, "cc_lb_request_unaccounted_duration_seconds");
        assert_eq!(unaccounted.len(), 1);
        assert_histogram(unaccounted[0], &[0]);
    }
    #[test]
    fn retry_attempt_clears_stale_stage_metrics_before_dispatch_failure() {
        let event_id = "retry-pre-dispatch-failure";
        let (_, samples) = capture_metrics([
            started(event_id, Some("proxy")),
            upstream_started(event_id, Some(5), Some(6), Some(7)),
            upstream_attempt(event_id, 2),
            terminated(
                event_id,
                TerminationReason::ErrorCode("upstream_dispatch".to_owned()),
                500,
                TerminalTiming {
                    duration_ms: 50,
                    request_body_read_ms: Some(3),
                    request_body_bytes: Some(1_024),
                    proxy_setup_ms: Some(4),
                    upstream_body_ms: None,
                    first_content_delta_ms: None,
                    finalize_ms: None,
                    io_timings: RequestIoTimings {
                        retry_overhead_ms: Some(8.625),
                        ..RequestIoTimings::default()
                    },
                },
            ),
        ]);

        let stage_samples = named(&samples, "cc_lb_request_stage_duration_seconds");
        for stage in ["shape", "sign", "upstream_ttfb"] {
            assert!(
                stage_samples
                    .iter()
                    .all(|sample| sample.labels.get("stage").map(String::as_str) != Some(stage)),
                "{stage} from the prior attempt must not be emitted"
            );
        }
        let retry = stage_samples
            .iter()
            .filter(|sample| {
                sample.labels.get("stage").map(String::as_str) == Some("retry_overhead_mixed")
            })
            .collect::<Vec<_>>();
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0].value, MetricValue::Histogram(vec![0.008_625]));
    }

    #[test]
    fn client_cancelled_499_uses_terminal_partial_body_and_fixed_outcome() {
        let event_id = "cancelled";
        let (_, samples) = capture_metrics([
            started(event_id, Some("proxy")),
            upstream_started(event_id, Some(5), Some(6), Some(7)),
            stream_completed(event_id, 80),
            terminated(
                event_id,
                TerminationReason::Dropped,
                499,
                TerminalTiming {
                    duration_ms: 80,
                    request_body_read_ms: Some(3),
                    request_body_bytes: Some(1_024),
                    proxy_setup_ms: Some(4),
                    upstream_body_ms: Some(25),
                    first_content_delta_ms: None,
                    finalize_ms: Some(9),
                    io_timings: RequestIoTimings::default(),
                },
            ),
        ]);

        let response_body = named(&samples, "cc_lb_request_stage_duration_seconds")
            .into_iter()
            .filter(|sample| {
                sample.labels.get("stage").map(String::as_str) == Some("response_body")
            })
            .collect::<Vec<_>>();
        assert_eq!(response_body.len(), 1);
        assert_labels(
            response_body[0],
            &[
                ("source_kind", "proxy"),
                ("stage", "response_body"),
                ("outcome", "client_cancelled"),
            ],
        );
        assert_histogram(response_body[0], &[25]);
    }
    #[test]
    fn terminal_metrics_use_final_attempt_and_observed_decisions() {
        let event_id = "terminal-metrics";
        let (_, samples) = capture_metrics([
            started(event_id, Some("proxy")),
            LifecycleEvent::RouteCompleted {
                event_id: event_id.to_owned(),
                result: Err(RouteFailure::RouteNotConfigured),
                routing_trace: None,
            },
            LifecycleEvent::LimitDecision {
                event_id: event_id.to_owned(),
                decision: LimitDecisionKind::Reserved {
                    reservation_id: "reservation".to_owned(),
                    amount: 1,
                    limit_reserve_ms: Some(2),
                },
            },
            upstream_attempt(event_id, 1),
            upstream_started_with_attempt_stages(event_id, Some(1), Some(2), Some(3), Some(20)),
            upstream_attempt(event_id, 3),
            upstream_started_with_attempt_stages(event_id, Some(2), Some(3), Some(5), Some(20)),
            stream_completed_with_first_delta(event_id, 100, 30),
            LifecycleEvent::RequestTerminated {
                event_id: event_id.to_owned(),
                reason: TerminationReason::Success,
                client_status: 200,
                duration_ms: 240,
                request_body_read_ms: None,
                request_body_bytes: None,
                proxy_setup_ms: None,
                setup_timings: Default::default(),
                io_timings: RequestIoTimings::default(),
                upstream_body_ms: Some(100),
                first_content_delta_ms: None,
                dns_ms: Some(3),
                connect_ms: Some(5),
                connection_reused: None,
                first_body_chunk_ms: Some(90),
                finalize_ms: None,
                internal_errors: Vec::new(),
                event_kind: None,
            },
        ]);

        let completed = named(&samples, "cc_lb_requests_completed_total");
        assert_eq!(completed.len(), 1);
        assert_labels(
            completed[0],
            &[
                ("source_kind", "proxy"),
                ("terminal_outcome", "success"),
                ("client_status_class", "2xx"),
            ],
        );
        assert_eq!(completed[0].value, MetricValue::Counter(1));

        let completion = named(&samples, "cc_lb_request_completion_duration_seconds");
        assert_eq!(completion.len(), 1);
        assert_labels(
            completion[0],
            &[("source_kind", "proxy"), ("terminal_outcome", "success")],
        );
        assert_histogram(completion[0], &[240]);

        let retry = named(&samples, "cc_lb_request_retry_attempts");
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0].value, MetricValue::Histogram(vec![2.0]));

        let decisions = named(&samples, "cc_lb_request_decisions_total");
        assert_eq!(decisions.len(), 2);
        for (stage, outcome) in [("route", "route_not_configured"), ("limit", "reserved")] {
            let sample = decisions
                .iter()
                .find(|sample| sample.labels.get("stage").map(String::as_str) == Some(stage))
                .unwrap_or_else(|| panic!("missing observed {stage} decision"));
            assert_labels(sample, &[("stage", stage), ("outcome", outcome)]);
            assert_eq!(sample.value, MetricValue::Counter(1));
        }

        let stages = named(&samples, "cc_lb_request_stage_duration_seconds");
        for (stage, duration_ms) in [
            ("bulkhead_wait", 2),
            ("dns", 3),
            ("connect", 5),
            ("first_body_chunk", 90),
            ("first_content_delta", 30),
        ] {
            let sample = stages
                .iter()
                .find(|sample| sample.labels.get("stage").map(String::as_str) == Some(stage))
                .unwrap_or_else(|| panic!("missing final-attempt stage {stage}"));
            assert_histogram(sample, &[duration_ms]);
        }
    }

    #[test]
    fn terminal_metrics_record_zero_retries_without_upstream_attempt() {
        let event_id = "terminal-metrics-no-attempt";
        let (_, samples) = capture_metrics([
            started(event_id, Some("proxy")),
            terminated(
                event_id,
                TerminationReason::RateLimited,
                429,
                TerminalTiming {
                    duration_ms: 12,
                    ..Default::default()
                },
            ),
        ]);

        let retry = named(&samples, "cc_lb_request_retry_attempts");
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0].value, MetricValue::Histogram(vec![0.0]));
    }

    #[test]
    fn renewal_records_only_the_source_specific_cycle() {
        let event_id = "renewal";
        let (_, samples) = capture_metrics([
            started(event_id, Some("renewal")),
            upstream_started(event_id, Some(5), Some(6), Some(7)),
            stream_completed(event_id, 8),
            terminated(
                event_id,
                TerminationReason::Success,
                200,
                TerminalTiming {
                    duration_ms: 500,
                    request_body_read_ms: Some(3),
                    request_body_bytes: Some(1_024),
                    proxy_setup_ms: Some(4),
                    upstream_body_ms: Some(8),
                    first_content_delta_ms: None,
                    finalize_ms: Some(9),
                    io_timings: RequestIoTimings::default(),
                },
            ),
        ]);

        let request_samples = samples
            .iter()
            .filter(|sample| sample.name == "cc_lb_request_stage_duration_seconds")
            .collect::<Vec<_>>();
        assert_eq!(request_samples.len(), 1);
        assert_eq!(
            request_samples[0].name,
            "cc_lb_request_stage_duration_seconds"
        );
        assert_labels(
            request_samples[0],
            &[
                ("source_kind", "renewal"),
                ("stage", "renewal_cycle"),
                ("outcome", "success"),
            ],
        );
        assert_histogram(request_samples[0], &[500]);
    }

    #[test]
    fn legacy_and_mixed_timings_are_excluded_from_budget_metrics() {
        let legacy_id = "legacy";
        let mixed_id = "mixed";
        let (_, samples) = capture_metrics([
            started(legacy_id, None),
            terminated(
                legacy_id,
                TerminationReason::Success,
                200,
                TerminalTiming {
                    duration_ms: 100,
                    proxy_setup_ms: Some(4),
                    upstream_body_ms: Some(8),
                    ..Default::default()
                },
            ),
            started(mixed_id, Some("proxy")),
            upstream_started(mixed_id, Some(5), Some(6), Some(7)),
            stream_completed(mixed_id, 8),
            terminated(
                mixed_id,
                TerminationReason::Success,
                200,
                TerminalTiming {
                    duration_ms: 100,
                    proxy_setup_ms: Some(4),
                    upstream_body_ms: Some(8),
                    ..Default::default()
                },
            ),
        ]);

        assert!(
            named(&samples, "cc_lb_request_unaccounted_duration_seconds").is_empty(),
            "legacy and mixed requests must not enter the budget denominator"
        );
        assert!(
            named(&samples, "cc_lb_request_unaccounted_over_budget_total").is_empty(),
            "legacy and mixed requests must not increment the budget counter"
        );
        let unknown_stages = named(&samples, "cc_lb_request_stage_duration_seconds")
            .into_iter()
            .filter(|sample| {
                sample.labels.get("source_kind").map(String::as_str) == Some("unknown")
            })
            .collect::<Vec<_>>();
        assert!(
            !unknown_stages.is_empty(),
            "present legacy stages remain observable with the fixed unknown label"
        );
    }

    #[test]
    fn over_budget_counter_requires_complete_success_and_residual_above_ten_ms() {
        let mut events = complete_proxy_events("over-budget", TerminationReason::Success, 200, 53);
        events.extend(complete_proxy_events(
            "at-budget",
            TerminationReason::Success,
            200,
            52,
        ));
        events.extend(complete_proxy_events(
            "failed",
            TerminationReason::ErrorCode("upstream_error".to_owned()),
            500,
            80,
        ));
        let (_, samples) = capture_metrics(events);

        let success_unaccounted = named(&samples, "cc_lb_request_unaccounted_duration_seconds")
            .into_iter()
            .find(|sample| sample.labels.get("outcome").map(String::as_str) == Some("success"))
            .expect("complete successes must enter the budget denominator");
        assert_labels(
            success_unaccounted,
            &[("source_kind", "proxy"), ("outcome", "success")],
        );
        assert_histogram(success_unaccounted, &[11, 10]);

        let over_budget = named(&samples, "cc_lb_request_unaccounted_over_budget_total");
        assert_eq!(over_budget.len(), 1);
        assert_labels(
            over_budget[0],
            &[("source_kind", "proxy"), ("outcome", "success")],
        );
        assert_eq!(over_budget[0].value, MetricValue::Counter(1));
    }
}
