use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinSet;
use tokio::time::{Instant, sleep_until};

use crate::executor_metrics::{ConnectionCounts, ExecutorEvidence, ExecutorTally};
use crate::manifest::PlannedRequest;
use crate::traffic::{ExpectedLabel, ExpectedStatusClass, RequestBodyShape, SlowReaderPolicy};

const DEFAULT_SAFETY_CAP: usize = 10_000;

pub type SendFuture = Pin<Box<dyn Future<Output = RequestObservation> + Send>>;

pub use crate::executor_metrics::RequestObservation;

pub trait RequestSender: Send + Sync {
    fn send(&self, request: PlannedRequest) -> SendFuture;

    fn connection_counts(&self) -> ConnectionCounts {
        ConnectionCounts::default()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ExecutorConfig {
    pub safety_cap: usize,
    pub late_tolerance: Duration,
}

impl ExecutorConfig {
    pub const fn with_late_tolerance(late_tolerance: Duration) -> Self {
        Self {
            safety_cap: DEFAULT_SAFETY_CAP,
            late_tolerance,
        }
    }
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self::with_late_tolerance(Duration::from_millis(25))
    }
}

#[derive(Debug)]
pub struct ExecutorReport {
    pub evidence: ExecutorEvidence,
}

pub async fn execute_open_loop(
    mut requests: Vec<PlannedRequest>,
    sender: Arc<dyn RequestSender>,
    config: ExecutorConfig,
) -> ExecutorReport {
    requests.sort_by_key(|request| request.scheduled_send_at_ms);
    let started_at = Instant::now();
    let mut tally = ExecutorTally::new();
    let mut in_flight = JoinSet::new();

    for request in requests {
        let target = started_at + Duration::from_millis(request.scheduled_send_at_ms);
        sleep_until(target).await;
        let sent_at = Instant::now();
        tally.record_attempt(
            sent_at.duration_since(started_at),
            sent_at.saturating_duration_since(target),
            config.late_tolerance,
        );
        if in_flight.len() >= config.safety_cap {
            tally.record_drop(sent_at.duration_since(started_at));
            continue;
        }
        tally.record_sent(sent_at.duration_since(started_at));
        let expected = request.expected_status_class;
        let sender = Arc::clone(&sender);
        in_flight.spawn(async move { (expected, sender.send(request).await) });
        drain_finished(&mut in_flight, &mut tally, started_at);
    }

    while let Some(joined) = in_flight.join_next().await {
        tally.record_join(joined, Instant::now().duration_since(started_at));
    }
    ExecutorReport {
        evidence: tally.finish(sender.connection_counts()),
    }
}

fn drain_finished(
    in_flight: &mut JoinSet<(ExpectedStatusClass, RequestObservation)>,
    tally: &mut ExecutorTally,
    started_at: Instant,
) {
    while let Some(joined) = in_flight.try_join_next() {
        tally.record_join(joined, Instant::now().duration_since(started_at));
    }
}

pub fn planned_request(
    request_id: &str,
    scheduled_send_at_ms: u64,
    stream: bool,
) -> PlannedRequest {
    PlannedRequest {
        request_id: request_id.to_owned(),
        scheduled_send_at_ms,
        wave_id: "self-check".to_owned(),
        persona_id: "self-check".to_owned(),
        principal_id: "self-check".to_owned(),
        session_id: "self-check".to_owned(),
        body: format!("{{\"stream\":{stream}}}"),
        body_hash: "self-check".to_owned(),
        body_shape: RequestBodyShape::MessageText,
        stream,
        max_tokens: Some(4),
        model: Some("fake-alpha".to_owned()),
        hot_prefix_group: "self-check".to_owned(),
        provider_headers: Default::default(),
        slow_reader_policy: SlowReaderPolicy::Eager,
        fake_script_id: "self-check".to_owned(),
        expected_label: ExpectedLabel::Healthy,
        expected_status_class: ExpectedStatusClass::Success2xx,
    }
}
