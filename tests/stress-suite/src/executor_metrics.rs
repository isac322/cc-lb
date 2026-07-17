use std::collections::BTreeMap;
use std::time::Duration;

use tokio::task::JoinError;

pub use crate::executor_evidence::{
    ConnectionCounts, ExecutionTerminal, ExecutorEvidence, RequestObservation, classify_terminal,
};
use crate::executor_evidence::{Counters, TimelinePoint, TimingSamples, percentile, percentiles};
use crate::sse_timing::SseSummary;
use crate::traffic::ExpectedStatusClass;
use crate::verdict::Verdict;

pub struct ExecutorTally {
    counters: Counters,
    status_counts: BTreeMap<u16, u64>,
    timeline: BTreeMap<u64, TimelinePoint>,
    ttfb: Vec<u64>,
    ttft: Vec<u64>,
    first_delta: Vec<u64>,
    gaps: Vec<u64>,
    latency: Vec<u64>,
    drift: Vec<u64>,
    truncations: u64,
}

impl ExecutorTally {
    pub fn new() -> Self {
        Self {
            counters: Counters::default(),
            status_counts: BTreeMap::new(),
            timeline: BTreeMap::new(),
            ttfb: Vec::new(),
            ttft: Vec::new(),
            first_delta: Vec::new(),
            gaps: Vec::new(),
            latency: Vec::new(),
            drift: Vec::new(),
            truncations: 0,
        }
    }

    pub fn record_attempt(&mut self, elapsed: Duration, drift: Duration, tolerance: Duration) {
        self.counters.attempted = self.counters.attempted.saturating_add(1);
        let bucket = self.bucket(elapsed);
        bucket.attempted = bucket.attempted.saturating_add(1);
        self.drift.push(duration_ms(drift));
        if drift > tolerance {
            self.counters.late = self.counters.late.saturating_add(1);
        }
    }

    pub fn record_drop(&mut self, elapsed: Duration) {
        self.counters.dropped_by_cap = self.counters.dropped_by_cap.saturating_add(1);
        let _ = self.bucket(elapsed);
    }

    pub fn record_sent(&mut self, elapsed: Duration) {
        self.counters.sent = self.counters.sent.saturating_add(1);
        let bucket = self.bucket(elapsed);
        bucket.sent = bucket.sent.saturating_add(1);
    }

    pub fn record_join(
        &mut self,
        joined: Result<(ExpectedStatusClass, RequestObservation), JoinError>,
        elapsed: Duration,
    ) {
        self.counters.completed = self.counters.completed.saturating_add(1);
        let bucket = self.bucket(elapsed);
        bucket.completed = bucket.completed.saturating_add(1);
        match joined {
            Ok((expected, observation)) => self.record_observation(expected, observation),
            Err(_) => {
                self.counters.unexpected_outcomes =
                    self.counters.unexpected_outcomes.saturating_add(1)
            }
        }
    }

    pub fn finish(self, connection_counts: ConnectionCounts) -> ExecutorEvidence {
        let samples = TimingSamples {
            ttfb_ms: percentile(&self.ttfb, 50),
            ttft_ms: percentile(&self.ttft, 50),
            first_delta_ms: percentile(&self.first_delta, 50),
            inter_delta_gap_ms: percentiles(&self.gaps),
            latency_ms: percentiles(&self.latency),
            coordinated_omission_drift_ms: percentiles(&self.drift),
        };
        let verdict_contribution =
            if self.counters.unexpected_outcomes == 0 && self.truncations == 0 {
                Verdict::Pass
            } else {
                Verdict::Fail
            };
        ExecutorEvidence {
            schema_version: 1,
            counters: self.counters,
            samples,
            status_counts: self.status_counts,
            connection_counts,
            over_time: self.timeline.into_values().collect(),
            unexpected_stream_truncation_count: self.truncations,
            verdict_contribution,
        }
    }

    fn record_observation(
        &mut self,
        expected: ExpectedStatusClass,
        observation: RequestObservation,
    ) {
        if let Some(status) = observation.status {
            *self.status_counts.entry(status).or_default() += 1;
        }
        self.ttfb.extend(observation.ttfb_ms);
        self.latency.push(observation.latency_ms);
        match observation.terminal {
            ExecutionTerminal::TimedOut => {
                self.counters.timed_out = self.counters.timed_out.saturating_add(1)
            }
            ExecutionTerminal::ClientCancelled => {
                self.counters.client_cancelled = self.counters.client_cancelled.saturating_add(1)
            }
            ExecutionTerminal::Response(_)
            | ExecutionTerminal::TransportError
            | ExecutionTerminal::MalformedResponse => {}
        }
        if let Some(stream) = observation.stream {
            self.record_stream(&stream);
            if stream.malformed_sse || stream.malformed_json || stream.unexpected_truncation {
                self.counters.unexpected_outcomes =
                    self.counters.unexpected_outcomes.saturating_add(1);
                return;
            }
        }
        if classify_terminal(expected, observation.terminal)
            == crate::request_classifier::Classification::Unexpected
        {
            self.counters.unexpected_outcomes = self.counters.unexpected_outcomes.saturating_add(1);
        }
    }

    fn record_stream(&mut self, stream: &SseSummary) {
        self.ttft.extend(stream.ttft_ms);
        self.first_delta.extend(stream.first_delta_ms);
        self.gaps.extend(stream.inter_delta_gaps_ms.iter().copied());
        self.counters.malformed_sse = self
            .counters
            .malformed_sse
            .saturating_add(u64::from(stream.malformed_sse));
        self.counters.malformed_json = self
            .counters
            .malformed_json
            .saturating_add(u64::from(stream.malformed_json));
        self.truncations = self
            .truncations
            .saturating_add(u64::from(stream.unexpected_truncation));
    }

    fn bucket(&mut self, elapsed: Duration) -> &mut TimelinePoint {
        let second = elapsed.as_secs();
        self.timeline.entry(second).or_insert(TimelinePoint {
            second,
            ..TimelinePoint::default()
        })
    }
}

fn duration_ms(value: Duration) -> u64 {
    value.as_millis().min(u128::from(u64::MAX)) as u64
}
