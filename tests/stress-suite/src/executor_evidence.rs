use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::request_classifier::Classification;
use crate::sse_timing::SseSummary;
use crate::traffic::ExpectedStatusClass;
use crate::verdict::Verdict;

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
pub struct ConnectionCounts {
    pub new: u64,
    pub reused: u64,
}

#[derive(Clone, Copy, Debug)]
pub enum ExecutionTerminal {
    Response(u16),
    ClientCancelled,
    TimedOut,
    TransportError,
    MalformedResponse,
}

#[derive(Clone, Debug)]
pub struct RequestObservation {
    pub status: Option<u16>,
    pub ttfb_ms: Option<u64>,
    pub latency_ms: u64,
    pub terminal: ExecutionTerminal,
    pub stream: Option<SseSummary>,
}

impl RequestObservation {
    #[cfg(test)]
    pub const fn completed(status: u16) -> Self {
        Self {
            status: Some(status),
            ttfb_ms: None,
            latency_ms: 0,
            terminal: ExecutionTerminal::Response(status),
            stream: None,
        }
    }

    pub const fn transport_error(latency_ms: u64) -> Self {
        Self {
            status: None,
            ttfb_ms: None,
            latency_ms,
            terminal: ExecutionTerminal::TransportError,
            stream: None,
        }
    }

    pub const fn timed_out(latency_ms: u64) -> Self {
        Self {
            status: None,
            ttfb_ms: None,
            latency_ms,
            terminal: ExecutionTerminal::TimedOut,
            stream: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
pub struct Counters {
    pub attempted: u64,
    pub sent: u64,
    pub late: u64,
    pub dropped_by_cap: u64,
    pub completed: u64,
    pub timed_out: u64,
    pub client_cancelled: u64,
    pub malformed_sse: u64,
    pub malformed_json: u64,
    pub unexpected_outcomes: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Percentiles {
    pub p50: Option<u64>,
    pub p95: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct TimingSamples {
    pub ttfb_ms: Option<u64>,
    pub ttft_ms: Option<u64>,
    pub first_delta_ms: Option<u64>,
    pub inter_delta_gap_ms: Percentiles,
    pub latency_ms: Percentiles,
    pub coordinated_omission_drift_ms: Percentiles,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
pub struct TimelinePoint {
    pub second: u64,
    pub attempted: u64,
    pub sent: u64,
    pub completed: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExecutorEvidence {
    pub schema_version: u16,
    pub counters: Counters,
    pub samples: TimingSamples,
    pub status_counts: BTreeMap<u16, u64>,
    pub connection_counts: ConnectionCounts,
    pub over_time: Vec<TimelinePoint>,
    pub unexpected_stream_truncation_count: u64,
    pub verdict_contribution: Verdict,
}

pub fn classify_terminal(
    expected: ExpectedStatusClass,
    terminal: ExecutionTerminal,
) -> Classification {
    match terminal {
        ExecutionTerminal::Response(status) => match expected {
            ExpectedStatusClass::Success2xx if (200..300).contains(&status) => {
                Classification::Expected
            }
            ExpectedStatusClass::Provider529 if status == 529 => Classification::Expected,
            ExpectedStatusClass::Malformed400 if status == 400 => Classification::Expected,
            ExpectedStatusClass::Unsupported4xx if (400..500).contains(&status) => {
                Classification::Expected
            }
            ExpectedStatusClass::Success2xx
            | ExpectedStatusClass::Provider529
            | ExpectedStatusClass::ClientCancelled
            | ExpectedStatusClass::Malformed400
            | ExpectedStatusClass::Unsupported4xx => Classification::Unexpected,
        },
        ExecutionTerminal::ClientCancelled => match expected {
            ExpectedStatusClass::ClientCancelled => Classification::Expected,
            ExpectedStatusClass::Success2xx
            | ExpectedStatusClass::Provider529
            | ExpectedStatusClass::Malformed400
            | ExpectedStatusClass::Unsupported4xx => Classification::Unexpected,
        },
        ExecutionTerminal::TimedOut
        | ExecutionTerminal::TransportError
        | ExecutionTerminal::MalformedResponse => Classification::Unexpected,
    }
}

pub fn percentiles(values: &[u64]) -> Percentiles {
    Percentiles {
        p50: percentile(values, 50),
        p95: percentile(values, 95),
    }
}

pub fn percentile(values: &[u64], percent: usize) -> Option<u64> {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let index = sorted
        .len()
        .checked_mul(percent)?
        .checked_add(99)?
        .checked_div(100)?
        .checked_sub(1)?;
    sorted.get(index).copied()
}
