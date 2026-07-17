use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::executor_evidence::ExecutorEvidence;

#[derive(Debug, Deserialize, Serialize)]
pub struct Dispersion {
    pub iqr: u64,
    pub mad: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MetricSummary {
    pub min: u64,
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub max: u64,
    pub samples: u64,
    pub dispersion: Dispersion,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RpsEvidence {
    pub attempted: u64,
    pub sent: u64,
    pub late: u64,
    pub dropped_by_cap: u64,
    pub completed: u64,
    pub achieved: f64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct WaveMetrics {
    pub latency_ms: MetricSummary,
    pub ttfb_ms: MetricSummary,
    pub ttft_ms: MetricSummary,
    pub schedule_drift_ms: MetricSummary,
    pub prometheus: BTreeMap<String, MetricSummary>,
}

pub struct MetricsInput<'a> {
    pub latency_ms: &'a [u64],
    pub ttfb_ms: &'a [u64],
    pub ttft_ms: &'a [u64],
    pub schedule_drift_ms: &'a [u64],
    pub prometheus: &'a BTreeMap<String, Vec<u64>>,
}

pub fn summarize_metrics(input: MetricsInput<'_>) -> WaveMetrics {
    let prometheus = input
        .prometheus
        .iter()
        .map(|(name, values)| (name.clone(), summarize_samples(values)))
        .collect();
    WaveMetrics {
        latency_ms: summarize_samples(input.latency_ms),
        ttfb_ms: summarize_samples(input.ttfb_ms),
        ttft_ms: summarize_samples(input.ttft_ms),
        schedule_drift_ms: summarize_samples(input.schedule_drift_ms),
        prometheus,
    }
}

pub fn summarize_samples(values: &[u64]) -> MetricSummary {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let p50 = percentile(&sorted, 50);
    let mut deviations = sorted
        .iter()
        .map(|value| value.abs_diff(p50))
        .collect::<Vec<_>>();
    deviations.sort_unstable();
    MetricSummary {
        min: sorted.first().copied().unwrap_or_default(),
        p50,
        p95: percentile(&sorted, 95),
        p99: percentile(&sorted, 99),
        max: sorted.last().copied().unwrap_or_default(),
        samples: u64::try_from(sorted.len()).unwrap_or(u64::MAX),
        dispersion: Dispersion {
            iqr: percentile(&sorted, 75).saturating_sub(percentile(&sorted, 25)),
            mad: percentile(&deviations, 50),
        },
    }
}

pub fn rps_from_executor(executor: &ExecutorEvidence, duration_ms: u64) -> RpsEvidence {
    let counters = executor.counters;
    let achieved = if duration_ms == 0 {
        0.0
    } else {
        counters.completed as f64 * 1_000.0 / duration_ms as f64
    };
    RpsEvidence {
        attempted: counters.attempted,
        sent: counters.sent,
        late: counters.late,
        dropped_by_cap: counters.dropped_by_cap,
        completed: counters.completed,
        achieved,
    }
}

fn percentile(sorted: &[u64], percent: usize) -> u64 {
    let Some(index) = sorted
        .len()
        .checked_mul(percent)
        .and_then(|value| value.checked_add(99))
        .and_then(|value| value.checked_div(100))
        .and_then(|value| value.checked_sub(1))
    else {
        return 0;
    };
    sorted.get(index).copied().unwrap_or_default()
}
