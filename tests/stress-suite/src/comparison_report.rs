use serde::{Deserialize, Serialize};

use crate::comparison_baseline::ConfidenceBand;

#[derive(Debug, Deserialize, Serialize)]
pub struct ComparisonReport {
    pub verdict: crate::verdict::Verdict,
    pub reasons: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_stage: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<String>,
    pub comparison: ComparisonSummary,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ComparisonSummary {
    pub throughput: Delta,
    pub waves: Vec<WaveComparison>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct WaveComparison {
    pub name: String,
    pub throughput: Delta,
    pub latency_ms: PercentileComparison,
    pub ttfb_ms: PercentileComparison,
    pub ttft_ms: PercentileComparison,
    pub schedule_drift_ms: PercentileComparison,
    pub qdisc: Vec<QdiscComparison>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Delta {
    pub baseline: f64,
    pub evidence: f64,
    pub delta: f64,
    pub delta_ratio: f64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PercentileComparison {
    pub p95: BandComparison,
    pub p99: BandComparison,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BandComparison {
    pub baseline: ConfidenceBand,
    pub evidence: ConfidenceBand,
    pub delta: f64,
    pub delta_ratio: f64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct QdiscComparison {
    pub edge: String,
    pub baseline_drop_ratio: f64,
    pub evidence_drop_ratio: f64,
    pub drop_ratio_delta: f64,
}

pub fn delta(baseline: f64, evidence: f64) -> Delta {
    Delta {
        baseline,
        evidence,
        delta: evidence - baseline,
        delta_ratio: ratio_f64(evidence, baseline),
    }
}

pub fn ratio_f64(value: f64, baseline: f64) -> f64 {
    if baseline == 0.0 {
        0.0
    } else {
        (value - baseline) / baseline
    }
}

pub fn empty_comparison() -> ComparisonSummary {
    ComparisonSummary {
        throughput: delta(0.0, 0.0),
        waves: Vec::new(),
    }
}
