use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::json;

pub const BASELINE_PATH: &str = "tests/load/baseline.json";
pub const EVIDENCE_PATH: &str = ".omo/evidence/task-40-perf-budget.json";
pub const LIVE_TAIL_EVIDENCE_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Baseline {
    pub schema_version: u32,
    pub thresholds: Thresholds,
    pub regression: RegressionBudget,
    pub reference: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Thresholds {
    pub non_streaming: NonStreamingThresholds,
    pub streaming: StreamingThresholds,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NonStreamingThresholds {
    pub p50_overhead_ms_lt: f64,
    pub p99_overhead_ms_lt: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StreamingThresholds {
    pub p50_event_overhead_ms_lt: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RegressionBudget {
    pub tolerance_percent: f64,
    pub non_streaming: NonStreamingRegressionBudget,
    pub streaming: StreamingRegressionBudget,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NonStreamingRegressionBudget {
    pub p50_overhead_ms_baseline: f64,
    pub p99_overhead_ms_baseline: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StreamingRegressionBudget {
    pub p50_event_overhead_ms_baseline: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EndpointSummary {
    pub requests: usize,
    pub concurrency: usize,
    pub success_count: usize,
    pub p50_ms: f64,
    pub p99_ms: f64,
    pub min_ms: f64,
    pub max_ms: f64,
    pub mean_ms: f64,
    pub total_sse_events: usize,
    pub p50_sse_events_per_response: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ModeSummary {
    pub mode: String,
    pub tool: String,
    pub requests: usize,
    pub concurrency: usize,
    pub direct: EndpointSummary,
    pub proxy: EndpointSummary,
    pub direct_p50_ms: f64,
    pub proxy_p50_ms: f64,
    pub p50_overhead_ms: f64,
    pub direct_p99_ms: f64,
    pub proxy_p99_ms: f64,
    pub p99_overhead_ms: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub streaming_events_per_response: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub streaming_direct_p50_ms_per_event: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub streaming_proxy_p50_ms_per_event: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub streaming_p50_event_overhead_ms: Option<f64>,
    pub thresholds: serde_json::Value,
    pub passes: BTreeMap<String, bool>,
    pub passed: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ToolInfo {
    pub preferred: String,
    pub selected: String,
    pub oha_available: bool,
    pub fallback: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Evidence {
    pub task: String,
    pub generated_at_unix_ms: u128,
    pub benchmark_tool: ToolInfo,
    pub baseline_file: String,
    pub modes: BTreeMap<String, ModeSummary>,
    pub complete: bool,
    pub passed: bool,
}

impl Baseline {
    pub fn read(path: &Path) -> Result<Self, String> {
        let text = fs::read_to_string(path)
            .map_err(|source| format!("read baseline {}: {source}", path.display()))?;
        serde_json::from_str(&text)
            .map_err(|source| format!("parse baseline {}: {source}", path.display()))
    }
}

impl Evidence {
    pub fn empty(tool: ToolInfo, generated_at_unix_ms: u128) -> Self {
        Self {
            task: "T40 load test perf budget".to_owned(),
            generated_at_unix_ms,
            benchmark_tool: tool,
            baseline_file: BASELINE_PATH.to_owned(),
            modes: BTreeMap::new(),
            complete: false,
            passed: false,
        }
    }

    pub fn read(path: &Path) -> Result<Self, String> {
        let text = fs::read_to_string(path)
            .map_err(|source| format!("read evidence {}: {source}", path.display()))?;
        serde_json::from_str(&text)
            .map_err(|source| format!("parse evidence {}: {source}", path.display()))
    }
}

pub fn mode_key(mode: &str) -> Result<&'static str, String> {
    match mode {
        "non-streaming" => Ok("non_streaming"),
        "streaming" => Ok("streaming"),
        other => Err(format!("unsupported mode {other}")),
    }
}

pub fn evaluate_summary(
    mut summary: ModeSummary,
    baseline: &Baseline,
) -> Result<ModeSummary, String> {
    let tolerance_multiplier = 1.0 + (baseline.regression.tolerance_percent / 100.0);
    let mut passes = BTreeMap::new();
    let thresholds = match summary.mode.as_str() {
        "non-streaming" => {
            let thresholds = &baseline.thresholds.non_streaming;
            let regression = &baseline.regression.non_streaming;
            let p50_regression_limit = regression.p50_overhead_ms_baseline * tolerance_multiplier;
            let p99_regression_limit = regression.p99_overhead_ms_baseline * tolerance_multiplier;
            passes.insert(
                "non_streaming_p50_added_latency_under_5ms".to_owned(),
                summary.p50_overhead_ms < thresholds.p50_overhead_ms_lt,
            );
            passes.insert(
                "non_streaming_p99_added_latency_under_20ms".to_owned(),
                summary.p99_overhead_ms < thresholds.p99_overhead_ms_lt,
            );
            passes.insert(
                "non_streaming_p50_regression_within_10_percent".to_owned(),
                summary.p50_overhead_ms <= p50_regression_limit,
            );
            passes.insert(
                "non_streaming_p99_regression_within_10_percent".to_owned(),
                summary.p99_overhead_ms <= p99_regression_limit,
            );
            json!({
                "p50_overhead_ms_lt": thresholds.p50_overhead_ms_lt,
                "p99_overhead_ms_lt": thresholds.p99_overhead_ms_lt,
                "regression_tolerance_percent": baseline.regression.tolerance_percent,
                "p50_regression_limit_ms": round3(p50_regression_limit),
                "p99_regression_limit_ms": round3(p99_regression_limit)
            })
        }
        "streaming" => {
            let thresholds = &baseline.thresholds.streaming;
            let regression = &baseline.regression.streaming;
            let event_overhead = summary
                .streaming_p50_event_overhead_ms
                .ok_or_else(|| "streaming summary missing event overhead".to_owned())?;
            let event_regression_limit =
                regression.p50_event_overhead_ms_baseline * tolerance_multiplier;
            passes.insert(
                "streaming_p50_added_latency_per_sse_event_under_2ms".to_owned(),
                event_overhead < thresholds.p50_event_overhead_ms_lt,
            );
            passes.insert(
                "streaming_p50_event_regression_within_10_percent".to_owned(),
                event_overhead <= event_regression_limit,
            );
            json!({
                "p50_event_overhead_ms_lt": thresholds.p50_event_overhead_ms_lt,
                "regression_tolerance_percent": baseline.regression.tolerance_percent,
                "p50_event_regression_limit_ms": round3(event_regression_limit)
            })
        }
        other => return Err(format!("unsupported summary mode {other}")),
    };

    summary.thresholds = thresholds;
    summary.passed = passes.values().all(|passed| *passed);
    summary.passes = passes;
    Ok(summary)
}

pub fn assert_evidence_against_baseline(root: &Path) -> Result<(), String> {
    let baseline = Baseline::read(&root.join(BASELINE_PATH))?;
    let evidence = Evidence::read(&root.join(EVIDENCE_PATH))?;

    for key in ["non_streaming", "streaming"] {
        let summary = evidence
            .modes
            .get(key)
            .ok_or_else(|| format!("evidence missing {key} mode"))?;
        let evaluated = evaluate_summary(summary.clone(), &baseline)?;
        if summary.passes != evaluated.passes {
            return Err(format!(
                "{key} budget booleans do not match recomputed thresholds"
            ));
        }
        if !evaluated.passed {
            return Err(format!("{key} budget failed: {:?}", evaluated.passes));
        }
    }

    if !evidence.complete {
        return Err("evidence is not complete; both modes are required".to_owned());
    }
    if !evidence.passed {
        return Err("evidence top-level passed=false".to_owned());
    }

    Ok(())
}

pub fn round3(value: f64) -> f64 {
    (value * 1000.0).round() / 1000.0
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SoakProfile {
    Smoke,
    Soak,
    Burst,
    Leak,
}

impl SoakProfile {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Soak => "soak",
            Self::Burst => "burst",
            Self::Leak => "leak",
        }
    }

    pub const fn thresholds(self) -> LiveTailSoakThresholds {
        LiveTailSoakThresholds {
            proxy_min_success_rate: 0.999,
            storage_tail_lag_p95_max_ms: 1_000.0,
            storage_tail_lag_max_ms: 5_000.0,
            lifecycle_full_dropped_events_max: 0.0,
            reset_events_max: 0.0,
            rss_slope_max_mib_per_min: 1.0,
            assembler_in_flight_max: match self {
                Self::Burst => 10_000.0,
                Self::Smoke | Self::Soak | Self::Leak => 5_000.0,
            },
            rps_tolerance_percent: 5.0,
        }
    }
}

impl std::str::FromStr for SoakProfile {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "smoke" => Ok(Self::Smoke),
            "soak" => Ok(Self::Soak),
            "burst" => Ok(Self::Burst),
            "leak" => Ok(Self::Leak),
            other => Err(format!("unsupported live-tail soak profile {other}")),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MetricSeriesSummary {
    pub min: f64,
    pub p50: f64,
    pub p95: f64,
    pub max: f64,
    pub samples_count: usize,
}

impl MetricSeriesSummary {
    pub fn from_samples(samples: &[f64]) -> Self {
        let mut sorted = samples
            .iter()
            .copied()
            .filter(|value| value.is_finite())
            .collect::<Vec<_>>();
        sorted.sort_by(f64::total_cmp);
        Self {
            min: round3(*sorted.first().unwrap_or(&0.0)),
            p50: round3(percentile_sorted(&sorted, 0.50)),
            p95: round3(percentile_sorted(&sorted, 0.95)),
            max: round3(*sorted.last().unwrap_or(&0.0)),
            samples_count: sorted.len(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LiveTailSoakThresholds {
    pub proxy_min_success_rate: f64,
    pub storage_tail_lag_p95_max_ms: f64,
    pub storage_tail_lag_max_ms: f64,
    pub lifecycle_full_dropped_events_max: f64,
    pub reset_events_max: f64,
    pub rss_slope_max_mib_per_min: f64,
    pub assembler_in_flight_max: f64,
    pub rps_tolerance_percent: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LiveTailSoakEvidence {
    pub schema_version: u32,
    pub profile: SoakProfile,
    pub rps: u64,
    pub duration_secs: u64,
    pub stream_ratio: u8,
    pub max_in_flight: usize,
    pub sse_subscribers_count: usize,
    pub reconnect_churn_secs: u64,
    pub started_at_unix_ms: u128,
    pub completed_at_unix_ms: u128,
    pub actual_duration_secs: f64,
    pub actual_rps: f64,
    pub proxy_success_count: u64,
    pub proxy_failure_count: u64,
    pub streaming_request_count: u64,
    pub non_streaming_request_count: u64,
    pub sse_events_received_per_subscriber: Vec<u64>,
    pub sse_messages_received_per_subscriber: Vec<u64>,
    pub sse_resets_received_per_subscriber: Vec<u64>,
    pub sse_reconnects_per_subscriber: Vec<u64>,
    pub sse_last_event_id_per_subscriber: Vec<Option<String>>,
    pub sse_final_events_observed: u64,
    pub storage_final_events_estimate: u64,
    pub metric_series: BTreeMap<String, MetricSeriesSummary>,
    pub metric_last_values: BTreeMap<String, f64>,
    pub rss_initial_mib: f64,
    pub rss_final_mib: f64,
    pub rss_growth_mib: f64,
    pub rss_slope_mib_per_min: f64,
    pub thresholds: LiveTailSoakThresholds,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AssertionFailure {
    pub name: String,
    pub expected: String,
    pub actual: String,
}

pub fn evaluate_live_tail_soak(
    evidence: &LiveTailSoakEvidence,
    profile: SoakProfile,
) -> Result<(), Vec<AssertionFailure>> {
    let thresholds = profile.thresholds();
    let mut failures = Vec::new();
    let total = evidence.proxy_success_count + evidence.proxy_failure_count;
    let success_rate = if total == 0 {
        0.0
    } else {
        evidence.proxy_success_count as f64 / total as f64
    };
    push_failure_if(
        &mut failures,
        success_rate < thresholds.proxy_min_success_rate,
        "proxy_success_rate",
        format!(">= {}", thresholds.proxy_min_success_rate),
        round3(success_rate).to_string(),
    );

    let target_min = evidence.rps as f64 * (1.0 - thresholds.rps_tolerance_percent / 100.0);
    let target_max = evidence.rps as f64 * (1.0 + thresholds.rps_tolerance_percent / 100.0);
    push_failure_if(
        &mut failures,
        evidence.actual_rps < target_min || evidence.actual_rps > target_max,
        "actual_rps",
        format!("between {} and {}", round3(target_min), round3(target_max)),
        round3(evidence.actual_rps).to_string(),
    );

    if let Some(lag) = evidence.metric_series.get("sse_storage_tail_lag_ms") {
        push_failure_if(
            &mut failures,
            lag.p95 > thresholds.storage_tail_lag_p95_max_ms,
            "sse_storage_tail_lag_ms_p95",
            format!("<= {}", thresholds.storage_tail_lag_p95_max_ms),
            lag.p95.to_string(),
        );
        push_failure_if(
            &mut failures,
            lag.max > thresholds.storage_tail_lag_max_ms,
            "sse_storage_tail_lag_ms_max",
            format!("<= {}", thresholds.storage_tail_lag_max_ms),
            lag.max.to_string(),
        );
    }

    let lifecycle_drops = evidence
        .metric_last_values
        .iter()
        .filter(|(name, _)| {
            name.starts_with("cc_lb_dropped_events_total{reason=\"lifecycle_")
                && name.contains("_full\"")
        })
        .map(|(_, value)| *value)
        .sum::<f64>();
    push_failure_if(
        &mut failures,
        lifecycle_drops > thresholds.lifecycle_full_dropped_events_max,
        "lifecycle_full_dropped_events",
        "== 0".to_owned(),
        lifecycle_drops.to_string(),
    );

    let reset_events = ["backfill_cap", "bus_lagged", "storage_error"]
        .iter()
        .map(|reason| {
            evidence
                .metric_last_values
                .get(&format!(
                    "sse_reset_events_sent_total{{reason=\"{reason}\"}}"
                ))
                .copied()
                .unwrap_or(0.0)
        })
        .sum::<f64>();
    push_failure_if(
        &mut failures,
        reset_events > thresholds.reset_events_max,
        "sse_reset_events_sent_total",
        "== 0 for backfill_cap|bus_lagged|storage_error".to_owned(),
        reset_events.to_string(),
    );

    if profile != SoakProfile::Smoke {
        push_failure_if(
            &mut failures,
            evidence.rss_slope_mib_per_min > thresholds.rss_slope_max_mib_per_min,
            "rss_slope_mib_per_min",
            format!("<= {}", thresholds.rss_slope_max_mib_per_min),
            round3(evidence.rss_slope_mib_per_min).to_string(),
        );
    }

    if let Some(in_flight) = evidence
        .metric_series
        .get("cc_lb_lifecycle_assembler_in_flight")
    {
        push_failure_if(
            &mut failures,
            in_flight.max > thresholds.assembler_in_flight_max,
            "cc_lb_lifecycle_assembler_in_flight",
            format!("<= {}", thresholds.assembler_in_flight_max),
            in_flight.max.to_string(),
        );
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures)
    }
}

fn push_failure_if(
    failures: &mut Vec<AssertionFailure>,
    failed: bool,
    name: &str,
    expected: String,
    actual: String,
) {
    if failed {
        failures.push(AssertionFailure {
            name: name.to_owned(),
            expected,
            actual,
        });
    }
}

fn percentile_sorted(sorted: &[f64], quantile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = ((sorted.len() - 1) as f64 * quantile).ceil() as usize;
    sorted[rank.min(sorted.len() - 1)]
}
