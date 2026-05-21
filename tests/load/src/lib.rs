use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::json;

pub const BASELINE_PATH: &str = "tests/load/baseline.json";
pub const EVIDENCE_PATH: &str = ".omo/evidence/task-40-perf-budget.json";

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
