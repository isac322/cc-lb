use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::evidence_environment::EnvironmentManifest;
use crate::evidence_metrics::{Dispersion, MetricSummary, WaveMetrics};
use crate::evidence_qdisc::QdiscStats;
use crate::evidence_schema::{EVIDENCE_SCHEMA_VERSION, EvidenceDocument, WaveEvidence};

pub const BASELINE_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Deserialize, Serialize)]
pub struct BaselineDocument {
    pub schema_version: u16,
    pub evidence_schema_version: u16,
    pub policy: ComparisonPolicy,
    pub waves: Vec<BaselineWave>,
    pub environment: EnvironmentBounds,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ComparisonPolicy {
    pub min_samples: u64,
    pub rps_budget_ratio: f64,
    pub qdisc_drop_ratio_delta: f64,
    pub max_qdisc_schedule_drift_ms: u64,
    pub max_confidence_band_ratio: f64,
    pub max_host_load_ratio: f64,
}

impl Default for ComparisonPolicy {
    fn default() -> Self {
        Self {
            min_samples: 5,
            rps_budget_ratio: 0.10,
            qdisc_drop_ratio_delta: 0.02,
            max_qdisc_schedule_drift_ms: 100,
            max_confidence_band_ratio: 0.25,
            max_host_load_ratio: 2.0,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BaselineWave {
    pub name: String,
    pub sample_count: u64,
    pub achieved_rps: f64,
    pub metrics: BaselineMetrics,
    pub qdisc: Vec<QdiscBaseline>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BaselineMetrics {
    pub latency_ms: MetricBaseline,
    pub ttfb_ms: MetricBaseline,
    pub ttft_ms: MetricBaseline,
    pub schedule_drift_ms: MetricBaseline,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MetricBaseline {
    pub samples: u64,
    pub dispersion: Dispersion,
    pub p95: ConfidenceBand,
    pub p99: ConfidenceBand,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ConfidenceBand {
    pub lower: f64,
    pub upper: f64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct QdiscBaseline {
    pub edge: String,
    pub device: String,
    pub packets: u64,
    pub drop_ratio: f64,
    pub overlimits: u64,
    pub requeues: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct EnvironmentBounds {
    pub kernel: String,
    pub tc_version: String,
    pub docker_version: String,
    pub image_digests: BTreeMap<String, String>,
    pub binary_hashes: BTreeMap<String, String>,
    pub git_sha: String,
    pub git_dirty: bool,
    pub cpu_count: usize,
    pub cpu_model: String,
    pub cgroup: String,
    pub memory_limit: String,
    pub fd_soft_limit: String,
    pub max_one_minute_load: f64,
}

impl BaselineDocument {
    pub fn from_evidence(evidence: &EvidenceDocument) -> Self {
        Self {
            schema_version: BASELINE_SCHEMA_VERSION,
            evidence_schema_version: evidence.schema_version,
            policy: ComparisonPolicy::default(),
            waves: evidence.waves.iter().map(BaselineWave::from).collect(),
            environment: EnvironmentBounds::from(&evidence.environment),
        }
    }
}

impl From<&WaveEvidence> for BaselineWave {
    fn from(wave: &WaveEvidence) -> Self {
        Self {
            name: wave.name.clone(),
            sample_count: wave.sample_count,
            achieved_rps: wave.rps.achieved,
            metrics: BaselineMetrics::from(&wave.metrics),
            qdisc: wave.netem.qdisc.iter().map(QdiscBaseline::from).collect(),
        }
    }
}

impl From<&WaveMetrics> for BaselineMetrics {
    fn from(metrics: &WaveMetrics) -> Self {
        Self {
            latency_ms: MetricBaseline::from(&metrics.latency_ms),
            ttfb_ms: MetricBaseline::from(&metrics.ttfb_ms),
            ttft_ms: MetricBaseline::from(&metrics.ttft_ms),
            schedule_drift_ms: MetricBaseline::from(&metrics.schedule_drift_ms),
        }
    }
}

impl From<&MetricSummary> for MetricBaseline {
    fn from(summary: &MetricSummary) -> Self {
        Self {
            samples: summary.samples,
            dispersion: Dispersion {
                iqr: summary.dispersion.iqr,
                mad: summary.dispersion.mad,
            },
            p95: confidence_band(
                summary.p95,
                summary.dispersion.iqr,
                summary.dispersion.mad,
                summary.samples,
            ),
            p99: confidence_band(
                summary.p99,
                summary.dispersion.iqr,
                summary.dispersion.mad,
                summary.samples,
            ),
        }
    }
}

impl From<&QdiscStats> for QdiscBaseline {
    fn from(stats: &QdiscStats) -> Self {
        Self {
            edge: stats.edge.clone(),
            device: stats.device.clone(),
            packets: stats.packets,
            drop_ratio: ratio(stats.dropped, stats.packets),
            overlimits: stats.overlimits,
            requeues: stats.requeues,
        }
    }
}

impl From<&EnvironmentManifest> for EnvironmentBounds {
    fn from(environment: &EnvironmentManifest) -> Self {
        Self {
            kernel: environment.kernel.clone(),
            tc_version: environment.tc_version.clone(),
            docker_version: environment.docker_version.clone(),
            image_digests: environment.image_digests.clone(),
            binary_hashes: environment.binary_hashes.clone(),
            git_sha: environment.git_sha.clone(),
            git_dirty: environment.git_dirty,
            cpu_count: environment.cpu.count,
            cpu_model: environment.cpu.model.clone(),
            cgroup: environment.limits.cgroup.clone(),
            memory_limit: environment.limits.memory_limit.clone(),
            fd_soft_limit: environment.limits.fd_soft_limit.clone(),
            max_one_minute_load: environment
                .host_load_samples
                .iter()
                .map(|sample| sample.one_minute)
                .fold(0.0, f64::max),
        }
    }
}

pub fn read_baseline(path: &Path) -> Result<BaselineDocument, String> {
    read_json(path, "baseline")
}

pub fn read_evidence(path: &Path) -> Result<EvidenceDocument, String> {
    read_json(path, "evidence")
}

pub fn write_baseline(path: &Path, baseline: &BaselineDocument) -> Result<(), String> {
    crate::evidence_redaction::write_redacted_json(path, baseline)
}

fn confidence_band(value: u64, iqr: u64, mad: u64, samples: u64) -> ConfidenceBand {
    let scale = iqr.max(mad).max(1) as f64;
    let half_width = 1.96 * scale / (samples.max(1) as f64).sqrt();
    ConfidenceBand {
        lower: (value as f64 - half_width).max(0.0),
        upper: value as f64 + half_width,
    }
}

pub fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path, label: &str) -> Result<T, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|error| format!("read {label} {}: {error}", path.display()))?;
    serde_json::from_str(&text)
        .map_err(|error| format!("parse {label} {}: {error}", path.display()))
}

pub fn evidence_schema_matches(evidence: &EvidenceDocument, baseline: &BaselineDocument) -> bool {
    evidence.schema_version == EVIDENCE_SCHEMA_VERSION
        && baseline.evidence_schema_version == EVIDENCE_SCHEMA_VERSION
        && baseline.schema_version == BASELINE_SCHEMA_VERSION
}
