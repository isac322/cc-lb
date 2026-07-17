use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::evidence_environment::EnvironmentManifest;
use crate::evidence_metrics::{
    MetricsInput, RpsEvidence, WaveMetrics, rps_from_executor, summarize_metrics,
};
use crate::evidence_qdisc::NetemEvidence;
use crate::executor_evidence::ExecutorEvidence;
use crate::multi_replica_evidence::StorageEvidence;
use crate::verdict::Verdict;

pub const EVIDENCE_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Deserialize, Serialize)]
pub struct EvidenceDocument {
    pub schema_version: u16,
    pub verdict: Verdict,
    pub waves: Vec<WaveEvidence>,
    pub environment: EnvironmentManifest,
    pub replica_process_metrics: Vec<ReplicaProcessMetrics>,
    pub storage: StorageEvidence,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct WaveEvidence {
    pub name: String,
    #[serde(default)]
    pub load_workers: u64,
    pub rps: RpsEvidence,
    pub recovery_ms: u64,
    pub sample_count: u64,
    pub metrics: WaveMetrics,
    pub netem: NetemEvidence,
    pub events: EventCorrelation,
    pub executor: ExecutorEvidence,
    pub storage: StorageEvidence,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ReplicaProcessMetrics {
    pub replica: String,
    pub rss_bytes: u64,
    pub open_fds: u64,
    pub cpu_percent: f64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TimedEvent {
    pub request_id: Option<String>,
    pub name: String,
    pub recorded_at_unix_ms: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CorrelatedEvent {
    pub request_id: String,
    pub request_at_unix_ms: u64,
    pub admin_at_unix_ms: u64,
    pub correlation_lag_ms: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct EventCorrelation {
    pub request_events: Vec<TimedEvent>,
    pub admin_events: Vec<TimedEvent>,
    pub correlated: Vec<CorrelatedEvent>,
}

pub struct WaveInput {
    pub name: String,
    pub load_workers: u64,
    pub duration_ms: u64,
    pub recovery_ms: u64,
    pub executor: ExecutorEvidence,
    pub latency_ms: Vec<u64>,
    pub ttfb_ms: Vec<u64>,
    pub ttft_ms: Vec<u64>,
    pub schedule_drift_ms: Vec<u64>,
    pub prometheus: BTreeMap<String, Vec<u64>>,
    pub netem: NetemEvidence,
    pub events: EventCorrelation,
    pub storage: StorageEvidence,
}

impl WaveEvidence {
    pub fn from_input(input: WaveInput) -> Self {
        let sample_count = u64::try_from(input.latency_ms.len()).unwrap_or(u64::MAX);
        let metrics = summarize_metrics(MetricsInput {
            latency_ms: &input.latency_ms,
            ttfb_ms: &input.ttfb_ms,
            ttft_ms: &input.ttft_ms,
            schedule_drift_ms: &input.schedule_drift_ms,
            prometheus: &input.prometheus,
        });
        let rps = rps_from_executor(&input.executor, input.duration_ms);
        Self {
            name: input.name,
            load_workers: input.load_workers,
            rps,
            recovery_ms: input.recovery_ms,
            sample_count,
            metrics,
            netem: input.netem,
            events: input.events,
            executor: input.executor,
            storage: input.storage,
        }
    }
}

impl EventCorrelation {
    pub fn new(request_events: Vec<TimedEvent>, admin_events: Vec<TimedEvent>) -> Self {
        let correlated = request_events
            .iter()
            .filter_map(|request| {
                let request_id = request.request_id.as_ref()?;
                admin_events
                    .iter()
                    .filter(|admin| admin.request_id.as_ref() == Some(request_id))
                    .map(|admin| CorrelatedEvent {
                        request_id: request_id.clone(),
                        request_at_unix_ms: request.recorded_at_unix_ms,
                        admin_at_unix_ms: admin.recorded_at_unix_ms,
                        correlation_lag_ms: admin
                            .recorded_at_unix_ms
                            .saturating_sub(request.recorded_at_unix_ms),
                    })
                    .next()
            })
            .collect();
        Self {
            request_events,
            admin_events,
            correlated,
        }
    }
}
