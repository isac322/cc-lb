use std::collections::BTreeMap;
use std::path::Path;
use std::process::ExitCode;

use crate::evidence_environment::EnvironmentManifest;
use crate::evidence_qdisc::{NetemEvidence, QdiscCapture, record_qdisc};
use crate::evidence_redaction::{write_redacted_json, write_redacted_value};
use crate::evidence_schema::{
    EVIDENCE_SCHEMA_VERSION, EventCorrelation, EvidenceDocument, ReplicaProcessMetrics, TimedEvent,
    WaveEvidence, WaveInput,
};
use crate::executor_evidence::{
    ConnectionCounts, Counters, ExecutorEvidence, Percentiles, TimelinePoint, TimingSamples,
};
use crate::multi_replica_evidence::{HotpathEffect, StorageEvidence, StoragePlaneEffect};
use crate::verdict::Verdict;

pub fn run_evidence_schema(output: &Path) -> ExitCode {
    match synthetic_document().and_then(|document| write_redacted_json(output, &document)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("write evidence-schema self-check: {error}");
            ExitCode::FAILURE
        }
    }
}

pub fn run_redaction(input: &Path, output: &Path) -> ExitCode {
    let fixture = match std::fs::read_to_string(input) {
        Ok(fixture) => fixture,
        Err(error) => {
            eprintln!("read redaction fixture {}: {error}", input.display());
            return ExitCode::FAILURE;
        }
    };
    let value = match serde_json::from_str(&fixture) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("parse redaction fixture {}: {error}", input.display());
            return ExitCode::FAILURE;
        }
    };
    match write_redacted_value(output, value) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("write redaction self-check: {error}");
            ExitCode::FAILURE
        }
    }
}

pub fn synthetic_document() -> Result<EvidenceDocument, String> {
    let executor = synthetic_executor();
    let qdisc = record_qdisc(QdiscCapture {
        edge: "probe-to-cc-lb",
        device: "eth0",
        output: "qdisc netem 1: root refcnt 2 limit 1000 delay 10ms\n Sent 480 bytes 4 pkt (dropped 0, overlimits 0 requeues 0)\n",
        scheduled_at_unix_ms: 1_000,
        command_started_at_unix_ms: 1_005,
        command_completed_at_unix_ms: 1_020,
    })?;
    let storage = StorageEvidence {
        backend: "postgres".to_owned(),
        plane_effect: StoragePlaneEffect {
            classified: true,
            errors: Vec::new(),
            admin_mutation_during_impairment: true,
            propagation_latency_ms: Some(20),
            cross_replica_key_usable: true,
            request_event_persistence_gap: 0,
            listen_notify_reconnect_or_drop_observed: false,
            storage_tail_backlog_rows: 0,
            storage_tail_lag_ms: Some(5),
        },
        hotpath_effect: HotpathEffect {
            managed_key_requests: 2,
            healthy_requests: 2,
            unexpected_5xx_on_healthy: 0,
            storage_errors_classified: 0,
        },
    };
    let wave = WaveEvidence::from_input(WaveInput {
        name: "healthy-traffic".to_owned(),
        load_workers: 1,
        duration_ms: 1_000,
        recovery_ms: 125,
        executor,
        latency_ms: vec![10, 20, 30, 40, 100],
        ttfb_ms: vec![5, 10, 15],
        ttft_ms: vec![8, 12, 16],
        schedule_drift_ms: vec![1, 2, 3],
        prometheus: BTreeMap::from([("cc_lb_request_duration_ms".to_owned(), vec![10, 20, 30])]),
        netem: NetemEvidence { qdisc: vec![qdisc] },
        events: EventCorrelation::new(
            vec![TimedEvent {
                request_id: Some("request-001".to_owned()),
                name: "request_completed".to_owned(),
                recorded_at_unix_ms: 1_100,
            }],
            vec![TimedEvent {
                request_id: Some("request-001".to_owned()),
                name: "request_persisted".to_owned(),
                recorded_at_unix_ms: 1_120,
            }],
        ),
        storage: storage.clone(),
    });
    Ok(EvidenceDocument {
        schema_version: EVIDENCE_SCHEMA_VERSION,
        verdict: Verdict::Pass,
        waves: vec![wave],
        environment: EnvironmentManifest::synthetic(),
        replica_process_metrics: vec![ReplicaProcessMetrics {
            replica: "replica-0".to_owned(),
            rss_bytes: 1_048_576,
            open_fds: 12,
            cpu_percent: 3.5,
        }],
        storage,
    })
}

fn synthetic_executor() -> ExecutorEvidence {
    ExecutorEvidence {
        schema_version: 1,
        counters: Counters {
            attempted: 5,
            sent: 5,
            late: 1,
            dropped_by_cap: 0,
            completed: 5,
            timed_out: 0,
            client_cancelled: 0,
            malformed_sse: 0,
            malformed_json: 0,
            unexpected_outcomes: 0,
        },
        samples: TimingSamples {
            ttfb_ms: Some(10),
            ttft_ms: Some(12),
            first_delta_ms: Some(12),
            inter_delta_gap_ms: Percentiles {
                p50: Some(4),
                p95: Some(8),
            },
            latency_ms: Percentiles {
                p50: Some(30),
                p95: Some(100),
            },
            coordinated_omission_drift_ms: Percentiles {
                p50: Some(2),
                p95: Some(3),
            },
        },
        status_counts: BTreeMap::from([(200, 5)]),
        connection_counts: ConnectionCounts { new: 1, reused: 4 },
        over_time: vec![TimelinePoint {
            second: 0,
            attempted: 5,
            sent: 5,
            completed: 5,
        }],
        unexpected_stream_truncation_count: 0,
        verdict_contribution: Verdict::Pass,
    }
}
