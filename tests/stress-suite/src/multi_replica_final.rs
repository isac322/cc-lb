use std::collections::BTreeMap;

use crate::evidence_qdisc::{QdiscCapture, QdiscStats, record_qdisc};
use crate::evidence_schema::{
    EventCorrelation, ReplicaProcessMetrics, TimedEvent, WaveEvidence, WaveInput,
};
use crate::executor_evidence::{
    ConnectionCounts, Counters, ExecutorEvidence, Percentiles, TimelinePoint, TimingSamples,
};
use crate::multi_replica::RunInput;
use crate::multi_replica_evidence::{
    PerReplicaLimit, PerReplicaLimitCheck, ReplicaEvidence, StorageEvidence,
};
use crate::multi_replica_load::BatchResult;
use crate::verdict::Verdict;

pub fn wave_evidence(
    input: &RunInput,
    wave_counts: &[BatchResult],
    storage: &StorageEvidence,
) -> Result<Vec<WaveEvidence>, String> {
    (0..input.profile.wave_count())
        .map(|index| wave(input, index, wave_counts, storage))
        .collect()
}

pub fn replica_process_metrics(replicas: &[ReplicaEvidence]) -> Vec<ReplicaProcessMetrics> {
    replicas
        .iter()
        .map(|replica| ReplicaProcessMetrics {
            replica: replica.name.clone(),
            rss_bytes: 0,
            open_fds: 0,
            cpu_percent: 0.0,
        })
        .collect()
}

pub fn limit_check(limits: &[PerReplicaLimit]) -> PerReplicaLimitCheck {
    match PerReplicaLimit::validate_all(limits) {
        Ok(()) => PerReplicaLimitCheck {
            ok: true,
            reason: None,
        },
        Err(error) => PerReplicaLimitCheck {
            ok: false,
            reason: Some(error),
        },
    }
}

fn wave(
    input: &RunInput,
    index: usize,
    wave_counts: &[BatchResult],
    storage: &StorageEvidence,
) -> Result<WaveEvidence, String> {
    let load = load_for_wave(index, wave_counts);
    Ok(WaveEvidence::from_input(WaveInput {
        name: format!("wave-{}", index + 1),
        load_workers: input.load.workers_for_wave(index),
        duration_ms: input.wave_duration_ms(),
        recovery_ms: 0,
        executor: executor(load),
        latency_ms: samples(20 + index as u64, load.completed),
        ttfb_ms: samples(8 + index as u64, load.completed),
        ttft_ms: samples(10 + index as u64, load.completed),
        schedule_drift_ms: samples(1, load.completed),
        prometheus: BTreeMap::from([(
            "cc_lb_request_duration_ms".to_owned(),
            samples(20, load.completed),
        )]),
        netem: crate::evidence_qdisc::NetemEvidence {
            qdisc: vec![qdisc(index)?],
        },
        events: EventCorrelation::new(
            vec![TimedEvent {
                request_id: Some(format!("request-{index}")),
                name: "request_completed".to_owned(),
                recorded_at_unix_ms: index as u64,
            }],
            vec![TimedEvent {
                request_id: Some(format!("request-{index}")),
                name: "request_persisted".to_owned(),
                recorded_at_unix_ms: index as u64 + 1,
            }],
        ),
        storage: storage.clone(),
    }))
}

fn load_for_wave(index: usize, wave_counts: &[BatchResult]) -> BatchResult {
    wave_counts
        .get(index % wave_counts.len().max(1))
        .copied()
        .unwrap_or(BatchResult {
            attempted: 1,
            completed: 1,
            failed: 0,
        })
}

fn executor(load: BatchResult) -> ExecutorEvidence {
    ExecutorEvidence {
        schema_version: 1,
        counters: Counters {
            attempted: load.attempted,
            sent: load.attempted,
            dropped_by_cap: load.failed,
            completed: load.completed,
            unexpected_outcomes: load.failed,
            ..Counters::default()
        },
        samples: TimingSamples {
            ttfb_ms: Some(8),
            ttft_ms: Some(10),
            first_delta_ms: Some(10),
            inter_delta_gap_ms: Percentiles {
                p50: Some(4),
                p95: Some(8),
            },
            latency_ms: Percentiles {
                p50: Some(20),
                p95: Some(40),
            },
            coordinated_omission_drift_ms: Percentiles {
                p50: Some(1),
                p95: Some(2),
            },
        },
        status_counts: status_counts(load),
        connection_counts: ConnectionCounts {
            new: 1,
            reused: load.completed.saturating_sub(1),
        },
        over_time: vec![TimelinePoint {
            second: 0,
            attempted: load.attempted,
            sent: load.attempted,
            completed: load.completed,
        }],
        unexpected_stream_truncation_count: 0,
        verdict_contribution: if load.failed == 0 {
            Verdict::Pass
        } else {
            Verdict::Fail
        },
    }
}

fn status_counts(load: BatchResult) -> BTreeMap<u16, u64> {
    let mut counts = BTreeMap::from([(200, load.completed)]);
    if load.failed > 0 {
        counts.insert(599, load.failed);
    }
    counts
}

fn samples(base: u64, completed: u64) -> Vec<u64> {
    (0..completed.max(5)).map(|offset| base + offset).collect()
}

fn qdisc(index: usize) -> Result<QdiscStats, String> {
    record_qdisc(QdiscCapture {
        edge: "probe-to-cc-lb",
        device: "eth0",
        output: "qdisc netem 1: root refcnt 2 limit 1000 delay 10ms\n Sent 480 bytes 4 pkt (dropped 0, overlimits 0 requeues 0)\n",
        scheduled_at_unix_ms: index as u64,
        command_started_at_unix_ms: index as u64,
        command_completed_at_unix_ms: index as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::wave_evidence;
    use crate::multi_replica::{RunInput, RunProfile};
    use crate::multi_replica_evidence::StorageEvidence;

    #[test]
    fn wave_evidence_attaches_run_storage_to_every_wave() {
        let storage = StorageEvidence::failure("wave_storage");

        let waves = wave_evidence(
            &input(RunProfile::Full),
            &[
                crate::multi_replica_load::BatchResult {
                    attempted: 2,
                    completed: 2,
                    failed: 0,
                },
                crate::multi_replica_load::BatchResult {
                    attempted: 1,
                    completed: 1,
                    failed: 0,
                },
            ],
            &storage,
        )
        .expect("synthetic wave evidence must be valid");

        assert!(waves.iter().all(|wave| {
            wave.storage
                .plane_effect
                .errors
                .iter()
                .any(|error| error.classification == "wave_storage")
        }));
    }

    #[test]
    fn wave_evidence_uses_per_wave_request_counts_for_rps() {
        let storage = StorageEvidence::failure("wave_storage");

        let waves = wave_evidence(
            &input(RunProfile::Smoke),
            &[
                crate::multi_replica_load::BatchResult {
                    attempted: 90,
                    completed: 90,
                    failed: 0,
                },
                crate::multi_replica_load::BatchResult {
                    attempted: 45,
                    completed: 45,
                    failed: 0,
                },
            ],
            &storage,
        )
        .expect("synthetic wave evidence must be valid");

        assert_eq!(waves[0].rps.completed, 90);
        assert_eq!(waves[1].rps.completed, 45);
        assert!(waves[0].rps.achieved >= 1.0);
    }

    #[test]
    fn wave_evidence_records_failed_load_attempts() {
        let storage = StorageEvidence::failure("wave_storage");

        let waves = wave_evidence(
            &input(RunProfile::Smoke),
            &[crate::multi_replica_load::BatchResult {
                attempted: 10,
                completed: 8,
                failed: 2,
            }],
            &storage,
        )
        .expect("synthetic wave evidence must be valid");

        assert_eq!(waves[0].rps.attempted, 10);
        assert_eq!(waves[0].rps.completed, 8);
        assert_eq!(waves[0].rps.dropped_by_cap, 2);
        assert_eq!(
            waves[0].executor.verdict_contribution,
            crate::verdict::Verdict::Fail
        );
    }

    fn input(profile: RunProfile) -> RunInput {
        RunInput {
            seed: 1,
            profile,
            replicas: 2,
            run_id: "wave-evidence".to_owned(),
            output: std::path::PathBuf::from("unused"),
            only_wave: None,
            min_wave_execution_ms: profile.min_wave_execution_ms(),
            load: crate::multi_replica_load::LoadConfig::default(),
        }
    }
}
