use serde::{Deserialize, Serialize};

use crate::comparison_report::ComparisonSummary;
use crate::evidence_environment::EnvironmentManifest;
use crate::evidence_schema::{EVIDENCE_SCHEMA_VERSION, ReplicaProcessMetrics, WaveEvidence};
use crate::verdict::Verdict;

#[derive(Debug, Deserialize, Serialize)]
pub struct RunEvidence {
    pub schema_version: u16,
    pub verdict: Verdict,
    pub profile: String,
    pub run_id: String,
    pub setup_ms: Option<u64>,
    pub wave_execution_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_stage: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<String>,
    pub waves: Vec<WaveEvidence>,
    pub environment: EnvironmentManifest,
    pub replica_process_metrics: Vec<ReplicaProcessMetrics>,
    pub replicas: Vec<ReplicaEvidence>,
    pub storage: StorageEvidence,
    pub auth_wave: AuthWaveEvidence,
    pub per_replica_limits: Vec<PerReplicaLimit>,
    pub per_replica_limit_check: PerReplicaLimitCheck,
    pub comparison: ComparisonSummary,
    pub cleanup: RunCleanupEvidence,
}

impl RunEvidence {
    pub fn new(profile: &str, run_id: &str) -> Self {
        Self {
            schema_version: EVIDENCE_SCHEMA_VERSION,
            verdict: Verdict::Pass,
            profile: profile.to_owned(),
            run_id: run_id.to_owned(),
            setup_ms: None,
            wave_execution_ms: None,
            blocked_stage: None,
            blocked_reason: None,
            waves: Vec::new(),
            environment: EnvironmentManifest::synthetic(),
            replica_process_metrics: Vec::new(),
            replicas: Vec::new(),
            storage: StorageEvidence::failure("not_executed"),
            auth_wave: AuthWaveEvidence {
                managed_key: false,
                cross_replica_usable: false,
            },
            per_replica_limits: Vec::new(),
            per_replica_limit_check: PerReplicaLimitCheck {
                ok: false,
                reason: Some("not_executed".to_owned()),
            },
            comparison: crate::comparison_report::empty_comparison(),
            cleanup: RunCleanupEvidence {
                attempted: false,
                labeled_containers_remaining: 0,
                labeled_networks_remaining: 0,
            },
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ReplicaEvidence {
    pub name: String,
    pub proxy_port: u16,
    pub admin_port: u16,
    pub metrics_port: u16,
    pub requests: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AuthWaveEvidence {
    pub managed_key: bool,
    pub cross_replica_usable: bool,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RunCleanupEvidence {
    pub attempted: bool,
    pub labeled_containers_remaining: u64,
    pub labeled_networks_remaining: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StorageEvidence {
    pub backend: String,
    pub plane_effect: StoragePlaneEffect,
    pub hotpath_effect: HotpathEffect,
}

impl StorageEvidence {
    pub fn failure(classification: &str) -> Self {
        Self {
            backend: "postgres".to_owned(),
            plane_effect: StoragePlaneEffect {
                classified: true,
                errors: vec![StorageError {
                    source: "orchestration".to_owned(),
                    classification: classification.to_owned(),
                }],
                admin_mutation_during_impairment: false,
                propagation_latency_ms: Some(0),
                cross_replica_key_usable: false,
                request_event_persistence_gap: 0,
                listen_notify_reconnect_or_drop_observed: false,
                storage_tail_backlog_rows: 0,
                storage_tail_lag_ms: Some(0),
            },
            hotpath_effect: HotpathEffect {
                managed_key_requests: 0,
                healthy_requests: 0,
                unexpected_5xx_on_healthy: 0,
                storage_errors_classified: 0,
            },
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.backend != "postgres" {
            return Err("multi-replica storage backend must be postgres".to_owned());
        }
        self.plane_effect.validate()?;
        self.hotpath_effect.validate()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StoragePlaneEffect {
    pub classified: bool,
    pub errors: Vec<StorageError>,
    pub admin_mutation_during_impairment: bool,
    pub propagation_latency_ms: Option<u64>,
    pub cross_replica_key_usable: bool,
    pub request_event_persistence_gap: u64,
    pub listen_notify_reconnect_or_drop_observed: bool,
    pub storage_tail_backlog_rows: u64,
    pub storage_tail_lag_ms: Option<u64>,
}

impl StoragePlaneEffect {
    #[cfg(test)]
    pub const fn healthy() -> Self {
        Self {
            classified: true,
            errors: Vec::new(),
            admin_mutation_during_impairment: true,
            propagation_latency_ms: Some(0),
            cross_replica_key_usable: true,
            request_event_persistence_gap: 0,
            listen_notify_reconnect_or_drop_observed: true,
            storage_tail_backlog_rows: 0,
            storage_tail_lag_ms: Some(0),
        }
    }

    fn validate(&self) -> Result<(), String> {
        if !self.classified {
            return Err("storage plane errors must be classified".to_owned());
        }
        if self
            .errors
            .iter()
            .any(|error| error.classification.is_empty())
        {
            return Err("storage plane error classification is missing".to_owned());
        }
        if self.propagation_latency_ms.is_none() || self.storage_tail_lag_ms.is_none() {
            return Err("storage plane probes are incomplete".to_owned());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StorageError {
    pub source: String,
    pub classification: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HotpathEffect {
    pub managed_key_requests: u64,
    pub healthy_requests: u64,
    pub unexpected_5xx_on_healthy: u64,
    pub storage_errors_classified: u64,
}

impl HotpathEffect {
    #[cfg(test)]
    pub const fn healthy() -> Self {
        Self {
            managed_key_requests: 1,
            healthy_requests: 1,
            unexpected_5xx_on_healthy: 0,
            storage_errors_classified: 0,
        }
    }

    fn validate(&self) -> Result<(), String> {
        if self.managed_key_requests == 0 || self.healthy_requests == 0 {
            return Err("managed-key hot path was not exercised".to_owned());
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PerReplicaLimit {
    pub replica: String,
    pub allowed: u64,
    pub observed: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PerReplicaLimitCheck {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl PerReplicaLimit {
    pub fn new(replica: &str, allowed: u64, observed: u64) -> Self {
        Self {
            replica: replica.to_owned(),
            allowed,
            observed,
        }
    }

    pub fn validate_all(limits: &[Self]) -> Result<(), String> {
        if limits.is_empty() {
            return Err("per-replica limit evidence is empty".to_owned());
        }
        if limits.iter().any(|limit| limit.observed > limit.allowed) {
            return Err("per-replica limit exceeded".to_owned());
        }
        Ok(())
    }
}
