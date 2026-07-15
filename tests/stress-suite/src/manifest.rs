//! Versioned materialized stress plans. Determinism covers the generated command and schedule
//! sequence, not packet timing or other runtime outcomes observed during execution.

use std::collections::BTreeMap;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::topology_decision::TopologyDecision;
use crate::traffic::{ExpectedLabel, ExpectedStatusClass, RequestBodyShape, SlowReaderPolicy};
use crate::verdict::EvidenceSkeleton;

pub const SCHEMA_VERSION: u16 = 2;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Profile {
    Smoke,
    Soak,
    Burst,
    Leak,
}

impl Profile {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Soak => "soak",
            Self::Burst => "burst",
            Self::Leak => "leak",
        }
    }
}

impl FromStr for Profile {
    type Err = ProfileError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "smoke" => Ok(Self::Smoke),
            "soak" => Ok(Self::Soak),
            "burst" => Ok(Self::Burst),
            "leak" => Ok(Self::Leak),
            _ => Err(ProfileError::Unsupported(value.to_owned())),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProfileError {
    #[error("unsupported stress profile {0}")]
    Unsupported(String),
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ManifestLifecycle {
    Planned,
    Finalized,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SymbolicNode {
    Probe,
    CcLb,
    Postgres,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GeneratorIdentity {
    pub version: String,
    pub content_hash: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DirectedTopology {
    pub lifecycle: ManifestLifecycle,
    pub symbolic_nodes: Vec<SymbolicNode>,
    pub edges: Vec<DirectedEdge>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DirectedEdge {
    pub edge_id: String,
    pub sender: SymbolicNode,
    pub receiver: SymbolicNode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination_ipv4: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub classid: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Wave {
    pub wave_id: String,
    pub starts_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NetemScheduleEntry {
    pub edge_id: String,
    pub wave_id: String,
    pub trigger: String,
    pub seed: u64,
    pub command_reference: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PlannedRequest {
    pub request_id: String,
    pub scheduled_send_at_ms: u64,
    pub wave_id: String,
    pub persona_id: String,
    pub principal_id: String,
    pub session_id: String,
    pub body: String,
    pub body_hash: String,
    pub body_shape: RequestBodyShape,
    pub stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub hot_prefix_group: String,
    pub provider_headers: BTreeMap<String, String>,
    pub slow_reader_policy: SlowReaderPolicy,
    pub fake_script_id: String,
    pub expected_label: ExpectedLabel,
    pub expected_status_class: ExpectedStatusClass,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Persona {
    pub persona_id: String,
    pub principal_id: String,
    pub session_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Principal {
    pub principal_id: String,
    pub credential_reference: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Session {
    pub session_id: String,
    pub persona_id: String,
    pub principal_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FakeScriptReference {
    pub fake_script_id: String,
    pub path: String,
    pub content_hash: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StorageSchedule {
    pub backend: String,
    pub reset_at_wave: String,
    pub collect_after_wave: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Manifest {
    pub schema_version: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generated_at: Option<u64>,
    pub generator: GeneratorIdentity,
    pub seed: u64,
    pub profile: Profile,
    pub run_id: String,
    pub topology: DirectedTopology,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topology_decision: Option<TopologyDecision>,
    pub wave_schedule: Vec<Wave>,
    pub netem_schedule: Vec<NetemScheduleEntry>,
    pub requests: Vec<PlannedRequest>,
    pub personas: Vec<Persona>,
    pub principals: Vec<Principal>,
    pub sessions: Vec<Session>,
    pub fake_scripts: Vec<FakeScriptReference>,
    pub storage_schedule: StorageSchedule,
    pub environment_compat_keys: BTreeMap<String, String>,
    pub evidence_skeleton: EvidenceSkeleton,
    pub schedule_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub integrity_hash: Option<String>,
}

impl Manifest {
    #[cfg(test)]
    pub fn normalized_canonical_json(&self) -> Result<Vec<u8>, ManifestError> {
        let mut normalized = self.clone();
        normalized.generated_at = None;
        serde_json::to_vec(&normalized).map_err(ManifestError::Serialize)
    }

    pub fn computed_schedule_hash(&self) -> Result<String, ManifestError> {
        let schedule = ScheduleMaterial {
            waves: &self.wave_schedule,
            netem: &self.netem_schedule,
            requests: &self.requests,
            storage: &self.storage_schedule,
        };
        serde_json::to_vec(&schedule)
            .map(|bytes| Self::sha256_hex(&bytes))
            .map_err(ManifestError::Serialize)
    }

    pub fn computed_integrity_hash(&self) -> Result<String, ManifestError> {
        let mut unsigned = self.clone();
        unsigned.generated_at = None;
        unsigned.integrity_hash = None;
        serde_json::to_vec(&unsigned)
            .map(|bytes| Self::sha256_hex(&bytes))
            .map_err(ManifestError::Serialize)
    }

    pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
        hex::encode(Sha256::digest(bytes))
    }
}

#[derive(Serialize)]
struct ScheduleMaterial<'a> {
    waves: &'a [Wave],
    netem: &'a [NetemScheduleEntry],
    requests: &'a [PlannedRequest],
    storage: &'a StorageSchedule,
}

#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error("manifest serialization failed: {0}")]
    Serialize(serde_json::Error),
}
