use uuid::Uuid;

use crate::{CacheKeepaliveConfigSnapshot, CacheTtl};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheKeepaliveEvidenceRow {
    pub evidence_id: String,
    pub cycle_id: Option<Uuid>,
    pub evidence_kind: String,
    pub request_start_ms: u64,
    pub prefix_fingerprint: Option<String>,
    pub prefix_hash: Option<String>,
    pub prefix_policy_version: Option<String>,
    pub prefix_schema_version: Option<String>,
    pub principal_id: String,
    pub upstream_id: Uuid,
    pub model: String,
    pub ttl: CacheTtl,
    pub hit_miss: Option<String>,
    pub cache_read_input_tokens: Option<u64>,
    pub cost_micros: Option<i64>,
    pub request_ref: Option<String>,
    pub event_ref: Option<String>,
    pub session_key_hash: Option<String>,
    pub cycle_anchor_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheKeepaliveTurnRow {
    pub source_ref_id: String,
    pub session_key_hash: String,
    pub principal_id: String,
    pub accounting_key_id: Option<String>,
    pub upstream_id: Uuid,
    pub cycle_id: Option<Uuid>,
    pub model: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_creation_input_tokens: u64,
    pub cache_creation_input_tokens_5m: u64,
    pub cache_creation_input_tokens_1h: u64,
    pub cache_read_input_tokens: u64,
    /// Actual cost when available; known signed values and zero remain distinct from `None`.
    pub cost_micros: Option<i64>,
    pub hit_miss: String,
    pub ts: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheKeepaliveDecisionRow {
    pub source_ref_id: String,
    pub principal_id: String,
    pub session_key_hash: Option<String>,
    pub upstream_id: Uuid,
    pub decision: String,
    pub reason: String,
    pub error: Option<String>,
    pub generation: u64,
    pub ttl: CacheTtl,
    pub config_snapshot: Option<CacheKeepaliveConfigSnapshot>,
    pub last_message_at_ms: u64,
    pub ts: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestEventProjections {
    pub turn: Option<CacheKeepaliveTurnRow>,
    pub decision: CacheKeepaliveDecisionRow,
    pub evidence: Vec<CacheKeepaliveEvidenceRow>,
}
