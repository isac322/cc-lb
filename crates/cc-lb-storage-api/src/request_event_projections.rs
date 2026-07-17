use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheKeepaliveTurnRow {
    pub source_ref_id: String,
    pub session_key_hash: String,
    pub principal_id: String,
    pub accounting_key_id: Option<String>,
    pub upstream_id: Uuid,
    pub model: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_creation_input_tokens: u64,
    pub cache_creation_input_tokens_5m: u64,
    pub cache_creation_input_tokens_1h: u64,
    pub cache_read_input_tokens: u64,
    pub cost_micros: i64,
    pub hit_miss: String,
    pub ts: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheKeepaliveDecisionRow {
    pub source_ref_id: String,
    pub decision: String,
    pub reason: String,
    pub generation: u64,
    pub ts: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestEventProjections {
    pub turn: CacheKeepaliveTurnRow,
    pub decision: CacheKeepaliveDecisionRow,
}
