use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Per-candidate cache-affinity trace row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheAffinityCandidate {
    /// Upstream identifier this trace row describes.
    pub upstream_id: Uuid,
    /// Whether the candidate survived the filter.
    pub kept: bool,
    /// Predicted prompt-cache read tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicted_cache_read_tokens: Option<u32>,
    /// Predicted cache expiry in Unix seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicted_expires_at_unix_secs: Option<u64>,
}

/// Structured trace payload emitted by the built-in cache-affinity filter.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheAffinityTrace {
    /// One entry per candidate in filter input order.
    pub candidates: Vec<CacheAffinityCandidate>,
}
