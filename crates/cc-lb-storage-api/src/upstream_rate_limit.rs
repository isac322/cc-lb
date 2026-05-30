use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::StorageResult;

/// Re-export of [`cc_lb_plugin_api::RateLimitKind`] so that storage-layer code
/// and plugin-facing code share a single canonical enum (no stub, no mapping).
pub use cc_lb_plugin_api::RateLimitKind;

/// Observation record for upstream rate limit state.
///
/// Tracks rate limit observations emitted by upstreams, used to
/// maintain downstream quotas and enforce global limits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpstreamRateLimitObservationRecord {
    /// Upstream identifier.
    pub upstream_id: Uuid,
    pub window: String,
    /// Rate limit kind (requests, tokens, input tokens, output tokens).
    pub kind: RateLimitKind,
    pub limit: Option<u64>,
    /// Remaining quota reported by upstream.
    pub remaining: Option<u64>,
    pub reset: Option<String>,
    pub observed_at_unix_secs: u64,
}

#[async_trait]
pub trait UpstreamRateLimitStateStore: Send + Sync {
    /// Store a rate limit observation from an upstream.
    async fn put_observation(
        &self,
        record: &UpstreamRateLimitObservationRecord,
    ) -> StorageResult<()>;

    /// List all observations for the given upstream identifiers.
    async fn list_for_upstream_ids(
        &self,
        upstream_ids: &[Uuid],
    ) -> StorageResult<Vec<UpstreamRateLimitObservationRecord>>;
}
