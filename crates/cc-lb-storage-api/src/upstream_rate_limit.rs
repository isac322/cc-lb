use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::StorageResult;

/// Stub RateLimitKind enum for Wave 1 development.
/// Will be replaced with cc_lb_plugin_api::RateLimitKind once Task 1 is merged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RateLimitKind {
    /// Request rate limit.
    Requests,
    /// Input token rate limit.
    InputTokens,
    /// Output token rate limit.
    OutputTokens,
}

impl RateLimitKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Requests => "requests",
            Self::InputTokens => "input_tokens",
            Self::OutputTokens => "output_tokens",
        }
    }
}

/// Observation record for upstream rate limit state.
///
/// Tracks rate limit observations emitted by upstreams, used to
/// maintain downstream quotas and enforce global limits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpstreamRateLimitObservationRecord {
    /// Observation timestamp in milliseconds since Unix epoch.
    pub ts_ms: u64,
    /// Upstream identifier.
    pub upstream_id: String,
    /// Rate limit kind (requests, input tokens, output tokens).
    pub kind: RateLimitKind,
    /// Remaining quota reported by upstream.
    pub remaining: u64,
    /// Reset timestamp in milliseconds since Unix epoch.
    pub reset_at_ms: u64,
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
        upstream_ids: &[String],
    ) -> StorageResult<Vec<UpstreamRateLimitObservationRecord>>;
}
