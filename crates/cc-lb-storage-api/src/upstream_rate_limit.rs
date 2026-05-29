use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::StorageResult;

/// Stub RateLimitKind enum for Wave 1 development.
/// Will be replaced with cc_lb_plugin_api::RateLimitKind once Task 1 is merged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RateLimitKind {
    /// Request rate limit.
    Requests,
    Tokens,
    /// Input token rate limit.
    InputTokens,
    /// Output token rate limit.
    OutputTokens,
}

impl RateLimitKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Requests => "requests",
            Self::Tokens => "tokens",
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
    /// Upstream identifier.
    pub upstream_id: Uuid,
    pub window: String,
    /// Rate limit kind (requests, input tokens, output tokens).
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
