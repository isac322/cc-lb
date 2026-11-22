//! Upstream affinity policies for opaque provider-owned state.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Handling for OpenAI Responses requests whose only unestablished opaque
/// affinity state is `reasoning.encrypted_content` (and for unmatched pending
/// `reasoning.encrypted_content` on a resolved OpenAI owner).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum UnknownOpenaiReasoningPolicy {
    /// Fail closed with `503 upstream_affinity_unavailable`.
    FailClosed,
    /// Preserve unknown `reasoning.encrypted_content` verbatim when a single
    /// eligible OpenAI upstream exists (or an owner is already resolved) and
    /// retry once with only the unmatched `reasoning` items removed if the
    /// upstream rejects with `invalid_encrypted_content`; when multiple
    /// eligible OpenAI upstreams exist and no owner is established, strip
    /// unknown `reasoning` items before dispatch instead of guessing.
    #[default]
    Hybrid,
}
