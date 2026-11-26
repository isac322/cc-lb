//! Upstream affinity policies for opaque provider-owned state.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Handling for OpenAI Responses requests whose only unestablished opaque
/// affinity state is `reasoning.encrypted_content` (and for unmatched pending
/// `reasoning.encrypted_content` on a resolved OpenAI owner).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum UnknownOpenaiReasoningPolicy {
    /// Fail closed with `409 upstream_affinity_unavailable`.
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

/// Handling for OpenAI Responses requests whose only referenced opaque
/// affinity state is `reasoning.encrypted_content` owned by an upstream that
/// is currently disabled or otherwise ineligible for routing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum UnavailableOpenaiReasoningOwnerPolicy {
    /// Reject with `409 upstream_affinity_unavailable` instead of stripping
    /// encrypted reasoning items and rerouting to another OpenAI upstream.
    FailClosed,
    /// Strip `reasoning.encrypted_content` items from `input` and fail over to
    /// another eligible OpenAI upstream when the recorded owner is ineligible
    /// and no `previous_response_id`, `conversation`, `compaction`, or
    /// `compaction_summary` is present; still fails closed with `409` if no
    /// other eligible OpenAI upstream exists.
    #[default]
    StripAndFailover,
}
