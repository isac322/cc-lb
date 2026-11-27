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
/// affinity state is `reasoning.encrypted_content` (or Codex backend WebSocket
/// chained `previous_response_id` with `store != true`) whose recorded owner
/// upstream is disabled, quota-exhausted, rate-limited (`429`), or otherwise
/// unavailable before the upstream starts generating a response.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum UnavailableOpenaiReasoningOwnerPolicy {
    /// Reject or stay pinned to the owner (relaying the owner's real upstream
    /// response such as `429`, or `409 upstream_affinity_unavailable` when
    /// unroutable) instead of stripping encrypted reasoning items or rerouting.
    FailClosed,
    /// Strip `reasoning.encrypted_content` items from `input` and fail over to
    /// another healthy non-exhausted eligible OpenAI upstream when the recorded
    /// owner is disabled, quota-exhausted (`subscription_hard_blocked`), or
    /// fails pre-processing (`429`, `401` after refresh, signer failure, or open
    /// circuit breaker) before a response starts—provided no `previous_response_id`,
    /// `conversation`, `prompt`, `compaction`, `compaction_summary`, or
    /// account-owned reference is present. On Codex backend WebSocket chained
    /// `previous_response_id` turns (`store != true`), evicts the owner and
    /// returns `400 previous_response_not_found` so the client replays full
    /// history onto a replacement upstream. When a multi-turn OpenAI Responses
    /// transcript carries `reasoning.encrypted_content` items from both an
    /// earlier failed-over owner and a later active owner, selects the
    /// surviving eligible owner, strips stale items from earlier owners, and
    /// preserves the surviving owner's reasoning verbatim. When no healthy
    /// replacement exists, preserves the owner's real response (for example
    /// `429`) or returns `409` if the owner itself cannot route.
    #[default]
    StripAndFailover,
}
