use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use crate::{CacheScore, SubscriptionQuotaCandidateSnapshot};

/// Upstream backends supported by the proxy routing contract.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Upstream {
    /// Direct Anthropic API endpoint.
    AnthropicDirect {
        /// Operator-configured base URL override for this upstream.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        base_url: Option<Url>,
    },
}

/// Upstream record kind exposed to router plugins for candidate selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpstreamKind {
    /// Anthropic API-key upstream.
    AnthropicApiKey,
    /// Anthropic OAuth upstream.
    AnthropicOauth,
}

impl UpstreamKind {
    /// Returns the stable snake_case wire name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AnthropicApiKey => "anthropic_api_key",
            Self::AnthropicOauth => "anthropic_oauth",
        }
    }
}

/// Available upstream candidate for routing decisions.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UpstreamCandidate {
    /// Stable upstream identifier.
    pub upstream_id: Uuid,
    /// Operator-facing upstream name.
    pub name: String,
    /// Upstream kind used to select compatible routing strategies.
    pub kind: UpstreamKind,
    /// Latest subscription quota snapshots for this candidate.
    #[serde(default)]
    pub subscription_quotas: Vec<SubscriptionQuotaCandidateSnapshot>,
    /// Unix timestamp in seconds for the candidate observation snapshot.
    pub observed_at_unix_secs: u64,
    /// Predicted cache utility for this candidate, if available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_score: Option<CacheScore>,
    /// Resolved upstream base URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    /// Plan-capacity ratio relative to Claude Pro.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_capacity_ratio: Option<f64>,
    /// Anthropic-reported organization type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub organization_type: Option<String>,
    /// Anthropic-reported rate-limit tier.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limit_tier: Option<String>,
    /// Anthropic-reported seat tier for team plans.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seat_tier: Option<String>,
}

/// Stable registry id for the built-in subscription-preference router filter.
pub const BUILTIN_SUBSCRIPTION_PREFERENCE_ID: Uuid = Uuid::from_u128(2);
/// Stable registry name for the built-in subscription-preference router filter.
pub const BUILTIN_SUBSCRIPTION_PREFERENCE_NAME: &str = "subscription-preference";

/// Anthropic identity header slots projected into lifecycle header snapshots.
pub const ANTHROPIC_IDENTITY_HEADERS: &[&str] =
    &["anthropic-organization-id", "anthropic-account-uuid"];
