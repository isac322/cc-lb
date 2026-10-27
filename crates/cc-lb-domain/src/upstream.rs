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
    /// Direct OpenAI API endpoint.
    OpenaiDirect {
        /// Operator-configured base URL override for this upstream.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        base_url: Option<Url>,
    },
    /// Direct Gemini API endpoint.
    GeminiDirect {
        /// Operator-configured base URL override for this upstream.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        base_url: Option<Url>,
    },
}

/// Provider implemented by an upstream.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provider {
    /// Anthropic API provider.
    Anthropic,
    /// OpenAI API provider.
    Openai,
    /// Gemini API provider.
    Gemini,
}

/// Credential kind used by an upstream.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialKind {
    /// Anthropic API key credential.
    ApiKey,
    /// Anthropic OAuth credential.
    Oauth,
}

/// Upstream record kind exposed to router plugins for candidate selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpstreamKind {
    /// Anthropic API-key upstream.
    AnthropicApiKey,
    /// Anthropic OAuth upstream.
    AnthropicOauth,
    /// OpenAI API-key upstream.
    OpenaiApiKey,
    /// OpenAI OAuth subscription upstream.
    OpenaiOauth,
    /// Gemini API-key upstream.
    GeminiApiKey,
    /// Gemini OAuth subscription upstream.
    GeminiOauth,
}
impl UpstreamKind {
    /// Returns the provider implemented by this upstream.
    pub const fn provider(self) -> Provider {
        match self {
            Self::AnthropicApiKey | Self::AnthropicOauth => Provider::Anthropic,
            Self::OpenaiApiKey | Self::OpenaiOauth => Provider::Openai,
            Self::GeminiApiKey | Self::GeminiOauth => Provider::Gemini,
        }
    }

    /// Returns the credential kind used by this upstream.
    pub const fn credential_kind(self) -> CredentialKind {
        match self {
            Self::AnthropicApiKey | Self::OpenaiApiKey | Self::GeminiApiKey => {
                CredentialKind::ApiKey
            }
            Self::AnthropicOauth | Self::OpenaiOauth | Self::GeminiOauth => CredentialKind::Oauth,
        }
    }

    /// Returns the stable snake_case wire name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AnthropicApiKey => "anthropic_api_key",
            Self::AnthropicOauth => "anthropic_oauth",
            Self::OpenaiApiKey => "openai_api_key",
            Self::OpenaiOauth => "openai_oauth",
            Self::GeminiApiKey => "gemini_api_key",
            Self::GeminiOauth => "gemini_oauth",
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

/// Anthropic request/response header naming the workspace an org-scoped API key acts in.
pub const ANTHROPIC_WORKSPACE_ID_HEADER: &str = "anthropic-workspace-id";

/// Anthropic identity header slots projected into lifecycle header snapshots.
pub const ANTHROPIC_IDENTITY_HEADERS: &[&str] = &[
    "anthropic-organization-id",
    "anthropic-account-uuid",
    ANTHROPIC_WORKSPACE_ID_HEADER,
];

#[cfg(test)]
mod tests {
    use super::UpstreamKind;
    use crate::{CredentialKind, Provider};

    #[test]
    fn anthropic_api_key_has_provider_neutral_classification() {
        assert_eq!(
            UpstreamKind::AnthropicApiKey.provider(),
            Provider::Anthropic
        );
        assert_eq!(
            UpstreamKind::AnthropicApiKey.credential_kind(),
            CredentialKind::ApiKey
        );
    }

    #[test]
    fn anthropic_oauth_has_provider_neutral_classification() {
        assert_eq!(UpstreamKind::AnthropicOauth.provider(), Provider::Anthropic);
        assert_eq!(
            UpstreamKind::AnthropicOauth.credential_kind(),
            CredentialKind::Oauth
        );
    }

    #[test]
    fn gemini_oauth_has_gemini_oauth_classification() {
        assert_eq!(UpstreamKind::GeminiOauth.provider(), Provider::Gemini);
        assert_eq!(
            UpstreamKind::GeminiOauth.credential_kind(),
            CredentialKind::Oauth
        );
        assert_eq!(UpstreamKind::GeminiOauth.as_str(), "gemini_oauth");
    }

    #[test]
    fn upstream_kind_wire_names_remain_exact() {
        assert_eq!(UpstreamKind::AnthropicApiKey.as_str(), "anthropic_api_key");
        assert_eq!(UpstreamKind::AnthropicOauth.as_str(), "anthropic_oauth");
    }
}
