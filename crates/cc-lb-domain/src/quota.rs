use serde::{Deserialize, Serialize};

/// Upstream rate-limit metric kind observed from upstream responses.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RateLimitKind {
    /// Request-count rate limit.
    Requests,
    /// Aggregate token rate limit.
    Tokens,
    /// Input-token rate limit.
    InputTokens,
    /// Output-token rate limit.
    OutputTokens,
}

impl RateLimitKind {
    /// Returns the stable snake_case wire name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Requests => "requests",
            Self::Tokens => "tokens",
            Self::InputTokens => "input_tokens",
            Self::OutputTokens => "output_tokens",
        }
    }
}

/// Latest upstream rate-limit observation exposed to router plugins.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RateLimitObservation {
    /// Observed rate-limit kind.
    pub kind: RateLimitKind,
    /// Provider-defined rate-limit window label.
    pub window: String,
    /// Optional maximum quota for the window.
    pub limit: Option<u64>,
    /// Optional remaining quota for the window.
    pub remaining: Option<u64>,
    /// Optional provider reset timestamp or duration string.
    pub reset: Option<String>,
}

/// Freshness state for subscription quota data exposed to router plugins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionQuotaDataState {
    /// Data is inside the configured routing freshness window.
    Fresh,
    /// Data exists but is older than the configured routing freshness window.
    Stale,
    /// No usable subscription quota data exists for the candidate/window.
    Missing,
}

/// Latest subscription quota snapshot for one candidate/window.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionQuotaCandidateSnapshot {
    /// Subscription quota window label.
    pub window: String,
    /// Freshness state for this window snapshot.
    pub state: SubscriptionQuotaDataState,
    /// Data source label, such as header, api, or merged.
    pub source: Option<String>,
    /// Provider-reported utilization fraction.
    pub utilization: Option<f64>,
    /// Provider-reported quota status.
    pub status: Option<String>,
    /// Provider reset timestamp in Unix seconds.
    pub resets_at_unix_secs: Option<u64>,
    /// Per-window threshold fraction that was crossed.
    pub surpassed_threshold: Option<f64>,
    /// Representative claim used for provenance/debugging.
    pub representative_claim: Option<String>,
    /// Provider reason the quota window is disabled.
    pub disabled_reason: Option<String>,
    /// Whether provider extra usage is enabled.
    pub extra_usage_enabled: Option<bool>,
    /// Provider extra-usage monthly credit limit.
    pub extra_usage_monthly_limit: Option<f64>,
    /// Provider extra-usage consumed credits.
    pub extra_usage_used_credits: Option<f64>,
    /// Observation timestamp in Unix milliseconds.
    pub observed_at_unix_millis: Option<u64>,
    /// Configured maximum age before this snapshot becomes stale.
    pub max_staleness_secs: u64,
    /// Whether the unified fallback signal is available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_available: Option<bool>,
    /// Whether unified overage is currently in use.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overage_in_use: Option<bool>,
    /// Monthly overage utilization fraction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overage_period_monthly_utilization: Option<f64>,
    /// Suggested upgrade paths.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upgrade_paths: Option<Vec<String>>,
}

/// Tier assigned by the subscription-preference filter to a candidate upstream.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionTier {
    /// All relevant base quota windows are fresh positive signals.
    KnownBase,
    /// At least one base window is a positive signal but not all.
    PartialBase,
    /// Base quotas are exhausted but extra usage is available.
    Overage,
    /// No signal exists, so a request probes the upstream's state.
    UnknownProbe,
}
