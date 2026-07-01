use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackendKind {
    Postgres,
    Sqlite,
}

impl BackendKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::Sqlite => "sqlite",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RequestEventStreamFilters {
    pub principal_id: Option<String>,
    pub model: Option<String>,
    pub upstream: Option<cc_lb_request_log::RequestEventUpstream>,
    pub upstream_id: Option<Uuid>,
    pub status_class: Option<StatusClass>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusClass {
    TwoXx,
    ThreeXx,
    FourXx,
    FiveXx,
}

impl StatusClass {
    pub fn matches(self, status: u16) -> bool {
        match self {
            Self::TwoXx => (200..=299).contains(&status),
            Self::ThreeXx => (300..=399).contains(&status),
            Self::FourXx => (400..=499).contains(&status),
            Self::FiveXx => (500..=599).contains(&status),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BucketKind {
    Requests,
    InputTokens,
    OutputTokens,
}

impl BucketKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Requests => "requests",
            Self::InputTokens => "input_tokens",
            Self::OutputTokens => "output_tokens",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalLimitKind {
    Requests,
    Tokens,
    InputTokens,
    OutputTokens,
}

impl PrincipalLimitKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Requests => "requests",
            Self::Tokens => "tokens",
            Self::InputTokens => "input_tokens",
            Self::OutputTokens => "output_tokens",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalLimitIdentityKind {
    Account,
    Credential,
    Unobserved,
}

impl PrincipalLimitIdentityKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Account => "account",
            Self::Credential => "credential",
            Self::Unobserved => "unobserved",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrincipalLimitState {
    pub principal_id: String,
    pub identity_kind: PrincipalLimitIdentityKind,
    pub identity_value: Option<String>,
    pub account_observed: bool,
    pub window: String,
    pub kind: PrincipalLimitKind,
    pub limit: Option<u64>,
    pub remaining: Option<u64>,
    pub reset: Option<String>,
    pub observed_at_unix_secs: u64,
    pub stored_at_unix_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ConfigDraftState {
    pub draft: Option<Value>,
    pub revision: u64,
    pub last_validated_revision: Option<u64>,
    pub last_validation_error: Option<String>,
    pub saved_at_unix_secs: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectiveConfig {
    pub revision: u64,
    pub config: Value,
    pub applied_at_unix_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistorySummary {
    pub tls_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub revision: u64,
    pub config_toml: String,
    pub applied_at_unix_secs: u64,
    pub summary: HistorySummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredHistoryEntry {
    pub config_toml: String,
    pub applied_at_unix_secs: u64,
    pub summary: HistorySummary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageRollupResolution {
    Minute,
    Hour,
}

impl UsageRollupResolution {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Minute => "minute",
            Self::Hour => "hour",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct UsageRollupKey {
    pub resolution: UsageRollupResolution,
    pub bucket_start: u64,
    pub principal: String,
    pub upstream_id: Uuid,
    pub upstream_name: String,
    pub model: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageRollup {
    pub resolution: UsageRollupResolution,
    pub bucket_start: u64,
    pub principal: String,
    pub upstream_id: Uuid,
    pub upstream_name: String,
    pub model: String,
    pub request_count: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    #[serde(default)]
    pub cache_creation_input_tokens: u64,
    #[serde(default)]
    pub cache_read_input_tokens: u64,
    pub error_count: u64,
    pub latency_count: u64,
    pub latency_ms_sum: u64,
    pub latency_ms_min: Option<u64>,
    pub latency_ms_max: Option<u64>,
    #[serde(default)]
    pub proxy_setup_ms_count: u64,
    #[serde(default)]
    pub proxy_setup_ms_sum: u64,
    #[serde(default)]
    pub shape_ms_count: u64,
    #[serde(default)]
    pub shape_ms_sum: u64,
    #[serde(default)]
    pub sign_ms_count: u64,
    #[serde(default)]
    pub sign_ms_sum: u64,
    #[serde(default)]
    pub upstream_ttfb_ms_count: u64,
    #[serde(default)]
    pub upstream_ttfb_ms_sum: u64,
    #[serde(default)]
    pub upstream_body_ms_count: u64,
    #[serde(default)]
    pub upstream_body_ms_sum: u64,
    pub virtual_cost_micros: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageTokenInterval {
    pub interval_id: u64,
    pub upstream_id: Uuid,
    pub start_unix_secs: u64,
    pub end_unix_secs: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageTokenIntervalSum {
    pub interval_id: u64,
    pub tokens: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageRollupRun {
    pub processed_events: u64,
    pub updated_rollups: u64,
    pub checkpoint: Option<u64>,
}
