use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestCacheState {
    Hit,
    Write,
    Refresh,
    Miss,
    None,
    Unknown,
}
/// Provider-native cache evidence. `unreported` is distinct from a provider
/// reporting zero cached tokens; callers must not infer a miss from absence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestCacheEvidence {
    Unreported,
    Zero,
    Positive,
    Invalid,
}

impl RequestCacheEvidence {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unreported => "unreported",
            Self::Zero => "zero",
            Self::Positive => "positive",
            Self::Invalid => "invalid",
        }
    }
}

/// Bounded provider-native cache facts. Raw prompts, credentials, and raw
/// provider cache keys are intentionally not represented.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RequestNativeCacheObservation {
    pub provider: String,
    pub mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boundary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_source: Option<String>,
    pub cached_tokens: Option<u64>,
    pub write_tokens: Option<u64>,
    pub evidence: RequestCacheEvidence,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_unix_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub analysis_ms: Option<f64>,
}

impl Default for RequestNativeCacheObservation {
    fn default() -> Self {
        Self {
            provider: String::new(),
            mode: String::new(),
            scope: None,
            identity_hash: None,
            boundary: None,
            evidence_source: None,
            cached_tokens: None,
            write_tokens: None,
            evidence: RequestCacheEvidence::Unreported,
            retention_secs: None,
            expires_at_unix_secs: None,
            analysis_ms: None,
        }
    }
}

impl RequestCacheState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Write => "write",
            Self::Refresh => "refresh",
            Self::Miss => "miss",
            Self::None => "none",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestCacheBreakpoint {
    pub block_index: u64,
    pub source: RequestCacheBreakpointSource,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_index: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl: Option<String>,
    pub prefix_hash: String,
    /// Serialized byte length of the prefix up to this breakpoint, measured by
    /// `token_estimate_source`; a token estimate (see
    /// `PrefixBytes::as_token_estimate`), not a provider-reported token count.
    /// The JSON key keeps its historical name.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub prefix_token_count: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lookback_prefixes: Vec<RequestCacheLookbackPrefix>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_estimate_source: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestCacheLookbackPrefix {
    pub prefix_hash: String,
    pub content_block_index: u64,
    pub lookback_distance: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestCacheBreakpointSource {
    System,
    Tools,
    Message,
}

pub(crate) fn is_zero(value: &u64) -> bool {
    *value == 0
}
