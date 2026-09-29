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
