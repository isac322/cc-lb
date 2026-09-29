use serde::{Deserialize, Serialize};

/// Prompt cache TTL class: immutable after entry creation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TtlClass {
    /// 5-minute TTL cache entry.
    #[default]
    Ephemeral5m,
    /// 1-hour TTL cache entry.
    Ephemeral1h,
}

/// Source of a cache breakpoint within the request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheBreakpointSource {
    /// Breakpoint from tools in the request.
    Tools,
    /// Breakpoint from system content.
    System,
    /// Breakpoint from message content.
    Message,
}

/// One v3 content-block lookback prefix that Anthropic may read for a breakpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheLookbackPrefix {
    /// Proxy-local v3 prefix key for this content-block prefix.
    pub prefix_hash: String,
    /// Content-block index in the flattened request sequence.
    pub content_block_index: u32,
    /// Distance from the requested breakpoint.
    pub lookback_distance: u32,
}

/// Cache breakpoint position in the request, for prompt cache optimization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheBreakpoint {
    /// Block index of the breakpoint.
    pub block_index: u32,
    /// Source of the breakpoint.
    pub source: CacheBreakpointSource,
    /// Dot-separated path within the request.
    pub path: String,
    /// Message index for message content.
    pub message_index: Option<u32>,
    /// Content hash of the prefix up to this breakpoint.
    pub prefix_hash: String,
    /// Token count of the prefix up to this breakpoint.
    pub prefix_token_count: u64,
    /// Requested TTL class for this breakpoint.
    pub requested_ttl: TtlClass,
    /// V3 lookback candidates in N through N-19 order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lookback_prefixes: Vec<CacheLookbackPrefix>,
    /// Source of the prefix-token estimate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_estimate_source: Option<String>,
}

/// Warm cache entry eligible for reuse in upstream requests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WarmCacheEntry {
    /// Content hash of the cached prefix.
    pub prefix_hash: String,
    /// Unix timestamp in seconds when this entry expires.
    pub expires_at_unix_secs: u64,
    /// TTL class of this cache entry.
    pub ttl_class: TtlClass,
    /// Last observed usage time in Unix seconds.
    pub last_observed_at_unix_secs: u64,
    /// Content-block index of the cached prefix.
    pub content_block_index: u32,
    /// Estimated prefix tokens for this cached prefix.
    pub estimated_prefix_tokens: u64,
    /// Source identifier of the prefix-token estimate.
    pub token_estimate_source: String,
    /// Hash schema version under which the key was derived.
    pub hash_schema_version: u8,
}

/// Cache utility prediction for routing decisions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CacheScore {
    /// Predicted input tokens read from cache.
    pub predicted_cache_read_tokens: u32,
    /// Predicted input tokens written to 5-minute cache.
    pub predicted_cache_creation_tokens_5m: u32,
    /// Predicted input tokens written to 1-hour cache.
    pub predicted_cache_creation_tokens_1h: u32,
    /// Predicted input tokens not read from cache.
    pub predicted_uncached_input_tokens: u32,
    /// Predicted Unix timestamp when the cache entry expires.
    pub predicted_expires_at_unix_secs: Option<u64>,
    /// Index of the selected cache breakpoint.
    pub matched_breakpoint_index: Option<u32>,
    /// Confidence score for this prediction.
    pub confidence: f32,
    /// Explanation for ambiguous predictions.
    pub ambiguity_reason: Option<String>,
    /// Matched proxy-local v3 cache key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_v3_cache_key: Option<String>,
    /// Requested breakpoint content-block index.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub breakpoint_content_block_index: Option<u32>,
    /// Matched content-block index.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_content_block_index: Option<u32>,
    /// Distance from breakpoint to matched block.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lookback_distance: Option<u32>,
    /// Source of the prefix-token estimate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_estimate_source: Option<String>,
}

/// Model-specific cache/input pricing exposed to router plugins.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CachePricingSummary {
    /// Pricing availability status.
    pub status: String,
    /// Base input token price in micros USD per million tokens.
    pub input_micros_per_million: Option<u64>,
    /// 5-minute cache creation price.
    pub cache_creation_5m_micros_per_million: Option<u64>,
    /// 1-hour cache creation price.
    pub cache_creation_1h_micros_per_million: Option<u64>,
    /// Cache read token price.
    pub cache_read_micros_per_million: Option<u64>,
}

impl Default for CachePricingSummary {
    fn default() -> Self {
        Self {
            status: "unknown".to_owned(),
            input_micros_per_million: None,
            cache_creation_5m_micros_per_million: None,
            cache_creation_1h_micros_per_million: None,
            cache_read_micros_per_million: None,
        }
    }
}
