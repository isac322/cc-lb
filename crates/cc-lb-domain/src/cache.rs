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

/// UTF-8 byte length of a serialized prompt-cache prefix.
///
/// Producers measure the byte length of the deterministic JSON serialization of
/// the request prefix up to a content block. The value is not a token count;
/// the only conversion to tokens is [`PrefixBytes::as_token_estimate`].
///
/// Persisted values carry the estimator identity in a separate source column.
/// Decode them only through [`PrefixBytes::from_persisted`], which rejects
/// values measured by any other estimator.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct PrefixBytes(u64);

impl PrefixBytes {
    /// Estimator identity persisted next to every stored prefix size.
    ///
    /// Changing what producers measure requires changing this constant, so
    /// values written under the previous estimator stop decoding.
    pub const ESTIMATE_SOURCE: &'static str = "serialized_prefix_bytes_v1";

    /// Wraps a byte length measured by the current estimator.
    pub const fn new(bytes: u64) -> Self {
        Self(bytes)
    }

    /// Returns the raw byte length.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Decodes a persisted value, accepting it only when `source` names the
    /// current estimator ([`PrefixBytes::ESTIMATE_SOURCE`]).
    pub fn from_persisted(value: u64, source: &str) -> Option<Self> {
        (source == Self::ESTIMATE_SOURCE).then_some(Self(value))
    }

    /// Saturating subtraction of two byte lengths.
    pub const fn saturating_sub(self, other: Self) -> Self {
        Self(self.0.saturating_sub(other.0))
    }

    /// Token-denominated estimate of this prefix, used for scoring, pricing,
    /// request events, and plugin wire `predicted_cache_*_tokens`.
    ///
    /// Returns the serialized byte length unchanged. A byte-level BPE token
    /// covers at least one byte, so this bounds byte-level BPE tokenization of
    /// the serialized JSON from above. It is NOT a bound on provider-billed
    /// tokens: image and document blocks are serialized as source references,
    /// so their billed tokens can exceed their serialized size. This is the
    /// only bytes-to-tokens conversion.
    pub const fn as_token_estimate(self) -> u64 {
        self.0
    }
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
    /// Serialized byte length of the prefix up to this breakpoint.
    #[serde(rename = "prefix_token_count")]
    pub prefix_bytes: PrefixBytes,
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
    /// Serialized byte length of the cached prefix, measured by
    /// [`PrefixBytes::ESTIMATE_SOURCE`].
    #[serde(rename = "estimated_prefix_tokens")]
    pub estimated_prefix_bytes: PrefixBytes,
    /// Hash schema version under which the key was derived.
    pub hash_schema_version: u8,
}

/// Cache utility prediction for routing decisions.
///
/// Token fields are estimates derived from serialized prefix bytes via
/// [`PrefixBytes::as_token_estimate`]; they are not provider-reported token counts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CacheScore {
    /// Token estimate derived from serialized prefix bytes (see
    /// [`PrefixBytes::as_token_estimate`]); not a provider-reported token count.
    pub predicted_cache_read_tokens: u32,
    /// Token estimate derived from serialized prefix bytes (see
    /// [`PrefixBytes::as_token_estimate`]); not a provider-reported token count.
    pub predicted_cache_creation_tokens_5m: u32,
    /// Token estimate derived from serialized prefix bytes (see
    /// [`PrefixBytes::as_token_estimate`]); not a provider-reported token count.
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

#[cfg(test)]
mod tests {
    use super::PrefixBytes;

    #[test]
    fn prefix_bytes_from_persisted_rejects_other_sources() {
        assert_eq!(
            PrefixBytes::from_persisted(4_096, PrefixBytes::ESTIMATE_SOURCE),
            Some(PrefixBytes::new(4_096))
        );
        for source in [
            "local_tiktoken_v1",
            "unknown",
            "",
            "serialized_prefix_bytes_v2",
        ] {
            assert_eq!(
                PrefixBytes::from_persisted(4_096, source),
                None,
                "source {source:?} must not decode as current-estimator bytes"
            );
        }
    }
}
