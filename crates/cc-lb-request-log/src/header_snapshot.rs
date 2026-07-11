use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Sanitized subset of upstream response headers carried by
/// `LifecycleEvent::UpstreamResponseStarted` (RFC-0002 §237-241).
///
/// Producers MUST NOT copy `Authorization` or similar credential-bearing
/// headers. The `anthropic_headers` map is a flat pass-through of every
/// header whose lowercased name starts with `anthropic-ratelimit-` OR
/// exactly matches one of the fixed Anthropic identity slots (see
/// `ANTHROPIC_IDENTITY_HEADERS`). Downstream subscribers parse these into
/// typed rate-limit and subscription-quota observations.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeaderSnapshot {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_encoding: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// `Retry-After` header. Anthropic sets this on 429/503.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after: Option<String>,
    /// All headers matching `anthropic-ratelimit-*` (any window/field
    /// combination — the vocabulary is open-ended, so we pass through
    /// raw values keyed by their lowercased header name) plus the fixed
    /// identity slots (`anthropic-organization-id`, etc.). Downstream
    /// subscribers reconstruct a `HeaderMap` and hand it to the existing
    /// `parse_anthropic_rate_limit_headers` / `parse_anthropic_unified_headers`
    /// parsers in `cc_lb_engine::rate_limit_headers`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub anthropic_headers: BTreeMap<String, String>,
}

impl HeaderSnapshot {
    pub fn is_empty(&self) -> bool {
        self.content_type.is_none()
            && self.content_encoding.is_none()
            && self.request_id.is_none()
            && self.retry_after.is_none()
            && self.anthropic_headers.is_empty()
    }
}

/// Cost breakdown in micro-USD, produced by the pricing subscriber.
///
/// Field semantics mirror `RequestEvent.cost_*_micros` for direct assembler
/// merge.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct CostBreakdown {
    pub total_micros: Option<i64>,
    pub input_micros: Option<i64>,
    pub output_micros: Option<i64>,
    pub cache_creation_5m_micros: Option<i64>,
    pub cache_creation_1h_micros: Option<i64>,
    pub cache_read_micros: Option<i64>,
}
