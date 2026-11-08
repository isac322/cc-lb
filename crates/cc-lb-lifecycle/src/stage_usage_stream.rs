use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

// ---------------------------------------------------------------------------
// Stage payloads: Usage
// ---------------------------------------------------------------------------

/// Compact snapshot of the Anthropic usage counters observed so far.
///
/// Mirrors the fields in `cc_lb_engine::usage_parser::UsageCounts` that are
/// safe to expose to subscribers.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct UsageSnapshot {
    pub input_tokens: u64,
    pub output_tokens: u64,
    /// Numeric OpenAI input image-token details, when reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_image_tokens: Option<u64>,
    /// Numeric OpenAI output image-token details, when reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_image_tokens: Option<u64>,
    /// Gemini `usageMetadata.toolUsePromptTokenCount`, when reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_use_prompt_tokens: Option<u64>,
    pub cache_creation_input_tokens: u64,
    pub cache_creation_input_tokens_5m: u64,
    pub cache_creation_input_tokens_1h: u64,
    pub cache_read_input_tokens: u64,
    /// Provider-reported cache evidence. `None` means the provider omitted
    /// the counter; zero is represented explicitly by `Some(Zero)`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_read_evidence: Option<cc_lb_request_log::RequestCacheEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_creation_evidence: Option<cc_lb_request_log::RequestCacheEvidence>,
    pub thinking_tokens: u64,
    pub web_search_requests: u64,
    pub web_fetch_requests: u64,
    pub service_tier: Option<String>,
    pub inference_geo: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iterations: Option<JsonValue>,
}

/// Which SSE frame produced the update.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum UsageSource {
    /// First-frame authoritative snapshot (`message_start`).
    MessageStart,
    /// Cumulative update (`message_delta.usage.*`).
    MessageDelta,
    /// Terminator (`message_stop`) boundary.
    MessageStop,
    /// Non-streaming JSON body top-level `usage.*`.
    NonStreamBody,
}

// ---------------------------------------------------------------------------
// Stage payloads: StreamCompleted
// ---------------------------------------------------------------------------

/// Stream ended normally.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct StreamSuccess {
    pub usage: UsageSnapshot,
    /// Whether `usage` contains measured counters, including explicit numeric zero.
    #[serde(default)]
    pub usage_present: bool,
    /// Number of SSE events observed on the tap.
    pub sse_event_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_chunk_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_body_chunk_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_message_start_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_content_block_start_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_first_content_delta_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_last_content_delta_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_message_stop_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_last_chunk_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_total_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_delta_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ping_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inter_token_avg_ms: Option<u64>,
}

/// Stream terminated on an error before completion (e.g. mid-stream
/// `event: error` payload from Anthropic).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamError {
    pub error_type: String,
    pub error_message: String,
}

#[cfg(test)]
mod tests {
    use super::{StreamSuccess, UsageSnapshot};

    #[test]
    fn stream_success_without_usage_presence_deserializes_as_unmeasured() {
        let success: StreamSuccess = serde_json::from_value(serde_json::json!({
            "usage": UsageSnapshot::default(),
            "sse_event_count": 0,
        }))
        .expect("legacy success payload deserializes");

        assert!(!success.usage_present);
        assert_eq!(success, StreamSuccess::default());
    }

    #[test]
    fn stream_success_roundtrip_preserves_measured_zero_usage() {
        let success = StreamSuccess {
            usage_present: true,
            ..Default::default()
        };

        let payload = serde_json::to_value(&success).expect("success payload serializes");
        let restored: StreamSuccess =
            serde_json::from_value(payload).expect("success payload deserializes");

        assert!(restored.usage_present);
        assert_eq!(restored.usage, UsageSnapshot::default());
        assert_eq!(restored, success);
    }
}
