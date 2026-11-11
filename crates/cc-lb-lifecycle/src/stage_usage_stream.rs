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
    /// Provider-native cache writes. `None` means unavailable; `Some(0)` is
    /// an explicitly reported zero.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write_input_tokens: Option<u64>,
    /// Native write validity is independent of Anthropic creation evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write_evidence: Option<cc_lb_request_log::RequestCacheEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write_input_tokens_30m: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write_input_tokens_12h: Option<u64>,
    pub web_search_requests: u64,
    pub web_fetch_requests: u64,
    pub service_tier: Option<String>,
    pub inference_geo: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iterations: Option<JsonValue>,
}

/// Where a usage snapshot was observed or finalized.
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
    /// Authoritative stream-end usage, retained like completion events at the
    /// durable assembler boundary rather than treated as droppable progress.
    Final,
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
    /// Provider-neutral average time between consecutive output frames.
    /// Anthropic SSE streams continue to use `inter_token_avg_ms`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_inter_output_avg_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_connection_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inter_token_avg_ms: Option<u64>,
}

/// Stream terminated on an error before completion (e.g. mid-stream
/// `event: error` payload from Anthropic).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamError {
    pub error_type: String,
    pub error_message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_error_code: Option<String>,
    /// Provider-neutral stream fields captured before an abnormal
    /// termination. These remain available to the lifecycle row even when
    /// the stream result is `Err`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_chunk_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_body_chunk_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_last_chunk_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_total_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ping_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_inter_output_avg_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_connection_id: Option<String>,
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
