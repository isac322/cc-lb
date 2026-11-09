use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use uuid::Uuid;

use crate::{RequestCacheBreakpoint, RequestEventKind, cache::is_zero};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct RequestEvent {
    #[serde(default, skip_serializing_if = "is_zero")]
    pub ts: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_ref_id: Option<String>,
    /// Endpoint classification captured at the earliest request boundary.
    /// Absent on rows persisted before this field existed (historical
    /// `unclassified`); `source_kind = "renewal"` stays authoritative via
    /// [`RequestEventKind::effective`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_kind: Option<RequestEventKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ts_ms: Option<u64>,
    pub principal_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_id: Option<String>,
    pub principal_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_name: Option<String>,
    pub model: Option<String>,
    pub status: u16,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_use_prompt_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_creation_input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_creation_input_tokens_5m: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_creation_input_tokens_1h: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_read_input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write_input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write_input_tokens_30m: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write_input_tokens_12h: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claude_agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claude_parent_agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_app: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking_budget_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    /// Requested provider-native thinking level; metadata only, not a routing hint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking_level: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cache_control_message_indices: Vec<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cache_breakpoints: Vec<RequestCacheBreakpoint>,
    /// Provider-native cache facts. Anthropic breakpoint fields remain
    /// unchanged and are intentionally kept separate from this extension.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_cache: Option<crate::RequestNativeCacheObservation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_estimate_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_usd_micros: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_input_micros: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_output_micros: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_cache_creation_5m_micros: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_cache_creation_1h_micros: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_cache_read_micros: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_cache_write_micros: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_cache_write_30m_micros: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_cache_write_12h_micros: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit_reserve_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub json_parse_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_structure_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_token_key_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_count_lookup_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_tokenizer_queue_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_serialize_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_tokenize_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prepare_signer_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bulkhead_wait_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dns_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connect_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_reused: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_body_read_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_body_first_chunk_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_body_receive_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_body_wait_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_body_process_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_body_chunk_count: Option<u64>,
    /// Actual ingress request bytes collected before parsing. This is distinct
    /// from the response [`Self::body_bytes`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_body_bytes: Option<u64>,
    pub duration_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_setup_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shape_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sign_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_ttfb_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_body_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_body_wait_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_body_process_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_body_downstream_poll_gap_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_overhead_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finalize_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_chunk_count: Option<u64>,
    /// Response body bytes relayed to the client.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_bytes: Option<u64>,
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
    pub stream_first_output_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_last_chunk_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_total_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sse_event_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_delta_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ping_count: Option<u64>,
    /// Provider-neutral average time between consecutive output frames.
    /// Anthropic SSE rows continue to use `inter_token_avg_ms`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_inter_output_avg_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_connection_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inter_token_avg_ms: Option<u64>,
    pub error_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing_trace: Option<cc_lb_domain::RoutingTrace>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub internal_errors: Vec<cc_lb_domain::InternalError>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_error_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_error_message: Option<String>,
    /// Upstream HTTP version (`HTTP/1.1`, `HTTP/2.0`) for streaming responses
    /// whose upstream body ended abnormally.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_http_version: Option<String>,
    /// Provider response request identifier, such as `x-request-id`,
    /// `request-id`, or `x-goog-request-id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_content_encoding: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_content_length: Option<u64>,
    /// Raw (still-encoded) upstream body bytes received.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_body_bytes: Option<u64>,
    /// `transport_error`, `decode_error_after_clean_end`, or
    /// `decode_error_mid_body`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_body_end: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_body_error_cause: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_body_error_io_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_body_error_h2_reason: Option<String>,
    /// Optional warning or close diagnostic retained after a stream ended.
    /// This may describe a trailing body/decode failure or another
    /// provider-neutral transport condition and is not mirrored into
    /// `upstream_error_type`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_stream_warning_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_stream_warning_message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iterations: Option<JsonValue>,
}

#[cfg(test)]
mod tests {
    use super::RequestEvent;

    #[test]
    fn request_event_source_metadata_roundtrips() {
        let event = RequestEvent {
            source_kind: Some("proxy".to_owned()),
            source_ref_id: Some("ingress-123".to_owned()),
            ..RequestEvent::default()
        };

        let json = serde_json::to_string(&event).expect("serialize request event");
        let restored: RequestEvent =
            serde_json::from_str(&json).expect("deserialize request event");

        assert_eq!(event, restored);
    }

    #[test]
    fn request_latency_fields_roundtrip_measured_zero_and_partial_values() {
        let event = RequestEvent {
            request_body_read_ms: Some(0),
            request_body_bytes: Some(854_336),
            request_body_first_chunk_ms: Some(0.0),
            request_body_receive_ms: Some(8.5),
            request_body_wait_ms: Some(7.25),
            request_body_process_ms: Some(1.25),
            request_body_chunk_count: Some(0),
            finalize_ms: Some(7),
            response_body_wait_ms: Some(12.5),
            response_body_process_ms: Some(0.75),
            response_body_downstream_poll_gap_ms: Some(4.0),
            retry_overhead_ms: None,
            json_parse_ms: Some(0.125),
            cache_structure_ms: Some(0.0),
            cache_token_key_ms: Some(0.03125),
            cache_count_lookup_ms: Some(1.75),
            cache_tokenizer_queue_ms: Some(0.0625),
            cache_serialize_ms: Some(2.5),
            cache_tokenize_ms: None,
            prepare_signer_ms: Some(8.25),
            ..RequestEvent::default()
        };

        let json = serde_json::to_value(&event).expect("serialize request latency fields");
        assert_eq!(json["request_body_read_ms"], 0);
        assert_eq!(json["request_body_bytes"], 854_336);
        assert_eq!(json["request_body_first_chunk_ms"], 0.0);
        assert_eq!(json["request_body_chunk_count"], 0);
        assert_eq!(json["response_body_wait_ms"], 12.5);
        assert_eq!(json["response_body_process_ms"], 0.75);
        assert!(json.get("retry_overhead_ms").is_none());
        assert_eq!(json["finalize_ms"], 7);
        assert_eq!(json["json_parse_ms"], 0.125);
        assert_eq!(json["cache_structure_ms"], 0.0);
        assert!(json.get("cache_tokenize_ms").is_none());

        let restored: RequestEvent =
            serde_json::from_value(json).expect("deserialize request latency fields");
        assert_eq!(restored, event);
    }

    #[test]
    fn websocket_neutral_timing_and_provider_identity_roundtrip() {
        let event = RequestEvent {
            stream_first_output_ms: Some(8),
            stream_last_chunk_ms: Some(34),
            body_chunk_count: Some(4),
            stream_total_ms: Some(40),
            stream_inter_output_avg_ms: Some(8),
            provider_session_id: Some("resp_123".to_owned()),
            provider_connection_id: Some("conn_456".to_owned()),
            ..RequestEvent::default()
        };
        let json = serde_json::to_value(&event).expect("serialize websocket fields");
        assert_eq!(json["stream_first_output_ms"], 8);
        assert_eq!(json["stream_inter_output_avg_ms"], 8);
        assert_eq!(json["provider_session_id"], "resp_123");
        assert_eq!(json["provider_connection_id"], "conn_456");
        assert_eq!(
            serde_json::from_value::<RequestEvent>(json).expect("deserialize websocket fields"),
            event
        );
    }

    const DIAGNOSTIC_KEYS: [&str; 11] = [
        "upstream_http_version",
        "upstream_request_id",
        "upstream_content_encoding",
        "upstream_content_length",
        "upstream_body_bytes",
        "upstream_body_end",
        "upstream_body_error_cause",
        "upstream_body_error_io_kind",
        "upstream_body_error_h2_reason",
        "upstream_stream_warning_type",
        "upstream_stream_warning_message",
    ];

    #[test]
    fn legacy_payload_without_upstream_diagnostics_deserializes_to_none() {
        let legacy = serde_json::json!({
            "ts": 1_730_000_000_000_u64,
            "request_id": "req-legacy",
            "status": 200,
            "duration_ms": 345,
            "stream_message_stop_ms": 12,
        });
        let restored: RequestEvent =
            serde_json::from_value(legacy).expect("deserialize legacy payload");
        assert_eq!(restored.request_id, "req-legacy");
        assert_eq!(restored.stream_message_stop_ms, Some(12));
        assert_eq!(restored.upstream_http_version, None);
        assert_eq!(restored.upstream_request_id, None);
        assert_eq!(restored.upstream_content_encoding, None);
        assert_eq!(restored.upstream_content_length, None);
        assert_eq!(restored.upstream_body_bytes, None);
        assert_eq!(restored.upstream_body_end, None);
        assert_eq!(restored.upstream_body_error_cause, None);
        assert_eq!(restored.upstream_body_error_io_kind, None);
        assert_eq!(restored.upstream_body_error_h2_reason, None);
        assert_eq!(restored.upstream_stream_warning_type, None);
        assert_eq!(restored.upstream_stream_warning_message, None);
    }

    #[test]
    fn upstream_diagnostics_fields_absent_when_none() {
        let json = serde_json::to_value(RequestEvent::default()).expect("serialize default");
        for key in DIAGNOSTIC_KEYS {
            assert!(json.get(key).is_none(), "{key} must be skipped when None");
        }
    }

    #[test]
    fn upstream_diagnostics_fields_roundtrip() {
        let event = RequestEvent {
            upstream_http_version: Some("HTTP/2.0".to_owned()),
            upstream_request_id: Some("req_011".to_owned()),
            upstream_content_encoding: Some("gzip".to_owned()),
            upstream_content_length: Some(4096),
            upstream_body_bytes: Some(3_210),
            upstream_body_end: Some("transport_error".to_owned()),
            upstream_body_error_cause: Some("upstream_body_error".to_owned()),
            upstream_body_error_io_kind: Some("connection_reset".to_owned()),
            upstream_body_error_h2_reason: Some("INTERNAL_ERROR".to_owned()),
            upstream_stream_warning_type: Some("upstream_response_body_error".to_owned()),
            upstream_stream_warning_message: Some("body error".to_owned()),
            stream_message_stop_ms: Some(77),
            ..RequestEvent::default()
        };
        let json = serde_json::to_value(&event).expect("serialize diagnostics");
        for key in DIAGNOSTIC_KEYS {
            assert!(json.get(key).is_some(), "{key} must be serialized when set");
        }
        assert_eq!(json["upstream_body_end"], "transport_error");
        assert_eq!(json["upstream_body_bytes"], 3_210);
        let restored: RequestEvent = serde_json::from_value(json).expect("deserialize diagnostics");
        assert_eq!(restored, event);
    }
    #[test]
    fn tool_use_prompt_tokens_roundtrip_and_legacy_default() {
        let event = RequestEvent {
            tool_use_prompt_tokens: Some(0),
            ..RequestEvent::default()
        };
        let json = serde_json::to_value(&event).expect("serialize tool usage");
        assert_eq!(json["tool_use_prompt_tokens"], 0);
        let restored: RequestEvent = serde_json::from_value(json).expect("deserialize tool usage");
        assert_eq!(restored.tool_use_prompt_tokens, Some(0));
        let mut legacy =
            serde_json::to_value(RequestEvent::default()).expect("serialize legacy event");
        legacy
            .as_object_mut()
            .expect("request event serializes as an object")
            .remove("tool_use_prompt_tokens");
        let legacy: RequestEvent =
            serde_json::from_value(legacy).expect("deserialize legacy event");
        assert_eq!(legacy.tool_use_prompt_tokens, None);
    }
    #[test]
    fn native_cache_observation_serializes_without_raw_identity() {
        let event = RequestEvent {
            native_cache: Some(crate::RequestNativeCacheObservation {
                provider: "openai".to_owned(),
                mode: "implicit".to_owned(),
                scope: Some("org=o1;region=us".to_owned()),
                identity_hash: Some("sha256:abc".to_owned()),
                boundary: Some("latest_message=3".to_owned()),
                evidence_source: Some("provider_usage_metadata".to_owned()),
                cached_tokens: Some(256),
                write_tokens: None,
                evidence: crate::RequestCacheEvidence::Positive,
                retention_secs: Some(1_800),
                expires_at_unix_secs: None,
                analysis_ms: Some(0.5),
            }),
            ..RequestEvent::default()
        };
        let json = serde_json::to_value(&event).expect("serialize native cache");
        assert_eq!(json["native_cache"]["provider"], "openai");
        assert_eq!(json["native_cache"]["cached_tokens"], 256);
        assert!(json.to_string().contains("sha256:abc"));
        assert!(!json.to_string().contains("raw prompt"));
    }
}
