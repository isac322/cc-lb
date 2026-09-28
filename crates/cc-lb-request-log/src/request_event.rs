use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use uuid::Uuid;

use crate::{
    RequestCacheBreakpoint, RequestCacheState, RequestEventKind, RequestEventUpstream,
    cache::is_zero,
};

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
    pub upstream: Option<RequestEventUpstream>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_name: Option<String>,
    pub model: Option<String>,
    pub status: u16,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_creation_input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_creation_input_tokens_5m: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_creation_input_tokens_1h: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_read_input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_state: Option<RequestCacheState>,
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
    pub message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_index: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_control_block_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking_budget_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cache_control_message_indices: Vec<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cache_breakpoints: Vec<RequestCacheBreakpoint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_prefix_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_v3_cache_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub breakpoint_content_block_index: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_content_block_index: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lookback_distance: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicted_cache_read_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicted_cache_creation_tokens_5m: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicted_cache_creation_tokens_1h: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_estimate_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_value_micros: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub formula_winner_upstream_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kept_upstream_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota_urgency_5h: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota_urgency_7d: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota_urgency_combined: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota_warning_multiplier: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage_would_have_predicted_read_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage_would_have_picked_upstream_id: Option<Uuid>,
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
    pub limit_reconcile_ms: Option<u64>,
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
    pub first_body_chunk_ms: Option<u64>,
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
    pub stream_last_chunk_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_total_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sse_event_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_delta_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ping_count: Option<u64>,
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
    pub web_search_requests: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub web_fetch_requests: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inference_geo: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_error_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_error_message: Option<String>,
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
    fn legacy_request_event_defaults_request_latency_fields_to_missing() {
        let event: RequestEvent = serde_json::from_str(
            r#"{"status":200,"duration_ms":1,"request_body_wait_ms":null,"response_body_wait_ms":null}"#,
        )
        .expect("deserialize legacy request event");

        assert_eq!(event.request_body_read_ms, None);
        assert_eq!(event.request_body_bytes, None);
        assert_eq!(event.request_body_first_chunk_ms, None);
        assert_eq!(event.request_body_receive_ms, None);
        assert_eq!(event.request_body_wait_ms, None);
        assert_eq!(event.request_body_process_ms, None);
        assert_eq!(event.request_body_chunk_count, None);
        assert_eq!(event.finalize_ms, None);
        assert_eq!(event.response_body_wait_ms, None);
        assert_eq!(event.response_body_process_ms, None);
        assert_eq!(event.response_body_downstream_poll_gap_ms, None);
        assert_eq!(event.retry_overhead_ms, None);
        assert_eq!(event.json_parse_ms, None);
        assert_eq!(event.cache_structure_ms, None);
        assert_eq!(event.cache_token_key_ms, None);
        assert_eq!(event.cache_count_lookup_ms, None);
        assert_eq!(event.cache_tokenizer_queue_ms, None);
        assert_eq!(event.cache_serialize_ms, None);
        assert_eq!(event.cache_tokenize_ms, None);
        assert_eq!(event.prepare_signer_ms, None);

        let json = serde_json::to_value(&event).expect("serialize missing I/O timings");
        for field in [
            "request_body_first_chunk_ms",
            "request_body_receive_ms",
            "request_body_wait_ms",
            "request_body_process_ms",
            "request_body_chunk_count",
            "response_body_wait_ms",
            "response_body_process_ms",
            "response_body_downstream_poll_gap_ms",
            "retry_overhead_ms",
        ] {
            assert!(json.get(field).is_none(), "{field} must be omitted");
        }
    }

    #[test]
    fn request_event_source_metadata_defaults_without_legacy_json_fields() {
        let legacy_event = r#"{"status":200,"duration_ms":0}"#;
        let event: RequestEvent = serde_json::from_str(legacy_event).expect("deserialize legacy");

        assert_eq!(event.source_kind, None);
        assert_eq!(event.source_ref_id, None);

        let event_json = serde_json::to_value(&event).expect("serialize legacy");
        let event_object = event_json
            .as_object()
            .expect("request event serializes to object");
        assert!(!event_object.contains_key("source_kind"));
        assert!(!event_object.contains_key("source_ref_id"));
    }
}
