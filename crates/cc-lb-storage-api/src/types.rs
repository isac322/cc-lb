pub use crate::audit::AuditEntry;
pub use crate::limits::{KeyStatus, Limit, LimitKind};
pub use crate::storage_types_common::*;
pub use crate::storage_types_keys::*;
pub use cc_lb_domain::PrincipalKindLite;
pub use cc_lb_request_log::{
    FinalRequestEventUpdate, RequestCacheBreakpoint, RequestCacheBreakpointSource,
    RequestCacheState, RequestEvent, RequestEventPartial, RequestEventPhase, RequestEventUpdate,
    RequestEventUpstream,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_event_serde_round_trips_new_optional_fields() {
        let original = RequestEvent {
            request_id: "req_round_trip".to_owned(),
            cache_creation_input_tokens: Some(700),
            cache_creation_input_tokens_5m: Some(400),
            cache_creation_input_tokens_1h: Some(300),
            cache_read_input_tokens: Some(200),
            cost_usd_micros: Some(987_654),
            cost_input_micros: Some(369_000),
            cost_output_micros: Some(675_000),
            cost_cache_creation_5m_micros: Some(150_000),
            cost_cache_creation_1h_micros: Some(180_000),
            cost_cache_read_micros: Some(60_000),
            ..Default::default()
        };

        let bytes = serde_json::to_vec(&original).expect("serialize");
        let parsed: RequestEvent = serde_json::from_slice(&bytes).expect("deserialize");

        assert_eq!(parsed, original);
    }

    #[test]
    fn request_event_serde_old_json_missing_new_fields_yields_none() {
        let old_json = serde_json::json!({
            "ts": 1_700_000_000u64,
            "request_id": "req_old",
            "principal_id": null,
            "principal_kind": null,
            "upstream": null,
            "model": null,
            "status": 200u16,
            "input_tokens": null,
            "output_tokens": null,
            "cache_creation_input_tokens": 1600u64,
            "cache_read_input_tokens": 0u64,
            "cost_usd_micros": 12_345i64,
            "duration_ms": 100u64,
        });

        let parsed: RequestEvent = serde_json::from_value(old_json).expect("deserialize old shape");

        assert_eq!(parsed.cache_creation_input_tokens, Some(1600));
        assert_eq!(parsed.cache_creation_input_tokens_5m, None);
        assert_eq!(parsed.cache_creation_input_tokens_1h, None);
        assert_eq!(parsed.cost_input_micros, None);
        assert_eq!(parsed.cost_output_micros, None);
        assert_eq!(parsed.cost_cache_creation_5m_micros, None);
        assert_eq!(parsed.cost_cache_creation_1h_micros, None);
        assert_eq!(parsed.cost_cache_read_micros, None);
    }

    #[test]
    fn request_event_serializes_new_latency_fields_round_trip() {
        let event = RequestEvent {
            auth_ms: Some(50),
            route_ms: Some(30),
            limit_reserve_ms: Some(10),
            bulkhead_wait_ms: Some(3),
            dns_ms: Some(12),
            connect_ms: Some(85),
            connection_reused: Some(false),
            limit_reconcile_ms: Some(15),
            observability_post_ms: Some(20),
            ..RequestEvent::default()
        };
        let json = serde_json::to_string(&event).expect("serializes");
        let decoded: RequestEvent = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(decoded.auth_ms, Some(50));
        assert_eq!(decoded.route_ms, Some(30));
        assert_eq!(decoded.limit_reserve_ms, Some(10));
        assert_eq!(decoded.bulkhead_wait_ms, Some(3));
        assert_eq!(decoded.dns_ms, Some(12));
        assert_eq!(decoded.connect_ms, Some(85));
        assert_eq!(decoded.connection_reused, Some(false));
        assert_eq!(decoded.limit_reconcile_ms, Some(15));
        assert_eq!(decoded.observability_post_ms, Some(20));
    }

    #[test]
    fn request_event_decodes_old_event_without_new_fields_yields_none() {
        let old_json =
            r#"{"ts": 1700000000, "request_id": "test", "status": 200, "duration_ms": 100}"#;
        let decoded: RequestEvent = serde_json::from_str(old_json).expect("deserializes old shape");
        assert_eq!(decoded.auth_ms, None);
        assert_eq!(decoded.route_ms, None);
        assert_eq!(decoded.limit_reserve_ms, None);
        assert_eq!(decoded.bulkhead_wait_ms, None);
        assert_eq!(decoded.dns_ms, None);
        assert_eq!(decoded.connect_ms, None);
        assert_eq!(decoded.connection_reused, None);
        assert_eq!(decoded.limit_reconcile_ms, None);
        assert_eq!(decoded.observability_post_ms, None);
    }

    #[test]
    fn request_event_serde_round_trips_terminal_observation_fields() {
        let iterations = serde_json::json!([
            {
                "type": "message",
                "input_tokens": 100,
                "output_tokens": 200,
                "model": "claude-x"
            }
        ]);
        let original = RequestEvent {
            request_id: "req_t1".to_owned(),
            event_id: Some("0193f76b-1ab2-7a4d-8a3c-44ab3c5e1f0a".to_owned()),
            thinking_tokens: Some(64),
            web_search_requests: Some(3),
            web_fetch_requests: Some(1),
            service_tier: Some("priority".to_owned()),
            inference_geo: Some("us-east".to_owned()),
            upstream_error_type: Some("overloaded_error".to_owned()),
            upstream_error_message: Some("upstream overloaded; retry".to_owned()),
            iterations: Some(iterations.clone()),
            ..Default::default()
        };

        let bytes = serde_json::to_vec(&original).expect("serialize");
        let parsed: RequestEvent = serde_json::from_slice(&bytes).expect("deserialize");
        assert_eq!(parsed, original);

        let legacy = serde_json::json!({
            "ts": 1_700_000_000u64,
            "request_id": "req_legacy",
            "status": 200u16,
            "duration_ms": 100u64,
        });
        let parsed_legacy: RequestEvent =
            serde_json::from_value(legacy).expect("deserialize legacy");
        assert_eq!(parsed_legacy.event_id, None);
        assert_eq!(parsed_legacy.thinking_tokens, None);
        assert_eq!(parsed_legacy.web_search_requests, None);
        assert_eq!(parsed_legacy.web_fetch_requests, None);
        assert_eq!(parsed_legacy.service_tier, None);
        assert_eq!(parsed_legacy.inference_geo, None);
        assert_eq!(parsed_legacy.upstream_error_type, None);
        assert_eq!(parsed_legacy.upstream_error_message, None);
        assert_eq!(parsed_legacy.iterations, None);
    }

    #[test]
    fn request_event_warm_pool_invariant_preserved_through_round_trip() {
        let event = RequestEvent {
            connection_reused: Some(true),
            dns_ms: None,
            connect_ms: None,
            ..RequestEvent::default()
        };
        let json = serde_json::to_string(&event).expect("serializes");
        assert!(!json.contains("dns_ms"));
        assert!(!json.contains("connect_ms"));
        let decoded: RequestEvent = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(decoded.connection_reused, Some(true));
        assert_eq!(decoded.dns_ms, None);
        assert_eq!(decoded.connect_ms, None);
    }
}
