use cc_lb_storage_api::RequestEvent;

#[test]
fn request_event_deserializes_old_payload_without_routing_trace_and_internal_errors() {
    // Old payload without the new fields
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
        "duration_ms": 100u64,
    });

    let parsed: RequestEvent = serde_json::from_value(old_json).expect("deserialize old payload");

    // New fields should default to None/empty
    assert_eq!(parsed.routing_trace, None);
    assert_eq!(parsed.internal_errors, Vec::new());
}

#[test]
fn request_event_with_new_fields_round_trips() {
    use cc_lb_domain::{InternalError, InternalErrorKind, InternalErrorStage};

    let original = RequestEvent {
        request_id: "req_new_fields".to_owned(),
        routing_trace: Some(cc_lb_domain::RoutingTrace {
            stages: vec![],
            terminal_decision: Default::default(),
        }),
        internal_errors: vec![InternalError {
            stage: InternalErrorStage::Router,
            kind: InternalErrorKind::PluginError,
            message: Some("test error".to_owned()),
        }],
        ..Default::default()
    };

    let bytes = serde_json::to_vec(&original).expect("serialize");
    let parsed: RequestEvent = serde_json::from_slice(&bytes).expect("deserialize");

    assert_eq!(parsed.routing_trace, original.routing_trace);
    assert_eq!(parsed.internal_errors, original.internal_errors);
}

#[test]
fn request_event_skips_serializing_none_routing_trace() {
    let event = RequestEvent {
        request_id: "req_skip_none".to_owned(),
        routing_trace: None,
        internal_errors: vec![],
        ..Default::default()
    };

    let json = serde_json::to_string(&event).expect("serialize");

    // None routing_trace and empty internal_errors should be omitted
    assert!(!json.contains("routing_trace"));
    assert!(!json.contains("internal_errors"));
}

#[test]
fn request_event_includes_populated_new_fields() {
    use cc_lb_domain::{InternalError, InternalErrorKind, InternalErrorStage, RoutingTrace};

    let event = RequestEvent {
        request_id: "req_with_trace".to_owned(),
        routing_trace: Some(RoutingTrace {
            stages: vec![],
            terminal_decision: Default::default(),
        }),
        internal_errors: vec![InternalError {
            stage: InternalErrorStage::Router,
            kind: InternalErrorKind::PluginError,
            message: Some("error".to_owned()),
        }],
        ..Default::default()
    };

    let json = serde_json::to_string(&event).expect("serialize");

    // Fields should be present when populated
    assert!(json.contains("routing_trace"));
    assert!(json.contains("internal_errors"));
}

#[test]
fn request_event_mixed_old_and_new_fields() {
    // Mixed payload with some old fields + new fields
    let mixed_json = serde_json::json!({
        "ts": 1_700_000_000u64,
        "request_id": "req_mixed",
        "status": 200u16,
        "duration_ms": 500u64,
        "auth_ms": 50u64,
        "route_ms": 30u64,
        "routing_trace": {
            "stages": [],
            "terminal_decision": {
                "upstream_id": null,
                "strategy": "first-pick"
            }
        },
        "internal_errors": [
            {
                "stage": "router",
                "kind": "plugin_error",
                "message": "test"
            }
        ]
    });

    let parsed: RequestEvent = serde_json::from_value(mixed_json).expect("deserialize mixed");

    assert_eq!(parsed.request_id, "req_mixed");
    assert_eq!(parsed.auth_ms, Some(50));
    assert_eq!(parsed.route_ms, Some(30));
    assert!(parsed.routing_trace.is_some());
    assert_eq!(parsed.internal_errors.len(), 1);
}

#[test]
fn request_event_thinking_budget_tokens_roundtrip() {
    let original = RequestEvent {
        request_id: "req_thinking_budget".to_owned(),
        thinking_budget_tokens: Some(18000),
        ..Default::default()
    };

    let json_str = serde_json::to_string(&original).expect("serialize to string");
    let parsed: RequestEvent = serde_json::from_str(&json_str).expect("deserialize from string");

    assert_eq!(parsed.thinking_budget_tokens, Some(18000));
}

#[test]
fn request_event_thinking_budget_tokens_backward_compat() {
    // Legacy payload without thinking_budget_tokens field
    let legacy_json = serde_json::json!({
        "ts": 1_700_000_000u64,
        "request_id": "req_legacy_no_thinking_budget",
        "status": 200u16,
        "duration_ms": 100u64,
    });

    let parsed: RequestEvent = serde_json::from_value(legacy_json).expect("deserialize legacy");

    // thinking_budget_tokens should default to None
    assert_eq!(parsed.thinking_budget_tokens, None);
}

#[test]
fn request_event_thinking_budget_tokens_skips_none() {
    let event = RequestEvent {
        request_id: "req_no_thinking_budget".to_owned(),
        thinking_budget_tokens: None,
        ..Default::default()
    };

    let json = serde_json::to_string(&event).expect("serialize");

    // None thinking_budget_tokens should be omitted
    assert!(!json.contains("thinking_budget_tokens"));
}

#[test]
fn request_event_thinking_budget_tokens_includes_some() {
    let event = RequestEvent {
        request_id: "req_with_thinking_budget".to_owned(),
        thinking_budget_tokens: Some(25000),
        ..Default::default()
    };

    let json = serde_json::to_string(&event).expect("serialize");

    // Some thinking_budget_tokens should be included
    assert!(json.contains("thinking_budget_tokens"));
    assert!(json.contains("25000"));
}
