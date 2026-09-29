use cc_lb_storage_api::RequestEvent;

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

#[test]
fn request_event_reasoning_effort_roundtrip() {
    let original = RequestEvent {
        request_id: "req_reasoning_effort".to_owned(),
        reasoning_effort: Some("max".to_owned()),
        ..Default::default()
    };

    let json_str = serde_json::to_string(&original).expect("serialize to string");
    let parsed: RequestEvent = serde_json::from_str(&json_str).expect("deserialize from string");

    assert_eq!(parsed.reasoning_effort.as_deref(), Some("max"));
}
