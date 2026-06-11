use cc_lb_observability::{
    REDACTED, ROUTING_REASON_MAX_BYTES, ROUTING_TRACE_SIZE_CAP_BYTES, enforce_routing_trace_caps,
    redact_internal_errors, redact_routing_trace, truncate_reason,
};
use cc_lb_plugin_api::types::{StageDecision, TerminalDecision};
use cc_lb_plugin_api::{
    InternalError, InternalErrorKind, InternalErrorStage, RoutingTrace, TerminalStrategy,
};

#[test]
fn routing_trace_reason_secrets_are_redacted() {
    let trace = RoutingTrace {
        stages: vec![StageDecision {
            stage_name: "router-filter".to_owned(),
            upstream_id: None,
            reason: Some("selected after token=plain-secret and sk-ant-oat01-deadbeef".to_owned()),
            duration_us: 42,
        }],
        terminal: Some(TerminalDecision {
            upstream_id: None,
            strategy: TerminalStrategy::FirstPick,
        }),
    };

    let redacted = redact_routing_trace(&trace);
    let reason = redacted.stages[0].reason.as_deref().unwrap();

    assert!(reason.contains(REDACTED));
    assert!(!reason.contains("plain-secret"));
    assert!(!reason.contains("sk-ant-oat01-deadbeef"));
    assert!(
        trace.stages[0]
            .reason
            .as_deref()
            .unwrap()
            .contains("plain-secret")
    );
}

#[test]
fn internal_error_messages_are_redacted() {
    let errors = vec![InternalError {
        stage: InternalErrorStage::Router,
        kind: InternalErrorKind::PluginError,
        message: Some("plugin returned x-api-key: secret-key and Bearer abc.def".to_owned()),
    }];

    let redacted = redact_internal_errors(&errors);
    let message = redacted[0].message.as_deref().unwrap();

    assert!(message.contains(REDACTED));
    assert!(!message.contains("secret-key"));
    assert!(!message.contains("abc.def"));
    assert!(errors[0].message.as_deref().unwrap().contains("secret-key"));
}

#[test]
fn truncate_reason_caps_long_reason() {
    let reason = "a".repeat(ROUTING_REASON_MAX_BYTES + 128);

    let truncated = truncate_reason(&reason);

    assert!(truncated.len() <= ROUTING_REASON_MAX_BYTES);
    assert!(truncated.ends_with("...[truncated]"));
}

#[test]
fn truncate_reason_preserves_utf8_boundary() {
    let reason = format!("{}한글", "a".repeat(ROUTING_REASON_MAX_BYTES));

    let truncated = truncate_reason(&reason);

    assert!(truncated.len() <= ROUTING_REASON_MAX_BYTES);
    assert!(truncated.is_char_boundary(truncated.len()));
}

#[test]
fn routing_trace_cap_removes_tail_stages_and_adds_marker() {
    let trace = RoutingTrace {
        stages: (0..40)
            .map(|index| StageDecision {
                stage_name: format!("stage-{index}"),
                upstream_id: None,
                reason: Some(format!("decision-{index}-{}", "x".repeat(350))),
                duration_us: index,
            })
            .collect(),
        terminal: Some(TerminalDecision {
            upstream_id: None,
            strategy: TerminalStrategy::FirstPick,
        }),
    };

    let capped = enforce_routing_trace_caps(&trace);
    let encoded = serde_json::to_vec(&capped).unwrap();
    let marker = capped.stages.last().unwrap();

    assert!(encoded.len() <= ROUTING_TRACE_SIZE_CAP_BYTES);
    assert_eq!(capped.stages.first().unwrap().stage_name, "stage-0");
    assert_eq!(marker.stage_name, "routing_trace_truncated");
    assert!(marker.reason.as_deref().unwrap().contains("removed"));
    assert!(
        !capped
            .stages
            .iter()
            .any(|stage| stage.stage_name == "stage-39")
    );
}
