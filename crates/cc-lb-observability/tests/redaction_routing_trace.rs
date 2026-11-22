use cc_lb_domain::{InternalError, InternalErrorKind, InternalErrorStage};
use cc_lb_observability::{
    REDACTED, ROUTING_REASON_MAX_BYTES, redact_internal_errors, truncate_reason,
};

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
