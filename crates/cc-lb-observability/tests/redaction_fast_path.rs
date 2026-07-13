use std::borrow::Cow;

use cc_lb_observability::{REDACTED, RedactionPolicy};

#[test]
fn redacts_known_secrets_when_secret_patterns_match() {
    let policy = RedactionPolicy::default();
    let value = "credential=sk-ant-oat01-deadbeef bearer=Bearer abcdef.ghijkl";

    let redacted = policy.redact_text(value);

    assert_eq!(redacted, format!("credential={REDACTED} bearer={REDACTED}"));
}

#[test]
fn returns_borrowed_input_when_no_redaction_pattern_matches() {
    let policy = RedactionPolicy::default();
    let value = "request completed for model claude-sonnet";

    let redacted = policy.redact_text(value);

    assert_eq!(redacted, value);
    assert!(matches!(redacted, Cow::Borrowed(_)));
}
