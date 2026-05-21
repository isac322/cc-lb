mod common;

use cc_lb_observability::{RedactionPolicy, REDACTED};

#[test]
fn user_prompt_redaction_defaults_off_and_can_be_enabled() {
    let prompt = "user-visible prompt text";
    let default_output = common::capture_event(RedactionPolicy::default(), || {
        tracing::info!(content = prompt, "content observed");
    });
    assert!(default_output.contains(prompt));

    let redacted_output = common::capture_event(RedactionPolicy::new(true), || {
        tracing::info!(content = prompt, "content observed");
    });
    assert!(redacted_output.contains(REDACTED));
    assert!(!redacted_output.contains(prompt));
}
