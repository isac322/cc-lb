mod common;

use cc_lb_observability::{REDACTED, RedactionPolicy};

#[test]
fn redacts_sensitive_field_name_from_tracing_output() {
    let token = "plain-token-without-known-pattern";
    let output = common::capture_event(RedactionPolicy::default(), || {
        tracing::info!(authorization = token, "authorization observed");
    });

    assert!(output.contains(REDACTED));
    assert!(!output.contains(token));
}
