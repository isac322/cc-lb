use crate::common;

use cc_lb_observability::{REDACTED, RedactionPolicy};

#[test]
fn redacts_anthropic_token_from_tracing_output() {
    let token = "sk-ant-oat01-deadbeef";
    let output = common::capture_event(RedactionPolicy::default(), || {
        tracing::info!(credential = token, "anthropic token observed");
    });

    assert!(output.contains(REDACTED));
    assert!(!output.contains(token));
}
