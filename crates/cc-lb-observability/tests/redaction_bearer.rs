use crate::common;

use cc_lb_observability::{REDACTED, RedactionPolicy};

#[test]
fn redacts_bearer_token_from_tracing_output() {
    let bearer = "Authorization: Bearer abcdef.ghijkl";
    let output = common::capture_event(RedactionPolicy::default(), || {
        tracing::info!(header = bearer, "bearer observed");
    });

    assert!(output.contains(REDACTED));
    assert!(!output.contains("Bearer abcdef.ghijkl"));
}
