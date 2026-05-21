mod common;

use cc_lb_observability::{RedactionPolicy, REDACTED};

#[test]
fn redacts_gcp_private_key_pem_from_tracing_output() {
    let private_key = "-----BEGIN PRIVATE KEY-----\nabc123\ndef456\n-----END PRIVATE KEY-----";
    let output = common::capture_event(RedactionPolicy::default(), || {
        tracing::info!(credential = private_key, "gcp key observed");
    });

    assert!(output.contains(REDACTED));
    assert!(!output.contains("BEGIN PRIVATE KEY"));
    assert!(!output.contains("abc123"));
}
