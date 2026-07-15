use crate::common;

use cc_lb_observability::{REDACTED, RedactionPolicy};
use regex::Regex;

#[test]
fn redacted_log_lines_snapshot() {
    let output = common::capture_event(RedactionPolicy::default(), || {
        tracing::info!(
            credential = "sk-ant-oat01-deadbeef",
            "anthropic token observed"
        );
        tracing::info!(
            authorization = "plain-token-without-known-pattern",
            "authorization observed"
        );
    });

    assert!(output.contains(REDACTED));
    assert!(!output.contains("sk-ant-oat01-deadbeef"));
    assert!(!output.contains("plain-token-without-known-pattern"));

    insta::assert_snapshot!(normalize_output(&output));
}

fn normalize_output(output: &str) -> String {
    let timestamp = Regex::new(r#""timestamp":"[^"]+""#).unwrap();
    timestamp
        .replace_all(output, r#""timestamp":"1970-01-01T00:00:00Z""#)
        .into_owned()
}
