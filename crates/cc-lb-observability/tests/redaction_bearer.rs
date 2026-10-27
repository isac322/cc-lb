use crate::common;

use cc_lb_observability::{REDACTED, RedactionPolicy, sanitize_url_for_log};

#[test]
fn redacts_bearer_token_from_tracing_output() {
    let bearer = "Authorization: Bearer abcdef.ghijkl";
    let output = common::capture_event(RedactionPolicy::default(), || {
        tracing::info!(header = bearer, "bearer observed");
    });

    assert!(output.contains(REDACTED));
    assert!(!output.contains("Bearer abcdef.ghijkl"));
}

#[test]
fn sanitizes_url_query_api_key_and_preserves_other_parameters() {
    let url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=AIzaSySecret12345678901234567890&alt=sse";
    assert_eq!(
        sanitize_url_for_log(url),
        "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=[REDACTED]&alt=sse"
    );
}

#[test]
fn redacts_google_api_key_headers_and_query_values() {
    let output = RedactionPolicy::default().redact_text(
        r#"{"x-goog-api-key":"AIzaSySecret12345678901234567890","url":"https://example.test/?key=AIzaSySecret12345678901234567890&alt=sse"}"#,
    );
    assert!(!output.contains("AIzaSySecret12345678901234567890"));
    assert!(output.matches(REDACTED).count() >= 2, "{output}");
}
