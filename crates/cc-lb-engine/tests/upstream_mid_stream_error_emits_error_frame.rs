use crate::sse_relay_support;

use sse_relay_support::{body_with_error_after, collect_response_body, numbered_events, relay_for};

#[tokio::test]
async fn upstream_mid_stream_error_emits_error_frame() {
    let relay = relay_for();
    let response = relay.into_response_from_body(body_with_error_after(numbered_events(6), 3));

    let output = collect_response_body(response).await;
    let text = std::str::from_utf8(&output).expect("relay output is utf8");
    assert!(text.contains("event: error\n"));
    assert!(text.contains("\"type\":\"api_error\""));
}
