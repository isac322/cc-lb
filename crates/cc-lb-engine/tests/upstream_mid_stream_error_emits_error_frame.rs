use crate::sse_relay_support;

use std::sync::Arc;
use std::time::Duration;

use cc_lb_engine::SseBatchConfig;
use sse_relay_support::{
    RecordingHook, body_with_error_after, collect_response_body, numbered_events, relay_for,
};

#[tokio::test]
async fn upstream_mid_stream_error_emits_error_frame() {
    let hook = Arc::new(RecordingHook::default());
    let relay = relay_for(
        Arc::clone(&hook),
        SseBatchConfig {
            max_events: 32,
            max_age: Duration::from_secs(60),
        },
    );
    let response = relay.into_response_from_body(body_with_error_after(numbered_events(6), 3));

    let output = collect_response_body(response).await;
    let text = std::str::from_utf8(&output).expect("relay output is utf8");
    assert!(text.contains("event: error\n"));
    assert!(text.contains("\"type\":\"api_error\""));
}
