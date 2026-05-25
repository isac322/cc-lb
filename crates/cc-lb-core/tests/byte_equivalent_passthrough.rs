mod sse_relay_support;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use cc_lb_core::SseBatchConfig;
use sse_relay_support::{
    RecordingHook, body_from_chunks, collect_response_body, fixture_1000_events, relay_for,
};

#[tokio::test]
async fn byte_equivalent_passthrough() {
    let input = fixture_1000_events();
    let hook = Arc::new(RecordingHook::default());
    let relay = relay_for(
        Arc::clone(&hook),
        SseBatchConfig {
            max_events: 32,
            max_age: Duration::from_secs(60),
        },
    );
    let response = relay.into_response_from_body(body_from_chunks(
        vec![
            Bytes::copy_from_slice(&input[..333]),
            Bytes::copy_from_slice(&input[333..]),
        ],
        Duration::ZERO,
        None,
    ));

    let output = collect_response_body(response).await;
    println!("byte_equivalent_len={}", output.len());
    assert_eq!(output, input);
}
