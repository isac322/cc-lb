mod sse_relay_support;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use cc_lb_core::SseBatchConfig;
use sse_relay_support::{RecordingHook, body_from_chunks, collect_response_body, relay_for};

#[tokio::test]
async fn utf8_boundary_handling() {
    let input = "event: content_block_delta\ndata: {\"text\":\"hi 👍 there\"}\n\n";
    let bytes = input.as_bytes();
    let split = input.find('👍').expect("fixture contains multibyte char") + 2;
    let chunks = vec![
        Bytes::copy_from_slice(&bytes[..split]),
        Bytes::copy_from_slice(&bytes[split..]),
    ];
    let hook = Arc::new(RecordingHook::default());
    let relay = relay_for(Arc::clone(&hook), SseBatchConfig::default());
    let response = relay.into_response_from_body(body_from_chunks(chunks, Duration::ZERO, None));

    let output = collect_response_body(response).await;
    assert_eq!(output, Bytes::copy_from_slice(bytes));
}
