use crate::sse_relay_support;

use std::time::Duration;

use bytes::Bytes;
use sse_relay_support::{body_from_chunks, collect_response_body, relay_for};

#[tokio::test]
async fn unknown_event_preserved() {
    let input = Bytes::from_static(b"event: foo\ndata: {}\n\n");
    let relay = relay_for();
    let response =
        relay.into_response_from_body(body_from_chunks(vec![input.clone()], Duration::ZERO, None));

    let output = collect_response_body(response).await;
    assert_eq!(output, input);
}
