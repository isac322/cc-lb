use crate::sse_relay_support;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use cc_lb_engine::SseBatchConfig;
use http_body_util::BodyExt;
use sse_relay_support::{RecordingHook, body_from_chunks, collect_response_body, relay_for};

const MESSAGE_START_WITH_CACHE: &str = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":10,\"cache_creation_input_tokens\":5,\"cache_read_input_tokens\":3,\"output_tokens\":1}}}\n\n";
const CONTENT_DELTA: &str = "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n";
const MESSAGE_DELTA_OUTPUT: &str =
    "event: message_delta\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":50}}\n\n";
const MESSAGE_STOP: &str = "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";

#[tokio::test]
async fn t2__full_stream_captures_anthropic_usage() {
    let relay = relay_for(
        Arc::new(RecordingHook::default()),
        SseBatchConfig::default(),
    );
    let handle = relay.clone();
    let stream =
        format!("{MESSAGE_START_WITH_CACHE}{CONTENT_DELTA}{MESSAGE_DELTA_OUTPUT}{MESSAGE_STOP}");

    let response = relay.into_response_from_body(body_from_chunks(
        vec![Bytes::from(stream)],
        Duration::ZERO,
        None,
    ));
    let _body = collect_response_body(response).await;

    let usage = handle.current_usage();
    assert_usage(
        usage.input_tokens,
        usage.output_tokens,
        usage.cache_creation_input_tokens,
        usage.cache_read_input_tokens,
        usage.complete,
        10,
        50,
        5,
        3,
        true,
    );
}

#[tokio::test]
async fn t2__mid_cancel_preserves_last_anthropic_usage() {
    let relay = relay_for(
        Arc::new(RecordingHook::default()),
        SseBatchConfig::default(),
    );
    let handle = relay.clone();
    let response = relay.into_response_from_body(body_from_chunks(
        vec![
            Bytes::from_static(MESSAGE_START_WITH_CACHE.as_bytes()),
            Bytes::from_static(MESSAGE_DELTA_OUTPUT.as_bytes()),
            Bytes::from_static(MESSAGE_STOP.as_bytes()),
        ],
        Duration::from_millis(50),
        None,
    ));
    let mut body = response.into_body();

    for _ in 0..2 {
        let frame = body.frame().await.expect("frame exists").expect("frame ok");
        assert!(frame.into_data().is_ok());
    }
    drop(body);

    let usage = handle.current_usage();
    assert_usage(
        usage.input_tokens,
        usage.output_tokens,
        usage.cache_creation_input_tokens,
        usage.cache_read_input_tokens,
        usage.complete,
        10,
        50,
        5,
        3,
        false,
    );
}

#[tokio::test]
async fn t2__no_message_delta_keeps_zero_output_tokens() {
    let relay = relay_for(
        Arc::new(RecordingHook::default()),
        SseBatchConfig::default(),
    );
    let handle = relay.clone();
    let stream = format!("{MESSAGE_START_WITH_CACHE}{CONTENT_DELTA}{MESSAGE_STOP}");

    let response = relay.into_response_from_body(body_from_chunks(
        vec![Bytes::from(stream)],
        Duration::ZERO,
        None,
    ));
    let _body = collect_response_body(response).await;

    let usage = handle.current_usage();
    assert_usage(
        usage.input_tokens,
        usage.output_tokens,
        usage.cache_creation_input_tokens,
        usage.cache_read_input_tokens,
        usage.complete,
        10,
        0,
        5,
        3,
        true,
    );
}

#[tokio::test]
async fn t2__message_delta_can_supply_cache_creation_tokens() {
    let relay = relay_for(
        Arc::new(RecordingHook::default()),
        SseBatchConfig::default(),
    );
    let handle = relay.clone();
    let message_start = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":10,\"cache_read_input_tokens\":3,\"output_tokens\":1}}}\n\n";
    let message_delta = "event: message_delta\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":50,\"cache_creation_input_tokens\":7}}\n\n";
    let stream = format!("{message_start}{message_delta}{MESSAGE_STOP}");

    let response = relay.into_response_from_body(body_from_chunks(
        vec![Bytes::from(stream)],
        Duration::ZERO,
        None,
    ));
    let _body = collect_response_body(response).await;

    let usage = handle.current_usage();
    assert_usage(
        usage.input_tokens,
        usage.output_tokens,
        usage.cache_creation_input_tokens,
        usage.cache_read_input_tokens,
        usage.complete,
        10,
        50,
        7,
        3,
        true,
    );
}

#[allow(clippy::too_many_arguments)]
fn assert_usage(
    actual_input_tokens: u64,
    actual_output_tokens: u64,
    actual_cache_creation_input_tokens: u64,
    actual_cache_read_input_tokens: u64,
    actual_complete: bool,
    expected_input_tokens: u64,
    expected_output_tokens: u64,
    expected_cache_creation_input_tokens: u64,
    expected_cache_read_input_tokens: u64,
    expected_complete: bool,
) {
    assert_eq!(actual_input_tokens, expected_input_tokens);
    assert_eq!(actual_output_tokens, expected_output_tokens);
    assert_eq!(
        actual_cache_creation_input_tokens,
        expected_cache_creation_input_tokens
    );
    assert_eq!(
        actual_cache_read_input_tokens,
        expected_cache_read_input_tokens
    );
    assert_eq!(actual_complete, expected_complete);
}
