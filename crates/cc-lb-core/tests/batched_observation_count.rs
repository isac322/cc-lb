mod sse_relay_support;

use std::sync::Arc;
use std::time::Duration;

use cc_lb_core::SseBatchConfig;
use sse_relay_support::{
    RecordingHook, body_from_chunks, collect_response_body, numbered_events, relay_for,
};

#[tokio::test]
async fn batched_observation_count() {
    let hook = Arc::new(RecordingHook::default());
    let relay = relay_for(
        Arc::clone(&hook),
        SseBatchConfig {
            max_events: 32,
            max_age: Duration::from_secs(60),
        },
    );
    let response =
        relay.into_response_from_body(body_from_chunks(numbered_events(100), Duration::ZERO, None));

    let _output = collect_response_body(response).await;
    let hook_calls = hook.chunk_calls();
    println!("hook_calls={hook_calls}");
    assert!((1..=4).contains(&hook_calls));
}
