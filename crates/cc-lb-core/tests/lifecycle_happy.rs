mod common;

use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::BodyExt;

use common::{
    lifecycle_with, messages_request, DispatchMode, MockDispatch, RecordingHook, TestAuthn,
    TestState,
};

#[tokio::test]
async fn happy_sse_relays_incrementally_and_observes_chunks() {
    let state = TestState::default();
    let hook = Arc::new(RecordingHook::default());
    let lifecycle = lifecycle_with(
        TestAuthn::new(state.clone()),
        MockDispatch {
            state: state.clone(),
            mode: DispatchMode::StreamingOk,
        },
        hook.clone(),
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":true}"#,
        )))
        .await
        .expect("lifecycle handles request");

    assert_eq!(response.status(), http::StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("stream collects")
        .to_bytes();
    let data_lines = String::from_utf8_lossy(&body).matches("data:").count();
    println!("data_lines={data_lines}");
    assert!(
        data_lines >= 50,
        "expected at least 50 data lines, got {data_lines}"
    );
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 1);
    assert!(hook
        .events
        .lock()
        .expect("events lock")
        .iter()
        .any(|event| matches!(event, cc_lb_plugin_api::ObserveEvent::Chunk { .. })));
}
