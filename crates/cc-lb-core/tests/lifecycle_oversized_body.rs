mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use http::StatusCode;

use common::{
    DispatchMode, MockDispatch, RecordingHook, TestAuthn, TestState, collect_body, lifecycle_with,
    messages_request,
};

#[tokio::test]
async fn oversized_messages_body_returns_413_before_upstream() {
    let state = TestState::default();
    let hook = Arc::new(RecordingHook::default());
    let lifecycle = lifecycle_with(
        TestAuthn::new(state.clone()),
        MockDispatch {
            state: state.clone(),
            mode: DispatchMode::StreamingOk,
        },
        hook,
    );

    let oversized = Bytes::from(vec![b'x'; 33 * 1024 * 1024]);
    let response = lifecycle
        .handle(messages_request(oversized))
        .await
        .expect("lifecycle handles request");
    let (status, _headers, body) = collect_body(response).await;

    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert!(String::from_utf8_lossy(&body).contains("body_too_large"));
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 0);
}
