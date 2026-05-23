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
async fn model_gate_rejects_disallowed_model_before_upstream() {
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

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"forbidden-model","messages":[]}"#,
        )))
        .await
        .expect("lifecycle handles request");
    let (status, _headers, body) = collect_body(response).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(String::from_utf8_lossy(&body).contains("model_not_allowed"));
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 0);
}
