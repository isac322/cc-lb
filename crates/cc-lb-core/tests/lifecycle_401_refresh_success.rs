mod common;

use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use http::StatusCode;

use common::{
    collect_body, lifecycle_with, messages_request, DispatchMode, MockDispatch, RecordingHook,
    TestAuthn, TestState,
};

#[tokio::test]
async fn unauthorized_refresh_retries_once_then_succeeds() {
    let state = TestState::default();
    let hook = Arc::new(RecordingHook::default());
    let lifecycle = lifecycle_with(
        TestAuthn::new(state.clone()),
        MockDispatch {
            state: state.clone(),
            mode: DispatchMode::Statuses(Arc::new(Mutex::new(VecDeque::from([
                StatusCode::UNAUTHORIZED,
                StatusCode::OK,
            ])))),
        },
        hook,
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        )))
        .await
        .expect("lifecycle handles request");
    let (status, _headers, _body) = collect_body(response).await;

    println!(
        "refresh_count={}",
        state.refresh_count.load(Ordering::Relaxed)
    );
    assert_eq!(status, StatusCode::OK);
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 2);
    assert_eq!(state.refresh_count.load(Ordering::Relaxed), 1);
}
