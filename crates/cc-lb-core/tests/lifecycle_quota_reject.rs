mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use http::StatusCode;

use common::{
    DispatchMode, MockDispatch, RecordingHook, TestAuthn, TestState, collect_body, lifecycle_with,
    messages_request,
};

#[tokio::test]
async fn quota_reject_returns_429_with_retry_after() {
    let state = TestState::default();
    let mut authn = TestAuthn::new(state.clone());
    authn.quotas.requests_per_window = 0;
    authn.quotas.window = Duration::from_secs(42);
    let hook = Arc::new(RecordingHook::default());
    let lifecycle = lifecycle_with(
        authn,
        MockDispatch {
            state: state.clone(),
            mode: DispatchMode::StreamingOk,
        },
        hook,
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        )))
        .await
        .expect("lifecycle handles request");
    let (status, headers, body) = collect_body(response).await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        headers
            .get("retry-after")
            .and_then(|value| value.to_str().ok()),
        Some("42")
    );
    assert!(String::from_utf8_lossy(&body).contains("rate_limit_error"));
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 0);
}
