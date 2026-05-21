mod common;

use std::sync::Arc;

use bytes::Bytes;
use http::{HeaderMap, HeaderValue, StatusCode};

use common::{
    collect_body, lifecycle_with, messages_request, DispatchMode, MockDispatch, RecordingHook,
    TestAuthn, TestState,
};

#[tokio::test]
async fn protocol_response_headers_pass_through_unchanged() {
    let state = TestState::default();
    let headers = protocol_headers();
    let hook = Arc::new(RecordingHook::default());
    let lifecycle = lifecycle_with(
        TestAuthn::new(state.clone()),
        MockDispatch {
            state,
            mode: DispatchMode::HeadersOk(headers.clone()),
        },
        hook,
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        )))
        .await
        .expect("lifecycle handles request");
    let (status, actual, _body) = collect_body(response).await;

    assert_eq!(status, StatusCode::OK);
    let mut preserved = 0;
    for (name, expected) in &headers {
        let actual = actual.get(name).and_then(|value| value.to_str().ok());
        assert_eq!(actual, expected.to_str().ok(), "header {name} changed");
        preserved += 1;
    }
    println!("protocol_headers_preserved={preserved}");
}

fn protocol_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("request-id", HeaderValue::from_static("req_protocol"));
    headers.insert(
        "anthropic-organization-id",
        HeaderValue::from_static("org_test"),
    );
    headers.insert(
        "anthropic-ratelimit-requests-remaining",
        HeaderValue::from_static("991"),
    );
    headers.insert(
        "anthropic-ratelimit-tokens-remaining",
        HeaderValue::from_static("992"),
    );
    headers.insert(
        "anthropic-ratelimit-input-tokens-remaining",
        HeaderValue::from_static("993"),
    );
    headers.insert(
        "anthropic-ratelimit-output-tokens-remaining",
        HeaderValue::from_static("994"),
    );
    headers.insert(
        "anthropic-ratelimit-requests-reset",
        HeaderValue::from_static("2026-05-20T00:00:01Z"),
    );
    headers.insert(
        "anthropic-ratelimit-tokens-reset",
        HeaderValue::from_static("2026-05-20T00:00:02Z"),
    );
    headers.insert("retry-after", HeaderValue::from_static("7"));
    headers.insert(
        "anthropic-dangerous-direct-browser-access",
        HeaderValue::from_static("true"),
    );
    headers
}
