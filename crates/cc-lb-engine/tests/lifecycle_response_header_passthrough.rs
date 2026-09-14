use crate::common;

use std::sync::Arc;

use bytes::Bytes;
use http::header::{CONNECTION, CONTENT_ENCODING, CONTENT_TYPE};
use http::{HeaderMap, HeaderValue, StatusCode};

use common::{
    DispatchMode, MockDispatch, RecordingHook, TestAuthn, TestState, collect_body, lifecycle_with,
    messages_request,
};

#[tokio::test]
async fn t2__protocol_response_headers_pass_through_unchanged() {
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

#[tokio::test]
async fn t2__upstream_error_response_passes_through_without_body_rewrite() {
    let state = TestState::default();
    let body = gzipped_anthropic_error_body();
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert("request-id", HeaderValue::from_static("req_error"));
    headers.insert(CONNECTION, HeaderValue::from_static("x-hop"));
    headers.insert("x-hop", HeaderValue::from_static("strip-me"));
    let hook = Arc::new(RecordingHook::default());
    let lifecycle = lifecycle_with(
        TestAuthn::new(state.clone()),
        MockDispatch {
            state,
            mode: DispatchMode::Raw {
                status: StatusCode::BAD_REQUEST,
                headers,
                body: body.clone(),
            },
        },
        hook,
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        )))
        .await
        .expect("lifecycle handles request");
    let (status, actual, actual_body) = collect_body(response).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(actual_body, body);
    assert_eq!(
        actual
            .get(CONTENT_ENCODING)
            .and_then(|value| value.to_str().ok()),
        Some("gzip")
    );
    assert_eq!(
        actual
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/json")
    );
    assert_eq!(
        actual
            .get("request-id")
            .and_then(|value| value.to_str().ok()),
        Some("req_error")
    );
    assert!(!actual.contains_key(CONNECTION));
    assert!(!actual.contains_key("x-hop"));
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

fn gzipped_anthropic_error_body() -> Bytes {
    Bytes::from_static(&[
        31, 139, 8, 0, 0, 0, 0, 0, 2, 255, 171, 86, 42, 169, 44, 72, 85, 178, 82, 74, 45, 42, 202,
        47, 82, 210, 129, 210, 86, 213, 48, 241, 204, 188, 178, 196, 156, 204, 148, 248, 162, 212,
        194, 210, 212, 226, 146, 120, 152, 186, 220, 212, 226, 226, 196, 116, 144, 138, 194, 210,
        252, 146, 68, 165, 218, 90, 0, 74, 93, 250, 125, 75, 0, 0, 0,
    ])
}
