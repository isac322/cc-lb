mod common;

use axum::body::{Body, to_bytes};
use bytes::Bytes;
use cc_lb_plugin_api::sign_request;
use fake_bedrock_runtime::{AppConfig, app};
use http::header::{CONTENT_TYPE, HOST};
use http::{HeaderMap, HeaderValue, Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

#[tokio::test]
async fn tamper_body_fails_sig() {
    let original_body =
        Bytes::from_static(br#"{"messages":[{"role":"user","content":"before"}],"max_tokens":8}"#);
    let mut headers = HeaderMap::new();
    headers.insert(HOST, HeaderValue::from_static("127.0.0.1"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    let shaped = common::shaped_request(
        "http://127.0.0.1/model/anthropic.claude-test:0/invoke",
        headers,
        original_body,
    );

    let signed = sign_request(&common::signer(), shaped)
        .await
        .expect("request signs");
    let (url, method, mut headers, _body) = signed.into_parts();
    headers.insert("x-fake-verify-body-hash", HeaderValue::from_static("true"));
    let tampered_body =
        Bytes::from_static(br#"{"messages":[{"role":"user","content":"after"}],"max_tokens":8}"#);

    let response = app(AppConfig::default())
        .oneshot(request_from_parts(url, method, headers, tampered_body))
        .await
        .expect("fake response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let value: Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(value["__type"], "UnrecognizedClientException");
    eprintln!("fake-bedrock-runtime rejected tampered SigV4 request: 401 Unauthorized");
}

fn request_from_parts(
    url: url::Url,
    method: http::Method,
    headers: HeaderMap,
    body: Bytes,
) -> Request<Body> {
    let mut builder = Request::builder()
        .method(method)
        .uri(common::request_uri(&url));
    for (name, value) in &headers {
        builder = builder.header(name, value);
    }
    builder.body(Body::from(body)).expect("request builds")
}
