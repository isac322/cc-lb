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
async fn integration_sign_with_fake() {
    let body =
        Bytes::from_static(br#"{"messages":[{"role":"user","content":"hi"}],"max_tokens":8}"#);
    let mut headers = HeaderMap::new();
    headers.insert(HOST, HeaderValue::from_static("127.0.0.1"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    let shaped = common::shaped_request(
        "http://127.0.0.1/model/anthropic.claude-test:0/invoke",
        headers,
        body,
    );

    let signed = sign_request(&common::signer(), shaped)
        .await
        .expect("request signs");
    let authorization = common::auth_header(signed.headers());
    let signature = common::signature_component(authorization);
    assert!(authorization.starts_with("AWS4-HMAC-SHA256 "));
    assert!(authorization.contains("Credential=AKIATEST/"));
    assert!(authorization.contains("SignedHeaders="));
    assert_eq!(signature.len(), 64);
    assert_ne!(
        signature,
        "0000000000000000000000000000000000000000000000000000000000000000"
    );

    let response = app(AppConfig::default())
        .oneshot(request_from_signed(signed))
        .await
        .expect("fake response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let value: Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(value["type"], "message");
    eprintln!("fake-bedrock-runtime accepted signed SigV4 request: 200 OK");
}

fn request_from_signed(signed: cc_lb_plugin_api::SignedRequest) -> Request<Body> {
    let (url, method, headers, body) = signed.into_parts();
    let mut builder = Request::builder()
        .method(method)
        .uri(common::request_uri(&url));
    for (name, value) in &headers {
        builder = builder.header(name, value);
    }
    builder.body(Body::from(body)).expect("request builds")
}
