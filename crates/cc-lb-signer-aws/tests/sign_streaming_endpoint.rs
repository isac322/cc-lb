mod common;

use bytes::Bytes;
use cc_lb_plugin_api::sign_request;
use http::header::{ACCEPT, CONTENT_TYPE, HOST};
use http::{HeaderMap, HeaderValue};

#[tokio::test]
async fn sign_streaming_endpoint_uses_full_body_hash() {
    let body = Bytes::from_static(
        br#"{"messages":[{"role":"user","content":"stream"}],"max_tokens":2,"stream":true}"#,
    );
    let mut headers = HeaderMap::new();
    headers.insert(
        HOST,
        HeaderValue::from_static("bedrock-runtime.us-east-1.amazonaws.com"),
    );
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(
        ACCEPT,
        HeaderValue::from_static("application/vnd.amazon.eventstream"),
    );
    let shaped = common::shaped_request(
        "https://bedrock-runtime.us-east-1.amazonaws.com/model/anthropic.claude-test%3A0/invoke-with-response-stream",
        headers,
        body.clone(),
    );

    let signed = sign_request(&common::signer(), shaped)
        .await
        .expect("streaming request signs");
    let payload_hash = signed
        .headers()
        .get("x-amz-content-sha256")
        .and_then(|value| value.to_str().ok())
        .expect("payload hash header");
    let authorization = common::auth_header(signed.headers());

    assert_eq!(payload_hash, common::hex_sha256(&body));
    assert_ne!(payload_hash, "STREAMING-AWS4-HMAC-SHA256-EVENTS");
    assert!(authorization
        .contains("SignedHeaders=accept;content-type;host;x-amz-content-sha256;x-amz-date"));
}
