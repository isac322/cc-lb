mod common;

use bytes::Bytes;
use cc_lb_plugin_api::sign_request;
use http::header::{ACCEPT, CONTENT_TYPE, HOST};
use http::{HeaderMap, HeaderValue};

#[tokio::test]
async fn sign_canonical_request() {
    let body =
        Bytes::from_static(br#"{"messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#);
    let mut headers = HeaderMap::new();
    headers.insert(
        HOST,
        HeaderValue::from_static("bedrock-runtime.us-east-1.amazonaws.com"),
    );
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    let shaped = common::shaped_request(
        "https://bedrock-runtime.us-east-1.amazonaws.com/model/anthropic.claude-test%3A0/invoke?trace=on",
        headers,
        body.clone(),
    );

    let signed = sign_request(&common::signer(), shaped)
        .await
        .expect("request signs");
    let authorization = common::auth_header(signed.headers());
    let signature = common::signature_component(authorization);

    assert_eq!(
        signed
            .headers()
            .get("x-amz-date")
            .and_then(|value| value.to_str().ok()),
        Some("20200101T000000Z")
    );
    assert_eq!(
        signed
            .headers()
            .get("x-amz-content-sha256")
            .and_then(|value| value.to_str().ok()),
        Some(common::hex_sha256(&body).as_str())
    );
    assert!(authorization.starts_with("AWS4-HMAC-SHA256 "));
    assert!(authorization.contains("Credential=AKIATEST/20200101/us-east-1/bedrock/aws4_request"));
    assert!(authorization
        .contains("SignedHeaders=accept;content-type;host;x-amz-content-sha256;x-amz-date"));
    assert_eq!(signature.len(), 64);
    assert!(signature.chars().all(|ch| ch.is_ascii_hexdigit()));
    assert_ne!(
        signature,
        "0000000000000000000000000000000000000000000000000000000000000000"
    );
    eprintln!("canonical SigV4 authorization: {authorization}");
}
