use aws_eventstream_codec::decode_messages;
use axum::body::{Body, to_bytes};
use fake_bedrock_runtime::{AppConfig, app};
use http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

#[tokio::test]
async fn invoke_returns_anthropic_shape_and_rejects_signature_mismatch() {
    let app = app(AppConfig::default());
    let accepted = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/model/anthropic.claude-test:0/invoke")
                .header("authorization", valid_auth_header())
                .body(Body::from(r#"{"messages":[]}"#))
                .expect("request builds"),
        )
        .await
        .expect("response returned");
    assert_eq!(accepted.status(), StatusCode::OK);
    let body = to_bytes(accepted.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let body: Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(body["type"], "message");
    assert_eq!(body["usage"]["output_tokens"], 50);

    let rejected = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/model/anthropic.claude-test:0/invoke")
                .header("authorization", "AWS4-HMAC-SHA256 Credential=AKIATEST/20260520/us-east-1/bedrock/aws4_request, SignedHeaders=host;x-amz-date, Signature=bad")
                .body(Body::from(r#"{"messages":[]}"#))
                .expect("request builds"),
        )
        .await
        .expect("response returned");
    assert_eq!(rejected.status(), StatusCode::UNAUTHORIZED);
    let body = to_bytes(rejected.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let body: Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(body["__type"], "UnrecognizedClientException");
}

#[tokio::test]
async fn stream_ok_mode_decodes_and_modes_return_bedrock_errors() {
    let app = app(AppConfig::default());
    let streamed = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/model/m/invoke-with-response-stream")
                .header("x-fake-mode", "ok")
                .header("authorization", valid_auth_header())
                .body(Body::from(r#"{"messages":[]}"#))
                .expect("request builds"),
        )
        .await
        .expect("response returned");
    assert_eq!(streamed.status(), StatusCode::OK);
    assert_eq!(
        streamed
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("application/vnd.amazon.eventstream")
    );
    let body = to_bytes(streamed.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let decoded = decode_messages(&body).expect("event-stream decodes");
    assert_eq!(decoded.len(), 55);
    assert_eq!(decoded[0].header_str(":event-type"), Some("chunk"));
    assert!(String::from_utf8_lossy(&decoded[54].payload).contains("message_stop"));

    let validation = json_request(app.clone(), "ValidationException").await;
    assert_eq!(validation.0, StatusCode::BAD_REQUEST);
    assert_eq!(validation.1["__type"], "ValidationException");

    let clock_skew = json_request(app, "clock-skew").await;
    assert_eq!(clock_skew.0, StatusCode::BAD_REQUEST);
    assert_eq!(clock_skew.1["__type"], "RequestTimeTooSkewed");
}

async fn json_request(app: axum::Router, mode: &str) -> (StatusCode, Value) {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/model/m/invoke")
                .header("x-fake-mode", mode)
                .body(Body::from("{}"))
                .expect("request builds"),
        )
        .await
        .expect("response returned");
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let body = serde_json::from_slice(&body).expect("json body");
    (status, body)
}

fn valid_auth_header() -> &'static str {
    "AWS4-HMAC-SHA256 Credential=AKIATEST/20260520/us-east-1/bedrock/aws4_request, SignedHeaders=host;x-amz-date, Signature=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
}
