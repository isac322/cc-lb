use axum::body::{Body, to_bytes};
use fake_bedrock_mantle::{AppConfig, app};
use http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

#[tokio::test]
async fn api_key_and_sigv4_auth_are_accepted() {
    let app = app(AppConfig::default());
    let api_key = post_messages(app.clone(), Some(("x-api-key", "sk-ant-test"))).await;
    assert_eq!(api_key.0, StatusCode::OK);
    assert_eq!(api_key.1["type"], "message");

    let sigv4 = post_messages(app, Some(("authorization", valid_auth_header()))).await;
    assert_eq!(sigv4.0, StatusCode::OK);
    assert_eq!(sigv4.1["type"], "message");
}

#[tokio::test]
async fn missing_or_bad_auth_is_rejected() {
    let app = app(AppConfig::default());
    let missing = post_messages(app.clone(), None).await;
    assert_eq!(missing.0, StatusCode::UNAUTHORIZED);
    assert_eq!(missing.1["error"]["type"], "authentication_error");

    let bad = post_messages(
        app,
        Some((
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKIATEST, SignedHeaders=host, Signature=bad",
        )),
    )
    .await;
    assert_eq!(bad.0, StatusCode::UNAUTHORIZED);
}

async fn post_messages(app: axum::Router, auth: Option<(&str, &str)>) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/anthropic/v1/messages");
    if let Some((name, value)) = auth {
        builder = builder.header(name, value);
    }
    let response = app
        .oneshot(
            builder
                .body(Body::from(r#"{"model":"c","messages":[],"max_tokens":10}"#))
                .expect("request builds"),
        )
        .await
        .expect("response returned");
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    (status, serde_json::from_slice(&body).expect("json body"))
}

fn valid_auth_header() -> &'static str {
    "AWS4-HMAC-SHA256 Credential=AKIATEST/20260520/us-east-1/bedrock/aws4_request, SignedHeaders=host;x-amz-date, Signature=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
}
