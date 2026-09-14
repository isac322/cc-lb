use axum::body::{Body, to_bytes};
use fake_anthropic::{AppConfig, app};
use http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

const PROXY_UPSTREAM_ERROR_MESSAGE_CAP_BYTES: usize = 1024;

async fn long_mode_error_message(app: axum::Router) -> String {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("x-api-key", "sk-ant-test")
                .header("anthropic-version", "2023-06-01")
                .header("x-fake-mode", "429-long")
                .body(Body::from(
                    r#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}],"max_tokens":10}"#,
                ))
                .expect("request builds"),
        )
        .await
        .expect("response returned");

    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        response
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok()),
        Some("1")
    );

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let body: Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(body["type"], "error");
    assert_eq!(body["error"]["type"], "rate_limit_error");
    body["error"]["message"]
        .as_str()
        .expect("string message")
        .to_owned()
}

#[tokio::test]
async fn t2__long_mode_message_exceeds_cap_and_is_multiline_with_unbroken_run() {
    // Given / When
    let message = long_mode_error_message(app(AppConfig::default())).await;

    // Then
    assert!(
        message.len() > PROXY_UPSTREAM_ERROR_MESSAGE_CAP_BYTES,
        "message must exceed the proxy cap so truncation is observable, got {} bytes",
        message.len()
    );
    assert!(message.len() > 500);
    assert!(message.contains('\n'));

    let longest_run = message
        .split(|c: char| c.is_whitespace())
        .map(str::len)
        .max()
        .unwrap_or(0);
    assert!(
        longest_run >= 200,
        "expected an unbroken whitespace-free run of >=200 bytes, got {longest_run}"
    );
}

#[tokio::test]
async fn t2__long_mode_message_is_deterministic() {
    // Given / When
    let first = long_mode_error_message(app(AppConfig::default())).await;
    let second = long_mode_error_message(app(AppConfig::default())).await;

    // Then
    assert_eq!(first, second);
}
