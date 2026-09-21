use axum::body::{Body, to_bytes};
use fake_anthropic::{AppConfig, app};
use http::{Request, StatusCode};
use serde_json::{Value, json};
use tower::ServiceExt;

async fn messages_body(mode: &str, stream: bool) -> (StatusCode, String) {
    let request_body = if stream {
        r#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}],"max_tokens":10,"stream":true}"#
    } else {
        r#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}],"max_tokens":10}"#
    };
    let response = app(AppConfig::default())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("x-api-key", "sk-ant-test")
                .header("anthropic-version", "2023-06-01")
                .header("x-fake-mode", mode)
                .body(Body::from(request_body))
                .expect("request builds"),
        )
        .await
        .expect("response returned");

    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    (status, String::from_utf8(body.to_vec()).expect("utf8 body"))
}

fn event_names(text: &str) -> Vec<&str> {
    text.lines()
        .filter_map(|line| line.strip_prefix("event: "))
        .collect()
}

fn message_delta(text: &str) -> Value {
    text.split("\n\n")
        .filter_map(|frame| {
            let mut lines = frame.lines();
            if lines.next() == Some("event: message_delta") {
                lines
                    .next()
                    .and_then(|line| line.strip_prefix("data: "))
                    .and_then(|data| serde_json::from_str::<Value>(data).ok())
            } else {
                None
            }
        })
        .next()
        .expect("message_delta frame")
}

#[tokio::test]
async fn refusal_mode_streams_abnormal_stop_without_content_blocks() {
    // Given / When
    let (status, text) = messages_body("refusal", true).await;

    // Then: HTTP 200 carrying a refusal stop, distinguishable from a success.
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        event_names(&text),
        ["message_start", "message_delta", "message_stop"]
    );
    assert!(!text.contains("content_block_delta"));
    assert!(!text.contains("ping"));

    let delta = message_delta(&text);
    assert_eq!(delta["delta"]["stop_reason"], "refusal");
    assert_eq!(
        delta["delta"]["stop_details"]["category"],
        "reasoning_extraction"
    );
}

#[tokio::test]
async fn context_window_exceeded_mode_streams_abnormal_stop_without_details() {
    // Given / When
    let (status, text) = messages_body("context-window-exceeded", true).await;

    // Then
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        event_names(&text),
        ["message_start", "message_delta", "message_stop"]
    );

    let delta = message_delta(&text);
    assert_eq!(
        delta["delta"]["stop_reason"],
        "model_context_window_exceeded"
    );
    assert!(delta["delta"]["stop_details"].is_null());
}

#[tokio::test]
async fn refusal_mode_non_streaming_returns_matching_stop_fields() {
    // Given / When
    let (status, text) = messages_body("refusal", false).await;

    // Then
    assert_eq!(status, StatusCode::OK);
    let body: Value = serde_json::from_str(&text).expect("json body");
    assert_eq!(body["stop_reason"], "refusal");
    assert_eq!(body["stop_details"]["category"], "reasoning_extraction");
    assert_eq!(body["content"], json!([]));
    assert_eq!(body["usage"]["output_tokens"], 0);
}
