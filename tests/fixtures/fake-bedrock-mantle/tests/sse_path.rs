use axum::body::{Body, to_bytes};
use fake_bedrock_mantle::{AppConfig, app};
use http::{Request, StatusCode};
use tower::ServiceExt;

#[tokio::test]
async fn streaming_returns_anthropic_sse_not_eventstream() {
    let response = app(AppConfig::default())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/anthropic/v1/messages")
                .header("x-api-key", "sk-ant-test")
                .header("accept", "text/event-stream")
                .body(Body::from(
                    r#"{"model":"c","messages":[],"max_tokens":10,"stream":true}"#,
                ))
                .expect("request builds"),
        )
        .await
        .expect("response returned");

    assert_eq!(response.status(), StatusCode::OK);
    assert_ne!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("application/vnd.amazon.eventstream")
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let text = String::from_utf8(body.to_vec()).expect("utf8 sse");
    assert!(text.contains("event: message_start"));
    assert!(text.contains("event: content_block_delta"));
    assert!(text.contains("event: message_stop"));
}
