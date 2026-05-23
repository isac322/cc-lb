use axum::body::{Body, to_bytes};
use fake_vertex::{AppConfig, app};
use http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

#[tokio::test]
async fn raw_predict_returns_anthropic_message_shape_without_body_model() {
    let response = app(AppConfig::default())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/projects/p/locations/us-central1/publishers/anthropic/models/claude-3-5-sonnet@20240620:rawPredict")
                .header("authorization", "Bearer ya29.test")
                .body(Body::from(r#"{"anthropic_version":"vertex-2023-10-16","messages":[],"max_tokens":10}"#))
                .expect("request builds"),
        )
        .await
        .expect("response returned");

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let body: Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(body["type"], "message");
    assert_eq!(body["model"], "claude-3-5-sonnet@20240620");
    assert_eq!(body["usage"]["output_tokens"], 50);
}

#[tokio::test]
async fn stream_raw_predict_returns_anthropic_sse() {
    let response = app(AppConfig::default())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/projects/p/locations/us-central1/publishers/anthropic/models/claude:streamRawPredict")
                .header("authorization", "Bearer ya29.test")
                .body(Body::from(r#"{"anthropic_version":"vertex-2023-10-16","messages":[],"stream":true}"#))
                .expect("request builds"),
        )
        .await
        .expect("response returned");

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let text = String::from_utf8(body.to_vec()).expect("utf8 sse");
    assert!(text.contains("event: message_start"));
    assert!(text.contains("event: content_block_delta"));
    assert!(text.contains("event: message_stop"));
}
