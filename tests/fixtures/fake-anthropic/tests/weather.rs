use axum::body::{Body, to_bytes};
use fake_anthropic::{AppConfig, app};
use http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

const STREAM_BODY: &str = r#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}],"max_tokens":10,"stream":true}"#;

#[tokio::test]
async fn fake_weather_determinism() {
    // Given
    let headers = [
        ("x-fake-weather-seed", "42"),
        ("x-fake-delta-count", "3"),
        ("x-fake-ttft-ms", "0"),
        ("x-fake-inter-token-ms", "0"),
        ("x-fake-jitter-ms", "0"),
    ];

    // When
    let first = streaming_body(app(AppConfig::default()), &headers).await;
    let second = streaming_body(app(AppConfig::default()), &headers).await;

    // Then
    assert_eq!(first, second);
    assert_eq!(content_delta_count(&first), 3);
}

#[tokio::test]
async fn fake_weather_defaults_preserve_stream_shape() {
    // Given / When
    let stream = streaming_body(app(AppConfig::default()), &[]).await;

    // Then
    assert_eq!(content_delta_count(&stream), 50);
    assert_eq!(event_count(&stream, "message_start"), 1);
    assert_eq!(event_count(&stream, "content_block_start"), 1);
    assert_eq!(event_count(&stream, "content_block_stop"), 1);
    assert_eq!(event_count(&stream, "message_delta"), 1);
    assert_eq!(event_count(&stream, "message_stop"), 1);
}

#[tokio::test]
async fn fake_529_retry_after() {
    // Given
    let app = app(AppConfig::default());

    // When
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("x-api-key", "sk-ant-test")
                .header("x-fake-mode", "529")
                .header("x-fake-retry-after", "7")
                .body(Body::from(STREAM_BODY))
                .expect("request builds"),
        )
        .await
        .expect("response returned");

    // Then
    assert_eq!(
        response.status(),
        StatusCode::from_u16(529).expect("529 is a valid HTTP status")
    );
    assert_eq!(
        response
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok()),
        Some("7")
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let body: Value = serde_json::from_slice(&body).expect("error JSON");
    assert_eq!(body["type"], "error");
    assert_eq!(body["error"]["type"], "overloaded_error");
}

async fn streaming_body(app: axum::Router, weather_headers: &[(&str, &str)]) -> String {
    let mut request = Request::builder()
        .method("POST")
        .uri("/v1/messages")
        .header("x-api-key", "sk-ant-test")
        .header("anthropic-version", "2023-06-01");
    for (name, value) in weather_headers {
        request = request.header(*name, *value);
    }
    let response = app
        .oneshot(
            request
                .body(Body::from(STREAM_BODY))
                .expect("request builds"),
        )
        .await
        .expect("response returned");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    String::from_utf8(body.to_vec()).expect("SSE is UTF-8")
}

fn content_delta_count(stream: &str) -> usize {
    event_count(stream, "content_block_delta")
}

fn event_count(stream: &str, event: &str) -> usize {
    stream
        .lines()
        .filter(|line| *line == format!("event: {event}"))
        .count()
}
