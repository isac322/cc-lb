use axum::body::{Body, to_bytes};
use fake_anthropic::{AppConfig, app};
use http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

#[tokio::test]
async fn t2__sse_ok_path_uses_single_content_block_index_and_terminal_events() {
    let response = app(AppConfig::default())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("x-api-key", "sk-ant-test")
                .header("anthropic-version", "2023-06-01")
                .body(Body::from(
                    r#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}],"max_tokens":10,"stream":true}"#,
                ))
                .expect("request builds"),
        )
        .await
        .expect("response returned");

    assert_eq!(response.status(), StatusCode::OK);
    assert_required_headers(response.headers());

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let text = String::from_utf8(body.to_vec()).expect("utf8 sse");

    assert!(text.contains("event: message_start"));
    assert!(text.contains("event: content_block_start"));
    assert!(text.contains("event: content_block_delta"));
    assert!(text.contains("event: content_block_stop"));
    assert!(text.contains("event: message_delta"));
    assert!(text.contains("event: message_stop"));
    assert!(text.contains("fake anthropic fixture response HELLO"));

    let indices = parse_indices(&text);
    assert_eq!(indices.len(), 52);
    assert!(indices.iter().all(|index| *index == 0));
}

#[tokio::test]
async fn t2__non_streaming_and_aux_endpoints_return_anthropic_like_shapes() {
    let app = app(AppConfig::default());

    let message = json_request(
        app.clone(),
        "POST",
        "/v1/messages",
        r#"{"model":"claude-3-5-haiku-20241022","messages":[{"role":"user","content":"hi"}],"max_tokens":10}"#,
    )
    .await;
    assert_eq!(message["type"], "message");
    assert_eq!(message["usage"]["input_tokens"], 100);

    let count = json_request(
        app.clone(),
        "POST",
        "/v1/messages/count_tokens",
        r#"{"messages":[{"role":"user","content":"hi"}]}"#,
    )
    .await;
    assert_eq!(count["input_tokens"], 100);

    let models = json_request(app.clone(), "GET", "/v1/models", "").await;
    assert!(
        models["data"]
            .as_array()
            .expect("models array")
            .iter()
            .any(|model| model["id"] == "claude-fable-5")
    );

    let model = json_request(app.clone(), "GET", "/v1/models/claude-test", "").await;
    assert_eq!(model["id"], "claude-test");

    let created = json_request(app.clone(), "POST", "/v1/files", "fixture body").await;
    assert_eq!(created["type"], "file");
    assert_eq!(created["id"], "file_abc123");

    let files = json_request(app.clone(), "GET", "/v1/files", "").await;
    assert_eq!(files["type"], "list");

    let file = json_request(app.clone(), "GET", "/v1/files/file_custom", "").await;
    assert_eq!(file["id"], "file_custom");

    let deleted = json_request(app, "DELETE", "/v1/files/file_custom", "").await;
    assert_eq!(deleted["deleted"], true);
}

async fn json_request(app: axum::Router, method: &str, uri: &str, body: &str) -> Value {
    let response = app
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("x-api-key", "sk-ant-test")
                .header("anthropic-version", "2023-06-01")
                .body(Body::from(body.to_owned()))
                .expect("request builds"),
        )
        .await
        .expect("response returned");

    assert_eq!(response.status(), StatusCode::OK);
    assert_required_headers(response.headers());
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    serde_json::from_slice(&body).expect("json body")
}

fn parse_indices(text: &str) -> Vec<u64> {
    text.lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter_map(|data| serde_json::from_str::<Value>(data).ok())
        .filter_map(|value| value.get("index").and_then(Value::as_u64))
        .collect()
}

fn assert_required_headers(headers: &http::HeaderMap) {
    assert!(
        headers
            .get("request-id")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("req_"))
    );
    assert_eq!(
        headers
            .get("anthropic-organization-id")
            .and_then(|value| value.to_str().ok()),
        Some("org_test")
    );
    assert_eq!(
        headers
            .get("anthropic-ratelimit-requests-remaining")
            .and_then(|value| value.to_str().ok()),
        Some("999")
    );
    assert_eq!(
        headers
            .get("anthropic-ratelimit-tokens-remaining")
            .and_then(|value| value.to_str().ok()),
        Some("999000")
    );
}
