use crate::common;

use serde_json::Value;

#[tokio::test]
async fn fable_non_streaming_request_returns_exact_model_through_proxy() {
    // Given
    let server = common::spawn_test_server().await;
    let request = r#"{"model":"claude-fable-5","messages":[{"role":"user","content":"hi"}],"max_tokens":10,"stream":false}"#;

    // When
    let response = common::http_post(server.proxy_addr, "/v1/messages", request, &[])
        .await
        .expect("post non-streaming Fable message");

    // Then
    assert_eq!(response.status, 200);
    let body: Value = serde_json::from_str(&response.body).expect("message json");
    assert_eq!(body["type"], "message");
    assert_eq!(body["model"], "claude-fable-5");
}

#[tokio::test]
async fn fable_streaming_request_returns_exact_model_in_message_start_through_proxy() {
    // Given
    let server = common::spawn_test_server().await;
    let request = r#"{"model":"claude-fable-5","messages":[{"role":"user","content":"hi"}],"max_tokens":10,"stream":true}"#;

    // When
    let response = common::http_post(
        server.proxy_addr,
        "/v1/messages",
        request,
        &[("accept", "text/event-stream")],
    )
    .await
    .expect("post streaming Fable message");

    // Then
    assert_eq!(response.status, 200);
    assert!(response.body.contains("event: message_start"));
    let message_start = response
        .body
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter_map(|data| serde_json::from_str::<Value>(data).ok())
        .find(|event| event["type"] == "message_start")
        .expect("message_start SSE payload");
    assert_eq!(message_start["message"]["model"], "claude-fable-5");
}
