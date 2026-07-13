use crate::common;

use std::sync::Arc;

use cc_lb_storage_api::{RequestEvent, RequestEventStore};
use cc_lb_storage_sqlite::open_sqlite;
use fake_anthropic::{AppConfig, MessageScript, ScriptedMessageResponse};
use serde_json::json;

const MODEL: &str = "claude-sonnet-4-5-20250929";

async fn persisted_model_event(server: &common::TestServer) -> RequestEvent {
    let database_url = format!("sqlite://{}", server.sqlite_path.display());
    let storage = open_sqlite(&database_url, Arc::new(cc_lb_engine::SystemClock))
        .await
        .expect("open server SQLite storage");
    // The final RequestEvent row is written asynchronously by the lifecycle
    // assembler task after the proxy response returns, so poll until it lands
    // rather than racing the single write (which flakes under CI load).
    for _ in 0..400 {
        let found = storage
            .query_recent_request_events(0, u64::MAX, 10)
            .await
            .expect("query persisted request events")
            .into_iter()
            .find(|event| event.model.as_deref() == Some(MODEL));
        if let Some(event) = found {
            return event;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("persisted event for proxied model did not appear within timeout");
}

#[tokio::test]
async fn thinking_enabled_and_priority_tier_are_persisted_after_proxy_request() {
    // Given
    let script = MessageScript::new();
    let mut upstream_response = ScriptedMessageResponse::ok();
    upstream_response.body["usage"]["service_tier"] = json!("priority");
    script.push_response(upstream_response);
    let server = common::spawn_test_server_with_fake_config(
        "",
        AppConfig {
            message_script: Some(script.clone()),
            ..AppConfig::default()
        },
    )
    .await;
    let request = json!({
        "model": MODEL,
        "max_tokens": 16,
        "messages": [{"role": "user", "content": "Reply with exactly: pong"}],
        "thinking": {"type": "enabled", "budget_tokens": 18000}
    });

    // When
    let response = common::http_post(server.proxy_addr, "/v1/messages", &request.to_string(), &[])
        .await
        .expect("send thinking request through cc-lb proxy");

    // Then
    assert_eq!(response.status, 200);
    assert_eq!(script.request_count(), 1, "stub upstream received request");
    let event = persisted_model_event(&server).await;
    assert_eq!(event.thinking_budget_tokens, Some(18_000));
    assert_eq!(event.service_tier.as_deref(), Some("priority"));
}

#[tokio::test]
async fn absent_thinking_and_standard_tier_are_persisted_after_proxy_request() {
    // Given
    let script = MessageScript::new();
    let mut upstream_response = ScriptedMessageResponse::ok();
    upstream_response.body["usage"]["service_tier"] = json!("standard");
    script.push_response(upstream_response);
    let server = common::spawn_test_server_with_fake_config(
        "",
        AppConfig {
            message_script: Some(script.clone()),
            ..AppConfig::default()
        },
    )
    .await;
    let request = json!({
        "model": MODEL,
        "max_tokens": 16,
        "messages": [{"role": "user", "content": "Reply with exactly: pong"}]
    });

    // When
    let response = common::http_post(server.proxy_addr, "/v1/messages", &request.to_string(), &[])
        .await
        .expect("send standard request through cc-lb proxy");

    // Then
    assert_eq!(response.status, 200);
    assert_eq!(script.request_count(), 1, "stub upstream received request");
    let event = persisted_model_event(&server).await;
    assert_eq!(event.thinking_budget_tokens, None);
    assert_eq!(event.service_tier.as_deref(), Some("standard"));
}
