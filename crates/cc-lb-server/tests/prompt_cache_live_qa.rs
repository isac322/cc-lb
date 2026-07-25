use crate::common;
#[path = "prompt_cache_live_qa/support.rs"]
mod support;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fake_anthropic::{AppConfig, MessageScript, ScriptedMessageResponse};
use serde_json::{Value, json};

const MODEL: &str = "claude-sonnet-4-5-20250929";
const TOKEN_ESTIMATE_SOURCE: &str = "local_tiktoken_v1";
const HASH_SCHEMA_VERSION: i64 = 4;

fn assert_message_response(response: &common::RawResponse, usage_field: &str) -> Value {
    assert_eq!(response.status, 200);
    let body: Value = serde_json::from_str(&response.body).expect("valid Anthropic message JSON");
    assert_eq!(body["type"], "message");
    assert_eq!(body["role"], "assistant");
    assert!(body["content"].is_array());
    assert!(body["usage"][usage_field].as_u64().unwrap_or_default() > 0);
    body
}

#[tokio::test]
async fn full_proxy_write_then_non_breakpoint_lookback_hit_preserves_smart_routing() {
    let script = MessageScript::new();
    let mut creation = ScriptedMessageResponse::ok();
    creation.body["usage"]["cache_creation_input_tokens"] = json!(4096);
    creation.body["usage"]["cache_read_input_tokens"] = json!(0);
    creation.body["usage"]["cache_creation"] = json!({
        "ephemeral_1h_input_tokens": 2048,
        "ephemeral_5m_input_tokens": 2048
    });
    let mut read = ScriptedMessageResponse::ok();
    read.body["usage"]["cache_creation_input_tokens"] = json!(0);
    read.body["usage"]["cache_read_input_tokens"] = json!(3072);
    read.delay = Duration::from_secs(3);
    script.push_response(creation);
    script.push_response(read);
    let fake_config = AppConfig {
        message_script: Some(script.clone()),
        ..AppConfig::default()
    };
    let extra_config = r#"
[prompt_cache_shadow]
refresh_debounce_secs = 0
"#;
    let server = common::spawn_test_server_with_two_upstreams(extra_config, fake_config).await;
    assert!(server.sqlite_path.starts_with(server._config_dir.path()));
    assert!(server.managed_key.is_none());
    let pool = support::open_sqlite_pool(&server).await;
    let upstream_rows = support::fetch_upstreams(&pool).await;
    assert_eq!(upstream_rows.len(), 2);
    assert_eq!(upstream_rows[0].name, "fake_anthropic");
    assert_eq!(upstream_rows[1].name, "fake_anthropic_secondary");
    assert_ne!(upstream_rows[0].id, upstream_rows[1].id);

    let one_hour_prefix = "one-hour stable cache prefix ".repeat(900);
    let five_minute_prefix = "five-minute stable cache prefix ".repeat(900);
    let tail = "new request tail ".repeat(160);
    let first_request = json!({
        "model": MODEL,
        "max_tokens": 16,
        "system": [
            {"type": "text", "text": one_hour_prefix, "cache_control": {"type": "ephemeral", "ttl": "1h"}},
            {"type": "text", "text": five_minute_prefix, "cache_control": {"type": "ephemeral"}}
        ],
        "messages": [{"role": "user", "content": [{"type": "text", "text": tail}]}]
    });
    let first_body = serde_json::to_string(&first_request).expect("serialize first request");
    let first_response = common::http_post(server.proxy_addr, "/v1/messages", &first_body, &[])
        .await
        .expect("send first request through cc-lb proxy");
    assert_message_response(&first_response, "cache_creation_input_tokens");

    let first_upstream_id = support::wait_for_initial_observations(&pool, MODEL).await;
    let second_request = json!({
        "model": MODEL,
        "max_tokens": 16,
        "system": [
            {"type": "text", "text": first_request["system"][0]["text"], "cache_control": {"type": "ephemeral", "ttl": "1h"}},
            {"type": "text", "text": first_request["system"][1]["text"]}
        ],
        "messages": [{"role": "user", "content": [{
            "type": "text",
            "text": first_request["messages"][0]["content"][0]["text"],
            "cache_control": {"type": "ephemeral"}
        }]}]
    });
    let second_body = serde_json::to_string(&second_request).expect("serialize second request");
    let second_request_started = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after unix epoch")
        .as_secs() as i64;
    let second_response = common::http_post(server.proxy_addr, "/v1/messages", &second_body, &[])
        .await
        .expect("send second request through cc-lb proxy");
    assert_message_response(&second_response, "cache_read_input_tokens");
    assert!(script.wait_for_requests(2, Duration::from_secs(2)).await);
    assert_eq!(script.request_count(), 2);

    let event = support::wait_for_lookback_event(&pool, MODEL).await;
    assert_eq!(event.upstream_id, first_upstream_id);
    assert_eq!(event.lookback_distance, 1);
    assert_eq!(event.breakpoint_index, 2);
    assert_eq!(event.matched_index, 1);
    assert!(event.matched_index < event.breakpoint_index);
    assert!(event.predicted_read_tokens > 0);
    assert!(event.predicted_creation_tokens_5m > 0);
    assert_eq!(event.predicted_creation_tokens_1h, 0);
    assert_eq!(event.token_estimate_source, TOKEN_ESTIMATE_SOURCE);

    let observation = support::fetch_matched_observation(&pool, MODEL, &event).await;
    assert_eq!(observation.ttl_class, "0");
    assert_eq!(observation.hash_schema_version, HASH_SCHEMA_VERSION);
    assert_eq!(observation.prefix_content_block_index, event.matched_index);
    assert!(observation.estimated_prefix_tokens > 0);
    assert_eq!(observation.token_estimate_source, TOKEN_ESTIMATE_SOURCE);
    assert!(observation.expires_at >= second_request_started + 269);
    assert!(observation.expires_at <= second_request_started + 271);
}
