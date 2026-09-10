use crate::common;
#[path = "prompt_cache_live_qa/support.rs"]
mod support;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fake_anthropic::{AppConfig, MessageScript, ScriptedMessageResponse};
use serde_json::{Value, json};

const MODEL: &str = "claude-sonnet-4-5-20250929";
const TOKEN_ESTIMATE_SOURCE: &str = "serialized_prefix_bytes_v1";
const HASH_SCHEMA_VERSION: i64 = cc_lb_engine::lifecycle::HASH_SCHEMA_VERSION as i64;

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
async fn full_proxy_write_then_non_breakpoint_lookback_hit_preserves_subscription_preference() {
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

/// Top-level `cache_control` (automatic caching) must produce a breakpoint and hold affinity.
///
/// Before this was modelled, such a request yielded zero breakpoints, so `build_cache_score`
/// returned `None` on its empty-breakpoints guard: no cache score, no warm entry, and routing was
/// blind to the request. Every assertion below fails on that pre-change behavior.
#[tokio::test]
async fn top_level_cache_control_creates_a_breakpoint_and_holds_affinity() {
    let script = MessageScript::new();
    let mut creation = ScriptedMessageResponse::ok();
    creation.body["usage"]["cache_creation_input_tokens"] = json!(4096);
    creation.body["usage"]["cache_read_input_tokens"] = json!(0);
    let mut read = ScriptedMessageResponse::ok();
    read.body["usage"]["cache_creation_input_tokens"] = json!(0);
    read.body["usage"]["cache_read_input_tokens"] = json!(3072);
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
    let pool = support::open_sqlite_pool(&server).await;

    // No block-level `cache_control` anywhere: the only caching signal is the top-level field.
    let request = json!({
        "model": MODEL,
        "max_tokens": 64,
        "cache_control": {"type": "ephemeral"},
        "system": [{"type": "text", "text": "automatic caching stable prefix ".repeat(900)}],
        "messages": [{"role": "user", "content": [
            {"type": "text", "text": "automatic caching tail ".repeat(160)}
        ]}]
    });
    let body = serde_json::to_string(&request).expect("serialize automatic-caching request");

    let first = common::http_post(server.proxy_addr, "/v1/messages", &body, &[])
        .await
        .expect("send first automatic-caching request through cc-lb proxy");
    assert_message_response(&first, "cache_creation_input_tokens");
    let first_event = support::wait_for_settled_event(&pool, MODEL, 0).await;
    assert!(
        first_event.cache_prefix_hash.is_some(),
        "top-level cache_control must yield a prefix hash; got {:?}",
        first_event.cache_prefix_hash
    );
    assert_eq!(
        first_event.cache_control_block_count,
        Some(1),
        "the synthesized automatic breakpoint must be counted exactly once"
    );

    let second = common::http_post(server.proxy_addr, "/v1/messages", &body, &[])
        .await
        .expect("send second automatic-caching request through cc-lb proxy");
    assert_message_response(&second, "cache_read_input_tokens");
    assert!(script.wait_for_requests(2, Duration::from_secs(2)).await);

    let second_event = support::wait_for_settled_event(&pool, MODEL, first_event.id).await;
    assert_eq!(
        second_event.cache_prefix_hash, first_event.cache_prefix_hash,
        "a byte-identical request must hash to the same prefix"
    );
    assert_eq!(
        second_event.upstream_id, first_event.upstream_id,
        "cache affinity must hold the warm upstream for an automatic-caching request"
    );
    assert!(
        second_event.matched_v3_cache_key.is_some(),
        "the second request must match the warm prefix written by the first"
    );
    assert!(
        second_event.predicted_read_tokens.unwrap_or_default() > 0,
        "a matched warm prefix must predict a cache read; got {:?}",
        second_event.predicted_read_tokens
    );
}

/// Base request for the invalidator-scope cases: a cached tools tier and a cached message tier,
/// both above the model minimum, so a scope violation is visible at whichever tier it touches.
fn scoped_invalidator_base(marker: &str) -> Value {
    json!({
        "model": MODEL,
        "max_tokens": 64,
        "tools": [{
            "name": "lookup",
            "description": format!("{marker} stable oversized tool description ").repeat(400),
            "input_schema": {"type": "object"},
            "cache_control": {"type": "ephemeral"}
        }],
        "messages": [{"role": "user", "content": [
            {
                "type": "text",
                "text": format!("{marker} stable message prefix ").repeat(900),
                "cache_control": {"type": "ephemeral"}
            }
        ]}]
    })
}

/// Sends two requests differing only in the mutation applied to `variant`, then asserts the
/// invalidator is scoped to the message tier: tools stays byte-stable, messages changes, and the
/// second request does not reuse the message prefix the first one wrote.
///
/// Asserting only that "the hash changed" would also pass under a whole-chain seed, which is the
/// design this scoping rejects — hence both directions.
async fn assert_message_scoped_invalidator(marker: &str, apply: impl FnOnce(&mut Value)) {
    let script = MessageScript::new();
    let mut creation = ScriptedMessageResponse::ok();
    creation.body["usage"]["cache_creation_input_tokens"] = json!(4096);
    creation.body["usage"]["cache_read_input_tokens"] = json!(0);
    script.push_response(creation.clone());
    script.push_response(creation);
    let fake_config = AppConfig {
        message_script: Some(script.clone()),
        ..AppConfig::default()
    };
    let extra_config = r#"
[prompt_cache_shadow]
refresh_debounce_secs = 0
"#;
    let server = common::spawn_test_server_with_two_upstreams(extra_config, fake_config).await;
    let pool = support::open_sqlite_pool(&server).await;

    let base = scoped_invalidator_base(marker);
    let mut variant = base.clone();
    apply(&mut variant);
    assert_ne!(base, variant, "{marker}: mutation must change the request");

    let base_body = serde_json::to_string(&base).expect("serialize base request");
    let variant_body = serde_json::to_string(&variant).expect("serialize variant request");

    let first = common::http_post(server.proxy_addr, "/v1/messages", &base_body, &[])
        .await
        .expect("send base request through cc-lb proxy");
    assert_eq!(first.status, 200);
    let first_event = support::wait_for_settled_event(&pool, MODEL, 0).await;

    let second = common::http_post(server.proxy_addr, "/v1/messages", &variant_body, &[])
        .await
        .expect("send variant request through cc-lb proxy");
    assert_eq!(second.status, 200);
    assert!(script.wait_for_requests(2, Duration::from_secs(2)).await);
    let second_event = support::wait_for_settled_event(&pool, MODEL, first_event.id).await;

    let first_tools = first_event
        .breakpoint_prefix_hash("tools")
        .expect("base request records a tools breakpoint");
    let second_tools = second_event
        .breakpoint_prefix_hash("tools")
        .expect("variant request records a tools breakpoint");
    assert_eq!(
        first_tools, second_tools,
        "{marker}: tools-tier prefix must stay byte-stable across this invalidator"
    );

    let first_message = first_event
        .breakpoint_prefix_hash("message")
        .expect("base request records a message breakpoint");
    let second_message = second_event
        .breakpoint_prefix_hash("message")
        .expect("variant request records a message breakpoint");
    assert_ne!(
        first_message, second_message,
        "{marker}: message-tier prefix must change, or we predict a hit the provider misses"
    );
    // Matching the tools-tier key would be correct — that prefix is byte-stable and the provider
    // still serves it. The only forbidden outcome is reusing the invalidated message prefix.
    assert_ne!(
        second_event.matched_v3_cache_key.as_deref(),
        Some(first_message.as_str()),
        "{marker}: must not reuse the invalidated message prefix"
    );
}

#[tokio::test]
async fn effort_change_breaks_message_prefix_but_not_the_tools_prefix() {
    assert_message_scoped_invalidator("effort", |request| {
        request["output_config"] = json!({"effort": "low"});
    })
    .await;
}

#[tokio::test]
async fn thinking_presence_breaks_message_prefix_but_not_the_tools_prefix() {
    // `thinking` is salted on presence: its default is per-model, so an explicit value must never
    // be folded onto the omitted key.
    assert_message_scoped_invalidator("thinking", |request| {
        request["thinking"] = json!({"type": "disabled"});
    })
    .await;
}

#[tokio::test]
async fn tool_choice_change_breaks_message_prefix_but_not_the_tools_prefix() {
    assert_message_scoped_invalidator("tool_choice", |request| {
        request["tool_choice"] = json!({"type": "any"});
    })
    .await;
}

/// Opus 4.7's minimum cacheable prefix is 2048 tokens, not the 4096 the table used to claim.
///
/// The fixed request exercises that former regression while the fake provider reports 3000
/// cache-creation tokens. The observable contract is that cc-lb persists a warm observation.
/// `estimated_prefix_tokens` now stores serialized prefix bytes, so its numeric value must not be
/// compared with the provider's token threshold.
#[tokio::test]
async fn opus_4_7_records_a_warm_entry_for_a_prefix_between_2048_and_4096_tokens() {
    const OPUS_4_7: &str = "claude-opus-4-7";

    let script = MessageScript::new();
    let mut creation = ScriptedMessageResponse::ok();
    creation.body["usage"]["cache_creation_input_tokens"] = json!(3000);
    creation.body["usage"]["cache_read_input_tokens"] = json!(0);
    script.push_response(creation);
    let fake_config = AppConfig {
        message_script: Some(script.clone()),
        ..AppConfig::default()
    };
    let extra_config = r#"
[prompt_cache_shadow]
refresh_debounce_secs = 0
"#;
    let server = common::spawn_test_server_with_two_upstreams(extra_config, fake_config).await;
    let pool = support::open_sqlite_pool(&server).await;

    let request = json!({
        "model": OPUS_4_7,
        "max_tokens": 64,
        "system": [{
            "type": "text",
            "text": "opus four seven threshold prefix ".repeat(420),
            "cache_control": {"type": "ephemeral"}
        }],
        "messages": [{"role": "user", "content": [{"type": "text", "text": "tail"}]}]
    });
    let body = serde_json::to_string(&request).expect("serialize opus-4-7 request");
    let response = common::http_post(server.proxy_addr, "/v1/messages", &body, &[])
        .await
        .expect("send opus-4-7 request through cc-lb proxy");
    assert_eq!(response.status, 200);

    let (count, _) = support::observation_stats(&pool, OPUS_4_7, Duration::from_secs(10)).await;
    assert!(
        count > 0,
        "an Opus 4.7 prefix above the 2048 provider minimum must persist a warm entry; the old \
         4096 threshold dropped it. observations={count}"
    );
}

/// `speed` salting is per-model: models that cannot serve fast mode must hash it away.
///
/// Opus 4.6 accepts `speed: "fast"` and silently runs at standard speed, so salting the requested
/// value would split our prefix population against an upstream that served every request the same
/// way — a self-inflicted miss.
#[tokio::test]
async fn requested_fast_speed_is_hash_neutral_on_a_model_that_ignores_it() {
    const IGNORES_FAST: &str = "claude-opus-4-6";

    let script = MessageScript::new();
    let mut creation = ScriptedMessageResponse::ok();
    creation.body["usage"]["cache_creation_input_tokens"] = json!(5000);
    creation.body["usage"]["cache_read_input_tokens"] = json!(0);
    script.push_response(creation.clone());
    script.push_response(creation);
    let fake_config = AppConfig {
        message_script: Some(script.clone()),
        ..AppConfig::default()
    };
    let extra_config = r#"
[prompt_cache_shadow]
refresh_debounce_secs = 0
"#;
    let server = common::spawn_test_server_with_two_upstreams(extra_config, fake_config).await;
    let pool = support::open_sqlite_pool(&server).await;

    let base = json!({
        "model": IGNORES_FAST,
        "max_tokens": 64,
        "system": [{
            "type": "text",
            "text": "speed denylist stable prefix ".repeat(900),
            "cache_control": {"type": "ephemeral"}
        }],
        "messages": [{"role": "user", "content": [{"type": "text", "text": "tail"}]}]
    });
    let mut fast = base.clone();
    fast["speed"] = json!("fast");

    let plain_body = serde_json::to_string(&base).expect("serialize standard-speed request");
    let fast_body = serde_json::to_string(&fast).expect("serialize fast-speed request");

    let first = common::http_post(server.proxy_addr, "/v1/messages", &plain_body, &[])
        .await
        .expect("send standard-speed request through cc-lb proxy");
    assert_eq!(first.status, 200);
    let first_event = support::wait_for_settled_event(&pool, IGNORES_FAST, 0).await;

    let second = common::http_post(server.proxy_addr, "/v1/messages", &fast_body, &[])
        .await
        .expect("send fast-speed request through cc-lb proxy");
    assert_eq!(second.status, 200);
    assert!(script.wait_for_requests(2, Duration::from_secs(2)).await);
    let second_event = support::wait_for_settled_event(&pool, IGNORES_FAST, first_event.id).await;

    assert!(
        first_event.cache_prefix_hash.is_some(),
        "the standard-speed request must produce a prefix hash to compare against"
    );
    assert_eq!(
        second_event.cache_prefix_hash, first_event.cache_prefix_hash,
        "Opus 4.6 ignores a fast-speed request, so it must not split the prefix population"
    );
}

/// A replayed `thinking` block occupies a provider prefix position, so it must change the hash of
/// every breakpoint after it — even though it can never carry a marker itself.
///
/// Excluding thinking from the chain drifted our block indices from the provider's twenty-position
/// lookback and left the hash blind to replayed reasoning: two requests differing only by a
/// thinking block hashed identically, predicting a hit the provider misses.
///
/// The block is APPENDED to an existing assistant turn on purpose. Block digests include the
/// block's `messages[i].content[j]` path, so inserting anywhere earlier would shift a following
/// block's path and move the hash regardless of whether thinking participates — the test would
/// then prove only that a path string changed. Appending leaves every other block's path byte-
/// identical, so the marked breakpoint's prefix hash moves if and only if thinking is in the chain.
#[tokio::test]
async fn a_replayed_thinking_block_changes_the_following_breakpoint_prefix() {
    let script = MessageScript::new();
    let mut creation = ScriptedMessageResponse::ok();
    creation.body["usage"]["cache_creation_input_tokens"] = json!(4096);
    creation.body["usage"]["cache_read_input_tokens"] = json!(0);
    script.push_response(creation.clone());
    script.push_response(creation);
    let fake_config = AppConfig {
        message_script: Some(script.clone()),
        ..AppConfig::default()
    };
    let extra_config = r#"
[prompt_cache_shadow]
refresh_debounce_secs = 0
"#;
    let server = common::spawn_test_server_with_two_upstreams(extra_config, fake_config).await;
    let pool = support::open_sqlite_pool(&server).await;

    let base = json!({
        "model": MODEL,
        "max_tokens": 64,
        "tools": [{
            "name": "lookup",
            "description": "thinking chain stable tool description ".repeat(400),
            "input_schema": {"type": "object"},
            "cache_control": {"type": "ephemeral"}
        }],
        "messages": [
            {"role": "user", "content": [
                {"type": "text", "text": "thinking chain opening turn ".repeat(500)}
            ]},
            {"role": "assistant", "content": [
                {"type": "text", "text": "thinking chain assistant reply ".repeat(400)}
            ]},
            {"role": "user", "content": [{
                "type": "text",
                "text": "thinking chain marked block ".repeat(400),
                "cache_control": {"type": "ephemeral"}
            }]}
        ]
    });
    let mut with_thinking = base.clone();
    with_thinking["messages"][1]["content"]
        .as_array_mut()
        .expect("assistant turn holds an array content")
        .push(json!({
            "type": "thinking",
            "thinking": "replayed reasoning from the prior assistant turn",
            "signature": "sig-live-qa"
        }));

    let base_body = serde_json::to_string(&base).expect("serialize base request");
    let thinking_body =
        serde_json::to_string(&with_thinking).expect("serialize thinking-block request");

    let first = common::http_post(server.proxy_addr, "/v1/messages", &base_body, &[])
        .await
        .expect("send base request through cc-lb proxy");
    assert_eq!(first.status, 200);
    let first_event = support::wait_for_settled_event(&pool, MODEL, 0).await;

    let second = common::http_post(server.proxy_addr, "/v1/messages", &thinking_body, &[])
        .await
        .expect("send thinking-block request through cc-lb proxy");
    assert_eq!(second.status, 200);
    assert!(script.wait_for_requests(2, Duration::from_secs(2)).await);
    let second_event = support::wait_for_settled_event(&pool, MODEL, first_event.id).await;

    let first_tools = first_event
        .breakpoint_prefix_hash("tools")
        .expect("base request records a tools breakpoint");
    let second_tools = second_event
        .breakpoint_prefix_hash("tools")
        .expect("thinking-block request records a tools breakpoint");
    assert_eq!(
        first_tools, second_tools,
        "a message-turn block cannot reach the tools tier"
    );

    let first_message = first_event
        .breakpoint_prefix_hash("message")
        .expect("base request records a message breakpoint");
    let second_message = second_event
        .breakpoint_prefix_hash("message")
        .expect("thinking-block request records a message breakpoint");
    assert_ne!(
        first_message, second_message,
        "a replayed thinking block must change the prefix of the breakpoint that follows it"
    );
}
