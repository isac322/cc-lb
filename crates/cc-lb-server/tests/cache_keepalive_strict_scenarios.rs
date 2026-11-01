//! #988 keepalive pre-deploy E2E scenarios.

use std::time::Duration;

use cc_lb_storage_api::cache_keepalive_job_key;
use serde_json::{Value, json};

use super::keepalive_support::harness::{DueJob, Harness, HarnessOptions, NOW};
use super::keepalive_support::mock_upstream::MockEvent;
const SESSION: &str = "00000000-0000-7000-8000-000000000101";
const AGENT: &str = "subagent-worker-01";
const ANTHROPIC_VERSION: &str = "2023-06-01";
const CLAUDE_CODE_BETA: &str =
    "claude-code-20250219,interleaved-thinking-2025-05-14,thinking-binding-controls-2026-08-01";

fn event_for_prefix<'a>(
    events: &'a [MockEvent],
    kind: &str,
    prefix: &str,
) -> Option<&'a MockEvent> {
    events
        .iter()
        .rev()
        .find(|event| event.kind == kind && event_has_prefix(event, prefix))
}

fn event_has_prefix(event: &MockEvent, prefix: &str) -> bool {
    event
        .body
        .get("system")
        .is_some_and(|system| system.to_string().contains(prefix))
}

#[tokio::test]
async fn b1_multi_turn_adaptive_keepalive_preserves_live_options_and_oauth() {
    let harness = Harness::new(8_388_608).await;
    let session_key_hash = harness.session_key_hash(SESSION, None);

    let first = post_with_claude_code_headers(
        &harness,
        strict_body(
            "claude-opus-5-5",
            "b1-session",
            83_900,
            "5m",
            "first",
            json!({"type": "auto"}),
        ),
        SESSION,
        None,
    )
    .await;
    assert_eq!(first["stop_reason"], "tool_use");
    harness
        .wait_for_enqueued_generation(&session_key_hash, 1)
        .await;

    let second = post_with_claude_code_headers(
        &harness,
        strict_body(
            "claude-opus-5-5",
            "b1-session",
            83_900,
            "5m",
            "second",
            json!({"type": "auto"}),
        ),
        SESSION,
        None,
    )
    .await;
    assert_eq!(second["stop_reason"], "tool_use");
    harness
        .wait_for_enqueued_generation(&session_key_hash, 2)
        .await;

    harness
        .advance_and_dispatch_due(
            Duration::from_secs(270),
            &[DueJob {
                session_key_hash: &session_key_hash,
                generation: 2,
                mock_prefix: Some("b1-session"),
            }],
        )
        .await;

    let events = harness.mock_events();
    let live = event_for_prefix(&events, "proxy", "b1-session").expect("B1 live event");
    let renewal = event_for_prefix(&events, "renewal", "b1-session").expect("B1 renewal event");
    assert!(
        renewal.accepted(),
        "renewal rejected: {:?}",
        renewal.rejection
    );

    assert!(renewal.synthetic);
    assert!(renewal.hit, "renewal must HIT the live cache key");
    assert_eq!(renewal.cache_key, live.cache_key);
    assert_eq!(renewal.body["thinking"], live.body["thinking"]);
    assert_eq!(
        renewal.body["output_config"]["effort"],
        live.body["output_config"]["effort"]
    );
    assert_eq!(renewal.body["tool_choice"], live.body["tool_choice"]);
    // A7: renewal must use OAuth transport headers, preserve the client's
    // beta flags, and add the OAuth beta flag while preserving the version.
    assert!(renewal.credential.starts_with("Bearer "));
    assert_eq!(renewal.credential, "Bearer sk-ant-oat-test");
    let beta = renewal
        .anthropic_beta
        .as_deref()
        .expect("renewal anthropic-beta");
    for flag in CLAUDE_CODE_BETA.split(',') {
        assert!(
            beta.split(',')
                .map(str::trim)
                .any(|forwarded| forwarded == flag)
        );
    }
    assert!(
        beta.split(',')
            .map(str::trim)
            .any(|flag| flag == "oauth-2025-04-20")
    );
    assert_eq!(
        renewal.anthropic_version.as_deref(),
        Some(ANTHROPIC_VERSION)
    );
}

#[tokio::test]
async fn b2_subagent_thread_has_its_own_session_and_renewal() {
    let harness = Harness::new(8_388_608).await;
    let parent_key = harness.session_key_hash(SESSION, None);
    let agent_key = harness.session_key_hash(SESSION, Some(AGENT));

    let response = post_with_claude_code_headers(
        &harness,
        strict_body(
            "claude-opus-5-5",
            "b2-session",
            83_900,
            "5m",
            "agent",
            json!({"type": "auto"}),
        ),
        SESSION,
        Some(AGENT),
    )
    .await;
    assert_eq!(response["stop_reason"], "tool_use");
    harness.wait_for_enqueued_generation(&agent_key, 1).await;

    assert_ne!(parent_key, agent_key);
    assert!(harness.read_session(&parent_key).await.is_none());

    harness
        .advance_and_dispatch_due(
            Duration::from_secs(270),
            &[DueJob {
                session_key_hash: &agent_key,
                generation: 1,
                mock_prefix: Some("b2-session"),
            }],
        )
        .await;

    let events = harness.mock_events();
    let Some(renewal) = event_for_prefix(&events, "renewal", "b2-session") else {
        panic!(
            "B2 renewal event missing; agent_key={agent_key}; {}; events={events:#?}",
            harness.debug_keepalive(&agent_key).await,
        );
    };
    assert!(
        renewal.accepted(),
        "renewal rejected: {:?}",
        renewal.rejection
    );
    assert!(renewal.hit, "subagent renewal must HIT its own live key");
    assert_eq!(
        renewal.cache_key,
        event_for_prefix(&events, "proxy", "b2-session")
            .expect("B2 live event")
            .cache_key
    );
}

#[tokio::test]
async fn b3_small_haiku_helper_does_not_create_keepalive() {
    let harness = Harness::new(8_388_608).await;
    let session_key_hash = harness.session_key_hash(SESSION, Some("small-helper"));

    let response = post_with_claude_code_headers(
        &harness,
        strict_body(
            "claude-haiku-5-5",
            "b3-small-helper",
            9_999,
            "5m",
            "helper",
            json!({"type": "auto"}),
        ),
        SESSION,
        Some("small-helper"),
    )
    .await;
    assert_eq!(response["stop_reason"], "tool_use");

    assert!(
        harness
            .wait_for_not_tracked(&session_key_hash, Duration::from_secs(1))
            .await
    );
    assert_eq!(harness.keepalive_job_count().await, 0);
}

#[tokio::test]
async fn b4_zero_token_incompatible_inputs_never_send_rejected_renewals() {
    let harness = Harness::new(8_388_608).await;
    let cases = [
        (
            "b4-any-tool",
            "00000000-0000-7000-8000-000000000103",
            "claude-opus-5-5",
            json!({"type": "adaptive"}),
            json!({"effort": "medium"}),
            json!({"type": "any"}),
            256,
        ),
        (
            "b4-named-tool",
            "00000000-0000-7000-8000-000000000104",
            "claude-opus-5-5",
            json!({"type": "adaptive"}),
            json!({"effort": "medium"}),
            json!({"type": "tool", "name": "bash"}),
            256,
        ),
        (
            "b4-structured-output",
            "00000000-0000-7000-8000-000000000105",
            "claude-opus-5-5",
            json!({"type": "adaptive"}),
            json!({"effort": "medium", "format": {"type": "json_schema"}}),
            json!({"type": "auto"}),
            256,
        ),
        (
            "b4-enabled-thinking",
            "00000000-0000-7000-8000-000000000106",
            "claude-opus-4-6",
            json!({"type": "enabled", "budget_tokens": 1024}),
            json!({"effort": "medium"}),
            json!({"type": "auto"}),
            4096,
        ),
    ];

    for (prefix, session, model, thinking, output_config, tool_choice, max_tokens) in cases {
        let session_key_hash = harness.session_key_hash(session, None);
        let response = post_with_claude_code_headers(
            &harness,
            strict_body_with_options(
                model,
                prefix,
                83_900,
                "5m",
                prefix,
                StrictBodyOptions {
                    max_tokens,
                    thinking,
                    output_config,
                    tool_choice,
                },
            ),
            session,
            None,
        )
        .await;
        assert_eq!(response["stop_reason"], "tool_use", "{prefix}");
        harness
            .wait_for_enqueued_generation(&session_key_hash, 1)
            .await;
        harness
            .advance_and_dispatch_due(
                Duration::from_secs(270),
                &[DueJob {
                    session_key_hash: &session_key_hash,
                    generation: 1,
                    mock_prefix: None,
                }],
            )
            .await;

        let events = harness.mock_events();
        let renewals = events
            .iter()
            .filter(|event| event.kind == "renewal" && event_has_prefix(event, prefix));
        for renewal in renewals {
            assert!(
                renewal.accepted(),
                "{prefix} renewal must not be rejected: {:?}",
                renewal.rejection
            );
        }
        let session_row = harness
            .read_session(&session_key_hash)
            .await
            .expect("B4 session row");
        assert_ne!(
            session_row.terminal_reason.as_deref(),
            Some("dispatch_error"),
            "{prefix} ended with dispatch_error: {session_row:?}"
        );
    }
}

#[tokio::test]
async fn b5_one_hour_ttl_uses_one_hour_lead_and_renews() {
    let mut options = HarnessOptions::default();
    options.keepalive.refresh_lead_time_1h_secs = 300;
    let harness = Harness::with_options(options)
        .await
        .expect("SQLite harness setup");
    let session_key_hash = harness.session_key_hash(SESSION, None);

    let response = post_with_claude_code_headers(
        &harness,
        strict_body(
            "claude-opus-5-5",
            "b5-one-hour",
            83_900,
            "1h",
            "one-hour",
            json!({"type": "auto"}),
        ),
        SESSION,
        None,
    )
    .await;
    assert_eq!(response["stop_reason"], "tool_use");
    harness
        .wait_for_enqueued_generation(&session_key_hash, 1)
        .await;

    let timing = harness
        .read_job_timing(&cache_keepalive_job_key(&session_key_hash, 1))
        .await
        .expect("B5 scheduled job timing");
    // cache_keepalive_enqueuer.rs floors fractional delays to whole seconds.
    let run_at = timing.run_at.expect("B5 run_at");
    let expected_run_at = (NOW + 3_600 - 300) as i64;
    assert!(
        (expected_run_at - 1..=expected_run_at).contains(&run_at),
        "run_at={run_at}, expected [{}, {expected_run_at}]",
        expected_run_at - 1
    );

    harness
        .advance_and_dispatch_due(
            Duration::from_secs(3_300),
            &[DueJob {
                session_key_hash: &session_key_hash,
                generation: 1,
                mock_prefix: Some("b5-one-hour"),
            }],
        )
        .await;

    let events = harness.mock_events();
    let live = event_for_prefix(&events, "proxy", "b5-one-hour").expect("B5 live event");
    let renewal = event_for_prefix(&events, "renewal", "b5-one-hour").expect("B5 renewal event");
    assert!(
        renewal.accepted(),
        "renewal rejected: {:?}",
        renewal.rejection
    );
    assert!(renewal.hit, "one-hour renewal must HIT before cache expiry");
    assert_eq!(renewal.cache_key, live.cache_key);
}

async fn post_with_claude_code_headers(
    harness: &Harness,
    body: Vec<u8>,
    session: &str,
    agent: Option<&str>,
) -> Value {
    let mut request = harness
        .client
        .post(format!("http://{}/v1/messages", harness.proxy_addr))
        .header("x-api-key", &harness.key)
        .header("anthropic-version", ANTHROPIC_VERSION)
        .header("anthropic-beta", CLAUDE_CODE_BETA)
        .header("x-claude-code-session-id", session)
        .body(body);
    if let Some(agent) = agent {
        request = request.header("x-claude-code-agent-id", agent);
    }
    let response = request.send().await.expect("proxy request");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    response.json().await.expect("proxy JSON")
}

fn strict_body(
    model: &str,
    prefix_label: &str,
    prefix_tokens: u64,
    ttl: &str,
    turn: &str,
    tool_choice: Value,
) -> Vec<u8> {
    strict_body_with_options(
        model,
        prefix_label,
        prefix_tokens,
        ttl,
        turn,
        StrictBodyOptions {
            max_tokens: 256,
            thinking: json!({
                "type": "adaptive",
                "display": "summarized",
                "block_binding": {"prefix_mismatch_behavior": "drop_block"}
            }),
            output_config: json!({"effort": "medium"}),
            tool_choice,
        },
    )
}

struct StrictBodyOptions {
    max_tokens: u64,
    thinking: Value,
    output_config: Value,
    tool_choice: Value,
}

fn strict_body_with_options(
    model: &str,
    prefix_label: &str,
    prefix_tokens: u64,
    ttl: &str,
    turn: &str,
    options: StrictBodyOptions,
) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "model": model,
        "max_tokens": options.max_tokens,
        "stream": false,
        "thinking": options.thinking,
        "output_config": options.output_config,
        "tool_choice": options.tool_choice,
        "tools": [{
            "name": "bash",
            "description": "test tool",
            "input_schema": {"type": "object"}
        }],
        "system": [{
            "type": "text",
            "text": format!("tokens={prefix_tokens};stable cached prefix {prefix_label}"),
            "cache_control": {"type": "ephemeral", "ttl": ttl}
        }],
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "cached breakpoint", "cache_control": {"type": "ephemeral", "ttl": ttl}},
                    {"type": "text", "text": format!("turn={turn}")}
                ]
            },
            {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "prior-tool", "name": "bash", "input": {}}]
            },
            {
                "role": "user",
                "content": [{"type": "tool_result", "tool_use_id": "prior-tool", "content": format!("tool=bash;stop=tool_use;turn={turn};")}]
            }
        ]
    }))
    .expect("request JSON")
}
