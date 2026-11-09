//! #988 keepalive pre-deploy E2E scenarios.

use std::time::Duration;

use cc_lb_storage_api::{
    CacheKeepaliveEnqueueState, CacheKeepaliveSessionStatus, CacheKeepaliveTerminalReason,
    cache_keepalive_job_key,
};
use serde_json::{Value, json};

use super::keepalive_support::harness::{DueJob, Harness, HarnessOptions, NOW};
use super::keepalive_support::mock_upstream::{MockEvent, ProxyPause};
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

    let row = harness
        .read_session(&session_key_hash)
        .await
        .expect("active adaptive session after renewal");
    assert_eq!(row.status, "active");
    assert!(
        row.error.is_none(),
        "unexpected adaptive renewal error: {row:?}"
    );
    assert_eq!(row.enqueue_state, "enqueued");
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
    let (event, _) = harness.read_renewal_accounting(&agent_key, 1).await;
    let response = harness.admin_get(&format!(
        "/admin/v1/events/recent?since_unix_secs={NOW}&until_unix_secs={}&event_kind=renewal&source_kind=all&thread_id={SESSION}",
        NOW + 300,
    )).await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let filtered: Value = response.json().await.expect("agent renewal thread query");
    assert_eq!(
        filtered["events"].as_array().expect("events array").len(),
        1
    );
    assert_eq!(
        filtered["events"][0]["event_id"],
        event.event_id.as_deref().expect("event id")
    );
    assert_eq!(event.thread_id.as_deref(), Some(SESSION));
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

    assert_eq!(
        harness
            .wait_for_enqueued_or_not_tracked(&session_key_hash)
            .await,
        None,
        "small helper must have a durable not-tracked decision"
    );
    assert_eq!(harness.keepalive_job_count().await, 0);
}

#[tokio::test]
async fn b4_zero_token_incompatible_inputs_persist_skip_without_renewal() {
    let harness = Harness::new(8_388_608).await;
    let cases = [
        (
            "b4-any-tool",
            "00000000-0000-7000-8000-000000000103",
            "claude-opus-4-6",
            json!({"type": "adaptive"}),
            json!({"effort": "medium"}),
            json!({"type": "any"}),
            256,
            "forced tool_choice",
        ),
        (
            "b4-named-tool",
            "00000000-0000-7000-8000-000000000104",
            "claude-opus-4-6",
            json!({"type": "adaptive"}),
            json!({"effort": "medium"}),
            json!({"type": "tool", "name": "bash"}),
            256,
            "forced tool_choice",
        ),
        (
            "b4-structured-output",
            "00000000-0000-7000-8000-000000000105",
            "claude-opus-5-5",
            json!({"type": "adaptive"}),
            json!({"effort": "medium", "format": {"type": "json_schema"}}),
            json!({"type": "auto"}),
            256,
            "structured output",
        ),
        (
            "b4-enabled-thinking",
            "00000000-0000-7000-8000-000000000106",
            "claude-opus-4-6",
            json!({"type": "enabled", "budget_tokens": 1024}),
            json!({"effort": "medium"}),
            json!({"type": "auto"}),
            4096,
            "enabled thinking",
        ),
    ];

    for (prefix, session, model, thinking, output_config, tool_choice, max_tokens, feature) in cases
    {
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

        let reason = harness
            .wait_for_not_tracked_reason(&session_key_hash, Duration::from_secs(5))
            .await
            .unwrap_or_else(|| panic!("{prefix}: missing durable not-tracked decision"));
        assert!(
            reason.starts_with("zero-token renewal unsupported: ") && reason.contains(feature),
            "{prefix}: unexpected decision reason {reason:?}"
        );
        assert!(
            harness.read_session(&session_key_hash).await.is_none(),
            "{prefix}: incompatible request must not create a session"
        );
        assert_eq!(
            harness.keepalive_job_count().await,
            0,
            "{prefix}: incompatible request must not enqueue a job"
        );
        assert!(
            !harness
                .mock_events()
                .iter()
                .any(|event| event.kind == "renewal" && event_has_prefix(event, prefix)),
            "{prefix}: incompatible request sent a renewal"
        );
    }
}

#[tokio::test]
async fn b6_forced_tool_choice_cancels_active_session_and_fences_queued_renewal() {
    let harness = Harness::new(8_388_608).await;
    let session = "00000000-0000-7000-8000-000000000107";
    let session_key_hash = harness.session_key_hash(session, None);

    let first = post_with_claude_code_headers(
        &harness,
        strict_body(
            "claude-opus-4-6",
            "b6-adaptive",
            83_900,
            "5m",
            "first",
            json!({"type": "auto"}),
        ),
        session,
        None,
    )
    .await;
    assert_eq!(first["stop_reason"], "tool_use");
    harness
        .wait_for_enqueued_generation(&session_key_hash, 1)
        .await;
    harness
        .advance_and_dispatch_due(
            Duration::from_secs(270),
            &[DueJob {
                session_key_hash: &session_key_hash,
                generation: 1,
                mock_prefix: Some("b6-adaptive"),
            }],
        )
        .await;
    let renewal_count = harness
        .mock_events()
        .iter()
        .filter(|event| event.kind == "renewal")
        .count();
    assert_eq!(renewal_count, 1);
    harness
        .wait_for_enqueued_generation(&session_key_hash, 2)
        .await;

    let mut forced_tool_body: Value = serde_json::from_slice(&strict_body(
        "claude-opus-4-6",
        "b6-forced-tool",
        83_900,
        "5m",
        "forced",
        json!({"type": "auto"}),
    ))
    .expect("forced tool request JSON");
    forced_tool_body["tool_choice"] = json!({"type": "any"});
    let second = post_with_claude_code_headers(
        &harness,
        serde_json::to_vec(&forced_tool_body).expect("forced tool request body"),
        session,
        None,
    )
    .await;
    assert_eq!(second["stop_reason"], "tool_use");
    let reason = harness
        .wait_for_not_tracked_reason(&session_key_hash, Duration::from_secs(5))
        .await
        .unwrap_or_else(|| panic!("B6 missing durable not-tracked decision"));
    assert!(
        reason.starts_with("zero-token renewal unsupported: ")
            && reason.contains("forced tool_choice"),
        "B6 unexpected decision reason {reason:?}"
    );
    let cancelled = harness
        .wait_for_session_terminal(&session_key_hash, Duration::from_secs(5))
        .await
        .expect("B6 cancelled session");
    assert_eq!(cancelled.terminal_reason.as_deref(), Some("cancelled"));
    assert!(cancelled.error.is_none());

    harness
        .advance_and_dispatch_due(
            Duration::from_secs(270),
            &[DueJob {
                session_key_hash: &session_key_hash,
                generation: 2,
                mock_prefix: None,
            }],
        )
        .await;
    assert_eq!(
        harness
            .mock_events()
            .iter()
            .filter(|event| event.kind == "renewal")
            .count(),
        renewal_count,
        "cancelled session must not renew its previously queued job"
    );
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

#[tokio::test]
async fn nested_one_hour_renewal_persists_ttl_split_and_cost() {
    let mut options = HarnessOptions::default();
    options.keepalive.max_total_duration_secs = 7_200;
    let harness = Harness::with_options(options)
        .await
        .expect("SQLite harness setup");
    let session = "00000000-0000-7000-8000-000000001020";
    let session_key_hash = harness.session_key_hash(session, None);
    let model = "claude-sonnet-4-6";
    let prefix = "nested-one-hour-accounting";

    // The server's built-in Sonnet catalog has $3/M input and $3.75/M 5m writes.
    // Read that catalog through the real finalizer; do not mutate the shared catalog.
    let response = post_with_claude_code_headers(
        &harness,
        strict_body(
            model,
            prefix,
            83_900,
            "1h",
            "first",
            json!({"type": "auto"}),
        ),
        session,
        None,
    )
    .await;
    assert_eq!(response["stop_reason"], "tool_use");
    harness
        .wait_for_enqueued_generation(&session_key_hash, 1)
        .await;
    harness.mock.respond_to_next_renewal(json!({
        "id": "msg-nested-one-hour",
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": [],
        "stop_reason": "max_tokens",
        "usage": {
            "input_tokens": 0,
            "output_tokens": 0,
            "cache_read_input_tokens": 0,
            "cache_creation_input_tokens": 42,
            "cache_creation": {
                "ephemeral_5m_input_tokens": 0,
                "ephemeral_1h_input_tokens": 42
            }
        }
    }));

    harness
        .advance_and_dispatch_due(
            Duration::from_secs(3_300),
            &[DueJob {
                session_key_hash: &session_key_hash,
                generation: 1,
                mock_prefix: Some(prefix),
            }],
        )
        .await;
    let events = harness.mock_events();
    let renewal = event_for_prefix(&events, "renewal", prefix).expect("nested renewal attempt");
    assert!(
        renewal.accepted(),
        "renewal rejected: {:?}",
        renewal.rejection
    );
    assert!(renewal.synthetic);
    assert_eq!(renewal.model, model);

    // Exact scheduler completion awaits finalization, including both durable rows.
    let (event, turn) = harness.read_renewal_accounting(&session_key_hash, 1).await;
    println!(
        "persisted renewal: tuple=({:?}, {:?}, {:?}) cost_input={:?} cost_1h={:?} total={:?}",
        event.cache_creation_input_tokens,
        event.cache_creation_input_tokens_5m,
        event.cache_creation_input_tokens_1h,
        event.cost_input_micros,
        event.cost_cache_creation_1h_micros,
        event.cost_usd_micros,
    );
    assert_eq!(event.source_kind.as_deref(), Some("renewal"));
    assert_eq!(event.cache_creation_input_tokens, Some(42));
    assert_eq!(event.cache_creation_input_tokens_5m, Some(0));
    assert_eq!(event.cache_creation_input_tokens_1h, Some(42));
    assert_eq!(event.input_tokens, Some(0));
    assert_eq!(event.output_tokens, Some(0));
    assert_eq!(event.cache_read_input_tokens, Some(0));
    assert_eq!(event.cost_input_micros, Some(0));
    assert_eq!(event.cost_output_micros, Some(0));
    assert_eq!(event.cost_cache_creation_5m_micros, Some(0));
    assert_eq!(event.cost_cache_creation_1h_micros, Some(252));
    assert_eq!(event.cost_cache_read_micros, Some(0));
    assert_eq!(event.cost_usd_micros, Some(252));
    assert_eq!(turn.cache_creation_input_tokens, 42);
    assert_eq!(turn.cache_creation_input_tokens_5m, 0);
    assert_eq!(turn.cache_creation_input_tokens_1h, 42);
    assert_eq!(turn.input_tokens, 0);
    assert_eq!(turn.output_tokens, 0);
    assert_eq!(turn.cache_read_input_tokens, 0);
    assert_eq!(turn.cost_micros, Some(252));
    assert_eq!(turn.hit_miss, "miss");
    assert_eq!(
        event.source_ref_id.as_deref(),
        Some(turn.source_ref_id.as_str())
    );
    assert_eq!(harness.count_renewal_rows().await, (1, 1));
    let event_id = event.event_id.as_deref().expect("renewal event id");
    let query = format!(
        "/admin/v1/events/recent?since_unix_secs={NOW}&until_unix_secs={}&event_kind=renewal&source_kind=all",
        NOW + 7_200,
    );
    let response = harness.admin_get(&query).await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let unfiltered: Value = response.json().await.expect("unfiltered renewal list");
    assert_eq!(
        unfiltered["events"].as_array().expect("events array").len(),
        1
    );
    assert_eq!(unfiltered["events"][0]["event_id"], event_id);
    let response = harness
        .admin_get(&format!("{query}&thread_id={session}"))
        .await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let filtered: Value = response.json().await.expect("thread filtered renewal list");
    assert_eq!(
        filtered["events"], unfiltered["events"],
        "thread filter must retain the completed renewal"
    );
    assert_eq!(event.thread_id.as_deref(), Some(session));
}

#[tokio::test]
async fn real_thread_followup_ttfb_fences_due_sqlite_renewal() {
    real_followup_fences_due_sqlite_renewal(ProxyPause::Ttfb, Some(SESSION)).await;
}

#[tokio::test]
async fn real_thread_followup_stream_fences_due_sqlite_renewal() {
    real_followup_fences_due_sqlite_renewal(ProxyPause::Streaming, Some(SESSION)).await;
}

#[tokio::test]
async fn real_prefix_followup_ttfb_fences_due_sqlite_renewal() {
    real_followup_fences_due_sqlite_renewal(ProxyPause::Ttfb, None).await;
}

#[tokio::test]
async fn real_prefix_followup_stream_fences_due_sqlite_renewal() {
    real_followup_fences_due_sqlite_renewal(ProxyPause::Streaming, None).await;
}

async fn real_followup_fences_due_sqlite_renewal(pause: ProxyPause, session: Option<&str>) {
    let harness = Harness::new(8_388_608).await;
    let prefix = "in-flight-real-followup";
    // Start off the whole-second boundary, then complete the follow-up across it.
    harness.advance_clock(Duration::from_millis(900));
    let first = claude_code_request(
        &harness,
        strict_body(
            "claude-opus-5-5",
            prefix,
            83_900,
            "5m",
            "first",
            json!({"type": "auto"}),
        ),
        session,
        None,
    )
    .send()
    .await
    .expect("initial real request");
    assert_eq!(first.status(), reqwest::StatusCode::OK);
    let first: Value = first.json().await.expect("initial real response");
    assert_eq!(first["stop_reason"], "tool_use");
    let (hash, old_generation) = harness.wait_for_single_enqueued_session().await;
    assert_eq!(old_generation, 1);
    if let Some(session) = session {
        assert_eq!(hash, harness.session_key_hash(session, None));
    } else {
        assert!(
            hash.contains("/prefix:"),
            "expected routed prefix session: {hash}"
        );
    }
    let timing = harness
        .read_job_timing(&cache_keepalive_job_key(&hash, old_generation))
        .await
        .expect("old scheduled SQLite job");
    let due_at = u64::try_from(timing.run_at.expect("old run_at")).expect("positive due time");
    harness.advance_clock(Duration::from_secs(due_at - NOW - 1));
    let mut followup: Value = serde_json::from_slice(&strict_body(
        "claude-opus-5-5",
        prefix,
        83_900,
        "5m",
        "followup",
        json!({"type": "auto"}),
    ))
    .expect("follow-up body");
    followup["stream"] = json!(pause == ProxyPause::Streaming);
    let mut gate = harness.mock.pause_next_proxy(pause);
    let stream_observed = tokio::sync::Notify::new();
    let request = async {
        let mut response = claude_code_request(
            &harness,
            serde_json::to_vec(&followup).expect("follow-up JSON"),
            session,
            None,
        )
        .send()
        .await
        .expect("follow-up response headers");
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        if pause == ProxyPause::Streaming {
            assert!(
                response.headers()["content-type"]
                    .to_str()
                    .expect("SSE content type")
                    .starts_with("text/event-stream")
            );
            let mut body = Vec::new();
            let mut acknowledged = false;
            while let Some(chunk) = response.chunk().await.expect("real SSE response chunk") {
                body.extend_from_slice(&chunk);
                if !acknowledged && String::from_utf8_lossy(&body).contains("event: message_start")
                {
                    acknowledged = true;
                    stream_observed.notify_one();
                }
            }
            let body = String::from_utf8(body).expect("SSE UTF-8");
            assert!(
                acknowledged,
                "client must receive the partial stream before renewal is due"
            );
            assert!(body.contains("event: message_stop"));
            assert!(body.contains("\"tool_use\""));
        } else {
            let body: Value = response.json().await.expect("real buffered follow-up");
            assert_eq!(body["stop_reason"], "tool_use");
        }
    };
    let while_in_flight = async {
        gate.wait_until_paused().await;
        if pause == ProxyPause::Streaming {
            tokio::time::timeout(Duration::from_secs(20), stream_observed.notified())
                .await
                .expect("client must observe the partial real SSE stream");
        }
        let events = harness.mock_events();
        let proxy =
            event_for_prefix(&events, "proxy", prefix).expect("real follow-up reached upstream");
        assert!(
            proxy.accepted(),
            "strict provider contract: {:?}",
            proxy.rejection
        );
        assert_eq!(
            events.iter().filter(|event| event.kind == "proxy").count(),
            2
        );
        // Release only this exact old job and await its real SchedulerDispatch completion.
        harness
            .advance_and_dispatch_due(
                Duration::from_secs(1),
                &[DueJob {
                    session_key_hash: &hash,
                    generation: old_generation,
                    mock_prefix: None,
                }],
            )
            .await;
        assert_eq!(
            harness
                .mock_events()
                .iter()
                .filter(|event| event.kind == "renewal")
                .count(),
            0,
            "a due old generation must not reach upstream during real TTFB/SSE"
        );
        assert_eq!(harness.count_renewal_rows().await, (0, 0));
        let cancelled = harness.read_session_record(&hash).await;
        assert_eq!(cancelled.generation, old_generation);
        assert_eq!(cancelled.status, CacheKeepaliveSessionStatus::Terminal);
        assert_eq!(
            cancelled.terminal_reason,
            Some(CacheKeepaliveTerminalReason::Cancelled)
        );
        // The real completion has not happened yet; the queued job cannot replace it.
        harness.advance_clock(Duration::from_millis(200));
        gate.release();
    };
    tokio::join!(request, while_in_flight);
    let (completed_hash, generation) = harness.wait_for_single_enqueued_session().await;
    assert_eq!(
        completed_hash, hash,
        "real completion must retain its routed session scope"
    );
    assert_eq!(generation, old_generation + 1);
    let completed = harness.read_session_record(&hash).await;
    assert_eq!(completed.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(
        completed.enqueue_state,
        CacheKeepaliveEnqueueState::Enqueued
    );
    assert_eq!(completed.terminal_reason, None);
    assert_eq!(completed.refresh_count, 0);
    assert!(
        harness
            .read_job_timing(&cache_keepalive_job_key(&hash, generation))
            .await
            .is_some(),
        "the real completion must durably enqueue its new generation"
    );
    assert_eq!(harness.count_renewal_rows().await, (0, 0));
}

async fn post_with_claude_code_headers(
    harness: &Harness,
    body: Vec<u8>,
    session: &str,
    agent: Option<&str>,
) -> Value {
    let request = claude_code_request(harness, body, Some(session), agent);
    let response = request.send().await.expect("proxy request");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    response.json().await.expect("proxy JSON")
}

fn claude_code_request(
    harness: &Harness,
    body: Vec<u8>,
    session: Option<&str>,
    agent: Option<&str>,
) -> reqwest::RequestBuilder {
    let mut request = harness
        .client
        .post(format!("http://{}/v1/messages", harness.proxy_addr))
        .header("x-api-key", &harness.key)
        .header("anthropic-version", ANTHROPIC_VERSION)
        .header("anthropic-beta", CLAUDE_CODE_BETA)
        .body(body);
    if let Some(session) = session {
        request = request.header("x-claude-code-session-id", session);
    }
    if let Some(agent) = agent {
        request = request.header("x-claude-code-agent-id", agent);
    }
    request
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
