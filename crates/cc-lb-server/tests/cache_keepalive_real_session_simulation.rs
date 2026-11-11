//! Deterministic end-to-end simulations for real prompt-cache sessions.

use std::time::Duration;

use cc_lb_aead::{AeadService, OAuthTokenBundle};
use cc_lb_clock::{Clock, unix_secs};
use cc_lb_engine::cache_keepalive::SessionKey;
use cc_lb_storage_api::upstream::{OAuthTokensCreate, UpstreamCreate, UpstreamKind};
use cc_lb_storage_api::{PrincipalStore, UpstreamStore};
use serde_json::{Value, json};
use sqlx::Row;
use uuid::Uuid;

use super::keepalive_support::harness::{
    DueJob, Harness, HarnessOptions, KeepaliveOptions, NOW, request_body,
};

#[tokio::test]
async fn successful_keepalive_publishes_observation_before_reschedule_cap() {
    #[derive(Debug, PartialEq, Eq)]
    struct Observation {
        upstream_id: String,
        model: String,
        key: String,
        ttl: String,
        expires_at: i64,
        observed_at: i64,
        schema: i64,
        block_index: i64,
        prefix_bytes: i64,
        estimate_source: String,
    }

    async fn observations(
        pool: &sqlx::SqlitePool,
        upstream_id: &str,
        observed_at: i64,
        expected_refreshes: usize,
    ) -> Vec<Observation> {
        let mut observations = Vec::new();
        let settled = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                observations = sqlx::query(
                    "SELECT upstream_id, canonical_model_id, v3_prefix_key, ttl_class, \
                     expires_at, last_observed_at, hash_schema_version, \
                     prefix_content_block_index, estimated_prefix_tokens, token_estimate_source \
                     FROM prompt_cache_observations WHERE upstream_id = ? \
                     ORDER BY prefix_content_block_index, v3_prefix_key",
                )
                .bind(upstream_id)
                .fetch_all(pool)
                .await
                .expect("query production prompt-cache observations")
                .into_iter()
                .map(|row| Observation {
                    upstream_id: row.get("upstream_id"),
                    model: row.get("canonical_model_id"),
                    key: row.get("v3_prefix_key"),
                    ttl: row.get("ttl_class"),
                    expires_at: row.get("expires_at"),
                    observed_at: row.get("last_observed_at"),
                    schema: row.get("hash_schema_version"),
                    block_index: row.get("prefix_content_block_index"),
                    prefix_bytes: row.get("estimated_prefix_tokens"),
                    estimate_source: row.get("token_estimate_source"),
                })
                .collect();
                if observations
                    .iter()
                    .filter(|row| row.observed_at == observed_at)
                    .count()
                    == expected_refreshes
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            settled.is_ok(),
            "expected {expected_refreshes} keyed observations at {observed_at}; \
             persisted observations: {observations:?}"
        );
        observations
    }

    // The final successful renewal publishes even when no next renewal is allowed.
    // A job at its deadline must not dispatch, so keep this first job one second
    // inside the duration cap; its next scheduled renewal would cross the cap.
    for (max_refreshes, max_duration, terminal_reason) in
        [(1, 1_800, "max_refreshes"), (3, 271, "max_duration")]
    {
        let harness = Harness::with_options(HarnessOptions {
            keepalive: KeepaliveOptions {
                max_refreshes_per_session: max_refreshes,
                max_total_duration_secs: max_duration,
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .expect("SQLite harness setup");
        // Discover the existing harness database through its effective config
        // rather than retaining a second copy of the pre-refactor harness.
        let editor_response = harness.admin_get("/admin/v1/config/editor").await;
        assert_eq!(editor_response.status(), reqwest::StatusCode::OK);
        let editor: Value = editor_response.json().await.expect("effective config JSON");
        let sqlite_path = editor["effective_config"]["storage"]["path"]
            .as_str()
            .expect("SQLite storage path");
        let clock = harness.clock.as_ref().expect("test clock").clone();
        let storage =
            cc_lb_storage_sqlite::open_sqlite(&format!("sqlite://{sqlite_path}"), clock.clone())
                .await
                .expect("open existing harness storage");
        let grace = editor["effective_config"]["prompt_cache_shadow"]["grace_margin_secs"]
            .as_u64()
            .expect("prompt-cache grace");
        let original_expires = NOW + 300 - grace;

        let cold_id = Uuid::new_v4();
        let cold_tokens = OAuthTokenBundle {
            access_token: "sk-ant-oat-cold".to_owned(),
            refresh_token: "sk-ant-ort-cold".to_owned(),
            expires_at_unix_secs: NOW + 86_400,
            refresh_token_expires_at_unix_secs: None,
            account_id: None,
            project_id: None,
            oauth_profile: Default::default(),
            scopes: vec!["messages".to_owned()],
            never_refresh: true,
        };
        let encrypted = cc_lb_aead::EncryptedOAuthTokens::encrypt(
            &AeadService::from_master_key([0; 32]),
            &cold_tokens,
            cold_id.as_bytes(),
        )
        .expect("encrypt cold OAuth tokens");
        UpstreamStore::create(
            &storage,
            UpstreamCreate {
                id: cold_id,
                name: "real-session-cold-upstream".to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                base_url: Some(harness.mock.base_url()),
                api_key_ciphertext: None,
                oauth_tokens: Some(OAuthTokensCreate {
                    tokens: encrypted,
                    never_refresh: true,
                }),
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
            },
        )
        .await
        .expect("create cold OAuth upstream");
        let principal = PrincipalStore::get_by_id(&storage, harness.principal_id)
            .await
            .expect("read principal")
            .expect("principal exists");
        // The admin mutation synchronously rebinds the dynamic view so both
        // candidates participate before the first proxy request.
        let update = harness
            .client
            .patch(format!(
                "http://{}/admin/v1/principals/{}",
                harness.admin_addr, harness.principal_id
            ))
            .header(
                "authorization",
                format!("Bearer {}", env!("CARGO_PKG_NAME")),
            )
            .header("if-match", format!("\"{}\"", principal.revision))
            .json(&json!({"allowed_upstreams": [harness.upstream_id, cold_id]}))
            .send()
            .await
            .expect("allow both upstream candidates");
        assert_eq!(update.status(), reqwest::StatusCode::OK);

        let session = "00000000-0000-7000-8000-000000001003";
        let session_key_hash = harness.session_key_hash(session, None);
        let mut request: Value = serde_json::from_slice(&request_body(
            65_536,
            "issue-1003",
            83_900,
            "bash",
            "tool_use",
            true,
        ))
        .expect("initial request JSON");
        // Oversized-prefix fixtures omit their label; retain it for the exact
        // generation-and-provider-attempt fence in the shared support API.
        request["system"][0]["text"] =
            Value::String(format!("tokens=83900;issue-1003;{}", "p".repeat(63_536)));
        let tool = &request["tools"][0];
        let system = &request["system"][0];
        let message = &request["messages"][0]["content"][0];
        let expected_prefix_bytes = i64::try_from(
            serde_json::to_vec(&json!({
                "model": "claude-opus-5-5",
                "content_blocks": [
                    {"source": "tools", "value": tool},
                    {"source": "system", "value": system},
                    {"source": "message", "value": message}
                ]
            }))
            .expect("independent prefix byte oracle")
            .len(),
        )
        .expect("prefix byte count fits i64");
        let body = serde_json::to_vec(&request).expect("initial request body");
        let first = harness.post(body.clone(), session, None).await;
        assert_eq!(first["stop_reason"], "tool_use");
        assert_eq!(first["usage"]["cache_read_input_tokens"], 0);
        assert_eq!(first["usage"]["cache_creation_input_tokens"], 83_900);
        harness
            .wait_for_enqueued_generation(&session_key_hash, 1)
            .await;
        let upstream_id = sqlx::query_scalar::<_, String>(
            "SELECT upstream_id FROM cache_keepalive_sessions WHERE session_key_hash = ?",
        )
        .bind(&session_key_hash)
        .fetch_one(storage.pool())
        .await
        .expect("session upstream");
        let initial = observations(storage.pool(), &upstream_id, NOW as i64, 2).await;
        assert_eq!(initial.len(), 2, "both eligible initial writes are durable");
        let initial_deepest = initial.last().expect("initial deeper observation");
        assert_eq!(initial_deepest.block_index, 2);
        assert_eq!(initial_deepest.prefix_bytes, expected_prefix_bytes);
        assert_eq!(initial_deepest.expires_at, original_expires as i64);

        harness
            .advance_and_dispatch_due(
                Duration::from_secs(270),
                &[DueJob {
                    session_key_hash: &session_key_hash,
                    generation: 1,
                    mock_prefix: Some("issue-1003"),
                }],
            )
            .await;
        let events = harness.mock_events();
        let renewals = events
            .iter()
            .filter(|event| event.kind == "renewal")
            .collect::<Vec<_>>();
        assert_eq!(renewals.len(), 1);
        let renewal = renewals.first().expect("provider renewal event");
        let live = harness.mock.last("proxy").expect("initial proxy event");
        assert!(renewal.accepted() && renewal.synthetic && renewal.hit);
        assert_eq!(renewal.cached_tokens, 83_900);
        assert_eq!(renewal.model, "claude-opus-5-5");
        assert_eq!(renewal.credential, live.credential);
        assert_eq!(renewal.cache_key, live.cache_key);
        let usage = sqlx::query_as::<_, (String, i64, i64, i64)>(
            "SELECT upstream_id, cache_read_input_tokens, cache_creation_input_tokens, list_status \
             FROM request_events_v1 WHERE source_kind = 'renewal'",
        )
        .fetch_all(storage.pool())
        .await
        .expect("persisted renewal usage");
        assert_eq!(usage, vec![(upstream_id.clone(), 83_900, 0, 200)]);
        let turns = sqlx::query_as::<_, (String, String, i64, i64)>(
            "SELECT session_key_hash, hit_miss, cache_read_input_tokens, cache_creation_input_tokens \
             FROM cache_keepalive_turns",
        )
        .fetch_all(storage.pool())
        .await
        .expect("persisted renewal turn");
        assert_eq!(
            turns,
            vec![(session_key_hash.clone(), "hit".to_owned(), 83_900, 0)]
        );
        let session_state = sqlx::query_as::<_, (String, String, String, i64)>(
            "SELECT status, terminal_reason, enqueue_state, generation \
             FROM cache_keepalive_sessions WHERE session_key_hash = ?",
        )
        .bind(&session_key_hash)
        .fetch_one(storage.pool())
        .await
        .expect("final capped session");
        assert_eq!(
            session_state,
            (
                "terminal".to_owned(),
                terminal_reason.to_owned(),
                "pending".to_owned(),
                if terminal_reason == "max_refreshes" {
                    2
                } else {
                    1
                }
            )
        );
        let renewed = observations(storage.pool(), &upstream_id, (NOW + 270) as i64, 1).await;
        assert_eq!(renewed.len(), 2, "renewal refreshes rather than adds a key");
        assert_eq!(
            renewed.first(),
            initial.first(),
            "shallower breakpoint is unchanged"
        );
        let refreshed = renewed.last().expect("renewed keyed observation");
        assert_eq!(refreshed.upstream_id, upstream_id);
        assert_eq!(refreshed.model, "claude-opus-5-5");
        assert_eq!(refreshed.key, initial_deepest.key);
        assert_eq!(refreshed.ttl, "0");
        assert_eq!(refreshed.expires_at, (NOW + 270 + 300 - grace) as i64);
        assert_eq!(refreshed.observed_at, (NOW + 270) as i64);
        assert_eq!(refreshed.schema, 6);
        assert_eq!(refreshed.block_index, 2);
        assert_eq!(refreshed.prefix_bytes, expected_prefix_bytes);
        assert_eq!(refreshed.estimate_source, "serialized_prefix_bytes_v1");

        // Latest master uses zero grace by default (#1002). Move beyond the
        // original full-TTL expiry, not only beyond the old 270-second expiry.
        harness.advance_clock(Duration::from_secs(original_expires + 1 - (NOW + 270)));
        let now = unix_secs(clock.now());
        assert!(now > original_expires);
        assert!(now < refreshed.expires_at as u64);
        let mut event_stream = harness.admin_get("/admin/v1/events/stream").await;
        assert_eq!(event_stream.status(), reqwest::StatusCode::OK);
        event_stream
            .chunk()
            .await
            .expect("connected event stream")
            .expect("connected event stream chunk");
        let follow_up = harness.post(body, session, None).await;
        assert_eq!(follow_up["usage"]["cache_read_input_tokens"], 83_900);
        assert_eq!(follow_up["usage"]["cache_creation_input_tokens"], 0);
        let follow_up_event = harness.mock.last("proxy").expect("follow-up proxy event");
        assert!(follow_up_event.accepted() && follow_up_event.hit);
        assert_eq!(follow_up_event.model, renewal.model);
        assert_eq!(follow_up_event.cache_key, renewal.cache_key);
        assert_eq!(follow_up_event.credential, renewal.credential);
        let routed_event = tokio::time::timeout(Duration::from_secs(20), async {
            let mut pending = String::new();
            loop {
                let data = event_stream
                    .chunk()
                    .await
                    .expect("live routing event stream")
                    .expect("live routing event chunk");
                pending.push_str(std::str::from_utf8(&data).expect("SSE UTF-8"));
                while let Some(end) = pending.find("\n\n") {
                    let frame = pending.drain(..end + 2).collect::<String>();
                    let Some(data) = frame.lines().find_map(|line| line.strip_prefix("data: "))
                    else {
                        continue;
                    };
                    let update: Value = serde_json::from_str(data).expect("live event JSON");
                    let event = &update["payload"]["event"];
                    if update["phase"] == "final"
                        && event["source_kind"] == "proxy"
                        && event["ts"] == now
                    {
                        return event.clone();
                    }
                }
            }
        })
        .await
        .expect("follow-up live routing event");
        assert_eq!(routed_event["upstream_id"], upstream_id);
    }
}

#[tokio::test]
async fn sim_principal_a_cap_crossing_long_gap_and_ask_tool() {
    let harness = Harness::new(8_388_608).await;
    let session = "00000000-0000-7000-8000-000000000001";
    let session_key_hash = harness.session_key_hash(session, None);
    let t1 = harness
        .post(
            request_body(355_453, "principal-a-t1", 83_900, "bash", "tool_use", false),
            session,
            None,
        )
        .await;
    assert_eq!(t1["stop_reason"], "tool_use");
    harness
        .wait_for_enqueued_generation(&session_key_hash, 1)
        .await;
    let t2 = harness
        .post(
            request_body(583_888, "principal-a-t2", 91_366, "bash", "tool_use", false),
            session,
            None,
        )
        .await;
    assert_eq!(t2["stop_reason"], "tool_use");
    harness
        .wait_for_enqueued_generation(&session_key_hash, 2)
        .await;
    let t3 = harness
        .post(
            request_body(
                3_368_863,
                "principal-a-gap",
                364_072,
                "wait",
                "tool_use",
                false,
            ),
            session,
            None,
        )
        .await;
    assert_eq!(t3["stop_reason"], "tool_use");
    harness
        .wait_for_enqueued_generation(&session_key_hash, 3)
        .await;
    for generation in 3..=5 {
        harness
            .advance_and_dispatch_due(
                Duration::from_secs(270),
                &[DueJob {
                    session_key_hash: &session_key_hash,
                    generation,
                    mock_prefix: Some("principal-a-gap"),
                }],
            )
            .await;
        let renewal = harness
            .mock
            .last("renewal")
            .expect("expected renewal attempt");
        assert!(
            renewal.accepted(),
            "renewal rejected: {:?}",
            renewal.rejection
        );
    }
    harness.advance_clock(Duration::from_secs(128));
    assert!(
        harness.mock_events().iter().all(|event| event.accepted()),
        "renewal rejected: {:?}",
        harness
            .mock_events()
            .iter()
            .filter(|event| !event.accepted())
            .collect::<Vec<_>>()
    );
    let t4 = harness
        .post(
            request_body(
                3_370_380,
                "principal-a-gap",
                364_072,
                "bash",
                "tool_use",
                false,
            ),
            session,
            None,
        )
        .await;
    assert_eq!(t4["usage"]["cache_read_input_tokens"], 364_072);
    let t4_generation = harness
        .wait_for_latest_enqueued_generation(&session_key_hash, Duration::from_secs(5))
        .await
        .expect("t4 session enqueue fence");
    assert!(t4_generation > 3, "t4 must follow three completed renewals");
    let ask = harness
        .post(
            request_body(
                2_902_959,
                "principal-a-ask",
                215_118,
                "ask",
                "tool_use",
                false,
            ),
            session,
            None,
        )
        .await;
    assert_eq!(ask["stop_reason"], "tool_use");
    harness.wait_for_session_count("cancelled", 1).await;
    assert!(!harness.mock.last("proxy").expect("latest proxy event").hit);
    let events = harness.mock_events();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.synthetic && event.hit)
            .count(),
        3
    );
    assert_eq!(
        events
            .iter()
            .rev()
            .find(|event| event.kind == "proxy")
            .map(|event| event.cached_tokens),
        Some(215_118)
    );
    let (turns, renewal_events) = harness.count_renewal_rows().await;
    assert_eq!(renewal_events, 3);
    assert_eq!(turns, 3);
    assert_eq!(harness.renewal_hit_count().await, 3);
    assert!(harness.session_count("terminal", Some("cancelled")).await >= 1);
}

#[tokio::test]
async fn sim_principal_b_multi_agent_interleaving_and_4mb_gap() {
    let harness = Harness::new(8_388_608).await;
    let session = "00000000-0000-4000-8000-000000000002";
    let parent = harness
        .post(
            request_body(689_605, "parent", 267_130, "bash", "tool_use", false),
            session,
            None,
        )
        .await;
    assert_eq!(parent["stop_reason"], "tool_use");
    harness.wait_for_session_count("active", 1).await;
    let small = harness
        .post(
            request_body(947, "agent-small", 0, "bash", "tool_use", false),
            session,
            Some("subagent-worker-01"),
        )
        .await;
    assert_eq!(small["stop_reason"], "tool_use");
    assert!(
        harness
            .wait_for_not_tracked(
                &harness.session_key_hash(session, Some("subagent-worker-01")),
                Duration::from_secs(4)
            )
            .await
    );
    let agent = harness
        .post(
            request_body(298_803, "agent", 95_637, "bash", "tool_use", false),
            session,
            Some("subagent-worker-01"),
        )
        .await;
    assert_eq!(agent["stop_reason"], "tool_use");
    harness.wait_for_session_count("active", 2).await;
    let done = harness
        .post(
            request_body(299_403, "agent-end", 95_637, "bash", "end_turn", false),
            session,
            Some("subagent-worker-01"),
        )
        .await;
    assert_eq!(done["stop_reason"], "end_turn");
    harness.wait_for_session_count("cancelled", 1).await;
    assert!(harness.session_count("active", None).await >= 1);
    let parent_gap = harness
        .post(
            request_body(4_182_148, "parent-gap", 598_453, "bash", "tool_use", false),
            session,
            None,
        )
        .await;
    assert_eq!(parent_gap["stop_reason"], "tool_use");
    let parent_session_key_hash = harness.session_key_hash(session, None);
    harness
        .wait_for_enqueued_generation(&parent_session_key_hash, 2)
        .await;
    for generation in 2..=4 {
        harness
            .advance_and_dispatch_due(
                Duration::from_secs(270),
                &[DueJob {
                    session_key_hash: &parent_session_key_hash,
                    generation,
                    mock_prefix: Some("parent-gap"),
                }],
            )
            .await;
        let renewal = harness
            .mock
            .last("renewal")
            .expect("expected renewal attempt");
        assert!(
            renewal.accepted(),
            "renewal rejected: {:?}",
            renewal.rejection
        );
    }
    harness.advance_clock(Duration::from_secs(80));
    let resumed = harness
        .post(
            request_body(4_182_731, "parent-gap", 598_453, "bash", "tool_use", false),
            session,
            None,
        )
        .await;
    assert_eq!(resumed["usage"]["cache_read_input_tokens"], 598_453);
    let haiku = harness
        .post(
            request_body(100_000, "haiku", 3_547, "bash", "tool_use", false),
            session,
            Some("haiku-agent"),
        )
        .await;
    assert_eq!(haiku["stop_reason"], "tool_use");
    assert!(
        harness
            .wait_for_not_tracked(
                &harness.session_key_hash(session, Some("haiku-agent")),
                Duration::from_secs(4)
            )
            .await
    );
    let oversized = harness
        .post(
            request_body(
                9 * 1024 * 1024,
                "oversized",
                700_000,
                "bash",
                "tool_use",
                true,
            ),
            session,
            None,
        )
        .await;
    assert_eq!(oversized["stop_reason"], "tool_use");
    // The oversized body reaches snapshot capture and persists the size-limit
    // reason on the parent completion.
    assert_eq!(
        harness
            .wait_for_not_tracked_reason(&parent_session_key_hash, Duration::from_secs(4))
            .await
            .as_deref(),
        Some("snapshot too large"),
        "the oversized parent completion must persist its own not-tracked reason"
    );
    harness.wait_for_session_count("cancelled", 2).await;
    let events = harness.mock_events();
    assert!(
        events
            .iter()
            .any(|event| event.cached_tokens == 598_453 && !event.hit)
    );
    assert!(
        events
            .iter()
            .filter(|event| event.synthetic && event.hit)
            .count()
            >= 3
    );
    assert!(
        events
            .iter()
            .any(|event| event.cached_tokens == 598_453 && event.hit)
    );
    assert!(harness.session_count("terminal", Some("cancelled")).await >= 2);
    assert!(harness.decision_count("cached prefix too small%").await >= 1);
    assert!(harness.decision_count("snapshot too large").await >= 1);
}

#[test]
fn session_key_agent_normalization_matches_real_headers() {
    let principal = Uuid::from_u128(7);
    assert_eq!(SessionKey::normalize_agent_id(Some(" main ")), None);
    assert_eq!(
        SessionKey::normalize_agent_id(Some(" subagent-worker-01 ")),
        Some("subagent-worker-01")
    );
    assert_ne!(
        SessionKey::from_thread_id(principal, "00000000-0000-4000-8000-000000000002"),
        SessionKey::from_thread_and_agent(
            principal,
            "00000000-0000-4000-8000-000000000002",
            "subagent-worker-01"
        )
    );
}
