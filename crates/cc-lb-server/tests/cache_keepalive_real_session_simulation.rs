//! Deterministic end-to-end simulations for real prompt-cache sessions.

use std::time::Duration;

use cc_lb_engine::cache_keepalive::SessionKey;
use uuid::Uuid;

use super::keepalive_support::harness::{DueJob, Harness, request_body};

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
