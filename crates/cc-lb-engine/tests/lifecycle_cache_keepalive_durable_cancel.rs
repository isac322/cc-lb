use crate::common;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_control::api_keys::principal_view::PrincipalView;
use cc_lb_engine::cache_keepalive::{
    CacheKeepaliveCancelRequest, CacheKeepaliveEnqueueError, CacheKeepaliveEnqueueRequest,
    CacheKeepaliveEnqueuer, CacheKeepaliveNotTrackedRequest, CancelReason,
};
use cc_lb_engine::{DispatchError, LifecycleConfig, UpstreamDispatch};
use cc_lb_storage_api::principal::{Limit, PrincipalKind, PrincipalRecord};
use cc_lb_storage_api::{CacheKeepaliveConfig, ClassifierConfig};
use cc_lb_upstream::SignedRequest;
use http::{Response, StatusCode};
use serde_json::json;

use common::{TestAuthn, TestState, collect_body, lifecycle_with_parts, managed_api_key, settle};

#[tokio::test]
async fn cache_keepalive_response_compaction_boundary_is_not_scheduled_json_and_sse() {
    let request = Bytes::from(
        serde_json::to_vec(&json!({
            "model": "claude-opus-4-6",
            "max_tokens": 4096,
            "context_management": {"edits": [{
                "type": "compact_20260112", "pause_after_compaction": true
            }]},
            "messages": [{"role": "user", "content": [{
                "type": "text", "text": "pre-compaction history",
                "cache_control": {"type": "ephemeral"}
            }]}]
        }))
        .expect("request JSON"),
    );
    for (stop_reason, content, streaming) in [
        (
            "compaction",
            json!([{"type": "compaction", "content": "summary"}]),
            false,
        ),
        (
            "end_turn",
            json!([
                {"type": "compaction", "content": "summary"},
                {"type": "text", "text": "done"}
            ]),
            false,
        ),
        (
            "compaction",
            json!([{"type": "compaction", "content": "summary"}]),
            true,
        ),
        (
            "end_turn",
            json!([
                {"type": "compaction", "content": "summary"},
                {"type": "text", "text": "done"}
            ]),
            true,
        ),
    ] {
        let response_body = Bytes::from(
            serde_json::to_vec(&json!({
                "type": "message",
                "id": "msg_compaction",
                "role": "assistant",
                "model": "claude-opus-4-6",
                "content": content,
                "stop_reason": stop_reason,
                "usage": {
                    "input_tokens": 0,
                    "output_tokens": 4,
                    "cache_read_input_tokens": 60_000
                }
            }))
            .expect("response JSON"),
        );
        let expected_body = if streaming {
            streaming_body_from_message(&response_body)
        } else {
            response_body.clone()
        };
        let enqueuer = Arc::new(RecordingEnqueuer::default());
        let response = handle_keepalive_response_with(
            response_body,
            Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>,
            principal_with_keepalive(),
            request.clone(),
            &[
                ("anthropic-beta", "compact-2026-01-12"),
                ("x-claude-code-request-class", "main"),
            ],
            streaming,
        )
        .await;
        let (status, _headers, body) = collect_body(response).await;
        settle().await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, expected_body, "streaming={streaming}");
        assert!(
            enqueuer.enqueued().is_empty(),
            "stop_reason={stop_reason}, streaming={streaming}"
        );
    }
}

#[tokio::test]
async fn explicit_compaction_request_class_is_excluded_from_keepalive() {
    let enqueuer = Arc::new(RecordingEnqueuer::default());
    let response = handle_keepalive_response_with(
        agent_turn_body("msg_explicit_compaction", "Bash", 12_000),
        Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>,
        principal_with_keepalive(),
        standard_keepalive_request(),
        &[("x-claude-code-request-class", "compaction")],
        false,
    )
    .await;
    let (status, _headers, _body) = collect_body(response).await;
    settle().await;
    assert_eq!(status, StatusCode::OK);
    assert!(enqueuer.enqueued().is_empty());
    assert!(enqueuer.cancelled().is_empty());
    assert!(enqueuer.not_tracked().is_empty());
}

#[tokio::test]
async fn compaction_block_precedes_tool_use_without_enqueue_json_and_sse() {
    for streaming in [false, true] {
        let response_body = Bytes::from(
            serde_json::to_vec(&json!({
                "type": "message",
                "id": "msg_compaction_tool",
                "role": "assistant",
                "content": [
                    {"type": "compaction", "content": "summary"},
                    {"type": "tool_use", "id": "toolu_1", "name": "Bash", "input": {}}
                ],
                "stop_reason": "tool_use",
                "usage": {"input_tokens": 0, "output_tokens": 4, "cache_read_input_tokens": 60_000}
            }))
            .expect("response JSON"),
        );
        let expected_body = if streaming {
            streaming_body_from_message(&response_body)
        } else {
            response_body.clone()
        };
        let enqueuer = Arc::new(RecordingEnqueuer::default());
        let response = handle_keepalive_response_with(
            response_body,
            Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>,
            principal_with_keepalive(),
            standard_keepalive_request(),
            &[],
            streaming,
        )
        .await;
        let (status, _headers, body) = collect_body(response).await;
        settle().await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, expected_body, "streaming={streaming}");
        assert!(enqueuer.enqueued().is_empty(), "streaming={streaming}");
    }
}

#[tokio::test]
async fn compaction_response_without_current_breakpoint_fences_generation_json_and_sse() {
    for (stop_reason, content) in [
        (
            "compaction",
            json!([{"type": "compaction", "content": "summary"}]),
        ),
        (
            "tool_use",
            json!([
                {"type": "compaction", "content": "summary"},
                {"type": "tool_use", "id": "tool-1", "name": "Bash", "input": {}}
            ]),
        ),
    ] {
        for streaming in [false, true] {
            let enqueuer = Arc::new(ScopedKeepaliveEnqueuer::default());
            let principal = principal_with_keepalive();
            let main_headers = [("x-claude-code-session-id", "no-breakpoint")];
            let child_headers = [
                ("x-claude-code-session-id", "no-breakpoint"),
                ("x-claude-code-agent-id", "child-agent"),
            ];
            establish_scoped_keepalive(&enqueuer, principal.clone(), &main_headers).await;
            establish_scoped_keepalive(&enqueuer, principal.clone(), &child_headers).await;

            let response_body = Bytes::from(
                serde_json::to_vec(&json!({
                    "type": "message",
                    "id": "msg_no_breakpoint_compaction",
                    "content": content.clone(),
                    "stop_reason": stop_reason,
                    "usage": {}
                }))
                .expect("response JSON"),
            );
            let expected_body = response_body.clone();
            let (response, _) = observed_keepalive_response(
                response_body,
                Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>,
                principal,
                no_breakpoint_request(),
                &main_headers,
                streaming,
            )
            .await;
            assert_keepalive_response_fidelity(response, &expected_body, streaming).await;
            enqueuer.wait_for_completion().await;

            let state = enqueuer.snapshot();
            assert_eq!(
                state.generations.len(),
                1,
                "only unrelated child generation remains: {stop_reason}, streaming={streaming}"
            );
            assert!(
                state
                    .generations
                    .keys()
                    .any(|key| key.ends_with("/thread:no-breakpoint/agent:child-agent"))
            );
            assert_eq!(
                state
                    .operations
                    .iter()
                    .filter(|operation| matches!(operation, KeepaliveOperation::Enqueue(_, _)))
                    .count(),
                2
            );
            assert_eq!(
                state
                    .operations
                    .iter()
                    .filter(|operation| matches!(operation, KeepaliveOperation::Cancel(key, _, CancelReason::UserTurnDetected) if key.ends_with("/thread:no-breakpoint")))
                    .count(),
                1
            );
            assert_eq!(
                state
                    .operations
                    .iter()
                    .filter(|operation| matches!(operation, KeepaliveOperation::NotTracked(_, _)))
                    .count(),
                0
            );
        }
    }
}

#[tokio::test]
async fn ordinary_response_without_cache_breakpoint_does_not_mutate_keepalive() {
    let enqueuer = Arc::new(ScopedKeepaliveEnqueuer::default());
    let principal = principal_with_keepalive();
    let headers = [("x-claude-code-session-id", "ordinary-no-breakpoint")];
    establish_scoped_keepalive(&enqueuer, principal.clone(), &headers).await;
    let before = enqueuer.snapshot();

    let response_body = agent_turn_body("msg_ordinary_no_breakpoint", "Bash", 12_000);
    let (response, _) = observed_keepalive_response(
        response_body.clone(),
        Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>,
        principal,
        no_breakpoint_request(),
        &headers,
        false,
    )
    .await;
    assert_keepalive_response_fidelity(response, &response_body, false).await;
    assert_eq!(enqueuer.snapshot(), before);
}

#[tokio::test]
async fn compaction_response_fences_matching_generation_and_allows_fresh_completion() {
    #[derive(Default)]
    struct GenerationRecordingEnqueuer {
        enqueued: Mutex<Vec<CacheKeepaliveEnqueueRequest>>,
        cancelled: Mutex<Vec<CacheKeepaliveCancelRequest>>,
    }

    #[async_trait]
    impl CacheKeepaliveEnqueuer for GenerationRecordingEnqueuer {
        async fn enqueue_cache_keepalive(
            &self,
            request: CacheKeepaliveEnqueueRequest,
        ) -> Result<(), CacheKeepaliveEnqueueError> {
            self.enqueued.lock().expect("enqueued lock").push(request);
            Ok(())
        }

        async fn cancel_cache_keepalive(
            &self,
            request: CacheKeepaliveCancelRequest,
        ) -> Result<(), CacheKeepaliveEnqueueError> {
            self.cancelled.lock().expect("cancelled lock").push(request);
            Ok(())
        }

        async fn record_cache_keepalive_not_tracked(
            &self,
            _request: CacheKeepaliveNotTrackedRequest,
        ) -> Result<(), CacheKeepaliveEnqueueError> {
            Ok(())
        }

        async fn observe_session_generation(
            &self,
            _session_key_hash: &str,
        ) -> Result<Option<u64>, CacheKeepaliveEnqueueError> {
            Ok(Some(42))
        }
    }

    let principal = principal_with_keepalive();
    let enqueuer = Arc::new(GenerationRecordingEnqueuer::default());
    let before = handle_keepalive_response_with(
        agent_turn_body("msg_before_compaction", "Bash", 12_000),
        Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>,
        principal.clone(),
        standard_keepalive_request(),
        &[("x-claude-code-session-id", "compaction-fence")],
        false,
    )
    .await;
    assert_eq!(collect_body(before).await.0, StatusCode::OK);
    settle().await;

    let boundary = handle_keepalive_response_with(
        Bytes::from_static(
            br#"{"type":"message","id":"msg_compaction_fence","content":[{"type":"compaction","content":"summary"}],"stop_reason":"compaction","usage":{"cache_read_input_tokens":60000}}"#,
        ),
        Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>,
        principal.clone(),
        standard_keepalive_request(),
        &[("x-claude-code-session-id", "compaction-fence")],
        false,
    )
    .await;
    assert_eq!(collect_body(boundary).await.0, StatusCode::OK);
    settle().await;

    assert_eq!(enqueuer.enqueued.lock().expect("enqueued lock").len(), 1);
    {
        let cancelled = enqueuer.cancelled.lock().expect("cancelled lock");
        let completion_cancellations = cancelled
            .iter()
            .filter(|request| request.reason == CancelReason::UserTurnDetected)
            .collect::<Vec<_>>();
        assert_eq!(completion_cancellations.len(), 2);
        assert!(completion_cancellations.iter().any(|request| {
            request
                .session_key_hash
                .ends_with("/thread:compaction-fence")
        }));
        assert!(completion_cancellations.iter().any(|request| {
            request
                .session_key_hash
                .ends_with("/thread:compaction-fence/agent:subagent")
        }));
        assert!(
            completion_cancellations
                .iter()
                .all(|request| request.expected_generation == Some(42))
        );
    }

    let after = handle_keepalive_response_with(
        agent_turn_body("msg_after_compaction", "Bash", 12_000),
        Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>,
        principal,
        standard_keepalive_request(),
        &[("x-claude-code-session-id", "compaction-fence")],
        false,
    )
    .await;
    assert_eq!(collect_body(after).await.0, StatusCode::OK);
    settle().await;

    let enqueued = enqueuer.enqueued.lock().expect("enqueued lock");
    assert_eq!(enqueued.len(), 2);
    assert_eq!(enqueued[0].expected_generation, Some(42));
    assert_eq!(enqueued[1].expected_generation, Some(42));
}

#[tokio::test]
async fn compaction_fence_retains_unrelated_agent_scope_state() {
    struct ScopedEnqueuer {
        active: Mutex<HashMap<String, u64>>,
        cancelled: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl CacheKeepaliveEnqueuer for ScopedEnqueuer {
        async fn enqueue_cache_keepalive(
            &self,
            request: CacheKeepaliveEnqueueRequest,
        ) -> Result<(), CacheKeepaliveEnqueueError> {
            self.active.lock().expect("active sessions lock").insert(
                request.session_key_hash,
                request.expected_generation.unwrap_or(1),
            );
            Ok(())
        }

        async fn cancel_cache_keepalive(
            &self,
            request: CacheKeepaliveCancelRequest,
        ) -> Result<(), CacheKeepaliveEnqueueError> {
            let mut active = self.active.lock().expect("active sessions lock");
            if active.get(&request.session_key_hash) == request.expected_generation.as_ref() {
                active.remove(&request.session_key_hash);
            }
            self.cancelled
                .lock()
                .expect("cancelled keys lock")
                .push(request.session_key_hash);
            Ok(())
        }

        async fn record_cache_keepalive_not_tracked(
            &self,
            _request: CacheKeepaliveNotTrackedRequest,
        ) -> Result<(), CacheKeepaliveEnqueueError> {
            Ok(())
        }

        async fn observe_session_generation(
            &self,
            session_key_hash: &str,
        ) -> Result<Option<u64>, CacheKeepaliveEnqueueError> {
            Ok(self
                .active
                .lock()
                .expect("active sessions lock")
                .get(session_key_hash)
                .copied())
        }
    }

    let principal = principal_with_keepalive();
    let enqueuer = Arc::new(ScopedEnqueuer {
        active: Mutex::new(HashMap::new()),
        cancelled: Mutex::new(Vec::new()),
    });
    for (response, headers) in [
        (
            agent_turn_body("msg_main_scope", "Bash", 12_000),
            vec![("x-claude-code-session-id", "scope-session")],
        ),
        (
            agent_turn_body("msg_child_scope", "Bash", 12_000),
            vec![
                ("x-claude-code-session-id", "scope-session"),
                ("x-claude-code-agent-id", "child-agent"),
            ],
        ),
        (
            Bytes::from_static(
                br#"{"type":"message","id":"msg_main_compaction","content":[{"type":"compaction","content":"summary"}],"stop_reason":"compaction","usage":{"cache_read_input_tokens":60000}}"#,
            ),
            vec![("x-claude-code-session-id", "scope-session")],
        ),
    ] {
        let response = handle_keepalive_response_with(
            response,
            Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>,
            principal.clone(),
            standard_keepalive_request(),
            &headers,
            false,
        )
        .await;
        assert_eq!(collect_body(response).await.0, StatusCode::OK);
        settle().await;
    }

    let active = enqueuer.active.lock().expect("active sessions lock");
    assert!(
        !active
            .keys()
            .any(|key| key.ends_with("/thread:scope-session"))
    );
    assert!(
        active
            .keys()
            .any(|key| key.ends_with("/thread:scope-session/agent:child-agent"))
    );
    drop(active);
    let cancelled = enqueuer.cancelled.lock().expect("cancelled keys lock");
    assert_eq!(
        cancelled
            .iter()
            .filter(|key| key.ends_with("/thread:scope-session"))
            .count(),
        3
    );
    assert_eq!(
        cancelled
            .iter()
            .filter(|key| key.ends_with("/thread:scope-session/agent:child-agent"))
            .count(),
        1
    );
}

#[tokio::test]
async fn cache_keepalive_user_turn_cancels_durable_without_scheduler() {
    let calls = Arc::new(AtomicUsize::new(0));
    let response = handle_keepalive_response(
        Bytes::from_static(
            br#"{"type":"message","id":"msg_user_turn","content":[{"type":"text","text":"done"}],"stop_reason":"end_turn","usage":{"input_tokens":10,"output_tokens":2}}"#,
        ),
        Arc::new(RecordingCancelEnqueuer {
            calls: Arc::clone(&calls),
        }),
    )
    .await;

    let (status, _headers, body) = collect_body(response).await;
    settle().await;
    assert_eq!(status, StatusCode::OK);
    assert!(String::from_utf8_lossy(&body).contains("msg_user_turn"));
    assert_eq!(calls.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn cache_keepalive_ambiguous_cancels_durable_without_scheduler() {
    let enqueuer = Arc::new(RecordingEnqueuer::default());
    let response = handle_keepalive_response_with(
        Bytes::from_static(
            br#"{"type":"message","id":"msg_ambiguous","content":[{"type":"text","text":"partial"}],"stop_reason":"max_tokens","usage":{"input_tokens":10,"output_tokens":2}}"#,
        ),
        Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>,
        principal_with_keepalive(),
        standard_keepalive_request(),
        &[("x-claude-code-session-id", "ambiguous-turn")],
        false,
    )
    .await;

    let (status, _headers, body) = collect_body(response).await;
    enqueuer.wait_for_completion().await;
    assert_eq!(status, StatusCode::OK);
    assert!(String::from_utf8_lossy(&body).contains("msg_ambiguous"));
    assert_eq!(enqueuer.completion_cancellations().len(), 1);
    assert_eq!(
        enqueuer.completion_cancellations()[0].reason,
        CancelReason::AmbiguousTurn
    );
}

#[tokio::test]
async fn cache_keepalive_cancel_failure_does_not_change_proxy_response() {
    let calls = Arc::new(AtomicUsize::new(0));
    let response = handle_keepalive_response(
        Bytes::from_static(
            br#"{"type":"message","id":"msg_cancel_fail","content":[{"type":"text","text":"done"}],"stop_reason":"end_turn","usage":{"input_tokens":10,"output_tokens":2}}"#,
        ),
        Arc::new(FailingCancelEnqueuer {
            calls: Arc::clone(&calls),
        }),
    )
    .await;

    let (status, _headers, body) = collect_body(response).await;
    settle().await;
    assert_eq!(status, StatusCode::OK);
    assert!(String::from_utf8_lossy(&body).contains("msg_cancel_fail"));
    assert_eq!(calls.load(Ordering::Relaxed), 2);
}
#[tokio::test]
async fn cache_keepalive_subagent_and_auxiliary_cancel_only_eligible_scopes() {
    let keys = Arc::new(std::sync::Mutex::new(Vec::new()));
    let enqueuer = Arc::new(KeyRecordingEnqueuer {
        keys: Arc::clone(&keys),
    });
    let state = TestState::default();
    let principal_view = Arc::new(PrincipalView::from_db(
        &[principal_with_keepalive()],
        std::collections::HashMap::new(),
    ));
    let lifecycle = lifecycle_with_parts(
        TestAuthn::with_principal_view(state, principal_view),
        Arc::new(FixedSuccessDispatch {
            body: Bytes::from_static(
                br#"{"type":"message","id":"msg_subagent","content":[{"type":"text","text":"done"}],"stop_reason":"end_turn","usage":{"input_tokens":10,"output_tokens":2}}"#,
            ),
        }),
        LifecycleConfig::default(),
    )
    .with_cache_keepalive_enqueuer(enqueuer);

    let subagent = http::Request::builder()
        .method(http::Method::POST)
        .uri("/v1/messages")
        .header("x-api-key", managed_api_key())
        .header("x-claude-code-session-id", "session-1")
        .header("x-claude-code-agent-id", "agent-1")
        .header("anthropic-version", "2023-06-01")
        .body(Bytes::from_static(
            br#"{"model":"claude-test","max_tokens":32,"system":[{"type":"text","text":"cached","cache_control":{"type":"ephemeral","ttl":"5m"}}],"messages":[{"role":"user","content":"hello"}]}"#,
        ))
        .expect("subagent request builds");
    let auth = lifecycle
        .authenticate(subagent.headers())
        .await
        .expect("subagent authenticates");
    lifecycle
        .handle(subagent, &auth)
        .await
        .expect("subagent response handles");
    settle().await;

    let auxiliary_body = Bytes::from_static(
        br#"{"model":"claude-test","max_tokens":32,"system":[{"type":"text","text":"cached","cache_control":{"type":"ephemeral","ttl":"5m"}}],"messages":[{"role":"user","content":"Generate a concise, sentence-case title (3-7 words)"}]}"#,
    );
    let auxiliary = http::Request::builder()
        .method(http::Method::POST)
        .uri("/v1/messages")
        .header("x-api-key", managed_api_key())
        .header("x-claude-code-session-id", "session-1")
        .header("x-claude-code-request-class", "session_title")
        .header("anthropic-version", "2023-06-01")
        .body(auxiliary_body)
        .expect("auxiliary request builds");
    let auth = lifecycle
        .authenticate(auxiliary.headers())
        .await
        .expect("auxiliary authenticates");
    lifecycle
        .handle(auxiliary, &auth)
        .await
        .expect("auxiliary response handles");
    settle().await;

    let keys = keys.lock().expect("recorded cancellation keys lock");
    assert_eq!(
        keys.len(),
        2,
        "request fence and user-turn cancellation stay in the subagent scope; auxiliary must not cancel"
    );
    assert!(keys[0].ends_with("/thread:session-1/agent:agent-1"));
    assert!(!keys.iter().any(|key| key.ends_with("/thread:session-1")));
}

#[tokio::test]
async fn cache_keepalive_trimmed_uncached_tail_is_enqueued() {
    let request_body = serde_json::to_vec(&json!({
        "model": "claude-test",
        "max_tokens": 32,
        "system": [{
            "type": "text",
            "text": "cached",
            "cache_control": {"type": "ephemeral", "ttl": "5m"}
        }],
        "messages": [{
            "role": "user",
            "content": "x".repeat(600_000)
        }]
    }))
    .expect("request JSON serializes");
    assert!(request_body.len() > 524_288);

    let enqueuer = Arc::new(RecordingEnqueuer::default());
    let response = handle_keepalive_response_with(
        Bytes::from(
            serde_json::to_vec(&json!({
                "type": "message",
                "id": "msg_trimmed_tail",
                "content": [{
                    "type": "tool_use",
                    "id": "toolu_trimmed_tail",
                    "name": "Bash",
                    "input": {}
                }],
                "stop_reason": "tool_use",
                "usage": {
                    "input_tokens": 10,
                    "cache_read_input_tokens": 12_000,
                    "output_tokens": 2
                }
            }))
            .expect("response JSON serializes"),
        ),
        Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>,
        principal_with_keepalive(),
        Bytes::from(request_body),
        &[("x-claude-code-session-id", "trimmed-tail")],
        false,
    )
    .await;

    let (status, _headers, _body) = collect_body(response).await;
    settle().await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(enqueuer.enqueued().len(), 1);
    assert!(enqueuer.completion_cancellations().is_empty());
    assert!(enqueuer.not_tracked().is_empty());
}

#[tokio::test]
async fn cache_keepalive_oversized_cached_prefix_durably_cancels_agent_and_user_turns() {
    let request_body = oversized_cached_prefix_request("oversized-prefix");
    for (response_body, expected_decision) in [
        (
            json!({
                "type": "message",
                "id": "msg_oversized_agent",
                "content": [{
                    "type": "tool_use",
                    "id": "toolu_oversized_agent",
                    "name": "Bash",
                    "input": {}
                }],
                "stop_reason": "tool_use",
                "usage": {"cache_read_input_tokens": 12_000}
            }),
            "agent",
        ),
        (
            json!({
                "type": "message",
                "id": "msg_oversized_user",
                "content": [{"type": "text", "text": "done"}],
                "stop_reason": "end_turn",
                "usage": {"input_tokens": 10}
            }),
            "user",
        ),
    ] {
        let enqueuer = Arc::new(RecordingEnqueuer::default());
        let response = handle_keepalive_response_with(
            Bytes::from(serde_json::to_vec(&response_body).expect("response JSON serializes")),
            Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>,
            principal_with_keepalive(),
            request_body.clone(),
            &[("x-claude-code-session-id", expected_decision)],
            false,
        )
        .await;

        let (status, _headers, _body) = collect_body(response).await;
        settle().await;
        assert_eq!(status, StatusCode::OK);
        assert!(enqueuer.enqueued().is_empty());
        assert_eq!(enqueuer.not_tracked().len(), 1);
        assert_eq!(enqueuer.not_tracked()[0].reason, "snapshot too large");
        assert_eq!(enqueuer.completion_cancellations().len(), 1);
        assert_eq!(
            enqueuer.completion_cancellations()[0].reason,
            CancelReason::SnapshotTooLarge
        );
    }
}

#[tokio::test]
async fn cache_keepalive_subagent_cancellation_is_scoped_away_from_parent() {
    let principal = principal_with_keepalive();
    let enqueuer = Arc::new(RecordingEnqueuer::default());
    let request_body = standard_keepalive_request();

    let parent_response = handle_keepalive_response_with(
        agent_turn_body("msg_parent", "Bash", 12_000),
        Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>,
        principal.clone(),
        request_body.clone(),
        &[("x-claude-code-session-id", "S")],
        false,
    )
    .await;
    let _ = collect_body(parent_response).await;
    settle().await;
    assert_eq!(enqueuer.enqueued().len(), 1);
    assert!(
        enqueuer.enqueued()[0]
            .session_key_hash
            .ends_with("/thread:S")
    );

    let subagent_response = handle_keepalive_response_with(
        end_turn_body("msg_subagent"),
        Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>,
        principal.clone(),
        request_body.clone(),
        &[
            ("x-claude-code-session-id", "S"),
            ("x-claude-code-agent-id", "agent-sub-1"),
        ],
        false,
    )
    .await;
    let _ = collect_body(subagent_response).await;
    settle().await;
    assert_eq!(enqueuer.completion_cancellations().len(), 1);
    assert!(
        enqueuer.completion_cancellations()[0]
            .session_key_hash
            .ends_with("/thread:S/agent:agent-sub-1")
    );
    assert!(
        !enqueuer.completion_cancellations()[0]
            .session_key_hash
            .ends_with("/thread:S")
    );

    let main_response = handle_keepalive_response_with(
        end_turn_body("msg_main"),
        Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>,
        principal,
        request_body,
        &[
            ("x-claude-code-session-id", "S"),
            ("x-claude-code-agent-id", "main"),
        ],
        false,
    )
    .await;
    let _ = collect_body(main_response).await;
    settle().await;
    assert_eq!(enqueuer.completion_cancellations().len(), 2);
    assert!(
        enqueuer.completion_cancellations()[1]
            .session_key_hash
            .ends_with("/thread:S")
    );
}

#[tokio::test]
async fn cache_keepalive_omp_ask_tool_use_cancels_as_user_turn() {
    let enqueuer = Arc::new(RecordingEnqueuer::default());
    let response = handle_keepalive_response_with(
        tool_use_body("msg_omp_ask", "ask", 12_000),
        Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>,
        principal_with_keepalive(),
        standard_keepalive_request(),
        &[("x-claude-code-session-id", "omp-ask")],
        false,
    )
    .await;
    let (status, _headers, _body) = collect_body(response).await;
    settle().await;

    assert_eq!(status, StatusCode::OK);
    assert!(enqueuer.enqueued().is_empty());
    assert_eq!(
        enqueuer.not_tracked()[0].reason,
        "user turn (stop_reason=tool_use)"
    );
    assert_eq!(
        enqueuer.completion_cancellations()[0].reason,
        CancelReason::UserTurnDetected
    );
}

#[tokio::test]
async fn cache_keepalive_small_cached_prefix_cancels_without_enqueue_json_and_sse() {
    for streaming in [false, true] {
        let enqueuer = Arc::new(RecordingEnqueuer::default());
        let response = handle_keepalive_response_with(
            tool_use_body("msg_small_prefix", "Bash", 3_547),
            Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>,
            principal_with_keepalive(),
            standard_keepalive_request(),
            &[(
                "x-claude-code-session-id",
                if streaming { "small-sse" } else { "small-json" },
            )],
            streaming,
        )
        .await;
        let (status, _headers, _body) = collect_body(response).await;
        settle().await;

        assert_eq!(status, StatusCode::OK);
        assert!(enqueuer.enqueued().is_empty());
        assert_eq!(
            enqueuer.not_tracked()[0].reason,
            "cached prefix too small (3547 tokens < minimum 10000)"
        );
        assert_eq!(
            enqueuer.completion_cancellations()[0].reason,
            CancelReason::CachedPrefixTooSmall
        );
    }
}

#[tokio::test]
async fn cache_keepalive_zero_token_incompatible_requests_are_not_tracked_json_and_sse() {
    for streaming in [false, true] {
        for (field, value, reason) in [
            (
                "thinking",
                json!({"type": "enabled", "budget_tokens": 1024}),
                "enabled thinking",
            ),
            (
                "output_config",
                json!({"format": {"type": "json_schema", "schema": {"type": "object"}}}),
                "structured output",
            ),
            (
                "output_config",
                json!({"format": null}),
                "structured output",
            ),
            ("tool_choice", json!({"type": "any"}), "forced tool_choice"),
            (
                "tool_choice",
                json!({"type": "tool", "name": "Bash"}),
                "forced tool_choice",
            ),
        ] {
            let enqueuer = Arc::new(RecordingEnqueuer::default());
            let mut request: serde_json::Value =
                serde_json::from_slice(&standard_keepalive_request()).expect("request fixture");
            request[field] = value;
            request["max_tokens"] = json!(2048);
            request["stream"] = json!(streaming);
            let response = handle_keepalive_response_with(
                tool_use_body("msg_incompatible", "Bash", 12_000),
                Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>,
                principal_with_keepalive(),
                Bytes::from(serde_json::to_vec(&request).expect("request serializes")),
                &[],
                streaming,
            )
            .await;
            assert_eq!(collect_body(response).await.0, StatusCode::OK);
            enqueuer.wait_for_completion().await;
            assert!(enqueuer.enqueued().is_empty());
            assert_eq!(enqueuer.not_tracked().len(), 1);
            assert_eq!(
                enqueuer.not_tracked()[0].reason,
                format!("zero-token renewal unsupported: {reason}")
            );
            assert_eq!(
                enqueuer.completion_cancellations()[0].reason.as_str(),
                "zero_token_renewal_unsupported"
            );
        }
    }
}

#[tokio::test]
async fn cache_keepalive_adaptive_auto_requests_remain_eligible_json_and_sse() {
    for streaming in [false, true] {
        let enqueuer = Arc::new(RecordingEnqueuer::default());
        let mut request: serde_json::Value =
            serde_json::from_slice(&standard_keepalive_request()).expect("request fixture");
        request["thinking"] = json!({"type": "adaptive"});
        request["output_config"] = json!({"effort": "medium"});
        request["tool_choice"] = json!({"type": "auto"});
        request["stream"] = json!(streaming);
        let response = handle_keepalive_response_with(
            tool_use_body("msg_adaptive", "Bash", 12_000),
            Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>,
            principal_with_keepalive(),
            Bytes::from(serde_json::to_vec(&request).expect("request serializes")),
            &[],
            streaming,
        )
        .await;
        assert_eq!(collect_body(response).await.0, StatusCode::OK);
        enqueuer.wait_for_completion().await;
        assert!(enqueuer.not_tracked().is_empty());
        assert!(enqueuer.completion_cancellations().is_empty());
        let enqueued = enqueuer.enqueued();
        assert_eq!(enqueued.len(), 1);
        let renewal: serde_json::Value = serde_json::from_slice(
            &enqueued[0]
                .snapshot
                .build_keepalive_body()
                .expect("renewal body"),
        )
        .expect("renewal JSON");
        for field in ["thinking", "output_config", "tool_choice"] {
            assert_eq!(renewal[field], request[field]);
        }
        assert_eq!(renewal["max_tokens"], 0);
        assert!(renewal.get("stream").is_none());
    }
}

#[tokio::test]
async fn cache_keepalive_shared_lifecycle_serializes_buffered_and_streaming_completions() {
    struct SequenceDispatch {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl UpstreamDispatch for SequenceDispatch {
        async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
            let idx = self.calls.fetch_add(1, Ordering::Relaxed);
            if idx == 0 {
                let mut response =
                    Response::new(Body::from(agent_turn_body("msg_1", "bash", 10_000)));
                *response.status_mut() = StatusCode::OK;
                response.headers_mut().insert(
                    http::header::CONTENT_TYPE,
                    http::HeaderValue::from_static("application/json"),
                );
                Ok(response)
            } else {
                let sse = streaming_body_from_message(&end_turn_body("msg_2"));
                let mut response = Response::new(Body::from(sse));
                *response.status_mut() = StatusCode::OK;
                response.headers_mut().insert(
                    http::header::CONTENT_TYPE,
                    http::HeaderValue::from_static("text/event-stream"),
                );
                Ok(response)
            }
        }
    }

    #[derive(Default)]
    struct SlowEnqueueEnqueuer {
        events: Mutex<Vec<&'static str>>,
    }

    #[async_trait]
    impl CacheKeepaliveEnqueuer for SlowEnqueueEnqueuer {
        async fn enqueue_cache_keepalive(
            &self,
            _request: CacheKeepaliveEnqueueRequest,
        ) -> Result<(), CacheKeepaliveEnqueueError> {
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            self.events.lock().expect("events lock").push("enqueue");
            Ok(())
        }

        async fn cancel_cache_keepalive(
            &self,
            _request: CacheKeepaliveCancelRequest,
        ) -> Result<(), CacheKeepaliveEnqueueError> {
            self.events.lock().expect("events lock").push("cancel");
            Ok(())
        }

        async fn record_cache_keepalive_not_tracked(
            &self,
            _request: CacheKeepaliveNotTrackedRequest,
        ) -> Result<(), CacheKeepaliveEnqueueError> {
            Ok(())
        }
    }

    let state = TestState::default();
    let principal_view = Arc::new(PrincipalView::from_db(
        &[principal_with_keepalive()],
        std::collections::HashMap::new(),
    ));
    let authn = TestAuthn::with_principal_view(state, principal_view);
    let dispatch: Arc<dyn UpstreamDispatch> = Arc::new(SequenceDispatch {
        calls: AtomicUsize::new(0),
    });
    let enqueuer = Arc::new(SlowEnqueueEnqueuer::default());
    let lifecycle = lifecycle_with_parts(authn, dispatch, LifecycleConfig::default())
        .with_cache_keepalive_enqueuer(Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>);

    for _ in 0..2 {
        let request = http::Request::builder()
            .method(http::Method::POST)
            .uri("/v1/messages")
            .header("x-api-key", managed_api_key())
            .header("x-session-id", "session-ordered")
            .header("anthropic-version", "2023-06-01")
            .body(standard_keepalive_request())
            .expect("test request builds");
        let auth = lifecycle
            .authenticate(request.headers())
            .await
            .expect("test request authenticates");
        let response = lifecycle
            .handle(request, &auth)
            .await
            .expect("lifecycle handles response");
        let _ = collect_body(response).await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    let events = enqueuer.events.lock().expect("events lock").clone();
    assert_eq!(events.last().copied(), Some("cancel"));
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum KeepaliveOperation {
    Observe(String, Option<u64>),
    Enqueue(String, Option<u64>),
    Cancel(String, Option<u64>, CancelReason),
    NotTracked(String, String),
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ScopedKeepaliveState {
    generations: std::collections::BTreeMap<String, u64>,
    request_owners: std::collections::BTreeMap<String, Option<u64>>,
    operations: Vec<KeepaliveOperation>,
    next_generation: u64,
}

#[derive(Default)]
struct ScopedKeepaliveEnqueuer {
    state: Mutex<ScopedKeepaliveState>,
    completed: tokio::sync::Notify,
    enqueued: tokio::sync::Notify,
}

impl ScopedKeepaliveEnqueuer {
    fn snapshot(&self) -> ScopedKeepaliveState {
        self.state.lock().expect("scoped keepalive state").clone()
    }

    async fn wait_for_completion(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(5), self.completed.notified())
            .await
            .expect("scoped keepalive completion");
    }

    async fn wait_for_enqueue(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(5), self.enqueued.notified())
            .await
            .expect("scoped keepalive enqueue");
    }
}

#[async_trait]
impl CacheKeepaliveEnqueuer for ScopedKeepaliveEnqueuer {
    async fn observe_session_generation(
        &self,
        key: &str,
    ) -> Result<Option<u64>, CacheKeepaliveEnqueueError> {
        let mut state = self.state.lock().expect("scoped keepalive state");
        let generation = state.generations.get(key).copied();
        state
            .operations
            .push(KeepaliveOperation::Observe(key.to_owned(), generation));
        Ok(generation)
    }

    async fn enqueue_cache_keepalive(
        &self,
        request: CacheKeepaliveEnqueueRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        let mut state = self.state.lock().expect("scoped keepalive state");
        let key = request.session_key_hash;
        let expected_owner = state
            .request_owners
            .remove(&key)
            .unwrap_or_else(|| state.generations.get(&key).copied());
        assert_eq!(request.expected_generation, expected_owner);
        state.operations.push(KeepaliveOperation::Enqueue(
            key.clone(),
            request.expected_generation,
        ));
        state.next_generation += 1;
        let generation = state.next_generation;
        state.generations.insert(key, generation);
        self.enqueued.notify_one();
        self.completed.notify_one();
        Ok(())
    }

    async fn cancel_cache_keepalive(
        &self,
        request: CacheKeepaliveCancelRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        let mut state = self.state.lock().expect("scoped keepalive state");
        let key = request.session_key_hash;
        let expected_owner = if request.reason == CancelReason::NewRequest {
            state
                .request_owners
                .insert(key.clone(), request.expected_generation);
            state.generations.get(&key).copied()
        } else {
            state
                .request_owners
                .remove(&key)
                .unwrap_or_else(|| state.generations.get(&key).copied())
        };
        assert_eq!(request.expected_generation, expected_owner);
        state.operations.push(KeepaliveOperation::Cancel(
            key.clone(),
            request.expected_generation,
            request.reason,
        ));
        state.generations.remove(&key);
        if request.reason.as_str() != "new_request" {
            self.completed.notify_one();
        }
        Ok(())
    }

    async fn record_cache_keepalive_not_tracked(
        &self,
        request: CacheKeepaliveNotTrackedRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        self.state
            .lock()
            .expect("scoped keepalive state")
            .operations
            .push(KeepaliveOperation::NotTracked(
                request.session_key_hash,
                request.reason,
            ));
        Ok(())
    }
}

async fn observed_keepalive_response(
    upstream_body: Bytes,
    enqueuer: Arc<dyn CacheKeepaliveEnqueuer>,
    principal: PrincipalRecord,
    request_body: Bytes,
    request_headers: &[(&str, &str)],
    streaming: bool,
) -> (Response<Body>, Option<String>) {
    let principal_name = principal.name.clone();
    let principal_view = Arc::new(PrincipalView::from_db(
        &[principal],
        std::collections::HashMap::new(),
    ));
    let mut authn = TestAuthn::with_principal_view(TestState::default(), principal_view);
    authn.authn = common::managed_authn(&principal_name);
    let dispatch: Arc<dyn UpstreamDispatch> = if streaming {
        Arc::new(FixedStreamingDispatch {
            body: streaming_body_from_message(&upstream_body),
        })
    } else {
        Arc::new(FixedSuccessDispatch {
            body: upstream_body,
        })
    };
    let bus = Arc::new(cc_lb_control::InMemoryBus::new());
    let mut events = bus.attach_lifecycle_assembler(128);
    let lifecycle = lifecycle_with_parts(authn, dispatch, LifecycleConfig::default())
        .with_cache_keepalive_enqueuer(enqueuer)
        .with_event_bus(bus);
    let mut request = http::Request::builder()
        .method(http::Method::POST)
        .uri("/v1/messages")
        .header("x-api-key", managed_api_key())
        .header("anthropic-version", "2023-06-01");
    for (name, value) in request_headers {
        request = request.header(*name, *value);
    }
    let request = request
        .body(request_body)
        .expect("observed keepalive request");
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("request handles");
    let mut reported_kind = None;
    let mut parsed = false;
    while let Ok(event) = events.try_recv() {
        if let cc_lb_lifecycle::LifecycleEvent::ParseCompleted {
            result: Ok(info), ..
        } = event
        {
            reported_kind = info.request_kind;
            parsed = true;
        }
    }
    assert!(parsed, "real lifecycle must report parsed request identity");
    (response, reported_kind)
}

async fn assert_keepalive_response_fidelity(
    response: Response<Body>,
    expected: &Bytes,
    streaming: bool,
) {
    let (status, headers, body) = collect_body(response).await;
    assert_eq!(status, StatusCode::OK);
    if streaming {
        assert!(
            headers[http::header::CONTENT_TYPE]
                .to_str()
                .expect("content type")
                .starts_with("text/event-stream")
        );
        assert_eq!(
            body.as_ref(),
            streaming_body_from_message(expected).as_ref()
        );
        assert!(body.ends_with(b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"));
    } else {
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).expect("downstream JSON"),
            serde_json::from_slice::<serde_json::Value>(expected).expect("fixture JSON")
        );
    }
}

async fn establish_scoped_keepalive(
    enqueuer: &Arc<ScopedKeepaliveEnqueuer>,
    principal: PrincipalRecord,
    headers: &[(&str, &str)],
) {
    let body = agent_turn_body("msg_legitimate_setup", "Bash", 12_000);
    let (response, _) = observed_keepalive_response(
        body.clone(),
        Arc::clone(enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>,
        principal,
        standard_keepalive_request(),
        headers,
        false,
    )
    .await;
    assert_keepalive_response_fidelity(response, &body, false).await;
    enqueuer.wait_for_completion().await;
    enqueuer.wait_for_enqueue().await;
}

#[tokio::test]
async fn auxiliary_unknown_kind_must_not_cancel_main_session_scope() {
    let enqueuer = Arc::new(ScopedKeepaliveEnqueuer::default());
    let principal = principal_with_keepalive();
    establish_scoped_keepalive(
        &enqueuer,
        principal.clone(),
        &[
            ("x-claude-code-session-id", "auxiliary-repro"),
            ("x-claude-code-request-class", "main"),
        ],
    )
    .await;
    let before = enqueuer.snapshot();
    assert_eq!(before.generations.len(), 1);
    let key = before.generations.keys().next().expect("main session key");
    assert!(key.ends_with("/thread:auxiliary-repro"));
    assert_eq!(
        before
            .operations
            .iter()
            .filter(|operation| matches!(
                operation, KeepaliveOperation::Enqueue(session, None) if session == key
            ))
            .count(),
        1
    );
    assert_eq!(
        before.operations.iter().filter(|operation| matches!(
            operation, KeepaliveOperation::Cancel(session, None, reason) if session == key && reason.as_str() == "new_request"
        )).count(),
        1
    );

    let body = end_turn_body("msg_auxiliary_repro");
    let (response, kind) = observed_keepalive_response(
        body.clone(),
        Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>,
        principal,
        standard_keepalive_request(),
        &[
            ("x-claude-code-session-id", "auxiliary-repro"),
            ("x-claude-code-request-class", "auxiliary"),
        ],
        false,
    )
    .await;
    assert_eq!(kind.as_deref(), Some("unknown"));
    assert_keepalive_response_fidelity(response, &body, false).await;
    settle().await;
    assert_eq!(
        enqueuer.snapshot(),
        before,
        "auxiliary must preserve the main generation and full operation journal"
    );
}

#[tokio::test]
async fn explicit_auxiliary_completions_do_not_mutate_keepalive_json_and_sse() {
    for streaming in [false, true] {
        for agent in [None, Some("auxiliary-agent")] {
            for tool_use in [false, true] {
                for active in [false, true] {
                    let enqueuer = Arc::new(ScopedKeepaliveEnqueuer::default());
                    let principal = principal_with_keepalive();
                    let mut headers = vec![("x-claude-code-session-id", "auxiliary-matrix")];
                    if let Some(agent) = agent {
                        headers.push(("x-claude-code-agent-id", agent));
                    }
                    if active {
                        establish_scoped_keepalive(&enqueuer, principal.clone(), &headers).await;
                    }
                    let before = enqueuer.snapshot();
                    headers.push(("x-claude-code-request-class", " AuXiLiArY "));
                    let body = if tool_use {
                        agent_turn_body("msg_auxiliary_tool", "Bash", 12_000)
                    } else {
                        end_turn_body("msg_auxiliary_end")
                    };
                    let (response, kind) = observed_keepalive_response(
                        body.clone(),
                        Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>,
                        principal,
                        standard_keepalive_request(),
                        &headers,
                        streaming,
                    )
                    .await;
                    assert_eq!(
                        kind.as_deref(),
                        Some(if agent.is_some() {
                            "subagent"
                        } else {
                            "unknown"
                        })
                    );
                    assert_keepalive_response_fidelity(response, &body, streaming).await;
                    settle().await;
                    assert_eq!(
                        enqueuer.snapshot(),
                        before,
                        "streaming={streaming}, agent={agent:?}, tool_use={tool_use}, active={active}"
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn explicit_auxiliary_preserves_all_generation_scopes() {
    let enqueuer = Arc::new(ScopedKeepaliveEnqueuer::default());
    let principal = principal_with_keepalive();
    for (session, agent) in [
        ("scope-session", None),
        ("scope-session", Some("own-agent")),
        ("scope-session", Some("other-agent")),
        ("other-session", None),
    ] {
        let mut headers = vec![("x-claude-code-session-id", session)];
        if let Some(agent) = agent {
            headers.push(("x-claude-code-agent-id", agent));
        }
        establish_scoped_keepalive(&enqueuer, principal.clone(), &headers).await;
    }
    let mut other_principal = principal_with_keepalive();
    other_principal.name = "other-principal".to_owned();
    establish_scoped_keepalive(
        &enqueuer,
        other_principal,
        &[("x-claude-code-session-id", "scope-session")],
    )
    .await;
    let before = enqueuer.snapshot();
    assert_eq!(before.generations.len(), 5);
    assert_eq!(
        before
            .generations
            .values()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        5
    );
    assert_eq!(
        before
            .generations
            .keys()
            .filter(|key| key.ends_with("/thread:scope-session"))
            .count(),
        2
    );
    assert_eq!(
        before
            .generations
            .keys()
            .filter(|key| key.ends_with("/thread:scope-session/agent:own-agent"))
            .count(),
        1
    );
    for agent in [None, Some("own-agent"), Some(" main ")] {
        for tool_use in [false, true] {
            let mut headers = vec![
                ("x-claude-code-session-id", "scope-session"),
                ("x-claude-code-request-class", "auxiliary"),
            ];
            if let Some(agent) = agent {
                headers.push(("x-claude-code-agent-id", agent));
            }
            let body = if tool_use {
                agent_turn_body("msg_scoped_tool", "Bash", 12_000)
            } else {
                end_turn_body("msg_scoped_end")
            };
            let (response, _) = observed_keepalive_response(
                body.clone(),
                Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>,
                principal.clone(),
                standard_keepalive_request(),
                &headers,
                false,
            )
            .await;
            assert_keepalive_response_fidelity(response, &body, false).await;
            settle().await;
            assert_eq!(
                enqueuer.snapshot(),
                before,
                "agent={agent:?}, tool_use={tool_use}"
            );
        }
    }
}

fn refined_keepalive_request(kind: &str) -> Bytes {
    let mut request: serde_json::Value =
        serde_json::from_slice(&standard_keepalive_request()).expect("request fixture");
    request["system"][0]["text"] = json!(match kind {
        "main" => "You are an interactive agent that helps users with software engineering tasks.",
        "advisor" =>
            "<system-conventions>rules</system-conventions>\nYou shadow the main agent as a peer programmer:",
        _ => panic!("unsupported refinement"),
    });
    Bytes::from(serde_json::to_vec(&request).expect("refined request fixture"))
}

#[tokio::test]
async fn explicit_auxiliary_exclusion_survives_main_and_advisor_refinement() {
    for kind in ["main", "advisor"] {
        for streaming in [false, true] {
            for tool_use in [false, true] {
                let enqueuer = Arc::new(ScopedKeepaliveEnqueuer::default());
                let principal = principal_with_keepalive();
                establish_scoped_keepalive(
                    &enqueuer,
                    principal.clone(),
                    &[("x-claude-code-session-id", "refined-auxiliary")],
                )
                .await;
                let before = enqueuer.snapshot();
                let body = if tool_use {
                    agent_turn_body("msg_refined_tool", "Bash", 12_000)
                } else {
                    end_turn_body("msg_refined_end")
                };
                let (response, reported_kind) = observed_keepalive_response(
                    body.clone(),
                    Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>,
                    principal,
                    refined_keepalive_request(kind),
                    &[
                        ("x-claude-code-session-id", "refined-auxiliary"),
                        ("x-claude-code-request-class", "auxiliary"),
                    ],
                    streaming,
                )
                .await;
                assert_eq!(
                    reported_kind.as_deref(),
                    Some(kind),
                    "refined reporting must be preserved"
                );
                assert_keepalive_response_fidelity(response, &body, streaming).await;
                settle().await;
                assert_eq!(
                    enqueuer.snapshot(),
                    before,
                    "kind={kind}, streaming={streaming}, tool_use={tool_use}"
                );
            }
        }
    }
}

#[tokio::test]
async fn non_auxiliary_turns_preserve_keepalive_lifecycle() {
    for (class, agent, refinement, expected_kind, expected_scope_suffix) in [
        (
            None,
            None,
            None,
            Some("unknown"),
            Some("/thread:legitimate-control"),
        ),
        (None, None, None, None, None),
        (
            Some("unrecognized"),
            None,
            None,
            Some("unknown"),
            Some("/thread:legitimate-control"),
        ),
        (
            None,
            Some("legitimate-agent"),
            None,
            Some("subagent"),
            Some("/thread:legitimate-control/agent:legitimate-agent"),
        ),
        (
            Some("main"),
            Some("main"),
            None,
            Some("main"),
            Some("/thread:legitimate-control"),
        ),
        (
            Some("subagent"),
            None,
            None,
            Some("subagent"),
            Some("/thread:legitimate-control/agent:subagent"),
        ),
        (
            None,
            None,
            Some("advisor"),
            Some("advisor"),
            Some("/thread:legitimate-control"),
        ),
        (
            Some(""),
            None,
            None,
            Some("unknown"),
            Some("/thread:legitimate-control"),
        ),
    ] {
        for streaming in [false, true] {
            let enqueuer = Arc::new(ScopedKeepaliveEnqueuer::default());
            let principal = principal_with_keepalive();
            let mut headers = Vec::new();
            if expected_kind.is_some() {
                headers.push(("x-claude-code-session-id", "legitimate-control"));
            }
            if let Some(class) = class {
                headers.push(("x-claude-code-request-class", class));
            }
            if let Some(agent) = agent {
                headers.push(("x-claude-code-agent-id", agent));
            }
            let request = refinement
                .map(refined_keepalive_request)
                .unwrap_or_else(standard_keepalive_request);
            let body = agent_turn_body("msg_control_agent", "Bash", 12_000);
            let (response, kind) = observed_keepalive_response(
                body.clone(),
                Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>,
                principal.clone(),
                request.clone(),
                &headers,
                streaming,
            )
            .await;
            assert_eq!(kind.as_deref(), expected_kind);
            assert_keepalive_response_fidelity(response, &body, streaming).await;
            enqueuer.wait_for_completion().await;
            let active = enqueuer.snapshot();
            assert_eq!(active.generations.len(), 1);
            let (key, generation) = active
                .generations
                .iter()
                .next()
                .expect("active control generation");
            if let Some(expected_scope_suffix) = expected_scope_suffix {
                assert!(
                    key.ends_with(expected_scope_suffix),
                    "expected scope {expected_scope_suffix}, got {key}"
                );
            } else {
                let (_, prefix_hash) = key
                    .split_once("/prefix:")
                    .expect("headerless control must use a cache-prefix scope");
                assert!(!prefix_hash.is_empty());
                assert!(
                    !key.contains("/agent:"),
                    "headerless control must not invent an agent scope"
                );
            }
            assert_eq!(
                active
                    .operations
                    .iter()
                    .filter(|operation| matches!(
                        operation, KeepaliveOperation::Enqueue(session, None) if session == key
                    ))
                    .count(),
                1
            );
            assert_eq!(
                active.operations.iter().filter(|operation| matches!(
                    operation, KeepaliveOperation::Cancel(session, None, reason) if session == key && reason.as_str() == "new_request"
                )).count(),
                1
            );
            let body = end_turn_body("msg_control_user");
            let (response, kind) = observed_keepalive_response(
                body.clone(),
                Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>,
                principal,
                request,
                &headers,
                streaming,
            )
            .await;
            assert_eq!(kind.as_deref(), expected_kind);
            assert_keepalive_response_fidelity(response, &body, streaming).await;
            enqueuer.wait_for_completion().await;
            let completed = enqueuer.snapshot();
            assert!(completed.generations.is_empty());
            assert_eq!(
                completed.operations.iter().filter(|operation| matches!(
                    operation, KeepaliveOperation::Cancel(session, Some(previous), reason)
                        if session == key && previous == generation && reason.as_str() == "new_request"
                )).count(),
                1
            );
            assert_eq!(
                completed
                    .operations
                    .iter()
                    .filter(|operation| matches!(
                        operation, KeepaliveOperation::NotTracked(session, reason)
                            if session == key && reason == "user turn (stop_reason=end_turn)"
                    ))
                    .count(),
                1
            );
            assert_eq!(
                completed.operations.iter().filter(|operation| matches!(
                    operation, KeepaliveOperation::Cancel(session, Some(owner), CancelReason::UserTurnDetected)
                        if session == key && owner == generation
                )).count(),
                1
            );
            assert_eq!(
                completed
                    .operations
                    .iter()
                    .filter(|operation| matches!(operation, KeepaliveOperation::Enqueue(_, _)))
                    .count(),
                1,
                "a user turn must not add a replacement generation"
            );
        }
    }
}

#[tokio::test]
async fn real_followup_fences_queued_generation_before_pending_ttfb_and_sse() {
    for (streaming, session_scoped) in [(false, true), (true, true), (false, false), (true, false)]
    {
        let enqueuer = Arc::new(ScopedKeepaliveEnqueuer::default());
        let principal = principal_with_keepalive();
        let headers = if session_scoped {
            vec![("x-claude-code-session-id", "active-followup")]
        } else {
            Vec::new()
        };
        establish_scoped_keepalive(&enqueuer, principal.clone(), &headers).await;
        establish_scoped_keepalive(
            &enqueuer,
            principal.clone(),
            &[
                ("x-claude-code-session-id", "active-followup"),
                ("x-claude-code-agent-id", "unrelated-agent"),
            ],
        )
        .await;
        establish_scoped_keepalive(
            &enqueuer,
            principal.clone(),
            &[("x-claude-code-session-id", "unrelated-followup")],
        )
        .await;
        let before = enqueuer.snapshot();
        let (key, old_generation) = before
            .generations
            .iter()
            .find(|(key, _)| {
                if session_scoped {
                    key.ends_with("/thread:active-followup")
                } else {
                    key.contains("/prefix:")
                }
            })
            .expect("setup creates the real session generation");
        let dispatch = Arc::new(PendingFollowupDispatch {
            streaming,
            body: agent_turn_body("msg_active_followup", "Bash", 12_000),
            entered: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
            calls: AtomicUsize::new(0),
        });
        let authn = TestAuthn::with_principal_view(
            TestState::default(),
            Arc::new(PrincipalView::from_db(&[principal], HashMap::new())),
        );
        let lifecycle =
            lifecycle_with_parts(authn, dispatch.clone(), LifecycleConfig::default())
                .with_cache_keepalive_enqueuer(
                    Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>
                );
        let request = followup_request(
            if session_scoped {
                "active-followup"
            } else {
                ""
            },
            false,
            true,
        );
        let auth = lifecycle
            .authenticate(request.headers())
            .await
            .expect("real followup authenticates");
        let completion = async {
            let response = lifecycle
                .handle(request, &auth)
                .await
                .expect("followup handles");
            assert_keepalive_response_fidelity(response, &dispatch.body, streaming).await;
        };
        tokio::pin!(completion);
        tokio::select! {
            () = dispatch.entered.notified() => {}
            () = &mut completion => panic!("upstream must remain pending until released"),
        }
        assert_eq!(dispatch.calls.load(Ordering::SeqCst), 1);
        let pending = enqueuer.snapshot();
        assert_ne!(
            pending.generations.get(key),
            Some(old_generation),
            "the old queued generation must be fenced before upstream completion, streaming={streaming}"
        );
        for (other_key, generation) in &before.generations {
            if other_key != key {
                assert_eq!(
                    pending.generations.get(other_key),
                    Some(generation),
                    "an unrelated thread must remain renewable"
                );
            }
        }
        dispatch.release.notify_one();
        completion.await;
        enqueuer.wait_for_enqueue().await;
        let completed = enqueuer.snapshot();
        assert!(
            completed
                .generations
                .get(key)
                .is_some_and(|generation| generation > old_generation),
            "successful agent completion must establish a fresh generation"
        );
        assert_eq!(
            completed.operations.iter().filter(
                |operation| matches!(operation, KeepaliveOperation::Enqueue(session, _) if session == key)
            ).count(),
            2,
            "one setup enqueue and exactly one completion enqueue"
        );
        for (other_key, generation) in &before.generations {
            if other_key != key {
                assert_eq!(completed.generations.get(other_key), Some(generation));
            }
        }
    }
}

#[tokio::test]
async fn real_dispatch_waits_for_durable_request_cancellation() {
    struct BlockingRequestFence {
        inner: Arc<ScopedKeepaliveEnqueuer>,
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    #[async_trait]
    impl CacheKeepaliveEnqueuer for BlockingRequestFence {
        async fn observe_session_generation(
            &self,
            key: &str,
        ) -> Result<Option<u64>, CacheKeepaliveEnqueueError> {
            self.inner.observe_session_generation(key).await
        }

        async fn enqueue_cache_keepalive(
            &self,
            request: CacheKeepaliveEnqueueRequest,
        ) -> Result<(), CacheKeepaliveEnqueueError> {
            self.inner.enqueue_cache_keepalive(request).await
        }

        async fn cancel_cache_keepalive(
            &self,
            request: CacheKeepaliveCancelRequest,
        ) -> Result<(), CacheKeepaliveEnqueueError> {
            if request.reason.as_str() == "new_request" {
                self.entered.notify_one();
                self.release.notified().await;
            }
            self.inner.cancel_cache_keepalive(request).await
        }

        async fn record_cache_keepalive_not_tracked(
            &self,
            request: CacheKeepaliveNotTrackedRequest,
        ) -> Result<(), CacheKeepaliveEnqueueError> {
            self.inner.record_cache_keepalive_not_tracked(request).await
        }
    }

    let inner = Arc::new(ScopedKeepaliveEnqueuer::default());
    let principal = principal_with_keepalive();
    establish_scoped_keepalive(
        &inner,
        principal.clone(),
        &[("x-claude-code-session-id", "blocked-fence")],
    )
    .await;
    let before = inner.snapshot();
    let fence = Arc::new(BlockingRequestFence {
        inner: Arc::clone(&inner),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let dispatch = Arc::new(PendingFollowupDispatch {
        streaming: false,
        body: agent_turn_body("msg_after_durable_fence", "Bash", 12_000),
        entered: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
        calls: AtomicUsize::new(0),
    });
    let lifecycle = lifecycle_with_parts(
        TestAuthn::with_principal_view(
            TestState::default(),
            Arc::new(PrincipalView::from_db(&[principal], HashMap::new())),
        ),
        dispatch.clone(),
        LifecycleConfig::default(),
    )
    .with_cache_keepalive_enqueuer(Arc::clone(&fence) as Arc<dyn CacheKeepaliveEnqueuer>);
    let request = followup_request("blocked-fence", false, true);
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("followup authenticates");
    let completion = async {
        let response = lifecycle
            .handle(request, &auth)
            .await
            .expect("followup handles");
        assert_keepalive_response_fidelity(response, &dispatch.body, false).await;
    };
    tokio::pin!(completion);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::select! {
            () = fence.entered.notified() => {}
            () = &mut completion => panic!("response must await the durable fence"),
        }
        assert_eq!(dispatch.calls.load(Ordering::SeqCst), 0);
        assert_eq!(inner.snapshot().generations, before.generations);
        fence.release.notify_one();
        tokio::select! {
            () = dispatch.entered.notified() => {}
            () = &mut completion => panic!("upstream remains pending until released"),
        }
        assert_eq!(dispatch.calls.load(Ordering::SeqCst), 1);
        assert!(inner.snapshot().generations.is_empty());
        dispatch.release.notify_one();
        completion.await;
        inner.wait_for_enqueue().await;
    })
    .await
    .expect("durable fence and dispatch finish");
    let after = inner.snapshot();
    assert_eq!(after.generations.len(), 1);
    let (key, old_generation) = before.generations.iter().next().expect("old generation");
    assert!(
        after
            .generations
            .get(key)
            .is_some_and(|generation| generation > old_generation)
    );
}

#[tokio::test]
async fn auxiliary_and_unauthenticated_followups_preserve_queued_generations() {
    let enqueuer = Arc::new(ScopedKeepaliveEnqueuer::default());
    let principal = principal_with_keepalive();
    establish_scoped_keepalive(
        &enqueuer,
        principal.clone(),
        &[("x-claude-code-session-id", "preserved-followup")],
    )
    .await;
    let before = enqueuer.snapshot();
    let dispatch = Arc::new(PendingFollowupDispatch {
        streaming: false,
        body: agent_turn_body("msg_auxiliary_followup", "Bash", 12_000),
        entered: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
        calls: AtomicUsize::new(0),
    });
    let authn = TestAuthn::with_principal_view(
        TestState::default(),
        Arc::new(PrincipalView::from_db(&[principal], HashMap::new())),
    );
    let lifecycle = lifecycle_with_parts(authn, dispatch.clone(), LifecycleConfig::default())
        .with_cache_keepalive_enqueuer(Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>);
    let unauthenticated = followup_request("preserved-followup", false, false);
    assert!(
        lifecycle
            .authenticate(unauthenticated.headers())
            .await
            .is_err()
    );
    assert_eq!(dispatch.calls.load(Ordering::SeqCst), 0);
    assert_eq!(enqueuer.snapshot(), before);

    let request = followup_request("preserved-followup", true, true);
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("auxiliary authenticates");
    let completion = async {
        let response = lifecycle
            .handle(request, &auth)
            .await
            .expect("auxiliary handles");
        assert_keepalive_response_fidelity(response, &dispatch.body, false).await;
    };
    tokio::pin!(completion);
    tokio::select! {
        () = dispatch.entered.notified() => {}
        () = &mut completion => panic!("auxiliary upstream must remain pending"),
    }
    assert_eq!(dispatch.calls.load(Ordering::SeqCst), 1);
    assert_eq!(enqueuer.snapshot(), before);
    dispatch.release.notify_one();
    completion.await;
    assert_eq!(enqueuer.snapshot(), before);
}

fn followup_request(session: &str, auxiliary: bool, authenticated: bool) -> http::Request<Bytes> {
    let mut request = http::Request::builder()
        .method(http::Method::POST)
        .uri("/v1/messages")
        .header("anthropic-version", "2023-06-01")
        .header(
            "x-claude-code-request-class",
            if auxiliary { "auxiliary" } else { "main" },
        );
    if !session.is_empty() {
        request = request.header("x-claude-code-session-id", session);
    }
    if authenticated {
        request = request.header("x-api-key", managed_api_key());
    }
    request
        .body(standard_keepalive_request())
        .expect("followup request")
}

struct PendingFollowupDispatch {
    streaming: bool,
    body: Bytes,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    calls: AtomicUsize,
}

#[async_trait]
impl UpstreamDispatch for PendingFollowupDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if !self.streaming {
            self.entered.notify_one();
            self.release.notified().await;
            let mut response = Response::new(Body::from(self.body.clone()));
            response.headers_mut().insert(
                http::header::CONTENT_TYPE,
                http::HeaderValue::from_static("application/json"),
            );
            return Ok(response);
        }
        let entered = Arc::clone(&self.entered);
        let release = Arc::clone(&self.release);
        let body = streaming_body_from_message(&self.body);
        let split = body
            .windows(2)
            .position(|bytes| bytes == b"\n\n")
            .expect("message_start boundary")
            + 2;
        let start = body.slice(..split);
        let rest = body.slice(split..);
        let stream = async_stream::stream! {
            yield Ok::<Bytes, std::convert::Infallible>(start);
            entered.notify_one();
            release.notified().await;
            yield Ok::<Bytes, std::convert::Infallible>(rest);
        };
        let mut response = Response::new(Body::from_stream(stream));
        response.headers_mut().insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("text/event-stream"),
        );
        Ok(response)
    }
}

struct BlockingCompletionEnqueuer {
    inner: ScopedKeepaliveEnqueuer,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[async_trait]
impl CacheKeepaliveEnqueuer for BlockingCompletionEnqueuer {
    async fn observe_session_generation(
        &self,
        key: &str,
    ) -> Result<Option<u64>, CacheKeepaliveEnqueueError> {
        self.inner.observe_session_generation(key).await
    }

    async fn enqueue_cache_keepalive(
        &self,
        request: CacheKeepaliveEnqueueRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        self.entered.notify_one();
        self.release.notified().await;
        self.inner.enqueue_cache_keepalive(request).await
    }

    async fn cancel_cache_keepalive(
        &self,
        request: CacheKeepaliveCancelRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        self.inner.cancel_cache_keepalive(request).await
    }

    async fn record_cache_keepalive_not_tracked(
        &self,
        request: CacheKeepaliveNotTrackedRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        self.inner.record_cache_keepalive_not_tracked(request).await
    }
}

#[tokio::test]
async fn cache_keepalive_response_completion_does_not_await_generation_persistence() {
    for streaming in [false, true] {
        let enqueuer = Arc::new(BlockingCompletionEnqueuer {
            inner: ScopedKeepaliveEnqueuer::default(),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let body = agent_turn_body("msg_detached_persistence", "Bash", 12_000);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::join!(
                async {
                    let (response, kind) = observed_keepalive_response(
                        body.clone(),
                        Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>,
                        principal_with_keepalive(),
                        standard_keepalive_request(),
                        &[
                            ("x-claude-code-session-id", "detached-persistence"),
                            ("x-claude-code-request-class", "main"),
                        ],
                        streaming,
                    )
                    .await;
                    assert_eq!(kind.as_deref(), Some("main"));
                    assert_keepalive_response_fidelity(response, &body, streaming).await;
                },
                enqueuer.entered.notified(),
            );
        })
        .await
        .expect("the complete downstream response must arrive while persistence is blocked");
        enqueuer.release.notify_one();
        enqueuer.inner.wait_for_enqueue().await;
        assert_eq!(enqueuer.inner.snapshot().generations.len(), 1);
    }
}

// The store issues durable ordering tickets, not the current active row's
// generation. A request's completion must return its dispatch ticket unchanged.
#[derive(Default)]
struct RequestOwnerEnqueuer {
    state: Mutex<RequestOwnerState>,
    completed: tokio::sync::Notify,
}

#[derive(Default)]
struct RequestOwnerState {
    next_ticket: u64,
    owner: Option<u64>,
    updated_at: u64,
    now: u64,
    completions: Vec<Option<u64>>,
    observations: usize,
}

#[async_trait]
impl CacheKeepaliveEnqueuer for RequestOwnerEnqueuer {
    async fn observe_session_generation(
        &self,
        _key: &str,
    ) -> Result<Option<u64>, CacheKeepaliveEnqueueError> {
        let mut state = self.state.lock().expect("request owner");
        state.next_ticket += 1;
        state.observations += 1;
        Ok(Some(state.next_ticket))
    }

    async fn cancel_cache_keepalive(
        &self,
        request: CacheKeepaliveCancelRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        let mut state = self.state.lock().expect("request owner");
        if request.reason == CancelReason::NewRequest {
            state.owner = request.expected_generation;
            state.updated_at = state.now;
        } else {
            state.completions.push(request.expected_generation);
            // Model the pre-fix monotonic-ticket acceptance rule, so an engine
            // that invents a fresh completion owner visibly mutates B's marker.
            if request.expected_generation >= state.owner {
                state.owner = None;
                state.updated_at = state.now;
            }
            self.completed.notify_one();
        }
        Ok(())
    }

    async fn enqueue_cache_keepalive(
        &self,
        request: CacheKeepaliveEnqueueRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        let mut state = self.state.lock().expect("request owner");
        state.completions.push(request.expected_generation);
        if request.expected_generation >= state.owner {
            state.owner = request.expected_generation;
            state.updated_at = state.now;
        }
        self.completed.notify_one();
        Ok(())
    }

    async fn record_cache_keepalive_not_tracked(
        &self,
        _request: CacheKeepaliveNotTrackedRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        Ok(())
    }
}

struct OwnedRequestDispatch {
    calls: AtomicUsize,
    entered: [tokio::sync::Notify; 2],
    release: [tokio::sync::Notify; 2],
    old_agent_turn: bool,
    streaming: bool,
}

#[async_trait]
impl UpstreamDispatch for OwnedRequestDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let index = self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered[index].notify_one();
        self.release[index].notified().await;
        let body = if index == 0 && !self.old_agent_turn {
            end_turn_body("msg_old_owner")
        } else {
            agent_turn_body(
                if index == 0 {
                    "msg_old_owner"
                } else {
                    "msg_current_owner"
                },
                "Bash",
                12_000,
            )
        };
        let mut response = Response::new(Body::from(if self.streaming {
            streaming_body_from_message(&body)
        } else {
            body
        }));
        response.headers_mut().insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static(if self.streaming {
                "text/event-stream"
            } else {
                "application/json"
            }),
        );
        Ok(response)
    }
}

#[tokio::test]
async fn request_start_owner_survives_late_first_byte_and_same_second_starts() {
    let mut violations = Vec::new();
    for (second_start, old_agent_turn, streaming) in [
        (105, false, false),
        (100, false, false),
        (105, true, false),
        (100, true, false),
        (105, false, true),
        (100, true, true),
    ] {
        let enqueuer = Arc::new(RequestOwnerEnqueuer::default());
        let dispatch = Arc::new(OwnedRequestDispatch {
            calls: AtomicUsize::new(0),
            entered: std::array::from_fn(|_| tokio::sync::Notify::new()),
            release: std::array::from_fn(|_| tokio::sync::Notify::new()),
            old_agent_turn,
            streaming,
        });
        let lifecycle = lifecycle_with_parts(
            TestAuthn::with_principal_view(
                TestState::default(),
                Arc::new(PrincipalView::from_db(
                    &[principal_with_keepalive()],
                    HashMap::new(),
                )),
            ),
            dispatch.clone(),
            LifecycleConfig::default(),
        )
        .with_cache_keepalive_enqueuer(enqueuer.clone());
        let request_a = followup_request("request-owner", false, true);
        let request_b = followup_request("request-owner", false, true);
        let auth_a = lifecycle
            .authenticate(request_a.headers())
            .await
            .expect("A authenticates");
        let auth_b = lifecycle
            .authenticate(request_b.headers())
            .await
            .expect("B authenticates");
        let completion_a = async {
            collect_body(
                lifecycle
                    .handle(request_a, &auth_a)
                    .await
                    .expect("A response"),
            )
            .await
        };
        let completion_b = async {
            collect_body(
                lifecycle
                    .handle(request_b, &auth_b)
                    .await
                    .expect("B response"),
            )
            .await
        };
        tokio::pin!(completion_a, completion_b);
        enqueuer.state.lock().expect("request owner").now = 100;
        tokio::select! {
            () = dispatch.entered[0].notified() => {}
            _ = &mut completion_a => panic!("A must wait for its first byte"),
        }
        enqueuer.state.lock().expect("request owner").now = second_start;
        tokio::select! {
            () = dispatch.entered[1].notified() => {}
            _ = &mut completion_b => panic!("B must wait for its first byte"),
        }
        let owner_b = enqueuer.state.lock().expect("request owner").owner;
        enqueuer.state.lock().expect("request owner").now = 110;
        dispatch.release[0].notify_one();
        assert_eq!(completion_a.await.0, StatusCode::OK);
        // Let detached completion persistence run before dispatching B's
        // healthy completion; no wall-clock delay or same-second race.
        tokio::task::yield_now().await;
        let stale_persisted = {
            let state = enqueuer.state.lock().expect("request owner");
            if (state.owner, state.updated_at, state.observations) != (owner_b, second_start, 2)
                || !state.completions.is_empty()
            {
                violations.push(format!(
                    "A100/B{second_start}/A-first-byte110: owner={:?}, marker={}, observations={}, completion tickets={:?}; expected owner={owner_b:?}, marker={second_start}, observations=2, no stale persistence; agent={old_agent_turn}, sse={streaming}",
                    state.owner, state.updated_at, state.observations, state.completions
                ));
            }
            !state.completions.is_empty()
        };
        if stale_persisted {
            enqueuer.completed.notified().await;
        }
        dispatch.release[1].notify_one();
        assert_eq!(completion_b.await.0, StatusCode::OK);
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            enqueuer.completed.notified(),
        )
        .await
        .expect("latest healthy completion persists");
        let state = enqueuer.state.lock().expect("request owner");
        if state.completions != vec![owner_b] || state.owner != owner_b || state.observations != 2 {
            violations.push(format!(
                "latest B completion must reuse {owner_b:?}; completions={:?}, owner={:?}, observations={}",
                state.completions, state.owner, state.observations
            ));
        }
    }
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

#[derive(Default)]
struct RecordingEnqueuer {
    enqueued: Mutex<Vec<CacheKeepaliveEnqueueRequest>>,
    cancelled: Mutex<Vec<CacheKeepaliveCancelRequest>>,
    not_tracked: Mutex<Vec<CacheKeepaliveNotTrackedRequest>>,
    completed: tokio::sync::Notify,
}

impl RecordingEnqueuer {
    async fn wait_for_completion(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(5), self.completed.notified())
            .await
            .expect("keepalive completion");
    }

    fn enqueued(&self) -> Vec<CacheKeepaliveEnqueueRequest> {
        self.enqueued.lock().expect("enqueued lock").clone()
    }

    fn cancelled(&self) -> Vec<CacheKeepaliveCancelRequest> {
        self.cancelled.lock().expect("cancelled lock").clone()
    }

    fn completion_cancellations(&self) -> Vec<CacheKeepaliveCancelRequest> {
        self.cancelled()
            .into_iter()
            .filter(|request| request.reason.as_str() != "new_request")
            .collect()
    }

    fn not_tracked(&self) -> Vec<CacheKeepaliveNotTrackedRequest> {
        self.not_tracked.lock().expect("not-tracked lock").clone()
    }
}

#[async_trait]
impl CacheKeepaliveEnqueuer for RecordingEnqueuer {
    async fn enqueue_cache_keepalive(
        &self,
        request: CacheKeepaliveEnqueueRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        self.enqueued.lock().expect("enqueued lock").push(request);
        self.completed.notify_one();
        Ok(())
    }

    async fn cancel_cache_keepalive(
        &self,
        request: CacheKeepaliveCancelRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        let is_completion = request.reason.as_str() != "new_request";
        self.cancelled.lock().expect("cancelled lock").push(request);
        if is_completion {
            self.completed.notify_one();
        }
        Ok(())
    }

    async fn record_cache_keepalive_not_tracked(
        &self,
        request: CacheKeepaliveNotTrackedRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        self.not_tracked
            .lock()
            .expect("not-tracked lock")
            .push(request);
        Ok(())
    }
}

struct FixedStreamingDispatch {
    body: Bytes,
}

struct FixedSuccessDispatch {
    body: Bytes,
}

struct RecordingCancelEnqueuer {
    calls: Arc<AtomicUsize>,
}

struct KeyRecordingEnqueuer {
    keys: Arc<std::sync::Mutex<Vec<String>>>,
}

struct FailingCancelEnqueuer {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl CacheKeepaliveEnqueuer for RecordingCancelEnqueuer {
    async fn enqueue_cache_keepalive(
        &self,
        _request: CacheKeepaliveEnqueueRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        Ok(())
    }

    async fn cancel_cache_keepalive(
        &self,
        request: CacheKeepaliveCancelRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        assert!(request.session_key_hash.contains("thread:session-1"));
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn record_cache_keepalive_not_tracked(
        &self,
        _request: CacheKeepaliveNotTrackedRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        Ok(())
    }
}

#[async_trait]
impl CacheKeepaliveEnqueuer for KeyRecordingEnqueuer {
    async fn enqueue_cache_keepalive(
        &self,
        _request: CacheKeepaliveEnqueueRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        Ok(())
    }

    async fn cancel_cache_keepalive(
        &self,
        request: CacheKeepaliveCancelRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        self.keys
            .lock()
            .expect("recorded cancellation keys lock")
            .push(request.session_key_hash);
        Ok(())
    }

    async fn record_cache_keepalive_not_tracked(
        &self,
        _request: CacheKeepaliveNotTrackedRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        Ok(())
    }
}

#[async_trait]
impl CacheKeepaliveEnqueuer for FailingCancelEnqueuer {
    async fn enqueue_cache_keepalive(
        &self,
        _request: CacheKeepaliveEnqueueRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        Ok(())
    }

    async fn cancel_cache_keepalive(
        &self,
        _request: CacheKeepaliveCancelRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Err(CacheKeepaliveEnqueueError(
            "forced cancellation failure".to_owned(),
        ))
    }

    async fn record_cache_keepalive_not_tracked(
        &self,
        _request: CacheKeepaliveNotTrackedRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        Ok(())
    }
}

#[async_trait]
impl UpstreamDispatch for FixedSuccessDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let mut response = Response::new(Body::from(self.body.clone()));
        *response.status_mut() = StatusCode::OK;
        response.headers_mut().insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/json"),
        );
        Ok(response)
    }
}
#[async_trait]
impl UpstreamDispatch for FixedStreamingDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let mut response = Response::new(Body::from(self.body.clone()));
        *response.status_mut() = StatusCode::OK;
        response.headers_mut().insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("text/event-stream"),
        );
        Ok(response)
    }
}

fn principal_with_keepalive() -> PrincipalRecord {
    PrincipalRecord {
        id: uuid::Uuid::new_v4(),
        name: "principal-test".to_owned(),
        kind: PrincipalKind::Machine,
        allowed_models: vec!["*".to_owned()],
        allowed_upstreams: Vec::new(),
        default_limits: Vec::<Limit>::new(),
        enabled: true,
        deleted_at_unix_secs: None,
        revision: 1,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        router_terminal_strategy: Default::default(),
        cache_keepalive: Some(CacheKeepaliveConfig {
            enabled: true,
            refresh_lead_time_5m_secs: 30,
            refresh_lead_time_1h_secs: 300,
            max_refreshes_per_session: 12,
            max_total_duration_secs: 14_400,
            snapshot_max_bytes: 524_288,
            min_cache_tokens: 10_000,
            classifier: ClassifierConfig::default(),
        }),
    }
}

async fn handle_keepalive_response(
    upstream_body: Bytes,
    enqueuer: Arc<dyn CacheKeepaliveEnqueuer>,
) -> Response<Body> {
    handle_keepalive_response_with(
        upstream_body,
        enqueuer,
        principal_with_keepalive(),
        standard_keepalive_request(),
        &[],
        false,
    )
    .await
}

async fn handle_keepalive_response_with(
    upstream_body: Bytes,
    enqueuer: Arc<dyn CacheKeepaliveEnqueuer>,
    principal: PrincipalRecord,
    request_body: Bytes,
    request_headers: &[(&str, &str)],
    streaming: bool,
) -> Response<Body> {
    let state = TestState::default();
    let principal_view = Arc::new(PrincipalView::from_db(
        &[principal],
        std::collections::HashMap::new(),
    ));
    let authn = TestAuthn::with_principal_view(state, principal_view);
    let upstream_body = if streaming {
        streaming_body_from_message(&upstream_body)
    } else {
        upstream_body
    };
    let dispatch: Arc<dyn UpstreamDispatch> = if streaming {
        Arc::new(FixedStreamingDispatch {
            body: upstream_body,
        })
    } else {
        Arc::new(FixedSuccessDispatch {
            body: upstream_body,
        })
    };
    let lifecycle = lifecycle_with_parts(authn, dispatch, LifecycleConfig::default())
        .with_cache_keepalive_enqueuer(enqueuer);

    let mut request = http::Request::builder()
        .method(http::Method::POST)
        .uri("/v1/messages")
        .header("x-api-key", managed_api_key())
        .header("x-session-id", "session-1")
        .header("anthropic-version", "2023-06-01");
    for (name, value) in request_headers {
        request = request.header(*name, *value);
    }
    let request = request.body(request_body).expect("test request builds");
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles keepalive response")
}

fn standard_keepalive_request() -> Bytes {
    Bytes::from_static(
        br#"{"model":"claude-test","max_tokens":32,"system":[{"type":"text","text":"cached","cache_control":{"type":"ephemeral","ttl":"5m"}}],"messages":[{"role":"user","content":"hello"}]}"#,
    )
}

fn no_breakpoint_request() -> Bytes {
    Bytes::from_static(
        br#"{"model":"claude-test","max_tokens":32,"messages":[{"role":"user","content":"hello"}]}"#,
    )
}

fn oversized_cached_prefix_request(session: &str) -> Bytes {
    Bytes::from(
        serde_json::to_vec(&json!({
            "model": "claude-test",
            "max_tokens": 32,
            "system": [{
                "type": "text",
                "text": "x".repeat(600_000),
                "cache_control": {"type": "ephemeral", "ttl": "5m"}
            }],
            "messages": [{"role": "user", "content": session}]
        }))
        .expect("oversized request JSON serializes"),
    )
}

fn agent_turn_body(id: &str, tool_name: &str, cached_tokens: u64) -> Bytes {
    tool_use_body(id, tool_name, cached_tokens)
}

fn tool_use_body(id: &str, tool_name: &str, cached_tokens: u64) -> Bytes {
    Bytes::from(
        serde_json::to_vec(&json!({
            "type": "message",
            "id": id,
            "content": [{
                "type": "tool_use",
                "id": format!("{id}-tool"),
                "name": tool_name,
                "input": {}
            }],
            "stop_reason": "tool_use",
            "usage": {
                "input_tokens": 10,
                "cache_read_input_tokens": cached_tokens,
                "output_tokens": 2
            }
        }))
        .expect("tool-use response JSON serializes"),
    )
}

fn end_turn_body(id: &str) -> Bytes {
    Bytes::from(
        serde_json::to_vec(&json!({
            "type": "message",
            "id": id,
            "content": [{"type": "text", "text": "done"}],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 10, "output_tokens": 2}
        }))
        .expect("end-turn response JSON serializes"),
    )
}

fn streaming_body_from_message(body: &Bytes) -> Bytes {
    let value: serde_json::Value =
        serde_json::from_slice(body).expect("streaming fixture message JSON parses");
    let message_start = json!({
        "type": "message_start",
        "message": {
            "id": value.get("id").cloned().unwrap_or_else(|| json!("msg")),
            "type": "message",
            "role": "assistant",
            "content": [],
            "usage": value.get("usage").cloned().unwrap_or_else(|| json!({}))
        }
    });
    let mut sse = format!("event: message_start\ndata: {message_start}\n\n");
    if let Some(content) = value.get("content").and_then(|content| content.as_array()) {
        for (index, block) in content.iter().enumerate() {
            let event = json!({
                "type": "content_block_start",
                "index": index,
                "content_block": block
            });
            sse.push_str(&format!("event: content_block_start\ndata: {event}\n\n"));
        }
    }
    let delta = json!({
        "type": "message_delta",
        "delta": {
            "stop_reason": value.get("stop_reason").cloned().unwrap_or_else(|| json!("end_turn"))
        }
    });
    sse.push_str(&format!("event: message_delta\ndata: {delta}\n\n"));
    sse.push_str("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n");
    Bytes::from(sse)
}
