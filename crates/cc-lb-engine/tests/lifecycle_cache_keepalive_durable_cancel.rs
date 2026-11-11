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
use cc_lb_storage_api::{CacheKeepaliveCompletionTicket, CacheKeepaliveConfig, ClassifierConfig};
use cc_lb_upstream::SignedRequest;
use http::{Response, StatusCode};
use serde_json::json;

use common::{TestAuthn, TestState, collect_body, lifecycle_with_parts, managed_api_key, settle};

fn completion_ticket(sequence: u64) -> CacheKeepaliveCompletionTicket {
    CacheKeepaliveCompletionTicket {
        epoch: uuid::Uuid::from_u128(1),
        sequence,
        valid_until_unix_secs: u64::MAX,
    }
}

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
    assert_no_breakpoint_compaction(false, &[false, true]).await;
}

#[tokio::test]
async fn shape_fallback_without_breakpoint_buffered_compaction_fences_generation() {
    assert_no_breakpoint_compaction(true, &[false]).await;
}

#[tokio::test]
async fn shape_fallback_without_breakpoint_sse_compaction_fences_generation() {
    assert_no_breakpoint_compaction(true, &[true]).await;
}

async fn assert_no_breakpoint_compaction(shape_fallback: bool, streaming_modes: &[bool]) {
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
        for &streaming in streaming_modes {
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
            let (response, _) = observed_keepalive_response_with_shape(
                response_body,
                Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>,
                principal,
                no_breakpoint_request(),
                &main_headers,
                streaming,
                shape_fallback,
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
        ) -> Result<Option<CacheKeepaliveCompletionTicket>, CacheKeepaliveEnqueueError> {
            Ok(Some(completion_ticket(42)))
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
                .all(|request| request.expected_generation == Some(completion_ticket(42)))
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
    assert_eq!(enqueued[0].expected_generation, Some(completion_ticket(42)));
    assert_eq!(enqueued[1].expected_generation, Some(completion_ticket(42)));
}

#[tokio::test]
async fn compaction_fence_retains_unrelated_agent_scope_state() {
    struct ScopedEnqueuer {
        active: Mutex<HashMap<String, CacheKeepaliveCompletionTicket>>,
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
                request
                    .expected_generation
                    .unwrap_or_else(|| completion_ticket(1)),
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
        ) -> Result<Option<CacheKeepaliveCompletionTicket>, CacheKeepaliveEnqueueError> {
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
    Observe(String, Option<CacheKeepaliveCompletionTicket>),
    Enqueue(String, Option<CacheKeepaliveCompletionTicket>),
    Cancel(String, Option<CacheKeepaliveCompletionTicket>, CancelReason),
    NotTracked(String, String),
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ScopedKeepaliveState {
    generations: std::collections::BTreeMap<String, CacheKeepaliveCompletionTicket>,
    request_owners: std::collections::BTreeMap<String, Option<CacheKeepaliveCompletionTicket>>,
    cancellations:
        std::collections::BTreeMap<String, (Option<CacheKeepaliveCompletionTicket>, CancelReason)>,
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
    ) -> Result<Option<CacheKeepaliveCompletionTicket>, CacheKeepaliveEnqueueError> {
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
        let generation = completion_ticket(state.next_generation);
        state.cancellations.remove(&key);
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
            state.generations.get(&key).copied()
        } else {
            state
                .request_owners
                .get(&key)
                .copied()
                .unwrap_or_else(|| state.generations.get(&key).copied())
        };
        state.operations.push(KeepaliveOperation::Cancel(
            key.clone(),
            request.expected_generation,
            request.reason,
        ));
        if request.expected_generation != expected_owner {
            // Storage rejects an older captured ticket after a new owner has
            // replaced the provisional marker; recording the attempt is useful
            // for checking that completion did not re-observe that newer owner.
            assert_ne!(request.reason, CancelReason::NewRequest);
            self.completed.notify_one();
            return Ok(());
        }
        if request.reason == CancelReason::NewRequest {
            state
                .request_owners
                .insert(key.clone(), request.expected_generation);
        } else {
            state.request_owners.remove(&key);
        }
        // The real store retains a terminal NewRequest marker, including its
        // owner, until a real completion replaces its reason or enqueues it.
        state
            .cancellations
            .insert(key.clone(), (request.expected_generation, request.reason));
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
    observed_keepalive_response_with_shape(
        upstream_body,
        enqueuer,
        principal,
        request_body,
        request_headers,
        streaming,
        false,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn observed_keepalive_response_with_shape(
    upstream_body: Bytes,
    enqueuer: Arc<dyn CacheKeepaliveEnqueuer>,
    principal: PrincipalRecord,
    request_body: Bytes,
    request_headers: &[(&str, &str)],
    streaming: bool,
    shape_fallback: bool,
) -> (Response<Body>, Option<String>) {
    let principal_name = principal.name.clone();
    let mut chains = HashMap::new();
    if shape_fallback {
        use cc_lb_control::api_keys::principal_view::{
            DialectCache, RouterPipelineCache, ShapePluginCache,
        };
        chains.insert(
            principal_name.clone(),
            (
                Some(Arc::new(RouterPipelineCache::empty(
                    cc_lb_domain::TerminalStrategy::FirstPick,
                ))),
                DialectCache::Explicit(ShapePluginCache {
                    dialect: Arc::new(FailingKeepaliveShape {
                        fail: true,
                        transform_sse: false,
                        on_shape: Mutex::new(None),
                        buffered_transform: None,
                    }),
                }),
            ),
        );
    }
    let principal_view = Arc::new(PrincipalView::from_db(&[principal], chains));
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

#[derive(Clone, Copy, Debug)]
enum CompletionFallbackResponse {
    InvalidJson,
    Empty,
    InvalidGzip,
    UnsupportedEncoding,
    AffinityRejected,
    SseTerminalOnly,
    ShapeFallbackBuffered,
    ShapeFallbackSse,
    ShapeFallbackTransformedSse,
    ShapeFallbackSseDropAfterTerminal,
    ShapeFallbackSseAbortBeforeTerminal,
    ShapeFallbackBodyFailure,
    ShapeFallbackTransportFailure,
    ShapeFallbackAbort,
    DynamicStartBuffered,
    DynamicStartSse,
    SseEmpty,
    SseAffinityRejected,
    HttpFailure,
    ProviderErrorEnvelope,
    ProviderFailedEnvelope,
    ProviderErrorWithoutDetails,
    ProviderFailedWithoutDetails,
    ContextWindowExceeded,
    SseRefusal,
    SseContextWindowExceeded,
    SseRefusalWithoutTerminal,
    SseContextWindowExceededWithoutTerminal,
    SseRefusalAbortBeforeTerminal,
    SseProviderError,
    SseProviderFailed,
    SseHttpErrorEof,
    SseHttpErrorPartialEof,
    SseHttpErrorTruncatedGzipEof,
    HttpErrorStreamBodyEof,
    SseProviderErrorIncomplete,
    SseRedirectTerminal,
    BufferedHookProviderError,
    BufferedHookFailure,
    BufferedCapFinalFrame,
    BufferedCapBeforeFinalFrame,
    AbnormalStop,
    BodyFailure,
    TransportFailure,
    ClientAbort,
}

struct CompletionFallbackDispatch {
    pending: PendingFollowupDispatch,
    response: CompletionFallbackResponse,
}

#[async_trait]
impl UpstreamDispatch for CompletionFallbackDispatch {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let mut response = self.pending.dispatch(request).await?;
        let body = match self.response {
            CompletionFallbackResponse::InvalidJson => Bytes::from_static(b"not completion JSON"),
            CompletionFallbackResponse::Empty | CompletionFallbackResponse::SseEmpty => {
                Bytes::new()
            }
            CompletionFallbackResponse::InvalidGzip => {
                response.headers_mut().insert(
                    http::header::CONTENT_ENCODING,
                    http::HeaderValue::from_static("gzip"),
                );
                Bytes::from_static(b"not a gzip member")
            }
            CompletionFallbackResponse::UnsupportedEncoding => {
                response.headers_mut().insert(
                    http::header::CONTENT_ENCODING,
                    http::HeaderValue::from_static("unsupported-test-encoding"),
                );
                self.pending.body.clone()
            }
            CompletionFallbackResponse::SseTerminalOnly => {
                Bytes::from_static(b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
            }
            CompletionFallbackResponse::SseAffinityRejected => {
                streaming_body_from_message(&self.pending.body)
            }
            CompletionFallbackResponse::SseProviderError => Bytes::from_static(
                b"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"fixture provider failed\"}}\n\n",
            ),
            CompletionFallbackResponse::SseProviderFailed => Bytes::from_static(
                b"event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_failed\",\"object\":\"response\",\"status\":\"failed\",\"error\":{\"type\":\"server_error\",\"code\":\"server_error\",\"message\":\"fixture provider failed\"}}}\n\n",
            ),
            CompletionFallbackResponse::SseHttpErrorEof => Bytes::from_static(
                b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_http_error\",\"type\":\"message\",\"content\":[]}}\n\n",
            ),
            CompletionFallbackResponse::SseHttpErrorPartialEof => Bytes::from_static(
                b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_http_error\",\"type\":\"message\",\"content\":[]}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":",
            ),
            CompletionFallbackResponse::SseHttpErrorTruncatedGzipEof => {
                use std::io::{Read as _, Write as _};
                let plaintext = b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_http_error\",\"type\":\"message\",\"content\":[]}}\n\n";
                let mut encoder =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                encoder.write_all(plaintext).expect("gzip fixture writes");
                let mut compressed = encoder.finish().expect("gzip fixture finishes");
                compressed.truncate(compressed.len() - 8);
                let mut decoded = Vec::new();
                flate2::read::GzDecoder::new(compressed.as_slice())
                    .read_to_end(&mut decoded)
                    .expect_err("missing CRC32/ISIZE trailer is a decode failure");
                assert_eq!(decoded, plaintext, "nonterminal event decodes before trailer failure");
                response.headers_mut().insert(
                    http::header::CONTENT_ENCODING,
                    http::HeaderValue::from_static("gzip"),
                );
                Bytes::from(compressed)
            }
            CompletionFallbackResponse::SseProviderErrorIncomplete => Bytes::from_static(
                b"event: error\ndata: {\"type\":\"error\",\"error\":",
            ),
            CompletionFallbackResponse::SseRefusal
            | CompletionFallbackResponse::SseContextWindowExceeded
            | CompletionFallbackResponse::SseRedirectTerminal => {
                streaming_body_from_message(&self.pending.body)
            }
            CompletionFallbackResponse::SseRefusalWithoutTerminal
            | CompletionFallbackResponse::SseContextWindowExceededWithoutTerminal
            | CompletionFallbackResponse::SseRefusalAbortBeforeTerminal => {
                let body = streaming_body_from_message(&self.pending.body);
                let terminal = b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";
                assert!(body.ends_with(terminal), "fixture must remove only message_stop");
                body.slice(..body.len() - terminal.len())
            }
            CompletionFallbackResponse::BodyFailure
            | CompletionFallbackResponse::ShapeFallbackBodyFailure => {
                let partial = self.pending.body.clone();
                *response.body_mut() = Body::from_stream(async_stream::stream! {
                    yield Ok::<Bytes, std::io::Error>(partial);
                    yield Err(std::io::Error::other("fixture upstream body reset"));
                });
                return Ok(response);
            }
            CompletionFallbackResponse::TransportFailure
            | CompletionFallbackResponse::ShapeFallbackTransportFailure => {
                return Err(DispatchError::Transport {
                    source: Box::new(std::io::Error::other("fixture upstream connection reset")),
                });
            }
            CompletionFallbackResponse::ShapeFallbackSse
            | CompletionFallbackResponse::DynamicStartSse
            | CompletionFallbackResponse::ShapeFallbackTransformedSse
            | CompletionFallbackResponse::ShapeFallbackSseDropAfterTerminal
            | CompletionFallbackResponse::ShapeFallbackSseAbortBeforeTerminal => {
                streaming_body_from_message(&self.pending.body)
            }
            CompletionFallbackResponse::ShapeFallbackBuffered
            | CompletionFallbackResponse::DynamicStartBuffered
            | CompletionFallbackResponse::ShapeFallbackAbort
            | CompletionFallbackResponse::AffinityRejected
            | CompletionFallbackResponse::HttpFailure
            | CompletionFallbackResponse::HttpErrorStreamBodyEof
            | CompletionFallbackResponse::ProviderErrorEnvelope
            | CompletionFallbackResponse::ProviderFailedEnvelope
            | CompletionFallbackResponse::ProviderErrorWithoutDetails
            | CompletionFallbackResponse::ProviderFailedWithoutDetails
            | CompletionFallbackResponse::BufferedHookProviderError
            | CompletionFallbackResponse::BufferedHookFailure
            | CompletionFallbackResponse::BufferedCapFinalFrame
            | CompletionFallbackResponse::BufferedCapBeforeFinalFrame
            | CompletionFallbackResponse::ContextWindowExceeded
            | CompletionFallbackResponse::AbnormalStop
            | CompletionFallbackResponse::ClientAbort => self.pending.body.clone(),
        };
        if matches!(
            self.response,
            CompletionFallbackResponse::SseTerminalOnly
                | CompletionFallbackResponse::SseEmpty
                | CompletionFallbackResponse::SseAffinityRejected
                | CompletionFallbackResponse::SseRefusal
                | CompletionFallbackResponse::SseContextWindowExceeded
                | CompletionFallbackResponse::SseRefusalWithoutTerminal
                | CompletionFallbackResponse::SseContextWindowExceededWithoutTerminal
                | CompletionFallbackResponse::SseRefusalAbortBeforeTerminal
                | CompletionFallbackResponse::SseProviderError
                | CompletionFallbackResponse::SseProviderFailed
                | CompletionFallbackResponse::SseHttpErrorEof
                | CompletionFallbackResponse::SseHttpErrorPartialEof
                | CompletionFallbackResponse::SseHttpErrorTruncatedGzipEof
                | CompletionFallbackResponse::SseProviderErrorIncomplete
                | CompletionFallbackResponse::SseRedirectTerminal
                | CompletionFallbackResponse::ShapeFallbackSse
                | CompletionFallbackResponse::DynamicStartSse
                | CompletionFallbackResponse::ShapeFallbackTransformedSse
                | CompletionFallbackResponse::ShapeFallbackSseDropAfterTerminal
                | CompletionFallbackResponse::ShapeFallbackSseAbortBeforeTerminal
        ) {
            response.headers_mut().insert(
                http::header::CONTENT_TYPE,
                http::HeaderValue::from_static("text/event-stream"),
            );
        }
        if matches!(
            self.response,
            CompletionFallbackResponse::HttpFailure
                | CompletionFallbackResponse::SseHttpErrorEof
                | CompletionFallbackResponse::SseHttpErrorPartialEof
                | CompletionFallbackResponse::SseHttpErrorTruncatedGzipEof
                | CompletionFallbackResponse::HttpErrorStreamBodyEof
        ) {
            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
        }
        if matches!(
            self.response,
            CompletionFallbackResponse::SseRedirectTerminal
        ) {
            *response.status_mut() = StatusCode::TEMPORARY_REDIRECT;
        }
        if matches!(
            self.response,
            CompletionFallbackResponse::ShapeFallbackSseDropAfterTerminal
                | CompletionFallbackResponse::ShapeFallbackSseAbortBeforeTerminal
                | CompletionFallbackResponse::SseRefusalAbortBeforeTerminal
                | CompletionFallbackResponse::BufferedCapBeforeFinalFrame
        ) {
            let body = if matches!(
                self.response,
                CompletionFallbackResponse::ShapeFallbackSseAbortBeforeTerminal
            ) {
                Bytes::from_static(b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"type\":\"message\",\"content\":[]}}\n\n")
            } else {
                body
            };
            *response.body_mut() = Body::from_stream(async_stream::stream! {
                yield Ok::<Bytes, std::io::Error>(body);
                std::future::pending::<()>().await;
            });
        } else {
            *response.body_mut() = Body::from(body);
        }
        Ok(response)
    }
}

#[tokio::test(start_paused = true)]
async fn completion_fallback_buffered_undecodable_retires_captured_owners() {
    let mut violations = Vec::new();
    for response in [
        CompletionFallbackResponse::InvalidJson,
        CompletionFallbackResponse::Empty,
        CompletionFallbackResponse::InvalidGzip,
        CompletionFallbackResponse::UnsupportedEncoding,
    ] {
        for replace in [false, true] {
            violations.extend(assert_completion_fallback(response, replace, true).await);
        }
    }
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

#[tokio::test(start_paused = true)]
async fn completion_fallback_buffered_affinity_rejection_retires_captured_owners() {
    let mut violations = Vec::new();
    for replace in [false, true] {
        violations.extend(
            assert_completion_fallback(CompletionFallbackResponse::AffinityRejected, replace, true)
                .await,
        );
    }
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

#[tokio::test(start_paused = true)]
async fn completion_fallback_sse_terminal_without_snapshot_retires_captured_owners() {
    let mut violations = Vec::new();
    for replace in [false, true] {
        violations.extend(
            assert_completion_fallback(CompletionFallbackResponse::SseTerminalOnly, replace, true)
                .await,
        );
    }
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

#[tokio::test(start_paused = true)]
async fn completion_fallback_incomplete_transport_body_and_abort_remain_neutral() {
    let mut violations = Vec::new();
    for response in [
        CompletionFallbackResponse::SseEmpty,
        CompletionFallbackResponse::SseAffinityRejected,
        CompletionFallbackResponse::SseRefusalWithoutTerminal,
        CompletionFallbackResponse::SseContextWindowExceededWithoutTerminal,
        CompletionFallbackResponse::SseRefusalAbortBeforeTerminal,
        CompletionFallbackResponse::BodyFailure,
        CompletionFallbackResponse::TransportFailure,
        CompletionFallbackResponse::ClientAbort,
    ] {
        for replace in [false, true] {
            violations.extend(assert_completion_fallback(response, replace, false).await);
        }
    }
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}
#[tokio::test(start_paused = true)]
async fn completion_fallback_shape_failure_buffered_retires_captured_owners() {
    let mut violations = Vec::new();
    for replace in [false, true] {
        violations.extend(
            assert_completion_fallback(
                CompletionFallbackResponse::ShapeFallbackBuffered,
                replace,
                true,
            )
            .await,
        );
    }
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

#[tokio::test(start_paused = true)]
async fn completion_fallback_shape_failure_sse_retires_captured_owners() {
    let mut violations = Vec::new();
    for replace in [false, true] {
        for response in [
            CompletionFallbackResponse::ShapeFallbackSse,
            CompletionFallbackResponse::ShapeFallbackTransformedSse,
            CompletionFallbackResponse::ShapeFallbackSseDropAfterTerminal,
        ] {
            violations.extend(assert_completion_fallback(response, replace, true).await);
        }
    }
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

#[tokio::test(start_paused = true)]
async fn completion_fallback_shape_failure_incomplete_remains_neutral() {
    let mut violations = Vec::new();
    for response in [
        CompletionFallbackResponse::ShapeFallbackBodyFailure,
        CompletionFallbackResponse::ShapeFallbackTransportFailure,
        CompletionFallbackResponse::ShapeFallbackAbort,
        CompletionFallbackResponse::ShapeFallbackSseAbortBeforeTerminal,
    ] {
        for replace in [false, true] {
            violations.extend(assert_completion_fallback(response, replace, false).await);
        }
    }
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}
#[tokio::test(start_paused = true)]
async fn completion_fallback_dynamic_start_without_initial_tracking_retires_owners() {
    let mut violations = Vec::new();
    for response in [
        CompletionFallbackResponse::DynamicStartBuffered,
        CompletionFallbackResponse::DynamicStartSse,
    ] {
        for replace in [false, true] {
            violations.extend(assert_completion_fallback(response, replace, true).await);
        }
    }
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}
#[tokio::test(start_paused = true)]
async fn completion_fallback_buffered_http_failure_retires_captured_owners() {
    let mut violations = Vec::new();
    for response in [CompletionFallbackResponse::HttpFailure] {
        for replace in [false, true] {
            violations.extend(assert_completion_fallback(response, replace, true).await);
        }
    }
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}
#[tokio::test(start_paused = true)]
async fn completion_fallback_buffered_abnormal_stop_retires_captured_owners() {
    let mut violations = Vec::new();
    // R6's refusal-neutral expectation confused an unsuccessful outcome with
    // absence of terminal evidence. Fully collected messages are terminal.
    for response in [
        CompletionFallbackResponse::AbnormalStop,
        CompletionFallbackResponse::ContextWindowExceeded,
    ] {
        for replace in [false, true] {
            violations.extend(assert_completion_fallback(response, replace, true).await);
        }
    }
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

#[tokio::test(start_paused = true)]
async fn completion_fallback_buffered_provider_error_retires_captured_owners() {
    let mut violations = Vec::new();
    for response in [
        CompletionFallbackResponse::ProviderErrorEnvelope,
        CompletionFallbackResponse::ProviderFailedEnvelope,
    ] {
        for replace in [false, true] {
            violations.extend(assert_completion_fallback(response, replace, true).await);
        }
    }
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

#[tokio::test(start_paused = true)]
async fn completion_fallback_buffered_declared_terminal_without_details_retires_captured_owners() {
    let mut violations = Vec::new();
    for response in [
        CompletionFallbackResponse::ProviderErrorWithoutDetails,
        CompletionFallbackResponse::ProviderFailedWithoutDetails,
    ] {
        for replace in [false, true] {
            violations.extend(assert_completion_fallback(response, replace, true).await);
        }
    }
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

#[tokio::test(start_paused = true)]
async fn completion_fallback_buffered_declared_terminal_lookalike_content_remains_renewable() {
    for lookalike in [
        json!({"type": "error", "error": null}),
        json!({"object": "response", "status": "failed", "error": null}),
    ] {
        let enqueuer = Arc::new(ScopedKeepaliveEnqueuer::default());
        let principal = principal_with_keepalive();
        let headers = [
            ("x-claude-code-session-id", "terminal-envelope-lookalike"),
            ("x-claude-code-agent-id", "main"),
        ];
        establish_scoped_keepalive(&enqueuer, principal.clone(), &headers).await;
        let before = enqueuer.snapshot();
        let mut message: serde_json::Value =
            serde_json::from_slice(&agent_turn_body("msg_error_lookalike", "Bash", 12_000))
                .expect("ordinary renewable message");
        message["content"]
            .as_array_mut()
            .expect("message content")
            .push(json!({"type": "text", "text": lookalike.to_string()}));
        let body = Bytes::from(serde_json::to_vec(&message).expect("lookalike JSON"));
        let (response, _) = observed_keepalive_response(
            body.clone(),
            Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>,
            principal,
            standard_keepalive_request(),
            &headers,
            false,
        )
        .await;
        assert_keepalive_response_fidelity(response, &body, false).await;
        enqueuer.wait_for_enqueue().await;
        let after = enqueuer.snapshot();
        let operations = &after.operations[before.operations.len()..];
        assert!(
            operations
                .iter()
                .any(|operation| matches!(operation, KeepaliveOperation::Enqueue(_, _))),
            "error-shaped text is not a declared terminal error: {operations:?}"
        );
        assert!(
            !operations.iter().any(|operation| matches!(
                operation,
                KeepaliveOperation::Cancel(_, _, reason) if reason.as_str() == "completed_not_renewable"
            )),
            "ordinary success with error-shaped content remains renewable: {operations:?}"
        );
        assert_ne!(after.generations, before.generations);
        eprintln!("lookalike content remains renewable: {lookalike}, operations={operations:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn completion_fallback_sse_abnormal_terminal_retires_captured_owners() {
    let mut violations = Vec::new();
    for response in [
        CompletionFallbackResponse::SseRefusal,
        CompletionFallbackResponse::SseContextWindowExceeded,
    ] {
        for replace in [false, true] {
            violations.extend(assert_completion_fallback(response, replace, true).await);
        }
    }
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

#[tokio::test(start_paused = true)]
async fn completion_fallback_stream_provider_terminal_and_http_error_eof_retire_captured_owners() {
    let mut violations = Vec::new();
    for response in [
        CompletionFallbackResponse::SseProviderError,
        CompletionFallbackResponse::SseProviderFailed,
        CompletionFallbackResponse::SseHttpErrorEof,
        CompletionFallbackResponse::HttpErrorStreamBodyEof,
    ] {
        for replace in [false, true] {
            violations.extend(assert_completion_fallback(response, replace, true).await);
        }
    }
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

#[tokio::test(start_paused = true)]
async fn completion_fallback_stream_redirect_terminal_retires_captured_owners() {
    let mut violations = Vec::new();
    for replace in [false, true] {
        violations.extend(
            assert_completion_fallback(
                CompletionFallbackResponse::SseRedirectTerminal,
                replace,
                true,
            )
            .await,
        );
    }
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

#[tokio::test(start_paused = true)]
async fn completion_fallback_stream_provider_terminal_incomplete_controls_remain_neutral() {
    let mut violations = Vec::new();
    for response in [
        CompletionFallbackResponse::SseProviderErrorIncomplete,
        CompletionFallbackResponse::SseRefusalWithoutTerminal,
        CompletionFallbackResponse::SseAffinityRejected,
    ] {
        for replace in [false, true] {
            violations.extend(assert_completion_fallback(response, replace, false).await);
        }
    }
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

#[tokio::test(start_paused = true)]
async fn completion_fallback_http_error_sse_unvalidated_eof_remains_neutral() {
    let mut violations = Vec::new();
    for response in [
        CompletionFallbackResponse::SseHttpErrorPartialEof,
        CompletionFallbackResponse::SseHttpErrorTruncatedGzipEof,
    ] {
        for replace in [false, true] {
            violations.extend(assert_completion_fallback(response, replace, false).await);
        }
    }
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

#[tokio::test(start_paused = true)]
async fn completion_fallback_buffered_hook_failure_retires_captured_owners() {
    let mut violations = Vec::new();
    for response in [
        CompletionFallbackResponse::BufferedHookProviderError,
        CompletionFallbackResponse::BufferedHookFailure,
    ] {
        for replace in [false, true] {
            violations.extend(assert_completion_fallback(response, replace, true).await);
        }
    }
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

#[tokio::test(start_paused = true)]
async fn completion_fallback_buffered_cap_final_frame_retires_captured_owners() {
    let mut violations = Vec::new();
    for replace in [false, true] {
        violations.extend(
            assert_completion_fallback(
                CompletionFallbackResponse::BufferedCapFinalFrame,
                replace,
                true,
            )
            .await,
        );
    }
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

#[tokio::test(start_paused = true)]
async fn completion_fallback_buffered_cap_before_final_frame_remains_neutral() {
    let mut violations = Vec::new();
    for replace in [false, true] {
        violations.extend(
            assert_completion_fallback(
                CompletionFallbackResponse::BufferedCapBeforeFinalFrame,
                replace,
                false,
            )
            .await,
        );
    }
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

struct FailingKeepaliveShape {
    fail: bool,
    transform_sse: bool,
    on_shape: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    buffered_transform: Option<CompletionFallbackResponse>,
}

impl cc_lb_upstream::UpstreamDialect for FailingKeepaliveShape {
    fn shape(
        &self,
        context: &cc_lb_upstream::DialectShapeContext,
        _upstream: &cc_lb_domain::Upstream,
        _principal: &cc_lb_domain::Principal,
        builder: &mut cc_lb_upstream::ShapedRequestBuilder,
    ) -> Result<cc_lb_upstream::ShapedRequest, cc_lb_upstream::DialectError> {
        if let Some(on_shape) = self.on_shape.lock().expect("shape callback").take() {
            on_shape();
        }
        if self.fail {
            return Err(cc_lb_upstream::DialectError::UnsupportedRequest {
                reason: "fixture shape failure exercises raw passthrough".to_owned(),
            });
        }
        Ok(builder.shaped_request(
            url::Url::parse("http://upstream.local/v1/messages").expect("fixture URL"),
            context.method.clone(),
            context.downstream_headers.clone(),
            context.body_bytes.clone(),
        ))
    }

    fn sse_event_transform_hook(&self) -> Option<&dyn cc_lb_upstream::SseEventTransformHook> {
        self.transform_sse.then_some(self)
    }

    fn response_transform_hook(&self) -> Option<&dyn cc_lb_upstream::ResponseTransformHook> {
        self.buffered_transform
            .map(|_| self as &dyn cc_lb_upstream::ResponseTransformHook)
    }
}

impl cc_lb_upstream::SseEventTransformHook for FailingKeepaliveShape {
    fn transform_sse_event(
        &self,
        _request: cc_lb_upstream::TransformSseEventRequest,
    ) -> Result<cc_lb_upstream::TransformSseEventResult, cc_lb_upstream::ResponseTransformError>
    {
        Ok(cc_lb_upstream::TransformSseEventResult::Unchanged)
    }
}

impl cc_lb_upstream::ResponseTransformHook for FailingKeepaliveShape {
    fn transform_response(
        &self,
        request: cc_lb_upstream::TransformResponseRequest,
    ) -> Result<cc_lb_upstream::TransformResponseResult, cc_lb_upstream::ResponseTransformError>
    {
        let upstream: serde_json::Value =
            serde_json::from_slice(&request.body).expect("hook receives fully collected JSON");
        assert_eq!(request.response_status, StatusCode::OK);
        assert_eq!(upstream["type"], "message");
        assert_eq!(upstream["stop_reason"], "tool_use");
        match self.buffered_transform {
            Some(CompletionFallbackResponse::BufferedHookProviderError) => {
                Ok(cc_lb_upstream::TransformResponseResult::Replace {
                    status: None,
                    headers: None,
                    body: Some(Bytes::from_static(
                        br#"{"type":"error","error":{"type":"api_error","message":"fixture buffered hook error"}}"#,
                    )),
                })
            }
            Some(CompletionFallbackResponse::BufferedHookFailure) => {
                Err(cc_lb_upstream::ResponseTransformError::Trap {
                    reason: "fixture buffered hook failed after full collection".to_owned(),
                })
            }
            _ => unreachable!("buffered hook is installed only for its declared fixture modes"),
        }
    }
}

async fn assert_completion_fallback(
    response: CompletionFallbackResponse,
    replace: bool,
    should_complete: bool,
) -> Vec<String> {
    let enqueuer = Arc::new(ScopedKeepaliveEnqueuer::default());
    let principal = principal_with_keepalive();
    let primary_headers = [
        ("x-claude-code-session-id", "completion-fallback"),
        ("x-claude-code-agent-id", "main"),
    ];
    let sibling_headers = [
        ("x-claude-code-session-id", "completion-fallback"),
        ("x-claude-code-request-class", "subagent"),
    ];
    establish_scoped_keepalive(&enqueuer, principal.clone(), &primary_headers).await;
    establish_scoped_keepalive(&enqueuer, principal.clone(), &sibling_headers).await;
    let captured = enqueuer.snapshot().generations;
    assert_eq!(
        captured.len(),
        2,
        "fixture must establish primary and sibling"
    );

    // A successful tool response is renewable unless the real response-affinity
    // gate rejects its unsolicited provider-owned container.
    let mut message: serde_json::Value =
        serde_json::from_slice(&agent_turn_body("msg_fallback", "Bash", 12_000))
            .expect("completion fixture");
    if matches!(
        response,
        CompletionFallbackResponse::AffinityRejected
            | CompletionFallbackResponse::SseAffinityRejected
    ) {
        message["container"] = json!({"id": "container-completion-fallback"});
    }
    if matches!(response, CompletionFallbackResponse::SseAffinityRejected) {
        message["content"]
            .as_array_mut()
            .expect("content")
            .push(json!({
                "type": "web_search_tool_result",
                "content": [{"encrypted_content": "container-completion-fallback"}]
            }));
    }
    if matches!(
        response,
        CompletionFallbackResponse::BufferedCapFinalFrame
            | CompletionFallbackResponse::BufferedCapBeforeFinalFrame
    ) {
        // Larger than the response cap, while the request stays below it.
        message["content"][0]["input"]["padding"] = json!("x".repeat(8_192));
    }
    match response {
        CompletionFallbackResponse::AbnormalStop
        | CompletionFallbackResponse::SseRefusal
        | CompletionFallbackResponse::SseRefusalWithoutTerminal
        | CompletionFallbackResponse::SseRefusalAbortBeforeTerminal => {
            message["stop_reason"] = json!("refusal");
        }
        CompletionFallbackResponse::ContextWindowExceeded
        | CompletionFallbackResponse::SseContextWindowExceeded
        | CompletionFallbackResponse::SseContextWindowExceededWithoutTerminal => {
            message["stop_reason"] = json!("model_context_window_exceeded");
        }
        CompletionFallbackResponse::ProviderErrorEnvelope
        | CompletionFallbackResponse::HttpFailure
        | CompletionFallbackResponse::HttpErrorStreamBodyEof => {
            message = json!({
                "type": "error",
                "error": {"type": "api_error", "message": "fixture provider failed"}
            });
        }
        CompletionFallbackResponse::ProviderFailedEnvelope => {
            message = json!({
                "id": "resp_failed",
                "object": "response",
                "status": "failed",
                "error": {"type": "server_error", "code": "server_error", "message": "fixture provider failed"}
            });
        }
        CompletionFallbackResponse::ProviderErrorWithoutDetails => {
            message = json!({"type": "error", "error": null});
        }
        CompletionFallbackResponse::ProviderFailedWithoutDetails => {
            message = json!({"object": "response", "status": "failed", "error": null});
        }
        _ => {}
    }
    let dispatch = Arc::new(CompletionFallbackDispatch {
        pending: PendingFollowupDispatch {
            streaming: false,
            body: Bytes::from(serde_json::to_vec(&message).expect("completion JSON")),
            entered: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
            calls: AtomicUsize::new(0),
        },
        response,
    });
    let dynamic_start = matches!(
        response,
        CompletionFallbackResponse::DynamicStartBuffered
            | CompletionFallbackResponse::DynamicStartSse
    );
    let dialect = Arc::new(FailingKeepaliveShape {
        fail: !dynamic_start
            && !matches!(
                response,
                CompletionFallbackResponse::BufferedHookProviderError
                    | CompletionFallbackResponse::BufferedHookFailure
                    | CompletionFallbackResponse::BufferedCapFinalFrame
                    | CompletionFallbackResponse::BufferedCapBeforeFinalFrame
            ),
        transform_sse: matches!(
            response,
            CompletionFallbackResponse::ShapeFallbackTransformedSse
        ),
        on_shape: Mutex::new(None),
        buffered_transform: matches!(
            response,
            CompletionFallbackResponse::BufferedHookProviderError
                | CompletionFallbackResponse::BufferedHookFailure
                | CompletionFallbackResponse::BufferedCapFinalFrame
                | CompletionFallbackResponse::BufferedCapBeforeFinalFrame
        )
        .then_some(response),
    });
    let mut chains = HashMap::new();
    if matches!(
        response,
        CompletionFallbackResponse::ShapeFallbackBuffered
            | CompletionFallbackResponse::ShapeFallbackSse
            | CompletionFallbackResponse::ShapeFallbackTransformedSse
            | CompletionFallbackResponse::ShapeFallbackSseDropAfterTerminal
            | CompletionFallbackResponse::ShapeFallbackSseAbortBeforeTerminal
            | CompletionFallbackResponse::ShapeFallbackBodyFailure
            | CompletionFallbackResponse::ShapeFallbackTransportFailure
            | CompletionFallbackResponse::ShapeFallbackAbort
            | CompletionFallbackResponse::DynamicStartBuffered
            | CompletionFallbackResponse::DynamicStartSse
            | CompletionFallbackResponse::BufferedHookProviderError
            | CompletionFallbackResponse::BufferedHookFailure
            | CompletionFallbackResponse::BufferedCapFinalFrame
            | CompletionFallbackResponse::BufferedCapBeforeFinalFrame
    ) {
        chains.insert(
            principal.name.clone(),
            (
                Some(Arc::new(
                    cc_lb_control::api_keys::principal_view::RouterPipelineCache::empty(
                        cc_lb_domain::TerminalStrategy::FirstPick,
                    ),
                )),
                cc_lb_control::api_keys::principal_view::DialectCache::Explicit(
                    cc_lb_control::api_keys::principal_view::ShapePluginCache {
                        dialect: dialect.clone(),
                    },
                ),
            ),
        );
    }
    let initial_principal = if dynamic_start {
        let mut initial = principal.clone();
        initial.cache_keepalive = None;
        initial
    } else {
        principal.clone()
    };
    let authn = TestAuthn::with_principal_view(
        TestState::default(),
        Arc::new(PrincipalView::from_db(&[initial_principal], chains.clone())),
    );
    let config = if matches!(
        response,
        CompletionFallbackResponse::BufferedCapFinalFrame
            | CompletionFallbackResponse::BufferedCapBeforeFinalFrame
    ) {
        assert!(dispatch.pending.body.len() > 4_096);
        assert!(standard_keepalive_request().len() < 4_096);
        LifecycleConfig {
            messages_body_cap_bytes: 4_096,
            ..LifecycleConfig::default()
        }
    } else {
        LifecycleConfig::default()
    };
    let mut lifecycle = lifecycle_with_parts(authn, dispatch.clone(), config)
        .with_cache_keepalive_enqueuer(enqueuer.clone());
    if dynamic_start {
        let holder = lifecycle.dynamic_view();
        let enabled = cc_lb_control::DynamicViewBuilder::from_view(&holder.load())
            .principal_view(Arc::new(PrincipalView::from_db(
                std::slice::from_ref(&principal),
                chains,
            )))
            .build();
        *dialect.on_shape.lock().expect("shape callback") = Some(Box::new(move || {
            holder.store(enabled);
        }));
    }
    if matches!(
        response,
        CompletionFallbackResponse::AffinityRejected
            | CompletionFallbackResponse::SseAffinityRejected
    ) {
        let store = Arc::new(crate::upstream_affinity::MemoryAffinityStore::default());
        store.fail_bind("fixture response-affinity storage unavailable");
        lifecycle = lifecycle.with_upstream_affinity_store(store);
    }
    let bus = Arc::new(cc_lb_control::InMemoryBus::new());
    let mut events = bus.attach_lifecycle_assembler(128);
    lifecycle = lifecycle.with_event_bus(bus);
    let request_body = if matches!(
        response,
        CompletionFallbackResponse::HttpErrorStreamBodyEof
            | CompletionFallbackResponse::BufferedCapFinalFrame
            | CompletionFallbackResponse::BufferedCapBeforeFinalFrame
    ) {
        let mut body: serde_json::Value =
            serde_json::from_slice(&standard_keepalive_request()).expect("request fixture");
        body["stream"] = json!(true);
        Bytes::from(serde_json::to_vec(&body).expect("stream request"))
    } else {
        standard_keepalive_request()
    };
    // No class/agent header: request-start must capture the generic sibling as
    // well as the primary, before upstream headers or body become available.
    let request = http::Request::builder()
        .method(http::Method::POST)
        .uri("/v1/messages")
        .header("x-api-key", managed_api_key())
        .header("anthropic-version", "2023-06-01")
        .header("x-claude-code-session-id", "completion-fallback")
        .body(request_body)
        .expect("fallback request");
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("auth");
    let mut handling = Box::pin(lifecycle.handle(request, &auth));
    tokio::select! {
        () = dispatch.pending.entered.notified() => {}
        _ = &mut handling => panic!("dispatch must wait for the deterministic release"),
    }
    let pending = enqueuer.snapshot();
    for (key, owner) in &captured {
        assert_eq!(
            pending.cancellations.get(key),
            Some(&(Some(*owner), CancelReason::NewRequest)),
            "fixture must retain each captured owner in its provisional marker"
        );
    }
    if replace {
        // These are new real completions, not a mutation of the expected result.
        // Explicit main identity prevents setup from capturing the sibling.
        establish_scoped_keepalive(&enqueuer, principal.clone(), &primary_headers).await;
        establish_scoped_keepalive(&enqueuer, principal, &sibling_headers).await;
    }
    let before_completion = enqueuer.snapshot();
    let operations_start = before_completion.operations.len();
    if matches!(
        response,
        CompletionFallbackResponse::ClientAbort | CompletionFallbackResponse::ShapeFallbackAbort
    ) {
        drop(handling);
    } else {
        dispatch.pending.release.notify_one();
        let response_body = handling.await.expect("HTTP lifecycle response");
        let (status, _headers, body) = if matches!(
            response,
            CompletionFallbackResponse::ShapeFallbackSseDropAfterTerminal
                | CompletionFallbackResponse::ShapeFallbackSseAbortBeforeTerminal
                | CompletionFallbackResponse::SseRefusalAbortBeforeTerminal
        ) {
            use http_body_util::BodyExt as _;
            let status = response_body.status();
            let headers = response_body.headers().clone();
            let mut downstream = response_body.into_body();
            let after_terminal = matches!(
                response,
                CompletionFallbackResponse::ShapeFallbackSseDropAfterTerminal,
            );
            let body = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                let mut body = Vec::new();
                loop {
                    let frame = downstream
                        .frame()
                        .await
                        .expect("SSE frame")
                        .expect("successful SSE frame")
                        .into_data()
                        .expect("data frame");
                    body.extend_from_slice(&frame);
                    let terminal = body
                        .windows(b"message_stop".len())
                        .any(|window| window == b"message_stop");
                    if terminal || !after_terminal {
                        assert_eq!(
                            terminal, after_terminal,
                            "drop occurs on the intended side of semantic completion"
                        );
                        break Bytes::from(body);
                    }
                }
            })
            .await
            .expect("semantic terminal is delivered without waiting for EOF");
            drop(downstream);
            (status, headers, body)
        } else {
            collect_body(response_body).await
        };
        match response {
            CompletionFallbackResponse::AffinityRejected => {
                assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
                assert!(!String::from_utf8_lossy(&body).contains("container-completion-fallback"));
            }
            CompletionFallbackResponse::ShapeFallbackBodyFailure
            | CompletionFallbackResponse::ShapeFallbackTransportFailure
            | CompletionFallbackResponse::BodyFailure
            | CompletionFallbackResponse::TransportFailure => {
                assert_eq!(status, StatusCode::BAD_GATEWAY);
            }
            CompletionFallbackResponse::SseAffinityRejected => {
                assert_eq!(status, StatusCode::OK);
                assert!(String::from_utf8_lossy(&body).contains("event: error"));
                assert!(!String::from_utf8_lossy(&body).contains("container-completion-fallback"));
            }
            CompletionFallbackResponse::SseEmpty => {
                assert_eq!(status, StatusCode::OK);
                assert!(
                    body.is_empty(),
                    "Anthropic preserves the empty upstream body"
                );
            }
            CompletionFallbackResponse::InvalidJson => {
                assert_eq!(status, StatusCode::OK);
                assert_eq!(body.as_ref(), b"not completion JSON");
            }
            CompletionFallbackResponse::Empty => {
                assert_eq!(status, StatusCode::OK);
                assert!(body.is_empty());
            }
            CompletionFallbackResponse::InvalidGzip => {
                assert_eq!(status, StatusCode::OK);
                assert_eq!(body.as_ref(), b"not a gzip member");
            }
            CompletionFallbackResponse::UnsupportedEncoding => {
                assert_eq!(status, StatusCode::OK);
                assert_eq!(body, dispatch.pending.body);
            }
            CompletionFallbackResponse::HttpFailure
            | CompletionFallbackResponse::HttpErrorStreamBodyEof => {
                assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
                assert_eq!(
                    body, dispatch.pending.body,
                    "HTTP error body remains truthful"
                );
            }
            CompletionFallbackResponse::SseProviderError
            | CompletionFallbackResponse::SseProviderFailed => {
                assert_eq!(status, StatusCode::OK);
                let body = String::from_utf8_lossy(&body);
                assert!(
                    body.contains("fixture provider failed"),
                    "native terminal error is delivered"
                );
                assert!(
                    !body.contains("message_stop"),
                    "provider errors supply their own terminal proof"
                );
            }
            CompletionFallbackResponse::SseHttpErrorEof => {
                assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
                let body = String::from_utf8_lossy(&body);
                assert!(
                    body.contains("msg_http_error"),
                    "HTTP error SSE body is fully drained"
                );
                assert!(
                    !body.contains("message_stop"),
                    "HTTP error EOF is independent terminal proof"
                );
            }
            CompletionFallbackResponse::SseHttpErrorPartialEof => {
                assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
                let body = String::from_utf8_lossy(&body);
                assert!(
                    body.contains("msg_http_error"),
                    "complete nonterminal prefix is relayed"
                );
                assert!(
                    body.contains("content_block_delta"),
                    "trailing partial event is exercised"
                );
                assert!(
                    !body.contains("message_stop"),
                    "partial stream has no native terminal"
                );
            }
            CompletionFallbackResponse::SseHttpErrorTruncatedGzipEof => {
                assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
                assert_eq!(_headers[http::header::CONTENT_ENCODING], "gzip");
                use std::io::Read as _;
                let mut decoded = Vec::new();
                flate2::read::GzDecoder::new(body.as_ref())
                    .read_to_end(&mut decoded)
                    .expect_err("downstream retains the truncated upstream gzip member");
                let decoded = String::from_utf8_lossy(&decoded);
                assert!(decoded.contains("msg_http_error"));
                assert!(
                    !decoded.contains("message_stop"),
                    "decoded stream has no native terminal"
                );
            }
            CompletionFallbackResponse::SseProviderErrorIncomplete => {
                assert_eq!(status, StatusCode::OK);
                assert!(
                    !String::from_utf8_lossy(&body).contains("fixture provider failed"),
                    "incomplete declaration never supplied a provider terminal"
                );
            }
            CompletionFallbackResponse::BufferedHookProviderError => {
                assert_eq!(
                    status,
                    StatusCode::OK,
                    "hook does not replace upstream status"
                );
                assert_eq!(
                    body.as_ref(),
                    br#"{"type":"error","error":{"type":"api_error","message":"fixture buffered hook error"}}"#,
                    "replacement error body is preserved"
                );
            }
            CompletionFallbackResponse::BufferedHookFailure => {
                assert_eq!(
                    status,
                    StatusCode::OK,
                    "hook failure preserves fail-open status"
                );
                assert_eq!(
                    body, dispatch.pending.body,
                    "hook failure preserves original body"
                );
            }
            CompletionFallbackResponse::BufferedCapFinalFrame
            | CompletionFallbackResponse::BufferedCapBeforeFinalFrame => {
                assert_eq!(status, StatusCode::BAD_GATEWAY);
                let error: serde_json::Value =
                    serde_json::from_slice(&body).expect("body-cap failure response JSON");
                assert_eq!(error["type"], "error");
                assert_eq!(error["error"]["type"], "api_error");
                assert_eq!(
                    error["error"]["message"],
                    "upstream response body could not be read"
                );
            }
            CompletionFallbackResponse::ProviderErrorEnvelope
            | CompletionFallbackResponse::ProviderFailedEnvelope
            | CompletionFallbackResponse::ProviderErrorWithoutDetails
            | CompletionFallbackResponse::ProviderFailedWithoutDetails
            | CompletionFallbackResponse::AbnormalStop
            | CompletionFallbackResponse::ContextWindowExceeded => {
                assert_eq!(status, StatusCode::OK);
                assert_eq!(
                    body, dispatch.pending.body,
                    "buffered terminal body is preserved"
                );
            }
            CompletionFallbackResponse::SseRedirectTerminal => {
                assert_eq!(status, StatusCode::TEMPORARY_REDIRECT);
                assert_eq!(body, streaming_body_from_message(&dispatch.pending.body));
                assert!(
                    body.ends_with(b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"),
                    "non-success status remains non-renewable despite explicit successful terminal"
                );
            }
            CompletionFallbackResponse::SseRefusal
            | CompletionFallbackResponse::SseContextWindowExceeded
            | CompletionFallbackResponse::SseRefusalWithoutTerminal
            | CompletionFallbackResponse::SseContextWindowExceededWithoutTerminal
            | CompletionFallbackResponse::SseRefusalAbortBeforeTerminal => {
                assert_eq!(status, StatusCode::OK);
                let terminal = b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";
                let terminal_seen = body.windows(terminal.len()).any(|part| part == terminal);
                assert_eq!(
                    terminal_seen,
                    matches!(
                        response,
                        CompletionFallbackResponse::SseRefusal
                            | CompletionFallbackResponse::SseContextWindowExceeded
                    ),
                    "abnormal stop hint must not stand in for explicit message_stop"
                );
                let stop = message["stop_reason"]
                    .as_str()
                    .expect("abnormal fixture stop");
                assert!(
                    String::from_utf8_lossy(&body).contains(stop),
                    "the abnormal stop hint must be delivered in both controls and terminals"
                );
            }
            _ => assert_eq!(status, StatusCode::OK),
        }
    }
    // The paused clock advances only after runnable completion work has been
    // polled. Notify is the existing persistence seam; the five-second bound is
    // unchanged, and no sleep/yield/re-observation substitutes for completion.
    let persisted =
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let notified = enqueuer.completed.notified();
                if enqueuer.snapshot().operations[operations_start..]
                .iter()
                .filter(|operation| matches!(
                    operation,
                    KeepaliveOperation::Cancel(_, _, reason) if reason.as_str() != "new_request"
                ))
                .count() >= captured.len()
            {
                break;
            }
                notified.await;
            }
        })
        .await
        .is_ok();
    let after = enqueuer.snapshot();
    let operations = &after.operations[operations_start..];
    let mut violations = Vec::new();
    if should_complete {
        if !persisted {
            violations.push(format!("{response:?}, newer={replace}: completed response left captured markers unresolved; operations={operations:?}, cancellations={:?}", after.cancellations));
        }
        for (key, owner) in &captured {
            if !operations.iter().any(|operation| {
                matches!(
                    operation,
                    KeepaliveOperation::Cancel(actual_key, Some(actual_owner), reason)
                        if actual_key == key && actual_owner == owner
                            && reason.as_str() == "completed_not_renewable"
                )
            }) {
                violations.push(format!("{response:?}, newer={replace}: missing completed_not_renewable for captured {key} owner {owner:?}; operations={operations:?}"));
            }
            if !replace
                && !after
                    .cancellations
                    .get(key)
                    .is_some_and(|(actual_owner, reason)| {
                        actual_owner == &Some(*owner)
                            && reason.as_str() == "completed_not_renewable"
                    })
            {
                violations.push(format!(
                    "{response:?}: provisional marker not retired: {key}"
                ));
            }
        }
    } else if persisted || after != before_completion {
        violations.push(format!("{response:?}, newer={replace}: incomplete/aborted response invented completion; before={before_completion:?}, after={after:?}"));
    }
    if operations.iter().any(|operation| {
        matches!(
            operation,
            KeepaliveOperation::Enqueue(_, _) | KeepaliveOperation::Observe(_, _)
        )
    }) {
        violations.push(format!("{response:?}, newer={replace}: completion must neither reschedule nor re-observe captured owners: {operations:?}"));
    }
    if after.generations != before_completion.generations {
        violations.push(format!(
            "{response:?}, newer={replace}: completion changed newer/unrelated active owners"
        ));
    }
    if replace
        && (after.cancellations != before_completion.cancellations
            || after.request_owners != before_completion.request_owners)
    {
        violations.push(format!(
            "{response:?}: old completion altered a newer owner's marker"
        ));
    }
    let mut terminal_reason = None;
    while let Ok(event) = events.try_recv() {
        if let cc_lb_lifecycle::LifecycleEvent::RequestTerminated { reason, .. } = event {
            terminal_reason = Some(reason);
        }
    }
    if matches!(
        response,
        CompletionFallbackResponse::ProviderErrorEnvelope
            | CompletionFallbackResponse::ProviderFailedEnvelope
            | CompletionFallbackResponse::ProviderErrorWithoutDetails
            | CompletionFallbackResponse::ProviderFailedWithoutDetails
            | CompletionFallbackResponse::HttpFailure
            | CompletionFallbackResponse::AbnormalStop
            | CompletionFallbackResponse::ContextWindowExceeded
            | CompletionFallbackResponse::SseRefusal
            | CompletionFallbackResponse::SseContextWindowExceeded
            | CompletionFallbackResponse::SseRefusalWithoutTerminal
            | CompletionFallbackResponse::SseContextWindowExceededWithoutTerminal
            | CompletionFallbackResponse::SseRefusalAbortBeforeTerminal
            | CompletionFallbackResponse::SseProviderError
            | CompletionFallbackResponse::SseProviderFailed
            | CompletionFallbackResponse::SseHttpErrorEof
            | CompletionFallbackResponse::SseHttpErrorPartialEof
            | CompletionFallbackResponse::SseHttpErrorTruncatedGzipEof
            | CompletionFallbackResponse::HttpErrorStreamBodyEof
            | CompletionFallbackResponse::SseProviderErrorIncomplete
            | CompletionFallbackResponse::SseRedirectTerminal
            | CompletionFallbackResponse::BufferedHookProviderError
            | CompletionFallbackResponse::BufferedHookFailure
            | CompletionFallbackResponse::BufferedCapFinalFrame
            | CompletionFallbackResponse::BufferedCapBeforeFinalFrame
    ) {
        eprintln!(
            "native terminal: {response:?}, newer={replace}, reason={terminal_reason:?}, operations={operations:?}"
        );
        assert!(
            terminal_reason.is_some(),
            "real lifecycle must emit its native termination"
        );
    }
    if matches!(response, CompletionFallbackResponse::SseTerminalOnly) {
        assert_eq!(
            terminal_reason,
            Some(cc_lb_lifecycle::TerminationReason::Success),
            "message_stop-only fixture must be admitted as semantic completion"
        );
    }
    if matches!(
        response,
        CompletionFallbackResponse::SseEmpty | CompletionFallbackResponse::SseAffinityRejected
    ) {
        assert_eq!(
            terminal_reason,
            Some(cc_lb_lifecycle::TerminationReason::ErrorCode(
                "upstream_stream_error".to_owned()
            )),
            "incomplete/affinity-aborted SSE is not semantic completion"
        );
    }
    violations
}

#[tokio::test]
async fn agent_completion_retires_captured_generic_subagent_marker() {
    for streaming in [false, true] {
        assert_agent_completion_resolves_generic_subagent(streaming, false).await;
    }
}

#[tokio::test]
async fn agent_completion_preserves_newer_generic_subagent_owner_without_reobserve() {
    for streaming in [false, true] {
        assert_agent_completion_resolves_generic_subagent(streaming, true).await;
    }
}

async fn assert_agent_completion_resolves_generic_subagent(streaming: bool, replace_sibling: bool) {
    let enqueuer = Arc::new(ScopedKeepaliveEnqueuer::default());
    let principal = principal_with_keepalive();
    let sibling_headers = [
        ("x-claude-code-session-id", "agent-completion-sibling"),
        ("x-claude-code-request-class", "subagent"),
    ];
    // Explicit subagent classification without an agent ID creates the generic
    // sibling. Renew it through another real eligible response before capture.
    for _ in 0..2 {
        establish_scoped_keepalive(&enqueuer, principal.clone(), &sibling_headers).await;
    }
    let before = enqueuer.snapshot();
    let (sibling_key, captured_owner) = before
        .generations
        .iter()
        .find(|(key, _)| key.ends_with("/thread:agent-completion-sibling/agent:subagent"))
        .expect("explicit subagent without agent ID establishes the generic sibling");
    let sibling_key = sibling_key.clone();
    let captured_owner = *captured_owner;
    let primary_key = sibling_key
        .strip_suffix("/agent:subagent")
        .expect("generic sibling suffix")
        .to_owned();
    let dispatch = Arc::new(PendingFollowupDispatch {
        streaming,
        body: agent_turn_body("msg_agent_completion_sibling", "Bash", 12_000),
        entered: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
        calls: AtomicUsize::new(0),
    });
    let lifecycle = lifecycle_with_parts(
        TestAuthn::with_principal_view(
            TestState::default(),
            Arc::new(PrincipalView::from_db(
                std::slice::from_ref(&principal),
                HashMap::new(),
            )),
        ),
        dispatch.clone(),
        LifecycleConfig::default(),
    )
    .with_cache_keepalive_enqueuer(enqueuer.clone());
    // Keep the thread header, but omit both request class and agent identity:
    // this is the plain main/unknown request that enables the sibling backstop.
    let request = http::Request::builder()
        .method(http::Method::POST)
        .uri("/v1/messages")
        .header("x-api-key", managed_api_key())
        .header("anthropic-version", "2023-06-01")
        .header("x-claude-code-session-id", "agent-completion-sibling")
        .body(standard_keepalive_request())
        .expect("plain followup request");
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("plain followup authenticates");
    let completion = async {
        let response = lifecycle
            .handle(request, &auth)
            .await
            .expect("plain followup handles");
        assert_keepalive_response_fidelity(response, &dispatch.body, streaming).await;
    };
    tokio::pin!(completion);
    tokio::select! {
        () = dispatch.entered.notified() => {}
        () = &mut completion => panic!("followup must remain pending until released"),
    }
    let pending = enqueuer.snapshot();
    assert_eq!(
        pending.cancellations.get(&sibling_key),
        Some(&(Some(captured_owner), CancelReason::NewRequest)),
        "request start must retain the captured sibling's provisional marker"
    );
    assert_eq!(
        pending.request_owners.get(&sibling_key),
        Some(&Some(captured_owner))
    );
    assert!(!pending.generations.contains_key(&sibling_key));
    let completion_operations_start;
    let replacement_owner = if replace_sibling {
        establish_scoped_keepalive(&enqueuer, principal, &sibling_headers).await;
        let replacement = enqueuer.snapshot();
        let owner = replacement.generations[&sibling_key];
        assert_ne!(owner, captured_owner);
        completion_operations_start = replacement.operations.len();
        Some(owner)
    } else {
        completion_operations_start = pending.operations.len();
        None
    };
    dispatch.release.notify_one();
    completion.await;
    enqueuer.wait_for_enqueue().await;
    let completed = enqueuer.snapshot();
    assert!(
        completed.generations.contains_key(&primary_key),
        "eligible AgentInTurn completion must enqueue the primary identity"
    );
    let completion_operations = &completed.operations[completion_operations_start..];
    assert!(
        !completion_operations.iter().any(
            |operation| matches!(operation, KeepaliveOperation::Observe(key, _) if key == &sibling_key)
        ),
        "completion must not re-observe the captured sibling"
    );
    if let Some(replacement_owner) = replacement_owner {
        assert_eq!(
            completed.generations.get(&sibling_key),
            Some(&replacement_owner),
            "an older primary completion must not cancel the newer sibling owner"
        );
        assert!(!completed.cancellations.contains_key(&sibling_key));
    } else {
        assert_eq!(
            completed.cancellations.get(&sibling_key),
            Some(&(Some(captured_owner), CancelReason::SessionIdentityChanged)),
            "a completed primary response must resolve the sibling marker to a real retirement reason, not NewRequest"
        );
        assert!(!completed.request_owners.contains_key(&sibling_key));
    }
    assert!(
        completion_operations.iter().any(|operation| matches!(
            operation,
            KeepaliveOperation::Cancel(key, Some(owner), CancelReason::SessionIdentityChanged)
                if key == &sibling_key && owner == &captured_owner
        )),
        "sibling retirement must use its captured start ticket even after a newer sibling takes over"
    );
}

#[tokio::test]
async fn stale_primary_agent_completion_retires_unchanged_captured_sibling() {
    for streaming in [false, true] {
        let enqueuer = Arc::new(ScopedKeepaliveEnqueuer::default());
        let principal = principal_with_keepalive();
        let session = "stale-primary-sibling";
        establish_scoped_keepalive(
            &enqueuer,
            principal.clone(),
            &[
                ("x-claude-code-session-id", session),
                ("x-claude-code-request-class", "subagent"),
            ],
        )
        .await;
        let established = enqueuer.snapshot();
        let (sibling_key, captured_owner) = established
            .generations
            .iter()
            .find(|(key, _)| key.ends_with("/thread:stale-primary-sibling/agent:subagent"))
            .expect("generic sibling established");
        let sibling_key = sibling_key.clone();
        let captured_owner = *captured_owner;
        let primary_key = sibling_key
            .strip_suffix("/agent:subagent")
            .expect("generic sibling suffix")
            .to_owned();
        let dispatch = Arc::new(OwnedRequestDispatch {
            calls: AtomicUsize::new(0),
            entered: std::array::from_fn(|_| tokio::sync::Notify::new()),
            release: std::array::from_fn(|_| tokio::sync::Notify::new()),
            old_agent_turn: true,
            streaming,
        });
        let lifecycle = lifecycle_with_parts(
            TestAuthn::with_principal_view(
                TestState::default(),
                Arc::new(PrincipalView::from_db(&[principal], HashMap::new())),
            ),
            dispatch.clone(),
            LifecycleConfig::default(),
        )
        .with_cache_keepalive_enqueuer(enqueuer.clone());
        let mut original = followup_request(session, false, true);
        original.headers_mut().remove("x-claude-code-request-class");
        let newer = followup_request(session, false, true);
        let original_auth = lifecycle
            .authenticate(original.headers())
            .await
            .expect("original plain request authenticates");
        let newer_auth = lifecycle
            .authenticate(newer.headers())
            .await
            .expect("newer explicit-main request authenticates");
        let original_completion = async {
            let response = lifecycle
                .handle(original, &original_auth)
                .await
                .expect("original plain response");
            assert_keepalive_response_fidelity(
                response,
                &agent_turn_body("msg_old_owner", "Bash", 12_000),
                streaming,
            )
            .await;
        };
        tokio::pin!(original_completion);
        tokio::select! {
            () = dispatch.entered[0].notified() => {}
            () = &mut original_completion => panic!("original response must wait for release"),
        }
        let pending = enqueuer.snapshot();
        assert_eq!(
            pending.cancellations.get(&sibling_key),
            Some(&(Some(captured_owner), CancelReason::NewRequest))
        );
        let newer_completion = async {
            let response = lifecycle
                .handle(newer, &newer_auth)
                .await
                .expect("newer explicit-main response");
            assert_keepalive_response_fidelity(
                response,
                &agent_turn_body("msg_current_owner", "Bash", 12_000),
                streaming,
            )
            .await;
        };
        tokio::pin!(newer_completion);
        tokio::select! {
            () = dispatch.entered[1].notified() => {}
            () = &mut newer_completion => panic!("newer response must wait for release"),
        }
        dispatch.release[1].notify_one();
        newer_completion.await;
        enqueuer.wait_for_enqueue().await;
        let superseded = enqueuer.snapshot();
        let newer_primary_owner = superseded.generations[&primary_key];
        assert_eq!(
            superseded.cancellations.get(&sibling_key),
            pending.cancellations.get(&sibling_key),
            "explicit-main request must supersede only the primary, not the captured sibling"
        );
        assert_eq!(
            superseded.request_owners.get(&sibling_key),
            Some(&Some(captured_owner))
        );
        let completion_operations_start = superseded.operations.len();
        dispatch.release[0].notify_one();
        original_completion.await;
        // Drain the detached completion on this current-thread runtime even
        // when stale-primary rejection makes no persistence call to notify.
        settle().await;
        let completed = enqueuer.snapshot();
        assert_eq!(
            completed.generations.get(&primary_key),
            Some(&newer_primary_owner),
            "stale primary completion must preserve the newer primary owner"
        );
        let completion_operations = &completed.operations[completion_operations_start..];
        assert!(
            !completion_operations.iter().any(
                |operation| matches!(operation, KeepaliveOperation::Observe(key, _) if key == &sibling_key)
            ),
            "retirement must not re-observe the already captured sibling"
        );
        assert_eq!(
            completed.cancellations.get(&sibling_key),
            Some(&(Some(captured_owner), CancelReason::SessionIdentityChanged)),
            "primary freshness must not gate retirement of its unchanged captured sibling"
        );
        assert!(!completed.request_owners.contains_key(&sibling_key));
        assert!(
            completion_operations.iter().any(|operation| matches!(
                operation,
                KeepaliveOperation::Cancel(key, Some(owner), CancelReason::SessionIdentityChanged)
                    if key == &sibling_key && owner == &captured_owner
            )),
            "stale primary completion must retire the sibling using its captured ticket"
        );
    }
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
            .map(|ticket| ticket.sequence)
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
                .is_some_and(|generation| generation.epoch == old_generation.epoch
                    && generation.sequence > old_generation.sequence),
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
        ) -> Result<Option<CacheKeepaliveCompletionTicket>, CacheKeepaliveEnqueueError> {
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
            .is_some_and(|generation| generation.epoch == old_generation.epoch
                && generation.sequence > old_generation.sequence)
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
    ) -> Result<Option<CacheKeepaliveCompletionTicket>, CacheKeepaliveEnqueueError> {
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
    owner: Option<CacheKeepaliveCompletionTicket>,
    updated_at: u64,
    now: u64,
    completions: Vec<Option<CacheKeepaliveCompletionTicket>>,
    observations: usize,
}

#[async_trait]
impl CacheKeepaliveEnqueuer for RequestOwnerEnqueuer {
    async fn observe_session_generation(
        &self,
        _key: &str,
    ) -> Result<Option<CacheKeepaliveCompletionTicket>, CacheKeepaliveEnqueueError> {
        let mut state = self.state.lock().expect("request owner");
        state.next_ticket += 1;
        state.observations += 1;
        Ok(Some(completion_ticket(state.next_ticket)))
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
            if request.expected_generation.map(|ticket| ticket.sequence)
                >= state.owner.map(|ticket| ticket.sequence)
            {
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
        if request.expected_generation.map(|ticket| ticket.sequence)
            >= state.owner.map(|ticket| ticket.sequence)
        {
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
