use crate::common;

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
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn cache_keepalive_ambiguous_cancels_durable_without_scheduler() {
    let calls = Arc::new(AtomicUsize::new(0));
    let response = handle_keepalive_response(
        Bytes::from_static(
            br#"{"type":"message","id":"msg_ambiguous","content":[{"type":"text","text":"partial"}],"stop_reason":"max_tokens","usage":{"input_tokens":10,"output_tokens":2}}"#,
        ),
        Arc::new(RecordingCancelEnqueuer {
            calls: Arc::clone(&calls),
        }),
    )
    .await;

    let (status, _headers, body) = collect_body(response).await;
    settle().await;
    assert_eq!(status, StatusCode::OK);
    assert!(String::from_utf8_lossy(&body).contains("msg_ambiguous"));
    assert_eq!(calls.load(Ordering::Relaxed), 1);
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
    assert_eq!(calls.load(Ordering::Relaxed), 1);
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
    assert_eq!(keys.len(), 1, "auxiliary request must not cancel");
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
    assert!(enqueuer.cancelled().is_empty());
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
        assert_eq!(enqueuer.cancelled().len(), 1);
        assert_eq!(
            enqueuer.cancelled()[0].reason,
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
    assert_eq!(enqueuer.cancelled().len(), 1);
    assert!(
        enqueuer.cancelled()[0]
            .session_key_hash
            .ends_with("/thread:S/agent:agent-sub-1")
    );
    assert!(
        !enqueuer.cancelled()[0]
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
    assert_eq!(enqueuer.cancelled().len(), 2);
    assert!(
        enqueuer.cancelled()[1]
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
        enqueuer.cancelled()[0].reason,
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
            enqueuer.cancelled()[0].reason,
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
                enqueuer.cancelled()[0].reason.as_str(),
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
        assert!(enqueuer.cancelled().is_empty());
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
        self.cancelled.lock().expect("cancelled lock").push(request);
        self.completed.notify_one();
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
