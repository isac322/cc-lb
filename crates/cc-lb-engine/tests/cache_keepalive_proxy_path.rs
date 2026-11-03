use crate::common;
#[path = "cache_keepalive_proxy_path/fixtures.rs"]
mod fixtures;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_control::api_keys::principal_view::PrincipalView;
use cc_lb_control::{DynamicViewBuilder, DynamicViewHolder};
use cc_lb_engine::LifecycleConfig;
use cc_lb_engine::cache_keepalive::{
    CacheKeepaliveCancelRequest, CacheKeepaliveEnqueueError, CacheKeepaliveEnqueueRequest,
    CacheKeepaliveEnqueuer, CacheKeepaliveNotTrackedRequest,
};
use cc_lb_storage_api::CacheTtl;
use http::{HeaderValue, StatusCode};
use serde_json::{Value, json};
use uuid::Uuid;

use common::{TestAuthn, TestState, collect_body, managed_key_id, messages_request};
use fixtures::{
    AgentTurnDispatch, FiveMinuteFinalBreakpointDialect, RecordingSignerFactory,
    enqueue_through_lifecycle, principal_with_keepalive, settle, upstream_record,
};

#[derive(Default)]
struct RecordingEnqueuer {
    enqueued: Mutex<Vec<CacheKeepaliveEnqueueRequest>>,
    cancelled: Mutex<Vec<CacheKeepaliveCancelRequest>>,
}

impl RecordingEnqueuer {
    fn enqueued(&self) -> Vec<CacheKeepaliveEnqueueRequest> {
        self.enqueued.lock().expect("enqueued lock").clone()
    }

    fn cancelled_count(&self) -> usize {
        self.cancelled.lock().expect("cancelled lock").len()
    }
}

#[async_trait]
impl CacheKeepaliveEnqueuer for RecordingEnqueuer {
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
}

#[tokio::test(start_paused = true)]
async fn lifecycle_enqueues_durable_keepalive_through_current_proxy_path() {
    let upstream_id =
        Uuid::parse_str("00000000-0000-0000-0000-0000000000aa").expect("upstream id parses");
    let upstream = upstream_record(upstream_id, "primary", "http://keepalive.local/");
    let principal_view = Arc::new(PrincipalView::from_db(
        &[principal_with_keepalive()],
        std::collections::HashMap::new(),
    ));
    let authn = TestAuthn::with_principal_view(TestState::default(), Arc::clone(&principal_view));
    let signer_log = Arc::new(Mutex::new(Vec::new()));
    let enqueuer = Arc::new(RecordingEnqueuer::default());
    let holder = Arc::new(DynamicViewHolder::new(
        DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(RecordingSignerFactory {
                label: "initial",
                calls: Arc::clone(&signer_log),
            }))
            .principal_view(Arc::clone(&principal_view))
            .upstream_records(vec![upstream.clone()])
            .build(),
    ));
    let lifecycle = cc_lb_engine::Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        Arc::clone(&holder),
        Arc::new(AgentTurnDispatch),
        LifecycleConfig::default(),
        Arc::new(cc_lb_clock::SystemClock),
    )
    .with_cache_keepalive_enqueuer(Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>);

    let mut request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","max_tokens":32,"tool_choice":{"type":"auto"},"system":[{"type":"text","text":"cached","cache_control":{"type":"ephemeral"}}],"messages":[{"role":"user","content":"hello"}]}"#,
    ));
    request.headers_mut().insert(
        "anthropic-beta",
        HeaderValue::from_static("prompt-caching-2024-07-31"),
    );

    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("request succeeds");
    let (status, _headers, _body) = collect_body(response).await;
    assert_eq!(status, StatusCode::OK);

    settle().await;

    let enqueued = enqueuer.enqueued();
    assert_eq!(enqueued.len(), 1);
    assert_eq!(enqueuer.cancelled_count(), 0);
    let enqueue = &enqueued[0];
    assert!(!enqueue.session_key_hash.is_empty());
    assert_eq!(enqueue.accounting_key_id.as_deref(), Some(managed_key_id()));
    let snapshot = &enqueue.snapshot;
    assert_eq!(snapshot.upstream_id, upstream_id);
    assert_eq!(snapshot.ttl, CacheTtl::Ttl5m);
    assert_eq!(
        snapshot.headers.get("anthropic-beta"),
        Some(&HeaderValue::from_static("prompt-caching-2024-07-31"))
    );
    assert!(snapshot.headers.get("x-api-key").is_none());
    assert!(snapshot.headers.get("authorization").is_none());
    let body: Value = serde_json::from_slice(&snapshot.body).expect("snapshot body is JSON");
    assert_eq!(body["system"][0]["cache_control"]["type"], "ephemeral");

    let calls = signer_log.lock().expect("signer log lock");
    assert!(calls.iter().any(|call| call.upstream_name == "primary"));
}

fn assert_keepalive_schedule(
    enqueue: &CacheKeepaliveEnqueueRequest,
    ttl: CacheTtl,
    refresh_delay_secs: u64,
) {
    assert_eq!(enqueue.snapshot.ttl, ttl);
    // The lifecycle deducts real response-body age, even with Tokio time paused.
    assert_eq!(
        enqueue.params.delay + enqueue.cache_anchor_age,
        Duration::from_secs(refresh_delay_secs),
    );
}

fn renewal_body(enqueue: &CacheKeepaliveEnqueueRequest) -> Value {
    let body = enqueue
        .snapshot
        .build_keepalive_body()
        .expect("keepalive body builds");
    let body: Value = serde_json::from_slice(&body).expect("keepalive body is JSON");
    assert_eq!(body["max_tokens"], 0);
    assert!(body.get("stream").is_none());
    body
}

#[tokio::test(start_paused = true)]
async fn mixed_ttl_keepalive_renews_full_prefix_on_last_breakpoint_ttl() {
    let request = json!({
        "model": "claude-test",
        "max_tokens": 32,
        "tool_choice": {"type": "auto"},
        "system": [{
            "type": "text",
            "text": "one-hour system prefix",
            "cache_control": {"type": "ephemeral", "ttl": "1h"}
        }],
        "messages": [{"role": "user", "content": [{
            "type": "text",
            "text": "five-minute cached suffix",
            "cache_control": {"type": "ephemeral", "ttl": "5m"}
        }]}]
    });

    let enqueue = enqueue_through_lifecycle(&request, None).await;
    let body = renewal_body(&enqueue);
    assert_eq!(body["system"], request["system"]);
    assert_eq!(body["messages"], request["messages"]);
    assert_keepalive_schedule(&enqueue, CacheTtl::Ttl5m, 270);
}

#[tokio::test(start_paused = true)]
async fn single_ttl_keepalive_preserves_one_hour_and_five_minute_cadences() {
    for (cache_control, ttl, delay_secs) in [
        (
            json!({"type": "ephemeral", "ttl": "1h"}),
            CacheTtl::Ttl1h,
            3300,
        ),
        (
            json!({"type": "ephemeral", "ttl": "5m"}),
            CacheTtl::Ttl5m,
            270,
        ),
        (json!({"type": "ephemeral"}), CacheTtl::Ttl5m, 270),
    ] {
        let request = json!({
            "model": "claude-test",
            "max_tokens": 32,
            "tool_choice": {"type": "auto"},
            "messages": [{"role": "user", "content": [{
                "type": "text",
                "text": "single cached prefix",
                "cache_control": cache_control
            }]}]
        });

        let enqueue = enqueue_through_lifecycle(&request, None).await;
        assert_eq!(renewal_body(&enqueue)["messages"], request["messages"]);
        assert_keepalive_schedule(&enqueue, ttl, delay_secs);
    }
}

#[tokio::test(start_paused = true)]
async fn multiple_breakpoints_keep_tools_system_and_final_message() {
    let request = json!({
        "model": "claude-test",
        "max_tokens": 32,
        "tool_choice": {"type": "auto"},
        "tools": [{
            "name": "Bash",
            "description": "Run a command",
            "input_schema": {"type": "object", "properties": {}},
            "cache_control": {"type": "ephemeral", "ttl": "1h"}
        }],
        "system": [{
            "type": "text",
            "text": "one-hour system prefix",
            "cache_control": {"type": "ephemeral", "ttl": "1h"}
        }],
        "messages": [{"role": "user", "content": [{
            "type": "text",
            "text": "five-minute final prefix",
            "cache_control": {"type": "ephemeral", "ttl": "5m"}
        }]}]
    });

    let enqueue = enqueue_through_lifecycle(&request, None).await;
    let body = renewal_body(&enqueue);
    assert_eq!(body["tools"], request["tools"]);
    assert_eq!(body["system"], request["system"]);
    assert_eq!(body["messages"], request["messages"]);
    assert_keepalive_schedule(&enqueue, CacheTtl::Ttl5m, 270);
}

#[tokio::test(start_paused = true)]
async fn automatic_final_breakpoint_renews_explicit_one_hour_prefix() {
    let request = json!({
        "model": "claude-test",
        "max_tokens": 32,
        "tool_choice": {"type": "auto"},
        "cache_control": {"type": "ephemeral", "ttl": "5m"},
        "system": [{
            "type": "text",
            "text": "explicit one-hour system",
            "cache_control": {"type": "ephemeral", "ttl": "1h"}
        }],
        "messages": [{"role": "user", "content": "automatically cached five-minute suffix"}]
    });

    let enqueue = enqueue_through_lifecycle(&request, None).await;
    let body = renewal_body(&enqueue);
    assert_eq!(body["cache_control"], request["cache_control"]);
    assert_eq!(body["system"], request["system"]);
    assert_eq!(body["messages"], request["messages"]);
    assert_keepalive_schedule(&enqueue, CacheTtl::Ttl5m, 270);
}

#[tokio::test(start_paused = true)]
async fn mixed_ttl_keepalive_preserves_tool_use_and_required_tool_result() {
    let request = json!({
        "model": "claude-test",
        "max_tokens": 32,
        "tool_choice": {"type": "auto"},
        "tools": [{
            "name": "Bash",
            "input_schema": {"type": "object", "properties": {}}
        }],
        "system": [{
            "type": "text",
            "text": "one-hour agent instructions",
            "cache_control": {"type": "ephemeral", "ttl": "1h"}
        }],
        "messages": [
            {"role": "user", "content": "Check the current directory"},
            {"role": "assistant", "content": [{
                "type": "tool_use",
                "id": "toolu_previous",
                "name": "Bash",
                "input": {"command": "pwd"},
                "cache_control": {"type": "ephemeral", "ttl": "5m"}
            }]},
            {"role": "user", "content": [{
                "type": "tool_result",
                "tool_use_id": "toolu_previous",
                "content": "/workspace"
            }]}
        ]
    });

    let enqueue = enqueue_through_lifecycle(&request, None).await;
    let body = renewal_body(&enqueue);
    assert_eq!(body["tools"], request["tools"]);
    assert_eq!(body["system"], request["system"]);
    assert_eq!(body["messages"], request["messages"]);
    assert_keepalive_schedule(&enqueue, CacheTtl::Ttl5m, 270);
}

#[tokio::test(start_paused = true)]
async fn shaped_final_breakpoint_ttl_controls_keepalive_schedule() {
    let request = json!({
        "model": "claude-test",
        "max_tokens": 32,
        "tool_choice": {"type": "auto"},
        "system": [{
            "type": "text",
            "text": "unchanged one-hour system prefix",
            "cache_control": {"type": "ephemeral", "ttl": "1h"}
        }],
        "messages": [{"role": "user", "content": [{
            "type": "text",
            "text": "suffix rewritten by the upstream dialect",
            "cache_control": {"type": "ephemeral", "ttl": "1h"}
        }]}]
    });

    let enqueue =
        enqueue_through_lifecycle(&request, Some(Arc::new(FiveMinuteFinalBreakpointDialect))).await;
    let captured: Value =
        serde_json::from_slice(&enqueue.snapshot.body).expect("captured upstream-shaped JSON");
    assert_eq!(captured["system"], request["system"]);
    assert_eq!(
        captured["messages"][0]["content"][0]["text"],
        request["messages"][0]["content"][0]["text"],
    );
    assert_eq!(
        captured["messages"][0]["content"][0]["cache_control"]["ttl"],
        "5m",
    );
    let body = renewal_body(&enqueue);
    assert_eq!(body["system"], captured["system"]);
    assert_eq!(body["messages"], captured["messages"]);
    assert_keepalive_schedule(&enqueue, CacheTtl::Ttl5m, 270);
}
