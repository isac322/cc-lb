use crate::common;
#[path = "cache_keepalive_proxy_path/fixtures.rs"]
mod fixtures;

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_control::api_keys::principal_view::PrincipalView;
use cc_lb_control::{DynamicViewBuilder, DynamicViewHolder};
use cc_lb_engine::cache_keepalive::{
    CacheKeepaliveCancelRequest, CacheKeepaliveEnqueueError, CacheKeepaliveEnqueueRequest,
    CacheKeepaliveEnqueuer, CacheKeepaliveNotTrackedRequest,
};
use cc_lb_engine::{Lifecycle, LifecycleConfig};
use cc_lb_storage_api::upstream::UpstreamRecord;
use cc_lb_storage_api::CacheTtl;
use http::{HeaderValue, StatusCode};
use serde_json::Value;
use uuid::Uuid;

use common::{TestAuthn, TestState, collect_body, managed_key_id, messages_request};
use fixtures::{
    AgentTurnDispatch, RecordingSignerFactory, principal_with_keepalive, settle, upstream_record,
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

fn lifecycle_for(
    principal: cc_lb_storage_api::principal::PrincipalRecord,
    upstream: UpstreamRecord,
    enqueuer: Arc<RecordingEnqueuer>,
) -> Lifecycle {
    let principal_view = Arc::new(PrincipalView::from_db(
        &[principal],
        std::collections::HashMap::new(),
    ));
    let authn = TestAuthn::with_principal_view(TestState::default(), principal_view.clone());
    let holder = Arc::new(DynamicViewHolder::new(
        DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(RecordingSignerFactory {
                label: "test",
                calls: Arc::new(Mutex::new(Vec::new())),
            }))
            .principal_view(principal_view)
            .upstream_records(vec![upstream])
            .build(),
    ));
    Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        holder,
        Arc::new(AgentTurnDispatch),
        LifecycleConfig::default(),
        Arc::new(cc_lb_clock::SystemClock),
    )
    .with_cache_keepalive_enqueuer(enqueuer as Arc<dyn CacheKeepaliveEnqueuer>)
}

fn upstream_for_route(id: Uuid, name: &str, base_url: Option<&str>) -> UpstreamRecord {
    let mut upstream =
        upstream_record(id, name, base_url.unwrap_or("https://api.anthropic.com/"));
    if base_url.is_none() {
        upstream.base_url = None;
    }
    upstream
}

fn request_with_billing_header(
    suffix: &str,
    thread_id: Option<&str>,
    include_stable_breakpoint: bool,
) -> http::Request<Bytes> {
    let mut system = vec![serde_json::json!({
        "type": "text",
        "text": format!("x-anthropic-billing-header: {suffix}"),
        "cache_control": {"type": "ephemeral", "ttl": "1h"}
    })];
    if include_stable_breakpoint {
        system.push(serde_json::json!({
            "type": "text",
            "text": "stable",
            "cache_control": {"type": "ephemeral"}
        }));
    }
    let body = serde_json::to_vec(&serde_json::json!({
        "model": "claude-sonnet-4-5",
        "max_tokens": 32,
        "tool_choice": {"type": "auto"},
        "tools": [{
            "name": "Bash",
            "description": "stable cached tool context ".repeat(2_000),
            "input_schema": {"type": "object", "properties": {}}
        }],
        "system": system,
        "messages": [{"role": "user", "content": "hello"}]
    }))
    .expect("request JSON serializes");
    let mut request = messages_request(Bytes::from(body));
    if let Some(thread_id) = thread_id {
        request.headers_mut().insert(
            "x-session-id",
            HeaderValue::from_str(thread_id).expect("thread header"),
        );
    }
    request
}

async fn complete_proxy_request(lifecycle: &Lifecycle, request: http::Request<Bytes>) {
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

#[tokio::test(start_paused = true)]
async fn threadless_keepalive_identity_uses_the_serving_cache_bundle() {
    let principal = principal_with_keepalive();

    let official_enqueuer = Arc::new(RecordingEnqueuer::default());
    let official = lifecycle_for(
        principal.clone(),
        upstream_for_route(Uuid::from_u128(0x1004), "official", None),
        Arc::clone(&official_enqueuer),
    );
    for suffix in [
        "cc_version=2.1.312; cc_entrypoint=sdk-cli;",
        "cc_version=2.1.312; cc_entrypoint=sdk-cli; cc_workload=cron;",
    ] {
        complete_proxy_request(
            &official,
            request_with_billing_header(suffix, None, true),
        )
        .await;
    }
    let official_enqueued = official_enqueuer.enqueued();
    assert_eq!(official_enqueued.len(), 2);
    assert_eq!(
        official_enqueued[0].session_key_hash,
        official_enqueued[1].session_key_hash,
        "official threadless identity must ignore billing-header churn"
    );
    assert!(
        official_enqueued
            .iter()
            .all(|request| request.snapshot.ttl == CacheTtl::Ttl5m)
    );

    let custom_enqueuer = Arc::new(RecordingEnqueuer::default());
    let custom = lifecycle_for(
        principal,
        upstream_for_route(
            Uuid::from_u128(0x1005),
            "custom",
            Some("https://proxy.example.com/"),
        ),
        Arc::clone(&custom_enqueuer),
    );
    for suffix in [
        "cc_version=2.1.312; cc_entrypoint=sdk-cli;",
        "cc_version=2.1.312; cc_entrypoint=sdk-cli; cc_workload=cron;",
    ] {
        complete_proxy_request(
            &custom,
            request_with_billing_header(suffix, None, true),
        )
        .await;
    }
    let custom_enqueued = custom_enqueuer.enqueued();
    assert_eq!(custom_enqueued.len(), 2);
    assert_ne!(
        custom_enqueued[0].session_key_hash,
        custom_enqueued[1].session_key_hash,
        "custom threadless identity must retain billing-header churn"
    );
    assert!(
        custom_enqueued
            .iter()
            .all(|request| request.session_key_hash.contains("/prefix:"))
    );
    assert!(
        custom_enqueued
            .iter()
            .all(|request| request.snapshot.ttl == CacheTtl::Ttl1h),
        "custom literal completion must retain the serving breakpoint TTL"
    );
}

#[tokio::test(start_paused = true)]
async fn official_header_only_completion_is_not_tracked_but_custom_literal_is() {
    let principal = principal_with_keepalive();

    let official_enqueuer = Arc::new(RecordingEnqueuer::default());
    let official = lifecycle_for(
        principal.clone(),
        upstream_for_route(
            Uuid::from_u128(0x1006),
            "official-header-only",
            None,
        ),
        Arc::clone(&official_enqueuer),
    );
    complete_proxy_request(
        &official,
        request_with_billing_header(
            "cc_version=2.1.312; cc_entrypoint=sdk-cli; cc_workload=cron;",
            None,
            false,
        ),
    )
    .await;
    assert!(
        official_enqueuer.enqueued().is_empty(),
        "an official billing-header block alone has zero effective breakpoints"
    );

    let custom_enqueuer = Arc::new(RecordingEnqueuer::default());
    let custom = lifecycle_for(
        principal,
        upstream_for_route(
            Uuid::from_u128(0x1007),
            "custom-header-only",
            Some("https://proxy.example.com/"),
        ),
        Arc::clone(&custom_enqueuer),
    );
    complete_proxy_request(
        &custom,
        request_with_billing_header(
            "cc_version=2.1.312; cc_entrypoint=sdk-cli; cc_workload=cron;",
            None,
            false,
        ),
    )
    .await;
    let enqueued = custom_enqueuer.enqueued();
    assert_eq!(enqueued.len(), 1);
    assert!(enqueued[0].session_key_hash.contains("/prefix:"));
    assert_eq!(enqueued[0].snapshot.ttl, CacheTtl::Ttl1h);
    let snapshot_body: Value =
        serde_json::from_slice(&enqueued[0].snapshot.body).expect("snapshot body is JSON");
    assert_eq!(
        snapshot_body["system"][0]["text"],
        "x-anthropic-billing-header: cc_version=2.1.312; cc_entrypoint=sdk-cli; cc_workload=cron;"
    );
}

#[tokio::test(start_paused = true)]
async fn explicit_thread_id_controls_identity_across_prefix_and_route_changes() {
    let principal = principal_with_keepalive();
    let mut keys = Vec::new();

    for (upstream_id, name, base_url) in [
        (Uuid::from_u128(0x1008), "official-threaded", None),
        (
            Uuid::from_u128(0x1009),
            "custom-threaded",
            Some("https://proxy.example.com/"),
        ),
    ] {
        let enqueuer = Arc::new(RecordingEnqueuer::default());
        let lifecycle = lifecycle_for(
            principal.clone(),
            upstream_for_route(upstream_id, name, base_url),
            Arc::clone(&enqueuer),
        );
        for suffix in [
            "cc_version=2.1.312; cc_entrypoint=sdk-cli;",
            "cc_version=2.1.312; cc_entrypoint=sdk-cli; cc_workload=cron;",
        ] {
            complete_proxy_request(
                &lifecycle,
                request_with_billing_header(suffix, Some("thread-stable"), true),
            )
            .await;
        }
        let enqueued = enqueuer.enqueued();
        assert_eq!(enqueued.len(), 2);
        assert!(
            enqueued
                .iter()
                .all(|request| request.session_key_hash.ends_with("/thread:thread-stable"))
        );
        assert_eq!(enqueued[0].session_key_hash, enqueued[1].session_key_hash);
        keys.push(enqueued[0].session_key_hash.clone());
    }

    assert_eq!(
        keys[0], keys[1],
        "explicit thread identity must not depend on serving route policy"
    );
}
