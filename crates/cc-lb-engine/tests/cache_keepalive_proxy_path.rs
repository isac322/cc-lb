use crate::common;
#[path = "cache_keepalive_proxy_path/fixtures.rs"]
mod fixtures;

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_engine::api_keys::principal_view::PrincipalView;
use cc_lb_engine::cache_keepalive::{
    CacheKeepaliveCancelRequest, CacheKeepaliveEnqueueError, CacheKeepaliveEnqueueRequest,
    CacheKeepaliveEnqueuer, CacheKeepaliveNotTrackedRequest,
};
use cc_lb_engine::{DynamicViewBuilder, DynamicViewHolder, LifecycleConfig};
use cc_lb_storage_api::CacheTtl;
use http::{HeaderValue, StatusCode};
use serde_json::Value;
use uuid::Uuid;

use common::{RecordingHook, TestAuthn, TestState, collect_body, messages_request};
use fixtures::{
    AgentTurnDispatch, FirstRouter, RecordingSignerFactory, principal_with_keepalive, settle,
    upstream_record,
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
            .global_router(Arc::new(FirstRouter))
            .global_observability_hooks(vec![Arc::new(RecordingHook::default())])
            .principal_view(Arc::clone(&principal_view))
            .upstream_records(vec![upstream.clone()])
            .build(),
    ));
    let lifecycle = cc_lb_engine::Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        Arc::clone(&holder),
        Arc::new(AgentTurnDispatch),
        LifecycleConfig::default(),
        Arc::new(cc_lb_engine::SystemClock),
    )
    .with_cache_keepalive_enqueuer(Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>);

    let mut request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","max_tokens":32,"tool_choice":{"type":"any"},"system":[{"type":"text","text":"cached","cache_control":{"type":"ephemeral"}}],"messages":[{"role":"user","content":"hello"}]}"#,
    ));
    request
        .headers_mut()
        .insert("x-api-key", HeaderValue::from_static("sk-cclb-downstream"));
    request.headers_mut().insert(
        "anthropic-beta",
        HeaderValue::from_static("prompt-caching-2024-07-31"),
    );

    let response = lifecycle.handle(request).await.expect("request succeeds");
    let (status, _headers, _body) = collect_body(response).await;
    assert_eq!(status, StatusCode::OK);

    settle().await;

    let enqueued = enqueuer.enqueued();
    assert_eq!(enqueued.len(), 1);
    assert_eq!(enqueuer.cancelled_count(), 0);
    let enqueue = &enqueued[0];
    assert!(!enqueue.session_key_hash.is_empty());
    assert_eq!(enqueue.accounting_key_id.as_deref(), Some("none-mode"));
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
