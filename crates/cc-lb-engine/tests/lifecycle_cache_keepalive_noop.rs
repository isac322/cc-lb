mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_engine::api_keys::principal_view::PrincipalView;
use cc_lb_engine::cache_keepalive::{
    CacheKeepaliveCancelRequest, CacheKeepaliveEnqueueError, CacheKeepaliveEnqueueRequest,
    CacheKeepaliveEnqueuer,
};
use cc_lb_engine::{DispatchError, LifecycleConfig, UpstreamDispatch};
use cc_lb_plugin_api::SignedRequest;
use cc_lb_storage_api::principal::{Limit, PrincipalKind, PrincipalRecord};
use cc_lb_storage_api::{CacheKeepaliveConfig, ClassifierConfig};
use http::{Response, StatusCode};
use uuid::Uuid;

use common::{RecordingHook, TestAuthn, TestRouter, TestState, collect_body, lifecycle_with_parts};

#[tokio::test]
async fn cache_keepalive_without_scheduler_is_response_noop() {
    let upstream_body = Bytes::from_static(
        br#"{"type":"message","id":"msg_keepalive_noop","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":10,"output_tokens":2}}"#,
    );
    let state = TestState::default();
    let lifecycle = lifecycle_with_parts(
        TestAuthn::new(state),
        Arc::new(TestRouter {
            base_url: "http://upstream.local/".parse().expect("test URL parses"),
        }),
        Arc::new(FixedSuccessDispatch {
            body: upstream_body.clone(),
        }),
        vec![Arc::new(RecordingHook::default())],
        LifecycleConfig::default(),
    );

    let request = http::Request::builder()
        .method(http::Method::POST)
        .uri("/v1/messages")
        .header("x-api-key", "sk-ant-downstream")
        .header("anthropic-version", "2023-06-01")
        .body(Bytes::from_static(
            br#"{"model":"claude-test","max_tokens":32,"system":[{"type":"text","text":"cached","cache_control":{"type":"ephemeral","ttl":"5m"}}],"messages":[{"role":"user","content":"hello"}]}"#,
        ))
        .expect("test request builds");

    let response = lifecycle
        .handle(request)
        .await
        .expect("lifecycle handles request without keepalive scheduler");

    let (status, _headers, body) = collect_body(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, upstream_body);
}

#[tokio::test]
async fn cache_keepalive_enqueue_failure_does_not_change_proxy_response() {
    let upstream_body = Bytes::from_static(
        br#"{"type":"message","id":"msg_keepalive_enqueue_fail","content":[{"type":"tool_use","id":"toolu_1","name":"Bash","input":{}}],"stop_reason":"tool_use","usage":{"input_tokens":10,"output_tokens":2}}"#,
    );
    let state = TestState::default();
    let principal_view = Arc::new(PrincipalView::from_db(
        &[principal_with_keepalive()],
        std::collections::HashMap::new(),
    ));
    let authn = TestAuthn::with_principal_view(state, principal_view);
    let calls = Arc::new(AtomicUsize::new(0));
    let lifecycle = lifecycle_with_parts(
        authn,
        Arc::new(TestRouter {
            base_url: "http://upstream.local/".parse().expect("test URL parses"),
        }),
        Arc::new(FixedSuccessDispatch {
            body: upstream_body.clone(),
        }),
        vec![Arc::new(RecordingHook::default())],
        LifecycleConfig::default(),
    )
    .with_cache_keepalive_enqueuer(Arc::new(FailingEnqueuer {
        calls: Arc::clone(&calls),
    }));

    let request = http::Request::builder()
        .method(http::Method::POST)
        .uri("/v1/messages")
        .header("x-api-key", "sk-ant-downstream")
        .header("x-session-id", "session-1")
        .header("anthropic-version", "2023-06-01")
        .body(Bytes::from_static(
            br#"{"model":"claude-test","max_tokens":32,"system":[{"type":"text","text":"cached","cache_control":{"type":"ephemeral","ttl":"5m"}}],"messages":[{"role":"user","content":"hello"}]}"#,
        ))
        .expect("test request builds");

    let response = lifecycle
        .handle(request)
        .await
        .expect("lifecycle handles request despite keepalive enqueue failure");

    let (status, _headers, body) = collect_body(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, upstream_body);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

struct FixedSuccessDispatch {
    body: Bytes,
}

struct FailingEnqueuer {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl CacheKeepaliveEnqueuer for FailingEnqueuer {
    async fn enqueue_cache_keepalive(
        &self,
        _request: CacheKeepaliveEnqueueRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Err(CacheKeepaliveEnqueueError(
            "forced enqueue failure".to_owned(),
        ))
    }

    async fn cancel_cache_keepalive(
        &self,
        _request: CacheKeepaliveCancelRequest,
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

fn principal_with_keepalive() -> PrincipalRecord {
    PrincipalRecord {
        id: Uuid::new_v4(),
        name: "principal-test".to_owned(),
        kind: PrincipalKind::Machine,
        allowed_models: vec!["*".to_owned()],
        allowed_upstreams: Vec::new(),
        default_limits: Vec::<Limit>::new(),
        enabled: true,
        last_apply_error: None,
        last_apply_at_unix_secs: None,
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
            classifier: ClassifierConfig::default(),
        }),
    }
}
