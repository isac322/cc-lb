mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_core::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_core::api_keys::limit_engine::LimitEngine;
use cc_lb_core::api_keys::principal_view::{
    DialectCache, ObservabilityHooksCache, PrincipalView, RouterPipelineCache,
};
use cc_lb_core::{
    DispatchError, DynamicViewBuilder, DynamicViewHolder, ErrorNormalizer, Lifecycle,
    LifecycleConfig, UpstreamDispatch,
};
use cc_lb_plugin_api::{
    DialectError, InternalErrorKind, InternalErrorStage, Principal, RequestContext, RouteDecision,
    RouteError, RouterPlugin, ShapedRequest, ShapedRequestBuilder, SignedRequest, TerminalStrategy,
    Upstream, UpstreamCandidate, UpstreamDialect,
};
use cc_lb_storage_api::types::{KeyStatus, StoredApiKeyRecord};
use cc_lb_storage_api::upstream::{UpstreamKind as StorageUpstreamKind, UpstreamRecord};
use cc_lb_storage_api::{RequestEventStore, Storage as StorageTrait};
use cc_lb_storage_redb::Storage as RedbStorage;
use http::{Response, StatusCode};
use serde_json::{Value, json};
use url::Url;
use uuid::Uuid;

use common::{TestAuthn, TestState, collect_body, messages_request};

#[tokio::test]
async fn shape_error_falls_back_to_raw_passthrough_and_records_internal_error()
-> Result<(), Box<dyn std::error::Error>> {
    let upstream_id = Uuid::from_u128(1);
    let state = TestState::default();
    let dispatch = Arc::new(RecordingDispatch::default());
    let principal_view = principal_view_with_failing_shape();
    let authn = TestAuthn::with_principal_view(state.clone(), principal_view.clone());
    let _dir = tempfile::tempdir()?;
    let storage = Arc::new(RedbStorage::open(
        &_dir.path().join("lifecycle-shape-passthrough.redb"),
        [19; 32],
    )?);
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .global_router(Arc::new(UnusedRouter))
        .dispatcher(dispatch.clone())
        .global_observability_hooks(Vec::new())
        .error_normalizer(Arc::new(ErrorNormalizer::new()))
        .principal_view(principal_view)
        .upstream_records(vec![upstream_record(upstream_id)])
        .build();
    let lifecycle = Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        Arc::new(DynamicViewHolder::new(view)),
        LifecycleConfig::default(),
    )
    .with_request_event_storage(Arc::clone(&storage) as Arc<dyn StorageTrait>)
    .with_static_limit_subject(
        LimitEngine::new(Arc::new(KeyConcurrencyManager::new())),
        "principal-test".to_owned(),
        "key-test".to_owned(),
        active_record(),
    );

    let request_body =
        Bytes::from_static(br#"{"model":"claude-test","messages":[],"max_tokens":16}"#);
    let response = lifecycle
        .handle(messages_request(request_body.clone()))
        .await?;
    let (status, _headers, body) = collect_body(response).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body_json(&body)["type"], "message");
    {
        let dispatched = dispatch.requests.lock().expect("dispatch lock");
        assert_eq!(dispatched.len(), 1);
        assert_eq!(dispatched[0].url, "http://raw-upstream.local/v1/messages");
        assert_eq!(dispatched[0].body, request_body);
    }

    let events = RequestEventStore::query_request_events(storage.as_ref(), 0, u64::MAX, 10).await?;
    assert_eq!(events.len(), 1);
    let error = events[0]
        .internal_errors
        .iter()
        .find(|error| error.stage == InternalErrorStage::Shape)
        .expect("shape internal error recorded");
    assert_eq!(error.kind, InternalErrorKind::Trap);
    let message = error.message.as_deref().expect("message recorded");
    assert!(message.contains("unsupported request: plugin shape failed"));
    assert!(message.contains("[REDACTED]"));
    assert!(!message.contains("sk-ant-shape-secret"));
    assert!(!message.contains("secret-token"));
    Ok(())
}

fn body_json(body: &Bytes) -> Value {
    serde_json::from_slice(body).expect("response body is json")
}

fn upstream_record(id: Uuid) -> UpstreamRecord {
    UpstreamRecord {
        id,
        name: "raw-upstream".to_owned(),
        kind: StorageUpstreamKind::AnthropicApiKey,
        base_url: Some(Url::parse("http://raw-upstream.local/").expect("test URL parses")),
        enabled: true,
        oauth_credentials: None,
        api_key_ciphertext: Some(Vec::new()),
        refresh_lease_holder: None,
        refresh_lease_until_unix_secs: None,
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs: None,
        revision: 1,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
    }
}

fn active_record() -> StoredApiKeyRecord {
    StoredApiKeyRecord {
        key_hash_b64: "key-test".to_owned(),
        status: KeyStatus::Active,
        ..StoredApiKeyRecord::default()
    }
}

fn principal_view_with_failing_shape() -> Arc<PrincipalView> {
    let dialect: Arc<dyn UpstreamDialect> = Arc::new(FailingShapeDialect);
    let mut chains = HashMap::new();
    chains.insert(
        "principal-test".to_owned(),
        (
            Some(Arc::new(RouterPipelineCache::empty(
                TerminalStrategy::FirstPick,
            ))),
            ObservabilityHooksCache::Inherit,
            DialectCache::Explicit(dialect),
        ),
    );
    Arc::new(PrincipalView::for_tests(
        "principal-test",
        true,
        vec!["*".to_owned()],
        Vec::new(),
        chains,
    ))
}

struct UnusedRouter;

impl RouterPlugin for UnusedRouter {
    fn route(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError> {
        panic!("terminal strategy selects upstream before legacy router")
    }
}

struct FailingShapeDialect;

impl UpstreamDialect for FailingShapeDialect {
    fn shape(
        &self,
        _ctx: &RequestContext,
        _upstream: &Upstream,
        _principal: &Principal,
        _builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        Err(DialectError::UnsupportedRequest {
            reason: "plugin shape failed: x-api-key: sk-ant-shape-secret token=secret-token"
                .to_owned(),
        })
    }

    fn normalize_error(&self, _status: StatusCode, _body: &Bytes) -> Option<Bytes> {
        None
    }
}

#[derive(Default)]
struct RecordingDispatch {
    requests: Mutex<Vec<DispatchedRequest>>,
}

struct DispatchedRequest {
    url: String,
    body: Bytes,
}

#[async_trait]
impl UpstreamDispatch for RecordingDispatch {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        self.requests
            .lock()
            .expect("dispatch lock")
            .push(DispatchedRequest {
                url: request.url().to_string(),
                body: request.body().clone(),
            });
        let mut response = Response::new(Body::from(Bytes::from(
            json!({"type":"message","usage":{"input_tokens":1,"output_tokens":1}}).to_string(),
        )));
        *response.status_mut() = StatusCode::OK;
        Ok(response)
    }
}
