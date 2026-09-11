use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use axum::body::Bytes;
use cc_lb_config::{DownstreamAuthMode, NoneModeConfig, NoneModeUpstreamKind};
use cc_lb_domain::{Principal, Upstream, UpstreamCandidate};
use cc_lb_engine::api_keys::builtin_authn::BuiltinAuthn;
use cc_lb_engine::api_keys::principal_view::PrincipalView;
use cc_lb_engine::{
    ApiKeyAwareSignerFactory, Body, DispatchError, DynamicViewBuilder, DynamicViewHolder,
    InMemoryBus, Lifecycle, LifecycleConfig, UpstreamDispatch,
};
use cc_lb_observability::NoopMetricsHook;
use cc_lb_routing::{RouteDecision, RouteError, RouterPlugin, RoutingContext};
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamRecord};
use cc_lb_testkit::{InMemoryStorage, fixed_clock};
use cc_lb_upstream::{
    DialectError, DialectShapeContext, RetryDecision, ShapedRequest, ShapedRequestBuilder,
    SignedRequest, Signer, SignerError, SignerFactory, SigningCapability, UpstreamDialect,
    UpstreamError,
};
use http::header::{CONTENT_TYPE, HeaderValue};
use http::{Method, Request, Response, StatusCode};
use http_body_util::BodyExt;
use serde_json::json;
use url::Url;
use uuid::Uuid;

const MODEL: &str = "claude-sonnet-4-5-20250929";
const NOW_UNIX_SECS: u64 = 1_800_000_000;
const UPSTREAM_ID: Uuid = Uuid::from_u128(1);

#[tokio::test]
async fn t2__thinking_enabled_and_priority_tier_are_persisted_after_proxy_request() {
    let request = json!({
        "model": MODEL,
        "max_tokens": 16,
        "messages": [{"role": "user", "content": "Reply with exactly: pong"}],
        "thinking": {"type": "enabled", "budget_tokens": 18000},
        "output_config": {"effort": "max"}
    });

    let (status, request_count, event) = proxy_and_persist(request, "priority").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(request_count, 1, "stub upstream received request");
    assert_eq!(event.thinking_budget_tokens, Some(18_000));
    assert_eq!(event.reasoning_effort.as_deref(), Some("max"));
    assert_eq!(event.service_tier.as_deref(), Some("priority"));
}

#[tokio::test]
async fn t2__absent_thinking_and_standard_tier_are_persisted_after_proxy_request() {
    let request = json!({
        "model": MODEL,
        "max_tokens": 16,
        "messages": [{"role": "user", "content": "Reply with exactly: pong"}]
    });

    let (status, request_count, event) = proxy_and_persist(request, "standard").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(request_count, 1, "stub upstream received request");
    assert_eq!(event.thinking_budget_tokens, None);
    assert_eq!(event.reasoning_effort, None);
    assert_eq!(event.service_tier.as_deref(), Some("standard"));
}

async fn proxy_and_persist(
    request_json: serde_json::Value,
    response_service_tier: &'static str,
) -> (StatusCode, usize, cc_lb_storage_api::RequestEvent) {
    let clock = fixed_clock(NOW_UNIX_SECS);
    let storage = Arc::new(InMemoryStorage::with_clock(clock.clone()));
    let bus = Arc::new(InMemoryBus::new());
    let assembler_rx =
        bus.attach_lifecycle_assembler(cc_lb_engine::DEFAULT_LIFECYCLE_ASSEMBLER_CAPACITY);
    let request_event_bus: Arc<dyn cc_lb_control::RequestEventBus> = bus.clone();
    let _assembler = cc_lb_engine::spawn_request_event_assembler(
        assembler_rx,
        storage.clone() as Arc<dyn cc_lb_storage_api::RequestEventStore>,
        Some(request_event_bus.clone()),
        Arc::new(NoopMetricsHook),
        clock.clone(),
    );

    let request_count = Arc::new(AtomicUsize::new(0));
    let dispatcher = Arc::new(ScriptedDispatcher {
        request_count: request_count.clone(),
        service_tier: response_service_tier,
    });
    let principal_view = Arc::new(PrincipalView::for_tests(
        "test-principal",
        true,
        vec!["*".to_owned()],
        Vec::new(),
        HashMap::new(),
    ));
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(TestSignerFactory))
        .global_router(Arc::new(TestRouter))
        .global_observability_hooks(Vec::new())
        .principal_view(principal_view)
        .upstream_records(vec![upstream_record()])
        .build();
    let lifecycle = Lifecycle::new_with_dynamic_view(
        Arc::new(BuiltinAuthn::new(
            DownstreamAuthMode::None,
            Some(NoneModeConfig {
                principal_id: "test-principal".to_owned(),
                upstream_kind: NoneModeUpstreamKind::AnthropicKey,
            }),
            None,
            clock.clone(),
        )),
        Arc::new(DynamicViewHolder::new(view)),
        dispatcher,
        LifecycleConfig::default(),
        clock,
    )
    .with_event_bus(request_event_bus);

    let response = lifecycle
        .handle(message_request(request_json))
        .await
        .expect("lifecycle handles message request");
    let status = response.status();
    response
        .into_body()
        .collect()
        .await
        .expect("response body collects");
    let events = storage.wait_for_request_events(1).await;
    let event = events
        .into_iter()
        .find(|event| event.model.as_deref() == Some(MODEL))
        .expect("persisted event for proxied model");
    (status, request_count.load(Ordering::Relaxed), event)
}

fn message_request(request_json: serde_json::Value) -> Request<Bytes> {
    Request::builder()
        .method(Method::POST)
        .uri("/v1/messages")
        .header("x-api-key", "sk-ant-downstream")
        .header("anthropic-version", "2023-06-01")
        .header(CONTENT_TYPE, "application/json")
        .body(Bytes::from(request_json.to_string()))
        .expect("message request builds")
}

fn upstream_record() -> UpstreamRecord {
    UpstreamRecord {
        id: UPSTREAM_ID,
        name: "scripted-upstream".to_owned(),
        kind: UpstreamKind::AnthropicApiKey,
        base_url: Some(Url::parse("http://upstream.invalid").expect("upstream URL parses")),
        enabled: true,
        api_key_ciphertext: Some(Vec::new()),
        revision: 1,
        created_at_unix_secs: NOW_UNIX_SECS,
        updated_at_unix_secs: NOW_UNIX_SECS,
        ..UpstreamRecord::default()
    }
}

struct TestRouter;

impl RouterPlugin for TestRouter {
    fn route(
        &self,
        _context: &RoutingContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError> {
        Ok(RouteDecision {
            upstream_id: Some(UPSTREAM_ID),
            upstream: Upstream::AnthropicDirect {
                base_url: Some(Url::parse("http://upstream.invalid").expect("upstream URL parses")),
            },
            dialect: Arc::new(TestDialect),
        })
    }
}

struct TestDialect;

impl UpstreamDialect for TestDialect {
    fn shape(
        &self,
        context: &DialectShapeContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        Ok(builder.shaped_request(
            Url::parse("http://upstream.invalid/v1/messages").expect("message URL parses"),
            context.method.clone(),
            context.downstream_headers.clone(),
            context.body_bytes.clone(),
        ))
    }
}

struct TestSignerFactory;

impl ApiKeyAwareSignerFactory for TestSignerFactory {
    fn with_router_choice(
        &self,
        _api_key: String,
        _router_chosen_upstream_name: String,
    ) -> Arc<dyn SignerFactory> {
        Arc::new(Self)
    }
}

#[async_trait]
impl SignerFactory for TestSignerFactory {
    async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        Ok(Arc::new(TestSigner))
    }
}

struct TestSigner;

#[async_trait]
impl Signer for TestSigner {
    async fn sign(
        &self,
        shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(&self, _error: &UpstreamError) -> RetryDecision {
        RetryDecision::Fail
    }
}

struct ScriptedDispatcher {
    request_count: Arc<AtomicUsize>,
    service_tier: &'static str,
}

#[async_trait]
impl UpstreamDispatch for ScriptedDispatcher {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        self.request_count.fetch_add(1, Ordering::Relaxed);
        let body = json!({
            "type": "message",
            "content": [],
            "usage": {
                "input_tokens": 1,
                "output_tokens": 1,
                "service_tier": self.service_tier
            }
        });
        let mut response = Response::new(Body::from(Bytes::from(body.to_string())));
        *response.status_mut() = StatusCode::OK;
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        Ok(response)
    }
}
