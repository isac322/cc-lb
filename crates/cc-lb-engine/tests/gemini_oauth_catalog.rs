use std::{collections::HashMap, sync::{Arc, Mutex}};
use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_control::{DynamicViewBuilder, DynamicViewHolder};
use cc_lb_control::api_keys::principal_view::{DialectCache, PrincipalRoutingArtifacts, PrincipalView, RouterPipelineCache, ShapePluginCache};
use cc_lb_domain::{Principal, Provider, TerminalStrategy, Upstream};
use cc_lb_engine::{DispatchError, Lifecycle, LifecycleConfig, ProviderScope, UpstreamDispatch};
use cc_lb_signer_anthropic_key::GeminiSubscriptionSigner;
use cc_lb_storage_api::{principal::PrincipalRecord, upstream::UpstreamKind};
use cc_lb_upstream::{ApiKeyAwareSignerFactory, DialectError, DialectShapeContext, ResponseTransformError, ResponseTransformHook, ShapedRequest, ShapedRequestBuilder, SignedRequest, Signer, SignerError, SignerFactory, TransformResponseRequest, TransformResponseResult, UpstreamDialect};
use http::{Method, Request, Response, StatusCode};
use serde_json::{Value, json};
use url::Url;
use uuid::Uuid;
use crate::common::{TestAuthn, TestState, collect_body};
use crate::router_lifecycle_support::api_key_record;

const ENDPOINT: &str = "http://catalog.local/prefix/v1internal:fetchAvailableModels";

#[derive(Default)]
struct Recorder {
    requests: Mutex<Vec<SignedRequest>>,
    hooks: Mutex<Vec<(String, Value)>>,
}

struct CatalogDialect(Arc<Recorder>);
impl UpstreamDialect for CatalogDialect {
    fn shape(&self, context: &DialectShapeContext, _: &Upstream, _: &Principal, builder: &mut ShapedRequestBuilder) -> Result<ShapedRequest, DialectError> {
        let mut url = Url::parse("http://original.local/").unwrap();
        url.set_path(&context.path);
        Ok(builder.shaped_request(url, context.method.clone(), context.downstream_headers.clone(), context.body_bytes.clone()))
    }
    fn response_transform_hook(&self) -> Option<&dyn ResponseTransformHook> { Some(self) }
}
impl ResponseTransformHook for CatalogDialect {
    fn transform_response(&self, request: TransformResponseRequest) -> Result<TransformResponseResult, ResponseTransformError> {
        self.0.hooks.lock().unwrap().push((request.principal.id, serde_json::from_slice(&request.body).unwrap()));
        Ok(TransformResponseResult::Unchanged)
    }
}

struct Factory { project: Option<&'static str> }
impl ApiKeyAwareSignerFactory for Factory {
    fn with_router_choice(&self, _: String) -> Arc<dyn SignerFactory> { Arc::new(Factory { project: self.project }) }
}
#[async_trait]
impl SignerFactory for Factory {
    async fn build(&self, _: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        Ok(Arc::new(GeminiSubscriptionSigner::new("stored-provider-token", self.project, None, Arc::new(cc_lb_clock::SystemClock))?
            .with_catalog_endpoint(Url::parse(ENDPOINT).unwrap())))
    }
}
struct Dispatch { recorder: Arc<Recorder>, status: StatusCode, body: Value }
#[async_trait]
impl UpstreamDispatch for Dispatch {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        self.recorder.requests.lock().unwrap().push(request);
        Ok(Response::builder().status(self.status).header("content-type", "application/json")
            .body(Body::from(self.body.to_string())).unwrap())
    }
}

fn lifecycle(recorder: Arc<Recorder>, project: Option<&'static str>, status: StatusCode, body: Value) -> Lifecycle {
    lifecycle_with_owner(recorder, project, status, body, true)
}

fn lifecycle_with_owner(recorder: Arc<Recorder>, project: Option<&'static str>, status: StatusCode, body: Value, owned: bool) -> Lifecycle {
    let mut record = api_key_record(Uuid::new_v4(), "catalog-owner", "http://catalog.local/prefix/");
    record.kind = UpstreamKind::GeminiOauth;
    let mut chains: HashMap<String, PrincipalRoutingArtifacts> = HashMap::new();
    chains.insert("principal-test".into(), (Some(Arc::new(RouterPipelineCache::empty(TerminalStrategy::FirstPick))),
        DialectCache::Explicit(ShapePluginCache { dialect: Arc::new(CatalogDialect(recorder.clone())) })));
    let principal = PrincipalRecord {
        id: Uuid::new_v4(), name: "principal-test".into(), kind: cc_lb_storage_api::PrincipalKind::Machine,
        allowed_models: vec!["*".into()], allowed_upstreams: vec![if owned { record.id } else { Uuid::new_v4() }], default_limits: vec![],
        enabled: true, deleted_at_unix_secs: None, revision: 1, created_at_unix_secs: 0, updated_at_unix_secs: 0,
        router_terminal_strategy: Default::default(), cache_keepalive: None,
    };
    let authn = TestAuthn::with_principal_view(TestState::default(), Arc::new(PrincipalView::from_db(&[principal], chains)));
    let view = DynamicViewBuilder::new(0).signer_factory(Arc::new(Factory { project }))
        .principal_view(authn.principal_view.clone()).upstream_records(vec![record]).build();
    Lifecycle::new_with_dynamic_view(authn.authn, Arc::new(DynamicViewHolder::new(view)),
        Arc::new(Dispatch { recorder, status, body }), LifecycleConfig::default(), Arc::new(cc_lb_clock::SystemClock))
}
fn request(path: &str, body: Bytes) -> Request<Bytes> {
    let mut request = crate::common::messages_request(body);
    *request.method_mut() = Method::GET;
    *request.uri_mut() = path.parse().unwrap();
    request.extensions_mut().insert(ProviderScope(Provider::Gemini));
    request
}

#[tokio::test]
async fn catalog_dispatches_resolved_endpoint_and_hook_retains_raw_metadata() {
    let recorder = Arc::new(Recorder::default());
    let raw = json!({"models": {"gemini-test": {"displayName": "Test", "description": "Description", "supportsImages": true, "maxTokens": 123, "future": [1]}},
        "experimentIds": [123], "tieredModelIds": {"flash": ["gemini-test"]}, "futureTop": {"x": true}, "response": {"mustNotUnwrap": true}});
    let lifecycle = lifecycle(recorder.clone(), Some("stored-project"), StatusCode::OK, raw.clone());
    let request = request("/v1beta/models", Bytes::new());
    let auth = lifecycle.authenticate(request.headers()).await.unwrap();
    let response = lifecycle.handle(request, &auth).await.unwrap();
    let (status, _, body) = collect_body(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(serde_json::from_slice::<Value>(&body).unwrap(), json!({"models": [{"name": "models/gemini-test", "displayName": "Test", "description": "Description"}]}));
    let requests = recorder.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].url().as_str(), ENDPOINT);
    assert_eq!(requests[0].method(), Method::POST);
    assert_eq!(serde_json::from_slice::<Value>(requests[0].body()).unwrap(), json!({"project": "stored-project"}));
    assert_eq!(requests[0].headers()["authorization"], "Bearer stored-provider-token");
    assert_eq!(recorder.hooks.lock().unwrap().as_slice(), &[("principal-test".into(), raw)]);
}

#[tokio::test]
async fn missing_project_never_dispatches() {
    let recorder = Arc::new(Recorder::default());
    let lifecycle = lifecycle(recorder.clone(), None, StatusCode::OK, json!({"models": {}}));
    let request = request("/v1/models", Bytes::new());
    let auth = lifecycle.authenticate(request.headers()).await.unwrap();
    let response = lifecycle.handle(request, &auth).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert!(recorder.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn provider_errors_stay_unprojected() {
    let recorder = Arc::new(Recorder::default());
    let raw = json!({"error": {"code": 429, "message": "limited"}});
    let lifecycle = lifecycle(recorder, Some("stored-project"), StatusCode::TOO_MANY_REQUESTS, raw.clone());
    let request = request("/v1/models", Bytes::new());
    let auth = lifecycle.authenticate(request.headers()).await.unwrap();
    let (status, _, body) = collect_body(lifecycle.handle(request, &auth).await.unwrap()).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(serde_json::from_slice::<Value>(&body).unwrap(), raw);
}

#[tokio::test]
async fn unauthenticated_malformed_catalog_never_dispatches() {
    let recorder = Arc::new(Recorder::default());
    let lifecycle = lifecycle(recorder.clone(), Some("stored-project"), StatusCode::OK, json!({"models": {}}));
    let mut request = request("/v1/models", Bytes::from_static(b"{"));
    request.headers_mut().remove("authorization");
    request.headers_mut().remove("x-api-key");
    assert!(lifecycle.authenticate(request.headers()).await.is_err());
    assert!(recorder.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn catalog_configuration_does_not_change_count_tokens_envelope() {
    let recorder = Arc::new(Recorder::default());
    let lifecycle = lifecycle(recorder.clone(), Some("stored-project"), StatusCode::OK, json!({"totalTokens": 42}));
    let mut request = request("/v1beta/models/gemini-test:countTokens",
        Bytes::from_static(br#"{"contents":[{"parts":[{"text":"hello"}]}]}"#));
    *request.method_mut() = Method::POST;
    let auth = lifecycle.authenticate(request.headers()).await.unwrap();
    let (status, _, body) = collect_body(lifecycle.handle(request, &auth).await.unwrap()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(serde_json::from_slice::<Value>(&body).unwrap(), json!({"totalTokens": 42}));
    let requests = recorder.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].url().path(), "/v1internal:countTokens");
    assert_eq!(serde_json::from_slice::<Value>(requests[0].body()).unwrap(),
        json!({"request": {"model": "models/gemini-test", "contents": [{"parts": [{"text": "hello"}]}]}}));
}

#[tokio::test]
async fn list_model_configs_shape_is_not_exposed_as_native_catalog() {
    let recorder = Arc::new(Recorder::default());
    let raw = json!({"modelConfigs": [{"descriptionText": "different operation"}]});
    let lifecycle = lifecycle(recorder.clone(), Some("stored-project"), StatusCode::OK, raw.clone());
    let request = request("/v1/models", Bytes::new());
    let auth = lifecycle.authenticate(request.headers()).await.unwrap();
    let response = lifecycle.handle(request, &auth).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(recorder.hooks.lock().unwrap()[0].1, raw);
}

#[tokio::test]
async fn principal_cannot_dispatch_another_owners_catalog() {
    let recorder = Arc::new(Recorder::default());
    let lifecycle = lifecycle_with_owner(recorder.clone(), Some("stored-project"),
        StatusCode::OK, json!({"models": {}}), false);
    let request = request("/v1/models", Bytes::new());
    let auth = lifecycle.authenticate(request.headers()).await.unwrap();
    let response = lifecycle.handle(request, &auth).await.unwrap();
    assert!(!response.status().is_success());
    assert!(recorder.requests.lock().unwrap().is_empty());
    assert!(recorder.hooks.lock().unwrap().is_empty());
}
