use crate::common::{TestAuthn, TestLifecycleBus, TestState, collect_body};
use crate::router_lifecycle_support::api_key_record;
use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_control::RequestEventBus;
use cc_lb_control::api_keys::principal_view::{
    DialectCache, PrincipalRoutingArtifacts, PrincipalView, RouterPipelineCache, ShapePluginCache,
};
use cc_lb_control::{DynamicViewBuilder, DynamicViewHolder};
use cc_lb_domain::{Principal, Provider, TerminalStrategy, Upstream};
use cc_lb_engine::{DispatchError, Lifecycle, LifecycleConfig, ProviderScope, UpstreamDispatch};
use cc_lb_lifecycle::{LifecycleEvent, TerminationReason};
use cc_lb_request_log::RequestEventUpdate;
use cc_lb_signer_anthropic_key::GeminiSubscriptionSigner;
use cc_lb_storage_api::{MetaStore, Storage};
use cc_lb_storage_api::{principal::PrincipalRecord, upstream::UpstreamKind};
use cc_lb_upstream::{
    ApiKeyAwareSignerFactory, DialectError, DialectShapeContext, ResponseTransformError,
    ResponseTransformHook, ShapedRequest, ShapedRequestBuilder, SignedRequest, Signer, SignerError,
    SignerFactory, TransformResponseRequest, TransformResponseResult, UpstreamDialect,
};
use http::{Method, Request, Response, StatusCode};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    convert::Infallible,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};
use url::Url;
use uuid::Uuid;

const ENDPOINT: &str = "http://catalog.local/prefix/v1internal:fetchAvailableModels";

#[derive(Default)]
struct Recorder {
    requests: Mutex<Vec<SignedRequest>>,
    hooks: Mutex<Vec<(String, Value)>>,
    hook_status: Mutex<Option<StatusCode>>,
    hook_body: Mutex<Option<Value>>,
    gzip_response: Mutex<bool>,
    api_key: Mutex<bool>,
    raw_response: Mutex<Option<Bytes>>,
    content_type: Mutex<Option<&'static str>>,
    body_poll_tripwire: Mutex<Option<Arc<AtomicUsize>>>,
    pending_body: Mutex<Option<PendingBody>>,
    hook_headers: Mutex<bool>,
    hook_fails: Mutex<bool>,
    catalog_endpoint: Mutex<Option<Url>>,
}

struct CatalogDialect(Arc<Recorder>);
impl UpstreamDialect for CatalogDialect {
    fn shape(
        &self,
        context: &DialectShapeContext,
        _: &Upstream,
        _: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        let mut url = Url::parse("http://original.local/").unwrap();
        url.set_path(&context.path);
        url.set_query(context.query.as_deref());
        Ok(builder.shaped_request(
            url,
            context.method.clone(),
            context.downstream_headers.clone(),
            context.body_bytes.clone(),
        ))
    }
    fn response_transform_hook(&self) -> Option<&dyn ResponseTransformHook> {
        Some(self)
    }
}
impl ResponseTransformHook for CatalogDialect {
    fn transform_response(
        &self,
        request: TransformResponseRequest,
    ) -> Result<TransformResponseResult, ResponseTransformError> {
        if let Ok(value) = serde_json::from_slice(&request.body) {
            self.0
                .hooks
                .lock()
                .unwrap()
                .push((request.principal.id, value));
        }
        if *self.0.hook_fails.lock().unwrap() {
            return Err(ResponseTransformError::Trap {
                reason: "catalog recorder failure".into(),
            });
        }
        let status = *self.0.hook_status.lock().unwrap();
        let body = self
            .0
            .hook_body
            .lock()
            .unwrap()
            .as_ref()
            .map(|body| Bytes::from(body.to_string()));
        let headers = (*self.0.hook_headers.lock().unwrap()).then(|| {
            let mut headers = http::HeaderMap::new();
            headers.insert("x-catalog-hook", http::HeaderValue::from_static("observed"));
            headers
        });
        if status.is_some() || body.is_some() || headers.is_some() {
            Ok(TransformResponseResult::Replace {
                status,
                headers,
                body,
            })
        } else {
            Ok(TransformResponseResult::Unchanged)
        }
    }
}

struct Factory {
    project: Option<&'static str>,
    api_key: bool,
    catalog_endpoint: Url,
}
impl ApiKeyAwareSignerFactory for Factory {
    fn with_router_choice(&self, _: String) -> Arc<dyn SignerFactory> {
        Arc::new(Factory {
            project: self.project,
            api_key: self.api_key,
            catalog_endpoint: self.catalog_endpoint.clone(),
        })
    }
}
#[async_trait]
impl SignerFactory for Factory {
    async fn build(&self, upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        if self.api_key {
            return cc_lb_signer_anthropic_key::ProviderKeySignerFactory::new(
                cc_lb_signer_anthropic_key::ProviderKeyKind::Gemini,
                "stored-api-key",
            )?
            .build(upstream)
            .await;
        }
        Ok(Arc::new(
            GeminiSubscriptionSigner::new(
                "stored-provider-token",
                self.project,
                None,
                Arc::new(cc_lb_clock::SystemClock),
            )?
            .with_catalog_endpoint(self.catalog_endpoint.clone()),
        ))
    }
}
struct Dispatch {
    recorder: Arc<Recorder>,
    status: StatusCode,
    body: Value,
}

struct PollTripwireBody(Arc<AtomicUsize>);

impl http_body::Body for PollTripwireBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("unexpected successful catalog SSE body must never be polled");
    }
}

#[derive(Clone)]
struct PendingBody {
    polled: Arc<tokio::sync::Notify>,
    polls: Arc<AtomicUsize>,
}

impl http_body::Body for PendingBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        self.polled.notify_one();
        Poll::Pending
    }
}

#[async_trait]
impl UpstreamDispatch for Dispatch {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        self.recorder.requests.lock().unwrap().push(request);
        let mut body = self
            .recorder
            .raw_response
            .lock()
            .unwrap()
            .as_ref()
            .map_or_else(|| self.body.to_string().into_bytes(), |body| body.to_vec());
        let mut response = Response::builder().status(self.status).header(
            "content-type",
            self.recorder
                .content_type
                .lock()
                .unwrap()
                .unwrap_or("application/json"),
        );
        if let Some(polls) = self.recorder.body_poll_tripwire.lock().unwrap().clone() {
            return Ok(response.body(Body::new(PollTripwireBody(polls))).unwrap());
        }
        if let Some(body) = self.recorder.pending_body.lock().unwrap().clone() {
            return Ok(response
                .header("retry-after", "17")
                .body(Body::new(body))
                .unwrap());
        }
        if *self.recorder.gzip_response.lock().unwrap() {
            use std::io::Write;
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(&body).unwrap();
            body = encoder.finish().unwrap();
            response = response.header("content-encoding", "gzip");
        }
        Ok(response
            .header("content-length", body.len())
            .body(Body::from(body))
            .unwrap())
    }
}

fn lifecycle(
    recorder: Arc<Recorder>,
    project: Option<&'static str>,
    status: StatusCode,
    body: Value,
) -> Lifecycle {
    lifecycle_with_owner(recorder, project, status, body, true)
}

fn lifecycle_with_owner(
    recorder: Arc<Recorder>,
    project: Option<&'static str>,
    status: StatusCode,
    body: Value,
    owned: bool,
) -> Lifecycle {
    let mut record = api_key_record(
        Uuid::new_v4(),
        "catalog-owner",
        "http://catalog.local/prefix/",
    );
    let api_key = *recorder.api_key.lock().unwrap();
    record.kind = if api_key {
        UpstreamKind::GeminiApiKey
    } else {
        UpstreamKind::GeminiOauth
    };
    let mut chains: HashMap<String, PrincipalRoutingArtifacts> = HashMap::new();
    chains.insert(
        "principal-test".into(),
        (
            Some(Arc::new(RouterPipelineCache::empty(
                TerminalStrategy::FirstPick,
            ))),
            DialectCache::Explicit(ShapePluginCache {
                dialect: Arc::new(CatalogDialect(recorder.clone())),
            }),
        ),
    );
    let principal = PrincipalRecord {
        id: Uuid::new_v4(),
        name: "principal-test".into(),
        kind: cc_lb_storage_api::PrincipalKind::Machine,
        allowed_models: vec!["*".into()],
        allowed_upstreams: vec![if owned { record.id } else { Uuid::new_v4() }],
        default_limits: vec![],
        enabled: true,
        deleted_at_unix_secs: None,
        revision: 1,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        router_terminal_strategy: Default::default(),
        cache_keepalive: None,
        openai_oauth_compat: None,
    };
    let authn = TestAuthn::with_principal_view(
        TestState::default(),
        Arc::new(PrincipalView::from_db(&[principal], chains)),
    );
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(Factory {
            project,
            api_key,
            catalog_endpoint: recorder
                .catalog_endpoint
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| Url::parse(ENDPOINT).unwrap()),
        }))
        .principal_view(authn.principal_view.clone())
        .upstream_records(vec![record])
        .build();
    Lifecycle::new_with_dynamic_view(
        authn.authn,
        Arc::new(DynamicViewHolder::new(view)),
        Arc::new(Dispatch {
            recorder,
            status,
            body,
        }),
        LifecycleConfig::default(),
        Arc::new(cc_lb_clock::SystemClock),
    )
}
fn request(path: &str, body: Bytes) -> Request<Bytes> {
    let mut request = crate::common::messages_request(body);
    *request.method_mut() = Method::GET;
    *request.uri_mut() = path.parse().unwrap();
    request
        .extensions_mut()
        .insert(ProviderScope(Provider::Gemini));
    request
}

#[tokio::test]
async fn catalog_dispatches_resolved_endpoint_and_hook_retains_raw_metadata() {
    let recorder = Arc::new(Recorder::default());
    let raw = json!({"models": {"gemini-test": {"displayName": "Test", "description": "Description", "supportsImages": true, "maxTokens": 123, "future": [1], "modelExperimentIds": ["experiment"]}},
        "experimentIds": [123], "deprecatedModelIds": {"old": {"newModelId": "gemini-test"}}, "tieredModelIds": {"flash": ["gemini-test"]}, "futureTop": {"x": true}, "response": {"mustNotUnwrap": true}});
    let lifecycle = lifecycle(
        recorder.clone(),
        Some("stored-project"),
        StatusCode::OK,
        raw.clone(),
    );
    let request = request("/v1beta/models", Bytes::new());
    let auth = lifecycle.authenticate(request.headers()).await.unwrap();
    let response = lifecycle.handle(request, &auth).await.unwrap();
    let (status, _, body) = collect_body(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap(),
        json!({"models": [{"name": "models/gemini-test", "displayName": "Test", "description": "Description"}]})
    );
    let requests = recorder.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].url().as_str(), ENDPOINT);
    assert_eq!(requests[0].method(), Method::POST);
    assert_eq!(
        serde_json::from_slice::<Value>(requests[0].body()).unwrap(),
        json!({"project": "stored-project"})
    );
    assert_eq!(
        requests[0].headers()["authorization"],
        "Bearer stored-provider-token"
    );
    assert_eq!(
        recorder.hooks.lock().unwrap().as_slice(),
        &[("principal-test".into(), raw)]
    );
}

#[tokio::test]
async fn missing_project_never_dispatches() {
    let recorder = Arc::new(Recorder::default());
    let lifecycle = lifecycle(
        recorder.clone(),
        None,
        StatusCode::OK,
        json!({"models": {}}),
    );
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
    let lifecycle = lifecycle(
        recorder,
        Some("stored-project"),
        StatusCode::TOO_MANY_REQUESTS,
        raw.clone(),
    );
    let request = request("/v1/models", Bytes::new());
    let auth = lifecycle.authenticate(request.headers()).await.unwrap();
    let (status, _, body) = collect_body(lifecycle.handle(request, &auth).await.unwrap()).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(serde_json::from_slice::<Value>(&body).unwrap(), raw);
}

#[tokio::test]
async fn error_sse_catalog_returns_status_before_pending_body() {
    for api_key in [true, false] {
        for status in [
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::TOO_MANY_REQUESTS,
        ] {
            let recorder = Arc::new(Recorder::default());
            *recorder.api_key.lock().unwrap() = api_key;
            *recorder.content_type.lock().unwrap() = Some("text/event-stream");
            let polled = Arc::new(tokio::sync::Notify::new());
            let polls = Arc::new(AtomicUsize::new(0));
            *recorder.pending_body.lock().unwrap() = Some(PendingBody {
                polled: polled.clone(),
                polls: polls.clone(),
            });
            let lifecycle = lifecycle(
                recorder.clone(),
                Some("stored-project"),
                status,
                Value::Null,
            );
            let request = request("/v1/models", Bytes::new());
            let auth = lifecycle.authenticate(request.headers()).await.unwrap();
            let response = tokio::select! {
                biased;
                response = lifecycle.handle(request, &auth) => response.unwrap(),
                () = polled.notified() => panic!(
                    "catalog error headers must arrive before awaiting body EOF: {status}, api_key={api_key}"
                ),
            };
            assert_eq!(response.status(), status);
            assert_eq!(response.headers()["content-type"], "text/event-stream");
            assert_eq!(response.headers()["retry-after"], "17");
            assert_eq!(polls.load(Ordering::SeqCst), 0);
            assert_eq!(recorder.requests.lock().unwrap().len(), 1);
            assert!(recorder.hooks.lock().unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn unauthenticated_malformed_catalog_never_dispatches() {
    let recorder = Arc::new(Recorder::default());
    let lifecycle = lifecycle(
        recorder.clone(),
        Some("stored-project"),
        StatusCode::OK,
        json!({"models": {}}),
    );
    for authorization in [None, Some("Bearer invalid-principal-token")] {
        let mut request = request("/v1/models", Bytes::from_static(b"{"));
        request.headers_mut().remove("authorization");
        request.headers_mut().remove("x-api-key");
        request.headers_mut().insert(
            "content-length",
            http::HeaderValue::from_static("999999999"),
        );
        if let Some(authorization) = authorization {
            request.headers_mut().insert(
                "authorization",
                http::HeaderValue::from_static(authorization),
            );
        }
        let error = match lifecycle.authenticate(request.headers()).await {
            Ok(_) => panic!("invalid principal must not authenticate"),
            Err(error) => error,
        };
        let rejection = cc_lb_engine::reject_unauthenticated_for_route(
            &error,
            request.method(),
            request.uri().path(),
            Some(Provider::Gemini),
            None,
        );
        assert_eq!(rejection.status(), StatusCode::UNAUTHORIZED);
        assert!(recorder.requests.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn catalog_configuration_does_not_change_count_tokens_envelope() {
    let recorder = Arc::new(Recorder::default());
    let lifecycle = lifecycle(
        recorder.clone(),
        Some("stored-project"),
        StatusCode::OK,
        json!({"totalTokens": 42}),
    );
    let mut request = request(
        "/v1beta/models/gemini-test:countTokens",
        Bytes::from_static(br#"{"contents":[{"parts":[{"text":"hello"}]}]}"#),
    );
    *request.method_mut() = Method::POST;
    let auth = lifecycle.authenticate(request.headers()).await.unwrap();
    let (status, _, body) = collect_body(lifecycle.handle(request, &auth).await.unwrap()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap(),
        json!({"totalTokens": 42})
    );
    let requests = recorder.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].url().path(), "/v1internal:countTokens");
    assert_eq!(
        serde_json::from_slice::<Value>(requests[0].body()).unwrap(),
        json!({"request": {"model": "models/gemini-test", "contents": [{"parts": [{"text": "hello"}]}]}})
    );
}

#[tokio::test]
async fn list_model_configs_shape_is_not_exposed_as_native_catalog() {
    let recorder = Arc::new(Recorder::default());
    let raw = json!({"allowedModelConfigs": [{"id": "separate", "displayName": "Separate", "descriptionText": "configuration", "future": true}],
        "defaultAgentModelConfig": {"id": "separate", "descriptionText": "different operation"}, "futureConfig": [1]});
    let lifecycle = lifecycle(
        recorder.clone(),
        Some("stored-project"),
        StatusCode::OK,
        raw.clone(),
    );
    let request = request("/v1/models", Bytes::new());
    let auth = lifecycle.authenticate(request.headers()).await.unwrap();
    let response = lifecycle.handle(request, &auth).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(recorder.hooks.lock().unwrap()[0].1, raw);
}

#[tokio::test]
async fn principal_cannot_dispatch_another_owners_catalog() {
    let recorder = Arc::new(Recorder::default());
    let lifecycle = lifecycle_with_owner(
        recorder.clone(),
        Some("stored-project"),
        StatusCode::OK,
        json!({"models": {}}),
        false,
    );
    let request = request("/v1/models", Bytes::new());
    let auth = lifecycle.authenticate(request.headers()).await.unwrap();
    let response = lifecycle.handle(request, &auth).await.unwrap();
    assert!(!response.status().is_success());
    assert!(recorder.requests.lock().unwrap().is_empty());
    assert!(recorder.hooks.lock().unwrap().is_empty());
}

#[tokio::test]
async fn single_model_metadata_is_explicitly_unsupported_before_dispatch() {
    for path in [
        "/v1/models/gemini-test",
        "/v1beta/models/gemini-test",
        "/v1/models/gemini",
        "/v1beta/models/gemini",
    ] {
        let recorder = Arc::new(Recorder::default());
        let lifecycle = lifecycle(
            recorder.clone(),
            Some("stored-project"),
            StatusCode::OK,
            json!({"models": {"gemini-test": {}}}),
        );
        let request = request(path, Bytes::new());
        let auth = lifecycle.authenticate(request.headers()).await.unwrap();
        let response = lifecycle.handle(request, &auth).await.unwrap();
        assert!(!response.status().is_success());
        assert!(recorder.requests.lock().unwrap().is_empty());
        assert!(recorder.hooks.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn status_only_hook_cannot_expose_raw_catalog_and_decoded_framing() {
    for gzip in [false, true] {
        let recorder = Arc::new(Recorder::default());
        *recorder.hook_status.lock().unwrap() = Some(StatusCode::FORBIDDEN);
        *recorder.gzip_response.lock().unwrap() = gzip;
        let raw = json!({"models": {"gemini-test": {"displayName": "Test", "supportsImages": true}},
            "experimentIds": [123], "future": {"private": true}});
        let lifecycle = lifecycle(
            recorder.clone(),
            Some("stored-project"),
            StatusCode::OK,
            raw.clone(),
        );
        let request = request("/v1/models", Bytes::new());
        let auth = lifecycle.authenticate(request.headers()).await.unwrap();
        let (status, headers, body) =
            collect_body(lifecycle.handle(request, &auth).await.unwrap()).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            json!({"models": [{"name": "models/gemini-test", "displayName": "Test"}]})
        );
        assert!(headers.get("content-encoding").is_none());
        assert_eq!(headers["content-length"], body.len().to_string());
        assert_eq!(recorder.hooks.lock().unwrap()[0].1, raw);
    }
}

#[tokio::test]
async fn intentional_hook_error_body_is_preserved_after_catalog_decode() {
    let recorder = Arc::new(Recorder::default());
    let error = json!({"error": {"message": "plugin denied"}});
    *recorder.hook_status.lock().unwrap() = Some(StatusCode::FORBIDDEN);
    *recorder.hook_body.lock().unwrap() = Some(error.clone());
    *recorder.gzip_response.lock().unwrap() = true;
    let lifecycle = lifecycle(
        recorder,
        Some("stored-project"),
        StatusCode::OK,
        json!({"models": {"gemini-test": {}}, "experimentIds": [123]}),
    );
    let request = request("/v1/models", Bytes::new());
    let auth = lifecycle.authenticate(request.headers()).await.unwrap();
    let (status, headers, body) =
        collect_body(lifecycle.handle(request, &auth).await.unwrap()).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(serde_json::from_slice::<Value>(&body).unwrap(), error);
    assert!(headers.get("content-encoding").is_none());
    assert_eq!(headers["content-length"], body.len().to_string());
}

#[tokio::test]
async fn api_key_catalog_keeps_native_fields_and_get_method() {
    let recorder = Arc::new(Recorder::default());
    *recorder.api_key.lock().unwrap() = true;
    let native = json!({"models": [{"name": "models/native", "displayName": "Native",
        "supportedGenerationMethods": ["generateContent"], "inputTokenLimit": 100,
        "outputTokenLimit": 20, "version": "1", "baseModelId": "native"}]});
    let lifecycle = lifecycle(recorder.clone(), None, StatusCode::OK, native.clone());
    let request = request("/v1beta/models", Bytes::new());
    let auth = lifecycle.authenticate(request.headers()).await.unwrap();
    let (status, _, body) = collect_body(lifecycle.handle(request, &auth).await.unwrap()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(serde_json::from_slice::<Value>(&body).unwrap(), native);
    let requests = recorder.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method(), Method::GET);
    assert_eq!(requests[0].url().path(), "/v1beta/models");
    assert_eq!(requests[0].headers()["x-goog-api-key"], "stored-api-key");
}

#[tokio::test]
async fn malformed_provider_catalog_bytes_fail_with_bad_gateway() {
    let recorder = Arc::new(Recorder::default());
    *recorder.raw_response.lock().unwrap() = Some(Bytes::from_static(b"{not-json"));
    let lifecycle = lifecycle(
        recorder.clone(),
        Some("stored-project"),
        StatusCode::OK,
        Value::Null,
    );
    let request = request("/v1/models", Bytes::new());
    let auth = lifecycle.authenticate(request.headers()).await.unwrap();
    let (status, _, body) = collect_body(lifecycle.handle(request, &auth).await.unwrap()).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(
        !body
            .windows(b"not-json".len())
            .any(|window| window == b"not-json")
    );
    assert_eq!(recorder.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn replacement_header_only_and_failed_hooks_keep_catalog_projection_contract() {
    for variant in ["replacement", "headers", "failure"] {
        let recorder = Arc::new(Recorder::default());
        let original =
            json!({"models": {"original": {"displayName": "Original"}}, "experimentIds": [123]});
        let replacement =
            json!({"models": {"replacement": {"description": "Replaced"}}, "future": true});
        match variant {
            "replacement" => *recorder.hook_body.lock().unwrap() = Some(replacement),
            "headers" => *recorder.hook_headers.lock().unwrap() = true,
            "failure" => *recorder.hook_fails.lock().unwrap() = true,
            _ => unreachable!(),
        }
        let lifecycle = lifecycle(
            recorder.clone(),
            Some("stored-project"),
            StatusCode::OK,
            original.clone(),
        );
        let request = request("/v1/models", Bytes::new());
        let auth = lifecycle.authenticate(request.headers()).await.unwrap();
        let (status, headers, body) =
            collect_body(lifecycle.handle(request, &auth).await.unwrap()).await;
        assert_eq!(status, StatusCode::OK);
        let expected = if variant == "replacement" {
            json!({"models": [{"name": "models/replacement", "description": "Replaced"}]})
        } else {
            json!({"models": [{"name": "models/original", "displayName": "Original"}]})
        };
        assert_eq!(serde_json::from_slice::<Value>(&body).unwrap(), expected);
        assert_eq!(recorder.hooks.lock().unwrap()[0].1, original);
        if variant == "headers" {
            assert_eq!(headers["x-catalog-hook"], "observed");
        }
    }
}

#[tokio::test]
async fn resolved_catalog_query_keeps_tenant_but_strips_api_keys_before_dispatch() {
    let recorder = Arc::new(Recorder::default());
    *recorder.catalog_endpoint.lock().unwrap() = Some(Url::parse(
        "http://catalog.local/prefix/v1internal:fetchAvailableModels?tenant=one&key=synthetic-provider-key",
    ).unwrap());
    let lifecycle = lifecycle(
        recorder.clone(),
        Some("stored-project"),
        StatusCode::OK,
        json!({"models": {"gemini-test": {}}}),
    );
    let request = request("/v1beta/models?key=dummy-principal", Bytes::new());
    let auth = lifecycle.authenticate(request.headers()).await.unwrap();
    let (status, _, _) = collect_body(lifecycle.handle(request, &auth).await.unwrap()).await;
    assert_eq!(status, StatusCode::OK);
    let requests = recorder.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method(), Method::POST);
    assert_eq!(
        requests[0].url().query_pairs().collect::<Vec<_>>(),
        vec![(
            std::borrow::Cow::Borrowed("tenant"),
            std::borrow::Cow::Borrowed("one")
        )]
    );
    assert_eq!(
        serde_json::from_slice::<Value>(requests[0].body()).unwrap(),
        json!({"project": "stored-project"})
    );
}

struct CatalogObservation {
    status: StatusCode,
    headers: http::HeaderMap,
    body: Bytes,
    events: Vec<LifecycleEvent>,
    recorded: cc_lb_storage_api::RequestEvent,
}

async fn observe_catalog(recorder: Arc<Recorder>, raw: Value) -> CatalogObservation {
    let storage = Arc::new(
        cc_lb_storage_sqlite::open_sqlite("sqlite::memory:", Arc::new(cc_lb_clock::SystemClock))
            .await
            .unwrap(),
    );
    storage.initialize().await.unwrap();
    let mut bus = TestLifecycleBus::new().with_assembler(storage as Arc<dyn Storage>);
    let mut events_rx = bus.bus.subscribe_lifecycle();
    let mut updates_rx = bus.bus.subscribe();
    let lifecycle = lifecycle(recorder, Some("stored-project"), StatusCode::OK, raw)
        .with_event_bus(bus.bus_arc());
    let request = request("/v1beta/models", Bytes::new());
    let auth = lifecycle.authenticate(request.headers()).await.unwrap();
    let (status, headers, body) =
        collect_body(lifecycle.handle(request, &auth).await.unwrap()).await;
    let events = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let mut events = Vec::new();
        loop {
            let event = events_rx.recv().await.expect("catalog lifecycle event");
            let terminated = matches!(event, LifecycleEvent::RequestTerminated { .. });
            events.push(event);
            if terminated {
                break;
            }
        }
        events
    })
    .await
    .expect("catalog termination is delivered");
    bus.shutdown().await;
    let recorded = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if let RequestEventUpdate::Final(update) =
                updates_rx.recv().await.expect("catalog request log update")
            {
                break update.event;
            }
        }
    })
    .await
    .expect("catalog final request log is delivered");
    CatalogObservation {
        status,
        headers,
        body,
        events,
        recorded,
    }
}

fn assert_catalog_failure(observation: &CatalogObservation) {
    let stream_results: Vec<_> = observation
        .events
        .iter()
        .filter_map(|event| {
            if let LifecycleEvent::StreamCompleted { result, .. } = event {
                Some(result)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(stream_results.len(), 1, "one catalog stream completion");
    let error = stream_results[0]
        .as_ref()
        .expect_err("invalid catalog is a failed stream");
    assert!(!error.error_type.is_empty(), "catalog failure has a type");
    assert!(
        !error.error_message.is_empty(),
        "catalog failure has detail"
    );
    let terminals: Vec<_> = observation
        .events
        .iter()
        .filter_map(|event| {
            if let LifecycleEvent::RequestTerminated {
                client_status,
                reason,
                ..
            } = event
            {
                Some((*client_status, reason))
            } else {
                None
            }
        })
        .collect();
    assert_eq!(terminals.len(), 1, "one catalog termination");
    assert_eq!(terminals[0].0, 502);
    let TerminationReason::ErrorCode(code) = terminals[0].1 else {
        panic!(
            "invalid catalog must not terminate successfully: {:?}",
            terminals[0].1
        );
    };
    assert!(!code.is_empty());
    assert_eq!(observation.recorded.status, 502);
    assert_eq!(
        observation.recorded.error_code.as_deref(),
        Some(code.as_str())
    );
    assert_eq!(
        observation.recorded.upstream_error_type.as_deref(),
        Some(error.error_type.as_str())
    );
    assert_eq!(
        observation.recorded.upstream_error_message.as_deref(),
        Some(error.error_message.as_str())
    );
}

fn sse_catalog_recorder() -> Arc<Recorder> {
    let recorder = Arc::new(Recorder::default());
    *recorder.content_type.lock().unwrap() = Some("text/event-stream");
    *recorder.raw_response.lock().unwrap() = Some(Bytes::from_static(
        b"data: {\"models\":{\"gemini-test\":{\"displayName\":\"Test\",\"modelExperimentIds\":[\"private-experiment\"]}},\"experimentIds\":[123],\"futureTop\":{\"private\":true}}\n\n",
    ));
    recorder
}

#[tokio::test]
async fn successful_sse_catalog_is_native_bad_gateway_without_provider_metadata() {
    let observation = observe_catalog(sse_catalog_recorder(), Value::Null).await;
    assert_eq!(observation.status, StatusCode::BAD_GATEWAY);
    assert_eq!(observation.headers["content-type"], "application/json");
    let error: Value = serde_json::from_slice(&observation.body).expect("native JSON error");
    assert_eq!(error["error"]["code"], 502);
    assert_eq!(error["error"]["status"], "UNAVAILABLE");
    assert!(
        error["error"]["message"]
            .as_str()
            .is_some_and(|message| !message.is_empty())
    );
    let body = std::str::from_utf8(&observation.body).unwrap();
    for metadata in ["private-experiment", "experimentIds", "futureTop", "data:"] {
        assert!(
            !body.contains(metadata),
            "raw provider metadata leaked: {metadata}"
        );
    }
}

#[tokio::test]
async fn successful_sse_catalog_records_failed_stream_and_terminal() {
    let observation = observe_catalog(sse_catalog_recorder(), Value::Null).await;
    assert_catalog_failure(&observation);
}

#[tokio::test]
async fn successful_sse_catalog_rejects_headers_without_polling_upstream_body() {
    let recorder = sse_catalog_recorder();
    let polls = Arc::new(AtomicUsize::new(0));
    *recorder.body_poll_tripwire.lock().unwrap() = Some(polls.clone());
    let observation = observe_catalog(recorder.clone(), Value::Null).await;
    assert_eq!(observation.status, StatusCode::BAD_GATEWAY);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert!(recorder.hooks.lock().unwrap().is_empty());
    assert_catalog_failure(&observation);
}

#[tokio::test]
async fn malformed_catalog_records_failed_stream_and_terminal() {
    let recorder = Arc::new(Recorder::default());
    *recorder.raw_response.lock().unwrap() = Some(Bytes::from_static(b"{not-json"));
    let observation = observe_catalog(recorder, Value::Null).await;
    assert_eq!(observation.status, StatusCode::BAD_GATEWAY);
    assert_catalog_failure(&observation);
}

#[tokio::test]
async fn invalid_catalog_shape_records_failed_stream_and_terminal() {
    let observation = observe_catalog(
        Arc::new(Recorder::default()),
        json!({"allowedModelConfigs": [{"id": "private-model"}]}),
    )
    .await;
    assert_eq!(observation.status, StatusCode::BAD_GATEWAY);
    assert_catalog_failure(&observation);
}

#[tokio::test]
async fn catalog_observation_preserves_json_api_key_and_intentional_transform_controls() {
    for variant in ["oauth", "api-key", "replacement", "intentional-error"] {
        let recorder = Arc::new(Recorder::default());
        let raw = if variant == "api-key" {
            *recorder.api_key.lock().unwrap() = true;
            json!({"models": [{"name": "models/native", "inputTokenLimit": 100}]})
        } else {
            json!({"models": {"original": {"displayName": "Original"}}, "experimentIds": [123]})
        };
        let (expected_status, expected_body) = match variant {
            "api-key" => (StatusCode::OK, raw.clone()),
            "replacement" => {
                *recorder.hook_body.lock().unwrap() =
                    Some(json!({"models": {"replacement": {"description": "Replaced"}}}));
                (
                    StatusCode::OK,
                    json!({"models": [{"name": "models/replacement", "description": "Replaced"}]}),
                )
            }
            "intentional-error" => {
                let error = json!({"error": {"message": "plugin denied"}});
                *recorder.hook_status.lock().unwrap() = Some(StatusCode::FORBIDDEN);
                *recorder.hook_body.lock().unwrap() = Some(error.clone());
                (StatusCode::FORBIDDEN, error)
            }
            _ => (
                StatusCode::OK,
                json!({"models": [{"name": "models/original", "displayName": "Original"}]}),
            ),
        };
        let observation = observe_catalog(recorder.clone(), raw).await;
        assert_eq!(observation.status, expected_status, "{variant}");
        assert_eq!(
            serde_json::from_slice::<Value>(&observation.body).unwrap(),
            expected_body,
            "{variant}"
        );
        assert!(
            observation.events.iter().any(|event| matches!(
                event,
                LifecycleEvent::StreamCompleted { result: Ok(_), .. }
            )),
            "{variant}"
        );
        assert!(observation.events.iter().any(|event| matches!(
            event, LifecycleEvent::RequestTerminated { client_status, reason: TerminationReason::Success, .. }
                if *client_status == expected_status.as_u16()
        )), "intentional transforms retain existing observation policy: {variant}");
        assert_eq!(observation.recorded.status, expected_status.as_u16());
        assert_eq!(observation.recorded.error_code, None, "{variant}");
        let requests = recorder.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        if variant == "api-key" {
            assert_eq!(requests[0].method(), Method::GET);
            assert_eq!(requests[0].url().path(), "/v1beta/models");
        } else {
            assert_eq!(requests[0].method(), Method::POST);
            assert_eq!(requests[0].url().as_str(), ENDPOINT);
        }
    }
}
