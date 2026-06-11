use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_config::{DownstreamAuthMode, NoneModeConfig, NoneModeUpstreamKind};
use cc_lb_core::api_keys::builtin_authn::BuiltinAuthn;
use cc_lb_core::api_keys::principal_view::PrincipalView;
use cc_lb_core::{
    ApiKeyAwareSignerFactory, Body, DispatchError, DynamicViewBuilder, DynamicViewHolder,
    ErrorNormalizer, Lifecycle, LifecycleConfig, UpstreamDispatch,
};
use cc_lb_plugin_api::{
    DialectError, ObservabilityHook, Principal, RequestContext, RetryDecision, RouteDecision,
    RouteError, RouterPlugin, ShapedRequest, ShapedRequestBuilder, SignedRequest, Signer,
    SignerError, SignerFactory, SigningCapability, Upstream, UpstreamCandidate, UpstreamDialect,
    UpstreamError,
};
use cc_lb_storage_api::principal::{PrincipalKind, PrincipalRecord};
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamRecord};
use http::{Request, Response, StatusCode};
use url::Url;
use uuid::Uuid;

#[tokio::test]
async fn none_mode_terminal_selects_bound_principal_upstream() {
    let first_id = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let second_id = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let state = Arc::new(ParityState::default());
    let principal_view = Arc::new(PrincipalView::from_db(
        &[principal("team-X", vec![second_id, first_id])],
        HashMap::new(),
    ));
    let authn = Arc::new(BuiltinAuthn::new(
        DownstreamAuthMode::None,
        Some(NoneModeConfig {
            principal_id: "team-X".to_owned(),
            upstream_kind: NoneModeUpstreamKind::AnthropicKey,
        }),
        None,
    ));
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(RecordingSignerFactory {
            state: state.clone(),
        }))
        .global_router(Arc::new(RecordingFallbackRouter {
            state: state.clone(),
        }))
        .dispatcher(Arc::new(RecordingDispatcher {
            state: state.clone(),
        }))
        .global_observability_hooks(Vec::<Arc<dyn ObservabilityHook>>::new())
        .error_normalizer(Arc::new(ErrorNormalizer::new()))
        .principal_view(principal_view)
        .upstream_records(vec![
            api_key_upstream(second_id, "second", "http://second.local/"),
            api_key_upstream(first_id, "first", "http://first.local/"),
        ])
        .build();
    let lifecycle = Lifecycle::new_with_dynamic_view(
        authn,
        Arc::new(DynamicViewHolder::new(view)),
        LifecycleConfig::default(),
    );

    let response = lifecycle
        .handle(messages_request())
        .await
        .expect("lifecycle responds");

    assert_eq!(response.status(), StatusCode::OK);
    assert!(state.principals.lock().unwrap().is_empty());
    assert!(state.candidates.lock().unwrap().is_empty());
    assert_eq!(
        state.signer_choices.lock().unwrap().as_slice(),
        &["first".to_owned()]
    );
    assert_eq!(
        state.dispatched_urls.lock().unwrap().as_slice(),
        &["http://first.local/v1/messages".to_owned()]
    );
}

#[derive(Default)]
struct ParityState {
    principals: Mutex<Vec<String>>,
    candidates: Mutex<Vec<Vec<Uuid>>>,
    signer_choices: Mutex<Vec<String>>,
    dispatched_urls: Mutex<Vec<String>>,
}

struct RecordingFallbackRouter {
    state: Arc<ParityState>,
}

impl RouterPlugin for RecordingFallbackRouter {
    fn route(
        &self,
        _ctx: &RequestContext,
        principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError> {
        self.state
            .principals
            .lock()
            .unwrap()
            .push(principal.id.clone());
        self.state.candidates.lock().unwrap().push(
            candidates
                .iter()
                .map(|candidate| candidate.upstream_id)
                .collect(),
        );
        Ok(RouteDecision {
            upstream_id: None,
            upstream: Upstream::AnthropicDirect,
            dialect: Arc::new(TestDialect),
        })
    }
}

struct RecordingSignerFactory {
    state: Arc<ParityState>,
}

impl ApiKeyAwareSignerFactory for RecordingSignerFactory {
    fn with_router_choice(
        &self,
        _api_key: String,
        router_chosen_upstream_name: String,
    ) -> Arc<dyn SignerFactory> {
        self.state
            .signer_choices
            .lock()
            .unwrap()
            .push(router_chosen_upstream_name);
        Arc::new(TestSignerFactory)
    }
}

struct TestSignerFactory;

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

    async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
        RetryDecision::Fail
    }
}

struct TestDialect;

impl UpstreamDialect for TestDialect {
    fn shape(
        &self,
        ctx: &RequestContext,
        upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        let _ = upstream;
        let mut url = Url::parse("http://router-choice-is-advisory.local/").unwrap();
        url.set_path(ctx.path.trim_start_matches('/'));
        url.set_query(ctx.query.as_deref());
        Ok(builder.shaped_request(
            url,
            ctx.method.clone(),
            ctx.downstream_headers.clone(),
            ctx.body_bytes.clone(),
        ))
    }

    fn normalize_error(&self, _status: StatusCode, _body: &Bytes) -> Option<Bytes> {
        None
    }
}

struct RecordingDispatcher {
    state: Arc<ParityState>,
}

#[async_trait]
impl UpstreamDispatch for RecordingDispatcher {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let (url, _method, _headers, _body) = request.into_parts();
        self.state
            .dispatched_urls
            .lock()
            .unwrap()
            .push(url.to_string());
        Ok(Response::builder()
            .status(StatusCode::OK)
            .body(Body::from(Bytes::from_static(
                br#"{"usage":{"input_tokens":1,"output_tokens":1}}"#,
            )))
            .unwrap())
    }
}

fn principal(name: &str, allowed_upstreams: Vec<Uuid>) -> PrincipalRecord {
    PrincipalRecord {
        id: Uuid::new_v4(),
        name: name.to_owned(),
        kind: PrincipalKind::Machine,
        allowed_models: Vec::new(),
        allowed_upstreams,
        default_limits: Vec::new(),
        enabled: true,
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs: None,
        revision: 1,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        router_terminal_strategy: Default::default(),
    }
}

fn api_key_upstream(id: Uuid, name: &str, base_url: &str) -> UpstreamRecord {
    UpstreamRecord {
        id,
        name: name.to_owned(),
        kind: UpstreamKind::AnthropicApiKey,
        base_url: Some(Url::parse(base_url).unwrap()),
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

fn messages_request() -> Request<Bytes> {
    Request::builder()
        .method("POST")
        .uri("/v1/messages")
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .body(Bytes::from_static(
            br#"{"model":"claude","max_tokens":1,"messages":[]}"#,
        ))
        .unwrap()
}
