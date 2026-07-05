use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_config::{DownstreamAuthMode, NoneModeConfig, NoneModeUpstreamKind};
use cc_lb_core::api_keys::builtin_authn::BuiltinAuthn;
use cc_lb_core::api_keys::principal_view::{
    DialectCache, ObservabilityHooksCache, PrincipalView, RouterPipelineCache,
};
use cc_lb_core::{
    ApiKeyAwareSignerFactory, Body, DispatchError, DynamicView, DynamicViewBuilder,
    DynamicViewHolder, ErrorNormalizer, Lifecycle, LifecycleConfig, RequestKind, UpstreamDispatch,
    build_candidates,
};
use cc_lb_plugin_api::{
    DialectError, FilterError, FilterOutput, FilterPlugin, ObservabilityError, ObservabilityHook,
    ObserveEvent, Principal, RequestContext, RetryDecision, RouteDecision, RouteError,
    RouterPlugin, ShapedRequest, ShapedRequestBuilder, SignedRequest, Signer, SignerError,
    SignerFactory, SigningCapability, TerminalStrategy, Upstream, UpstreamCandidate,
    UpstreamDialect, UpstreamError,
};
use cc_lb_storage_api::principal::{PrincipalKind, PrincipalRecord};
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamRecord};
use http::{Request, Response, StatusCode};
use url::Url;
use uuid::Uuid;

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

    async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
        RetryDecision::Fail
    }
}

struct TestRouter;

impl RouterPlugin for TestRouter {
    fn route(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError> {
        panic!("candidate builder test must not route")
    }
}

struct TestDispatcher;

#[async_trait]
impl UpstreamDispatch for TestDispatcher {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        Ok(Response::builder()
            .status(StatusCode::OK)
            .body(Body::from(Bytes::new()))
            .expect("test response builds"))
    }
}

struct TestHook;

impl ObservabilityHook for TestHook {
    fn observe(&self, _event: ObserveEvent) -> Result<(), ObservabilityError> {
        Ok(())
    }
}

#[test]
fn build_candidates_filters_by_principal_enabled_deleted_kind_and_sorts() {
    let allowed_low = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let allowed_mid = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let allowed_high = Uuid::parse_str("00000000-0000-0000-0000-000000000003").unwrap();
    let disabled = Uuid::parse_str("00000000-0000-0000-0000-000000000004").unwrap();
    let deleted = Uuid::parse_str("00000000-0000-0000-0000-000000000005").unwrap();
    let not_allowed = Uuid::parse_str("00000000-0000-0000-0000-000000000000").unwrap();

    let view = test_view(
        vec![
            principal("limited", vec![allowed_high, allowed_low, allowed_mid]),
            principal("all", Vec::new()),
        ],
        vec![
            upstream(allowed_high, UpstreamKind::AnthropicApiKey, true, None),
            upstream(disabled, UpstreamKind::AnthropicApiKey, false, None),
            upstream(allowed_mid, UpstreamKind::AnthropicOauth, true, None),
            upstream(deleted, UpstreamKind::AnthropicApiKey, true, Some(123)),
            upstream(not_allowed, UpstreamKind::AnthropicApiKey, true, None),
            upstream(allowed_low, UpstreamKind::AnthropicApiKey, true, None),
        ],
    );

    let limited = build_candidates(
        &view,
        "limited",
        RequestKind::AnthropicMessages,
        "",
        &[],
        &cc_lb_core::SystemClock,
    );
    assert_eq!(
        candidate_ids(&limited),
        vec![allowed_low, allowed_mid, allowed_high]
    );

    let all = build_candidates(
        &view,
        "all",
        RequestKind::AnthropicMessages,
        "",
        &[],
        &cc_lb_core::SystemClock,
    );
    assert_eq!(
        candidate_ids(&all),
        vec![not_allowed, allowed_low, allowed_mid, allowed_high]
    );

    assert!(
        build_candidates(
            &view,
            "missing",
            RequestKind::AnthropicMessages,
            "",
            &[],
            &cc_lb_core::SystemClock
        )
        .is_empty()
    );
}

#[tokio::test]
async fn lifecycle_filters_built_candidates_through_pipeline_before_terminal_strategy()
-> Result<(), Box<dyn std::error::Error>> {
    let first = Uuid::from_u128(1);
    let second = Uuid::from_u128(2);
    let third = Uuid::from_u128(3);
    let not_allowed = Uuid::from_u128(4);
    let filter_calls = Arc::new(Mutex::new(Vec::new()));
    let router_calls = Arc::new(Mutex::new(Vec::new()));
    let filter: Arc<dyn FilterPlugin> = Arc::new(KeepIdsFilter {
        kept_upstream_ids: vec![second, third],
        calls: Arc::clone(&filter_calls),
    });
    let pipeline = Arc::new(RouterPipelineCache {
        user_filters: vec![filter],
        terminal: TerminalStrategy::FirstPick,
        instantiation_error: None,
    });
    let mut chains = HashMap::new();
    chains.insert(
        "limited".to_owned(),
        (
            Some(pipeline),
            ObservabilityHooksCache::Inherit,
            DialectCache::Inherit,
        ),
    );
    let principal_view = Arc::new(PrincipalView::from_db(
        &[principal("limited", vec![third, first, second])],
        chains,
    ));
    let view = test_view_with_principal_view(
        Arc::clone(&principal_view),
        Arc::new(RecordingRouter {
            calls: Arc::clone(&router_calls),
        }),
        vec![
            upstream(third, UpstreamKind::AnthropicApiKey, true, None),
            upstream(not_allowed, UpstreamKind::AnthropicApiKey, true, None),
            upstream(first, UpstreamKind::AnthropicApiKey, true, None),
            upstream(second, UpstreamKind::AnthropicApiKey, true, None),
        ],
    );

    let built = build_candidates(
        &view,
        "limited",
        RequestKind::AnthropicMessages,
        "",
        &[],
        &cc_lb_core::SystemClock,
    );
    assert_eq!(candidate_ids(&built), vec![first, second, third]);

    let authn = Arc::new(BuiltinAuthn::new(
        DownstreamAuthMode::None,
        Some(NoneModeConfig {
            principal_id: "limited".to_owned(),
            upstream_kind: NoneModeUpstreamKind::AnthropicKey,
        }),
        None,
        Arc::new(cc_lb_core::SystemClock),
    ));
    let lifecycle = Lifecycle::new_with_dynamic_view(
        authn,
        Arc::new(DynamicViewHolder::new(view)),
        LifecycleConfig::default(),
        Arc::new(cc_lb_core::SystemClock),
    );

    let response = lifecycle
        .handle(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("x-api-key", "sk-ant-test")
                .body(Bytes::from_static(
                    br#"{"model":"claude-test","messages":[],"max_tokens":1}"#,
                ))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        filter_calls.lock().expect("filter calls lock").as_slice(),
        &[vec![first, second, third]],
    );
    assert!(
        router_calls.lock().expect("router calls lock").is_empty(),
        "terminal strategy must not invoke the legacy router"
    );
    Ok(())
}

fn test_view(principals: Vec<PrincipalRecord>, upstreams: Vec<UpstreamRecord>) -> Arc<DynamicView> {
    test_view_with_principal_view(
        Arc::new(PrincipalView::from_db(
            &principals,
            std::collections::HashMap::new(),
        )),
        Arc::new(TestRouter),
        upstreams,
    )
}

fn test_view_with_principal_view(
    principal_view: Arc<PrincipalView>,
    router: Arc<dyn RouterPlugin>,
    upstreams: Vec<UpstreamRecord>,
) -> Arc<DynamicView> {
    DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(TestSignerFactory))
        .global_router(router)
        .dispatcher(Arc::new(TestDispatcher))
        .global_observability_hooks(vec![Arc::new(TestHook)])
        .error_normalizer(Arc::new(ErrorNormalizer::new()))
        .principal_view(principal_view)
        .upstream_records(upstreams)
        .build()
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

fn upstream(
    id: Uuid,
    kind: UpstreamKind,
    enabled: bool,
    deleted_at_unix_secs: Option<u64>,
) -> UpstreamRecord {
    UpstreamRecord {
        id,
        name: format!("upstream-{id}"),
        kind,
        base_url: None,
        enabled,
        oauth_credentials: None,
        api_key_ciphertext: None,
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs,
        revision: 1,
        oauth_token_generation: 0,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        warmup_enabled: false,
        warmup_dialect_plugin: None,
        last_warmup_at_unix_secs: None,
    }
}

fn candidate_ids(candidates: &[UpstreamCandidate]) -> Vec<Uuid> {
    candidates
        .iter()
        .map(|candidate| candidate.upstream_id)
        .collect()
}

struct KeepIdsFilter {
    kept_upstream_ids: Vec<Uuid>,
    calls: Arc<Mutex<Vec<Vec<Uuid>>>>,
}

impl FilterPlugin for KeepIdsFilter {
    fn filter(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        self.calls
            .lock()
            .expect("filter calls lock")
            .push(candidate_ids(candidates));
        Ok(FilterOutput {
            kept_upstream_ids: self.kept_upstream_ids.clone(),
            reason: "candidate-builder pipeline kept survivors".to_owned(),
            per_candidate_reasons: Vec::new(),
            subscription_preference: None,
        })
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::from_u128(0x2801)
    }

    fn plugin_name(&self) -> &str {
        "candidate-builder-keep-ids"
    }
}

struct RecordingRouter {
    calls: Arc<Mutex<Vec<Vec<Uuid>>>>,
}

impl RouterPlugin for RecordingRouter {
    fn route(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError> {
        self.calls
            .lock()
            .expect("router calls lock")
            .push(candidate_ids(candidates));
        let candidate = candidates.first().ok_or_else(|| RouteError::NoRoute {
            reason: "no routed candidate".to_owned(),
        })?;
        Ok(RouteDecision {
            upstream_id: Some(candidate.upstream_id),
            upstream: Upstream::AnthropicDirect { base_url: None },
            dialect: Arc::new(PassthroughDialect),
        })
    }
}

struct PassthroughDialect;

impl UpstreamDialect for PassthroughDialect {
    fn shape(
        &self,
        ctx: &RequestContext,
        upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        let _ = upstream;
        let mut url = Url::parse("http://upstream.local/")?;
        url.set_path(ctx.path.trim_start_matches('/'));
        url.set_query(ctx.query.as_deref());
        Ok(builder.shaped_request(
            url,
            ctx.method.clone(),
            ctx.downstream_headers.clone(),
            ctx.body_bytes.clone(),
        ))
    }
}
