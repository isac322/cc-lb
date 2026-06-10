use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_core::api_keys::principal_view::PrincipalView;
use cc_lb_core::{
    ApiKeyAwareSignerFactory, Body, DispatchError, DynamicView, DynamicViewBuilder,
    ErrorNormalizer, RequestKind, UpstreamDispatch, build_candidates,
};
use cc_lb_plugin_api::{
    ObservabilityError, ObservabilityHook, ObserveEvent, Principal, RequestContext, RetryDecision,
    RouteDecision, RouteError, RouterPlugin, ShapedRequest, SignedRequest, Signer, SignerError,
    SignerFactory, SigningCapability, Upstream, UpstreamCandidate, UpstreamError,
};
use cc_lb_storage_api::principal::{PrincipalKind, PrincipalRecord};
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamRecord};
use http::{Response, StatusCode};
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

    let limited = build_candidates(&view, "limited", RequestKind::AnthropicMessages, "", &[]);
    assert_eq!(
        candidate_ids(&limited),
        vec![allowed_low, allowed_mid, allowed_high]
    );

    let all = build_candidates(&view, "all", RequestKind::AnthropicMessages, "", &[]);
    assert_eq!(
        candidate_ids(&all),
        vec![not_allowed, allowed_low, allowed_mid, allowed_high]
    );

    assert!(build_candidates(&view, "missing", RequestKind::AnthropicMessages, "", &[]).is_empty());
}

fn test_view(principals: Vec<PrincipalRecord>, upstreams: Vec<UpstreamRecord>) -> Arc<DynamicView> {
    DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(TestSignerFactory))
        .global_router(Arc::new(TestRouter))
        .dispatcher(Arc::new(TestDispatcher))
        .global_observability_hooks(vec![Arc::new(TestHook)])
        .error_normalizer(Arc::new(ErrorNormalizer::new()))
        .principal_view(Arc::new(PrincipalView::from_db(
            &principals,
            std::collections::HashMap::new(),
        )))
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
        refresh_lease_holder: None,
        refresh_lease_until_unix_secs: None,
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs,
        revision: 1,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
    }
}

fn candidate_ids(candidates: &[UpstreamCandidate]) -> Vec<Uuid> {
    candidates
        .iter()
        .map(|candidate| candidate.upstream_id)
        .collect()
}
