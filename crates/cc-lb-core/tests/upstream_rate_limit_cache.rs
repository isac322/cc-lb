mod common;

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_core::api_keys::principal_view::PrincipalView;
use cc_lb_core::{
    ApiKeyAwareSignerFactory, Body, DispatchError, DynamicView, DynamicViewBuilder,
    ErrorNormalizer, RequestKind, UpstreamDispatch, UpstreamRateLimitCache, build_candidates,
};
use cc_lb_plugin_api::{
    ObservabilityError, ObservabilityHook, ObserveEvent, Principal, RateLimitKind, RequestContext,
    RetryDecision, RouteDecision, RouteError, RouterPlugin, ShapedRequest, SignedRequest, Signer,
    SignerError, SignerFactory, SigningCapability, Upstream, UpstreamCandidate, UpstreamError,
};
use cc_lb_storage_api::principal::{PrincipalKind, PrincipalRecord};
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamRecord};
use cc_lb_storage_api::{RateLimitKind as StoredRateLimitKind, UpstreamRateLimitObservationRecord};
use http::header::{CONTENT_TYPE, HeaderName};
use http::{HeaderMap, HeaderValue, Response, StatusCode};
use parking_lot::RwLock;
use serde_json::json;
use uuid::Uuid;

use common::{
    DispatchMode, MockDispatch, RecordingHook, TestAuthn, TestState, collect_body, lifecycle_with,
    messages_request,
};

#[test]
fn build_candidates_populates_observations_from_dynamic_view_cache() {
    let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000101").unwrap();
    let other_upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000102").unwrap();
    let cache = Arc::new(RwLock::new(UpstreamRateLimitCache::from_records(
        vec![
            observation_record(upstream_id, StoredRateLimitKind::Requests, 77, 1234),
            observation_record(other_upstream_id, StoredRateLimitKind::Tokens, 88, 1235),
        ],
        1236,
    )));
    let view = test_view(
        vec![principal("principal", Vec::new())],
        vec![upstream(upstream_id), upstream(other_upstream_id)],
        cache,
    );

    let candidates = build_candidates(&view, "principal", RequestKind::AnthropicMessages);

    let candidate = candidates
        .iter()
        .find(|candidate| candidate.upstream_id == upstream_id)
        .expect("candidate exists");
    assert_eq!(candidate.observed_at_unix_secs, 1236);
    assert_eq!(candidate.observed_rate_limits.len(), 1);
    assert_eq!(
        candidate.observed_rate_limits[0].kind,
        RateLimitKind::Requests
    );
    assert_eq!(candidate.observed_rate_limits[0].remaining, Some(77));
}

#[tokio::test]
async fn lifecycle_updates_dynamic_view_cache_when_headers_are_observed() {
    let state = TestState::default();
    let lifecycle = lifecycle_with(
        TestAuthn::new(state.clone()),
        MockDispatch {
            state,
            mode: DispatchMode::HeadersOk(rate_limit_headers(321)),
        },
        Arc::new(RecordingHook::default()),
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        )))
        .await
        .expect("lifecycle handles request");
    let (status, _headers, _body) = collect_body(response).await;

    assert_eq!(status, StatusCode::OK);
    let view = lifecycle.dynamic_view().load();
    let cache = view.upstream_rate_limit_cache.read();
    let snapshots = cache
        .snapshots
        .get(&default_upstream_id())
        .expect("cache contains selected upstream");
    assert!(cache.updated_at_unix_secs > 0);
    assert!(snapshots.iter().any(|snapshot| {
        snapshot.kind == RateLimitKind::Requests && snapshot.remaining == Some(321)
    }));
}

fn test_view(
    principals: Vec<PrincipalRecord>,
    upstreams: Vec<UpstreamRecord>,
    cache: Arc<RwLock<UpstreamRateLimitCache>>,
) -> Arc<DynamicView> {
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
        .upstream_rate_limit_cache(cache)
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
    }
}

fn upstream(id: Uuid) -> UpstreamRecord {
    UpstreamRecord {
        id,
        name: format!("upstream-{id}"),
        kind: UpstreamKind::AnthropicApiKey,
        base_url: None,
        enabled: true,
        oauth_credentials: None,
        api_key_ciphertext: None,
        refresh_lease_holder: None,
        refresh_lease_until_unix_secs: None,
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs: None,
        shape_plugin: None,
        revision: 1,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
    }
}

fn observation_record(
    upstream_id: Uuid,
    kind: StoredRateLimitKind,
    remaining: u64,
    observed_at_unix_secs: u64,
) -> UpstreamRateLimitObservationRecord {
    UpstreamRateLimitObservationRecord {
        upstream_id,
        window: "default".to_owned(),
        kind,
        limit: Some(1000),
        remaining: Some(remaining),
        reset: Some("2026-05-29T00:00:00Z".to_owned()),
        observed_at_unix_secs,
    }
}

fn rate_limit_headers(requests_remaining: u64) -> HeaderMap {
    let mut headers = HeaderMap::new();
    insert_header(&mut headers, "anthropic-ratelimit-requests-limit", "1000");
    insert_header(
        &mut headers,
        "anthropic-ratelimit-requests-remaining",
        &requests_remaining.to_string(),
    );
    insert_header(
        &mut headers,
        "anthropic-ratelimit-requests-reset",
        "2026-05-29T00:00:00Z",
    );
    headers
}

fn insert_header(headers: &mut HeaderMap, name: &str, value: &str) {
    headers.insert(
        HeaderName::from_bytes(name.as_bytes()).expect("test header name parses"),
        HeaderValue::from_str(value).expect("test header value parses"),
    );
}

fn default_upstream_id() -> Uuid {
    Uuid::parse_str("00000000-0000-0000-0000-000000000001").expect("default upstream id parses")
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
        panic!("cache candidate test must not route")
    }
}

struct TestDispatcher;

#[async_trait]
impl UpstreamDispatch for TestDispatcher {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(Bytes::from(
                json!({"type":"message","usage":{"input_tokens":1,"output_tokens":1}}).to_string(),
            )))
            .expect("test response builds"))
    }
}

struct TestHook;

impl ObservabilityHook for TestHook {
    fn observe(&self, _event: ObserveEvent) -> Result<(), ObservabilityError> {
        Ok(())
    }
}
