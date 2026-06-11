#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_core::api_keys::principal_view::{
    DialectCache, ObservabilityHooksCache, PrincipalView, RouterPipelineCache,
};
use cc_lb_core::{
    ApiKeyAwareSignerFactory, DispatchError, DynamicViewBuilder, DynamicViewHolder,
    ErrorNormalizer, Lifecycle, LifecycleConfig, UpstreamDispatch,
};
use cc_lb_plugin_api::{
    DialectError, FilterError, FilterOutput, FilterPlugin, ObservabilityHook, Principal,
    RequestContext, RetryDecision, RouteDecision, RouteError, ShapedRequest, ShapedRequestBuilder,
    SignedRequest, Signer, SignerError, SignerFactory, SigningCapability, TerminalStrategy,
    Upstream, UpstreamCandidate, UpstreamDialect,
};
use cc_lb_storage_api::upstream::{UpstreamKind as StorageUpstreamKind, UpstreamRecord};
use http::{Response, StatusCode};
use serde_json::json;
use url::Url;
use uuid::Uuid;

use crate::common::{RecordingHook, TestAuthn, TestState};

const KEEP_FIRST_N_FILTER_ID: u128 = 0x0000_0000_0000_0000_0000_0000_0000_2701;
const DROP_ALL_FILTER_ID: u128 = 0x0000_0000_0000_0000_0000_0000_0000_2702;
const KEEP_ALL_FILTER_ID: u128 = 0x0000_0000_0000_0000_0000_0000_0000_2703;
const TRAPPING_FILTER_ID: u128 = 0x0000_0000_0000_0000_0000_0000_0000_2704;
const RUNTIME_ERROR_FILTER_ID: u128 = 0x0000_0000_0000_0000_0000_0000_0000_2705;
const INVALID_UNKNOWN_FILTER_ID: u128 = 0x0000_0000_0000_0000_0000_0000_0000_2706;
const INVALID_DUPLICATE_FILTER_ID: u128 = 0x0000_0000_0000_0000_0000_0000_0000_2707;
const INVALID_SUPERSET_FILTER_ID: u128 = 0x0000_0000_0000_0000_0000_0000_0000_2708;
const INVALID_UNKNOWN_UPSTREAM_ID: u128 = 0xffff_ffff_ffff_ffff_ffff_ffff_ffff_2700;

#[derive(Clone, Default)]
pub struct RouterLifecycleState {
    pub router_candidates: Arc<Mutex<Vec<Vec<Uuid>>>>,
    pub router_choice_names: Arc<Mutex<Vec<String>>>,
    pub dispatched_urls: Arc<Mutex<Vec<String>>>,
    pub dispatch_calls: Arc<Mutex<u64>>,
}

pub fn lifecycle_with_records(
    records: Vec<UpstreamRecord>,
    filters: Vec<Arc<dyn FilterPlugin>>,
    state: RouterLifecycleState,
) -> Lifecycle {
    let principal_view = principal_view_with_filters(filters);
    let authn = TestAuthn::with_principal_view(TestState::default(), principal_view.clone());
    let hook: Arc<dyn ObservabilityHook> = Arc::new(RecordingHook::default());
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(RecordingSignerFactory {
            choices: state.router_choice_names.clone(),
        }))
        .global_router(Arc::new(RecordingTerminalRouter {
            state: state.clone(),
        }))
        .dispatcher(Arc::new(RecordingDispatch {
            state: state.clone(),
        }))
        .global_observability_hooks(vec![hook])
        .error_normalizer(Arc::new(ErrorNormalizer::new()))
        .principal_view(principal_view)
        .upstream_records(records)
        .build();
    Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        Arc::new(DynamicViewHolder::new(view)),
        LifecycleConfig::default(),
    )
}

fn principal_view_with_filters(filters: Vec<Arc<dyn FilterPlugin>>) -> Arc<PrincipalView> {
    let pipeline = Arc::new(RouterPipelineCache {
        user_filters: filters,
        terminal: TerminalStrategy::FirstPick,
        instantiation_error: None,
    });
    let mut chains = HashMap::new();
    chains.insert(
        "principal-test".to_owned(),
        (
            Some(pipeline),
            ObservabilityHooksCache::Inherit,
            DialectCache::Inherit,
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

pub fn api_key_record(id: Uuid, name: &str, base_url: &str) -> UpstreamRecord {
    upstream_record(
        id,
        name,
        StorageUpstreamKind::AnthropicApiKey,
        Some(Url::parse(base_url).expect("test base URL parses")),
    )
}

pub fn anthropic_record(id: Uuid, name: &str) -> UpstreamRecord {
    upstream_record(id, name, StorageUpstreamKind::AnthropicApiKey, None)
}

fn upstream_record(
    id: Uuid,
    name: &str,
    kind: StorageUpstreamKind,
    base_url: Option<Url>,
) -> UpstreamRecord {
    UpstreamRecord {
        id,
        name: name.to_owned(),
        kind,
        base_url,
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

pub struct KeepFirstNFilter {
    pub n: usize,
}

impl FilterPlugin for KeepFirstNFilter {
    fn filter(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        let kept_upstream_ids = candidates
            .iter()
            .take(self.n)
            .map(|candidate| candidate.upstream_id)
            .collect::<Vec<_>>();
        Ok(filter_output(
            kept_upstream_ids,
            format!(
                "kept first {} of {} candidates",
                self.n.min(candidates.len()),
                candidates.len()
            ),
        ))
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::from_u128(KEEP_FIRST_N_FILTER_ID)
    }

    fn plugin_name(&self) -> &str {
        "keep-first-n"
    }
}

pub struct DropAllFilter;

impl FilterPlugin for DropAllFilter {
    fn filter(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        Ok(filter_output(
            Vec::new(),
            "dropped all candidates".to_owned(),
        ))
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::from_u128(DROP_ALL_FILTER_ID)
    }

    fn plugin_name(&self) -> &str {
        "drop-all"
    }
}

pub struct KeepAllFilter;

impl FilterPlugin for KeepAllFilter {
    fn filter(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        Ok(filter_output(
            candidate_ids(candidates),
            "kept all candidates".to_owned(),
        ))
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::from_u128(KEEP_ALL_FILTER_ID)
    }

    fn plugin_name(&self) -> &str {
        "keep-all"
    }
}

pub struct TrappingFilter;

impl FilterPlugin for TrappingFilter {
    fn filter(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        Err(FilterError::Trap {
            reason: "trapping filter fixture".to_owned(),
        })
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::from_u128(TRAPPING_FILTER_ID)
    }

    fn plugin_name(&self) -> &str {
        "trapping-filter"
    }
}

pub struct RuntimeErrorFilter;

impl FilterPlugin for RuntimeErrorFilter {
    fn filter(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        Err(FilterError::Runtime {
            reason: "runtime error filter fixture".to_owned(),
        })
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::from_u128(RUNTIME_ERROR_FILTER_ID)
    }

    fn plugin_name(&self) -> &str {
        "runtime-error-filter"
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvalidOutputKind {
    Unknown,
    Duplicate,
    Superset,
}

pub struct InvalidOutputFilter {
    pub kind: InvalidOutputKind,
}

impl FilterPlugin for InvalidOutputFilter {
    fn filter(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        let unknown = invalid_unknown_upstream_id();
        let kept_upstream_ids = match self.kind {
            InvalidOutputKind::Unknown => vec![unknown],
            InvalidOutputKind::Duplicate => candidates
                .first()
                .map(|candidate| vec![candidate.upstream_id, candidate.upstream_id])
                .unwrap_or_else(|| vec![unknown, unknown]),
            InvalidOutputKind::Superset => {
                let mut ids = candidate_ids(candidates);
                ids.push(unknown);
                ids
            }
        };
        Ok(filter_output(
            kept_upstream_ids,
            format!("invalid {:?} filter output", self.kind),
        ))
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::from_u128(match self.kind {
            InvalidOutputKind::Unknown => INVALID_UNKNOWN_FILTER_ID,
            InvalidOutputKind::Duplicate => INVALID_DUPLICATE_FILTER_ID,
            InvalidOutputKind::Superset => INVALID_SUPERSET_FILTER_ID,
        })
    }

    fn plugin_name(&self) -> &str {
        match self.kind {
            InvalidOutputKind::Unknown => "invalid-output-unknown",
            InvalidOutputKind::Duplicate => "invalid-output-duplicate",
            InvalidOutputKind::Superset => "invalid-output-superset",
        }
    }
}

pub fn invalid_unknown_upstream_id() -> Uuid {
    Uuid::from_u128(INVALID_UNKNOWN_UPSTREAM_ID)
}

fn filter_output(kept_upstream_ids: Vec<Uuid>, reason: String) -> FilterOutput {
    FilterOutput {
        kept_upstream_ids,
        reason,
        per_candidate_reasons: Vec::new(),
    }
}

fn candidate_ids(candidates: &[UpstreamCandidate]) -> Vec<Uuid> {
    candidates
        .iter()
        .map(|candidate| candidate.upstream_id)
        .collect()
}

struct RecordingTerminalRouter {
    state: RouterLifecycleState,
}

#[allow(deprecated)]
impl cc_lb_plugin_api::RouterPlugin for RecordingTerminalRouter {
    fn route(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError> {
        self.state
            .router_candidates
            .lock()
            .expect("router candidates lock")
            .push(candidate_ids(candidates));
        let candidate = candidates.first().ok_or_else(|| RouteError::NoRoute {
            reason: "no candidates".to_owned(),
        })?;
        Ok(RouteDecision {
            upstream_id: Some(candidate.upstream_id),
            upstream: Upstream::AnthropicDirect,
            dialect: Arc::new(UniversalDialect),
        })
    }
}

struct RecordingSignerFactory {
    choices: Arc<Mutex<Vec<String>>>,
}

impl ApiKeyAwareSignerFactory for RecordingSignerFactory {
    fn with_router_choice(
        &self,
        _api_key: String,
        router_chosen_upstream_name: String,
    ) -> Arc<dyn SignerFactory> {
        self.choices
            .lock()
            .expect("router choices lock")
            .push(router_chosen_upstream_name);
        Arc::new(RecordingSignerFactoryInner)
    }
}

struct RecordingSignerFactoryInner;

#[async_trait]
impl SignerFactory for RecordingSignerFactoryInner {
    async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        Ok(Arc::new(RecordingSigner))
    }
}

struct RecordingSigner;

#[async_trait]
impl Signer for RecordingSigner {
    async fn sign(
        &self,
        shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(&self, _err: &cc_lb_plugin_api::UpstreamError) -> RetryDecision {
        RetryDecision::Fail
    }
}

struct RecordingDispatch {
    state: RouterLifecycleState,
}

#[async_trait]
impl UpstreamDispatch for RecordingDispatch {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        self.state
            .dispatched_urls
            .lock()
            .expect("dispatched URLs lock")
            .push(request.url().to_string());
        *self
            .state
            .dispatch_calls
            .lock()
            .expect("dispatch calls lock") += 1;
        let mut response = Response::new(Body::from(Bytes::from(
            json!({"type":"message","usage":{"input_tokens":1,"output_tokens":1}}).to_string(),
        )));
        *response.status_mut() = StatusCode::OK;
        Ok(response)
    }
}

struct UniversalDialect;

impl UpstreamDialect for UniversalDialect {
    fn shape(
        &self,
        ctx: &RequestContext,
        upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        let _ = upstream;
        let mut url = Url::parse("https://api.anthropic.com/")?;
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
