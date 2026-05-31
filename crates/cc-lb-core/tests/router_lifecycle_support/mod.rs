#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_core::{
    ApiKeyAwareSignerFactory, DispatchError, DynamicViewBuilder, DynamicViewHolder,
    ErrorNormalizer, Lifecycle, LifecycleConfig, UpstreamDispatch,
};
use cc_lb_plugin_api::{
    DialectError, ObservabilityHook, Principal, RequestContext, RetryDecision, RouteDecision,
    RouteError, RouterPlugin, ShapedRequest, ShapedRequestBuilder, SignedRequest, Signer,
    SignerError, SignerFactory, SigningCapability, Upstream, UpstreamCandidate, UpstreamDialect,
};
use cc_lb_storage_api::upstream::{UpstreamKind as StorageUpstreamKind, UpstreamRecord};
use http::{Response, StatusCode};
use serde_json::json;
use url::Url;
use uuid::Uuid;

use crate::common::{RecordingHook, TestAuthn, TestState};

#[derive(Clone, Default)]
pub struct RouterLifecycleState {
    pub router_candidates: Arc<Mutex<Vec<Vec<Uuid>>>>,
    pub router_choice_names: Arc<Mutex<Vec<String>>>,
    pub dispatched_urls: Arc<Mutex<Vec<String>>>,
    pub dispatch_calls: Arc<Mutex<u64>>,
}

pub fn lifecycle_with_records(
    records: Vec<UpstreamRecord>,
    router: Arc<dyn RouterPlugin>,
    state: RouterLifecycleState,
) -> Lifecycle {
    let authn = TestAuthn::new(TestState::default());
    let hook: Arc<dyn ObservabilityHook> = Arc::new(RecordingHook::default());
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(RecordingSignerFactory {
            choices: state.router_choice_names.clone(),
        }))
        .global_router(router)
        .dispatcher(Arc::new(RecordingDispatch {
            state: state.clone(),
        }))
        .global_observability_hooks(vec![hook])
        .error_normalizer(Arc::new(ErrorNormalizer::new()))
        .principal_view(authn.principal_view.clone())
        .upstream_records(records)
        .build();
    Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        Arc::new(DynamicViewHolder::new(view)),
        LifecycleConfig::default(),
    )
}

pub fn custom_record(id: Uuid, name: &str, base_url: &str) -> UpstreamRecord {
    upstream_record(
        id,
        name,
        StorageUpstreamKind::Custom,
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

pub struct SelectingRouter {
    pub selected_id: Option<Uuid>,
    pub state: RouterLifecycleState,
    pub plugin_upstream: Upstream,
}

impl RouterPlugin for SelectingRouter {
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
            .push(
                candidates
                    .iter()
                    .map(|candidate| candidate.upstream_id)
                    .collect(),
            );
        Ok(RouteDecision {
            upstream_id: self.selected_id,
            upstream: self.plugin_upstream.clone(),
            dialect: Arc::new(UniversalDialect),
        })
    }
}

pub struct RejectingEmptyRouter {
    pub state: RouterLifecycleState,
}

impl RouterPlugin for RejectingEmptyRouter {
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
            .push(
                candidates
                    .iter()
                    .map(|candidate| candidate.upstream_id)
                    .collect(),
            );
        Err(RouteError::NoRoute {
            reason: "no candidates".to_owned(),
        })
    }
}

pub fn plugin_upstream(base_url: &str) -> Upstream {
    Upstream::CustomAnthropicSpec {
        base_url: Url::parse(base_url).expect("test plugin URL parses"),
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
        let mut url = match upstream {
            Upstream::AnthropicDirect => Url::parse("https://api.anthropic.com/")?,
            Upstream::CustomAnthropicSpec { base_url } => base_url.clone(),
        };
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
