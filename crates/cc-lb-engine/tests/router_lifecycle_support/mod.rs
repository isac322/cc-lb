use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_control::{DynamicViewBuilder, DynamicViewHolder};
use cc_lb_domain::Upstream;
use cc_lb_engine::{
    ApiKeyAwareSignerFactory, DispatchError, Lifecycle, LifecycleConfig, UpstreamDispatch,
};
use cc_lb_storage_api::upstream::{UpstreamKind as StorageUpstreamKind, UpstreamRecord};
use cc_lb_upstream::{
    RetryDecision, ShapedRequest, SignedRequest, Signer, SignerError, SignerFactory,
    SigningCapability,
};
use http::{Response, StatusCode};
use serde_json::json;
use url::Url;
use uuid::Uuid;

use crate::common::{TestAuthn, TestState};

#[derive(Clone, Default)]
pub struct RouterLifecycleState {
    pub router_choice_names: Arc<Mutex<Vec<String>>>,
    pub dispatched_urls: Arc<Mutex<Vec<String>>>,
    pub dispatch_calls: Arc<Mutex<u64>>,
}

pub fn lifecycle_with_records(
    records: Vec<UpstreamRecord>,
    state: RouterLifecycleState,
) -> Lifecycle {
    let authn = TestAuthn::new(TestState::default());
    let dispatcher = Arc::new(RecordingDispatch {
        state: state.clone(),
    });
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(RecordingSignerFactory {
            choices: state.router_choice_names.clone(),
        }))
        .principal_view(authn.principal_view.clone())
        .upstream_records(records)
        .build();
    Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        Arc::new(DynamicViewHolder::new(view)),
        dispatcher,
        LifecycleConfig::default(),
        Arc::new(cc_lb_clock::SystemClock),
    )
}

pub fn api_key_record(id: Uuid, name: &str, base_url: &str) -> UpstreamRecord {
    UpstreamRecord {
        id,
        name: name.to_owned(),
        kind: StorageUpstreamKind::AnthropicApiKey,
        base_url: Some(Url::parse(base_url).expect("test base URL parses")),
        enabled: true,
        oauth_credentials: None,
        oauth_never_refresh: false,
        api_key_ciphertext: Some(Vec::new()),
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs: None,
        revision: 1,
        oauth_token_generation: 0,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        warmup_enabled: false,
        warmup_dialect_plugin: None,
        last_warmup_at_unix_secs: None,
    }
}

struct RecordingSignerFactory {
    choices: Arc<Mutex<Vec<String>>>,
}

impl ApiKeyAwareSignerFactory for RecordingSignerFactory {
    fn with_router_choice(&self, router_chosen_upstream_name: String) -> Arc<dyn SignerFactory> {
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

    async fn on_unauthorized(&self, _err: &cc_lb_upstream::UpstreamError) -> RetryDecision {
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
