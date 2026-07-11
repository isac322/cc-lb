use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_engine::{ApiKeyAwareSignerFactory, DispatchError, UpstreamDispatch};
use cc_lb_plugin_api::{
    Principal, RequestContext, RetryDecision, RouteDecision, RouteError, RouterPlugin,
    ShapedRequest, SignedRequest, Signer, SignerError, SignerFactory, SigningCapability, Upstream,
    UpstreamCandidate,
};
use cc_lb_storage_api::principal::{Limit, PrincipalKind, PrincipalRecord};
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamRecord, UpstreamWarmupDialectPlugin};
use cc_lb_storage_api::{CacheKeepaliveConfig, ClassifierConfig};
use http::{HeaderValue, Response, StatusCode};
use serde_json::{Value, json};
use url::Url;
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct SignerCall {
    pub(crate) upstream_name: String,
}

pub(crate) struct RecordingSignerFactory {
    pub(crate) label: &'static str,
    pub(crate) calls: Arc<Mutex<Vec<SignerCall>>>,
}

impl ApiKeyAwareSignerFactory for RecordingSignerFactory {
    fn with_router_choice(
        &self,
        _api_key: String,
        router_chosen_upstream_name: String,
    ) -> Arc<dyn SignerFactory> {
        self.calls
            .lock()
            .expect("signer log lock")
            .push(SignerCall {
                upstream_name: router_chosen_upstream_name,
            });
        Arc::new(RecordingSigner { label: self.label })
    }
}

struct RecordingSigner {
    label: &'static str,
}

#[async_trait]
impl SignerFactory for RecordingSigner {
    async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        Ok(Arc::new(RecordingSigner { label: self.label }))
    }
}

#[async_trait]
impl Signer for RecordingSigner {
    async fn sign(
        &self,
        mut shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        let value = HeaderValue::from_str(&format!("sk-ant-{}", self.label)).map_err(|source| {
            SignerError::SigningFailed {
                reason: source.to_string(),
            }
        })?;
        shaped.headers_mut().insert("x-api-key", value);
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(&self, _err: &cc_lb_plugin_api::UpstreamError) -> RetryDecision {
        RetryDecision::Fail
    }
}

pub(crate) struct AgentTurnDispatch;

#[async_trait]
impl UpstreamDispatch for AgentTurnDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        Ok(json_response(json!({
            "type": "message",
            "id": "msg_agent_turn",
            "content": [{"type": "tool_use", "id": "toolu_1", "name": "Bash", "input": {}}],
            "stop_reason": "tool_use",
            "usage": {"input_tokens": 10, "output_tokens": 2}
        })))
    }
}

pub(crate) struct FirstRouter;

impl RouterPlugin for FirstRouter {
    fn route(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError> {
        Err(RouteError::NoRoute {
            reason: "test uses terminal routing".to_owned(),
        })
    }
}

pub(crate) fn principal_with_keepalive() -> PrincipalRecord {
    PrincipalRecord {
        id: Uuid::new_v4(),
        name: "principal-test".to_owned(),
        kind: PrincipalKind::Machine,
        allowed_models: vec!["*".to_owned()],
        allowed_upstreams: Vec::new(),
        default_limits: Vec::<Limit>::new(),
        enabled: true,
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs: None,
        revision: 1,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        router_terminal_strategy: Default::default(),
        cache_keepalive: Some(CacheKeepaliveConfig {
            enabled: true,
            refresh_lead_time_5m_secs: 299,
            refresh_lead_time_1h_secs: 3599,
            max_refreshes_per_session: 3,
            max_total_duration_secs: 60,
            snapshot_max_bytes: 524_288,
            classifier: ClassifierConfig::default(),
        }),
    }
}

pub(crate) fn upstream_record(id: Uuid, name: &str, base_url: &str) -> UpstreamRecord {
    UpstreamRecord {
        id,
        name: name.to_owned(),
        kind: UpstreamKind::AnthropicOauth,
        base_url: Some(Url::parse(base_url).expect("base URL parses")),
        enabled: true,
        oauth_credentials: None,
        api_key_ciphertext: None,
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs: None,
        revision: 1,
        oauth_token_generation: 0,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        warmup_enabled: false,
        warmup_dialect_plugin: Option::<UpstreamWarmupDialectPlugin>::None,
        last_warmup_at_unix_secs: None,
    }
}

fn json_response(body: Value) -> Response<Body> {
    let mut response = Response::new(Body::from(Bytes::from(body.to_string())));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}

pub(crate) async fn settle() {
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}
