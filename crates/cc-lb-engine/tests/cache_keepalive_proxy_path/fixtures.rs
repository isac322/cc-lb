use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_control::api_keys::principal_view::{DialectCache, PrincipalView, ShapePluginCache};
use cc_lb_control::{DynamicViewBuilder, DynamicViewHolder};
use cc_lb_domain::{Principal, Upstream};
use cc_lb_engine::cache_keepalive::{
    CacheKeepaliveCancelRequest, CacheKeepaliveEnqueueError, CacheKeepaliveEnqueueRequest,
    CacheKeepaliveEnqueuer, CacheKeepaliveNotTrackedRequest,
};
use cc_lb_engine::{
    ApiKeyAwareSignerFactory, DispatchError, Lifecycle, LifecycleConfig, UpstreamDispatch,
};
use cc_lb_storage_api::principal::{Limit, PrincipalKind, PrincipalRecord};
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamRecord, UpstreamWarmupDialectPlugin};
use cc_lb_storage_api::{CacheKeepaliveConfig, ClassifierConfig};
use cc_lb_upstream::{
    DialectError, DialectShapeContext, RetryDecision, ShapedRequest, ShapedRequestBuilder,
    SignedRequest, Signer, SignerError, SignerFactory, SigningCapability, UpstreamDialect,
};
use http::{HeaderValue, Response, StatusCode};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use url::Url;
use uuid::Uuid;

use super::common::{TestAuthn, TestState, collect_body, messages_request};

#[derive(Clone)]
pub(crate) struct SignerCall {
    pub(crate) upstream_name: String,
}

pub(crate) struct RecordingSignerFactory {
    pub(crate) label: &'static str,
    pub(crate) calls: Arc<Mutex<Vec<SignerCall>>>,
}

impl ApiKeyAwareSignerFactory for RecordingSignerFactory {
    fn with_router_choice(&self, router_chosen_upstream_name: String) -> Arc<dyn SignerFactory> {
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

    async fn on_unauthorized(&self, _err: &cc_lb_upstream::UpstreamError) -> RetryDecision {
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
            "usage": {
                "input_tokens": 10,
                "cache_read_input_tokens": 12_000,
                "output_tokens": 2
            }
        })))
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
            min_cache_tokens: 10_000,
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
        oauth_never_refresh: false,
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
        workspace_id: None,
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

struct ChannelEnqueuer {
    sender: mpsc::UnboundedSender<CacheKeepaliveEnqueueRequest>,
}

#[async_trait]
impl CacheKeepaliveEnqueuer for ChannelEnqueuer {
    async fn enqueue_cache_keepalive(
        &self,
        request: CacheKeepaliveEnqueueRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        self.sender
            .send(request)
            .map_err(|error| CacheKeepaliveEnqueueError(error.to_string()))
    }

    async fn cancel_cache_keepalive(
        &self,
        _request: CacheKeepaliveCancelRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        Ok(())
    }

    async fn record_cache_keepalive_not_tracked(
        &self,
        _request: CacheKeepaliveNotTrackedRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        Ok(())
    }
}

pub(crate) async fn enqueue_through_lifecycle(
    body: &Value,
    dialect: Option<Arc<dyn UpstreamDialect>>,
) -> CacheKeepaliveEnqueueRequest {
    let upstream_id = Uuid::from_u128(0xaa);
    let upstream = upstream_record(upstream_id, "primary", "http://keepalive.local/");
    let mut principal = principal_with_keepalive();
    let config = principal
        .cache_keepalive
        .as_mut()
        .expect("keepalive config");
    config.refresh_lead_time_5m_secs = 30;
    config.refresh_lead_time_1h_secs = 300;
    let chains = dialect.map_or_else(HashMap::new, |dialect| {
        HashMap::from([(
            principal.name.clone(),
            (None, DialectCache::Explicit(ShapePluginCache { dialect })),
        )])
    });
    let principal_view = Arc::new(PrincipalView::from_db(&[principal], chains));
    let authn = TestAuthn::with_principal_view(TestState::default(), Arc::clone(&principal_view));
    let holder = Arc::new(DynamicViewHolder::new(
        DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(RecordingSignerFactory {
                label: "initial",
                calls: Arc::new(Mutex::new(Vec::new())),
            }))
            .principal_view(principal_view)
            .upstream_records(vec![upstream])
            .build(),
    ));
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let lifecycle = Lifecycle::new_with_dynamic_view(
        authn.authn,
        holder,
        Arc::new(AgentTurnDispatch),
        LifecycleConfig::default(),
        Arc::new(cc_lb_clock::SystemClock),
    )
    .with_cache_keepalive_enqueuer(Arc::new(ChannelEnqueuer { sender }));
    let request = messages_request(Bytes::from(
        serde_json::to_vec(body).expect("request serializes"),
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("request succeeds");
    let (status, _, _) = collect_body(response).await;
    assert_eq!(status, StatusCode::OK);

    tokio::time::timeout(Duration::from_secs(1), receiver.recv())
        .await
        .expect("completed agent turn enqueues keepalive before the deadline")
        .expect("enqueue observer stays open")
}

pub(crate) struct FiveMinuteFinalBreakpointDialect;

impl UpstreamDialect for FiveMinuteFinalBreakpointDialect {
    fn shape(
        &self,
        context: &DialectShapeContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        let mut body: Value =
            serde_json::from_slice(&context.body_bytes).expect("fixture request is JSON");
        body["messages"][0]["content"][0]["cache_control"]["ttl"] = json!("5m");
        let mut url = Url::parse("http://keepalive.local/").expect("test URL parses");
        url.set_path(context.path.trim_start_matches('/'));
        url.set_query(context.query.as_deref());
        Ok(builder.shaped_request(
            url,
            context.method.clone(),
            context.downstream_headers.clone(),
            Bytes::from(serde_json::to_vec(&body).expect("shaped body serializes")),
        ))
    }
}
