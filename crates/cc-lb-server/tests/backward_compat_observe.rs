use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_config::{
    AuthStrategy, Config, DownstreamAuthMode, NoneModeConfig, NoneModeUpstreamKind, PluginRef,
    PrincipalSpec, UpstreamKind, UpstreamSpec,
};
use cc_lb_core::api_keys::builtin_authn::BuiltinAuthn;
use cc_lb_core::api_keys::key_store::KeyStore;
use cc_lb_core::api_keys::principal_view::PrincipalView;
use cc_lb_core::{
    ApiKeyAwareSignerFactory, Body, DispatchError, Lifecycle, LifecycleConfig, UpstreamDispatch,
};
use cc_lb_plugin_api::{
    ObservabilityError, ObservabilityHook, ObserveEvent, PrincipalKind, RetryDecision,
    ShapedRequest, SignedRequest, Signer, SignerError, SignerFactory, SigningCapability, Upstream,
};
use cc_lb_server::builtins::BuiltinRouter;
use cc_lb_storage_redb::{RedbManagedKeyStore, Storage};
use http::header::CONTENT_TYPE;
use http::{HeaderValue, Method, Request, Response, StatusCode};
use http_body_util::BodyExt;
use serde_json::{Value, json};

const EXPECTED_BYTES: &[u8] =
    include_bytes!("fixtures/per_principal_plugins/backward_compat_observe_stream.json");

#[tokio::test]
async fn zero_principal_plugins_observe_stream_matches_global_only_golden()
-> Result<(), Box<dyn std::error::Error>> {
    let zero_principal_config = observe_config(false);
    assert!(zero_principal_config.principals.is_empty());
    assert_eq!(zero_principal_config.plugins.observability_hooks.len(), 2);

    let zero_principal_bytes = observe_stream_bytes(zero_principal_config).await?;
    assert_eq!(zero_principal_bytes.as_slice(), EXPECTED_BYTES);

    let inherited_principal_bytes = observe_stream_bytes(observe_config(true)).await?;
    assert_eq!(inherited_principal_bytes, zero_principal_bytes);
    assert_eq!(inherited_principal_bytes.as_slice(), EXPECTED_BYTES);

    Ok(())
}

async fn observe_stream_bytes(config: Config) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let hooks = global_hooks_from_config(&config, events.clone());
    let view = PrincipalView::from_config(&config, HashMap::new())?;
    let storage_dir = tempfile::tempdir()?;
    let storage = Arc::new(Storage::open(
        &storage_dir.path().join("backward-compat-observe.redb"),
        [20; 32],
    )?);
    let authn = Arc::new(BuiltinAuthn::new(
        config.downstream_auth.mode.clone(),
        config.downstream_auth.none_mode.clone(),
        Arc::new(KeyStore::new(Arc::new(RedbManagedKeyStore::new(storage)))),
    ));
    let lifecycle = Lifecycle::new(
        authn,
        view,
        Arc::new(NoopApiKeySignerFactory),
        Arc::new(BuiltinRouter::new(&config)?),
        Arc::new(DeterministicDispatch::new()),
        hooks,
        LifecycleConfig::default(),
    );

    let requests = [
        SyntheticRequest {
            request_id: "raw-request-a",
            user_agent: "cc-lb-t20/1",
            body: br#"{"model":"claude-test","messages":[],"max_tokens":16}"#,
        },
        SyntheticRequest {
            request_id: "raw-request-b",
            user_agent: "cc-lb-t20/2",
            body: br#"{"model":"claude-test","messages":[{"role":"user","content":"ping"}],"max_tokens":32}"#,
        },
    ];

    for request in requests {
        let response = lifecycle.handle(messages_request(request)).await?;
        let (status, _body) = collect_body(response).await;
        assert_eq!(status, StatusCode::OK);
    }

    let events = events.lock().expect("events lock").clone();
    Ok(canonical_event_bytes(&events))
}

fn observe_config(with_inheriting_principal: bool) -> Config {
    let mut config = Config::default();
    config.upstreams.insert(
        "fake".to_owned(),
        UpstreamSpec {
            kind: UpstreamKind::Custom,
            base_url: Some("http://upstream.local/".parse().expect("test URL parses")),
            region: None,
            project: None,
            auth_strategy: AuthStrategy::ApiKey,
            credentials_ref: None,
        },
    );
    config.downstream_auth.mode = DownstreamAuthMode::None;
    config.downstream_auth.none_mode = Some(NoneModeConfig {
        principal_id: "principal-test".to_owned(),
        upstream_kind: NoneModeUpstreamKind::AnthropicKey,
        upstream_credential_ref: "test-upstream".to_owned(),
    });
    config.plugins.router_plugin = None;
    config.plugins.observability_hooks =
        vec![plugin_ref("global-hook-1"), plugin_ref("global-hook-2")];
    if with_inheriting_principal {
        config.principals.insert(
            "principal-test".to_owned(),
            PrincipalSpec {
                allowed_models: vec!["*".to_owned()],
                ..PrincipalSpec::default()
            },
        );
    }
    config
}

fn plugin_ref(name: &str) -> PluginRef {
    PluginRef {
        name: name.to_owned(),
        ..PluginRef::default()
    }
}

fn global_hooks_from_config(
    config: &Config,
    events: Arc<Mutex<Vec<RecordedEvent>>>,
) -> Vec<Arc<dyn ObservabilityHook>> {
    config
        .plugins
        .observability_hooks
        .iter()
        .map(|plugin| {
            Arc::new(RecordingHook {
                name: plugin.name.clone(),
                events: events.clone(),
            }) as Arc<dyn ObservabilityHook>
        })
        .collect()
}

struct SyntheticRequest {
    request_id: &'static str,
    user_agent: &'static str,
    body: &'static [u8],
}

fn messages_request(request: SyntheticRequest) -> Request<Bytes> {
    Request::builder()
        .method(Method::POST)
        .uri("/v1/messages")
        .header("request-id", request.request_id)
        .header("user-agent", request.user_agent)
        .header("anthropic-version", "2023-06-01")
        .body(Bytes::from_static(request.body))
        .expect("test request builds")
}

async fn collect_body(response: Response<Body>) -> (StatusCode, Bytes) {
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("test body collects")
        .to_bytes();
    (status, body)
}

#[derive(Clone)]
struct RecordedEvent {
    hook: String,
    event: ObserveEvent,
}

struct RecordingHook {
    name: String,
    events: Arc<Mutex<Vec<RecordedEvent>>>,
}

impl ObservabilityHook for RecordingHook {
    fn observe(&self, event: ObserveEvent) -> Result<(), ObservabilityError> {
        self.events
            .lock()
            .map(|mut events| {
                events.push(RecordedEvent {
                    hook: self.name.clone(),
                    event,
                });
            })
            .map_err(|_| ObservabilityError::Dropped {
                reason: "lock poisoned".to_owned(),
            })
    }
}

#[derive(Clone, Copy)]
struct ResponseSpec {
    input_tokens: u64,
    output_tokens: u64,
}

struct DeterministicDispatch {
    responses: Mutex<VecDeque<ResponseSpec>>,
}

impl DeterministicDispatch {
    fn new() -> Self {
        Self {
            responses: Mutex::new(VecDeque::from([
                ResponseSpec {
                    input_tokens: 3,
                    output_tokens: 5,
                },
                ResponseSpec {
                    input_tokens: 8,
                    output_tokens: 13,
                },
            ])),
        }
    }
}

#[async_trait]
impl UpstreamDispatch for DeterministicDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let spec = self
            .responses
            .lock()
            .expect("responses lock")
            .pop_front()
            .expect("synthetic response is queued");
        Ok(json_response(spec))
    }
}

fn json_response(spec: ResponseSpec) -> Response<Body> {
    let body = json!({
        "type": "message",
        "usage": {
            "input_tokens": spec.input_tokens,
            "output_tokens": spec.output_tokens,
        },
    });
    let mut response = Response::new(Body::from(Bytes::from(body.to_string())));
    *response.status_mut() = StatusCode::OK;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}

struct NoopApiKeySignerFactory;

impl ApiKeyAwareSignerFactory for NoopApiKeySignerFactory {
    fn with_api_key(&self, _api_key: String) -> Arc<dyn SignerFactory> {
        Arc::new(NoopSignerFactory)
    }
}

struct NoopSignerFactory;

#[async_trait]
impl SignerFactory for NoopSignerFactory {
    async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        Ok(Arc::new(NoopSigner))
    }
}

struct NoopSigner;

#[async_trait]
impl Signer for NoopSigner {
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

fn canonical_event_bytes(events: &[RecordedEvent]) -> Vec<u8> {
    let mut request_ids = HashMap::new();
    let mut next_request_id = 0_usize;
    let values = events
        .iter()
        .map(|record| {
            json!([
                record.hook,
                canonical_event(&record.event, &mut request_ids, &mut next_request_id)
            ])
        })
        .collect::<Vec<_>>();
    let mut bytes = serde_json::to_string_pretty(&values)
        .expect("canonical events serialize")
        .into_bytes();
    bytes.push(b'\n');
    bytes
}

fn canonical_event(
    event: &ObserveEvent,
    request_ids: &mut HashMap<String, String>,
    next_request_id: &mut usize,
) -> Value {
    match event {
        ObserveEvent::RequestStarted {
            request_id,
            downstream_user_agent,
        } => json!([
            "RequestStarted",
            normalized_request_id(request_ids, next_request_id, request_id),
            downstream_user_agent
        ]),
        ObserveEvent::AuthnComplete { principal_id, kind } => {
            json!(["AuthnComplete", principal_id, principal_kind_name(kind)])
        }
        ObserveEvent::UpstreamChosen { upstream } => {
            json!(["UpstreamChosen", canonical_upstream(upstream)])
        }
        ObserveEvent::Chunk {
            batch_index,
            event_count,
            total_bytes,
        } => json!(["Chunk", batch_index, event_count, total_bytes]),
        ObserveEvent::RequestFinished {
            status,
            input_tokens,
            output_tokens,
            duration_ms: _,
        } => json!([
            "RequestFinished",
            status.as_u16(),
            input_tokens,
            output_tokens,
            0
        ]),
        ObserveEvent::Error {
            code,
            message,
            source,
        } => json!(["Error", code, message, source]),
    }
}

fn normalized_request_id(
    request_ids: &mut HashMap<String, String>,
    next_request_id: &mut usize,
    request_id: &str,
) -> String {
    if let Some(normalized) = request_ids.get(request_id) {
        return normalized.clone();
    }
    *next_request_id += 1;
    let normalized = format!("req-{next_request_id}");
    request_ids.insert(request_id.to_owned(), normalized.clone());
    normalized
}

fn principal_kind_name(kind: &PrincipalKind) -> &'static str {
    match kind {
        PrincipalKind::ApiKey => "api_key",
        PrincipalKind::OAuthSubject => "oauth_subject",
        PrincipalKind::InternalKey => "internal_key",
        PrincipalKind::WorkloadIdentity => "workload_identity",
        PrincipalKind::SubscriptionBearer => "subscription_bearer",
    }
}

fn canonical_upstream(upstream: &Upstream) -> Value {
    match upstream {
        Upstream::AnthropicDirect => json!(["AnthropicDirect"]),
        Upstream::CustomAnthropicSpec { base_url } => {
            json!(["CustomAnthropicSpec", base_url.as_str()])
        }
    }
}
