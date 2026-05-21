#![allow(dead_code)]

use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_core::{DispatchError, Lifecycle, LifecycleConfig, UpstreamDispatch};
use cc_lb_plugin_api::{
    sign_request, AuthnError, AuthnOutcome, DialectError, ObservabilityError, ObservabilityHook,
    ObserveEvent, Principal, PrincipalKind, PrincipalQuotas, RequestContext, RetryDecision,
    RouteDecision, RouteError, RouterPlugin, ShapedRequest, ShapedRequestBuilder, SignedRequest,
    Signer, SignerError, SignerFactory, SigningCapability, Upstream, UpstreamDialect,
};
use http::header::CONTENT_TYPE;
use http::{HeaderMap, HeaderValue, Method, Request, Response, StatusCode};
use http_body_util::BodyExt;
use serde_json::json;
use tokio::sync::Notify;
use url::Url;

#[derive(Clone)]
pub struct TestState {
    pub upstream_calls: Arc<AtomicU64>,
    pub refresh_count: Arc<AtomicU64>,
    pub stream_dropped: Arc<AtomicBool>,
    pub stream_drop_notify: Arc<Notify>,
}

impl Default for TestState {
    fn default() -> Self {
        Self {
            upstream_calls: Arc::new(AtomicU64::new(0)),
            refresh_count: Arc::new(AtomicU64::new(0)),
            stream_dropped: Arc::new(AtomicBool::new(false)),
            stream_drop_notify: Arc::new(Notify::new()),
        }
    }
}

#[derive(Clone)]
pub struct TestAuthn {
    pub quotas: PrincipalQuotas,
    pub state: TestState,
    pub refresh_allowed: bool,
}

impl TestAuthn {
    pub fn new(state: TestState) -> Self {
        Self {
            quotas: PrincipalQuotas {
                requests_per_window: 100,
                input_tokens_per_window: 100_000,
                output_tokens_per_window: 100_000,
                window: Duration::from_secs(60),
                allowed_models: vec!["claude-*".to_owned()],
            },
            state,
            refresh_allowed: true,
        }
    }
}

#[async_trait]
impl cc_lb_plugin_api::AuthnPlugin for TestAuthn {
    async fn authenticate(&self, _ctx: &RequestContext) -> Result<AuthnOutcome, AuthnError> {
        Ok(AuthnOutcome {
            principal: Principal {
                id: "principal-test".to_owned(),
                kind: PrincipalKind::ApiKey,
                claims: serde_json::Map::new(),
            },
            signer_factory: Arc::new(TestSignerFactory {
                state: self.state.clone(),
                refresh_allowed: self.refresh_allowed,
            }),
            quotas: self.quotas.clone(),
        })
    }
}

#[derive(Clone)]
pub struct TestRouter {
    pub base_url: Url,
}

impl RouterPlugin for TestRouter {
    fn route(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
    ) -> Result<RouteDecision, RouteError> {
        Ok(RouteDecision {
            upstream: Upstream::CustomAnthropicSpec {
                base_url: self.base_url.clone(),
            },
            dialect: Box::new(PassthroughDialect),
        })
    }
}

pub struct PassthroughDialect;

impl UpstreamDialect for PassthroughDialect {
    fn shape(
        &self,
        ctx: &RequestContext,
        upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        let Upstream::CustomAnthropicSpec { base_url } = upstream else {
            return Err(DialectError::UpstreamMismatch {
                reason: "test dialect expects custom upstream".to_owned(),
            });
        };
        let mut url = base_url.clone();
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

pub struct TestSignerFactory {
    state: TestState,
    refresh_allowed: bool,
}

#[async_trait]
impl SignerFactory for TestSignerFactory {
    async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        Ok(Arc::new(TestSigner {
            state: self.state.clone(),
            refresh_allowed: self.refresh_allowed,
            key: "sk-ant-initial".to_owned(),
        }))
    }
}

pub struct TestSigner {
    state: TestState,
    refresh_allowed: bool,
    key: String,
}

#[async_trait]
impl Signer for TestSigner {
    async fn sign(
        &self,
        mut shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        let key =
            HeaderValue::from_str(&self.key).map_err(|source| SignerError::SigningFailed {
                reason: source.to_string(),
            })?;
        shaped.headers_mut().insert("x-api-key", key);
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(&self, _err: &cc_lb_plugin_api::UpstreamError) -> RetryDecision {
        if !self.refresh_allowed {
            return RetryDecision::Fail;
        }
        self.state.refresh_count.fetch_add(1, Ordering::Relaxed);
        RetryDecision::Refresh {
            new_signer: Arc::new(TestSigner {
                state: self.state.clone(),
                refresh_allowed: false,
                key: "sk-ant-refreshed".to_owned(),
            }),
        }
    }
}

#[derive(Default)]
pub struct RecordingHook {
    pub events: Mutex<Vec<ObserveEvent>>,
}

impl ObservabilityHook for RecordingHook {
    fn observe(&self, event: ObserveEvent) -> Result<(), ObservabilityError> {
        self.events
            .lock()
            .map(|mut events| events.push(event))
            .map_err(|_| ObservabilityError::Dropped {
                reason: "lock poisoned".to_owned(),
            })
    }
}

#[derive(Clone)]
pub enum DispatchMode {
    StreamingOk,
    Statuses(Arc<Mutex<VecDeque<StatusCode>>>),
    HeadersOk(HeaderMap),
}

#[derive(Clone)]
pub struct MockDispatch {
    pub state: TestState,
    pub mode: DispatchMode,
}

#[async_trait]
impl UpstreamDispatch for MockDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let call = self.state.upstream_calls.fetch_add(1, Ordering::Relaxed) + 1;
        println!("upstream_call={call}");

        match &self.mode {
            DispatchMode::StreamingOk => Ok(streaming_response(self.state.clone())),
            DispatchMode::Statuses(statuses) => {
                let status = statuses
                    .lock()
                    .map(|mut statuses| statuses.pop_front().unwrap_or(StatusCode::OK))
                    .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                Ok(json_response(status, default_json_for_status(status)))
            }
            DispatchMode::HeadersOk(headers) => {
                let mut response = json_response(
                    StatusCode::OK,
                    json!({"type":"message","usage":{"input_tokens":1,"output_tokens":1}}),
                );
                *response.headers_mut() = headers.clone();
                Ok(response)
            }
        }
    }
}

pub fn lifecycle_with(
    authn: TestAuthn,
    dispatcher: MockDispatch,
    hook: Arc<RecordingHook>,
) -> Lifecycle {
    Lifecycle::new(
        Arc::new(authn),
        Arc::new(TestRouter {
            base_url: Url::parse("http://upstream.local/").expect("test URL parses"),
        }),
        Arc::new(dispatcher),
        vec![hook],
        LifecycleConfig::default(),
    )
}

pub fn messages_request(body: Bytes) -> Request<Bytes> {
    Request::builder()
        .method(Method::POST)
        .uri("/v1/messages")
        .header("x-api-key", "sk-ant-downstream")
        .header("anthropic-version", "2023-06-01")
        .body(body)
        .expect("test request builds")
}

pub async fn collect_body(response: Response<Body>) -> (StatusCode, HeaderMap, Bytes) {
    let status = response.status();
    let headers = response.headers().clone();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("test body collects")
        .to_bytes();
    (status, headers, body)
}

pub async fn signed_request(base_url: &str) -> SignedRequest {
    let upstream = Upstream::CustomAnthropicSpec {
        base_url: Url::parse(base_url).expect("test URL parses"),
    };
    let ctx = RequestContext {
        request_id: "test-request".to_owned(),
        downstream_headers: HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::from_static(br#"{"model":"claude-test","messages":[]}"#),
    };
    let principal = Principal {
        id: "principal-test".to_owned(),
        kind: PrincipalKind::ApiKey,
        claims: serde_json::Map::new(),
    };
    let shaped = cc_lb_plugin_api::shape_request(&PassthroughDialect, &ctx, &upstream, &principal)
        .expect("test request shapes");
    sign_request(&NoopSigner, shaped)
        .await
        .expect("test request signs")
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

fn streaming_response(state: TestState) -> Response<Body> {
    let stream = async_stream::stream! {
        let _guard = StreamDropGuard { state: state.clone() };
        for index in 0..54_u64 {
            let frame = format!("event: content_block_delta\ndata: {{\"index\":{index}}}\n\n");
            yield Ok::<Bytes, Infallible>(Bytes::from(frame));
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = StatusCode::OK;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    response
}

struct StreamDropGuard {
    state: TestState,
}

impl Drop for StreamDropGuard {
    fn drop(&mut self) {
        self.state.stream_dropped.store(true, Ordering::Relaxed);
        self.state.stream_drop_notify.notify_waiters();
    }
}

fn json_response(status: StatusCode, body: serde_json::Value) -> Response<Body> {
    let mut response = Response::new(Body::from(Bytes::from(body.to_string())));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}

fn default_json_for_status(status: StatusCode) -> serde_json::Value {
    if status.is_success() {
        json!({"type":"message","usage":{"input_tokens":1,"output_tokens":1}})
    } else {
        json!({"type":"error","error":{"type":"authentication_error","message":"forced"}})
    }
}
