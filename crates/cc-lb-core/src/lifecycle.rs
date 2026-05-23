use std::convert::Infallible;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::body::Body as AxumBody;
use bytes::Bytes;
use cc_lb_dialect_bedrock::{convert_eventstream_to_sse_bytes, EventStreamConvertError};
use cc_lb_plugin_api::{
    shape_request, sign_request, ObservabilityHook, ObserveEvent, Principal, PrincipalKind,
    RequestContext, RetryDecision, RouterPlugin, SignedRequest, SignerFactory, Upstream,
    UpstreamError,
};
use cc_lb_pricing::{global_catalog, virtual_cost_micros_full};
use cc_lb_storage_redb::StoredApiKeyRecord;
use http::header::{CONTENT_LENGTH, CONTENT_TYPE, RETRY_AFTER};
use http::{HeaderMap, HeaderName, HeaderValue, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use serde_json::{json, Value};
use thiserror::Error;

use crate::api_keys::builtin_authn::BuiltinAuthn;
use crate::api_keys::limit_engine::{LimitEngine, RejectReason, Reservation as LimitReservation};
use crate::api_keys::types::LimitKind;
use crate::audit_writer::{AuditEntry, AuditWriterSink};
use crate::error_format::{anthropic_error_response, anthropic_error_response_with_retry_after};
use crate::error_normalizer::{ErrorNormalizer, UpstreamKind};
use crate::hop_by_hop::strip_hop_by_hop;

pub type Body = AxumBody;

const DEFAULT_MESSAGES_CAP_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_FILES_CAP_BYTES: usize = 100 * 1024 * 1024;

static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
pub struct LifecycleConfig {
    pub messages_body_cap_bytes: usize,
    pub files_body_cap_bytes: usize,
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            messages_body_cap_bytes: DEFAULT_MESSAGES_CAP_BYTES,
            files_body_cap_bytes: DEFAULT_FILES_CAP_BYTES,
        }
    }
}

#[derive(Debug, Error)]
pub enum ProxyError {
    #[error("response build failed: {reason}")]
    ResponseBuild { reason: String },
}

#[derive(Debug, Error)]
pub enum DispatchError {
    #[error("invalid upstream uri: {reason}")]
    InvalidUri { reason: String },
    #[error("upstream request build failed: {reason}")]
    RequestBuild { reason: String },
    #[error("upstream dispatch failed: {reason}")]
    Transport { reason: String },
    #[error("upstream bulkhead queue full; retry after {retry_after:?}")]
    BulkheadFull { retry_after: Duration },
}

#[derive(Debug, Error)]
enum ResponseConversionError {
    #[error("failed to read Bedrock event-stream body: {source}")]
    Read { source: axum::Error },
    #[error("failed to convert Bedrock event-stream body: {source}")]
    Convert { source: EventStreamConvertError },
}

#[async_trait]
pub trait UpstreamDispatch: Send + Sync {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError>;
}

pub trait LimitSubjectProvider: Send + Sync {
    fn limit_subject(&self, ctx: &RequestContext, principal: &Principal) -> Option<LimitSubject>;
}

pub trait ApiKeyAwareSignerFactory: Send + Sync {
    fn with_api_key(&self, api_key: String) -> Arc<dyn SignerFactory>;
}

#[derive(Clone, Debug)]
pub struct LimitSubject {
    pub principal_id: String,
    pub key_id: String,
    pub record: StoredApiKeyRecord,
}

struct StaticLimitSubjectProvider {
    subject: LimitSubject,
}

impl LimitSubjectProvider for StaticLimitSubjectProvider {
    fn limit_subject(&self, _ctx: &RequestContext, _principal: &Principal) -> Option<LimitSubject> {
        Some(self.subject.clone())
    }
}

#[derive(Clone)]
pub struct HyperDispatcher {
    client: Client<HttpConnector, Full<Bytes>>,
}

impl HyperDispatcher {
    pub fn new() -> Self {
        let connector = HttpConnector::new();
        Self {
            client: Client::builder(TokioExecutor::new()).build(connector),
        }
    }
}

impl Default for HyperDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl UpstreamDispatch for HyperDispatcher {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let (url, method, headers, body) = request.into_parts();
        let uri =
            url.as_str()
                .parse::<http::Uri>()
                .map_err(|source| DispatchError::InvalidUri {
                    reason: source.to_string(),
                })?;

        let mut builder = Request::builder().method(method).uri(uri);
        copy_headers(headers, builder.headers_mut());
        let request =
            builder
                .body(Full::new(body))
                .map_err(|source| DispatchError::RequestBuild {
                    reason: source.to_string(),
                })?;

        let response =
            self.client
                .request(request)
                .await
                .map_err(|source| DispatchError::Transport {
                    reason: source.to_string(),
                })?;
        let (parts, body) = response.into_parts();
        Ok(Response::from_parts(parts, Body::new(body)))
    }
}

pub struct Lifecycle {
    authn: Arc<BuiltinAuthn>,
    signer_factory: Arc<dyn ApiKeyAwareSignerFactory>,
    router: Arc<dyn RouterPlugin>,
    dispatcher: Arc<dyn UpstreamDispatch>,
    observability_hooks: Vec<Arc<dyn ObservabilityHook>>,
    error_normalizer: Arc<ErrorNormalizer>,
    config: LifecycleConfig,
    limit_engine: Option<Arc<LimitEngine>>,
    limit_subject_provider: Option<Arc<dyn LimitSubjectProvider>>,
    audit_sink: Option<Arc<AuditWriterSink>>,
}

impl Lifecycle {
    pub fn new(
        authn: Arc<BuiltinAuthn>,
        signer_factory: Arc<dyn ApiKeyAwareSignerFactory>,
        router: Arc<dyn RouterPlugin>,
        dispatcher: Arc<dyn UpstreamDispatch>,
        observability_hooks: Vec<Arc<dyn ObservabilityHook>>,
        config: LifecycleConfig,
    ) -> Self {
        Self {
            authn,
            signer_factory,
            router,
            dispatcher,
            observability_hooks,
            error_normalizer: Arc::new(ErrorNormalizer::new()),
            config,
            limit_engine: None,
            limit_subject_provider: None,
            audit_sink: None,
        }
    }

    pub fn with_error_normalizer(mut self, error_normalizer: Arc<ErrorNormalizer>) -> Self {
        self.error_normalizer = error_normalizer;
        self
    }

    pub fn with_audit_sink(mut self, audit_sink: Arc<AuditWriterSink>) -> Self {
        self.audit_sink = Some(audit_sink);
        self
    }

    pub fn with_limit_engine(
        mut self,
        limit_engine: Arc<LimitEngine>,
        limit_subject_provider: Arc<dyn LimitSubjectProvider>,
    ) -> Self {
        self.limit_engine = Some(limit_engine);
        self.limit_subject_provider = Some(limit_subject_provider);
        self
    }

    pub fn with_static_limit_subject(
        self,
        limit_engine: Arc<LimitEngine>,
        principal_id: String,
        key_id: String,
        record: StoredApiKeyRecord,
    ) -> Self {
        self.with_limit_engine(
            limit_engine,
            Arc::new(StaticLimitSubjectProvider {
                subject: LimitSubject {
                    principal_id,
                    key_id,
                    record,
                },
            }),
        )
    }

    fn enqueue_limit_audit(
        &self,
        ctx: &RequestContext,
        subject: &LimitSubject,
        request: &LimitRequest,
        route: &cc_lb_plugin_api::RouteDecision,
        limit_violation: &str,
    ) {
        let Some(audit_sink) = &self.audit_sink else {
            return;
        };
        let _ = audit_sink.try_enqueue(AuditEntry {
            ts: unix_now_secs(),
            request_id: ctx.request_id.clone(),
            principal_id: subject.principal_id.clone(),
            route: ctx.path.clone(),
            upstream: audit_upstream_name(&route.upstream).to_owned(),
            model: Some(request.model.clone()),
            status: StatusCode::TOO_MANY_REQUESTS.as_u16(),
            input_tokens: None,
            output_tokens: None,
            duration_ms: 0,
            agent_label: None,
            api_key_id: Some(subject.key_id.clone()),
            cost_usd_micros: None,
            limit_violation: Some(limit_violation.to_owned()),
            admin_action: None,
            actor: Some("system".to_owned()),
        });
    }

    pub async fn handle(&self, req: Request<Bytes>) -> Result<Response<Body>, ProxyError> {
        let started = Instant::now();
        let parsed = self.parse(req);
        let ctx = match parsed {
            Ok(ctx) => ctx,
            Err(response) => return Ok(*response),
        };

        self.observe(ObserveEvent::RequestStarted {
            request_id: ctx.request_id.clone(),
            downstream_user_agent: header_to_string(&ctx.downstream_headers, "user-agent"),
        });

        let success = match (
            self.authn.authenticate_none_mode(),
            self.authn.authenticate(&ctx.downstream_headers),
        ) {
            (Some(success), _) => success,
            (None, Ok(success)) => success,
            (None, Err(source)) => {
                self.observe_error("authentication_error", &source.to_string(), "authn");
                let response = anthropic_error_response(
                    StatusCode::UNAUTHORIZED,
                    "authentication_error",
                    &source.to_string(),
                );
                self.observe_finished(StatusCode::UNAUTHORIZED, started);
                return Ok(response);
            }
        };
        let principal = Principal {
            id: success.principal_id.clone(),
            kind: PrincipalKind::ApiKey,
            claims: serde_json::Map::new(),
        };

        self.observe(ObserveEvent::AuthnComplete {
            principal_id: principal.id.clone(),
            kind: principal.kind.clone(),
        });

        let route = match self.router.route(&ctx, &principal) {
            Ok(route) => route,
            Err(source) => {
                self.observe_error("route_not_configured", &source.to_string(), "router");
                let response = anthropic_error_response(
                    StatusCode::BAD_GATEWAY,
                    "route_not_configured",
                    "no upstream route is configured for this request",
                );
                self.observe_finished_for_principal(
                    StatusCode::BAD_GATEWAY,
                    started,
                    &principal,
                    &ctx.body_bytes,
                );
                return Ok(response);
            }
        };

        self.observe(ObserveEvent::UpstreamChosen {
            upstream: route.upstream.clone(),
        });

        let mut active_limit = match self.reserve_limit(&ctx, &principal, &route) {
            Ok(active_limit) => active_limit,
            Err(response) => {
                self.observe_finished_for_principal(
                    response.status(),
                    started,
                    &principal,
                    &ctx.body_bytes,
                );
                return Ok(response);
            }
        };

        let signer_factory = self
            .signer_factory
            .with_api_key(success.api_key.clone().unwrap_or_default());
        let signer = match signer_factory.build(&route.upstream).await {
            Ok(signer) => signer,
            Err(source) => {
                self.observe_error("signing_error", &source.to_string(), "signer_factory");
                let mut response = anthropic_error_response(
                    StatusCode::BAD_GATEWAY,
                    "api_error",
                    "failed to prepare upstream credentials",
                );
                self.attach_limit_headers(&mut response, active_limit.as_ref());
                self.observe_finished_for_principal(
                    StatusCode::BAD_GATEWAY,
                    started,
                    &principal,
                    &ctx.body_bytes,
                );
                return Ok(response);
            }
        };

        let mut response = match self.attempt(&ctx, &principal, &route, signer.clone()).await {
            Ok(response) => response,
            Err(response) => {
                let mut response = *response;
                self.attach_limit_headers(&mut response, active_limit.as_ref());
                self.observe_finished_for_principal(
                    response.status(),
                    started,
                    &principal,
                    &ctx.body_bytes,
                );
                return Ok(response);
            }
        };

        if response.status() == StatusCode::UNAUTHORIZED {
            let unauthorized = collect_error_response(response).await;
            let err = UpstreamError::Unauthorized {
                status: StatusCode::UNAUTHORIZED,
                body: Some(unauthorized.body.clone()),
            };
            if let RetryDecision::Refresh { new_signer } = signer.on_unauthorized(&err).await {
                response = match self.attempt(&ctx, &principal, &route, new_signer).await {
                    Ok(response) => response,
                    Err(response) => {
                        let mut response = *response;
                        self.attach_limit_headers(&mut response, active_limit.as_ref());
                        self.observe_finished_for_principal(
                            response.status(),
                            started,
                            &principal,
                            &ctx.body_bytes,
                        );
                        return Ok(response);
                    }
                };
            } else {
                let mut response = rebuild_error_response(
                    unauthorized,
                    &route.upstream,
                    route.dialect.as_ref(),
                    self.error_normalizer.as_ref(),
                );
                self.attach_limit_headers(&mut response, active_limit.as_ref());
                self.observe_finished_for_principal(
                    response.status(),
                    started,
                    &principal,
                    &ctx.body_bytes,
                );
                return Ok(response);
            }
        }

        if response.status().is_client_error() || response.status().is_server_error() {
            let collected = collect_error_response(response).await;
            let mut response = rebuild_error_response(
                collected,
                &route.upstream,
                route.dialect.as_ref(),
                self.error_normalizer.as_ref(),
            );
            self.attach_limit_headers(&mut response, active_limit.as_ref());
            self.observe_finished_for_principal(
                response.status(),
                started,
                &principal,
                &ctx.body_bytes,
            );
            return Ok(response);
        }

        response = match convert_success_response_if_needed(response, &route.upstream).await {
            Ok(response) => response,
            Err(source) => {
                self.observe_error("response_conversion_error", &source.to_string(), "dialect");
                let mut response = anthropic_error_response(
                    StatusCode::BAD_GATEWAY,
                    "api_error",
                    "failed to convert upstream response",
                );
                self.attach_limit_headers(&mut response, active_limit.as_ref());
                self.observe_finished_for_principal(
                    response.status(),
                    started,
                    &principal,
                    &ctx.body_bytes,
                );
                return Ok(response);
            }
        };

        let status = response.status();
        response = self
            .finish_success_response(response, active_limit.take())
            .await;
        self.observe_finished_for_principal(status, started, &principal, &ctx.body_bytes);
        Ok(response)
    }

    fn reserve_limit(
        &self,
        ctx: &RequestContext,
        principal: &Principal,
        route: &cc_lb_plugin_api::RouteDecision,
    ) -> Result<Option<ActiveLimit>, Response<Body>> {
        let (Some(limit_engine), Some(subject_provider)) = (
            self.limit_engine.as_ref(),
            self.limit_subject_provider.as_ref(),
        ) else {
            return Ok(None);
        };
        let Some(subject) = subject_provider.limit_subject(ctx, principal) else {
            return Ok(None);
        };
        let limit_request = LimitRequest::from_body(&ctx.body_bytes);
        let upstream_kind = pricing_upstream_kind(&route.upstream);
        let max_input_estimate = 4000_i64; // TODO(later): heuristic from messages length
        let cost_estimate = global_catalog()
            .estimate_max(
                &limit_request.model,
                max_input_estimate as u64,
                limit_request.max_tokens.max(0) as u64,
                upstream_kind,
            )
            .map(|cost| cost as i64);

        match limit_engine.reserve(
            &subject.record,
            &subject.principal_id,
            &limit_request.model,
            limit_request.max_tokens,
            max_input_estimate,
            cost_estimate,
        ) {
            Ok(reservation) => Ok(Some(ActiveLimit {
                subject,
                request: limit_request,
                upstream_kind,
                reservation: Some(reservation),
            })),
            Err(reason) => {
                if let Some(limit_violation) = limit_violation_name(&reason) {
                    self.enqueue_limit_audit(ctx, &subject, &limit_request, route, limit_violation);
                }
                let retry_after_seconds = limit_retry_after_secs(reason.clone());
                let mut response = limit_rejection_response(
                    reason,
                    &limit_request.model,
                    &subject.principal_id,
                    retry_after_seconds,
                );
                attach_limit_headers_from_engine(
                    response.headers_mut(),
                    limit_engine.as_ref(),
                    &subject.key_id,
                    &subject.principal_id,
                );
                Err(response)
            }
        }
    }

    async fn finish_success_response(
        &self,
        response: Response<Body>,
        active_limit: Option<ActiveLimit>,
    ) -> Response<Body> {
        let Some(mut active_limit) = active_limit else {
            return self.relay_response(response);
        };

        if active_limit.request.stream || is_sse_response(response.headers()) {
            let mut response = self.relay_response(response);
            self.attach_limit_headers(&mut response, Some(&active_limit));
            return response;
        }

        let (mut parts, body) = response.into_parts();
        let body = match body.collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(_source) => Bytes::new(),
        };
        let usage = usage_from_json_body(&body);
        if let (Some(limit_engine), Some(reservation)) =
            (self.limit_engine.as_ref(), active_limit.reservation.take())
        {
            let cost_micros = virtual_cost_micros_full(
                &active_limit.request.model,
                usage.input_tokens,
                usage.output_tokens,
                usage.cache_creation_input_tokens,
                usage.cache_read_input_tokens,
                active_limit.upstream_kind,
            )
            .micros_usd
            .unwrap_or(0) as i64;
            limit_engine.reconcile(
                reservation,
                usage.input_tokens,
                usage.output_tokens,
                cost_micros,
            );
            attach_limit_headers_from_engine(
                &mut parts.headers,
                limit_engine.as_ref(),
                &active_limit.subject.key_id,
                &active_limit.subject.principal_id,
            );
        }
        Response::from_parts(parts, Body::from(body))
    }

    fn attach_limit_headers(
        &self,
        response: &mut Response<Body>,
        active_limit: Option<&ActiveLimit>,
    ) {
        let (Some(limit_engine), Some(active_limit)) = (self.limit_engine.as_ref(), active_limit)
        else {
            return;
        };
        attach_limit_headers_from_engine(
            response.headers_mut(),
            limit_engine.as_ref(),
            &active_limit.subject.key_id,
            &active_limit.subject.principal_id,
        );
    }

    fn parse(&self, req: Request<Bytes>) -> Result<RequestContext, Box<Response<Body>>> {
        let (mut parts, body) = req.into_parts();
        let path = parts.uri.path().to_owned();
        let cap = body_cap_for_path(&self.config, &path);
        if body.len() > cap {
            let response = anthropic_error_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                "body_too_large",
                "request body exceeds configured cap",
            );
            return Err(Box::new(response));
        }

        let request_id = header_to_string(&parts.headers, "request-id")
            .or_else(|| header_to_string(&parts.headers, "x-request-id"))
            .unwrap_or_else(next_request_id);

        Ok(RequestContext {
            request_id,
            downstream_headers: {
                strip_hop_by_hop(&mut parts.headers);
                parts.headers
            },
            method: parts.method,
            path,
            query: parts.uri.query().map(ToOwned::to_owned),
            body_bytes: body,
        })
    }

    async fn attempt(
        &self,
        ctx: &RequestContext,
        principal: &Principal,
        route: &cc_lb_plugin_api::RouteDecision,
        signer: Arc<dyn cc_lb_plugin_api::Signer>,
    ) -> Result<Response<Body>, Box<Response<Body>>> {
        let shaped = shape_request(route.dialect.as_ref(), ctx, &route.upstream, principal)
            .map_err(|source| {
                self.observe_error("shape_error", &source.to_string(), "dialect");
                Box::new(anthropic_error_response(
                    StatusCode::BAD_GATEWAY,
                    "api_error",
                    "failed to shape upstream request",
                ))
            })?;
        let signed = sign_request(signer.as_ref(), shaped)
            .await
            .map_err(|source| {
                self.observe_error("signing_error", &source.to_string(), "signer");
                Box::new(anthropic_error_response(
                    StatusCode::BAD_GATEWAY,
                    "api_error",
                    "failed to sign upstream request",
                ))
            })?;
        self.dispatcher.dispatch(signed).await.map_err(|source| {
            self.observe_error("upstream_dispatch_error", &source.to_string(), "dispatch");
            match source {
                DispatchError::BulkheadFull { retry_after } => {
                    Box::new(anthropic_error_response_with_retry_after(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "overloaded_error",
                        "upstream bulkhead queue is full",
                        retry_after.as_secs().max(1),
                    ))
                }
                DispatchError::InvalidUri { .. }
                | DispatchError::RequestBuild { .. }
                | DispatchError::Transport { .. } => Box::new(anthropic_error_response(
                    StatusCode::BAD_GATEWAY,
                    "api_error",
                    "upstream request failed",
                )),
            }
        })
    }

    fn relay_response(&self, response: Response<Body>) -> Response<Body> {
        let (mut parts, mut body) = response.into_parts();
        strip_hop_by_hop(&mut parts.headers);
        let hooks = self.observability_hooks.clone();
        let stream = async_stream::stream! {
            let mut batch_index = 0_u64;
            while let Some(frame) = body.frame().await {
                match frame {
                    Ok(frame) => {
                        if let Ok(data) = frame.into_data() {
                            observe_many(&hooks, ObserveEvent::Chunk {
                                batch_index,
                                event_count: 1,
                                total_bytes: data.len(),
                            });
                            batch_index = batch_index.saturating_add(1);
                            yield Ok::<Bytes, Infallible>(data);
                        }
                    }
                    Err(_source) => break,
                }
            }
        };
        Response::from_parts(parts, Body::from_stream(stream))
    }

    fn observe(&self, event: ObserveEvent) {
        observe_many(&self.observability_hooks, event);
    }

    fn observe_error(&self, code: &str, message: &str, source: &str) {
        self.observe(ObserveEvent::Error {
            code: code.to_owned(),
            message: message.to_owned(),
            source: source.to_owned(),
        });
    }

    fn observe_finished(&self, status: StatusCode, started: Instant) {
        self.observe(ObserveEvent::RequestFinished {
            status,
            input_tokens: None,
            output_tokens: None,
            duration_ms: started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
        });
    }

    fn observe_finished_for_principal(
        &self,
        status: StatusCode,
        started: Instant,
        principal: &Principal,
        body: &Bytes,
    ) {
        let model = extract_model(body).unwrap_or_else(|| "unknown".to_owned());
        metrics::counter!(
            "cc_lb_requests_total",
            "principal" => principal.id.clone(),
            "upstream" => "unknown",
            "model" => model,
            "status" => status.as_u16().to_string(),
        )
        .increment(1);
        self.observe_finished(status, started);
    }
}

struct CollectedResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

async fn collect_error_response(response: Response<Body>) -> CollectedResponse {
    let (parts, body) = response.into_parts();
    let body = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_source) => Bytes::new(),
    };
    CollectedResponse {
        status: parts.status,
        headers: {
            let mut headers = parts.headers;
            strip_hop_by_hop(&mut headers);
            headers
        },
        body,
    }
}

async fn convert_success_response_if_needed(
    response: Response<Body>,
    upstream: &Upstream,
) -> Result<Response<Body>, ResponseConversionError> {
    if !matches!(upstream, Upstream::BedrockRuntime { .. })
        || !is_aws_eventstream(response.headers())
    {
        return Ok(response);
    }

    let (mut parts, body) = response.into_parts();
    let body = body
        .collect()
        .await
        .map_err(|source| ResponseConversionError::Read { source })?
        .to_bytes();
    let sse = convert_eventstream_to_sse_bytes(&body)
        .map_err(|source| ResponseConversionError::Convert { source })?;
    parts.headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream; charset=utf-8"),
    );
    parts.headers.remove(CONTENT_LENGTH);
    Ok(Response::from_parts(parts, Body::from(sse)))
}

fn is_aws_eventstream(headers: &HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value.split(';').next().is_some_and(|media_type| {
                media_type
                    .trim()
                    .eq_ignore_ascii_case("application/vnd.amazon.eventstream")
            })
        })
}

fn rebuild_error_response(
    collected: CollectedResponse,
    upstream: &Upstream,
    dialect: &dyn cc_lb_plugin_api::UpstreamDialect,
    normalizer: &ErrorNormalizer,
) -> Response<Body> {
    normalizer.build_http_error_response_with_dialect(
        UpstreamKind::from(upstream),
        collected.status,
        &collected.body,
        &collected.headers,
        Some(dialect),
    )
}

fn copy_headers(source: HeaderMap, target: Option<&mut HeaderMap>) {
    let Some(target) = target else {
        return;
    };

    for (name, value) in source {
        if let Some(name) = name {
            target.append(name, value);
        }
    }
}

fn body_cap_for_path(config: &LifecycleConfig, path: &str) -> usize {
    if path.starts_with("/v1/files") {
        config.files_body_cap_bytes
    } else {
        config.messages_body_cap_bytes
    }
}

fn header_to_string(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

fn next_request_id() -> String {
    let id = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut request_id = String::with_capacity("req_core_".len() + 20);
    request_id.push_str("req_core_");
    let _ = write!(&mut request_id, "{id}");
    request_id
}

fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn extract_model(body: &Bytes) -> Option<String> {
    serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|value| {
            value
                .get("model")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
}

#[derive(Clone, Debug)]
struct LimitRequest {
    model: String,
    max_tokens: i64,
    stream: bool,
}

impl LimitRequest {
    fn from_body(body: &Bytes) -> Self {
        let value = serde_json::from_slice::<Value>(body).unwrap_or(Value::Null);
        Self {
            model: value
                .get("model")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_owned(),
            max_tokens: value.get("max_tokens").and_then(Value::as_i64).unwrap_or(0),
            stream: value
                .get("stream")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        }
    }
}

struct ActiveLimit {
    subject: LimitSubject,
    request: LimitRequest,
    upstream_kind: Option<cc_lb_pricing::UpstreamKind>,
    reservation: Option<LimitReservation>,
}

#[derive(Default)]
struct UsageCounts {
    input_tokens: u64,
    output_tokens: u64,
    cache_creation_input_tokens: u64,
    cache_read_input_tokens: u64,
}

fn usage_from_json_body(body: &Bytes) -> UsageCounts {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return UsageCounts::default();
    };
    let Some(usage) = value.get("usage") else {
        return UsageCounts::default();
    };
    UsageCounts {
        input_tokens: usage
            .get("input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        output_tokens: usage
            .get("output_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        cache_creation_input_tokens: usage
            .get("cache_creation_input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        cache_read_input_tokens: usage
            .get("cache_read_input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
    }
}

fn attach_limit_headers_from_engine(
    headers: &mut HeaderMap,
    limit_engine: &LimitEngine,
    key_id: &str,
    principal_id: &str,
) {
    for (name, value) in limit_engine.headers_for(key_id, principal_id) {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(&value),
        ) {
            headers.insert(name, value);
        }
    }
}

fn limit_rejection_response(
    reason: RejectReason,
    model: &str,
    principal_id: &str,
    retry_after_seconds: Option<u64>,
) -> Response<Body> {
    let (status, error_type, message, limit_kind) = match reason {
        RejectReason::ModelNotAllowed => (
            StatusCode::FORBIDDEN,
            "forbidden",
            format!("model {model} not allowed for principal {principal_id}"),
            None,
        ),
        RejectReason::Expired => (
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            "api key expired".to_owned(),
            None,
        ),
        RejectReason::KeyDisabled => (
            StatusCode::FORBIDDEN,
            "forbidden",
            "api key disabled".to_owned(),
            None,
        ),
        RejectReason::KeyRevoked => (
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            "api key revoked".to_owned(),
            None,
        ),
        RejectReason::PrincipalDisabled => (
            StatusCode::FORBIDDEN,
            "forbidden",
            "principal disabled".to_owned(),
            None,
        ),
        RejectReason::PrincipalMissing => (
            StatusCode::FORBIDDEN,
            "forbidden",
            "principal missing".to_owned(),
            None,
        ),
        RejectReason::RequestsRateLimit => (
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limit_error",
            "requests/window cap exceeded".to_owned(),
            Some("requests"),
        ),
        RejectReason::TokenRateLimit { kind } => (
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limit_error",
            "token/window cap exceeded".to_owned(),
            Some(limit_kind_name(kind)),
        ),
        RejectReason::CostRateLimit => (
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limit_error",
            "cost/window cap exceeded".to_owned(),
            Some("cost_usd"),
        ),
        RejectReason::ConcurrentRateLimit => (
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limit_error",
            "concurrent request cap exceeded".to_owned(),
            Some("concurrent"),
        ),
        RejectReason::CostUnavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            "server_error",
            format!("cost limits unavailable for model {model}"),
            None,
        ),
        RejectReason::OutputCapExceeded { cap, requested } => (
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            format!("max_tokens {requested} exceeds key output limit {cap}"),
            None,
        ),
    };

    let mut error = json!({
        "type": error_type,
        "message": message,
    });
    if let Some(limit_kind) = limit_kind {
        error["limit_kind"] = json!(limit_kind);
    }
    if let Some(retry_after_seconds) = retry_after_seconds {
        error["retry_after_seconds"] = json!(retry_after_seconds);
    }

    let mut response = Response::new(Body::from(Bytes::from(
        json!({"type":"error","error":error}).to_string(),
    )));
    *response.status_mut() = status;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/json; charset=utf-8"),
    );
    if status == StatusCode::TOO_MANY_REQUESTS {
        if let Some(retry_after_seconds) = retry_after_seconds {
            if let Ok(value) = HeaderValue::from_str(&retry_after_seconds.to_string()) {
                response.headers_mut().insert(RETRY_AFTER, value);
            }
        }
    }
    response
}

fn limit_retry_after_secs(reason: RejectReason) -> Option<u64> {
    match reason {
        RejectReason::ConcurrentRateLimit => Some(1),
        RejectReason::RequestsRateLimit
        | RejectReason::TokenRateLimit { .. }
        | RejectReason::CostRateLimit => Some(60),
        _ => None,
    }
}

fn limit_violation_name(reason: &RejectReason) -> Option<&'static str> {
    match reason {
        RejectReason::RequestsRateLimit => Some("Requests"),
        RejectReason::TokenRateLimit { kind } => Some(audit_limit_kind_name(*kind)),
        RejectReason::CostRateLimit => Some("CostUsd"),
        RejectReason::ConcurrentRateLimit => Some("Concurrent"),
        RejectReason::PrincipalMissing
        | RejectReason::PrincipalDisabled
        | RejectReason::KeyDisabled
        | RejectReason::KeyRevoked
        | RejectReason::Expired
        | RejectReason::ModelNotAllowed
        | RejectReason::CostUnavailable
        | RejectReason::OutputCapExceeded { .. } => None,
    }
}

fn audit_limit_kind_name(kind: LimitKind) -> &'static str {
    match kind {
        LimitKind::InputTokens => "InputTokens",
        LimitKind::OutputTokens => "OutputTokens",
        LimitKind::TotalTokens => "TotalTokens",
        LimitKind::Requests => "Requests",
        LimitKind::CostUsd => "CostUsd",
        LimitKind::Concurrent => "Concurrent",
    }
}

fn limit_kind_name(kind: LimitKind) -> &'static str {
    match kind {
        LimitKind::InputTokens => "input_tokens",
        LimitKind::OutputTokens => "output_tokens",
        LimitKind::TotalTokens => "total_tokens",
        LimitKind::Requests => "requests",
        LimitKind::CostUsd => "cost_usd",
        LimitKind::Concurrent => "concurrent",
    }
}

fn audit_upstream_name(upstream: &Upstream) -> &'static str {
    match upstream {
        Upstream::AnthropicDirect => "anthropic_direct",
        Upstream::BedrockRuntime { .. } => "bedrock_runtime",
        Upstream::BedrockMantle { .. } => "bedrock_mantle",
        Upstream::Vertex { .. } => "vertex",
        Upstream::CustomAnthropicSpec { .. } => "custom_anthropic_spec",
    }
}

fn pricing_upstream_kind(upstream: &Upstream) -> Option<cc_lb_pricing::UpstreamKind> {
    match upstream {
        Upstream::AnthropicDirect | Upstream::CustomAnthropicSpec { .. } => {
            Some(cc_lb_pricing::UpstreamKind::AnthropicKey)
        }
        Upstream::BedrockRuntime { .. } | Upstream::BedrockMantle { .. } => {
            Some(cc_lb_pricing::UpstreamKind::AwsSigV4)
        }
        Upstream::Vertex { .. } => Some(cc_lb_pricing::UpstreamKind::GcpOAuth),
    }
}

fn is_sse_response(headers: &HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value.split(';').next().is_some_and(|media_type| {
                media_type.trim().eq_ignore_ascii_case("text/event-stream")
            })
        })
}

fn observe_many(hooks: &[Arc<dyn ObservabilityHook>], event: ObserveEvent) {
    for hook in hooks {
        let _result = hook.observe(event.clone());
    }
}
