use std::convert::Infallible;
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use axum::body::Body as AxumBody;
use bytes::{Bytes, BytesMut};
use cc_lb_dialect_bedrock::{EventStreamConvertError, convert_eventstream_to_sse_bytes};
use cc_lb_plugin_api::{
    AuthnPlugin, ObservabilityHook, ObserveEvent, Principal, PrincipalQuotas, RequestContext,
    RetryDecision, RouterPlugin, SignedRequest, Upstream, UpstreamError, shape_request,
    sign_request,
};
use cc_lb_pricing::virtual_cost_micros;
use cc_lb_storage_redb::{
    PrincipalLimitIdentityKind, PrincipalLimitKind, PrincipalLimitState, RequestEvent,
    RequestEventUpstream,
};
use http::header::{CONTENT_LENGTH, CONTENT_TYPE};
use http::{HeaderMap, HeaderValue, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use serde_json::Value;
use thiserror::Error;

use crate::dashboard_broadcaster::DashboardBroadcaster;
use crate::error_format::{anthropic_error_response, anthropic_error_response_with_retry_after};
use crate::error_normalizer::{ErrorNormalizer, UpstreamKind};
use crate::hop_by_hop::strip_hop_by_hop;
use crate::limit_state_writer::PrincipalLimitStateSink;
use crate::rate_limit_headers::{
    AnthropicRateLimitKind, LimitIdentity, derive_limit_identity,
    parse_anthropic_rate_limit_headers,
};
use crate::request_events::RequestEventSink;
use crate::sse_relay::{self, Usage};

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
    authn: Arc<dyn AuthnPlugin>,
    router: Arc<dyn RouterPlugin>,
    dispatcher: Arc<dyn UpstreamDispatch>,
    observability_hooks: Vec<Arc<dyn ObservabilityHook>>,
    error_normalizer: Arc<ErrorNormalizer>,
    principal_limit_state_sink: Option<Arc<PrincipalLimitStateSink>>,
    request_event_sink: Option<Arc<RequestEventSink>>,
    dashboard_broadcaster: Option<Arc<DashboardBroadcaster>>,
    config: LifecycleConfig,
}

impl Lifecycle {
    pub fn new(
        authn: Arc<dyn AuthnPlugin>,
        router: Arc<dyn RouterPlugin>,
        dispatcher: Arc<dyn UpstreamDispatch>,
        observability_hooks: Vec<Arc<dyn ObservabilityHook>>,
        config: LifecycleConfig,
    ) -> Self {
        Self {
            authn,
            router,
            dispatcher,
            observability_hooks,
            error_normalizer: Arc::new(ErrorNormalizer::new()),
            principal_limit_state_sink: None,
            request_event_sink: None,
            dashboard_broadcaster: None,
            config,
        }
    }

    pub fn with_error_normalizer(mut self, error_normalizer: Arc<ErrorNormalizer>) -> Self {
        self.error_normalizer = error_normalizer;
        self
    }

    pub fn with_principal_limit_state_sink(
        mut self,
        sink: Option<Arc<PrincipalLimitStateSink>>,
    ) -> Self {
        self.principal_limit_state_sink = sink;
        self
    }

    pub fn with_request_event_sink(mut self, sink: Option<Arc<RequestEventSink>>) -> Self {
        self.request_event_sink = sink;
        self
    }

    pub fn with_dashboard_broadcaster(
        mut self,
        broadcaster: Option<Arc<DashboardBroadcaster>>,
    ) -> Self {
        self.dashboard_broadcaster = broadcaster;
        self
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

        let authn = match self.authn.authenticate(&ctx).await {
            Ok(outcome) => outcome,
            Err(source) => {
                self.observe_error("authentication_error", &source.to_string(), "authn");
                let response = anthropic_error_response(
                    StatusCode::UNAUTHORIZED,
                    "authentication_error",
                    "authentication failed",
                );
                self.observe_finished(StatusCode::UNAUTHORIZED, started, Some(&ctx.request_id));
                return Ok(response);
            }
        };

        self.observe(ObserveEvent::AuthnComplete {
            principal_id: authn.principal.id.clone(),
            kind: authn.principal.kind.clone(),
        });

        let model = extract_model(&ctx.body_bytes);

        let route = match self.router.route(&ctx, &authn.principal) {
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
                    Some(&ctx.request_id),
                    &authn.principal,
                    None,
                    model.as_deref(),
                );
                return Ok(response);
            }
        };

        self.observe(ObserveEvent::UpstreamChosen {
            upstream: route.upstream.clone(),
        });

        if let Some(response) = self.reject_for_quota(&authn.quotas) {
            self.observe_finished_for_principal(
                response.status(),
                started,
                Some(&ctx.request_id),
                &authn.principal,
                Some(&route.upstream),
                model.as_deref(),
            );
            return Ok(response);
        }

        if !model_allowed(model.as_deref(), &authn.quotas) {
            let response = anthropic_error_response(
                StatusCode::FORBIDDEN,
                "model_not_allowed",
                "the requested model is not allowed for this principal",
            );
            self.observe_error(
                "model_not_allowed",
                "model rejected by principal gate",
                "quota",
            );
            self.observe_finished_for_principal(
                StatusCode::FORBIDDEN,
                started,
                Some(&ctx.request_id),
                &authn.principal,
                Some(&route.upstream),
                model.as_deref(),
            );
            return Ok(response);
        }

        let signer = match authn.signer_factory.build(&route.upstream).await {
            Ok(signer) => signer,
            Err(source) => {
                self.observe_error("signing_error", &source.to_string(), "signer_factory");
                let response = anthropic_error_response(
                    StatusCode::BAD_GATEWAY,
                    "api_error",
                    "failed to prepare upstream credentials",
                );
                self.observe_finished_for_principal(
                    StatusCode::BAD_GATEWAY,
                    started,
                    Some(&ctx.request_id),
                    &authn.principal,
                    Some(&route.upstream),
                    model.as_deref(),
                );
                return Ok(response);
            }
        };

        let mut response = match self
            .attempt(&ctx, &authn.principal, &route, signer.clone())
            .await
        {
            Ok(response) => response,
            Err(response) => {
                self.observe_finished_for_principal(
                    response.status(),
                    started,
                    Some(&ctx.request_id),
                    &authn.principal,
                    Some(&route.upstream),
                    model.as_deref(),
                );
                return Ok(*response);
            }
        };

        if response.status() == StatusCode::UNAUTHORIZED {
            let unauthorized = collect_error_response(response).await;
            self.enqueue_limit_states_from_headers(&authn.principal, &unauthorized.headers);
            let err = UpstreamError::Unauthorized {
                status: StatusCode::UNAUTHORIZED,
                body: Some(unauthorized.body.clone()),
            };
            if let RetryDecision::Refresh { new_signer } = signer.on_unauthorized(&err).await {
                response = match self
                    .attempt(&ctx, &authn.principal, &route, new_signer)
                    .await
                {
                    Ok(response) => response,
                    Err(response) => {
                        self.observe_finished_for_principal(
                            response.status(),
                            started,
                            Some(&ctx.request_id),
                            &authn.principal,
                            Some(&route.upstream),
                            model.as_deref(),
                        );
                        return Ok(*response);
                    }
                };
            } else {
                let response = rebuild_error_response(
                    unauthorized,
                    &route.upstream,
                    route.dialect.as_ref(),
                    self.error_normalizer.as_ref(),
                );
                self.observe_finished_for_principal(
                    response.status(),
                    started,
                    Some(&ctx.request_id),
                    &authn.principal,
                    Some(&route.upstream),
                    model.as_deref(),
                );
                return Ok(response);
            }
        }

        if response.status().is_client_error() || response.status().is_server_error() {
            let collected = collect_error_response(response).await;
            self.enqueue_limit_states_from_headers(&authn.principal, &collected.headers);
            let response = rebuild_error_response(
                collected,
                &route.upstream,
                route.dialect.as_ref(),
                self.error_normalizer.as_ref(),
            );
            self.observe_finished_for_principal(
                response.status(),
                started,
                Some(&ctx.request_id),
                &authn.principal,
                Some(&route.upstream),
                model.as_deref(),
            );
            return Ok(response);
        }

        self.enqueue_limit_states_from_headers(&authn.principal, response.headers());

        response = match convert_success_response_if_needed(response, &route.upstream).await {
            Ok(response) => response,
            Err(source) => {
                self.observe_error("response_conversion_error", &source.to_string(), "dialect");
                let response = anthropic_error_response(
                    StatusCode::BAD_GATEWAY,
                    "api_error",
                    "failed to convert upstream response",
                );
                self.observe_finished_for_principal(
                    response.status(),
                    started,
                    Some(&ctx.request_id),
                    &authn.principal,
                    Some(&route.upstream),
                    model.as_deref(),
                );
                return Ok(response);
            }
        };

        let status = response.status();
        let labels = RequestMetricLabels::new(
            status,
            &authn.principal,
            Some(&route.upstream),
            model.as_deref(),
        );
        response = self
            .relay_success_response(response, status, started, Some(&ctx.request_id), labels)
            .await;
        Ok(response)
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

    fn reject_for_quota(&self, quotas: &PrincipalQuotas) -> Option<Response<Body>> {
        if quotas.requests_per_window == 0 {
            self.observe_error(
                "rate_limit_error",
                "principal request quota exhausted",
                "quota",
            );
            return Some(anthropic_error_response_with_retry_after(
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limit_error",
                "request quota exhausted",
                quotas.window.as_secs().max(1),
            ));
        }
        None
    }

    #[allow(clippy::too_many_arguments)] // relay_success_response fans out response + lifecycle labels for metrics emission; grouping into a struct would split adjacent metric updates across two borrows.
    async fn relay_success_response(
        &self,
        response: Response<Body>,
        status: StatusCode,
        started: Instant,
        request_id: Option<&str>,
        labels: RequestMetricLabels,
    ) -> Response<Body> {
        if is_sse_response(response.headers()) {
            self.relay_streaming_response(response, status, started, request_id, labels)
        } else {
            self.relay_full_response(response, status, started, request_id, labels)
                .await
        }
    }

    async fn relay_full_response(
        &self,
        response: Response<Body>,
        status: StatusCode,
        started: Instant,
        request_id: Option<&str>,
        labels: RequestMetricLabels,
    ) -> Response<Body> {
        let (mut parts, body) = response.into_parts();
        strip_hop_by_hop(&mut parts.headers);
        let body = match body.collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(_source) => Bytes::new(),
        };
        let usage = sse_relay::usage_from_json_bytes(&body);
        observe_finished_many(
            &self.observability_hooks,
            self.request_event_sink.as_deref(),
            self.dashboard_broadcaster.as_deref(),
            request_id,
            status,
            started,
            &labels,
            &usage,
        );
        Response::from_parts(parts, Body::from(body))
    }

    fn relay_streaming_response(
        &self,
        response: Response<Body>,
        status: StatusCode,
        started: Instant,
        request_id: Option<&str>,
        labels: RequestMetricLabels,
    ) -> Response<Body> {
        let (mut parts, mut body) = response.into_parts();
        strip_hop_by_hop(&mut parts.headers);
        let hooks = self.observability_hooks.clone();
        let request_event_sink = self.request_event_sink.clone();
        let dashboard_broadcaster = self.dashboard_broadcaster.clone();
        let request_id = request_id.map(ToOwned::to_owned);
        let stream = async_stream::stream! {
            let mut batch_index = 0_u64;
            let mut usage = Usage::default();
            let mut usage_buffer = BytesMut::new();
            while let Some(frame) = body.frame().await {
                match frame {
                    Ok(frame) => {
                        if let Ok(data) = frame.into_data() {
                            usage_buffer.extend_from_slice(&data);
                            while let Some(end) = sse_relay::find_sse_event_end(&usage_buffer) {
                                let raw = usage_buffer.split_to(end).freeze();
                                sse_relay::update_usage_from_sse_event_bytes(raw, &mut usage).await;
                            }
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
            observe_finished_many(
                &hooks,
                request_event_sink.as_deref(),
                dashboard_broadcaster.as_deref(),
                request_id.as_deref(),
                status,
                started,
                &labels,
                &usage,
            );
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

    fn observe_finished(&self, status: StatusCode, started: Instant, request_id: Option<&str>) {
        let labels = RequestMetricLabels::unknown(status);
        observe_finished_many(
            &self.observability_hooks,
            self.request_event_sink.as_deref(),
            self.dashboard_broadcaster.as_deref(),
            request_id,
            status,
            started,
            &labels,
            &Usage::default(),
        );
    }

    fn observe_finished_for_principal(
        &self,
        status: StatusCode,
        started: Instant,
        request_id: Option<&str>,
        principal: &Principal,
        upstream: Option<&Upstream>,
        model: Option<&str>,
    ) {
        let labels = RequestMetricLabels::new(status, principal, upstream, model);
        observe_finished_many(
            &self.observability_hooks,
            self.request_event_sink.as_deref(),
            self.dashboard_broadcaster.as_deref(),
            request_id,
            status,
            started,
            &labels,
            &Usage::default(),
        );
    }

    fn enqueue_limit_states_from_headers(&self, principal: &Principal, headers: &HeaderMap) {
        let Some(sink) = &self.principal_limit_state_sink else {
            return;
        };
        let snapshots = parse_anthropic_rate_limit_headers(headers);
        if snapshots.is_empty() {
            return;
        }

        let (identity_kind, identity_value, account_observed) =
            storage_identity(derive_limit_identity(principal, headers));
        let observed_at_unix_secs = now_unix_secs();
        let principal_id = principal.id.clone();
        for snapshot in snapshots {
            let state = PrincipalLimitState {
                principal_id: principal_id.clone(),
                identity_kind,
                identity_value: identity_value.clone(),
                account_observed,
                window: snapshot.window,
                kind: storage_limit_kind(snapshot.kind),
                limit: snapshot.limit,
                remaining: snapshot.remaining,
                reset: snapshot.reset,
                observed_at_unix_secs,
                stored_at_unix_secs: observed_at_unix_secs,
            };
            let _result = sink.enqueue(state);
        }
    }
}

#[derive(Clone)]
struct RequestMetricLabels {
    principal: String,
    principal_kind: Option<String>,
    upstream: String,
    upstream_event: Option<RequestEventUpstream>,
    model: String,
    status: String,
}

impl RequestMetricLabels {
    fn unknown(status: StatusCode) -> Self {
        Self {
            principal: "unknown".to_owned(),
            principal_kind: None,
            upstream: "unknown".to_owned(),
            upstream_event: None,
            model: "unknown".to_owned(),
            status: status_label(status),
        }
    }

    fn new(
        status: StatusCode,
        principal: &Principal,
        upstream: Option<&Upstream>,
        model: Option<&str>,
    ) -> Self {
        Self {
            principal: bounded_label(&principal.id),
            principal_kind: Some(principal_kind_label(&principal.kind).to_owned()),
            upstream: upstream
                .map(upstream_label)
                .unwrap_or_else(|| "unknown".to_owned()),
            upstream_event: upstream.map(request_event_upstream),
            model: model
                .map(bounded_label)
                .unwrap_or_else(|| "unknown".to_owned()),
            status: status_label(status),
        }
    }
}

// observe_finished_many fans out request labels to metrics, hooks, and event sinks in one pass.
#[allow(clippy::too_many_arguments)]
fn observe_finished_many(
    hooks: &[Arc<dyn ObservabilityHook>],
    request_event_sink: Option<&RequestEventSink>,
    dashboard_broadcaster: Option<&DashboardBroadcaster>,
    request_id: Option<&str>,
    status: StatusCode,
    started: Instant,
    labels: &RequestMetricLabels,
    usage: &Usage,
) {
    let duration_ms = started.elapsed().as_millis().try_into().unwrap_or(u64::MAX);
    metrics::counter!(
        "cc_lb_requests_total",
        "principal" => labels.principal.clone(),
        "upstream" => labels.upstream.clone(),
        "model" => labels.model.clone(),
        "status" => labels.status.clone(),
    )
    .increment(1);
    metrics::histogram!(
        "cc_lb_request_duration_seconds",
        "principal" => labels.principal.clone(),
        "upstream" => labels.upstream.clone(),
        "model" => labels.model.clone(),
        "status" => labels.status.clone(),
    )
    .record(duration_ms as f64 / 1000.0);
    if let Some(input_tokens) = usage.input_tokens {
        metrics::counter!(
            "cc_lb_tokens_total",
            "principal" => labels.principal.clone(),
            "upstream" => labels.upstream.clone(),
            "model" => labels.model.clone(),
            "direction" => "input",
            "status" => labels.status.clone(),
        )
        .increment(input_tokens);
    }
    if let Some(output_tokens) = usage.output_tokens {
        metrics::counter!(
            "cc_lb_tokens_total",
            "principal" => labels.principal.clone(),
            "upstream" => labels.upstream.clone(),
            "model" => labels.model.clone(),
            "direction" => "output",
            "status" => labels.status.clone(),
        )
        .increment(output_tokens);
    }
    let cost = virtual_cost_micros(
        &labels.model,
        usage.input_tokens.unwrap_or(0),
        usage.output_tokens.unwrap_or(0),
    );
    metrics::counter!(
        "cc_lb_virtual_cost_usd_total",
        "principal" => labels.principal.clone(),
        "upstream" => labels.upstream.clone(),
        "model" => labels.model.clone(),
        "pricing_status" => cost.pricing_status.as_label(),
    )
    .increment(cost.micros_usd.unwrap_or(0));
    observe_many(
        hooks,
        ObserveEvent::RequestFinished {
            status,
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
            duration_ms,
        },
    );
    enqueue_request_event(
        request_event_sink,
        dashboard_broadcaster,
        request_id,
        status,
        duration_ms,
        labels,
        usage,
    );
}

fn enqueue_request_event(
    request_event_sink: Option<&RequestEventSink>,
    dashboard_broadcaster: Option<&DashboardBroadcaster>,
    request_id: Option<&str>,
    status: StatusCode,
    duration_ms: u64,
    labels: &RequestMetricLabels,
    usage: &Usage,
) {
    if request_event_sink.is_none() && dashboard_broadcaster.is_none() {
        return;
    }
    let event = RequestEvent {
        ts: now_unix_secs(),
        request_id: request_id.unwrap_or("unknown").to_owned(),
        principal_id: known_label(&labels.principal),
        principal_kind: labels.principal_kind.clone(),
        upstream: labels.upstream_event,
        model: known_label(&labels.model),
        status: status.as_u16(),
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        duration_ms,
        error_code: None,
    };
    if let Some(sink) = request_event_sink {
        let _result = sink.enqueue(event.clone());
    }
    if let Some(broadcaster) = dashboard_broadcaster {
        broadcaster.publish(event);
    }
}

fn storage_identity(identity: LimitIdentity) -> (PrincipalLimitIdentityKind, Option<String>, bool) {
    match identity {
        LimitIdentity::Account(value) => (PrincipalLimitIdentityKind::Account, Some(value), true),
        LimitIdentity::Credential(value) => {
            (PrincipalLimitIdentityKind::Credential, Some(value), false)
        }
        LimitIdentity::Unobserved => (PrincipalLimitIdentityKind::Unobserved, None, false),
    }
}

fn storage_limit_kind(kind: AnthropicRateLimitKind) -> PrincipalLimitKind {
    match kind {
        AnthropicRateLimitKind::Requests => PrincipalLimitKind::Requests,
        AnthropicRateLimitKind::Tokens => PrincipalLimitKind::Tokens,
        AnthropicRateLimitKind::InputTokens => PrincipalLimitKind::InputTokens,
        AnthropicRateLimitKind::OutputTokens => PrincipalLimitKind::OutputTokens,
    }
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
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

fn status_label(status: StatusCode) -> String {
    status.as_u16().to_string()
}

fn upstream_label(upstream: &Upstream) -> String {
    match upstream {
        Upstream::AnthropicDirect => "anthropic_direct",
        Upstream::BedrockRuntime { .. } => "bedrock_runtime",
        Upstream::BedrockMantle { .. } => "bedrock_mantle",
        Upstream::Vertex { .. } => "vertex",
        Upstream::CustomAnthropicSpec { .. } => "custom_anthropic_spec",
    }
    .to_owned()
}

fn request_event_upstream(upstream: &Upstream) -> RequestEventUpstream {
    match upstream {
        Upstream::AnthropicDirect => RequestEventUpstream::AnthropicDirect,
        Upstream::BedrockRuntime { .. } => RequestEventUpstream::BedrockRuntime,
        Upstream::BedrockMantle { .. } => RequestEventUpstream::BedrockMantle,
        Upstream::Vertex { .. } => RequestEventUpstream::Vertex,
        Upstream::CustomAnthropicSpec { .. } => RequestEventUpstream::CustomAnthropicSpec,
    }
}

fn principal_kind_label(kind: &cc_lb_plugin_api::PrincipalKind) -> &'static str {
    match kind {
        cc_lb_plugin_api::PrincipalKind::ApiKey => "api_key",
        cc_lb_plugin_api::PrincipalKind::OAuthSubject => "oauth_subject",
        cc_lb_plugin_api::PrincipalKind::InternalKey => "internal_key",
        cc_lb_plugin_api::PrincipalKind::WorkloadIdentity => "workload_identity",
        cc_lb_plugin_api::PrincipalKind::SubscriptionBearer => "subscription_bearer",
    }
}

fn known_label(value: &str) -> Option<String> {
    if value == "unknown" {
        None
    } else {
        Some(value.to_owned())
    }
}

fn bounded_label(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return "unknown".to_owned();
    }

    let mut label = String::new();
    for ch in trimmed.chars().take(64) {
        if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | ':' | '@') {
            label.push(ch);
        } else {
            label.push('_');
        }
    }
    if label.is_empty() {
        "unknown".to_owned()
    } else {
        label
    }
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

fn model_allowed(model: Option<&str>, quotas: &PrincipalQuotas) -> bool {
    let Some(model) = model else {
        return true;
    };
    if quotas.allowed_models.is_empty() {
        return true;
    }
    quotas
        .allowed_models
        .iter()
        .any(|pattern| glob_match(pattern, model))
}

fn glob_match(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    let Some(star_index) = pattern.find('*') else {
        return pattern == value;
    };
    let (prefix, suffix_with_star) = pattern.split_at(star_index);
    let suffix = &suffix_with_star[1..];
    value.starts_with(prefix) && value.ends_with(suffix)
}

fn observe_many(hooks: &[Arc<dyn ObservabilityHook>], event: ObserveEvent) {
    for hook in hooks {
        let _result = hook.observe(event.clone());
    }
}
