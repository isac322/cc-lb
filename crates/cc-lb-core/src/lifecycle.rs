use std::convert::Infallible;
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::body::Body as AxumBody;
use bytes::Bytes;
use cc_lb_plugin_api::{
    ObservabilityHook, ObserveEvent, Principal, PrincipalKind, RequestContext, RetryDecision,
    RouterPlugin, SignedRequest, SignerFactory, Upstream, UpstreamError, shape_request,
    sign_request,
};
use cc_lb_pricing::{global_catalog, virtual_cost_micros_full};
use cc_lb_storage_api::types::StoredApiKeyRecord;
use cc_lb_storage_redb::{RequestEvent, Storage};
use http::header::{CONTENT_TYPE, RETRY_AFTER};
use http::{HeaderMap, HeaderName, HeaderValue, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use serde_json::{Value, json};
use thiserror::Error;

use crate::api_keys::builtin_authn::{AuthnSuccess, BuiltinAuthError, BuiltinAuthn};
use crate::api_keys::limit_engine::{LimitEngine, RejectReason, Reservation as LimitReservation};
use crate::api_keys::principal_view::PrincipalView;
use crate::api_keys::types::LimitKind;
use crate::audit_writer::{AuditEntry, AuditWriterSink};
use crate::error_format::{anthropic_error_response, anthropic_error_response_with_retry_after};
use crate::error_normalizer::{ErrorNormalizer, UpstreamKind};
use crate::hop_by_hop::strip_hop_by_hop;
use crate::sse_relay;

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

#[async_trait]
pub trait UpstreamDispatch: Send + Sync {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError>;
}

#[async_trait]
pub trait LimitSubjectProvider: Send + Sync {
    async fn limit_subject(
        &self,
        ctx: &RequestContext,
        principal: &Principal,
        authn_success: &AuthnSuccess,
    ) -> Option<LimitSubject>;
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

#[async_trait]
impl LimitSubjectProvider for StaticLimitSubjectProvider {
    async fn limit_subject(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        _authn_success: &AuthnSuccess,
    ) -> Option<LimitSubject> {
        Some(self.subject.clone())
    }
}

#[async_trait]
impl LimitSubjectProvider for BuiltinAuthn {
    async fn limit_subject(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        authn_success: &AuthnSuccess,
    ) -> Option<LimitSubject> {
        let mut record = authn_success.record.clone();
        record.key_hash_b64 = authn_success.key_id.clone();
        Some(LimitSubject {
            principal_id: authn_success.principal_id.clone(),
            key_id: authn_success.key_id.clone(),
            record,
        })
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
    principal_view: Arc<arc_swap::ArcSwap<PrincipalView>>,
    signer_factory: Arc<dyn ApiKeyAwareSignerFactory>,
    global_router: Arc<dyn RouterPlugin>,
    dispatcher: Arc<dyn UpstreamDispatch>,
    global_observability_hooks: Arc<[Arc<dyn ObservabilityHook>]>,
    error_normalizer: Arc<ErrorNormalizer>,
    config: LifecycleConfig,
    limit_engine: Option<Arc<LimitEngine>>,
    limit_subject_provider: Option<Arc<dyn LimitSubjectProvider>>,
    audit_sink: Option<Arc<AuditWriterSink>>,
    request_event_storage: Option<Arc<Storage>>,
}

impl Lifecycle {
    pub fn new(
        authn: Arc<BuiltinAuthn>,
        signer_factory: Arc<dyn ApiKeyAwareSignerFactory>,
        global_router: Arc<dyn RouterPlugin>,
        dispatcher: Arc<dyn UpstreamDispatch>,
        global_observability_hooks: Vec<Arc<dyn ObservabilityHook>>,
        config: LifecycleConfig,
    ) -> Self {
        Self {
            principal_view: authn.principal_view_cell(),
            authn,
            signer_factory,
            global_router,
            dispatcher,
            global_observability_hooks: Arc::from(global_observability_hooks),
            error_normalizer: Arc::new(ErrorNormalizer::new()),
            config,
            limit_engine: None,
            limit_subject_provider: None,
            audit_sink: None,
            request_event_storage: None,
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

    pub fn with_request_event_storage(mut self, storage: Arc<Storage>) -> Self {
        self.request_event_storage = Some(storage);
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

    #[allow(clippy::explicit_auto_deref)]
    pub async fn handle(&self, req: Request<Bytes>) -> Result<Response<Body>, ProxyError> {
        let view = self.principal_view.load_full();
        let started = Instant::now();
        let parsed = self.parse(req);
        let ctx = match parsed {
            Ok(ctx) => ctx,
            Err(response) => return Ok(*response),
        };

        // Pre-authn observe: global hooks only (no principal context). Silent no-op when global is empty.
        observe_many(
            &self.global_observability_hooks,
            ObserveEvent::RequestStarted {
                request_id: ctx.request_id.clone(),
                downstream_user_agent: header_to_string(&ctx.downstream_headers, "user-agent"),
            },
        );

        let success = if let Some(success) = self
            .authn
            .authenticate_none_mode(&ctx.downstream_headers)
            .await
        {
            success
        } else {
            match self
                .authn
                .authenticate(&ctx.downstream_headers, &view)
                .await
            {
                Ok(success) => success,
                Err(source) => {
                    record_key_auth_failure_metric(&source);
                    observe_error(
                        &self.global_observability_hooks,
                        "authentication_error",
                        &source.to_string(),
                        "authn",
                    );
                    let status = StatusCode::from_u16(source.http_status())
                        .unwrap_or(StatusCode::UNAUTHORIZED);
                    let response = match &source {
                        BuiltinAuthError::Unavailable => anthropic_error_response_with_retry_after(
                            StatusCode::SERVICE_UNAVAILABLE,
                            "authentication_error",
                            &source.to_string(),
                            1,
                        ),
                        _ => anthropic_error_response(
                            status,
                            "authentication_error",
                            &source.to_string(),
                        ),
                    };
                    observe_finished(&self.global_observability_hooks, status, started);
                    return Ok(response);
                }
            }
        };
        let principal_id = success.principal_id.clone();
        let Some(cached) = view.get(&principal_id) else {
            tracing::error!(%principal_id, "authenticated principal missing from principal view");
            observe_error(
                &self.global_observability_hooks,
                "principal_missing",
                "authenticated principal is unavailable",
                "authn",
            );
            let response = anthropic_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "api_error",
                "authenticated principal is unavailable",
            );
            observe_finished(
                &self.global_observability_hooks,
                StatusCode::INTERNAL_SERVER_ERROR,
                started,
            );
            return Ok(response);
        };
        let router = cached.resolved_router(&self.global_router);
        let hooks = cached.resolved_hooks(&self.global_observability_hooks);
        let stream_hooks = StreamHooks::new(hooks);
        let principal = Principal {
            id: principal_id,
            kind: PrincipalKind::ApiKey,
            claims: serde_json::Map::new(),
        };

        observe_many(
            hooks,
            ObserveEvent::AuthnComplete {
                principal_id: principal.id.clone(),
                kind: principal.kind.clone(),
            },
        );

        let route = match router.route(&ctx, &principal) {
            Ok(route) => route,
            Err(source) => {
                observe_error(hooks, "route_not_configured", &source.to_string(), "router");
                let response = anthropic_error_response(
                    StatusCode::BAD_GATEWAY,
                    "route_not_configured",
                    "no upstream route is configured for this request",
                );
                observe_finished_for_principal(
                    hooks,
                    StatusCode::BAD_GATEWAY,
                    started,
                    &principal,
                    &ctx.body_bytes,
                );
                return Ok(response);
            }
        };
        let metric_context = ApiKeyMetricContext::new(&success, &route.upstream, &ctx.body_bytes);

        observe_many(
            hooks,
            ObserveEvent::UpstreamChosen {
                upstream: route.upstream.clone(),
            },
        );

        let mut active_limit = match self
            .reserve_limit(&view, &ctx, &principal, &route, &success)
            .await
        {
            Ok(active_limit) => active_limit,
            Err(response) => {
                observe_finished_for_principal(
                    hooks,
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
                observe_error(
                    hooks,
                    "signing_error",
                    &source.to_string(),
                    "signer_factory",
                );
                let mut response = anthropic_error_response(
                    StatusCode::BAD_GATEWAY,
                    "api_error",
                    "failed to prepare upstream credentials",
                );
                self.attach_limit_headers(&mut response, active_limit.as_ref());
                observe_finished_for_principal(
                    hooks,
                    StatusCode::BAD_GATEWAY,
                    started,
                    &principal,
                    &ctx.body_bytes,
                );
                return Ok(response);
            }
        };

        let mut response = match self
            .attempt(hooks, &ctx, &principal, &route, signer.clone())
            .await
        {
            Ok(response) => response,
            Err(response) => {
                let mut response = *response;
                self.attach_limit_headers(&mut response, active_limit.as_ref());
                observe_finished_for_principal(
                    hooks,
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
                response = match self
                    .attempt(hooks, &ctx, &principal, &route, new_signer)
                    .await
                {
                    Ok(response) => response,
                    Err(response) => {
                        let mut response = *response;
                        self.attach_limit_headers(&mut response, active_limit.as_ref());
                        observe_finished_for_principal(
                            hooks,
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
                record_api_key_request_metric(&metric_context, response.status());
                observe_finished_for_principal(
                    hooks,
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
            record_api_key_request_metric(&metric_context, response.status());
            observe_finished_for_principal(
                hooks,
                response.status(),
                started,
                &principal,
                &ctx.body_bytes,
            );
            return Ok(response);
        }

        let status = response.status();
        record_api_key_request_metric(&metric_context, status);
        response = self
            .finish_success_response(
                response,
                active_limit.take(),
                &metric_context,
                started.elapsed(),
                status,
                hooks,
                stream_hooks,
            )
            .await;
        observe_finished_for_principal(hooks, status, started, &principal, &ctx.body_bytes);
        Ok(response)
    }

    #[allow(clippy::result_large_err)]
    async fn reserve_limit(
        &self,
        view: &PrincipalView,
        ctx: &RequestContext,
        principal: &Principal,
        route: &cc_lb_plugin_api::RouteDecision,
        authn_success: &AuthnSuccess,
    ) -> Result<Option<ActiveLimit>, Response<Body>> {
        let (Some(limit_engine), Some(subject_provider)) = (
            self.limit_engine.as_ref(),
            self.limit_subject_provider.as_ref(),
        ) else {
            return Ok(None);
        };
        let Some(subject) = subject_provider
            .limit_subject(ctx, principal, authn_success)
            .await
        else {
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
            view,
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
                record_limit_reject_metrics(&reason, &subject.key_id);
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

    #[allow(clippy::too_many_arguments)]
    async fn finish_success_response(
        &self,
        response: Response<Body>,
        active_limit: Option<ActiveLimit>,
        metric_context: &ApiKeyMetricContext,
        duration: Duration,
        status: StatusCode,
        hooks: &[Arc<dyn ObservabilityHook>],
        stream_hooks: StreamHooks,
    ) -> Response<Body> {
        let mut active_limit = active_limit;
        if active_limit
            .as_ref()
            .is_some_and(|active_limit| active_limit.request.stream)
            || is_sse_response(response.headers())
        {
            let response_status = response.status();
            let mut response = self.relay_response(
                response,
                response_status,
                Instant::now() - duration,
                stream_hooks,
            );
            self.attach_limit_headers(&mut response, active_limit.as_ref());
            return response;
        }

        let (mut parts, body) = response.into_parts();
        let body = match body.collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(_source) => Bytes::new(),
        };
        let usage = usage_from_json_body(&body);
        if usage.present {
            observe_many(
                hooks,
                ObserveEvent::RequestFinished {
                    status,
                    input_tokens: Some(usage.input_tokens),
                    output_tokens: Some(usage.output_tokens),
                    duration_ms: duration_to_ms(duration),
                },
            );
        }
        let cost_micros = if usage.present {
            let cost_model = active_limit
                .as_ref()
                .map(|active_limit| active_limit.request.model.as_str())
                .unwrap_or(metric_context.model.as_str());
            let pricing_upstream_kind = active_limit
                .as_ref()
                .and_then(|active_limit| active_limit.upstream_kind)
                .or(metric_context.pricing_upstream_kind);
            let cost_micros = virtual_cost_micros_full(
                cost_model,
                usage.input_tokens,
                usage.output_tokens,
                usage.cache_creation_input_tokens,
                usage.cache_read_input_tokens,
                pricing_upstream_kind,
            )
            .micros_usd
            .unwrap_or(0);
            record_api_key_usage_metrics(metric_context, &usage, cost_micros);
            cost_micros
        } else {
            0
        };

        if let (Some(storage), Some(active_limit)) =
            (self.request_event_storage.as_ref(), active_limit.as_ref())
        {
            let event = RequestEvent {
                ts_ms: unix_now_ms(),
                principal_id: active_limit.subject.principal_id.clone(),
                key_id: active_limit.subject.key_id.clone(),
                model: active_limit.request.model.clone(),
                input_tokens: usage.input_tokens,
                output_tokens: usage.output_tokens,
                cache_creation_input_tokens: usage.cache_creation_input_tokens,
                cache_read_input_tokens: usage.cache_read_input_tokens,
                cost_usd_micros: cost_micros as i64,
                duration_ms: duration_to_ms(duration),
                status: status.as_u16(),
            };
            if let Err(error) = storage.append_request_event(&event) {
                tracing::warn!(%error, "failed to append api key request event");
            }
        }

        if let (Some(limit_engine), Some(active_limit)) =
            (self.limit_engine.as_ref(), active_limit.as_mut())
        {
            let Some(reservation) = active_limit.reservation.take() else {
                return Response::from_parts(parts, Body::from(body));
            };
            let cost_micros = cost_micros as i64;
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
        hooks: &[Arc<dyn ObservabilityHook>],
        ctx: &RequestContext,
        principal: &Principal,
        route: &cc_lb_plugin_api::RouteDecision,
        signer: Arc<dyn cc_lb_plugin_api::Signer>,
    ) -> Result<Response<Body>, Box<Response<Body>>> {
        let shaped = shape_request(route.dialect.as_ref(), ctx, &route.upstream, principal)
            .map_err(|source| {
                observe_error(hooks, "shape_error", &source.to_string(), "dialect");
                Box::new(anthropic_error_response(
                    StatusCode::BAD_GATEWAY,
                    "api_error",
                    "failed to shape upstream request",
                ))
            })?;
        let signed = sign_request(signer.as_ref(), shaped)
            .await
            .map_err(|source| {
                observe_error(hooks, "signing_error", &source.to_string(), "signer");
                Box::new(anthropic_error_response(
                    StatusCode::BAD_GATEWAY,
                    "api_error",
                    "failed to sign upstream request",
                ))
            })?;
        self.dispatcher.dispatch(signed).await.map_err(|source| {
            observe_error(
                hooks,
                "upstream_dispatch_error",
                &source.to_string(),
                "dispatch",
            );
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

    fn relay_response(
        &self,
        response: Response<Body>,
        status: StatusCode,
        started: Instant,
        hooks: StreamHooks,
    ) -> Response<Body> {
        let (mut parts, mut body) = response.into_parts();
        strip_hop_by_hop(&mut parts.headers);
        let stream = async_stream::stream! {
            let mut batch_index = 0_u64;
            let mut buffer: Vec<u8> = Vec::new();
            let mut usage = UsageCounts::default();
            while let Some(frame) = body.frame().await {
                match frame {
                    Ok(frame) => {
                        if let Ok(data) = frame.into_data() {
                            buffer.extend_from_slice(&data);
                            while let Some(end) = sse_relay::find_sse_event_end(&buffer) {
                                let raw = buffer.drain(..end).collect::<Vec<u8>>();
                                accumulate_sse_usage(&raw, &mut usage);
                            }
                            observe_many(hooks.as_slice(), ObserveEvent::Chunk {
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
            let (input_tokens, output_tokens) = if usage.present {
                (Some(usage.input_tokens), Some(usage.output_tokens))
            } else {
                (None, None)
            };
            observe_many(hooks.as_slice(), ObserveEvent::RequestFinished {
                status,
                input_tokens,
                output_tokens,
                duration_ms: started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
            });
        };
        Response::from_parts(parts, Body::from_stream(stream))
    }
}

#[derive(Clone)]
struct StreamHooks {
    hooks: Arc<[Arc<dyn ObservabilityHook>]>,
}

impl StreamHooks {
    fn new(hooks: &[Arc<dyn ObservabilityHook>]) -> Self {
        Self {
            hooks: hooks.iter().cloned().collect(),
        }
    }

    fn as_slice(&self) -> &[Arc<dyn ObservabilityHook>] {
        &self.hooks
    }
}

fn observe_error(hooks: &[Arc<dyn ObservabilityHook>], code: &str, message: &str, source: &str) {
    observe_many(
        hooks,
        ObserveEvent::Error {
            code: code.to_owned(),
            message: message.to_owned(),
            source: source.to_owned(),
        },
    );
}

fn observe_finished(hooks: &[Arc<dyn ObservabilityHook>], status: StatusCode, started: Instant) {
    observe_many(
        hooks,
        ObserveEvent::RequestFinished {
            status,
            input_tokens: None,
            output_tokens: None,
            duration_ms: started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
        },
    );
}

fn observe_finished_for_principal(
    hooks: &[Arc<dyn ObservabilityHook>],
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
    let usage = sse_relay::usage_from_json_bytes(body);
    let input_tokens = (usage.input_tokens > 0).then_some(usage.input_tokens);
    let output_tokens = (usage.output_tokens > 0).then_some(usage.output_tokens);
    observe_many(
        hooks,
        ObserveEvent::RequestFinished {
            status,
            input_tokens,
            output_tokens,
            duration_ms: started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
        },
    );
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
    present: bool,
    input_tokens: u64,
    output_tokens: u64,
    cache_creation_input_tokens: u64,
    cache_read_input_tokens: u64,
}

struct ApiKeyMetricContext {
    key_id: String,
    principal_id: String,
    model: String,
    upstream_kind: &'static str,
    pricing_upstream_kind: Option<cc_lb_pricing::UpstreamKind>,
}

impl ApiKeyMetricContext {
    fn new(success: &AuthnSuccess, upstream: &Upstream, body: &Bytes) -> Self {
        Self {
            key_id: success.key_id.clone(),
            principal_id: success.principal_id.clone(),
            model: extract_model(body).unwrap_or_else(|| "unknown".to_owned()),
            upstream_kind: audit_upstream_name(upstream),
            pricing_upstream_kind: pricing_upstream_kind(upstream),
        }
    }
}

fn unix_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn duration_to_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn record_api_key_request_metric(context: &ApiKeyMetricContext, status: StatusCode) {
    metrics::counter!(
        "cclb_api_key_requests_total",
        "key_id" => context.key_id.clone(),
        "principal_id" => context.principal_id.clone(),
        "model" => context.model.clone(),
        "upstream_kind" => context.upstream_kind,
        "status" => status.as_u16().to_string()
    )
    .increment(1);
}

fn record_api_key_usage_metrics(
    context: &ApiKeyMetricContext,
    usage: &UsageCounts,
    cost_micros: u64,
) {
    increment_token_metric(&context.key_id, "input", usage.input_tokens);
    increment_token_metric(&context.key_id, "output", usage.output_tokens);
    increment_token_metric(
        &context.key_id,
        "cache_creation",
        usage.cache_creation_input_tokens,
    );
    increment_token_metric(&context.key_id, "cache_read", usage.cache_read_input_tokens);

    if cost_micros > 0 {
        metrics::counter!(
            "cclb_api_key_cost_usd_micro_total",
            "key_id" => context.key_id.clone()
        )
        .increment(cost_micros);
    }

    metrics::counter!(
        "cc_lb_tokens_total",
        "principal" => context.principal_id.clone(),
        "upstream" => context.upstream_kind,
        "model" => context.model.clone(),
        "direction" => "input"
    )
    .increment(usage.input_tokens);
    metrics::counter!(
        "cc_lb_tokens_total",
        "principal" => context.principal_id.clone(),
        "upstream" => context.upstream_kind,
        "model" => context.model.clone(),
        "direction" => "output"
    )
    .increment(usage.output_tokens);
    metrics::counter!(
        "cc_lb_virtual_cost_usd_total",
        "principal" => context.principal_id.clone(),
        "upstream" => context.upstream_kind,
        "model" => context.model.clone()
    )
    .increment(cost_micros);
}

fn increment_token_metric(key_id: &str, kind: &'static str, value: u64) {
    if value == 0 {
        return;
    }

    metrics::counter!(
        "cclb_api_key_tokens_total",
        "key_id" => key_id.to_owned(),
        "kind" => kind
    )
    .increment(value);
}

fn record_key_auth_failure_metric(source: &BuiltinAuthError) {
    metrics::counter!(
        "cclb_key_auth_failures_total",
        "reason" => key_auth_failure_reason(source)
    )
    .increment(1);
}

fn key_auth_failure_reason(source: &BuiltinAuthError) -> &'static str {
    match source {
        BuiltinAuthError::Expired => "Expired",
        BuiltinAuthError::KeyDisabled => "Disabled",
        BuiltinAuthError::KeyRevoked => "Revoked",
        BuiltinAuthError::PrincipalDisabled => "PrincipalDisabled",
        BuiltinAuthError::Unavailable => "Unavailable",
        BuiltinAuthError::MissingHeader
        | BuiltinAuthError::InvalidFormat
        | BuiltinAuthError::NotFound
        | BuiltinAuthError::SignatureMismatch
        | BuiltinAuthError::PrincipalMissing => "InvalidKey",
    }
}

fn record_limit_reject_metrics(reason: &RejectReason, key_id: &str) {
    if let Some(kind) = limit_reject_metric_kind(reason) {
        metrics::counter!(
            "cclb_limit_hits_total",
            "kind" => kind,
            "key_id" => key_id.to_owned()
        )
        .increment(1);
    }

    if matches!(reason, RejectReason::ConcurrentRateLimit) {
        metrics::counter!(
            "cclb_concurrent_rejects_total",
            "key_id" => key_id.to_owned()
        )
        .increment(1);
    }
}

fn limit_reject_metric_kind(reason: &RejectReason) -> Option<&'static str> {
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

fn accumulate_sse_usage(raw: &[u8], usage: &mut UsageCounts) {
    let text = match std::str::from_utf8(raw) {
        Ok(text) => text,
        Err(_) => return,
    };
    for line in text.lines() {
        let Some(payload) = line.strip_prefix("data:").map(str::trim_start) else {
            continue;
        };
        let Ok(value) = serde_json::from_str::<Value>(payload) else {
            continue;
        };
        let reported = value
            .get("usage")
            .or_else(|| value.get("message").and_then(|m| m.get("usage")));
        let Some(reported) = reported else {
            continue;
        };
        usage.present = true;
        if let Some(input_tokens) = reported.get("input_tokens").and_then(Value::as_u64) {
            usage.input_tokens = input_tokens;
        }
        if let Some(output_tokens) = reported.get("output_tokens").and_then(Value::as_u64) {
            usage.output_tokens = output_tokens;
        }
        if let Some(cache_creation) = reported
            .get("cache_creation_input_tokens")
            .and_then(Value::as_u64)
        {
            usage.cache_creation_input_tokens = cache_creation;
        }
        if let Some(cache_read) = reported
            .get("cache_read_input_tokens")
            .and_then(Value::as_u64)
        {
            usage.cache_read_input_tokens = cache_read;
        }
    }
}

fn usage_from_json_body(body: &Bytes) -> UsageCounts {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return UsageCounts::default();
    };
    let Some(usage) = value.get("usage") else {
        return UsageCounts::default();
    };
    UsageCounts {
        present: true,
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
    if status == StatusCode::TOO_MANY_REQUESTS
        && let Some(value) =
            retry_after_seconds.and_then(|sec| HeaderValue::from_str(&sec.to_string()).ok())
    {
        response.headers_mut().insert(RETRY_AFTER, value);
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
        Upstream::CustomAnthropicSpec { .. } => "custom_anthropic_spec",
    }
}

fn pricing_upstream_kind(upstream: &Upstream) -> Option<cc_lb_pricing::UpstreamKind> {
    match upstream {
        Upstream::AnthropicDirect | Upstream::CustomAnthropicSpec { .. } => {
            Some(cc_lb_pricing::UpstreamKind::AnthropicKey)
        }
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
