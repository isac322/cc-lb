use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use cc_lb_plugin_api::{
    DialectError, Principal, RequestContext, RetryDecision, RouteDecision, RouteError,
    RouterPlugin, ShapedRequest, ShapedRequestBuilder, SignedRequest, Signer, SignerError,
    SignerFactory, SigningCapability, SubscriptionQuotaCandidateSnapshot,
    SubscriptionQuotaDataState, Upstream, UpstreamCandidate, UpstreamDialect, UpstreamError,
};
use cc_lb_plugin_wire::augmented_metadata::AugmentedMetadata;
use cc_lb_plugin_wire::identity::{CC_LB_PLUGIN_MAGIC, PluginIdentity};
use cc_lb_plugin_wire::v1::CandidateWire;
use cc_lb_plugin_wire::v1::build_signer::{BuildSignerFn, BuildSignerRequest};
use cc_lb_plugin_wire::v1::common::{
    DialectBinding, HeaderWire, Principal as PrincipalWire, RateLimitObservationWire, RequestWire,
    ShapedRequestWire, SubscriptionQuotaCandidateSnapshotWire, UpstreamErrorCategory,
    UpstreamErrorWire, UpstreamWire,
};
use cc_lb_plugin_wire::v1::normalize_error::{NormalizeErrorFn, NormalizeErrorRequest};
use cc_lb_plugin_wire::v1::observe::ObserveFn;
use cc_lb_plugin_wire::v1::on_unauthorized::{OnUnauthorizedFn, OnUnauthorizedRequest};
use cc_lb_plugin_wire::v1::route::{RouteFn, RouteRequest};
use cc_lb_plugin_wire::v1::shape::{ShapeFn, ShapeRequest};
use cc_lb_plugin_wire::v1::sign::{SignFn, SignRequest};
use cc_lb_plugin_wire::wire_function::{FallbackPolicy, WireFunction};
use http::header::{HeaderName, HeaderValue};
use http::{HeaderMap, Method, StatusCode};
use serde_json::Value;
use tokio::sync::oneshot;
use url::Url;
use uuid::Uuid;

use crate::dispatch::{DispatchOutcome, dispatch_wire_call};
use crate::{PluginCell, PluginSlot, ResourceLimits};

#[derive(Clone)]
pub(crate) struct ExtismRouterPlugin {
    slot: Arc<PluginSlot>,
}

impl ExtismRouterPlugin {
    pub(crate) fn new(slot: Arc<PluginSlot>) -> Self {
        Self { slot }
    }
}

impl RouterPlugin for ExtismRouterPlugin {
    fn route(
        &self,
        ctx: &RequestContext,
        principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError> {
        let request = RouteRequest {
            request_id: ctx.request_id.clone(),
            headers: request_headers_to_wire(ctx),
            method: ctx.method.as_str().to_owned(),
            path: ctx.path.clone(),
            query: ctx.query.clone(),
            body_base64: BASE64.encode(&ctx.body_bytes),
            principal: principal_to_wire(principal),
            candidates: candidates_to_wire(candidates),
        };
        let response = match self.slot.dispatch_wire_call_sync::<RouteFn>(request) {
            DispatchOutcome::Ok(response) => response,
            DispatchOutcome::Fallback(FallbackPolicy::UseDefault) => {
                return Ok(default_route_decision(self.slot.clone()));
            }
            DispatchOutcome::Fallback(policy) => return Err(route_unexpected_fallback(policy)),
        };
        let dialect: Arc<dyn UpstreamDialect> = match response.dialect {
            DialectBinding::SelfReferenced => Arc::new(ExtismDialectPlugin::new(self.slot.clone())),
        };
        let upstream = upstream_from_wire(response.upstream).map_err(route_runtime_message)?;
        let upstream_id = response
            .upstream_id
            .map(|upstream_id| Uuid::parse_str(&upstream_id))
            .transpose()
            .map_err(|source| RouteError::Runtime {
                reason: format!("plugin returned invalid upstream_id: {source}"),
            })?;
        Ok(RouteDecision {
            upstream_id,
            upstream,
            dialect,
        })
    }
}

#[derive(Clone)]
pub(crate) struct ExtismDialectPlugin {
    slot: Arc<PluginSlot>,
}

impl ExtismDialectPlugin {
    pub(crate) fn new(slot: Arc<PluginSlot>) -> Self {
        Self { slot }
    }
}

impl UpstreamDialect for ExtismDialectPlugin {
    fn shape(
        &self,
        ctx: &RequestContext,
        upstream: &Upstream,
        principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        let request = ShapeRequest {
            request: request_to_wire(ctx),
            upstream: upstream_to_wire(upstream),
            principal: principal_to_wire(principal),
        };
        let response = match self.slot.dispatch_wire_call_sync::<ShapeFn>(request) {
            DispatchOutcome::Ok(response) => response,
            DispatchOutcome::Fallback(FallbackPolicy::FailRequest) => {
                return Err(DialectError::UnsupportedRequest {
                    reason: "plugin shape failed".to_owned(),
                });
            }
            DispatchOutcome::Fallback(policy) => return Err(dialect_unexpected_fallback(policy)),
        };
        let url =
            Url::parse(&response.url).map_err(|source| DialectError::InvalidUrl { source })?;
        let method = response.method.parse::<Method>().map_err(|source| {
            DialectError::UnsupportedRequest {
                reason: format!("plugin returned invalid method: {source}"),
            }
        })?;
        let mut headers = headers_from_wire(response.headers).map_err(|source| {
            DialectError::UnsupportedRequest {
                reason: source.to_string(),
            }
        })?;
        let body = BASE64.decode(response.body_base64).map_err(|source| {
            DialectError::UnsupportedRequest {
                reason: format!("plugin returned invalid body_base64: {source}"),
            }
        })?;
        // Body may have been mutated by the plugin; drop stale Content-Length
        // so hyper recomputes it from the actual body bytes.
        headers.remove(http::header::CONTENT_LENGTH);
        Ok(builder.shaped_request(url, method, headers, Bytes::from(body)))
    }

    fn normalize_error(&self, status: StatusCode, body: &Bytes) -> Option<Bytes> {
        let request = NormalizeErrorRequest {
            status: status.as_u16(),
            body_base64: BASE64.encode(body),
        };
        let response = match self
            .slot
            .dispatch_wire_call_sync::<NormalizeErrorFn>(request)
        {
            DispatchOutcome::Ok(response) => response,
            DispatchOutcome::Fallback(FallbackPolicy::PassThrough) => return None,
            DispatchOutcome::Fallback(_) => return None,
        };
        response
            .body_base64
            .and_then(|body| BASE64.decode(body).ok())
            .map(Bytes::from)
    }
}

#[derive(Clone)]
pub(crate) struct ExtismSignerFactory {
    slot: Arc<PluginSlot>,
    factory_state: Value,
}

impl ExtismSignerFactory {
    pub(crate) fn new(slot: Arc<PluginSlot>, factory_state: Value) -> Self {
        Self {
            slot,
            factory_state,
        }
    }
}

#[async_trait]
impl SignerFactory for ExtismSignerFactory {
    async fn build(&self, upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        let request = BuildSignerRequest {
            upstream: upstream_to_wire(upstream),
            factory_state: self.factory_state.clone(),
        };
        let response = match self
            .slot
            .dispatch_wire_call_async::<BuildSignerFn>(request)
            .await
        {
            DispatchOutcome::Ok(response) => response,
            DispatchOutcome::Fallback(FallbackPolicy::FailRequest) => {
                return Err(SignerError::SigningFailed {
                    reason: "plugin build_signer failed".to_owned(),
                });
            }
            DispatchOutcome::Fallback(policy) => return Err(signer_unexpected_fallback(policy)),
        };
        Ok(Arc::new(ExtismSigner {
            slot: self.slot.clone(),
            signer_state: response.signer_state,
        }))
    }
}

pub(crate) struct ExtismSigner {
    slot: Arc<PluginSlot>,
    signer_state: Value,
}

#[async_trait]
impl Signer for ExtismSigner {
    async fn sign(
        &self,
        mut shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        let request = SignRequest {
            shaped: shaped_request_to_wire(&shaped),
            signer_state: self.signer_state.clone(),
        };
        let response = match self.slot.dispatch_wire_call_async::<SignFn>(request).await {
            DispatchOutcome::Ok(response) => response,
            DispatchOutcome::Fallback(FallbackPolicy::FailRequest) => {
                return Err(SignerError::SigningFailed {
                    reason: "plugin sign failed".to_owned(),
                });
            }
            DispatchOutcome::Fallback(policy) => return Err(signer_unexpected_fallback(policy)),
        };
        if let Some(url) = response.url {
            shaped.set_url(
                Url::parse(&url).map_err(|source| SignerError::SigningFailed {
                    reason: format!("plugin returned invalid url: {source}"),
                })?,
            );
        }
        if let Some(method) = response.method {
            shaped.set_method(method.parse::<Method>().map_err(|source| {
                SignerError::SigningFailed {
                    reason: format!("plugin returned invalid method: {source}"),
                }
            })?);
        }
        if let Some(headers) = response.headers {
            let headers =
                headers_from_wire(headers).map_err(|source| SignerError::SigningFailed {
                    reason: source.to_string(),
                })?;
            shaped.headers_mut().clear();
            for (name, value) in headers {
                if let Some(name) = name {
                    shaped.headers_mut().append(name, value);
                }
            }
        }
        if let Some(body_base64) = response.body_base64 {
            let body = BASE64
                .decode(body_base64)
                .map_err(|source| SignerError::SigningFailed {
                    reason: format!("plugin returned invalid body_base64: {source}"),
                })?;
            shaped.set_body(Bytes::from(body));
        }
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(&self, err: &UpstreamError) -> RetryDecision {
        let request = OnUnauthorizedRequest {
            error: upstream_error_to_wire(err),
            signer_state: self.signer_state.clone(),
        };
        let response = match self
            .slot
            .dispatch_wire_call_async::<OnUnauthorizedFn>(request)
            .await
        {
            DispatchOutcome::Ok(response) => response,
            DispatchOutcome::Fallback(FallbackPolicy::PassThrough) => return RetryDecision::Fail,
            DispatchOutcome::Fallback(_) => return RetryDecision::Fail,
        };
        match response.decision.as_str() {
            "refresh" => RetryDecision::Refresh {
                new_signer: Arc::new(ExtismSigner {
                    slot: self.slot.clone(),
                    signer_state: response.signer_state,
                }),
            },
            _ => RetryDecision::Fail,
        }
    }
}

impl PluginSlot {
    pub(crate) fn dispatch_wire_call_sync<F>(
        self: &Arc<Self>,
        request: F::Request,
    ) -> DispatchOutcome<F::Response>
    where
        F: WireFunction + Send + 'static,
        F::Request: Send + 'static,
        F::Response: Send + 'static,
    {
        let slot = self.clone();
        let thread = match std::thread::Builder::new()
            .name(format!("cc-lb-extism-{}", F::NAME))
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build();
                match runtime {
                    Ok(runtime) => runtime.block_on(slot.dispatch_wire_call_async::<F>(request)),
                    Err(source) => runtime_dispatch_fallback::<F, F::Response>(
                        "runtime_build",
                        source.to_string(),
                    ),
                }
            }) {
            Ok(thread) => thread,
            Err(source) => {
                return runtime_dispatch_fallback::<F, F::Response>(
                    "thread_spawn",
                    source.to_string(),
                );
            }
        };
        match thread.join() {
            Ok(outcome) => outcome,
            Err(_) => runtime_dispatch_fallback::<F, F::Response>("thread_join", "panic"),
        }
    }

    pub(crate) async fn dispatch_wire_call_async<F>(
        &self,
        request: F::Request,
    ) -> DispatchOutcome<F::Response>
    where
        F: WireFunction + Send + 'static,
        F::Request: Send + 'static,
        F::Response: Send + 'static,
    {
        let cell = self.current.load_full();
        let metadata = self.dispatch_metadata();
        let limits = match self.limits_for_dispatch() {
            Some(limits) => limits,
            None => {
                return runtime_dispatch_fallback::<F, F::Response>("slot_metadata", "unavailable");
            }
        };
        let started = Instant::now();
        let outcome =
            dispatch_with_timeout::<F>(cell, metadata, request, limits.max_call_duration).await;
        metrics::histogram!(
            "cc_lb_extism_call_duration_seconds",
            "plugin" => self.name.clone(),
            "hook" => F::NAME.to_owned(),
        )
        .record(started.elapsed().as_secs_f64());
        outcome
    }

    fn limits_for_dispatch(&self) -> Option<ResourceLimits> {
        self.entry.read().ok().map(|entry| entry.limits.clone())
    }

    fn dispatch_metadata(&self) -> AugmentedMetadata {
        let Ok(entry) = self.entry.read() else {
            return legacy_dispatch_metadata(&self.name);
        };
        entry
            .original
            .metadata
            .get("augmented_metadata")
            .cloned()
            .and_then(|value| serde_json::from_value(value).ok())
            .unwrap_or_else(|| legacy_dispatch_metadata(&entry.name))
    }
}

async fn dispatch_with_timeout<F>(
    cell: Arc<PluginCell>,
    metadata: AugmentedMetadata,
    request: F::Request,
    timeout_duration: Duration,
) -> DispatchOutcome<F::Response>
where
    F: WireFunction + Send + 'static,
    F::Request: Send + 'static,
    F::Response: Send + 'static,
{
    let (cancel_tx, cancel_rx) = oneshot::channel();
    let task = tokio::task::spawn_blocking(move || {
        let mut plugin = match cell.plugin.lock() {
            Ok(plugin) => plugin,
            Err(_) => {
                return runtime_dispatch_fallback::<F, F::Response>("plugin_lock", "poisoned");
            }
        };
        let _ = cancel_tx.send(plugin.cancel_handle());
        dispatch_wire_call::<F>(&mut plugin, &metadata, request)
    });

    let cancel_handle = tokio::time::timeout(Duration::from_millis(25), cancel_rx)
        .await
        .ok()
        .and_then(Result::ok);

    match tokio::time::timeout(timeout_duration, task).await {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(source)) => {
            runtime_dispatch_fallback::<F, F::Response>("task_join", source.to_string())
        }
        Err(_) => {
            if let Some(cancel_handle) = cancel_handle {
                let _ = cancel_handle.cancel();
            }
            runtime_dispatch_fallback::<F, F::Response>("timeout", "plugin hook timed out")
        }
    }
}

fn runtime_dispatch_fallback<F, R>(
    stage: &'static str,
    reason: impl fmt::Display,
) -> DispatchOutcome<R>
where
    F: WireFunction,
{
    metrics::counter!(
        "cc_lb_plugin_dispatch_errors_total",
        "function" => F::NAME,
        "stage" => stage,
    )
    .increment(1);
    tracing::warn!(
        target: "cc_lb_plugin.dispatch",
        function = F::NAME,
        stage,
        reason = %reason,
        "dispatch runtime error, applying fallback {:?}",
        F::FALLBACK
    );
    DispatchOutcome::Fallback(F::FALLBACK)
}

fn legacy_dispatch_metadata(plugin_name: &str) -> AugmentedMetadata {
    AugmentedMetadata {
        identity: PluginIdentity {
            magic: CC_LB_PLUGIN_MAGIC,
            abi_envelope: 1,
            plugin_name: plugin_name.to_owned(),
            plugin_version: "legacy".to_owned(),
        },
        negotiated_functions: BTreeMap::from([
            function_version::<RouteFn>(),
            function_version::<ShapeFn>(),
            function_version::<NormalizeErrorFn>(),
            function_version::<BuildSignerFn>(),
            function_version::<SignFn>(),
            function_version::<OnUnauthorizedFn>(),
            function_version::<ObserveFn>(),
        ]),
        negotiated_capabilities: BTreeSet::new(),
        handshake_completed_at: 1,
        self_check_passed: true,
        self_check_completed_at: 1,
        expires_at: i64::MAX,
    }
}

fn function_version<F: WireFunction>() -> (String, u32) {
    let version = F::SUPPORTED_VERSIONS
        .iter()
        .copied()
        .max()
        .unwrap_or_default();
    (F::NAME.to_owned(), version)
}

fn candidates_to_wire(candidates: &[UpstreamCandidate]) -> Vec<CandidateWire> {
    candidates
        .iter()
        .map(|candidate| CandidateWire {
            upstream_id: candidate.upstream_id.to_string(),
            name: candidate.name.clone(),
            kind: candidate.kind.as_str().to_owned(),
            observed_rate_limits: candidate
                .observed_rate_limits
                .iter()
                .map(|observation| RateLimitObservationWire {
                    kind: observation.kind.as_str().to_owned(),
                    window: observation.window.clone(),
                    limit: observation.limit,
                    remaining: observation.remaining,
                    reset: observation.reset.clone(),
                })
                .collect(),
            subscription_quotas: candidate
                .subscription_quotas
                .iter()
                .map(|snapshot| {
                    subscription_quota_to_wire(snapshot, candidate.observed_at_unix_secs)
                })
                .collect(),
            observed_at_unix_secs: candidate.observed_at_unix_secs,
        })
        .collect()
}

fn subscription_quota_to_wire(
    snapshot: &SubscriptionQuotaCandidateSnapshot,
    observed_at_unix_secs: u64,
) -> SubscriptionQuotaCandidateSnapshotWire {
    SubscriptionQuotaCandidateSnapshotWire {
        window: snapshot.window.clone(),
        source: snapshot
            .source
            .clone()
            .unwrap_or_else(|| "missing".to_owned()),
        data_state: subscription_quota_data_state_to_wire(snapshot.state).to_owned(),
        utilization: snapshot.utilization,
        status: snapshot.status.clone(),
        resets_at_unix_secs: snapshot.resets_at_unix_secs,
        surpassed_threshold: snapshot.surpassed_threshold,
        representative_claim: snapshot.representative_claim.clone(),
        disabled_reason: snapshot.disabled_reason.clone(),
        observed_at_unix_millis: snapshot.observed_at_unix_millis,
        age_secs: snapshot
            .observed_at_unix_millis
            .map(|observed_at| observed_at_unix_secs.saturating_sub(observed_at / 1_000)),
    }
}

fn subscription_quota_data_state_to_wire(state: SubscriptionQuotaDataState) -> &'static str {
    match state {
        SubscriptionQuotaDataState::Fresh => "fresh",
        SubscriptionQuotaDataState::Stale => "stale",
        SubscriptionQuotaDataState::Missing => "missing",
    }
}

pub(crate) fn request_to_wire(ctx: &RequestContext) -> RequestWire {
    RequestWire {
        request_id: ctx.request_id.clone(),
        headers: request_headers_to_wire(ctx),
        method: ctx.method.as_str().to_owned(),
        path: ctx.path.clone(),
        query: ctx.query.clone(),
        body_base64: BASE64.encode(&ctx.body_bytes),
    }
}

fn request_headers_to_wire(ctx: &RequestContext) -> Vec<HeaderWire> {
    let mut headers = ctx.downstream_headers.clone();
    headers.remove(http::header::HOST);
    headers.remove(http::header::AUTHORIZATION);
    headers.remove("x-api-key");
    headers_to_wire(&headers)
}

pub(crate) fn principal_to_wire(principal: &Principal) -> PrincipalWire {
    PrincipalWire {
        id: principal.id.clone(),
        kind: principal_kind_to_wire(principal),
        claims: principal.claims.clone(),
    }
}

fn principal_kind_to_wire(principal: &Principal) -> String {
    serde_json::to_value(&principal.kind)
        .ok()
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| "unknown".to_owned())
}

pub(crate) fn upstream_to_wire(upstream: &Upstream) -> UpstreamWire {
    match upstream {
        Upstream::AnthropicDirect => UpstreamWire::AnthropicDirect,
        Upstream::CustomAnthropicSpec { base_url } => UpstreamWire::CustomAnthropicSpec {
            base_url: base_url.to_string(),
        },
    }
}

fn upstream_from_wire(upstream: UpstreamWire) -> Result<Upstream, WireError> {
    match upstream {
        UpstreamWire::AnthropicDirect => Ok(Upstream::AnthropicDirect),
        UpstreamWire::CustomAnthropicSpec { base_url } => {
            let base_url = Url::parse(&base_url).map_err(|source| WireError::InvalidUrl {
                url: base_url,
                source,
            })?;
            Ok(Upstream::CustomAnthropicSpec { base_url })
        }
    }
}

fn shaped_request_to_wire(shaped: &ShapedRequest) -> ShapedRequestWire {
    ShapedRequestWire {
        url: shaped.url().to_string(),
        method: shaped.method().as_str().to_owned(),
        headers: headers_to_wire(shaped.headers()),
        body_base64: BASE64.encode(shaped.body()),
    }
}

fn upstream_error_to_wire(value: &UpstreamError) -> UpstreamErrorWire {
    match value {
        UpstreamError::Unauthorized { status, body } => UpstreamErrorWire {
            status: status.as_u16(),
            body_base64: body.as_ref().map(|body| BASE64.encode(body)),
            category: UpstreamErrorCategory::Unauthorized,
        },
        UpstreamError::Retryable { status, body } => UpstreamErrorWire {
            status: status.as_u16(),
            body_base64: body.as_ref().map(|body| BASE64.encode(body)),
            category: UpstreamErrorCategory::Retryable,
        },
        UpstreamError::Failed { status, body } => UpstreamErrorWire {
            status: status.as_u16(),
            body_base64: body.as_ref().map(|body| BASE64.encode(body)),
            category: UpstreamErrorCategory::Failed,
        },
    }
}

pub(crate) fn headers_to_wire(headers: &HeaderMap) -> Vec<HeaderWire> {
    headers
        .iter()
        .map(|(name, value)| HeaderWire {
            name: name.as_str().to_owned(),
            value_base64: BASE64.encode(value.as_bytes()),
        })
        .collect()
}

fn headers_from_wire(headers: Vec<HeaderWire>) -> Result<HeaderMap, WireError> {
    let mut out = HeaderMap::new();
    for header in headers {
        let name = HeaderName::from_bytes(header.name.as_bytes()).map_err(|source| {
            WireError::InvalidHeaderName {
                name: header.name.clone(),
                source,
            }
        })?;
        let value = BASE64.decode(header.value_base64).map_err(|source| {
            WireError::InvalidHeaderValueBase64 {
                name: header.name.clone(),
                source,
            }
        })?;
        let value =
            HeaderValue::from_bytes(&value).map_err(|source| WireError::InvalidHeaderValue {
                name: header.name.clone(),
                source,
            })?;
        out.append(name, value);
    }
    Ok(out)
}

#[derive(Debug)]
pub(crate) enum WireError {
    InvalidUrl {
        url: String,
        source: url::ParseError,
    },
    InvalidHeaderName {
        name: String,
        source: http::header::InvalidHeaderName,
    },
    InvalidHeaderValueBase64 {
        name: String,
        source: base64::DecodeError,
    },
    InvalidHeaderValue {
        name: String,
        source: http::header::InvalidHeaderValue,
    },
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUrl { url, source } => {
                write!(f, "invalid upstream url {url}: {source}")
            }
            Self::InvalidHeaderName { name, source } => {
                write!(f, "invalid header name {name}: {source}")
            }
            Self::InvalidHeaderValueBase64 { name, source } => {
                write!(f, "invalid header value base64 for {name}: {source}")
            }
            Self::InvalidHeaderValue { name, source } => {
                write!(f, "invalid header value for {name}: {source}")
            }
        }
    }
}

impl std::error::Error for WireError {}

fn route_runtime_message(source: WireError) -> RouteError {
    RouteError::Runtime {
        reason: source.to_string(),
    }
}

fn default_route_decision(slot: Arc<PluginSlot>) -> RouteDecision {
    RouteDecision {
        upstream_id: None,
        upstream: Upstream::AnthropicDirect,
        dialect: Arc::new(ExtismDialectPlugin::new(slot)),
    }
}

fn route_unexpected_fallback(policy: FallbackPolicy) -> RouteError {
    RouteError::Runtime {
        reason: format!("unexpected plugin route fallback policy: {policy:?}"),
    }
}

fn dialect_unexpected_fallback(policy: FallbackPolicy) -> DialectError {
    DialectError::UnsupportedRequest {
        reason: format!("unexpected plugin dialect fallback policy: {policy:?}"),
    }
}

fn signer_unexpected_fallback(policy: FallbackPolicy) -> SignerError {
    SignerError::SigningFailed {
        reason: format!("unexpected plugin signer fallback policy: {policy:?}"),
    }
}
