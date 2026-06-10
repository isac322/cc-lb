use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use cc_lb_plugin_api::{
    DialectError, FilterError, FilterOutput, FilterPlugin, PerCandidateReason, Principal,
    RequestContext, RetryDecision, ShapedRequest, ShapedRequestBuilder, SignedRequest, Signer,
    SignerError, SignerFactory, SigningCapability, SubscriptionQuotaCandidateSnapshot,
    SubscriptionQuotaDataState, Upstream, UpstreamCandidate, UpstreamDialect, UpstreamError,
};
use cc_lb_plugin_wire::augmented_metadata::AugmentedMetadata;
use cc_lb_plugin_wire::identity::{CC_LB_PLUGIN_MAGIC, PluginIdentity};
use cc_lb_plugin_wire::v1::build_signer::{BuildSignerFn, BuildSignerRequest};
use cc_lb_plugin_wire::v1::common::{
    HeaderWire, Principal as PrincipalWire, RequestWire, ShapedRequestWire, UpstreamErrorCategory,
    UpstreamErrorWire, UpstreamWire,
};
use cc_lb_plugin_wire::v1::normalize_error::{NormalizeErrorFn, NormalizeErrorRequest};
use cc_lb_plugin_wire::v1::observe::ObserveFn;
use cc_lb_plugin_wire::v1::on_unauthorized::{OnUnauthorizedFn, OnUnauthorizedRequest};
use cc_lb_plugin_wire::v1::shape::{ShapeFn, ShapeRequest};
use cc_lb_plugin_wire::v1::sign::{SignFn, SignRequest};
use cc_lb_plugin_wire::v2::common::{
    self as v2_common, CandidateWire as UpstreamCandidateWire, Principal as FilterPrincipalWire,
};
use cc_lb_plugin_wire::v2::shape::{ShapeFn as ShapeFnV2, ShapeRequest as ShapeRequestV2};
use cc_lb_plugin_wire::v3::filter::{
    FilterFn, FilterRequest, FilterResponse, PerCandidateReasonWire,
};
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
pub struct ExtismFilterPlugin {
    slot: Arc<PluginSlot>,
    plugin_id: Uuid,
}

impl ExtismFilterPlugin {
    pub(crate) fn new(slot: Arc<PluginSlot>, plugin_id: Uuid) -> Self {
        Self { slot, plugin_id }
    }
}

impl FilterPlugin for ExtismFilterPlugin {
    fn filter(
        &self,
        ctx: &RequestContext,
        principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        let request = filter_request_to_wire(ctx, principal, candidates);
        let response = self.slot.dispatch_filter_call_sync(request)?;
        filter_response_to_output(response)
    }

    fn plugin_id(&self) -> Uuid {
        self.plugin_id
    }

    fn plugin_name(&self) -> &str {
        &self.slot.name
    }
}

#[derive(Clone)]
pub(crate) struct ExtismDialectPlugin {
    slot: Arc<PluginSlot>,
    base_url: Option<String>,
}

impl ExtismDialectPlugin {
    pub(crate) fn new(slot: Arc<PluginSlot>) -> Self {
        Self {
            slot,
            base_url: None,
        }
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
        let use_v2 = matches!(self.slot.negotiated_wire_version(), Ok(2));
        let response = if use_v2 {
            let request = ShapeRequestV2 {
                request: request_to_wire_v2(ctx),
                upstream: v2_upstream_to_wire(upstream),
                principal: principal_to_wire_v2(principal),
                upstream_base_url: self.base_url.clone(),
            };
            match self.slot.dispatch_wire_call_sync::<ShapeFnV2>(request) {
                DispatchOutcome::Ok(response) => v2_shape_response_to_v1(response),
                DispatchOutcome::Fallback(FallbackPolicy::FailRequest) => {
                    return Err(DialectError::UnsupportedRequest {
                        reason: "plugin shape failed".to_owned(),
                    });
                }
                DispatchOutcome::Fallback(policy) => {
                    return Err(dialect_unexpected_fallback(policy));
                }
            }
        } else {
            let request = ShapeRequest {
                request: request_to_wire(ctx),
                upstream: upstream_to_wire(upstream),
                principal: principal_to_wire(principal),
            };
            match self.slot.dispatch_wire_call_sync::<ShapeFn>(request) {
                DispatchOutcome::Ok(response) => response,
                DispatchOutcome::Fallback(FallbackPolicy::FailRequest) => {
                    return Err(DialectError::UnsupportedRequest {
                        reason: "plugin shape failed".to_owned(),
                    });
                }
                DispatchOutcome::Fallback(policy) => {
                    return Err(dialect_unexpected_fallback(policy));
                }
            }
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

    pub(crate) fn dispatch_filter_call_sync(
        self: &Arc<Self>,
        request: FilterRequest,
    ) -> Result<FilterResponse, FilterError> {
        let slot = self.clone();
        let thread = std::thread::Builder::new()
            .name(format!("cc-lb-extism-{}", FilterFn::NAME))
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build();
                match runtime {
                    Ok(runtime) => runtime.block_on(slot.dispatch_filter_call_async(request)),
                    Err(source) => Err(filter_runtime_error("runtime_build", source.to_string())),
                }
            })
            .map_err(|source| filter_runtime_error("thread_spawn", source.to_string()))?;
        match thread.join() {
            Ok(outcome) => outcome,
            Err(_) => Err(filter_trap_error("thread_join", "panic")),
        }
    }

    async fn dispatch_filter_call_async(
        &self,
        request: FilterRequest,
    ) -> Result<FilterResponse, FilterError> {
        let cell = self.current.load_full();
        let metadata = self.dispatch_metadata();
        let limits = self
            .limits_for_dispatch()
            .ok_or_else(|| filter_runtime_error("slot_metadata", "unavailable"))?;
        let started = Instant::now();
        let outcome =
            dispatch_filter_with_timeout(cell, metadata, request, limits.max_call_duration).await;
        metrics::histogram!(
            "cc_lb_extism_call_duration_seconds",
            "plugin" => self.name.clone(),
            "hook" => FilterFn::NAME.to_owned(),
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

async fn dispatch_filter_with_timeout(
    cell: Arc<PluginCell>,
    metadata: AugmentedMetadata,
    request: FilterRequest,
    timeout_duration: Duration,
) -> Result<FilterResponse, FilterError> {
    let (cancel_tx, cancel_rx) = oneshot::channel();
    let task = tokio::task::spawn_blocking(move || {
        let mut plugin = cell
            .plugin
            .lock()
            .map_err(|_| filter_runtime_error("plugin_lock", "poisoned"))?;
        let _ = cancel_tx.send(plugin.cancel_handle());
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            dispatch_filter_call_inner(&mut plugin, &metadata, request)
        })) {
            Ok(outcome) => outcome,
            Err(_) => Err(filter_trap_error("panic", "dispatch panicked")),
        }
    });

    let cancel_handle = tokio::time::timeout(Duration::from_millis(25), cancel_rx)
        .await
        .ok()
        .and_then(Result::ok);

    match tokio::time::timeout(timeout_duration, task).await {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(source)) => Err(filter_trap_error("task_join", source.to_string())),
        Err(_) => {
            if let Some(cancel_handle) = cancel_handle {
                let _ = cancel_handle.cancel();
            }
            Err(filter_trap_error("timeout", "plugin hook timed out"))
        }
    }
}

fn dispatch_filter_call_inner(
    plugin: &mut extism::Plugin,
    metadata: &AugmentedMetadata,
    request: FilterRequest,
) -> Result<FilterResponse, FilterError> {
    let negotiated_version = *metadata
        .negotiated_functions
        .get(FilterFn::NAME)
        .ok_or_else(|| filter_runtime_error("version_lookup", "negotiated version missing"))?;

    let request_value = serde_json::to_value(request)
        .map_err(|source| filter_runtime_error("request_envelope", source.to_string()))?;
    let Value::Object(mut request_map) = request_value else {
        return Err(filter_runtime_error(
            "request_envelope",
            "request serialized to non-object JSON",
        ));
    };
    request_map.insert("_v".to_owned(), Value::from(negotiated_version));

    let input = serde_json::to_string(&Value::Object(request_map))
        .map_err(|source| filter_runtime_error("serialize_request", source.to_string()))?;
    let output = plugin
        .call::<String, String>(FilterFn::NAME, input)
        .map_err(|source| filter_plugin_call_error(source.to_string()))?;

    let response_value: Value = serde_json::from_str(&output)
        .map_err(|source| filter_runtime_error("deserialize_response", source.to_string()))?;
    let Value::Object(mut response_map) = response_value else {
        return Err(filter_runtime_error(
            "response_envelope",
            "response envelope was not a JSON object",
        ));
    };
    let response_version = response_map
        .remove("_v")
        .ok_or_else(|| filter_runtime_error("response_envelope", "response envelope missing _v"))?;
    let actual_version = response_version.as_u64().ok_or_else(|| {
        filter_runtime_error(
            "response_envelope",
            "response envelope _v was not an unsigned integer",
        )
    })?;
    if actual_version != u64::from(negotiated_version) {
        return Err(filter_runtime_error(
            "response_envelope",
            format!(
                "response envelope version mismatch: expected {negotiated_version}, actual {actual_version}"
            ),
        ));
    }

    serde_json::from_value(Value::Object(response_map))
        .map_err(|source| filter_runtime_error("response_decode", source.to_string()))
}

fn filter_plugin_call_error(reason: String) -> FilterError {
    if is_wasm_trap(&reason) {
        filter_trap_error("plugin_call", reason)
    } else {
        filter_runtime_error("plugin_call", reason)
    }
}

fn is_wasm_trap(reason: &str) -> bool {
    let reason = reason.to_ascii_lowercase();
    reason.contains("trap")
        || reason.contains("unreachable")
        || reason.contains("wasm backtrace")
        || reason.contains("fuel")
        || reason.contains("timeout")
        || reason.contains("timed out")
}

fn filter_runtime_error(stage: &'static str, reason: impl fmt::Display) -> FilterError {
    filter_error_metric(stage);
    let reason = reason.to_string();
    tracing::warn!(
        target: "cc_lb_plugin.dispatch",
        function = FilterFn::NAME,
        stage,
        reason = %reason,
        "filter dispatch runtime error"
    );
    FilterError::Runtime {
        reason: format!("{stage}: {reason}"),
    }
}

fn filter_trap_error(stage: &'static str, reason: impl fmt::Display) -> FilterError {
    filter_error_metric(stage);
    let reason = reason.to_string();
    tracing::warn!(
        target: "cc_lb_plugin.dispatch",
        function = FilterFn::NAME,
        stage,
        reason = %reason,
        "filter dispatch trap error"
    );
    FilterError::Trap {
        reason: format!("{stage}: {reason}"),
    }
}

fn filter_error_metric(stage: &'static str) {
    metrics::counter!(
        "cc_lb_plugin_dispatch_errors_total",
        "function" => FilterFn::NAME,
        "stage" => stage,
    )
    .increment(1);
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
            function_version::<ShapeFn>(),
            function_version::<NormalizeErrorFn>(),
            function_version::<BuildSignerFn>(),
            function_version::<SignFn>(),
            function_version::<OnUnauthorizedFn>(),
            function_version::<ObserveFn>(),
            function_version::<FilterFn>(),
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

fn candidates_to_wire_v2(candidates: &[UpstreamCandidate]) -> Vec<v2_common::CandidateWire> {
    candidates
        .iter()
        .map(|candidate| v2_common::CandidateWire {
            upstream_id: candidate.upstream_id.to_string(),
            name: candidate.name.clone(),
            kind: candidate.kind.as_str().to_owned(),
            observed_rate_limits: candidate
                .observed_rate_limits
                .iter()
                .map(|observation| v2_common::RateLimitObservationWire {
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
                    subscription_quota_to_wire_v2(snapshot, candidate.observed_at_unix_secs)
                })
                .collect(),
            observed_at_unix_secs: candidate.observed_at_unix_secs,
            cache_score: candidate
                .cache_score
                .as_ref()
                .map(|score| v2_common::CacheScoreWire {
                    predicted_cache_read_tokens: score.predicted_cache_read_tokens,
                    predicted_cache_creation_tokens_5m: score.predicted_cache_creation_tokens_5m,
                    predicted_cache_creation_tokens_1h: score.predicted_cache_creation_tokens_1h,
                    predicted_uncached_input_tokens: score.predicted_uncached_input_tokens,
                    predicted_expires_at_unix_secs: score.predicted_expires_at_unix_secs,
                    matched_breakpoint_index: score.matched_breakpoint_index,
                    confidence: score.confidence,
                    ambiguity_reason: score.ambiguity_reason.clone(),
                }),
        })
        .collect()
}

fn candidates_to_wire_v3(candidates: &[UpstreamCandidate]) -> Vec<UpstreamCandidateWire> {
    candidates_to_wire_v2(candidates)
}

fn subscription_quota_to_wire_v2(
    snapshot: &SubscriptionQuotaCandidateSnapshot,
    observed_at_unix_secs: u64,
) -> v2_common::SubscriptionQuotaCandidateSnapshotWire {
    v2_common::SubscriptionQuotaCandidateSnapshotWire {
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

fn filter_request_to_wire(
    ctx: &RequestContext,
    principal: &Principal,
    candidates: &[UpstreamCandidate],
) -> FilterRequest {
    FilterRequest {
        request_id: ctx.request_id.clone(),
        headers: request_headers_to_wire_v2(ctx),
        method: ctx.method.as_str().to_owned(),
        path: ctx.path.clone(),
        query: ctx.query.clone(),
        body_base64: BASE64.encode(&ctx.body_bytes),
        principal: principal_to_wire_v3(principal),
        candidates: candidates_to_wire_v3(candidates),
    }
}

fn request_headers_to_wire(ctx: &RequestContext) -> Vec<HeaderWire> {
    let mut headers = ctx.downstream_headers.clone();
    headers.remove(http::header::HOST);
    headers.remove(http::header::AUTHORIZATION);
    headers.remove("x-api-key");
    headers_to_wire(&headers)
}

fn request_headers_to_wire_v2(ctx: &RequestContext) -> Vec<v2_common::HeaderWire> {
    let mut headers = ctx.downstream_headers.clone();
    headers.remove(http::header::HOST);
    headers.remove(http::header::AUTHORIZATION);
    headers.remove("x-api-key");
    headers_to_wire_v2(&headers)
}

pub(crate) fn principal_to_wire(principal: &Principal) -> PrincipalWire {
    PrincipalWire {
        id: principal.id.clone(),
        kind: principal_kind_to_wire(principal),
        claims: principal.claims.clone(),
    }
}

fn principal_to_wire_v2(principal: &Principal) -> v2_common::Principal {
    v2_common::Principal {
        id: principal.id.clone(),
        kind: principal_kind_to_wire(principal),
        claims: principal.claims.clone(),
    }
}

fn principal_to_wire_v3(principal: &Principal) -> FilterPrincipalWire {
    principal_to_wire_v2(principal)
}

fn filter_response_to_output(response: FilterResponse) -> Result<FilterOutput, FilterError> {
    let mut kept_upstream_ids = Vec::new();
    let mut per_candidate_reasons = Vec::new();
    let mut reasons = Vec::new();

    for result in response.results {
        let upstream_id =
            Uuid::parse_str(&result.upstream_id).map_err(|source| FilterError::Runtime {
                reason: format!("plugin returned invalid upstream_id: {source}"),
            })?;
        if result.decision == "accept" {
            kept_upstream_ids.push(upstream_id);
        } else {
            per_candidate_reasons.push(per_candidate_reason_from_wire(&result));
        }
        if !result.reason.is_empty() {
            reasons.push(format!("{}: {}", result.upstream_id, result.reason));
        }
    }

    Ok(FilterOutput {
        kept_upstream_ids,
        reason: reasons.join("; "),
        per_candidate_reasons,
    })
}

fn per_candidate_reason_from_wire(result: &PerCandidateReasonWire) -> PerCandidateReason {
    let label = if result.decision == "accept" {
        result.reason.as_str()
    } else {
        result.decision.as_str()
    };
    let label = label.replace('-', "_").to_ascii_lowercase();
    if label.contains("rate_limit") || label.contains("rate_limited") {
        PerCandidateReason::RateLimited
    } else if label.contains("quota") {
        PerCandidateReason::InsufficientQuota
    } else if label.contains("unhealthy") {
        PerCandidateReason::Unhealthy
    } else {
        PerCandidateReason::RejectedByPlugin
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

fn headers_to_wire_v2(headers: &HeaderMap) -> Vec<v2_common::HeaderWire> {
    headers
        .iter()
        .map(|(name, value)| v2_common::HeaderWire {
            name: name.as_str().to_owned(),
            value_base64: BASE64.encode(value.as_bytes()),
        })
        .collect()
}

fn request_to_wire_v2(ctx: &RequestContext) -> v2_common::RequestWire {
    v2_common::RequestWire {
        request_id: ctx.request_id.clone(),
        headers: request_headers_to_wire_v2(ctx),
        method: ctx.method.as_str().to_owned(),
        path: ctx.path.clone(),
        query: ctx.query.clone(),
        body_base64: BASE64.encode(&ctx.body_bytes),
    }
}

fn v2_upstream_to_wire(upstream: &Upstream) -> v2_common::UpstreamWire {
    match upstream {
        Upstream::AnthropicDirect => v2_common::UpstreamWire::AnthropicDirect,
    }
}

fn v2_shape_response_to_v1(
    response: cc_lb_plugin_wire::v2::shape::ShapeResponse,
) -> cc_lb_plugin_wire::v1::shape::ShapeResponse {
    cc_lb_plugin_wire::v1::shape::ShapeResponse {
        url: response.url,
        method: response.method,
        headers: response
            .headers
            .into_iter()
            .map(|h| HeaderWire {
                name: h.name,
                value_base64: h.value_base64,
            })
            .collect(),
        body_base64: response.body_base64,
    }
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

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use cc_lb_plugin_api::{PrincipalKind, RateLimitKind, RateLimitObservation, UpstreamKind};
    use http::{HeaderMap, Method};
    use serde_json::json;
    use uuid::Uuid;

    use super::*;

    fn test_candidate_without_cache_score() -> UpstreamCandidate {
        UpstreamCandidate {
            upstream_id: Uuid::parse_str("11111111-1111-1111-1111-111111111111")
                .expect("fixture UUID parses"),
            name: "anthropic-direct".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            observed_rate_limits: vec![RateLimitObservation {
                kind: RateLimitKind::Requests,
                window: "minute".to_owned(),
                limit: Some(100),
                remaining: Some(42),
                reset: Some("60".to_owned()),
            }],
            subscription_quotas: Vec::new(),
            observed_at_unix_secs: 1_800_000_000,
            cache_score: None,
            base_url: None,
        }
    }

    fn test_candidate_with_cache_score() -> UpstreamCandidate {
        UpstreamCandidate {
            cache_score: serde_json::from_value(json!({
                "predicted_cache_read_tokens": 1024,
                "predicted_cache_creation_tokens_5m": 256,
                "predicted_cache_creation_tokens_1h": 128,
                "predicted_uncached_input_tokens": 64,
                "predicted_expires_at_unix_secs": 1900000000u64,
                "matched_breakpoint_index": 0,
                "confidence": 0.875,
                "ambiguity_reason": "fixture confidence"
            }))
            .expect("cache score fixture deserializes"),
            ..test_candidate_without_cache_score()
        }
    }

    #[test]
    fn candidates_to_wire_versioned_v2_wire_includes_cache_score() {
        let candidates = vec![test_candidate_with_cache_score()];

        let wire = candidates_to_wire_v2(&candidates);
        let wire_score = wire[0]
            .cache_score
            .as_ref()
            .expect("v2 candidate carries cache score");

        assert_eq!(wire_score.predicted_cache_read_tokens, 1_024);
        assert_eq!(wire_score.predicted_cache_creation_tokens_5m, 256);
        assert_eq!(wire_score.predicted_cache_creation_tokens_1h, 128);
        assert_eq!(wire_score.predicted_uncached_input_tokens, 64);
        assert_eq!(
            wire_score.predicted_expires_at_unix_secs,
            Some(1_900_000_000)
        );
        assert_eq!(wire_score.matched_breakpoint_index, Some(0));
        assert_eq!(wire_score.confidence, 0.875);
        assert_eq!(
            wire_score.ambiguity_reason.as_deref(),
            Some("fixture confidence")
        );

        let json = serde_json::to_value(&wire[0]).expect("v2 candidate serializes");
        assert!(json.get("cache_score").is_some());
    }
}
