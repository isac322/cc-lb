mod compat_realtime;
mod compat_request;
mod compat_response;
mod gemini_native;
pub(crate) use compat_request::CompatResponsePlan;
#[path = "realtime_websocket_lifecycle.rs"]
mod realtime_websocket_lifecycle;
pub use realtime_websocket_lifecycle::live_eligible;
mod cache;
mod identity;
#[cfg(test)]
pub(crate) use self::identity::MAX_IDENTITY_VALUE_BYTES;
mod limits;
mod routing;
mod streaming;
#[cfg(test)]
mod tests;
mod types;

pub(crate) use self::cache::{
    PromptCacheBundle, PromptCacheBundles, PromptCacheUsage, RequestCacheMetadata,
    SelectedCacheMatch, build_candidates_with_matches_for_bundles,
    build_candidates_with_native_cache_for_bundles, decode_prompt_cache_observations_pure,
    prompt_cache_observation_context, prompt_cache_policy_for_upstream,
    publish_prompt_cache_observations, publish_prompt_cache_observations_with_usage,
};
pub use self::cache::{PromptCacheObservationContext, build_candidates};
use self::cache::{
    cache_pricing_summary_for_model, compat_service_tier_for_upstream,
    fetch_prompt_cache_warm_entries_for_bundles, fetch_provider_native_cache_scores,
    prompt_cache_token_scope, publish_provider_cache_observation,
    request_cache_metadata_from_value_with_analysis_for_capability,
};
use self::identity::{
    OpenaiSessionHintCache, ThreadStickySubagentLatch, apply_thread_sticky_subagent_latch,
    is_openai_prewarm_request, request_identity_metadata,
};
use self::limits::{
    ActiveLimit, LimitRequest, admission_fanout_multiplier, attach_limit_headers_from_engine,
    attach_limit_headers_from_record, limit_rejection_response, limit_retry_after_secs,
    limit_violation_name, use_image_rates_for_admission,
};
pub(crate) use self::limits::{INPUT_ESTIMATE_MAX_BYTES, INPUT_ESTIMATE_MAX_NODES};
use self::routing::{
    FailoverReason, FailoverRouter, FilterPipelineResult, RoutedUpstream, announce_attempt,
    emit_route_completed, execute_filter_pipeline, filter_codex_models_catalog, route_for_record,
    route_info_for_routed, terminal_candidates, upstream_for_record,
};
pub use self::routing::{native_route_dialect, observe_rate_limits};
use self::streaming::{
    ANTHROPIC_PROVIDER_STATE_MARKER_PROBE_BYTES, ErrorBodyClass, SINGLE_OWNER_AMBIGUOUS_MESSAGE,
    SseDownstreamBoundaryTracker, UPSTREAM_AFFINITY_KEY_LIMIT_MESSAGE,
    UPSTREAM_AFFINITY_NOT_ESTABLISHED_MESSAGE, UPSTREAM_AFFINITY_TARGET_INELIGIBLE_MESSAGE,
    UPSTREAM_AFFINITY_UNAVAILABLE_MESSAGE, UPSTREAM_BUFFERED_BODY_FAILED_MESSAGE,
    UPSTREAM_RESPONSE_BODY_ERROR_TYPE, UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
    UPSTREAM_RESPONSE_DECODE_ERROR_TYPE, UPSTREAM_RESPONSE_FRAMING_ERROR_TYPE,
    UPSTREAM_RESPONSE_INCOMPLETE_ERROR_TYPE, UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE,
    UpstreamBodyDiagnostics, affinity_stream_error, append_bounded_upstream_error_body,
    classify_error_body, collect_error_response, decompression_output_budget_bytes,
    drive_with_activity_heartbeat, has_json_content_type, held_error_body_is_json,
    incomplete_sse_event_budget_bytes, is_semantic_content_frame, is_sse_response,
    is_upstream_response_decode_error, keepalive_http_end_is_terminal, make_stream_error_frame,
    maybe_emit_chunk_heartbeat, new_activity_heartbeat_interval, observe_sse_lifecycle_markers,
    observe_sse_terminal_event, response_from_collected, scrubbed_upstream_error_body,
    semantic_body_bytes, semantic_terminal_event_type, split_sse_event, sse_buffer_is_blank,
    sse_bytes_may_carry_anthropic_provider_state, timed_body_frame_with_heartbeat,
    unsolicited_anthropic_sse_affinity_keys, upstream_end_after_message_stop_is_acceptable,
};
pub(crate) use self::streaming::{
    abnormal_stop_error_message, keepalive_event_reaches_upstream_terminal,
    sse_event_completes_response,
};
use self::types::{
    AttemptFailure, AttemptResponse, CompatBodyTimingGuard, ConcurrentAttemptFuture,
    ConcurrentAttemptInputs, LimitRejectionErr, LimitRejectionInfo, RawPassthroughDialect,
    RawPassthroughError, RequestAffinityFailure, RequestAffinityResolution, RequestBodyView,
    ResponseAffinityContext, ResponseBodyFuture, apply_prompt_cache_timings,
    bind_upstream_affinity_keys, body_cap_for_path, canonical_model_for_capability,
    db_principal_kind_as_str, duration_to_ms, gemini_model_from_path, header_snapshot_from,
    header_to_string, lifecycle_request_model, multipart_request_model, next_request_id,
    principal_kind_lite, push_buffered_data, raw_passthrough_request, record_shape_internal_error,
    reduce_fanout, response_start_unix_secs, responses_usage_value_to_counts,
    rewrite_json_request_body_headers, signer_error_kind, store_setup_timings,
    system_time_to_unix_millis, to_usage_snapshot, unix_now_ms, unsupported_operation_response,
};
pub(crate) use self::types::{AttemptTimings, DEFAULT_MAX_INPUT_ESTIMATE, RequestEventContext};
pub use self::types::{
    AuthLimitSubject, Body, DispatchError, HASH_SCHEMA_VERSION, LifecycleConfig,
    LimitCostEstimator, LimitSubjectProvider, ProxyError, RequestKind, UpstreamDispatch,
    is_multipart_operation,
};

use std::collections::{HashMap, HashSet, hash_map::DefaultHasher};
use std::convert::Infallible;
use std::fmt::Write as _;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use axum::body::Body as AxumBody;
use bytes::{Bytes, BytesMut};
use cc_lb_config::UpstreamAffinityConfig;
#[cfg(test)]
use cc_lb_control::dynamic_view::DynamicViewBuilder;
use cc_lb_domain::{
    BUILTIN_SUBSCRIPTION_PREFERENCE_ID, CacheBreakpoint, CacheBreakpointSource,
    CacheLookbackPrefix, CachePricingSummary, CacheScore, CredentialKind, InternalError,
    InternalErrorKind, InternalErrorStage, NativeCachePricing, NativeCachePricingComponent,
    NativeCachePricingLifetime, NativeCachePricingProvider, NativeCachePricingRate,
    NativeCachePricingStatus, PrefixBytes, Principal, PrincipalKind, Provider, TerminalStrategy,
    TtlClass, Upstream, UpstreamCandidate, UpstreamKind as CandidateUpstreamKind, WarmCacheEntry,
    vertex_origin,
};
use cc_lb_quota::build_subscription_quota_samples;
use cc_lb_quota::rate_limit_headers::{
    parse_anthropic_rate_limit_headers, parse_openai_rate_limit_headers,
    parse_standard_rate_limit_headers,
};
use cc_lb_request_log::{
    HeaderSnapshot, RequestCacheBreakpoint, RequestCacheBreakpointSource,
    RequestCacheLookbackPrefix,
};
use cc_lb_routing::{FilterError, FilterOutput, FilterPlugin, RouteDecision};
use cc_lb_routing_trace::{RoutingTrace, StageDecision, TerminalDecision};
use cc_lb_storage_api::{
    PrincipalKind as DbPrincipalKind, PrincipalKindLite, PromptCacheObservationRecord,
    ProviderPromptCacheObservationRecord, StoredApiKeyRecord, UpstreamAffinityBinding,
    UpstreamAffinityKey, UpstreamAffinityKind, UpstreamAffinityStore,
    UpstreamRateLimitObservationRecord, UpstreamRecord,
    upstream::UpstreamKind as StorageUpstreamKind,
};
#[cfg(test)]
use cc_lb_upstream::{ApiKeyAwareSignerFactory, SignerFactory};
use cc_lb_upstream::{
    DialectError, DialectShapeContext, ResponseTransformError, RetryDecision, ShapedRequest,
    ShapedRequestBuilder, SignedRequest, Signer, SignerError, SseEventTransformHook,
    TransformResponseRequest, UpstreamDialect, UpstreamError, shape_request, sign_request,
};
use http::header::{CONTENT_LENGTH, CONTENT_TYPE, LOCATION, RETRY_AFTER};
use http::{HeaderMap, HeaderName, HeaderValue, Method, Request, Response, StatusCode};
use http_body::Body as _;
use http_body_util::BodyExt;
use rand::{RngExt, SeedableRng, rngs::StdRng};
#[cfg(test)]
use serde_json::json;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tracing::Instrument as _;
use url::Url;
use uuid::Uuid;

use crate::attempt_rail::{AttemptIntent, Reserved, ResponseAccountingGuard, Scoped};
use crate::body_io_timing::{
    BodyIoPhase, BodyIoTiming, BodyIoTimingObserverGuard, TimedResponseStream, timed_body_frame,
};
use crate::builtin_filters::subscription_preference::subscription_hard_blocked;
use crate::completion_observer::{CompletionObserver, StreamCompletionLog, StreamLatency};
use crate::downstream_stream_drop_guard::{
    DownstreamStreamDropGuard, StreamTerminationAttribution,
};
#[cfg(test)]
use crate::error_format::anthropic_error_body;
use crate::error_format::{
    anthropic_error_response, provider_error_body, provider_error_body_with_retry_after,
    provider_error_response, provider_error_response_with_retry_after,
};
use crate::hop_by_hop::strip_hop_by_hop;
use crate::lifecycle::compat_request::{
    CompatResponseKind, effective_policy, prepare_request, retain_api_key_candidates,
};
use crate::limit_input_estimate::estimate_anthropic_input;
use crate::model_resolution::{cache_threshold_tokens, canonical_model_id};
#[cfg(test)]
use crate::prompt_cache_simulator::cacheable_breakpoint_prefix_keys;
use crate::prompt_cache_simulator::{
    PromptCacheAnalysisExecutor, PromptCacheAnalysisOutput, PromptCacheAnalysisTimings,
    PromptCachePolicy, V3_TOKEN_ESTIMATE_SOURCE, V3PromptCacheAnalysis, V3PromptCacheBlockSource,
    analyze_v3_prompt_cache,
};
use crate::provider_cache;
use crate::provider_capability::{
    ProviderCapability, body_is_consistency_safe, canonical_gemini_model_id,
    classify as classify_provider_capability,
};
use crate::provider_input_estimate::estimate_provider_input;
use crate::request_classification::{
    classify_client_request_kind_with_signals, codex::CodexRequestSignals,
    is_thread_sticky_subagent_seed,
};
use crate::request_context::{CodexBackendSurface, ProviderScope, RequestContext};
use crate::request_timing::{REQUEST_STAGE_TIMINGS, RequestStageTimings};
use crate::response_transform::{
    ComposedSseResponseTransformHook, GEMINI_OAUTH_RESPONSE_TRANSFORM, ResponseTransformContext,
    SseTransformOutcome, apply_buffered_transform_result, make_response_transform_error_frame,
    sanitize_downstream_response_headers, sanitize_downstream_stream_headers,
    sanitized_response_headers_for_plugin, transform_sse_event_bytes, translate_vertex_response,
    unwrap_gemini_code_assist_body,
};
use crate::responses_websocket::{
    PreparedWebSocketTurn, WebSocketCandidate, WebSocketError, WebSocketIngress,
    WebSocketPluginRequest, WsUnknownReasoningPlan,
};
use crate::sse_error_frame::{
    ResponsesSseErrorContext, make_provider_error_frame, make_responses_failed_frame,
};
use crate::subscription_quota_events::SubscriptionQuotaSink;
use crate::terminal_observer::{
    InternalFailure, LifecycleContext, StreamErrorClassification, StreamTerminationCause,
    TerminalClassification, UpstreamErrorCode, classify_stream_error, error_codes,
    numeric_attribute,
};
use crate::upstream_affinity::{
    OpenaiResponseReferenceScope, UpstreamAffinityExtractionError,
    collect_openai_pending_reasoning_digests, extract_request_affinity_keys,
    extract_request_affinity_keys_with_scope, extract_response_affinity_keys,
    extract_response_affinity_keys_from_sse_event, extract_websocket_response_affinity_keys,
    gemini_cached_content_expiry, is_openai_invalid_encrypted_content_http_error,
    is_openai_unknown_reasoning_only, path_affinity_key, realtime_call_affinity_key,
    request_observes_response_affinity, strip_openai_reasoning_encrypted_items,
};
use crate::usage_decoder::{UsageDecoder, decode_full_body};
#[cfg(test)]
use crate::usage_parser::accumulate_sse_usage;
use crate::usage_parser::{self, UsageCounts, parse_sse_event};
use cc_lb_clock::{Clock, ClockHandle, unix_millis, unix_secs};
use cc_lb_control::RequestEventBus;
use cc_lb_control::api_keys::builtin_authn::{AuthnSuccess, BuiltinAuthError, BuiltinAuthn};
use cc_lb_control::api_keys::limit_engine::{
    LimitEngine, RejectReason, Reservation as LimitReservation,
};
use cc_lb_control::api_keys::principal_view::{
    PrincipalSpecCached, PrincipalView, RouterPipelineCache,
};
use cc_lb_control::api_keys::types::{Limit, LimitKind};
use cc_lb_control::dynamic_view::{DynamicView, DynamicViewHolder};
use cc_lb_control::subscription_metadata_hook::{MetadataHookHandle, MetadataHookRequest};
use cc_lb_control::{
    PromptCacheObservationEnqueueError, PromptCacheObservationSinkLike, SubscriptionQuotaCacheLike,
};
use cc_lb_domain::ReplicaIdentity;
use cc_lb_observability::truncate_reason;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WebSocketTurnOwner {
    None,
    SessionHint(Uuid),
    PrewarmResponse(Uuid),
    Response(Uuid),
}

pub struct Lifecycle {
    authn: Arc<BuiltinAuthn>,
    dynamic_view: Arc<DynamicViewHolder>,
    dispatcher: Arc<dyn UpstreamDispatch>,
    upstream_affinity_store: Option<Arc<dyn UpstreamAffinityStore>>,
    config: LifecycleConfig,
    limit_engine: Option<Arc<LimitEngine>>,
    limit_subject_provider: Option<Arc<dyn LimitSubjectProvider>>,
    limit_cost_estimator: Option<Arc<dyn LimitCostEstimator>>,
    event_bus: Option<Arc<dyn RequestEventBus>>,
    subscription_quota_sink: Option<SubscriptionQuotaSink>,
    subscription_metadata_hook: Option<MetadataHookHandle>,
    subscription_quota_cache: Option<Arc<dyn SubscriptionQuotaCacheLike>>,
    cache_keepalive: crate::cache_keepalive::LifecycleKeepalive,
    thread_sticky_subagent_latch: ThreadStickySubagentLatch,
    openai_session_hint: OpenaiSessionHintCache,
    prompt_cache_analysis_executor: PromptCacheAnalysisExecutor,
    completion_observer: Arc<CompletionObserver>,
    clock: ClockHandle,
    rng: Mutex<StdRng>,
}

impl Lifecycle {
    pub fn new_with_dynamic_view(
        authn: Arc<BuiltinAuthn>,
        dynamic_view: Arc<DynamicViewHolder>,
        dispatcher: Arc<dyn UpstreamDispatch>,
        config: LifecycleConfig,
        clock: ClockHandle,
    ) -> Self {
        Self {
            authn,
            dynamic_view: Arc::clone(&dynamic_view),
            dispatcher,
            upstream_affinity_store: None,
            config,
            limit_engine: None,
            limit_subject_provider: None,
            limit_cost_estimator: None,
            event_bus: None,
            subscription_quota_sink: None,
            subscription_metadata_hook: None,
            subscription_quota_cache: None,
            cache_keepalive: crate::cache_keepalive::LifecycleKeepalive::new(
                None,
                Arc::clone(&dynamic_view),
            ),
            thread_sticky_subagent_latch: ThreadStickySubagentLatch::default(),
            openai_session_hint: OpenaiSessionHintCache::default(),
            prompt_cache_analysis_executor: PromptCacheAnalysisExecutor::default(),
            completion_observer: Arc::new(CompletionObserver::new()),
            clock,
            rng: Mutex::new(rand::make_rng()),
        }
    }

    pub fn with_cache_keepalive_enqueuer(
        mut self,
        enqueuer: Arc<dyn crate::cache_keepalive::CacheKeepaliveEnqueuer>,
    ) -> Self {
        self.cache_keepalive = self.cache_keepalive.with_enqueuer(enqueuer);
        self
    }

    pub fn with_upstream_affinity_store(mut self, store: Arc<dyn UpstreamAffinityStore>) -> Self {
        self.upstream_affinity_store = Some(store);
        self
    }

    pub fn with_terminal_rng_seed(mut self, seed: [u8; 32]) -> Self {
        self.rng = Mutex::new(StdRng::from_seed(seed));
        self
    }

    pub fn dynamic_view(&self) -> Arc<DynamicViewHolder> {
        Arc::clone(&self.dynamic_view)
    }

    pub fn replica_identity(&self) -> Option<ReplicaIdentity> {
        self.config.replica_identity.clone()
    }

    pub fn event_bus(&self) -> Option<Arc<dyn RequestEventBus>> {
        self.event_bus.clone()
    }

    /// Drains accepted streaming completion observations and stops the dedicated worker.
    pub async fn shutdown(&self) {
        self.completion_observer.shutdown().await;
    }

    pub fn with_event_bus(mut self, bus: Arc<dyn RequestEventBus>) -> Self {
        self.event_bus = Some(bus);
        self
    }

    pub fn with_subscription_quota_sink(mut self, sink: SubscriptionQuotaSink) -> Self {
        self.subscription_quota_sink = Some(sink);
        self
    }

    pub fn with_subscription_metadata_hook(mut self, hook: MetadataHookHandle) -> Self {
        self.subscription_metadata_hook = Some(hook);
        self
    }

    pub async fn enqueue_metadata_refresh(
        &self,
        upstream_id: Uuid,
        credential_generation: u64,
        traceparent: Option<String>,
    ) {
        if let Some(hook) = &self.subscription_metadata_hook
            && let Err(error) = hook
                .enqueue(MetadataHookRequest {
                    upstream_id,
                    credential_generation,
                    traceparent,
                })
                .await
        {
            tracing::warn!(%error, %upstream_id, "metadata refresh enqueue failed");
        }
    }

    pub fn with_subscription_quota_cache(
        mut self,
        cache: Arc<dyn SubscriptionQuotaCacheLike>,
    ) -> Self {
        self.subscription_quota_cache = Some(cache);
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

    pub fn with_limit_cost_estimator(mut self, estimator: Arc<dyn LimitCostEstimator>) -> Self {
        self.limit_cost_estimator = Some(estimator);
        self
    }

    fn select_terminal_upstream(
        &self,
        strategy: TerminalStrategy,
        candidates: &[UpstreamCandidate],
    ) -> TerminalDecision {
        let upstream_id = match strategy {
            TerminalStrategy::FirstPick => {
                candidates.first().map(|candidate| candidate.upstream_id)
            }
            TerminalStrategy::Random => {
                if candidates.is_empty() {
                    None
                } else {
                    let mut rng = self.rng.lock().expect("terminal rng lock");
                    let index = rng.random_range(..candidates.len());
                    Some(candidates[index].upstream_id)
                }
            }
        };

        TerminalDecision {
            upstream_id,
            strategy,
        }
    }

    async fn resolve_request_affinities(
        &self,
        keys: &[UpstreamAffinityKey],
    ) -> Result<Option<RequestAffinityResolution>, RequestAffinityFailure> {
        if keys.is_empty() {
            return Ok(None);
        }
        let Some(store) = self.upstream_affinity_store.as_ref() else {
            return Err(RequestAffinityFailure {
                status: StatusCode::SERVICE_UNAVAILABLE,
                error_type: "api_error",
                message: UPSTREAM_AFFINITY_UNAVAILABLE_MESSAGE,
            });
        };
        let now_unix_secs = unix_secs(self.clock.now());
        let bindings = store
            .resolve_upstream_affinities(
                keys,
                now_unix_secs,
                self.config.upstream_affinity_ttl.as_secs(),
            )
            .await
            .map_err(|_| RequestAffinityFailure {
                status: StatusCode::SERVICE_UNAVAILABLE,
                error_type: "api_error",
                message: UPSTREAM_AFFINITY_UNAVAILABLE_MESSAGE,
            })?;
        let requested_keys = keys.iter().collect::<HashSet<_>>();
        let mut matched_keys = HashSet::with_capacity(bindings.len().min(keys.len()));
        let mut target_upstream_id = None;
        let mut owner_epoch = None;
        for binding in &bindings {
            if !requested_keys.contains(&binding.key) {
                continue;
            }
            matched_keys.insert(&binding.key);
            match target_upstream_id {
                Some(target) if target != binding.upstream_id => {
                    return Err(RequestAffinityFailure {
                        status: StatusCode::BAD_REQUEST,
                        error_type: "invalid_request_error",
                        message: "opaque request content resolves to multiple upstreams",
                    });
                }
                Some(_) => {}
                None => target_upstream_id = Some(binding.upstream_id),
            }
            if binding.key.provider == "gemini"
                && owner_epoch.is_some_and(|epoch| epoch != binding.owner_epoch)
            {
                return Err(RequestAffinityFailure {
                    status: StatusCode::SERVICE_UNAVAILABLE,
                    error_type: "api_error",
                    message: UPSTREAM_AFFINITY_UNAVAILABLE_MESSAGE,
                });
            }
            owner_epoch = Some(binding.owner_epoch);
        }
        let Some(target_upstream_id) = target_upstream_id else {
            return Err(RequestAffinityFailure {
                status: StatusCode::SERVICE_UNAVAILABLE,
                error_type: "api_error",
                message: UPSTREAM_AFFINITY_NOT_ESTABLISHED_MESSAGE,
            });
        };
        let pending_keys = keys
            .iter()
            .filter(|key| !matched_keys.contains(key))
            .cloned()
            .collect();
        Ok(Some(RequestAffinityResolution {
            target_upstream_id,
            owner_epoch: owner_epoch.expect("resolved affinity has a matching binding"),
            pending_keys,
        }))
    }

    /// Authenticate a request before doing anything else with it. Reads only
    /// headers: the body is not touched until the returned proof exists.
    pub async fn authenticate(
        &self,
        headers: &http::HeaderMap,
    ) -> Result<crate::authn_rail::Authenticated, BuiltinAuthError> {
        let view = self.dynamic_view.load();
        let span = tracing::info_span!(
            "proxy.authenticate",
            cc_lb.auth.outcome = tracing::field::Empty,
            cc_lb.auth.duration_ms = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
            error.type = tracing::field::Empty,
        );
        let result =
            crate::authn_rail::authenticate_first(&self.authn, headers, &view.principal_view)
                .instrument(span.clone())
                .await;
        match &result {
            Ok(auth) => {
                span.record("cc_lb.auth.outcome", "success");
                span.record(
                    "cc_lb.auth.duration_ms",
                    numeric_attribute(Some(auth.auth_ms())).as_value(),
                );
            }
            Err(_) => {
                span.record("cc_lb.auth.outcome", "error");
                span.record("otel.status_code", "ERROR");
                span.record("error.type", error_codes::AUTHENTICATION_FAILED);
            }
        }
        result
    }

    /// Build authentication metadata for a WebSocket turn from the proof and
    /// the current principal view, without touching the client body.
    pub(crate) fn websocket_auth_info(
        &self,
        auth: &crate::Authenticated,
    ) -> cc_lb_lifecycle::AuthInfo {
        let principal_kind = self
            .dynamic_view
            .load()
            .principal_view
            .get(auth.principal_id())
            .map(|cached| db_principal_kind_as_str(cached.principal_kind()).to_owned());
        cc_lb_lifecycle::AuthInfo {
            principal_id: auth.principal_id().to_owned(),
            key_id: Some(auth.key_id().to_owned()),
            principal_kind,
            auth_ms: Some(auth.auth_ms()),
        }
    }

    /// Allocate observation state without recording a successful upgrade as a turn.
    pub(crate) fn websocket_observer(&self, path: &str) -> (Option<LifecycleContext>, String) {
        let request_id = next_request_id();
        let observer = self.event_bus.as_ref().map(|bus| {
            let observer = LifecycleContext::new(request_id.clone(), Arc::clone(bus), &self.clock);
            observer.set_event_kind(cc_lb_request_log::RequestEventKind::from_path(path));
            observer
        });
        (observer, request_id)
    }

    /// Queue an already-received provider frame; this never reads a client body
    /// or dispatches upstream. Connection ownership is checked by the gateway.
    pub(crate) fn observe_websocket_quota_frame(
        &self,
        upstream_id: Uuid,
        frame: bytes::Bytes,
        event_id: Option<&str>,
        matched_response_id: Option<&str>,
    ) {
        if let Some(bus) = self.event_bus.as_ref() {
            bus.publish_lifecycle(
                cc_lb_lifecycle::LifecycleEvent::UpstreamQuotaFrameObserved {
                    event_id: event_id.unwrap_or_default().to_owned(),
                    upstream_id,
                    observed_at_unix_millis: self
                        .clock
                        .now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|duration| duration.as_millis() as u64)
                        .unwrap_or(0),
                    frame,
                    matched_response_id: matched_response_id.map(str::to_owned),
                },
            );
        }
    }

    pub fn reject_responses_websocket_authentication(
        &self,
        error: &BuiltinAuthError,
        path: &str,
    ) -> Response<Body> {
        self.reject_responses_websocket_authentication_for_provider(error, path, None)
    }

    pub fn reject_responses_websocket_authentication_for_provider(
        &self,
        error: &BuiltinAuthError,
        path: &str,
        provider: Option<cc_lb_domain::Provider>,
    ) -> Response<Body> {
        let (observer, _) = self.websocket_observer(path);
        if let Some(observer) = observer.as_ref() {
            observer.mark_authn_reached();
        }
        crate::authn_rail::reject_unauthenticated_for_route(
            error,
            &http::Method::GET,
            path,
            provider,
            observer.as_ref(),
        )
    }

    pub(crate) fn publish_websocket_provider_cache_observation(
        &self,
        context: &PromptCacheObservationContext,
        usage: &UsageCounts,
    ) {
        publish_provider_cache_observation(
            context,
            PromptCacheUsage::from(usage),
            unix_secs(self.clock.now()),
        );
    }

    #[allow(clippy::too_many_arguments)]
    /// Route and reserve one logical Responses WebSocket turn.
    ///
    /// `codex_backend` selects the raw Codex backend surface; public
    /// Responses turns retain the compatibility transformations.
    pub(crate) async fn prepare_websocket_turn_with_surface(
        &self,
        auth: &crate::Authenticated,
        ingress: WebSocketIngress<'_>,
        body: Arc<Value>,
        owner: WebSocketTurnOwner,
        observer: Option<LifecycleContext>,
        request_id: &str,
        codex_backend: bool,
    ) -> Result<PreparedWebSocketTurn, WebSocketError> {
        let span = tracing::info_span!(
            "proxy.responses_websocket.turn",
            cc_lb.request.id = request_id,
            cc_lb.event.id = observer.as_ref().map(|observer| observer.event_id()),
            gen_ai.response.id = tracing::field::Empty,
            cc_lb.responses.stream_id = tracing::field::Empty,
            cc_lb.responses.connection_id = tracing::field::Empty,
            cc_lb.upstream.id = tracing::field::Empty,
            gen_ai.request.model = tracing::field::Empty,
            gen_ai.request.stream = true,
            http.request.method = "GET",
            url.path = "/v1/responses",
        );
        let stream_span = tracing::info_span!(
            "proxy.response_stream",
            cc_lb.request.id = request_id,
            cc_lb.event.id = observer.as_ref().map(|observer| observer.event_id()),
            cc_lb.responses.stream_id = tracing::field::Empty,
            cc_lb.responses.connection_id = tracing::field::Empty,
            cc_lb.upstream.id = tracing::field::Empty,
            gen_ai.request.model = tracing::field::Empty,
            gen_ai.request.stream = true,
            transport = "websocket",
            otel.status_code = tracing::field::Empty,
            http.response.status_code = tracing::field::Empty,
            cc_lb.request.outcome = tracing::field::Empty,
            cc_lb.request.duration_ms = tracing::field::Empty,
            cc_lb.request.finalize_ms = tracing::field::Empty,
            cc_lb.request.unaccounted_ms = tracing::field::Empty,
            stream.outcome = tracing::field::Empty,
            error.cause = tracing::field::Empty,
            error.chain = tracing::field::Empty,
            error.io.kind = tracing::field::Empty,
            error.io.os_error = tracing::field::Empty,
            error.type = tracing::field::Empty,
            stream_first_output_ms = tracing::field::Empty,
            stream_last_output_ms = tracing::field::Empty,
            stream_event_count = tracing::field::Empty,
            stream_total_ms = tracing::field::Empty,
            stream_inter_output_avg_ms = tracing::field::Empty,
            provider_session_id = tracing::field::Empty,
            provider_connection_id = tracing::field::Empty,
            total_bytes = tracing::field::Empty,
        );
        if let Some(observer) = observer.as_ref() {
            observer.mark_authn_reached();
            observer.set_protocol(cc_lb_request_log::RequestProtocol::Websocket);
            observer.set_request_span(span.clone());
            observer.set_response_stream_span(stream_span.clone());
            observer.emit_request_started(true);
        }
        let mut setup_timings = cc_lb_lifecycle::RequestSetupTimings::default();
        let result = async {
            let view = self.dynamic_view.load();
            let cached = view
                .principal_view
                .get(auth.principal_id())
                .ok_or_else(|| {
                    WebSocketError::new(
                        403,
                        error_codes::PRINCIPAL_MISSING,
                        "authenticated principal is unavailable",
                    )
                })?;
            let principal = Principal {
                id: auth.principal_id().to_owned(),
                kind: PrincipalKind::InternalKey,
            };
            if let Some(observer) = observer.as_ref() {
                let cache_structure_started = Instant::now();
                let cache_metadata = request_cache_metadata_from_value_with_analysis_for_capability(
                    ingress.headers,
                    Some(body.as_ref()),
                    None,
                    Some(ProviderCapability::OpenaiResponses),
                    None,
                );
                setup_timings.cache_structure_ms =
                    Some(cache_structure_started.elapsed().as_secs_f64() * 1_000.0);
                observer.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::ParseCompleted {
                    event_id: observer.event_id().to_owned(),
                    result: Ok(cc_lb_lifecycle::ParseInfo {
                        path: "/v1/responses".to_owned(),
                        method: "GET".to_owned(),
                        model: body.get("model").and_then(Value::as_str).map(str::to_owned),
                        stream: true,
                        body_bytes: ingress.frame.len() as u64,
                        thinking_budget_tokens: None,
                        reasoning_effort: body
                            .pointer("/reasoning/effort")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        thinking_level: None,
                        cache_breakpoints: Vec::new(),
                        cache_prefix_hash: None,
                        thread_id: cache_metadata.thread_id.clone(),
                        observed_session_id: cache_metadata.observed_session_id.clone(),
                        request_kind: cache_metadata.request_kind.clone(),
                        claude_agent_id: cache_metadata.claude_agent_id.clone(),
                        claude_parent_agent_id: cache_metadata.claude_parent_agent_id.clone(),
                        parent_session_id: cache_metadata.parent_session_id.clone(),
                        client_app: cache_metadata.client_app.clone(),
                        session_id_source: cache_metadata.session_id_source.clone(),
                        message_id: None,
                        message_index: None,
                        message_count: None,
                        cache_control_message_indices: Vec::new(),
                        native_cache: cache_metadata.native_cache.clone(),
                        keepalive_session_key_hash:
                            crate::cache_keepalive::follow_up_session_key_hash(
                                &principal,
                                &cache_metadata,
                            ),
                    }),
                });
            }
            let route_started = Instant::now();
            let (ctx, mut pipeline_result, unknown_reasoning_plan, fallback_candidates) = self
                .websocket_routing(
                    &view,
                    auth,
                    ingress,
                    body.as_ref(),
                    owner,
                    request_id.to_owned(),
                    codex_backend,
                    false,
                )
                .await
                .inspect_err(|error| {
                    if let Some(observer) = observer.as_ref() {
                        observer.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::RouteCompleted {
                            event_id: observer.event_id().to_owned(),
                            result: Err(
                                if error.code == error_codes::ROUTER_PIPELINE_UNAVAILABLE {
                                    cc_lb_lifecycle::RouteFailure::RouterPipelineUnavailable
                                } else {
                                    cc_lb_lifecycle::RouteFailure::RouteNoUpstreamAfterFilter
                                },
                            ),
                            routing_trace: None,
                        });
                    }
                })?;
            let effective_body: Arc<Value> = match &unknown_reasoning_plan {
                WsUnknownReasoningPlan::StrippedUpfront { body: stripped, .. } => {
                    Arc::clone(stripped)
                }
                _ => Arc::clone(&body),
            };
            if let Some(observer) = observer.as_ref() {
                for error in pipeline_result.internal_errors.drain(..) {
                    observer.record_internal_error(error);
                }
            }
            let terminal = self.select_terminal_upstream(
                cached.resolved_pipeline(None).terminal.clone(),
                &pipeline_result.candidates,
            );
            let selected_id = terminal.upstream_id.ok_or_else(|| {
                WebSocketError::new(
                    503,
                    error_codes::ROUTE_NO_UPSTREAM_AFTER_FILTER,
                    "no eligible Responses upstream",
                )
            })?;
            let record = view
                .upstreams_snapshot()
                .iter()
                .find(|record| record.id == selected_id)
                .ok_or_else(|| {
                    WebSocketError::new(
                        502,
                        error_codes::ROUTE_NOT_CONFIGURED,
                        "selected upstream is unavailable",
                    )
                })?;
            let (route, base_url) = route_for_record(record, cached).map_err(|message| {
                WebSocketError::new(502, error_codes::ROUTE_NOT_CONFIGURED, message)
            })?;
            let routed = RoutedUpstream::new(
                record,
                route,
                base_url,
                &pipeline_result,
                terminal,
                None,
                duration_to_ms(route_started.elapsed()),
            );
            let model = effective_body
                .get("model")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let prompt_cache_observation_context = prompt_cache_observation_context(
                &view,
                selected_id,
                &ctx.canonical_model_id,
                &[],
                &HashSet::new(),
                None,
                provider_cache::analyze(
                    ProviderCapability::OpenaiResponses,
                    effective_body.as_ref(),
                    &ctx.canonical_model_id,
                    ingress.headers,
                ),
            );
            span.record("gen_ai.request.model", model.as_str());
            span.record("cc_lb.upstream.id", tracing::field::display(selected_id));
            let route_info = route_info_for_routed(&routed, Some(&model));
            if let Some(observer) = observer.as_ref() {
                observer.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::RouteCompleted {
                    event_id: observer.event_id().to_owned(),
                    result: Ok(route_info.clone()),
                    routing_trace: route_info.routing_trace.clone(),
                });
            }
            let body_view = RequestBodyView {
                parsed: Ok(effective_body.clone()),
                json_parse_ms: 0.0,
            };
            let reserve_started = Instant::now();
            let mut active_limit = match self
                .reserve_limit(
                    &view.principal_view,
                    &ctx,
                    &principal,
                    record,
                    auth.success(),
                    || {
                        body_view.limit_request(
                            &ctx.canonical_model_id,
                            Some(ProviderCapability::OpenaiResponses),
                            &routed.cache_bundle.cache_breakpoints,
                            true,
                        )
                    },
                )
                .await
            {
                Ok(limit) => limit,
                Err(LimitRejectionErr { response, info }) => {
                    if let Some(observer) = observer.as_ref() {
                        observer.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::LimitDecision {
                            event_id: observer.event_id().to_owned(),
                            decision: cc_lb_lifecycle::LimitDecisionKind::Rejected {
                                reason: info.reason_label.clone(),
                                subject: info.subject,
                                request_summary: info.request_summary,
                                route_summary: info.route_summary,
                                limit_violation: info.limit_violation,
                            },
                        });
                    }
                    return Err(WebSocketError::new(
                        response.status().as_u16(),
                        error_codes::LIMIT_REJECTED,
                        info.reason_label,
                    ));
                }
            };
            let reservation_id = active_limit
                .as_ref()
                .and_then(|limit| limit.reservation.as_ref())
                .map(|reservation| reservation.id().to_owned())
                .unwrap_or_default();
            if let Some(observer) = observer.as_ref() {
                if let Some(engine) = self.limit_engine.as_ref()
                    && !reservation_id.is_empty()
                {
                    observer.attach_limit_reservation(Arc::clone(engine), reservation_id.clone());
                }
                observer.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::LimitDecision {
                    event_id: observer.event_id().to_owned(),
                    decision: cc_lb_lifecycle::LimitDecisionKind::Reserved {
                        reservation_id,
                        amount: active_limit.as_ref().map_or(0, |limit| {
                            limit.request.max_tokens.unwrap_or(0).max(0) as u64
                        }),
                        limit_reserve_ms: Some(duration_to_ms(reserve_started.elapsed())),
                    },
                });
            }
            let explicit_store = body.get("store").and_then(Value::as_bool);
            let mut candidates =
                Vec::with_capacity(pipeline_result.candidates.len() + fallback_candidates.len());
            candidates.push(WebSocketCandidate {
                upstream_id: selected_id,
                upstream_name: record.name.clone(),
                socket_per_lane: record.kind == StorageUpstreamKind::OpenaiOauth,
                is_api_key: record.kind == StorageUpstreamKind::OpenaiApiKey,
            });
            candidates.extend(
                pipeline_result
                    .candidates
                    .into_iter()
                    .chain(fallback_candidates)
                    .filter(|candidate| candidate.upstream_id != selected_id)
                    .map(|candidate| WebSocketCandidate {
                        upstream_id: candidate.upstream_id,
                        upstream_name: candidate.name,
                        socket_per_lane: candidate.kind == cc_lb_domain::UpstreamKind::OpenaiOauth,
                        is_api_key: candidate.kind.credential_kind() == CredentialKind::ApiKey,
                    }),
            );
            Ok(PreparedWebSocketTurn {
                candidates,
                route_info,
                observer: observer.clone(),
                completion_observer: Arc::clone(&self.completion_observer),
                prompt_cache_observation_context,
                reservation: active_limit
                    .as_mut()
                    .and_then(|limit| limit.reservation.take()),
                limit_engine: self.limit_engine.clone(),
                oauth_compat: false,
                oauth_transport: record.kind == StorageUpstreamKind::OpenaiOauth,
                explicit_store,
                principal_id: auth.principal_id().to_owned(),
                key_id: auth.key_id().to_owned(),
                model,
                store: explicit_store.unwrap_or(record.kind != StorageUpstreamKind::OpenaiOauth),
                span: stream_span.clone(),
                setup_timings,
                request_id: request_id.to_owned(),
                unknown_reasoning_plan,
            })
        }
        .instrument(span.clone())
        .await;
        if let (Err(error), Some(observer)) = (&result, observer.as_ref()) {
            observer.terminate_failure(InternalFailure {
                status: StatusCode::from_u16(error.status).unwrap_or(StatusCode::BAD_GATEWAY),
                error_code: error.code,
                error: InternalError {
                    stage: if error.code == error_codes::PRINCIPAL_MISSING
                        || error.code == "principal_unavailable"
                    {
                        InternalErrorStage::Authn
                    } else if error.code == "invalid_request_error" {
                        InternalErrorStage::Ingress
                    } else {
                        InternalErrorStage::Router
                    },
                    kind: if error.status < 500 {
                        InternalErrorKind::InvalidInput
                    } else {
                        InternalErrorKind::Unavailable
                    },
                    message: Some(error.message.clone()),
                },
            });
        }
        result
    }

    pub(crate) fn record_openai_session_hint(
        &self,
        principal_id: &str,
        headers: &HeaderMap,
        body: &Value,
        upstream_id: Uuid,
    ) {
        if is_openai_prewarm_request(body) {
            return;
        }
        let now_unix_secs = unix_secs(self.clock.now());
        if let Some(cache_key) = body
            .get("prompt_cache_key")
            .and_then(Value::as_str)
            .filter(|key| !key.is_empty())
        {
            self.openai_session_hint
                .record(principal_id, cache_key, upstream_id, now_unix_secs);
        }
        let identity = request_identity_metadata(
            headers,
            Some(body),
            Some(ProviderCapability::OpenaiResponses),
        );
        if let Some(session_id) = identity
            .observed_session_id
            .as_deref()
            .filter(|id| !id.is_empty())
        {
            self.openai_session_hint
                .record(principal_id, session_id, upstream_id, now_unix_secs);
        }
    }

    fn record_openai_session_hint_keys(
        &self,
        principal_id: &str,
        keys: &[String],
        upstream_id: Uuid,
    ) {
        if keys.is_empty() {
            return;
        }
        let now_unix_secs = unix_secs(self.clock.now());
        for key in keys {
            self.openai_session_hint
                .record(principal_id, key, upstream_id, now_unix_secs);
        }
    }

    pub(crate) fn is_openai_oauth_upstream(&self, upstream_id: Uuid) -> Option<bool> {
        let view = self.dynamic_view.load();
        view.upstreams_snapshot()
            .iter()
            .find(|record| record.id == upstream_id)
            .map(|record| record.kind == StorageUpstreamKind::OpenaiOauth)
    }

    /// Recheck a retained connection against the current view without reserving again.
    #[cfg(test)]
    pub(crate) async fn validate_websocket_turn(
        &self,
        auth: &crate::Authenticated,
        ingress: WebSocketIngress<'_>,
        body: &Value,
        upstream_id: Uuid,
        codex_backend: bool,
    ) -> Result<(), WebSocketError> {
        self.validate_websocket_turn_with_policy(
            auth,
            ingress,
            body,
            upstream_id,
            codex_backend,
            false,
        )
        .await
    }

    /// Recheck a retained or fallback candidate against the current view.
    pub(crate) async fn validate_websocket_turn_with_policy(
        &self,
        auth: &crate::Authenticated,
        ingress: WebSocketIngress<'_>,
        body: &Value,
        upstream_id: Uuid,
        codex_backend: bool,
        reject_exhausted_subscription: bool,
    ) -> Result<(), WebSocketError> {
        let view = self.dynamic_view.load();
        self.websocket_routing(
            &view,
            auth,
            ingress,
            body,
            WebSocketTurnOwner::Response(upstream_id),
            next_request_id(),
            codex_backend,
            reject_exhausted_subscription,
        )
        .await
        .map(|_| ())
    }

    #[allow(clippy::too_many_arguments)]
    async fn websocket_routing(
        &self,
        view: &DynamicView,
        auth: &crate::Authenticated,
        ingress: WebSocketIngress<'_>,
        body: &Value,
        owner: WebSocketTurnOwner,
        request_id: String,
        codex_backend: bool,
        reject_exhausted_subscription: bool,
    ) -> Result<
        (
            RequestContext,
            FilterPipelineResult,
            WsUnknownReasoningPlan,
            Vec<UpstreamCandidate>,
        ),
        WebSocketError,
    > {
        use cc_lb_control::api_keys::principal_view::PrincipalStatus;
        enum WsUnknownReasoningRecovery {
            None,
            Unestablished,
            Pending {
                pending_digests: HashSet<[u8; 32]>,
                pending_keys: Vec<UpstreamAffinityKey>,
            },
        }
        if view.principal_view.principal_status(auth.principal_id()) != PrincipalStatus::Active {
            return Err(WebSocketError::new(
                403,
                "principal_unavailable",
                "authenticated principal is unavailable",
            ));
        }
        let model = body
            .get("model")
            .and_then(Value::as_str)
            .filter(|model| !model.is_empty())
            .ok_or_else(|| {
                WebSocketError::new(
                    400,
                    "invalid_request_error",
                    "response.create requires a model",
                )
            })?;
        if !view
            .principal_view
            .is_model_allowed(auth.principal_id(), model)
        {
            return Err(WebSocketError::new(
                403,
                "model_not_allowed",
                "model is not allowed for this principal",
            ));
        }
        let has_connection_owner = matches!(
            owner,
            WebSocketTurnOwner::Response(_) | WebSocketTurnOwner::PrewarmResponse(_)
        );
        let keys = extract_request_affinity_keys_with_scope(
            ProviderCapability::OpenaiResponses,
            body,
            auth.principal_id(),
            if has_connection_owner {
                OpenaiResponseReferenceScope::ConnectionLocal
            } else {
                OpenaiResponseReferenceScope::Durable
            },
        )
        .map_err(|_| {
            WebSocketError::new(
                400,
                "invalid_request_error",
                UPSTREAM_AFFINITY_KEY_LIMIT_MESSAGE,
            )
        })?;
        let has_previous = body
            .get("previous_response_id")
            .and_then(Value::as_str)
            .is_some_and(|id| !id.is_empty());
        let mut recovery = WsUnknownReasoningRecovery::None;
        let affinity = match self.resolve_request_affinities(&keys).await {
            Ok(affinity) => {
                if let Some(ref resolved) = affinity
                    && self.config.unknown_openai_reasoning
                        == cc_lb_domain::UnknownOpenaiReasoningPolicy::Hybrid
                    && !has_previous
                {
                    let pending_digests = collect_openai_pending_reasoning_digests(
                        ProviderCapability::OpenaiResponses,
                        body,
                        &resolved.pending_keys,
                    );
                    if !pending_digests.is_empty() {
                        recovery = WsUnknownReasoningRecovery::Pending {
                            pending_digests,
                            pending_keys: resolved.pending_keys.clone(),
                        };
                    }
                }
                affinity
            }
            Err(failure) => {
                let allowed_upstreams = view.principal_view.allowed_upstreams(auth.principal_id());
                let mut has_openai_oauth = false;
                let mut has_openai_api_key = false;
                for record in view.upstreams_snapshot().iter().filter(|record| {
                    record.enabled
                        && allowed_upstreams.is_none_or(|allowed| {
                            allowed.is_empty() || allowed.contains(&record.id)
                        })
                }) {
                    match record.kind {
                        StorageUpstreamKind::OpenaiOauth => has_openai_oauth = true,
                        StorageUpstreamKind::OpenaiApiKey => has_openai_api_key = true,
                        _ => {}
                    }
                }
                let oauth_only = has_openai_oauth && (codex_backend || !has_openai_api_key);
                let not_established = failure.message == UPSTREAM_AFFINITY_NOT_ESTABLISHED_MESSAGE;
                if !has_connection_owner && has_previous && oauth_only && not_established {
                    return Err(WebSocketError::new(
                        400,
                        "previous_response_not_found",
                        "The previous response is not available on this connection",
                    ));
                }
                if not_established
                    && self.config.unknown_openai_reasoning
                        == cc_lb_domain::UnknownOpenaiReasoningPolicy::Hybrid
                    && is_openai_unknown_reasoning_only(ProviderCapability::OpenaiResponses, body)
                {
                    recovery = WsUnknownReasoningRecovery::Unestablished;
                    None
                } else {
                    return Err(WebSocketError::new(
                        failure.status.as_u16(),
                        error_codes::UPSTREAM_AFFINITY_UNAVAILABLE,
                        failure.message,
                    ));
                }
            }
        };
        let target = match (
            owner,
            affinity
                .as_ref()
                .map(|affinity| affinity.target_upstream_id),
        ) {
            (WebSocketTurnOwner::Response(owner_id), Some(affinity_id))
                if owner_id != affinity_id =>
            {
                return Err(WebSocketError::new(
                    400,
                    error_codes::UPSTREAM_AFFINITY_UNAVAILABLE,
                    "provider state belongs to a different upstream",
                ));
            }
            (WebSocketTurnOwner::PrewarmResponse(owner_id), Some(affinity_id))
                if owner_id != affinity_id =>
            {
                return Err(WebSocketError::new(
                    400,
                    "previous_response_not_found",
                    "The previous response is not available on this connection",
                ));
            }
            (
                WebSocketTurnOwner::Response(owner_id)
                | WebSocketTurnOwner::PrewarmResponse(owner_id),
                _,
            ) => Some(owner_id),
            (WebSocketTurnOwner::SessionHint(_), Some(affinity_id)) => Some(affinity_id),
            (WebSocketTurnOwner::SessionHint(hint_id), None) => Some(hint_id),
            (WebSocketTurnOwner::None, affinity_id) => affinity_id,
        };
        let cached = view
            .principal_view
            .get(auth.principal_id())
            .ok_or_else(|| {
                WebSocketError::new(
                    403,
                    error_codes::PRINCIPAL_MISSING,
                    "authenticated principal is unavailable",
                )
            })?;
        let pipeline = cached.resolved_pipeline(None);
        if let Some(error) = pipeline.instantiation_error.as_deref() {
            return Err(WebSocketError::new(
                502,
                error_codes::ROUTER_PIPELINE_UNAVAILABLE,
                error,
            ));
        }
        let mut downstream_headers = ingress.headers.clone();
        strip_hop_by_hop(&mut downstream_headers);
        let canonical_model = canonical_model_id(Provider::Openai, model).to_owned();
        let service_tier = body.get("service_tier").and_then(Value::as_str);
        let identity = request_identity_metadata(
            ingress.headers,
            Some(body),
            Some(ProviderCapability::OpenaiResponses),
        );
        let observed_session_id = identity.observed_session_id;
        let mut ctx = RequestContext::builder()
            .request_id(request_id)
            .thread_id(identity.thread_id)
            .downstream_headers(downstream_headers)
            .method(http::Method::GET)
            .codex_backend(codex_backend)
            .path("/v1/responses".to_owned())
            .query(ingress.query.map(str::to_owned))
            .body_bytes(ingress.frame.clone())
            .canonical_model_id(canonical_model.clone())
            .requested_service_tier(service_tier.map(str::to_owned))
            .cache_pricing(cache_pricing_summary_for_model(
                cc_lb_pricing::global_catalog(),
                &canonical_model,
                service_tier,
            ))
            .build();
        let principal = Principal {
            id: auth.principal_id().to_owned(),
            kind: PrincipalKind::InternalKey,
        };
        let native_cache = provider_cache::analyze(
            ProviderCapability::OpenaiResponses,
            body,
            &canonical_model,
            ingress.headers,
        );
        let native_cache_scores = fetch_provider_native_cache_scores(
            view,
            &principal.id,
            RequestKind::OpenaiResponses,
            &canonical_model,
            native_cache.as_ref(),
            &*self.clock,
        )
        .await;
        let mut candidates = build_candidates_with_native_cache_for_bundles(
            view,
            auth.principal_id(),
            RequestKind::OpenaiResponses,
            &[],
            &HashMap::new(),
            None,
            &native_cache_scores,
            &*self.clock,
        )
        .0;
        if codex_backend {
            candidates
                .retain(|candidate| candidate.kind == cc_lb_domain::UpstreamKind::OpenaiOauth);
        }
        if let Some(target) = target {
            candidates.retain(|candidate| candidate.upstream_id == target);
        }
        let prewarm_hint_id = if target.is_none() && is_openai_prewarm_request(body) {
            let now_unix_secs = unix_secs(self.clock.now());
            let ttl_secs = self.config.upstream_affinity_ttl.as_secs();
            let cache_key = body
                .get("prompt_cache_key")
                .and_then(Value::as_str)
                .filter(|key| !key.is_empty());
            let candidate_hint = cache_key
                .and_then(|key| {
                    self.openai_session_hint.lookup(
                        auth.principal_id(),
                        key,
                        now_unix_secs,
                        ttl_secs,
                    )
                })
                .or_else(|| {
                    observed_session_id.as_deref().and_then(|session_id| {
                        self.openai_session_hint.lookup(
                            auth.principal_id(),
                            session_id,
                            now_unix_secs,
                            ttl_secs,
                        )
                    })
                });
            candidate_hint.filter(|hint_id| {
                candidates.iter().any(|candidate| {
                    candidate.upstream_id == *hint_id
                        && !subscription_hard_blocked(ctx.canonical_model_id.as_str(), candidate)
                })
            })
        } else {
            None
        };
        let filters = pipeline.filters_for(cc_lb_domain::Provider::Openai);
        let pre_filter_candidates = (target.is_none()
            && candidates.len() > 1
            && filters
                .iter()
                .any(|filter| filter.plugin_id() == BUILTIN_SUBSCRIPTION_PREFERENCE_ID))
        .then(|| candidates.clone());
        let mut result = if let Some(hint_id) = prewarm_hint_id {
            let hinted: Vec<UpstreamCandidate> = candidates
                .iter()
                .filter(|candidate| candidate.upstream_id == hint_id)
                .cloned()
                .collect();
            let hinted_result = execute_filter_pipeline(filters, &ctx, &principal, hinted);
            if !hinted_result.candidates.is_empty()
                && !subscription_hard_blocked(
                    ctx.canonical_model_id.as_str(),
                    &hinted_result.candidates[0],
                )
            {
                hinted_result
            } else {
                execute_filter_pipeline(filters, &ctx, &principal, candidates)
            }
        } else {
            execute_filter_pipeline(filters, &ctx, &principal, candidates)
        };
        if let Some(target) = target {
            result
                .candidates
                .retain(|candidate| candidate.upstream_id == target);
        }
        if result.candidates.is_empty() {
            return Err(WebSocketError::new(
                503,
                if target.is_some() {
                    error_codes::UPSTREAM_AFFINITY_UNAVAILABLE
                } else {
                    error_codes::ROUTE_NO_UPSTREAM_AFTER_FILTER
                },
                if target.is_some() {
                    UPSTREAM_AFFINITY_TARGET_INELIGIBLE_MESSAGE
                } else {
                    "no upstream candidates remain after routing filters"
                },
            ));
        }
        if reject_exhausted_subscription
            && filters
                .iter()
                .any(|filter| filter.plugin_id() == BUILTIN_SUBSCRIPTION_PREFERENCE_ID)
            && result.candidates.iter().all(|candidate| {
                subscription_hard_blocked(ctx.canonical_model_id.as_str(), candidate)
            })
        {
            return Err(WebSocketError::new(
                503,
                error_codes::ROUTE_NO_UPSTREAM_AFTER_FILTER,
                "fallback subscription quota is exhausted",
            ));
        }
        // Built-in `subscription-preference` returns a single winner per filter pass;
        // precompute ordered fallback candidates (matching HTTP failover) so initial
        // WebSocket handshake failures can still fail over across eligible upstreams.
        const MAX_WS_FALLBACK_CANDIDATES: usize = 16;
        let mut fallback_candidates = Vec::new();
        if let Some(pool) = pre_filter_candidates
            && result.candidates.len() == 1
        {
            let mut excluded = vec![result.candidates[0].upstream_id];
            while fallback_candidates.len() < MAX_WS_FALLBACK_CANDIDATES {
                let remaining: Vec<UpstreamCandidate> = pool
                    .iter()
                    .filter(|candidate| !excluded.contains(&candidate.upstream_id))
                    .cloned()
                    .collect();
                if remaining.is_empty() {
                    break;
                }
                let next_result = execute_filter_pipeline(filters, &ctx, &principal, remaining);
                let terminal = self
                    .select_terminal_upstream(pipeline.terminal.clone(), &next_result.candidates);
                let Some(next_id) = terminal.upstream_id else {
                    break;
                };
                let Some(next_candidate) = next_result
                    .candidates
                    .into_iter()
                    .find(|candidate| candidate.upstream_id == next_id)
                else {
                    break;
                };
                if subscription_hard_blocked(ctx.canonical_model_id.as_str(), &next_candidate) {
                    break;
                }
                excluded.push(next_id);
                fallback_candidates.push(next_candidate);
            }
            // Keep eligible public API-key fallbacks reachable even when more than
            // `MAX_WS_FALLBACK_CANDIDATES` OAuth candidates precede them, or when
            // one API key sits at the end of the ordinary fallback list with its
            // breaker open; native Codex routes remain OAuth-only.
            if !codex_backend {
                const MIN_WS_API_KEY_CANDIDATES: usize = 4;
                let mut api_key_candidates = result
                    .candidates
                    .iter()
                    .chain(fallback_candidates.iter())
                    .filter(|candidate| candidate.kind.credential_kind() == CredentialKind::ApiKey)
                    .count();
                while api_key_candidates < MIN_WS_API_KEY_CANDIDATES {
                    let remaining_api_keys: Vec<UpstreamCandidate> = pool
                        .iter()
                        .filter(|candidate| {
                            !excluded.contains(&candidate.upstream_id)
                                && candidate.kind.credential_kind() == CredentialKind::ApiKey
                        })
                        .cloned()
                        .collect();
                    if remaining_api_keys.is_empty() {
                        break;
                    }
                    let next_result =
                        execute_filter_pipeline(filters, &ctx, &principal, remaining_api_keys);
                    let terminal = self.select_terminal_upstream(
                        pipeline.terminal.clone(),
                        &next_result.candidates,
                    );
                    let Some(next_id) = terminal.upstream_id else {
                        break;
                    };
                    let Some(next_candidate) = next_result
                        .candidates
                        .into_iter()
                        .find(|candidate| candidate.upstream_id == next_id)
                    else {
                        break;
                    };
                    excluded.push(next_id);
                    fallback_candidates.push(next_candidate);
                    api_key_candidates += 1;
                }
            }
        }
        let dispatchable_candidates = result.candidates.len() + fallback_candidates.len();
        let plan = match recovery {
            WsUnknownReasoningRecovery::None => WsUnknownReasoningPlan::None,
            WsUnknownReasoningRecovery::Unestablished => {
                if dispatchable_candidates > 1 {
                    if let Some(stripped) = strip_openai_reasoning_encrypted_items(body, None)
                        && let Ok(bytes) = serde_json::to_vec(&stripped)
                    {
                        let frame = Bytes::from(bytes);
                        ctx.body_bytes = frame.clone();
                        WsUnknownReasoningPlan::StrippedUpfront {
                            body: Arc::new(stripped),
                            frame,
                        }
                    } else {
                        WsUnknownReasoningPlan::None
                    }
                } else {
                    WsUnknownReasoningPlan::RetryStripAll { unknown_keys: keys }
                }
            }
            WsUnknownReasoningRecovery::Pending {
                pending_digests,
                pending_keys,
            } => WsUnknownReasoningPlan::RetryStripPending {
                pending_digests,
                pending_keys,
            },
        };
        Ok((ctx, result, plan, fallback_candidates))
    }

    /// Shape one WebSocket frame for its selected upstream.
    ///
    /// `codex_backend` forwards native Codex frames unchanged;
    /// public frames use the Responses compatibility contract.
    pub(crate) fn shape_websocket_frame_with_surface(
        &self,
        auth: &crate::Authenticated,
        ingress: WebSocketIngress<'_>,
        upstream_id: Uuid,
        request_id: &str,
        observer: Option<&LifecycleContext>,
        codex_backend: bool,
    ) -> Result<Option<WebSocketPluginRequest>, WebSocketError> {
        let view = self.dynamic_view.load();
        let cached = view
            .principal_view
            .get(auth.principal_id())
            .ok_or_else(|| {
                WebSocketError::new(
                    403,
                    error_codes::PRINCIPAL_MISSING,
                    "authenticated principal is unavailable",
                )
            })?;
        let record = view
            .upstreams_snapshot()
            .iter()
            .find(|record| record.id == upstream_id && record.enabled)
            .ok_or_else(|| {
                WebSocketError::new(
                    503,
                    error_codes::ROUTE_NOT_CONFIGURED,
                    "upstream is no longer available",
                )
            })?;
        let plugin_dialect = cached.resolved_dialect(upstream_id).cloned();
        let openai_oauth = record.kind == StorageUpstreamKind::OpenaiOauth;
        if plugin_dialect.is_none() && !openai_oauth {
            return Ok(None);
        }
        let upstream = upstream_for_record(record).map_err(|message| {
            WebSocketError::new(502, error_codes::ROUTE_NOT_CONFIGURED, message)
        })?;
        let mut downstream_headers = ingress.headers.clone();
        strip_hop_by_hop(&mut downstream_headers);
        // BuiltinAuthn authenticates managed cc-lb keys, not provider keys or
        // OAuth subjects. Human/machine/admin is a separate storage taxonomy.
        let principal = Principal {
            id: auth.principal_id().to_owned(),
            kind: PrincipalKind::InternalKey,
        };
        let (mut frame, headers) = match plugin_dialect.as_ref() {
            Some(dialect) => {
                let context = DialectShapeContext {
                    request_id: request_id.to_owned(),
                    downstream_headers,
                    method: http::Method::POST,
                    path: "/v1/responses".to_owned(),
                    query: ingress.query.map(str::to_owned),
                    body_bytes: ingress.frame.clone(),
                };
                match shape_request(dialect.as_ref(), &context, &upstream, &principal) {
                    Ok(shaped) => (shaped.body().clone(), shaped.headers().clone()),
                    Err(error) => {
                        tracing::warn!(%error, "websocket shape_request failed; forwarding raw frame");
                        record_shape_internal_error(observer, &error.to_string());
                        (ingress.frame.clone(), context.downstream_headers)
                    }
                }
            }
            None => (ingress.frame.clone(), downstream_headers),
        };
        let mut oauth_compat = false;
        let mut echo_metadata = None;
        if openai_oauth && !codex_backend {
            let (shaped_frame, public_shape, normalized_echo_metadata) =
                self.normalize_oauth_websocket_frame(cached, &frame)?;
            frame = shaped_frame;
            oauth_compat = public_shape;
            echo_metadata = normalized_echo_metadata;
        }
        Ok(Some(WebSocketPluginRequest {
            echo_metadata,
            frame,
            headers,
            oauth_compat,
            context: ResponseTransformContext {
                principal,
                upstream,
                request_method: http::Method::POST,
                request_path: "/v1/responses".to_owned(),
                dialect: plugin_dialect.unwrap_or_else(|| native_route_dialect(record)),
                gemini_oauth: false,
                vertex: false,
                codex_backend,
                vertex_location: None,
            },
        }))
    }

    /// Apply the public `/v1` compatibility rules to one `response.create`
    /// frame bound for a ChatGPT-subscription upstream. Returns the frame to
    /// send and whether the client used the public shape (provider frames for
    /// that turn are then normalized too). `stream_id` never reaches the
    /// backend: cc-lb emulates lanes with one upstream socket per lane.
    fn normalize_oauth_websocket_frame(
        &self,
        cached: &PrincipalSpecCached,
        frame: &Bytes,
    ) -> Result<(Bytes, bool, Option<Value>), WebSocketError> {
        let body: Value = sonic_rs::from_slice(frame).map_err(|_| {
            WebSocketError::new(400, "invalid_request_error", "WebSocket frame must be JSON")
        })?;
        let policy = cached
            .openai_oauth_compat()
            .map(|override_policy| override_policy.resolve(&self.config.openai_oauth_compat))
            .unwrap_or(self.config.openai_oauth_compat);
        let normalized = crate::openai_compat::request::normalize_responses_request(
            crate::openai_compat::request::NormalizeInput {
                body: &body,
                policy: &policy,
                transport: crate::openai_compat::request::Transport::WebSocket,
            },
        )
        .map_err(|error| {
            WebSocketError::new(400, "invalid_request_error", error.message)
                .with_native_code(error.code)
                .with_param(error.param)
        })?;
        let store_missing = body.get("store").is_none_or(Value::is_null);
        let public_shape =
            normalized.body.is_some() || normalized.echo_metadata.is_some() || store_missing;
        if !public_shape && body.get("stream_id").is_none() {
            return Ok((frame.clone(), false, None));
        }
        let mut out = normalized.body.unwrap_or(body);
        if let Some(object) = out.as_object_mut() {
            object.remove("stream_id");
            if store_missing {
                object.insert("store".to_owned(), Value::Bool(false));
            }
        }
        let bytes = serde_json::to_vec(&out).map_err(|_| {
            WebSocketError::new(500, "server_error", "could not encode WebSocket frame")
        })?;
        Ok((Bytes::from(bytes), public_shape, normalized.echo_metadata))
    }

    /// Sign only a newly opened upstream connection, using its current credentials.
    #[cfg(test)]
    pub(crate) async fn sign_websocket_connection(
        &self,
        auth: &crate::Authenticated,
        headers: &HeaderMap,
        candidate: &WebSocketCandidate,
        query: Option<&str>,
    ) -> Result<(Url, HeaderMap), WebSocketError> {
        self.sign_websocket_connection_with_signer(auth, headers, candidate, query)
            .await
            .map(|(url, headers, _, _, _)| (url, headers))
    }

    pub(crate) async fn sign_websocket_connection_with_signer(
        &self,
        auth: &crate::Authenticated,
        headers: &HeaderMap,
        candidate: &WebSocketCandidate,
        query: Option<&str>,
    ) -> Result<(Url, HeaderMap, Arc<dyn Signer>, f64, u64), WebSocketError> {
        let view = self.dynamic_view.load();
        let record = view
            .upstreams_snapshot()
            .iter()
            .find(|record| {
                record.id == candidate.upstream_id
                    && record.enabled
                    && matches!(
                        record.kind,
                        StorageUpstreamKind::OpenaiApiKey | StorageUpstreamKind::OpenaiOauth
                    )
            })
            .ok_or_else(|| {
                WebSocketError::new(
                    503,
                    error_codes::ROUTE_NOT_CONFIGURED,
                    "upstream is no longer available",
                )
            })?;
        let upstream = upstream_for_record(record).map_err(|message| {
            WebSocketError::new(502, error_codes::ROUTE_NOT_CONFIGURED, message)
        })?;
        let mut downstream_headers = headers.clone();
        strip_hop_by_hop(&mut downstream_headers);
        let ctx = RequestContext::builder()
            .request_id(next_request_id())
            .downstream_headers(downstream_headers)
            .method(http::Method::GET)
            .path("/v1/responses".to_owned())
            .query(query.map(str::to_owned))
            .build();
        let principal = Principal {
            id: auth.principal_id().to_owned(),
            kind: PrincipalKind::InternalKey,
        };
        let shaped = raw_passthrough_request(
            record.base_url.as_ref(),
            &ctx,
            &principal,
            &upstream,
            record.kind == StorageUpstreamKind::OpenaiOauth,
        )
        .map_err(|error| {
            WebSocketError::new(502, error_codes::ROUTE_NOT_CONFIGURED, error.to_string())
        })?;
        let factory = view.signer_factory.with_router_choice(record.name.clone());
        let signer_started = Instant::now();
        let signer: Arc<dyn Signer> = factory.build(&upstream).await.map_err(|error| {
            WebSocketError::new(502, "upstream_signing_failed", error.to_string())
        })?;
        let prepare_signer_ms = signer_started.elapsed().as_secs_f64() * 1_000.0;
        let sign_started = Instant::now();
        let signed = sign_request(signer.as_ref(), shaped)
            .await
            .map_err(|error| {
                WebSocketError::new(502, "upstream_signing_failed", error.to_string())
            })?;
        let sign_ms = duration_to_ms(sign_started.elapsed());
        let mut url = signed.url().clone();
        let scheme = match url.scheme() {
            "https" => "wss",
            "http" => "ws",
            "wss" => "wss",
            "ws" => "ws",
            _ => {
                return Err(WebSocketError::new(
                    502,
                    "invalid_upstream_url",
                    "unsupported upstream WebSocket URL scheme",
                ));
            }
        };
        url.set_scheme(scheme).map_err(|_| {
            WebSocketError::new(
                502,
                "invalid_upstream_url",
                "invalid upstream WebSocket URL",
            )
        })?;
        let mut headers = signed.headers().clone();
        strip_hop_by_hop(&mut headers);
        Ok((url, headers, signer, prepare_signer_ms, sign_ms))
    }

    /// Persist recognized state before the gateway exposes it to another connection.
    #[cfg(test)]
    pub(crate) async fn bind_websocket_response(
        &self,
        auth: &crate::Authenticated,
        upstream_id: Uuid,
        response: &Value,
        output_item: Option<&Value>,
        store: bool,
    ) -> Result<(), WebSocketError> {
        self.bind_websocket_response_with_pending(
            auth,
            upstream_id,
            response,
            output_item,
            store,
            &[],
        )
        .await
    }

    /// Persist recognized state (plus any verbatim-accepted pending request keys)
    /// before the gateway exposes the response to another connection.
    pub(crate) async fn bind_websocket_response_with_pending(
        &self,
        auth: &crate::Authenticated,
        upstream_id: Uuid,
        response: &Value,
        output_item: Option<&Value>,
        store: bool,
        pending_request_keys: &[UpstreamAffinityKey],
    ) -> Result<(), WebSocketError> {
        let mut keys = extract_websocket_response_affinity_keys(
            response,
            output_item,
            auth.principal_id(),
            if store {
                OpenaiResponseReferenceScope::Durable
            } else {
                OpenaiResponseReferenceScope::ConnectionLocal
            },
        )
        .map_err(|_| {
            WebSocketError::new(
                502,
                error_codes::UPSTREAM_AFFINITY_UNAVAILABLE,
                "upstream response carries invalid provider state",
            )
        })?;
        keys.extend(pending_request_keys.iter().cloned());
        let context = ResponseAffinityContext {
            capability: ProviderCapability::OpenaiResponses,
            usage_shape: None,
            precomputed_usage: None,
            store: self.upstream_affinity_store.clone(),
            principal_id: auth.principal_id().to_owned(),
            upstream_id,
            pending_request_keys: Vec::new(),
            observe_response_keys: true,
            owner_epoch: Some(0),
            ttl_secs: self.config.upstream_affinity_ttl.as_secs(),
            session_hint_keys: Vec::new(),
        };
        bind_upstream_affinity_keys(&context, &keys, unix_secs(self.clock.now()), None)
            .await
            .map_err(|_| {
                WebSocketError::new(
                    503,
                    error_codes::UPSTREAM_AFFINITY_UNAVAILABLE,
                    UPSTREAM_AFFINITY_UNAVAILABLE_MESSAGE,
                )
            })
    }

    /// Record the authenticated principal identity on the lifecycle context,
    /// resolving the real principal kind from the current view.
    ///
    /// Called by the server handler right after [`Self::authenticate`] and
    /// before the request body is read, so requests that terminate on a
    /// body-read error or the size cap still carry their real principal in
    /// metrics. `LifecycleContext::emit_authentication_completed` is
    /// idempotent, so the later call inside [`Self::handle`] is a no-op on the
    /// HTTP path; a caller that drives `handle` without the handler still gets
    /// exactly one event. Emits nothing when the principal is absent from the
    /// current view — `handle`'s `PrincipalMissing` path reports that case.
    pub fn record_authentication_completed(
        &self,
        auth: &crate::authn_rail::Authenticated,
        observer: &LifecycleContext,
    ) {
        let view = self.dynamic_view.load();
        let principal_id = auth.principal_id();
        let Some(cached) = view.principal_view.get(principal_id) else {
            return;
        };
        let kind = principal_kind_lite(cached.principal_kind());
        observer.emit_authentication_completed(principal_id.to_owned(), kind);
    }

    /// Handle a request that has already been authenticated. `auth` is the
    /// proof: callers must obtain it from [`Lifecycle::authenticate`] before
    /// the request body is read.
    #[allow(clippy::explicit_auto_deref)]
    #[tracing::instrument(
        name = "proxy.handle",
        skip_all,
        fields(
            cc_lb.request.id = tracing::field::Empty,
            http.request.method = tracing::field::Empty,
            url.path = tracing::field::Empty,
            gen_ai.request.model = tracing::field::Empty,
            gen_ai.request.stream = tracing::field::Empty,
            cc_lb.principal.id = tracing::field::Empty,
            cc_lb.upstream.name = tracing::field::Empty,
            cc_lb.upstream.id = tracing::field::Empty,
            cc_lb.request.finalize_ms = tracing::field::Empty,
            cc_lb.request.unaccounted_ms = tracing::field::Empty,
            cc_lb.request.outcome = tracing::field::Empty,
            cc_lb.request.duration_ms = tracing::field::Empty,
            cc_lb.request.retry_count = tracing::field::Empty,
            http.response.status_code = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
            error.type = tracing::field::Empty,
            cc_lb.upstream.bulkhead_wait_ms = tracing::field::Empty,
            cc_lb.upstream.dns_ms = tracing::field::Empty,
            cc_lb.upstream.connect_ms = tracing::field::Empty,
            cc_lb.upstream.connection_reused = tracing::field::Empty,
            cc_lb.upstream.shape_ms = tracing::field::Empty,
            cc_lb.upstream.sign_ms = tracing::field::Empty,
            cc_lb.upstream.ttfb_ms = tracing::field::Empty,
            cc_lb.response.first_body_chunk_ms = tracing::field::Empty,
            cc_lb.response.first_content_delta_ms = tracing::field::Empty,
            gen_ai.usage.input_tokens = tracing::field::Empty,
            gen_ai.usage.output_tokens = tracing::field::Empty,
            gen_ai.usage.cache_creation_input_tokens = tracing::field::Empty,
            gen_ai.usage.cache_read_input_tokens = tracing::field::Empty,
            cc_lb.usage.completeness = tracing::field::Empty,
        )
    )]
    pub async fn handle(
        &self,
        req: Request<Bytes>,
        auth: &crate::authn_rail::Authenticated,
    ) -> Result<Response<Body>, ProxyError> {
        let view = self.dynamic_view.load();
        let principal_view = Arc::clone(&view.principal_view);
        let started = Instant::now();
        let observer_from_ext = req.extensions().get::<LifecycleContext>().cloned();
        let (mut ctx, body_too_large) = self.parse(req);
        let content_mime = ctx
            .downstream_headers
            .get(http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .map(str::trim);
        let multipart = is_multipart_operation(&ctx.path)
            && content_mime.is_some_and(|mime| mime.eq_ignore_ascii_case("multipart/form-data"));
        // A public Realtime call carries an SDP offer, optionally wrapped with its
        // session config in a multipart form; neither envelope is JSON.
        let realtime_call_envelope = ctx.path == "/v1/realtime/calls"
            && content_mime.is_some_and(|mime| {
                mime.eq_ignore_ascii_case("application/sdp")
                    || mime.eq_ignore_ascii_case("multipart/form-data")
            });
        let non_json_body = multipart || realtime_call_envelope;
        let empty_body = Bytes::new();
        let mut body_view = RequestBodyView::new(if non_json_body {
            &empty_body
        } else {
            &ctx.body_bytes
        });
        let path_model = if multipart && body_too_large.is_none() {
            multipart_request_model(auth, &ctx.downstream_headers, &ctx.body_bytes).await
        } else {
            gemini_model_from_path(&ctx.path)
        };
        let request_capability =
            classify_provider_capability(&ctx.method, &ctx.path, ctx.provider_scope);
        let request_model = path_model
            .clone()
            .or_else(|| body_view.model())
            .map(|model| lifecycle_request_model(request_capability, &model));
        let mut setup_timings = cc_lb_lifecycle::RequestSetupTimings {
            json_parse_ms: Some(body_view.json_parse_ms()),
            ..cc_lb_lifecycle::RequestSetupTimings::default()
        };
        let handle_span = tracing::Span::current();
        handle_span.record("cc_lb.request.id", ctx.request_id.as_str());
        handle_span.record("http.request.method", ctx.method.as_str());
        handle_span.record("url.path", ctx.path.as_str());
        if let Some(model) = request_model.as_deref() {
            handle_span.record("gen_ai.request.model", model);
        }
        if body_view.stream() {
            handle_span.record("gen_ai.request.stream", true);
        }
        let observer: Option<LifecycleContext> = observer_from_ext.or_else(|| {
            self.event_bus
                .as_ref()
                .map(|bus| LifecycleContext::new(ctx.request_id.clone(), bus.clone(), &self.clock))
        });
        if let Some(o) = observer.as_ref() {
            // Holding `auth` is proof that authentication already succeeded, so
            // this request is inside the observation guarantee. Mark it here
            // rather than at the HTTP handler: `handle` is a public entry point
            // and every caller reaching it has passed the credential check.
            o.mark_authn_reached();
            o.set_request_span(handle_span.clone());
            o.set_event_kind(cc_lb_request_log::RequestEventKind::from_path(&ctx.path));
            o.emit_request_started(body_view.stream());
        }
        store_setup_timings(observer.as_ref(), setup_timings);
        // Authentication already succeeded: `auth` is the proof obtained from
        // `Lifecycle::authenticate` before the body was read. Resolve the
        // principal view and emit the `AuthCompleted` event before any
        // body-dependent exit (413 / invalid-JSON 400) so the request keeps
        // its real principal in metrics and request events on those paths.
        let success = auth.success();
        let auth_ms = auth.auth_ms();
        let principal_id = success.principal_id.clone();
        handle_span.record("cc_lb.principal.id", principal_id.as_str());
        let Some(cached) = principal_view.get(&principal_id) else {
            tracing::error!(%principal_id, "authenticated principal missing from principal view");
            let response = request_capability.map_or_else(
                || {
                    anthropic_error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "api_error",
                        "authenticated principal is unavailable",
                    )
                },
                |capability| {
                    provider_error_response(
                        capability,
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "api_error",
                        "authenticated principal is unavailable",
                        None,
                    )
                },
            );
            if let Some(o) = observer.as_ref() {
                o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::AuthCompleted {
                    event_id: o.event_id().to_owned(),
                    result: Err(cc_lb_lifecycle::AuthFailure::PrincipalMissing {
                        principal_id: principal_id.clone(),
                    }),
                });
                o.terminate_failure(InternalFailure {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    error_code: error_codes::PRINCIPAL_MISSING,
                    error: InternalError {
                        stage: InternalErrorStage::Authn,
                        kind: InternalErrorKind::Unavailable,
                        message: Some("authenticated principal is unavailable".to_owned()),
                    },
                });
            }
            return Ok(response);
        };
        let db_principal_kind = cached.principal_kind();
        let lifecycle_principal_kind = principal_kind_lite(db_principal_kind);
        if let Some(o) = observer.as_ref() {
            o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::AuthCompleted {
                event_id: o.event_id().to_owned(),
                result: Ok(cc_lb_lifecycle::AuthInfo {
                    principal_id: principal_id.clone(),
                    key_id: Some(success.key_id.clone()),
                    principal_kind: Some(db_principal_kind_as_str(db_principal_kind).to_owned()),
                    auth_ms: Some(auth_ms),
                }),
            });
            // Metrics-only principal identity. The server handler already
            // emitted it at entry (before the body read), so this call no-ops
            // on the HTTP path; a caller that drives `handle` directly still
            // records exactly one event with the real principal kind.
            o.emit_authentication_completed(principal_id.clone(), lifecycle_principal_kind);
        }
        let principal = Principal {
            id: principal_id,
            kind: PrincipalKind::InternalKey,
        };
        ctx.openai_oauth_compat = Some(effective_policy(
            &self.config.openai_oauth_compat,
            cached.openai_oauth_compat(),
        ));
        let Some(request_capability) = request_capability else {
            let response =
                unsupported_operation_response(ctx.provider_scope, &ctx.method, &ctx.path);
            if let Some(o) = observer.as_ref() {
                o.terminate_local(TerminalClassification::OPERATION_NOT_SUPPORTED);
            }
            return Ok(response);
        };
        if body_view
            .value()
            .is_some_and(|body| !body_is_consistency_safe(request_capability, body))
        {
            let response = provider_error_response(
                request_capability,
                StatusCode::NOT_FOUND,
                "operation_not_supported",
                "operation is not supported by the consistency-safe proxy boundary",
                None,
            );
            if let Some(o) = observer.as_ref() {
                o.terminate_local(TerminalClassification::OPERATION_NOT_SUPPORTED);
            }
            return Ok(response);
        }
        let request_kind = if request_capability == ProviderCapability::OpenaiImages
            && ctx.path == "/v1/images/variations"
        {
            RequestKind::OpenaiImageVariations
        } else {
            RequestKind::from(request_capability)
        };

        if body_too_large.is_some() {
            if let Some(o) = observer.as_ref() {
                o.terminate_body_too_large(body_cap_for_path(&self.config, &ctx.path) as u64);
            }
            return Ok(provider_error_response(
                request_capability,
                StatusCode::PAYLOAD_TOO_LARGE,
                "body_too_large",
                "request body exceeds configured cap",
                None,
            ));
        }
        if !ctx.body_bytes.is_empty()
            && request_kind != RequestKind::OpenaiAccountOwned
            && !body_view.is_valid_json()
            && !non_json_body
        {
            if let Some(o) = observer.as_ref() {
                o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::ParseCompleted {
                    event_id: o.event_id().to_owned(),
                    result: Err(cc_lb_lifecycle::ParseFailure::InvalidJson),
                });
                o.terminate_failure(InternalFailure {
                    status: StatusCode::BAD_REQUEST,
                    error_code: error_codes::INVALID_JSON,
                    error: InternalError {
                        stage: InternalErrorStage::Ingress,
                        kind: InternalErrorKind::InvalidInput,
                        message: Some("request body must be valid JSON".to_owned()),
                    },
                });
            }
            return Ok(provider_error_response(
                request_capability,
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "request body must be valid JSON",
                None,
            ));
        }
        ctx.compat_api_key_only_features = if ctx.codex_backend {
            Vec::new()
        } else {
            compat_request::api_key_only_features(body_view.value(), request_capability)
        };
        let prompt_cache_token_scope = prompt_cache_token_scope(&ctx.downstream_headers);
        let prompt_cache_analysis = match body_view.value_arc() {
            // Prompt-cache tokenization models Anthropic `cache_control`
            // breakpoints only. OpenAI Responses and Gemini bodies get neutral
            // cache metadata and are forwarded unchanged.
            Some(_) if request_capability != ProviderCapability::AnthropicMessages => {
                Some(PromptCacheAnalysisOutput {
                    analysis: V3PromptCacheAnalysis {
                        blocks: Vec::new(),
                        breakpoints: Vec::new(),
                        literal_breakpoints: Vec::new(),
                    },
                    cacheable_breakpoint_prefix_keys: Vec::new(),
                    literal_cacheable_breakpoint_prefix_keys: Vec::new(),
                    timings: PromptCacheAnalysisTimings::default(),
                })
            }
            Some(value) => {
                let canonical_model = value
                    .get("model")
                    .and_then(Value::as_str)
                    .map(|model| canonical_model_for_capability(Some(request_capability), model))
                    .unwrap_or_default()
                    .to_owned();
                let token_threshold = Some(cache_threshold_tokens(&canonical_model));
                let output = self
                    .prompt_cache_analysis_executor
                    .analyze(
                        value,
                        canonical_model,
                        prompt_cache_token_scope,
                        token_threshold,
                    )
                    .await;
                apply_prompt_cache_timings(&mut setup_timings, output.timings);
                store_setup_timings(observer.as_ref(), setup_timings);
                Some(output)
            }
            None => None,
        };
        let mut cache_metadata = request_cache_metadata_from_value_with_analysis_for_capability(
            &ctx.downstream_headers,
            body_view.value(),
            prompt_cache_analysis,
            Some(request_capability),
            path_model.as_deref(),
        );
        apply_thread_sticky_subagent_latch(
            &self.thread_sticky_subagent_latch,
            &principal.id,
            body_view.value(),
            &mut cache_metadata,
        );
        if let Some(o) = observer.as_ref() {
            o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::ParseCompleted {
                event_id: o.event_id().to_owned(),
                result: Ok(cc_lb_lifecycle::ParseInfo {
                    path: ctx.path.clone(),
                    method: ctx.method.to_string(),
                    model: request_model.clone(),
                    stream: body_view.stream(),
                    body_bytes: ctx.body_bytes.len() as u64,
                    thinking_budget_tokens: cache_metadata.thinking_budget_tokens,
                    reasoning_effort: cache_metadata.reasoning_effort.clone(),
                    thinking_level: cache_metadata.thinking_level.clone(),
                    cache_breakpoints: cache_metadata.cache_breakpoints.clone(),
                    cache_prefix_hash: cache_metadata.cache_prefix_hash.clone(),
                    thread_id: cache_metadata.thread_id.clone(),
                    observed_session_id: cache_metadata.observed_session_id.clone(),
                    request_kind: cache_metadata.request_kind.clone(),
                    claude_agent_id: cache_metadata.claude_agent_id.clone(),
                    claude_parent_agent_id: cache_metadata.claude_parent_agent_id.clone(),
                    parent_session_id: cache_metadata.parent_session_id.clone(),
                    client_app: cache_metadata.client_app.clone(),
                    session_id_source: cache_metadata.session_id_source.clone(),
                    message_id: cache_metadata.message_id.clone(),
                    message_index: cache_metadata.message_index,
                    message_count: cache_metadata.message_count,
                    cache_control_message_indices: cache_metadata
                        .cache_control_message_indices
                        .clone(),
                    native_cache: cache_metadata.native_cache.clone(),
                    keepalive_session_key_hash: crate::cache_keepalive::follow_up_session_key_hash(
                        &principal,
                        &cache_metadata,
                    ),
                }),
            });
        }
        ctx.cache_breakpoints = cache_metadata.plugin_cache_breakpoints();
        ctx.canonical_model_id = if request_capability.provider() == cc_lb_domain::Provider::Gemini
        {
            std::mem::take(&mut cache_metadata.canonical_model_id)
        } else {
            path_model
                .as_deref()
                .map(|model| canonical_model_id(request_capability.provider(), model))
                .unwrap_or(&cache_metadata.canonical_model_id)
                .to_owned()
        };
        ctx.thread_id = cache_metadata.thread_id.clone();
        ctx.requested_service_tier = cache_metadata.requested_service_tier.clone();
        ctx.cache_pricing = cache_pricing_summary_for_model(
            cc_lb_pricing::global_catalog(),
            &ctx.canonical_model_id,
            ctx.requested_service_tier.as_deref(),
        );

        // Authentication was already recorded before the body-dependent exits
        // above (413 / invalid-JSON 400), so no `AuthCompleted` event here.
        let request_affinity_keys = match body_view
            .value()
            .map(|value| extract_request_affinity_keys(request_capability, value, &principal.id))
            .transpose()
        {
            Ok(keys) => {
                let mut keys = keys.unwrap_or_default();
                let path_key = path_affinity_key(request_capability, &ctx.path, &principal.id);
                if request_capability == ProviderCapability::OpenaiRealtimeHangup
                    && path_key.is_none()
                {
                    if let Some(o) = observer.as_ref() {
                        o.terminate_local(TerminalClassification::OPERATION_NOT_SUPPORTED);
                    }
                    return Ok(provider_error_response(
                        request_capability,
                        StatusCode::NOT_FOUND,
                        "realtime_call_not_found",
                        "No realtime call with this id belongs to this API key",
                        None,
                    ));
                }
                keys.extend(path_key);
                keys
            }
            Err(_) => {
                if let Some(o) = observer.as_ref() {
                    o.terminate_failure(InternalFailure {
                        status: StatusCode::BAD_REQUEST,
                        error_code: error_codes::INVALID_JSON,
                        error: InternalError {
                            stage: InternalErrorStage::Ingress,
                            kind: InternalErrorKind::InvalidInput,
                            message: Some(UPSTREAM_AFFINITY_KEY_LIMIT_MESSAGE.to_owned()),
                        },
                    });
                }
                return Ok(provider_error_response(
                    request_capability,
                    StatusCode::BAD_REQUEST,
                    "invalid_request_error",
                    UPSTREAM_AFFINITY_KEY_LIMIT_MESSAGE,
                    None,
                ));
            }
        };
        enum HttpUnknownReasoningRecovery {
            None,
            Unestablished {
                unknown_keys: Vec<UpstreamAffinityKey>,
            },
            PendingOnOwner {
                pending_reasoning_digests: HashSet<[u8; 32]>,
            },
        }
        let mut unknown_reasoning_recovery = HttpUnknownReasoningRecovery::None;
        let request_affinity = match self
            .resolve_request_affinities(&request_affinity_keys)
            .await
        {
            Ok(resolution) => {
                if let Some(ref resolved) = resolution
                    && self.config.unknown_openai_reasoning
                        == cc_lb_domain::UnknownOpenaiReasoningPolicy::Hybrid
                    && let Some(body_val) = body_view.value()
                {
                    let pending_reasoning_digests = collect_openai_pending_reasoning_digests(
                        request_capability,
                        body_val,
                        &resolved.pending_keys,
                    );
                    if !pending_reasoning_digests.is_empty() {
                        unknown_reasoning_recovery = HttpUnknownReasoningRecovery::PendingOnOwner {
                            pending_reasoning_digests,
                        };
                    }
                }
                resolution
            }
            // A response cc-lb never saw (or one past the affinity TTL) has no
            // recorded owner; the single-owner rule below still places it safely.
            Err(failure)
                if request_capability == ProviderCapability::OpenaiStoredResponse
                    && failure.message == UPSTREAM_AFFINITY_NOT_ESTABLISHED_MESSAGE =>
            {
                None
            }
            Err(failure)
                if failure.message == UPSTREAM_AFFINITY_NOT_ESTABLISHED_MESSAGE
                    && self.config.unknown_openai_reasoning
                        == cc_lb_domain::UnknownOpenaiReasoningPolicy::Hybrid
                    && body_view.value().is_some_and(|value| {
                        is_openai_unknown_reasoning_only(request_capability, value)
                    }) =>
            {
                unknown_reasoning_recovery = HttpUnknownReasoningRecovery::Unestablished {
                    unknown_keys: request_affinity_keys,
                };
                None
            }
            Err(failure) => {
                // An unknown or foreign call id owns nothing; never guess an account.
                let realtime_call_missing = request_capability
                    == ProviderCapability::OpenaiRealtimeHangup
                    && failure.message == UPSTREAM_AFFINITY_NOT_ESTABLISHED_MESSAGE;
                let status = if realtime_call_missing {
                    StatusCode::NOT_FOUND
                } else {
                    failure.status
                };
                if let Some(o) = observer.as_ref() {
                    o.terminate_failure(InternalFailure {
                        status,
                        error_code: error_codes::UPSTREAM_AFFINITY_UNAVAILABLE,
                        error: InternalError {
                            stage: InternalErrorStage::Storage,
                            kind: InternalErrorKind::Unavailable,
                            message: Some(failure.message.to_owned()),
                        },
                    });
                }
                if realtime_call_missing {
                    return Ok(provider_error_response(
                        request_capability,
                        StatusCode::NOT_FOUND,
                        "realtime_call_not_found",
                        "No realtime call with this id belongs to this API key",
                        None,
                    ));
                }
                return Ok(failure.response(request_capability));
            }
        };
        let keepalive_response_enabled =
            self.cache_keepalive.has_enqueuer() && cached.cache_keepalive().is_some();
        let track_keepalive_response = keepalive_response_enabled
            && (cache_metadata.cache_keepalive_surface.is_some()
                || !cache_metadata.bundles.official.cache_breakpoints.is_empty()
                || !cache_metadata.bundles.literal.cache_breakpoints.is_empty());

        let router_pipeline = cached.resolved_pipeline(None);
        if let Some(error) = router_pipeline.instantiation_error.as_deref() {
            let response = provider_error_response(
                request_capability,
                StatusCode::BAD_GATEWAY,
                "route_not_configured",
                "router pipeline is unavailable for this request",
                None,
            );
            if let Some(o) = observer.as_ref() {
                o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::RouteCompleted {
                    event_id: o.event_id().to_owned(),
                    result: Err(cc_lb_lifecycle::RouteFailure::RouterPipelineUnavailable),
                    routing_trace: None,
                });
                o.terminate_failure(InternalFailure {
                    status: StatusCode::BAD_GATEWAY,
                    error_code: error_codes::ROUTER_PIPELINE_UNAVAILABLE,
                    error: InternalError {
                        stage: InternalErrorStage::Router,
                        kind: InternalErrorKind::ConfigError,
                        message: Some(error.to_owned()),
                    },
                });
            }
            return Ok(response);
        }

        let route_start = Instant::now();
        // Shared-store warmth read: one bounded batch query scoped to this
        // request's candidate upstreams and prefix keys, awaited before the
        // synchronous routing region. A failure is reported and degrades to
        // neutral (no cache affinity) — it never fails the request.
        let warm_entries = fetch_prompt_cache_warm_entries_for_bundles(
            &view,
            &principal.id,
            request_kind,
            &ctx.canonical_model_id,
            &cache_metadata.bundles,
            &*self.clock,
        )
        .await;
        let native_cache_scores = fetch_provider_native_cache_scores(
            &view,
            &principal.id,
            request_kind,
            &ctx.canonical_model_id,
            cache_metadata.native_cache.as_ref(),
            &*self.clock,
        )
        .await;
        // Synchronous routing region: no .await is permitted while `route_guard` is held.
        let route_span = tracing::info_span!(
            "proxy.route",
            cc_lb.route.outcome = tracing::field::Empty,
            cc_lb.route.candidate_count = tracing::field::Empty,
            cc_lb.route.duration_ms = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
            error.type = tracing::field::Empty,
        );
        let route_guard = route_span.enter();
        let (mut candidates, mut selected_cache_matches) =
            build_candidates_with_native_cache_for_bundles(
                &view,
                &principal.id,
                request_kind,
                &ctx.cache_breakpoints,
                &warm_entries,
                Some(&cache_metadata.bundles),
                &native_cache_scores,
                &*self.clock,
            );
        if request_affinity.is_none() && !ctx.codex_backend {
            retain_api_key_candidates(&mut candidates, &ctx.compat_api_key_only_features);
            if request_kind == RequestKind::OpenaiAudioTranscriptions
                && ctx.openai_oauth_compat.is_some_and(|policy| {
                    policy.transcription == cc_lb_domain::EnabledOrDisabled::Disabled
                })
            {
                candidates
                    .retain(|candidate| candidate.kind != cc_lb_domain::UpstreamKind::OpenaiOauth);
            }
        }
        if ctx.codex_backend {
            candidates
                .retain(|candidate| candidate.kind == cc_lb_domain::UpstreamKind::OpenaiOauth);
        }
        if let Some(affinity) = request_affinity.as_ref() {
            candidates.retain(|candidate| candidate.upstream_id == affinity.target_upstream_id);
            selected_cache_matches
                .retain(|upstream_id, _| *upstream_id == affinity.target_upstream_id);
        }
        // Account-owned OpenAI resources (files, batches, stored responses with no
        // recorded owner, …) live in one API account. Route them only when exactly
        // one OpenAI API-key upstream can own them; never guess between accounts.
        let single_owner = request_affinity.is_none()
            && matches!(
                request_kind,
                RequestKind::OpenaiAccountOwned | RequestKind::OpenaiStoredResponse
            );
        if single_owner && candidates.is_empty() {
            if let Some(o) = observer.as_ref() {
                o.terminate_local(TerminalClassification::OPERATION_NOT_SUPPORTED);
            }
            return Ok(unsupported_operation_response(
                ctx.provider_scope,
                &ctx.method,
                &ctx.path,
            ));
        }
        if single_owner && candidates.len() > 1 {
            route_span.record("cc_lb.route.outcome", "error");
            route_span.record("otel.status_code", "ERROR");
            route_span.record("error.type", error_codes::UPSTREAM_AFFINITY_UNAVAILABLE);
            if let Some(o) = observer.as_ref() {
                o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::RouteCompleted {
                    event_id: o.event_id().to_owned(),
                    result: Err(cc_lb_lifecycle::RouteFailure::RouteNoUpstreamAfterFilter),
                    routing_trace: None,
                });
                o.terminate_failure(InternalFailure {
                    status: StatusCode::SERVICE_UNAVAILABLE,
                    error_code: error_codes::UPSTREAM_AFFINITY_UNAVAILABLE,
                    error: InternalError {
                        stage: InternalErrorStage::RouterFilter,
                        kind: InternalErrorKind::Unavailable,
                        message: Some(SINGLE_OWNER_AMBIGUOUS_MESSAGE.to_owned()),
                    },
                });
            }
            return Ok(provider_error_response(
                request_capability,
                StatusCode::SERVICE_UNAVAILABLE,
                error_codes::UPSTREAM_AFFINITY_UNAVAILABLE,
                SINGLE_OWNER_AMBIGUOUS_MESSAGE,
                None,
            ));
        }
        let mut pipeline_result = execute_filter_pipeline(
            router_pipeline.filters_for(request_kind.provider()),
            &ctx,
            &principal,
            candidates,
        );
        // Hard affinity: provider-owned state lives on exactly one upstream.
        // Filters may only narrow the candidate set; if they (or the eligible
        // candidate set itself) exclude the owner, the request cannot be
        // served anywhere else, so fail with an explicit affinity error rather
        // than a generic routing miss.
        if let Some(affinity) = request_affinity.as_ref() {
            pipeline_result
                .candidates
                .retain(|candidate| candidate.upstream_id == affinity.target_upstream_id);
        }
        let mut pending_request_keys = request_affinity
            .as_ref()
            .map(|affinity| affinity.pending_keys.clone())
            .unwrap_or_default();
        let mut single_unestablished_probe = false;
        if !pipeline_result.candidates.is_empty()
            && let HttpUnknownReasoningRecovery::Unestablished { unknown_keys } =
                &unknown_reasoning_recovery
        {
            if pipeline_result.candidates.len() > 1 {
                if let Some(stripped) = body_view
                    .value()
                    .and_then(|value| strip_openai_reasoning_encrypted_items(value, None))
                    && let Ok(bytes) = serde_json::to_vec(&stripped)
                {
                    ctx.body_bytes = Bytes::from(bytes);
                    rewrite_json_request_body_headers(
                        &mut ctx.downstream_headers,
                        ctx.body_bytes.len(),
                    );
                    body_view.parsed = Ok(Arc::new(stripped));
                }
                unknown_reasoning_recovery = HttpUnknownReasoningRecovery::None;
                pending_request_keys.clear();
            } else {
                pending_request_keys = unknown_keys.clone();
                single_unestablished_probe = true;
            }
        }
        if let Some(o) = observer.as_ref() {
            for error in pipeline_result.internal_errors.drain(..) {
                o.record_internal_error(error);
            }
        }
        let terminal_decision = self.select_terminal_upstream(
            router_pipeline.terminal.clone(),
            &pipeline_result.candidates,
        );
        route_span.record(
            "cc_lb.route.candidate_count",
            numeric_attribute(Some(pipeline_result.candidates.len() as u64)).as_value(),
        );
        if request_affinity.is_some() && pipeline_result.candidates.is_empty() {
            route_span.record("cc_lb.route.outcome", "error");
            route_span.record("otel.status_code", "ERROR");
            route_span.record("error.type", error_codes::UPSTREAM_AFFINITY_UNAVAILABLE);
            if let Some(o) = observer.as_ref() {
                o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::RouteCompleted {
                    event_id: o.event_id().to_owned(),
                    result: Err(cc_lb_lifecycle::RouteFailure::RouteNoUpstreamAfterFilter),
                    routing_trace: Some(pipeline_result.routing_trace(terminal_decision.clone())),
                });
                o.terminate_failure(InternalFailure {
                    status: StatusCode::SERVICE_UNAVAILABLE,
                    error_code: error_codes::UPSTREAM_AFFINITY_UNAVAILABLE,
                    error: InternalError {
                        stage: InternalErrorStage::RouterFilter,
                        kind: InternalErrorKind::Unavailable,
                        message: Some(UPSTREAM_AFFINITY_TARGET_INELIGIBLE_MESSAGE.to_owned()),
                    },
                });
            }
            return Ok(provider_error_response(
                request_capability,
                StatusCode::SERVICE_UNAVAILABLE,
                error_codes::UPSTREAM_AFFINITY_UNAVAILABLE,
                UPSTREAM_AFFINITY_TARGET_INELIGIBLE_MESSAGE,
                None,
            ));
        }
        if pipeline_result.candidates.is_empty() {
            route_span.record("cc_lb.route.outcome", "error");
            route_span.record("otel.status_code", "ERROR");
            route_span.record("error.type", error_codes::ROUTE_NO_UPSTREAM_AFTER_FILTER);
            let message = "no upstream candidates remain after routing filters";
            self.emit_routing_failure_event(
                observer.as_ref(),
                StatusCode::SERVICE_UNAVAILABLE,
                Some(pipeline_result.routing_trace(terminal_decision.clone())),
            );
            let response = provider_error_response(
                request_capability,
                StatusCode::SERVICE_UNAVAILABLE,
                "route_no_upstream_after_filter",
                message,
                None,
            );
            return Ok(response);
        }

        let routed_candidates =
            terminal_candidates(&pipeline_result.candidates, &terminal_decision);

        let resolved_upstream_id = routed_candidates
            .first()
            .expect("routed candidates are non-empty after terminal selection")
            .upstream_id;
        let Some(resolved_record) = view
            .upstreams_snapshot()
            .iter()
            .find(|record| record.id == resolved_upstream_id)
        else {
            route_span.record("cc_lb.route.outcome", "error");
            route_span.record("otel.status_code", "ERROR");
            route_span.record("error.type", error_codes::ROUTE_NOT_CONFIGURED);
            let response = provider_error_response(
                request_capability,
                StatusCode::BAD_GATEWAY,
                "route_not_configured",
                "no upstream route is configured for this request",
                None,
            );
            if let Some(o) = observer.as_ref() {
                o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::RouteCompleted {
                    event_id: o.event_id().to_owned(),
                    result: Err(cc_lb_lifecycle::RouteFailure::RouteNotConfigured),
                    routing_trace: None,
                });
                o.terminate_failure(InternalFailure {
                    status: StatusCode::BAD_GATEWAY,
                    error_code: error_codes::ROUTE_NOT_CONFIGURED,
                    error: InternalError {
                        stage: InternalErrorStage::Router,
                        kind: InternalErrorKind::ConfigError,
                        message: Some(
                            "router selected an upstream missing from the dynamic view".to_owned(),
                        ),
                    },
                });
            }
            return Ok(response);
        };
        handle_span.record("cc_lb.upstream.name", resolved_record.name.as_str());
        handle_span.record(
            "cc_lb.upstream.id",
            tracing::field::display(resolved_upstream_id),
        );
        let (route, raw_passthrough_base_url) = match route_for_record(resolved_record, cached) {
            Ok(resolved) => resolved,
            Err(reason) => {
                route_span.record("cc_lb.route.outcome", "error");
                route_span.record("otel.status_code", "ERROR");
                route_span.record("error.type", error_codes::ROUTE_NOT_CONFIGURED);
                let response = provider_error_response(
                    request_capability,
                    StatusCode::BAD_GATEWAY,
                    "route_not_configured",
                    "no upstream route is configured for this request",
                    None,
                );
                if let Some(o) = observer.as_ref() {
                    o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::RouteCompleted {
                        event_id: o.event_id().to_owned(),
                        result: Err(cc_lb_lifecycle::RouteFailure::RouteNotConfigured),
                        routing_trace: None,
                    });
                    o.terminate_failure(InternalFailure {
                        status: StatusCode::BAD_GATEWAY,
                        error_code: error_codes::ROUTE_NOT_CONFIGURED,
                        error: InternalError {
                            stage: InternalErrorStage::Router,
                            kind: InternalErrorKind::ConfigError,
                            message: Some(reason),
                        },
                    });
                }
                return Ok(response);
            }
        };
        drop(route_guard);
        let route_ms = duration_to_ms(route_start.elapsed());
        route_span.record("cc_lb.route.outcome", "success");
        route_span.record(
            "cc_lb.route.duration_ms",
            numeric_attribute(Some(route_ms)).as_value(),
        );
        let mut routed = RoutedUpstream::new(
            resolved_record,
            route,
            raw_passthrough_base_url,
            &pipeline_result,
            terminal_decision,
            selected_cache_matches.remove(&resolved_upstream_id),
            route_ms,
        )
        .with_cache_bundle(cache_metadata.bundles.for_upstream(resolved_record).clone());

        emit_route_completed(observer.as_ref(), &routed, request_model.as_deref());
        let chat_fanout_n = (request_capability == ProviderCapability::OpenaiChatCompletions)
            .then(|| {
                body_view
                    .value()
                    .and_then(|value| value.get("n"))
                    .and_then(Value::as_u64)
                    .filter(|n| *n > 1)
                    .map(|n| n.min(8) as usize)
            })
            .flatten();

        let mut active_limit: Option<ActiveLimit> = None;
        let mut reserved: Option<Reserved> = None;

        // Bounded cross-upstream failover. Only failures that happen before
        // the upstream processed the request (credential preparation, signing,
        // 401, 429) move to another upstream; nothing has reached the client
        // yet. Keep an unchanged estimate's hold, or refund it before reserving
        // the next serving bundle's estimate.
        let mut failover = FailoverRouter {
            view: &view,
            cached,
            router_pipeline: &router_pipeline,
            ctx: &ctx,
            principal: &principal,
            warm_entries: &warm_entries,
            cache_bundles: &cache_metadata.bundles,
            request_kind,
            compat_api_key_only_features: &ctx.compat_api_key_only_features,
            feature_pinned_api_key: !ctx.compat_api_key_only_features.is_empty()
                && resolved_record.kind == StorageUpstreamKind::OpenaiApiKey,
            transcription_oauth_disabled: request_kind == RequestKind::OpenaiAudioTranscriptions
                && ctx.openai_oauth_compat.is_some_and(|policy| {
                    policy.transcription == cc_lb_domain::EnabledOrDisabled::Disabled
                }),
            observer: observer.as_ref(),
            handle_span: &handle_span,
            enabled: request_affinity.is_none() && !single_owner && !single_unestablished_probe,
            excluded: Vec::new(),
            vertex_rate_limited_scopes: HashSet::new(),
            upstream_attempts: 1,
        };
        let mut attempt_num: u32 = 0;
        let mut first_dispatch: Option<(Instant, u64)> = None;
        let mut attempt_timings = AttemptTimings::default();
        let mut keepalive_shaped_body = None;
        let mut compat_response_plan: Option<CompatResponsePlan>;
        let keepalive_request_started = Arc::new(tokio::sync::OnceCell::new());
        let mut fanout_responses: Option<Vec<Response<Body>>>;
        let affinity_owner_epoch_required = request_capability
            == ProviderCapability::GeminiCachedContentsCreate
            || (request_capability.provider() == cc_lb_domain::Provider::Gemini
                && request_affinity.is_some());
        let mut routed_owner_epoch;
        let (mut response, dispatch_started, dispatch_unix_secs, signer) = 'attempts: loop {
            fanout_responses = None;
            // A failed-over upstream is announced before its signer is built,
            // so a terminal failure here never reports the previous upstream's
            // response status or timings under the new route.
            let failover_entry = failover.upstream_attempts > 1;
            if failover_entry {
                announce_attempt(
                    observer.as_ref(),
                    &mut attempt_num,
                    routed.route.upstream_id,
                    &mut attempt_timings,
                    first_dispatch.map(|(dispatch_started, _)| dispatch_started),
                );
                emit_route_completed(observer.as_ref(), &routed, request_model.as_deref());
            }

            routed_owner_epoch = if affinity_owner_epoch_required {
                let owner_epoch = match self.upstream_affinity_store.as_ref() {
                    None => None,
                    Some(store) => store
                        .read_upstream_owner_epoch(routed.route.upstream_id, routed.record.revision)
                        .await
                        .ok()
                        .flatten(),
                };
                let owner_epoch = owner_epoch.filter(|epoch| {
                    request_affinity
                        .as_ref()
                        .is_none_or(|affinity| affinity.owner_epoch == *epoch)
                });
                if owner_epoch.is_none() {
                    if let Some(o) = observer.as_ref() {
                        o.terminate_failure(InternalFailure {
                            status: StatusCode::SERVICE_UNAVAILABLE,
                            error_code: error_codes::UPSTREAM_AFFINITY_UNAVAILABLE,
                            error: InternalError {
                                stage: InternalErrorStage::Storage,
                                kind: InternalErrorKind::Unavailable,
                                message: Some(UPSTREAM_AFFINITY_UNAVAILABLE_MESSAGE.to_owned()),
                            },
                        });
                    }
                    return Ok(provider_error_response(
                        request_capability,
                        StatusCode::SERVICE_UNAVAILABLE,
                        error_codes::UPSTREAM_AFFINITY_UNAVAILABLE,
                        UPSTREAM_AFFINITY_UNAVAILABLE_MESSAGE,
                        None,
                    ));
                }
                owner_epoch
            } else {
                Some(0)
            };
            let replacement_request = active_limit.as_ref().and_then(|limit| {
                let capability =
                    classify_provider_capability(&ctx.method, &ctx.path, ctx.provider_scope);
                let request = body_view.limit_request(
                    &ctx.canonical_model_id,
                    capability,
                    &routed.cache_bundle.cache_breakpoints,
                    use_image_rates_for_admission(capability, &ctx.path),
                );
                let multiplier = admission_fanout_multiplier(
                    routed.record.kind,
                    capability,
                    request.fanout_multiplier,
                );
                let tier = if ctx.codex_backend {
                    request.service_tier.clone()
                } else {
                    compat_service_tier_for_upstream(
                        capability.unwrap_or(ProviderCapability::OpenaiResponses),
                        routed.record.kind,
                        request.service_tier.as_deref(),
                        ctx.openai_oauth_compat.as_ref().is_some_and(|policy| {
                            policy.service_tier == cc_lb_domain::CoerceOrReject::Coerce
                        }),
                    )
                };
                (request != limit.request
                    || multiplier != limit.fanout_multiplier
                    || tier != limit.service_tier)
                    .then_some(request)
            });
            if !failover_entry || replacement_request.is_some() {
                // Every prior attempt scope has ended. Its pre-processing
                // failure is unbilled, so RAII refunds that hold in full before
                // the replacement is checked; never hold both estimates.
                drop(reserved.take());
                if let Some(o) = observer.as_ref() {
                    o.clear_limit_reservation();
                }
                let limit_reserve_start = Instant::now();
                active_limit = match self
                    .reserve_limit(
                        &principal_view,
                        &ctx,
                        &principal,
                        routed.record,
                        success,
                        || {
                            replacement_request.unwrap_or_else(|| {
                                body_view.limit_request(
                                    &ctx.canonical_model_id,
                                    classify_provider_capability(
                                        &ctx.method,
                                        &ctx.path,
                                        ctx.provider_scope,
                                    ),
                                    &routed.cache_bundle.cache_breakpoints,
                                    use_image_rates_for_admission(
                                        classify_provider_capability(
                                            &ctx.method,
                                            &ctx.path,
                                            ctx.provider_scope,
                                        ),
                                        &ctx.path,
                                    ),
                                )
                            })
                        },
                    )
                    .await
                {
                    Ok(active_limit) => active_limit,
                    Err(LimitRejectionErr { response, info }) => {
                        let status = response.status();
                        if let Some(o) = observer.as_ref() {
                            let proxy_setup_ms = duration_to_ms(started.elapsed());
                            o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::LimitDecision {
                                event_id: o.event_id().to_owned(),
                                decision: cc_lb_lifecycle::LimitDecisionKind::Rejected {
                                    reason: info.reason_label.clone(),
                                    subject: info.subject,
                                    request_summary: info.request_summary,
                                    route_summary: info.route_summary,
                                    limit_violation: info.limit_violation,
                                },
                            });
                            o.set_termination_timings(Some(proxy_setup_ms), None, None);
                            o.terminate_failure(InternalFailure {
                                status,
                                error_code: error_codes::LIMIT_REJECTED,
                                error: InternalError {
                                    stage: InternalErrorStage::Router,
                                    kind: InternalErrorKind::Unavailable,
                                    message: Some(info.reason_label),
                                },
                            });
                        }
                        return Ok(response);
                    }
                };
                let limit_reserve_ms = duration_to_ms(limit_reserve_start.elapsed());
                let reservation_id = active_limit
                    .as_ref()
                    .and_then(|limit| limit.reservation.as_ref())
                    .map(|reservation| reservation.id().to_owned())
                    .unwrap_or_default();
                if let Some(o) = observer.as_ref() {
                    if let Some(engine) = self.limit_engine.as_ref()
                        && !reservation_id.is_empty()
                    {
                        o.attach_limit_reservation(Arc::clone(engine), reservation_id.clone());
                    }
                    o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::LimitDecision {
                        event_id: o.event_id().to_owned(),
                        decision: cc_lb_lifecycle::LimitDecisionKind::Reserved {
                            reservation_id,
                            amount: active_limit.as_ref().map_or(0, |limit| {
                                limit.request.max_tokens.unwrap_or(0).max(0) as u64
                            }),
                            limit_reserve_ms: Some(limit_reserve_ms),
                        },
                    });
                }
                reserved = active_limit
                    .as_mut()
                    .and_then(|limit| limit.reservation.take())
                    .map(|reservation| {
                        AttemptIntent::from_reservation(reservation).into_reserved()
                    });
            }
            let signer_factory = view
                .signer_factory
                .with_router_choice(routed.record.name.clone());
            let prepare_signer_started = Instant::now();
            let signer_result = signer_factory
                .build(&routed.route.upstream)
                .instrument(tracing::info_span!("proxy.prepare_signer"))
                .await;
            setup_timings.prepare_signer_ms =
                Some(prepare_signer_started.elapsed().as_secs_f64() * 1_000.0);
            store_setup_timings(observer.as_ref(), setup_timings);
            let mut signer = match signer_result {
                Ok(signer) => signer,
                Err(source) => {
                    if let Some(next) = failover.next(self, &routed, FailoverReason::SignerBuild) {
                        routed = next;
                        continue;
                    }
                    let mut response = provider_error_response(
                        request_capability,
                        StatusCode::BAD_GATEWAY,
                        "api_error",
                        "failed to prepare upstream credentials",
                        None,
                    );
                    self.attach_limit_headers(
                        &mut response,
                        active_limit.as_ref(),
                        request_capability.provider(),
                    );
                    if let Some(o) = observer.as_ref() {
                        o.terminate_failure(InternalFailure {
                            status: StatusCode::BAD_GATEWAY,
                            error_code: error_codes::SIGNER_FAILED,
                            error: InternalError {
                                stage: InternalErrorStage::Signer,
                                kind: signer_error_kind(&source),
                                message: Some(source.to_string()),
                            },
                        });
                    }
                    return Ok(response);
                }
            };
            cache_metadata.cache_breakpoints = routed.cache_bundle.request_breakpoints.clone();
            cache_metadata.cache_prefix_hash = routed.cache_bundle.cache_prefix_hash.clone();

            let (dispatch_started, dispatch_unix_secs) = *first_dispatch
                .get_or_insert_with(|| (Instant::now(), unix_now_ms(&*self.clock) / 1000));
            if !failover_entry {
                announce_attempt(
                    observer.as_ref(),
                    &mut attempt_num,
                    routed.route.upstream_id,
                    &mut attempt_timings,
                    Some(dispatch_started),
                );
            }
            let mut response = if let Some(count) =
                chat_fanout_n.filter(|_| routed.record.kind == StorageUpstreamKind::OpenaiOauth)
            {
                let (results, first_timings, shaped_body) = self
                    .run_upstream_attempts_concurrently(
                        count,
                        ConcurrentAttemptInputs {
                            authenticated: auth,
                            reserved: reserved.as_ref(),
                            ctx: &ctx,
                            principal: &principal,
                            routed: &routed,
                            signer: signer.clone(),
                            observer: observer.as_ref(),
                            cache_metadata: &cache_metadata,
                            shared_request_started: keepalive_request_started.clone(),
                        },
                    )
                    .await;
                attempt_timings = first_timings;
                keepalive_shaped_body = shaped_body;
                let mut attempts = Vec::with_capacity(results.len());
                for result in results {
                    match result {
                        Ok(attempt) => attempts.push(attempt),
                        Err(failure) => {
                            if matches!(failure, AttemptFailure::Sign(_))
                                && let Some(next) =
                                    failover.next(self, &routed, FailoverReason::Sign)
                            {
                                routed = next;
                                continue 'attempts;
                            }
                            return Ok(self.relay_attempt_failure(
                                failure,
                                request_capability,
                                active_limit.as_ref(),
                                observer.as_ref(),
                                &attempt_timings,
                                request_capability.provider(),
                            ));
                        }
                    }
                }
                let (first, rest) = reduce_fanout(attempts);
                let AttemptResponse {
                    response: first_response,
                    compat_response_plan: first_plan,
                } = first;
                compat_response_plan = first_plan;
                fanout_responses = rest;
                first_response
            } else {
                match self
                    .run_upstream_attempt(
                        auth,
                        reserved.as_ref(),
                        &ctx,
                        &principal,
                        &routed,
                        signer.clone(),
                        observer.as_ref(),
                        &mut attempt_timings,
                        &mut keepalive_shaped_body,
                        &cache_metadata,
                        &keepalive_request_started,
                    )
                    .await
                {
                    Ok(attempt) => {
                        compat_response_plan = attempt.compat_response_plan;
                        attempt.response
                    }
                    Err(failure) => {
                        if matches!(failure, AttemptFailure::Sign(_))
                            && let Some(next) = failover.next(self, &routed, FailoverReason::Sign)
                        {
                            routed = next;
                            continue;
                        }
                        return Ok(self.relay_attempt_failure(
                            failure,
                            request_capability,
                            active_limit.as_ref(),
                            observer.as_ref(),
                            &attempt_timings,
                            request_capability.provider(),
                        ));
                    }
                }
            };

            if response.status() == StatusCode::UNAUTHORIZED {
                let unauthorized = collect_error_response(response)
                    .instrument(tracing::info_span!("proxy.collect_upstream_error_body"))
                    .await;
                let err = UpstreamError::Unauthorized {
                    status: StatusCode::UNAUTHORIZED,
                    // Body-sensitive signers see the upstream body when it was
                    // fully read inside the collection bound; a stalled or
                    // oversized body surfaces as `None` instead of blocking
                    // the refresh/failover decision.
                    body: unauthorized.body.clone(),
                };
                let RetryDecision::Refresh { new_signer } = signer
                    .on_unauthorized(&err)
                    .instrument(tracing::info_span!("proxy.refresh_signer"))
                    .await
                else {
                    if let Some(next) = failover.next(self, &routed, FailoverReason::Unauthorized) {
                        routed = next;
                        continue;
                    }
                    if unauthorized.body.is_none() {
                        // A proxy-generated body is not an upstream response:
                        // do not pass it through the custom response hook.
                        let mut response =
                            response_from_collected(unauthorized, request_capability);
                        self.attach_limit_headers(
                            &mut response,
                            active_limit.as_ref(),
                            request_capability.provider(),
                        );
                        if let Some(o) = observer.as_ref() {
                            o.set_upstream_error(response.status(), UpstreamErrorCode::Upstream5xx);
                            o.finish();
                        }
                        return Ok(response);
                    }
                    break (
                        response_from_collected(unauthorized, request_capability),
                        dispatch_started,
                        dispatch_unix_secs,
                        signer,
                    );
                };
                announce_attempt(
                    observer.as_ref(),
                    &mut attempt_num,
                    routed.route.upstream_id,
                    &mut attempt_timings,
                    Some(dispatch_started),
                );
                // Only subscription routes emulate `n`; native routes pass it through.
                response = if let Some(count) =
                    chat_fanout_n.filter(|_| routed.record.kind == StorageUpstreamKind::OpenaiOauth)
                {
                    let (results, first_timings, shaped_body) = self
                        .run_upstream_attempts_concurrently(
                            count,
                            ConcurrentAttemptInputs {
                                authenticated: auth,
                                reserved: reserved.as_ref(),
                                ctx: &ctx,
                                principal: &principal,
                                routed: &routed,
                                signer: new_signer.clone(),
                                observer: observer.as_ref(),
                                cache_metadata: &cache_metadata,
                                shared_request_started: keepalive_request_started.clone(),
                            },
                        )
                        .await;
                    attempt_timings = first_timings;
                    keepalive_shaped_body = shaped_body;
                    let mut attempts = Vec::with_capacity(results.len());
                    for result in results {
                        match result {
                            Ok(attempt) => attempts.push(attempt),
                            Err(failure) => {
                                if matches!(failure, AttemptFailure::Sign(_))
                                    && let Some(next) =
                                        failover.next(self, &routed, FailoverReason::Sign)
                                {
                                    routed = next;
                                    continue 'attempts;
                                }
                                return Ok(self.relay_attempt_failure(
                                    failure,
                                    request_capability,
                                    active_limit.as_ref(),
                                    observer.as_ref(),
                                    &attempt_timings,
                                    request_capability.provider(),
                                ));
                            }
                        }
                    }
                    let (first, rest) = reduce_fanout(attempts);
                    let AttemptResponse {
                        response: first_response,
                        compat_response_plan: first_plan,
                    } = first;
                    compat_response_plan = first_plan;
                    fanout_responses = rest;
                    first_response
                } else {
                    match self
                        .run_upstream_attempt(
                            auth,
                            reserved.as_ref(),
                            &ctx,
                            &principal,
                            &routed,
                            new_signer.clone(),
                            observer.as_ref(),
                            &mut attempt_timings,
                            &mut keepalive_shaped_body,
                            &cache_metadata,
                            &keepalive_request_started,
                        )
                        .await
                    {
                        Ok(attempt) => {
                            compat_response_plan = attempt.compat_response_plan;
                            attempt.response
                        }
                        Err(failure) => {
                            if matches!(failure, AttemptFailure::Sign(_))
                                && let Some(next) =
                                    failover.next(self, &routed, FailoverReason::Sign)
                            {
                                routed = next;
                                continue;
                            }
                            return Ok(self.relay_attempt_failure(
                                failure,
                                request_capability,
                                active_limit.as_ref(),
                                observer.as_ref(),
                                &attempt_timings,
                                request_capability.provider(),
                            ));
                        }
                    }
                };
                // Everything the refreshed attempt returns is scrubbed with
                // the signer that produced it.
                signer = new_signer;
                if response.status() == StatusCode::UNAUTHORIZED {
                    let retry_err = UpstreamError::Unauthorized {
                        status: StatusCode::UNAUTHORIZED,
                        body: None,
                    };
                    signer
                        .on_retry_unauthorized(&retry_err)
                        .instrument(tracing::info_span!("proxy.record_retry_unauthorized"))
                        .await;
                }
                if response.status() == StatusCode::UNAUTHORIZED
                    && let Some(next) = failover.next(self, &routed, FailoverReason::Unauthorized)
                {
                    // Never read a discarded body: its size and pace are
                    // upstream-controlled. Dropping it closes the connection.
                    drop(response);
                    routed = next;
                    continue;
                }
                if response.status() == StatusCode::UNAUTHORIZED {
                    // A refreshed attempt has the same terminal-body contract
                    // as the original 401: never transform a generated error
                    // or forward a body that could not be collected completely.
                    let unauthorized = collect_error_response(response).await;
                    if unauthorized.body.is_none() {
                        let mut response =
                            response_from_collected(unauthorized, request_capability);
                        self.attach_limit_headers(
                            &mut response,
                            active_limit.as_ref(),
                            request_capability.provider(),
                        );
                        if let Some(o) = observer.as_ref() {
                            o.set_upstream_error(response.status(), UpstreamErrorCode::Upstream5xx);
                            o.finish();
                        }
                        return Ok(response);
                    }
                    response = response_from_collected(unauthorized, request_capability);
                }
            }

            if response.status() == StatusCode::TOO_MANY_REQUESTS
                && let Some(next) = failover.next(self, &routed, FailoverReason::RateLimited)
            {
                // Never read a discarded body: its size and pace are
                // upstream-controlled. Dropping it closes the connection.
                drop(response);
                routed = next;
                continue;
            }
            break (response, dispatch_started, dispatch_unix_secs, signer);
        };
        drop(failover);
        if response.status() == StatusCode::BAD_REQUEST
            && !matches!(
                unknown_reasoning_recovery,
                HttpUnknownReasoningRecovery::None
            )
        {
            let collected = collect_error_response(response)
                .instrument(tracing::info_span!("proxy.collect_upstream_error_body"))
                .await;
            let retry_stripped = if collected.body.as_deref().is_some_and(|body| {
                is_openai_invalid_encrypted_content_http_error(
                    collected.status,
                    &collected.headers,
                    body,
                )
            }) {
                let only_digests = match &unknown_reasoning_recovery {
                    HttpUnknownReasoningRecovery::PendingOnOwner {
                        pending_reasoning_digests,
                    } => Some(pending_reasoning_digests),
                    HttpUnknownReasoningRecovery::Unestablished { .. }
                    | HttpUnknownReasoningRecovery::None => None,
                };
                body_view
                    .value()
                    .and_then(|value| strip_openai_reasoning_encrypted_items(value, only_digests))
                    .and_then(|stripped| {
                        serde_json::to_vec(&stripped)
                            .ok()
                            .map(|bytes| (stripped, Bytes::from(bytes)))
                    })
            } else {
                None
            };
            if let Some((stripped, bytes)) = retry_stripped {
                ctx.body_bytes = bytes;
                rewrite_json_request_body_headers(
                    &mut ctx.downstream_headers,
                    ctx.body_bytes.len(),
                );
                body_view.parsed = Ok(Arc::new(stripped));
                match &unknown_reasoning_recovery {
                    HttpUnknownReasoningRecovery::Unestablished { .. } => {
                        pending_request_keys.clear();
                    }
                    HttpUnknownReasoningRecovery::PendingOnOwner {
                        pending_reasoning_digests,
                    } => {
                        pending_request_keys.retain(|key| {
                            !(key.provider == "openai"
                                && key.kind == UpstreamAffinityKind::OpenaiResponseId
                                && pending_reasoning_digests.contains(&key.value_sha256))
                        });
                    }
                    HttpUnknownReasoningRecovery::None => {}
                }
                keepalive_shaped_body = None;
                announce_attempt(
                    observer.as_ref(),
                    &mut attempt_num,
                    routed.route.upstream_id,
                    &mut attempt_timings,
                    Some(dispatch_started),
                );
                response = match self
                    .run_upstream_attempt(
                        auth,
                        reserved.as_ref(),
                        &ctx,
                        &principal,
                        &routed,
                        signer.clone(),
                        observer.as_ref(),
                        &mut attempt_timings,
                        &mut keepalive_shaped_body,
                        &cache_metadata,
                        &keepalive_request_started,
                    )
                    .await
                {
                    Ok(attempt) => {
                        compat_response_plan = attempt.compat_response_plan;
                        attempt.response
                    }
                    Err(failure) => {
                        return Ok(self.relay_attempt_failure(
                            failure,
                            request_capability,
                            active_limit.as_ref(),
                            observer.as_ref(),
                            &attempt_timings,
                            request_capability.provider(),
                        ));
                    }
                };
            } else {
                response = response_from_collected(collected, request_capability);
            }
        }
        let mut precomputed_usage: Option<UsageCounts> = None;
        let chat_fanout_plan = compat_response_plan.as_ref().and_then(|plan| {
            let CompatResponseKind::Chat { plan } = &plan.kind else {
                return None;
            };
            (response.status().is_success()
                && routed.record.kind == StorageUpstreamKind::OpenaiOauth
                && plan.n > 1)
                .then_some(plan.clone())
        });
        if let Some(plan) = chat_fanout_plan {
            // Every branch below installs the merged or failure response.
            let (mut response_parts, response_body) =
                std::mem::replace(&mut response, Response::new(Body::empty())).into_parts();
            let mut bodies = Vec::with_capacity(plan.n as usize);
            match compat_response::collect_body(
                response_body,
                decompression_output_budget_bytes(self.config.messages_body_cap_bytes),
            )
            .await
            {
                Ok(body) => {
                    let body_refs = [body.as_ref()];
                    precomputed_usage =
                        crate::openai_compat::chat::responses_usage_from_sse_bodies(&body_refs)
                            .map(|usage| responses_usage_value_to_counts(&usage));
                    bodies.push(body);
                }
                Err(_) => {
                    response = provider_error_response(
                        request_capability,
                        StatusCode::BAD_GATEWAY,
                        "server_error",
                        "upstream response exceeded the compatibility buffer",
                        None,
                    );
                    compat_response_plan = None;
                }
            }
            if compat_response_plan.is_some() {
                let mut fanout_body_results = fanout_responses.take().map(|responses| async {
                    Self::collect_response_bodies_concurrently(
                        responses,
                        decompression_output_budget_bytes(self.config.messages_body_cap_bytes),
                    )
                    .await
                });
                let mut fanout_body_results = match fanout_body_results.take() {
                    Some(future) => Some(future.await),
                    None => None,
                };
                for _ in 1..plan.n {
                    let (status, headers, body_result) =
                        if let Some(results) = fanout_body_results.as_mut() {
                            results.remove(0)
                        } else {
                            let attempt_response = match self
                                .run_upstream_attempt(
                                    auth,
                                    reserved.as_ref(),
                                    &ctx,
                                    &principal,
                                    &routed,
                                    signer.clone(),
                                    observer.as_ref(),
                                    &mut attempt_timings,
                                    &mut keepalive_shaped_body,
                                    &cache_metadata,
                                    &keepalive_request_started,
                                )
                                .await
                            {
                                Ok(attempt) => attempt.response,
                                Err(failure) => {
                                    if let Some(usage) = precomputed_usage.as_ref()
                                        && let Some(observer) = observer.as_ref()
                                    {
                                        observer.set_usage_counts(usage, false);
                                    }
                                    return Ok(self.relay_attempt_failure(
                                        failure,
                                        request_capability,
                                        active_limit.as_ref(),
                                        observer.as_ref(),
                                        &attempt_timings,
                                        request_capability.provider(),
                                    ));
                                }
                            };
                            let (parts, body) = attempt_response.into_parts();
                            (
                                parts.status,
                                parts.headers,
                                compat_response::collect_body(
                                    body,
                                    decompression_output_budget_bytes(
                                        self.config.messages_body_cap_bytes,
                                    ),
                                )
                                .await,
                            )
                        };
                    let body_failed = body_result.is_err();
                    let body = body_result.unwrap_or_default();
                    bodies.push(body.clone());
                    let body_refs = bodies.iter().map(|body| body.as_ref()).collect::<Vec<_>>();
                    precomputed_usage =
                        crate::openai_compat::chat::responses_usage_from_sse_bodies(&body_refs)
                            .map(|usage| responses_usage_value_to_counts(&usage));
                    if !status.is_success() {
                        let converted = match compat_response_plan.as_ref() {
                            Some(error_plan) => compat_response::convert_buffered_plan(
                                error_plan,
                                status,
                                headers,
                                body,
                                &|_| true,
                            ),
                            None => Err(compat_response::CollectError::BodyRead),
                        };
                        response = converted.unwrap_or_else(|_| {
                            provider_error_response(
                                request_capability,
                                StatusCode::BAD_GATEWAY,
                                "server_error",
                                "upstream chat fan-out attempt failed",
                                None,
                            )
                        });
                        compat_response_plan = None;
                        break;
                    }
                    if body_failed {
                        response = provider_error_response(
                            request_capability,
                            StatusCode::BAD_GATEWAY,
                            "server_error",
                            "upstream response exceeded the compatibility buffer",
                            None,
                        );
                        compat_response_plan = None;
                        break;
                    }
                }
            }
            if compat_response_plan.is_some() {
                let body_refs = bodies.iter().map(|body| body.as_ref()).collect::<Vec<_>>();
                if let Some(raw_usage) =
                    crate::openai_compat::chat::responses_usage_from_sse_bodies(&body_refs)
                {
                    precomputed_usage = Some(responses_usage_value_to_counts(&raw_usage));
                }
                match crate::openai_compat::chat::chat_completions_from_sse_bodies(
                    &plan, &body_refs,
                ) {
                    Ok(body) => {
                        response_parts.status = StatusCode::OK;
                        response_parts.headers.remove(CONTENT_LENGTH);
                        response_parts
                            .headers
                            .remove(http::header::CONTENT_ENCODING);
                        response_parts.headers.insert(
                            CONTENT_TYPE,
                            HeaderValue::from_static(if plan.stream {
                                "text/event-stream"
                            } else {
                                "application/json"
                            }),
                        );
                        response = Response::from_parts(response_parts, Body::from(body));
                        compat_response_plan = None;
                    }
                    Err(crate::openai_compat::chat::ChatSseMergeError::Failed(event)) => {
                        let mut aggregator =
                            crate::openai_compat::response::SseAggregator::new(None);
                        aggregator.push(&event);
                        if let crate::openai_compat::response::AggregateOutcome::Error {
                            status,
                            body,
                        } = aggregator.finish()
                        {
                            response_parts.status =
                                StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
                            response_parts.headers.remove(CONTENT_LENGTH);
                            response_parts
                                .headers
                                .remove(http::header::CONTENT_ENCODING);
                            response_parts
                                .headers
                                .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
                            response = Response::from_parts(response_parts, Body::from(body));
                        } else {
                            response = provider_error_response(
                                request_capability,
                                StatusCode::BAD_GATEWAY,
                                "server_error",
                                "upstream chat fan-out response failed",
                                None,
                            );
                        }
                        compat_response_plan = None;
                    }
                    Err(crate::openai_compat::chat::ChatSseMergeError::Truncated) => {
                        response = provider_error_response(
                            request_capability,
                            StatusCode::BAD_GATEWAY,
                            "server_error",
                            "upstream chat fan-out response was incomplete",
                            None,
                        );
                        compat_response_plan = None;
                    }
                }
            }
        }
        if compat_response_plan
            .as_ref()
            .is_some_and(|plan| plan.truncation_emulate)
            && routed.record.kind == StorageUpstreamKind::OpenaiOauth
        {
            const MAX_TRUNCATION_RETRIES: usize = 3;
            let initial = compat_response::inspect_truncation_response(
                response,
                decompression_output_budget_bytes(self.config.messages_body_cap_bytes),
            )
            .await;
            let (mut inspected, mut overflow) = match initial {
                Ok(result) => result,
                Err(error) => {
                    let failed = provider_error_response(
                        request_capability,
                        StatusCode::BAD_GATEWAY,
                        "server_error",
                        error.public_message(),
                        None,
                    );
                    (failed, false)
                }
            };
            let mut retry_body = body_view.value().cloned();
            for _ in 0..MAX_TRUNCATION_RETRIES {
                if !overflow {
                    break;
                }
                let Some(current) = retry_body.as_ref() else {
                    break;
                };
                let Some(next) = crate::openai_compat::request::trim_oldest_input(current) else {
                    break;
                };
                let body = match serde_json::to_vec(&next) {
                    Ok(body) => Bytes::from(body),
                    Err(_) => break,
                };
                let mut retry_ctx = ctx.clone();
                retry_ctx.body_bytes = body;
                let retry_attempt = match self
                    .run_upstream_attempt(
                        auth,
                        reserved.as_ref(),
                        &retry_ctx,
                        &principal,
                        &routed,
                        signer.clone(),
                        observer.as_ref(),
                        &mut attempt_timings,
                        &mut keepalive_shaped_body,
                        &cache_metadata,
                        &keepalive_request_started,
                    )
                    .await
                {
                    Ok(attempt) => attempt,
                    Err(_) => break,
                };
                compat_response_plan = retry_attempt.compat_response_plan;
                let retry_response = retry_attempt.response;
                retry_body = Some(next);
                (inspected, overflow) = match compat_response::inspect_truncation_response(
                    retry_response,
                    decompression_output_budget_bytes(self.config.messages_body_cap_bytes),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => break,
                };
            }
            response = inspected;
        }

        let proxy_setup_ms = duration_to_ms(dispatch_started.saturating_duration_since(started));
        let resolved_upstream_id = routed.route.upstream_id;
        let prompt_cache_observation_context = prompt_cache_observation_context(
            &view,
            resolved_upstream_id,
            &ctx.canonical_model_id,
            &routed.cache_bundle.cache_breakpoints,
            &routed.cache_bundle.cacheable_breakpoint_prefix_keys,
            routed.selected_cache_match.take(),
            cache_metadata.native_cache.clone(),
        );
        cache_metadata.cache_control_message_indices = routed
            .cache_bundle
            .request_breakpoints
            .iter()
            .filter_map(|breakpoint| breakpoint.message_index)
            .collect();
        cache_metadata.cache_control_message_indices.sort_unstable();
        cache_metadata.cache_control_message_indices.dedup();
        let status = response.status();
        let native_sse_framing_error =
            gemini_native::rejects_json_success(&ctx, routed.record.kind, &response);
        // Renewal capture is optional; ownership acquired at dispatch is not.
        // In particular, shaping fallback or a dynamic config change can leave
        // an owner even when the initial response-tracking decision was false.
        let compaction_response_enabled = keepalive_response_enabled && !track_keepalive_response;
        let keepalive_shaped_body = keepalive_shaped_body.filter(|_| track_keepalive_response);
        let (request_dispatched_at, request_start_order) = keepalive_request_started
            .get()
            .map(|(instant, order)| (*instant, order.clone()))
            .unwrap_or((dispatch_started, None));
        let keepalive_completion = (request_start_order.is_some()
            || keepalive_shaped_body.is_some()
            || compaction_response_enabled)
            .then(|| {
                let context = crate::cache_keepalive::LifecycleKeepaliveContext::new(
                    principal.clone(),
                    request_dispatched_at,
                    request_start_order,
                );
                if let Some(shaped_body) = keepalive_shaped_body {
                    let mut metadata = cache_metadata.clone();
                    if track_keepalive_response {
                        metadata.request_json = body_view.value().cloned();
                    }
                    context.with_renewal(
                        success.key_id.clone(),
                        metadata,
                        resolved_upstream_id,
                        shaped_body,
                        ctx.body_bytes.clone(),
                        ctx.downstream_headers.clone(),
                    )
                } else if compaction_response_enabled {
                    context.with_compaction_candidate(&cache_metadata)
                } else {
                    context
                }
            });
        let session_hint_keys = if request_capability == ProviderCapability::OpenaiResponses
            && !body_view.value().is_some_and(is_openai_prewarm_request)
        {
            let mut keys = Vec::new();
            if let Some(key) = body_view
                .value()
                .and_then(|value| value.get("prompt_cache_key"))
                .and_then(Value::as_str)
                .filter(|key| !key.is_empty())
            {
                keys.push(key.to_owned());
            }
            if let Some(session_id) = cache_metadata
                .observed_session_id
                .as_deref()
                .filter(|session_id| !session_id.is_empty())
                && !keys.iter().any(|existing| existing == session_id)
            {
                keys.push(session_id.to_owned());
            }
            keys
        } else {
            Vec::new()
        };
        let response_affinity_context = ResponseAffinityContext {
            capability: request_capability,
            usage_shape: (request_capability == ProviderCapability::OpenaiChatCompletions
                && routed.record.kind == StorageUpstreamKind::OpenaiOauth)
                .then_some(crate::provider_capability::UsageShape::OpenaiResponses),
            precomputed_usage: precomputed_usage.clone(),
            store: self.upstream_affinity_store.clone(),
            principal_id: principal.id.clone(),
            upstream_id: resolved_upstream_id,
            pending_request_keys,
            observe_response_keys: body_view
                .value()
                .is_some_and(|value| request_observes_response_affinity(request_capability, value)),
            owner_epoch: routed_owner_epoch,
            ttl_secs: self.config.upstream_affinity_ttl.as_secs(),
            session_hint_keys,
        };
        response = self
            .finish_success_response(
                response,
                signer,
                body_view.stream(),
                native_sse_framing_error,
                active_limit.take(),
                reserved.map(|reserved| reserved.into_response_accounting_guard()),
                started.elapsed(),
                status,
                RequestEventContext {
                    request_id: ctx.request_id.clone(),
                    canonical_model_id: ctx.canonical_model_id.clone(),
                    upstream_name: routed.record.name.clone(),
                    requested_model: request_model.clone(),
                    proxy_setup_ms: Some(proxy_setup_ms),
                    stage_timings: attempt_timings,
                },
                ResponseTransformContext {
                    principal: principal.clone(),
                    upstream: routed.route.upstream.clone(),
                    request_method: ctx.method.clone(),
                    request_path: ctx.path.clone(),
                    dialect: routed.route.dialect.clone(),
                    gemini_oauth: routed.record.kind == StorageUpstreamKind::GeminiOauth,
                    vertex: matches!(
                        routed.record.kind,
                        StorageUpstreamKind::VertexApiKey | StorageUpstreamKind::VertexCredential
                    ),
                    codex_backend: ctx.codex_backend,
                    vertex_location: routed
                        .record
                        .vertex
                        .as_ref()
                        .and_then(|config| match &config.target {
                            cc_lb_domain::VertexTarget::Regional { location, .. } => {
                                Some(location.to_string())
                            }
                            cc_lb_domain::VertexTarget::Express => None,
                        }),
                },
                compat_response_plan.clone(),
                prompt_cache_observation_context,
                response_affinity_context,
                observer.clone(),
                keepalive_completion,
                dispatch_started,
                dispatch_unix_secs,
            )
            .await;
        Ok(response)
    }

    #[allow(clippy::result_large_err)]
    #[tracing::instrument(
        name = "proxy.reserve_limit",
        skip_all,
        fields(
            cc_lb.limit.decision = tracing::field::Empty,
            cc_lb.limit.violation = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
            error.type = tracing::field::Empty,
        )
    )]
    async fn reserve_limit(
        &self,
        view: &PrincipalView,
        ctx: &RequestContext,
        principal: &Principal,
        upstream_record: &UpstreamRecord,
        authn_success: &AuthnSuccess,
        make_limit_request: impl FnOnce() -> LimitRequest,
    ) -> Result<Option<ActiveLimit>, LimitRejectionErr> {
        let (Some(limit_engine), Some(subject_provider)) = (
            self.limit_engine.as_ref(),
            self.limit_subject_provider.as_ref(),
        ) else {
            tracing::Span::current().record("cc_lb.limit.decision", "not_configured");
            return Ok(None);
        };
        let Some(subject) = subject_provider
            .limit_subject(ctx, principal, authn_success)
            .await
        else {
            tracing::Span::current().record("cc_lb.limit.decision", "not_applicable");
            return Ok(None);
        };
        let limit_request = make_limit_request();
        let fanout_multiplier = admission_fanout_multiplier(
            upstream_record.kind,
            classify_provider_capability(&ctx.method, &ctx.path, ctx.provider_scope),
            limit_request.fanout_multiplier,
        );
        let max_input_estimate = limit_request
            .input_tokens_estimate
            .saturating_mul(fanout_multiplier);
        let local_cache_input = limit_request.local_cache_input.map(|mut input| {
            input.uncached_input_tokens = input
                .uncached_input_tokens
                .saturating_mul(fanout_multiplier as u64);
            input.cache_prefix_5m_tokens = input
                .cache_prefix_5m_tokens
                .saturating_mul(fanout_multiplier as u64);
            input.cache_prefix_1h_tokens = input
                .cache_prefix_1h_tokens
                .saturating_mul(fanout_multiplier as u64);
            input
        });
        let sent_service_tier = if ctx.codex_backend {
            limit_request.service_tier.clone()
        } else {
            compat_service_tier_for_upstream(
                classify_provider_capability(&ctx.method, &ctx.path, ctx.provider_scope)
                    .unwrap_or(ProviderCapability::OpenaiResponses),
                upstream_record.kind,
                limit_request.service_tier.as_deref(),
                ctx.openai_oauth_compat.as_ref().is_some_and(|policy| {
                    policy.service_tier == cc_lb_domain::CoerceOrReject::Coerce
                }),
            )
        };
        let cost_estimate = if limit_request.model.is_empty() {
            Some(0)
        } else {
            self.limit_cost_estimator.as_ref().and_then(|estimator| {
                if let Some(input) = local_cache_input {
                    estimator.estimate_max_with_local_cache_and_images(
                        &limit_request.model,
                        input,
                        limit_request.max_tokens.unwrap_or(0).max(0) as u64,
                        limit_request.use_image_rates,
                        sent_service_tier.as_deref(),
                    )
                } else {
                    let estimated_cache_tokens = limit_request.estimated_cache_tokens;
                    let can_cross_cache_cliff = limit_request.can_cross_cache_cliff;
                    estimator.estimate_max_with_cache_and_images(
                        &limit_request.model,
                        max_input_estimate as u64,
                        limit_request.max_tokens.unwrap_or(0).max(0) as u64,
                        estimated_cache_tokens,
                        can_cross_cache_cliff,
                        limit_request.use_image_rates,
                        sent_service_tier.as_deref(),
                    )
                }
            })
        };

        match limit_engine.reserve(
            view,
            &subject.record,
            &subject.principal_id,
            &limit_request.model,
            limit_request.max_tokens,
            max_input_estimate,
            cost_estimate,
        ) {
            Ok(reservation) => {
                tracing::Span::current().record("cc_lb.limit.decision", "allowed");
                Ok(Some(ActiveLimit {
                    subject,
                    request: limit_request,
                    fanout_multiplier,
                    service_tier: sent_service_tier,
                    reservation: Some(reservation),
                }))
            }
            Err(reason) => {
                let limit_violation = limit_violation_name(&reason).map(|v| v.to_owned());
                let span = tracing::Span::current();
                span.record("cc_lb.limit.decision", "rejected");
                span.record("otel.status_code", "ERROR");
                span.record("error.type", error_codes::LIMIT_REJECTED);
                if let Some(violation) = limit_violation.as_deref() {
                    span.record("cc_lb.limit.violation", violation);
                }
                let retry_after_seconds = limit_retry_after_secs(reason.clone());
                let reason_label = "limit_rejected".to_owned();
                let capability = crate::provider_capability::classify(
                    &ctx.method,
                    &ctx.path,
                    ctx.provider_scope,
                )
                .unwrap_or(ProviderCapability::AnthropicMessages);
                let mut response = limit_rejection_response(
                    capability,
                    reason,
                    &limit_request.model,
                    &subject.principal_id,
                    retry_after_seconds,
                );
                attach_limit_headers_from_record(
                    response.headers_mut(),
                    limit_engine.as_ref(),
                    capability.provider(),
                    &subject.key_id,
                    &subject.record,
                    view.default_limits(&subject.principal_id),
                );
                let info = LimitRejectionInfo {
                    subject: cc_lb_lifecycle::LimitSubject {
                        principal_id: subject.principal_id.clone(),
                        key_id: subject.key_id.clone(),
                    },
                    request_summary: cc_lb_lifecycle::LimitRequestSummary {
                        model: limit_request.model.clone(),
                        path: ctx.path.clone(),
                        method: ctx.method.as_str().to_owned(),
                    },
                    route_summary: cc_lb_lifecycle::RouteSummary {
                        upstream_name: upstream_record.name.clone(),
                    },
                    limit_violation,
                    reason_label,
                };
                Err(LimitRejectionErr { response, info })
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(
        name = "proxy.finish_response",
        skip_all,
        fields(
            http.response.status_code = i64::from(status.as_u16()),
            cc_lb.response_body_ms = tracing::field::Empty,
            cc_lb.request.finalize_ms = tracing::field::Empty,
        )
    )]
    async fn finish_success_response(
        &self,
        mut response: Response<Body>,
        signer: Arc<dyn Signer>,
        stream_requested: bool,
        native_sse_framing_error: bool,
        active_limit: Option<ActiveLimit>,
        response_accounting_guard: Option<ResponseAccountingGuard>,
        duration: Duration,
        status: StatusCode,
        event_ctx: RequestEventContext,
        transform_ctx: ResponseTransformContext,
        compat_response_plan: Option<CompatResponsePlan>,
        prompt_cache_observation_context: Option<PromptCacheObservationContext>,
        response_affinity_context: ResponseAffinityContext,
        observer: Option<LifecycleContext>,
        keepalive_completion: Option<crate::cache_keepalive::LifecycleKeepaliveContext>,
        dispatch_started: Instant,
        dispatch_unix_secs: u64,
    ) -> Response<Body> {
        let mut compat_compact_pending = false;
        let mut compat_body_timing: Option<CompatBodyTimingGuard> = None;
        if let Some(plan) = compat_response_plan.as_ref() {
            match &plan.kind {
                CompatResponseKind::ResponsesAggregate { echo_metadata } => {
                    let mut guard = observer.clone().map(CompatBodyTimingGuard::start);
                    let aggregated = compat_response::aggregate_response(
                        response,
                        echo_metadata.clone(),
                        decompression_output_budget_bytes(self.config.messages_body_cap_bytes),
                        guard.as_mut().map(|g| &mut g.timing),
                    )
                    .await;
                    compat_body_timing = guard;
                    response = match aggregated {
                        Ok(response) => response,
                        Err(error) => provider_error_response(
                            response_affinity_context.capability,
                            StatusCode::BAD_GATEWAY,
                            "server_error",
                            error.public_message(),
                            None,
                        ),
                    };
                }
                CompatResponseKind::Compact => {
                    let mut guard = observer.clone().map(CompatBodyTimingGuard::start);
                    let aggregated = compat_response::aggregate_response(
                        response,
                        None,
                        decompression_output_budget_bytes(self.config.messages_body_cap_bytes),
                        guard.as_mut().map(|g| &mut g.timing),
                    )
                    .await;
                    compat_body_timing = guard;
                    response = match aggregated {
                        Ok(response) => {
                            compat_compact_pending = true;
                            response
                        }
                        Err(error) => provider_error_response(
                            response_affinity_context.capability,
                            StatusCode::BAD_GATEWAY,
                            "server_error",
                            error.public_message(),
                            None,
                        ),
                    };
                }
                _ => {}
            }
        }
        // A translated OAuth Chat stream must not commit HTTP 200 before the
        // first output-bearing event. If Responses fails in its prefix, the
        // held headers can still carry the public provider error status/body.
        if let Some(CompatResponseKind::Chat { plan }) =
            compat_response_plan.as_ref().map(|plan| &plan.kind)
            && plan.stream
            && response.status().is_success()
        {
            response = match compat_response::inspect_chat_stream_response(
                response,
                compat_response::CHAT_STREAM_PREFIX_CAP_BYTES,
            )
            .await
            {
                Ok(compat_response::ChatStreamInspection::Ready(response))
                | Ok(compat_response::ChatStreamInspection::Failed(response)) => response,
                Err(error) => provider_error_response(
                    response_affinity_context.capability,
                    StatusCode::BAD_GATEWAY,
                    "server_error",
                    error.public_message(),
                    None,
                ),
            };
        }

        let status = response.status();
        // Audio endpoints also stream binary/text payloads without a JSON
        // `stream` flag. Buffered transforms apply to semantic JSON media
        // types and Gemini OAuth envelopes, not arbitrary streamed bytes.
        let response_is_json = has_json_content_type(response.headers());
        let gemini_catalog = transform_ctx.gemini_oauth
            && response_affinity_context.capability == ProviderCapability::GeminiModels;
        let response_is_sse = is_sse_response(
            response.headers(),
            stream_requested,
            response_affinity_context.capability,
        );
        let unexpected_catalog_sse = gemini_catalog && status.is_success() && response_is_sse;
        let audio_stream = matches!(
            response_affinity_context.capability,
            ProviderCapability::OpenaiAudio | ProviderCapability::OpenaiAudioTranscriptions
        ) && !response_is_json;
        let stream_response = stream_requested || audio_stream;
        // A streamed public Responses request still gets a non-SSE error body
        // (Codex `{detail}`) buffered so it is rewritten to the public error shape.
        let compat_force_buffered = compat_response_plan.as_ref().is_some_and(|plan| {
            compat_response::requires_buffered_response(plan)
                || (matches!(&plan.kind, CompatResponseKind::ResponsesStream { .. })
                    && !response_is_sse)
                || (matches!(&plan.kind, CompatResponseKind::Chat { .. }) && !status.is_success())
        });
        let buffered_transform_required = compat_force_buffered
            || transform_ctx.gemini_oauth
            || (response_is_json && transform_ctx.dialect.response_transform_hook().is_some());
        let restricted_codex_models = transform_ctx.codex_backend
            && response_affinity_context.capability == ProviderCapability::OpenaiModels
            && self
                .dynamic_view
                .load()
                .principal_view
                .has_model_allowlist(&transform_ctx.principal.id);
        let force_buffered_response = compat_force_buffered
            || restricted_codex_models
            || (gemini_catalog && status.is_success())
            || response_affinity_context.capability
                == ProviderCapability::GeminiCachedContentsCreate;
        if native_sse_framing_error
            || (!force_buffered_response
                && (response_is_sse || (stream_response && !buffered_transform_required)))
        {
            let provider = response_affinity_context.capability.provider();
            let response_status = response.status();
            let mut response = self.relay_response(
                response,
                signer,
                response_status,
                stream_requested,
                native_sse_framing_error,
                response_accounting_guard,
                event_ctx.clone(),
                transform_ctx,
                compat_response_plan.clone(),
                prompt_cache_observation_context,
                response_affinity_context,
                observer,
                keepalive_completion,
                dispatch_started,
                dispatch_unix_secs,
            );
            self.attach_limit_headers(&mut response, active_limit.as_ref(), provider);
            return response;
        }

        let (mut parts, mut body) = response.into_parts();
        if unexpected_catalog_sse {
            // The headers already violate the catalog protocol. Drop the stream
            // without polling it; it may be unbounded or never produce a frame.
            body = Body::empty();
        }
        strip_hop_by_hop(&mut parts.headers);
        let mut body_buf: Vec<u8> = Vec::new();
        // Ordinary buffered responses remain uncapped. Requested streams
        // requiring JSON/OAuth transforms use the path's cap; non-SSE,
        // non-JSON streaming responses instead relay incrementally above.
        let body_cap = if stream_response {
            body_cap_for_path(&self.config, &transform_ctx.request_path)
        } else {
            usize::MAX
        };
        let mut body_collect_failed = false;
        let mut body_limit_terminal = false;
        let body_collect_started;
        let first_body_chunk_at;
        let body_chunk_count;
        if let Some(mut guard) = compat_body_timing.take() {
            while !unexpected_catalog_sse && let Some(frame) = body.frame().await {
                match frame {
                    Ok(frame) => {
                        if let Ok(data) = frame.into_data()
                            && !push_buffered_data(
                                &data,
                                &body,
                                body_cap,
                                &mut body_buf,
                                &mut body_collect_failed,
                                &mut body_limit_terminal,
                            )
                        {
                            break;
                        }
                    }
                    Err(_source) => {
                        body_collect_failed = true;
                        break;
                    }
                }
            }
            body_collect_started = guard.timing.collect_started;
            first_body_chunk_at = guard.timing.first_body_chunk_at;
            body_chunk_count = guard.timing.body_chunk_count;
            guard.publish();
        } else {
            body_collect_started = Instant::now();
            let mut last_activity_heartbeat_at = body_collect_started;
            let mut activity_heartbeat = new_activity_heartbeat_interval();
            let response_body_timing = BodyIoTiming::default();
            let mut response_body_timing_guard =
                BodyIoTimingObserverGuard::new(observer.clone(), response_body_timing.clone());
            response_body_timing.transition(Some(BodyIoPhase::Process));
            let mut first_chunk_at: Option<Instant> = None;
            let mut chunk_count: u64 = 0;
            while !unexpected_catalog_sse
                && let Some(frame) = timed_body_frame_with_heartbeat(
                    &mut body,
                    &response_body_timing,
                    observer.as_ref(),
                    &mut last_activity_heartbeat_at,
                    &mut activity_heartbeat,
                )
                .await
            {
                match frame {
                    Ok(frame) => {
                        if let Ok(data) = frame.into_data() {
                            let chunk_now = Instant::now();
                            maybe_emit_chunk_heartbeat(
                                chunk_now,
                                &mut last_activity_heartbeat_at,
                                &mut activity_heartbeat,
                                observer.as_ref(),
                            );
                            if first_chunk_at.is_none() {
                                first_chunk_at = Some(chunk_now);
                                if let Some(o) = observer.as_ref() {
                                    o.set_termination_timings(
                                        None,
                                        None,
                                        Some(duration_to_ms(
                                            chunk_now
                                                .saturating_duration_since(body_collect_started),
                                        )),
                                    );
                                }
                            }
                            chunk_count = chunk_count.saturating_add(1);
                            if !push_buffered_data(
                                &data,
                                &body,
                                body_cap,
                                &mut body_buf,
                                &mut body_collect_failed,
                                &mut body_limit_terminal,
                            ) {
                                break;
                            }
                        }
                    }
                    Err(_source) => {
                        body_collect_failed = true;
                        break;
                    }
                }
            }
            response_body_timing.stop();
            response_body_timing_guard.record();
            first_body_chunk_at = first_chunk_at;
            body_chunk_count = chunk_count;
        }
        let finalize_started = Instant::now();
        let body = if body_collect_failed {
            Bytes::new()
        } else {
            Bytes::from(body_buf)
        };
        let body_collect_ms = duration_to_ms(body_collect_started.elapsed());
        let first_body_chunk_ms = first_body_chunk_at
            .map(|t| duration_to_ms(t.saturating_duration_since(body_collect_started)));
        tracing::info!(
            request_id = %event_ctx.request_id,
            status = status.as_u16(),
            proxy_setup_ms = event_ctx.proxy_setup_ms,
            shape_ms = ?event_ctx.stage_timings.shape_ms,
            sign_ms = ?event_ctx.stage_timings.sign_ms,
            upstream_ttfb_ms = ?event_ctx.stage_timings.upstream_ttfb_ms,
            first_body_chunk_ms = ?first_body_chunk_ms,
            upstream_body_ms = body_collect_ms,
            body_chunk_count = body_chunk_count,
            body_bytes = body.len(),
            total_ms = duration_to_ms(duration),
            "request latency breakdown"
        );
        let semantic_body = if body_collect_failed || unexpected_catalog_sse {
            None
        } else {
            match decode_full_body(
                &parts.headers,
                &body,
                decompression_output_budget_bytes(self.config.messages_body_cap_bytes),
            ) {
                Ok(Some(plaintext)) => Some(semantic_body_bytes(&body, plaintext)),
                Ok(None) => {
                    if let Some(encoding) = parts
                        .headers
                        .get(http::header::CONTENT_ENCODING)
                        .and_then(|v| v.to_str().ok())
                    {
                        tracing::warn!(
                            request_id = %event_ctx.request_id,
                            content_encoding = encoding,
                            "skipping usage extraction: unsupported content-encoding"
                        );
                        None
                    } else {
                        Some(body.clone())
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        request_id = %event_ctx.request_id,
                        %error,
                        "skipping usage extraction: failed to decode response body"
                    );
                    None
                }
            }
        };
        let gemini_unwrapped_body = (!gemini_catalog && transform_ctx.gemini_oauth)
            .then_some(semantic_body.as_ref())
            .flatten()
            .and_then(|body| unwrap_gemini_code_assist_body(body).ok().flatten());
        let mut vertex_response_translated = false;
        let mut vertex_translation_failed = false;
        let mut vertex_catalog_translation_failed = false;
        let mut semantic_body = gemini_unwrapped_body.clone().or(semantic_body);
        if transform_ctx.vertex
            && status.is_success()
            && !body_collect_failed
            && let Some(body) = semantic_body.as_deref()
        {
            match translate_vertex_response(
                body,
                &transform_ctx.request_path,
                response_affinity_context.capability,
                transform_ctx.vertex_location.as_deref(),
            ) {
                Ok(None) => {}
                Ok(Some(translated)) => {
                    vertex_response_translated = true;
                    parts.headers = sanitize_downstream_response_headers(
                        std::mem::take(&mut parts.headers),
                        translated.len(),
                        None,
                    );
                    semantic_body = Some(translated);
                }
                Err(reason) => {
                    tracing::warn!(
                        request_id = %event_ctx.request_id,
                        reason,
                        "invalid Vertex response shape"
                    );
                    vertex_translation_failed = true;
                    vertex_catalog_translation_failed =
                        response_affinity_context.capability == ProviderCapability::GeminiModels;
                    semantic_body = None;
                    parts.status = if vertex_catalog_translation_failed {
                        StatusCode::BAD_GATEWAY
                    } else {
                        StatusCode::SERVICE_UNAVAILABLE
                    };
                }
            }
        }
        let response_body_json = semantic_body
            .as_ref()
            .and_then(|body| sonic_rs::from_slice::<Value>(body).ok());
        let usage_shape = compat_response_plan
            .as_ref()
            .filter(|plan| matches!(plan.kind, CompatResponseKind::Chat { .. }))
            .map(|_| crate::provider_capability::UsageShape::OpenaiResponses)
            .unwrap_or_else(|| response_affinity_context.capability.usage_shape());
        let observation = response_body_json
            .as_ref()
            .map_or_else(usage_parser::NonStreamObservation::default, |value| {
                usage_parser::observe_non_stream_json_value_for_shape(value, usage_shape)
            });
        let chat_raw_usage = response_affinity_context
            .precomputed_usage
            .clone()
            .or_else(|| {
                compat_response_plan
                    .as_ref()
                    .filter(|plan| matches!(plan.kind, CompatResponseKind::Chat { .. }))
                    .and_then(|_| {
                        crate::openai_compat::chat::responses_usage_from_sse(&body)
                            .map(|usage| responses_usage_value_to_counts(&usage))
                    })
            });
        let observation = match chat_raw_usage {
            Some(usage) => usage_parser::NonStreamObservation {
                usage,
                canonical_error: observation.canonical_error,
            },
            None => observation,
        };
        let abnormal_stop = response_body_json
            .as_ref()
            .and_then(usage_parser::detect_abnormal_stop_non_stream);
        let usage = observation.usage;
        let native_response_failed = response_body_json.as_ref().is_some_and(|value| {
            value.get("object").and_then(Value::as_str) == Some("response")
                && value.get("status").and_then(Value::as_str) == Some("failed")
        });
        let declared_provider_error = matches!(
            response_body_json
                .as_ref()
                .and_then(|value| value.get("type"))
                .and_then(Value::as_str),
            Some("error" | "response.failed")
        );
        let canonical_upstream_error = if status.is_client_error() || status.is_server_error() {
            response_body_json
                .as_ref()
                .and_then(usage_parser::observe_http_error_value)
        } else if native_response_failed {
            observation.canonical_error
        } else {
            None
        };
        let non_error_status_response_failed = !(status.is_client_error()
            || status.is_server_error())
            && canonical_upstream_error.is_some();
        // Provider error text can echo the credential; the parser masks it
        // before its store cap so a secret split by the cap cannot survive.
        // Successful responses never get here.
        let retry_after = canonical_upstream_error
            .as_ref()
            .and_then(usage_parser::CanonicalUpstreamError::retry_after)
            .map(ToOwned::to_owned);
        let (upstream_error_type, parsed_upstream_message, upstream_error_code) =
            canonical_upstream_error
                .map(|error| error.into_parts(signer.as_ref()))
                .unwrap_or_default();
        let upstream_error_message = match parsed_upstream_message {
            Some(message) => Some(message),
            None if status.is_client_error() || status.is_server_error() => semantic_body
                .as_ref()
                .filter(|body| !body.is_empty())
                .map(|body| scrubbed_upstream_error_body(signer.as_ref(), body, false)),
            None => None,
        };
        let mut downstream_body = if body_collect_failed {
            parts.status = if response_affinity_context.capability
                == ProviderCapability::GeminiCachedContentsCreate
            {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::BAD_GATEWAY
            };
            parts.headers.clear();
            parts.headers.insert(
                CONTENT_TYPE,
                HeaderValue::from_static("application/json; charset=utf-8"),
            );
            provider_error_body(
                response_affinity_context.capability,
                parts.status,
                "api_error",
                UPSTREAM_BUFFERED_BODY_FAILED_MESSAGE,
                None,
            )
        } else if vertex_translation_failed {
            let (status, error_type, message) = if vertex_catalog_translation_failed {
                (
                    StatusCode::BAD_GATEWAY,
                    "api_error",
                    "invalid Vertex model catalog",
                )
            } else {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    error_codes::UPSTREAM_AFFINITY_UNAVAILABLE,
                    UPSTREAM_AFFINITY_UNAVAILABLE_MESSAGE,
                )
            };
            parts.status = status;
            parts.headers.clear();
            parts.headers.insert(
                CONTENT_TYPE,
                HeaderValue::from_static("application/json; charset=utf-8"),
            );
            provider_error_body(
                response_affinity_context.capability,
                status,
                error_type,
                message,
                None,
            )
        } else if gemini_unwrapped_body.is_some() || gemini_catalog || vertex_response_translated {
            semantic_body.clone().unwrap_or(body)
        } else {
            body
        };
        if status.is_success()
            && let Some(unwrapped) = gemini_unwrapped_body
        {
            downstream_body = unwrapped;
            parts.headers = sanitize_downstream_response_headers(
                std::mem::take(&mut parts.headers),
                downstream_body.len(),
                None,
            );
        }
        if gemini_catalog
            && status.is_success()
            && !body_collect_failed
            && let Some(decoded) = semantic_body.as_ref()
        {
            downstream_body = decoded.clone();
            parts.headers = sanitize_downstream_response_headers(
                std::mem::take(&mut parts.headers),
                downstream_body.len(),
                None,
            );
        }
        if !body_collect_failed {
            if compat_compact_pending {
                if parts.status.is_success() {
                    match compat_response::compact_response_body(&downstream_body) {
                        Ok(compacted) => {
                            downstream_body = Bytes::from(compacted);
                            parts.headers = sanitize_downstream_response_headers(
                                std::mem::take(&mut parts.headers),
                                downstream_body.len(),
                                None,
                            );
                        }
                        Err(_) => {
                            parts.status = StatusCode::BAD_GATEWAY;
                            downstream_body = provider_error_body(
                                response_affinity_context.capability,
                                parts.status,
                                "server_error",
                                "invalid compaction response",
                                None,
                            );
                        }
                    }
                }
            } else if let Some(plan) = compat_response_plan.as_ref() {
                let principal_view = self.dynamic_view.load();
                let principal_id = transform_ctx.principal.id.clone();
                let is_model_allowed = |model: &str| {
                    principal_view
                        .principal_view
                        .is_model_allowed(&principal_id, model)
                };
                match compat_response::convert_buffered_plan(
                    plan,
                    parts.status,
                    std::mem::take(&mut parts.headers),
                    downstream_body.clone(),
                    &is_model_allowed,
                ) {
                    Ok(converted) => {
                        let (converted_parts, converted_body) = converted.into_parts();
                        parts = converted_parts;
                        downstream_body = match converted_body.collect().await {
                            Ok(collected) => collected.to_bytes(),
                            Err(_) => Bytes::new(),
                        };
                    }
                    Err(_) => {
                        parts.status = StatusCode::BAD_GATEWAY;
                        downstream_body = provider_error_body(
                            response_affinity_context.capability,
                            parts.status,
                            "server_error",
                            "invalid compatibility response",
                            None,
                        );
                    }
                }
            }
        }
        if !body_collect_failed
            && transform_ctx.codex_backend
            && parts.status.is_success()
            && response_affinity_context.capability == ProviderCapability::OpenaiModels
        {
            let view = self.dynamic_view.load();
            let principal_id = &transform_ctx.principal.id;
            if view.principal_view.has_model_allowlist(principal_id) {
                match filter_codex_models_catalog(
                    &downstream_body,
                    &view.principal_view,
                    principal_id,
                ) {
                    Ok(filtered) => downstream_body = filtered,
                    Err(()) => {
                        parts.status = StatusCode::BAD_GATEWAY;
                        downstream_body = provider_error_body(
                            response_affinity_context.capability,
                            parts.status,
                            "server_error",
                            "invalid models catalog",
                            None,
                        );
                    }
                }
                // Both arms replace the body, so the upstream length and
                // encoding headers no longer describe it.
                parts.headers = sanitize_downstream_response_headers(
                    std::mem::take(&mut parts.headers),
                    downstream_body.len(),
                    None,
                );
            }
        }
        let mut buffered_transform_error: Option<ResponseTransformError> = None;
        let mut keepalive_transformed_json = None;
        let mut keepalive_body_replaced = false;
        let mut catalog_body_replaced = false;
        let mut catalog_error = None;
        if !body_collect_failed
            && !unexpected_catalog_sse
            && (transform_ctx.gemini_oauth || response_is_json)
            && let (Some(hook), Some(semantic_body)) = (
                transform_ctx.dialect.response_transform_hook(),
                semantic_body,
            )
        {
            match hook.transform_response(TransformResponseRequest {
                request_id: event_ctx.request_id.clone(),
                principal: transform_ctx.principal.clone(),
                upstream: transform_ctx.upstream.clone(),
                request_method: transform_ctx.request_method.clone(),
                request_path: transform_ctx.request_path.clone(),
                canonical_model_id: event_ctx.canonical_model_id.clone(),
                response_status: parts.status,
                response_headers: sanitized_response_headers_for_plugin(&parts.headers),
                body: semantic_body.clone(),
            }) {
                Ok(result) => {
                    catalog_body_replaced = gemini_catalog
                        && matches!(
                            &result,
                            cc_lb_upstream::TransformResponseResult::Replace { body: Some(_), .. }
                        );
                    if keepalive_completion
                        .as_ref()
                        .is_some_and(|context| context.observes_response())
                        && let cc_lb_upstream::TransformResponseResult::Replace {
                            body: Some(body),
                            ..
                        } = &result
                    {
                        keepalive_body_replaced = true;
                        keepalive_transformed_json = sonic_rs::from_slice::<Value>(body).ok();
                    }
                    let transformed = apply_buffered_transform_result(
                        parts.status,
                        std::mem::take(&mut parts.headers),
                        downstream_body,
                        result,
                    );
                    parts.status = transformed.status;
                    parts.headers = transformed.headers;
                    downstream_body = transformed.body;
                }
                Err(error) => {
                    tracing::warn!(
                        request_id = %event_ctx.request_id,
                        %error,
                        "response transform failed open to original upstream response"
                    );
                    buffered_transform_error = Some(error);
                }
            }
        }
        if gemini_catalog
            && status.is_success()
            && !body_collect_failed
            && (!catalog_body_replaced || parts.status.is_success())
        {
            let replacement_json = catalog_body_replaced
                .then(|| sonic_rs::from_slice::<Value>(&downstream_body).ok())
                .flatten();
            let catalog_json = if catalog_body_replaced {
                replacement_json.as_ref()
            } else {
                response_body_json.as_ref()
            };
            match catalog_json
                .filter(|_| !unexpected_catalog_sse)
                .ok_or(crate::gemini_oauth_catalog::CatalogError::InvalidModels)
                .and_then(crate::gemini_oauth_catalog::project_fetch_available_models)
                .and_then(|native| {
                    serde_json::to_vec(&native)
                        .map_err(|_| crate::gemini_oauth_catalog::CatalogError::InvalidModels)
                }) {
                Ok(native) => {
                    downstream_body = Bytes::from(native);
                    parts.headers = sanitize_downstream_response_headers(
                        std::mem::take(&mut parts.headers),
                        downstream_body.len(),
                        None,
                    );
                }
                Err(_) => {
                    let message = "invalid Gemini OAuth catalog response";
                    catalog_error = Some(message);
                    parts.status = StatusCode::BAD_GATEWAY;
                    downstream_body = provider_error_body(
                        response_affinity_context.capability,
                        parts.status,
                        "api_error",
                        message,
                        None,
                    );
                    parts.headers = sanitize_downstream_response_headers(
                        std::mem::take(&mut parts.headers),
                        downstream_body.len(),
                        None,
                    );
                    parts
                        .headers
                        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
                }
            }
        }
        let mut affinity_bind_failed = false;
        if status.is_success() && !body_collect_failed && !vertex_translation_failed {
            let mut affinity_keys = response_affinity_context.pending_request_keys.clone();
            if response_affinity_context.capability == ProviderCapability::OpenaiRealtimeCall
                && let Some(location) = parts
                    .headers
                    .get(LOCATION)
                    .and_then(|value| value.to_str().ok())
                && let Some(key) =
                    realtime_call_affinity_key(&response_affinity_context.principal_id, location)
            {
                affinity_keys.push(key);
            }
            // Do not gate this scan on request tools: a successful buffered response can
            // introduce unsolicited provider state (e.g. an Anthropic web-search result or
            // an OpenAI response id) that later requests must pin.
            let now_unix_secs = unix_secs(self.clock.now());
            let response_expiry = if response_affinity_context.capability
                == ProviderCapability::GeminiCachedContentsCreate
            {
                response_body_json
                    .as_ref()
                    .and_then(|response_json| gemini_cached_content_expiry(response_json).ok())
                    .filter(|expiry| *expiry > now_unix_secs)
            } else {
                None
            };
            let extraction_failed = if let Some(response_json) = response_body_json.as_ref() {
                match extract_response_affinity_keys(
                    response_affinity_context.capability,
                    response_json,
                    &response_affinity_context.principal_id,
                ) {
                    Ok(response_keys) => {
                        affinity_keys.extend(response_keys);
                        response_affinity_context.capability
                            == ProviderCapability::GeminiCachedContentsCreate
                            && response_expiry.is_none()
                    }
                    Err(_) => true,
                }
            } else {
                response_affinity_context.capability
                    == ProviderCapability::GeminiCachedContentsCreate
            };
            if extraction_failed
                || bind_upstream_affinity_keys(
                    &response_affinity_context,
                    &affinity_keys,
                    now_unix_secs,
                    response_expiry,
                )
                .await
                .is_err()
            {
                tracing::warn!("failed to safely bind upstream affinity before buffered response");
                affinity_bind_failed = true;
                parts.status = StatusCode::SERVICE_UNAVAILABLE;
                parts.headers.clear();
                parts.headers.insert(
                    CONTENT_TYPE,
                    HeaderValue::from_static("application/json; charset=utf-8"),
                );
                downstream_body = provider_error_body(
                    response_affinity_context.capability,
                    StatusCode::SERVICE_UNAVAILABLE,
                    "api_error",
                    UPSTREAM_AFFINITY_UNAVAILABLE_MESSAGE,
                    None,
                );
            }
        }
        let client_status = parts.status;
        let keepalive_response_json = if keepalive_body_replaced {
            keepalive_transformed_json.as_ref()
        } else {
            response_body_json.as_ref()
        };
        if let Some(context) = keepalive_completion
            && (!body_collect_failed || body_limit_terminal)
        {
            let cache_anchor_age = first_body_chunk_at
                .unwrap_or(context.request_dispatched_at)
                .elapsed();
            let can_renew_response = !body_collect_failed
                && status.is_success()
                && !non_error_status_response_failed
                && !native_response_failed
                && !declared_provider_error
                && abnormal_stop.is_none()
                && !affinity_bind_failed
                && buffered_transform_error.is_none()
                && !(keepalive_body_replaced
                    && keepalive_response_json.is_some_and(|value| {
                        matches!(
                            value.get("type").and_then(Value::as_str),
                            Some("error" | "response.failed")
                        ) || (value.get("object").and_then(Value::as_str) == Some("response")
                            && value.get("status").and_then(Value::as_str) == Some("failed"))
                    }));
            context.complete(
                &self.cache_keepalive,
                can_renew_response
                    .then_some(keepalive_response_json)
                    .flatten(),
                cache_anchor_age,
                usage
                    .cache_read_input_tokens
                    .saturating_add(usage.cache_creation_input_tokens),
            );
        }
        if !body_collect_failed
            && status.is_success()
            && !non_error_status_response_failed
            && !native_response_failed
            && !declared_provider_error
            && !vertex_translation_failed
            && abnormal_stop.is_none()
            && !affinity_bind_failed
            && buffered_transform_error.is_none()
            && !response_affinity_context.session_hint_keys.is_empty()
        {
            self.record_openai_session_hint_keys(
                &response_affinity_context.principal_id,
                &response_affinity_context.session_hint_keys,
                response_affinity_context.upstream_id,
            );
        }
        if status.is_success()
            && let Some(context) = prompt_cache_observation_context.as_ref()
        {
            let now_unix_secs = unix_secs(self.clock.now());
            let prompt_cache_usage = PromptCacheUsage::from(&usage);
            if usage.has_billable_usage() {
                let decode = decode_prompt_cache_observations_pure(
                    context,
                    prompt_cache_usage,
                    dispatch_unix_secs,
                );
                publish_prompt_cache_observations_with_usage(
                    context,
                    &decode,
                    prompt_cache_usage,
                    now_unix_secs,
                );
            } else {
                publish_provider_cache_observation(context, prompt_cache_usage, now_unix_secs);
            }
        }
        // Hand the reservation to the reconcile subscriber for every buffered outcome:
        // it reconciles a measured success and refunds errors on `RequestTerminated`.
        if let Some(response_accounting_guard) = response_accounting_guard {
            response_accounting_guard.forget();
        }
        if let (Some(limit_engine), Some(active_limit)) =
            (self.limit_engine.as_ref(), active_limit.as_ref())
        {
            attach_limit_headers_from_engine(
                &mut parts.headers,
                limit_engine.as_ref(),
                response_affinity_context.capability.provider(),
                &active_limit.subject.key_id,
                &active_limit.subject.principal_id,
            );
        }

        let finalize_ms = if let Some(o) = observer.as_ref() {
            if usage.has_billable_usage() {
                o.set_usage_counts(&usage, true);
            }
            if usage.has_billable_usage() {
                o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::UsageObserved {
                    event_id: o.event_id().to_owned(),
                    usage: to_usage_snapshot(&usage),
                    source: cc_lb_lifecycle::UsageSource::NonStreamBody,
                });
            }
            let stream_result = if body_collect_failed {
                Err(cc_lb_lifecycle::StreamError {
                    error_type: UPSTREAM_RESPONSE_BODY_ERROR_TYPE.to_owned(),
                    error_message: UPSTREAM_BUFFERED_BODY_FAILED_MESSAGE.to_owned(),
                    ..Default::default()
                })
            } else if let Some(message) = catalog_error {
                Err(cc_lb_lifecycle::StreamError {
                    error_type: "api_error".to_owned(),
                    error_message: message.to_owned(),
                    ..Default::default()
                })
            } else if affinity_bind_failed {
                Err(affinity_stream_error())
            } else if let Some(error) = buffered_transform_error.as_ref() {
                Err(cc_lb_lifecycle::StreamError {
                    error_type: "response_transform_error".to_owned(),
                    error_message: error.to_string(),
                    ..Default::default()
                })
            } else if non_error_status_response_failed {
                Err(cc_lb_lifecycle::StreamError {
                    error_type: upstream_error_type
                        .clone()
                        .unwrap_or_else(|| "response_failed".to_owned()),
                    error_message: upstream_error_message
                        .clone()
                        .unwrap_or_else(|| "The provider response failed".to_owned()),
                    ..Default::default()
                })
            } else {
                Ok(cc_lb_lifecycle::StreamSuccess {
                    usage: to_usage_snapshot(&usage),
                    usage_present: usage.has_billable_usage(),
                    sse_event_count: 0,
                    body_bytes: Some(downstream_body.len() as u64),
                    body_chunk_count: Some(body_chunk_count),
                    first_body_chunk_ms,
                    ..Default::default()
                })
            };
            o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::StreamCompleted {
                event_id: o.event_id().to_owned(),
                result: stream_result,
            });
            o.set_termination_timings(
                event_ctx.proxy_setup_ms,
                Some(body_collect_ms),
                first_body_chunk_ms,
            );
            if status.is_client_error()
                || status.is_server_error()
                || non_error_status_response_failed
            {
                if upstream_error_type.is_some()
                    || upstream_error_message.is_some()
                    || (status == StatusCode::TOO_MANY_REQUESTS
                        && parts.headers.get(RETRY_AFTER).is_none()
                        && retry_after.is_some())
                {
                    o.emit_lifecycle(
                        cc_lb_lifecycle::LifecycleEvent::RequestLogUpstreamErrorObserved {
                            event_id: o.event_id().to_owned(),
                            error_type: upstream_error_type.unwrap_or_default(),
                            error_message: upstream_error_message.unwrap_or_default(),
                            upstream_error_code,
                            retry_after: (status == StatusCode::TOO_MANY_REQUESTS
                                && parts.headers.get(RETRY_AFTER).is_none())
                            .then_some(retry_after.clone())
                            .flatten(),
                        },
                    );
                }
                let code = if status.is_client_error() {
                    UpstreamErrorCode::Upstream4xx
                } else if status.is_server_error() {
                    UpstreamErrorCode::Upstream5xx
                } else {
                    UpstreamErrorCode::StreamError
                };
                o.set_upstream_error(status, code);
            } else if body_collect_failed {
                o.set_upstream_error(client_status, UpstreamErrorCode::StreamError);
            } else if let Some(message) = catalog_error {
                o.emit_lifecycle(
                    cc_lb_lifecycle::LifecycleEvent::RequestLogUpstreamErrorObserved {
                        event_id: o.event_id().to_owned(),
                        error_type: "api_error".to_owned(),
                        error_message: message.to_owned(),
                        upstream_error_code: None,
                        retry_after: None,
                    },
                );
                o.set_upstream_error(client_status, UpstreamErrorCode::StreamError);
            } else if affinity_bind_failed {
                o.set_failure(InternalFailure {
                    status: StatusCode::SERVICE_UNAVAILABLE,
                    error_code: error_codes::UPSTREAM_AFFINITY_UNAVAILABLE,
                    error: InternalError {
                        stage: InternalErrorStage::Storage,
                        kind: InternalErrorKind::Unavailable,
                        message: Some(UPSTREAM_AFFINITY_UNAVAILABLE_MESSAGE.to_owned()),
                    },
                });
            } else if buffered_transform_error.is_some() {
                o.set_upstream_error(status, UpstreamErrorCode::StreamError);
            } else if let Some(stop) = abnormal_stop.as_ref() {
                o.emit_lifecycle(
                    cc_lb_lifecycle::LifecycleEvent::RequestLogUpstreamErrorObserved {
                        event_id: o.event_id().to_owned(),
                        error_type: stop.stop_reason.clone(),
                        error_message: abnormal_stop_error_message(stop),
                        upstream_error_code: None,
                        retry_after: None,
                    },
                );
                let code = match stop.stop_reason.as_str() {
                    "refusal" => UpstreamErrorCode::Refusal,
                    _ => UpstreamErrorCode::ContextWindowExceeded,
                };
                o.set_upstream_error(client_status, code);
            } else {
                o.set_success_status(client_status);
            }
            let finalize_ms = duration_to_ms(finalize_started.elapsed());
            o.set_finalize_ms(finalize_ms);
            o.finish();
            finalize_ms
        } else {
            duration_to_ms(finalize_started.elapsed())
        };
        let response_span = tracing::Span::current();
        response_span.record(
            "cc_lb.response_body_ms",
            numeric_attribute(Some(body_collect_ms)).as_value(),
        );
        response_span.record(
            "cc_lb.request.finalize_ms",
            numeric_attribute(Some(finalize_ms)).as_value(),
        );
        Response::from_parts(parts, Body::from(downstream_body))
    }

    async fn join_local(
        mut futures: Vec<ConcurrentAttemptFuture<'_>>,
    ) -> Vec<Result<AttemptResponse, AttemptFailure>> {
        let mut futures = futures.drain(..).map(Some).collect::<Vec<_>>();
        let mut outputs = (0..futures.len())
            .map(|_| None)
            .collect::<Vec<Option<Result<AttemptResponse, AttemptFailure>>>>();
        std::future::poll_fn(move |cx| {
            let mut all_ready = true;
            for index in 0..futures.len() {
                let Some(mut future) = futures[index].take() else {
                    continue;
                };
                match future.as_mut().poll(cx) {
                    Poll::Ready(Ok(output)) => outputs[index] = Some(Ok(output)),
                    Poll::Ready(Err(error)) => return Poll::Ready(vec![Err(error)]),
                    Poll::Pending => {
                        futures[index] = Some(future);
                        all_ready = false;
                    }
                }
            }
            if all_ready {
                Poll::Ready(outputs.iter_mut().filter_map(Option::take).collect())
            } else {
                Poll::Pending
            }
        })
        .await
    }
    async fn join_values<T: Send>(
        mut futures: Vec<Pin<Box<dyn Future<Output = T> + Send + '_>>>,
    ) -> Vec<T> {
        let mut futures = futures.drain(..).map(Some).collect::<Vec<_>>();
        let mut outputs = (0..futures.len()).map(|_| None).collect::<Vec<Option<T>>>();
        std::future::poll_fn(move |cx| {
            let mut all_ready = true;
            for index in 0..futures.len() {
                let Some(mut future) = futures[index].take() else {
                    continue;
                };
                match future.as_mut().poll(cx) {
                    Poll::Ready(output) => outputs[index] = Some(output),
                    Poll::Pending => {
                        futures[index] = Some(future);
                        all_ready = false;
                    }
                }
            }
            if all_ready {
                Poll::Ready(outputs.iter_mut().filter_map(Option::take).collect())
            } else {
                Poll::Pending
            }
        })
        .await
    }

    async fn collect_response_bodies_concurrently(
        responses: Vec<Response<Body>>,
        cap: usize,
    ) -> Vec<(
        StatusCode,
        HeaderMap,
        Result<Bytes, compat_response::CollectError>,
    )> {
        let mut futures: Vec<ResponseBodyFuture<'_>> = Vec::with_capacity(responses.len());
        for response in responses {
            let (parts, body) = response.into_parts();
            futures.push(Box::pin(async move {
                let result = compat_response::collect_body(body, cap).await;
                (parts.status, parts.headers, result)
            }));
        }
        Self::join_values(futures).await
    }

    async fn run_upstream_attempts_concurrently(
        &self,
        count: usize,
        inputs: ConcurrentAttemptInputs<'_, '_>,
    ) -> (
        Vec<Result<AttemptResponse, AttemptFailure>>,
        AttemptTimings,
        Option<Bytes>,
    ) {
        let ConcurrentAttemptInputs {
            authenticated,
            reserved,
            ctx,
            principal,
            routed,
            signer,
            observer,
            cache_metadata,
            shared_request_started,
        } = inputs;
        let mut states = (0..count)
            .map(|_| {
                (
                    AttemptTimings::default(),
                    None::<Bytes>,
                    shared_request_started.clone(),
                )
            })
            .collect::<Vec<_>>();
        let mut futures: Vec<ConcurrentAttemptFuture<'_>> = Vec::with_capacity(count);
        for (timings, shaped_body, dispatched_at) in &mut states {
            futures.push(Box::pin(self.run_upstream_attempt(
                authenticated,
                reserved,
                ctx,
                principal,
                routed,
                signer.clone(),
                observer,
                timings,
                shaped_body,
                cache_metadata,
                dispatched_at,
            )));
        }
        let results = Self::join_local(futures).await;
        let (timings, shaped_body, _) = states
            .into_iter()
            .next()
            .expect("concurrent fan-out has at least one attempt");
        (results, timings, shaped_body)
    }

    /// Dispatch one attempt to `routed` (already announced with
    /// `UpstreamAttempt`), and on a response emit `UpstreamResponseStarted` so
    /// header subscribers ingest it even when the attempt later fails over.
    #[allow(clippy::too_many_arguments)]
    async fn run_upstream_attempt(
        &self,
        authenticated: &crate::authn_rail::Authenticated,
        reserved: Option<&Reserved>,
        ctx: &RequestContext,
        principal: &Principal,
        routed: &RoutedUpstream<'_>,
        signer: Arc<dyn Signer>,
        observer: Option<&LifecycleContext>,
        attempt_timings: &mut AttemptTimings,
        shaped_body_out: &mut Option<Bytes>,
        cache_metadata: &RequestCacheMetadata,
        keepalive_request_started: &Arc<
            tokio::sync::OnceCell<(Instant, Option<crate::cache_keepalive::RequestStartOrder>)>,
        >,
    ) -> Result<AttemptResponse, AttemptFailure> {
        let attempt_scope = match reserved {
            Some(reserved) => reserved.begin_attempt(),
            None => Scoped::unreserved_for_lifecycle(),
        };
        let attempt_result = self
            .attempt(
                authenticated,
                self.dispatcher.as_ref(),
                attempt_scope,
                ctx,
                principal,
                &routed.route,
                routed.record.kind,
                signer,
                routed.raw_passthrough_base_url.as_ref(),
                observer,
                attempt_timings,
                shaped_body_out,
                cache_metadata,
                keepalive_request_started,
            )
            .await?;
        let response = &attempt_result.response;
        if let Some(o) = observer {
            let stream_requested = o.stream_requested();
            let capability =
                classify_provider_capability(&ctx.method, &ctx.path, ctx.provider_scope)
                    .unwrap_or(ProviderCapability::AnthropicMessages);
            let response_is_sse = is_sse_response(response.headers(), stream_requested, capability);
            let compat_forces_buffered =
                attempt_result
                    .compat_response_plan
                    .as_ref()
                    .is_some_and(|plan| {
                        compat_response::requires_buffered_response(plan)
                            || (matches!(&plan.kind, CompatResponseKind::Chat { .. })
                                && !response.status().is_success())
                    });
            let audio_stream = matches!(
                capability,
                ProviderCapability::OpenaiAudio | ProviderCapability::OpenaiAudioTranscriptions
            ) && !has_json_content_type(response.headers());
            let protocol = if response_is_sse && !compat_forces_buffered {
                cc_lb_request_log::RequestProtocol::HttpSse
            } else if !compat_forces_buffered
                && response.status().is_success()
                && !has_json_content_type(response.headers())
                && (stream_requested || audio_stream)
            {
                cc_lb_request_log::RequestProtocol::HttpStream
            } else {
                cc_lb_request_log::RequestProtocol::Http
            };
            o.set_attempt_timings(
                attempt_timings.shape_ms,
                attempt_timings.sign_ms,
                attempt_timings.upstream_ttfb_ms,
            );
            o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::UpstreamResponseStarted {
                event_id: o.event_id().to_owned(),
                status: response.status().as_u16(),
                headers: header_snapshot_from(response.headers(), routed.record.kind),
                observed_at_unix_millis: Some(unix_now_ms(&*self.clock)),
                bulkhead_wait_ms: attempt_timings.bulkhead_wait_ms,
                dns_ms: attempt_timings.dns_ms,
                connect_ms: attempt_timings.connect_ms,
                connection_reused: attempt_timings.connection_reused,
                shape_ms: attempt_timings.shape_ms,
                sign_ms: attempt_timings.sign_ms,
                upstream_ttfb_ms: attempt_timings.upstream_ttfb_ms,
                protocol: Some(protocol),
            });
        }
        Ok(attempt_result)
    }

    /// Terminal relay of a failed attempt that is not failed over.
    fn relay_attempt_failure(
        &self,
        failure: AttemptFailure,
        capability: ProviderCapability,
        active_limit: Option<&ActiveLimit>,
        observer: Option<&LifecycleContext>,
        attempt_timings: &AttemptTimings,
        provider: Provider,
    ) -> Response<Body> {
        let (mut response, failure) = failure.into_response_and_failure(capability);
        self.attach_limit_headers(&mut response, active_limit, provider);
        if let Some(o) = observer {
            o.set_attempt_timings(
                attempt_timings.shape_ms,
                attempt_timings.sign_ms,
                attempt_timings.upstream_ttfb_ms,
            );
            o.set_attempt_connection_timings(
                attempt_timings.dns_ms,
                attempt_timings.connect_ms,
                attempt_timings.connection_reused,
            );
            o.terminate_failure(failure);
        }
        response
    }

    fn emit_routing_failure_event(
        &self,
        observer: Option<&LifecycleContext>,
        status: StatusCode,
        routing_trace: Option<RoutingTrace>,
    ) {
        if let Some(o) = observer {
            o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::RouteCompleted {
                event_id: o.event_id().to_owned(),
                result: Err(cc_lb_lifecycle::RouteFailure::RouteNoUpstreamAfterFilter),
                routing_trace,
            });
            o.set_failure(InternalFailure {
                status,
                error_code: error_codes::ROUTE_NO_UPSTREAM_AFTER_FILTER,
                error: InternalError {
                    stage: InternalErrorStage::RouterFilter,
                    kind: InternalErrorKind::Unavailable,
                    message: Some("no upstream candidates remain after routing filters".to_owned()),
                },
            });
            o.finish();
        }
    }

    fn attach_limit_headers(
        &self,
        response: &mut Response<Body>,
        active_limit: Option<&ActiveLimit>,
        provider: Provider,
    ) {
        let (Some(limit_engine), Some(active_limit)) = (self.limit_engine.as_ref(), active_limit)
        else {
            return;
        };
        attach_limit_headers_from_engine(
            response.headers_mut(),
            limit_engine.as_ref(),
            provider,
            &active_limit.subject.key_id,
            &active_limit.subject.principal_id,
        );
    }

    /// Admin fire-now warmup only. Main request path feeds
    /// `lifecycle_subscription_quota_subscriber` via UpstreamResponseStarted events.
    pub fn ingest_subscription_quota_headers(
        &self,
        headers: &HeaderMap,
        upstream_id: Uuid,
        observed_at: SystemTime,
    ) {
        if self.subscription_quota_sink.is_none() && self.subscription_quota_cache.is_none() {
            return;
        }
        let observed_at_unix_millis = system_time_to_unix_millis(observed_at);
        for record in
            build_subscription_quota_samples(headers, upstream_id, observed_at_unix_millis)
        {
            if let Some(cache) = &self.subscription_quota_cache {
                cache.upsert_observation(&record);
            }
            if let Some(sink) = &self.subscription_quota_sink {
                let _ = sink.enqueue(record);
            }
        }
    }

    /// Admin-native warmup responses use their provider-specific quota parser.
    pub fn ingest_subscription_quota_observations(
        &self,
        observations: &[cc_lb_quota::UnifiedQuotaObservation],
        upstream_id: Uuid,
        observed_at: SystemTime,
    ) {
        if self.subscription_quota_sink.is_none() && self.subscription_quota_cache.is_none() {
            return;
        }
        let observed_at_unix_millis = system_time_to_unix_millis(observed_at);
        for observation in observations {
            let record = cc_lb_quota::unified_observation_to_sample(
                upstream_id,
                observation.clone(),
                observed_at_unix_millis,
            );
            if let Some(cache) = &self.subscription_quota_cache {
                cache.upsert_observation(&record);
            }
            if let Some(sink) = &self.subscription_quota_sink {
                let _ = sink.enqueue(record);
            }
        }
    }

    fn parse(&self, req: Request<Bytes>) -> (RequestContext, Option<Box<Response<Body>>>) {
        let (mut parts, body) = req.into_parts();
        let provider_scope = parts.extensions.get::<ProviderScope>().copied();
        let path = parts.uri.path().to_owned();
        let cap = body_cap_for_path(&self.config, &path);
        let body_too_large = (body.len() > cap).then(|| {
            Box::new(anthropic_error_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                "body_too_large",
                "request body exceeds configured cap",
            ))
        });

        let request_id = header_to_string(&parts.headers, "request-id")
            .or_else(|| header_to_string(&parts.headers, "x-request-id"))
            .unwrap_or_else(next_request_id);

        let downstream_headers = {
            strip_hop_by_hop(&mut parts.headers);
            parts.headers.remove("x-cc-lb-emulate");
            parts.headers
        };
        let ctx = RequestContext::builder()
            .request_id(request_id)
            .thread_id(None)
            .requested_service_tier(None)
            .provider_scope(provider_scope.map(|scope| scope.0))
            .codex_backend(parts.extensions.get::<CodexBackendSurface>().is_some())
            .downstream_headers(downstream_headers)
            .method(parts.method)
            .path(path)
            .query(parts.uri.query().map(ToOwned::to_owned))
            .body_bytes(body)
            .cache_breakpoints(Vec::new())
            .canonical_model_id(String::new())
            .cache_pricing(CachePricingSummary::default())
            .build();
        (ctx, body_too_large)
    }

    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(
        name = "proxy.upstream_attempt",
        skip_all,
        fields(
            cc_lb.upstream.id = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
            error.type = tracing::field::Empty,
            http.response.status_code = tracing::field::Empty,
            cc_lb.upstream.bulkhead_wait_ms = tracing::field::Empty,
            cc_lb.upstream.dns_ms = tracing::field::Empty,
            cc_lb.upstream.connect_ms = tracing::field::Empty,
            cc_lb.upstream.connection_reused = tracing::field::Empty,
            cc_lb.upstream.shape_ms = tracing::field::Empty,
            cc_lb.upstream.sign_ms = tracing::field::Empty,
            cc_lb.upstream.ttfb_ms = tracing::field::Empty,
        )
    )]
    async fn attempt(
        &self,
        authenticated: &crate::authn_rail::Authenticated,
        dispatcher: &dyn UpstreamDispatch,
        attempt_scope: Scoped<'_>,
        ctx: &RequestContext,
        principal: &Principal,
        route: &RouteDecision,
        upstream_kind: StorageUpstreamKind,
        signer: Arc<dyn Signer>,
        raw_passthrough_base_url: Option<&Url>,
        observer: Option<&LifecycleContext>,
        timings: &mut AttemptTimings,
        shaped_body_out: &mut Option<Bytes>,
        cache_metadata: &RequestCacheMetadata,
        keepalive_request_started: &Arc<
            tokio::sync::OnceCell<(Instant, Option<crate::cache_keepalive::RequestStartOrder>)>,
        >,
    ) -> Result<AttemptResponse, AttemptFailure> {
        tracing::Span::current().record(
            "cc_lb.upstream.id",
            tracing::field::display(route.upstream_id),
        );
        let shape_start = Instant::now();
        *shaped_body_out = None;
        let mut shaped_ctx = ctx.clone();
        let mut compat_response_plan = None;
        if upstream_kind == StorageUpstreamKind::OpenaiOauth
            && !ctx.codex_backend
            && let Some(capability) =
                classify_provider_capability(&ctx.method, &ctx.path, ctx.provider_scope)
        {
            let prepared = if capability == ProviderCapability::OpenaiRealtimeCall {
                compat_realtime::prepare_realtime_call(ctx)
                    .await
                    .map_err(AttemptFailure::CompatReject)?
            } else {
                prepare_request(
                    authenticated,
                    ctx,
                    capability,
                    upstream_kind,
                    ctx.openai_oauth_compat
                        .as_ref()
                        .unwrap_or(&self.config.openai_oauth_compat),
                    self.config.files_body_cap_bytes,
                )
                .await
                .map_err(AttemptFailure::CompatReject)?
            };
            let realtime_conversion = capability == ProviderCapability::OpenaiRealtimeCall
                && prepared.query.as_deref()
                    == Some(crate::openai_compat::realtime::REALTIME_CALL_QUERY);
            compat_response_plan = prepared.response_plan;
            let changed = prepared.body != ctx.body_bytes
                || prepared.path != ctx.path
                || prepared.query != ctx.query;
            shaped_ctx.path = prepared.path;
            shaped_ctx.query = prepared.query;
            shaped_ctx.body_bytes = prepared.body;
            if changed {
                shaped_ctx.downstream_headers = compat_request::rewritten_headers(
                    shaped_ctx.downstream_headers,
                    shaped_ctx.body_bytes.len(),
                    true,
                );
            }
            // A converted response is parsed by cc-lb, so ask the backend for an
            // uncompressed body instead of the client's accepted encodings.
            if compat_response_plan.is_some() {
                shaped_ctx
                    .downstream_headers
                    .remove(http::header::ACCEPT_ENCODING);
            }
            if realtime_conversion {
                shaped_ctx
                    .downstream_headers
                    .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
                shaped_ctx.downstream_headers.insert(
                    HeaderName::from_static("openai-alpha"),
                    compat_realtime::realtime_call_headers(),
                );
            }
            if capability == ProviderCapability::OpenaiResponsesInputTokens {
                shaped_ctx.downstream_headers.insert(
                    HeaderName::from_static("x-cc-lb-emulate"),
                    HeaderValue::from_static("input-tokens-ws"),
                );
            }
        }
        if ctx.codex_backend
            && classify_provider_capability(&ctx.method, &ctx.path, ctx.provider_scope)
                == Some(ProviderCapability::OpenaiModels)
            && self
                .dynamic_view
                .load()
                .principal_view
                .has_model_allowlist(authenticated.principal_id())
        {
            shaped_ctx
                .downstream_headers
                .remove(http::header::ACCEPT_ENCODING);
        }
        let shape_context = shaped_ctx.dialect_shape_context();
        // Codex's backend has no hangup route. Both public and Codex-alias
        // hangups go to the Realtime origin with the OAuth token of the account
        // that created the call; affinity already pinned this attempt to it.
        let realtime_hangup = upstream_kind == StorageUpstreamKind::OpenaiOauth
            && classify_provider_capability(&ctx.method, &ctx.path, ctx.provider_scope)
                == Some(ProviderCapability::OpenaiRealtimeHangup);
        let (shaped, capture_shaped_body) = match tracing::info_span!("proxy.shape").in_scope(
            || {
                if realtime_hangup {
                    return shape_request(
                        &RawPassthroughDialect {
                            base_url: self.config.openai_realtime_origin.clone(),
                            openai_oauth: false,
                        },
                        &shape_context,
                        &route.upstream,
                        principal,
                    );
                }
                shape_request(
                    route.dialect.as_ref(),
                    &shape_context,
                    &route.upstream,
                    principal,
                )
            },
        ) {
            Ok(shaped) => (shaped, true),
            Err(source) => {
                let message = source.to_string();
                tracing::warn!(%source, "shape_request failed; falling back to raw passthrough");
                record_shape_internal_error(observer, &message);
                if matches!(
                    upstream_kind,
                    StorageUpstreamKind::VertexApiKey | StorageUpstreamKind::VertexCredential
                ) {
                    return Err(AttemptFailure::RawPassthrough(RawPassthroughError::Shape(
                        source,
                    )));
                }
                (
                    raw_passthrough_request(
                        raw_passthrough_base_url,
                        &shaped_ctx,
                        principal,
                        &route.upstream,
                        upstream_kind == StorageUpstreamKind::OpenaiOauth,
                    )?,
                    false,
                )
            }
        };
        timings.shape_ms = Some(duration_to_ms(shape_start.elapsed()));
        if let Some(shape_ms) = timings.shape_ms {
            tracing::Span::current().record(
                "cc_lb.upstream.shape_ms",
                numeric_attribute(Some(shape_ms)).as_value(),
            );
        }
        if capture_shaped_body {
            *shaped_body_out = Some(shaped.body().clone());
        }

        let sign_start = Instant::now();
        let signed = attempt_scope
            .sign(signer.as_ref(), shaped)
            .instrument(tracing::info_span!("proxy.sign"))
            .await
            .map_err(|source| {
                tracing::error!(%source, "sign_request failed");
                timings.sign_ms = Some(duration_to_ms(sign_start.elapsed()));
                let span = tracing::Span::current();
                span.record("otel.status_code", "ERROR");
                span.record("error.type", error_codes::SIGNER_FAILED);
                if let Some(sign_ms) = timings.sign_ms {
                    span.record(
                        "cc_lb.upstream.sign_ms",
                        numeric_attribute(Some(sign_ms)).as_value(),
                    );
                }
                AttemptFailure::Sign(source)
            })?;
        timings.sign_ms = Some(duration_to_ms(sign_start.elapsed()));
        if let Some(sign_ms) = timings.sign_ms {
            tracing::Span::current().record(
                "cc_lb.upstream.sign_ms",
                numeric_attribute(Some(sign_ms)).as_value(),
            );
        }

        let signed_header_names: Vec<String> = signed
            .headers()
            .keys()
            .map(|k| k.as_str().to_owned())
            .collect();
        tracing::info!(
            url = %cc_lb_observability::sanitize_url_for_log(signed.url().as_str()),
            method = %signed.method(),
            header_count = signed.headers().len(),
            headers = ?signed_header_names,
            body_bytes = signed.body_len(),
            "about to dispatch signed request"
        );

        let _ = keepalive_request_started
            .get_or_init(|| async {
                let order = self
                    .cache_keepalive
                    .on_request_started(authenticated, principal, cache_metadata)
                    .await;
                (Instant::now(), order)
            })
            .await;

        let dispatch_start = Instant::now();
        let stage_timings_carrier = Arc::new(Mutex::new(RequestStageTimings::default()));
        let dispatch_result = drive_with_activity_heartbeat(
            REQUEST_STAGE_TIMINGS.scope(stage_timings_carrier.clone(), signed.dispatch(dispatcher)),
            observer,
        )
        .await;
        let connection_snapshot = stage_timings_carrier.lock().unwrap().clone();
        timings.bulkhead_wait_ms = connection_snapshot.bulkhead_wait_ms;
        timings.dns_ms = connection_snapshot.dns_ms;
        timings.connect_ms = connection_snapshot.connect_ms;
        timings.connection_reused = connection_snapshot.connection_reused;
        if let Some(o) = observer {
            o.set_attempt_bulkhead_wait_ms(timings.bulkhead_wait_ms);
        }
        let attempt_span = tracing::Span::current();
        for (field, value) in [
            ("cc_lb.upstream.bulkhead_wait_ms", timings.bulkhead_wait_ms),
            ("cc_lb.upstream.dns_ms", timings.dns_ms),
            ("cc_lb.upstream.connect_ms", timings.connect_ms),
        ] {
            if let Some(value) = value {
                attempt_span.record(field, numeric_attribute(Some(value)).as_value());
            }
        }
        if let Some(reused) = timings.connection_reused {
            attempt_span.record("cc_lb.upstream.connection_reused", reused);
        }
        let response = dispatch_result.map_err(|source| {
            let attempt_span = tracing::Span::current();
            attempt_span.record("otel.status_code", "ERROR");
            attempt_span.record(
                "error.type",
                match &source {
                    DispatchError::BulkheadFull { .. } => "bulkhead_full",
                    DispatchError::InvalidUri { .. } => "invalid_uri",
                    DispatchError::RequestBuild { .. } => "request_build",
                    DispatchError::Transport { .. } => "transport",
                },
            );
            AttemptFailure::Dispatch(source)
        })?;
        // hyper dispatch().await resolves at response HEADERS, not full body, so this is real TTFB.
        timings.upstream_ttfb_ms = Some(duration_to_ms(dispatch_start.elapsed()));
        if let Some(ttfb_ms) = timings.upstream_ttfb_ms {
            attempt_span.record(
                "cc_lb.upstream.ttfb_ms",
                numeric_attribute(Some(ttfb_ms)).as_value(),
            );
        }
        attempt_span.record(
            "http.response.status_code",
            i64::from(response.status().as_u16()),
        );
        if response.status().is_client_error() || response.status().is_server_error() {
            attempt_span.record("otel.status_code", "ERROR");
            attempt_span.record(
                "error.type",
                if response.status().is_client_error() {
                    error_codes::UPSTREAM_4XX
                } else {
                    error_codes::UPSTREAM_5XX
                },
            );
        }
        let mut response = response;
        // The Codex backend streams every Responses-backed call as SSE but may
        // omit Content-Type. Converted plans always requested a stream upstream,
        // so label the body once here for every buffered and streamed converter.
        if upstream_kind == StorageUpstreamKind::OpenaiOauth
            && response.status().is_success()
            && !response.headers().contains_key(CONTENT_TYPE)
            && compat_response_plan.as_ref().is_some_and(|plan| {
                matches!(
                    plan.kind,
                    CompatResponseKind::ResponsesAggregate { .. }
                        | CompatResponseKind::ResponsesStream { .. }
                        | CompatResponseKind::Compact
                        | CompatResponseKind::Chat { .. }
                )
            })
        {
            response
                .headers_mut()
                .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
        }
        Ok(AttemptResponse {
            response,
            compat_response_plan,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn relay_response(
        &self,
        response: Response<Body>,
        signer: Arc<dyn Signer>,
        status: StatusCode,
        stream_requested: bool,
        native_sse_framing_error: bool,
        response_accounting_guard: Option<ResponseAccountingGuard>,
        event_ctx: RequestEventContext,
        transform_ctx: ResponseTransformContext,
        compat_response_plan: Option<CompatResponsePlan>,
        prompt_cache_observation_context: Option<PromptCacheObservationContext>,
        response_affinity_context: ResponseAffinityContext,
        observer: Option<LifecycleContext>,
        keepalive_completion: Option<crate::cache_keepalive::LifecycleKeepaliveContext>,
        dispatch_started: Instant,
        dispatch_unix_secs: u64,
    ) -> Response<Body> {
        let (mut parts, mut body) = response.into_parts();
        let upstream_has_content_length = parts.headers.contains_key(CONTENT_LENGTH);
        let upstream_diagnostics = UpstreamBodyDiagnostics::from_parts(&parts);
        strip_hop_by_hop(&mut parts.headers);
        let decompression_output_budget_bytes =
            decompression_output_budget_bytes(self.config.messages_body_cap_bytes);
        let incomplete_sse_event_budget_bytes =
            incomplete_sse_event_budget_bytes(self.config.messages_body_cap_bytes);
        let usage_decoder = if native_sse_framing_error {
            // The request/response framing mismatch is known from headers;
            // reject it without reading or decoding the upstream JSON body.
            UsageDecoder::Identity
        } else {
            UsageDecoder::from_headers(&parts.headers, decompression_output_budget_bytes)
        };
        let downstream_stream_is_identity = matches!(&usage_decoder, UsageDecoder::Identity);
        let upstream_is_sse = is_sse_response(
            &parts.headers,
            stream_requested,
            response_affinity_context.capability,
        );
        if native_sse_framing_error {
            sanitize_downstream_stream_headers(&mut parts.headers);
            parts
                .headers
                .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
        }
        let success_sse_affinity_gate = upstream_is_sse
            && status.is_success()
            && (response_affinity_context.observe_response_keys
                || !response_affinity_context.pending_request_keys.is_empty());
        // Anthropic responses can introduce provider-owned state (container
        // ids, Files API ids, web-search `encrypted_content`) without any
        // request-side hint. Those streams skip the request-hinted gate; only
        // events that actually carry new provider state are bound to the
        // selected upstream before their raw bytes are forwarded.
        let unsolicited_sse_affinity_inspection = upstream_is_sse
            && status.is_success()
            && !success_sse_affinity_gate
            && response_affinity_context.capability == ProviderCapability::AnthropicMessages;
        let compat_transform_requested = compat_response_plan.as_ref().is_some_and(|plan| {
            compat_response::normalize_stream_events(plan)
                || matches!(
                    &plan.kind,
                    CompatResponseKind::Chat { plan } if plan.stream
                )
        });
        let transform_requested = upstream_is_sse
            && (compat_transform_requested
                || transform_ctx.dialect.sse_event_transform_hook().is_some()
                || transform_ctx.gemini_oauth);
        let transform_decode_supported = usage_decoder.unsupported_encoding().is_none();
        // Uncompressed, untransformed unsolicited inspection holds raw chunks
        // only while the unforwarded bytes may belong to a provider-state event;
        // such an event is relayed whole only after its state is bound, so a
        // state-carrying event split across upstream chunks never reaches the
        // client early. Every other event keeps the raw chunk passthrough, and
        // compressed and transform streams keep their existing relay paths.
        let unsolicited_sse_affinity_hold_eligible = unsolicited_sse_affinity_inspection
            && downstream_stream_is_identity
            && !(transform_requested && transform_decode_supported);
        let response_headers = (transform_requested && transform_decode_supported)
            .then(|| sanitized_response_headers_for_plugin(&parts.headers));
        if (transform_requested && transform_decode_supported)
            || (success_sse_affinity_gate
                && transform_decode_supported
                && !downstream_stream_is_identity)
        {
            sanitize_downstream_stream_headers(&mut parts.headers);
        }
        if upstream_is_sse || native_sse_framing_error {
            // SSE inspection can replace, append, or suppress bytes even when
            // no transform hook is active. The upstream length is therefore
            // not a downstream framing contract. Preserve Content-Encoding
            // unless the decode/transform path above already removed it.
            parts.headers.remove(CONTENT_LENGTH);
        }
        if let Some(encoding) = usage_decoder.unsupported_encoding() {
            tracing::warn!(
                request_id = %event_ctx.request_id,
                content_encoding = encoding,
                "skipping streaming usage extraction: unsupported content-encoding"
            );
        }
        let relay_start = Instant::now();
        let keepalive = self.cache_keepalive.clone();
        let affinity_clock = self.clock.clone();
        let session_hint_cache = self.openai_session_hint.clone();
        let upstream_error_status = status.is_client_error() || status.is_server_error();
        if upstream_error_status && let Some(o) = observer.as_ref() {
            let code = if status.is_client_error() {
                UpstreamErrorCode::Upstream4xx
            } else {
                UpstreamErrorCode::Upstream5xx
            };
            o.set_upstream_error(status, code);
        }
        let stream_span = tracing::info_span!(
            "proxy.response_stream",
            otel.kind = "internal",
            otel.status_code = tracing::field::Empty,
            http.response.status_code = tracing::field::Empty,
            cc_lb.response_body_ms = tracing::field::Empty,
            cc_lb.request.finalize_ms = tracing::field::Empty,
            stream.outcome = tracing::field::Empty,
            error.cause = tracing::field::Empty,
            error.chain = tracing::field::Empty,
            error.io.kind = tracing::field::Empty,
            error.io.os_error = tracing::field::Empty,
            error.h2.reason = tracing::field::Empty,
            stream_first_chunk_ms = tracing::field::Empty,
            stream_message_start_ms = tracing::field::Empty,
            stream_content_block_start_ms = tracing::field::Empty,
            stream_first_content_delta_ms = tracing::field::Empty,
            stream_last_content_delta_ms = tracing::field::Empty,
            stream_message_stop_ms = tracing::field::Empty,
            stream_last_chunk_ms = tracing::field::Empty,
            stream_total_ms = tracing::field::Empty,
            sse_event_count = tracing::field::Empty,
            content_delta_count = tracing::field::Empty,
            ping_count = tracing::field::Empty,
            inter_token_avg_ms = tracing::field::Empty,
            total_bytes = tracing::field::Empty,
            cc_lb.request.outcome = tracing::field::Empty,
            cc_lb.request.duration_ms = tracing::field::Empty,
            cc_lb.request.retry_count = tracing::field::Empty,
            cc_lb.request.unaccounted_ms = tracing::field::Empty,
            error.type = tracing::field::Empty,
            cc_lb.upstream.bulkhead_wait_ms = tracing::field::Empty,
            cc_lb.upstream.dns_ms = tracing::field::Empty,
            cc_lb.upstream.connect_ms = tracing::field::Empty,
            cc_lb.upstream.connection_reused = tracing::field::Empty,
            cc_lb.upstream.shape_ms = tracing::field::Empty,
            cc_lb.upstream.sign_ms = tracing::field::Empty,
            cc_lb.upstream.ttfb_ms = tracing::field::Empty,
            cc_lb.response.first_body_chunk_ms = tracing::field::Empty,
            cc_lb.response.first_content_delta_ms = tracing::field::Empty,
            gen_ai.usage.input_tokens = tracing::field::Empty,
            gen_ai.usage.output_tokens = tracing::field::Empty,
            gen_ai.usage.cache_creation_input_tokens = tracing::field::Empty,
            gen_ai.usage.cache_read_input_tokens = tracing::field::Empty,
            cc_lb.usage.completeness = tracing::field::Empty,
        );
        if let Some(o) = observer.as_ref() {
            o.set_response_stream_span(stream_span.clone());
        }
        let stream_latency_log_dispatch = tracing::enabled!(
            target: "cc_lb_engine::lifecycle",
            tracing::Level::INFO
        )
        .then(|| tracing::dispatcher::get_default(|dispatch| dispatch.clone()));
        let response_body_timing = BodyIoTiming::default();
        let downstream_drop_guard = DownstreamStreamDropGuard::armed(
            observer.clone(),
            stream_span.clone(),
            status,
            upstream_error_status.then_some(StreamTerminationCause::ProviderError),
            relay_start,
            response_body_timing.clone(),
            StreamTerminationAttribution::new(
                &event_ctx.upstream_name,
                event_ctx.requested_model.as_deref(),
            ),
        )
        .with_response_accounting_guard(response_accounting_guard)
        .with_keepalive_completion(keepalive, keepalive_completion);
        let completion_observer = Arc::clone(&self.completion_observer);
        let explicit_non_sse_non_json = parts.headers.contains_key(CONTENT_TYPE)
            && !upstream_is_sse
            && !has_json_content_type(&parts.headers);
        let parse_sse_events =
            upstream_is_sse || (!upstream_error_status && !explicit_non_sse_non_json);
        let stream_body_io_timing = response_body_timing.clone();
        let downstream_poll_timing = response_body_timing;
        let has_retry_after_header = parts.headers.get(RETRY_AFTER).is_some();
        let chat_sse_hook = compat_response_plan.as_ref().and_then(|plan| {
            if let CompatResponseKind::Chat { plan } = &plan.kind
                && plan.stream
            {
                Some(crate::openai_compat::chat::ChatSseTransformHook::new(
                    plan.clone(),
                ))
            } else {
                None
            }
        });
        let compat_sse_hook: Option<Box<dyn SseEventTransformHook>> =
            compat_response_plan.as_ref().and_then(|plan| {
                compat_response::normalize_stream_events(plan).then(|| {
                    Box::new(compat_response::CompatSseTransformHook {
                        echo_metadata: compat_response::stream_echo_metadata(plan).cloned(),
                        normalize_events: true,
                    }) as Box<dyn SseEventTransformHook>
                })
            });
        let stream = async_stream::stream! {
            let custom_sse_hook = transform_ctx.dialect.sse_event_transform_hook();
            let composed_sse_hook = custom_sse_hook.map(|custom| {
                ComposedSseResponseTransformHook {
                    gemini: &GEMINI_OAUTH_RESPONSE_TRANSFORM,
                    custom,
                }
            });
            let sse_transform_hook: Option<&dyn SseEventTransformHook> =
                if let Some(chat) = chat_sse_hook.as_ref() {
                    Some(chat as &dyn SseEventTransformHook)
                } else if let Some(compat) = compat_sse_hook.as_ref() {
                    Some(compat.as_ref() as &dyn SseEventTransformHook)
                } else if transform_ctx.gemini_oauth {
                    composed_sse_hook
                        .as_ref()
                        .map(|hook| hook as &dyn SseEventTransformHook)
                        .or(Some(
                            &GEMINI_OAUTH_RESPONSE_TRANSFORM as &dyn SseEventTransformHook,
                        ))
                } else {
                    custom_sse_hook
                };
            let mut downstream_drop_guard = downstream_drop_guard;
            let mut usage_decoder = usage_decoder;
            let mut upstream_diagnostics = upstream_diagnostics;
            let mut batch_index = 0_u64;
            let mut buffer = BytesMut::new();
            let precomputed_usage_present = response_affinity_context.precomputed_usage.is_some();
            let mut usage = response_affinity_context
                .precomputed_usage
                .clone()
                .unwrap_or_default();
            let mut responses_error_context = ResponsesSseErrorContext::default();
            let mut first_chunk_at: Option<Instant> = None;
            let mut last_chunk_at: Option<Instant> = None;
            let mut last_activity_heartbeat_at = relay_start;
            let mut activity_heartbeat = new_activity_heartbeat_interval();
            let mut message_start_at: Option<Instant> = None;
            let mut content_block_start_at: Option<Instant> = None;
            let mut first_content_delta_at: Option<Instant> = None;
            let mut last_content_delta_at: Option<Instant> = None;
            let mut message_stop_at: Option<Instant> = None;
            // #663: semantic stop uses the JSON `type`; legacy message-stop
            // timing still follows the event name or usage parser fallback.
            let mut prompt_cache_observations_published = false;
            let mut semantic_message_stop_seen = false;
            let mut semantic_completion_observed = false;
            let mut semantic_completion_emitted = false;
            // A finished choice can precede the final usage chunk. Only an
            // emitted [DONE] permits accepting a later Chat body fault.
            let mut chat_done_emitted = false;
            let mut sse_event_count: u64 = 0;
            let mut content_delta_count: u64 = 0;
            let mut ping_count: u64 = 0;
            let mut total_bytes: u64 = 0;
            let mut sse_transform_active = transform_requested && transform_decode_supported;
            let mut transformed_output_started = false;
            let mut stream_transform_error: Option<ResponseTransformError> = None;
            let mut stream_upstream_error: Option<cc_lb_lifecycle::StreamError> = None;
            let mut stream_affinity_error: Option<cc_lb_lifecycle::StreamError> = None;
            let mut bound_response_affinity_keys = HashSet::new();
            let mut pending_request_affinity_keys =
                response_affinity_context.pending_request_keys.clone();
            let mut stream_upstream_error_frame_emitted = false;
            let mut downstream_sse_boundary = SseDownstreamBoundaryTracker::default();
            let mut stream_provider_error_seen = false;
            // Cloud Code Assist answers a rejected `streamGenerateContent`
            // with a plain JSON error under `text/event-stream`. An error
            // status body on a transformed stream is therefore held unparsed
            // until its first non-whitespace byte shows whether it can be
            // JSON; SSE splitting would otherwise consume blank lines inside
            // the JSON and replace the provider error with a framing error.
            let mut error_body_class = if upstream_error_status && sse_transform_active {
                ErrorBodyClass::Pending
            } else {
                ErrorBodyClass::Sse
            };
            // Observation precedes affinity inspection and transformation; only
            // a validated terminal sent downstream or committed to the deferred
            // Content-Length flush takes precedence over local errors.
            let mut stream_provider_error_forwarded = false;
            let mut last_partial_at: Option<Instant> = None;
            let mut last_partial_output_tokens: u64 = 0;
            let mut upstream_error_body: Vec<u8> = Vec::new();
            let mut upstream_error_body_truncated = false;
            let mut upstream_error_body_decode_failed = false;
            let mut parse_sse_events_active = parse_sse_events;
            let mut raw_before_transform_output: Vec<Bytes> = Vec::new();
            let mut keepalive_raw_passthrough_active = false;
            // Defer bytes produced from the final upstream Content-Length frame
            // until lifecycle finalization. Keep this upstream EOF bookkeeping
            // independent of the downstream framing, which permits rewritten
            // SSE bytes and no longer advertises the upstream length.
            let mut deferred_terminal_chunks: Vec<Bytes> = Vec::new();
            // Commit Gemini framing failures before exposing their terminal error frame.
            let mut deferred_gemini_framing_error: Option<Bytes> = None;
            // Dynamic unsolicited hold: `unsolicited_hold_pending` is set while
            // the unforwarded buffered bytes may carry provider state, and
            // `unsolicited_forwarded_bytes` counts the leading bytes of the
            // buffered partial event the raw passthrough already relayed.
            let mut unsolicited_hold_pending = false;
            let mut unsolicited_forwarded_bytes: usize = 0;
            // #663: true only once the upstream body stream delivered its final
            // frame. A decoder finish failure after cc-lb aborted the loop
            // locally is not upstream evidence.
            let mut upstream_body_eof = false;
            // A held JSON candidate finishes a compressed decoder at EOF so its
            // decision sees the whole body. Only a complete JSON document
            // absorbs the tail there; otherwise the result (tail or failure)
            // goes to the normal post-loop handling, exactly as without a hold.
            let mut early_decoder_finish: Option<std::io::Result<Vec<u8>>> = None;
            'upstream: loop {
                if native_sse_framing_error || upstream_body_eof {
                    // Reject known framing mismatches before polling; only a held-body replay runs after EOF.
                    break;
                }
                let frame = match timed_body_frame_with_heartbeat(
                    &mut body,
                    &stream_body_io_timing,
                    observer.as_ref(),
                    &mut last_activity_heartbeat_at,
                    &mut activity_heartbeat,
                )
                .await
                {
                    Some(frame) => frame,
                    None => {
                        upstream_body_eof = true;
                        if sse_transform_active
                            && let Some(chat_hook) = chat_sse_hook.as_ref()
                            && let Some(event) = chat_hook.truncated_error_event()
                        {
                            stream_provider_error_seen = true;
                            stream_provider_error_forwarded = true;
                            yield Ok::<Bytes, Infallible>(
                                crate::openai_compat::chat::sse_event_bytes(&event),
                            );
                        }
                        let mut held_json = error_body_class == ErrorBodyClass::JsonCandidate;
                        if held_json && !matches!(usage_decoder, UsageDecoder::Identity) {
                            let finished =
                                std::mem::replace(&mut usage_decoder, UsageDecoder::Identity)
                                    .finish();
                            held_json = match &finished {
                                Ok(tail) => {
                                    let mut whole = Vec::with_capacity(buffer.len() + tail.len());
                                    whole.extend_from_slice(&buffer);
                                    whole.extend_from_slice(tail);
                                    held_error_body_is_json(&whole)
                                }
                                Err(_) => false,
                            };
                            early_decoder_finish = Some(match finished {
                                Ok(tail) if held_json => {
                                    if upstream_error_status {
                                        append_bounded_upstream_error_body(
                                            &mut upstream_error_body,
                                            &tail,
                                            &mut upstream_error_body_truncated,
                                        );
                                    }
                                    buffer.extend_from_slice(&tail);
                                    Ok(Vec::new())
                                }
                                other => other,
                            });
                        } else if held_json {
                            held_json = held_error_body_is_json(&buffer);
                        }
                        // EOF ends JSON candidacy. A held body that is not one
                        // complete document is parsed as SSE through this chunk
                        // path, exactly where it would have been without the
                        // hold; only a real remainder reaches the
                        // incomplete-event check.
                        if error_body_class == ErrorBodyClass::JsonCandidate && !held_json {
                            error_body_class = ErrorBodyClass::Sse;
                            Ok(http_body::Frame::data(Bytes::new()))
                        } else {
                            break;
                        }
                    }
                };
                match frame {
                    Ok(frame) => {
                        if let Ok(data) = frame.into_data() {
                            let defer_terminal_raw_chunk =
                                upstream_has_content_length && body.is_end_stream();
                            let mut raw_passthrough_current_chunk = false;
                            let mut raw_chunk_completes_response = false;
                            let mut raw_chunk_done = false;
                            let mut provider_error_in_chunk = false;
                            let now = Instant::now();
                            maybe_emit_chunk_heartbeat(
                                now,
                                &mut last_activity_heartbeat_at,
                                &mut activity_heartbeat,
                                observer.as_ref(),
                            );
                            if first_chunk_at.is_none() {
                                first_chunk_at = Some(now);
                                if let Some(o) = observer.as_ref() {
                                    o.set_termination_timings(
                                        None,
                                        None,
                                        Some(duration_to_ms(now.saturating_duration_since(relay_start))),
                                    );
                                }
                            }
                            last_chunk_at = Some(now);
                            total_bytes = total_bytes.saturating_add(data.len() as u64);
                            if success_sse_affinity_gate && !transform_decode_supported {
                                stream_upstream_error = Some(cc_lb_lifecycle::StreamError {
                                    error_type: UPSTREAM_RESPONSE_DECODE_ERROR_TYPE.to_owned(),
                                    error_message: UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE.to_owned(),
                                    ..Default::default()
                                });
                                let frame = make_stream_error_frame(
                                    response_affinity_context.capability,
                                    &responses_error_context,
                                    UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                    UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE,
                                    false,
                                );
                                if let Some(o) = observer.as_ref() {
                                    o.set_upstream_error(
                                        StatusCode::OK,
                                        UpstreamErrorCode::StreamError,
                                    );
                                }
                                downstream_drop_guard.mark_upstream_error(
                                    StreamTerminationCause::DecodeError,
                                    None,
                                );
                                yield Ok::<Bytes, Infallible>(frame);
                                break 'upstream;
                            }
                            match usage_decoder.push(&data) {
                                Ok(plaintext) => {
                                    if upstream_error_status {
                                        append_bounded_upstream_error_body(
                                            &mut upstream_error_body,
                                            &plaintext,
                                            &mut upstream_error_body_truncated,
                                        );
                                    }
                                    if parse_sse_events_active {
                                        buffer.extend_from_slice(&plaintext);
                                        if unsolicited_sse_affinity_hold_eligible
                                            && !unsolicited_hold_pending
                                        {
                                            let probe_start = unsolicited_forwarded_bytes
                                                .saturating_sub(
                                                    ANTHROPIC_PROVIDER_STATE_MARKER_PROBE_BYTES,
                                                )
                                                .min(buffer.len());
                                            unsolicited_hold_pending =
                                                sse_bytes_may_carry_anthropic_provider_state(
                                                    &buffer[probe_start..],
                                                );
                                        }
                                    }
                                }
                                Err(error) => {
                                    let upstream_decode_failed =
                                        is_upstream_response_decode_error(&error);
                                    tracing::warn!(
                                        parent: &stream_span,
                                        request_id = %event_ctx.request_id,
                                        %error,
                                        upstream_decode_failed,
                                        "skipping streaming usage extraction: chunk decode failed"
                                    );
                                    upstream_diagnostics.record_body_end(
                                        cc_lb_lifecycle::UpstreamBodyEnd::DecodeErrorMidBody,
                                        || classify_stream_error(&error),
                                    );
                                    // #663: once cc-lb parsed the upstream message_stop and it is
                                    // the SSE emitter of record, a decode failure on a later chunk
                                    // completes the stream with a warning instead of an error.
                                    // The plaintext of the failing chunk is discarded, so a
                                    // message_stop carried by that same chunk stays fatal.
                                    if upstream_decode_failed
                                        && (success_sse_affinity_gate || sse_transform_active)
                                        && upstream_end_after_message_stop_is_acceptable(
                                            status,
                                            match response_affinity_context.capability {
                                                ProviderCapability::OpenaiChatCompletions => chat_done_emitted,
                                                ProviderCapability::OpenaiResponses
                                                | ProviderCapability::OpenaiResponsesCompact => semantic_completion_emitted,
                                                _ => semantic_message_stop_seen,
                                            },
                                            stream_upstream_error.is_some()
                                                || stream_transform_error.is_some()
                                                || stream_affinity_error.is_some(),
                                            stream_provider_error_seen
                                                || (response_affinity_context.capability == ProviderCapability::OpenaiChatCompletions
                                                    && stream_provider_error_forwarded),
                                            upstream_diagnostics.warning_recorded(),
                                            &buffer,
                                        )
                                    {
                                        let classification = classify_stream_error(&error);
                                        tracing::warn!(
                                            parent: &stream_span,
                                            request_id = %event_ctx.request_id,
                                            error_chain = %classification.redacted_chain,
                                            warning_type = UPSTREAM_RESPONSE_DECODE_ERROR_TYPE,
                                            "accepting upstream SSE response after message_stop despite chunk decode failure"
                                        );
                                        upstream_diagnostics.record_warning(
                                            UPSTREAM_RESPONSE_DECODE_ERROR_TYPE,
                                            truncate_reason(&format!(
                                                "upstream response decoding failed: {error}"
                                            )),
                                        );
                                        upstream_error_body_decode_failed = true;
                                        parse_sse_events_active = false;
                                        buffer.clear();
                                        break 'upstream;
                                    }
                                    upstream_error_body_decode_failed = true;
                                    parse_sse_events_active = false;
                                    buffer.clear();
                                    if stream_provider_error_forwarded {
                                        break 'upstream;
                                    }
                                    if success_sse_affinity_gate {
                                        if !stream_provider_error_seen {
                                            stream_upstream_error =
                                                Some(cc_lb_lifecycle::StreamError {
                                                    error_type:
                                                        UPSTREAM_RESPONSE_DECODE_ERROR_TYPE.to_owned(),
                                                    error_message:
                                                        UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE
                                                            .to_owned(),
                                                    ..Default::default()
                                                });
                                        }
                                        let frame = make_stream_error_frame(
                                            response_affinity_context.capability,
                                            &responses_error_context,
                                            UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                            UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE,
                                            false,
                                        );
                                        if let Some(o) = observer.as_ref() {
                                            o.set_upstream_error(
                                                StatusCode::OK,
                                                UpstreamErrorCode::StreamError,
                                            );
                                        }
                                        if !stream_provider_error_seen {
                                            downstream_drop_guard.mark_upstream_error(
                                                StreamTerminationCause::DecodeError,
                                                None,
                                            );
                                        }
                                        stream_upstream_error_frame_emitted = true;
                                        yield Ok::<Bytes, Infallible>(frame);
                                        break 'upstream;
                                    }
                                    if upstream_decode_failed && sse_transform_active {
                                        let error_message =
                                            format!("upstream response decoding failed: {error}");
                                        if !stream_provider_error_seen {
                                            stream_upstream_error =
                                                Some(cc_lb_lifecycle::StreamError {
                                                    error_type:
                                                        UPSTREAM_RESPONSE_DECODE_ERROR_TYPE.to_owned(),
                                                    error_message: error_message.clone(),
                                                    ..Default::default()
                                                });
                                        }
                                        if transformed_output_started
                                            || !downstream_stream_is_identity
                                        {
                                            let frame = make_stream_error_frame(
                                                response_affinity_context.capability,
                                                &responses_error_context,
                                                UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                                &error_message,
                                                false,
                                            );
                                            stream_upstream_error_frame_emitted = true;
                                            if let Some(o) = observer.as_ref()
                                                && !upstream_error_status
                                            {
                                                o.set_upstream_error(
                                                    status,
                                                    UpstreamErrorCode::StreamError,
                                                );
                                            }
                                            if !stream_provider_error_seen {
                                                downstream_drop_guard.mark_upstream_error(
                                                    StreamTerminationCause::DecodeError,
                                                    None,
                                                );
                                            }
                                            yield Ok::<Bytes, Infallible>(frame);
                                            break 'upstream;
                                        }
                                    }
                                    if sse_transform_active {
                                        if transformed_output_started
                                            || !downstream_stream_is_identity
                                        {
                                            let transform_error =
                                                ResponseTransformError::Runtime {
                                                    reason: if transformed_output_started {
                                                        format!(
                                                            "streaming response decode failed after transform output: {error}"
                                                        )
                                                    } else {
                                                        format!(
                                                            "streaming response decode failed before transform output: {error}"
                                                        )
                                                    },
                                                };
                                            let frame =
                                                make_response_transform_error_frame(
                                                    response_affinity_context.capability,
                                                    &responses_error_context,
                                                    &transform_error,
                                                );
                                            stream_transform_error = Some(transform_error);
                                            if let Some(o) = observer.as_ref()
                                                && !upstream_error_status
                                            {
                                                o.set_upstream_error(
                                                    StatusCode::OK,
                                                    UpstreamErrorCode::StreamError,
                                                );
                                            }
                                            downstream_drop_guard.mark_proxy_error(
                                                StreamTerminationCause::TransformError,
                                            );
                                            yield Ok::<Bytes, Infallible>(frame);
                                            break 'upstream;
                                        }
                                        debug_assert!(downstream_stream_is_identity);
                                        sse_transform_active = false;
                                        keepalive_raw_passthrough_active = true;
                                        raw_passthrough_current_chunk = true;
                                    }
                                }
                            }
                            let hold_unsolicited_chunk = unsolicited_hold_pending;
                            let mut unsolicited_event_completed = false;
                                while !raw_passthrough_current_chunk {
                                    if error_body_class != ErrorBodyClass::Sse {
                                        error_body_class =
                                            classify_error_body(error_body_class, &buffer);
                                        if error_body_class == ErrorBodyClass::JsonCandidate {
                                            break;
                                        }
                                    }
                                    let Some(end) = usage_parser::find_sse_event_end(&buffer) else {
                                        break;
                                    };
                                let raw = split_sse_event(&mut buffer, end);
                                let parsed_event = parse_sse_event(&raw);
                                if matches!(
                                    response_affinity_context.capability,
                                    ProviderCapability::OpenaiResponses
                                        | ProviderCapability::OpenaiResponsesCompact
                                ) {
                                    responses_error_context.observe(
                                        parsed_event.value().ok().flatten(),
                                    );
                                }
                                let usage_update = usage_parser::accumulate_sse_usage_for_shape(
                                    &parsed_event,
                                    &mut usage,
                                    if precomputed_usage_present {
                                        crate::provider_capability::UsageShape::None
                                    } else {
                                        response_affinity_context
                                            .usage_shape
                                            .unwrap_or_else(|| {
                                                response_affinity_context.capability.usage_shape()
                                            })
                                    },
                                );
                                downstream_drop_guard.set_keepalive_cached_tokens(
                                    usage
                                        .cache_read_input_tokens
                                        .saturating_add(usage.cache_creation_input_tokens),
                                );
                                if keepalive_event_reaches_upstream_terminal(
                                    response_affinity_context.capability,
                                    &parsed_event,
                                ) {
                                    downstream_drop_guard.mark_upstream_terminal();
                                }
                                let mut provider_error_in_event = false;
                                if let Some(err) =
                                    usage_parser::detect_mid_stream_error(&parsed_event, signer.as_ref())
                                {
                                    provider_error_in_event = true;
                                    provider_error_in_chunk = true;
                                    stream_provider_error_seen = true;
                                    downstream_drop_guard.mark_upstream_error(
                                        StreamTerminationCause::ProviderError,
                                        None,
                                    );
                                    if let Some(o) = observer.as_ref() {
                                        let error = cc_lb_lifecycle::StreamError {
                                            error_type: err.error_type.clone().unwrap_or_default(),
                                            error_message: err
                                                .error_message
                                                .clone()
                                                .unwrap_or_default(),
                                            upstream_error_code: err.upstream_error_code.clone(),
                                            ..Default::default()
                                        };
                                        o.emit_lifecycle(
                                            cc_lb_lifecycle::LifecycleEvent::StreamCompleted {
                                                event_id: o.event_id().to_owned(),
                                                result: Err(error),
                                            },
                                        );
                                        if !upstream_error_status {
                                            o.set_upstream_error(
                                                StatusCode::OK,
                                                UpstreamErrorCode::StreamError,
                                            );
                                        }
                                    }
                                }
                                if !downstream_drop_guard.has_abnormal_stop()
                                    && let Some(stop) =
                                        usage_parser::detect_abnormal_stop(&parsed_event)
                                {
                                    let error_message = abnormal_stop_error_message(&stop);
                                    downstream_drop_guard
                                        .mark_abnormal_stop(stop.stop_reason, error_message);
                                }
                                sse_event_count = sse_event_count.saturating_add(1);
                                let mut affinity_persist_failed = false;
                                let affinity_binding = if success_sse_affinity_gate {
                                    let response_keys = match
                                        extract_response_affinity_keys_from_sse_event(
                                            response_affinity_context.capability,
                                            &parsed_event,
                                            &response_affinity_context.principal_id,
                                        )
                                    {
                                        Ok(keys) => keys,
                                        Err(_) => {
                                            tracing::warn!(
                                                parent: &stream_span,
                                                "failed to inspect upstream affinity in SSE event"
                                            );
                                            if !stream_provider_error_forwarded {
                                                stream_upstream_error =
                                                    Some(cc_lb_lifecycle::StreamError {
                                                        error_type:
                                                            UPSTREAM_RESPONSE_FRAMING_ERROR_TYPE
                                                                .to_owned(),
                                                        error_message:
                                                            UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE
                                                                .to_owned(),
                                                        ..Default::default()
                                                    });
                                            }
                                            parse_sse_events_active = false;
                                            buffer.clear();
                                            raw_before_transform_output.clear();
                                            if !stream_provider_error_forwarded {
                                                let frame = make_stream_error_frame(
                                                    response_affinity_context.capability,
                                                    &responses_error_context,
                                                    UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                                    UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE,
                                                    false,
                                                );
                                                batch_index = batch_index.saturating_add(1);
                                                if let Some(o) = observer.as_ref() {
                                                    o.set_upstream_error(
                                                        StatusCode::OK,
                                                        UpstreamErrorCode::StreamError,
                                                    );
                                                }
                                                downstream_drop_guard.mark_upstream_error(
                                                    StreamTerminationCause::FramingError,
                                                    None,
                                                );
                                                for chunk in
                                                    std::mem::take(&mut deferred_terminal_chunks)
                                                {
                                                    yield Ok::<Bytes, Infallible>(chunk);
                                                }
                                                yield Ok::<Bytes, Infallible>(frame);
                                            }
                                            break 'upstream;
                                        }
                                    };
                                    let bind_pending_request = sse_event_completes_response(
                                        response_affinity_context.capability,
                                        &parsed_event,
                                    );
                                    Some((response_keys, bind_pending_request))
                                } else if unsolicited_sse_affinity_inspection {
                                    match unsolicited_anthropic_sse_affinity_keys(
                                        &parsed_event,
                                        &response_affinity_context.principal_id,
                                        &bound_response_affinity_keys,
                                    ) {
                                        Ok(keys) if keys.is_empty() => None,
                                        Ok(keys) => Some((keys, false)),
                                        Err(_) => {
                                            affinity_persist_failed = true;
                                            None
                                        }
                                    }
                                } else {
                                    None
                                };
                                if let Some((response_keys, bind_pending_request)) =
                                    affinity_binding
                                {
                                    let mut affinity_keys = response_keys
                                        .iter()
                                        .filter(|key| {
                                            !bound_response_affinity_keys.contains(*key)
                                        })
                                        .cloned()
                                        .collect::<Vec<_>>();
                                    if bind_pending_request {
                                        affinity_keys.extend(
                                            pending_request_affinity_keys.iter().cloned(),
                                        );
                                    }
                                    if bind_upstream_affinity_keys(
                                        &response_affinity_context,
                                        &affinity_keys,
                                        unix_secs(affinity_clock.now()),
                                        None,
                                    )
                                    .await
                                    .is_err()
                                    {
                                        affinity_persist_failed = true;
                                    } else {
                                        bound_response_affinity_keys.extend(response_keys);
                                        if bind_pending_request {
                                            pending_request_affinity_keys.clear();
                                        }
                                    }
                                }
                                if affinity_persist_failed {
                                    tracing::warn!(
                                        parent: &stream_span,
                                        "failed to persist upstream affinity before SSE event"
                                    );
                                    if !stream_provider_error_forwarded {
                                        stream_affinity_error = Some(affinity_stream_error());
                                    }
                                    parse_sse_events_active = false;
                                    buffer.clear();
                                    raw_before_transform_output.clear();
                                    if !stream_provider_error_forwarded
                                        && (success_sse_affinity_gate
                                            || sse_transform_active
                                            || downstream_stream_is_identity)
                                    {
                                        let frame = make_stream_error_frame(
                                            response_affinity_context.capability,
                                            &responses_error_context,
                                            UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                            UPSTREAM_AFFINITY_UNAVAILABLE_MESSAGE,
                                            false,
                                        );
                                        batch_index = batch_index.saturating_add(1);
                                        for chunk in std::mem::take(&mut deferred_terminal_chunks) {
                                            yield Ok::<Bytes, Infallible>(chunk);
                                        }
                                        yield Ok::<Bytes, Infallible>(frame);
                                    }
                                    if !stream_provider_error_forwarded {
                                        if let Some(o) = observer.as_ref() {
                                            o.set_upstream_error(
                                                StatusCode::OK,
                                                UpstreamErrorCode::StreamError,
                                            );
                                        }
                                        downstream_drop_guard.mark_proxy_error(
                                            StreamTerminationCause::AffinityError,
                                        );
                                    }
                                    break 'upstream;
                                }
                                if !sse_transform_active
                                    && downstream_drop_guard.observes_keepalive_response()
                                {
                                    downstream_drop_guard.observe_keepalive(&parsed_event);
                                }
                                let semantic_content_frame = is_semantic_content_frame(
                                    response_affinity_context.capability,
                                    &parsed_event,
                                );
                                if semantic_content_frame {
                                    if first_content_delta_at.is_none() {
                                        first_content_delta_at = Some(now);
                                        if let Some(o) = observer.as_ref() {
                                            o.set_first_content_delta_ms(duration_to_ms(
                                                now.saturating_duration_since(relay_start),
                                            ));
                                        }
                                    }
                                    last_content_delta_at = Some(now);
                                    content_delta_count =
                                        content_delta_count.saturating_add(1);
                                }
                                observe_sse_lifecycle_markers(
                                    response_affinity_context.capability,
                                    &parsed_event,
                                    &mut message_start_at,
                                    &mut content_block_start_at,
                                    &mut ping_count,
                                    Some(&mut downstream_drop_guard),
                                    now,
                                );
                                if usage_update.message_start_usage {
                                    if message_start_at.is_none() {
                                        message_start_at = Some(now);
                                        downstream_drop_guard.set_keepalive_anchor_at(now);
                                    }
                                    if status == StatusCode::OK
                                        && !prompt_cache_observations_published
                                        && let Some(context) =
                                            prompt_cache_observation_context.as_ref()
                                    {
                                        let now_unix_secs = response_start_unix_secs(
                                            dispatch_unix_secs,
                                            dispatch_started,
                                            message_start_at,
                                        );
                                        let decode = decode_prompt_cache_observations_pure(
                                            context,
                                            PromptCacheUsage::from(&usage),
                                            dispatch_unix_secs,
                                        );
                                        // Early direct publish: valid usage evidence is
                                        // enqueued at message_start, before any downstream
                                        // disconnect or message_stop can drop it.
                                        publish_prompt_cache_observations(
                                            context,
                                            &decode,
                                            now_unix_secs,
                                        );
                                        prompt_cache_observations_published = true;
                                    }
                                }
                                observe_sse_terminal_event(
                                    response_affinity_context.capability,
                                    &parsed_event,
                                    usage_update.message_stop,
                                    &mut semantic_message_stop_seen,
                                    &mut message_stop_at,
                                    || now,
                                );
                                let event_completes_response = sse_event_completes_response(
                                    response_affinity_context.capability,
                                    &parsed_event,
                                );
                                raw_chunk_completes_response |= event_completes_response;
                                raw_chunk_done |= parsed_event.is_done();
                                if !semantic_completion_observed && event_completes_response {
                                    semantic_completion_observed = true;
                                    if let Some(o) = observer.as_ref() {
                                        if usage.has_billable_usage() {
                                            o.set_usage_counts(&usage, true);
                                            o.emit_lifecycle(
                                                cc_lb_lifecycle::LifecycleEvent::UsageObserved {
                                                    event_id: o.event_id().to_owned(),
                                                    usage: to_usage_snapshot(&usage),
                                                    source:
                                                        cc_lb_lifecycle::UsageSource::MessageDelta,
                                                },
                                            );
                                        }
                                        o.set_success_status(status);
                                    }
                                }
                                if let Some(o) = observer.as_ref()
                                    && usage.present
                                {
                                    if usage_update.message_start_usage {
                                        if usage.has_billable_usage() {
                                            o.set_usage_counts(&usage, false);
                                        }
                                        if usage.has_billable_usage() {
                                            o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::UsageObserved {
                                                event_id: o.event_id().to_owned(),
                                                usage: to_usage_snapshot(&usage),
                                                source: cc_lb_lifecycle::UsageSource::MessageStart,
                                            });
                                        }
                                    } else if usage_update.message_stop {
                                        if usage.has_billable_usage() {
                                            o.set_usage_counts(&usage, true);
                                        }
                                        if usage.has_billable_usage() {
                                            o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::UsageObserved {
                                                event_id: o.event_id().to_owned(),
                                                usage: to_usage_snapshot(&usage),
                                                source: cc_lb_lifecycle::UsageSource::MessageStop,
                                            });
                                        }
                                    }
                                    let force_publish = usage_update.message_start_usage
                                        || usage_update.message_stop;
                                    let time_since_last = last_partial_at
                                        .map(|t| now.duration_since(t))
                                        .unwrap_or(Duration::MAX);
                                    let tokens_since_last = usage
                                        .output_tokens
                                        .saturating_sub(last_partial_output_tokens);
                                    let throttle_ok = time_since_last
                                        >= Duration::from_millis(1000)
                                        || tokens_since_last >= 100;
                                    if force_publish || throttle_ok {
                                        if !usage_update.message_start_usage
                                            && !usage_update.message_stop
                                            && usage.has_billable_usage()
                                        {
                                            o.set_usage_counts(&usage, false);
                                            o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::UsageObserved {
                                                event_id: o.event_id().to_owned(),
                                                usage: to_usage_snapshot(&usage),
                                                source: cc_lb_lifecycle::UsageSource::MessageDelta,
                                            });
                                        }
                                        last_partial_at = Some(now);
                                        last_partial_output_tokens = usage.output_tokens;
                                    }
                                }
                                if sse_transform_active {
                                    let mut outgoing_provider_error = provider_error_in_event;
                                    let mut outgoing_completes_response = event_completes_response;
                                    let mut outgoing_done = parsed_event.is_done();
                                    let outgoing = if let Some(response_headers) = response_headers.as_ref() {
                                        match transform_sse_event_bytes(
                                            sse_transform_hook,
                                            &transform_ctx,
                                            &event_ctx,
                                            status,
                                            response_headers,
                                            raw.clone(),
                                        ) {
                                            outcome @ (SseTransformOutcome::Emit(_)
                                            | SseTransformOutcome::EmitTransformed(_)) => {
                                                let transformed =
                                                    matches!(&outcome, SseTransformOutcome::EmitTransformed(_));
                                                let bytes = match outcome {
                                                    SseTransformOutcome::Emit(bytes)
                                                    | SseTransformOutcome::EmitTransformed(bytes) => bytes,
                                                    _ => unreachable!("matched emit outcome"),
                                                };
                                                if transformed {
                                                    // Rewritten output is committed: raw JSON
                                                    // must not follow it (ADR 0007).
                                                    error_body_class = ErrorBodyClass::Sse;
                                                    let output = responses_error_context
                                                        .observe_transformed_output(
                                                            &bytes,
                                                            response_affinity_context.capability,
                                                        );
                                                    outgoing_provider_error = output.emitted_error;
                                                    outgoing_completes_response = output.completes_response;
                                                    outgoing_done = output.done;
                                                }
                                                raw_before_transform_output.clear();
                                                transformed_output_started = true;
                                                if downstream_drop_guard.observes_keepalive_response() {
                                                    downstream_drop_guard
                                                        .observe_keepalive_emitted(
                                                            &bytes,
                                                            incomplete_sse_event_budget_bytes,
                                                        );
                                                }
                                                bytes
                                            }
                                            SseTransformOutcome::Drop => {
                                                error_body_class = ErrorBodyClass::Sse;
                                                raw_before_transform_output.clear();
                                                transformed_output_started = true;
                                                continue;
                                            }
                                            SseTransformOutcome::FailOpen => {
                                                if stream_provider_error_forwarded {
                                                    parse_sse_events_active = false;
                                                    buffer.clear();
                                                    raw_before_transform_output.clear();
                                                    break 'upstream;
                                                }
                                                if success_sse_affinity_gate
                                                    || transformed_output_started
                                                    || !downstream_stream_is_identity
                                                {
                                                    let error = ResponseTransformError::Runtime {
                                                        reason: if transformed_output_started {
                                                            "failed to parse SSE event after transform output started".to_owned()
                                                        } else {
                                                            "failed to parse SSE event before transform output".to_owned()
                                                        },
                                                    };
                                                    let frame =
                                                        make_response_transform_error_frame(
                                                            response_affinity_context.capability,
                                                            &responses_error_context,
                                                            &error,
                                                        );
                                                    stream_transform_error = Some(error);
                                                    if let Some(o) = observer.as_ref()
                                                        && !upstream_error_status
                                                    {
                                                        o.set_upstream_error(
                                                            StatusCode::OK,
                                                            UpstreamErrorCode::StreamError,
                                                        );
                                                    }
                                                    downstream_drop_guard.mark_proxy_error(
                                                        StreamTerminationCause::TransformError,
                                                    );
                                                    yield Ok::<Bytes, Infallible>(frame);
                                                    break 'upstream;
                                                }
                                                parse_sse_events_active = false;
                                                buffer.clear();
                                                sse_transform_active = false;
                                                keepalive_raw_passthrough_active = true;
                                                raw_passthrough_current_chunk = true;
                                                for raw in raw_before_transform_output.drain(..) {
                                                    downstream_sse_boundary.observe(&raw);
                                                    downstream_drop_guard.observe_keepalive_raw(
                                                        &raw,
                                                        incomplete_sse_event_budget_bytes,
                                                        response_affinity_context.capability,
                                                    );
                                                    batch_index = batch_index.saturating_add(1);
                                                    yield Ok::<Bytes, Infallible>(raw);
                                                }
                                                Bytes::new()
                                            }
                                            SseTransformOutcome::Error(error) => {
                                                tracing::warn!(
                                                    parent: &stream_span,
                                                    request_id = %event_ctx.request_id,
                                                    %error,
                                                    "sse response transform failed"
                                                );
                                                if stream_provider_error_forwarded {
                                                    parse_sse_events_active = false;
                                                    buffer.clear();
                                                    raw_before_transform_output.clear();
                                                    break 'upstream;
                                                }
                                                if success_sse_affinity_gate
                                                    || transformed_output_started
                                                    || !downstream_stream_is_identity
                                                {
                                                    let frame = if success_sse_affinity_gate {
                                                        // The affinity gate must not expose transform
                                                        // hook details to the client.
                                                        make_stream_error_frame(
                                                            response_affinity_context.capability,
                                                            &responses_error_context,
                                                            UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                                            UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE,
                                                            false,
                                                        )
                                                    } else {
                                                        make_response_transform_error_frame(
                                                            response_affinity_context.capability,
                                                            &responses_error_context,
                                                            &error,
                                                        )
                                                    };
                                                    stream_transform_error = Some(error);
                                                    if let Some(o) = observer.as_ref()
                                                        && !upstream_error_status
                                                    {
                                                        o.set_upstream_error(
                                                            StatusCode::OK,
                                                            UpstreamErrorCode::StreamError,
                                                        );
                                                    }
                                                    downstream_drop_guard.mark_proxy_error(
                                                        StreamTerminationCause::TransformError,
                                                    );
                                                    yield Ok::<Bytes, Infallible>(frame);
                                                    break 'upstream;
                                                }
                                                parse_sse_events_active = false;
                                                buffer.clear();
                                                sse_transform_active = false;
                                                keepalive_raw_passthrough_active = true;
                                                raw_passthrough_current_chunk = true;
                                                for raw in raw_before_transform_output.drain(..) {
                                                    downstream_sse_boundary.observe(&raw);
                                                    downstream_drop_guard.observe_keepalive_raw(
                                                        &raw,
                                                        incomplete_sse_event_budget_bytes,
                                                        response_affinity_context.capability,
                                                    );
                                                    batch_index = batch_index.saturating_add(1);
                                                    yield Ok::<Bytes, Infallible>(raw);
                                                }
                                                Bytes::new()
                                            }
                                        }
                                    } else if success_sse_affinity_gate
                                        && !stream_provider_error_forwarded
                                    {
                                        stream_upstream_error =
                                            Some(cc_lb_lifecycle::StreamError {
                                                error_type:
                                                    UPSTREAM_RESPONSE_FRAMING_ERROR_TYPE.to_owned(),
                                                error_message:
                                                    UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE
                                                        .to_owned(),
                                                ..Default::default()
                                            });
                                        let frame = make_stream_error_frame(
                                            response_affinity_context.capability,
                                        &responses_error_context,
                                        UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                            UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE,
                                            false,
                                        );
                                        yield Ok::<Bytes, Infallible>(frame);
                                        break 'upstream;
                                    } else {
                                        sse_transform_active = false;
                                        keepalive_raw_passthrough_active = true;
                                        raw_passthrough_current_chunk = true;
                                        Bytes::new()
                                    };
                                    if raw_passthrough_current_chunk {
                                        break;
                                    }
                                    batch_index = batch_index.saturating_add(1);
                                    if outgoing_completes_response {
                                        semantic_completion_emitted = true;
                                        downstream_drop_guard.mark_semantic_completion();
                                    }
                                    if response_affinity_context.capability == ProviderCapability::OpenaiChatCompletions {
                                        chat_done_emitted |= outgoing_done;
                                    }
                                    if outgoing_provider_error && !outgoing.is_empty() {
                                        stream_provider_error_forwarded = true;
                                    }
                                    yield Ok::<Bytes, Infallible>(outgoing);
                                    let chat_should_stop = chat_sse_hook
                                        .as_ref()
                                        .is_some_and(|hook| hook.should_stop_upstream());
                                    if chat_should_stop {
                                        if !outgoing_done && !outgoing_completes_response {
                                            stream_provider_error_forwarded = true;
                                        }
                                        break 'upstream;
                                    }
                                } else if success_sse_affinity_gate {
                                    downstream_sse_boundary.observe(&raw);
                                    batch_index = batch_index.saturating_add(1);
                                    if defer_terminal_raw_chunk {
                                        if event_completes_response {
                                            semantic_completion_emitted = true;
                                        }
                                        deferred_terminal_chunks.push(raw);
                                        if provider_error_in_event {
                                            stream_provider_error_forwarded = true;
                                        }
                                    } else {
                                        if event_completes_response {
                                            semantic_completion_emitted = true;
                                            downstream_drop_guard.mark_semantic_completion();
                                        }
                                        if provider_error_in_event {
                                            stream_provider_error_forwarded = true;
                                        }
                                        yield Ok::<Bytes, Infallible>(raw);
                                    }
                                } else if hold_unsolicited_chunk {
                                    // The raw passthrough already relayed the
                                    // leading bytes of the first completed event.
                                    let forwarded = std::mem::take(&mut unsolicited_forwarded_bytes);
                                    unsolicited_event_completed = true;
                                    let outgoing = raw.slice(forwarded.min(raw.len())..);
                                    if !outgoing.is_empty() {
                                        downstream_sse_boundary.observe(&outgoing);
                                        batch_index = batch_index.saturating_add(1);
                                        if defer_terminal_raw_chunk {
                                            if event_completes_response {
                                                semantic_completion_emitted = true;
                                            }
                                            deferred_terminal_chunks.push(outgoing);
                                            if provider_error_in_event {
                                                stream_provider_error_forwarded = true;
                                            }
                                        } else {
                                            if event_completes_response {
                                                semantic_completion_emitted = true;
                                                downstream_drop_guard.mark_semantic_completion();
                                            }
                                            if provider_error_in_event {
                                                stream_provider_error_forwarded = true;
                                            }
                                            yield Ok::<Bytes, Infallible>(outgoing);
                                        }
                                    }
                                }
                            }
                            if hold_unsolicited_chunk && unsolicited_event_completed {
                                // The held candidate completed: re-probe the new
                                // partial event, which this chunk alone delivered.
                                unsolicited_hold_pending =
                                    sse_bytes_may_carry_anthropic_provider_state(&buffer);
                            }
                            if hold_unsolicited_chunk && !unsolicited_hold_pending {
                                // Back to the raw passthrough: relay the held
                                // remainder, which carries no provider state.
                                let start = unsolicited_forwarded_bytes.min(buffer.len());
                                let tail = Bytes::copy_from_slice(&buffer[start..]);
                                unsolicited_forwarded_bytes = buffer.len();
                                if !tail.is_empty() {
                                    downstream_sse_boundary.observe(&tail);
                                    batch_index = batch_index.saturating_add(1);
                                    if defer_terminal_raw_chunk {
                                        deferred_terminal_chunks.push(tail);
                                    } else {
                                        yield Ok::<Bytes, Infallible>(tail);
                                    }
                                }
                            }
                            if parse_sse_events_active
                                && buffer.len() > incomplete_sse_event_budget_bytes
                            {
                                tracing::warn!(
                                    parent: &stream_span,
                                    request_id = %event_ctx.request_id,
                                    buffered_bytes = buffer.len(),
                                    budget_bytes = incomplete_sse_event_budget_bytes,
                                    "stopping SSE parsing: incomplete event exceeds configured body-derived budget"
                                );
                                parse_sse_events_active = false;
                                buffer.clear();
                                unsolicited_forwarded_bytes = 0;
                                if stream_provider_error_forwarded {
                                    break 'upstream;
                                }
                                if success_sse_affinity_gate || unsolicited_hold_pending {
                                    stream_upstream_error =
                                        Some(cc_lb_lifecycle::StreamError {
                                            error_type:
                                                UPSTREAM_RESPONSE_FRAMING_ERROR_TYPE.to_owned(),
                                            error_message:
                                                UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE.to_owned(),
                                            ..Default::default()
                                        });
                                    let frame = make_stream_error_frame(
                                        response_affinity_context.capability,
                                        &responses_error_context,
                                        UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                        UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE,
                                        false,
                                    );
                                    if let Some(o) = observer.as_ref() {
                                        o.set_upstream_error(
                                            StatusCode::OK,
                                            UpstreamErrorCode::StreamError,
                                        );
                                    }
                                    downstream_drop_guard.mark_upstream_error(
                                        StreamTerminationCause::FramingError,
                                        None,
                                    );
                                    for chunk in std::mem::take(&mut deferred_terminal_chunks) {
                                        yield Ok::<Bytes, Infallible>(chunk);
                                    }
                                    yield Ok::<Bytes, Infallible>(frame);
                                    break 'upstream;
                                }
                                if sse_transform_active {
                                    if !stream_provider_error_forwarded
                                        && (transformed_output_started
                                            || !downstream_stream_is_identity)
                                    {
                                        let error = ResponseTransformError::Runtime {
                                            reason: if transformed_output_started {
                                                format!(
                                                    "incomplete SSE event exceeds {incomplete_sse_event_budget_bytes}-byte budget after transform output"
                                                )
                                            } else {
                                                format!(
                                                    "incomplete SSE event exceeds {incomplete_sse_event_budget_bytes}-byte budget before transform output"
                                                )
                                            },
                                        };
                                        let frame = make_response_transform_error_frame(
                                            response_affinity_context.capability,
                                            &responses_error_context,
                                            &error,
                                        );
                                        stream_transform_error = Some(error);
                                        if let Some(o) = observer.as_ref()
                                            && !upstream_error_status
                                        {
                                            o.set_upstream_error(
                                                StatusCode::OK,
                                                UpstreamErrorCode::StreamError,
                                            );
                                        }
                                        downstream_drop_guard.mark_proxy_error(
                                            StreamTerminationCause::TransformError,
                                        );
                                        yield Ok::<Bytes, Infallible>(frame);
                                        break 'upstream;
                                    }
                                    sse_transform_active = false;
                                    keepalive_raw_passthrough_active = true;
                                    for raw in raw_before_transform_output.drain(..) {
                                        downstream_sse_boundary.observe(&raw);
                                        downstream_drop_guard.observe_keepalive_raw(
                                            &raw,
                                            incomplete_sse_event_budget_bytes,
                                            response_affinity_context.capability,
                                        );
                                        batch_index = batch_index.saturating_add(1);
                                        yield Ok::<Bytes, Infallible>(raw);
                                    }
                                }
                            }
                            if !sse_transform_active {
                                if !success_sse_affinity_gate && !hold_unsolicited_chunk {
                                    // Chunk fanout stays inline — high-volume, not bus-worthy.
                                    if upstream_is_sse && downstream_stream_is_identity {
                                        if keepalive_raw_passthrough_active {
                                            downstream_drop_guard
                                                .observe_keepalive_raw(
                                                    &data,
                                                    incomplete_sse_event_budget_bytes,
                                                    response_affinity_context.capability,
                                                );
                                        }
                                        downstream_sse_boundary.observe(&data);
                                    }
                                    batch_index = batch_index.saturating_add(1);
                                    if defer_terminal_raw_chunk {
                                        if raw_chunk_completes_response {
                                            semantic_completion_emitted = true;
                                        }
                                        debug_assert!(deferred_terminal_chunks.is_empty());
                                        deferred_terminal_chunks.push(data);
                                        if response_affinity_context.capability == ProviderCapability::OpenaiChatCompletions {
                                            chat_done_emitted |= raw_chunk_done;
                                        }
                                        if provider_error_in_chunk {
                                            stream_provider_error_forwarded = true;
                                        }
                                    } else {
                                        if raw_chunk_completes_response {
                                            semantic_completion_emitted = true;
                                            downstream_drop_guard.mark_semantic_completion();
                                        }
                                        if response_affinity_context.capability == ProviderCapability::OpenaiChatCompletions {
                                            chat_done_emitted |= raw_chunk_done;
                                        }
                                        if provider_error_in_chunk {
                                            stream_provider_error_forwarded = true;
                                        }
                                        yield Ok::<Bytes, Infallible>(data);
                                    }
                                    if unsolicited_sse_affinity_hold_eligible {
                                        unsolicited_forwarded_bytes = buffer.len();
                                    }
                                }
                            } else if !transformed_output_started {
                                raw_before_transform_output.push(data);
                            }
                        }
                    }
                    Err(source) => {
                        let classification = classify_stream_error(&source);
                        let client_error_message = classification.redacted_message.clone();
                        tracing::warn!(
                            parent: &stream_span,
                            request_id = %event_ctx.request_id,
                            cause = classification.cause.as_str(),
                            error_chain = %classification.redacted_chain,
                            io_kind = classification.io_kind.as_deref().unwrap_or(""),
                            io_os_error = ?classification.io_os_error,
                            h2_reason = classification.h2_reason.as_deref().unwrap_or(""),
                            "upstream response body stream failed"
                        );
                        // #663: a transport failure after cc-lb parsed the upstream
                        // message_stop, with no partial event pending, completes the
                        // stream with a warning when cc-lb is the SSE emitter of record
                        // (transform, affinity gate, or uncompressed SSE passthrough).
                        let body_end_accepted = upstream_is_sse
                            && (success_sse_affinity_gate
                                || sse_transform_active
                                || (downstream_stream_is_identity
                                    && !downstream_sse_boundary.partial_event_pending()))
                            && upstream_end_after_message_stop_is_acceptable(
                                status,
                                match response_affinity_context.capability {
                                    ProviderCapability::OpenaiChatCompletions => chat_done_emitted,
                                    ProviderCapability::OpenaiResponses
                                    | ProviderCapability::OpenaiResponsesCompact => semantic_completion_emitted,
                                    _ => semantic_message_stop_seen,
                                },
                                stream_upstream_error.is_some()
                                    || stream_transform_error.is_some()
                                    || stream_affinity_error.is_some(),
                                stream_provider_error_seen
                                    || (response_affinity_context.capability == ProviderCapability::OpenaiChatCompletions
                                        && stream_provider_error_forwarded),
                                upstream_diagnostics.warning_recorded(),
                                &buffer,
                            );
                        upstream_diagnostics.record_body_end(
                            cc_lb_lifecycle::UpstreamBodyEnd::TransportError,
                            || classification.clone(),
                        );
                        if body_end_accepted {
                            tracing::warn!(
                                parent: &stream_span,
                                request_id = %event_ctx.request_id,
                                error_chain = %classification.redacted_chain,
                                warning_type = UPSTREAM_RESPONSE_BODY_ERROR_TYPE,
                                "accepting upstream SSE response after message_stop despite body stream failure"
                            );
                            upstream_diagnostics.record_warning(
                                UPSTREAM_RESPONSE_BODY_ERROR_TYPE,
                                classification.redacted_message,
                            );
                            break;
                        }
                        let body_error_is_primary =
                            !stream_provider_error_seen && stream_upstream_error.is_none();
                        if body_error_is_primary {
                            stream_upstream_error = Some(cc_lb_lifecycle::StreamError {
                                error_type: UPSTREAM_RESPONSE_BODY_ERROR_TYPE.to_owned(),
                                error_message: classification.redacted_message.clone(),
                                ..Default::default()
                            });
                            downstream_drop_guard.mark_upstream_error(
                                classification.cause,
                                Some(&classification),
                            );
                        }
                        if !stream_provider_error_forwarded
                            && !stream_upstream_error_frame_emitted
                            && upstream_is_sse
                            && (success_sse_affinity_gate
                                || sse_transform_active
                                || downstream_stream_is_identity)
                        {
                            let client_error_message = if success_sse_affinity_gate {
                                UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE
                            } else {
                                &client_error_message
                            };
                            let frame = make_stream_error_frame(
                                response_affinity_context.capability,
                                &responses_error_context,
                                UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                client_error_message,
                                !success_sse_affinity_gate
                                    && downstream_stream_is_identity
                                    && !sse_transform_active
                                    && downstream_sse_boundary.partial_event_pending(),
                            );
                            stream_upstream_error_frame_emitted = true;
                            if let Some(o) = observer.as_ref()
                                && !upstream_error_status
                            {
                                o.set_upstream_error(status, UpstreamErrorCode::StreamError);
                            }
                            yield Ok::<Bytes, Infallible>(frame);
                        }
                        break;
                    }
                }
            }
            let mut keepalive_decode_complete = false;
            match early_decoder_finish.unwrap_or_else(|| usage_decoder.finish()) {
                Ok(tail) if !tail.is_empty() => {
                    keepalive_decode_complete = true;
                    if upstream_error_status {
                        append_bounded_upstream_error_body(
                            &mut upstream_error_body,
                            &tail,
                            &mut upstream_error_body_truncated,
                        );
                    }
                    if parse_sse_events_active {
                        buffer.extend_from_slice(&tail);
                        loop {
                            if error_body_class != ErrorBodyClass::Sse {
                                error_body_class = classify_error_body(error_body_class, &buffer);
                                if error_body_class == ErrorBodyClass::JsonCandidate {
                                    break;
                                }
                            }
                            let Some(end) = usage_parser::find_sse_event_end(&buffer) else {
                                break;
                            };
                            let raw = split_sse_event(&mut buffer, end);
                            let parsed_event = parse_sse_event(&raw);
                            if matches!(
                                response_affinity_context.capability,
                                ProviderCapability::OpenaiResponses
                                    | ProviderCapability::OpenaiResponsesCompact
                            ) {
                                responses_error_context
                                    .observe(parsed_event.value().ok().flatten());
                            }
                            let usage_update = usage_parser::accumulate_sse_usage_for_shape(
                                &parsed_event,
                                &mut usage,
                                if precomputed_usage_present {
                                    crate::provider_capability::UsageShape::None
                                } else {
                                    response_affinity_context
                                        .usage_shape
                                        .unwrap_or_else(|| {
                                            response_affinity_context.capability.usage_shape()
                                        })
                                },
                            );
                            downstream_drop_guard.set_keepalive_cached_tokens(
                                usage
                                    .cache_read_input_tokens
                                    .saturating_add(usage.cache_creation_input_tokens),
                            );
                            if keepalive_event_reaches_upstream_terminal(
                                response_affinity_context.capability,
                                &parsed_event,
                            ) {
                                downstream_drop_guard.mark_upstream_terminal();
                            }
                            let mut affinity_persist_failed = false;
                            let affinity_binding = if success_sse_affinity_gate {
                                let response_keys = match
                                    extract_response_affinity_keys_from_sse_event(
                                        response_affinity_context.capability,
                                        &parsed_event,
                                        &response_affinity_context.principal_id,
                                    )
                                {
                                    Ok(keys) => keys,
                                    Err(_) => {
                                        tracing::warn!(
                                            parent: &stream_span,
                                            "failed to inspect upstream affinity in final SSE event"
                                        );
                                        if !stream_provider_error_forwarded {
                                            stream_upstream_error =
                                                Some(cc_lb_lifecycle::StreamError {
                                                    error_type:
                                                        UPSTREAM_RESPONSE_FRAMING_ERROR_TYPE
                                                            .to_owned(),
                                                    error_message:
                                                        UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE
                                                            .to_owned(),
                                                    ..Default::default()
                                                });
                                        }
                                        if !stream_provider_error_forwarded {
                                            downstream_drop_guard.mark_upstream_error(
                                                StreamTerminationCause::FramingError,
                                                None,
                                            );
                                        }
                                        parse_sse_events_active = false;
                                        buffer.clear();
                                        raw_before_transform_output.clear();
                                        if !stream_provider_error_forwarded {
                                            let frame = make_stream_error_frame(
                                                response_affinity_context.capability,
                                                &responses_error_context,
                                                UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                                UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE,
                                                false,
                                            );
                                            batch_index = batch_index.saturating_add(1);
                                            yield Ok::<Bytes, Infallible>(frame);
                                        }
                                        break;
                                    }
                                };
                                let bind_pending_request = sse_event_completes_response(
                                    response_affinity_context.capability,
                                    &parsed_event,
                                );
                                Some((response_keys, bind_pending_request))
                            } else if unsolicited_sse_affinity_inspection {
                                match unsolicited_anthropic_sse_affinity_keys(
                                    &parsed_event,
                                    &response_affinity_context.principal_id,
                                    &bound_response_affinity_keys,
                                ) {
                                    Ok(keys) if keys.is_empty() => None,
                                    Ok(keys) => Some((keys, false)),
                                    Err(_) => {
                                        affinity_persist_failed = true;
                                        None
                                    }
                                }
                            } else {
                                None
                            };
                            if let Some((response_keys, bind_pending_request)) = affinity_binding {
                                let mut affinity_keys = response_keys
                                    .iter()
                                    .filter(|key| {
                                        !bound_response_affinity_keys.contains(*key)
                                    })
                                    .cloned()
                                    .collect::<Vec<_>>();
                                if bind_pending_request {
                                    affinity_keys.extend(
                                        pending_request_affinity_keys.iter().cloned(),
                                    );
                                }
                                if bind_upstream_affinity_keys(
                                    &response_affinity_context,
                                    &affinity_keys,
                                    unix_secs(affinity_clock.now()),
                                    None,
                                )
                                .await
                                .is_err()
                                {
                                    affinity_persist_failed = true;
                                } else {
                                    bound_response_affinity_keys.extend(response_keys);
                                    if bind_pending_request {
                                        pending_request_affinity_keys.clear();
                                    }
                                }
                            }
                            if affinity_persist_failed {
                                tracing::warn!(
                                    parent: &stream_span,
                                    "failed to persist upstream affinity before final SSE event"
                                );
                                if !stream_provider_error_forwarded {
                                    stream_affinity_error = Some(affinity_stream_error());
                                    downstream_drop_guard.mark_proxy_error(
                                        StreamTerminationCause::AffinityError,
                                    );
                                }
                                parse_sse_events_active = false;
                                buffer.clear();
                                raw_before_transform_output.clear();
                                if !stream_provider_error_forwarded
                                    && (success_sse_affinity_gate
                                        || sse_transform_active
                                        || downstream_stream_is_identity)
                                {
                                    let frame = make_stream_error_frame(
                                        response_affinity_context.capability,
                                        &responses_error_context,
                                        UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                        UPSTREAM_AFFINITY_UNAVAILABLE_MESSAGE,
                                        false,
                                    );
                                    batch_index = batch_index.saturating_add(1);
                                    yield Ok::<Bytes, Infallible>(frame);
                                }
                                break;
                            }
                            if !sse_transform_active
                                && downstream_drop_guard.observes_keepalive_response()
                            {
                                downstream_drop_guard.observe_keepalive(&parsed_event);
                            }
                            let mut provider_error_in_event = false;
                            if let Some(err) =
                                usage_parser::detect_mid_stream_error(&parsed_event, signer.as_ref())
                            {
                                provider_error_in_event = true;
                                stream_provider_error_seen = true;
                                downstream_drop_guard.mark_upstream_error(
                                    StreamTerminationCause::ProviderError,
                                    None,
                                );
                                if let Some(o) = observer.as_ref() {
                                    let error = cc_lb_lifecycle::StreamError {
                                        error_type: err.error_type.clone().unwrap_or_default(),
                                        error_message: err
                                            .error_message
                                            .clone()
                                            .unwrap_or_default(),
                                        upstream_error_code: err.upstream_error_code.clone(),
                                        ..Default::default()
                                    };
                                    o.emit_lifecycle(
                                        cc_lb_lifecycle::LifecycleEvent::StreamCompleted {
                                            event_id: o.event_id().to_owned(),
                                            result: Err(error),
                                        },
                                    );
                                    if !upstream_error_status {
                                        o.set_upstream_error(
                                            StatusCode::OK,
                                            UpstreamErrorCode::StreamError,
                                        );
                                    }
                                }
                            }
                            if !downstream_drop_guard.has_abnormal_stop()
                                && let Some(stop) =
                                    usage_parser::detect_abnormal_stop(&parsed_event)
                            {
                                let error_message = abnormal_stop_error_message(&stop);
                                downstream_drop_guard
                                    .mark_abnormal_stop(stop.stop_reason, error_message);
                            }
                            observe_sse_terminal_event(
                                response_affinity_context.capability,
                                &parsed_event,
                                usage_update.message_stop,
                                &mut semantic_message_stop_seen,
                                &mut message_stop_at,
                                Instant::now,
                            );
                            let event_completes_response = sse_event_completes_response(
                                response_affinity_context.capability,
                                &parsed_event,
                            );
                            if !semantic_completion_observed && event_completes_response {
                                semantic_completion_observed = true;
                                if let Some(o) = observer.as_ref() {
                                    if usage.has_billable_usage() {
                                        o.set_usage_counts(&usage, true);
                                    }
                                    if usage.has_billable_usage() {
                                        o.emit_lifecycle(
                                            cc_lb_lifecycle::LifecycleEvent::UsageObserved {
                                                event_id: o.event_id().to_owned(),
                                                usage: to_usage_snapshot(&usage),
                                                source: cc_lb_lifecycle::UsageSource::MessageDelta,
                                            },
                                        );
                                    }
                                    o.set_success_status(status);
                                }
                            }
                            sse_event_count = sse_event_count.saturating_add(1);
                            if success_sse_affinity_gate || unsolicited_hold_pending {
                                let mut outgoing_provider_error = provider_error_in_event;
                                let mut outgoing_completes_response = event_completes_response;
                                let outgoing = if sse_transform_active {
                                    let Some(response_headers) = response_headers.as_ref() else {
                                        if !stream_provider_error_forwarded {
                                            stream_upstream_error =
                                                Some(cc_lb_lifecycle::StreamError {
                                                    error_type:
                                                        UPSTREAM_RESPONSE_FRAMING_ERROR_TYPE
                                                            .to_owned(),
                                                    error_message:
                                                        UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE
                                                            .to_owned(),
                                                    ..Default::default()
                                                });
                                            downstream_drop_guard.mark_upstream_error(
                                                StreamTerminationCause::FramingError,
                                                None,
                                            );
                                            let frame = make_stream_error_frame(
                                                response_affinity_context.capability,
                                                &responses_error_context,
                                                UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                                UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE,
                                                false,
                                            );
                                            batch_index = batch_index.saturating_add(1);
                                            yield Ok::<Bytes, Infallible>(frame);
                                        }
                                        break;
                                    };
                                    match transform_sse_event_bytes(
                                        sse_transform_hook,
                                        &transform_ctx,
                                        &event_ctx,
                                        status,
                                        response_headers,
                                        raw.clone(),
                                    ) {
                                        outcome @ (SseTransformOutcome::Emit(_)
                                        | SseTransformOutcome::EmitTransformed(_)) => {
                                            let transformed =
                                                matches!(&outcome, SseTransformOutcome::EmitTransformed(_));
                                            let bytes = match outcome {
                                                SseTransformOutcome::Emit(bytes)
                                                | SseTransformOutcome::EmitTransformed(bytes) => bytes,
                                                _ => unreachable!("matched emit outcome"),
                                            };
                                            if transformed {
                                                let output = responses_error_context
                                                    .observe_transformed_output(
                                                        &bytes,
                                                        response_affinity_context.capability,
                                                    );
                                                outgoing_provider_error = output.emitted_error;
                                                outgoing_completes_response = output.completes_response;
                                            }
                                            if downstream_drop_guard.observes_keepalive_response() {
                                                downstream_drop_guard
                                                    .observe_keepalive_emitted(
                                                        &bytes,
                                                        incomplete_sse_event_budget_bytes,
                                                    );
                                            }
                                            bytes
                                        }
                                        SseTransformOutcome::Drop => continue,
                                        SseTransformOutcome::FailOpen
                                        | SseTransformOutcome::Error(_) => {
                                            if !stream_provider_error_forwarded {
                                                stream_transform_error =
                                                    Some(ResponseTransformError::Runtime {
                                                        reason:
                                                            "failed to safely transform SSE event"
                                                                .to_owned(),
                                                    });
                                                downstream_drop_guard.mark_proxy_error(
                                                    StreamTerminationCause::TransformError,
                                                );
                                                let frame = make_stream_error_frame(
                                                    response_affinity_context.capability,
                                                    &responses_error_context,
                                                    UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                                    UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE,
                                                    false,
                                                );
                                                batch_index = batch_index.saturating_add(1);
                                                yield Ok::<Bytes, Infallible>(frame);
                                            }
                                            break;
                                        }
                                    }
                                } else if success_sse_affinity_gate {
                                    raw
                                } else {
                                    let forwarded =
                                        std::mem::take(&mut unsolicited_forwarded_bytes);
                                    let outgoing = raw.slice(forwarded.min(raw.len())..);
                                    if outgoing.is_empty() {
                                        continue;
                                    }
                                    outgoing
                                };
                                downstream_sse_boundary.observe(&outgoing);
                                batch_index = batch_index.saturating_add(1);
                                if outgoing_completes_response {
                                    semantic_completion_emitted = true;
                                    downstream_drop_guard.mark_semantic_completion();
                                }
                                if outgoing_provider_error && !outgoing.is_empty() {
                                    stream_provider_error_forwarded = true;
                                }
                                yield Ok::<Bytes, Infallible>(outgoing);
                            }
                        }
                        if buffer.len() > incomplete_sse_event_budget_bytes {
                            tracing::warn!(
                                parent: &stream_span,
                                request_id = %event_ctx.request_id,
                                buffered_bytes = buffer.len(),
                                budget_bytes = incomplete_sse_event_budget_bytes,
                                "stopping SSE parsing: final incomplete event exceeds configured body-derived budget"
                            );
                            buffer.clear();
                            if !stream_provider_error_forwarded
                                && (success_sse_affinity_gate || unsolicited_hold_pending)
                            {
                                stream_upstream_error =
                                    Some(cc_lb_lifecycle::StreamError {
                                        error_type:
                                            UPSTREAM_RESPONSE_FRAMING_ERROR_TYPE.to_owned(),
                                        error_message:
                                            UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE.to_owned(),
                                        ..Default::default()
                                    });
                                downstream_drop_guard.mark_upstream_error(
                                    StreamTerminationCause::FramingError,
                                    None,
                                );
                                let frame = make_stream_error_frame(
                                    response_affinity_context.capability,
                                    &responses_error_context,
                                    UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                    UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE,
                                    false,
                                );
                                yield Ok::<Bytes, Infallible>(frame);
                            } else if !stream_provider_error_forwarded
                                && sse_transform_active
                                && stream_transform_error.is_none()
                            {
                                if transformed_output_started
                                    || !downstream_stream_is_identity
                                {
                                    let error = ResponseTransformError::Runtime {
                                        reason: if transformed_output_started {
                                            format!(
                                                "incomplete SSE event exceeds {incomplete_sse_event_budget_bytes}-byte budget after transform output"
                                            )
                                        } else {
                                            format!(
                                                "incomplete SSE event exceeds {incomplete_sse_event_budget_bytes}-byte budget before transform output"
                                            )
                                        },
                                    };
                                    let frame = make_response_transform_error_frame(
                                        response_affinity_context.capability,
                                        &responses_error_context,
                                        &error,
                                    );
                                    stream_transform_error = Some(error);
                                    downstream_drop_guard.mark_proxy_error(
                                        StreamTerminationCause::TransformError,
                                    );
                                    if let Some(o) = observer.as_ref()
                                        && !upstream_error_status
                                    {
                                        o.set_upstream_error(
                                            StatusCode::OK,
                                            UpstreamErrorCode::StreamError,
                                        );
                                    }
                                    yield Ok::<Bytes, Infallible>(frame);
                                } else {
                                    for raw in raw_before_transform_output.drain(..) {
                                        downstream_sse_boundary.observe(&raw);
                                        downstream_drop_guard.observe_keepalive_raw(
                                            &raw,
                                            incomplete_sse_event_budget_bytes,
                                            response_affinity_context.capability,
                                        );
                                        batch_index = batch_index.saturating_add(1);
                                        yield Ok::<Bytes, Infallible>(raw);
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(_) => keepalive_decode_complete = true,
                Err(error) => {
                    upstream_error_body_decode_failed = true;
                    let upstream_decode_failed = is_upstream_response_decode_error(&error);
                    tracing::warn!(
                        parent: &stream_span,
                        request_id = %event_ctx.request_id,
                        %error,
                        upstream_decode_failed,
                        "streaming usage extractor decoder finish failed"
                    );
                    if upstream_body_eof {
                        upstream_diagnostics.record_body_end(
                            cc_lb_lifecycle::UpstreamBodyEnd::DecodeErrorAfterCleanEnd,
                            || classify_stream_error(&error),
                        );
                    }
                    if upstream_diagnostics.warning_recorded() {
                        // #663: an earlier accepted failure (push or body error) already
                        // owns the single warning; the decoder finish failure follows
                        // from it and must not turn the stream into an error.
                    } else if upstream_body_eof
                        && upstream_decode_failed
                        && upstream_is_sse
                        && (success_sse_affinity_gate || sse_transform_active)
                        && upstream_end_after_message_stop_is_acceptable(
                            status,
                            match response_affinity_context.capability {
                                ProviderCapability::OpenaiChatCompletions => chat_done_emitted,
                                ProviderCapability::OpenaiResponses
                                | ProviderCapability::OpenaiResponsesCompact => semantic_completion_emitted,
                                _ => semantic_message_stop_seen,
                            },
                            stream_upstream_error.is_some()
                                || stream_transform_error.is_some()
                                || stream_affinity_error.is_some(),
                            stream_provider_error_seen
                                || (response_affinity_context.capability == ProviderCapability::OpenaiChatCompletions
                                    && stream_provider_error_forwarded),
                            false,
                            &buffer,
                        )
                    {
                        // #663: the upstream body ended after cc-lb parsed message_stop,
                        // but the decoder could not verify the gzip trailer (missing,
                        // truncated, or CRC/ISIZE mismatch are indistinguishable).
                        let classification = classify_stream_error(&error);
                        tracing::warn!(
                            parent: &stream_span,
                            request_id = %event_ctx.request_id,
                            error_chain = %classification.redacted_chain,
                            warning_type = UPSTREAM_RESPONSE_DECODE_ERROR_TYPE,
                            "accepting upstream SSE response after message_stop despite decoder finish failure"
                        );
                        upstream_diagnostics.record_warning(
                            UPSTREAM_RESPONSE_DECODE_ERROR_TYPE,
                            truncate_reason(&format!("upstream response decoding failed: {error}")),
                        );
                    } else if success_sse_affinity_gate
                        && !stream_provider_error_forwarded
                        && !stream_upstream_error_frame_emitted
                        && stream_transform_error.is_none()
                        && stream_affinity_error.is_none()
                        && stream_upstream_error.is_none()
                    {
                        if !stream_provider_error_seen {
                            stream_upstream_error = Some(cc_lb_lifecycle::StreamError {
                                error_type: UPSTREAM_RESPONSE_DECODE_ERROR_TYPE.to_owned(),
                                error_message: UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE.to_owned(),
                                ..Default::default()
                            });
                            downstream_drop_guard.mark_upstream_error(
                                StreamTerminationCause::DecodeError,
                                None,
                            );
                        }
                        let frame = make_stream_error_frame(
                            response_affinity_context.capability,
                            &responses_error_context,
                            UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                            UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE,
                            false,
                        );
                        stream_upstream_error_frame_emitted = true;
                        yield Ok::<Bytes, Infallible>(frame);
                    } else if upstream_is_sse
                        && upstream_decode_failed
                        && stream_transform_error.is_none()
                        && stream_affinity_error.is_none()
                    {
                        let decode_error_is_primary =
                            !stream_provider_error_seen && stream_upstream_error.is_none();
                        if decode_error_is_primary {
                            stream_upstream_error = Some(cc_lb_lifecycle::StreamError {
                                error_type: UPSTREAM_RESPONSE_DECODE_ERROR_TYPE.to_owned(),
                                error_message: format!(
                                    "upstream response decoding failed: {error}"
                                ),
                                ..Default::default()
                            });
                        }
                        if decode_error_is_primary {
                            downstream_drop_guard.mark_upstream_error(
                                StreamTerminationCause::DecodeError,
                                None,
                            );
                        }
                        if !stream_provider_error_forwarded
                            && sse_transform_active
                            && !stream_upstream_error_frame_emitted
                        {
                            let error_message = stream_upstream_error
                                .as_ref()
                                .map(|error| std::borrow::Cow::Borrowed(error.error_message.as_str()))
                                .unwrap_or_else(|| {
                                    std::borrow::Cow::Owned(format!(
                                        "upstream response decoding failed: {error}"
                                    ))
                                });
                            let frame = make_stream_error_frame(
                                response_affinity_context.capability,
                                &responses_error_context,
                                UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                &error_message,
                                false,
                            );
                            if let Some(o) = observer.as_ref()
                                && !upstream_error_status
                            {
                                o.set_upstream_error(status, UpstreamErrorCode::StreamError);
                            }
                            stream_upstream_error_frame_emitted = true;
                            yield Ok::<Bytes, Infallible>(frame);
                        }
                    }
                    else if sse_transform_active
                        && !stream_provider_error_forwarded
                        && !stream_upstream_error_frame_emitted
                        && stream_transform_error.is_none()
                        && stream_affinity_error.is_none()
                    {
                        if transformed_output_started || !downstream_stream_is_identity {
                            let transform_error = ResponseTransformError::Runtime {
                                reason: if transformed_output_started {
                                    format!(
                                        "streaming response decode failed after transform output: {error}"
                                    )
                                } else {
                                    format!(
                                        "streaming response decode failed before transform output: {error}"
                                    )
                                },
                            };
                            let frame = make_response_transform_error_frame(
                                response_affinity_context.capability,
                                &responses_error_context,
                                &transform_error,
                            );
                            stream_transform_error = Some(transform_error);
                            downstream_drop_guard.mark_proxy_error(
                                StreamTerminationCause::TransformError,
                            );
                            yield Ok::<Bytes, Infallible>(frame);
                        } else {
                            debug_assert!(downstream_stream_is_identity);
                        }
                    }
                }
            }
            // A held error body that is complete, valid JSON is the provider's
            // error response: relay it unchanged. A candidate that fails
            // validation stays in `buffer` and reaches the framing error below.
            // Any blank events before it already passed through unchanged.
            // The body is the response's last bytes, so it joins the final
            // flush after the terminal proof below, like other final frames.
            if error_body_class == ErrorBodyClass::JsonCandidate
                && upstream_body_eof
                && parse_sse_events_active
                && sse_transform_active
                && stream_upstream_error.is_none()
                && !stream_provider_error_forwarded
                && !stream_upstream_error_frame_emitted
                && stream_transform_error.is_none()
                && stream_affinity_error.is_none()
                && held_error_body_is_json(&buffer)
            {
                raw_before_transform_output.clear();
                stream_provider_error_forwarded = true;
                batch_index = batch_index.saturating_add(1);
                deferred_terminal_chunks.push(buffer.split().freeze());
            }
            let mut keepalive_framing_complete = true;
            if native_sse_framing_error
                || (upstream_is_sse
                    && (!upstream_error_status || sse_transform_active)
                    && parse_sse_events_active
                    && buffer
                        .iter()
                        .any(|byte| !matches!(byte, b'\r' | b'\n'))
                    && stream_upstream_error.is_none()
                    && !stream_provider_error_forwarded
                    && !stream_upstream_error_frame_emitted
                    && stream_transform_error.is_none()
                    && stream_affinity_error.is_none())
            {
                keepalive_framing_complete = false;
                let error_message = if native_sse_framing_error {
                    gemini_native::JSON_SSE_FRAMING_ERROR_MESSAGE.to_owned()
                } else if transformed_output_started {
                    "upstream SSE response ended with an incomplete event after transform output"
                        .to_owned()
                } else {
                    "upstream SSE response ended with an incomplete event before transform output"
                        .to_owned()
                };
                if !stream_provider_error_seen {
                    stream_upstream_error = Some(cc_lb_lifecycle::StreamError {
                        error_type: UPSTREAM_RESPONSE_FRAMING_ERROR_TYPE.to_owned(),
                        error_message: error_message.clone(),
                        ..Default::default()
                    });
                    downstream_drop_guard.mark_upstream_error(
                        StreamTerminationCause::FramingError,
                        None,
                    );
                }
                if native_sse_framing_error
                    || success_sse_affinity_gate
                    || sse_transform_active
                    || downstream_stream_is_identity
                {
                    let frame = make_stream_error_frame(
                        response_affinity_context.capability,
                        &responses_error_context,
                        UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                        &error_message,
                        downstream_stream_is_identity
                            && !sse_transform_active
                            && downstream_sse_boundary.partial_event_pending(),
                    );
                    if let Some(o) = observer.as_ref()
                        && !upstream_error_status
                    {
                        o.set_upstream_error(status, UpstreamErrorCode::StreamError);
                    }
                    if response_affinity_context.capability
                        == ProviderCapability::GeminiGenerateContent
                    {
                        deferred_gemini_framing_error = Some(frame);
                    } else if deferred_terminal_chunks.is_empty() {
                        yield Ok::<Bytes, Infallible>(frame);
                    } else {
                        // Preserve byte order while retaining final-frame
                        // lifecycle bookkeeping, not the upstream byte length.
                        deferred_terminal_chunks.push(frame);
                    }
                }
                buffer.clear();
            }
            if upstream_is_sse
                && status.is_success()
                && parse_sse_events_active
                && transform_decode_supported
                && (semantic_terminal_event_type(response_affinity_context.capability).is_some()
                    || response_affinity_context.capability == ProviderCapability::OpenaiChatCompletions)
                && !semantic_completion_emitted
                && sse_buffer_is_blank(&buffer)
                && !downstream_drop_guard.has_abnormal_stop()
                && !stream_provider_error_seen
                && !stream_provider_error_forwarded
                && stream_upstream_error.is_none()
                && stream_transform_error.is_none()
                && stream_affinity_error.is_none()
            {
                if let Some(o) = observer.as_ref()
                    && usage.has_billable_usage()
                {
                    o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::UsageObserved {
                        event_id: o.event_id().to_owned(),
                        usage: to_usage_snapshot(&usage),
                        source: if response_affinity_context.capability == ProviderCapability::OpenaiChatCompletions {
                            cc_lb_lifecycle::UsageSource::Final
                        } else {
                            cc_lb_lifecycle::UsageSource::MessageDelta
                        },
                    });
                }
                let incomplete_message = if matches!(
                    response_affinity_context.capability,
                    ProviderCapability::OpenaiResponses
                        | ProviderCapability::OpenaiResponsesCompact
                ) {
                    "upstream SSE response ended before a terminal Responses event"
                } else if response_affinity_context.capability == ProviderCapability::OpenaiChatCompletions {
                    "upstream SSE response ended before a finished choice or [DONE]"
                } else {
                    "upstream SSE response ended before message_stop"
                };
                stream_upstream_error = Some(cc_lb_lifecycle::StreamError {
                    error_type: UPSTREAM_RESPONSE_INCOMPLETE_ERROR_TYPE.to_owned(),
                    error_message: incomplete_message.to_owned(),
                    ..Default::default()
                });
                downstream_drop_guard.mark_upstream_error(
                    StreamTerminationCause::Unknown,
                    None,
                );
                if matches!(
                    response_affinity_context.capability,
                    ProviderCapability::OpenaiResponses
                        | ProviderCapability::OpenaiResponsesCompact
                ) {
                    if let Some(o) = observer.as_ref() {
                        o.set_upstream_error(status, UpstreamErrorCode::StreamError);
                    }
                    let frame = make_stream_error_frame(
                        response_affinity_context.capability,
                        &responses_error_context,
                        UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                        incomplete_message,
                        false,
                    );
                    batch_index = batch_index.saturating_add(1);
                    for chunk in std::mem::take(&mut deferred_terminal_chunks) {
                        yield Ok::<Bytes, Infallible>(chunk);
                    }
                    yield Ok::<Bytes, Infallible>(frame);
                }
                if response_affinity_context.capability == ProviderCapability::OpenaiChatCompletions
                    && (success_sse_affinity_gate || sse_transform_active || downstream_stream_is_identity)
                {
                    let frame = make_stream_error_frame(
                        response_affinity_context.capability,
                        &responses_error_context,
                        UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                        incomplete_message,
                        false,
                    );
                    batch_index = batch_index.saturating_add(1);
                    // Commit the incomplete outcome before a client can close
                    // on this error frame, using the existing final flush.
                    deferred_terminal_chunks.push(frame);
                }
            }
            // EOF is evidence only after codec trailers and complete event
            // framing have been validated. Error flags alone never prove it.
            let sse_clean_terminal = upstream_body_eof
                && keepalive_decode_complete
                && !upstream_error_body_decode_failed
                && transform_decode_supported
                && keepalive_framing_complete
                && sse_buffer_is_blank(&buffer)
                && (!upstream_is_sse || parse_sse_events_active)
                && keepalive_http_end_is_terminal(
                    response_affinity_context.capability,
                    upstream_is_sse,
                    status,
                    semantic_completion_observed,
                );
            if sse_clean_terminal {
                downstream_drop_guard.mark_upstream_terminal();
            }
            if stream_affinity_error.is_none()
                && stream_upstream_error.is_none()
                && stream_transform_error.is_none()
                && !stream_provider_error_seen
                && status.is_success()
                && !pending_request_affinity_keys.is_empty()
                && bind_upstream_affinity_keys(
                    &response_affinity_context,
                    &pending_request_affinity_keys,
                    unix_secs(affinity_clock.now()),
                    None,
                )
                .await
                .is_err()
            {
                tracing::warn!(
                    parent: &stream_span,
                    "failed to persist upstream affinity at SSE completion"
                );
                stream_affinity_error = Some(affinity_stream_error());
                downstream_drop_guard.mark_proxy_error(
                    StreamTerminationCause::AffinityError,
                );
                if deferred_terminal_chunks.is_empty()
                    && (success_sse_affinity_gate
                        || sse_transform_active
                        || downstream_stream_is_identity)
                {
                    let frame = make_stream_error_frame(
                        response_affinity_context.capability,
                        &responses_error_context,
                        UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                        UPSTREAM_AFFINITY_UNAVAILABLE_MESSAGE,
                        false,
                    );
                    batch_index = batch_index.saturating_add(1);
                    yield Ok::<Bytes, Infallible>(frame);
                }
            }
            if sse_clean_terminal
                && stream_affinity_error.is_none()
                && stream_upstream_error.is_none()
                && stream_transform_error.is_none()
                && !stream_provider_error_seen
                && status.is_success()
                && !response_affinity_context.session_hint_keys.is_empty()
            {
                let hint_now = unix_secs(affinity_clock.now());
                for key in &response_affinity_context.session_hint_keys {
                    session_hint_cache.record(
                        &response_affinity_context.principal_id,
                        key,
                        response_affinity_context.upstream_id,
                        hint_now,
                    );
                }
            }
            downstream_drop_guard.detach_lifecycle_observer();
            stream_body_io_timing.stop();
            let response_body_completed_at = Instant::now();
            let elapsed_ms = |to: Option<Instant>| {
                to.map(|t| duration_to_ms(t.saturating_duration_since(relay_start)))
            };
            let stream_total_ms = duration_to_ms(
                response_body_completed_at.saturating_duration_since(relay_start),
            );
            let inter_token_avg_ms = match (first_content_delta_at, last_content_delta_at) {
                (Some(first), Some(last)) if content_delta_count > 1 => {
                    let span = last.saturating_duration_since(first);
                    Some(duration_to_ms(span) / (content_delta_count - 1))
                }
                _ => None,
            };
            // Final outcomes are settled by the reconcile subscriber. A
            // pre-terminal client cancellation still refunds through RAII.
            downstream_drop_guard.handoff_response_accounting();
            if stream_provider_error_seen {
                downstream_drop_guard.mark_upstream_error(
                    StreamTerminationCause::ProviderError,
                    None,
                );
            } else if stream_affinity_error.is_some() {
                downstream_drop_guard.mark_proxy_error(StreamTerminationCause::AffinityError);
            } else if let Some(error) = stream_upstream_error.as_ref() {
                let cause = match error.error_type.as_str() {
                    UPSTREAM_RESPONSE_DECODE_ERROR_TYPE => StreamTerminationCause::DecodeError,
                    UPSTREAM_RESPONSE_FRAMING_ERROR_TYPE => StreamTerminationCause::FramingError,
                    UPSTREAM_RESPONSE_BODY_ERROR_TYPE => StreamTerminationCause::Unknown,
                    _ => StreamTerminationCause::Unknown,
                };
                downstream_drop_guard.mark_upstream_error(cause, None);
            } else if stream_transform_error.is_some() {
                downstream_drop_guard.mark_proxy_error(StreamTerminationCause::TransformError);
            }
            if status.is_success()
                && stream_affinity_error.is_none()
                && stream_upstream_error.is_none()
                && stream_transform_error.is_none()
                && !stream_provider_error_seen
                && let Some(context) = prompt_cache_observation_context.as_ref()
            {
                publish_provider_cache_observation(
                    context,
                    PromptCacheUsage::from(&usage),
                    unix_secs(affinity_clock.now()),
                );
            }

            let finalize_ms = if let Some(o) = observer.as_ref() {
                if usage.has_billable_usage() {
                    o.set_usage_counts(&usage, message_stop_at.is_some());
                    if response_affinity_context.capability == ProviderCapability::OpenaiChatCompletions
                        && stream_provider_error_seen
                    {
                        // The provider error already completed the stream.
                        // Flush usage that partial-update throttling held back.
                        o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::UsageObserved {
                            event_id: o.event_id().to_owned(),
                            usage: to_usage_snapshot(&usage),
                            source: cc_lb_lifecycle::UsageSource::Final,
                        });
                    }
                }
                if let Some(first_content_delta_ms) = elapsed_ms(first_content_delta_at) {
                    o.set_first_content_delta_ms(first_content_delta_ms);
                }
                // #663: attribution evidence for abnormal upstream SSE body ends,
                // emitted at most once and before the terminal StreamCompleted.
                if upstream_is_sse
                    && let Some(diagnostics) = upstream_diagnostics
                        .into_lifecycle(total_bytes, elapsed_ms(message_stop_at))
                {
                    o.emit_lifecycle(
                        cc_lb_lifecycle::LifecycleEvent::UpstreamStreamDiagnosticsObserved {
                            event_id: o.event_id().to_owned(),
                            diagnostics,
                        },
                    );
                }
                if let Some(error) = stream_affinity_error.as_ref() {
                    o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::StreamCompleted {
                        event_id: o.event_id().to_owned(),
                        result: Err(error.clone()),
                    });
                } else if let Some(error) = stream_upstream_error.as_ref() {
                    o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::StreamCompleted {
                        event_id: o.event_id().to_owned(),
                        result: Err(error.clone()),
                    });
                } else if let Some(error) = stream_transform_error.as_ref() {
                    o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::StreamCompleted {
                        event_id: o.event_id().to_owned(),
                        result: Err(cc_lb_lifecycle::StreamError {
                            error_type: "response_transform_error".to_owned(),
                            error_message: error.to_string(),
                            ..Default::default()
                        }),
                    });
                } else if response_affinity_context.capability != ProviderCapability::OpenaiChatCompletions
                    || !stream_provider_error_seen
                {
                    o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::StreamCompleted {
                        event_id: o.event_id().to_owned(),
                        result: Ok(cc_lb_lifecycle::StreamSuccess {
                            usage: to_usage_snapshot(&usage),
                            usage_present: usage.has_billable_usage(),
                            sse_event_count,
                            body_bytes: Some(total_bytes),
                            body_chunk_count: Some(batch_index),
                            first_body_chunk_ms: elapsed_ms(first_chunk_at),
                            stream_message_start_ms: elapsed_ms(message_start_at),
                            stream_content_block_start_ms: elapsed_ms(content_block_start_at),
                            stream_first_content_delta_ms: elapsed_ms(first_content_delta_at),
                            stream_last_content_delta_ms: elapsed_ms(last_content_delta_at),
                            stream_message_stop_ms: elapsed_ms(message_stop_at),
                            stream_last_chunk_ms: elapsed_ms(last_chunk_at),
                            stream_total_ms: Some(stream_total_ms),
                            content_delta_count: Some(content_delta_count),
                            ping_count: Some(ping_count),
                            stream_inter_output_avg_ms: None,
                            provider_session_id: None,
                            provider_connection_id: None,
                            inter_token_avg_ms,
                        }),
                    });
                }
                o.set_termination_timings(
                    event_ctx.proxy_setup_ms,
                    Some(stream_total_ms),
                    elapsed_ms(first_chunk_at),
                );
                let canonical_error =
                    usage_parser::observe_http_error_body(&upstream_error_body);
                let retry_after = canonical_error
                    .as_ref()
                    .and_then(usage_parser::CanonicalUpstreamError::retry_after)
                    .map(ToOwned::to_owned);
                if upstream_error_status
                    && !upstream_error_body_decode_failed
                    && !upstream_error_body.is_empty()
                {
                    let (error_type, message, upstream_error_code) = canonical_error
                        .map(|error| error.into_parts(signer.as_ref()))
                        .unwrap_or_default();
                    o.emit_lifecycle(
                        cc_lb_lifecycle::LifecycleEvent::RequestLogUpstreamErrorObserved {
                            event_id: o.event_id().to_owned(),
                            error_type: error_type.unwrap_or_default(),
                            error_message: match message {
                                Some(message) => message,
                                None => scrubbed_upstream_error_body(
                                    signer.as_ref(),
                                    &upstream_error_body,
                                    upstream_error_body_truncated,
                                ),
                            },
                            upstream_error_code,
                            retry_after: (status == StatusCode::TOO_MANY_REQUESTS
                                && !has_retry_after_header)
                                .then_some(retry_after)
                                .flatten(),
                        },
                    );
                }
                if stream_affinity_error.is_some() && !upstream_error_status {
                    o.set_failure(InternalFailure {
                        status: StatusCode::OK,
                        error_code: error_codes::UPSTREAM_STREAM_ERROR,
                        error: InternalError {
                            stage: InternalErrorStage::Storage,
                            kind: InternalErrorKind::Unavailable,
                            message: Some(UPSTREAM_AFFINITY_UNAVAILABLE_MESSAGE.to_owned()),
                        },
                    });
                } else if stream_upstream_error.is_some() && !upstream_error_status {
                    o.set_upstream_error(status, UpstreamErrorCode::StreamError);
                } else if stream_transform_error.is_some() && !upstream_error_status {
                    o.set_upstream_error(StatusCode::OK, UpstreamErrorCode::StreamError);
                } else if !upstream_error_status
                    && !downstream_drop_guard.record_abnormal_stop(o)
                {
                    o.set_success_status(status);
                }
                let finalize_ms = duration_to_ms(response_body_completed_at.elapsed());
                o.set_finalize_ms(finalize_ms);
                o.set_io_timings(stream_body_io_timing.snapshot());
                o.finish();
                finalize_ms
            } else {
                duration_to_ms(response_body_completed_at.elapsed())
            };
            downstream_drop_guard.complete_keepalive();
            // Mandatory lifecycle, accounting, affinity, and termination work is complete.
            // Only best-effort latency logging crosses this bounded boundary.
            let stream_latency = StreamLatency {
                status: status.as_u16(),
                stream_first_chunk_ms: elapsed_ms(first_chunk_at),
                stream_message_start_ms: elapsed_ms(message_start_at),
                stream_content_block_start_ms: elapsed_ms(content_block_start_at),
                stream_first_content_delta_ms: elapsed_ms(first_content_delta_at),
                stream_last_content_delta_ms: elapsed_ms(last_content_delta_at),
                stream_message_stop_ms: elapsed_ms(message_stop_at),
                stream_last_chunk_ms: elapsed_ms(last_chunk_at),
                stream_first_output_ms: None,
                stream_last_output_ms: None,
                stream_event_count: 0,
                stream_inter_output_avg_ms: None,
                provider_session_id: None,
                provider_connection_id: None,
                stream_total_ms,
                sse_event_count,
                content_delta_count,
                ping_count,
                inter_token_avg_ms,
                total_bytes,
            };
            stream_latency.record_on_span(&stream_span);
            downstream_drop_guard.finish(stream_total_ms, finalize_ms);
            if let Some(dispatch) = stream_latency_log_dispatch {
                let _ = completion_observer.enqueue(StreamCompletionLog::new(
                    event_ctx.request_id,
                    &stream_span,
                    stream_latency,
                    dispatch,
                ));
            }
            for chunk in deferred_terminal_chunks {
                yield Ok::<Bytes, Infallible>(chunk);
            }
            if let Some(frame) = deferred_gemini_framing_error {
                yield Ok::<Bytes, Infallible>(frame);
            }
        };
        Response::from_parts(
            parts,
            Body::from_stream(TimedResponseStream::new(stream, downstream_poll_timing)),
        )
    }
}
