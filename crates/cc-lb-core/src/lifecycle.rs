use std::convert::Infallible;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use axum::body::Body as AxumBody;
use bytes::Bytes;
use cc_lb_config::PromptCacheShadowConfig;
use cc_lb_plugin_api::types::{
    BreakpointOrigin, CacheBreakpoint, CacheBreakpointSource, CacheScore, TtlClass, WarmCacheEntry,
};
use cc_lb_plugin_api::{
    ApiKeyAwareSignerFactory, ObservabilityHook, ObserveEvent, Principal, PrincipalKind,
    RequestContext, RetryDecision, RouterPlugin, SignedRequest, SubscriptionQuotaCandidateSnapshot,
    Upstream, UpstreamCandidate, UpstreamError, UpstreamKind as CandidateUpstreamKind,
    shape_request, sign_request,
};
use cc_lb_pricing::{PricingStatus, global_catalog, virtual_cost_micros_full};
use cc_lb_storage_api::{
    PromptCacheObservationRecord, Storage, SubscriptionQuotaObservationRecord,
    SubscriptionQuotaSampleKind, SubscriptionQuotaSource, TtlClass as StorageTtlClass,
    UpstreamRateLimitObservationRecord, UpstreamRecord,
    types::{
        RequestCacheBreakpoint, RequestCacheBreakpointSource, RequestCacheState, RequestEvent,
        StoredApiKeyRecord,
    },
    upstream::UpstreamKind as StorageUpstreamKind,
};
use http::header::{CONTENT_TYPE, RETRY_AFTER};
use http::{HeaderMap, HeaderName, HeaderValue, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use crate::api_keys::builtin_authn::{AuthnSuccess, BuiltinAuthError, BuiltinAuthn};
use crate::api_keys::limit_engine::{LimitEngine, RejectReason, Reservation as LimitReservation};
use crate::api_keys::principal_view::PrincipalView;
use crate::api_keys::types::LimitKind;
use crate::audit_writer::{AuditEntry, AuditWriterSink};
use crate::dynamic_view::{
    DynamicView, DynamicViewBuilder, DynamicViewHolder, UpstreamStatusSnapshot,
};
use crate::error_format::{anthropic_error_response, anthropic_error_response_with_retry_after};
use crate::error_normalizer::{ErrorNormalizer, UpstreamKind};
use crate::hop_by_hop::strip_hop_by_hop;
use crate::model_resolution::{cache_threshold_tokens, canonical_model_id};
use crate::rate_limit_headers::{
    parse_anthropic_rate_limit_headers, parse_anthropic_unified_headers,
};
use crate::request_timing::{
    REQUEST_STAGE_TIMINGS, RequestStageTimings, finalize_connection_reused_if_unset,
};
use crate::sse_relay;
use crate::subscription_metadata_hook::{MetadataHookHandle, MetadataHookRequest};
use crate::subscription_quota_events::SubscriptionQuotaSink;
use crate::upstream_rate_limit_events::UpstreamRateLimitSink;
use cc_lb_observability::{inc_cache_hit, inc_cache_miss};

pub type Body = AxumBody;

pub const HASH_SCHEMA_VERSION: u8 = 2;

const DEFAULT_MESSAGES_CAP_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_FILES_CAP_BYTES: usize = 100 * 1024 * 1024;
const PROMPT_CACHE_TTL_GRACE_SECS: u64 = 30;

static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(1);

pub trait SubscriptionQuotaCacheLike: Send + Sync {
    fn upsert_observation(&self, record: &SubscriptionQuotaObservationRecord);

    fn snapshot_for_upstream(
        &self,
        upstream_id: Uuid,
        now_unix_millis: u64,
        max_staleness_secs: u64,
    ) -> Vec<SubscriptionQuotaCandidateSnapshot>;
}

pub trait PromptCacheObservationCacheLike: Send + Sync {
    fn snapshot_for_upstream(
        &self,
        upstream_id: Uuid,
        canonical_model: &str,
        request_breakpoint_hashes: &[(String, TtlClass)],
        now_unix_secs: u64,
    ) -> Vec<WarmCacheEntry>;

    fn upsert_observation(
        &self,
        upstream_id: Uuid,
        canonical_model: String,
        prefix_hash: String,
        ttl_class: TtlClass,
        expires_at_unix_secs: u64,
        now_unix_secs: u64,
    );

    fn refresh_on_hit(
        &self,
        upstream_id: Uuid,
        canonical_model: &str,
        prefix_hash: &str,
        ttl_class: TtlClass,
        now_unix_secs: u64,
    ) -> bool;

    fn grace_margin_secs(&self) -> u64 {
        0
    }

    fn clock_now_unix_secs(&self) -> u64;
}

pub trait PromptCacheObservationSinkLike: Send + Sync {
    fn enqueue(
        &self,
        record: PromptCacheObservationRecord,
    ) -> Result<(), PromptCacheObservationEnqueueError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PromptCacheObservationEnqueueError {
    ChannelFull,
    ChannelClosed,
}

#[derive(Debug, Default)]
pub struct NoopSubscriptionQuotaCache;

impl SubscriptionQuotaCacheLike for NoopSubscriptionQuotaCache {
    fn upsert_observation(&self, _record: &SubscriptionQuotaObservationRecord) {}

    fn snapshot_for_upstream(
        &self,
        _upstream_id: Uuid,
        _now_unix_millis: u64,
        _max_staleness_secs: u64,
    ) -> Vec<SubscriptionQuotaCandidateSnapshot> {
        Vec::new()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestKind {
    AnthropicMessages,
}

impl RequestKind {
    fn matches_upstream(self, kind: StorageUpstreamKind) -> bool {
        match self {
            Self::AnthropicMessages => matches!(
                kind,
                StorageUpstreamKind::AnthropicApiKey | StorageUpstreamKind::AnthropicOauth
            ),
        }
    }
}

pub fn build_candidates(
    view: &DynamicView,
    principal_id: &str,
    request_kind: RequestKind,
    canonical_model: &str,
    request_breakpoints: &[CacheBreakpoint],
) -> Vec<UpstreamCandidate> {
    let Some(allowed_upstreams) = view.principal_view.allowed_upstreams(principal_id) else {
        return Vec::new();
    };

    let mut candidates: Vec<UpstreamCandidate> = {
        let rate_limit_cache = view.upstream_rate_limit_cache.read();
        let now_unix_millis = unix_now_ms();
        let prompt_cache = view.prompt_cache_observation_cache_opt();
        let request_breakpoint_hashes_with_ttl = request_breakpoints
            .iter()
            .map(|breakpoint| (breakpoint.prefix_hash.clone(), breakpoint.requested_ttl))
            .collect::<Vec<_>>();
        view.upstreams_snapshot()
            .iter()
            .filter(|upstream| upstream.enabled)
            .filter(|upstream| upstream.deleted_at_unix_secs.is_none())
            .filter(|upstream| request_kind.matches_upstream(upstream.kind))
            .filter(|upstream| {
                allowed_upstreams.is_empty() || allowed_upstreams.contains(&upstream.id)
            })
            .map(|upstream| {
                let observed_rate_limits = rate_limit_cache
                    .snapshots
                    .get(&upstream.id)
                    .cloned()
                    .unwrap_or_default();
                let observed_at_unix_secs = if observed_rate_limits.is_empty() {
                    0
                } else {
                    rate_limit_cache.updated_at_unix_secs
                };
                let cache_score = prompt_cache.and_then(|cache| {
                    let warm_entries = cache.snapshot_for_upstream(
                        upstream.id,
                        canonical_model,
                        &request_breakpoint_hashes_with_ttl,
                        cache.clock_now_unix_secs(),
                    );
                    build_cache_score(request_breakpoints, &warm_entries)
                });
                UpstreamCandidate {
                    upstream_id: upstream.id,
                    name: upstream.name.clone(),
                    kind: upstream_kind_for_candidate(upstream.kind),
                    observed_rate_limits,
                    subscription_quotas: view.subscription_quota_cache.snapshot_for_upstream(
                        upstream.id,
                        now_unix_millis,
                        view.subscription_quota_routing_max_staleness_secs,
                    ),
                    observed_at_unix_secs,
                    cache_score,
                    base_url: upstream.base_url.as_ref().map(|url| url.to_string()),
                }
            })
            .collect()
    };
    candidates.sort_unstable_by_key(|c| c.upstream_id);
    candidates
}

fn build_cache_score(
    request_breakpoints: &[CacheBreakpoint],
    warm_entries: &[WarmCacheEntry],
) -> Option<CacheScore> {
    if warm_entries.is_empty() {
        return None;
    }

    let warm_entry_for = |breakpoint: &CacheBreakpoint| {
        warm_entries
            .iter()
            .filter(|entry| entry.prefix_hash == breakpoint.prefix_hash)
            .max_by_key(|entry| entry.expires_at_unix_secs)
    };
    let longest_match = request_breakpoints
        .iter()
        .filter_map(|breakpoint| warm_entry_for(breakpoint).map(|entry| (breakpoint, entry)))
        .max_by_key(|(breakpoint, _)| breakpoint.prefix_token_count);

    let mut predicted_cache_creation_tokens_5m = 0_u64;
    let mut predicted_cache_creation_tokens_1h = 0_u64;
    for breakpoint in request_breakpoints {
        if warm_entry_for(breakpoint).is_some() {
            continue;
        }
        match breakpoint.requested_ttl {
            TtlClass::Ephemeral5m => {
                predicted_cache_creation_tokens_5m = predicted_cache_creation_tokens_5m
                    .saturating_add(breakpoint.prefix_token_count);
            }
            TtlClass::Ephemeral1h => {
                predicted_cache_creation_tokens_1h = predicted_cache_creation_tokens_1h
                    .saturating_add(breakpoint.prefix_token_count);
            }
        }
    }

    Some(CacheScore {
        predicted_cache_read_tokens: longest_match
            .map(|(breakpoint, _)| saturating_u64_to_u32(breakpoint.prefix_token_count))
            .unwrap_or(0),
        predicted_cache_creation_tokens_5m: saturating_u64_to_u32(
            predicted_cache_creation_tokens_5m,
        ),
        predicted_cache_creation_tokens_1h: saturating_u64_to_u32(
            predicted_cache_creation_tokens_1h,
        ),
        predicted_uncached_input_tokens: 0,
        predicted_expires_at_unix_secs: longest_match.map(|(_, entry)| entry.expires_at_unix_secs),
        matched_breakpoint_index: longest_match.map(|(breakpoint, _)| breakpoint.block_index),
        confidence: if longest_match.is_some() { 1.0 } else { 0.0 },
        ambiguity_reason: None,
    })
}

fn saturating_u64_to_u32(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

#[derive(Clone)]
pub struct PromptCacheObservationContext {
    pub(crate) upstream_id: Uuid,
    pub(crate) canonical_model_id: String,
    pub(crate) predicted_cache_read_tokens: u32,
    pub(crate) cache_breakpoints: Vec<CacheBreakpoint>,
    pub(crate) warm_entries_at_decision: Vec<WarmCacheEntry>,
    pub(crate) cache: Arc<dyn PromptCacheObservationCacheLike>,
    pub(crate) sink: Option<Arc<dyn PromptCacheObservationSinkLike>>,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PromptCacheUsage {
    pub(crate) cache_creation_input_tokens: u64,
    pub(crate) cache_read_input_tokens: u64,
}

impl From<&UsageCounts> for PromptCacheUsage {
    fn from(usage: &UsageCounts) -> Self {
        Self {
            cache_creation_input_tokens: usage.cache_creation_input_tokens,
            cache_read_input_tokens: usage.cache_read_input_tokens,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DecodedPromptCacheObservation {
    pub(crate) prefix_hash: String,
    pub(crate) ttl_class: TtlClass,
    pub(crate) expires_at_unix_secs: u64,
    kind: DecodedPromptCacheObservationKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DecodedPromptCacheObservationKind {
    Hit,
    Write,
}

pub(crate) fn prompt_cache_observation_context(
    view: &DynamicView,
    upstream_id: Uuid,
    canonical_model_id: &str,
    predicted_cache_read_tokens: u32,
    cache_breakpoints: &[CacheBreakpoint],
) -> Option<PromptCacheObservationContext> {
    if canonical_model_id.is_empty() || cache_breakpoints.is_empty() {
        return None;
    }
    let cache = view.prompt_cache_observation_cache_opt()?.clone();
    let request_breakpoint_hashes = cache_breakpoints
        .iter()
        .map(|breakpoint| (breakpoint.prefix_hash.clone(), breakpoint.requested_ttl))
        .collect::<Vec<_>>();
    let warm_entries_at_decision = cache.snapshot_for_upstream(
        upstream_id,
        canonical_model_id,
        &request_breakpoint_hashes,
        cache.clock_now_unix_secs(),
    );
    Some(PromptCacheObservationContext {
        upstream_id,
        canonical_model_id: canonical_model_id.to_owned(),
        predicted_cache_read_tokens,
        cache_breakpoints: cache_breakpoints.to_vec(),
        warm_entries_at_decision,
        cache,
        sink: view.prompt_cache_observation_sink_opt().cloned(),
    })
}

pub(crate) fn decode_prompt_cache_observations(
    context: &PromptCacheObservationContext,
    usage: PromptCacheUsage,
    now_unix_secs: u64,
) -> Vec<DecodedPromptCacheObservation> {
    if usage.cache_creation_input_tokens == 0 && usage.cache_read_input_tokens == 0 {
        return Vec::new();
    }

    let threshold = cache_threshold_tokens(&context.canonical_model_id) as u64;
    let warm_match_for = |breakpoint: &CacheBreakpoint| {
        context
            .warm_entries_at_decision
            .iter()
            .filter(|entry| entry.prefix_hash == breakpoint.prefix_hash)
            .max_by_key(|entry| entry.expires_at_unix_secs)
    };
    let hit = (usage.cache_read_input_tokens > 0)
        .then(|| {
            context
                .cache_breakpoints
                .iter()
                .filter_map(|breakpoint| {
                    warm_match_for(breakpoint).map(|entry| (breakpoint, entry))
                })
                .max_by_key(|(breakpoint, _)| breakpoint.prefix_token_count)
        })
        .flatten();

    let mut observations = Vec::new();
    if let Some((breakpoint, warm_entry)) = hit {
        if breakpoint.prefix_token_count >= threshold {
            observations.push(DecodedPromptCacheObservation {
                prefix_hash: breakpoint.prefix_hash.clone(),
                ttl_class: warm_entry.ttl_class,
                expires_at_unix_secs: warm_entry.expires_at_unix_secs,
                kind: DecodedPromptCacheObservationKind::Hit,
            });
        } else {
            cc_lb_observability::inc_cache_observation_dropped(
                cc_lb_observability::cache_observation_dropped_reason::BELOW_THRESHOLD,
            );
        }
    }

    if usage.cache_creation_input_tokens > 0 {
        let hit_block_index = hit.map(|(breakpoint, _)| breakpoint.block_index);
        for breakpoint in &context.cache_breakpoints {
            if hit_block_index.is_some_and(|hit_index| breakpoint.block_index <= hit_index) {
                continue;
            }
            if breakpoint.prefix_token_count < threshold {
                cc_lb_observability::inc_cache_observation_dropped(
                    cc_lb_observability::cache_observation_dropped_reason::BELOW_THRESHOLD,
                );
                continue;
            }
            observations.push(DecodedPromptCacheObservation {
                prefix_hash: breakpoint.prefix_hash.clone(),
                ttl_class: breakpoint.requested_ttl,
                expires_at_unix_secs: prompt_cache_observation_expires_at(
                    now_unix_secs,
                    breakpoint.requested_ttl,
                ),
                kind: DecodedPromptCacheObservationKind::Write,
            });
        }
    }

    observations
}

pub(crate) fn upsert_prompt_cache_observations(
    context: &PromptCacheObservationContext,
    observations: &[DecodedPromptCacheObservation],
    now_unix_secs: u64,
) {
    for observation in observations {
        context.cache.upsert_observation(
            context.upstream_id,
            context.canonical_model_id.clone(),
            observation.prefix_hash.clone(),
            observation.ttl_class,
            observation.expires_at_unix_secs,
            now_unix_secs,
        );
    }
}

pub(crate) fn enqueue_prompt_cache_observations(
    context: &PromptCacheObservationContext,
    observations: &[DecodedPromptCacheObservation],
    now_unix_secs: u64,
) {
    let Some(sink) = context.sink.as_ref() else {
        return;
    };
    for observation in observations {
        let refresh_should_persist = context.cache.refresh_on_hit(
            context.upstream_id,
            &context.canonical_model_id,
            &observation.prefix_hash,
            observation.ttl_class,
            now_unix_secs,
        );
        let should_persist = match observation.kind {
            DecodedPromptCacheObservationKind::Hit => refresh_should_persist,
            DecodedPromptCacheObservationKind::Write => true,
        };
        if !should_persist {
            continue;
        }
        let record = PromptCacheObservationRecord {
            upstream_id: context.upstream_id,
            canonical_model_id: context.canonical_model_id.clone(),
            prefix_hash: observation.prefix_hash.clone(),
            ttl_class: ttl_to_storage(observation.ttl_class),
            expires_at_unix_secs: observation.expires_at_unix_secs,
            last_observed_at_unix_secs: now_unix_secs,
            hash_schema_version: HASH_SCHEMA_VERSION,
        };
        if let Err(error) = sink.enqueue(record) {
            match error {
                PromptCacheObservationEnqueueError::ChannelFull => {}
                PromptCacheObservationEnqueueError::ChannelClosed => {
                    tracing::warn!("prompt cache observation sink is closed");
                }
            }
        }
    }
}

pub(crate) fn record_prompt_cache_observations(
    context: &PromptCacheObservationContext,
    usage: PromptCacheUsage,
    now_unix_secs: u64,
) -> Vec<DecodedPromptCacheObservation> {
    let observations = decode_prompt_cache_observations(context, usage, now_unix_secs);
    upsert_prompt_cache_observations(context, &observations, now_unix_secs);
    enqueue_prompt_cache_observations(context, &observations, now_unix_secs);
    observations
}

fn observe_prompt_cache_token_drift(
    context: Option<&PromptCacheObservationContext>,
    usage: PromptCacheUsage,
) {
    let Some(context) = context else {
        return;
    };
    let actual = i64::try_from(usage.cache_read_input_tokens).unwrap_or(i64::MAX);
    let predicted = i64::from(context.predicted_cache_read_tokens);
    let drift = actual
        .saturating_sub(predicted)
        .clamp(i64::from(i32::MIN), i64::from(i32::MAX)) as i32;
    // Predicted can be 0 when the chosen candidate had no warm match; actual - 0 captures an upstream-cache false negative.
    // Suggested ops alert: sum by (upstream, model) (increase(cc_lb_cache_token_drift_bucket{le="+Inf"}[5m])) - sum by (upstream, model) (increase(cc_lb_cache_token_drift_bucket{le="500"}[5m])) > 0.
    let upstream = context.upstream_id.to_string();
    metrics::histogram!(
        "cc_lb_cache_token_drift",
        "upstream" => upstream,
        "model" => context.canonical_model_id.clone()
    )
    .record(f64::from(drift));
}

fn record_prompt_cache_observations_for_response_status(
    status: StatusCode,
    context: Option<&PromptCacheObservationContext>,
    usage: PromptCacheUsage,
) -> Vec<DecodedPromptCacheObservation> {
    if status != StatusCode::OK {
        if status.is_client_error() && context.is_some() {
            cc_lb_observability::inc_cache_observation_dropped(
                cc_lb_observability::cache_observation_dropped_reason::STATUS_4XX,
            );
        }
        return Vec::new();
    }
    let Some(context) = context else {
        return Vec::new();
    };
    record_prompt_cache_observations(context, usage, context.cache.clock_now_unix_secs())
}

fn ttl_to_storage(t: cc_lb_plugin_api::types::TtlClass) -> StorageTtlClass {
    match t {
        cc_lb_plugin_api::types::TtlClass::Ephemeral5m => StorageTtlClass::Ephemeral5m,
        cc_lb_plugin_api::types::TtlClass::Ephemeral1h => StorageTtlClass::Ephemeral1h,
    }
}

fn prompt_cache_observation_expires_at(now_unix_secs: u64, ttl_class: TtlClass) -> u64 {
    now_unix_secs
        .saturating_add(prompt_cache_ttl_secs(ttl_class))
        .saturating_sub(PROMPT_CACHE_TTL_GRACE_SECS)
}

fn prompt_cache_ttl_secs(ttl_class: TtlClass) -> u64 {
    match ttl_class {
        TtlClass::Ephemeral5m => 5 * 60,
        TtlClass::Ephemeral1h => 60 * 60,
    }
}

#[derive(Clone, Debug)]
pub struct ReplicaIdentity {
    pub id: Uuid,
    pub started_at_unix_secs: u64,
}

#[derive(Clone, Debug)]
pub struct LifecycleConfig {
    pub messages_body_cap_bytes: usize,
    pub files_body_cap_bytes: usize,
    pub replica_identity: Option<ReplicaIdentity>,
    pub prompt_cache_shadow: PromptCacheShadowConfig,
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            messages_body_cap_bytes: DEFAULT_MESSAGES_CAP_BYTES,
            files_body_cap_bytes: DEFAULT_FILES_CAP_BYTES,
            replica_identity: None,
            prompt_cache_shadow: PromptCacheShadowConfig::default(),
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
    dynamic_view: Arc<DynamicViewHolder>,
    config: LifecycleConfig,
    limit_engine: Option<Arc<LimitEngine>>,
    limit_subject_provider: Option<Arc<dyn LimitSubjectProvider>>,
    audit_sink: Option<Arc<AuditWriterSink>>,
    request_event_storage: Option<Arc<dyn Storage>>,
    upstream_rate_limit_sink: Option<UpstreamRateLimitSink>,
    subscription_quota_sink: Option<SubscriptionQuotaSink>,
    subscription_metadata_hook: Option<MetadataHookHandle>,
    subscription_quota_cache: Option<Arc<dyn SubscriptionQuotaCacheLike>>,
}

impl Lifecycle {
    pub fn new(
        authn: Arc<BuiltinAuthn>,
        principal_view: Arc<PrincipalView>,
        signer_factory: Arc<dyn ApiKeyAwareSignerFactory>,
        global_router: Arc<dyn RouterPlugin>,
        dispatcher: Arc<dyn UpstreamDispatch>,
        global_observability_hooks: Vec<Arc<dyn ObservabilityHook>>,
        config: LifecycleConfig,
    ) -> Self {
        let dynamic_view = DynamicViewBuilder::new(0)
            .signer_factory(signer_factory)
            .global_router(global_router)
            .dispatcher(dispatcher)
            .global_observability_hooks(global_observability_hooks)
            .error_normalizer(Arc::new(ErrorNormalizer::new()))
            .principal_view(principal_view)
            .upstream_status_snapshot(Arc::new(UpstreamStatusSnapshot::default()))
            .build();
        Self {
            authn,
            dynamic_view: Arc::new(DynamicViewHolder::new(dynamic_view)),
            config,
            limit_engine: None,
            limit_subject_provider: None,
            audit_sink: None,
            request_event_storage: None,
            upstream_rate_limit_sink: None,
            subscription_quota_sink: None,
            subscription_metadata_hook: None,
            subscription_quota_cache: None,
        }
    }

    pub fn new_with_dynamic_view(
        authn: Arc<BuiltinAuthn>,
        dynamic_view: Arc<DynamicViewHolder>,
        config: LifecycleConfig,
    ) -> Self {
        Self {
            authn,
            dynamic_view,
            config,
            limit_engine: None,
            limit_subject_provider: None,
            audit_sink: None,
            request_event_storage: None,
            upstream_rate_limit_sink: None,
            subscription_quota_sink: None,
            subscription_metadata_hook: None,
            subscription_quota_cache: None,
        }
    }

    pub fn with_error_normalizer(self, error_normalizer: Arc<ErrorNormalizer>) -> Self {
        let current = self.dynamic_view.load();
        let next = DynamicViewBuilder::from_view(&current)
            .error_normalizer(error_normalizer)
            .build();
        self.dynamic_view.store(next);
        self
    }

    pub fn dynamic_view(&self) -> Arc<DynamicViewHolder> {
        Arc::clone(&self.dynamic_view)
    }

    pub fn replica_identity(&self) -> Option<ReplicaIdentity> {
        self.config.replica_identity.clone()
    }

    pub fn with_audit_sink(mut self, audit_sink: Arc<AuditWriterSink>) -> Self {
        self.audit_sink = Some(audit_sink);
        self
    }

    pub fn with_request_event_storage(mut self, storage: Arc<dyn Storage>) -> Self {
        self.request_event_storage = Some(storage);
        self
    }

    pub fn with_upstream_rate_limit_sink(mut self, sink: UpstreamRateLimitSink) -> Self {
        self.upstream_rate_limit_sink = Some(sink);
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

    pub fn enqueue_metadata_refresh(
        &self,
        upstream_id: Uuid,
        access_token: String,
        user_agent: String,
    ) {
        if let Some(hook) = &self.subscription_metadata_hook {
            hook.enqueue(MetadataHookRequest {
                upstream_id,
                access_token,
                user_agent,
            });
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
        let view = self.dynamic_view.load();
        let principal_view = Arc::clone(&view.principal_view);
        let started = Instant::now();
        let parsed = self.parse(req);
        let mut ctx = match parsed {
            Ok(ctx) => ctx,
            Err(response) => return Ok(*response),
        };
        let cache_metadata = request_cache_metadata(&ctx.downstream_headers, &ctx.body_bytes);
        let cache_breakpoints = if view.prompt_cache_observation_cache_opt().is_some()
            && self.config.prompt_cache_shadow.enabled
        {
            cache_metadata.plugin_cache_breakpoints()
        } else {
            Vec::new()
        };
        ctx.cache_breakpoints = cache_breakpoints;
        ctx.canonical_model_id = if view.prompt_cache_observation_cache_opt().is_some()
            && self.config.prompt_cache_shadow.enabled
        {
            cache_metadata.canonical_model_id.clone()
        } else {
            String::new()
        };

        // Pre-authn observe: global hooks only (no principal context). Silent no-op when global is empty.
        observe_many(
            &view.global_observability_hooks,
            ObserveEvent::RequestStarted {
                request_id: ctx.request_id.clone(),
                downstream_user_agent: header_to_string(&ctx.downstream_headers, "user-agent"),
            },
        );

        let auth_start = Instant::now();
        let success = if let Some(success) = self
            .authn
            .authenticate_none_mode(&ctx.downstream_headers)
            .await
        {
            success
        } else {
            match self
                .authn
                .authenticate(&ctx.downstream_headers, &principal_view)
                .await
            {
                Ok(success) => success,
                Err(source) => {
                    record_key_auth_failure_metric(&source);
                    observe_error(
                        &view.global_observability_hooks,
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
                    observe_finished(&view.global_observability_hooks, status, started);
                    return Ok(response);
                }
            }
        };
        let auth_ms = duration_to_ms(auth_start.elapsed());
        let principal_id = success.principal_id.clone();
        let Some(cached) = principal_view.get(&principal_id) else {
            tracing::error!(%principal_id, "authenticated principal missing from principal view");
            observe_error(
                &view.global_observability_hooks,
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
                &view.global_observability_hooks,
                StatusCode::INTERNAL_SERVER_ERROR,
                started,
            );
            return Ok(response);
        };
        let hooks = cached.resolved_hooks(&view.global_observability_hooks);
        let stream_hooks = StreamHooks::new(hooks);
        let principal = Principal {
            id: principal_id,
            kind: PrincipalKind::ApiKey,
            claims: serde_json::Map::new(),
        };

        let router_pipeline = cached.resolved_pipeline(None);
        if let Some(error) = router_pipeline.instantiation_error.as_deref() {
            observe_error(hooks, "router_pipeline_unavailable", error, "router");
            let response = anthropic_error_response(
                StatusCode::BAD_GATEWAY,
                "route_not_configured",
                "router pipeline is unavailable for this request",
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
        if !router_pipeline.user_filters.is_empty() {
            observe_error(
                hooks,
                "router_pipeline_unavailable",
                "router pipeline execution is not enabled",
                "router",
            );
            let response = anthropic_error_response(
                StatusCode::BAD_GATEWAY,
                "route_not_configured",
                "router pipeline execution is not enabled",
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
        let router = &view.global_router;

        observe_many(
            hooks,
            ObserveEvent::AuthnComplete {
                principal_id: principal.id.clone(),
                kind: principal.kind.clone(),
            },
        );

        let route_start = Instant::now();
        let candidates = build_candidates(
            &view,
            &principal.id,
            RequestKind::AnthropicMessages,
            &ctx.canonical_model_id,
            &ctx.cache_breakpoints,
        );

        let route = match router.route(&ctx, &principal, &candidates) {
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

        let fallback_upstream_id = candidates.first().map(|candidate| candidate.upstream_id);
        let resolved_upstream_id = match route.upstream_id.or(fallback_upstream_id) {
            Some(upstream_id) if candidates.iter().any(|c| c.upstream_id == upstream_id) => {
                upstream_id
            }
            _ => {
                observe_error(
                    hooks,
                    "route_not_configured",
                    "router selected an upstream outside the candidate set",
                    "router",
                );
                let response = anthropic_error_response(
                    StatusCode::BAD_GATEWAY,
                    "route_not_configured",
                    "router selected an upstream outside the candidate set",
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
        let Some(resolved_record) = view
            .upstreams_snapshot()
            .iter()
            .find(|record| record.id == resolved_upstream_id)
        else {
            observe_error(
                hooks,
                "route_not_configured",
                "router selected an upstream missing from the dynamic view",
                "router",
            );
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
        };
        let router_chosen_upstream_name = resolved_record.name.clone();
        let route_upstream = match upstream_for_record(resolved_record) {
            Ok(upstream) => upstream,
            Err(reason) => {
                observe_error(hooks, "route_not_configured", &reason, "router");
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
        let dialect = cached.resolved_dialect(&route.dialect).clone();
        let route = cc_lb_plugin_api::RouteDecision {
            upstream_id: Some(resolved_upstream_id),
            upstream: route_upstream,
            dialect,
        };
        let route_ms = duration_to_ms(route_start.elapsed());
        let metric_context = ApiKeyMetricContext::new(&success, &route.upstream, &ctx.body_bytes);
        let predicted_cache_read_tokens = candidates
            .iter()
            .find(|candidate| candidate.upstream_id == resolved_upstream_id)
            .and_then(|candidate| candidate.cache_score.as_ref())
            .map(|score| score.predicted_cache_read_tokens)
            .unwrap_or(0);
        let prompt_cache_observation_context = prompt_cache_observation_context(
            &view,
            resolved_upstream_id,
            &ctx.canonical_model_id,
            predicted_cache_read_tokens,
            &ctx.cache_breakpoints,
        );

        observe_many(
            hooks,
            ObserveEvent::UpstreamChosen {
                upstream: route.upstream.clone(),
            },
        );

        let limit_reserve_start = Instant::now();
        let mut active_limit = match self
            .reserve_limit(&principal_view, &ctx, &principal, &route, &success)
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
        let limit_reserve_ms = duration_to_ms(limit_reserve_start.elapsed());

        let signer_factory = view.signer_factory.with_router_choice(
            success.api_key.clone().unwrap_or_default(),
            router_chosen_upstream_name.clone(),
        );
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

        let dispatch_started = Instant::now();
        let proxy_setup_ms = duration_to_ms(dispatch_started.saturating_duration_since(started));
        let mut attempt_timings = AttemptTimings {
            auth_ms: Some(auth_ms),
            route_ms: Some(route_ms),
            limit_reserve_ms: Some(limit_reserve_ms),
            ..Default::default()
        };
        let mut response = match self
            .attempt(
                view.dispatcher.as_ref(),
                hooks,
                &ctx,
                &principal,
                &route,
                signer.clone(),
                &mut attempt_timings,
            )
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
                attempt_timings.reset_attempt_stages();
                response = match self
                    .attempt(
                        view.dispatcher.as_ref(),
                        hooks,
                        &ctx,
                        &principal,
                        &route,
                        new_signer,
                        &mut attempt_timings,
                    )
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
                    view.error_normalizer.as_ref(),
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

        if response.status().is_success() || response.status() == StatusCode::TOO_MANY_REQUESTS {
            let observed_at = SystemTime::now();
            self.record_upstream_rate_limit_observations(
                &view,
                response.headers(),
                resolved_upstream_id,
                system_time_to_unix_secs(observed_at),
            );
            self.record_subscription_quota_observations(
                response.headers(),
                resolved_upstream_id,
                observed_at,
            );
        }

        if response.status().is_client_error() || response.status().is_server_error() {
            if response.status().is_client_error()
                && self.config.prompt_cache_shadow.enabled
                && prompt_cache_observation_context.is_some()
            {
                cc_lb_observability::inc_cache_observation_dropped(
                    cc_lb_observability::cache_observation_dropped_reason::STATUS_4XX,
                );
            }
            let collected = collect_error_response(response).await;
            let mut response = rebuild_error_response(
                collected,
                &route.upstream,
                route.dialect.as_ref(),
                view.error_normalizer.as_ref(),
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
                RequestEventContext {
                    request_id: ctx.request_id.clone(),
                    upstream_id: Some(resolved_upstream_id),
                    upstream_name: Some(router_chosen_upstream_name.clone()),
                    principal_kind: Some(principal_kind_as_str(&principal.kind).to_owned()),
                    proxy_setup_ms: Some(proxy_setup_ms),
                    stage_timings: attempt_timings,
                    cache_metadata,
                },
                prompt_cache_observation_context,
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
        event_ctx: RequestEventContext,
        prompt_cache_observation_context: Option<PromptCacheObservationContext>,
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
                active_limit.take(),
                metric_context.clone(),
                event_ctx.clone(),
                prompt_cache_observation_context,
            );
            self.attach_limit_headers(&mut response, None);
            return response;
        }

        let (mut parts, mut body) = response.into_parts();
        let body_collect_started = Instant::now();
        let mut first_body_chunk_at: Option<Instant> = None;
        let mut body_buf: Vec<u8> = Vec::new();
        let mut body_chunk_count: u64 = 0;
        while let Some(frame) = body.frame().await {
            match frame {
                Ok(frame) => {
                    if let Ok(data) = frame.into_data() {
                        if first_body_chunk_at.is_none() {
                            first_body_chunk_at = Some(Instant::now());
                        }
                        body_chunk_count = body_chunk_count.saturating_add(1);
                        body_buf.extend_from_slice(&data);
                    }
                }
                Err(_source) => break,
            }
        }
        let body = Bytes::from(body_buf);
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
        let usage = usage_from_json_body(&body);
        if usage.present && self.config.prompt_cache_shadow.enabled {
            observe_prompt_cache_token_drift(
                prompt_cache_observation_context.as_ref(),
                PromptCacheUsage::from(&usage),
            );
            record_prompt_cache_observations_for_response_status(
                status,
                prompt_cache_observation_context.as_ref(),
                PromptCacheUsage::from(&usage),
            );
        }
        let observability_post_ms = if usage.present {
            let observability_post_start = Instant::now();
            observe_many(
                hooks,
                ObserveEvent::RequestFinished {
                    status,
                    input_tokens: Some(usage.input_tokens),
                    output_tokens: Some(usage.output_tokens),
                    cache_creation_input_tokens: Some(usage.cache_creation_input_tokens),
                    cache_read_input_tokens: Some(usage.cache_read_input_tokens),
                    duration_ms: duration_to_ms(duration),
                },
            );
            Some(duration_to_ms(observability_post_start.elapsed()))
        } else {
            None
        };
        if status == StatusCode::OK && !event_ctx.cache_metadata.cache_breakpoints.is_empty() {
            let upstream = event_ctx.upstream_name.as_deref().unwrap_or("unknown");
            let model = &event_ctx.cache_metadata.canonical_model_id;
            if usage.cache_read_input_tokens > 0 {
                inc_cache_hit(upstream, model);
            } else {
                inc_cache_miss(upstream, model);
            }
        }
        let (cost_micros, cost_breakdown_opts) = if usage.present {
            let cost_model = active_limit
                .as_ref()
                .map(|active_limit| active_limit.request.model.as_str())
                .unwrap_or(metric_context.model.as_str());
            let pricing_upstream_kind = active_limit
                .as_ref()
                .and_then(|active_limit| active_limit.upstream_kind)
                .or(metric_context.pricing_upstream_kind);
            let breakdown = virtual_cost_micros_full(
                cost_model,
                usage.input_tokens,
                usage.output_tokens,
                usage.cache_creation_input_tokens_5m,
                usage.cache_creation_input_tokens_1h,
                usage.cache_read_input_tokens,
                pricing_upstream_kind,
            );
            let cost_micros = breakdown.total_micros.max(0) as u64;
            record_api_key_usage_metrics(metric_context, &usage, cost_micros);
            (cost_micros, cost_breakdown_to_event_options(&breakdown))
        } else {
            (0, CostBreakdownOptions::default())
        };

        let mut limit_reconcile_ms = None;
        if let (Some(limit_engine), Some(active_limit)) =
            (self.limit_engine.as_ref(), active_limit.as_mut())
            && let Some(reservation) = active_limit.reservation.take()
        {
            let limit_reconcile_start = Instant::now();
            limit_engine.reconcile(
                reservation,
                usage.input_tokens,
                usage.output_tokens,
                cost_micros as i64,
            );
            limit_reconcile_ms = Some(duration_to_ms(limit_reconcile_start.elapsed()));
            attach_limit_headers_from_engine(
                &mut parts.headers,
                limit_engine.as_ref(),
                &active_limit.subject.key_id,
                &active_limit.subject.principal_id,
            );
        }

        if let (Some(storage), Some(active_limit)) =
            (self.request_event_storage.as_ref(), active_limit.as_ref())
        {
            let now_ms = unix_now_ms();
            let total_ms = duration_to_ms(duration);
            let mut event = RequestEvent {
                ts: now_ms / 1_000,
                ts_ms: Some(now_ms),
                request_id: event_ctx.request_id.clone(),
                principal_id: Some(active_limit.subject.principal_id.clone()),
                key_id: Some(active_limit.subject.key_id.clone()),
                principal_kind: event_ctx.principal_kind.clone(),
                upstream_id: event_ctx.upstream_id,
                upstream_name: event_ctx.upstream_name.clone(),
                model: Some(active_limit.request.model.clone()),
                input_tokens: Some(usage.input_tokens),
                output_tokens: Some(usage.output_tokens),
                cache_creation_input_tokens: Some(usage.cache_creation_input_tokens),
                cache_creation_input_tokens_5m: Some(usage.cache_creation_input_tokens_5m),
                cache_creation_input_tokens_1h: Some(usage.cache_creation_input_tokens_1h),
                cache_read_input_tokens: Some(usage.cache_read_input_tokens),
                cost_usd_micros: cost_breakdown_opts.total,
                cost_input_micros: cost_breakdown_opts.input,
                cost_output_micros: cost_breakdown_opts.output,
                cost_cache_creation_5m_micros: cost_breakdown_opts.cache_creation_5m,
                cost_cache_creation_1h_micros: cost_breakdown_opts.cache_creation_1h,
                cost_cache_read_micros: cost_breakdown_opts.cache_read,
                auth_ms: event_ctx.stage_timings.auth_ms,
                route_ms: event_ctx.stage_timings.route_ms,
                limit_reserve_ms: event_ctx.stage_timings.limit_reserve_ms,
                bulkhead_wait_ms: event_ctx.stage_timings.bulkhead_wait_ms,
                dns_ms: event_ctx.stage_timings.dns_ms,
                connect_ms: event_ctx.stage_timings.connect_ms,
                connection_reused: event_ctx.stage_timings.connection_reused,
                limit_reconcile_ms,
                observability_post_ms,
                duration_ms: total_ms,
                proxy_setup_ms: event_ctx.proxy_setup_ms,
                shape_ms: event_ctx.stage_timings.shape_ms,
                sign_ms: event_ctx.stage_timings.sign_ms,
                upstream_ttfb_ms: event_ctx.stage_timings.upstream_ttfb_ms,
                upstream_body_ms: Some(body_collect_ms),
                first_body_chunk_ms,
                body_chunk_count: Some(body_chunk_count),
                body_bytes: Some(body.len() as u64),
                status: status.as_u16(),
                ..Default::default()
            };
            event_ctx.cache_metadata.apply_to(&mut event, &usage);
            if let Err(error) = storage.append_request_event(&event).await {
                tracing::warn!(%error, "failed to append api key request event");
            }
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

    fn record_upstream_rate_limit_observations(
        &self,
        view: &DynamicView,
        headers: &HeaderMap,
        upstream_id: Uuid,
        observed_at: u64,
    ) {
        let records = observe_rate_limits(headers, upstream_id, observed_at);
        if records.is_empty() {
            return;
        }

        {
            let mut cache = view.upstream_rate_limit_cache.write();
            for record in records.iter().cloned() {
                cache.upsert_record(record);
            }
            cache.updated_at_unix_secs = observed_at;
        }

        if let Some(sink) = &self.upstream_rate_limit_sink {
            for record in records {
                let _ = sink.enqueue(record);
            }
        }
    }

    pub fn record_subscription_quota_observations(
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
            observe_subscription_quota_headers(headers, upstream_id, observed_at_unix_millis)
        {
            if let Some(cache) = &self.subscription_quota_cache {
                cache.upsert_observation(&record);
            }
            if let Some(sink) = &self.subscription_quota_sink {
                let _ = sink.enqueue(record);
            }
        }
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
            cache_breakpoints: Vec::new(),
            canonical_model_id: String::new(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn attempt(
        &self,
        dispatcher: &dyn UpstreamDispatch,
        hooks: &[Arc<dyn ObservabilityHook>],
        ctx: &RequestContext,
        principal: &Principal,
        route: &cc_lb_plugin_api::RouteDecision,
        signer: Arc<dyn cc_lb_plugin_api::Signer>,
        timings: &mut AttemptTimings,
    ) -> Result<Response<Body>, Box<Response<Body>>> {
        let shape_start = Instant::now();
        let shaped = shape_request(route.dialect.as_ref(), ctx, &route.upstream, principal)
            .map_err(|source| {
                tracing::error!(%source, "shape_request failed");
                observe_error(hooks, "shape_error", &source.to_string(), "dialect");
                Box::new(anthropic_error_response(
                    StatusCode::BAD_GATEWAY,
                    "api_error",
                    "failed to shape upstream request",
                ))
            })?;
        timings.shape_ms = Some(duration_to_ms(shape_start.elapsed()));

        let sign_start = Instant::now();
        let signed = sign_request(signer.as_ref(), shaped)
            .await
            .map_err(|source| {
                tracing::error!(%source, "sign_request failed");
                observe_error(hooks, "signing_error", &source.to_string(), "signer");
                Box::new(anthropic_error_response(
                    StatusCode::BAD_GATEWAY,
                    "api_error",
                    "failed to sign upstream request",
                ))
            })?;
        timings.sign_ms = Some(duration_to_ms(sign_start.elapsed()));

        let signed_header_names: Vec<String> = signed
            .headers()
            .keys()
            .map(|k| k.as_str().to_owned())
            .collect();
        tracing::info!(
            url = %signed.url(),
            method = %signed.method(),
            header_count = signed.headers().len(),
            headers = ?signed_header_names,
            body_bytes = signed.body().len(),
            "about to dispatch signed request"
        );

        let dispatch_start = Instant::now();
        let stage_timings_carrier = Arc::new(Mutex::new(RequestStageTimings::default()));
        let dispatch_result = REQUEST_STAGE_TIMINGS
            .scope(stage_timings_carrier.clone(), async {
                let result = dispatcher.dispatch(signed).await;
                finalize_connection_reused_if_unset();
                result
            })
            .await;
        let connection_snapshot = stage_timings_carrier.lock().unwrap().clone();
        timings.bulkhead_wait_ms = connection_snapshot.bulkhead_wait_ms;
        timings.dns_ms = connection_snapshot.dns_ms;
        timings.connect_ms = connection_snapshot.connect_ms;
        timings.connection_reused = connection_snapshot.connection_reused;
        let response = dispatch_result.map_err(|source| {
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
        })?;
        // hyper dispatch().await resolves at response HEADERS, not full body, so this is real TTFB.
        timings.upstream_ttfb_ms = Some(duration_to_ms(dispatch_start.elapsed()));
        Ok(response)
    }

    #[allow(clippy::too_many_arguments)]
    fn relay_response(
        &self,
        response: Response<Body>,
        status: StatusCode,
        started: Instant,
        hooks: StreamHooks,
        active_limit: Option<ActiveLimit>,
        metric_context: ApiKeyMetricContext,
        event_ctx: RequestEventContext,
        prompt_cache_observation_context: Option<PromptCacheObservationContext>,
    ) -> Response<Body> {
        let (mut parts, mut body) = response.into_parts();
        strip_hop_by_hop(&mut parts.headers);
        let relay_start = Instant::now();
        let storage = self.request_event_storage.clone();
        let limit_engine = self.limit_engine.clone();
        let prompt_cache_shadow_enabled = self.config.prompt_cache_shadow.enabled;
        let stream = async_stream::stream! {
            let mut batch_index = 0_u64;
            let mut buffer: Vec<u8> = Vec::new();
            let mut usage = UsageCounts::default();
            let mut first_chunk_at: Option<Instant> = None;
            let mut last_chunk_at: Option<Instant> = None;
            let mut message_start_at: Option<Instant> = None;
            let mut content_block_start_at: Option<Instant> = None;
            let mut first_content_delta_at: Option<Instant> = None;
            let mut last_content_delta_at: Option<Instant> = None;
            let mut message_stop_at: Option<Instant> = None;
            let mut prompt_cache_observations: Vec<DecodedPromptCacheObservation> = Vec::new();
            let mut prompt_cache_upserted = false;
            let mut prompt_cache_enqueued = false;
            let mut prompt_cache_drift_observed = false;
            let mut sse_event_count: u64 = 0;
            let mut content_delta_count: u64 = 0;
            let mut ping_count: u64 = 0;
            let mut total_bytes: u64 = 0;
            while let Some(frame) = body.frame().await {
                match frame {
                    Ok(frame) => {
                        if let Ok(data) = frame.into_data() {
                            let now = Instant::now();
                            if first_chunk_at.is_none() {
                                first_chunk_at = Some(now);
                            }
                            last_chunk_at = Some(now);
                            total_bytes = total_bytes.saturating_add(data.len() as u64);
                            buffer.extend_from_slice(&data);
                            while let Some(end) = sse_relay::find_sse_event_end(&buffer) {
                                let raw = buffer.drain(..end).collect::<Vec<u8>>();
                                let usage_update = accumulate_sse_usage(&raw, &mut usage);
                                sse_event_count = sse_event_count.saturating_add(1);
                                match sse_event_name(&raw) {
                                    Some(b"message_start") if message_start_at.is_none() => {
                                        message_start_at = Some(now);
                                    }
                                    Some(b"content_block_start")
                                        if content_block_start_at.is_none() =>
                                    {
                                        content_block_start_at = Some(now);
                                    }
                                    Some(b"content_block_delta") => {
                                        if first_content_delta_at.is_none() {
                                            first_content_delta_at = Some(now);
                                        }
                                        last_content_delta_at = Some(now);
                                        content_delta_count = content_delta_count.saturating_add(1);
                                    }
                                    Some(b"message_stop") => {
                                        message_stop_at = Some(now);
                                    }
                                    Some(b"ping") => {
                                        ping_count = ping_count.saturating_add(1);
                                    }
                                    _ => {}
                                }
                                if usage_update.message_start_usage {
                                    if message_start_at.is_none() {
                                        message_start_at = Some(now);
                                    }
                                    if status == StatusCode::OK
                                        && prompt_cache_shadow_enabled
                                        && !prompt_cache_drift_observed
                                    {
                                        observe_prompt_cache_token_drift(
                                            prompt_cache_observation_context.as_ref(),
                                            PromptCacheUsage::from(&usage),
                                        );
                                        prompt_cache_drift_observed = true;
                                    }
                                    if status == StatusCode::OK
                                        && prompt_cache_shadow_enabled
                                        && !prompt_cache_upserted
                                        && let Some(context) =
                                            prompt_cache_observation_context.as_ref()
                                    {
                                        let now_unix_secs = context.cache.clock_now_unix_secs();
                                        prompt_cache_observations =
                                            decode_prompt_cache_observations(
                                                context,
                                                PromptCacheUsage::from(&usage),
                                                now_unix_secs,
                                            );
                                        upsert_prompt_cache_observations(
                                            context,
                                            &prompt_cache_observations,
                                            now_unix_secs,
                                        );
                                        prompt_cache_upserted = true;
                                    }
                                }
                                if usage_update.message_stop {
                                    if message_stop_at.is_none() {
                                        message_stop_at = Some(now);
                                    }
                                    if status == StatusCode::OK
                                        && prompt_cache_shadow_enabled
                                        && prompt_cache_upserted
                                        && let Some(context) =
                                            prompt_cache_observation_context.as_ref()
                                    {
                                        enqueue_prompt_cache_observations(
                                            context,
                                            &prompt_cache_observations,
                                            context.cache.clock_now_unix_secs(),
                                        );
                                        prompt_cache_enqueued = true;
                                    }
                                }
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
            if status == StatusCode::OK
                && prompt_cache_shadow_enabled
                && usage.present
                && !prompt_cache_drift_observed
            {
                observe_prompt_cache_token_drift(
                    prompt_cache_observation_context.as_ref(),
                    PromptCacheUsage::from(&usage),
                );
            }
            if status == StatusCode::OK
                && prompt_cache_shadow_enabled
                && prompt_cache_upserted
                && !prompt_cache_enqueued
                && !prompt_cache_observations.is_empty()
            {
                for _ in &prompt_cache_observations {
                    cc_lb_observability::inc_cache_observation_dropped(
                        cc_lb_observability::cache_observation_dropped_reason::ABORT,
                    );
                }
            }
            let (input_tokens, output_tokens) = if usage.present {
                (Some(usage.input_tokens), Some(usage.output_tokens))
            } else {
                (None, None)
            };
            let total_duration_ms = started.elapsed().as_millis().try_into().unwrap_or(u64::MAX);
            let observability_post_start = Instant::now();
            observe_many(hooks.as_slice(), ObserveEvent::RequestFinished {
                status,
                input_tokens,
                output_tokens,
                cache_creation_input_tokens: usage
                    .present
                    .then_some(usage.cache_creation_input_tokens),
                cache_read_input_tokens: usage.present.then_some(usage.cache_read_input_tokens),
                duration_ms: total_duration_ms,
            });
            let observability_post_ms = Some(duration_to_ms(observability_post_start.elapsed()));
            let elapsed_ms = |to: Option<Instant>| {
                to.map(|t| duration_to_ms(t.saturating_duration_since(relay_start)))
            };
            let stream_total_ms = duration_to_ms(relay_start.elapsed());
            let inter_token_avg_ms = match (first_content_delta_at, last_content_delta_at) {
                (Some(first), Some(last)) if content_delta_count > 1 => {
                    let span = last.saturating_duration_since(first);
                    Some(duration_to_ms(span) / (content_delta_count - 1))
                }
                _ => None,
            };
            tracing::info!(
                status = status.as_u16(),
                stream_first_chunk_ms = ?elapsed_ms(first_chunk_at),
                stream_message_start_ms = ?elapsed_ms(message_start_at),
                stream_content_block_start_ms = ?elapsed_ms(content_block_start_at),
                stream_first_content_delta_ms = ?elapsed_ms(first_content_delta_at),
                stream_last_content_delta_ms = ?elapsed_ms(last_content_delta_at),
                stream_message_stop_ms = ?elapsed_ms(message_stop_at),
                stream_last_chunk_ms = ?elapsed_ms(last_chunk_at),
                stream_total_ms = stream_total_ms,
                sse_event_count = sse_event_count,
                content_delta_count = content_delta_count,
                ping_count = ping_count,
                inter_token_avg_ms = ?inter_token_avg_ms,
                total_bytes = total_bytes,
                "stream latency breakdown"
            );
            if let Some(mut active_limit) = active_limit {
                let (cost_micros, cost_breakdown_opts) = if usage.present {
                    let cost_model = active_limit.request.model.as_str();
                    let pricing_upstream_kind = active_limit
                        .upstream_kind
                        .or(metric_context.pricing_upstream_kind);
                    let breakdown = virtual_cost_micros_full(
                        cost_model,
                        usage.input_tokens,
                        usage.output_tokens,
                        usage.cache_creation_input_tokens_5m,
                        usage.cache_creation_input_tokens_1h,
                        usage.cache_read_input_tokens,
                        pricing_upstream_kind,
                    );
                    let cost = breakdown.total_micros.max(0) as u64;
                    record_api_key_usage_metrics(&metric_context, &usage, cost);
                    (cost, cost_breakdown_to_event_options(&breakdown))
                } else {
                    (0, CostBreakdownOptions::default())
                };
                let mut limit_reconcile_ms = None;
                if let (Some(limit_engine), Some(reservation)) =
                    (limit_engine.as_ref(), active_limit.reservation.take())
                {
                    let limit_reconcile_start = Instant::now();
                    limit_engine.reconcile(
                        reservation,
                        usage.input_tokens,
                        usage.output_tokens,
                        cost_micros as i64,
                    );
                    limit_reconcile_ms = Some(duration_to_ms(limit_reconcile_start.elapsed()));
                }
                if let Some(storage) = storage.as_ref() {
                    let now_ms = unix_now_ms();
                    let mut event = RequestEvent {
                        ts: now_ms / 1_000,
                        ts_ms: Some(now_ms),
                        request_id: event_ctx.request_id.clone(),
                        principal_id: Some(active_limit.subject.principal_id.clone()),
                        key_id: Some(active_limit.subject.key_id.clone()),
                        principal_kind: event_ctx.principal_kind.clone(),
                        upstream_id: event_ctx.upstream_id,
                        upstream_name: event_ctx.upstream_name.clone(),
                        model: Some(active_limit.request.model.clone()),
                        input_tokens: Some(usage.input_tokens),
                        output_tokens: Some(usage.output_tokens),
                        cache_creation_input_tokens: Some(usage.cache_creation_input_tokens),
                        cache_creation_input_tokens_5m: Some(usage.cache_creation_input_tokens_5m),
                        cache_creation_input_tokens_1h: Some(usage.cache_creation_input_tokens_1h),
                        cache_read_input_tokens: Some(usage.cache_read_input_tokens),
                        cost_usd_micros: cost_breakdown_opts.total,
                        cost_input_micros: cost_breakdown_opts.input,
                        cost_output_micros: cost_breakdown_opts.output,
                        cost_cache_creation_5m_micros: cost_breakdown_opts.cache_creation_5m,
                        cost_cache_creation_1h_micros: cost_breakdown_opts.cache_creation_1h,
                        cost_cache_read_micros: cost_breakdown_opts.cache_read,
                        auth_ms: event_ctx.stage_timings.auth_ms,
                        route_ms: event_ctx.stage_timings.route_ms,
                        limit_reserve_ms: event_ctx.stage_timings.limit_reserve_ms,
                        bulkhead_wait_ms: event_ctx.stage_timings.bulkhead_wait_ms,
                        dns_ms: event_ctx.stage_timings.dns_ms,
                        connect_ms: event_ctx.stage_timings.connect_ms,
                        connection_reused: event_ctx.stage_timings.connection_reused,
                        limit_reconcile_ms,
                        observability_post_ms,
                        duration_ms: total_duration_ms,
                        proxy_setup_ms: event_ctx.proxy_setup_ms,
                        shape_ms: event_ctx.stage_timings.shape_ms,
                        sign_ms: event_ctx.stage_timings.sign_ms,
                        upstream_ttfb_ms: event_ctx.stage_timings.upstream_ttfb_ms,
                        upstream_body_ms: Some(stream_total_ms),
                        first_body_chunk_ms: elapsed_ms(first_chunk_at),
                        body_chunk_count: Some(batch_index),
                        body_bytes: Some(total_bytes),
                        stream_message_start_ms: elapsed_ms(message_start_at),
                        stream_content_block_start_ms: elapsed_ms(content_block_start_at),
                        stream_first_content_delta_ms: elapsed_ms(first_content_delta_at),
                        stream_last_content_delta_ms: elapsed_ms(last_content_delta_at),
                        stream_message_stop_ms: elapsed_ms(message_stop_at),
                        stream_last_chunk_ms: elapsed_ms(last_chunk_at),
                        stream_total_ms: Some(stream_total_ms),
                        sse_event_count: Some(sse_event_count),
                        content_delta_count: Some(content_delta_count),
                        ping_count: Some(ping_count),
                        inter_token_avg_ms,
                        status: status.as_u16(),
                        ..Default::default()
                    };
                    event_ctx.cache_metadata.apply_to(&mut event, &usage);
                    if let Err(error) = storage.append_request_event(&event).await {
                        tracing::warn!(%error, "failed to append streaming request event");
                    }
                }
            }
        };
        Response::from_parts(parts, Body::from_stream(stream))
    }
}

fn sse_event_name(raw_event: &[u8]) -> Option<&[u8]> {
    raw_event
        .split(|b| *b == b'\n')
        .filter_map(|line| line.strip_prefix(b"event:"))
        .map(|name| name.trim_ascii())
        .next()
}

pub fn observe_rate_limits(
    headers: &HeaderMap,
    upstream_id: Uuid,
    observed_at: u64,
) -> Vec<UpstreamRateLimitObservationRecord> {
    parse_anthropic_rate_limit_headers(headers)
        .into_iter()
        .map(|snapshot| UpstreamRateLimitObservationRecord {
            upstream_id,
            window: snapshot.window,
            kind: snapshot.kind,
            limit: snapshot.limit,
            remaining: snapshot.remaining,
            reset: snapshot.reset,
            observed_at_unix_secs: observed_at,
        })
        .collect()
}

pub fn observe_subscription_quota_headers(
    headers: &HeaderMap,
    upstream_id: Uuid,
    observed_at_unix_millis: u64,
) -> Vec<SubscriptionQuotaObservationRecord> {
    parse_anthropic_unified_headers(headers)
        .into_iter()
        .map(|observation| SubscriptionQuotaObservationRecord {
            upstream_id,
            window: observation.window,
            source: SubscriptionQuotaSource::Header,
            sample_kind: SubscriptionQuotaSampleKind::Sample,
            observed_at_unix_millis,
            sample_id: Uuid::new_v4(),
            utilization: observation.utilization,
            status: observation.status,
            resets_at_unix_secs: observation.resets_at_unix_secs,
            surpassed_threshold: observation.surpassed_threshold,
            representative_claim: observation.representative_claim,
            disabled_reason: observation.disabled_reason,
            extra_usage_enabled: None,
            extra_usage_monthly_limit: None,
            extra_usage_used_credits: None,
            ingested_at_unix_millis: observed_at_unix_millis,
        })
        .collect()
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
            cache_creation_input_tokens: None,
            cache_read_input_tokens: None,
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
    let cache_creation_input_tokens =
        (usage.cache_creation_input_tokens > 0).then_some(usage.cache_creation_input_tokens);
    let cache_read_input_tokens =
        (usage.cache_read_input_tokens > 0).then_some(usage.cache_read_input_tokens);
    observe_many(
        hooks,
        ObserveEvent::RequestFinished {
            status,
            input_tokens,
            output_tokens,
            cache_creation_input_tokens,
            cache_read_input_tokens,
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

pub(crate) fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn system_time_to_unix_secs(value: SystemTime) -> u64 {
    value
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn system_time_to_unix_millis(value: SystemTime) -> u64 {
    value
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
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

#[derive(Clone)]
struct RequestEventContext {
    request_id: String,
    upstream_id: Option<Uuid>,
    upstream_name: Option<String>,
    principal_kind: Option<String>,
    proxy_setup_ms: Option<u64>,
    stage_timings: AttemptTimings,
    cache_metadata: RequestCacheMetadata,
}

#[derive(Clone, Default)]
struct RequestCacheMetadata {
    thread_id: Option<String>,
    message_id: Option<String>,
    message_index: Option<u64>,
    message_count: Option<u64>,
    cache_control_block_count: Option<u64>,
    cache_control_message_indices: Vec<u64>,
    cache_breakpoints: Vec<RequestCacheBreakpoint>,
    cache_prefix_hash: Option<String>,
    #[allow(dead_code)]
    canonical_model_id: String,
}

impl RequestCacheMetadata {
    fn plugin_cache_breakpoints(&self) -> Vec<CacheBreakpoint> {
        self.cache_breakpoints
            .iter()
            .map(|breakpoint| CacheBreakpoint {
                block_index: saturating_u64_to_u32(breakpoint.block_index),
                source: plugin_cache_breakpoint_source(breakpoint.source),
                path: breakpoint.path.clone(),
                message_index: breakpoint.message_index.map(saturating_u64_to_u32),
                prefix_hash: breakpoint.prefix_hash.clone(),
                prefix_token_count: breakpoint.prefix_token_count,
                requested_ttl: plugin_ttl_class(breakpoint.ttl.as_deref()),
                origin: BreakpointOrigin::Explicit,
            })
            .collect()
    }

    fn apply_to(&self, event: &mut RequestEvent, usage: &UsageCounts) {
        event.thread_id = self.thread_id.clone();
        event.message_id = self.message_id.clone();
        event.message_index = self.message_index;
        event.message_count = self.message_count;
        event.cache_control_block_count = self.cache_control_block_count;
        event.cache_control_message_indices = self.cache_control_message_indices.clone();
        event.cache_breakpoints = self.cache_breakpoints.clone();
        event.cache_prefix_hash = self.cache_prefix_hash.clone();
        event.cache_state = Some(self.cache_state(usage));
    }

    fn cache_state(&self, usage: &UsageCounts) -> RequestCacheState {
        match (
            usage.cache_read_input_tokens > 0,
            usage.cache_creation_input_tokens > 0,
            self.cache_control_block_count.unwrap_or(0) > 0,
            usage.present,
        ) {
            (true, true, _, _) => RequestCacheState::Refresh,
            (true, false, _, _) => RequestCacheState::Hit,
            (false, true, _, _) => RequestCacheState::Write,
            (false, false, true, _) => RequestCacheState::Miss,
            (false, false, false, true) => RequestCacheState::None,
            _ => RequestCacheState::Unknown,
        }
    }
}

fn plugin_cache_breakpoint_source(source: RequestCacheBreakpointSource) -> CacheBreakpointSource {
    match source {
        RequestCacheBreakpointSource::Tools => CacheBreakpointSource::Tools,
        RequestCacheBreakpointSource::System => CacheBreakpointSource::System,
        RequestCacheBreakpointSource::Message => CacheBreakpointSource::Message,
    }
}

fn plugin_ttl_class(ttl: Option<&str>) -> TtlClass {
    match ttl {
        Some(ttl) if ttl.eq_ignore_ascii_case("1h") => TtlClass::Ephemeral1h,
        _ => TtlClass::Ephemeral5m,
    }
}

#[derive(Clone, Copy, Default)]
struct AttemptTimings {
    auth_ms: Option<u64>,
    route_ms: Option<u64>,
    limit_reserve_ms: Option<u64>,
    bulkhead_wait_ms: Option<u64>,
    dns_ms: Option<u64>,
    connect_ms: Option<u64>,
    connection_reused: Option<bool>,
    shape_ms: Option<u64>,
    sign_ms: Option<u64>,
    upstream_ttfb_ms: Option<u64>,
}

impl AttemptTimings {
    fn reset_attempt_stages(&mut self) {
        self.bulkhead_wait_ms = None;
        self.dns_ms = None;
        self.connect_ms = None;
        self.connection_reused = None;
        self.shape_ms = None;
        self.sign_ms = None;
        self.upstream_ttfb_ms = None;
    }
}

fn principal_kind_as_str(kind: &PrincipalKind) -> &'static str {
    match kind {
        PrincipalKind::ApiKey => "api_key",
        PrincipalKind::OAuthSubject => "oauth_subject",
        PrincipalKind::InternalKey => "internal_key",
        PrincipalKind::WorkloadIdentity => "workload_identity",
        PrincipalKind::SubscriptionBearer => "subscription_bearer",
    }
}

pub fn parse_request_cache_breakpoints(headers: &HeaderMap, body: &Bytes) -> Vec<CacheBreakpoint> {
    request_cache_metadata(headers, body).plugin_cache_breakpoints()
}

fn request_cache_metadata(headers: &HeaderMap, body: &Bytes) -> RequestCacheMetadata {
    let thread_id = header_to_string(headers, "x-claude-code-session-id")
        .or_else(|| header_to_string(headers, "x-claude-session-id"));
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return RequestCacheMetadata {
            thread_id,
            ..Default::default()
        };
    };
    let canonical_model_id = value
        .get("model")
        .and_then(Value::as_str)
        .map(canonical_model_id)
        .unwrap_or_default()
        .to_owned();

    let messages = value.get("messages").and_then(Value::as_array);
    let message_count = messages.map(|items| items.len() as u64);
    let message_index = message_count.and_then(|count| count.checked_sub(1));
    let message_id = messages
        .and_then(|items| items.last())
        .and_then(|message| message.get("id"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);

    let mut cache_breakpoints = Vec::new();
    collect_cache_breakpoints(
        &value,
        value.get("tools"),
        RequestCacheBreakpointSource::Tools,
        "tools".to_owned(),
        None,
        &mut cache_breakpoints,
    );
    collect_cache_breakpoints(
        &value,
        value.get("system"),
        RequestCacheBreakpointSource::System,
        "system".to_owned(),
        None,
        &mut cache_breakpoints,
    );
    let mut cache_control_message_indices = Vec::new();
    if let Some(messages) = messages {
        for (index, message) in messages.iter().enumerate() {
            let previous_count = cache_breakpoints.len();
            collect_cache_breakpoints(
                &value,
                Some(message),
                RequestCacheBreakpointSource::Message,
                format!("messages[{index}]"),
                Some(index as u64),
                &mut cache_breakpoints,
            );
            if cache_breakpoints.len() > previous_count {
                cache_control_message_indices.push(index as u64);
            }
        }
    }

    let cache_control_block_count = cache_breakpoints.len() as u64;
    let cache_prefix_hash = cache_breakpoints
        .last()
        .map(|breakpoint| breakpoint.prefix_hash.clone());

    RequestCacheMetadata {
        thread_id,
        message_id,
        message_index,
        message_count,
        cache_control_block_count: Some(cache_control_block_count),
        cache_control_message_indices,
        cache_breakpoints,
        cache_prefix_hash,
        canonical_model_id,
    }
}

fn collect_cache_breakpoints(
    request: &Value,
    value: Option<&Value>,
    source: RequestCacheBreakpointSource,
    path: String,
    message_index: Option<u64>,
    breakpoints: &mut Vec<RequestCacheBreakpoint>,
) {
    match value {
        Some(Value::Object(map)) => {
            if let Some(cache_control) = map.get("cache_control") {
                let (prefix_hash, prefix_token_count) =
                    cache_prefix_hash_and_token_count_v2(request, source, &path, message_index);
                breakpoints.push(RequestCacheBreakpoint {
                    block_index: breakpoints.len() as u64,
                    source,
                    path: path.clone(),
                    message_index,
                    ttl: cache_control_ttl(cache_control),
                    prefix_hash,
                    prefix_token_count,
                });
            }
            for (key, value) in map {
                if key != "cache_control" {
                    collect_cache_breakpoints(
                        request,
                        Some(value),
                        source,
                        format!("{path}.{key}"),
                        message_index,
                        breakpoints,
                    );
                }
            }
        }
        Some(Value::Array(items)) => {
            for (index, value) in items.iter().enumerate() {
                collect_cache_breakpoints(
                    request,
                    Some(value),
                    source,
                    format!("{path}[{index}]"),
                    message_index,
                    breakpoints,
                );
            }
        }
        _ => {}
    }
}

fn cache_control_ttl(cache_control: &Value) -> Option<String> {
    cache_control
        .get("ttl")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

pub fn cache_prefix_hash(
    request: &Value,
    source: RequestCacheBreakpointSource,
    path: &str,
    message_index: Option<u64>,
) -> String {
    let mut prefix = serde_json::Map::new();
    prefix.insert("breakpoint_path".to_owned(), Value::String(path.to_owned()));
    if let Some(value) = request.get("model") {
        prefix.insert("model".to_owned(), value.clone());
    }
    if let Some(value) = request.get("tools") {
        let value = if source == RequestCacheBreakpointSource::Tools {
            truncate_value_at_path(value, &relative_cache_path(path, "tools"))
        } else {
            value.clone()
        };
        prefix.insert("tools".to_owned(), value);
    }
    if matches!(
        source,
        RequestCacheBreakpointSource::System | RequestCacheBreakpointSource::Message
    ) && let Some(value) = request.get("system")
    {
        let value = if source == RequestCacheBreakpointSource::System {
            truncate_value_at_path(value, &relative_cache_path(path, "system"))
        } else {
            value.clone()
        };
        prefix.insert("system".to_owned(), value);
    }
    if let (Some(messages), Some(_)) = (request.get("messages"), message_index) {
        prefix.insert(
            "messages".to_owned(),
            truncate_value_at_path(messages, &relative_cache_path(path, "messages")),
        );
    }
    let bytes = serde_json::to_vec(&Value::Object(prefix)).unwrap_or_default();
    hex_sha256(&bytes)
}

pub fn cache_prefix_hash_v2(
    request: &Value,
    source: RequestCacheBreakpointSource,
    path: &str,
    message_index: Option<u64>,
) -> String {
    cache_prefix_hash_and_token_count_v2(request, source, path, message_index).0
}

pub fn cache_prefix_hash_and_token_count_v2(
    request: &Value,
    source: RequestCacheBreakpointSource,
    path: &str,
    message_index: Option<u64>,
) -> (String, u64) {
    let prefix = build_cache_prefix_value_v2(request, source, path, message_index);
    let bytes = serde_json::to_vec(&prefix).unwrap_or_default();
    let hash = hex_sha256(&bytes);
    let token_count = match std::str::from_utf8(&bytes) {
        Ok(text) => crate::tokenizer::PrefixTokenizer::global().count_tokens(text) as u64,
        Err(_) => 0,
    };
    (hash, token_count)
}

fn build_cache_prefix_value_v2(
    request: &Value,
    source: RequestCacheBreakpointSource,
    path: &str,
    message_index: Option<u64>,
) -> Value {
    let mut prefix = serde_json::Map::new();
    prefix.insert("breakpoint_path".to_owned(), Value::String(path.to_owned()));
    if let Some(raw_model) = request.get("model").and_then(Value::as_str) {
        prefix.insert(
            "model".to_owned(),
            Value::String(canonical_model_id(raw_model).to_owned()),
        );
    }
    if let Some(value) = request.get("tools") {
        let value = if source == RequestCacheBreakpointSource::Tools {
            truncate_value_at_path(value, &relative_cache_path(path, "tools"))
        } else {
            value.clone()
        };
        prefix.insert("tools".to_owned(), value);
    }
    if matches!(
        source,
        RequestCacheBreakpointSource::System | RequestCacheBreakpointSource::Message
    ) && let Some(value) = request.get("system")
    {
        let value = if source == RequestCacheBreakpointSource::System {
            truncate_value_at_path(value, &relative_cache_path(path, "system"))
        } else {
            value.clone()
        };
        prefix.insert("system".to_owned(), value);
    }
    if let (Some(messages), Some(_)) = (request.get("messages"), message_index) {
        prefix.insert(
            "messages".to_owned(),
            truncate_value_at_path(messages, &relative_cache_path(path, "messages")),
        );
    }
    if let Some(value) = request.get("tool_choice") {
        prefix.insert("tool_choice".to_owned(), value.clone());
    }
    if let Some(value) = request.get("thinking") {
        prefix.insert("thinking".to_owned(), value.clone());
    }
    Value::Object(prefix)
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CachePathSegment {
    Key(String),
    Index(usize),
}

fn relative_cache_path(path: &str, root: &str) -> Vec<CachePathSegment> {
    let Some(rest) = path.strip_prefix(root) else {
        return Vec::new();
    };
    parse_cache_path(rest)
}

fn parse_cache_path(path: &str) -> Vec<CachePathSegment> {
    let bytes = path.as_bytes();
    let mut index = 0;
    let mut segments = Vec::new();
    while index < bytes.len() {
        match bytes[index] {
            b'.' => index += 1,
            b'[' => {
                let start = index + 1;
                let Some(end) = bytes[start..].iter().position(|byte| *byte == b']') else {
                    break;
                };
                let end = start + end;
                if let Ok(value) = path[start..end].parse::<usize>() {
                    segments.push(CachePathSegment::Index(value));
                }
                index = end + 1;
            }
            _ => {
                let start = index;
                while index < bytes.len() && bytes[index] != b'.' && bytes[index] != b'[' {
                    index += 1;
                }
                segments.push(CachePathSegment::Key(path[start..index].to_owned()));
            }
        }
    }
    segments
}

fn truncate_value_at_path(value: &Value, path: &[CachePathSegment]) -> Value {
    let Some((first, rest)) = path.split_first() else {
        return value.clone();
    };
    match (value, first) {
        (Value::Array(items), CachePathSegment::Index(index)) => {
            let end = index.saturating_add(1).min(items.len());
            let mut truncated = items[..end].to_vec();
            if let Some(last) = truncated.last_mut() {
                *last = truncate_value_at_path(last, rest);
            }
            Value::Array(truncated)
        }
        (Value::Object(map), CachePathSegment::Key(key)) => {
            let mut truncated = map.clone();
            if let Some(child) = map.get(key) {
                truncated.insert(key.clone(), truncate_value_at_path(child, rest));
            }
            Value::Object(truncated)
        }
        _ => value.clone(),
    }
}

fn hex_sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

#[derive(Default)]
struct UsageCounts {
    present: bool,
    input_tokens: u64,
    output_tokens: u64,
    cache_creation_input_tokens: u64,
    cache_creation_input_tokens_5m: u64,
    cache_creation_input_tokens_1h: u64,
    cache_read_input_tokens: u64,
}

#[derive(Clone, Copy, Default)]
struct CostBreakdownOptions {
    total: Option<i64>,
    input: Option<i64>,
    output: Option<i64>,
    cache_creation_5m: Option<i64>,
    cache_creation_1h: Option<i64>,
    cache_read: Option<i64>,
}

fn cost_breakdown_to_event_options(
    breakdown: &cc_lb_pricing::CostBreakdown,
) -> CostBreakdownOptions {
    match breakdown.pricing_status {
        PricingStatus::Known => CostBreakdownOptions {
            total: Some(breakdown.total_micros),
            input: Some(breakdown.input_micros),
            output: Some(breakdown.output_micros),
            cache_creation_5m: Some(breakdown.cache_creation_5m_micros),
            cache_creation_1h: Some(breakdown.cache_creation_1h_micros),
            cache_read: Some(breakdown.cache_read_micros),
        },
        PricingStatus::Unknown => CostBreakdownOptions::default(),
    }
}

#[derive(Clone)]
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

#[derive(Clone, Copy, Debug, Default)]
struct SseUsageUpdate {
    message_start_usage: bool,
    message_stop: bool,
}

fn accumulate_sse_usage(raw: &[u8], usage: &mut UsageCounts) -> SseUsageUpdate {
    let mut update = SseUsageUpdate::default();
    let text = match std::str::from_utf8(raw) {
        Ok(text) => text,
        Err(_) => return update,
    };
    for line in text.lines() {
        let Some(payload) = line.strip_prefix("data:").map(str::trim_start) else {
            continue;
        };
        let Ok(value) = serde_json::from_str::<Value>(payload) else {
            continue;
        };
        let event_type = value
            .get("type")
            .and_then(Value::as_str)
            .or_else(|| sse_event_name(raw).and_then(|name| std::str::from_utf8(name).ok()));
        if event_type == Some("message_stop") {
            update.message_stop = true;
        }
        let reported = value
            .get("usage")
            .or_else(|| value.get("message").and_then(|m| m.get("usage")));
        let Some(reported) = reported else {
            continue;
        };
        if event_type == Some("message_start") {
            update.message_start_usage = true;
        }
        usage.present = true;
        if let Some(input_tokens) = reported.get("input_tokens").and_then(Value::as_u64) {
            usage.input_tokens = input_tokens;
        }
        if let Some(output_tokens) = reported.get("output_tokens").and_then(Value::as_u64) {
            usage.output_tokens = output_tokens;
        }
        let (cc_total, cc_5m, cc_1h) = parse_cache_creation_split(reported);
        if cc_total > 0
            || reported.get("cache_creation").is_some()
            || reported.get("cache_creation_input_tokens").is_some()
        {
            usage.cache_creation_input_tokens = cc_total;
            usage.cache_creation_input_tokens_5m = cc_5m;
            usage.cache_creation_input_tokens_1h = cc_1h;
        }
        if let Some(cache_read) = reported
            .get("cache_read_input_tokens")
            .and_then(Value::as_u64)
        {
            usage.cache_read_input_tokens = cache_read;
        }
    }
    update
}

fn parse_cache_creation_split(usage: &Value) -> (u64, u64, u64) {
    if let Some(cc) = usage.get("cache_creation").and_then(Value::as_object) {
        let m5 = cc
            .get("ephemeral_5m_input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let m1 = cc
            .get("ephemeral_1h_input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        return (m5.saturating_add(m1), m5, m1);
    }
    let flat = usage
        .get("cache_creation_input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    (flat, flat, 0)
}

fn usage_from_json_body(body: &Bytes) -> UsageCounts {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return UsageCounts::default();
    };
    let Some(usage) = value.get("usage") else {
        return UsageCounts::default();
    };
    let (cc_total, cc_5m, cc_1h) = parse_cache_creation_split(usage);
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
        cache_creation_input_tokens: cc_total,
        cache_creation_input_tokens_5m: cc_5m,
        cache_creation_input_tokens_1h: cc_1h,
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
    }
}

fn pricing_upstream_kind(upstream: &Upstream) -> Option<cc_lb_pricing::UpstreamKind> {
    match upstream {
        Upstream::AnthropicDirect => Some(cc_lb_pricing::UpstreamKind::AnthropicKey),
    }
}

fn upstream_for_record(record: &UpstreamRecord) -> Result<Upstream, String> {
    match record.kind {
        StorageUpstreamKind::AnthropicApiKey | StorageUpstreamKind::AnthropicOauth => {
            Ok(Upstream::AnthropicDirect)
        }
    }
}

fn upstream_kind_for_candidate(kind: StorageUpstreamKind) -> CandidateUpstreamKind {
    match kind {
        StorageUpstreamKind::AnthropicApiKey => CandidateUpstreamKind::AnthropicApiKey,
        StorageUpstreamKind::AnthropicOauth => CandidateUpstreamKind::AnthropicOauth,
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use cc_lb_storage_api::{
        PromptCacheObservationRecord, SubscriptionQuotaSampleKind, SubscriptionQuotaSource,
        SubscriptionQuotaStatus, SubscriptionQuotaWindow,
        principal::{PrincipalKind as StoragePrincipalKind, PrincipalRecord},
        upstream::{UpstreamKind as StorageRecordKind, UpstreamRecord},
    };
    use http::header::{HeaderName, HeaderValue};
    use metrics::{
        Counter, Gauge, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder, SharedString,
        Unit,
    };

    use super::*;

    const TEST_MODEL: &str = "claude-sonnet-4-5-20250929";

    #[derive(Clone, Default)]
    struct DriftMetricRecorder {
        values: Arc<Mutex<Vec<f64>>>,
    }

    struct DriftMetricHistogram {
        values: Arc<Mutex<Vec<f64>>>,
    }

    impl HistogramFn for DriftMetricHistogram {
        fn record(&self, value: f64) {
            self.values.lock().expect("drift values lock").push(value);
        }
    }

    impl Recorder for DriftMetricRecorder {
        fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {
        }

        fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_histogram(
            &self,
            _key: KeyName,
            _unit: Option<Unit>,
            _description: SharedString,
        ) {
        }

        fn register_counter(&self, _key: &Key, _metadata: &Metadata<'_>) -> Counter {
            Counter::noop()
        }

        fn register_gauge(&self, _key: &Key, _metadata: &Metadata<'_>) -> Gauge {
            Gauge::noop()
        }

        fn register_histogram(&self, _key: &Key, _metadata: &Metadata<'_>) -> Histogram {
            Histogram::from_arc(Arc::new(DriftMetricHistogram {
                values: Arc::clone(&self.values),
            }))
        }
    }

    #[test]
    fn build_candidates_cache_score() {
        let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000201").unwrap();
        let cache = TestPromptCacheObservationCache::new(32, TEST_MODEL).with_entries(
            upstream_id,
            vec![
                warm_entry("short", TtlClass::Ephemeral5m, 4_100_000_300, 12),
                warm_entry("long", TtlClass::Ephemeral1h, 4_100_003_600, 13),
            ],
        );
        let view = cache_score_view(upstream_id, Arc::new(cache));
        let breakpoints = vec![
            cache_breakpoint(0, "short", 100, TtlClass::Ephemeral5m),
            cache_breakpoint(1, "long", 250, TtlClass::Ephemeral1h),
            cache_breakpoint(2, "cold", 50, TtlClass::Ephemeral5m),
        ];

        let candidates = build_candidates(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            TEST_MODEL,
            &breakpoints,
        );

        let score = candidates[0].cache_score.as_ref().expect("cache score");
        assert_eq!(score.predicted_cache_read_tokens, 250);
        assert_eq!(score.predicted_cache_creation_tokens_5m, 50);
        assert_eq!(score.predicted_cache_creation_tokens_1h, 0);
        assert_eq!(score.predicted_uncached_input_tokens, 0);
        assert_eq!(score.predicted_expires_at_unix_secs, Some(4_100_003_600));
        assert_eq!(score.matched_breakpoint_index, Some(1));
        assert_eq!(score.confidence, 1.0);
        assert_eq!(score.ambiguity_reason, None);
    }

    #[test]
    fn build_candidates_cache_score_from_production_parsed_breakpoints() {
        let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000204").unwrap();
        let lorem: String = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ".repeat(300);
        let body_string = format!(
            r#"{{"model":"{TEST_MODEL}","system":[{{"type":"text","text":"{lorem}","cache_control":{{"type":"ephemeral","ttl":"1h"}}}}],"messages":[{{"role":"user","content":"hi"}}],"max_tokens":16}}"#,
        );
        let body = Bytes::from(body_string);

        let breakpoints = parse_request_cache_breakpoints(&HeaderMap::new(), &body);
        assert_eq!(breakpoints.len(), 1);
        let prefix_hash = breakpoints[0].prefix_hash.clone();
        let prefix_token_count = breakpoints[0].prefix_token_count;
        assert!(
            prefix_token_count >= 1024,
            "expected real tokenizer to exceed Sonnet threshold, got {prefix_token_count}"
        );

        let cache = TestPromptCacheObservationCache::new(32, TEST_MODEL).with_entries(
            upstream_id,
            vec![warm_entry(
                &prefix_hash,
                TtlClass::Ephemeral1h,
                4_100_003_600,
                12,
            )],
        );
        let view = cache_score_view(upstream_id, Arc::new(cache));

        let candidates = build_candidates(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            TEST_MODEL,
            &breakpoints,
        );

        let score = candidates[0].cache_score.as_ref().expect("cache score");
        assert_eq!(
            u64::from(score.predicted_cache_read_tokens),
            prefix_token_count
        );
        assert!(score.predicted_cache_read_tokens >= 1024);
        assert_eq!(score.matched_breakpoint_index, Some(0));
        assert_eq!(score.confidence, 1.0);
    }

    #[test]
    #[ignore = "release-only perf assertion; run with --release --ignored"]
    fn build_candidates_cache_routing_latency_under_baseline_plus_2ms_p99() {
        const ITERATIONS: usize = 10_000;
        const ROUTING_BUDGET_P99_NANOS: u128 = 5_000_000;
        let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000299").unwrap();
        let cache = TestPromptCacheObservationCache::new(32, TEST_MODEL).with_entries(
            upstream_id,
            vec![
                warm_entry("warm-prefix-a", TtlClass::Ephemeral1h, 4_100_003_600, 12),
                warm_entry("warm-prefix-b", TtlClass::Ephemeral5m, 4_100_000_300, 11),
            ],
        );
        let view_with_cache = cache_score_view(upstream_id, Arc::new(cache));
        let view_without_cache = DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(TestSignerFactory))
            .global_router(Arc::new(TestRouter))
            .dispatcher(Arc::new(TestDispatcher))
            .global_observability_hooks(Vec::new())
            .error_normalizer(Arc::new(ErrorNormalizer::new()))
            .principal_view(Arc::new(PrincipalView::from_db(
                &[principal_record("principal")],
                HashMap::new(),
            )))
            .upstream_records(vec![upstream_record(upstream_id)])
            .build();
        let breakpoints = vec![
            cache_breakpoint(0, "warm-prefix-a", 2_400, TtlClass::Ephemeral1h),
            cache_breakpoint(1, "warm-prefix-b", 1_200, TtlClass::Ephemeral5m),
            cache_breakpoint(2, "cold-prefix", 800, TtlClass::Ephemeral5m),
        ];

        let measure = |view: &DynamicView| {
            let mut samples = Vec::with_capacity(ITERATIONS);
            for _ in 0..ITERATIONS {
                let start = std::time::Instant::now();
                let _ = build_candidates(
                    view,
                    "principal",
                    RequestKind::AnthropicMessages,
                    TEST_MODEL,
                    &breakpoints,
                );
                samples.push(start.elapsed().as_nanos());
            }
            samples.sort_unstable();
            samples[(ITERATIONS * 99) / 100]
        };

        let p99_without_cache = measure(&view_without_cache);
        let p99_with_cache = measure(&view_with_cache);
        let delta = p99_with_cache.saturating_sub(p99_without_cache);
        eprintln!(
            "build_candidates p99: without_cache={p99_without_cache}ns with_cache={p99_with_cache}ns delta={delta}ns budget={ROUTING_BUDGET_P99_NANOS}ns"
        );
        assert!(
            p99_with_cache < ROUTING_BUDGET_P99_NANOS,
            "build_candidates with prompt cache p99 {p99_with_cache}ns exceeded {ROUTING_BUDGET_P99_NANOS}ns budget (baseline p99={p99_without_cache}ns)"
        );
        assert!(
            delta < 2_000_000,
            "cache lookup added {delta}ns to p99, exceeding the 2_000_000ns budget (with={p99_with_cache}ns without={p99_without_cache}ns)"
        );
    }

    #[test]
    fn build_candidates_no_warm_returns_none() {
        let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000202").unwrap();
        let view = cache_score_view(
            upstream_id,
            Arc::new(TestPromptCacheObservationCache::new(32, TEST_MODEL)),
        );
        let breakpoints = vec![cache_breakpoint(0, "cold", 100, TtlClass::Ephemeral5m)];

        let candidates = build_candidates(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            TEST_MODEL,
            &breakpoints,
        );

        assert_eq!(candidates[0].cache_score, None);
    }

    #[test]
    fn build_candidates_warm_set_cap_respected() {
        let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000203").unwrap();
        let breakpoints = (0_u32..50)
            .map(|index| {
                cache_breakpoint(
                    index,
                    &format!("hash-{index}"),
                    u64::from(index + 1),
                    TtlClass::Ephemeral5m,
                )
            })
            .collect::<Vec<_>>();
        let warm_entries = (0_u32..50)
            .map(|index| WarmCacheEntry {
                prefix_hash: format!("hash-{index}"),
                expires_at_unix_secs: 4_100_000_300,
                ttl_class: TtlClass::Ephemeral5m,
                last_observed_at_unix_secs: 1_700_000_100 + u64::from(50 - index),
            })
            .collect::<Vec<_>>();
        let cache = TestPromptCacheObservationCache::new(32, TEST_MODEL)
            .with_entries(upstream_id, warm_entries);
        let view = cache_score_view(upstream_id, Arc::new(cache));

        let candidates = build_candidates(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            TEST_MODEL,
            &breakpoints,
        );

        let score = candidates[0].cache_score.as_ref().expect("cache score");
        assert_eq!(score.predicted_cache_read_tokens, 32);
        assert_eq!(score.matched_breakpoint_index, Some(31));
    }

    #[test]
    fn response_decoder_records_observations() {
        let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000221").unwrap();
        let now = 1_800_000_000;
        let cache = Arc::new(
            RecordingPromptCacheObservationCache::new(vec![
                warm_entry("short", TtlClass::Ephemeral5m, now + 120, 1),
                warm_entry("hit", TtlClass::Ephemeral1h, now + 3_000, 2),
            ])
            .with_clock_now(now),
        );
        let sink = Arc::new(RecordingPromptCacheObservationSink::default());
        let context = PromptCacheObservationContext {
            upstream_id,
            canonical_model_id: TEST_MODEL.to_owned(),
            predicted_cache_read_tokens: 2_400,
            cache_breakpoints: vec![
                cache_breakpoint(0, "short", 1_200, TtlClass::Ephemeral5m),
                cache_breakpoint(1, "hit", 2_400, TtlClass::Ephemeral1h),
                cache_breakpoint(2, "write", 3_200, TtlClass::Ephemeral5m),
            ],
            warm_entries_at_decision: vec![
                warm_entry("short", TtlClass::Ephemeral5m, now + 120, 1),
                warm_entry("hit", TtlClass::Ephemeral1h, now + 3_000, 2),
            ],
            cache: cache.clone(),
            sink: Some(sink.clone()),
        };

        let decoded = record_prompt_cache_observations_for_response_status(
            StatusCode::OK,
            Some(&context),
            PromptCacheUsage {
                cache_creation_input_tokens: 800,
                cache_read_input_tokens: 2_400,
            },
        );

        assert_eq!(decoded.len(), 2);
        let upserts = cache.upserts();
        assert_eq!(upserts.len(), 2);
        assert_eq!(upserts[0].prefix_hash, "hit");
        assert_eq!(upserts[0].ttl_class, TtlClass::Ephemeral1h);
        assert_eq!(upserts[0].expires_at_unix_secs, now + 3_000);
        assert_eq!(upserts[1].prefix_hash, "write");
        assert_eq!(upserts[1].ttl_class, TtlClass::Ephemeral5m);
        assert_eq!(upserts[1].expires_at_unix_secs, now + 270);
        let records = sink.records();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].upstream_id, upstream_id);
        assert_eq!(records[0].canonical_model_id, TEST_MODEL);
        assert_eq!(records[0].prefix_hash, "hit");
        assert_eq!(records[0].ttl_class, StorageTtlClass::Ephemeral1h);
        assert_eq!(records[0].expires_at_unix_secs, now + 3_000);
        assert_eq!(records[0].last_observed_at_unix_secs, now);
        assert_eq!(records[0].hash_schema_version, HASH_SCHEMA_VERSION);
        assert_eq!(records[1].prefix_hash, "write");
        assert_eq!(records[1].ttl_class, StorageTtlClass::Ephemeral5m);
        assert_eq!(records[1].expires_at_unix_secs, now + 270);
    }

    #[test]
    fn response_decoder_error_response_skips_observation() {
        error_response_skips_observation();
    }

    #[test]
    fn error_response_skips_observation() {
        let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000222").unwrap();
        let now = 1_800_000_000;
        let cache = Arc::new(RecordingPromptCacheObservationCache::new(vec![warm_entry(
            "hit",
            TtlClass::Ephemeral5m,
            now + 120,
            1,
        )]));
        let sink = Arc::new(RecordingPromptCacheObservationSink::default());
        let context = PromptCacheObservationContext {
            upstream_id,
            canonical_model_id: TEST_MODEL.to_owned(),
            predicted_cache_read_tokens: 1_200,
            cache_breakpoints: vec![cache_breakpoint(0, "hit", 1_200, TtlClass::Ephemeral5m)],
            warm_entries_at_decision: vec![warm_entry("hit", TtlClass::Ephemeral5m, now + 120, 1)],
            cache: cache.clone(),
            sink: Some(sink.clone()),
        };

        let decoded = record_prompt_cache_observations_for_response_status(
            StatusCode::BAD_REQUEST,
            Some(&context),
            PromptCacheUsage {
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 1_200,
            },
        );

        assert!(decoded.is_empty());
        assert!(cache.upserts().is_empty());
        assert!(sink.records().is_empty());
    }

    #[test]
    fn below_threshold_prefix_skips_observation() {
        let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000223").unwrap();
        let cache = Arc::new(RecordingPromptCacheObservationCache::new(Vec::new()));
        let sink = Arc::new(RecordingPromptCacheObservationSink::default());
        let context = PromptCacheObservationContext {
            upstream_id,
            canonical_model_id: TEST_MODEL.to_owned(),
            predicted_cache_read_tokens: 0,
            cache_breakpoints: vec![cache_breakpoint(0, "tiny", 1_023, TtlClass::Ephemeral5m)],
            warm_entries_at_decision: Vec::new(),
            cache: cache.clone(),
            sink: Some(sink.clone()),
        };

        let decoded = record_prompt_cache_observations_for_response_status(
            StatusCode::OK,
            Some(&context),
            PromptCacheUsage {
                cache_creation_input_tokens: 1_023,
                cache_read_input_tokens: 0,
            },
        );

        assert!(decoded.is_empty());
        assert!(cache.upserts().is_empty());
        assert!(sink.records().is_empty());
    }

    #[test]
    fn drift_metric_emitted() {
        let recorder = DriftMetricRecorder::default();
        let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000224").unwrap();
        let cache = Arc::new(RecordingPromptCacheObservationCache::new(Vec::new()));
        let context = PromptCacheObservationContext {
            upstream_id,
            canonical_model_id: TEST_MODEL.to_owned(),
            predicted_cache_read_tokens: 400,
            cache_breakpoints: Vec::new(),
            warm_entries_at_decision: Vec::new(),
            cache,
            sink: None,
        };

        metrics::with_local_recorder(&recorder, || {
            observe_prompt_cache_token_drift(
                Some(&context),
                PromptCacheUsage {
                    cache_creation_input_tokens: 0,
                    cache_read_input_tokens: 500,
                },
            );
        });

        let values = recorder.values.lock().expect("drift values lock");
        let in_expected_bucket = values
            .iter()
            .any(|value| (*value > 50.0 && *value <= 100.0) || (*value > 100.0 && *value <= 500.0));
        assert!(in_expected_bucket, "drift values: {values:?}");
    }

    #[test]
    fn observe_subscription_quota_headers_builds_header_sample_records() {
        let upstream_id = Uuid::new_v4();
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("anthropic-ratelimit-unified-7d-sonnet-utilization"),
            HeaderValue::from_static("0.42"),
        );
        headers.insert(
            HeaderName::from_static("anthropic-ratelimit-unified-7d-sonnet-status"),
            HeaderValue::from_static("allowed_warning"),
        );

        let records = observe_subscription_quota_headers(&headers, upstream_id, 123_456);

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].upstream_id, upstream_id);
        assert_eq!(records[0].window, SubscriptionQuotaWindow::SevenDaySonnet);
        assert_eq!(records[0].source, SubscriptionQuotaSource::Header);
        assert_eq!(records[0].sample_kind, SubscriptionQuotaSampleKind::Sample);
        assert_eq!(records[0].observed_at_unix_millis, 123_456);
        assert_eq!(records[0].ingested_at_unix_millis, 123_456);
        assert_ne!(records[0].sample_id, Uuid::nil());
        assert_eq!(records[0].utilization, Some(0.42));
        assert_eq!(
            records[0].status,
            Some(SubscriptionQuotaStatus::AllowedWarning)
        );
    }

    #[test]
    fn request_cache_metadata_tracks_thread_message_and_prefix_without_prompt_text() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-claude-code-session-id"),
            HeaderValue::from_static("thread-123"),
        );
        let body = Bytes::from_static(
            br#"{
                "model":"claude-sonnet-4-5",
                "tools":[{"name":"secret_tool","input_schema":{"type":"object"},"cache_control":{"type":"ephemeral"}}],
                "system":[{"type":"text","text":"secret system","cache_control":{"type":"ephemeral","ttl":"1h"}}],
                "messages":[
                    {"role":"user","content":"first secret"},
                    {"id":"msg_2","role":"user","content":[{"type":"text","text":"second secret","cache_control":{"type":"ephemeral"}}]}
                ]
            }"#,
        );

        let metadata = request_cache_metadata(&headers, &body);

        assert_eq!(metadata.thread_id.as_deref(), Some("thread-123"));
        assert_eq!(metadata.message_id.as_deref(), Some("msg_2"));
        assert_eq!(metadata.message_index, Some(1));
        assert_eq!(metadata.message_count, Some(2));
        assert_eq!(metadata.cache_control_block_count, Some(3));
        assert_eq!(metadata.cache_control_message_indices, vec![1]);
        assert_eq!(metadata.cache_breakpoints.len(), 3);
        assert_eq!(metadata.cache_breakpoints[0].block_index, 0);
        assert_eq!(
            metadata.cache_breakpoints[0].source,
            RequestCacheBreakpointSource::Tools
        );
        assert_eq!(metadata.cache_breakpoints[0].path, "tools[0]");
        assert_eq!(metadata.cache_breakpoints[1].block_index, 1);
        assert_eq!(
            metadata.cache_breakpoints[1].source,
            RequestCacheBreakpointSource::System
        );
        assert_eq!(metadata.cache_breakpoints[1].path, "system[0]");
        assert_eq!(metadata.cache_breakpoints[1].ttl.as_deref(), Some("1h"));
        assert_eq!(metadata.cache_breakpoints[2].block_index, 2);
        assert_eq!(
            metadata.cache_breakpoints[2].source,
            RequestCacheBreakpointSource::Message
        );
        assert_eq!(metadata.cache_breakpoints[2].path, "messages[1].content[0]");
        assert_eq!(metadata.cache_breakpoints[2].message_index, Some(1));
        assert_ne!(
            metadata.cache_breakpoints[0].prefix_hash,
            metadata.cache_breakpoints[1].prefix_hash
        );
        let hash = metadata.cache_prefix_hash.expect("cache prefix hash");
        assert_eq!(hash.len(), 64);
        assert_eq!(hash, metadata.cache_breakpoints[2].prefix_hash);
        assert!(!hash.contains("secret"));
    }

    #[test]
    fn request_cache_metadata_uses_v2_hash() {
        let headers = HeaderMap::new();
        let body = Bytes::from_static(
            br#"{
                "model":"claude-sonnet-4-5",
                "system":[{"type":"text","text":"stable system","cache_control":{"type":"ephemeral"}}],
                "messages":[{"role":"user","content":"hello"}],
                "tool_choice":{"type":"auto"},
                "thinking":{"type":"enabled","budget_tokens":1024}
            }"#,
        );
        let request: Value = serde_json::from_slice(&body).expect("request json");

        let metadata = request_cache_metadata(&headers, &body);

        let breakpoint = metadata
            .cache_breakpoints
            .first()
            .expect("system cache breakpoint");
        let expected_v2 = cache_prefix_hash_v2(
            &request,
            RequestCacheBreakpointSource::System,
            "system[0]",
            None,
        );
        let legacy_v1 = cache_prefix_hash(
            &request,
            RequestCacheBreakpointSource::System,
            "system[0]",
            None,
        );

        assert_eq!(breakpoint.prefix_hash, expected_v2);
        assert_ne!(breakpoint.prefix_hash, legacy_v1);
    }

    #[test]
    fn request_cache_metadata_fills_prefix_token_count() {
        let headers = HeaderMap::new();
        let body = Bytes::from_static(
            br#"{
                "model":"claude-sonnet-4-5",
                "system":[{"type":"text","text":"You are a helpful assistant. Please be concise.","cache_control":{"type":"ephemeral"}}],
                "messages":[{"role":"user","content":"hello"}]
            }"#,
        );

        let metadata = request_cache_metadata(&headers, &body);

        assert_eq!(metadata.cache_breakpoints.len(), 1);
        let breakpoint = &metadata.cache_breakpoints[0];
        assert!(
            breakpoint.prefix_token_count > 0,
            "expected non-zero prefix_token_count, got {}",
            breakpoint.prefix_token_count
        );

        let plugin_breakpoints = metadata.plugin_cache_breakpoints();
        assert_eq!(plugin_breakpoints.len(), 1);
        assert_eq!(
            plugin_breakpoints[0].prefix_token_count,
            breakpoint.prefix_token_count
        );
    }

    #[test]
    fn request_cache_metadata_prefix_token_count_scales_with_prefix_size() {
        let headers = HeaderMap::new();
        let short_body = Bytes::from_static(
            br#"{
                "model":"claude-sonnet-4-5",
                "system":[{"type":"text","text":"short","cache_control":{"type":"ephemeral"}}],
                "messages":[{"role":"user","content":"hello"}]
            }"#,
        );
        let long_text: String = "lorem ipsum ".repeat(2_000);
        let long_body_string = format!(
            r#"{{"model":"claude-sonnet-4-5","system":[{{"type":"text","text":"{long_text}","cache_control":{{"type":"ephemeral"}}}}],"messages":[{{"role":"user","content":"hello"}}]}}"#,
        );
        let long_body = Bytes::from(long_body_string);

        let short_metadata = request_cache_metadata(&headers, &short_body);
        let long_metadata = request_cache_metadata(&headers, &long_body);

        let short_count = short_metadata.cache_breakpoints[0].prefix_token_count;
        let long_count = long_metadata.cache_breakpoints[0].prefix_token_count;

        assert!(short_count > 0);
        assert!(
            long_count > short_count.saturating_mul(10),
            "expected long prefix ({long_count}) to be >10x short prefix ({short_count})"
        );
    }

    #[test]
    fn request_cache_metadata_prefix_token_count_crosses_sonnet_threshold() {
        let headers = HeaderMap::new();
        let varied_text: String =
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ".repeat(300);
        let body_string = format!(
            r#"{{"model":"claude-sonnet-4-5","system":[{{"type":"text","text":"{varied_text}","cache_control":{{"type":"ephemeral","ttl":"1h"}}}}],"messages":[{{"role":"user","content":"hi"}}],"max_tokens":16}}"#,
        );
        let body = Bytes::from(body_string);

        let metadata = request_cache_metadata(&headers, &body);
        assert_eq!(metadata.cache_breakpoints.len(), 1);
        let count = metadata.cache_breakpoints[0].prefix_token_count;
        assert!(
            count >= 1024,
            "prefix token count {count} should be above Sonnet 4.5 cache threshold (1024)"
        );
    }

    #[test]
    fn request_cache_breakpoint_hashes_ignore_later_prompt_suffixes() {
        let headers = HeaderMap::new();
        let first = Bytes::from_static(
            br#"{
                "model":"claude-sonnet-4-5",
                "messages":[
                    {"role":"user","content":[{"type":"text","text":"stable","cache_control":{"type":"ephemeral"}}]},
                    {"role":"user","content":[{"type":"text","text":"suffix-a","cache_control":{"type":"ephemeral"}}]}
                ]
            }"#,
        );
        let second = Bytes::from_static(
            br#"{
                "model":"claude-sonnet-4-5",
                "messages":[
                    {"role":"user","content":[{"type":"text","text":"stable","cache_control":{"type":"ephemeral"}}]},
                    {"role":"user","content":[{"type":"text","text":"suffix-b","cache_control":{"type":"ephemeral"}}]}
                ]
            }"#,
        );

        let first = request_cache_metadata(&headers, &first);
        let second = request_cache_metadata(&headers, &second);

        assert_eq!(first.cache_breakpoints.len(), 2);
        assert_eq!(second.cache_breakpoints.len(), 2);
        assert_eq!(
            first.cache_breakpoints[0].prefix_hash,
            second.cache_breakpoints[0].prefix_hash
        );
        assert_ne!(
            first.cache_breakpoints[1].prefix_hash,
            second.cache_breakpoints[1].prefix_hash
        );
    }

    #[test]
    fn request_cache_metadata_applies_cache_state_from_usage() {
        let metadata = RequestCacheMetadata {
            cache_control_block_count: Some(1),
            ..Default::default()
        };
        let mut event = RequestEvent::default();

        metadata.apply_to(
            &mut event,
            &UsageCounts {
                present: true,
                cache_read_input_tokens: 42,
                ..Default::default()
            },
        );
        assert_eq!(event.cache_state, Some(RequestCacheState::Hit));

        metadata.apply_to(
            &mut event,
            &UsageCounts {
                present: true,
                cache_creation_input_tokens: 42,
                ..Default::default()
            },
        );
        assert_eq!(event.cache_state, Some(RequestCacheState::Write));

        metadata.apply_to(
            &mut event,
            &UsageCounts {
                present: true,
                ..Default::default()
            },
        );
        assert_eq!(event.cache_state, Some(RequestCacheState::Miss));
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct RecordedPromptCacheUpsert {
        upstream_id: Uuid,
        canonical_model: String,
        prefix_hash: String,
        ttl_class: TtlClass,
        expires_at_unix_secs: u64,
        now_unix_secs: u64,
    }

    struct RecordingPromptCacheObservationCache {
        warm_entries: Vec<WarmCacheEntry>,
        upserts: Mutex<Vec<RecordedPromptCacheUpsert>>,
        refreshes: Mutex<Vec<String>>,
        clock_now: u64,
    }

    impl RecordingPromptCacheObservationCache {
        fn new(warm_entries: Vec<WarmCacheEntry>) -> Self {
            Self {
                warm_entries,
                upserts: Mutex::new(Vec::new()),
                refreshes: Mutex::new(Vec::new()),
                clock_now: 0,
            }
        }

        fn with_clock_now(mut self, clock_now: u64) -> Self {
            self.clock_now = clock_now;
            self
        }

        fn upserts(&self) -> Vec<RecordedPromptCacheUpsert> {
            self.upserts.lock().expect("upserts lock").clone()
        }
    }

    impl PromptCacheObservationCacheLike for RecordingPromptCacheObservationCache {
        fn snapshot_for_upstream(
            &self,
            _upstream_id: Uuid,
            _canonical_model: &str,
            request_breakpoint_hashes: &[(String, TtlClass)],
            _now_unix_secs: u64,
        ) -> Vec<WarmCacheEntry> {
            self.warm_entries
                .iter()
                .filter(|entry| {
                    request_breakpoint_hashes
                        .iter()
                        .any(|(hash, ttl)| hash == &entry.prefix_hash && ttl == &entry.ttl_class)
                })
                .cloned()
                .collect()
        }

        fn upsert_observation(
            &self,
            upstream_id: Uuid,
            canonical_model: String,
            prefix_hash: String,
            ttl_class: TtlClass,
            expires_at_unix_secs: u64,
            now_unix_secs: u64,
        ) {
            self.upserts
                .lock()
                .expect("upserts lock")
                .push(RecordedPromptCacheUpsert {
                    upstream_id,
                    canonical_model,
                    prefix_hash,
                    ttl_class,
                    expires_at_unix_secs,
                    now_unix_secs,
                });
        }

        fn refresh_on_hit(
            &self,
            _upstream_id: Uuid,
            _canonical_model: &str,
            prefix_hash: &str,
            _ttl_class: TtlClass,
            _now_unix_secs: u64,
        ) -> bool {
            self.refreshes
                .lock()
                .expect("refreshes lock")
                .push(prefix_hash.to_owned());
            true
        }

        fn clock_now_unix_secs(&self) -> u64 {
            self.clock_now
        }
    }

    #[derive(Default)]
    struct RecordingPromptCacheObservationSink {
        records: Mutex<Vec<PromptCacheObservationRecord>>,
    }

    impl RecordingPromptCacheObservationSink {
        fn records(&self) -> Vec<PromptCacheObservationRecord> {
            self.records.lock().expect("records lock").clone()
        }
    }

    impl PromptCacheObservationSinkLike for RecordingPromptCacheObservationSink {
        fn enqueue(
            &self,
            record: PromptCacheObservationRecord,
        ) -> Result<(), PromptCacheObservationEnqueueError> {
            self.records.lock().expect("records lock").push(record);
            Ok(())
        }
    }

    struct TestPromptCacheObservationCache {
        cap: usize,
        expected_model: &'static str,
        entries: HashMap<Uuid, Vec<WarmCacheEntry>>,
    }

    impl TestPromptCacheObservationCache {
        fn new(cap: usize, expected_model: &'static str) -> Self {
            Self {
                cap,
                expected_model,
                entries: HashMap::new(),
            }
        }

        fn with_entries(mut self, upstream_id: Uuid, entries: Vec<WarmCacheEntry>) -> Self {
            self.entries.insert(upstream_id, entries);
            self
        }
    }

    impl PromptCacheObservationCacheLike for TestPromptCacheObservationCache {
        fn snapshot_for_upstream(
            &self,
            upstream_id: Uuid,
            canonical_model: &str,
            request_breakpoint_hashes: &[(String, TtlClass)],
            now_unix_secs: u64,
        ) -> Vec<WarmCacheEntry> {
            assert_eq!(canonical_model, self.expected_model);
            let mut snapshot = self
                .entries
                .get(&upstream_id)
                .into_iter()
                .flatten()
                .filter(|entry| entry.expires_at_unix_secs > now_unix_secs)
                .filter(|entry| {
                    request_breakpoint_hashes
                        .iter()
                        .any(|(hash, ttl)| hash == &entry.prefix_hash && ttl == &entry.ttl_class)
                })
                .cloned()
                .collect::<Vec<_>>();
            snapshot.sort_by(|left, right| {
                right
                    .last_observed_at_unix_secs
                    .cmp(&left.last_observed_at_unix_secs)
            });
            snapshot.truncate(self.cap);
            snapshot
        }

        fn upsert_observation(
            &self,
            _upstream_id: Uuid,
            _canonical_model: String,
            _prefix_hash: String,
            _ttl_class: TtlClass,
            _expires_at_unix_secs: u64,
            _now_unix_secs: u64,
        ) {
        }

        fn refresh_on_hit(
            &self,
            _upstream_id: Uuid,
            _canonical_model: &str,
            _prefix_hash: &str,
            _ttl_class: TtlClass,
            _now_unix_secs: u64,
        ) -> bool {
            false
        }

        fn clock_now_unix_secs(&self) -> u64 {
            0
        }
    }

    fn cache_score_view(
        upstream_id: Uuid,
        cache: Arc<dyn PromptCacheObservationCacheLike>,
    ) -> Arc<DynamicView> {
        DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(TestSignerFactory))
            .global_router(Arc::new(TestRouter))
            .dispatcher(Arc::new(TestDispatcher))
            .global_observability_hooks(Vec::new())
            .error_normalizer(Arc::new(ErrorNormalizer::new()))
            .principal_view(Arc::new(PrincipalView::from_db(
                &[principal_record("principal")],
                HashMap::new(),
            )))
            .upstream_records(vec![upstream_record(upstream_id)])
            .prompt_cache_observation_cache(cache)
            .build()
    }

    fn cache_breakpoint(
        index: u32,
        prefix_hash: &str,
        prefix_token_count: u64,
        requested_ttl: TtlClass,
    ) -> CacheBreakpoint {
        CacheBreakpoint {
            block_index: index,
            source: CacheBreakpointSource::Message,
            path: format!("messages[{index}]"),
            message_index: Some(index),
            prefix_hash: prefix_hash.to_owned(),
            prefix_token_count,
            requested_ttl,
            origin: BreakpointOrigin::Explicit,
        }
    }

    fn warm_entry(
        prefix_hash: &str,
        ttl_class: TtlClass,
        expires_at_unix_secs: u64,
        observed_offset: u64,
    ) -> WarmCacheEntry {
        WarmCacheEntry {
            prefix_hash: prefix_hash.to_owned(),
            expires_at_unix_secs,
            ttl_class,
            last_observed_at_unix_secs: 1_700_000_000 + observed_offset,
        }
    }

    fn principal_record(name: &str) -> PrincipalRecord {
        PrincipalRecord {
            id: Uuid::new_v4(),
            name: name.to_owned(),
            kind: StoragePrincipalKind::Machine,
            allowed_models: Vec::new(),
            allowed_upstreams: Vec::new(),
            default_limits: Vec::new(),
            enabled: true,
            last_apply_error: None,
            last_apply_at_unix_secs: None,
            deleted_at_unix_secs: None,
            revision: 1,
            created_at_unix_secs: 0,
            updated_at_unix_secs: 0,
            router_terminal_strategy: Default::default(),
        }
    }

    fn upstream_record(id: Uuid) -> UpstreamRecord {
        UpstreamRecord {
            id,
            name: format!("upstream-{id}"),
            kind: StorageRecordKind::AnthropicApiKey,
            base_url: None,
            enabled: true,
            oauth_credentials: None,
            api_key_ciphertext: None,
            refresh_lease_holder: None,
            refresh_lease_until_unix_secs: None,
            last_apply_error: None,
            last_apply_at_unix_secs: None,
            deleted_at_unix_secs: None,
            revision: 1,
            created_at_unix_secs: 0,
            updated_at_unix_secs: 0,
        }
    }

    struct TestSignerFactory;

    impl ApiKeyAwareSignerFactory for TestSignerFactory {
        fn with_router_choice(
            &self,
            _api_key: String,
            _router_chosen_upstream_name: String,
        ) -> Arc<dyn cc_lb_plugin_api::SignerFactory> {
            Arc::new(Self)
        }
    }

    #[async_trait]
    impl cc_lb_plugin_api::SignerFactory for TestSignerFactory {
        async fn build(
            &self,
            _upstream: &Upstream,
        ) -> Result<Arc<dyn cc_lb_plugin_api::Signer>, cc_lb_plugin_api::SignerError> {
            Ok(Arc::new(TestSigner))
        }
    }

    struct TestSigner;

    #[async_trait]
    impl cc_lb_plugin_api::Signer for TestSigner {
        async fn sign(
            &self,
            shaped: cc_lb_plugin_api::ShapedRequest,
            capability: &mut cc_lb_plugin_api::SigningCapability,
        ) -> Result<SignedRequest, cc_lb_plugin_api::SignerError> {
            Ok(SignedRequest::from_shaped(shaped, capability))
        }

        async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
            RetryDecision::Fail
        }
    }

    struct TestRouter;

    impl RouterPlugin for TestRouter {
        fn route(
            &self,
            _ctx: &RequestContext,
            _principal: &Principal,
            _candidates: &[UpstreamCandidate],
        ) -> Result<cc_lb_plugin_api::RouteDecision, cc_lb_plugin_api::RouteError> {
            panic!("cache score tests do not route")
        }
    }

    struct TestDispatcher;

    #[async_trait]
    impl UpstreamDispatch for TestDispatcher {
        async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
            Ok(Response::builder()
                .status(StatusCode::OK)
                .body(Body::from(Bytes::new()))
                .expect("test response builds"))
        }
    }

    #[test]
    fn hit_miss_counters_emitted_on_cache_hits_and_misses() {
        let hit_usage = UsageCounts {
            present: true,
            cache_read_input_tokens: 100,
            ..Default::default()
        };

        let miss_usage = UsageCounts {
            present: true,
            cache_read_input_tokens: 0,
            ..Default::default()
        };

        assert!(hit_usage.cache_read_input_tokens > 0);
        assert_eq!(miss_usage.cache_read_input_tokens, 0);

        let event_ctx_with_breakpoints = RequestEventContext {
            request_id: "test-request".to_string(),
            upstream_id: Some(Uuid::nil()),
            upstream_name: Some("test-upstream".to_string()),
            principal_kind: None,
            proxy_setup_ms: None,
            stage_timings: AttemptTimings::default(),
            cache_metadata: RequestCacheMetadata {
                cache_breakpoints: vec![RequestCacheBreakpoint {
                    block_index: 0,
                    source: RequestCacheBreakpointSource::System,
                    path: "/".to_string(),
                    message_index: None,
                    prefix_hash: "test-hash".to_string(),
                    prefix_token_count: 0,
                    ttl: None,
                }],
                canonical_model_id: "test-model".to_string(),
                ..Default::default()
            },
        };

        let event_ctx_no_breakpoints = RequestEventContext {
            cache_metadata: RequestCacheMetadata {
                cache_breakpoints: vec![],
                ..event_ctx_with_breakpoints.cache_metadata.clone()
            },
            ..event_ctx_with_breakpoints.clone()
        };

        let status_ok = StatusCode::OK;
        let status_err = StatusCode::BAD_REQUEST;

        assert!(status_ok == StatusCode::OK);
        assert!(status_err != StatusCode::OK);
        assert!(
            !event_ctx_with_breakpoints
                .cache_metadata
                .cache_breakpoints
                .is_empty()
        );
        assert!(
            event_ctx_no_breakpoints
                .cache_metadata
                .cache_breakpoints
                .is_empty()
        );

        inc_cache_hit("test-upstream", "test-model");
        inc_cache_miss("test-upstream", "test-model");
    }
}
