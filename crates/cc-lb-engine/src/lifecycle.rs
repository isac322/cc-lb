use std::collections::HashSet;
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
    BreakpointOrigin, CacheBreakpoint, CacheBreakpointSource, CacheLookbackPrefix,
    CachePricingSummary, CacheScore, StageDecision, TerminalDecision, TtlClass, WarmCacheEntry,
};
use cc_lb_plugin_api::{
    ApiKeyAwareSignerFactory, FilterError, FilterOutput, InternalError, InternalErrorKind,
    InternalErrorStage, ObservabilityHook, ObserveEvent, Principal, PrincipalKind, RequestContext,
    ResponseTransformError, RetryDecision, RouterPlugin, RoutingTrace, ShapedRequest,
    ShapedRequestBuilder, SignedRequest, TerminalStrategy, TransformResponseRequest, Upstream,
    UpstreamCandidate, UpstreamDialect, UpstreamError, UpstreamKind as CandidateUpstreamKind,
    shape_request, sign_request,
};
use cc_lb_storage_api::{
    SubscriptionQuotaSample, SubscriptionQuotaSampleKind, SubscriptionQuotaSource,
    UpstreamRateLimitObservationRecord, UpstreamRecord,
    types::{RequestCacheBreakpoint, RequestCacheBreakpointSource, StoredApiKeyRecord},
    upstream::UpstreamKind as StorageUpstreamKind,
};
use http::header::{CONTENT_TYPE, RETRY_AFTER};
use http::{HeaderMap, HeaderName, HeaderValue, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use rand::{RngExt, SeedableRng, rngs::StdRng};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;
use url::Url;
use uuid::Uuid;

use crate::api_keys::builtin_authn::{AuthnSuccess, BuiltinAuthError, BuiltinAuthn};
use crate::api_keys::limit_engine::{LimitEngine, RejectReason, Reservation as LimitReservation};
use crate::api_keys::principal_view::PrincipalView;
use crate::api_keys::types::LimitKind;
use crate::clock::{Clock, ClockHandle, unix_millis};
use crate::error_format::{anthropic_error_response, anthropic_error_response_with_retry_after};
use crate::error_normalizer::ErrorNormalizer;
use crate::hop_by_hop::strip_hop_by_hop;
use crate::model_resolution::{cache_threshold_tokens, canonical_model_id};
use crate::prompt_cache_simulator::{
    V3_TOKEN_ESTIMATE_SOURCE, V3PromptCacheBlockSource, analyze_v3_prompt_cache,
};
use crate::rate_limit_headers::{
    parse_anthropic_rate_limit_headers, parse_anthropic_unified_headers,
};
use crate::request_timing::{
    REQUEST_STAGE_TIMINGS, RequestStageTimings, finalize_connection_reused_if_unset,
};
use crate::subscription_metadata_hook::{MetadataHookHandle, MetadataHookRequest};
use crate::subscription_quota_events::SubscriptionQuotaSink;
use crate::terminal_observer::{LifecycleContext, error_codes};
use crate::usage_decoder::{UsageDecoder, decode_full_body};
use crate::usage_parser::{
    self, UsageCounts, accumulate_sse_usage, sse_event_name, usage_from_json_body,
};
use cc_lb_contract::{ReplicaIdentity, RequestEventBus};
use cc_lb_control::dynamic_view::{
    DynamicView, DynamicViewBuilder, DynamicViewHolder, UpstreamStatusSnapshot,
};
pub use cc_lb_control::{
    PromptCacheObservationCacheLike, PromptCacheObservationEnqueueError,
    PromptCacheObservationSinkLike, PromptCacheThreadUsage, SubscriptionQuotaCacheLike,
};
use cc_lb_observability::{redact_internal_errors, truncate_reason};

pub type Body = AxumBody;

pub const HASH_SCHEMA_VERSION: u8 = 3;

const DEFAULT_MESSAGES_CAP_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_FILES_CAP_BYTES: usize = 100 * 1024 * 1024;
const DEFAULT_ANTHROPIC_BASE_URL: &str = "https://api.anthropic.com/";
const DEFAULT_MAX_INPUT_ESTIMATE: i64 = 4000;

static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(1);

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

#[derive(Debug, Clone)]
pub struct PreviewRouteInput {
    pub principal_id: String,
    /// Drives WRH tiebreak in `subscription-preference`. When `None`, a random
    /// `preview-<uuid>` id is generated so repeated calls do not collide.
    pub request_id: Option<String>,
    pub headers: HeaderMap,
    pub body_bytes: Bytes,
}

#[derive(Debug, Clone)]
pub struct PreviewRouteOutcome {
    pub trace: RoutingTrace,
    pub winner_upstream_id: Option<Uuid>,
    pub winner_upstream_name: Option<String>,
}

#[derive(Debug, Clone, Error)]
pub enum PreviewRouteError {
    #[error("principal not found: {0}")]
    PrincipalNotFound(String),
    #[error("router pipeline instantiation error: {0}")]
    PipelineInstantiationError(String),
}

pub fn build_candidates(
    view: &DynamicView,
    principal_id: &str,
    request_kind: RequestKind,
    canonical_model: &str,
    request_breakpoints: &[CacheBreakpoint],
    _thread_id: Option<&str>,
    clock: &dyn Clock,
) -> Vec<UpstreamCandidate> {
    let Some(allowed_upstreams) = view.principal_view.allowed_upstreams(principal_id) else {
        return Vec::new();
    };

    let mut candidates: Vec<UpstreamCandidate> = {
        let rate_limit_cache = view.upstream_rate_limit_cache.read();
        let now_unix_millis = unix_now_ms(clock);
        let prompt_cache = view.prompt_cache_observation_cache_opt();
        let request_breakpoint_hashes_with_ttl =
            request_lookback_hashes_with_ttl(request_breakpoints);
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
                    let now_unix_secs = cache.clock_now_unix_secs();
                    let warm_entries = cache.snapshot_for_upstream(
                        upstream.id,
                        canonical_model,
                        &request_breakpoint_hashes_with_ttl,
                        now_unix_secs,
                    );
                    build_cache_score(request_breakpoints, &warm_entries)
                });
                let plan_info = view.plan_info_by_upstream.get(&upstream.id);
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
                    plan_capacity_ratio: plan_info.map(|p| p.capacity_ratio),
                    organization_type: plan_info.and_then(|p| p.organization_type.clone()),
                    rate_limit_tier: plan_info.and_then(|p| p.rate_limit_tier.clone()),
                    seat_tier: plan_info.and_then(|p| p.seat_tier.clone()),
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
    if request_breakpoints.is_empty() {
        return None;
    }

    let warm_entry_for = |prefix_hash: &str| {
        warm_entries
            .iter()
            .filter(|entry| entry.prefix_hash == prefix_hash)
            .max_by_key(|entry| entry.expires_at_unix_secs)
    };
    let longest_match = request_breakpoints
        .iter()
        .flat_map(|breakpoint| {
            breakpoint
                .lookback_prefixes
                .iter()
                .filter_map(move |prefix| {
                    warm_entry_for(&prefix.prefix_hash).map(|entry| (breakpoint, prefix, entry))
                })
        })
        .max_by(|left, right| {
            left.1
                .prefix_token_count
                .cmp(&right.1.prefix_token_count)
                .then_with(|| right.1.lookback_distance.cmp(&left.1.lookback_distance))
        });

    let matched_prefix_tokens = longest_match
        .map(|(_, prefix, _)| prefix.prefix_token_count)
        .unwrap_or(0);
    let mut missing_breakpoints = request_breakpoints
        .iter()
        .filter(|breakpoint| breakpoint.prefix_token_count > matched_prefix_tokens)
        .collect::<Vec<_>>();
    missing_breakpoints.sort_unstable_by_key(|breakpoint| breakpoint.prefix_token_count);

    let mut predicted_cache_creation_tokens_5m = 0_u64;
    let mut predicted_cache_creation_tokens_1h = 0_u64;
    let mut previous_prefix_tokens = matched_prefix_tokens;
    for breakpoint in missing_breakpoints {
        let segment_tokens = breakpoint
            .prefix_token_count
            .saturating_sub(previous_prefix_tokens);
        previous_prefix_tokens = breakpoint.prefix_token_count;
        match breakpoint.requested_ttl {
            TtlClass::Ephemeral5m => {
                predicted_cache_creation_tokens_5m =
                    predicted_cache_creation_tokens_5m.saturating_add(segment_tokens);
            }
            TtlClass::Ephemeral1h => {
                predicted_cache_creation_tokens_1h =
                    predicted_cache_creation_tokens_1h.saturating_add(segment_tokens);
            }
        }
    }

    Some(CacheScore {
        predicted_cache_read_tokens: longest_match
            .map(|(_, prefix, _)| saturating_u64_to_u32(prefix.prefix_token_count))
            .unwrap_or(0),
        predicted_cache_creation_tokens_5m: saturating_u64_to_u32(
            predicted_cache_creation_tokens_5m,
        ),
        predicted_cache_creation_tokens_1h: saturating_u64_to_u32(
            predicted_cache_creation_tokens_1h,
        ),
        predicted_uncached_input_tokens: 0,
        predicted_expires_at_unix_secs: longest_match
            .map(|(_, _, entry)| entry.expires_at_unix_secs),
        matched_breakpoint_index: longest_match.map(|(breakpoint, _, _)| breakpoint.block_index),
        confidence: if longest_match.is_some() { 1.0 } else { 0.0 },
        ambiguity_reason: None,
        matched_v3_cache_key: longest_match.map(|(_, prefix, _)| prefix.prefix_hash.clone()),
        breakpoint_content_block_index: longest_match
            .map(|(breakpoint, _, _)| breakpoint.block_index),
        matched_content_block_index: longest_match.map(|(_, prefix, _)| prefix.content_block_index),
        lookback_distance: longest_match.map(|(_, prefix, _)| prefix.lookback_distance),
        token_estimate_source: longest_match
            .and_then(|(breakpoint, _, _)| breakpoint.token_estimate_source.clone()),
    })
}

fn request_lookback_hashes_with_ttl(
    request_breakpoints: &[CacheBreakpoint],
) -> Vec<(String, TtlClass)> {
    request_breakpoints
        .iter()
        .flat_map(|breakpoint| {
            breakpoint
                .lookback_prefixes
                .iter()
                .map(move |prefix| (prefix.prefix_hash.clone(), breakpoint.requested_ttl))
        })
        .collect()
}

#[cfg(test)]
mod cache_score_tests {
    use super::{anthropic_family_cache_pricing_summary, build_cache_score};
    use cc_lb_plugin_api::types::{
        BreakpointOrigin, CacheBreakpoint, CacheBreakpointSource, TtlClass, WarmCacheEntry,
    };

    #[test]
    fn build_cache_score_counts_missing_breakpoints_as_incremental_segments_by_ttl() {
        let score = build_cache_score(
            &[
                breakpoint(0, 100_000, TtlClass::Ephemeral1h),
                breakpoint(1, 300_000, TtlClass::Ephemeral5m),
                breakpoint(2, 500_000, TtlClass::Ephemeral5m),
            ],
            &[],
        )
        .expect("cache score for cacheable request");

        assert_eq!(score.predicted_cache_read_tokens, 0);
        assert_eq!(score.predicted_cache_creation_tokens_1h, 100_000);
        assert_eq!(score.predicted_cache_creation_tokens_5m, 400_000);
    }

    #[test]
    fn build_cache_score_only_counts_segments_after_longest_live_breakpoint() {
        let score = build_cache_score(
            &[
                breakpoint(0, 100_000, TtlClass::Ephemeral1h),
                breakpoint(1, 300_000, TtlClass::Ephemeral5m),
                breakpoint(2, 500_000, TtlClass::Ephemeral5m),
            ],
            &[warm_entry("bp-1", TtlClass::Ephemeral5m)],
        )
        .expect("cache score for cacheable request");

        assert_eq!(score.predicted_cache_read_tokens, 300_000);
        assert_eq!(score.predicted_cache_creation_tokens_1h, 0);
        assert_eq!(score.predicted_cache_creation_tokens_5m, 200_000);
        assert_eq!(score.matched_breakpoint_index, Some(1));
    }

    #[test]
    fn anthropic_family_cache_pricing_fallback_covers_opus_4_8_alias() {
        let pricing = anthropic_family_cache_pricing_summary("claude-opus-4-8")
            .expect("opus family fallback exists");

        assert_eq!(pricing.status, "known");
        assert_eq!(pricing.input_micros_per_million, Some(15_000_000));
        assert_eq!(
            pricing.cache_creation_5m_micros_per_million,
            Some(18_750_000)
        );
        assert_eq!(
            pricing.cache_creation_1h_micros_per_million,
            Some(30_000_000)
        );
        assert_eq!(pricing.cache_read_micros_per_million, Some(1_500_000));
    }

    #[test]
    fn anthropic_family_cache_pricing_fallback_leaves_non_claude_unknown() {
        assert!(anthropic_family_cache_pricing_summary("gpt-5.5").is_none());
    }

    fn breakpoint(
        block_index: u32,
        prefix_token_count: u64,
        requested_ttl: TtlClass,
    ) -> CacheBreakpoint {
        CacheBreakpoint {
            block_index,
            source: CacheBreakpointSource::Message,
            path: format!("messages.{block_index}.content"),
            message_index: Some(block_index),
            prefix_hash: format!("bp-{block_index}"),
            prefix_token_count,
            requested_ttl,
            origin: BreakpointOrigin::Explicit,
            lookback_prefixes: vec![cc_lb_plugin_api::types::CacheLookbackPrefix {
                prefix_hash: format!("bp-{block_index}"),
                content_block_index: block_index,
                prefix_token_count,
                lookback_distance: 0,
            }],
            token_estimate_source: Some("test".to_owned()),
        }
    }

    fn warm_entry(prefix_hash: &str, ttl_class: TtlClass) -> WarmCacheEntry {
        WarmCacheEntry {
            prefix_hash: prefix_hash.to_owned(),
            expires_at_unix_secs: 1_700_000_300,
            ttl_class,
            last_observed_at_unix_secs: 1_700_000_000,
        }
    }
}

fn cache_pricing_summary_for_model(model: &str) -> CachePricingSummary {
    if model.is_empty() {
        return CachePricingSummary {
            status: "unknown".to_owned(),
            input_micros_per_million: None,
            cache_creation_5m_micros_per_million: None,
            cache_creation_1h_micros_per_million: None,
            cache_read_micros_per_million: None,
        };
    }

    let normalized = cc_lb_pricing::normalize_model_id(model, None);
    let snapshot = cc_lb_pricing::global_catalog().current();
    let input = snapshot
        .models
        .get(&normalized)
        .map(|pricing| pricing.input_per_million_usd.as_micros_usd());
    let cache_creation_5m = snapshot
        .cache_creation_per_million_usd
        .get(&normalized)
        .map(|price| price.as_micros_usd());
    let cache_creation_1h = cache_creation_5m.map(cache_creation_1h_micros_from_5m);
    let cache_read = snapshot
        .cache_read_per_million_usd
        .get(&normalized)
        .map(|price| price.as_micros_usd());
    let status = if input.is_some() && cache_creation_5m.is_some() && cache_read.is_some() {
        "known"
    } else {
        if let Some(fallback) = anthropic_family_cache_pricing_summary(&normalized) {
            return fallback;
        }
        "unknown"
    };

    CachePricingSummary {
        status: status.to_owned(),
        input_micros_per_million: input,
        cache_creation_5m_micros_per_million: cache_creation_5m,
        cache_creation_1h_micros_per_million: cache_creation_1h,
        cache_read_micros_per_million: cache_read,
    }
}

fn anthropic_family_cache_pricing_summary(model: &str) -> Option<CachePricingSummary> {
    let input_micros_per_million = anthropic_family_input_micros_per_million(model)?;
    Some(CachePricingSummary {
        status: "known".to_owned(),
        input_micros_per_million: Some(input_micros_per_million),
        cache_creation_5m_micros_per_million: Some(cache_creation_5m_micros_from_input(
            input_micros_per_million,
        )),
        cache_creation_1h_micros_per_million: Some(input_micros_per_million.saturating_mul(2)),
        cache_read_micros_per_million: Some(cache_read_micros_from_input(input_micros_per_million)),
    })
}

fn anthropic_family_input_micros_per_million(model: &str) -> Option<u64> {
    if model.starts_with("claude-opus") {
        Some(15_000_000)
    } else if model.starts_with("claude-sonnet") {
        Some(3_000_000)
    } else if model.starts_with("claude-haiku") {
        Some(1_000_000)
    } else {
        None
    }
}

fn cache_creation_5m_micros_from_input(input_micros: u64) -> u64 {
    (u128::from(input_micros) * 5 / 4)
        .try_into()
        .unwrap_or(u64::MAX)
}

fn cache_read_micros_from_input(input_micros: u64) -> u64 {
    (u128::from(input_micros) / 10)
        .try_into()
        .unwrap_or(u64::MAX)
}

fn cache_creation_1h_micros_from_5m(cache_creation_5m_micros: u64) -> u64 {
    let micros = (u128::from(cache_creation_5m_micros) * 8 + 2) / 5;
    micros.try_into().unwrap_or(u64::MAX)
}

fn saturating_u64_to_u32(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

#[derive(Clone)]
pub struct PromptCacheObservationContext {
    pub(crate) upstream_id: Uuid,
    pub(crate) canonical_model_id: String,
    pub(crate) cache_breakpoints: Vec<CacheBreakpoint>,
    pub(crate) warm_entries_at_decision: Vec<WarmCacheEntry>,
    pub(crate) cache: Arc<dyn PromptCacheObservationCacheLike>,
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
    pub(crate) prefix_content_block_index: u32,
    pub(crate) estimated_prefix_tokens: u64,
    pub(crate) token_estimate_source: Option<String>,
    kind: DecodedPromptCacheObservationKind,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct PromptCacheObservationDecodeResult {
    pub(crate) observations: Vec<DecodedPromptCacheObservation>,
    pub(crate) dropped_below_threshold: u32,
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
        cache_breakpoints: cache_breakpoints.to_vec(),
        warm_entries_at_decision,
        cache,
    })
}

pub(crate) fn decode_prompt_cache_observations_pure(
    context: &PromptCacheObservationContext,
    usage: PromptCacheUsage,
    now_unix_secs: u64,
) -> PromptCacheObservationDecodeResult {
    if usage.cache_creation_input_tokens == 0 && usage.cache_read_input_tokens == 0 {
        return PromptCacheObservationDecodeResult::default();
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

    let grace_secs = context.cache.grace_margin_secs();
    let mut observations = Vec::new();
    let mut dropped_below_threshold = 0_u32;
    if let Some((breakpoint, warm_entry)) = hit {
        if breakpoint.prefix_token_count >= threshold {
            // Sliding TTL: Anthropic refreshes the prompt-cache lifetime on every
            // hit (per the prompt-caching docs). Emit a fresh `now + ttl - grace`
            // so downstream observation storage extends the window and the router
            // does not falsely conclude the upstream went cold while it was still
            // being kept warm by continuous reads.
            observations.push(DecodedPromptCacheObservation {
                prefix_hash: breakpoint.prefix_hash.clone(),
                ttl_class: warm_entry.ttl_class,
                expires_at_unix_secs: prompt_cache_observation_expires_at(
                    now_unix_secs,
                    warm_entry.ttl_class,
                    grace_secs,
                ),
                prefix_content_block_index: breakpoint.block_index,
                estimated_prefix_tokens: breakpoint.prefix_token_count,
                token_estimate_source: breakpoint.token_estimate_source.clone(),
                kind: DecodedPromptCacheObservationKind::Hit,
            });
        } else {
            dropped_below_threshold = dropped_below_threshold.saturating_add(1);
        }
    }

    if usage.cache_creation_input_tokens > 0 {
        let hit_block_index = hit.map(|(breakpoint, _)| breakpoint.block_index);
        for breakpoint in &context.cache_breakpoints {
            if hit_block_index.is_some_and(|hit_index| breakpoint.block_index <= hit_index) {
                continue;
            }
            if breakpoint.prefix_token_count < threshold {
                dropped_below_threshold = dropped_below_threshold.saturating_add(1);
                continue;
            }
            observations.push(DecodedPromptCacheObservation {
                prefix_hash: breakpoint.prefix_hash.clone(),
                ttl_class: breakpoint.requested_ttl,
                expires_at_unix_secs: prompt_cache_observation_expires_at(
                    now_unix_secs,
                    breakpoint.requested_ttl,
                    grace_secs,
                ),
                prefix_content_block_index: breakpoint.block_index,
                estimated_prefix_tokens: breakpoint.prefix_token_count,
                token_estimate_source: breakpoint.token_estimate_source.clone(),
                kind: DecodedPromptCacheObservationKind::Write,
            });
        }
    }

    PromptCacheObservationDecodeResult {
        observations,
        dropped_below_threshold,
    }
}

pub(crate) fn prompt_cache_observations_to_wire(
    observations: &[DecodedPromptCacheObservation],
) -> Vec<cc_lb_contract::PromptCacheObservationWire> {
    observations
        .iter()
        .map(|observation| cc_lb_contract::PromptCacheObservationWire {
            prefix_hash: observation.prefix_hash.clone(),
            ttl_class: observation.ttl_class,
            expires_at_unix_secs: observation.expires_at_unix_secs,
            kind: match observation.kind {
                DecodedPromptCacheObservationKind::Hit => {
                    cc_lb_contract::PromptCacheObservationKindWire::Hit
                }
                DecodedPromptCacheObservationKind::Write => {
                    cc_lb_contract::PromptCacheObservationKindWire::Write
                }
            },
            prefix_content_block_index: observation.prefix_content_block_index,
            estimated_prefix_tokens: observation.estimated_prefix_tokens,
            token_estimate_source: observation.token_estimate_source.clone(),
        })
        .collect()
}

fn emit_prompt_cache_observations_produced(
    observer: &LifecycleContext,
    context: &PromptCacheObservationContext,
    decode: &PromptCacheObservationDecodeResult,
    dropped_aborted: u32,
) {
    observer.emit_lifecycle(
        cc_lb_contract::LifecycleEvent::PromptCacheObservationsProduced {
            event_id: observer.event_id().to_owned(),
            upstream_id: context.upstream_id,
            canonical_model_id: context.canonical_model_id.clone(),
            observations: if dropped_aborted == 0 {
                prompt_cache_observations_to_wire(&decode.observations)
            } else {
                Vec::new()
            },
            dropped_below_threshold: if dropped_aborted == 0 {
                decode.dropped_below_threshold
            } else {
                0
            },
            dropped_aborted,
        },
    );
}

fn prompt_cache_observation_expires_at(
    now_unix_secs: u64,
    ttl_class: TtlClass,
    grace_secs: u64,
) -> u64 {
    now_unix_secs
        .saturating_add(prompt_cache_ttl_secs(ttl_class))
        .saturating_sub(grace_secs)
}

fn prompt_cache_ttl_secs(ttl_class: TtlClass) -> u64 {
    match ttl_class {
        TtlClass::Ephemeral5m => 5 * 60,
        TtlClass::Ephemeral1h => 60 * 60,
    }
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
    ) -> Option<AuthLimitSubject>;
}

pub trait LimitCostEstimator: Send + Sync {
    fn estimate_max(
        &self,
        model: &str,
        max_input: u64,
        max_output: u64,
        upstream_kind: Option<&str>,
    ) -> Option<i64>;
}

#[derive(Clone, Debug)]
pub struct AuthLimitSubject {
    pub principal_id: String,
    pub key_id: String,
    pub record: StoredApiKeyRecord,
}

struct LimitRejectionInfo {
    subject: cc_lb_contract::LimitSubject,
    request_summary: cc_lb_contract::LimitRequestSummary,
    route_summary: cc_lb_contract::RouteSummary,
    limit_violation: Option<String>,
    reason_label: String,
}

struct LimitRejectionErr {
    response: Response<Body>,
    info: LimitRejectionInfo,
}

struct StaticLimitSubjectProvider {
    subject: AuthLimitSubject,
}

#[async_trait]
impl LimitSubjectProvider for StaticLimitSubjectProvider {
    async fn limit_subject(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        _authn_success: &AuthnSuccess,
    ) -> Option<AuthLimitSubject> {
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
    ) -> Option<AuthLimitSubject> {
        let mut record = authn_success.record.clone();
        record.key_hash_b64 = authn_success.key_id.clone();
        Some(AuthLimitSubject {
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
    dispatcher: Arc<dyn UpstreamDispatch>,
    config: LifecycleConfig,
    limit_engine: Option<Arc<LimitEngine>>,
    limit_subject_provider: Option<Arc<dyn LimitSubjectProvider>>,
    limit_cost_estimator: Option<Arc<dyn LimitCostEstimator>>,
    event_bus: Option<Arc<dyn RequestEventBus>>,
    subscription_quota_sink: Option<SubscriptionQuotaSink>,
    subscription_metadata_hook: Option<MetadataHookHandle>,
    subscription_quota_cache: Option<Arc<dyn SubscriptionQuotaCacheLike>>,
    clock: ClockHandle,
    rng: Mutex<StdRng>,
}

pub struct LifecycleStaticView {
    pub principal_view: Arc<PrincipalView>,
    pub signer_factory: Arc<dyn ApiKeyAwareSignerFactory>,
    pub global_router: Arc<dyn RouterPlugin>,
    pub dispatcher: Arc<dyn UpstreamDispatch>,
    pub global_observability_hooks: Vec<Arc<dyn ObservabilityHook>>,
}

impl Lifecycle {
    pub fn new(
        authn: Arc<BuiltinAuthn>,
        static_view: LifecycleStaticView,
        config: LifecycleConfig,
        clock: ClockHandle,
    ) -> Self {
        let dispatcher = static_view.dispatcher;
        let dynamic_view = DynamicViewBuilder::new(0)
            .signer_factory(static_view.signer_factory)
            .global_router(static_view.global_router)
            .global_observability_hooks(static_view.global_observability_hooks)
            .principal_view(static_view.principal_view)
            .upstream_status_snapshot(Arc::new(UpstreamStatusSnapshot::default()))
            .build();
        Self {
            authn,
            dynamic_view: Arc::new(DynamicViewHolder::new(dynamic_view)),
            dispatcher,
            config,
            limit_engine: None,
            limit_subject_provider: None,
            limit_cost_estimator: None,
            event_bus: None,
            subscription_quota_sink: None,
            subscription_metadata_hook: None,
            subscription_quota_cache: None,
            clock,
            rng: Mutex::new(rand::make_rng()),
        }
    }

    pub fn new_with_dynamic_view(
        authn: Arc<BuiltinAuthn>,
        dynamic_view: Arc<DynamicViewHolder>,
        dispatcher: Arc<dyn UpstreamDispatch>,
        config: LifecycleConfig,
        clock: ClockHandle,
    ) -> Self {
        Self {
            authn,
            dynamic_view,
            dispatcher,
            config,
            limit_engine: None,
            limit_subject_provider: None,
            limit_cost_estimator: None,
            event_bus: None,
            subscription_quota_sink: None,
            subscription_metadata_hook: None,
            subscription_quota_cache: None,
            clock,
            rng: Mutex::new(rand::make_rng()),
        }
    }

    pub fn with_terminal_rng_seed(mut self, seed: [u8; 32]) -> Self {
        self.rng = Mutex::new(StdRng::from_seed(seed));
        self
    }

    pub fn with_error_normalizer(self, _error_normalizer: Arc<ErrorNormalizer>) -> Self {
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

    pub fn preview_route(
        &self,
        input: PreviewRouteInput,
    ) -> Result<PreviewRouteOutcome, PreviewRouteError> {
        let view = self.dynamic_view.load();
        let principal_spec = view
            .principal_view
            .get(&input.principal_id)
            .ok_or_else(|| PreviewRouteError::PrincipalNotFound(input.principal_id.clone()))?;
        let resolved_pipeline = principal_spec.resolved_pipeline(None);
        if let Some(err) = &resolved_pipeline.instantiation_error {
            return Err(PreviewRouteError::PipelineInstantiationError(
                err.as_ref().to_owned(),
            ));
        }

        let body_value = sonic_rs::from_slice::<Value>(&input.body_bytes).ok();
        let cache_metadata = request_cache_metadata_from_value(&input.headers, body_value.as_ref());
        let (cache_breakpoints, canonical_model_id) =
            if view.prompt_cache_observation_cache_opt().is_some()
                && self.config.prompt_cache_shadow.enabled
            {
                (
                    cache_metadata.plugin_cache_breakpoints(),
                    cache_metadata.canonical_model_id.clone(),
                )
            } else {
                (Vec::new(), String::new())
            };

        let request_id = input
            .request_id
            .clone()
            .unwrap_or_else(|| format!("preview-{}", Uuid::new_v4()));
        let ctx = RequestContext {
            request_id,
            thread_id: cache_metadata.thread_id.clone(),
            downstream_headers: input.headers,
            method: http::Method::POST,
            path: "/v1/messages".to_owned(),
            query: None,
            body_bytes: input.body_bytes,
            cache_breakpoints,
            canonical_model_id,
            cache_pricing: cache_pricing_summary_for_model(&cache_metadata.canonical_model_id),
        };
        let principal = Principal {
            id: input.principal_id,
            kind: PrincipalKind::ApiKey,
            claims: serde_json::Map::new(),
        };

        let candidates = build_candidates(
            &view,
            &principal.id,
            RequestKind::AnthropicMessages,
            &ctx.canonical_model_id,
            &ctx.cache_breakpoints,
            ctx.thread_id.as_deref(),
            &*self.clock,
        );
        let pipeline_result = execute_filter_pipeline(
            &resolved_pipeline.user_filters,
            &ctx,
            &principal,
            candidates,
            None,
        );
        let terminal_decision = self.select_terminal_upstream(
            resolved_pipeline.terminal.clone(),
            &pipeline_result.candidates,
        );
        let winner_upstream_id = terminal_decision.upstream_id;
        let winner_upstream_name = winner_upstream_id.and_then(|id| {
            pipeline_result
                .candidates
                .iter()
                .find(|candidate| candidate.upstream_id == id)
                .map(|candidate| candidate.name.clone())
                .or_else(|| {
                    view.upstreams_snapshot()
                        .iter()
                        .find(|record| record.id == id)
                        .map(|record| record.name.clone())
                })
        });
        let trace = pipeline_result.routing_trace(terminal_decision);

        Ok(PreviewRouteOutcome {
            trace,
            winner_upstream_id,
            winner_upstream_name,
        })
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
                subject: AuthLimitSubject {
                    principal_id,
                    key_id,
                    record,
                },
            }),
        )
    }

    #[allow(clippy::explicit_auto_deref)]
    pub async fn handle(&self, req: Request<Bytes>) -> Result<Response<Body>, ProxyError> {
        let view = self.dynamic_view.load();
        let principal_view = Arc::clone(&view.principal_view);
        let started = Instant::now();
        let observer_from_ext = req.extensions().get::<LifecycleContext>().cloned();
        let (mut ctx, body_too_large) = self.parse(req);
        let body_view = RequestBodyView::new(&ctx.body_bytes);
        let observer: Option<LifecycleContext> = observer_from_ext.or_else(|| {
            self.event_bus
                .as_ref()
                .map(|bus| LifecycleContext::new(ctx.request_id.clone(), bus.clone(), &self.clock))
        });
        if let Some(o) = observer.as_ref() {
            o.emit_request_started(body_view.stream());
        }
        if let Some(response) = body_too_large {
            if let Some(o) = observer.as_ref() {
                let cap = body_cap_for_path(&self.config, &ctx.path);
                o.emit_lifecycle(cc_lb_contract::LifecycleEvent::ParseCompleted {
                    event_id: o.event_id().to_owned(),
                    result: Err(cc_lb_contract::ParseFailure::BodyTooLarge {
                        limit_bytes: cap as u64,
                    }),
                });
                o.set_terminal(StatusCode::PAYLOAD_TOO_LARGE, error_codes::BODY_TOO_LARGE);
                o.finish();
            }
            return Ok(*response);
        }
        if ctx.path == "/v1/messages" && !ctx.body_bytes.is_empty() && !body_view.is_valid_json() {
            if let Some(o) = observer.as_ref() {
                o.emit_lifecycle(cc_lb_contract::LifecycleEvent::ParseCompleted {
                    event_id: o.event_id().to_owned(),
                    result: Err(cc_lb_contract::ParseFailure::InvalidJson),
                });
                o.set_terminal(StatusCode::BAD_REQUEST, error_codes::INVALID_JSON);
                o.finish();
            }
            return Ok(anthropic_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "request body must be valid JSON",
            ));
        }
        let cache_metadata =
            request_cache_metadata_from_value(&ctx.downstream_headers, body_view.value());
        if let Some(o) = observer.as_ref() {
            o.emit_lifecycle(cc_lb_contract::LifecycleEvent::ParseCompleted {
                event_id: o.event_id().to_owned(),
                result: Ok(cc_lb_contract::ParseInfo {
                    path: ctx.path.clone(),
                    method: ctx.method.to_string(),
                    model: body_view.model(),
                    stream: body_view.stream(),
                    body_bytes: ctx.body_bytes.len() as u64,
                    cache_control_block_count: cache_metadata.cache_control_block_count,
                    cache_breakpoints: cache_metadata.cache_breakpoints.clone(),
                    cache_prefix_hash: cache_metadata.cache_prefix_hash.clone(),
                    matched_v3_cache_key: cache_metadata.cache_prefix_hash.clone(),
                    thread_id: cache_metadata.thread_id.clone(),
                    message_id: cache_metadata.message_id.clone(),
                    message_index: cache_metadata.message_index,
                    message_count: cache_metadata.message_count,
                    cache_control_message_indices: cache_metadata
                        .cache_control_message_indices
                        .clone(),
                }),
            });
        }
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
        ctx.cache_pricing = cache_pricing_summary_for_model(&ctx.canonical_model_id);
        ctx.thread_id = cache_metadata.thread_id.clone();

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
                    if let Some(o) = observer.as_ref() {
                        o.emit_provider_error("authentication_error", &source.to_string(), "authn");
                    }
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
                    if let Some(o) = observer.as_ref() {
                        o.emit_lifecycle(cc_lb_contract::LifecycleEvent::AuthCompleted {
                            event_id: o.event_id().to_owned(),
                            result: Err(cc_lb_contract::AuthFailure::AuthenticationFailed {
                                http_status: status.as_u16(),
                                reason: Some(key_auth_failure_reason(&source).to_owned()),
                            }),
                        });
                        o.set_terminal(status, error_codes::AUTHENTICATION_FAILED);
                        o.finish();
                    }
                    return Ok(response);
                }
            }
        };
        let auth_ms = duration_to_ms(auth_start.elapsed());
        let principal_id = success.principal_id.clone();
        if let Some(o) = observer.as_ref() {
            o.emit_lifecycle(cc_lb_contract::LifecycleEvent::AuthCompleted {
                event_id: o.event_id().to_owned(),
                result: Ok(cc_lb_contract::AuthInfo {
                    principal_id: principal_id.clone(),
                    key_id: Some(success.key_id.clone()),
                    principal_kind: Some(
                        principal_kind_lite_as_str(&success.record.principal_kind).to_owned(),
                    ),
                    auth_ms: Some(auth_ms),
                }),
            });
        }
        let Some(cached) = principal_view.get(&principal_id) else {
            tracing::error!(%principal_id, "authenticated principal missing from principal view");
            if let Some(o) = observer.as_ref() {
                o.emit_provider_error(
                    "principal_missing",
                    "authenticated principal is unavailable",
                    "authn",
                );
            }
            let response = anthropic_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "api_error",
                "authenticated principal is unavailable",
            );
            if let Some(o) = observer.as_ref() {
                o.emit_lifecycle(cc_lb_contract::LifecycleEvent::AuthCompleted {
                    event_id: o.event_id().to_owned(),
                    result: Err(cc_lb_contract::AuthFailure::PrincipalMissing {
                        principal_id: principal_id.clone(),
                    }),
                });
                o.set_terminal(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    error_codes::PRINCIPAL_MISSING,
                );
                o.finish();
            }
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
            if let Some(o) = observer.as_ref() {
                o.emit_provider_error("router_pipeline_unavailable", error, "router");
            }
            let response = anthropic_error_response(
                StatusCode::BAD_GATEWAY,
                "route_not_configured",
                "router pipeline is unavailable for this request",
            );
            if let Some(o) = observer.as_ref() {
                o.emit_lifecycle(cc_lb_contract::LifecycleEvent::RouteCompleted {
                    event_id: o.event_id().to_owned(),
                    result: Err(cc_lb_contract::RouteFailure::RouterPipelineUnavailable),
                    routing_trace: None,
                });
                o.set_terminal(
                    StatusCode::BAD_GATEWAY,
                    error_codes::ROUTER_PIPELINE_UNAVAILABLE,
                );
                o.finish();
            }
            return Ok(response);
        }
        if let Some(o) = observer.as_ref() {
            o.emit_authentication_completed(principal.id.clone(), success.record.principal_kind);
        }

        let route_start = Instant::now();
        let candidates = build_candidates(
            &view,
            &principal.id,
            RequestKind::AnthropicMessages,
            &ctx.canonical_model_id,
            &ctx.cache_breakpoints,
            ctx.thread_id.as_deref(),
            &*self.clock,
        );
        let pipeline_result = execute_filter_pipeline(
            &router_pipeline.user_filters,
            &ctx,
            &principal,
            candidates,
            observer.as_ref(),
        );
        let terminal_decision = self.select_terminal_upstream(
            router_pipeline.terminal.clone(),
            &pipeline_result.candidates,
        );
        if pipeline_result.candidates.is_empty() {
            let message = "no upstream candidates remain after routing filters";
            if let Some(o) = observer.as_ref() {
                o.emit_provider_error("route_no_upstream_after_filter", message, "router");
            }
            let internal_errors = redact_internal_errors(&[InternalError {
                stage: InternalErrorStage::RouterFilter,
                kind: InternalErrorKind::Unavailable,
                message: Some(message.to_owned()),
            }]);
            self.emit_routing_failure_event(
                observer.as_ref(),
                StatusCode::SERVICE_UNAVAILABLE,
                Some(pipeline_result.routing_trace(terminal_decision.clone())),
                internal_errors,
            );
            let response = anthropic_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "route_no_upstream_after_filter",
                message,
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
            if let Some(o) = observer.as_ref() {
                o.emit_provider_error(
                    "route_not_configured",
                    "router selected an upstream missing from the dynamic view",
                    "router",
                );
            }
            let response = anthropic_error_response(
                StatusCode::BAD_GATEWAY,
                "route_not_configured",
                "no upstream route is configured for this request",
            );
            if let Some(o) = observer.as_ref() {
                o.emit_lifecycle(cc_lb_contract::LifecycleEvent::RouteCompleted {
                    event_id: o.event_id().to_owned(),
                    result: Err(cc_lb_contract::RouteFailure::RouteNotConfigured),
                    routing_trace: None,
                });
                o.set_terminal(StatusCode::BAD_GATEWAY, error_codes::ROUTE_NOT_CONFIGURED);
                o.finish();
            }
            return Ok(response);
        };
        let router_chosen_upstream_name = resolved_record.name.clone();
        let raw_passthrough_base_url = resolved_record.base_url.clone();
        let route_dialect: Arc<dyn UpstreamDialect> = Arc::new(RawPassthroughDialect {
            base_url: raw_passthrough_base_url
                .clone()
                .unwrap_or_else(default_anthropic_base_url),
        });
        let route_upstream = match upstream_for_record(resolved_record) {
            Ok(upstream) => upstream,
            Err(reason) => {
                if let Some(o) = observer.as_ref() {
                    o.emit_provider_error("route_not_configured", &reason, "router");
                }
                let response = anthropic_error_response(
                    StatusCode::BAD_GATEWAY,
                    "route_not_configured",
                    "no upstream route is configured for this request",
                );
                if let Some(o) = observer.as_ref() {
                    o.emit_lifecycle(cc_lb_contract::LifecycleEvent::RouteCompleted {
                        event_id: o.event_id().to_owned(),
                        result: Err(cc_lb_contract::RouteFailure::RouteNotConfigured),
                        routing_trace: None,
                    });
                    o.set_terminal(StatusCode::BAD_GATEWAY, error_codes::ROUTE_NOT_CONFIGURED);
                    o.finish();
                }
                return Ok(response);
            }
        };
        let dialect = cached.resolved_dialect(&route_dialect).clone();
        let route = cc_lb_plugin_api::RouteDecision {
            upstream_id: Some(resolved_upstream_id),
            upstream: route_upstream,
            dialect,
        };
        let route_ms = duration_to_ms(route_start.elapsed());
        let routing_trace_value = pipeline_result.routing_trace(terminal_decision.clone());
        let selected_cache_score = pipeline_result
            .candidates
            .iter()
            .find(|candidate| candidate.upstream_id == resolved_upstream_id)
            .and_then(|candidate| candidate.cache_score.as_ref())
            .cloned();
        let predicted_cache_read_tokens = selected_cache_score
            .as_ref()
            .map_or(0, |score| score.predicted_cache_read_tokens);
        let subscription_trace = subscription_preference_trace(&routing_trace_value);
        let lineage_counterfactual = lineage_counterfactual_from_thread_usage(
            view.prompt_cache_observation_cache_opt(),
            &pipeline_result.candidates,
            &ctx.canonical_model_id,
            ctx.thread_id.as_deref(),
        );
        if let Some(o) = observer.as_ref() {
            o.emit_lifecycle(cc_lb_contract::LifecycleEvent::RouteCompleted {
                event_id: o.event_id().to_owned(),
                result: Ok(cc_lb_contract::RouteInfo {
                    upstream_id: resolved_upstream_id,
                    upstream_name: router_chosen_upstream_name.clone(),
                    model: body_view.model(),
                    upstream_kind: pricing_upstream_kind_label(&route.upstream).map(str::to_owned),
                    route_ms: Some(route_ms),
                    routing_trace: Some(routing_trace_value.clone()),
                    predicted_cache_read_tokens: Some(predicted_cache_read_tokens),
                    matched_v3_cache_key: selected_cache_score
                        .as_ref()
                        .and_then(|score| score.matched_v3_cache_key.clone()),
                    breakpoint_content_block_index: selected_cache_score
                        .as_ref()
                        .and_then(|score| score.breakpoint_content_block_index),
                    matched_content_block_index: selected_cache_score
                        .as_ref()
                        .and_then(|score| score.matched_content_block_index),
                    lookback_distance: selected_cache_score
                        .as_ref()
                        .and_then(|score| score.lookback_distance),
                    predicted_cache_creation_tokens_5m: selected_cache_score
                        .as_ref()
                        .map(|score| score.predicted_cache_creation_tokens_5m),
                    predicted_cache_creation_tokens_1h: selected_cache_score
                        .as_ref()
                        .map(|score| score.predicted_cache_creation_tokens_1h),
                    token_estimate_source: selected_cache_score
                        .as_ref()
                        .and_then(|score| score.token_estimate_source.clone()),
                    cache_value_micros: subscription_trace.and_then(|trace| {
                        trace
                            .candidates
                            .iter()
                            .find(|candidate| candidate.upstream_id == resolved_upstream_id)
                            .and_then(|candidate| candidate.cache_value_micros)
                    }),
                    formula_winner_upstream_id: subscription_trace
                        .and_then(|trace| trace.formula_winner_upstream_id),
                    kept_upstream_id: subscription_trace.and_then(|trace| trace.kept_upstream_id),
                    wrh_key_source: subscription_trace.map(|trace| match trace.wrh_key_source {
                        cc_lb_plugin_api::types::WrhKeySource::CacheHash => "cache_hash".to_owned(),
                        cc_lb_plugin_api::types::WrhKeySource::RequestId => "request_id".to_owned(),
                    }),
                    lineage_would_have_predicted_read_tokens: subscription_trace
                        .and_then(|trace| trace.lineage_would_have_predicted_read_tokens)
                        .or(lineage_counterfactual.map(|counterfactual| counterfactual.0)),
                    lineage_would_have_picked_upstream_id: subscription_trace
                        .and_then(|trace| trace.lineage_would_have_picked_upstream_id)
                        .or(lineage_counterfactual.map(|counterfactual| counterfactual.1)),
                }),
                routing_trace: Some(routing_trace_value.clone()),
            });
        }
        let prompt_cache_observation_context = prompt_cache_observation_context(
            &view,
            resolved_upstream_id,
            &ctx.canonical_model_id,
            &ctx.cache_breakpoints,
        );

        let limit_reserve_start = Instant::now();
        let mut active_limit = match self
            .reserve_limit(
                &principal_view,
                &ctx,
                &principal,
                &route,
                &success,
                &body_view,
            )
            .await
        {
            Ok(active_limit) => active_limit,
            Err(LimitRejectionErr { response, info }) => {
                let status = response.status();
                if let Some(o) = observer.as_ref() {
                    let proxy_setup_ms = duration_to_ms(started.elapsed());
                    o.emit_lifecycle(cc_lb_contract::LifecycleEvent::LimitDecision {
                        event_id: o.event_id().to_owned(),
                        decision: cc_lb_contract::LimitDecisionKind::Rejected {
                            reason: info.reason_label,
                            subject: Some(info.subject),
                            request_summary: Some(info.request_summary),
                            route_summary: Some(info.route_summary),
                            limit_violation: info.limit_violation,
                        },
                    });
                    o.set_termination_timings(None, None, Some(proxy_setup_ms), None, None);
                    o.set_terminal(status, error_codes::LIMIT_REJECTED);
                    o.finish();
                }
                return Ok(response);
            }
        };
        let limit_reserve_ms = duration_to_ms(limit_reserve_start.elapsed());
        if let Some(o) = observer.as_ref() {
            let decision = if let Some(limit) = active_limit.as_ref() {
                cc_lb_contract::LimitDecisionKind::Reserved {
                    reservation_id: limit
                        .reservation
                        .as_ref()
                        .map(|r| r.id().to_owned())
                        .unwrap_or_default(),
                    amount: limit.request.max_tokens as u64,
                    limit_reserve_ms: Some(limit_reserve_ms),
                }
            } else {
                cc_lb_contract::LimitDecisionKind::Reserved {
                    reservation_id: String::new(),
                    amount: 0,
                    limit_reserve_ms: Some(limit_reserve_ms),
                }
            };
            o.emit_lifecycle(cc_lb_contract::LifecycleEvent::LimitDecision {
                event_id: o.event_id().to_owned(),
                decision,
            });
        }

        let signer_factory = view.signer_factory.with_router_choice(
            success.api_key.clone().unwrap_or_default(),
            router_chosen_upstream_name.clone(),
        );
        let signer = match signer_factory.build(&route.upstream).await {
            Ok(signer) => signer,
            Err(source) => {
                if let Some(o) = observer.as_ref() {
                    o.emit_provider_error("signing_error", &source.to_string(), "signer_factory");
                }
                let mut response = anthropic_error_response(
                    StatusCode::BAD_GATEWAY,
                    "api_error",
                    "failed to prepare upstream credentials",
                );
                self.attach_limit_headers(&mut response, active_limit.as_ref());
                if let Some(o) = observer.as_ref() {
                    o.set_terminal(StatusCode::BAD_GATEWAY, error_codes::SIGNER_FAILED);
                    o.finish();
                }
                return Ok(response);
            }
        };

        let dispatch_started = Instant::now();
        let proxy_setup_ms = duration_to_ms(dispatch_started.saturating_duration_since(started));
        let mut attempt_timings = AttemptTimings::default();
        let mut internal_errors = pipeline_result.internal_errors.clone();
        if let Some(o) = observer.as_ref() {
            o.emit_lifecycle(cc_lb_contract::LifecycleEvent::UpstreamAttempt {
                event_id: o.event_id().to_owned(),
                attempt_num: 1,
                upstream_id: resolved_upstream_id,
            });
        }
        let mut response = match self
            .attempt(
                self.dispatcher.as_ref(),
                &ctx,
                &principal,
                &route,
                raw_passthrough_base_url.as_ref(),
                signer.clone(),
                observer.as_ref(),
                &mut attempt_timings,
                &mut internal_errors,
            )
            .await
        {
            Ok(response) => {
                let status = response.status();
                if let Some(o) = observer.as_ref() {
                    o.emit_lifecycle(cc_lb_contract::LifecycleEvent::UpstreamResponseStarted {
                        event_id: o.event_id().to_owned(),
                        status: status.as_u16(),
                        headers: header_snapshot_from(response.headers()),
                        bulkhead_wait_ms: attempt_timings.bulkhead_wait_ms,
                        dns_ms: attempt_timings.dns_ms,
                        connect_ms: attempt_timings.connect_ms,
                        connection_reused: attempt_timings.connection_reused,
                        shape_ms: attempt_timings.shape_ms,
                        sign_ms: attempt_timings.sign_ms,
                        upstream_ttfb_ms: attempt_timings.upstream_ttfb_ms,
                    });
                }
                response
            }
            Err(response) => {
                let mut response = *response;
                self.attach_limit_headers(&mut response, active_limit.as_ref());
                let status = response.status();
                if let Some(o) = observer.as_ref() {
                    o.set_terminal(status, error_codes::UPSTREAM_DISPATCH_FAILED);
                    o.finish();
                }
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
                if let Some(o) = observer.as_ref() {
                    o.emit_lifecycle(cc_lb_contract::LifecycleEvent::UpstreamAttempt {
                        event_id: o.event_id().to_owned(),
                        attempt_num: 2,
                        upstream_id: resolved_upstream_id,
                    });
                }
                response = match self
                    .attempt(
                        self.dispatcher.as_ref(),
                        &ctx,
                        &principal,
                        &route,
                        raw_passthrough_base_url.as_ref(),
                        new_signer,
                        observer.as_ref(),
                        &mut attempt_timings,
                        &mut internal_errors,
                    )
                    .await
                {
                    Ok(response) => {
                        if let Some(o) = observer.as_ref() {
                            o.emit_lifecycle(
                                cc_lb_contract::LifecycleEvent::UpstreamResponseStarted {
                                    event_id: o.event_id().to_owned(),
                                    status: response.status().as_u16(),
                                    headers: header_snapshot_from(response.headers()),
                                    bulkhead_wait_ms: attempt_timings.bulkhead_wait_ms,
                                    dns_ms: attempt_timings.dns_ms,
                                    connect_ms: attempt_timings.connect_ms,
                                    connection_reused: attempt_timings.connection_reused,
                                    shape_ms: attempt_timings.shape_ms,
                                    sign_ms: attempt_timings.sign_ms,
                                    upstream_ttfb_ms: attempt_timings.upstream_ttfb_ms,
                                },
                            );
                        }
                        response
                    }
                    Err(response) => {
                        let mut response = *response;
                        self.attach_limit_headers(&mut response, active_limit.as_ref());
                        let status = response.status();
                        if let Some(o) = observer.as_ref() {
                            o.set_terminal(status, error_codes::UPSTREAM_DISPATCH_FAILED);
                            o.finish();
                        }
                        return Ok(response);
                    }
                };
            } else {
                let mut response = response_from_collected(unauthorized);
                self.attach_limit_headers(&mut response, active_limit.as_ref());
                let status = response.status();
                if let Some(o) = observer.as_ref() {
                    let code = if status.is_client_error() {
                        error_codes::UPSTREAM_4XX
                    } else {
                        error_codes::UPSTREAM_5XX
                    };
                    o.emit_provider_error(code, status.as_str(), "upstream");
                    o.set_terminal(status, code);
                    o.finish();
                }
                return Ok(response);
            }
        }

        let status = response.status();
        response = self
            .finish_success_response(
                response,
                active_limit.take(),
                started.elapsed(),
                status,
                stream_hooks,
                RequestEventContext {
                    request_id: ctx.request_id.clone(),
                    thread_id: ctx.thread_id.clone(),
                    canonical_model_id: ctx.canonical_model_id.clone(),
                    proxy_setup_ms: Some(proxy_setup_ms),
                    stage_timings: attempt_timings,
                    internal_errors,
                },
                ResponseTransformContext {
                    principal: principal.clone(),
                    upstream: route.upstream.clone(),
                    request_method: ctx.method.clone(),
                    request_path: ctx.path.clone(),
                    dialect: route.dialect.clone(),
                },
                prompt_cache_observation_context,
                observer.clone(),
            )
            .await;
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
        body_view: &RequestBodyView,
    ) -> Result<Option<ActiveLimit>, LimitRejectionErr> {
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
        let limit_request = body_view.limit_request();
        let upstream_kind = pricing_upstream_kind_label(&route.upstream);
        let max_input_estimate = DEFAULT_MAX_INPUT_ESTIMATE;
        let cost_estimate = self.limit_cost_estimator.as_ref().and_then(|estimator| {
            estimator.estimate_max(
                &limit_request.model,
                max_input_estimate as u64,
                limit_request.max_tokens.max(0) as u64,
                upstream_kind,
            )
        });

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
                reservation: Some(reservation),
            })),
            Err(reason) => {
                let limit_violation = limit_violation_name(&reason).map(|v| v.to_owned());
                let retry_after_seconds = limit_retry_after_secs(reason.clone());
                let reason_label = "limit_rejected".to_owned();
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
                let info = LimitRejectionInfo {
                    subject: cc_lb_contract::LimitSubject {
                        principal_id: subject.principal_id.clone(),
                        key_id: subject.key_id.clone(),
                    },
                    request_summary: cc_lb_contract::LimitRequestSummary {
                        model: limit_request.model.clone(),
                        path: ctx.path.clone(),
                        method: ctx.method.as_str().to_owned(),
                    },
                    route_summary: cc_lb_contract::RouteSummary {
                        upstream_name: audit_upstream_name(&route.upstream).to_owned(),
                    },
                    limit_violation,
                    reason_label,
                };
                Err(LimitRejectionErr { response, info })
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn finish_success_response(
        &self,
        response: Response<Body>,
        active_limit: Option<ActiveLimit>,
        duration: Duration,
        status: StatusCode,
        stream_hooks: StreamHooks,
        event_ctx: RequestEventContext,
        transform_ctx: ResponseTransformContext,
        prompt_cache_observation_context: Option<PromptCacheObservationContext>,
        observer: Option<LifecycleContext>,
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
                stream_hooks,
                active_limit.take(),
                event_ctx.clone(),
                transform_ctx,
                prompt_cache_observation_context,
                observer,
            );
            self.attach_limit_headers(&mut response, None);
            return response;
        }

        let (mut parts, mut body) = response.into_parts();
        strip_hop_by_hop(&mut parts.headers);
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
        let semantic_body = match decode_full_body(&parts.headers, &body) {
            Ok(Some(plaintext)) => Some(Bytes::copy_from_slice(&plaintext)),
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
        };
        let usage = semantic_body
            .as_ref()
            .map_or_else(UsageCounts::default, |body| usage_from_json_body(body));
        let mut downstream_body = body;
        let mut buffered_transform_error: Option<ResponseTransformError> = None;
        if let (Some(hook), Some(semantic_body)) = (
            transform_ctx.dialect.response_transform_hook(),
            semantic_body,
        ) {
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
        if let Some(o) = observer.as_ref()
            && usage.present
            && status == StatusCode::OK
            && self.config.prompt_cache_shadow.enabled
            && let Some(context) = prompt_cache_observation_context.as_ref()
        {
            let now_unix_secs = context.cache.clock_now_unix_secs();
            record_thread_usage_from_response(&event_ctx, context, &usage, now_unix_secs);
            let decode = decode_prompt_cache_observations_pure(
                context,
                PromptCacheUsage::from(&usage),
                now_unix_secs,
            );
            emit_prompt_cache_observations_produced(o, context, &decode, 0);
        }
        if let (Some(limit_engine), Some(active_limit)) =
            (self.limit_engine.as_ref(), active_limit.as_mut())
            && active_limit.reservation.is_some()
        {
            active_limit.hand_off_reservation_to_reconcile_subscriber();
            attach_limit_headers_from_engine(
                &mut parts.headers,
                limit_engine.as_ref(),
                &active_limit.subject.key_id,
                &active_limit.subject.principal_id,
            );
        }

        if let Some(o) = observer.as_ref() {
            o.emit_lifecycle(cc_lb_contract::LifecycleEvent::UsageObserved {
                event_id: o.event_id().to_owned(),
                usage: to_usage_snapshot(&usage),
                source: cc_lb_contract::UsageSource::NonStreamBody,
            });
            let stream_result = if let Some(error) = buffered_transform_error.as_ref() {
                Err(cc_lb_contract::StreamError {
                    error_type: "response_transform_error".to_owned(),
                    error_message: error.to_string(),
                })
            } else {
                Ok(cc_lb_contract::StreamSuccess {
                    usage: to_usage_snapshot(&usage),
                    sse_event_count: 0,
                    body_bytes: Some(downstream_body.len() as u64),
                    body_chunk_count: Some(body_chunk_count),
                    first_body_chunk_ms,
                    ..Default::default()
                })
            };
            o.emit_lifecycle(cc_lb_contract::LifecycleEvent::StreamCompleted {
                event_id: o.event_id().to_owned(),
                result: stream_result,
            });
            o.set_termination_timings(
                None,
                None,
                event_ctx.proxy_setup_ms,
                Some(body_collect_ms),
                first_body_chunk_ms,
            );
            o.set_internal_errors(event_ctx.internal_errors.clone());
            if status.is_client_error() || status.is_server_error() {
                let code = if status.is_client_error() {
                    error_codes::UPSTREAM_4XX
                } else {
                    error_codes::UPSTREAM_5XX
                };
                o.emit_provider_error(code, status.as_str(), "upstream");
                o.set_terminal(status, code);
            } else if buffered_transform_error.is_some() {
                o.set_terminal(status, error_codes::UPSTREAM_STREAM_ERROR);
            } else {
                o.set_success_status(status);
            }
            o.finish();
        }
        observe_many(
            stream_hooks.as_slice(),
            ObserveEvent::RequestFinished {
                status,
                input_tokens: usage.present.then_some(usage.input_tokens),
                output_tokens: usage.present.then_some(usage.output_tokens),
                cache_creation_input_tokens: usage
                    .present
                    .then_some(usage.cache_creation_input_tokens),
                cache_read_input_tokens: usage.present.then_some(usage.cache_read_input_tokens),
                duration_ms: duration_to_ms(duration),
            },
        );
        Response::from_parts(parts, Body::from(downstream_body))
    }

    fn emit_routing_failure_event(
        &self,
        observer: Option<&LifecycleContext>,
        status: StatusCode,
        routing_trace: Option<RoutingTrace>,
        internal_errors: Vec<InternalError>,
    ) {
        if let Some(o) = observer {
            o.set_internal_errors(internal_errors);
            o.emit_lifecycle(cc_lb_contract::LifecycleEvent::RouteCompleted {
                event_id: o.event_id().to_owned(),
                result: Err(cc_lb_contract::RouteFailure::RouteNoUpstreamAfterFilter),
                routing_trace,
            });
            o.set_terminal(status, error_codes::ROUTE_NO_UPSTREAM_AFTER_FILTER);
            o.finish();
        }
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

    fn parse(&self, req: Request<Bytes>) -> (RequestContext, Option<Box<Response<Body>>>) {
        let (mut parts, body) = req.into_parts();
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

        let ctx = RequestContext {
            request_id,
            thread_id: None,
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
            cache_pricing: CachePricingSummary::default(),
        };
        (ctx, body_too_large)
    }

    #[allow(clippy::too_many_arguments)]
    async fn attempt(
        &self,
        dispatcher: &dyn UpstreamDispatch,
        ctx: &RequestContext,
        principal: &Principal,
        route: &cc_lb_plugin_api::RouteDecision,
        raw_passthrough_base_url: Option<&Url>,
        signer: Arc<dyn cc_lb_plugin_api::Signer>,
        observer: Option<&LifecycleContext>,
        timings: &mut AttemptTimings,
        internal_errors: &mut Vec<InternalError>,
    ) -> Result<Response<Body>, Box<Response<Body>>> {
        let shape_start = Instant::now();
        let shaped = match shape_request(route.dialect.as_ref(), ctx, &route.upstream, principal) {
            Ok(shaped) => shaped,
            Err(source) => {
                let message = source.to_string();
                tracing::warn!(%source, "shape_request failed; falling back to raw passthrough");
                if let Some(o) = observer {
                    o.emit_provider_error("shape_error", &message, "dialect");
                }
                push_shape_internal_error(internal_errors, &message);
                raw_passthrough_request(raw_passthrough_base_url, ctx, principal)?
            }
        };
        timings.shape_ms = Some(duration_to_ms(shape_start.elapsed()));

        let sign_start = Instant::now();
        let signed = sign_request(signer.as_ref(), shaped)
            .await
            .map_err(|source| {
                tracing::error!(%source, "sign_request failed");
                if let Some(o) = observer {
                    o.emit_provider_error("signing_error", &source.to_string(), "signer");
                }
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
            if let Some(o) = observer {
                o.emit_provider_error("upstream_dispatch_error", &source.to_string(), "dispatch");
            }
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
        hooks: StreamHooks,
        active_limit: Option<ActiveLimit>,
        event_ctx: RequestEventContext,
        transform_ctx: ResponseTransformContext,
        prompt_cache_observation_context: Option<PromptCacheObservationContext>,
        observer: Option<LifecycleContext>,
    ) -> Response<Body> {
        let (mut parts, mut body) = response.into_parts();
        strip_hop_by_hop(&mut parts.headers);
        let usage_decoder = UsageDecoder::from_headers(&parts.headers);
        let transform_requested = transform_ctx.dialect.sse_event_transform_hook().is_some();
        let transform_decode_supported = usage_decoder.unsupported_encoding().is_none();
        let response_headers = (transform_requested && transform_decode_supported)
            .then(|| sanitized_response_headers_for_plugin(&parts.headers));
        if transform_requested && transform_decode_supported {
            sanitize_downstream_stream_headers(&mut parts.headers);
        }
        if let Some(encoding) = usage_decoder.unsupported_encoding() {
            tracing::warn!(
                request_id = %event_ctx.request_id,
                content_encoding = encoding,
                "skipping streaming usage extraction: unsupported content-encoding"
            );
        }
        let relay_start = Instant::now();
        let limit_engine = self.limit_engine.clone();
        let prompt_cache_shadow_enabled = self.config.prompt_cache_shadow.enabled;
        let stream = async_stream::stream! {
            let mut usage_decoder = usage_decoder;
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
            let mut prompt_cache_decode = PromptCacheObservationDecodeResult::default();
            let mut prompt_cache_observations_buffered = false;
            let mut prompt_cache_observations_emitted = false;
            let mut sse_event_count: u64 = 0;
            let mut content_delta_count: u64 = 0;
            let mut ping_count: u64 = 0;
            let mut total_bytes: u64 = 0;
            let mut sse_transform_active = transform_requested && transform_decode_supported;
            let mut transformed_output_started = false;
            let mut stream_transform_error: Option<ResponseTransformError> = None;
            let mut last_partial_at: Option<Instant> = None;
            let mut last_partial_output_tokens: u64 = 0;
            while let Some(frame) = body.frame().await {
                match frame {
                    Ok(frame) => {
                        if let Ok(data) = frame.into_data() {
                            let mut raw_passthrough_current_chunk = false;
                            let now = Instant::now();
                            if first_chunk_at.is_none() {
                                first_chunk_at = Some(now);
                            }
                            last_chunk_at = Some(now);
                            total_bytes = total_bytes.saturating_add(data.len() as u64);
                            match usage_decoder.push(&data) {
                                Ok(plaintext) => buffer.extend_from_slice(&plaintext),
                                Err(error) => {
                                    tracing::warn!(
                                        request_id = %event_ctx.request_id,
                                        %error,
                                        "skipping streaming usage extraction: chunk decode failed"
                                    );
                                        if sse_transform_active {
                                            if transformed_output_started {
                                            let transform_error = ResponseTransformError::Runtime {
                                                reason: format!("streaming response decode failed after transform output: {error}"),
                                            };
                                            let frame = make_response_transform_error_frame(&transform_error);
                                            observe_many(hooks.as_slice(), ObserveEvent::Chunk {
                                                batch_index,
                                                event_count: 1,
                                                total_bytes: frame.len(),
                                            });
                                            stream_transform_error = Some(transform_error);
                                                yield Ok::<Bytes, Infallible>(frame);
                                                break;
                                            }
                                            sse_transform_active = false;
                                            raw_passthrough_current_chunk = true;
                                        }
                                    }
                                }
                                while !raw_passthrough_current_chunk {
                                    let Some(end) = usage_parser::find_sse_event_end(&buffer) else {
                                        break;
                                    };
                                let raw = buffer.drain(..end).collect::<Vec<u8>>();
                                let raw = Bytes::from(raw);
                                let usage_update = accumulate_sse_usage(&raw, &mut usage);
                                if let Some(o) = observer.as_ref()
                                    && let Some(err) =
                                        usage_parser::detect_mid_stream_error(&raw)
                                {
                                    o.emit_lifecycle(cc_lb_contract::LifecycleEvent::StreamCompleted {
                                        event_id: o.event_id().to_owned(),
                                        result: Err(cc_lb_contract::StreamError {
                                            error_type: err.error_type.clone().unwrap_or_default(),
                                            error_message: err.error_message.clone().unwrap_or_default(),
                                        }),
                                    });
                                    o.set_terminal(
                                        StatusCode::OK,
                                        error_codes::UPSTREAM_STREAM_ERROR,
                                    );
                                }
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
                                        && !prompt_cache_observations_buffered
                                        && let Some(context) =
                                            prompt_cache_observation_context.as_ref()
                                    {
                                        let now_unix_secs = context.cache.clock_now_unix_secs();
                                        prompt_cache_decode =
                                            decode_prompt_cache_observations_pure(
                                                context,
                                                PromptCacheUsage::from(&usage),
                                                now_unix_secs,
                                            );
                                        prompt_cache_observations_buffered = true;
                                    }
                                }
                                if usage_update.message_stop {
                                    if message_stop_at.is_none() {
                                        message_stop_at = Some(now);
                                    }
                                    if status == StatusCode::OK
                                        && prompt_cache_shadow_enabled
                                        && prompt_cache_observations_buffered
                                        && !prompt_cache_observations_emitted
                                        && let Some(o) = observer.as_ref()
                                        && let Some(context) =
                                            prompt_cache_observation_context.as_ref()
                                    {
                                        emit_prompt_cache_observations_produced(
                                            o,
                                            context,
                                            &prompt_cache_decode,
                                            0,
                                        );
                                        prompt_cache_observations_emitted = true;
                                    }
                                }
                                if let Some(o) = observer.as_ref() {
                                    if usage_update.message_start_usage {
                                        o.emit_lifecycle(cc_lb_contract::LifecycleEvent::UsageObserved {
                                            event_id: o.event_id().to_owned(),
                                            usage: to_usage_snapshot(&usage),
                                            source: cc_lb_contract::UsageSource::MessageStart,
                                        });
                                    } else if usage_update.message_stop {
                                        o.emit_lifecycle(cc_lb_contract::LifecycleEvent::UsageObserved {
                                            event_id: o.event_id().to_owned(),
                                            usage: to_usage_snapshot(&usage),
                                            source: cc_lb_contract::UsageSource::MessageStop,
                                        });
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
                                        if !usage_update.message_start_usage && !usage_update.message_stop {
                                            o.emit_lifecycle(cc_lb_contract::LifecycleEvent::UsageObserved {
                                                event_id: o.event_id().to_owned(),
                                                usage: to_usage_snapshot(&usage),
                                                source: cc_lb_contract::UsageSource::MessageDelta,
                                            });
                                        }
                                        last_partial_at = Some(now);
                                        last_partial_output_tokens = usage.output_tokens;
                                    }
                                }
                                if sse_transform_active {
                                    let outgoing = if let Some(response_headers) = response_headers.as_ref() {
                                        match transform_sse_event_bytes(
                                            transform_ctx.dialect.sse_event_transform_hook(),
                                            &transform_ctx,
                                            &event_ctx,
                                            status,
                                            response_headers,
                                            raw.clone(),
                                        ) {
                                            SseTransformOutcome::Emit(bytes) => {
                                                transformed_output_started = true;
                                                bytes
                                            }
                                            SseTransformOutcome::Drop => {
                                                transformed_output_started = true;
                                                continue;
                                            }
                                            SseTransformOutcome::FailOpen => {
                                                if transformed_output_started {
                                                    let error = ResponseTransformError::Runtime {
                                                        reason: "failed to parse SSE event after transform output started".to_owned(),
                                                    };
                                                    let frame = make_response_transform_error_frame(&error);
                                                    observe_many(hooks.as_slice(), ObserveEvent::Chunk {
                                                        batch_index,
                                                        event_count: 1,
                                                        total_bytes: frame.len(),
                                                    });
                                                    stream_transform_error = Some(error);
                                                    yield Ok::<Bytes, Infallible>(frame);
                                                    break;
                                                }
                                                sse_transform_active = false;
                                                raw_passthrough_current_chunk = true;
                                                Bytes::new()
                                            }
                                            SseTransformOutcome::Error(error) => {
                                                tracing::warn!(
                                                    request_id = %event_ctx.request_id,
                                                    %error,
                                                    "sse response transform failed"
                                                );
                                                    if transformed_output_started {
                                                    let frame = make_response_transform_error_frame(&error);
                                                    observe_many(hooks.as_slice(), ObserveEvent::Chunk {
                                                        batch_index,
                                                        event_count: 1,
                                                        total_bytes: frame.len(),
                                                    });
                                                    stream_transform_error = Some(error);
                                                        yield Ok::<Bytes, Infallible>(frame);
                                                        break;
                                                }
                                                sse_transform_active = false;
                                                raw_passthrough_current_chunk = true;
                                                Bytes::new()
                                            }
                                        }
                                    } else {
                                        sse_transform_active = false;
                                        raw_passthrough_current_chunk = true;
                                        Bytes::new()
                                    };
                                    if raw_passthrough_current_chunk {
                                        break;
                                    }
                                    observe_many(hooks.as_slice(), ObserveEvent::Chunk {
                                        batch_index,
                                        event_count: 1,
                                        total_bytes: outgoing.len(),
                                    });
                                    batch_index = batch_index.saturating_add(1);
                                    yield Ok::<Bytes, Infallible>(outgoing);
                                }
                            }
                            if !sse_transform_active {
                                // Chunk fanout stays inline — high-volume, not bus-worthy.
                                observe_many(hooks.as_slice(), ObserveEvent::Chunk {
                                    batch_index,
                                    event_count: 1,
                                    total_bytes: data.len(),
                                });
                                batch_index = batch_index.saturating_add(1);
                                yield Ok::<Bytes, Infallible>(data);
                            }
                        }
                    }
                    Err(_source) => break,
                }
            }
            match usage_decoder.finish() {
                Ok(tail) if !tail.is_empty() => {
                    buffer.extend_from_slice(&tail);
                    while let Some(end) = usage_parser::find_sse_event_end(&buffer) {
                        let raw = buffer.drain(..end).collect::<Vec<u8>>();
                        let _ = accumulate_sse_usage(&raw, &mut usage);
                        if let Some(o) = observer.as_ref()
                            && usage_parser::detect_mid_stream_error(&raw).is_some()
                        {
                            o.set_terminal(
                                StatusCode::OK,
                                error_codes::UPSTREAM_STREAM_ERROR,
                            );
                        }
                        sse_event_count = sse_event_count.saturating_add(1);
                    }
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(
                        request_id = %event_ctx.request_id,
                        %error,
                        "streaming usage extractor decoder finish failed"
                    );
                }
            }
            if status == StatusCode::OK
                && prompt_cache_shadow_enabled
                && prompt_cache_observations_buffered
                && !prompt_cache_observations_emitted
                && !prompt_cache_decode.observations.is_empty()
                && let Some(o) = observer.as_ref()
                && let Some(context) = prompt_cache_observation_context.as_ref()
            {
                let dropped_aborted = u32::try_from(prompt_cache_decode.observations.len())
                    .unwrap_or(u32::MAX);
                emit_prompt_cache_observations_produced(
                    o,
                    context,
                    &prompt_cache_decode,
                    dropped_aborted,
                );
            }
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
            if let Some(mut active_limit) = active_limit
                && limit_engine.is_some()
                && active_limit.reservation.is_some()
            {
                active_limit.hand_off_reservation_to_reconcile_subscriber();
            }
            if let Some(o) = observer.as_ref() {
                if status == StatusCode::OK
                    && prompt_cache_shadow_enabled
                    && let Some(context) = prompt_cache_observation_context.as_ref()
                {
                    let now_unix_secs = context.cache.clock_now_unix_secs();
                    record_thread_usage_from_response(&event_ctx, context, &usage, now_unix_secs);
                }
                if let Some(error) = stream_transform_error.as_ref() {
                    o.emit_lifecycle(cc_lb_contract::LifecycleEvent::StreamCompleted {
                        event_id: o.event_id().to_owned(),
                        result: Err(cc_lb_contract::StreamError {
                            error_type: "response_transform_error".to_owned(),
                            error_message: error.to_string(),
                        }),
                    });
                } else {
                    o.emit_lifecycle(cc_lb_contract::LifecycleEvent::StreamCompleted {
                        event_id: o.event_id().to_owned(),
                        result: Ok(cc_lb_contract::StreamSuccess {
                            usage: to_usage_snapshot(&usage),
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
                            inter_token_avg_ms,
                        }),
                    });
                }
                o.set_termination_timings(
                    None,
                    None,
                    event_ctx.proxy_setup_ms,
                    Some(stream_total_ms),
                    elapsed_ms(first_chunk_at),
                );
                o.set_internal_errors(event_ctx.internal_errors.clone());
                if stream_transform_error.is_some() {
                    o.set_terminal(StatusCode::OK, error_codes::UPSTREAM_STREAM_ERROR);
                } else if status.is_client_error() || status.is_server_error() {
                    let code = if status.is_client_error() {
                        error_codes::UPSTREAM_4XX
                    } else {
                        error_codes::UPSTREAM_5XX
                    };
                    o.emit_provider_error(code, status.as_str(), "upstream");
                    o.set_terminal(status, code);
                } else {
                    o.set_success_status(status);
                }
                o.finish();
            }
            observe_many(hooks.as_slice(), ObserveEvent::RequestFinished {
                status,
                input_tokens: usage.present.then_some(usage.input_tokens),
                output_tokens: usage.present.then_some(usage.output_tokens),
                cache_creation_input_tokens: usage.present.then_some(usage.cache_creation_input_tokens),
                cache_read_input_tokens: usage.present.then_some(usage.cache_read_input_tokens),
                duration_ms: stream_total_ms,
            });
        };
        Response::from_parts(parts, Body::from_stream(stream))
    }
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

pub fn build_subscription_quota_samples(
    headers: &HeaderMap,
    upstream_id: Uuid,
    observed_at_unix_millis: u64,
) -> Vec<SubscriptionQuotaSample> {
    parse_anthropic_unified_headers(headers)
        .into_iter()
        .map(|observation| SubscriptionQuotaSample {
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
            fallback_percentage: observation.fallback_percentage,
            fallback_available: observation.fallback_available,
            overage_in_use: observation.overage_in_use,
            overage_period_monthly_utilization: observation.overage_period_monthly_utilization,
            upgrade_paths: observation.upgrade_paths,
            disabled_reason: observation.disabled_reason,
            extra_usage_enabled: None,
            extra_usage_monthly_limit: None,
            extra_usage_used_credits: None,
            ingested_at_unix_millis: observed_at_unix_millis,
        })
        .collect()
}

struct FilterPipelineResult {
    candidates: Vec<UpstreamCandidate>,
    stages: Vec<StageDecision>,
    internal_errors: Vec<InternalError>,
}

impl FilterPipelineResult {
    fn routing_trace(&self, terminal_decision: TerminalDecision) -> RoutingTrace {
        RoutingTrace {
            stages: self.stages.clone(),
            terminal_decision: Some(terminal_decision),
        }
    }
}

fn subscription_preference_trace(
    routing_trace: &RoutingTrace,
) -> Option<&cc_lb_plugin_api::SubscriptionPreferenceTrace> {
    routing_trace
        .stages
        .iter()
        .find_map(|stage| stage.subscription_preference.as_ref())
}

fn execute_filter_pipeline(
    filters: &[Arc<dyn cc_lb_plugin_api::FilterPlugin>],
    ctx: &RequestContext,
    principal: &Principal,
    candidates: Vec<UpstreamCandidate>,
    observer: Option<&LifecycleContext>,
) -> FilterPipelineResult {
    let mut current = candidates;
    let mut stages = Vec::with_capacity(filters.len());
    let mut internal_errors = Vec::new();

    for (stage_index, filter) in filters.iter().enumerate() {
        let stage_name = filter.plugin_name().to_owned();
        let stage_started = Instant::now();
        match filter.filter(ctx, principal, &current) {
            Ok(output) => {
                let stage_elapsed = stage_started.elapsed();
                let duration_ms = duration_to_ms(stage_elapsed);
                let stage_idx = u8::try_from(stage_index).unwrap_or(u8::MAX);
                let output = match validate_filter_output(
                    &output,
                    &current,
                    stage_idx,
                    &mut internal_errors,
                ) {
                    Ok(output) => output,
                    Err(error) => {
                        let message = format!("filter output validation failed: {error}");
                        tracing::warn!(
                            stage = stage_name.as_str(),
                            duration_ms,
                            error = message.as_str(),
                            "router filter returned invalid output; passing candidates through"
                        );
                        if let Some(o) = observer {
                            o.emit_provider_error(
                                "router_filter_invalid_output",
                                &message,
                                "router",
                            );
                        }
                        stages.push(StageDecision {
                            stage_name,
                            upstream_id: current.first().map(|candidate| candidate.upstream_id),
                            reason: Some(message.clone()),
                            duration_us: duration_to_us(stage_elapsed),
                            subscription_preference: None,
                            cache_affinity: None,
                        });
                        internal_errors.push(InternalError {
                            stage: InternalErrorStage::RouterFilter,
                            kind: InternalErrorKind::InvalidOutput,
                            message: Some(message),
                        });
                        continue;
                    }
                };
                tracing::debug!(
                    stage = stage_name.as_str(),
                    duration_ms,
                    input_candidates = current.len(),
                    kept_candidates = output.kept_upstream_ids.len(),
                    "router filter stage completed"
                );
                stages.push(StageDecision {
                    stage_name,
                    upstream_id: output.kept_upstream_ids.first().copied(),
                    reason: Some(output.reason.clone()),
                    duration_us: duration_to_us(stage_elapsed),
                    subscription_preference: output.subscription_preference.clone(),
                    cache_affinity: output.cache_affinity.clone(),
                });
                current = keep_filter_candidates(&current, &output.kept_upstream_ids);
            }
            Err(error @ (FilterError::Trap { .. } | FilterError::Runtime { .. })) => {
                let stage_elapsed = stage_started.elapsed();
                let duration_ms = duration_to_ms(stage_elapsed);
                let message = error.to_string();
                tracing::warn!(
                    stage = stage_name.as_str(),
                    duration_ms,
                    error = message.as_str(),
                    "router filter stage failed; passing candidates through"
                );
                if let Some(o) = observer {
                    o.emit_provider_error("router_filter_passthrough", &message, "router");
                }
                stages.push(StageDecision {
                    stage_name,
                    upstream_id: current.first().map(|candidate| candidate.upstream_id),
                    reason: Some(message.clone()),
                    duration_us: duration_to_us(stage_elapsed),
                    subscription_preference: None,
                    cache_affinity: None,
                });
                internal_errors.push(InternalError {
                    stage: InternalErrorStage::Router,
                    kind: InternalErrorKind::PluginError,
                    message: Some(message),
                });
            }
        }
    }

    FilterPipelineResult {
        candidates: current,
        stages,
        internal_errors,
    }
}

struct ValidatedOutput {
    kept_upstream_ids: Vec<Uuid>,
    reason: String,
    subscription_preference: Option<cc_lb_plugin_api::SubscriptionPreferenceTrace>,
    cache_affinity: Option<cc_lb_plugin_api::types::CacheAffinityTrace>,
}

#[derive(Debug, Error, PartialEq, Eq)]
enum ValidationError {
    #[error("unknown upstream id {unknown_id}")]
    Unknown { unknown_id: Uuid },
    #[error("duplicate upstream id {id}")]
    Duplicate { id: Uuid },
    #[error("kept output length {kept_len} exceeds input length {input_len}")]
    Superset { kept_len: usize, input_len: usize },
}

fn validate_filter_output(
    out: &FilterOutput,
    input: &[UpstreamCandidate],
    stage_idx: u8,
    internal_errors: &mut Vec<InternalError>,
) -> Result<ValidatedOutput, ValidationError> {
    let _ = stage_idx;
    let mut seen = HashSet::with_capacity(out.kept_upstream_ids.len());
    for id in &out.kept_upstream_ids {
        if !seen.insert(*id) {
            return Err(ValidationError::Duplicate { id: *id });
        }
    }

    if out.kept_upstream_ids.len() > input.len() {
        return Err(ValidationError::Superset {
            kept_len: out.kept_upstream_ids.len(),
            input_len: input.len(),
        });
    }

    let input_ids = input
        .iter()
        .map(|candidate| candidate.upstream_id)
        .collect::<HashSet<_>>();
    for id in &out.kept_upstream_ids {
        if !input_ids.contains(id) {
            return Err(ValidationError::Unknown { unknown_id: *id });
        }
    }

    let mut per_candidate_reasons = out.per_candidate_reasons.clone();
    if !per_candidate_reasons.is_empty() && per_candidate_reasons.len() != input.len() {
        if per_candidate_reasons.len() > input.len() {
            per_candidate_reasons.truncate(input.len());
        }
        internal_errors.push(InternalError {
            stage: InternalErrorStage::RouterFilter,
            kind: InternalErrorKind::InvalidOutput,
            message: Some("per_candidate_reasons sanitized".to_owned()),
        });
    }

    Ok(ValidatedOutput {
        kept_upstream_ids: out.kept_upstream_ids.clone(),
        reason: out.reason.clone(),
        subscription_preference: out.subscription_preference.clone(),
        cache_affinity: out.cache_affinity.clone(),
    })
}

fn keep_filter_candidates(
    candidates: &[UpstreamCandidate],
    kept_upstream_ids: &[Uuid],
) -> Vec<UpstreamCandidate> {
    candidates
        .iter()
        .filter(|candidate| kept_upstream_ids.contains(&candidate.upstream_id))
        .cloned()
        .collect()
}

fn terminal_candidates(
    candidates: &[UpstreamCandidate],
    terminal_decision: &TerminalDecision,
) -> Vec<UpstreamCandidate> {
    terminal_decision
        .upstream_id
        .and_then(|upstream_id| {
            candidates
                .iter()
                .find(|candidate| candidate.upstream_id == upstream_id)
        })
        .cloned()
        .into_iter()
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

fn response_from_collected(collected: CollectedResponse) -> Response<Body> {
    let mut response = Response::new(Body::from(collected.body));
    *response.status_mut() = collected.status;
    *response.headers_mut() = collected.headers;
    response
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

fn push_shape_internal_error(internal_errors: &mut Vec<InternalError>, message: &str) {
    let errors = redact_internal_errors(&[InternalError {
        stage: InternalErrorStage::Shape,
        kind: InternalErrorKind::Trap,
        message: Some(truncate_reason(message)),
    }]);
    internal_errors.extend(errors);
}

fn default_anthropic_base_url() -> Url {
    Url::parse(DEFAULT_ANTHROPIC_BASE_URL).expect("default Anthropic base URL parses")
}

fn raw_passthrough_request(
    base_url: Option<&Url>,
    ctx: &RequestContext,
    principal: &Principal,
) -> Result<ShapedRequest, Box<Response<Body>>> {
    let base_url = match base_url {
        Some(base_url) => base_url.clone(),
        None => Url::parse(DEFAULT_ANTHROPIC_BASE_URL).map_err(|source| {
            tracing::error!(%source, "default raw passthrough base URL failed to parse");
            Box::new(anthropic_error_response(
                StatusCode::BAD_GATEWAY,
                "api_error",
                "failed to prepare raw upstream request",
            ))
        })?,
    };
    let upstream = Upstream::AnthropicDirect {
        base_url: Some(base_url.clone()),
    };
    shape_request(
        &RawPassthroughDialect { base_url },
        ctx,
        &upstream,
        principal,
    )
    .map_err(|source| {
        tracing::error!(%source, "raw passthrough request failed");
        Box::new(anthropic_error_response(
            StatusCode::BAD_GATEWAY,
            "api_error",
            "failed to prepare raw upstream request",
        ))
    })
}

struct RawPassthroughDialect {
    base_url: Url,
}

impl UpstreamDialect for RawPassthroughDialect {
    fn shape(
        &self,
        ctx: &RequestContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, cc_lb_plugin_api::DialectError> {
        let mut url = self.base_url.clone();
        url.set_path(ctx.path.trim_start_matches('/'));
        url.set_query(ctx.query.as_deref());
        Ok(builder.shaped_request(
            url,
            ctx.method.clone(),
            ctx.downstream_headers.clone(),
            ctx.body_bytes.clone(),
        ))
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

/// Project the upstream response headers into a `HeaderSnapshot` for the
/// `UpstreamResponseStarted` lifecycle event.
///
/// Strips `Authorization` and similar credential-bearing headers by pattern
/// (we only copy the fields we explicitly whitelist). The `anthropic_headers`
/// map is a flat pass-through of every header whose lowercased name starts
/// with `anthropic-ratelimit-` OR exactly matches one of the identity slots
/// (`ANTHROPIC_IDENTITY_HEADERS`). Downstream subscribers reconstruct a
/// `HeaderMap` and parse into typed rate-limit/subscription-quota records.
fn header_snapshot_from(headers: &HeaderMap) -> cc_lb_contract::HeaderSnapshot {
    let mut anthropic_headers: std::collections::BTreeMap<String, String> =
        std::collections::BTreeMap::new();
    for (name, value) in headers.iter() {
        let name_lc = name.as_str();
        let Ok(val) = value.to_str() else { continue };
        if name_lc.starts_with("anthropic-ratelimit-")
            || cc_lb_contract::ANTHROPIC_IDENTITY_HEADERS.contains(&name_lc)
        {
            anthropic_headers.insert(name_lc.to_owned(), val.to_owned());
        }
    }
    cc_lb_contract::HeaderSnapshot {
        content_type: header_to_string(headers, "content-type"),
        content_encoding: header_to_string(headers, "content-encoding"),
        request_id: header_to_string(headers, "x-request-id")
            .or_else(|| header_to_string(headers, "request-id")),
        retry_after: header_to_string(headers, "retry-after"),
        anthropic_headers,
    }
}

fn next_request_id() -> String {
    let id = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut request_id = String::with_capacity("req_core_".len() + 20);
    request_id.push_str("req_core_");
    let _ = write!(&mut request_id, "{id}");
    request_id
}

fn system_time_to_unix_millis(value: SystemTime) -> u64 {
    value
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

// Shared parse of the request body inside `handle`. Every downstream consumer
// (stream / model / max_tokens / cache metadata) reads through this view so
// the body JSON is parsed exactly once per request instead of up to 7 times.
//
// Parser: `sonic_rs::from_slice` — hot-path SIMD JSON parser. Yields a
// `serde_json::Value` so all downstream consumers stay compatible without
// touching the serialization side (see `docs/adr/0002-json-library-strategy.md`).
struct RequestBodyView {
    parsed: Result<Value, sonic_rs::Error>,
}

impl RequestBodyView {
    fn new(body: &Bytes) -> Self {
        Self {
            parsed: sonic_rs::from_slice::<Value>(body),
        }
    }

    fn value(&self) -> Option<&Value> {
        self.parsed.as_ref().ok()
    }

    fn is_valid_json(&self) -> bool {
        self.parsed.is_ok()
    }

    fn stream(&self) -> bool {
        self.value()
            .and_then(|v| v.get("stream"))
            .and_then(Value::as_bool)
            .unwrap_or(false)
    }

    fn model(&self) -> Option<String> {
        self.value()
            .and_then(|v| v.get("model"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
    }

    fn limit_request(&self) -> LimitRequest {
        LimitRequest::from_value(self.value())
    }
}

#[derive(Clone, Debug)]
struct LimitRequest {
    model: String,
    max_tokens: i64,
    stream: bool,
}

impl LimitRequest {
    fn from_value(value: Option<&Value>) -> Self {
        Self {
            model: value
                .and_then(|v| v.get("model"))
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_owned(),
            max_tokens: value
                .and_then(|v| v.get("max_tokens"))
                .and_then(Value::as_i64)
                .unwrap_or(0),
            stream: value
                .and_then(|v| v.get("stream"))
                .and_then(Value::as_bool)
                .unwrap_or(false),
        }
    }
}

struct ActiveLimit {
    subject: AuthLimitSubject,
    request: LimitRequest,
    reservation: Option<LimitReservation>,
}

impl ActiveLimit {
    fn hand_off_reservation_to_reconcile_subscriber(&mut self) {
        if let Some(reservation) = self.reservation.take() {
            reservation.forget();
        }
    }
}

#[derive(Clone)]
pub(crate) struct RequestEventContext {
    pub(crate) request_id: String,
    pub(crate) thread_id: Option<String>,
    pub(crate) canonical_model_id: String,
    pub(crate) proxy_setup_ms: Option<u64>,
    pub(crate) stage_timings: AttemptTimings,
    pub(crate) internal_errors: Vec<InternalError>,
}

pub(crate) use crate::response_transform::{
    ResponseTransformContext, SseTransformOutcome, apply_buffered_transform_result,
    make_response_transform_error_frame, sanitize_downstream_stream_headers,
    sanitized_response_headers_for_plugin, transform_sse_event_bytes,
};

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
                lookback_prefixes: breakpoint
                    .lookback_prefixes
                    .iter()
                    .map(|prefix| CacheLookbackPrefix {
                        prefix_hash: prefix.prefix_hash.clone(),
                        content_block_index: saturating_u64_to_u32(prefix.content_block_index),
                        prefix_token_count: prefix.prefix_token_count,
                        lookback_distance: saturating_u64_to_u32(prefix.lookback_distance),
                    })
                    .collect(),
                token_estimate_source: breakpoint.token_estimate_source.clone(),
            })
            .collect()
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
pub(crate) struct AttemptTimings {
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

pub fn parse_request_cache_breakpoints(headers: &HeaderMap, body: &Bytes) -> Vec<CacheBreakpoint> {
    request_cache_metadata(headers, body).plugin_cache_breakpoints()
}

fn request_cache_metadata(headers: &HeaderMap, body: &Bytes) -> RequestCacheMetadata {
    let value = sonic_rs::from_slice::<Value>(body).ok();
    request_cache_metadata_from_value(headers, value.as_ref())
}

fn request_cache_metadata_from_value(
    headers: &HeaderMap,
    value: Option<&Value>,
) -> RequestCacheMetadata {
    let thread_id = header_to_string(headers, "x-claude-code-session-id")
        .or_else(|| header_to_string(headers, "x-claude-session-id"))
        .or_else(|| header_to_string(headers, "x-session-affinity"))
        .or_else(|| header_to_string(headers, "x-session-id"));
    let Some(value) = value else {
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

    let analysis = analyze_v3_prompt_cache(value, &canonical_model_id);
    let cache_breakpoints = analysis
        .breakpoints
        .iter()
        .map(|breakpoint| RequestCacheBreakpoint {
            block_index: breakpoint.block_index,
            source: request_breakpoint_source_from_v3(breakpoint.source),
            path: breakpoint.path.clone(),
            message_index: breakpoint.message_index,
            ttl: breakpoint.ttl.clone(),
            prefix_hash: breakpoint.prefix_key.clone(),
            prefix_token_count: breakpoint.prefix_token_count,
            lookback_prefixes: breakpoint
                .lookback_prefixes
                .iter()
                .map(|prefix| cc_lb_contract::RequestCacheLookbackPrefix {
                    prefix_hash: prefix.prefix_key.clone(),
                    content_block_index: prefix.content_block_index,
                    prefix_token_count: prefix.prefix_token_count,
                    lookback_distance: prefix.lookback_distance,
                })
                .collect(),
            token_estimate_source: Some(V3_TOKEN_ESTIMATE_SOURCE.to_owned()),
        })
        .collect::<Vec<_>>();
    let mut cache_control_message_indices = cache_breakpoints
        .iter()
        .filter_map(|breakpoint| breakpoint.message_index)
        .collect::<Vec<_>>();
    cache_control_message_indices.sort_unstable();
    cache_control_message_indices.dedup();

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

fn request_breakpoint_source_from_v3(
    source: V3PromptCacheBlockSource,
) -> RequestCacheBreakpointSource {
    match source {
        V3PromptCacheBlockSource::Tools => RequestCacheBreakpointSource::Tools,
        V3PromptCacheBlockSource::System => RequestCacheBreakpointSource::System,
        V3PromptCacheBlockSource::Message => RequestCacheBreakpointSource::Message,
    }
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

fn unix_now_ms(clock: &dyn Clock) -> u64 {
    unix_millis(clock.now()).min(u128::from(u64::MAX)) as u64
}

fn duration_to_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn duration_to_us(duration: Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
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
        Upstream::AnthropicDirect { .. } => "anthropic_direct",
    }
}

fn pricing_upstream_kind_label(upstream: &Upstream) -> Option<&'static str> {
    match upstream {
        Upstream::AnthropicDirect { .. } => Some("anthropic_key"),
    }
}

fn upstream_for_record(record: &UpstreamRecord) -> Result<Upstream, String> {
    match record.kind {
        StorageUpstreamKind::AnthropicApiKey | StorageUpstreamKind::AnthropicOauth => {
            Ok(Upstream::AnthropicDirect {
                base_url: record.base_url.clone(),
            })
        }
    }
}

fn upstream_kind_for_candidate(kind: StorageUpstreamKind) -> CandidateUpstreamKind {
    match kind {
        StorageUpstreamKind::AnthropicApiKey => CandidateUpstreamKind::AnthropicApiKey,
        StorageUpstreamKind::AnthropicOauth => CandidateUpstreamKind::AnthropicOauth,
    }
}

fn lineage_counterfactual_from_thread_usage(
    cache: Option<&Arc<dyn PromptCacheObservationCacheLike>>,
    candidates: &[UpstreamCandidate],
    canonical_model_id: &str,
    thread_id: Option<&str>,
) -> Option<(u32, Uuid)> {
    let cache = cache?;
    let thread_id = thread_id.filter(|id| !id.is_empty())?;
    let now_unix_secs = cache.clock_now_unix_secs();
    candidates
        .iter()
        .filter_map(|candidate| {
            let score = cache.thread_usage_score(
                candidate.upstream_id,
                canonical_model_id,
                thread_id,
                now_unix_secs,
            )?;
            (score.predicted_cache_read_tokens > 0)
                .then_some((score.predicted_cache_read_tokens, candidate.upstream_id))
        })
        .max_by(|left, right| left.0.cmp(&right.0).then_with(|| right.1.cmp(&left.1)))
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

fn to_usage_snapshot(u: &UsageCounts) -> cc_lb_contract::UsageSnapshot {
    cc_lb_contract::UsageSnapshot {
        input_tokens: u.input_tokens,
        output_tokens: u.output_tokens,
        cache_creation_input_tokens: u.cache_creation_input_tokens,
        cache_creation_input_tokens_5m: u.cache_creation_input_tokens_5m,
        cache_creation_input_tokens_1h: u.cache_creation_input_tokens_1h,
        cache_read_input_tokens: u.cache_read_input_tokens,
        thinking_tokens: u.thinking_tokens,
        web_search_requests: u.web_search_requests,
        web_fetch_requests: u.web_fetch_requests,
        service_tier: u.service_tier.clone(),
        inference_geo: u.inference_geo.clone(),
        iterations: u.iterations.clone(),
    }
}

fn record_thread_usage_from_response(
    event_ctx: &RequestEventContext,
    context: &PromptCacheObservationContext,
    usage: &UsageCounts,
    now_unix_secs: u64,
) {
    let Some(thread_id) = event_ctx.thread_id.as_deref().filter(|id| !id.is_empty()) else {
        return;
    };
    if event_ctx.canonical_model_id.is_empty() {
        return;
    }
    context.cache.record_thread_usage(
        context.upstream_id,
        &event_ctx.canonical_model_id,
        thread_id,
        PromptCacheThreadUsage {
            cache_read_input_tokens: usage.cache_read_input_tokens,
            cache_creation_input_tokens_5m: usage.cache_creation_input_tokens_5m,
            cache_creation_input_tokens_1h: usage.cache_creation_input_tokens_1h,
        },
        now_unix_secs,
    );
}

fn principal_kind_lite_as_str(kind: &cc_lb_storage_api::types::PrincipalKindLite) -> &'static str {
    match kind {
        cc_lb_storage_api::types::PrincipalKindLite::Machine => "machine",
        cc_lb_storage_api::types::PrincipalKindLite::Human => "human",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use cc_lb_storage_api::{
        SubscriptionQuotaSampleKind, SubscriptionQuotaSource, SubscriptionQuotaStatus,
        SubscriptionQuotaWindow,
        principal::{PrincipalKind as StoragePrincipalKind, PrincipalRecord},
        upstream::{UpstreamKind as StorageRecordKind, UpstreamRecord},
    };
    use http::header::{HeaderName, HeaderValue};

    use super::*;

    const TEST_MODEL: &str = "claude-sonnet-4-5-20250929";

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
            cache_breakpoint(2, "cold", 300, TtlClass::Ephemeral5m),
        ];

        let candidates = build_candidates(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            TEST_MODEL,
            &breakpoints,
            None,
            &crate::clock::SystemClock,
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
            None,
            &crate::clock::SystemClock,
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
            .global_observability_hooks(Vec::new())
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
                    None,
                    &crate::clock::SystemClock,
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
    fn build_candidates_no_warm_returns_cold_creation_estimate() {
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
            None,
            &crate::clock::SystemClock,
        );

        assert_eq!(
            candidates[0].cache_score,
            Some(CacheScore {
                predicted_cache_read_tokens: 0,
                predicted_cache_creation_tokens_5m: 100,
                predicted_cache_creation_tokens_1h: 0,
                predicted_uncached_input_tokens: 0,
                predicted_expires_at_unix_secs: None,
                matched_breakpoint_index: None,
                confidence: 0.0,
                ambiguity_reason: None,
                matched_v3_cache_key: None,
                breakpoint_content_block_index: None,
                matched_content_block_index: None,
                lookback_distance: None,
                token_estimate_source: None,
            })
        );
    }

    #[test]
    fn build_candidates_ignores_positive_thread_score_for_active_cache_score() {
        let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000204").unwrap();
        let thread_id = "thread-cache-positive";
        let cache = TestPromptCacheObservationCache::new(32, TEST_MODEL).with_thread_score(
            upstream_id,
            TEST_MODEL,
            thread_id,
            CacheScore {
                predicted_cache_read_tokens: 120_000,
                predicted_cache_creation_tokens_5m: 0,
                predicted_cache_creation_tokens_1h: 0,
                predicted_uncached_input_tokens: 0,
                predicted_expires_at_unix_secs: Some(4_100_000_300),
                matched_breakpoint_index: None,
                confidence: 0.5,
                ambiguity_reason: Some("thread_usage_lineage".to_owned()),
                matched_v3_cache_key: Some("lineage-key".to_owned()),
                breakpoint_content_block_index: Some(0),
                matched_content_block_index: Some(0),
                lookback_distance: Some(0),
                token_estimate_source: Some("thread_usage_lineage".to_owned()),
            },
        );
        let view = cache_score_view(upstream_id, Arc::new(cache));
        let breakpoints = vec![cache_breakpoint(0, "cold", 100, TtlClass::Ephemeral5m)];

        let candidates = build_candidates(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            TEST_MODEL,
            &breakpoints,
            Some(thread_id),
            &crate::clock::SystemClock,
        );

        let score = candidates[0].cache_score.as_ref().expect("cache score");
        assert_eq!(score.predicted_cache_read_tokens, 0);
        assert_eq!(score.predicted_cache_creation_tokens_5m, 100);
        assert_eq!(score.ambiguity_reason, None);
        assert_eq!(score.matched_v3_cache_key, None);
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
            None,
            &crate::clock::SystemClock,
        );

        let score = candidates[0].cache_score.as_ref().expect("cache score");
        assert_eq!(score.predicted_cache_read_tokens, 32);
        assert_eq!(score.matched_breakpoint_index, Some(31));
    }

    #[test]
    fn response_decoder_decodes_observations_without_side_effects() {
        let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000221").unwrap();
        let now = 1_800_000_000;
        let cache = Arc::new(
            RecordingPromptCacheObservationCache::new(vec![
                warm_entry("short", TtlClass::Ephemeral5m, now + 120, 1),
                warm_entry("hit", TtlClass::Ephemeral1h, now + 3_000, 2),
            ])
            .with_clock_now(now),
        );
        let context = PromptCacheObservationContext {
            upstream_id,
            canonical_model_id: TEST_MODEL.to_owned(),
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
        };

        let decoded = decode_prompt_cache_observations_pure(
            &context,
            PromptCacheUsage {
                cache_creation_input_tokens: 800,
                cache_read_input_tokens: 2_400,
            },
            now,
        );

        assert_eq!(decoded.dropped_below_threshold, 0);
        assert_eq!(decoded.observations.len(), 2);
        assert_eq!(decoded.observations[0].prefix_hash, "hit");
        assert_eq!(decoded.observations[0].ttl_class, TtlClass::Ephemeral1h);
        assert_eq!(decoded.observations[0].expires_at_unix_secs, now + 3_570);
        assert_eq!(decoded.observations[1].prefix_hash, "write");
        assert_eq!(decoded.observations[1].ttl_class, TtlClass::Ephemeral5m);
        assert_eq!(decoded.observations[1].expires_at_unix_secs, now + 270);
        assert!(cache.upserts().is_empty());
    }

    #[test]
    fn hit_observation_uses_sliding_ttl_from_now() {
        let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000224").unwrap();
        let now = 1_800_000_000;
        let near_expiry = now + 30;
        let cache = Arc::new(
            RecordingPromptCacheObservationCache::new(vec![warm_entry(
                "hit",
                TtlClass::Ephemeral5m,
                near_expiry,
                1,
            )])
            .with_clock_now(now),
        );
        let context = PromptCacheObservationContext {
            upstream_id,
            canonical_model_id: TEST_MODEL.to_owned(),
            cache_breakpoints: vec![cache_breakpoint(0, "hit", 2_400, TtlClass::Ephemeral5m)],
            warm_entries_at_decision: vec![warm_entry(
                "hit",
                TtlClass::Ephemeral5m,
                near_expiry,
                1,
            )],
            cache: cache.clone(),
        };

        let decoded = decode_prompt_cache_observations_pure(
            &context,
            PromptCacheUsage {
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 2_400,
            },
            now,
        );

        assert_eq!(decoded.dropped_below_threshold, 0);
        assert_eq!(decoded.observations.len(), 1);
        let observation = &decoded.observations[0];
        assert_eq!(observation.prefix_hash, "hit");
        assert_eq!(observation.ttl_class, TtlClass::Ephemeral5m);
        assert_eq!(
            observation.kind,
            DecodedPromptCacheObservationKind::Hit,
            "cache_read>0 must produce a Hit observation"
        );
        assert_eq!(
            observation.expires_at_unix_secs,
            now + 270,
            "Hit observation must carry a fresh `now + ttl - grace` (Ephemeral5m -> 300 - 30) so the sliding-TTL semantics of Anthropic's prompt cache are reflected in cc-lb's observation store; regression guard for the router false-cold bug"
        );
        assert!(
            observation.expires_at_unix_secs > near_expiry,
            "sliding refresh must strictly extend the near-expiry stored value ({near_expiry}); got {}",
            observation.expires_at_unix_secs
        );
    }

    #[test]
    fn hit_observation_honors_configured_grace_margin() {
        let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000225").unwrap();
        let now = 1_800_000_000;
        let cache = Arc::new(
            RecordingPromptCacheObservationCache::new(vec![warm_entry(
                "hit",
                TtlClass::Ephemeral5m,
                now + 30,
                1,
            )])
            .with_clock_now(now)
            .with_grace_secs(60),
        );
        let context = PromptCacheObservationContext {
            upstream_id,
            canonical_model_id: TEST_MODEL.to_owned(),
            cache_breakpoints: vec![cache_breakpoint(0, "hit", 2_400, TtlClass::Ephemeral5m)],
            warm_entries_at_decision: vec![warm_entry("hit", TtlClass::Ephemeral5m, now + 30, 1)],
            cache: cache.clone(),
        };

        let decoded = decode_prompt_cache_observations_pure(
            &context,
            PromptCacheUsage {
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 2_400,
            },
            now,
        );

        assert_eq!(decoded.observations.len(), 1);
        assert_eq!(
            decoded.observations[0].expires_at_unix_secs,
            now + 240,
            "grace_secs=60 must be applied end-to-end: expected now + 300 - 60"
        );
    }

    #[test]
    fn response_decoder_error_response_skips_observation() {
        empty_usage_decodes_no_observations();
    }

    #[test]
    fn empty_usage_decodes_no_observations() {
        let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000222").unwrap();
        let now = 1_800_000_000;
        let cache = Arc::new(RecordingPromptCacheObservationCache::new(vec![warm_entry(
            "hit",
            TtlClass::Ephemeral5m,
            now + 120,
            1,
        )]));
        let context = PromptCacheObservationContext {
            upstream_id,
            canonical_model_id: TEST_MODEL.to_owned(),
            cache_breakpoints: vec![cache_breakpoint(0, "hit", 1_200, TtlClass::Ephemeral5m)],
            warm_entries_at_decision: vec![warm_entry("hit", TtlClass::Ephemeral5m, now + 120, 1)],
            cache: cache.clone(),
        };

        let decoded = decode_prompt_cache_observations_pure(
            &context,
            PromptCacheUsage {
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
            },
            now,
        );

        assert!(decoded.observations.is_empty());
        assert_eq!(decoded.dropped_below_threshold, 0);
        assert!(cache.upserts().is_empty());
    }

    #[test]
    fn below_threshold_prefix_skips_observation() {
        let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000223").unwrap();
        let cache = Arc::new(RecordingPromptCacheObservationCache::new(Vec::new()));
        let context = PromptCacheObservationContext {
            upstream_id,
            canonical_model_id: TEST_MODEL.to_owned(),
            cache_breakpoints: vec![cache_breakpoint(0, "tiny", 1_023, TtlClass::Ephemeral5m)],
            warm_entries_at_decision: Vec::new(),
            cache: cache.clone(),
        };

        let decoded = decode_prompt_cache_observations_pure(
            &context,
            PromptCacheUsage {
                cache_creation_input_tokens: 1_023,
                cache_read_input_tokens: 0,
            },
            cache.clock_now_unix_secs(),
        );

        assert!(decoded.observations.is_empty());
        assert_eq!(decoded.dropped_below_threshold, 1);
        assert!(cache.upserts().is_empty());
    }

    #[test]
    fn build_subscription_quota_samples_from_headers() {
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
        headers.insert(
            HeaderName::from_static("anthropic-ratelimit-unified-7d-sonnet-fallback-percentage"),
            HeaderValue::from_static("0.5"),
        );
        headers.insert(
            HeaderName::from_static("anthropic-ratelimit-unified-7d-sonnet-surpassed-threshold"),
            HeaderValue::from_static("0.75"),
        );
        headers.insert(
            HeaderName::from_static("anthropic-ratelimit-unified-fallback"),
            HeaderValue::from_static("available"),
        );
        headers.insert(
            HeaderName::from_static("anthropic-ratelimit-unified-overage-in-use"),
            HeaderValue::from_static("true"),
        );
        headers.insert(
            HeaderName::from_static(
                "anthropic-ratelimit-unified-overage-period-monthly-utilization",
            ),
            HeaderValue::from_static("0.20"),
        );
        headers.insert(
            HeaderName::from_static("anthropic-ratelimit-unified-upgrade-paths"),
            HeaderValue::from_static("team_growth,max_5x"),
        );

        let records = build_subscription_quota_samples(&headers, upstream_id, 123_456);

        assert_eq!(records.len(), 2);
        let unified = records
            .iter()
            .find(|r| r.window == SubscriptionQuotaWindow::Unified)
            .expect("unified top-level record");
        let sonnet = records
            .iter()
            .find(|r| r.window == SubscriptionQuotaWindow::SevenDaySonnet)
            .expect("7d-sonnet record");

        assert_eq!(sonnet.upstream_id, upstream_id);
        assert_eq!(sonnet.source, SubscriptionQuotaSource::Header);
        assert_eq!(sonnet.sample_kind, SubscriptionQuotaSampleKind::Sample);
        assert_eq!(sonnet.observed_at_unix_millis, 123_456);
        assert_eq!(sonnet.ingested_at_unix_millis, 123_456);
        assert_ne!(sonnet.sample_id, Uuid::nil());
        assert_eq!(sonnet.utilization, Some(0.42));
        assert_eq!(sonnet.status, Some(SubscriptionQuotaStatus::AllowedWarning));
        assert_eq!(sonnet.fallback_percentage, Some(0.5));
        assert_eq!(sonnet.surpassed_threshold, Some(0.75));

        assert_eq!(unified.fallback_available, Some(true));
        assert_eq!(unified.overage_in_use, Some(true));
        assert_eq!(unified.overage_period_monthly_utilization, Some(0.20));
        assert_eq!(
            unified.upgrade_paths.as_deref(),
            Some(["max_5x".to_owned(), "team_growth".to_owned()].as_slice())
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
        assert_eq!(metadata.cache_breakpoints[2].block_index, 3);
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
    fn request_cache_metadata_uses_opencode_session_affinity_as_thread_id() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-session-affinity"),
            HeaderValue::from_static("opencode-session-123"),
        );
        let body = Bytes::from_static(br#"{"model":"claude-sonnet-4-5"}"#);

        let metadata = request_cache_metadata(&headers, &body);

        assert_eq!(metadata.thread_id.as_deref(), Some("opencode-session-123"));
    }

    #[test]
    fn request_cache_metadata_uses_opencode_x_session_id_as_thread_id() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-session-id"),
            HeaderValue::from_static("opencode-session-456"),
        );
        let body = Bytes::from_static(br#"{"model":"claude-sonnet-4-5"}"#);

        let metadata = request_cache_metadata(&headers, &body);

        assert_eq!(metadata.thread_id.as_deref(), Some("opencode-session-456"));
    }

    #[test]
    fn request_cache_metadata_uses_v3_content_block_hash() {
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
        let expected_v3 =
            analyze_v3_prompt_cache(&request, canonical_model_id("claude-sonnet-4-5"))
                .breakpoints
                .first()
                .expect("system cache breakpoint")
                .prefix_key
                .clone();
        let legacy_v2 = cache_prefix_hash_v2(
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

        assert_eq!(breakpoint.prefix_hash, expected_v3);
        assert_ne!(breakpoint.prefix_hash, legacy_v2);
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
        grace_secs: u64,
    }

    impl RecordingPromptCacheObservationCache {
        fn new(warm_entries: Vec<WarmCacheEntry>) -> Self {
            Self {
                warm_entries,
                upserts: Mutex::new(Vec::new()),
                refreshes: Mutex::new(Vec::new()),
                clock_now: 0,
                grace_secs: 30,
            }
        }

        fn with_clock_now(mut self, clock_now: u64) -> Self {
            self.clock_now = clock_now;
            self
        }

        fn with_grace_secs(mut self, grace_secs: u64) -> Self {
            self.grace_secs = grace_secs;
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

        fn grace_margin_secs(&self) -> u64 {
            self.grace_secs
        }

        fn clock_now_unix_secs(&self) -> u64 {
            self.clock_now
        }
    }

    struct TestPromptCacheObservationCache {
        cap: usize,
        expected_model: &'static str,
        entries: HashMap<Uuid, Vec<WarmCacheEntry>>,
        thread_scores: HashMap<(Uuid, String, String), CacheScore>,
    }

    impl TestPromptCacheObservationCache {
        fn new(cap: usize, expected_model: &'static str) -> Self {
            Self {
                cap,
                expected_model,
                entries: HashMap::new(),
                thread_scores: HashMap::new(),
            }
        }

        fn with_entries(mut self, upstream_id: Uuid, entries: Vec<WarmCacheEntry>) -> Self {
            self.entries.insert(upstream_id, entries);
            self
        }

        fn with_thread_score(
            mut self,
            upstream_id: Uuid,
            canonical_model: &str,
            thread_id: &str,
            score: CacheScore,
        ) -> Self {
            self.thread_scores.insert(
                (
                    upstream_id,
                    canonical_model.to_owned(),
                    thread_id.to_owned(),
                ),
                score,
            );
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

        fn thread_usage_score(
            &self,
            upstream_id: Uuid,
            canonical_model: &str,
            thread_id: &str,
            _now_unix_secs: u64,
        ) -> Option<CacheScore> {
            self.thread_scores
                .get(&(
                    upstream_id,
                    canonical_model.to_owned(),
                    thread_id.to_owned(),
                ))
                .cloned()
        }

        fn grace_margin_secs(&self) -> u64 {
            30
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
            .global_observability_hooks(Vec::new())
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
            lookback_prefixes: vec![CacheLookbackPrefix {
                prefix_hash: prefix_hash.to_owned(),
                content_block_index: index,
                prefix_token_count,
                lookback_distance: 0,
            }],
            token_estimate_source: Some("test".to_owned()),
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
            enabled: true,
            revision: 1,
            oauth_token_generation: 0,
            created_at_unix_secs: 0,
            updated_at_unix_secs: 0,
            ..UpstreamRecord::default()
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

    #[test]
    fn gzip_encoded_sse_stream_yields_usage_via_decoder() {
        use std::io::Write as _;

        let sse_plaintext = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":48,\"output_tokens\":1}}}\n\n",
            "event: content_block_delta\n",
            "data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n",
            "event: message_delta\n",
            "data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":72}}\n\n",
            "event: message_stop\n",
            "data: {\"type\":\"message_stop\"}\n\n",
        )
        .as_bytes();

        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(sse_plaintext).expect("gzip encode");
        let compressed = encoder.finish().expect("finish gzip");

        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::CONTENT_ENCODING,
            HeaderValue::from_static("gzip"),
        );
        let mut decoder = UsageDecoder::from_headers(&headers);
        assert!(decoder.is_active(), "gzip decoder should be active");

        let mut buffer: Vec<u8> = Vec::new();
        let mut usage = UsageCounts::default();
        let mut events_seen = 0_u64;

        for chunk in compressed.chunks(7) {
            let plaintext = decoder.push(chunk).expect("decode chunk");
            buffer.extend_from_slice(&plaintext);
            while let Some(end) = usage_parser::find_sse_event_end(&buffer) {
                let raw = buffer.drain(..end).collect::<Vec<u8>>();
                let _ = accumulate_sse_usage(&raw, &mut usage);
                events_seen += 1;
            }
        }
        let tail = decoder.finish().expect("decoder finish");
        buffer.extend_from_slice(&tail);
        while let Some(end) = usage_parser::find_sse_event_end(&buffer) {
            let raw = buffer.drain(..end).collect::<Vec<u8>>();
            let _ = accumulate_sse_usage(&raw, &mut usage);
            events_seen += 1;
        }

        assert_eq!(
            events_seen, 4,
            "expected 4 SSE events parsed from decoded stream"
        );
        assert!(usage.present, "usage must be marked present after parsing");
        assert_eq!(usage.input_tokens, 48);
        assert_eq!(usage.output_tokens, 72);
    }
}
