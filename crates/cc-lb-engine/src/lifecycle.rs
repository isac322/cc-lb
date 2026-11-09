#[path = "realtime_websocket_lifecycle.rs"]
mod realtime_websocket_lifecycle;

use std::collections::{HashMap, HashSet, hash_map::DefaultHasher};
use std::convert::Infallible;
use std::fmt::Write as _;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use axum::body::Body as AxumBody;
use bytes::{Bytes, BytesMut};
use cc_lb_config::{PromptCacheShadowConfig, UpstreamAffinityConfig};
#[cfg(test)]
use cc_lb_control::dynamic_view::DynamicViewBuilder;
use cc_lb_domain::{
    BUILTIN_SUBSCRIPTION_PREFERENCE_ID, CacheBreakpoint, CacheBreakpointSource,
    CacheLookbackPrefix, CachePricingSummary, CacheScore, CredentialKind, InternalError,
    InternalErrorKind, InternalErrorStage, NativeCachePricing, NativeCachePricingComponent,
    NativeCachePricingLifetime, NativeCachePricingProvider, NativeCachePricingRate,
    NativeCachePricingStatus, PrefixBytes, Principal, PrincipalKind, Provider, RoutingTrace,
    StageDecision, TerminalDecision, TerminalStrategy, TtlClass, Upstream, UpstreamCandidate,
    UpstreamKind as CandidateUpstreamKind, WarmCacheEntry,
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
use cc_lb_storage_api::{
    PrincipalKind as DbPrincipalKind, PrincipalKindLite, PromptCacheObservationRecord,
    ProviderPromptCacheObservationRecord, StoredApiKeyRecord, UpstreamAffinityBinding,
    UpstreamAffinityKey, UpstreamAffinityStore, UpstreamRateLimitObservationRecord, UpstreamRecord,
    upstream::UpstreamKind as StorageUpstreamKind,
};
#[cfg(test)]
use cc_lb_upstream::{ApiKeyAwareSignerFactory, SignerFactory};
use cc_lb_upstream::{
    DialectError, DialectShapeContext, ResponseTransformError, RetryDecision, ShapedRequest,
    ShapedRequestBuilder, SignedRequest, Signer, SignerError, SseEventTransformHook,
    TransformResponseRequest, UpstreamDialect, UpstreamError, shape_request, sign_request,
};
use http::header::{CONTENT_LENGTH, CONTENT_TYPE, RETRY_AFTER};
use http::{HeaderMap, HeaderName, HeaderValue, Method, Request, Response, StatusCode};
use http_body::Body as _;
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use rand::{RngExt, SeedableRng, rngs::StdRng};
use serde_json::Value;
#[cfg(test)]
use serde_json::json;
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
use crate::error_format::{
    anthropic_error_body, anthropic_error_response, provider_error_body,
    provider_error_body_with_retry_after, provider_error_response,
    provider_error_response_with_retry_after,
};
use crate::hop_by_hop::strip_hop_by_hop;
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
    ProviderCapability, body_is_consistency_safe, classify as classify_provider_capability,
};
use crate::request_classification::{
    classify_client_request_kind_with_class, is_thread_sticky_subagent_seed,
};
use crate::request_context::{ProviderScope, RequestContext};
use crate::request_timing::{REQUEST_STAGE_TIMINGS, RequestStageTimings};
use crate::responses_websocket::{
    PreparedWebSocketTurn, WebSocketCandidate, WebSocketError, WebSocketIngress,
    WebSocketPluginRequest,
};
use crate::sse_error_frame::make_provider_error_frame;
use crate::subscription_quota_events::SubscriptionQuotaSink;
use crate::terminal_observer::{
    InternalFailure, LifecycleContext, StreamErrorClassification, StreamTerminationCause,
    TerminalClassification, UpstreamErrorCode, classify_stream_error, error_codes,
    numeric_attribute,
};
use crate::upstream_affinity::{
    UpstreamAffinityExtractionError, extract_request_affinity_keys, extract_response_affinity_keys,
    extract_response_affinity_keys_from_sse_event, gemini_cached_content_expiry,
    request_observes_response_affinity,
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

pub type Body = AxumBody;

/// Prefix-hash schema persisted with prompt-cache observations.
///
/// Version 6 excludes the official Anthropic billing-header system block from
/// effective cache identity while preserving literal custom-upstream identity.
/// The version filter cold-starts older persisted observations.
pub use cc_lb_domain::HASH_SCHEMA_VERSION;

const DEFAULT_MESSAGES_CAP_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_FILES_CAP_BYTES: usize = 100 * 1024 * 1024;
const DEFAULT_ANTHROPIC_BASE_URL: &str = "https://api.anthropic.com/";
const DEFAULT_OPENAI_BASE_URL: &str = "https://api.openai.com/";
const DEFAULT_OPENAI_CODEX_BASE_URL: &str = "https://chatgpt.com/backend-api/codex";
const DEFAULT_GEMINI_BASE_URL: &str = "https://generativelanguage.googleapis.com/";
const DEFAULT_GEMINI_CODE_ASSIST_BASE_URL: &str = "https://cloudcode-pa.googleapis.com/v1internal";
pub(super) const DEFAULT_MAX_INPUT_ESTIMATE: i64 = 4000;
const DECOMPRESSION_OUTPUT_BUDGET_BODY_CAP_MULTIPLIER: usize = 4;
const INCOMPLETE_SSE_EVENT_BUDGET_BODY_CAP_MULTIPLIER: usize = 1;
const SSE_BUFFER_MAX_RETAINED_CAPACITY_BYTES: usize = 1024 * 1024;
const SSE_BUFFER_SMALL_EVENT_CAPACITY_FRACTION: usize = 8;
// `upstream_response` names the proxy leg where decoding failed. It does not
// attribute the malformed bytes to the provider: transport corruption or a
// local decoder defect can produce the same classification.
const UPSTREAM_RESPONSE_BODY_ERROR_TYPE: &str = "upstream_response_body_error";
const UPSTREAM_RESPONSE_DECODE_ERROR_TYPE: &str = "upstream_response_decode_error";
const UPSTREAM_RESPONSE_FRAMING_ERROR_TYPE: &str = "upstream_response_framing_error";
const UPSTREAM_RESPONSE_INCOMPLETE_ERROR_TYPE: &str = "upstream_response_incomplete";
const UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE: &str = "api_error";
const UPSTREAM_AFFINITY_ERROR_TYPE: &str = "upstream_affinity_error";
const UPSTREAM_AFFINITY_UNAVAILABLE_MESSAGE: &str =
    "upstream affinity is unavailable for opaque request content";
const UPSTREAM_AFFINITY_KEY_LIMIT_MESSAGE: &str = "too many upstream affinity keys in request";
const UPSTREAM_AFFINITY_TARGET_INELIGIBLE_MESSAGE: &str =
    "the upstream that owns the referenced provider state is not eligible for this request";
const UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE: &str =
    "upstream SSE response could not be safely processed";
const UPSTREAM_BUFFERED_BODY_FAILED_MESSAGE: &str = "upstream response body could not be read";

fn is_upstream_response_decode_error(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::InvalidData
            | std::io::ErrorKind::InvalidInput
            | std::io::ErrorKind::UnexpectedEof
    )
}

const UPSTREAM_DIAGNOSTIC_HEADER_MAX_BYTES: usize = 256;

/// True when the unconsumed SSE bytes hold no partial event: only line
/// terminators remain, matching the end-of-stream framing check.
fn sse_buffer_is_blank(buffer: &[u8]) -> bool {
    buffer.iter().all(|byte| matches!(byte, b'\r' | b'\n'))
}

/// Accept-with-warning predicate for an upstream SSE body that fails after
/// cc-lb already parsed the upstream `message_stop` (#663). The caller adds
/// the emitter-of-record and error-class conditions specific to its failure
/// site.
fn upstream_end_after_message_stop_is_acceptable(
    status: StatusCode,
    semantic_message_stop_seen: bool,
    terminal_error_recorded: bool,
    provider_error_seen: bool,
    warning_recorded: bool,
    unparsed_sse: &[u8],
) -> bool {
    status.is_success()
        && semantic_message_stop_seen
        && !terminal_error_recorded
        && !provider_error_seen
        && !warning_recorded
        && sse_buffer_is_blank(unparsed_sse)
}

fn bounded_diagnostic_header(value: Option<&HeaderValue>) -> Option<String> {
    let text = value?.to_str().ok()?;
    // `to_str` only accepts visible ASCII, so every index is a char boundary.
    text.get(..text.len().min(UPSTREAM_DIAGNOSTIC_HEADER_MAX_BYTES))
        .map(str::to_owned)
}

/// Upstream body-end evidence collected while relaying an SSE response.
/// Header values are kept as cheap `HeaderValue` clones and only rendered
/// into strings when the body ended abnormally.
struct UpstreamBodyDiagnostics {
    http_version: http::Version,
    request_id: Option<HeaderValue>,
    content_encoding: Option<HeaderValue>,
    content_length: Option<u64>,
    body_end: Option<cc_lb_lifecycle::UpstreamBodyEnd>,
    error: Option<StreamErrorClassification>,
    warning: Option<(&'static str, String)>,
}

impl UpstreamBodyDiagnostics {
    fn from_parts(parts: &http::response::Parts) -> Self {
        Self {
            http_version: parts.version,
            request_id: parts.headers.get("request-id").cloned(),
            content_encoding: parts.headers.get(http::header::CONTENT_ENCODING).cloned(),
            content_length: parts
                .headers
                .get(CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.trim().parse().ok()),
            body_end: None,
            error: None,
            warning: None,
        }
    }

    /// Records how the upstream body ended. The first abnormal end wins, so a
    /// transport error followed by a decoder finish failure stays a transport
    /// error.
    fn record_body_end(
        &mut self,
        body_end: cc_lb_lifecycle::UpstreamBodyEnd,
        classify: impl FnOnce() -> StreamErrorClassification,
    ) {
        if self.body_end.is_none() {
            self.body_end = Some(body_end);
            self.error = Some(classify());
        }
    }

    fn record_warning(&mut self, warning_type: &'static str, message: String) {
        if self.warning.is_none() {
            self.warning = Some((warning_type, message));
        }
    }

    fn warning_recorded(&self) -> bool {
        self.warning.is_some()
    }

    /// Renders the lifecycle payload, or `None` when the body ended cleanly.
    fn into_lifecycle(
        self,
        body_bytes: u64,
        message_stop_ms: Option<u64>,
    ) -> Option<cc_lb_lifecycle::UpstreamStreamDiagnostics> {
        let body_end = self.body_end?;
        let (body_error_cause, body_error_io_kind, body_error_h2_reason) = match self.error {
            Some(error) => (
                Some(error.cause.as_str().to_owned()),
                error.io_kind,
                error.h2_reason,
            ),
            None => (None, None, None),
        };
        let (warning_type, warning_message) = match self.warning {
            Some((warning_type, message)) => (Some(warning_type.to_owned()), Some(message)),
            None => (None, None),
        };
        Some(cc_lb_lifecycle::UpstreamStreamDiagnostics {
            http_version: Some(format!("{:?}", self.http_version)),
            request_id: bounded_diagnostic_header(self.request_id.as_ref()),
            content_encoding: bounded_diagnostic_header(self.content_encoding.as_ref()),
            content_length: self.content_length,
            body_bytes,
            body_end: Some(body_end),
            body_error_cause,
            body_error_io_kind,
            body_error_h2_reason,
            message_stop_ms,
            warning_type,
            warning_message,
        })
    }
}

#[derive(Default)]
struct SseDownstreamBoundaryTracker {
    trailing: u32,
    trailing_len: u8,
    observed_bytes: bool,
    at_event_boundary: bool,
}

impl SseDownstreamBoundaryTracker {
    fn observe(&mut self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        if bytes.len() >= 4 {
            let tail = &bytes[bytes.len() - 4..];
            self.trailing = u32::from_be_bytes([tail[0], tail[1], tail[2], tail[3]]);
            self.trailing_len = 4;
        } else {
            for &byte in bytes {
                self.trailing = (self.trailing << 8) | u32::from(byte);
                self.trailing_len = self.trailing_len.saturating_add(1).min(4);
            }
        }
        self.observed_bytes = true;
        self.at_event_boundary = (self.trailing_len >= 2
            && matches!(self.trailing & 0xffff, 0x0a0a | 0x0d0d))
            || (self.trailing_len == 4 && self.trailing == 0x0d0a0d0a);
    }

    const fn partial_event_pending(&self) -> bool {
        self.observed_bytes && !self.at_event_boundary
    }
}

fn make_stream_error_frame(
    capability: ProviderCapability,
    error_type: &str,
    message: &str,
    separate_from_partial_event: bool,
) -> Bytes {
    let frame = make_provider_error_frame(
        capability,
        StatusCode::BAD_GATEWAY,
        error_type,
        message,
        None,
    );
    if !separate_from_partial_event {
        return frame;
    }

    let mut separated = BytesMut::with_capacity(2 + frame.len());
    separated.extend_from_slice(b"\n\n");
    separated.extend_from_slice(&frame);
    separated.freeze()
}

const fn decompression_output_budget_bytes(body_cap_bytes: usize) -> usize {
    body_cap_bytes.saturating_mul(DECOMPRESSION_OUTPUT_BUDGET_BODY_CAP_MULTIPLIER)
}

const fn incomplete_sse_event_budget_bytes(body_cap_bytes: usize) -> usize {
    body_cap_bytes.saturating_mul(INCOMPLETE_SSE_EVENT_BUDGET_BODY_CAP_MULTIPLIER)
}

fn split_sse_event(buffer: &mut BytesMut, end: usize) -> Bytes {
    let raw = buffer.split_to(end).freeze();
    let retained_capacity = buffer.capacity();
    if retained_capacity > SSE_BUFFER_MAX_RETAINED_CAPACITY_BYTES
        && raw.len() < retained_capacity / SSE_BUFFER_SMALL_EVENT_CAPACITY_FRACTION
    {
        *buffer = BytesMut::from(buffer.as_ref());
    }
    raw
}

fn semantic_body_bytes(body: &Bytes, plaintext: std::borrow::Cow<'_, [u8]>) -> Bytes {
    match plaintext {
        std::borrow::Cow::Owned(bytes) => Bytes::from(bytes),
        std::borrow::Cow::Borrowed(_) => body.clone(),
    }
}

/// Cap on the retained non-200 upstream error body: an admin-visible diagnostic
/// that can echo request text, so the byte cap bounds the blast radius.
const REQUEST_LOG_UPSTREAM_ERROR_BODY_MAX_BYTES: usize = 8 * 1024;
/// Bytes of an upstream error body considered for a stored sink. The margin
/// over the store cap lets a secret that straddles the cap be masked whole
/// before truncation, so no fragment of it survives the cut.
const REQUEST_LOG_UPSTREAM_ERROR_BODY_COLLECT_BYTES: usize =
    REQUEST_LOG_UPSTREAM_ERROR_BODY_MAX_BYTES + 512;
const REQUEST_LOG_UPSTREAM_ERROR_BODY_TRUNCATION_MARKER: &str = "\n...[truncated]";

fn append_bounded_upstream_error_body(buf: &mut Vec<u8>, chunk: &[u8], truncated: &mut bool) {
    if *truncated || chunk.is_empty() {
        return;
    }
    let remaining = REQUEST_LOG_UPSTREAM_ERROR_BODY_COLLECT_BYTES.saturating_sub(buf.len());
    if chunk.len() <= remaining {
        buf.extend_from_slice(chunk);
    } else {
        buf.extend_from_slice(&chunk[..remaining]);
        *truncated = true;
    }
}

/// Render bounded upstream error bytes as a lossy UTF-8 string, appending a
/// truncation marker when the body was capped. The result never exceeds the byte
/// cap and always ends on a UTF-8 char boundary.
fn bounded_lossy_upstream_error_body(bytes: &[u8], truncated: bool) -> String {
    let text = String::from_utf8_lossy(bytes);
    let marker = REQUEST_LOG_UPSTREAM_ERROR_BODY_TRUNCATION_MARKER;
    let needs_marker = truncated || text.len() > REQUEST_LOG_UPSTREAM_ERROR_BODY_MAX_BYTES;
    let max_text_len = if needs_marker {
        REQUEST_LOG_UPSTREAM_ERROR_BODY_MAX_BYTES.saturating_sub(marker.len())
    } else {
        REQUEST_LOG_UPSTREAM_ERROR_BODY_MAX_BYTES
    };

    let mut end = text.len().min(max_text_len);
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }

    let mut out = String::with_capacity(end + usize::from(needs_marker) * marker.len());
    out.push_str(&text[..end]);
    if needs_marker {
        out.push_str(marker);
    }
    out
}

/// Render an upstream error body for a stored sink. Only the first
/// `REQUEST_LOG_UPSTREAM_ERROR_BODY_COLLECT_BYTES` count; the signer masks its
/// secret in them before the store cap and truncation marker are applied.
fn scrubbed_upstream_error_body(signer: &dyn Signer, bytes: &[u8], truncated: bool) -> String {
    let collected = bytes
        .get(..REQUEST_LOG_UPSTREAM_ERROR_BODY_COLLECT_BYTES)
        .unwrap_or(bytes);
    let truncated = truncated || collected.len() < bytes.len();
    let text = String::from_utf8_lossy(collected);
    let scrubbed = signer.scrub_secrets(&text);
    bounded_lossy_upstream_error_body(scrubbed.as_bytes(), truncated)
}

/// Render the request-log `upstream_error_message` for an abnormal upstream
/// `stop_reason` (`refusal`, `model_context_window_exceeded`): `category=<..>;
/// <explanation>` when `stop_details` is present, else the bare stop reason.
/// Bounded with the same helper used for upstream error bodies.
fn abnormal_stop_error_message(stop: &usage_parser::AbnormalStop) -> String {
    let message = match (&stop.category, &stop.explanation) {
        (Some(category), Some(explanation)) => {
            format!("category={category}; {explanation}")
        }
        (Some(category), None) => format!("category={category}"),
        (None, Some(explanation)) => explanation.clone(),
        (None, None) => stop.stop_reason.clone(),
    };
    bounded_lossy_upstream_error_body(message.as_bytes(), false)
}

static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestKind {
    AnthropicMessages,
    OpenaiResponses,
    GeminiGenerateContent,
    GeminiCachedContentsCreate,
    GeminiCountTokens,
    AnthropicModels,
    AnthropicComplete,
    OpenaiResponsesInputTokens,
    OpenaiResponsesCompact,
    OpenaiChatCompletions,
    OpenaiCompletions,
    OpenaiEmbeddings,
    OpenaiModerations,
    OpenaiAudio,
    OpenaiImages,
    OpenaiModels,
    OpenaiCodexAuxiliary,
    GeminiEmbedContent,
    GeminiPredict,
    GeminiModels,
    GeminiOpenaiChatCompletions,
    GeminiOpenaiEmbeddings,
    GeminiOpenaiImages,
    GeminiOpenaiModels,
    OpenaiImageVariations,
}

impl RequestKind {
    fn matches_upstream(self, kind: StorageUpstreamKind) -> bool {
        use StorageUpstreamKind as K;
        if self.provider()
            != match kind {
                K::AnthropicApiKey | K::AnthropicOauth => cc_lb_domain::Provider::Anthropic,
                K::OpenaiApiKey | K::OpenaiOauth => cc_lb_domain::Provider::Openai,
                K::GeminiApiKey | K::GeminiOauth => cc_lb_domain::Provider::Gemini,
            }
        {
            return false;
        }
        match kind {
            K::AnthropicApiKey | K::GeminiApiKey => true,
            K::OpenaiApiKey => self != Self::OpenaiCodexAuxiliary,
            K::AnthropicOauth => matches!(self, Self::AnthropicMessages | Self::AnthropicModels),
            K::OpenaiOauth => matches!(
                self,
                Self::OpenaiResponses
                    | Self::OpenaiResponsesCompact
                    | Self::OpenaiImages
                    | Self::OpenaiModels
                    | Self::OpenaiCodexAuxiliary
            ),
            K::GeminiOauth => matches!(self, Self::GeminiGenerateContent | Self::GeminiCountTokens),
        }
    }

    fn capability(self) -> ProviderCapability {
        match self {
            Self::AnthropicMessages => ProviderCapability::AnthropicMessages,
            Self::OpenaiResponses => ProviderCapability::OpenaiResponses,
            Self::GeminiGenerateContent => ProviderCapability::GeminiGenerateContent,
            Self::GeminiCachedContentsCreate => ProviderCapability::GeminiCachedContentsCreate,
            Self::GeminiCountTokens => ProviderCapability::GeminiCountTokens,
            Self::AnthropicModels => ProviderCapability::AnthropicModels,
            Self::AnthropicComplete => ProviderCapability::AnthropicComplete,
            Self::OpenaiResponsesInputTokens => ProviderCapability::OpenaiResponsesInputTokens,
            Self::OpenaiResponsesCompact => ProviderCapability::OpenaiResponsesCompact,
            Self::OpenaiChatCompletions => ProviderCapability::OpenaiChatCompletions,
            Self::OpenaiCompletions => ProviderCapability::OpenaiCompletions,
            Self::OpenaiEmbeddings => ProviderCapability::OpenaiEmbeddings,
            Self::OpenaiModerations => ProviderCapability::OpenaiModerations,
            Self::OpenaiAudio => ProviderCapability::OpenaiAudio,
            Self::OpenaiImages => ProviderCapability::OpenaiImages,
            Self::OpenaiModels => ProviderCapability::OpenaiModels,
            Self::OpenaiCodexAuxiliary => ProviderCapability::OpenaiCodexAuxiliary,
            Self::GeminiEmbedContent => ProviderCapability::GeminiEmbedContent,
            Self::GeminiPredict => ProviderCapability::GeminiPredict,
            Self::GeminiModels => ProviderCapability::GeminiModels,
            Self::GeminiOpenaiChatCompletions => ProviderCapability::GeminiOpenaiChatCompletions,
            Self::GeminiOpenaiEmbeddings => ProviderCapability::GeminiOpenaiEmbeddings,
            Self::GeminiOpenaiImages => ProviderCapability::GeminiOpenaiImages,
            Self::GeminiOpenaiModels => ProviderCapability::GeminiOpenaiModels,
            Self::OpenaiImageVariations => ProviderCapability::OpenaiImages,
        }
    }

    fn provider(self) -> cc_lb_domain::Provider {
        self.capability().provider()
    }
}

impl From<ProviderCapability> for RequestKind {
    fn from(capability: ProviderCapability) -> Self {
        match capability {
            ProviderCapability::AnthropicMessages => Self::AnthropicMessages,
            ProviderCapability::AnthropicCountTokens => Self::AnthropicMessages,
            ProviderCapability::GeminiGenerateContent => Self::GeminiGenerateContent,
            ProviderCapability::GeminiCachedContentsCreate => Self::GeminiCachedContentsCreate,
            ProviderCapability::GeminiCountTokens => Self::GeminiCountTokens,
            ProviderCapability::AnthropicModels => Self::AnthropicModels,
            ProviderCapability::AnthropicComplete => Self::AnthropicComplete,
            ProviderCapability::OpenaiResponses => Self::OpenaiResponses,
            ProviderCapability::OpenaiResponsesInputTokens => Self::OpenaiResponsesInputTokens,
            ProviderCapability::OpenaiResponsesCompact => Self::OpenaiResponsesCompact,
            ProviderCapability::OpenaiChatCompletions => Self::OpenaiChatCompletions,
            ProviderCapability::OpenaiCompletions => Self::OpenaiCompletions,
            ProviderCapability::OpenaiEmbeddings => Self::OpenaiEmbeddings,
            ProviderCapability::OpenaiModerations => Self::OpenaiModerations,
            ProviderCapability::OpenaiAudio => Self::OpenaiAudio,
            ProviderCapability::OpenaiImages => Self::OpenaiImages,
            ProviderCapability::OpenaiModels => Self::OpenaiModels,
            ProviderCapability::OpenaiCodexAuxiliary => Self::OpenaiCodexAuxiliary,
            ProviderCapability::GeminiEmbedContent => Self::GeminiEmbedContent,
            ProviderCapability::GeminiPredict => Self::GeminiPredict,
            ProviderCapability::GeminiModels => Self::GeminiModels,
            ProviderCapability::GeminiOpenaiChatCompletions => Self::GeminiOpenaiChatCompletions,
            ProviderCapability::GeminiOpenaiEmbeddings => Self::GeminiOpenaiEmbeddings,
            ProviderCapability::GeminiOpenaiImages => Self::GeminiOpenaiImages,
            ProviderCapability::GeminiOpenaiModels => Self::GeminiOpenaiModels,
        }
    }
}

fn gemini_model_from_path(path: &str) -> Option<String> {
    let model_path = path
        .strip_prefix("/v1beta/models/")
        .or_else(|| path.strip_prefix("/v1/models/"))?;
    let (model, action) = model_path.rsplit_once(':')?;
    (matches!(
        action,
        "generateContent"
            | "streamGenerateContent"
            | "countTokens"
            | "embedContent"
            | "batchEmbedContents"
            | "predict"
    ) && !model.is_empty())
    .then(|| model.to_owned())
}

#[derive(Debug, Clone)]
pub struct PreviewRouteInput {
    pub principal_id: String,
    /// Request id exposed to routing filters through the preview `RequestContext`.
    /// When `None`, a random `preview-<uuid>` id is generated so repeated calls do
    /// not collide.
    pub request_id: Option<String>,
    pub headers: HeaderMap,
    pub body_bytes: Bytes,
    /// Provider scope selected by the admin caller.
    pub provider: Option<cc_lb_domain::Provider>,
    /// Provider-native path used to classify the request capability.
    pub path: String,
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
    #[error("operation not supported for preview: {method} {path}")]
    UnsupportedOperation { method: String, path: String },
}

#[derive(Clone, Debug)]
pub(crate) struct SelectedCacheMatch {
    pub(crate) prefix_hash: String,
    pub(crate) content_block_index: u32,
    pub(crate) ttl_class: TtlClass,
    pub(crate) estimated_prefix_bytes: PrefixBytes,
}

#[derive(Clone, Debug)]
struct ResolvedWarmMatch {
    entry: WarmCacheEntry,
    prefix_hash: String,
    matched_breakpoint_block_index: u32,
    content_block_index: u32,
    lookback_distance: u32,
    token_estimate_source: Option<String>,
}

struct StructuralCandidate<'a> {
    prefix_hash: &'a str,
    content_block_index: u32,
    memberships: Vec<(&'a CacheBreakpoint, &'a CacheLookbackPrefix)>,
}

/// Fetch warm prompt-cache observations for this request's candidate
/// upstreams and prefix keys from the shared observation store.
///
/// This is the only warmth authority: a single bounded batch read scoped to
/// the request's candidate keys. A read failure is reported and degrades to
/// an empty (neutral) observation set — never to a request failure.
#[cfg(test)]
async fn fetch_prompt_cache_warm_entries(
    view: &DynamicView,
    principal_id: &str,
    request_kind: RequestKind,
    canonical_model: &str,
    request_breakpoints: &[CacheBreakpoint],
    clock: &dyn Clock,
) -> HashMap<Uuid, Vec<WarmCacheEntry>> {
    fetch_prompt_cache_warm_entries_for_bundles(
        view,
        principal_id,
        request_kind,
        canonical_model,
        &PromptCacheBundles::single(request_breakpoints),
        clock,
    )
    .await
}

async fn fetch_prompt_cache_warm_entries_for_bundles(
    view: &DynamicView,
    principal_id: &str,
    request_kind: RequestKind,
    canonical_model: &str,
    bundles: &PromptCacheBundles,
    clock: &dyn Clock,
) -> HashMap<Uuid, Vec<WarmCacheEntry>> {
    let mut warm_entries: HashMap<Uuid, Vec<WarmCacheEntry>> = HashMap::new();
    let (Some(store), Some(allowed_upstreams)) = (
        view.prompt_cache_observation_store_opt(),
        view.principal_view.allowed_upstreams(principal_id),
    ) else {
        return warm_entries;
    };
    if canonical_model.is_empty()
        || bundles.official.cache_breakpoints.is_empty()
            && bundles.literal.cache_breakpoints.is_empty()
    {
        return warm_entries;
    }
    let upstream_ids: Vec<Uuid> = view
        .upstreams_snapshot()
        .iter()
        .filter(|upstream| upstream.enabled)
        .filter(|upstream| upstream.deleted_at_unix_secs.is_none())
        .filter(|upstream| request_kind.matches_upstream(upstream.kind))
        .filter(|upstream| allowed_upstreams.is_empty() || allowed_upstreams.contains(&upstream.id))
        .map(|upstream| upstream.id)
        .collect();
    if upstream_ids.is_empty() {
        return warm_entries;
    }
    let mut prefix_keys: Vec<String> = Vec::new();
    let mut seen_prefix_keys: HashSet<&str> = HashSet::new();
    for bundle in [&bundles.official, &bundles.literal] {
        for breakpoint in &bundle.cache_breakpoints {
            for prefix in &breakpoint.lookback_prefixes {
                if seen_prefix_keys.insert(prefix.prefix_hash.as_str()) {
                    prefix_keys.push(prefix.prefix_hash.clone());
                }
            }
        }
    }
    if prefix_keys.is_empty() {
        return warm_entries;
    }
    let now_unix_secs = unix_secs(clock.now());
    let lookup_span = tracing::info_span!(
        "proxy.prompt_cache_observation_lookup",
        otel.status_code = tracing::field::Empty,
    );
    let records = store
        .list_active_for_candidates(&upstream_ids, canonical_model, &prefix_keys, now_unix_secs)
        .instrument(lookup_span.clone())
        .await;
    match records {
        Ok(records) => {
            // A successful empty lookup is known-cold; a failed lookup is
            // unknown. Preserve that distinction for routing cost estimates.
            for upstream_id in upstream_ids {
                warm_entries.insert(upstream_id, Vec::new());
            }
            for record in records {
                // The store bounds by candidate keys and expiry; the schema
                // version guard stays here so a backend cannot warm-route a
                // request on keys derived under a different hash schema.
                if record.hash_schema_version != HASH_SCHEMA_VERSION {
                    continue;
                }
                // A size measured by another estimator is in a different
                // unit; such a row is known-cold, not an error.
                let Some(estimated_prefix_bytes) = PrefixBytes::from_persisted(
                    record.estimated_prefix_tokens,
                    &record.token_estimate_source,
                ) else {
                    continue;
                };
                warm_entries
                    .entry(record.upstream_id)
                    .or_default()
                    .push(WarmCacheEntry {
                        prefix_hash: record.v3_prefix_key,
                        expires_at_unix_secs: record.expires_at_unix_secs,
                        ttl_class: record.ttl_class,
                        last_observed_at_unix_secs: record.last_observed_at_unix_secs,
                        content_block_index: record.prefix_content_block_index,
                        estimated_prefix_bytes,
                        hash_schema_version: record.hash_schema_version,
                    });
            }
        }
        Err(error) => {
            lookup_span.record("otel.status_code", "ERROR");
            cc_lb_observability::inc_cache_observation_read_failed();
            tracing::warn!(
                %error,
                "prompt-cache observation lookup failed; routing without cache affinity"
            );
        }
    }
    warm_entries
}
/// Fetch provider-native cache observations for a request's eligible upstreams.
///
/// The storage API is keyed by one upstream at a time, so this performs one
/// bounded lookup per eligible candidate, before `route_guard` is entered.
/// Invalid request metadata and lookup failures both produce a neutral result.
async fn fetch_provider_native_cache_scores(
    view: &DynamicView,
    principal_id: &str,
    request_kind: RequestKind,
    canonical_model: &str,
    native_cache: Option<&cc_lb_request_log::RequestNativeCacheObservation>,
    clock: &dyn Clock,
) -> HashMap<Uuid, CacheScore> {
    let Some((provider, mode, scope, identity_hash, retention_secs)) =
        provider_native_lookup_params(native_cache, canonical_model, request_kind)
    else {
        return HashMap::new();
    };
    let Some(allowed_upstreams) = view.principal_view.allowed_upstreams(principal_id) else {
        return HashMap::new();
    };
    let Some(store) = view.prompt_cache_observation_store_opt() else {
        return HashMap::new();
    };
    let now_unix_secs = unix_secs(clock.now());
    let upstream_ids: Vec<Uuid> = view
        .upstreams_snapshot()
        .iter()
        .filter(|upstream| upstream.enabled)
        .filter(|upstream| upstream.deleted_at_unix_secs.is_none())
        .filter(|upstream| request_kind.matches_upstream(upstream.kind))
        .filter(|upstream| allowed_upstreams.is_empty() || allowed_upstreams.contains(&upstream.id))
        .filter(|upstream| upstream_credentials_routable(view, upstream))
        .map(|upstream| upstream.id)
        .collect();

    let lookup_span = tracing::info_span!(
        "proxy.provider_prompt_cache_observation_lookup",
        otel.status_code = tracing::field::Empty,
    );
    let records = store
        .list_active_provider_observations_for_candidates(
            &upstream_ids,
            provider,
            canonical_model,
            mode,
            scope,
            identity_hash,
            Some(retention_secs),
            now_unix_secs,
        )
        .instrument(lookup_span.clone())
        .await;
    let records = match records {
        Ok(records) => records,
        Err(error) => {
            lookup_span.record("otel.status_code", "ERROR");
            cc_lb_observability::inc_cache_observation_read_failed();
            tracing::warn!(
                %error,
                "provider prompt-cache observation lookup failed; routing without native cache preference"
            );
            return HashMap::new();
        }
    };
    let mut scores: HashMap<Uuid, CacheScore> = HashMap::new();
    for record in records {
        if !upstream_ids.contains(&record.upstream_id)
            || record.provider != provider
            || record.canonical_model_id != canonical_model
            || record.mode != mode
            || record.isolation_scope != scope
            || record.identity_hash != identity_hash
            || record.retention_secs != Some(retention_secs)
            || !record
                .expires_at_unix_secs
                .is_some_and(|expires| expires > now_unix_secs)
            || !record
                .observed_cached_tokens
                .is_some_and(|tokens| tokens > 0)
        {
            continue;
        }
        scores
            .entry(record.upstream_id)
            .and_modify(|score| {
                if record.observed_cached_tokens.unwrap_or_default()
                    > score.predicted_cache_read_tokens.into()
                {
                    *score = native_cache_score_from_record(&record);
                }
            })
            .or_insert_with(|| native_cache_score_from_record(&record));
    }
    scores
}

fn native_cache_scope(scope: Option<&str>) -> Option<&str> {
    let scope = scope?;
    let (organization, region) = scope.strip_prefix("org=")?.split_once(";region=")?;
    (!organization.is_empty()
        && !organization.contains(';')
        && !region.is_empty()
        && !region.contains(';'))
    .then_some(scope)
}
fn provider_native_lookup_params<'a>(
    native_cache: Option<&'a cc_lb_request_log::RequestNativeCacheObservation>,
    canonical_model: &str,
    request_kind: RequestKind,
) -> Option<(&'a str, &'a str, &'a str, &'a str, u64)> {
    let native = native_cache?;
    if native.provider == "openai" && provider_cache::supports_modern_openai_cache(canonical_model)
    {
        return None;
    }
    let scope = native_cache_scope(native.scope.as_deref())?;
    let identity_hash = native
        .identity_hash
        .as_deref()
        .filter(|identity| is_provider_native_identity(identity))?;
    let retention_secs = native.retention_secs?;
    (native.provider == "openai"
        && !native.mode.is_empty()
        && !canonical_model.is_empty()
        && (request_kind.matches_upstream(StorageUpstreamKind::OpenaiApiKey)
            || request_kind.matches_upstream(StorageUpstreamKind::OpenaiOauth)))
    .then_some((
        native.provider.as_str(),
        native.mode.as_str(),
        scope,
        identity_hash,
        retention_secs,
    ))
}

fn is_provider_native_identity(identity_hash: &str) -> bool {
    let Some(digest) = identity_hash.strip_prefix("sha256:") else {
        return false;
    };
    digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn native_cache_score_from_record(record: &ProviderPromptCacheObservationRecord) -> CacheScore {
    CacheScore {
        predicted_cache_read_tokens: record
            .observed_cached_tokens
            .unwrap_or_default()
            .min(u64::from(u32::MAX)) as u32,
        predicted_cache_creation_tokens_5m: 0,
        predicted_cache_creation_tokens_1h: 0,
        predicted_uncached_input_tokens: 0,
        predicted_expires_at_unix_secs: record.expires_at_unix_secs,
        matched_breakpoint_index: None,
        confidence: 1.0,
        ambiguity_reason: Some("provider_native_observation".to_owned()),
        matched_v3_cache_key: None,
        breakpoint_content_block_index: None,
        matched_content_block_index: None,
        lookback_distance: None,
        token_estimate_source: None,
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_candidates_with_native_cache_for_bundles(
    view: &DynamicView,
    principal_id: &str,
    request_kind: RequestKind,
    request_breakpoints: &[CacheBreakpoint],
    warm_entries: &HashMap<Uuid, Vec<WarmCacheEntry>>,
    bundles: Option<&PromptCacheBundles>,
    native_cache_scores: &HashMap<Uuid, CacheScore>,
    clock: &dyn Clock,
) -> (Vec<UpstreamCandidate>, HashMap<Uuid, SelectedCacheMatch>) {
    build_candidates_with_matches_for_bundles_and_native(
        view,
        principal_id,
        request_kind,
        request_breakpoints,
        warm_entries,
        bundles,
        Some(native_cache_scores),
        clock,
    )
}

pub fn build_candidates(
    view: &DynamicView,
    principal_id: &str,
    request_kind: RequestKind,
    request_breakpoints: &[CacheBreakpoint],
    warm_entries: &HashMap<Uuid, Vec<WarmCacheEntry>>,
    clock: &dyn Clock,
) -> Vec<UpstreamCandidate> {
    build_candidates_with_matches_for_bundles(
        view,
        principal_id,
        request_kind,
        request_breakpoints,
        warm_entries,
        None,
        clock,
    )
    .0
}

pub(crate) fn build_candidates_with_matches_for_bundles(
    view: &DynamicView,
    principal_id: &str,
    request_kind: RequestKind,
    request_breakpoints: &[CacheBreakpoint],
    warm_entries: &HashMap<Uuid, Vec<WarmCacheEntry>>,
    bundles: Option<&PromptCacheBundles>,
    clock: &dyn Clock,
) -> (Vec<UpstreamCandidate>, HashMap<Uuid, SelectedCacheMatch>) {
    build_candidates_with_matches_for_bundles_and_native(
        view,
        principal_id,
        request_kind,
        request_breakpoints,
        warm_entries,
        bundles,
        None,
        clock,
    )
}

#[allow(clippy::too_many_arguments)]
fn build_candidates_with_matches_for_bundles_and_native(
    view: &DynamicView,
    principal_id: &str,
    request_kind: RequestKind,
    request_breakpoints: &[CacheBreakpoint],
    warm_entries: &HashMap<Uuid, Vec<WarmCacheEntry>>,
    bundles: Option<&PromptCacheBundles>,
    native_cache_scores: Option<&HashMap<Uuid, CacheScore>>,
    clock: &dyn Clock,
) -> (Vec<UpstreamCandidate>, HashMap<Uuid, SelectedCacheMatch>) {
    let Some(allowed_upstreams) = view.principal_view.allowed_upstreams(principal_id) else {
        return (Vec::new(), HashMap::new());
    };

    let mut candidates: Vec<UpstreamCandidate> = Vec::new();
    let mut selected_matches: HashMap<Uuid, SelectedCacheMatch> = HashMap::new();
    {
        let rate_limit_cache = view.upstream_rate_limit_cache.read();
        let now_unix_millis = unix_now_ms(clock);
        let now_unix_secs = unix_secs(clock.now());
        for upstream in view
            .upstreams_snapshot()
            .iter()
            .filter(|upstream| upstream.enabled)
            .filter(|upstream| upstream.deleted_at_unix_secs.is_none())
            .filter(|upstream| request_kind.matches_upstream(upstream.kind))
            .filter(|upstream| {
                allowed_upstreams.is_empty() || allowed_upstreams.contains(&upstream.id)
            })
            .filter(|upstream| upstream_credentials_routable(view, upstream))
        {
            let has_rate_limit_snapshot = rate_limit_cache
                .snapshots
                .get(&upstream.id)
                .is_some_and(|snapshots| !snapshots.is_empty());
            let observed_at_unix_secs = if has_rate_limit_snapshot {
                rate_limit_cache.updated_at_unix_secs
            } else {
                0
            };
            let bundle_breakpoints = bundles
                .map(|bundles| bundles.for_upstream(upstream).cache_breakpoints.as_slice())
                .unwrap_or(request_breakpoints);
            let cache_score = native_cache_scores
                .and_then(|scores| scores.get(&upstream.id).cloned())
                .or_else(|| {
                    warm_entries.get(&upstream.id).and_then(|entries| {
                        let matched =
                            resolve_longest_warm_match(entries, bundle_breakpoints, now_unix_secs);
                        let score =
                            build_cache_score_from_match(bundle_breakpoints, matched.as_ref());
                        if score.is_some()
                            && let Some(matched) = matched.as_ref()
                        {
                            selected_matches.insert(
                                upstream.id,
                                SelectedCacheMatch {
                                    prefix_hash: matched.prefix_hash.clone(),
                                    content_block_index: matched.content_block_index,
                                    ttl_class: matched.entry.ttl_class,
                                    estimated_prefix_bytes: matched.entry.estimated_prefix_bytes,
                                },
                            );
                        }
                        score
                    })
                });

            let plan_info = view.plan_info_by_upstream.get(&upstream.id);
            candidates.push(UpstreamCandidate {
                upstream_id: upstream.id,
                name: upstream.name.clone(),
                kind: upstream_kind_for_candidate(upstream.kind),
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
            });
        }
    }
    candidates.sort_unstable_by_key(|c| c.upstream_id);
    (candidates, selected_matches)
}

/// Excludes upstreams whose credentials are known dead: OAuth accounts that
/// need an operator reconnect and any upstream whose last apply failed.
fn upstream_credentials_routable(view: &DynamicView, upstream: &UpstreamRecord) -> bool {
    if matches!(
        upstream.kind,
        StorageUpstreamKind::AnthropicOauth
            | StorageUpstreamKind::OpenaiOauth
            | StorageUpstreamKind::GeminiOauth
    ) && cc_lb_oauth_protocol::refresh_requires_reconnect(upstream.last_apply_error.as_deref())
    {
        return false;
    }
    !view
        .upstream_status_snapshot
        .entries
        .get(&upstream.name)
        .is_some_and(|entry| entry.status == cc_lb_control::ApplyStatus::Error)
}

#[cfg(test)]
fn build_cache_score(
    request_breakpoints: &[CacheBreakpoint],
    warm_entries: &[WarmCacheEntry],
) -> Option<CacheScore> {
    let matched = resolve_warm_match(request_breakpoints, |prefix_hash, eligible_ttls| {
        warm_entries
            .iter()
            .filter(|entry| entry.prefix_hash == prefix_hash)
            .filter(|entry| eligible_ttls.contains(&entry.ttl_class))
            .max_by_key(|entry| entry.expires_at_unix_secs)
            .cloned()
    });
    build_cache_score_from_match(request_breakpoints, matched.as_ref())
}

fn build_cache_score_from_match(
    request_breakpoints: &[CacheBreakpoint],
    matched: Option<&ResolvedWarmMatch>,
) -> Option<CacheScore> {
    if request_breakpoints.is_empty() {
        return None;
    }

    if !valid_ttl_order(request_breakpoints) {
        return None;
    }

    let matched_prefix_bytes = matched.map_or(PrefixBytes::default(), |matched| {
        matched.entry.estimated_prefix_bytes
    });
    let anchor = matched.map(|matched| (matched.content_block_index, matched.entry.ttl_class));

    let deepest_1h = deepest_breakpoint(request_breakpoints, TtlClass::Ephemeral1h, anchor);
    let deepest_5m = deepest_breakpoint(request_breakpoints, TtlClass::Ephemeral5m, anchor);

    let mut non_monotonic = false;
    let one_hour_baseline = matched
        .filter(|matched| matched.entry.ttl_class == TtlClass::Ephemeral1h)
        .map_or(PrefixBytes::default(), |matched| {
            matched.entry.estimated_prefix_bytes
        });
    let predicted_cache_creation_bytes_1h =
        deepest_1h.map_or(PrefixBytes::default(), |breakpoint| {
            non_monotonic |= breakpoint.prefix_bytes < one_hour_baseline;
            breakpoint.prefix_bytes.saturating_sub(one_hour_baseline)
        });
    let boundary = deepest_1h.map_or(matched_prefix_bytes, |breakpoint| breakpoint.prefix_bytes);
    let predicted_cache_creation_bytes_5m =
        deepest_5m.map_or(PrefixBytes::default(), |breakpoint| {
            non_monotonic |= breakpoint.prefix_bytes < boundary;
            breakpoint.prefix_bytes.saturating_sub(boundary)
        });
    let ambiguity_reason = non_monotonic.then(|| "non_monotonic_token_estimate".to_owned());

    Some(CacheScore {
        predicted_cache_read_tokens: token_estimate_u32(matched_prefix_bytes),
        predicted_cache_creation_tokens_5m: token_estimate_u32(predicted_cache_creation_bytes_5m),
        predicted_cache_creation_tokens_1h: token_estimate_u32(predicted_cache_creation_bytes_1h),
        predicted_uncached_input_tokens: 0,
        predicted_expires_at_unix_secs: matched.map(|matched| matched.entry.expires_at_unix_secs),
        matched_breakpoint_index: matched.map(|matched| matched.matched_breakpoint_block_index),
        confidence: if matched.is_some() { 1.0 } else { 0.0 },
        ambiguity_reason,
        matched_v3_cache_key: matched.map(|matched| matched.prefix_hash.clone()),
        breakpoint_content_block_index: matched
            .map(|matched| matched.matched_breakpoint_block_index),
        matched_content_block_index: matched.map(|matched| matched.content_block_index),
        lookback_distance: matched.map(|matched| matched.lookback_distance),
        token_estimate_source: matched.and_then(|matched| matched.token_estimate_source.clone()),
    })
}

fn valid_ttl_order(breakpoints: &[CacheBreakpoint]) -> bool {
    let deepest_1h = breakpoints
        .iter()
        .filter(|breakpoint| breakpoint.requested_ttl == TtlClass::Ephemeral1h)
        .map(|breakpoint| breakpoint.block_index)
        .max();
    let shallowest_5m = breakpoints
        .iter()
        .filter(|breakpoint| breakpoint.requested_ttl == TtlClass::Ephemeral5m)
        .map(|breakpoint| breakpoint.block_index)
        .min();
    match (deepest_1h, shallowest_5m) {
        (Some(latest_1h), Some(earliest_5m)) => latest_1h < earliest_5m,
        _ => true,
    }
}

fn deepest_breakpoint(
    breakpoints: &[CacheBreakpoint],
    ttl: TtlClass,
    anchor: Option<(u32, TtlClass)>,
) -> Option<&CacheBreakpoint> {
    breakpoints
        .iter()
        .filter(|breakpoint| breakpoint.requested_ttl == ttl)
        .filter(|breakpoint| breakpoint_needs_write(breakpoint, anchor))
        .max_by_key(|breakpoint| breakpoint.block_index)
}

fn breakpoint_needs_write(breakpoint: &CacheBreakpoint, anchor: Option<(u32, TtlClass)>) -> bool {
    let Some((matched_index, matched_ttl)) = anchor else {
        return true;
    };
    match breakpoint.block_index.cmp(&matched_index) {
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Equal => {
            !eligible_ttls(breakpoint.requested_ttl).contains(&matched_ttl)
        }
    }
}

fn eligible_ttls(requested: TtlClass) -> Vec<TtlClass> {
    match requested {
        TtlClass::Ephemeral5m => vec![TtlClass::Ephemeral5m, TtlClass::Ephemeral1h],
        TtlClass::Ephemeral1h => vec![TtlClass::Ephemeral1h],
    }
}

fn resolve_longest_warm_match(
    warm_entries: &[WarmCacheEntry],
    request_breakpoints: &[CacheBreakpoint],
    now_unix_secs: u64,
) -> Option<ResolvedWarmMatch> {
    resolve_warm_match(request_breakpoints, |prefix_hash, eligible_ttls| {
        warm_entries
            .iter()
            .filter(|entry| entry.prefix_hash == prefix_hash)
            .filter(|entry| eligible_ttls.contains(&entry.ttl_class))
            // Selection-time expiry re-check: a record committed between the
            // batch read and this decision must not warm-route once expired.
            .filter(|entry| entry.expires_at_unix_secs > now_unix_secs)
            .max_by_key(|entry| entry.expires_at_unix_secs)
            .cloned()
    })
}

fn resolve_warm_match(
    request_breakpoints: &[CacheBreakpoint],
    mut lookup: impl FnMut(&str, &[TtlClass]) -> Option<WarmCacheEntry>,
) -> Option<ResolvedWarmMatch> {
    let mut candidates: Vec<StructuralCandidate<'_>> = Vec::new();
    for breakpoint in request_breakpoints {
        for prefix in &breakpoint.lookback_prefixes {
            if let Some(candidate) = candidates.iter_mut().find(|candidate| {
                candidate.prefix_hash == prefix.prefix_hash
                    && candidate.content_block_index == prefix.content_block_index
            }) {
                candidate.memberships.push((breakpoint, prefix));
            } else {
                candidates.push(StructuralCandidate {
                    prefix_hash: &prefix.prefix_hash,
                    content_block_index: prefix.content_block_index,
                    memberships: vec![(breakpoint, prefix)],
                });
            }
        }
    }
    candidates.sort_by(|left, right| {
        right
            .content_block_index
            .cmp(&left.content_block_index)
            .then_with(|| left.prefix_hash.cmp(right.prefix_hash))
    });
    for candidate in candidates {
        let eligible_ttls = if candidate
            .memberships
            .iter()
            .any(|(breakpoint, _)| breakpoint.requested_ttl == TtlClass::Ephemeral5m)
        {
            vec![TtlClass::Ephemeral5m, TtlClass::Ephemeral1h]
        } else {
            vec![TtlClass::Ephemeral1h]
        };
        let Some(entry) = lookup(candidate.prefix_hash, &eligible_ttls) else {
            continue;
        };
        let (breakpoint, prefix) = candidate
            .memberships
            .into_iter()
            .filter(|(breakpoint, _)| {
                eligible_ttls_for_entry(breakpoint.requested_ttl, entry.ttl_class)
            })
            .min_by_key(|(breakpoint, prefix)| {
                (prefix.lookback_distance, breakpoint.block_index)
            })?;
        return Some(ResolvedWarmMatch {
            entry,
            prefix_hash: candidate.prefix_hash.to_owned(),
            matched_breakpoint_block_index: breakpoint.block_index,
            content_block_index: candidate.content_block_index,
            lookback_distance: prefix.lookback_distance,
            token_estimate_source: breakpoint.token_estimate_source.clone(),
        });
    }
    None
}

fn eligible_ttls_for_entry(requested: TtlClass, entry: TtlClass) -> bool {
    eligible_ttls(requested).contains(&entry)
}

#[cfg(test)]
mod cache_score_tests {
    use std::collections::BTreeMap;

    use super::{
        HASH_SCHEMA_VERSION, anthropic_family_cache_pricing_summary,
        anthropic_family_input_micros_per_million, build_cache_score,
        cache_pricing_summary_for_model,
    };
    use cc_lb_domain::{
        CacheBreakpoint, CacheBreakpointSource, CacheLookbackPrefix, PrefixBytes, TtlClass,
        WarmCacheEntry,
    };
    use cc_lb_pricing::{
        CatalogSnapshot, CatalogStatus, NativePricingLifetime, NativePricingProvider,
        NativePricingRate, PriceCatalog, Pricing, ProviderNativePricing, TierRate, UsdPerMillion,
    };
    use proptest::prelude::*;

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
            &[warm_entry("bp-1", TtlClass::Ephemeral5m, 300_000)],
        )
        .expect("cache score for cacheable request");

        assert_eq!(score.predicted_cache_read_tokens, 300_000);
        assert_eq!(score.predicted_cache_creation_tokens_1h, 0);
        assert_eq!(score.predicted_cache_creation_tokens_5m, 200_000);
        assert_eq!(score.matched_breakpoint_index, Some(1));
    }

    #[test]
    fn build_cache_score_prices_read_from_matched_prefix_not_owning_breakpoint() {
        let breakpoint = CacheBreakpoint {
            block_index: 2,
            source: CacheBreakpointSource::Message,
            path: "messages.2.content".to_owned(),
            message_index: Some(2),
            prefix_hash: "bp-2".to_owned(),
            prefix_bytes: PrefixBytes::new(300_000),
            requested_ttl: TtlClass::Ephemeral5m,
            lookback_prefixes: vec![
                cc_lb_domain::CacheLookbackPrefix {
                    prefix_hash: "bp-2".to_owned(),
                    content_block_index: 2,
                    lookback_distance: 0,
                },
                cc_lb_domain::CacheLookbackPrefix {
                    prefix_hash: "warm-0".to_owned(),
                    content_block_index: 0,
                    lookback_distance: 2,
                },
            ],
            token_estimate_source: Some("test".to_owned()),
        };
        let score = build_cache_score(
            std::slice::from_ref(&breakpoint),
            &[warm_entry("warm-0", TtlClass::Ephemeral5m, 100_000)],
        )
        .expect("cache score for cacheable request");

        assert_eq!(score.predicted_cache_read_tokens, 100_000);
        assert_eq!(score.predicted_cache_creation_tokens_5m, 200_000);
        assert_eq!(score.matched_content_block_index, Some(0));
    }

    #[test]
    fn anthropic_family_cache_pricing_fallback_covers_opus_4_8_alias() {
        let pricing = anthropic_family_cache_pricing_summary("claude-opus-4-8")
            .expect("opus family fallback exists");

        assert_eq!(pricing.status, "known");
        assert_eq!(pricing.input_micros_per_million, Some(5_000_000));
        assert_eq!(
            pricing.cache_creation_5m_micros_per_million,
            Some(6_250_000)
        );
        assert_eq!(
            pricing.cache_creation_1h_micros_per_million,
            Some(10_000_000)
        );
        assert_eq!(pricing.cache_read_micros_per_million, Some(500_000));
    }

    #[test]
    fn anthropic_family_input_pricing_splits_opus_generations() {
        // A blanket `claude-opus => $15` overpriced every Opus from 4.5 on by 3x. Bare
        // `claude-opus-4` must not be shadowed by the `claude-opus-4-N` arms that precede it.
        for current in [
            "claude-opus-4-5",
            "claude-opus-4-5-20251101",
            "claude-opus-4-6",
            "claude-opus-4-7",
            "claude-opus-4-8",
            "claude-opus-4-8-20250514",
            "claude-opus-5",
        ] {
            assert_eq!(
                anthropic_family_input_micros_per_million(current),
                Some(5_000_000),
                "{current} is current-generation Opus pricing"
            );
        }
        for legacy in [
            "claude-opus-4",
            "claude-opus-4-1",
            "claude-opus-4-1-20250805",
        ] {
            assert_eq!(
                anthropic_family_input_micros_per_million(legacy),
                Some(15_000_000),
                "{legacy} is legacy Opus pricing"
            );
        }
    }

    #[test]
    fn anthropic_family_input_pricing_covers_haiku_and_sonnet_generations() {
        assert_eq!(
            anthropic_family_input_micros_per_million("claude-3-5-haiku-20241022"),
            Some(800_000),
            "Haiku 3.5 uses the pre-4.0 id ordering and is cheaper than Haiku 4.5"
        );
        assert_eq!(
            anthropic_family_input_micros_per_million("claude-haiku-4-5-20251001"),
            Some(1_000_000)
        );
        assert_eq!(
            anthropic_family_input_micros_per_million("claude-sonnet-5"),
            Some(3_000_000),
            "Sonnet 5 uses standard Sonnet rates after 2026-08-31"
        );
        assert_eq!(
            anthropic_family_input_micros_per_million("claude-sonnet-4-6"),
            Some(3_000_000)
        );
    }

    #[test]
    fn anthropic_family_cache_pricing_fallback_covers_fable_5() {
        let pricing = anthropic_family_cache_pricing_summary("claude-fable-5")
            .expect("fable family fallback exists");

        assert_eq!(pricing.status, "known");
        assert_eq!(pricing.input_micros_per_million, Some(10_000_000));
        assert_eq!(
            pricing.cache_creation_5m_micros_per_million,
            Some(12_500_000)
        );
        assert_eq!(
            pricing.cache_creation_1h_micros_per_million,
            Some(20_000_000)
        );
        assert_eq!(pricing.cache_read_micros_per_million, Some(1_000_000));
    }

    #[test]
    fn anthropic_family_cache_pricing_fallback_covers_mythos_5() {
        let pricing = anthropic_family_cache_pricing_summary("claude-mythos-5")
            .expect("mythos family fallback exists");

        assert_eq!(pricing.status, "known");
        assert_eq!(pricing.input_micros_per_million, Some(10_000_000));
        assert_eq!(
            pricing.cache_creation_5m_micros_per_million,
            Some(12_500_000)
        );
        assert_eq!(
            pricing.cache_creation_1h_micros_per_million,
            Some(20_000_000)
        );
        assert_eq!(pricing.cache_read_micros_per_million, Some(1_000_000));
    }

    #[test]
    fn anthropic_family_cache_pricing_fallback_leaves_non_claude_unknown() {
        assert!(anthropic_family_cache_pricing_summary("gpt-5.5").is_none());
    }

    #[test]
    fn cache_pricing_summary_uses_requested_service_tier() {
        // Given a catalog whose priority tier differs from every base cache rate.
        let catalog = PriceCatalog::new_empty();
        let mut snapshot = CatalogSnapshot::empty_cost_disabled();
        snapshot.status = CatalogStatus::Ok;
        snapshot.models.insert(
            "tier-model".to_owned(),
            Pricing {
                native: None,
                model: "tier-model".to_owned(),
                input_per_million_usd: UsdPerMillion::from_whole_usd(2),
                output_per_million_usd: UsdPerMillion::from_whole_usd(8),
                by_tier: BTreeMap::from([(
                    "priority".to_owned(),
                    TierRate {
                        input_per_million_usd: UsdPerMillion::from_whole_usd(3),
                        output_per_million_usd: UsdPerMillion::from_whole_usd(12),
                    },
                )]),
                input_image_per_million_usd: None,
                output_image_per_million_usd: None,
            },
        );
        snapshot
            .cache_creation_per_million_usd
            .insert("tier-model".to_owned(), UsdPerMillion::from_whole_usd(4));
        snapshot.cache_creation_per_million_usd_by_tier.insert(
            "tier-model".to_owned(),
            BTreeMap::from([("priority".to_owned(), UsdPerMillion::from_whole_usd(6))]),
        );
        snapshot
            .cache_read_per_million_usd
            .insert("tier-model".to_owned(), UsdPerMillion::from_whole_usd(1));
        snapshot.cache_read_per_million_usd_by_tier.insert(
            "tier-model".to_owned(),
            BTreeMap::from([("priority".to_owned(), UsdPerMillion::from_whole_usd(2))]),
        );
        catalog.install_snapshot(snapshot);

        // When the routing summary is built with the requested priority tier.
        let pricing = cache_pricing_summary_for_model(&catalog, "tier-model", Some("priority"));

        // Then all cache estimate rates reflect that tier: the 1h write derives from the tier's
        // own base input price (2.0x), not from the tier 5m price.
        assert_eq!(pricing.input_micros_per_million, Some(3_000_000));
        assert_eq!(
            pricing.cache_creation_5m_micros_per_million,
            Some(6_000_000)
        );
        assert_eq!(
            pricing.cache_creation_1h_micros_per_million,
            Some(6_000_000)
        );
        assert_eq!(pricing.cache_read_micros_per_million, Some(2_000_000));
        for alias in ["service_tier", "serviceTier"] {
            let request = serde_json::json!({(alias): "priority"});
            let metadata = super::request_cache_metadata_from_value_with_analysis_for_capability(
                &http::HeaderMap::new(),
                Some(&request),
                None,
                Some(super::ProviderCapability::GeminiGenerateContent),
            );
            let gemini_pricing = cache_pricing_summary_for_model(
                &catalog,
                "tier-model",
                metadata.requested_service_tier.as_deref(),
            );
            assert_eq!(gemini_pricing.input_micros_per_million, Some(3_000_000));
            assert_eq!(
                gemini_pricing.cache_read_micros_per_million,
                Some(2_000_000)
            );
        }
    }
    #[test]
    fn native_cache_pricing_context_maps_rates_without_legacy_fallback() {
        let catalog = PriceCatalog::new_empty();
        let mut snapshot = CatalogSnapshot::empty_cost_disabled();
        snapshot.status = CatalogStatus::Ok;
        snapshot.models.insert(
            "gpt-native".to_owned(),
            Pricing {
                native: Some(ProviderNativePricing {
                    provider: NativePricingProvider::OpenAi,
                    ordinary_input: NativePricingRate::known(UsdPerMillion::from_whole_usd(2)),
                    cached_input_read: NativePricingRate::known(UsdPerMillion::from_micros_usd(
                        100_000,
                    )),
                    cached_input_read_by_tier: BTreeMap::new(),
                    cached_input_read_above_tokens: BTreeMap::new(),
                    total_write: BTreeMap::from([(
                        NativePricingLifetime::ThirtyMinutes,
                        NativePricingRate::known(UsdPerMillion::from_whole_usd(3)),
                    )]),
                    explicit_resource: Default::default(),
                }),
                model: "gpt-native".to_owned(),
                input_per_million_usd: UsdPerMillion::from_whole_usd(2),
                output_per_million_usd: UsdPerMillion::from_whole_usd(8),
                input_image_per_million_usd: None,
                output_image_per_million_usd: None,
                by_tier: BTreeMap::new(),
            },
        );
        snapshot
            .cache_creation_per_million_usd
            .insert("gpt-native".to_owned(), UsdPerMillion::from_whole_usd(3));
        snapshot
            .cache_read_per_million_usd
            .insert("gpt-native".to_owned(), UsdPerMillion::from_whole_usd(1));
        catalog.install_snapshot(snapshot);

        let pricing = cache_pricing_summary_for_model(&catalog, "gpt-native", None);

        assert_eq!(pricing.status, "unknown");
        assert_eq!(pricing.input_micros_per_million, None);
        assert_eq!(pricing.cache_read_micros_per_million, None);
        assert_eq!(pricing.cache_creation_5m_micros_per_million, None);
        assert_eq!(pricing.cache_creation_1h_micros_per_million, None);
        let native = pricing.native.expect("native pricing context");
        assert_eq!(native.version, 1);
        assert_eq!(
            native.provider,
            cc_lb_domain::NativeCachePricingProvider::OpenAi
        );
        let ordinary = native
            .rates
            .iter()
            .find(|rate| rate.component == cc_lb_domain::NativeCachePricingComponent::OrdinaryInput)
            .expect("ordinary input rate");
        assert_eq!(
            ordinary.status,
            cc_lb_domain::NativeCachePricingStatus::Known
        );
        assert_eq!(ordinary.micros_per_million, Some(2_000_000));
        let total_write = native
            .rates
            .iter()
            .find(|rate| {
                rate.component == cc_lb_domain::NativeCachePricingComponent::TotalWrite
                    && rate.lifetime
                        == Some(cc_lb_domain::NativeCachePricingLifetime::ThirtyMinutes)
            })
            .expect("total write rate");
        assert_eq!(
            total_write.status,
            cc_lb_domain::NativeCachePricingStatus::Known
        );
        assert_eq!(total_write.micros_per_million, Some(3_000_000));
    }

    #[test]
    fn native_zero_rate_is_not_emitted_as_known_zero() {
        let catalog = PriceCatalog::new_empty();
        let mut snapshot = CatalogSnapshot::empty_cost_disabled();
        snapshot.status = CatalogStatus::Ok;
        snapshot.models.insert(
            "gpt-zero-native".to_owned(),
            Pricing {
                native: Some(ProviderNativePricing {
                    provider: NativePricingProvider::OpenAi,
                    ordinary_input: NativePricingRate::known(UsdPerMillion::from_micros_usd(0)),
                    cached_input_read: NativePricingRate::unknown(),
                    cached_input_read_by_tier: BTreeMap::new(),
                    cached_input_read_above_tokens: BTreeMap::new(),
                    total_write: BTreeMap::new(),
                    explicit_resource: Default::default(),
                }),
                model: "gpt-zero-native".to_owned(),
                input_per_million_usd: UsdPerMillion::from_whole_usd(2),
                output_per_million_usd: UsdPerMillion::from_whole_usd(8),
                input_image_per_million_usd: None,
                output_image_per_million_usd: None,
                by_tier: BTreeMap::new(),
            },
        );
        catalog.install_snapshot(snapshot);

        let pricing = cache_pricing_summary_for_model(&catalog, "gpt-zero-native", None);
        let ordinary = pricing
            .native
            .expect("native pricing context")
            .rates
            .into_iter()
            .find(|rate| rate.component == cc_lb_domain::NativeCachePricingComponent::OrdinaryInput)
            .expect("ordinary input rate");
        assert_eq!(
            ordinary.status,
            cc_lb_domain::NativeCachePricingStatus::Unknown
        );
        assert_eq!(ordinary.micros_per_million, None);
    }

    #[test]
    fn fold_decreasing_tokens_picks_later_structural() {
        let score = build_cache_score(
            &[
                breakpoint(0, 100, TtlClass::Ephemeral5m),
                breakpoint(1, 90, TtlClass::Ephemeral5m),
            ],
            &[],
        )
        .expect("cache score for cacheable request");

        assert_eq!(score.predicted_cache_read_tokens, 0);
        assert_eq!(score.predicted_cache_creation_tokens_5m, 90);
        assert_eq!(score.predicted_cache_creation_tokens_1h, 0);
    }

    #[test]
    fn fold_all_5m() {
        let score = build_cache_score(
            &[
                breakpoint(0, 100_000, TtlClass::Ephemeral5m),
                breakpoint(1, 250_000, TtlClass::Ephemeral5m),
                breakpoint(2, 500_000, TtlClass::Ephemeral5m),
            ],
            &[],
        )
        .expect("cache score for cacheable request");

        assert_eq!(score.predicted_cache_creation_tokens_5m, 500_000);
        assert_eq!(score.predicted_cache_creation_tokens_1h, 0);
    }

    #[test]
    fn fold_all_1h() {
        let score = build_cache_score(
            &[
                breakpoint(0, 100_000, TtlClass::Ephemeral1h),
                breakpoint(1, 250_000, TtlClass::Ephemeral1h),
                breakpoint(2, 500_000, TtlClass::Ephemeral1h),
            ],
            &[],
        )
        .expect("cache score for cacheable request");

        assert_eq!(score.predicted_cache_creation_tokens_1h, 500_000);
        assert_eq!(score.predicted_cache_creation_tokens_5m, 0);
    }

    #[test]
    fn fold_1h_then_5m_index_split() {
        let score = build_cache_score(
            &[
                breakpoint(0, 100_000, TtlClass::Ephemeral1h),
                breakpoint(1, 300_000, TtlClass::Ephemeral5m),
            ],
            &[],
        )
        .expect("cache score for cacheable request");

        assert_eq!(score.predicted_cache_creation_tokens_1h, 100_000);
        assert_eq!(score.predicted_cache_creation_tokens_5m, 200_000);
        assert_eq!(score.ambiguity_reason, None);
    }

    #[test]
    fn fold_non_monotonic_1h_then_5m_saturates_and_flags() {
        let score = build_cache_score(
            &[
                breakpoint(0, 100, TtlClass::Ephemeral1h),
                breakpoint(1, 90, TtlClass::Ephemeral5m),
            ],
            &[],
        )
        .expect("cache score for cacheable request");

        assert_eq!(score.predicted_cache_creation_tokens_1h, 100);
        assert_eq!(score.predicted_cache_creation_tokens_5m, 0);
        assert!(score.ambiguity_reason.is_some());
    }

    #[test]
    fn fold_same_index_ttl_upgrade() {
        let score = build_cache_score(
            &[breakpoint(2, 200_000, TtlClass::Ephemeral1h)],
            &[warm_entry("bp-2", TtlClass::Ephemeral5m, 200_000)],
        )
        .expect("cache score for cacheable request");

        assert_eq!(score.predicted_cache_read_tokens, 0);
        assert_eq!(score.matched_breakpoint_index, None);
        assert_eq!(score.predicted_cache_creation_tokens_1h, 200_000);
        assert_eq!(score.predicted_cache_creation_tokens_5m, 0);
    }

    #[test]
    fn five_minute_request_matches_one_hour_entry_at_same_key() {
        let score = build_cache_score(
            &[breakpoint(2, 200_000, TtlClass::Ephemeral5m)],
            &[warm_entry("bp-2", TtlClass::Ephemeral1h, 200_000)],
        )
        .expect("cache score for cacheable request");

        assert_eq!(score.predicted_cache_read_tokens, 200_000);
        assert_eq!(score.matched_breakpoint_index, Some(2));
        assert_eq!(score.predicted_cache_creation_tokens_5m, 0);
        assert_eq!(score.predicted_cache_creation_tokens_1h, 0);
    }

    #[test]
    fn overlapping_memberships_select_greatest_eligible_expiry() {
        let breakpoints = overlapping_mixed_ttl_breakpoints();
        let mut one_hour = warm_entry("shared", TtlClass::Ephemeral1h, 90_000);
        one_hour.expires_at_unix_secs = 1_700_000_200;
        let mut five_minute = warm_entry("shared", TtlClass::Ephemeral5m, 100_000);
        five_minute.expires_at_unix_secs = 1_700_000_300;

        let score = build_cache_score(&breakpoints, &[one_hour, five_minute])
            .expect("cache score for overlapping memberships");

        assert_eq!(score.predicted_cache_read_tokens, 100_000);
        assert_eq!(score.predicted_expires_at_unix_secs, Some(1_700_000_300));
        assert_eq!(score.matched_breakpoint_index, Some(15));
    }

    #[test]
    fn overlapping_five_minute_hit_prices_same_index_one_hour_upgrade() {
        let breakpoints = overlapping_mixed_ttl_breakpoints();
        let score = build_cache_score(
            &breakpoints,
            &[warm_entry("shared", TtlClass::Ephemeral5m, 100_000)],
        )
        .expect("cache score for overlapping memberships");

        assert_eq!(score.predicted_cache_read_tokens, 100_000);
        assert_eq!(score.predicted_cache_creation_tokens_1h, 100_000);
        assert_eq!(score.predicted_cache_creation_tokens_5m, 50_000);
        assert_eq!(score.matched_breakpoint_index, Some(15));
        assert_eq!(score.matched_content_block_index, Some(10));
    }

    #[test]
    fn invalid_ttl_order_marks_ineligible() {
        let score = build_cache_score(
            &[
                breakpoint(0, 100_000, TtlClass::Ephemeral5m),
                breakpoint(1, 200_000, TtlClass::Ephemeral1h),
            ],
            &[],
        );

        assert!(score.is_none());
    }

    fn reference_cache_score(
        breakpoints: &[CacheBreakpoint],
        warm: &[WarmCacheEntry],
    ) -> Option<(u64, u64, u64, Option<u32>)> {
        if breakpoints.is_empty() {
            return None;
        }
        let latest_1h = breakpoints
            .iter()
            .filter(|bp| bp.requested_ttl == TtlClass::Ephemeral1h)
            .map(|bp| bp.block_index)
            .max();
        let earliest_5m = breakpoints
            .iter()
            .filter(|bp| bp.requested_ttl == TtlClass::Ephemeral5m)
            .map(|bp| bp.block_index)
            .min();
        if let (Some(last_1h), Some(first_5m)) = (latest_1h, earliest_5m)
            && last_1h >= first_5m
        {
            return None;
        }
        let mut best: Option<(u32, u64, TtlClass, u64)> = None;
        for breakpoint in breakpoints {
            for prefix in &breakpoint.lookback_prefixes {
                let accepts_five_minute = breakpoints.iter().any(|candidate| {
                    candidate.requested_ttl == TtlClass::Ephemeral5m
                        && candidate.lookback_prefixes.iter().any(|candidate_prefix| {
                            candidate_prefix.prefix_hash == prefix.prefix_hash
                                && candidate_prefix.content_block_index
                                    == prefix.content_block_index
                        })
                });
                let Some(entry) = warm
                    .iter()
                    .filter(|entry| entry.prefix_hash == prefix.prefix_hash)
                    .filter(|entry| {
                        entry.ttl_class == TtlClass::Ephemeral1h
                            || accepts_five_minute && entry.ttl_class == TtlClass::Ephemeral5m
                    })
                    .max_by_key(|entry| entry.expires_at_unix_secs)
                else {
                    continue;
                };
                if best.is_none_or(|(index, _, _, expiry)| {
                    prefix.content_block_index > index
                        || prefix.content_block_index == index
                            && entry.expires_at_unix_secs > expiry
                }) {
                    best = Some((
                        prefix.content_block_index,
                        entry.estimated_prefix_bytes.get(),
                        entry.ttl_class,
                        entry.expires_at_unix_secs,
                    ));
                }
            }
        }
        let matched_index = best.map(|(index, _, _, _)| index);
        let matched_tokens = best.map_or(0, |(_, tokens, _, _)| tokens);
        let matched_ttl = best.map(|(_, _, ttl, _)| ttl);
        let deepest = |ttl: TtlClass| -> Option<u64> {
            breakpoints
                .iter()
                .filter(|bp| bp.requested_ttl == ttl)
                .filter(|bp| match (matched_index, matched_ttl) {
                    (None, None) => true,
                    (Some(index), Some(_)) if bp.block_index < index => false,
                    (Some(index), _) if bp.block_index > index => true,
                    (Some(_), Some(entry_ttl)) => match bp.requested_ttl {
                        TtlClass::Ephemeral5m => false,
                        TtlClass::Ephemeral1h => entry_ttl != TtlClass::Ephemeral1h,
                    },
                    _ => true,
                })
                .max_by_key(|bp| bp.block_index)
                .map(|bp| bp.prefix_bytes.get())
        };
        let deepest_1h = deepest(TtlClass::Ephemeral1h);
        let deepest_5m = deepest(TtlClass::Ephemeral5m);
        let one_hour_baseline = if matched_ttl == Some(TtlClass::Ephemeral1h) {
            matched_tokens
        } else {
            0
        };
        let creation_1h = deepest_1h.map_or(0, |tokens| tokens.saturating_sub(one_hour_baseline));
        let boundary = deepest_1h.unwrap_or(matched_tokens);
        let creation_5m = deepest_5m.map_or(0, |tokens| tokens.saturating_sub(boundary));
        Some((matched_tokens, creation_5m, creation_1h, matched_index))
    }

    fn lookback_from(index: u32) -> Vec<CacheLookbackPrefix> {
        let start = index.saturating_sub(2);
        (start..=index)
            .rev()
            .map(|position| CacheLookbackPrefix {
                prefix_hash: format!("h{position}"),
                content_block_index: position,
                lookback_distance: index - position,
            })
            .collect()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        #[test]
        fn build_cache_score_matches_reference_oracle(
            tokens in proptest::collection::vec(0u64..=500_000, 1..=5),
            ttl_1h in proptest::collection::vec(any::<bool>(), 5),
            warm_flags in proptest::collection::vec(any::<bool>(), 5),
            warm_ttl_1h in proptest::collection::vec(any::<bool>(), 5),
        ) {
            let count = tokens.len();
            let breakpoints: Vec<CacheBreakpoint> = (0..count)
                .map(|position| {
                    let index = u32::try_from(position).expect("small index");
                    CacheBreakpoint {
                        block_index: index,
                        source: CacheBreakpointSource::Message,
                        path: format!("messages.{index}.content"),
                        message_index: Some(index),
                        prefix_hash: format!("h{index}"),
                        prefix_bytes: PrefixBytes::new(tokens[position]),
                        requested_ttl: if ttl_1h[position] {
                            TtlClass::Ephemeral1h
                        } else {
                            TtlClass::Ephemeral5m
                        },
                        lookback_prefixes: lookback_from(index),
                        token_estimate_source: Some("test".to_owned()),
                    }
                })
                .collect();
            let warm: Vec<WarmCacheEntry> = (0..count)
                .filter(|&position| warm_flags[position])
                .map(|position| {
                    let index = u32::try_from(position).expect("small index");
                    WarmCacheEntry {
                        prefix_hash: format!("h{index}"),
                        expires_at_unix_secs: 1_700_000_300 + u64::from(index),
                        ttl_class: if warm_ttl_1h[position] {
                            TtlClass::Ephemeral1h
                        } else {
                            TtlClass::Ephemeral5m
                        },
                        last_observed_at_unix_secs: 1_700_000_000,
                        content_block_index: index,
                        estimated_prefix_bytes: PrefixBytes::new(tokens[position]),
                        hash_schema_version: HASH_SCHEMA_VERSION,
                    }
                })
                .collect();

            let got = build_cache_score(&breakpoints, &warm);
            let want = reference_cache_score(&breakpoints, &warm);
            match (got, want) {
                (None, None) => {}
                (Some(score), Some((read, creation_5m, creation_1h, matched_index))) => {
                    prop_assert_eq!(u64::from(score.predicted_cache_read_tokens), read);
                    prop_assert_eq!(
                        u64::from(score.predicted_cache_creation_tokens_5m),
                        creation_5m
                    );
                    prop_assert_eq!(
                        u64::from(score.predicted_cache_creation_tokens_1h),
                        creation_1h
                    );
                    prop_assert_eq!(score.matched_content_block_index, matched_index);
                }
                (got, want) => prop_assert!(
                    false,
                    "eligibility mismatch: got_some={} want_some={}",
                    got.is_some(),
                    want.is_some()
                ),
            }
        }
    }

    fn breakpoint(block_index: u32, prefix_bytes: u64, requested_ttl: TtlClass) -> CacheBreakpoint {
        CacheBreakpoint {
            block_index,
            source: CacheBreakpointSource::Message,
            path: format!("messages.{block_index}.content"),
            message_index: Some(block_index),
            prefix_hash: format!("bp-{block_index}"),
            prefix_bytes: PrefixBytes::new(prefix_bytes),
            requested_ttl,
            lookback_prefixes: vec![cc_lb_domain::CacheLookbackPrefix {
                prefix_hash: format!("bp-{block_index}"),
                content_block_index: block_index,
                lookback_distance: 0,
            }],
            token_estimate_source: Some("test".to_owned()),
        }
    }

    fn overlapping_mixed_ttl_breakpoints() -> [CacheBreakpoint; 2] {
        let mut one_hour = breakpoint(10, 100_000, TtlClass::Ephemeral1h);
        one_hour.prefix_hash = "shared".to_owned();
        one_hour.lookback_prefixes = vec![CacheLookbackPrefix {
            prefix_hash: "shared".to_owned(),
            content_block_index: 10,
            lookback_distance: 0,
        }];
        let mut five_minute = breakpoint(15, 150_000, TtlClass::Ephemeral5m);
        five_minute.lookback_prefixes.push(CacheLookbackPrefix {
            prefix_hash: "shared".to_owned(),
            content_block_index: 10,
            lookback_distance: 5,
        });
        [one_hour, five_minute]
    }

    fn warm_entry(
        prefix_hash: &str,
        ttl_class: TtlClass,
        estimated_prefix_bytes: u64,
    ) -> WarmCacheEntry {
        WarmCacheEntry {
            prefix_hash: prefix_hash.to_owned(),
            expires_at_unix_secs: 1_700_000_300,
            ttl_class,
            last_observed_at_unix_secs: 1_700_000_000,
            content_block_index: 0,
            estimated_prefix_bytes: PrefixBytes::new(estimated_prefix_bytes),
            hash_schema_version: HASH_SCHEMA_VERSION,
        }
    }
}

#[cfg(test)]
mod provider_native_cache_tests {
    use super::{
        RequestKind, extract_request_affinity_keys, native_cache_score_from_record,
        provider_native_lookup_params,
    };
    use cc_lb_request_log::{RequestCacheEvidence, RequestNativeCacheObservation};
    use cc_lb_storage_api::ProviderPromptCacheObservationRecord;
    use serde_json::json;
    use uuid::Uuid;

    fn native(
        provider: &str,
        mode: &str,
        scope: Option<&str>,
        identity_hash: Option<&str>,
        retention_secs: Option<u64>,
    ) -> RequestNativeCacheObservation {
        RequestNativeCacheObservation {
            provider: provider.to_owned(),
            mode: mode.to_owned(),
            scope: scope.map(str::to_owned),
            identity_hash: identity_hash.map(str::to_owned),
            retention_secs,
            evidence: RequestCacheEvidence::Unreported,
            ..Default::default()
        }
    }

    #[test]
    fn native_lookup_requires_complete_scope_hashed_key_and_matching_request() {
        let valid = native(
            "openai",
            "implicit",
            Some("org=acme;region=us"),
            Some("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            Some(86_400),
        );
        assert!(
            provider_native_lookup_params(Some(&valid), "gpt-5", RequestKind::OpenaiResponses,)
                .is_some()
        );

        for invalid in [
            native(
                "openai",
                "implicit",
                Some("org=acme;region="),
                Some("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                Some(86_400),
            ),
            native(
                "openai",
                "implicit",
                Some("org=acme"),
                Some("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                Some(86_400),
            ),
            native(
                "openai",
                "implicit",
                Some("org=acme;region=us"),
                None,
                Some(86_400),
            ),
            native(
                "openai",
                "implicit",
                Some("org=acme;region=us"),
                Some("v3:local-anthropic-prefix"),
                Some(86_400),
            ),
            native(
                "gemini",
                "implicit",
                Some("org=acme;region=us"),
                Some("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                Some(86_400),
            ),
        ] {
            assert!(
                provider_native_lookup_params(
                    Some(&invalid),
                    "gpt-5",
                    RequestKind::OpenaiResponses,
                )
                .is_none()
            );
        }
    }

    #[test]
    fn native_lookup_includes_api_key_only_openai_candidates() {
        let request = native(
            "openai",
            "implicit",
            Some("org=acme;region=us"),
            Some("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            Some(86_400),
        );
        assert_eq!(
            provider_native_lookup_params(
                Some(&request),
                "gpt-5",
                RequestKind::OpenaiResponsesInputTokens,
            )
            .map(|(_, _, _, _, retention_secs)| retention_secs),
            Some(86_400)
        );
    }

    #[test]
    fn native_lookup_rejects_missing_retention() {
        let request = native(
            "openai",
            "implicit",
            Some("org=acme;region=us"),
            Some("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            None,
        );
        assert!(
            provider_native_lookup_params(Some(&request), "gpt-4o", RequestKind::OpenaiResponses,)
                .is_none()
        );
    }

    #[test]
    fn native_lookup_preserves_zero_in_memory_retention() {
        let request = native(
            "openai",
            "implicit",
            Some("org=acme;region=us"),
            Some("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            Some(0),
        );
        assert_eq!(
            provider_native_lookup_params(Some(&request), "gpt-4o", RequestKind::OpenaiResponses,)
                .map(|(_, _, _, _, retention_secs)| retention_secs),
            Some(0)
        );
    }

    #[test]
    fn missing_key_has_no_native_score_even_for_modern_openai() {
        let request = native(
            "openai",
            "implicit",
            Some("org=acme;region=us"),
            None,
            Some(1_800),
        );
        assert!(
            provider_native_lookup_params(Some(&request), "gpt-5.6", RequestKind::OpenaiResponses,)
                .is_none()
        );
    }

    #[test]
    fn present_key_has_no_native_score_for_modern_openai() {
        let request = native(
            "openai",
            "implicit",
            Some("org=acme;region=us"),
            Some("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            Some(1_800),
        );
        assert!(
            provider_native_lookup_params(Some(&request), "gpt-5.6", RequestKind::OpenaiResponses,)
                .is_none()
        );
    }

    #[test]
    fn gemini_native_evidence_never_becomes_soft_score_but_cached_content_stays_affine() {
        for mode in ["implicit", "explicit"] {
            let request = native(
                "gemini",
                mode,
                Some("project"),
                Some("sha256:cached-content"),
                Some(1_800),
            );
            assert!(
                provider_native_lookup_params(
                    Some(&request),
                    "gemini-2.5-pro",
                    RequestKind::GeminiGenerateContent,
                )
                .is_none()
            );
        }

        let keys = extract_request_affinity_keys(
            RequestKind::GeminiGenerateContent.capability(),
            &json!({"cachedContent": "projects/acme/locations/us/cachedContents/1"}),
            "principal",
        )
        .expect("cachedContent affinity extraction");
        assert_eq!(keys.len(), 1);
    }

    #[test]
    fn native_score_exposes_observed_tokens_without_anthropic_cache_key() {
        let record = ProviderPromptCacheObservationRecord {
            upstream_id: Uuid::from_u128(1),
            provider: "openai".to_owned(),
            canonical_model_id: "gpt-5".to_owned(),
            mode: "implicit".to_owned(),
            isolation_scope: "org=acme;region=us".to_owned(),
            identity_hash: "sha256:key".to_owned(),
            boundary: "provider".to_owned(),
            evidence_source: "provider_usage_metadata".to_owned(),
            observed_cached_tokens: Some(u64::from(u32::MAX) + 1),
            observed_write_tokens: None,
            retention_secs: Some(86_400),
            expires_at_unix_secs: Some(1_800_000_000),
            last_observed_at_unix_secs: 1_799_999_000,
        };
        let score = native_cache_score_from_record(&record);
        assert_eq!(score.predicted_cache_read_tokens, u32::MAX);
        assert_eq!(score.predicted_expires_at_unix_secs, Some(1_800_000_000));
        assert!(score.matched_v3_cache_key.is_none());
    }
}

fn cache_pricing_summary_for_model(
    catalog: &cc_lb_pricing::PriceCatalog,
    model: &str,
    service_tier: Option<&str>,
) -> CachePricingSummary {
    if model.is_empty() {
        return CachePricingSummary {
            native: None,
            status: "unknown".to_owned(),
            input_micros_per_million: None,
            cache_creation_5m_micros_per_million: None,
            cache_creation_1h_micros_per_million: None,
            cache_read_micros_per_million: None,
        };
    }

    let normalized = cc_lb_pricing::normalize_model_id(model);
    let native = catalog
        .native_pricing(model, service_tier)
        .map(native_cache_pricing_from_catalog);
    if matches!(
        native.as_ref().map(|pricing| pricing.provider),
        Some(NativeCachePricingProvider::OpenAi | NativeCachePricingProvider::Gemini)
    ) {
        return CachePricingSummary {
            native,
            ..CachePricingSummary::default()
        };
    }
    let pricing = catalog.routing_cache_pricing(model, service_tier);
    let input = pricing.map(|rates| rates.input_per_million_usd.as_micros_usd());
    let cache_creation_5m = pricing.and_then(|rates| {
        rates
            .cache_creation_5m_per_million_usd
            .map(|price| price.as_micros_usd())
    });
    let cache_creation_1h = pricing.and_then(|rates| {
        rates
            .cache_creation_1h_per_million_usd
            .map(|price| price.as_micros_usd())
    });
    let cache_read = pricing.and_then(|rates| {
        rates
            .cache_read_per_million_usd
            .map(|price| price.as_micros_usd())
    });
    let status = if input.is_some() && cache_creation_5m.is_some() && cache_read.is_some() {
        "known"
    } else {
        if let Some(mut fallback) = anthropic_family_cache_pricing_summary(&normalized) {
            fallback.native = native;
            return fallback;
        }
        "unknown"
    };

    CachePricingSummary {
        native,
        status: status.to_owned(),
        input_micros_per_million: input,
        cache_creation_5m_micros_per_million: cache_creation_5m,
        cache_creation_1h_micros_per_million: cache_creation_1h,
        cache_read_micros_per_million: cache_read,
    }
}

fn native_cache_pricing_from_catalog(
    pricing: cc_lb_pricing::ProviderNativePricing,
) -> NativeCachePricing {
    let mut rates = Vec::with_capacity(5 + pricing.total_write.len());
    rates.push(native_cache_pricing_rate(
        NativeCachePricingComponent::OrdinaryInput,
        None,
        pricing.ordinary_input,
    ));
    rates.push(native_cache_pricing_rate(
        NativeCachePricingComponent::CachedInputRead,
        None,
        pricing.cached_input_read,
    ));
    for (lifetime, rate) in pricing.total_write {
        rates.push(native_cache_pricing_rate(
            NativeCachePricingComponent::TotalWrite,
            Some(native_cache_pricing_lifetime(lifetime)),
            rate,
        ));
    }
    rates.push(native_cache_pricing_rate(
        NativeCachePricingComponent::ExplicitResourceToken,
        None,
        pricing.explicit_resource.token,
    ));
    rates.push(native_cache_pricing_rate(
        NativeCachePricingComponent::ExplicitResourceStorage,
        Some(NativeCachePricingLifetime::ExplicitResourceTtl),
        pricing.explicit_resource.storage,
    ));
    rates.push(native_cache_pricing_rate(
        NativeCachePricingComponent::ExplicitResourceTtl,
        Some(NativeCachePricingLifetime::ExplicitResourceTtl),
        pricing.explicit_resource.ttl,
    ));
    NativeCachePricing {
        version: 1,
        provider: native_cache_pricing_provider(pricing.provider),
        rates,
    }
}

fn native_cache_pricing_provider(
    provider: cc_lb_pricing::NativePricingProvider,
) -> NativeCachePricingProvider {
    match provider {
        cc_lb_pricing::NativePricingProvider::Anthropic => NativeCachePricingProvider::Anthropic,
        cc_lb_pricing::NativePricingProvider::OpenAi => NativeCachePricingProvider::OpenAi,
        cc_lb_pricing::NativePricingProvider::Gemini => NativeCachePricingProvider::Gemini,
        cc_lb_pricing::NativePricingProvider::Unknown => NativeCachePricingProvider::Unknown,
    }
}

fn native_cache_pricing_lifetime(
    lifetime: cc_lb_pricing::NativePricingLifetime,
) -> NativeCachePricingLifetime {
    match lifetime {
        cc_lb_pricing::NativePricingLifetime::ModelRetention => {
            NativeCachePricingLifetime::ModelRetention
        }
        cc_lb_pricing::NativePricingLifetime::ThirtyMinutes => {
            NativeCachePricingLifetime::ThirtyMinutes
        }
        cc_lb_pricing::NativePricingLifetime::OneHour => NativeCachePricingLifetime::OneHour,
        cc_lb_pricing::NativePricingLifetime::TwelveHours => {
            NativeCachePricingLifetime::TwelveHours
        }
        cc_lb_pricing::NativePricingLifetime::ExplicitResourceTtl => {
            NativeCachePricingLifetime::ExplicitResourceTtl
        }
    }
}

fn native_cache_pricing_rate(
    component: NativeCachePricingComponent,
    lifetime: Option<NativeCachePricingLifetime>,
    rate: cc_lb_pricing::NativePricingRate,
) -> NativeCachePricingRate {
    let status = match rate.status {
        cc_lb_pricing::NativePricingStatus::Known
            if rate
                .price_per_million_usd
                .map(|price| price.as_micros_usd())
                != Some(0) =>
        {
            NativeCachePricingStatus::Known
        }
        cc_lb_pricing::NativePricingStatus::NotApplicable => {
            NativeCachePricingStatus::NotApplicable
        }
        cc_lb_pricing::NativePricingStatus::Known | cc_lb_pricing::NativePricingStatus::Unknown => {
            NativeCachePricingStatus::Unknown
        }
    };
    let micros_per_million = if status == NativeCachePricingStatus::Known {
        rate.price_per_million_usd
            .map(|price| price.as_micros_usd())
    } else {
        None
    };
    NativeCachePricingRate {
        component,
        lifetime,
        status,
        micros_per_million,
    }
}

fn anthropic_family_cache_pricing_summary(model: &str) -> Option<CachePricingSummary> {
    let input_micros_per_million = anthropic_family_input_micros_per_million(model)?;
    Some(CachePricingSummary {
        native: None,
        status: "known".to_owned(),
        input_micros_per_million: Some(input_micros_per_million),
        cache_creation_5m_micros_per_million: Some(cache_creation_5m_micros_from_input(
            input_micros_per_million,
        )),
        cache_creation_1h_micros_per_million: Some(
            cc_lb_pricing::cache_creation_1h_micros_from_input(input_micros_per_million),
        ),
        cache_read_micros_per_million: Some(cache_read_micros_from_input(input_micros_per_million)),
    })
}

/// Base input price per million tokens for an Anthropic model, used only when the live price
/// catalog has no entry for it.
///
/// Matching is version-aware, not family-wide. A blanket `claude-opus => $15` was correct only
/// for Opus 4 and 4.1: Opus 4.5 through Opus 5 are $5/MTok, so the family rule overpriced every
/// newer Opus by 3x on input and, through the derived multipliers, on both cache-write rates and
/// the cache-read rate. Prefixes are ordered most-specific first because `claude-opus-4` is a
/// prefix of `claude-opus-4-5`.
///
/// Source: <https://platform.claude.com/docs/en/build-with-claude/prompt-caching#pricing>
/// (retrieved 2026-07-27).
fn anthropic_family_input_micros_per_million(model: &str) -> Option<u64> {
    const FABLE_AND_MYTHOS: u64 = 10_000_000;
    const OPUS_CURRENT: u64 = 5_000_000;
    const OPUS_LEGACY: u64 = 15_000_000;
    const SONNET_STANDARD: u64 = 3_000_000;
    const HAIKU_CURRENT: u64 = 1_000_000;
    const HAIKU_LEGACY: u64 = 800_000;

    // Most specific first.
    const PREFIXES: [(&str, u64); 14] = [
        ("claude-fable", FABLE_AND_MYTHOS),
        ("claude-mythos", FABLE_AND_MYTHOS),
        ("claude-opus-4-1", OPUS_LEGACY),
        ("claude-opus-4-5", OPUS_CURRENT),
        ("claude-opus-4-6", OPUS_CURRENT),
        ("claude-opus-4-7", OPUS_CURRENT),
        ("claude-opus-4-8", OPUS_CURRENT),
        ("claude-opus-5", OPUS_CURRENT),
        // Bare `claude-opus-4` (no minor) is the retired original at legacy pricing. It must come
        // after every `claude-opus-4-N` arm above.
        ("claude-opus-4", OPUS_LEGACY),
        // Catch-all for future generations. Without it a `claude-opus-6` would return None and
        // lose cache pricing entirely, disabling cache-value scoring for that model; inheriting
        // current pricing is the cheaper wrong answer.
        ("claude-opus", OPUS_CURRENT),
        ("claude-sonnet-5", SONNET_STANDARD),
        ("claude-sonnet", SONNET_STANDARD),
        // Haiku 3.5 is addressed as `claude-3-5-haiku-*`; `claude-haiku-3-5` is not an id
        // Anthropic emits.
        ("claude-3-5-haiku", HAIKU_LEGACY),
        ("claude-haiku", HAIKU_CURRENT),
    ];

    PREFIXES
        .iter()
        .find(|(prefix, _)| model.starts_with(prefix))
        .map(|(_, micros)| *micros)
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

fn saturating_u64_to_u32(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

/// Token estimate of a prefix size, saturated to the `CacheScore` width.
fn token_estimate_u32(bytes: PrefixBytes) -> u32 {
    saturating_u64_to_u32(bytes.as_token_estimate())
}

#[derive(Clone)]
pub struct PromptCacheObservationContext {
    pub(crate) upstream_id: Uuid,
    pub(crate) canonical_model_id: String,
    pub(crate) cache_breakpoints: Vec<CacheBreakpoint>,
    pub(crate) cacheable_breakpoint_prefix_keys: HashSet<String>,
    pub(crate) selected_match: Option<SelectedCacheMatch>,
    pub(crate) native_cache: Option<cc_lb_request_log::RequestNativeCacheObservation>,
    /// Direct non-blocking publication port for decoded observations.
    /// Replaces the old lifecycle-bus hop: enqueue failures are reported
    /// and never fail the request.
    pub(crate) sink: Arc<dyn PromptCacheObservationSinkLike>,
    /// Margin already subtracted at observation creation; reads compare
    /// `expires_at > now` only.
    pub(crate) grace_margin_secs: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PromptCacheUsage {
    pub(crate) cache_creation_input_tokens: u64,
    pub(crate) cache_read_input_tokens: u64,
    pub(crate) cache_creation_evidence: cc_lb_request_log::RequestCacheEvidence,
    pub(crate) cache_read_evidence: cc_lb_request_log::RequestCacheEvidence,
    /// True only for present valid positive reads and complete numeric zero
    /// creation evidence, never from accounting counters' zero defaults.
    pub(crate) read_only_evidence: bool,
}

impl From<&UsageCounts> for PromptCacheUsage {
    fn from(usage: &UsageCounts) -> Self {
        Self {
            cache_creation_input_tokens: usage.cache_creation_input_tokens,
            cache_read_input_tokens: usage.cache_read_input_tokens,
            cache_creation_evidence: request_cache_evidence(usage.cache_creation_evidence),
            cache_read_evidence: request_cache_evidence(usage.cache_read_evidence),
            read_only_evidence: usage.has_affirmative_read_only_cache_usage(),
        }
    }
}

fn request_cache_evidence(
    evidence: usage_parser::CacheTokenEvidence,
) -> cc_lb_request_log::RequestCacheEvidence {
    match evidence {
        usage_parser::CacheTokenEvidence::Unreported => {
            cc_lb_request_log::RequestCacheEvidence::Unreported
        }
        usage_parser::CacheTokenEvidence::Zero => cc_lb_request_log::RequestCacheEvidence::Zero,
        usage_parser::CacheTokenEvidence::Positive => {
            cc_lb_request_log::RequestCacheEvidence::Positive
        }
        usage_parser::CacheTokenEvidence::Invalid => {
            cc_lb_request_log::RequestCacheEvidence::Invalid
        }
    }
}

fn native_tokens_for_evidence(
    evidence: cc_lb_request_log::RequestCacheEvidence,
    tokens: u64,
) -> Option<u64> {
    match evidence {
        cc_lb_request_log::RequestCacheEvidence::Zero
        | cc_lb_request_log::RequestCacheEvidence::Positive => Some(tokens),
        cc_lb_request_log::RequestCacheEvidence::Unreported
        | cc_lb_request_log::RequestCacheEvidence::Invalid => None,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DecodedPromptCacheObservation {
    pub(crate) prefix_hash: String,
    pub(crate) ttl_class: TtlClass,
    pub(crate) expires_at_unix_secs: u64,
    pub(crate) prefix_content_block_index: u32,
    pub(crate) estimated_prefix_bytes: PrefixBytes,
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
    cacheable_breakpoint_prefix_keys: &HashSet<String>,
    selected_match: Option<SelectedCacheMatch>,
    native_cache: Option<cc_lb_request_log::RequestNativeCacheObservation>,
) -> Option<PromptCacheObservationContext> {
    if canonical_model_id.is_empty() || (cache_breakpoints.is_empty() && native_cache.is_none()) {
        return None;
    }
    let sink = Arc::clone(&view.prompt_cache_observation_sink);
    Some(PromptCacheObservationContext {
        upstream_id,
        canonical_model_id: canonical_model_id.to_owned(),
        cache_breakpoints: cache_breakpoints.to_vec(),
        cacheable_breakpoint_prefix_keys: cacheable_breakpoint_prefix_keys.clone(),
        selected_match,
        native_cache,
        sink,
        grace_margin_secs: view.prompt_cache_grace_margin_secs,
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

    let hit = (usage.cache_read_input_tokens > 0)
        .then_some(context.selected_match.as_ref())
        .flatten();
    let grace_secs = context.grace_margin_secs;
    let mut observations = Vec::new();
    let mut dropped_below_threshold = 0_u32;
    if let Some(matched) = hit {
        observations.push(DecodedPromptCacheObservation {
            prefix_hash: matched.prefix_hash.clone(),
            ttl_class: matched.ttl_class,
            expires_at_unix_secs: prompt_cache_observation_expires_at(
                now_unix_secs,
                matched.ttl_class,
                grace_secs,
            ),
            prefix_content_block_index: matched.content_block_index,
            estimated_prefix_bytes: matched.estimated_prefix_bytes,
            kind: DecodedPromptCacheObservationKind::Hit,
        });
    } else if context.selected_match.is_none()
        && usage.cache_creation_input_tokens == 0
        && usage.cache_read_input_tokens > 0
        && usage.read_only_evidence
        && let Some(breakpoint) = context
            .cache_breakpoints
            .iter()
            .filter(|breakpoint| {
                context
                    .cacheable_breakpoint_prefix_keys
                    .contains(&breakpoint.prefix_hash)
            })
            .max_by_key(|breakpoint| breakpoint.block_index)
    {
        // Explicit and automatic controls are already resolved to the same
        // breakpoint representation. Infer only its deepest eligible boundary,
        // never a lookback key or a replacement for a carried predicted match.
        observations.push(DecodedPromptCacheObservation {
            prefix_hash: breakpoint.prefix_hash.clone(),
            ttl_class: breakpoint.requested_ttl,
            expires_at_unix_secs: prompt_cache_observation_expires_at(
                now_unix_secs,
                breakpoint.requested_ttl,
                grace_secs,
            ),
            prefix_content_block_index: breakpoint.block_index,
            estimated_prefix_bytes: breakpoint.prefix_bytes,
            kind: DecodedPromptCacheObservationKind::Hit,
        });
    }

    if usage.cache_creation_input_tokens > 0 {
        let anchor = hit.map(|matched| (matched.content_block_index, matched.ttl_class));
        for breakpoint in &context.cache_breakpoints {
            if !breakpoint_needs_write(breakpoint, anchor) {
                continue;
            }
            if !context
                .cacheable_breakpoint_prefix_keys
                .contains(&breakpoint.prefix_hash)
            {
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
                estimated_prefix_bytes: breakpoint.prefix_bytes,
                kind: DecodedPromptCacheObservationKind::Write,
            });
        }
    }

    PromptCacheObservationDecodeResult {
        observations,
        dropped_below_threshold,
    }
}

/// Publish decoded observations directly to the bounded sink.
///
/// Runs at the first valid usage evidence (SSE `message_start`, compat SSE
/// same point, or buffered response completion). Enqueue is non-blocking;
/// failures are logged and counted by the sink — the request continues.
pub(crate) fn publish_prompt_cache_observations(
    context: &PromptCacheObservationContext,
    decode: &PromptCacheObservationDecodeResult,
    now_unix_secs: u64,
) {
    for _ in 0..decode.dropped_below_threshold {
        cc_lb_observability::inc_cache_observation_dropped(
            cc_lb_observability::cache_observation_dropped_reason::BELOW_THRESHOLD,
        );
    }
    let sink = &context.sink;
    for observation in &decode.observations {
        let record = PromptCacheObservationRecord {
            upstream_id: context.upstream_id,
            canonical_model_id: context.canonical_model_id.clone(),
            v3_prefix_key: observation.prefix_hash.clone(),
            ttl_class: observation.ttl_class,
            expires_at_unix_secs: observation.expires_at_unix_secs,
            last_observed_at_unix_secs: now_unix_secs,
            hash_schema_version: HASH_SCHEMA_VERSION,
            prefix_content_block_index: observation.prefix_content_block_index,
            estimated_prefix_tokens: observation.estimated_prefix_bytes.get(),
            token_estimate_source: PrefixBytes::ESTIMATE_SOURCE.to_owned(),
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

pub(crate) fn publish_prompt_cache_observations_with_usage(
    context: &PromptCacheObservationContext,
    decode: &PromptCacheObservationDecodeResult,
    usage: PromptCacheUsage,
    now_unix_secs: u64,
) {
    publish_prompt_cache_observations(context, decode, now_unix_secs);
    publish_provider_cache_observation(context, usage, now_unix_secs);
}

fn publish_provider_cache_observation(
    context: &PromptCacheObservationContext,
    usage: PromptCacheUsage,
    now_unix_secs: u64,
) {
    let Some(native) = context.native_cache.as_ref() else {
        return;
    };
    let record = ProviderPromptCacheObservationRecord {
        upstream_id: context.upstream_id,
        provider: native.provider.clone(),
        canonical_model_id: context.canonical_model_id.clone(),
        mode: native.mode.clone(),
        isolation_scope: native.scope.clone().unwrap_or_default(),
        identity_hash: native.identity_hash.clone().unwrap_or_default(),
        boundary: native.boundary.clone().unwrap_or_default(),
        evidence_source: native.evidence_source.clone().unwrap_or_default(),
        observed_cached_tokens: native_tokens_for_evidence(
            usage.cache_read_evidence,
            usage.cache_read_input_tokens,
        ),
        observed_write_tokens: native_tokens_for_evidence(
            usage.cache_creation_evidence,
            usage.cache_creation_input_tokens,
        ),
        retention_secs: native.retention_secs,
        expires_at_unix_secs: native.expires_at_unix_secs.or_else(|| {
            native
                .retention_secs
                .map(|ttl| now_unix_secs.saturating_add(ttl))
        }),
        last_observed_at_unix_secs: now_unix_secs,
    };
    if let Err(error) = context.sink.enqueue_provider(record) {
        match error {
            PromptCacheObservationEnqueueError::ChannelFull => {}
            PromptCacheObservationEnqueueError::ChannelClosed => {
                tracing::warn!("provider prompt cache observation sink is closed");
            }
        }
    }
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
    pub upstream_affinity_ttl: Duration,
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            messages_body_cap_bytes: DEFAULT_MESSAGES_CAP_BYTES,
            files_body_cap_bytes: DEFAULT_FILES_CAP_BYTES,
            replica_identity: None,
            prompt_cache_shadow: PromptCacheShadowConfig::default(),
            upstream_affinity_ttl: Duration::from_secs(
                UpstreamAffinityConfig::default().ttl_secs(),
            ),
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
    /// Transport failure with the typed source preserved: `source()` yields
    /// the hyper/connect/io error chain so terminal classification can
    /// distinguish DNS, TCP, TLS, and timeout failures without string
    /// matching.
    #[error("upstream dispatch failed: {source}")]
    Transport {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("upstream bulkhead queue full; retry after {retry_after:?}")]
    BulkheadFull { retry_after: Duration },
}

/// Failure of the raw-passthrough fallback used when dialect shaping fails.
#[derive(Debug, Error)]
enum RawPassthroughError {
    /// The configured or default upstream base URL failed to parse.
    #[error("invalid raw passthrough base url: {0}")]
    BaseUrl(#[from] url::ParseError),
    /// The raw-passthrough dialect failed to shape the request.
    #[error("raw passthrough shaping failed: {0}")]
    Shape(#[from] DialectError),
}

/// A failed upstream attempt carrying the typed cause until the outer
/// response-conversion point.
///
/// The attempt boundary used to fold every failure into a boxed
/// `Response<Body>`, which destroyed the error chain and made the terminal
/// commit reasonless. This enum keeps the real cause so the caller can build
/// the client response and the [`InternalFailure`] diagnostic from the same
/// value.
#[derive(Debug)]
enum AttemptFailure {
    /// The raw-passthrough fallback itself failed after dialect shaping had
    /// already failed.
    RawPassthrough(RawPassthroughError),
    /// Request signing failed.
    Sign(SignerError),
    /// Dispatch to the upstream failed.
    Dispatch(DispatchError),
}

impl From<RawPassthroughError> for AttemptFailure {
    fn from(source: RawPassthroughError) -> Self {
        Self::RawPassthrough(source)
    }
}

impl AttemptFailure {
    /// Convert into the client-visible response and the terminal diagnostic.
    ///
    /// Response status, body, and `Retry-After` semantics are identical to the
    /// previous inline mapping.
    fn into_response_and_failure(
        self,
        capability: ProviderCapability,
    ) -> (Response<Body>, InternalFailure) {
        match self {
            Self::RawPassthrough(source) => {
                let response = provider_error_response(
                    capability,
                    StatusCode::BAD_GATEWAY,
                    "api_error",
                    "failed to prepare raw upstream request",
                    None,
                );
                let failure = InternalFailure {
                    status: StatusCode::BAD_GATEWAY,
                    error_code: error_codes::UPSTREAM_DISPATCH_FAILED,
                    error: InternalError {
                        stage: InternalErrorStage::Relay,
                        kind: InternalErrorKind::Unavailable,
                        message: Some(source.to_string()),
                    },
                };
                (response, failure)
            }
            Self::Sign(source) => {
                let response = provider_error_response(
                    capability,
                    StatusCode::BAD_GATEWAY,
                    "api_error",
                    "failed to sign upstream request",
                    None,
                );
                let failure = InternalFailure {
                    status: StatusCode::BAD_GATEWAY,
                    error_code: error_codes::SIGNER_FAILED,
                    error: InternalError {
                        stage: InternalErrorStage::Signer,
                        kind: signer_error_kind(&source),
                        message: Some(source.to_string()),
                    },
                };
                (response, failure)
            }
            Self::Dispatch(source) => {
                let (response, kind, message) = match &source {
                    DispatchError::BulkheadFull { retry_after } => (
                        provider_error_response_with_retry_after(
                            capability,
                            StatusCode::SERVICE_UNAVAILABLE,
                            "overloaded_error",
                            "upstream bulkhead queue is full",
                            None,
                            retry_after.as_secs().max(1),
                        ),
                        InternalErrorKind::Unavailable,
                        source.to_string(),
                    ),
                    DispatchError::InvalidUri { .. } | DispatchError::RequestBuild { .. } => (
                        provider_error_response(
                            capability,
                            StatusCode::BAD_GATEWAY,
                            "api_error",
                            "upstream request failed",
                            None,
                        ),
                        InternalErrorKind::ConfigError,
                        source.to_string(),
                    ),
                    DispatchError::Transport { source: transport } => {
                        let failure_kind =
                            crate::bulkhead::classify_transport_error(transport.as_ref());
                        let chain =
                            crate::bulkhead::transport_error_chain(transport.as_ref()).join(" <- ");
                        (
                            provider_error_response(
                                capability,
                                StatusCode::BAD_GATEWAY,
                                "api_error",
                                "upstream request failed",
                                None,
                            ),
                            match failure_kind {
                                crate::bulkhead::TransportFailureKind::Timeout => {
                                    InternalErrorKind::Timeout
                                }
                                _ => InternalErrorKind::Unavailable,
                            },
                            format!("{failure_kind}: {chain}"),
                        )
                    }
                };
                let failure = InternalFailure {
                    status: response.status(),
                    error_code: error_codes::UPSTREAM_DISPATCH_FAILED,
                    error: InternalError {
                        stage: InternalErrorStage::Relay,
                        kind,
                        message: Some(message),
                    },
                };
                (response, failure)
            }
        }
    }
}

/// Map a signing failure to its diagnostic kind.
fn signer_error_kind(error: &SignerError) -> InternalErrorKind {
    match error {
        SignerError::MissingCredentials { .. } => InternalErrorKind::ConfigError,
        SignerError::StorageUnavailable { .. } | SignerError::ExpiredToken { .. } => {
            InternalErrorKind::Unavailable
        }
        SignerError::SigningFailed { .. } => InternalErrorKind::PluginError,
    }
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
        service_tier: Option<&str>,
    ) -> Option<i64>;

    /// Prices disjoint submitted-input regions without adding a prefix twice.
    fn estimate_max_with_local_cache(
        &self,
        model: &str,
        input: cc_lb_pricing::LocalCacheInputEstimate,
        max_output: u64,
        service_tier: Option<&str>,
    ) -> Option<i64>;

    fn estimate_max_with_local_cache_and_images(
        &self,
        model: &str,
        input: cc_lb_pricing::LocalCacheInputEstimate,
        max_output: u64,
        use_image_rates: bool,
        service_tier: Option<&str>,
    ) -> Option<i64> {
        let _ = use_image_rates;
        self.estimate_max_with_local_cache(model, input, max_output, service_tier)
    }

    fn estimate_max_with_cache(
        &self,
        model: &str,
        max_input: u64,
        max_output: u64,
        estimated_cache_tokens: u64,
        _can_cross_cache_cliff: bool,
        service_tier: Option<&str>,
    ) -> Option<i64> {
        self.estimate_max(
            model,
            max_input.saturating_add(estimated_cache_tokens),
            max_output,
            service_tier,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn estimate_max_with_cache_and_images(
        &self,
        model: &str,
        max_input: u64,
        max_output: u64,
        estimated_cache_tokens: u64,
        can_cross_cache_cliff: bool,
        use_image_rates: bool,
        service_tier: Option<&str>,
    ) -> Option<i64> {
        let _ = use_image_rates;
        self.estimate_max_with_cache(
            model,
            max_input,
            max_output,
            estimated_cache_tokens,
            can_cross_cache_cliff,
            service_tier,
        )
    }
}

#[derive(Clone, Debug)]
pub struct AuthLimitSubject {
    pub principal_id: String,
    pub key_id: String,
    pub record: StoredApiKeyRecord,
}

struct LimitRejectionInfo {
    subject: cc_lb_lifecycle::LimitSubject,
    request_summary: cc_lb_lifecycle::LimitRequestSummary,
    route_summary: cc_lb_lifecycle::RouteSummary,
    limit_violation: Option<String>,
    reason_label: String,
}

struct LimitRejectionErr {
    response: Response<Body>,
    info: LimitRejectionInfo,
}

struct RequestAffinityResolution {
    target_upstream_id: Uuid,
    owner_epoch: u64,
    pending_keys: Vec<UpstreamAffinityKey>,
}

struct RequestAffinityFailure {
    status: StatusCode,
    error_type: &'static str,
    message: &'static str,
}

impl RequestAffinityFailure {
    fn response(&self, capability: ProviderCapability) -> Response<Body> {
        provider_error_response(capability, self.status, self.error_type, self.message, None)
    }
}

#[derive(Clone)]
struct ResponseAffinityContext {
    /// Provider protocol of the routed request; selects which response
    /// shapes create provider-owned state.
    capability: ProviderCapability,
    store: Option<Arc<dyn UpstreamAffinityStore>>,
    principal_id: String,
    upstream_id: Uuid,
    pending_request_keys: Vec<UpstreamAffinityKey>,
    observe_response_keys: bool,
    owner_epoch: Option<u64>,
    ttl_secs: u64,
}

async fn bind_upstream_affinity_keys(
    context: &ResponseAffinityContext,
    keys: &[UpstreamAffinityKey],
    now_unix_secs: u64,
    expires_at_unix_secs: Option<u64>,
) -> Result<(), ()> {
    if keys.is_empty() {
        return Ok(());
    }
    let Some(store) = context.store.as_ref() else {
        return Err(());
    };
    let owner_epoch = context.owner_epoch.ok_or(())?;
    let mut seen = HashSet::with_capacity(keys.len());
    let mut bindings = Vec::with_capacity(keys.len());
    for key in keys {
        if seen.insert(key) {
            bindings.push(UpstreamAffinityBinding {
                key: key.clone(),
                upstream_id: context.upstream_id,
                owner_epoch,
                observed_at_unix_secs: now_unix_secs,
                expires_at_unix_secs,
            });
        }
    }
    store
        .bind_upstream_affinities(&bindings, now_unix_secs, context.ttl_secs)
        .await
        .map_err(|_| ())
}

fn affinity_stream_error() -> cc_lb_lifecycle::StreamError {
    cc_lb_lifecycle::StreamError {
        error_type: UPSTREAM_AFFINITY_ERROR_TYPE.to_owned(),
        error_message: UPSTREAM_AFFINITY_UNAVAILABLE_MESSAGE.to_owned(),
        ..Default::default()
    }
}

/// Provider-owned state a successful Anthropic SSE event introduces when the
/// request carried no affinity hint. Only the events that can create state are
/// inspected, malformed event data carries no usable state, and keys already
/// bound for this response are skipped, so stateless events keep the exact
/// ungated passthrough.
fn unsolicited_anthropic_sse_affinity_keys(
    event: &usage_parser::ParsedSseEvent<'_>,
    principal_id: &str,
    bound_keys: &HashSet<UpstreamAffinityKey>,
) -> Result<Vec<UpstreamAffinityKey>, UpstreamAffinityExtractionError> {
    if !matches!(
        event.event_type(),
        Some("content_block_start" | "message_start" | "message_delta")
    ) {
        return Ok(Vec::new());
    }
    let keys = match extract_response_affinity_keys_from_sse_event(
        ProviderCapability::AnthropicMessages,
        event,
        principal_id,
    ) {
        Ok(keys) => keys,
        Err(UpstreamAffinityExtractionError::InvalidSseData) => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    Ok(keys
        .into_iter()
        .filter(|key| !bound_keys.contains(key))
        .collect())
}

/// JSON key fragments every provider-state field the unsolicited Anthropic
/// extractor binds must contain: `encrypted_content`, `container` (which also
/// covers the `container_upload` block type), and `file_id`. `\u` covers keys
/// spelled with JSON escapes. Event bytes holding none of them cannot introduce
/// provider state, so they keep the raw chunk passthrough.
const ANTHROPIC_PROVIDER_STATE_MARKERS: [&[u8]; 4] =
    [b"encrypted_content", b"container", b"file_id", b"\\u"];

/// Already-forwarded bytes of a partial event re-scanned together with the
/// next chunk, so a marker split across upstream chunks is detected before
/// the bytes that follow it are forwarded.
const ANTHROPIC_PROVIDER_STATE_MARKER_PROBE_BYTES: usize = b"encrypted_content".len() - 1;

/// Whether unforwarded SSE bytes may belong to an event that introduces
/// unsolicited Anthropic provider state and therefore must be held until the
/// event completes and its state is bound.
fn sse_bytes_may_carry_anthropic_provider_state(bytes: &[u8]) -> bool {
    ANTHROPIC_PROVIDER_STATE_MARKERS
        .iter()
        .any(|marker| bytes.windows(marker.len()).any(|window| window == *marker))
}

/// Whether an SSE frame carries user-visible streamed content for its wire
/// protocol. This is deliberately provider-aware: relay/usage/terminal
/// processing still sees every parsed frame, while telemetry counts only
/// semantic text frames.
fn is_semantic_content_frame(
    capability: ProviderCapability,
    event: &usage_parser::ParsedSseEvent<'_>,
) -> bool {
    let value = event.value().ok().flatten();
    let non_empty_string = |value: Option<&Value>| {
        value
            .and_then(Value::as_str)
            .is_some_and(|text| !text.is_empty())
    };

    match capability.usage_shape() {
        crate::provider_capability::UsageShape::AnthropicMessages => {
            event.event_name() == Some(b"content_block_delta")
        }
        crate::provider_capability::UsageShape::OpenaiChatCompletions => value
            .and_then(|value| value.get("choices"))
            .and_then(Value::as_array)
            .is_some_and(|choices| {
                choices.iter().any(|choice| {
                    non_empty_string(choice.get("delta").and_then(|delta| delta.get("content")))
                })
            }),
        crate::provider_capability::UsageShape::OpenaiCompletions => value
            .and_then(|value| value.get("choices"))
            .and_then(Value::as_array)
            .is_some_and(|choices| {
                choices
                    .iter()
                    .any(|choice| non_empty_string(choice.get("text")))
            }),
        crate::provider_capability::UsageShape::OpenaiResponses => {
            event.event_type() == Some("response.output_text.delta")
                && non_empty_string(value.and_then(|value| value.get("delta")))
        }
        crate::provider_capability::UsageShape::GeminiGenerateContent => {
            let candidates = value
                .and_then(|value| value.get("candidates"))
                .or_else(|| value.and_then(|value| value.get("response")?.get("candidates")));
            candidates
                .and_then(Value::as_array)
                .is_some_and(|candidates| {
                    candidates.iter().any(|candidate| {
                        candidate
                            .get("content")
                            .and_then(|content| content.get("parts"))
                            .and_then(Value::as_array)
                            .is_some_and(|parts| {
                                parts.iter().any(|part| {
                                    !part
                                        .get("thought")
                                        .and_then(Value::as_bool)
                                        .unwrap_or(false)
                                        && non_empty_string(part.get("text"))
                                })
                            })
                    })
                })
        }
        _ => false,
    }
}

/// Whether a successful SSE event is the provider's terminal success event,
/// before which request-referenced provider state still pending a binding must
/// be persisted. Gemini streams carry no terminal event, so their pending keys
/// are bound once the upstream stream completes cleanly.
fn sse_event_completes_response(
    capability: ProviderCapability,
    event: &usage_parser::ParsedSseEvent<'_>,
    _anthropic_message_stop: bool,
) -> bool {
    match capability.usage_shape() {
        crate::provider_capability::UsageShape::AnthropicMessages => {
            event
                .value()
                .ok()
                .flatten()
                .and_then(|value| value.get("type"))
                .and_then(serde_json::Value::as_str)
                == Some("message_stop")
        }
        crate::provider_capability::UsageShape::OpenaiResponses => {
            event.value().ok().flatten().is_some_and(|_| {
                event.event_name() == Some(b"response.completed")
                    || event.event_name() == Some(b"response.incomplete")
            }) || event
                .value()
                .ok()
                .flatten()
                .and_then(|value| value.get("type"))
                .and_then(serde_json::Value::as_str)
                .is_some_and(|event_type| {
                    event_type == "response.completed" || event_type == "response.incomplete"
                })
        }
        crate::provider_capability::UsageShape::OpenaiChatCompletions
        | crate::provider_capability::UsageShape::OpenaiCompletions => {
            event.is_done() || event.has_finished_choices()
        }
        crate::provider_capability::UsageShape::GeminiGenerateContent
        | crate::provider_capability::UsageShape::GeminiEmbed
        | crate::provider_capability::UsageShape::OpenaiEmbeddings
        | crate::provider_capability::UsageShape::OpenaiAudio
        | crate::provider_capability::UsageShape::OpenaiImages
        | crate::provider_capability::UsageShape::None => false,
    }
}

/// Transforms can drop or rewrite the upstream terminal event. Only their
/// complete emitted terminal frames authorize successful downstream closure.
fn transformed_sse_completes_response(capability: ProviderCapability, mut bytes: &[u8]) -> bool {
    while let Some(end) = usage_parser::find_sse_event_end(bytes) {
        let event = parse_sse_event(&bytes[..end]);
        if sse_event_completes_response(capability, &event, false) {
            return true;
        }
        bytes = &bytes[end..];
    }
    false
}

/// JSON `type` of the provider's terminal success SSE event, used by the #663
/// accept-with-warning path. Gemini streams have no terminal event.
const fn semantic_terminal_event_type(capability: ProviderCapability) -> Option<&'static str> {
    match capability.usage_shape() {
        crate::provider_capability::UsageShape::AnthropicMessages => Some("message_stop"),
        crate::provider_capability::UsageShape::OpenaiResponses => Some("response.completed"),
        crate::provider_capability::UsageShape::OpenaiChatCompletions
        | crate::provider_capability::UsageShape::OpenaiCompletions
        | crate::provider_capability::UsageShape::OpenaiEmbeddings
        | crate::provider_capability::UsageShape::OpenaiAudio
        | crate::provider_capability::UsageShape::OpenaiImages
        | crate::provider_capability::UsageShape::GeminiGenerateContent
        | crate::provider_capability::UsageShape::GeminiEmbed
        | crate::provider_capability::UsageShape::None => None,
    }
}

fn observe_sse_terminal_event(
    capability: ProviderCapability,
    event: &usage_parser::ParsedSseEvent<'_>,
    usage_message_stop: bool,
    semantic_message_stop_seen: &mut bool,
    message_stop_at: &mut Option<Instant>,
    now: impl FnOnce() -> Instant,
) {
    // OpenAI Responses providers identify terminal events in the SSE event
    // name even when the JSON payload omits its redundant `type` field.
    // Anthropic keeps the stricter JSON-type-only rule so a named
    // `message_stop` event cannot turn malformed payloads into completion.
    let event_name_terminal = matches!(
        capability.usage_shape(),
        crate::provider_capability::UsageShape::OpenaiResponses
    ) && (event.event_name() == Some(b"response.completed")
        || event.event_name() == Some(b"response.incomplete"))
        && event.value().ok().flatten().is_some();
    let semantic_event_type = event
        .value()
        .ok()
        .flatten()
        .and_then(|value| value.get("type"))
        .and_then(serde_json::Value::as_str);
    let response_terminal_type = matches!(
        capability.usage_shape(),
        crate::provider_capability::UsageShape::OpenaiResponses
    ) && semantic_event_type.is_some_and(|event_type| {
        event_type == "response.completed" || event_type == "response.incomplete"
    });
    *semantic_message_stop_seen |= event_name_terminal
        || response_terminal_type
        || semantic_terminal_event_type(capability)
            .is_some_and(|terminal| semantic_event_type == Some(terminal));
    if event.event_name() == Some(b"message_stop")
        || (usage_message_stop && message_stop_at.is_none())
    {
        *message_stop_at = Some(now());
    }
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
        crate::bulkhead::dispatch_with_client(&self.client, request).await
    }
}

const THREAD_STICKY_SUBAGENT_SHARD_COUNT: usize = 16;
const MAX_THREAD_STICKY_SUBAGENTS_PER_SHARD: usize = 512;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ThreadStickySubagentKey {
    principal_id: String,
    observed_session_id: String,
}

#[derive(Clone)]
struct ThreadStickySubagentLatch {
    next_sequence: Arc<AtomicU64>,
    shards: Arc<[Mutex<HashMap<ThreadStickySubagentKey, u64>>; THREAD_STICKY_SUBAGENT_SHARD_COUNT]>,
}

impl ThreadStickySubagentLatch {
    fn new() -> Self {
        Self {
            next_sequence: Arc::new(AtomicU64::new(0)),
            shards: Arc::new(std::array::from_fn(|_| Mutex::new(HashMap::new()))),
        }
    }

    fn shard_index(key: &ThreadStickySubagentKey) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % THREAD_STICKY_SUBAGENT_SHARD_COUNT
    }

    fn observe(&self, key: ThreadStickySubagentKey, seed: bool) -> bool {
        let shard = &self.shards[Self::shard_index(&key)];
        let mut shard = shard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if seed {
            let sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed) + 1;
            if !shard.contains_key(&key)
                && shard.len() >= MAX_THREAD_STICKY_SUBAGENTS_PER_SHARD
                && let Some(oldest_key) = shard
                    .iter()
                    .min_by_key(|(_, sequence)| **sequence)
                    .map(|(key, _)| key.clone())
            {
                shard.remove(&oldest_key);
            }
            shard.insert(key, sequence);
            true
        } else {
            shard.contains_key(&key)
        }
    }
}

impl Default for ThreadStickySubagentLatch {
    fn default() -> Self {
        Self::new()
    }
}

fn apply_thread_sticky_subagent_latch(
    latch: &ThreadStickySubagentLatch,
    principal_id: &str,
    value: Option<&Value>,
    metadata: &mut RequestCacheMetadata,
) {
    if metadata.claude_agent_id.is_some() || metadata.explicit_request_class.is_some() {
        return;
    }
    let Some(observed_session_id) = metadata
        .observed_session_id
        .as_deref()
        .or(metadata.thread_id.as_deref())
    else {
        return;
    };
    let key = ThreadStickySubagentKey {
        principal_id: principal_id.to_owned(),
        observed_session_id: observed_session_id.to_owned(),
    };
    if metadata.request_kind.as_deref() == Some("subagent")
        && value.is_some_and(|value| {
            is_thread_sticky_subagent_seed(
                value,
                Some(observed_session_id),
                metadata.parent_session_id.as_deref(),
            )
        })
    {
        latch.observe(key, true);
    } else if matches!(metadata.request_kind.as_deref(), Some("main" | "unknown"))
        && latch.observe(key, false)
    {
        metadata.request_kind = Some("subagent".to_owned());
    }
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
    cache_keepalive_enqueuer: Option<Arc<dyn crate::cache_keepalive::CacheKeepaliveEnqueuer>>,
    cache_keepalive: crate::cache_keepalive::LifecycleKeepalive,
    thread_sticky_subagent_latch: ThreadStickySubagentLatch,
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
            cache_keepalive_enqueuer: None,
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
        self.cache_keepalive = self.cache_keepalive.with_enqueuer(Arc::clone(&enqueuer));
        self.cache_keepalive_enqueuer = Some(enqueuer);
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

    pub async fn preview_route(
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
        let method = http::Method::POST;
        let request_capability = classify_provider_capability(&method, &input.path, input.provider)
            .ok_or_else(|| PreviewRouteError::UnsupportedOperation {
                method: method.to_string(),
                path: input.path.clone(),
            })?;
        let request_kind = RequestKind::from(request_capability);
        let path_model = gemini_model_from_path(&input.path);
        let prompt_cache_analysis = match (request_capability, body_value.as_ref()) {
            (ProviderCapability::AnthropicMessages, Some(_))
            | (ProviderCapability::AnthropicMessages, None) => None,
            (_, Some(_)) => Some(PromptCacheAnalysisOutput {
                analysis: V3PromptCacheAnalysis {
                    blocks: Vec::new(),
                    breakpoints: Vec::new(),
                    literal_breakpoints: Vec::new(),
                },
                cacheable_breakpoint_prefix_keys: Vec::new(),
                literal_cacheable_breakpoint_prefix_keys: Vec::new(),
                timings: PromptCacheAnalysisTimings::default(),
            }),
            (_, None) => None,
        };
        let cache_metadata = request_cache_metadata_from_value_with_analysis_for_capability(
            &input.headers,
            body_value.as_ref(),
            prompt_cache_analysis,
            Some(request_capability),
        );
        let canonical_model = path_model
            .as_deref()
            .map(canonical_model_id)
            .unwrap_or(&cache_metadata.canonical_model_id)
            .to_owned();
        let cache_breakpoints = cache_metadata.plugin_cache_breakpoints();

        let request_id = input
            .request_id
            .clone()
            .unwrap_or_else(|| format!("preview-{}", Uuid::new_v4()));
        let ctx = RequestContext::builder()
            .request_id(request_id)
            .thread_id(cache_metadata.thread_id.clone())
            .requested_service_tier(cache_metadata.requested_service_tier.clone())
            .provider_scope(input.provider)
            .downstream_headers(input.headers)
            .method(method)
            .path(input.path)
            .query(None)
            .body_bytes(input.body_bytes)
            .cache_breakpoints(cache_breakpoints)
            .canonical_model_id(canonical_model.clone())
            .cache_pricing(cache_pricing_summary_for_model(
                cc_lb_pricing::global_catalog(),
                &canonical_model,
                cache_metadata.requested_service_tier.as_deref(),
            ))
            .build();
        let principal = Principal {
            id: input.principal_id,
            kind: PrincipalKind::InternalKey,
        };

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
        let candidates = build_candidates_with_native_cache_for_bundles(
            &view,
            &principal.id,
            request_kind,
            &ctx.cache_breakpoints,
            &warm_entries,
            Some(&cache_metadata.bundles),
            &native_cache_scores,
            &*self.clock,
        )
        .0;
        let pipeline_result = execute_filter_pipeline(
            resolved_pipeline.filters_for(request_capability.provider()),
            &ctx,
            &principal,
            candidates,
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
                message: "upstream affinity is not established for opaque request content",
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

    /// Route and reserve one logical create; never dispatch an HTTP request.
    /// The gateway records authentication before parsing any client frame.
    pub(crate) async fn prepare_websocket_turn(
        &self,
        auth: &crate::Authenticated,
        ingress: WebSocketIngress<'_>,
        body: Arc<Value>,
        owner: Option<Uuid>,
        observer: Option<LifecycleContext>,
        request_id: &str,
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
            observer.set_request_span(span.clone());
            observer.set_response_stream_span(stream_span.clone());
            observer.emit_request_started(true);
        }
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
            let identity = request_identity_metadata(ingress.headers, Some(body.as_ref()));
            if let Some(observer) = observer.as_ref() {
                observer.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::ParseCompleted {
                    event_id: observer.event_id().to_owned(),
                    result: Ok(cc_lb_lifecycle::ParseInfo {
                        path: "/v1/responses".to_owned(),
                        method: "GET".to_owned(),
                        model: body.get("model").and_then(Value::as_str).map(str::to_owned),
                        stream: true,
                        body_bytes: ingress.frame.len() as u64,
                        cache_control_block_count: None,
                        thinking_budget_tokens: None,
                        reasoning_effort: body
                            .pointer("/reasoning/effort")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        thinking_level: None,
                        cache_breakpoints: Vec::new(),
                        cache_prefix_hash: None,
                        thread_id: identity.thread_id.clone(),
                        observed_session_id: identity.observed_session_id.clone(),
                        request_kind: identity.request_kind.clone(),
                        claude_agent_id: identity.claude_agent_id.clone(),
                        claude_parent_agent_id: identity.claude_parent_agent_id.clone(),
                        parent_session_id: identity.parent_session_id.clone(),
                        client_app: identity.client_app.clone(),
                        session_id_source: identity.session_id_source.clone(),
                        message_id: None,
                        message_index: None,
                        message_count: None,
                        cache_control_message_indices: Vec::new(),
                        native_cache: provider_cache::analyze(
                            ProviderCapability::OpenaiResponses,
                            body.as_ref(),
                            body.get("model")
                                .and_then(Value::as_str)
                                .unwrap_or_default(),
                            ingress.headers,
                        ),
                    }),
                });
            }
            let route_started = Instant::now();
            let (ctx, mut pipeline_result) = self
                .websocket_routing(
                    &view,
                    auth,
                    ingress,
                    body.as_ref(),
                    owner,
                    request_id.to_owned(),
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
            let model = body
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
                    body.as_ref(),
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
                parsed: Ok(body.clone()),
                json_parse_ms: 0.0,
            };
            let principal = Principal {
                id: auth.principal_id().to_owned(),
                kind: PrincipalKind::InternalKey,
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
            if let Some(observer) = observer.as_ref() {
                observer.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::LimitDecision {
                    event_id: observer.event_id().to_owned(),
                    decision: cc_lb_lifecycle::LimitDecisionKind::Reserved {
                        reservation_id: active_limit
                            .as_ref()
                            .and_then(|limit| limit.reservation.as_ref())
                            .map(|reservation| reservation.id().to_owned())
                            .unwrap_or_default(),
                        amount: active_limit.as_ref().map_or(0, |limit| {
                            limit.request.max_tokens.unwrap_or(0).max(0) as u64
                        }),
                        limit_reserve_ms: Some(duration_to_ms(reserve_started.elapsed())),
                    },
                });
            }
            let mut candidates = Vec::with_capacity(pipeline_result.candidates.len());
            candidates.push(WebSocketCandidate {
                upstream_id: selected_id,
                upstream_name: record.name.clone(),
            });
            candidates.extend(
                pipeline_result
                    .candidates
                    .into_iter()
                    .filter(|candidate| candidate.upstream_id != selected_id)
                    .map(|candidate| WebSocketCandidate {
                        upstream_id: candidate.upstream_id,
                        upstream_name: candidate.name,
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
                principal_id: auth.principal_id().to_owned(),
                key_id: auth.key_id().to_owned(),
                model,
                store: body.get("store").and_then(Value::as_bool).unwrap_or(true),
                span: stream_span.clone(),
                request_id: request_id.to_owned(),
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

    /// Recheck a retained connection against the current view without reserving again.
    pub(crate) async fn validate_websocket_turn(
        &self,
        auth: &crate::Authenticated,
        ingress: WebSocketIngress<'_>,
        body: &Value,
        upstream_id: Uuid,
    ) -> Result<(), WebSocketError> {
        let view = self.dynamic_view.load();
        self.websocket_routing(
            &view,
            auth,
            ingress,
            body,
            Some(upstream_id),
            next_request_id(),
        )
        .await
        .map(|_| ())
    }

    async fn websocket_routing(
        &self,
        view: &DynamicView,
        auth: &crate::Authenticated,
        ingress: WebSocketIngress<'_>,
        body: &Value,
        owner: Option<Uuid>,
        request_id: String,
    ) -> Result<(RequestContext, FilterPipelineResult), WebSocketError> {
        use cc_lb_control::api_keys::principal_view::PrincipalStatus;
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
        let mut keys = extract_request_affinity_keys(
            ProviderCapability::OpenaiResponses,
            body,
            auth.principal_id(),
        )
        .map_err(|_| {
            WebSocketError::new(
                400,
                "invalid_request_error",
                UPSTREAM_AFFINITY_KEY_LIMIT_MESSAGE,
            )
        })?;
        // A response id owned by this live connection need not exist in durable storage.
        // Other recognized state still has to agree with that connection's owner.
        if owner.is_some() {
            keys.retain(|key| {
                key.kind != cc_lb_storage_api::UpstreamAffinityKind::OpenaiResponseId
            });
        }
        let affinity = self
            .resolve_request_affinities(&keys)
            .await
            .map_err(|failure| {
                WebSocketError::new(
                    failure.status.as_u16(),
                    error_codes::UPSTREAM_AFFINITY_UNAVAILABLE,
                    failure.message,
                )
            })?;
        let target = match (
            owner,
            affinity
                .as_ref()
                .map(|affinity| affinity.target_upstream_id),
        ) {
            (Some(owner), Some(affinity)) if owner != affinity => {
                return Err(WebSocketError::new(
                    400,
                    error_codes::UPSTREAM_AFFINITY_UNAVAILABLE,
                    "provider state belongs to a different upstream",
                ));
            }
            (owner, affinity) => owner.or(affinity),
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
        let canonical_model = canonical_model_id(model).to_owned();
        let service_tier = body.get("service_tier").and_then(Value::as_str);
        let identity = request_identity_metadata(ingress.headers, Some(body));
        let ctx = RequestContext::builder()
            .request_id(request_id)
            .thread_id(identity.thread_id)
            .downstream_headers(downstream_headers)
            .method(http::Method::GET)
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
        if let Some(target) = target {
            candidates.retain(|candidate| candidate.upstream_id == target);
        }
        let mut result = execute_filter_pipeline(
            pipeline.filters_for(cc_lb_domain::Provider::Openai),
            &ctx,
            &principal,
            candidates,
        );
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
        Ok((ctx, result))
    }

    /// Match HTTP Responses shaping while keeping the WebSocket URL and method
    /// host-owned. The returned headers apply only when opening a connection.
    pub(crate) fn shape_websocket_frame(
        &self,
        auth: &crate::Authenticated,
        ingress: WebSocketIngress<'_>,
        upstream_id: Uuid,
        request_id: &str,
        observer: Option<&LifecycleContext>,
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
        let Some(dialect) = cached.resolved_dialect(upstream_id) else {
            return Ok(None);
        };
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
        let upstream = upstream_for_record(record).map_err(|message| {
            WebSocketError::new(502, error_codes::ROUTE_NOT_CONFIGURED, message)
        })?;
        let mut downstream_headers = ingress.headers.clone();
        strip_hop_by_hop(&mut downstream_headers);
        let context = DialectShapeContext {
            request_id: request_id.to_owned(),
            downstream_headers,
            method: http::Method::POST,
            path: "/v1/responses".to_owned(),
            query: ingress.query.map(str::to_owned),
            body_bytes: ingress.frame.clone(),
        };
        // BuiltinAuthn authenticates managed cc-lb keys, not provider keys or
        // OAuth subjects. Human/machine/admin is a separate storage taxonomy.
        let principal = Principal {
            id: auth.principal_id().to_owned(),
            kind: PrincipalKind::InternalKey,
        };
        let (frame, headers) =
            match shape_request(dialect.as_ref(), &context, &upstream, &principal) {
                Ok(shaped) => (shaped.body().clone(), shaped.headers().clone()),
                Err(error) => {
                    tracing::warn!(%error, "websocket shape_request failed; forwarding raw frame");
                    record_shape_internal_error(observer, &error.to_string());
                    (ingress.frame.clone(), context.downstream_headers)
                }
            };
        Ok(Some(WebSocketPluginRequest {
            frame,
            headers,
            context: ResponseTransformContext {
                principal,
                upstream,
                request_method: http::Method::POST,
                request_path: "/v1/responses".to_owned(),
                dialect: dialect.clone(),
                gemini_oauth: false,
            },
        }))
    }

    /// Sign only a newly opened upstream connection, using its current credentials.
    pub(crate) async fn sign_websocket_connection(
        &self,
        auth: &crate::Authenticated,
        headers: &HeaderMap,
        candidate: &WebSocketCandidate,
        query: Option<&str>,
    ) -> Result<(Url, HeaderMap), WebSocketError> {
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
        let signer = factory.build(&upstream).await.map_err(|error| {
            WebSocketError::new(502, "upstream_signing_failed", error.to_string())
        })?;
        let signed = sign_request(signer.as_ref(), shaped)
            .await
            .map_err(|error| {
                WebSocketError::new(502, "upstream_signing_failed", error.to_string())
            })?;
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
        Ok((url, headers))
    }

    /// Persist recognized state before the gateway exposes it to another connection.
    pub(crate) async fn bind_websocket_response(
        &self,
        auth: &crate::Authenticated,
        upstream_id: Uuid,
        response: &Value,
        store: bool,
    ) -> Result<(), WebSocketError> {
        let mut keys = extract_response_affinity_keys(
            ProviderCapability::OpenaiResponses,
            response,
            auth.principal_id(),
        )
        .map_err(|_| {
            WebSocketError::new(
                502,
                error_codes::UPSTREAM_AFFINITY_UNAVAILABLE,
                "upstream response carries invalid provider state",
            )
        })?;
        if !store {
            // Only unstored response ids are connection-local; conversations
            // retain their existing durable ownership across connections.
            keys.retain(|key| {
                key.kind != cc_lb_storage_api::UpstreamAffinityKind::OpenaiResponseId
            });
        }
        let context = ResponseAffinityContext {
            capability: ProviderCapability::OpenaiResponses,
            store: self.upstream_affinity_store.clone(),
            principal_id: auth.principal_id().to_owned(),
            upstream_id,
            pending_request_keys: Vec::new(),
            observe_response_keys: true,
            owner_epoch: Some(0),
            ttl_secs: self.config.upstream_affinity_ttl.as_secs(),
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
        let multipart = is_multipart_operation(&ctx.path)
            && ctx
                .downstream_headers
                .get(http::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| {
                    value
                        .split(';')
                        .next()
                        .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("multipart/form-data"))
                });
        let empty_body = Bytes::new();
        let body_view = RequestBodyView::new(if multipart {
            &empty_body
        } else {
            &ctx.body_bytes
        });
        let path_model = if multipart && body_too_large.is_none() {
            multipart_request_model(auth, &ctx.downstream_headers, &ctx.body_bytes).await
        } else {
            gemini_model_from_path(&ctx.path)
        };
        let request_model = path_model.clone().or_else(|| body_view.model());
        let request_capability =
            classify_provider_capability(&ctx.method, &ctx.path, ctx.provider_scope);
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
            let response = anthropic_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "api_error",
                "authenticated principal is unavailable",
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
        if !ctx.body_bytes.is_empty() && !body_view.is_valid_json() && !multipart {
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
                    .map(canonical_model_id)
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
        );
        if request_capability == ProviderCapability::GeminiCachedContentsCreate
            && let Some(model) = cache_metadata.canonical_model_id.strip_prefix("models/")
        {
            cache_metadata.canonical_model_id = model.to_owned();
        }
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
                    cache_control_block_count: cache_metadata.cache_control_block_count,
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
                }),
            });
        }
        ctx.cache_breakpoints = cache_metadata.plugin_cache_breakpoints();
        ctx.canonical_model_id = path_model
            .as_deref()
            .map(canonical_model_id)
            .unwrap_or(&cache_metadata.canonical_model_id)
            .to_owned();
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
            Ok(keys) => keys.unwrap_or_default(),
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
        let request_affinity = match self
            .resolve_request_affinities(&request_affinity_keys)
            .await
        {
            Ok(resolution) => resolution,
            Err(failure) => {
                if let Some(o) = observer.as_ref() {
                    o.terminate_failure(InternalFailure {
                        status: failure.status,
                        error_code: error_codes::UPSTREAM_AFFINITY_UNAVAILABLE,
                        error: InternalError {
                            stage: InternalErrorStage::Storage,
                            kind: InternalErrorKind::Unavailable,
                            message: Some(failure.message.to_owned()),
                        },
                    });
                }
                return Ok(failure.response(request_capability));
            }
        };
        let keepalive_response_enabled =
            self.cache_keepalive_enqueuer.is_some() && cached.cache_keepalive().is_some();
        let track_keepalive_response = keepalive_response_enabled
            && (cache_metadata.cache_keepalive_surface.is_some()
                || !cache_metadata.bundles.official.cache_breakpoints.is_empty()
                || !cache_metadata.bundles.literal.cache_breakpoints.is_empty());
        if track_keepalive_response {
            cache_metadata.request_json = body_view.value().cloned();
        }

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
        if let Some(affinity) = request_affinity.as_ref() {
            candidates.retain(|candidate| candidate.upstream_id == affinity.target_upstream_id);
            selected_cache_matches
                .retain(|upstream_id, _| *upstream_id == affinity.target_upstream_id);
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
            observer: observer.as_ref(),
            handle_span: &handle_span,
            enabled: request_affinity.is_none(),
            excluded: Vec::new(),
            upstream_attempts: 1,
        };
        let mut attempt_num: u32 = 0;
        let mut first_dispatch: Option<(Instant, u64)> = None;
        let mut attempt_timings = AttemptTimings::default();
        let mut keepalive_shaped_body = None;
        let mut keepalive_request_dispatched_at = None;
        let affinity_owner_epoch_required = request_capability
            == ProviderCapability::GeminiCachedContentsCreate
            || (request_capability.provider() == cc_lb_domain::Provider::Gemini
                && request_affinity.is_some());
        let mut routed_owner_epoch;
        let (mut response, dispatch_started, dispatch_unix_secs, signer) = loop {
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
                let request = body_view.limit_request(
                    &ctx.canonical_model_id,
                    classify_provider_capability(&ctx.method, &ctx.path, ctx.provider_scope),
                    &routed.cache_bundle.cache_breakpoints,
                    use_image_rates_for_admission(
                        classify_provider_capability(&ctx.method, &ctx.path, ctx.provider_scope),
                        &ctx.path,
                    ),
                );
                (request != limit.request).then_some(request)
            });
            if !failover_entry || replacement_request.is_some() {
                // Every prior attempt scope has ended. Its pre-processing
                // failure is unbilled, so RAII refunds that hold in full before
                // the replacement is checked; never hold both estimates.
                drop(reserved.take());
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
                if let Some(o) = observer.as_ref() {
                    o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::LimitDecision {
                        event_id: o.event_id().to_owned(),
                        decision: cc_lb_lifecycle::LimitDecisionKind::Reserved {
                            reservation_id: active_limit
                                .as_ref()
                                .and_then(|limit| limit.reservation.as_ref())
                                .map(|reservation| reservation.id().to_owned())
                                .unwrap_or_default(),
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
            let mut response = match self
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
                    &mut keepalive_request_dispatched_at,
                )
                .await
            {
                Ok(response) => response,
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
                        let mut response = response_from_collected(unauthorized);
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
                        response_from_collected(unauthorized),
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
                response = match self
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
                        &mut keepalive_request_dispatched_at,
                    )
                    .await
                {
                    Ok(response) => response,
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
                };
                // Everything the refreshed attempt returns is scrubbed with
                // the signer that produced it.
                signer = new_signer;
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
                        let mut response = response_from_collected(unauthorized);
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
                    response = response_from_collected(unauthorized);
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
        let keepalive_shaped_body = if track_keepalive_response {
            keepalive_shaped_body
        } else if keepalive_response_enabled {
            Some(ctx.body_bytes.clone())
        } else {
            None
        };
        let keepalive_completion = keepalive_shaped_body.map(|shaped_body| {
            crate::cache_keepalive::LifecycleKeepaliveContext {
                principal: principal.clone(),
                accounting_key_id: success.key_id.clone(),
                cache_metadata: cache_metadata.clone(),
                upstream_id: resolved_upstream_id,
                shaped_body,
                original_body: ctx.body_bytes.clone(),
                downstream_headers: ctx.downstream_headers.clone(),
                request_dispatched_at: keepalive_request_dispatched_at.unwrap_or(dispatch_started),
            }
        });
        let response_affinity_context = ResponseAffinityContext {
            capability: request_capability,
            store: self.upstream_affinity_store.clone(),
            principal_id: principal.id.clone(),
            upstream_id: resolved_upstream_id,
            pending_request_keys: request_affinity
                .map(|affinity| affinity.pending_keys)
                .unwrap_or_default(),
            observe_response_keys: body_view
                .value()
                .is_some_and(|value| request_observes_response_affinity(request_capability, value)),
            owner_epoch: routed_owner_epoch,
            ttl_secs: self.config.upstream_affinity_ttl.as_secs(),
        };
        response = self
            .finish_success_response(
                response,
                signer,
                body_view.stream(),
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
                },
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
        let max_input_estimate = limit_request.input_tokens_estimate;
        let cost_estimate = if limit_request.model.is_empty() {
            Some(0)
        } else {
            self.limit_cost_estimator.as_ref().and_then(|estimator| {
                if let Some(input) = limit_request.local_cache_input {
                    estimator.estimate_max_with_local_cache_and_images(
                        &limit_request.model,
                        input,
                        limit_request.max_tokens.unwrap_or(0).max(0) as u64,
                        limit_request.use_image_rates,
                        limit_request.service_tier.as_deref(),
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
                        limit_request.service_tier.as_deref(),
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
        response: Response<Body>,
        signer: Arc<dyn Signer>,
        stream_requested: bool,
        active_limit: Option<ActiveLimit>,
        response_accounting_guard: Option<ResponseAccountingGuard>,
        duration: Duration,
        status: StatusCode,
        event_ctx: RequestEventContext,
        transform_ctx: ResponseTransformContext,
        prompt_cache_observation_context: Option<PromptCacheObservationContext>,
        response_affinity_context: ResponseAffinityContext,
        observer: Option<LifecycleContext>,
        keepalive_completion: Option<crate::cache_keepalive::LifecycleKeepaliveContext>,
        dispatch_started: Instant,
        dispatch_unix_secs: u64,
    ) -> Response<Body> {
        // Audio endpoints also stream binary/text payloads without a JSON
        // `stream` flag. Keep their JSON responses and ordinary JSON-protocol
        // operations on the existing buffered path.
        let audio_stream = response_affinity_context.capability == ProviderCapability::OpenaiAudio
            && !response
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.split(';').next())
                .is_some_and(|mime| {
                    let mime = mime.trim();
                    mime.eq_ignore_ascii_case("application/json")
                        || mime
                            .rsplit_once('+')
                            .is_some_and(|(_, suffix)| suffix.eq_ignore_ascii_case("json"))
                });
        let stream_response = stream_requested || audio_stream;
        let buffered_transform_required =
            transform_ctx.gemini_oauth || transform_ctx.dialect.response_transform_hook().is_some();
        let force_buffered_response =
            response_affinity_context.capability == ProviderCapability::GeminiCachedContentsCreate;
        if !force_buffered_response
            && (is_sse_response(
                response.headers(),
                stream_requested,
                response_affinity_context.capability,
            ) || (stream_response && !buffered_transform_required))
        {
            let provider = response_affinity_context.capability.provider();
            let response_status = response.status();
            let mut response = self.relay_response(
                response,
                signer,
                response_status,
                stream_requested,
                response_accounting_guard,
                event_ctx.clone(),
                transform_ctx,
                prompt_cache_observation_context,
                response_affinity_context,
                observer,
                keepalive_completion,
                dispatch_started,
                dispatch_unix_secs,
            );
            self.attach_limit_headers(&mut response, None, provider);
            return response;
        }

        let (mut parts, mut body) = response.into_parts();
        strip_hop_by_hop(&mut parts.headers);
        let body_collect_started = Instant::now();
        let response_body_timing = BodyIoTiming::default();
        let mut response_body_timing_guard =
            BodyIoTimingObserverGuard::new(observer.clone(), response_body_timing.clone());
        response_body_timing.transition(Some(BodyIoPhase::Process));
        let mut first_body_chunk_at: Option<Instant> = None;
        let mut body_buf: Vec<u8> = Vec::new();
        let mut body_chunk_count: u64 = 0;
        // Preserve uncapped ordinary responses. Buffering a requested stream
        // or an audio stream for a transform uses the request path's cap.
        let body_cap = if stream_response {
            body_cap_for_path(&self.config, &transform_ctx.request_path)
        } else {
            usize::MAX
        };
        let mut body_collect_failed = false;
        while let Some(frame) = timed_body_frame(&mut body, &response_body_timing).await {
            match frame {
                Ok(frame) => {
                    if let Ok(data) = frame.into_data() {
                        if first_body_chunk_at.is_none() {
                            first_body_chunk_at = Some(Instant::now());
                            if let Some(o) = observer.as_ref() {
                                o.set_termination_timings(
                                    None,
                                    None,
                                    Some(duration_to_ms(body_collect_started.elapsed())),
                                );
                            }
                        }
                        body_chunk_count = body_chunk_count.saturating_add(1);
                        if data.len() > body_cap.saturating_sub(body_buf.len()) {
                            body_collect_failed = true;
                            break;
                        }
                        body_buf.extend_from_slice(&data);
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
        let semantic_body = if body_collect_failed {
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
        let gemini_unwrapped_body = transform_ctx
            .gemini_oauth
            .then_some(semantic_body.as_ref())
            .flatten()
            .and_then(|body| unwrap_gemini_code_assist_body(body).ok().flatten());
        let semantic_body = gemini_unwrapped_body.clone().or(semantic_body);
        let response_body_json = semantic_body
            .as_ref()
            .and_then(|body| sonic_rs::from_slice::<Value>(body).ok());
        let observation = response_body_json.as_ref().map_or_else(
            usage_parser::NonStreamObservation::default,
            |value| {
                usage_parser::observe_non_stream_json_value_for_shape(
                    value,
                    response_affinity_context.capability.usage_shape(),
                )
            },
        );
        let abnormal_stop = response_body_json
            .as_ref()
            .and_then(usage_parser::detect_abnormal_stop_non_stream);
        let usage = observation.usage;
        let canonical_upstream_error = if status.is_client_error() || status.is_server_error() {
            response_body_json
                .as_ref()
                .and_then(usage_parser::observe_http_error_value)
        } else if response_body_json.as_ref().is_some_and(|value| {
            value.get("object").and_then(Value::as_str) == Some("response")
                && value.get("status").and_then(Value::as_str) == Some("failed")
        }) {
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
        let (upstream_error_type, parsed_upstream_message) = canonical_upstream_error
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
            if response_affinity_context.capability
                == ProviderCapability::GeminiCachedContentsCreate
            {
                provider_error_body(
                    response_affinity_context.capability,
                    parts.status,
                    "api_error",
                    UPSTREAM_BUFFERED_BODY_FAILED_MESSAGE,
                    None,
                )
            } else {
                anthropic_error_body("api_error", UPSTREAM_BUFFERED_BODY_FAILED_MESSAGE)
            }
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
        let mut buffered_transform_error: Option<ResponseTransformError> = None;
        let mut keepalive_transformed_json = None;
        let mut keepalive_body_replaced = false;
        if !body_collect_failed
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
                    if keepalive_completion.is_some()
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
        let mut affinity_bind_failed = false;
        if status.is_success() && !body_collect_failed {
            let mut affinity_keys = response_affinity_context.pending_request_keys.clone();
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
                downstream_body = if response_affinity_context.capability
                    == ProviderCapability::GeminiCachedContentsCreate
                {
                    provider_error_body(
                        response_affinity_context.capability,
                        StatusCode::SERVICE_UNAVAILABLE,
                        "api_error",
                        UPSTREAM_AFFINITY_UNAVAILABLE_MESSAGE,
                        None,
                    )
                } else {
                    anthropic_error_body("api_error", UPSTREAM_AFFINITY_UNAVAILABLE_MESSAGE)
                };
            }
        }
        let client_status = parts.status;
        let keepalive_response_json = if keepalive_body_replaced {
            keepalive_transformed_json.as_ref()
        } else {
            response_body_json.as_ref()
        };
        if !affinity_bind_failed
            && let (Some(context), Some(response_json)) =
                (keepalive_completion, keepalive_response_json)
        {
            self.cache_keepalive.on_response_completed(
                response_json,
                &context.principal,
                context.accounting_key_id,
                &context.cache_metadata,
                context.upstream_id,
                context.shaped_body,
                context.original_body,
                &context.downstream_headers,
                first_body_chunk_at
                    .unwrap_or(context.request_dispatched_at)
                    .elapsed(),
                usage
                    .cache_read_input_tokens
                    .saturating_add(usage.cache_creation_input_tokens),
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
        keepalive_request_dispatched_at: &mut Option<Instant>,
    ) -> Result<Response<Body>, AttemptFailure> {
        let attempt_scope = match reserved {
            Some(reserved) => reserved.begin_attempt(),
            None => Scoped::unreserved_for_lifecycle(),
        };
        let response = self
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
                keepalive_request_dispatched_at,
            )
            .await?;
        if let Some(o) = observer {
            o.set_attempt_timings(
                attempt_timings.shape_ms,
                attempt_timings.sign_ms,
                attempt_timings.upstream_ttfb_ms,
            );
            o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::UpstreamResponseStarted {
                event_id: o.event_id().to_owned(),
                status: response.status().as_u16(),
                headers: header_snapshot_from(response.headers(), routed.record.kind),
                bulkhead_wait_ms: attempt_timings.bulkhead_wait_ms,
                dns_ms: attempt_timings.dns_ms,
                connect_ms: attempt_timings.connect_ms,
                connection_reused: attempt_timings.connection_reused,
                shape_ms: attempt_timings.shape_ms,
                sign_ms: attempt_timings.sign_ms,
                upstream_ttfb_ms: attempt_timings.upstream_ttfb_ms,
            });
        }
        Ok(response)
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
            parts.headers
        };
        let ctx = RequestContext::builder()
            .request_id(request_id)
            .thread_id(None)
            .requested_service_tier(None)
            .provider_scope(provider_scope.map(|scope| scope.0))
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
        keepalive_request_dispatched_at: &mut Option<Instant>,
    ) -> Result<Response<Body>, AttemptFailure> {
        tracing::Span::current().record(
            "cc_lb.upstream.id",
            tracing::field::display(route.upstream_id),
        );
        let shape_start = Instant::now();
        *shaped_body_out = None;
        let shape_context = ctx.dialect_shape_context();
        let (shaped, capture_shaped_body) = match tracing::info_span!("proxy.shape").in_scope(
            || {
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
                (
                    raw_passthrough_request(
                        raw_passthrough_base_url,
                        ctx,
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

        if keepalive_request_dispatched_at.is_none() {
            self.cache_keepalive
                .on_request_started(authenticated, principal, cache_metadata)
                .await;
            *keepalive_request_dispatched_at = Some(Instant::now());
        }

        let dispatch_start = Instant::now();
        let stage_timings_carrier = Arc::new(Mutex::new(RequestStageTimings::default()));
        let dispatch_result = REQUEST_STAGE_TIMINGS
            .scope(stage_timings_carrier.clone(), signed.dispatch(dispatcher))
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
        Ok(response)
    }

    #[allow(clippy::too_many_arguments)]
    fn relay_response(
        &self,
        response: Response<Body>,
        signer: Arc<dyn Signer>,
        status: StatusCode,
        stream_requested: bool,
        response_accounting_guard: Option<ResponseAccountingGuard>,
        event_ctx: RequestEventContext,
        transform_ctx: ResponseTransformContext,
        prompt_cache_observation_context: Option<PromptCacheObservationContext>,
        response_affinity_context: ResponseAffinityContext,
        observer: Option<LifecycleContext>,
        keepalive_completion: Option<crate::cache_keepalive::LifecycleKeepaliveContext>,
        dispatch_started: Instant,
        dispatch_unix_secs: u64,
    ) -> Response<Body> {
        let (mut parts, mut body) = response.into_parts();
        let upstream_diagnostics = UpstreamBodyDiagnostics::from_parts(&parts);
        strip_hop_by_hop(&mut parts.headers);
        let decompression_output_budget_bytes =
            decompression_output_budget_bytes(self.config.messages_body_cap_bytes);
        let incomplete_sse_event_budget_bytes =
            incomplete_sse_event_budget_bytes(self.config.messages_body_cap_bytes);
        let usage_decoder =
            UsageDecoder::from_headers(&parts.headers, decompression_output_budget_bytes);
        let downstream_stream_is_identity = matches!(&usage_decoder, UsageDecoder::Identity);
        let upstream_is_sse = is_sse_response(
            &parts.headers,
            stream_requested,
            response_affinity_context.capability,
        );
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
        let transform_requested = upstream_is_sse
            && (transform_ctx.dialect.sse_event_transform_hook().is_some()
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
        let upstream_error_status = status.is_client_error() || status.is_server_error();
        let response_has_content_length = parts.headers.contains_key(CONTENT_LENGTH);
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
        let parse_sse_events = !upstream_error_status || upstream_is_sse;
        let stream_body_io_timing = response_body_timing.clone();
        let downstream_poll_timing = response_body_timing;
        let has_retry_after_header = parts.headers.get(RETRY_AFTER).is_some();
        let stream = async_stream::stream! {
            let custom_sse_hook = transform_ctx.dialect.sse_event_transform_hook();
            let composed_sse_hook = custom_sse_hook.map(|custom| {
                ComposedSseResponseTransformHook {
                    gemini: &GEMINI_OAUTH_RESPONSE_TRANSFORM,
                    custom,
                }
            });
            let sse_transform_hook: Option<&dyn SseEventTransformHook> =
                if transform_ctx.gemini_oauth {
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
            let mut usage = UsageCounts::default();
            let mut first_chunk_at: Option<Instant> = None;
            let mut last_chunk_at: Option<Instant> = None;
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
            let mut last_partial_at: Option<Instant> = None;
            let mut last_partial_output_tokens: u64 = 0;
            let mut upstream_error_body: Vec<u8> = Vec::new();
            let mut upstream_error_body_truncated = false;
            let mut upstream_error_body_decode_failed = false;
            let mut parse_sse_events_active = parse_sse_events;
            let mut raw_before_transform_output: Vec<Bytes> = Vec::new();
            let mut keepalive_raw_passthrough_active = false;
            // Downstream bytes produced from the final upstream frame of a
            // Content-Length body. A client stops polling once the declared
            // length is satisfied, so these bytes are yielded only after the
            // lifecycle finalization below; yielding them earlier lets the drop
            // guard record a completed response as a client cancellation.
            let mut deferred_terminal_chunks: Vec<Bytes> = Vec::new();
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
            'upstream: loop {
                let Some(frame) = timed_body_frame(&mut body, &stream_body_io_timing).await
                else {
                    upstream_body_eof = true;
                    break;
                };
                match frame {
                    Ok(frame) => {
                        if let Ok(data) = frame.into_data() {
                            let defer_terminal_raw_chunk =
                                response_has_content_length && body.is_end_stream();
                            let mut raw_passthrough_current_chunk = false;
                            let mut raw_chunk_completes_response = false;
                            let now = Instant::now();
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
                                            semantic_message_stop_seen,
                                            stream_upstream_error.is_some()
                                                || stream_transform_error.is_some()
                                                || stream_affinity_error.is_some(),
                                            stream_provider_error_seen,
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
                                    if success_sse_affinity_gate {
                                        stream_upstream_error =
                                            Some(cc_lb_lifecycle::StreamError {
                                                error_type:
                                                    UPSTREAM_RESPONSE_DECODE_ERROR_TYPE.to_owned(),
                                                error_message:
                                                    UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE
                                                        .to_owned(),
                                                ..Default::default()
                                            });
                                        let frame = make_stream_error_frame(
                                            response_affinity_context.capability,
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
                                    if upstream_decode_failed && sse_transform_active {
                                        if stream_provider_error_seen {
                                            downstream_drop_guard.mark_upstream_error(
                                                StreamTerminationCause::ProviderError,
                                                None,
                                            );
                                            break 'upstream;
                                        }
                                        let error_message =
                                            format!("upstream response decoding failed: {error}");
                                        stream_upstream_error =
                                            Some(cc_lb_lifecycle::StreamError {
                                                error_type:
                                                    UPSTREAM_RESPONSE_DECODE_ERROR_TYPE.to_owned(),
                                                error_message: error_message.clone(),
                                                ..Default::default()
                                            });
                                        if transformed_output_started
                                            || !downstream_stream_is_identity
                                        {
                                            let frame = make_stream_error_frame(
                                                response_affinity_context.capability,
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
                                            downstream_drop_guard.mark_upstream_error(
                                                StreamTerminationCause::DecodeError,
                                                None,
                                            );
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
                                    let Some(end) = usage_parser::find_sse_event_end(&buffer) else {
                                        break;
                                    };
                                let raw = split_sse_event(&mut buffer, end);
                                let parsed_event = parse_sse_event(&raw);
                                let usage_update = usage_parser::accumulate_sse_usage_for_shape(
                                    &parsed_event,
                                    &mut usage,
                                    response_affinity_context.capability.usage_shape(),
                                );
                                downstream_drop_guard.set_keepalive_cached_tokens(
                                    usage
                                        .cache_read_input_tokens
                                        .saturating_add(usage.cache_creation_input_tokens),
                                );
                                if let Some(err) =
                                    usage_parser::detect_mid_stream_error(&parsed_event, signer.as_ref())
                                {
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
                                let event_name = parsed_event.event_name();
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
                                            parse_sse_events_active = false;
                                            buffer.clear();
                                            raw_before_transform_output.clear();
                                            let frame = make_stream_error_frame(
                                                response_affinity_context.capability,
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
                                            for chunk in std::mem::take(&mut deferred_terminal_chunks) {
                                                yield Ok::<Bytes, Infallible>(chunk);
                                            }
                                            yield Ok::<Bytes, Infallible>(frame);
                                            break 'upstream;
                                        }
                                    };
                                    let bind_pending_request = sse_event_completes_response(
                                        response_affinity_context.capability,
                                        &parsed_event,
                                        usage_update.message_stop,
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
                                    stream_affinity_error = Some(affinity_stream_error());
                                    parse_sse_events_active = false;
                                    buffer.clear();
                                    raw_before_transform_output.clear();
                                    if success_sse_affinity_gate
                                        || sse_transform_active
                                        || downstream_stream_is_identity
                                    {
                                        let frame = make_stream_error_frame(
                                            response_affinity_context.capability,
                                            "api_error",
                                            UPSTREAM_AFFINITY_UNAVAILABLE_MESSAGE,
                                            false,
                                        );
                                        batch_index = batch_index.saturating_add(1);
                                        for chunk in std::mem::take(&mut deferred_terminal_chunks) {
                                            yield Ok::<Bytes, Infallible>(chunk);
                                        }
                                        yield Ok::<Bytes, Infallible>(frame);
                                    }
                                    if let Some(o) = observer.as_ref() {
                                        o.set_upstream_error(
                                            StatusCode::OK,
                                            UpstreamErrorCode::StreamError,
                                        );
                                    }
                                    downstream_drop_guard.mark_proxy_error(
                                        StreamTerminationCause::AffinityError,
                                    );
                                    break 'upstream;
                                }
                                if !sse_transform_active
                                    && downstream_drop_guard.has_keepalive_completion()
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
                                match event_name {
                                    Some(b"message_start") if message_start_at.is_none() => {
                                        message_start_at = Some(now);
                                        downstream_drop_guard.set_keepalive_anchor_at(now);
                                    }
                                    Some(b"content_block_start")
                                        if content_block_start_at.is_none() =>
                                    {
                                        content_block_start_at = Some(now);
                                    }
                                    Some(b"ping") => {
                                        ping_count = ping_count.saturating_add(1);
                                    }
                                    _ => {}
                                }
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
                                    usage_update.message_stop,
                                );
                                raw_chunk_completes_response |= event_completes_response;
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
                                    let outgoing = if let Some(response_headers) = response_headers.as_ref() {
                                        match transform_sse_event_bytes(
                                            sse_transform_hook,
                                            &transform_ctx,
                                            &event_ctx,
                                            status,
                                            response_headers,
                                            raw.clone(),
                                        ) {
                                            SseTransformOutcome::Emit(bytes) => {
                                                raw_before_transform_output.clear();
                                                transformed_output_started = true;
                                                if downstream_drop_guard.has_keepalive_completion() {
                                                    downstream_drop_guard
                                                        .observe_keepalive_emitted(
                                                            &bytes,
                                                            incomplete_sse_event_budget_bytes,
                                                        );
                                                }
                                                bytes
                                            }
                                            SseTransformOutcome::Drop => {
                                                raw_before_transform_output.clear();
                                                transformed_output_started = true;
                                                continue;
                                            }
                                            SseTransformOutcome::FailOpen => {
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
                                                    downstream_drop_guard.observe_keepalive_emitted(
                                                        &raw,
                                                        incomplete_sse_event_budget_bytes,
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
                                                if success_sse_affinity_gate
                                                    || transformed_output_started
                                                    || !downstream_stream_is_identity
                                                {
                                                    let frame = if success_sse_affinity_gate {
                                                        // The affinity gate must not expose transform
                                                        // hook details to the client.
                                                        make_stream_error_frame(
                                                            response_affinity_context.capability,
                                                            UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                                            UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE,
                                                            false,
                                                        )
                                                    } else {
                                                        make_response_transform_error_frame(
                                                            response_affinity_context.capability,
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
                                                    downstream_drop_guard.observe_keepalive_emitted(
                                                        &raw,
                                                        incomplete_sse_event_budget_bytes,
                                                    );
                                                    batch_index = batch_index.saturating_add(1);
                                                    yield Ok::<Bytes, Infallible>(raw);
                                                }
                                                Bytes::new()
                                            }
                                        }
                                    } else if success_sse_affinity_gate {
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
                                    if event_completes_response
                                        && transformed_sse_completes_response(
                                            response_affinity_context.capability,
                                            &outgoing,
                                        )
                                    {
                                        downstream_drop_guard.mark_semantic_completion();
                                    }
                                    yield Ok::<Bytes, Infallible>(outgoing);
                                } else if success_sse_affinity_gate {
                                    downstream_sse_boundary.observe(&raw);
                                    batch_index = batch_index.saturating_add(1);
                                    if defer_terminal_raw_chunk {
                                        deferred_terminal_chunks.push(raw);
                                    } else {
                                        if event_completes_response {
                                            downstream_drop_guard.mark_semantic_completion();
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
                                            deferred_terminal_chunks.push(outgoing);
                                        } else {
                                            if event_completes_response {
                                                downstream_drop_guard.mark_semantic_completion();
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
                                        downstream_drop_guard.observe_keepalive_emitted(
                                            &raw,
                                            incomplete_sse_event_budget_bytes,
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
                                                .observe_keepalive_emitted(
                                                    &data,
                                                    incomplete_sse_event_budget_bytes,
                                                );
                                        }
                                        downstream_sse_boundary.observe(&data);
                                    }
                                    batch_index = batch_index.saturating_add(1);
                                    if defer_terminal_raw_chunk {
                                        debug_assert!(deferred_terminal_chunks.is_empty());
                                        deferred_terminal_chunks.push(data);
                                    } else {
                                        if raw_chunk_completes_response {
                                            downstream_drop_guard.mark_semantic_completion();
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
                                semantic_message_stop_seen,
                                stream_upstream_error.is_some()
                                    || stream_transform_error.is_some()
                                    || stream_affinity_error.is_some(),
                                stream_provider_error_seen,
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
                        if body_error_is_primary
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
            match usage_decoder.finish() {
                Ok(tail) if !tail.is_empty() => {
                    if upstream_error_status {
                        append_bounded_upstream_error_body(
                            &mut upstream_error_body,
                            &tail,
                            &mut upstream_error_body_truncated,
                        );
                    }
                    if parse_sse_events_active {
                        buffer.extend_from_slice(&tail);
                        while let Some(end) = usage_parser::find_sse_event_end(&buffer) {
                            let raw = split_sse_event(&mut buffer, end);
                            let parsed_event = parse_sse_event(&raw);
                            let usage_update = usage_parser::accumulate_sse_usage_for_shape(
                                &parsed_event,
                                &mut usage,
                                response_affinity_context.capability.usage_shape(),
                            );
                            downstream_drop_guard.set_keepalive_cached_tokens(
                                usage
                                    .cache_read_input_tokens
                                    .saturating_add(usage.cache_creation_input_tokens),
                            );
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
                                        stream_upstream_error =
                                            Some(cc_lb_lifecycle::StreamError {
                                                error_type:
                                                    UPSTREAM_RESPONSE_FRAMING_ERROR_TYPE.to_owned(),
                                                error_message:
                                                    UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE
                                                        .to_owned(),
                                                ..Default::default()
                                            });
                                        downstream_drop_guard.mark_upstream_error(
                                            StreamTerminationCause::FramingError,
                                            None,
                                        );
                                        parse_sse_events_active = false;
                                        buffer.clear();
                                        raw_before_transform_output.clear();
                                        let frame = make_stream_error_frame(
                                            response_affinity_context.capability,
                                            UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                            UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE,
                                            false,
                                        );
                                        batch_index = batch_index.saturating_add(1);
                                        yield Ok::<Bytes, Infallible>(frame);
                                        break;
                                    }
                                };
                                let bind_pending_request = sse_event_completes_response(
                                    response_affinity_context.capability,
                                    &parsed_event,
                                    usage_update.message_stop,
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
                                stream_affinity_error = Some(affinity_stream_error());
                                downstream_drop_guard.mark_proxy_error(
                                    StreamTerminationCause::AffinityError,
                                );
                                parse_sse_events_active = false;
                                buffer.clear();
                                raw_before_transform_output.clear();
                                if success_sse_affinity_gate
                                    || sse_transform_active
                                    || downstream_stream_is_identity
                                {
                                    let frame = make_stream_error_frame(
                                        response_affinity_context.capability,
                                        "api_error",
                                        UPSTREAM_AFFINITY_UNAVAILABLE_MESSAGE,
                                        false,
                                    );
                                    batch_index = batch_index.saturating_add(1);
                                    yield Ok::<Bytes, Infallible>(frame);
                                }
                                break;
                            }
                            if !sse_transform_active
                                && downstream_drop_guard.has_keepalive_completion()
                            {
                                downstream_drop_guard.observe_keepalive(&parsed_event);
                            }
                            if let Some(err) =
                                usage_parser::detect_mid_stream_error(&parsed_event, signer.as_ref())
                            {
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
                                usage_update.message_stop,
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
                                let outgoing = if sse_transform_active {
                                    let Some(response_headers) = response_headers.as_ref() else {
                                        stream_upstream_error =
                                            Some(cc_lb_lifecycle::StreamError {
                                                error_type:
                                                    UPSTREAM_RESPONSE_FRAMING_ERROR_TYPE.to_owned(),
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
                                            UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                            UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE,
                                            false,
                                        );
                                        batch_index = batch_index.saturating_add(1);
                                        yield Ok::<Bytes, Infallible>(frame);
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
                                        SseTransformOutcome::Emit(bytes) => {
                                            if downstream_drop_guard.has_keepalive_completion() {
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
                                                UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                                UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE,
                                                false,
                                            );
                                            batch_index = batch_index.saturating_add(1);
                                            yield Ok::<Bytes, Infallible>(frame);
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
                                if event_completes_response
                                    && (!sse_transform_active
                                        || transformed_sse_completes_response(
                                            response_affinity_context.capability,
                                            &outgoing,
                                        ))
                                {
                                    downstream_drop_guard.mark_semantic_completion();
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
                            if success_sse_affinity_gate || unsolicited_hold_pending {
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
                                    UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                    UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE,
                                    false,
                                );
                                yield Ok::<Bytes, Infallible>(frame);
                            } else if sse_transform_active && stream_transform_error.is_none() {
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
                                        downstream_drop_guard.observe_keepalive_emitted(
                                            &raw,
                                            incomplete_sse_event_budget_bytes,
                                        );
                                        batch_index = batch_index.saturating_add(1);
                                        yield Ok::<Bytes, Infallible>(raw);
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(_) => {}
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
                            semantic_message_stop_seen,
                            stream_upstream_error.is_some()
                                || stream_transform_error.is_some()
                                || stream_affinity_error.is_some(),
                            stream_provider_error_seen,
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
                        && stream_transform_error.is_none()
                        && stream_affinity_error.is_none()
                        && stream_upstream_error.is_none()
                    {
                        stream_upstream_error = Some(cc_lb_lifecycle::StreamError {
                            error_type: UPSTREAM_RESPONSE_DECODE_ERROR_TYPE.to_owned(),
                            error_message: UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE.to_owned(),
                            ..Default::default()
                        });
                        downstream_drop_guard.mark_upstream_error(
                            StreamTerminationCause::DecodeError,
                            None,
                        );
                        let frame = make_stream_error_frame(
                            response_affinity_context.capability,
                            UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                            UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE,
                            false,
                        );
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
                        if decode_error_is_primary
                            && sse_transform_active
                            && !stream_upstream_error_frame_emitted
                        {
                            let error_message = stream_upstream_error
                                .as_ref()
                                .expect("upstream stream error set above")
                                .error_message
                                .clone();
                            let frame = make_stream_error_frame(
                                response_affinity_context.capability,
                                UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                &error_message,
                                false,
                            );
                            if let Some(o) = observer.as_ref()
                                && !upstream_error_status
                            {
                                o.set_upstream_error(status, UpstreamErrorCode::StreamError);
                            }
                            yield Ok::<Bytes, Infallible>(frame);
                        }
                    }
                    else if sse_transform_active
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
            if upstream_is_sse
                && (!upstream_error_status || sse_transform_active)
                && parse_sse_events_active
                && buffer
                    .iter()
                    .any(|byte| !matches!(byte, b'\r' | b'\n'))
                && stream_upstream_error.is_none()
                && !stream_provider_error_seen
                && stream_transform_error.is_none()
                && stream_affinity_error.is_none()
            {
                let error_message = if transformed_output_started {
                    "upstream SSE response ended with an incomplete event after transform output"
                        .to_owned()
                } else {
                    "upstream SSE response ended with an incomplete event before transform output"
                        .to_owned()
                };
                stream_upstream_error = Some(cc_lb_lifecycle::StreamError {
                    error_type: UPSTREAM_RESPONSE_FRAMING_ERROR_TYPE.to_owned(),
                    error_message: error_message.clone(),
                    ..Default::default()
                });
                downstream_drop_guard.mark_upstream_error(
                    StreamTerminationCause::FramingError,
                    None,
                );
                // A retained Content-Length cannot carry an extra error frame.
                // Keep the original final bytes and record the failure below.
                if deferred_terminal_chunks.is_empty()
                    && (success_sse_affinity_gate
                        || sse_transform_active
                        || downstream_stream_is_identity)
                {
                    let frame = make_stream_error_frame(
                        response_affinity_context.capability,
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
                    yield Ok::<Bytes, Infallible>(frame);
                }
                buffer.clear();
            }
            if upstream_is_sse
                && status.is_success()
                && parse_sse_events_active
                && transform_decode_supported
                && semantic_terminal_event_type(response_affinity_context.capability)
                    == Some("message_stop")
                && !semantic_message_stop_seen
                && sse_buffer_is_blank(&buffer)
                && !downstream_drop_guard.has_abnormal_stop()
                && !stream_provider_error_seen
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
                        source: cc_lb_lifecycle::UsageSource::MessageDelta,
                    });
                }
                stream_upstream_error = Some(cc_lb_lifecycle::StreamError {
                    error_type: UPSTREAM_RESPONSE_INCOMPLETE_ERROR_TYPE.to_owned(),
                    error_message:
                        "upstream SSE response ended before message_stop".to_owned(),
                    ..Default::default()
                });
                downstream_drop_guard.mark_upstream_error(
                    StreamTerminationCause::Unknown,
                    None,
                );
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
                        "api_error",
                        UPSTREAM_AFFINITY_UNAVAILABLE_MESSAGE,
                        false,
                    );
                    batch_index = batch_index.saturating_add(1);
                    yield Ok::<Bytes, Infallible>(frame);
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
                } else {
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
                    let (error_type, message) = canonical_error
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
        };
        Response::from_parts(
            parts,
            Body::from_stream(TimedResponseStream::new(stream, downstream_poll_timing)),
        )
    }
}

pub fn observe_rate_limits(
    headers: &HeaderMap,
    upstream_id: Uuid,
    observed_at: u64,
) -> Vec<UpstreamRateLimitObservationRecord> {
    let mut observations = parse_anthropic_rate_limit_headers(headers)
        .into_iter()
        .chain(parse_openai_rate_limit_headers(headers))
        .chain(parse_standard_rate_limit_headers(headers))
        .map(|snapshot| UpstreamRateLimitObservationRecord {
            upstream_id,
            window: snapshot.window,
            kind: snapshot.kind,
            limit: snapshot.limit,
            remaining: snapshot.remaining,
            reset: snapshot.reset,
            observed_at_unix_secs: observed_at,
        })
        .collect::<Vec<_>>();
    if observations.is_empty() && headers.get(RETRY_AFTER).is_some() {
        observations.extend(
            cc_lb_quota::parse_retry_after_rate_limit_headers(headers)
                .into_iter()
                .map(|snapshot| UpstreamRateLimitObservationRecord {
                    upstream_id,
                    window: snapshot.window,
                    kind: snapshot.kind,
                    limit: snapshot.limit,
                    remaining: snapshot.remaining,
                    reset: snapshot.reset,
                    observed_at_unix_secs: observed_at,
                }),
        );
    }
    observations
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
) -> Option<&cc_lb_domain::SubscriptionPreferenceTrace> {
    routing_trace
        .stages
        .iter()
        .find_map(|stage| stage.subscription_preference.as_ref())
}

pub(crate) fn resolved_candidate_urgency(
    routing_trace: &RoutingTrace,
    resolved_upstream_id: Uuid,
) -> Option<&cc_lb_domain::CandidateUrgency> {
    subscription_preference_trace(routing_trace).and_then(|trace| {
        trace
            .candidates
            .iter()
            .find(|candidate| candidate.upstream_id == resolved_upstream_id)
    })
}

fn execute_filter_pipeline(
    filters: &[Arc<dyn FilterPlugin>],
    ctx: &RequestContext,
    principal: &Principal,
    candidates: Vec<UpstreamCandidate>,
) -> FilterPipelineResult {
    let routing_context = ctx.routing_context();
    let mut current = candidates;
    let mut stages = Vec::with_capacity(filters.len());
    let mut internal_errors = Vec::new();

    for (stage_index, filter) in filters.iter().enumerate() {
        let stage_name = filter.plugin_name().to_owned();
        let stage_started = Instant::now();
        match filter.filter(&routing_context, principal, &current) {
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
                        stages.push(StageDecision {
                            stage_name,
                            upstream_id: current.first().map(|candidate| candidate.upstream_id),
                            reason: Some(message.clone()),
                            duration_us: duration_to_us(stage_elapsed),
                            subscription_preference: None,
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
                stages.push(StageDecision {
                    stage_name,
                    upstream_id: current.first().map(|candidate| candidate.upstream_id),
                    reason: Some(message.clone()),
                    duration_us: duration_to_us(stage_elapsed),
                    subscription_preference: None,
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
    subscription_preference: Option<cc_lb_domain::SubscriptionPreferenceTrace>,
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

struct CollectedResponse {
    status: StatusCode,
    headers: HeaderMap,
    /// The upstream body, or `None` when it could not be read to EOF within
    /// [`ERROR_BODY_COLLECT_CAP_BYTES`] and [`ERROR_BODY_COLLECT_DEADLINE`].
    /// `None` means "no trustworthy body"; it is never a truncated prefix.
    body: Option<Bytes>,
}

/// Deadline for reading a discarded upstream error body to EOF. A stalled
/// body must not hold the refresh/failover decision (and with it the rest of
/// the request timeout) hostage; the bound is short because the terminal
/// relay regenerates a response when the body never arrives.
const ERROR_BODY_COLLECT_DEADLINE: Duration = Duration::from_secs(10);
/// Byte cap for the same read. Bodies that cannot be relayed completely are
/// never relayed at all, so the cap only bounds how much memory a partial
/// read may hold before it is dropped: the stored-sink collection bound
/// already used for upstream error text.
const ERROR_BODY_COLLECT_CAP_BYTES: usize = REQUEST_LOG_UPSTREAM_ERROR_BODY_COLLECT_BYTES;

async fn collect_terminal_error_body(body: Body) -> Option<Bytes> {
    let collected = tokio::time::timeout(
        ERROR_BODY_COLLECT_DEADLINE,
        http_body_util::Limited::new(body, ERROR_BODY_COLLECT_CAP_BYTES).collect(),
    )
    .await
    .ok()?
    .ok()?;
    Some(collected.to_bytes())
}

async fn collect_error_response(response: Response<Body>) -> CollectedResponse {
    let (parts, body) = response.into_parts();
    CollectedResponse {
        status: parts.status,
        headers: {
            let mut headers = parts.headers;
            strip_hop_by_hop(&mut headers);
            headers
        },
        body: collect_terminal_error_body(body).await,
    }
}

fn response_from_collected(collected: CollectedResponse) -> Response<Body> {
    let Some(body) = collected.body else {
        // A stalled, truncated, or failed upstream body is never relayed in
        // part; the relayed status stays honest instead of echoing the
        // upstream code next to proxy-generated bytes.
        return anthropic_error_response(
            StatusCode::BAD_GATEWAY,
            "api_error",
            UPSTREAM_BUFFERED_BODY_FAILED_MESSAGE,
        );
    };
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = collected.status;
    *response.headers_mut() = collected.headers;
    response
}

fn record_shape_internal_error(observer: Option<&LifecycleContext>, message: &str) {
    if let Some(observer) = observer {
        observer.record_internal_error(InternalError {
            stage: InternalErrorStage::Shape,
            kind: InternalErrorKind::Trap,
            message: Some(truncate_reason(message)),
        });
    }
}

fn raw_passthrough_request(
    base_url: Option<&Url>,
    ctx: &RequestContext,
    principal: &Principal,
    upstream: &Upstream,
    openai_oauth: bool,
) -> Result<ShapedRequest, RawPassthroughError> {
    let base_url = base_url.cloned().unwrap_or_else(|| match upstream {
        Upstream::AnthropicDirect { .. } => default_base_url(StorageUpstreamKind::AnthropicApiKey),
        Upstream::OpenaiDirect { .. } => {
            if openai_oauth {
                default_base_url(StorageUpstreamKind::OpenaiOauth)
            } else {
                default_base_url(StorageUpstreamKind::OpenaiApiKey)
            }
        }
        Upstream::GeminiDirect { .. } => default_base_url(StorageUpstreamKind::GeminiApiKey),
    });
    let shape_context = ctx.dialect_shape_context();
    shape_request(
        &RawPassthroughDialect {
            base_url,
            openai_oauth,
        },
        &shape_context,
        upstream,
        principal,
    )
    .map_err(|source| {
        tracing::error!(%source, "raw passthrough request failed");
        RawPassthroughError::Shape(source)
    })
}

struct RawPassthroughDialect {
    base_url: Url,
    openai_oauth: bool,
}

impl UpstreamDialect for RawPassthroughDialect {
    fn shape(
        &self,
        context: &DialectShapeContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        let scrubbed_query = context
            .query
            .as_deref()
            .map(cc_lb_upstream::scrub_query_api_key);
        let scrubbed_query = scrubbed_query.as_deref().filter(|query| !query.is_empty());
        let url = if let Some(suffix) = context
            .path
            .strip_prefix("/v1/")
            .filter(|_| self.openai_oauth)
        {
            let mut url = self.base_url.clone();
            let path = format!("{}/{suffix}", url.path().trim_end_matches('/'));
            url.set_path(&path);
            url.set_query(scrubbed_query);
            url
        } else {
            cc_lb_upstream::upstream_request_url(&self.base_url, &context.path, scrubbed_query)
        };
        Ok(builder.shaped_request(
            url,
            context.method.clone(),
            context.downstream_headers.clone(),
            context.body_bytes.clone(),
        ))
    }
}

fn use_image_rates_for_admission(capability: Option<ProviderCapability>, path: &str) -> bool {
    matches!(
        capability,
        Some(ProviderCapability::OpenaiResponses | ProviderCapability::OpenaiChatCompletions)
    ) || (capability == Some(ProviderCapability::OpenaiImages)
        && matches!(path, "/v1/images/edits" | "/v1/images/variations"))
}

fn body_cap_for_path(config: &LifecycleConfig, path: &str) -> usize {
    if path.starts_with("/v1/files") || is_multipart_operation(path) {
        config.files_body_cap_bytes
    } else {
        config.messages_body_cap_bytes
    }
}

pub fn is_multipart_operation(path: &str) -> bool {
    matches!(
        path,
        "/v1/audio/transcriptions"
            | "/v1/audio/translations"
            | "/v1/images/edits"
            | "/v1/images/variations"
    )
}

async fn multipart_request_model(
    _auth: &crate::authn_rail::Authenticated,
    headers: &HeaderMap,
    body: &Bytes,
) -> Option<String> {
    let content_type = headers.get(http::header::CONTENT_TYPE)?.to_str().ok()?;
    let boundary = multer::parse_boundary(content_type).ok()?;
    let stream = Body::from(body.clone()).into_data_stream();
    let mut multipart = multer::Multipart::new(stream, boundary);
    while let Some(field) = multipart.next_field().await.ok()? {
        if field.name() == Some("model") && field.file_name().is_none() {
            return field.text().await.ok();
        }
    }
    None
}

fn unsupported_operation_response(
    scope: Option<cc_lb_domain::Provider>,
    method: &Method,
    path: &str,
) -> Response<Body> {
    let Some(provider) = scope else {
        return anthropic_error_response(
            StatusCode::NOT_FOUND,
            "operation_not_supported",
            "operation is not supported by the consistency-safe proxy boundary",
        );
    };
    crate::error_format::provider_scope_error_response(
        provider,
        StatusCode::NOT_FOUND,
        "operation_not_supported",
        &format!("{method} {path} is not supported by cc-lb yet"),
    )
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
/// (we only copy the fields explicitly whitelisted). Provider-specific quota
/// fields are carried in separate maps and parsed by the off-path subscriber.
fn header_snapshot_from(headers: &HeaderMap, upstream_kind: StorageUpstreamKind) -> HeaderSnapshot {
    let mut anthropic_headers = std::collections::BTreeMap::new();
    let mut provider_headers = std::collections::BTreeMap::new();
    let is_openai = matches!(
        upstream_kind,
        StorageUpstreamKind::OpenaiApiKey | StorageUpstreamKind::OpenaiOauth
    );
    let is_gemini = matches!(
        upstream_kind,
        StorageUpstreamKind::GeminiApiKey | StorageUpstreamKind::GeminiOauth
    );
    for (name, value) in headers.iter() {
        let name_lc = name.as_str();
        let Ok(val) = value.to_str() else { continue };
        let is_anthropic = name_lc.starts_with("anthropic-ratelimit-")
            || cc_lb_domain::ANTHROPIC_IDENTITY_HEADERS.contains(&name_lc);
        let is_codex = name_lc.starts_with("x-codex-");
        let is_openai_standard = is_openai && name_lc.starts_with("x-ratelimit-");
        let is_common = matches!(
            name_lc,
            "ratelimit-limit" | "ratelimit-remaining" | "ratelimit-reset"
        );
        if is_anthropic || is_common {
            anthropic_headers.insert(name_lc.to_owned(), val.to_owned());
        }
        if is_codex || is_openai_standard || (is_gemini && name_lc == "x-goog-request-id") {
            provider_headers.insert(name_lc.to_owned(), val.to_owned());
        }
    }
    HeaderSnapshot {
        content_type: header_to_string(headers, "content-type"),
        content_encoding: header_to_string(headers, "content-encoding"),
        request_id: header_to_string(headers, "x-request-id")
            .or_else(|| header_to_string(headers, "request-id"))
            .or_else(|| header_to_string(headers, "x-goog-request-id")),
        retry_after: header_to_string(headers, "retry-after"),
        provider_headers,
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

fn apply_prompt_cache_timings(
    setup: &mut cc_lb_lifecycle::RequestSetupTimings,
    prompt_cache: PromptCacheAnalysisTimings,
) {
    setup.cache_structure_ms = prompt_cache.cache_structure_ms;
    setup.cache_token_key_ms = prompt_cache.cache_token_key_ms;
    setup.cache_count_lookup_ms = prompt_cache.cache_count_lookup_ms;
    setup.cache_tokenizer_queue_ms = prompt_cache.cache_tokenizer_queue_ms;
    setup.cache_serialize_ms = prompt_cache.cache_serialize_ms;
    setup.cache_tokenize_ms = prompt_cache.cache_tokenize_ms;
}

fn store_setup_timings(
    observer: Option<&LifecycleContext>,
    timings: cc_lb_lifecycle::RequestSetupTimings,
) {
    if let Some(observer) = observer {
        observer.set_setup_timings(timings);
    }
}

// Shared parse of the request body inside `handle`. Every downstream consumer
// (stream / model / max_tokens / cache metadata) reads through this view so
// the body JSON is parsed exactly once per request instead of up to 7 times.
//
// Parser: `sonic_rs::from_slice` — hot-path SIMD JSON parser. Yields a
// `serde_json::Value` so all downstream consumers stay compatible without
// touching the serialization side (see `docs/adr/0002-json-library-strategy.md`).
struct RequestBodyView {
    parsed: Result<Arc<Value>, sonic_rs::Error>,
    json_parse_ms: f64,
}

impl RequestBodyView {
    fn new(body: &Bytes) -> Self {
        let started = Instant::now();
        let parsed = sonic_rs::from_slice::<Value>(body);
        let json_parse_ms = started.elapsed().as_secs_f64() * 1_000.0;
        Self {
            parsed: parsed.map(Arc::new),
            json_parse_ms,
        }
    }

    fn value(&self) -> Option<&Value> {
        self.parsed.as_deref().ok()
    }

    fn value_arc(&self) -> Option<Arc<Value>> {
        self.parsed.as_ref().ok().cloned()
    }

    fn is_valid_json(&self) -> bool {
        self.parsed.is_ok()
    }

    fn json_parse_ms(&self) -> f64 {
        self.json_parse_ms
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

    fn limit_request(
        &self,
        fallback_model: &str,
        capability: Option<ProviderCapability>,
        breakpoints: &[CacheBreakpoint],
        use_image_rates: bool,
    ) -> LimitRequest {
        LimitRequest::from_value(
            self.value(),
            fallback_model,
            capability,
            breakpoints,
            use_image_rates,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LimitRequest {
    model: String,
    max_tokens: Option<i64>,
    input_tokens_estimate: i64,
    local_cache_input: Option<cc_lb_pricing::LocalCacheInputEstimate>,
    estimated_cache_tokens: u64,
    can_cross_cache_cliff: bool,
    use_image_rates: bool,
    stream: bool,
    service_tier: Option<String>,
}
impl LimitRequest {
    /// Native Gemini operations use the normalized canonical model. Other
    /// operations prefer the body's model and use the canonical fallback only
    /// when that field is absent. Catalog/default-moderation operations have no
    /// model; required-model operations retain the existing `unknown` sentinel.
    fn from_value(
        value: Option<&Value>,
        fallback_model: &str,
        capability: Option<ProviderCapability>,
        breakpoints: &[CacheBreakpoint],
        use_image_rates: bool,
    ) -> Self {
        let anthropic_input = value
            .filter(|_| capability == Some(ProviderCapability::AnthropicMessages))
            .map(|value| estimate_anthropic_input(value, breakpoints));
        let body_model = value
            .and_then(|v| v.get("model"))
            .and_then(Value::as_str)
            .filter(|model| !model.is_empty());
        let native_gemini = matches!(
            capability,
            Some(
                ProviderCapability::GeminiGenerateContent
                    | ProviderCapability::GeminiCountTokens
                    | ProviderCapability::GeminiCachedContentsCreate
                    | ProviderCapability::GeminiEmbedContent
                    | ProviderCapability::GeminiPredict
            )
        );
        let model_optional = matches!(
            capability,
            Some(
                ProviderCapability::AnthropicModels
                    | ProviderCapability::OpenaiModels
                    | ProviderCapability::GeminiModels
                    | ProviderCapability::GeminiOpenaiModels
                    | ProviderCapability::OpenaiModerations
                    | ProviderCapability::OpenaiCodexAuxiliary
            )
        );
        let fallback = (!fallback_model.is_empty()).then_some(fallback_model);
        let model = if native_gemini {
            fallback.or(body_model)
        } else {
            body_model.or(fallback)
        }
        .unwrap_or(if model_optional { "" } else { "unknown" });
        let max_tokens = match capability {
            Some(ProviderCapability::OpenaiResponses) => value
                .and_then(|v| v.get("max_output_tokens"))
                .and_then(Value::as_i64),
            Some(ProviderCapability::AnthropicMessages) => value
                .and_then(|v| v.get("max_tokens"))
                .and_then(Value::as_i64),
            Some(ProviderCapability::AnthropicComplete) => value
                .and_then(|v| v.get("max_tokens_to_sample"))
                .and_then(Value::as_i64),
            Some(
                ProviderCapability::OpenaiChatCompletions
                | ProviderCapability::GeminiOpenaiChatCompletions,
            ) => value.and_then(|v| {
                ["max_completion_tokens", "max_tokens"]
                    .into_iter()
                    .filter_map(|key| v.get(key).and_then(Value::as_i64))
                    .max()
            }),
            Some(ProviderCapability::OpenaiCompletions) => value
                .and_then(|v| v.get("max_tokens"))
                .and_then(Value::as_i64),
            Some(ProviderCapability::GeminiGenerateContent) => value
                .and_then(|v| v.get("generationConfig"))
                .and_then(|config| config.get("maxOutputTokens"))
                .and_then(Value::as_i64),
            _ => Some(0),
        };
        Self {
            model: model.to_owned(),
            max_tokens,
            input_tokens_estimate: anthropic_input
                .map(|input| {
                    let whole = input
                        .uncached_input_tokens
                        .saturating_add(input.cache_prefix_5m_tokens)
                        .saturating_add(input.cache_prefix_1h_tokens);
                    i64::try_from(whole).unwrap_or(i64::MAX)
                })
                .or_else(|| value.map(estimate_input_tokens))
                .unwrap_or(if model_optional {
                    0
                } else {
                    DEFAULT_MAX_INPUT_ESTIMATE
                }),
            local_cache_input: anthropic_input.filter(|_| !breakpoints.is_empty()),
            estimated_cache_tokens: breakpoints
                .iter()
                .map(|bp| bp.prefix_bytes.as_token_estimate())
                .max()
                .unwrap_or(0),
            can_cross_cache_cliff: breakpoints.is_empty()
                && value.is_some_and(request_can_cross_cache_cliff),
            use_image_rates,
            stream: value
                .and_then(|v| v.get("stream"))
                .and_then(Value::as_bool)
                .unwrap_or(false),
            service_tier: value
                .and_then(|value| request_service_tier(value, capability))
                .map(str::to_owned),
        }
    }
}

#[cfg(test)]
mod limit_request_estimate_tests {
    use super::{LimitRequest, ProviderCapability, RequestBodyView};
    use bytes::Bytes;
    use serde_json::json;
    #[test]
    fn image_admission_rates_are_limited_to_image_capable_routes() {
        assert!(super::use_image_rates_for_admission(
            Some(ProviderCapability::OpenaiResponses),
            "/v1/responses",
        ));
        assert!(super::use_image_rates_for_admission(
            Some(ProviderCapability::OpenaiChatCompletions),
            "/v1/chat/completions",
        ));
        assert!(super::use_image_rates_for_admission(
            Some(ProviderCapability::OpenaiImages),
            "/v1/images/edits",
        ));
        assert!(super::use_image_rates_for_admission(
            Some(ProviderCapability::OpenaiImages),
            "/v1/images/variations",
        ));
        assert!(!super::use_image_rates_for_admission(
            Some(ProviderCapability::OpenaiImages),
            "/v1/images/generations",
        ));
        assert!(!super::use_image_rates_for_admission(
            Some(ProviderCapability::GeminiOpenaiImages),
            "/v1beta/openai/images/generations",
        ));
    }

    #[test]
    fn output_caps_are_capability_aware() {
        let cases = [
            (
                ProviderCapability::AnthropicMessages,
                json!({"max_tokens": 0, "max_output_tokens": 99}),
                Some(0),
            ),
            (
                ProviderCapability::AnthropicMessages,
                json!({"max_tokens": -1, "max_output_tokens": 99}),
                Some(-1),
            ),
            (
                ProviderCapability::AnthropicComplete,
                json!({"max_tokens": 99, "max_tokens_to_sample": 37}),
                Some(37),
            ),
            (
                ProviderCapability::OpenaiResponses,
                json!({"max_tokens": 0, "max_output_tokens": 99}),
                Some(99),
            ),
            (
                ProviderCapability::OpenaiResponses,
                json!({"max_output_tokens": -2, "max_tokens": 99}),
                Some(-2),
            ),
            (
                ProviderCapability::OpenaiChatCompletions,
                json!({"max_completion_tokens": -2, "max_tokens": -3}),
                Some(-2),
            ),
            (
                ProviderCapability::OpenaiChatCompletions,
                json!({"max_completion_tokens": 40, "max_tokens": 99}),
                Some(99),
            ),
            (
                ProviderCapability::GeminiGenerateContent,
                json!({"generationConfig":{"maxOutputTokens": 88}, "max_tokens": 99}),
                Some(88),
            ),
            (
                ProviderCapability::GeminiGenerateContent,
                json!({"generationConfig":{"maxOutputTokens": -3}, "max_tokens": 99}),
                Some(-3),
            ),
        ];
        for (capability, value, expected) in cases {
            let request =
                LimitRequest::from_value(Some(&value), "test-model", Some(capability), &[], false);
            assert_eq!(request.max_tokens, expected, "{capability:?}");
        }
        for capability in [
            ProviderCapability::OpenaiResponses,
            ProviderCapability::AnthropicMessages,
            ProviderCapability::AnthropicComplete,
            ProviderCapability::OpenaiChatCompletions,
            ProviderCapability::OpenaiCompletions,
            ProviderCapability::GeminiGenerateContent,
        ] {
            let request = LimitRequest::from_value(
                Some(&json!({"max_tokens": "wrong", "max_output_tokens": null})),
                "test-model",
                Some(capability),
                &[],
                false,
            );
            assert_eq!(request.max_tokens, None, "{capability:?}");
        }
    }

    #[test]
    fn zero_and_negative_output_caps_remain_visible_for_fail_closed_rejection() {
        for (capability, value, expected) in [
            (
                ProviderCapability::AnthropicMessages,
                json!({"max_tokens": 0, "max_completion_tokens": 99, "max_output_tokens": 99}),
                Some(0),
            ),
            (
                ProviderCapability::AnthropicMessages,
                json!({"max_tokens": -1, "max_completion_tokens": 99, "max_output_tokens": 99}),
                Some(-1),
            ),
            (
                ProviderCapability::OpenaiResponses,
                json!({"max_output_tokens": -2, "max_tokens": 99}),
                Some(-2),
            ),
        ] {
            let request =
                LimitRequest::from_value(Some(&value), "test-model", Some(capability), &[], false);
            assert_eq!(request.max_tokens, expected, "{capability:?}");
        }
    }

    #[test]
    fn non_generation_operations_reserve_zero_output_tokens() {
        for capability in [
            ProviderCapability::AnthropicCountTokens,
            ProviderCapability::GeminiCountTokens,
            ProviderCapability::OpenaiEmbeddings,
            ProviderCapability::OpenaiModerations,
            ProviderCapability::OpenaiModels,
            ProviderCapability::GeminiModels,
        ] {
            let request = LimitRequest::from_value(
                Some(&json!({"max_tokens": 99, "max_output_tokens": 88})),
                "test-model",
                Some(capability),
                &[],
                false,
            );
            assert_eq!(request.max_tokens, Some(0), "{capability:?}");
        }
    }

    #[test]
    fn openai_chat_completions_accept_max_completion_tokens() {
        let body = json!({"model":"gpt-test","max_completion_tokens":100});
        for capability in [
            ProviderCapability::OpenaiChatCompletions,
            ProviderCapability::GeminiOpenaiChatCompletions,
        ] {
            let request = LimitRequest::from_value(Some(&body), "", Some(capability), &[], false);
            assert_eq!(request.max_tokens, Some(100), "{capability:?}");
        }
    }

    #[test]
    fn malformed_json_keeps_existing_default_without_local_regions() {
        let view = RequestBodyView::new(&Bytes::from_static(b"{"));
        let request = view.limit_request(
            "claude-test",
            Some(ProviderCapability::AnthropicMessages),
            &[],
            false,
        );
        assert_eq!(request.input_tokens_estimate, 4_000);
        assert_eq!(request.max_tokens, None);
        assert!(request.local_cache_input.is_none());
    }

    #[test]
    fn optional_model_operations_do_not_invent_model_ids() {
        for capability in [
            ProviderCapability::AnthropicModels,
            ProviderCapability::OpenaiModels,
            ProviderCapability::GeminiModels,
            ProviderCapability::GeminiOpenaiModels,
            ProviderCapability::OpenaiModerations,
            ProviderCapability::OpenaiCodexAuxiliary,
        ] {
            let request = LimitRequest::from_value(None, "", Some(capability), &[], false);
            assert_eq!(request.model, "", "{capability:?}");
            assert_eq!(request.input_tokens_estimate, 0);
            assert_eq!(request.max_tokens, Some(0));
        }
        let body = json!({"model":"omni-moderation-latest"});
        let explicit = LimitRequest::from_value(
            Some(&body),
            "",
            Some(ProviderCapability::OpenaiModerations),
            &[],
            false,
        );
        assert_eq!(explicit.model, "omni-moderation-latest");
        let missing = LimitRequest::from_value(
            None,
            "",
            Some(ProviderCapability::OpenaiResponses),
            &[],
            false,
        );
        assert_eq!(missing.model, "unknown");
        let empty = json!({"model":""});
        let empty_required = LimitRequest::from_value(
            Some(&empty),
            "",
            Some(ProviderCapability::OpenaiResponses),
            &[],
            false,
        );
        assert_eq!(empty_required.model, "unknown");
    }

    #[test]
    fn native_gemini_model_is_authoritative_but_openai_compat_uses_body() {
        let body = json!({"model":"body-model"});
        for capability in [
            ProviderCapability::GeminiGenerateContent,
            ProviderCapability::GeminiCountTokens,
            ProviderCapability::GeminiEmbedContent,
            ProviderCapability::GeminiPredict,
        ] {
            let request =
                LimitRequest::from_value(Some(&body), "path-model", Some(capability), &[], false);
            assert_eq!(request.model, "path-model");
        }
        for capability in [
            ProviderCapability::OpenaiChatCompletions,
            ProviderCapability::GeminiOpenaiChatCompletions,
        ] {
            let request =
                LimitRequest::from_value(Some(&body), "fallback", Some(capability), &[], false);
            assert_eq!(request.model, "body-model");
        }
    }
}

pub(super) const INPUT_ESTIMATE_MAX_NODES: usize = 16_384;
pub(super) const INPUT_ESTIMATE_MAX_BYTES: usize = 64 * 1024 * 1024;

fn estimate_input_tokens(value: &Value) -> i64 {
    let mut nodes = 0;
    let mut bytes = 0;
    for key in [
        "tools",
        "system",
        "messages",
        "instructions",
        "input",
        "systemInstruction",
        "contents",
    ] {
        if let Some(selected) = value.get(key) {
            collect_input_bytes(selected, &mut nodes, &mut bytes);
        }
    }
    if let Some(request) = value.get("generateContentRequest") {
        for key in ["tools", "contents", "systemInstruction"] {
            if let Some(selected) = request.get(key) {
                collect_input_bytes(selected, &mut nodes, &mut bytes);
            }
        }
    }
    i64::try_from(bytes / 4)
        .unwrap_or(i64::MAX)
        .max(DEFAULT_MAX_INPUT_ESTIMATE)
}

fn request_can_cross_cache_cliff(value: &Value) -> bool {
    if [
        "previous_response_id",
        "conversation",
        "cachedContent",
        "container",
        "prompt_cache_key",
        "prompt_cache_options",
        "prompt_cache_retention",
    ]
    .iter()
    .any(|key| value.get(*key).is_some_and(|v| !v.is_null()))
    {
        return true;
    }
    if value
        .get("generateContentRequest")
        .and_then(|req| req.get("cachedContent"))
        .is_some_and(|v| !v.is_null())
    {
        return true;
    }
    let mut nodes = 0;
    value_contains_cache_marker(value, &mut nodes)
}

fn value_contains_cache_marker(value: &Value, nodes: &mut usize) -> bool {
    if *nodes >= INPUT_ESTIMATE_MAX_NODES {
        return false;
    }
    *nodes += 1;
    match value {
        Value::Object(map) => {
            if map.contains_key("cache_control") || map.contains_key("prompt_cache_breakpoint") {
                return true;
            }
            map.values()
                .any(|child| value_contains_cache_marker(child, nodes))
        }
        Value::Array(items) => items
            .iter()
            .any(|child| value_contains_cache_marker(child, nodes)),
        _ => false,
    }
}

fn collect_input_bytes(value: &Value, nodes: &mut usize, bytes: &mut usize) {
    if *nodes >= INPUT_ESTIMATE_MAX_NODES || *bytes >= INPUT_ESTIMATE_MAX_BYTES {
        return;
    }
    *nodes = (*nodes).saturating_add(1);
    match value {
        Value::String(text) => {
            *bytes = bytes
                .saturating_add(text.len())
                .min(INPUT_ESTIMATE_MAX_BYTES);
        }
        Value::Array(values) => {
            for value in values {
                collect_input_bytes(value, nodes, bytes);
                if *nodes >= INPUT_ESTIMATE_MAX_NODES || *bytes >= INPUT_ESTIMATE_MAX_BYTES {
                    break;
                }
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                collect_input_bytes(value, nodes, bytes);
                if *nodes >= INPUT_ESTIMATE_MAX_NODES || *bytes >= INPUT_ESTIMATE_MAX_BYTES {
                    break;
                }
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

struct ActiveLimit {
    subject: AuthLimitSubject,
    request: LimitRequest,
    reservation: Option<LimitReservation>,
}

#[derive(Clone)]
pub(crate) struct RequestEventContext {
    pub(crate) request_id: String,
    pub(crate) canonical_model_id: String,
    /// Routed upstream name, matching the `RouteCompleted` lifecycle event.
    pub(crate) upstream_name: String,
    /// Model requested in the body, matching the `RouteCompleted` lifecycle event.
    pub(crate) requested_model: Option<String>,
    pub(crate) proxy_setup_ms: Option<u64>,
    pub(crate) stage_timings: AttemptTimings,
}

pub(crate) use crate::response_transform::{
    ComposedSseResponseTransformHook, GEMINI_OAUTH_RESPONSE_TRANSFORM, ResponseTransformContext,
    SseTransformOutcome, apply_buffered_transform_result, make_response_transform_error_frame,
    sanitize_downstream_response_headers, sanitize_downstream_stream_headers,
    sanitized_response_headers_for_plugin, transform_sse_event_bytes,
    unwrap_gemini_code_assist_body,
};

#[derive(Clone, Default)]
pub(crate) struct PromptCacheBundle {
    pub(crate) cache_breakpoints: Vec<CacheBreakpoint>,
    pub(crate) cacheable_breakpoint_prefix_keys: HashSet<String>,
    pub(crate) request_breakpoints: Vec<RequestCacheBreakpoint>,
    pub(crate) cache_prefix_hash: Option<String>,
}

#[derive(Clone, Default)]
pub(crate) struct PromptCacheBundles {
    pub(crate) official: PromptCacheBundle,
    pub(crate) literal: PromptCacheBundle,
}

impl PromptCacheBundles {
    #[cfg(test)]
    fn single(cache_breakpoints: &[CacheBreakpoint]) -> Self {
        let request_breakpoints = cache_breakpoints
            .iter()
            .map(request_cache_breakpoint_from_plugin)
            .collect::<Vec<_>>();
        let cache_prefix_hash = cache_breakpoints
            .last()
            .map(|breakpoint| breakpoint.prefix_hash.clone());
        let cacheable_breakpoint_prefix_keys = cache_breakpoints
            .iter()
            .map(|breakpoint| breakpoint.prefix_hash.clone())
            .collect();
        let bundle = PromptCacheBundle {
            cache_breakpoints: cache_breakpoints.to_vec(),
            cacheable_breakpoint_prefix_keys,
            request_breakpoints,
            cache_prefix_hash,
        };
        Self {
            official: bundle.clone(),
            literal: bundle,
        }
    }

    fn for_upstream(&self, upstream: &UpstreamRecord) -> &PromptCacheBundle {
        match prompt_cache_policy_for_upstream(upstream) {
            PromptCachePolicy::AnthropicOfficial => &self.official,
            PromptCachePolicy::Literal => &self.literal,
        }
    }
}

#[cfg(test)]
fn request_cache_breakpoint_from_plugin(breakpoint: &CacheBreakpoint) -> RequestCacheBreakpoint {
    RequestCacheBreakpoint {
        block_index: u64::from(breakpoint.block_index),
        source: request_breakpoint_source_from_plugin(breakpoint.source),
        path: breakpoint.path.clone(),
        message_index: breakpoint.message_index.map(u64::from),
        ttl: Some(match breakpoint.requested_ttl {
            TtlClass::Ephemeral1h => "1h".to_owned(),
            TtlClass::Ephemeral5m => "5m".to_owned(),
        }),
        prefix_hash: breakpoint.prefix_hash.clone(),
        prefix_token_count: breakpoint.prefix_bytes.get(),
        lookback_prefixes: breakpoint
            .lookback_prefixes
            .iter()
            .map(|prefix| RequestCacheLookbackPrefix {
                prefix_hash: prefix.prefix_hash.clone(),
                content_block_index: u64::from(prefix.content_block_index),
                lookback_distance: u64::from(prefix.lookback_distance),
            })
            .collect(),
        token_estimate_source: Some(V3_TOKEN_ESTIMATE_SOURCE.to_owned()),
    }
}

#[cfg(test)]
fn request_breakpoint_source_from_plugin(
    source: CacheBreakpointSource,
) -> RequestCacheBreakpointSource {
    match source {
        CacheBreakpointSource::Tools => RequestCacheBreakpointSource::Tools,
        CacheBreakpointSource::System => RequestCacheBreakpointSource::System,
        CacheBreakpointSource::Message => RequestCacheBreakpointSource::Message,
    }
}

pub(crate) fn prompt_cache_policy_for_upstream(upstream: &UpstreamRecord) -> PromptCachePolicy {
    let official_kind = matches!(
        upstream.kind,
        StorageUpstreamKind::AnthropicApiKey | StorageUpstreamKind::AnthropicOauth
    );
    let official_url = upstream
        .base_url
        .as_ref()
        .is_none_or(is_official_anthropic_base_url);
    if official_kind && official_url {
        PromptCachePolicy::AnthropicOfficial
    } else {
        PromptCachePolicy::Literal
    }
}

fn is_official_anthropic_base_url(url: &Url) -> bool {
    url.scheme() == "https"
        && url.host_str() == Some("api.anthropic.com")
        && url.port().is_none_or(|port| port == 443)
        && url.username().is_empty()
        && url.password().is_none()
}

#[cfg(test)]
mod prompt_cache_policy_tests {
    use super::*;
    use crate::prompt_cache_simulator::{block_digest_call_count, reset_block_digest_call_count};
    use cc_lb_control::traits::NoopPromptCacheObservationSink;
    #[test]
    fn official_url_policy_accepts_only_default_anthropic_authority() {
        for (url, expected) in [
            ("https://api.anthropic.com", true),
            ("https://@api.anthropic.com", true),
            ("https://api.anthropic.com/v1/messages?x=1#fragment", true),
            ("https://api.anthropic.com:443/proxy", true),
            ("http://api.anthropic.com", false),
            ("https://api.anthropic.com:8443", false),
            ("https://user:pass@api.anthropic.com", false),
            ("https://proxy.example.com", false),
        ] {
            let url = url.parse().expect("test URL parses");
            assert_eq!(is_official_anthropic_base_url(&url), expected, "{url}");
        }
    }

    #[test]
    fn empty_userinfo_is_canonicalized_to_the_official_url() {
        let empty_userinfo: Url = "https://@api.anthropic.com"
            .parse()
            .expect("empty-userinfo URL parses");
        let official: Url = "https://api.anthropic.com"
            .parse()
            .expect("official URL parses");
        assert_eq!(empty_userinfo, official);
        assert_eq!(empty_userinfo.as_str(), "https://api.anthropic.com/");
        assert!(empty_userinfo.username().is_empty());
        assert!(empty_userinfo.password().is_none());
        assert!(is_official_anthropic_base_url(&empty_userinfo));
    }

    #[test]
    fn policy_bundles_digest_each_raw_block_once_per_request() {
        for include_billing_header in [false, true] {
            let mut system = Vec::new();
            if include_billing_header {
                system.push(json!({"type":"text","text":"x-anthropic-billing-header: cron-a"}));
            }
            system.push(json!({
                "type":"text",
                "text":"stable",
                "cache_control":{"type":"ephemeral"}
            }));
            let request = serde_json::to_vec(&json!({
                "model":"claude-sonnet-4-5",
                "system":system,
            }))
            .expect("request serializes");
            reset_block_digest_call_count();
            let _ = request_cache_metadata(&HeaderMap::new(), &Bytes::from(request));
            assert_eq!(
                block_digest_call_count(),
                if include_billing_header { 2 } else { 1 },
                "billing_header={include_billing_header}"
            );
        }
    }

    #[test]
    fn policy_bundles_tokenize_each_raw_prefix_boundary_once()
    -> Result<(), Box<dyn std::error::Error>> {
        for include_billing_header in [false, true] {
            let mut system = Vec::new();
            if include_billing_header {
                system.push(json!({"type":"text","text":"x-anthropic-billing-header: cron-a"}));
            }
            system.push(json!({
                "type": "text",
                "text": "a".repeat(2_000),
                "cache_control": {"type": "ephemeral"}
            }));
            let request = json!({"model":"claude-sonnet-4-5","system":system});
            let canonical_model = canonical_model_id("claude-sonnet-4-5");
            crate::tokenizer::reset_tokenizer_call_count();
            let analysis = analyze_v3_prompt_cache(&request, canonical_model);
            let eligible = cacheable_breakpoint_prefix_keys(
                &analysis,
                canonical_model,
                cache_threshold_tokens(canonical_model),
            );
            assert_eq!(crate::tokenizer::tokenizer_call_count(), 1);
            let output = PromptCacheAnalysisOutput {
                analysis,
                cacheable_breakpoint_prefix_keys: eligible,
                literal_cacheable_breakpoint_prefix_keys: Vec::new(),
                timings: PromptCacheAnalysisTimings::default(),
            };
            let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
                &HeaderMap::new(),
                Some(&request),
                Some(output),
                None,
            );
            assert_eq!(metadata.bundles.official.cache_breakpoints.len(), 1);
            assert_eq!(metadata.bundles.literal.cache_breakpoints.len(), 1);
            assert_eq!(
                crate::tokenizer::tokenizer_call_count(),
                1,
                "shared raw boundary was tokenized again, billing_header={include_billing_header}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn literal_only_billing_breakpoint_records_custom_cache_creation()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = json!({
            "model": "claude-sonnet-4-5",
            "tools": [{
                "name": "large_tool",
                "description": "tool schema context ".repeat(2_000),
                "input_schema": {"type": "object"}
            }],
            "system": [{
                "type": "text",
                "text": "x-anthropic-billing-header: cron-attribution",
                "cache_control": {"type": "ephemeral"}
            }]
        });
        let canonical_model = canonical_model_id("claude-sonnet-4-5");
        let output = PromptCacheAnalysisExecutor::default()
            .analyze(
                Arc::new(request.clone()),
                canonical_model.to_owned(),
                [0; 32],
                Some(cache_threshold_tokens(canonical_model)),
            )
            .await;
        assert!(output.analysis.breakpoints.is_empty());
        let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
            &HeaderMap::new(),
            Some(&request),
            Some(output),
            None,
        );
        assert!(metadata.bundles.official.cache_breakpoints.is_empty());
        let literal = &metadata.bundles.literal;
        let breakpoint = literal
            .cache_breakpoints
            .first()
            .ok_or("literal breakpoint missing")?;
        assert!(
            literal
                .cacheable_breakpoint_prefix_keys
                .contains(&breakpoint.prefix_hash)
        );
        let context = PromptCacheObservationContext {
            upstream_id: Uuid::from_u128(1004),
            canonical_model_id: metadata.canonical_model_id.clone(),
            cache_breakpoints: literal.cache_breakpoints.clone(),
            cacheable_breakpoint_prefix_keys: literal.cacheable_breakpoint_prefix_keys.clone(),
            selected_match: None,
            native_cache: None,
            sink: Arc::new(NoopPromptCacheObservationSink),
            grace_margin_secs: 0,
        };
        let decoded = decode_prompt_cache_observations_pure(
            &context,
            PromptCacheUsage {
                cache_creation_input_tokens: 8_192,
                cache_read_input_tokens: 0,
                read_only_evidence: false,
                ..PromptCacheUsage::default()
            },
            1_800_000_000,
        );
        assert_eq!(decoded.observations.len(), 1);
        assert_eq!(
            decoded
                .observations
                .first()
                .map(|observation| observation.prefix_hash.as_str()),
            Some(breakpoint.prefix_hash.as_str())
        );
        assert_eq!(decoded.dropped_below_threshold, 0);
        Ok(())
    }
}

fn prompt_cache_bundle_from_analysis(
    analysis: &V3PromptCacheAnalysis,
    cacheable_prefix_keys: HashSet<String>,
) -> PromptCacheBundle {
    prompt_cache_bundle_from_breakpoints(&analysis.breakpoints, cacheable_prefix_keys)
}

fn prompt_cache_bundle_from_breakpoints(
    breakpoints: &[crate::prompt_cache_simulator::V3PromptCacheBreakpoint],
    cacheable_prefix_keys: HashSet<String>,
) -> PromptCacheBundle {
    let cache_breakpoints = breakpoints
        .iter()
        .map(plugin_cache_breakpoint_from_v3)
        .collect();
    PromptCacheBundle {
        request_breakpoints: breakpoints
            .iter()
            .map(request_cache_breakpoint_from_v3)
            .collect(),
        cache_prefix_hash: breakpoints
            .last()
            .map(|breakpoint| breakpoint.prefix_key.clone()),
        cacheable_breakpoint_prefix_keys: cacheable_prefix_keys,
        cache_breakpoints,
    }
}

fn request_cache_breakpoint_from_v3(
    breakpoint: &crate::prompt_cache_simulator::V3PromptCacheBreakpoint,
) -> RequestCacheBreakpoint {
    RequestCacheBreakpoint {
        block_index: breakpoint.block_index,
        source: request_breakpoint_source_from_v3(breakpoint.source),
        path: breakpoint.path.clone(),
        message_index: breakpoint.message_index,
        ttl: breakpoint.ttl.clone(),
        prefix_hash: breakpoint.prefix_key.clone(),
        prefix_token_count: breakpoint.prefix_bytes.get(),
        lookback_prefixes: breakpoint
            .lookback_prefixes
            .iter()
            .map(|prefix| RequestCacheLookbackPrefix {
                prefix_hash: prefix.prefix_key.clone(),
                content_block_index: prefix.content_block_index,
                lookback_distance: prefix.lookback_distance,
            })
            .collect(),
        token_estimate_source: Some(V3_TOKEN_ESTIMATE_SOURCE.to_owned()),
    }
}

fn plugin_cache_breakpoint_from_v3(
    breakpoint: &crate::prompt_cache_simulator::V3PromptCacheBreakpoint,
) -> CacheBreakpoint {
    CacheBreakpoint {
        block_index: saturating_u64_to_u32(breakpoint.block_index),
        source: plugin_cache_breakpoint_source(request_breakpoint_source_from_v3(
            breakpoint.source,
        )),
        path: breakpoint.path.clone(),
        message_index: breakpoint.message_index.map(saturating_u64_to_u32),
        prefix_hash: breakpoint.prefix_key.clone(),
        prefix_bytes: breakpoint.prefix_bytes,
        requested_ttl: plugin_ttl_class(breakpoint.ttl.as_deref()),
        lookback_prefixes: breakpoint
            .lookback_prefixes
            .iter()
            .map(|prefix| CacheLookbackPrefix {
                prefix_hash: prefix.prefix_key.clone(),
                content_block_index: saturating_u64_to_u32(prefix.content_block_index),
                lookback_distance: saturating_u64_to_u32(prefix.lookback_distance),
            })
            .collect(),
        token_estimate_source: Some(V3_TOKEN_ESTIMATE_SOURCE.to_owned()),
    }
}

#[derive(Clone, Default)]
pub(crate) struct RequestCacheMetadata {
    pub(crate) request_json: Option<Value>,
    pub(crate) thread_id: Option<String>,
    pub(crate) observed_session_id: Option<String>,
    pub(crate) request_kind: Option<String>,
    pub(crate) explicit_request_class: Option<String>,
    pub(crate) explicit_auxiliary: bool,
    pub(crate) claude_agent_id: Option<String>,
    pub(crate) claude_parent_agent_id: Option<String>,
    pub(crate) parent_session_id: Option<String>,
    pub(crate) client_app: Option<String>,
    pub(crate) session_id_source: Option<String>,
    pub(crate) cache_keepalive_surface: Option<String>,
    pub(crate) cache_keepalive_ttl: Option<cc_lb_storage_api::CacheTtl>,
    pub(crate) openai_selected_boundary: Option<String>,
    pub(crate) openai_replay_identity: Option<String>,
    requested_service_tier: Option<String>,
    message_id: Option<String>,
    message_index: Option<u64>,
    message_count: Option<u64>,
    cache_control_block_count: Option<u64>,
    thinking_budget_tokens: Option<u64>,
    reasoning_effort: Option<String>,
    thinking_level: Option<String>,
    cache_control_message_indices: Vec<u64>,
    pub(crate) cache_breakpoints: Vec<RequestCacheBreakpoint>,
    pub(crate) cache_prefix_hash: Option<String>,
    pub(crate) bundles: PromptCacheBundles,
    pub(crate) native_cache: Option<cc_lb_request_log::RequestNativeCacheObservation>,
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
                // Produced in-process by the current estimator (see
                // `request_cache_metadata_from_value_with_analysis_for_capability`).
                prefix_bytes: PrefixBytes::new(breakpoint.prefix_token_count),
                requested_ttl: plugin_ttl_class(breakpoint.ttl.as_deref()),
                lookback_prefixes: breakpoint
                    .lookback_prefixes
                    .iter()
                    .map(|prefix| CacheLookbackPrefix {
                        prefix_hash: prefix.prefix_hash.clone(),
                        content_block_index: saturating_u64_to_u32(prefix.content_block_index),
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
fn prompt_cache_token_scope(headers: &HeaderMap) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"cc-lb-token-cache-scope-v1\0");
    let credential = headers
        .get("x-api-key")
        .map(|value| ("x-api-key", value))
        .or_else(|| {
            headers
                .get("authorization")
                .map(|value| ("authorization", value))
        });
    if let Some((name, value)) = credential {
        hasher.update(&(name.len() as u64).to_le_bytes());
        hasher.update(name.as_bytes());
        hasher.update(&(value.as_bytes().len() as u64).to_le_bytes());
        hasher.update(value.as_bytes());
    } else {
        hasher.update(b"unauthenticated");
    }
    *hasher.finalize().as_bytes()
}

#[cfg(test)]
mod upstream_error_scrub_tests {
    use cc_lb_signer_anthropic_key::AnthropicKeySigner;

    use super::*;

    const KEY: &str = "sk-ant-api03-CANARYc4n4ry0123456789abcdefABCDEF-AAAA";
    const MARKER: &str = REQUEST_LOG_UPSTREAM_ERROR_BODY_TRUNCATION_MARKER;

    fn signer() -> AnthropicKeySigner {
        AnthropicKeySigner::new(KEY, None).expect("signer")
    }

    fn assert_no_key_fragment(stored: &str) {
        for fragment in KEY.as_bytes().windows(12) {
            let fragment = std::str::from_utf8(fragment).expect("ascii key");
            assert!(
                !stored.contains(fragment),
                "stored text keeps key fragment {fragment:?}"
            );
        }
    }

    /// Feeds `body` through the streaming collector in small chunks, the way
    /// the relay does, and renders the stored text.
    fn collected_and_stored(body: &[u8]) -> String {
        let mut collected = Vec::new();
        let mut truncated = false;
        for chunk in body.chunks(97) {
            append_bounded_upstream_error_body(&mut collected, chunk, &mut truncated);
        }
        scrubbed_upstream_error_body(&signer(), &collected, truncated)
    }

    fn body_with_key_at(offset: usize, total: usize) -> Vec<u8> {
        let mut body = vec![b'x'; offset];
        body.extend_from_slice(KEY.as_bytes());
        body.resize(total.max(body.len()), b'y');
        body
    }

    /// SEC-13: a key at the start, middle, end, or straddling the store cap is
    /// masked before truncation, on both the buffered and streaming paths.
    #[test]
    fn sec_13_key_anywhere_before_cap_leaves_no_fragment() {
        let cap = REQUEST_LOG_UPSTREAM_ERROR_BODY_MAX_BYTES;
        let total = cap + 4 * 1024;
        for offset in [
            0,
            cap / 2,
            cap - MARKER.len() - 20,
            cap - 20,
            cap - 1,
            cap + 100,
        ] {
            let body = body_with_key_at(offset, total);
            for stored in [
                collected_and_stored(&body),
                scrubbed_upstream_error_body(&signer(), &body, false),
            ] {
                assert!(stored.len() <= cap, "offset {offset}: {}", stored.len());
                assert!(stored.ends_with(MARKER), "offset {offset}");
                assert_no_key_fragment(&stored);
            }
        }
    }

    /// SEC-13: a short error body that echoes the key twice keeps its text
    /// but loses both copies, with no truncation marker.
    #[test]
    fn sec_13_repeated_key_in_short_body_is_masked() {
        let body = format!(r#"{{"error":"bad key {KEY} (again: {KEY})"}}"#);

        let stored = collected_and_stored(body.as_bytes());

        assert_eq!(
            stored,
            r#"{"error":"bad key [REDACTED] (again: [REDACTED])"}"#
        );
    }

    /// SEC-14 (unit): provider text without the key is handed back unchanged,
    /// and a key in the structured message is masked.
    #[test]
    fn sec_14_structured_message_is_masked_and_clean_text_kept() {
        let clean = br#"{"error":{"type":"rate_limit_error","message":"rate limit exceeded"}}"#;
        let echoed = format!(r#"{{"error":{{"type":"auth","message":"echo {KEY}"}}}}"#);

        let clean = usage_parser::observe_http_error_body(clean)
            .map(|error| error.into_parts(&signer()))
            .unwrap_or_default();
        let echoed = usage_parser::observe_http_error_body(echoed.as_bytes())
            .map(|error| error.into_parts(&signer()))
            .unwrap_or_default();

        assert_eq!(clean.1.as_deref(), Some("rate limit exceeded"));
        assert_eq!(echoed.1.as_deref(), Some("echo [REDACTED]"));
    }

    fn message_with_key_at(offset: usize) -> String {
        let mut message = "m".repeat(offset);
        message.push_str(KEY);
        message.push_str(&"t".repeat(2048));
        message
    }

    /// SEC-13: a key inside a structured `error.message`, at or straddling
    /// the 1024-byte message cap, is masked before the cap on the buffered,
    /// unauthorized, and mid-stream paths.
    #[test]
    fn sec_13_structured_message_key_at_cap_leaves_no_fragment() {
        for offset in [0, 1000, 1010, 1015, 1020, 1021, 1023, 1030] {
            let message = message_with_key_at(offset);
            let body = serde_json::json!({
                "type": "error",
                "error": { "type": "invalid_request_error", "message": message },
            })
            .to_string();
            let (_, buffered) = usage_parser::observe_http_error_body(body.as_bytes())
                .map(|error| error.into_parts(&signer()))
                .unwrap_or_default();
            let raw = format!("event: error\ndata: {body}\n\n");
            let event = parse_sse_event(raw.as_bytes());
            let streamed = usage_parser::detect_mid_stream_error(&event, &signer())
                .and_then(|error| error.error_message);
            for stored in [buffered, streamed] {
                let stored = stored.expect("message extracted");
                assert!(stored.len() <= 1024, "offset {offset}: {}", stored.len());
                assert!(stored.ends_with("..."), "offset {offset}");
                assert_no_key_fragment(&stored);
            }
        }
    }
}
#[cfg(test)]
mod prompt_cache_token_scope_tests {
    use super::*;

    #[test]
    fn x_api_key_scope_ignores_lower_precedence_authorization() {
        let mut first = HeaderMap::new();
        first.insert("x-api-key", HeaderValue::from_static("stable-key"));
        first.insert("authorization", HeaderValue::from_static("Bearer first"));
        let mut second = HeaderMap::new();
        second.insert("x-api-key", HeaderValue::from_static("stable-key"));
        second.insert("authorization", HeaderValue::from_static("Bearer second"));
        assert_eq!(
            prompt_cache_token_scope(&first),
            prompt_cache_token_scope(&second)
        );
    }

    #[test]
    fn authorization_scope_changes_without_x_api_key() {
        let mut first = HeaderMap::new();
        first.insert("authorization", HeaderValue::from_static("Bearer first"));
        let mut second = HeaderMap::new();
        second.insert("authorization", HeaderValue::from_static("Bearer second"));
        assert_ne!(
            prompt_cache_token_scope(&first),
            prompt_cache_token_scope(&second)
        );
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
    let analysis = value.map(|value| {
        let canonical_model = value
            .get("model")
            .and_then(Value::as_str)
            .map(canonical_model_id)
            .unwrap_or_default();
        PromptCacheAnalysisOutput {
            analysis: analyze_v3_prompt_cache(value, canonical_model),
            cacheable_breakpoint_prefix_keys: Vec::new(),
            literal_cacheable_breakpoint_prefix_keys: Vec::new(),
            timings: PromptCacheAnalysisTimings::default(),
        }
    });
    request_cache_metadata_from_value_with_analysis_for_capability(headers, value, analysis, None)
}

fn request_reasoning_metadata(
    value: &Value,
    capability: Option<ProviderCapability>,
) -> (Option<u64>, Option<String>) {
    let anthropic_thinking_budget = || {
        value
            .get("thinking")
            .filter(|thinking| thinking.get("type").and_then(Value::as_str) == Some("enabled"))
            .and_then(|thinking| thinking.get("budget_tokens"))
            .and_then(Value::as_u64)
            .filter(|budget| *budget <= 9_007_199_254_740_991)
    };
    let anthropic_reasoning_effort = || {
        value
            .get("output_config")
            .and_then(|config| config.get("effort"))
            .and_then(Value::as_str)
            .filter(|effort| !effort.is_empty() && effort.len() <= 64)
            .map(str::to_owned)
    };
    match capability {
        // Callers without a capability retain the historical Anthropic-shaped
        // extraction used by cache and preview metadata.
        None | Some(ProviderCapability::AnthropicMessages) => {
            (anthropic_thinking_budget(), anthropic_reasoning_effort())
        }
        Some(
            ProviderCapability::OpenaiChatCompletions
            | ProviderCapability::GeminiOpenaiChatCompletions,
        ) => (
            None,
            value
                .get("reasoning_effort")
                .and_then(Value::as_str)
                .filter(|effort| !effort.is_empty() && effort.len() <= 64)
                .map(str::to_owned),
        ),
        Some(ProviderCapability::OpenaiResponses) => (
            None,
            value
                .get("reasoning")
                .and_then(|reasoning| reasoning.get("effort"))
                .and_then(Value::as_str)
                .filter(|effort| !effort.is_empty() && effort.len() <= 64)
                .map(str::to_owned),
        ),
        Some(ProviderCapability::GeminiGenerateContent) => (
            value
                .get("generationConfig")
                .and_then(|config| config.get("thinkingConfig"))
                .and_then(|thinking| {
                    thinking
                        .get("thinkingBudget")
                        .or_else(|| thinking.get("thinking_budget"))
                })
                .and_then(Value::as_u64)
                .filter(|budget| *budget <= 9_007_199_254_740_991),
            None,
        ),
        _ => (None, None),
    }
}

/// Normalize field names only. Keep the requested value, including unknown
/// casing, visible to routing and admission rather than treating it as absent.
fn request_service_tier(value: &Value, capability: Option<ProviderCapability>) -> Option<&str> {
    let tier = value.get("service_tier").or_else(|| {
        (capability == Some(ProviderCapability::GeminiGenerateContent))
            .then(|| value.get("serviceTier"))
            .flatten()
    })?;
    let tier = tier.as_str()?;
    if capability == Some(ProviderCapability::GeminiGenerateContent) && tier.trim().is_empty() {
        return None;
    }
    Some(tier)
}

fn request_thinking_level(value: &Value, capability: Option<ProviderCapability>) -> Option<String> {
    if capability != Some(ProviderCapability::GeminiGenerateContent) {
        return None;
    }
    value
        .get("generationConfig")
        .and_then(|config| config.get("thinkingConfig"))
        .and_then(|thinking| {
            thinking
                .get("thinkingLevel")
                .or_else(|| thinking.get("thinking_level"))
        })
        .and_then(Value::as_str)
        .filter(|level| !level.is_empty() && level.len() <= 64)
        .map(str::to_owned)
}

fn bounded_openai_replay_component(raw: &str) -> String {
    let digest = Sha256::digest(raw.as_bytes());
    format!(
        "sha256:{}",
        digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
}
fn openai_input_digest_and_visible_tokens(value: &Value) -> (String, u64) {
    let Some(input) = value.get("input") else {
        return (String::new(), 0);
    };
    let Ok(input_bytes) = sonic_rs::to_vec(input) else {
        return (String::new(), 0);
    };
    let visible_tokens = (input_bytes.len() as u64 / 4).min(10_000_000);
    let digest = Sha256::digest(&input_bytes);
    let digest = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    (digest, visible_tokens)
}

#[cfg(test)]
pub(crate) fn openai_replay_identity(
    value: &Value,
    headers: &HeaderMap,
    model: &str,
    ttl: Option<cc_lb_storage_api::CacheTtl>,
    prefix_hash: Option<&str>,
) -> Option<String> {
    let (input_digest, _) = openai_input_digest_and_visible_tokens(value);
    openai_replay_identity_with_boundary(
        value,
        headers,
        model,
        ttl,
        prefix_hash,
        None,
        &input_digest,
    )
}

fn openai_replay_identity_with_boundary(
    value: &Value,
    headers: &HeaderMap,
    model: &str,
    ttl: Option<cc_lb_storage_api::CacheTtl>,
    prefix_hash: Option<&str>,
    selected_boundary: Option<&str>,
    input_digest: &str,
) -> Option<String> {
    let prompt_cache_key = value
        .get("prompt_cache_key")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let conversation_identity = match value.get("conversation") {
        Some(Value::String(id)) => format!("string:{id}"),
        Some(Value::Object(object)) => object
            .get("id")
            .and_then(Value::as_str)
            .map(|id| format!("object:{id}"))
            .unwrap_or_default(),
        _ => String::new(),
    };
    let organization = value
        .get("organization")
        .or_else(|| value.get("organization_id"))
        .and_then(Value::as_str)
        .or_else(|| {
            headers
                .get("openai-organization")
                .and_then(|value| value.to_str().ok())
        })
        .unwrap_or_default();
    let processing_region = value
        .get("processing_region")
        .or_else(|| value.get("inference_geo"))
        .and_then(Value::as_str)
        .or_else(|| {
            headers
                .get("openai-processing-region")
                .and_then(|value| value.to_str().ok())
        })
        .unwrap_or_default();
    let lifetime = ttl.map(|ttl| match ttl {
        cc_lb_storage_api::CacheTtl::Ttl30m => "30m",
        cc_lb_storage_api::CacheTtl::Ttl12h => "12h",
        cc_lb_storage_api::CacheTtl::Ttl5m => "5m",
        cc_lb_storage_api::CacheTtl::Ttl1h => "1h",
    });
    let namespace = format!(
        "openai-replay-v2|model={}|lifetime={}|prefix={}|boundary={}|organization={}|region={}|prompt_cache_key={}|conversation={}|input={}",
        bounded_openai_replay_component(model),
        bounded_openai_replay_component(lifetime.unwrap_or("unknown")),
        bounded_openai_replay_component(prefix_hash.unwrap_or("")),
        bounded_openai_replay_component(selected_boundary.unwrap_or("unknown")),
        bounded_openai_replay_component(organization),
        bounded_openai_replay_component(processing_region),
        bounded_openai_replay_component(prompt_cache_key),
        bounded_openai_replay_component(&conversation_identity),
        bounded_openai_replay_component(input_digest),
    );
    let digest = Sha256::digest(namespace.as_bytes());
    Some(format!(
        "openai-replay-v2:{}",
        digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    ))
}

fn request_cache_metadata_from_value_with_analysis_for_capability(
    headers: &HeaderMap,
    value: Option<&Value>,
    analysis: Option<PromptCacheAnalysisOutput>,
    capability: Option<ProviderCapability>,
) -> RequestCacheMetadata {
    let identity = request_identity_metadata(headers, value);
    let thread_id = identity.thread_id.clone();
    let observed_session_id = identity.observed_session_id.clone();
    let request_kind = identity.request_kind.clone();
    let explicit_request_class = identity.explicit_request_class.clone();
    let explicit_auxiliary = identity.explicit_auxiliary;
    let claude_agent_id = identity.claude_agent_id.clone();
    let claude_parent_agent_id = identity.claude_parent_agent_id.clone();
    let parent_session_id = identity.parent_session_id.clone();
    let client_app = identity.client_app.clone();
    let session_id_source = identity.session_id_source.clone();

    let Some(value) = value else {
        return RequestCacheMetadata {
            thread_id,
            observed_session_id,
            request_kind,
            explicit_request_class,
            explicit_auxiliary,
            claude_agent_id,
            claude_parent_agent_id,
            parent_session_id,
            client_app,
            session_id_source,
            ..Default::default()
        };
    };
    let canonical_model_id = value
        .get("model")
        .and_then(Value::as_str)
        .map(canonical_model_id)
        .unwrap_or_default()
        .to_owned();
    let modern_openai_interpretation = if capability == Some(ProviderCapability::OpenaiResponses)
        && crate::cache_keepalive::openai::is_gpt56_or_newer(&canonical_model_id)
    {
        Some(crate::cache_keepalive::openai::interpret_modern(value))
    } else {
        None
    };
    let native_cache = capability.and_then(|capability| {
        provider_cache::analyze_with_openai_interpretation(
            capability,
            value,
            &canonical_model_id,
            headers,
            modern_openai_interpretation.as_ref().map(Option::as_ref),
        )
    });
    let requested_service_tier = request_service_tier(value, capability).map(str::to_owned);

    let messages = value.get("messages").and_then(Value::as_array);
    let message_count = messages.map(|items| items.len() as u64);
    let message_index = message_count.and_then(|count| count.checked_sub(1));
    let message_id = messages
        .and_then(|items| items.last())
        .and_then(|message| message.get("id"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);

    let output = analysis.unwrap_or_else(|| PromptCacheAnalysisOutput {
        analysis: analyze_v3_prompt_cache(value, &canonical_model_id),
        cacheable_breakpoint_prefix_keys: Vec::new(),
        literal_cacheable_breakpoint_prefix_keys: Vec::new(),
        timings: PromptCacheAnalysisTimings::default(),
    });
    let literal_cacheable_prefix_keys = output
        .literal_cacheable_breakpoint_prefix_keys
        .into_iter()
        .collect::<HashSet<_>>();
    let official_cacheable_prefix_keys = output
        .cacheable_breakpoint_prefix_keys
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    let analysis = output.analysis;
    let official_bundle =
        prompt_cache_bundle_from_analysis(&analysis, official_cacheable_prefix_keys.clone());
    let has_billing_header = analysis.blocks.iter().any(|block| {
        block.source == V3PromptCacheBlockSource::System
            && block.path == "system[0]"
            && block.value.get("type").and_then(Value::as_str) == Some("text")
            && block
                .value
                .get("text")
                .and_then(Value::as_str)
                .is_some_and(|text| text.starts_with("x-anthropic-billing-header:"))
    });
    let literal_bundle = if has_billing_header {
        prompt_cache_bundle_from_breakpoints(
            &analysis.literal_breakpoints,
            literal_cacheable_prefix_keys,
        )
    } else {
        official_bundle.clone()
    };
    let cache_breakpoints = official_bundle.request_breakpoints.clone();
    let raw_cache_breakpoints = &literal_bundle.request_breakpoints;
    let mut cache_control_message_indices = raw_cache_breakpoints
        .iter()
        .filter_map(|breakpoint| breakpoint.message_index)
        .collect::<Vec<_>>();
    cache_control_message_indices.sort_unstable();
    cache_control_message_indices.dedup();
    let cache_control_block_count = raw_cache_breakpoints.len() as u64;
    let (thinking_budget_tokens, reasoning_effort) = request_reasoning_metadata(value, capability);
    let cache_prefix_hash = cache_breakpoints
        .last()
        .map(|breakpoint| breakpoint.prefix_hash.clone());
    let (openai_input_digest, openai_visible_prefix_tokens) =
        if capability == Some(ProviderCapability::OpenaiResponses) {
            openai_input_digest_and_visible_tokens(value)
        } else {
            (String::new(), 0)
        };
    let (cache_keepalive_surface, cache_keepalive_ttl, openai_selected_boundary) =
        if capability == Some(ProviderCapability::OpenaiResponses) {
            let interpretation = modern_openai_interpretation.flatten();
            let lifetime = interpretation.as_ref().and_then(|value| value.lifetime);
            let eligible = lifetime.is_some_and(|lifetime| {
                crate::cache_keepalive::openai::eligible_prefix(
                    &canonical_model_id,
                    openai_visible_prefix_tokens,
                    Some(lifetime),
                )
            });
            if eligible {
                (
                    Some("responses".to_owned()),
                    lifetime.map(|lifetime| match lifetime {
                        crate::cache_keepalive::openai::Lifetime::ThirtyMinutes => {
                            cc_lb_storage_api::CacheTtl::Ttl30m
                        }
                        crate::cache_keepalive::openai::Lifetime::TwelveHours => {
                            cc_lb_storage_api::CacheTtl::Ttl12h
                        }
                    }),
                    interpretation.and_then(|value| value.selected_boundary),
                )
            } else {
                (None, None, None)
            }
        } else {
            (None, None, None)
        };
    let replay_identity = (capability == Some(ProviderCapability::OpenaiResponses))
        .then(|| {
            openai_replay_identity_with_boundary(
                value,
                headers,
                &canonical_model_id,
                cache_keepalive_ttl,
                cache_prefix_hash.as_deref(),
                openai_selected_boundary.as_deref(),
                &openai_input_digest,
            )
        })
        .flatten();
    RequestCacheMetadata {
        request_json: None,
        thread_id,
        observed_session_id,
        request_kind,
        explicit_request_class,
        explicit_auxiliary,
        claude_agent_id,
        claude_parent_agent_id,
        parent_session_id,
        client_app,
        session_id_source,
        cache_keepalive_surface,
        cache_keepalive_ttl,
        openai_selected_boundary,
        openai_replay_identity: replay_identity,
        requested_service_tier,
        message_id,
        message_index,
        message_count,
        cache_control_block_count: Some(cache_control_block_count),
        thinking_budget_tokens,
        reasoning_effort,
        thinking_level: request_thinking_level(value, capability),
        cache_control_message_indices,
        cache_breakpoints,
        cache_prefix_hash,
        bundles: PromptCacheBundles {
            official: official_bundle,
            literal: literal_bundle,
        },
        native_cache,
        canonical_model_id,
    }
}

const HERMES_PRODUCT_TAG: &str = "product=hermes-agent";
const HERMES_CONVERSATION_TAG_PREFIX: &str = "conversation=";
const MAX_IDENTITY_VALUE_BYTES: usize = 512;

#[derive(Default)]
struct MetadataIdentity {
    session_id: Option<String>,
    parent_session_id: Option<String>,
}

#[derive(Default)]
struct RequestIdentityMetadata {
    thread_id: Option<String>,
    observed_session_id: Option<String>,
    request_kind: Option<String>,
    explicit_request_class: Option<String>,
    explicit_auxiliary: bool,
    claude_agent_id: Option<String>,
    claude_parent_agent_id: Option<String>,
    parent_session_id: Option<String>,
    client_app: Option<String>,
    session_id_source: Option<String>,
}

fn metadata_identity(value: &Value) -> MetadataIdentity {
    let Some(user_id) = value
        .get("metadata")
        .and_then(|metadata| metadata.get("user_id"))
        .and_then(Value::as_str)
    else {
        return MetadataIdentity::default();
    };

    if let Ok(parsed) = sonic_rs::from_str::<Value>(user_id)
        && parsed.is_object()
    {
        return MetadataIdentity {
            session_id: parsed
                .get("session_id")
                .and_then(Value::as_str)
                .and_then(bounded_identity),
            parent_session_id: parsed
                .get("parent_session_id")
                .and_then(Value::as_str)
                .and_then(bounded_identity),
        };
    }
    if let Some((_, rest)) = user_id.rsplit_once("_session_")
        && let Some(id) = bounded_identity(rest.trim())
    {
        return MetadataIdentity {
            session_id: Some(id),
            parent_session_id: None,
        };
    }
    if uuid::Uuid::parse_str(user_id.trim()).is_ok() {
        return MetadataIdentity {
            session_id: bounded_identity(user_id.trim()),
            parent_session_id: None,
        };
    }
    MetadataIdentity::default()
}

fn bounded_identity(value: &str) -> Option<String> {
    (!value.is_empty() && value.len() <= MAX_IDENTITY_VALUE_BYTES).then(|| value.to_owned())
}

fn header_identity(headers: &HeaderMap, name: &str) -> Option<String> {
    header_to_string(headers, name).and_then(|value| bounded_identity(&value))
}

fn client_metadata_session_identity(value: &Value) -> Option<(String, &'static str)> {
    ["/client_metadata/session_id", "/client_metadata/session-id"]
        .into_iter()
        .find_map(|pointer| {
            value
                .pointer(pointer)
                .and_then(Value::as_str)
                .and_then(bounded_identity)
                .map(|session_id| (session_id, "client_metadata.session_id"))
        })
}

fn request_session_identity(
    headers: &HeaderMap,
    metadata: &MetadataIdentity,
    value: Option<&Value>,
) -> Option<(String, &'static str)> {
    const SESSION_HEADERS: [&str; 12] = [
        "x-claude-code-session-id",
        "x-claude-session-id",
        "x-hermes-session-id",
        "x-aside-provider-session-id",
        "x-aside-session-id",
        "x-session-affinity",
        "x-opencode-session-id",
        "x-opencode-session",
        "x-session-id",
        "session_id",
        "session-id",
        "conversation_id",
    ];

    SESSION_HEADERS
        .into_iter()
        .find_map(|header| header_identity(headers, header).map(|value| (value, header)))
        .or_else(|| {
            let raw = header_to_string(headers, "x-codex-turn-metadata")?;
            (raw.len() <= 4096)
                .then(|| sonic_rs::from_str::<Value>(&raw).ok())?
                .and_then(|metadata| {
                    metadata
                        .get("session_id")
                        .and_then(Value::as_str)
                        .and_then(bounded_identity)
                        .map(|session_id| (session_id, "x-codex-turn-metadata"))
                })
        })
        .or_else(|| value.and_then(hermes_body_session_identity))
        .or_else(|| value.and_then(client_metadata_session_identity))
        .or_else(|| {
            metadata
                .session_id
                .clone()
                .map(|session_id| (session_id, "metadata.user_id"))
        })
}

fn derived_parent_session_id(session_id: &str) -> Option<String> {
    if let Some(rest) = session_id.strip_prefix("aside:worker:")
        && let Some((parent, profile)) = rest.split_once(':')
        && !parent.is_empty()
        && !profile.is_empty()
    {
        return bounded_identity(parent);
    }
    for marker in [":side:", ":spec:", ":btw:"] {
        if let Some((parent, suffix)) = session_id.split_once(marker)
            && !parent.is_empty()
            && !suffix.is_empty()
        {
            return bounded_identity(parent);
        }
    }
    None
}

fn request_identity_metadata(
    headers: &HeaderMap,
    value: Option<&Value>,
) -> RequestIdentityMetadata {
    let metadata = value.map(metadata_identity).unwrap_or_default();
    let (observed_session_id, session_id_source) =
        match request_session_identity(headers, &metadata, value) {
            Some((session_id, source)) => (Some(session_id), Some(source.to_owned())),
            None => (None, None),
        };
    let claude_agent_id = header_identity(headers, "x-claude-code-agent-id");
    let claude_parent_agent_id = header_identity(headers, "x-claude-code-parent-agent-id");
    let client_app = header_identity(headers, "x-app");
    let parent_session_id = header_identity(headers, "x-parent-session-id")
        .or_else(|| header_identity(headers, "x-opencode-parent-session-id"))
        .or_else(|| metadata.parent_session_id.clone())
        .or_else(|| {
            observed_session_id
                .as_deref()
                .and_then(derived_parent_session_id)
        })
        .or_else(|| {
            let observed = observed_session_id.as_deref()?;
            let meta_session = metadata.session_id.as_deref()?;
            (meta_session != observed && derived_parent_session_id(observed).is_some())
                .then(|| meta_session.to_owned())
        })
        .filter(|parent| observed_session_id.as_deref() != Some(parent.as_str()));
    let claude_request_class = header_identity(headers, "x-claude-code-request-class");
    let explicit_request_class = claude_request_class.clone();
    let explicit_auxiliary = claude_request_class
        .as_deref()
        .is_some_and(|class| class.trim().eq_ignore_ascii_case("auxiliary"));
    let request_kind = classify_client_request_kind_with_class(
        value,
        observed_session_id.as_deref(),
        parent_session_id.as_deref(),
        claude_agent_id.is_some(),
        claude_request_class.as_deref(),
    )
    .map(str::to_owned);

    RequestIdentityMetadata {
        thread_id: observed_session_id.clone(),
        observed_session_id,
        request_kind,
        explicit_request_class,
        explicit_auxiliary,
        claude_agent_id,
        claude_parent_agent_id,
        parent_session_id,
        client_app,
        session_id_source,
    }
}

fn hermes_body_session_identity(value: &Value) -> Option<(String, &'static str)> {
    let tags = value.get("tags").and_then(Value::as_array)?;
    if !tags
        .iter()
        .any(|tag| tag.as_str() == Some(HERMES_PRODUCT_TAG))
    {
        return None;
    }

    value
        .get("session_id")
        .and_then(Value::as_str)
        .and_then(bounded_identity)
        .map(|session_id| (session_id, "session_id"))
        .or_else(|| {
            tags.iter().filter_map(Value::as_str).find_map(|tag| {
                tag.strip_prefix(HERMES_CONVERSATION_TAG_PREFIX)
                    .and_then(bounded_identity)
                    .map(|session_id| (session_id, "tags.conversation"))
            })
        })
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

fn unix_now_ms(clock: &dyn Clock) -> u64 {
    unix_millis(clock.now()).min(u128::from(u64::MAX)) as u64
}

fn response_start_unix_secs(
    dispatch_unix_secs: u64,
    dispatch_started: Instant,
    response_start_at: Option<Instant>,
) -> u64 {
    match response_start_at {
        Some(at) => dispatch_unix_secs
            .saturating_add(at.saturating_duration_since(dispatch_started).as_secs()),
        None => dispatch_unix_secs,
    }
}

fn duration_to_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn duration_to_us(duration: Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}

fn attach_limit_headers_from_engine(
    headers: &mut HeaderMap,
    limit_engine: &LimitEngine,
    provider: Provider,
    key_id: &str,
    principal_id: &str,
) {
    for (name, value) in limit_engine.headers_for_provider(provider, key_id, principal_id) {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(&value),
        ) {
            headers.insert(name, value);
        }
    }
}

fn attach_limit_headers_from_record(
    headers: &mut HeaderMap,
    limit_engine: &LimitEngine,
    provider: Provider,
    key_id: &str,
    record: &StoredApiKeyRecord,
    default_limits: &[Limit],
) {
    for (name, value) in
        limit_engine.headers_for_provider_record(provider, key_id, record, default_limits)
    {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(&value),
        ) {
            headers.insert(name, value);
        }
    }
}

fn limit_rejection_response(
    capability: ProviderCapability,
    reason: RejectReason,
    model: &str,
    principal_id: &str,
    retry_after_seconds: Option<u64>,
) -> Response<Body> {
    let should_retry = match &reason {
        RejectReason::Unsatisfiable { .. } => Some("false"),
        RejectReason::RequestsRateLimit
        | RejectReason::TokenRateLimit { .. }
        | RejectReason::CostRateLimit
        | RejectReason::ConcurrentRateLimit => Some("true"),
        _ => None,
    };
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
        RejectReason::Unsatisfiable {
            kind,
            cap,
            requested,
        } => (
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limit_error",
            format!(
                "requested {} reservation {requested} cannot fit configured cap {cap}",
                limit_kind_name(kind)
            ),
            Some(limit_kind_name(kind)),
        ),
        RejectReason::CostUnavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            "server_error",
            format!("cost limits unavailable for model {model}"),
            None,
        ),
        RejectReason::UnsupportedLimitWindow { window_secs } => (
            StatusCode::SERVICE_UNAVAILABLE,
            "server_error",
            format!("API key limit window {window_secs}s is unsupported"),
            None,
        ),
        RejectReason::InvalidOutputCap { requested } => (
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            format!("requested output token bound {requested} must be zero or greater"),
            None,
        ),
        RejectReason::OutputCapExceeded { cap, requested } => (
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            format!("max_tokens {requested} exceeds key output limit {cap}"),
            None,
        ),
        RejectReason::OutputCapRequired { cap } => (
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            format!("provider output limit is required for configured cap {cap}"),
            None,
        ),
    };

    let mut response = Response::new(Body::from(provider_error_body_with_retry_after(
        capability,
        status,
        error_type,
        &message,
        limit_kind,
        retry_after_seconds,
    )));
    *response.status_mut() = status;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/json; charset=utf-8"),
    );
    if let Some(value) = should_retry {
        response
            .headers_mut()
            .insert("x-should-retry", HeaderValue::from_static(value));
    }
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
        RejectReason::PrincipalMissing
        | RejectReason::PrincipalDisabled
        | RejectReason::KeyRevoked
        | RejectReason::Expired
        | RejectReason::ModelNotAllowed
        | RejectReason::CostUnavailable
        | RejectReason::UnsupportedLimitWindow { .. }
        | RejectReason::InvalidOutputCap { .. }
        | RejectReason::OutputCapExceeded { .. }
        | RejectReason::OutputCapRequired { .. }
        | RejectReason::Unsatisfiable { .. } => None,
    }
}

fn limit_violation_name(reason: &RejectReason) -> Option<&'static str> {
    match reason {
        RejectReason::RequestsRateLimit => Some("Requests"),
        RejectReason::TokenRateLimit { kind } | RejectReason::Unsatisfiable { kind, .. } => {
            Some(audit_limit_kind_name(*kind))
        }
        RejectReason::CostRateLimit => Some("CostUsd"),
        RejectReason::ConcurrentRateLimit => Some("Concurrent"),
        RejectReason::PrincipalMissing
        | RejectReason::PrincipalDisabled
        | RejectReason::KeyRevoked
        | RejectReason::Expired
        | RejectReason::ModelNotAllowed
        | RejectReason::CostUnavailable
        | RejectReason::UnsupportedLimitWindow { .. }
        | RejectReason::InvalidOutputCap { .. }
        | RejectReason::OutputCapExceeded { .. }
        | RejectReason::OutputCapRequired { .. } => None,
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

fn upstream_for_record(record: &UpstreamRecord) -> Result<Upstream, String> {
    let base_url = record
        .base_url
        .clone()
        .or_else(|| Some(default_base_url(record.kind)));
    match record.kind {
        StorageUpstreamKind::AnthropicApiKey | StorageUpstreamKind::AnthropicOauth => {
            Ok(Upstream::AnthropicDirect { base_url })
        }
        StorageUpstreamKind::OpenaiOauth | StorageUpstreamKind::OpenaiApiKey => {
            Ok(Upstream::OpenaiDirect { base_url })
        }
        StorageUpstreamKind::GeminiApiKey | StorageUpstreamKind::GeminiOauth => {
            Ok(Upstream::GeminiDirect { base_url })
        }
    }
}

fn default_base_url(kind: StorageUpstreamKind) -> Url {
    let value = match kind {
        StorageUpstreamKind::AnthropicApiKey | StorageUpstreamKind::AnthropicOauth => {
            DEFAULT_ANTHROPIC_BASE_URL
        }
        StorageUpstreamKind::OpenaiOauth => DEFAULT_OPENAI_CODEX_BASE_URL,
        StorageUpstreamKind::OpenaiApiKey => DEFAULT_OPENAI_BASE_URL,
        StorageUpstreamKind::GeminiApiKey => DEFAULT_GEMINI_BASE_URL,
        StorageUpstreamKind::GeminiOauth => DEFAULT_GEMINI_CODE_ASSIST_BASE_URL,
    };
    Url::parse(value).expect("provider base URL parses")
}

fn upstream_kind_for_candidate(kind: StorageUpstreamKind) -> CandidateUpstreamKind {
    match kind {
        StorageUpstreamKind::AnthropicApiKey => CandidateUpstreamKind::AnthropicApiKey,
        StorageUpstreamKind::AnthropicOauth => CandidateUpstreamKind::AnthropicOauth,
        StorageUpstreamKind::OpenaiOauth => CandidateUpstreamKind::OpenaiOauth,
        StorageUpstreamKind::OpenaiApiKey => CandidateUpstreamKind::OpenaiApiKey,
        StorageUpstreamKind::GeminiApiKey => CandidateUpstreamKind::GeminiApiKey,
        StorageUpstreamKind::GeminiOauth => CandidateUpstreamKind::GeminiOauth,
    }
}

fn storage_upstream_kind_label(kind: StorageUpstreamKind) -> &'static str {
    match kind {
        StorageUpstreamKind::AnthropicApiKey => "anthropic_key",
        StorageUpstreamKind::AnthropicOauth => "anthropic_oauth",
        StorageUpstreamKind::OpenaiOauth => "openai_oauth",
        StorageUpstreamKind::OpenaiApiKey => "openai_api_key",
        StorageUpstreamKind::GeminiApiKey => "gemini_api_key",
        StorageUpstreamKind::GeminiOauth => "gemini_oauth",
    }
}

/// Distinct upstreams one request may try, counting the first. The
/// same-upstream 401 refresh retry does not count against it.
const MAX_UPSTREAM_ATTEMPTS: u8 = 3;

/// Why an attempt moved to another upstream. Every reason is a failure that
/// happened before the upstream processed the request, so retrying elsewhere
/// cannot duplicate a generation.
#[derive(Clone, Copy, Debug)]
enum FailoverReason {
    SignerBuild,
    Sign,
    Unauthorized,
    RateLimited,
}

impl FailoverReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::SignerBuild => "signer_build_failed",
            Self::Sign => "sign_failed",
            Self::Unauthorized => "upstream_401",
            Self::RateLimited => "upstream_429",
        }
    }
}

/// One routing decision: the selected upstream and the trace data that
/// `RouteCompleted` reports for it.
struct RoutedUpstream<'v> {
    record: &'v UpstreamRecord,
    route: RouteDecision,
    raw_passthrough_base_url: Option<Url>,
    routing_trace: RoutingTrace,
    selected_cache_score: Option<CacheScore>,
    selected_cache_match: Option<SelectedCacheMatch>,
    cache_bundle: PromptCacheBundle,
    route_ms: u64,
}

impl<'v> RoutedUpstream<'v> {
    fn new(
        record: &'v UpstreamRecord,
        route: RouteDecision,
        raw_passthrough_base_url: Option<Url>,
        pipeline_result: &FilterPipelineResult,
        terminal_decision: TerminalDecision,
        selected_cache_match: Option<SelectedCacheMatch>,
        route_ms: u64,
    ) -> Self {
        let selected_cache_score = pipeline_result
            .candidates
            .iter()
            .find(|candidate| candidate.upstream_id == route.upstream_id)
            .and_then(|candidate| candidate.cache_score.clone());
        Self {
            record,
            route,
            raw_passthrough_base_url,
            routing_trace: pipeline_result.routing_trace(terminal_decision),
            selected_cache_score,
            selected_cache_match,
            cache_bundle: PromptCacheBundle::default(),
            route_ms,
        }
    }

    fn with_cache_bundle(mut self, cache_bundle: PromptCacheBundle) -> Self {
        self.cache_bundle = cache_bundle;
        self
    }
}

fn route_for_record(
    record: &UpstreamRecord,
    cached: &PrincipalSpecCached,
) -> Result<(RouteDecision, Option<Url>), String> {
    let raw_passthrough_base_url = record.base_url.clone().or_else(|| {
        (record.kind == StorageUpstreamKind::GeminiOauth).then(|| default_base_url(record.kind))
    });
    let upstream = upstream_for_record(record)?;
    let dialect = cached
        .resolved_dialect(record.id)
        .cloned()
        .unwrap_or_else(|| native_route_dialect(record));
    Ok((
        RouteDecision {
            upstream_id: record.id,
            upstream,
            dialect,
        },
        raw_passthrough_base_url,
    ))
}

/// Construct the native HTTP dialect outside request execution when composing
/// a response-only provider plugin. OAuth path handling matches ordinary routes.
pub fn native_route_dialect(record: &UpstreamRecord) -> Arc<dyn UpstreamDialect> {
    Arc::new(RawPassthroughDialect {
        base_url: record
            .base_url
            .clone()
            .unwrap_or_else(|| default_base_url(record.kind)),
        openai_oauth: record.kind == StorageUpstreamKind::OpenaiOauth,
    })
}

/// Emit `RouteCompleted` for `routed`. A failover emits it again for the next
/// upstream; the request-event assembler keeps the last one.
fn emit_route_completed(
    observer: Option<&LifecycleContext>,
    routed: &RoutedUpstream<'_>,
    model: Option<&str>,
) {
    let Some(o) = observer else {
        return;
    };
    o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::RouteCompleted {
        event_id: o.event_id().to_owned(),
        result: Ok(route_info_for_routed(routed, model)),
        routing_trace: Some(routed.routing_trace.clone()),
    });
}

fn route_info_for_routed(
    routed: &RoutedUpstream<'_>,
    model: Option<&str>,
) -> cc_lb_lifecycle::RouteInfo {
    let upstream_id = routed.route.upstream_id;
    let selected_cache_score = routed.selected_cache_score.as_ref();
    let selected_quota_candidate = resolved_candidate_urgency(&routed.routing_trace, upstream_id);
    let subscription_trace = subscription_preference_trace(&routed.routing_trace);
    cc_lb_lifecycle::RouteInfo {
        upstream_id,
        upstream_name: routed.record.name.clone(),
        model: model.map(ToOwned::to_owned),
        upstream_kind: Some(storage_upstream_kind_label(routed.record.kind).to_owned()),
        route_ms: Some(routed.route_ms),
        routing_trace: Some(routed.routing_trace.clone()),
        predicted_cache_read_tokens: Some(
            selected_cache_score.map_or(0, |score| score.predicted_cache_read_tokens),
        ),
        matched_v3_cache_key: selected_cache_score
            .and_then(|score| score.matched_v3_cache_key.clone()),
        breakpoint_content_block_index: selected_cache_score
            .and_then(|score| score.breakpoint_content_block_index),
        matched_content_block_index: selected_cache_score
            .and_then(|score| score.matched_content_block_index),
        lookback_distance: selected_cache_score.and_then(|score| score.lookback_distance),
        predicted_cache_creation_tokens_5m: selected_cache_score
            .map(|score| score.predicted_cache_creation_tokens_5m),
        predicted_cache_creation_tokens_1h: selected_cache_score
            .map(|score| score.predicted_cache_creation_tokens_1h),
        token_estimate_source: selected_cache_score
            .and_then(|score| score.token_estimate_source.clone()),
        cache_breakpoints: Some(routed.cache_bundle.request_breakpoints.clone()),
        cache_prefix_hash: routed.cache_bundle.cache_prefix_hash.clone(),
        cache_value_micros: subscription_trace.and_then(|trace| {
            trace
                .candidates
                .iter()
                .find(|candidate| candidate.upstream_id == upstream_id)
                .and_then(|candidate| candidate.cache_value_micros)
        }),
        formula_winner_upstream_id: subscription_trace
            .and_then(|trace| trace.formula_winner_upstream_id),
        kept_upstream_id: subscription_trace.and_then(|trace| trace.kept_upstream_id),
        quota_urgency_5h: selected_quota_candidate.and_then(|candidate| candidate.quota_urgency_5h),
        quota_urgency_7d: selected_quota_candidate.and_then(|candidate| candidate.quota_urgency_7d),
        quota_urgency_combined: selected_quota_candidate
            .and_then(|candidate| candidate.quota_urgency_combined),
        quota_warning_multiplier: selected_quota_candidate
            .map(|candidate| candidate.warning_multiplier),
    }
}

/// Re-routes a request after a failover-eligible failure. Re-runs the
/// principal's filter pipeline over fresh candidates minus every upstream
/// already tried, so one routing convention covers the first pick and each
/// fallback.
struct FailoverRouter<'a> {
    view: &'a DynamicView,
    cached: &'a PrincipalSpecCached,
    router_pipeline: &'a RouterPipelineCache,
    ctx: &'a RequestContext,
    principal: &'a Principal,
    warm_entries: &'a HashMap<Uuid, Vec<WarmCacheEntry>>,
    cache_bundles: &'a PromptCacheBundles,
    request_kind: RequestKind,
    observer: Option<&'a LifecycleContext>,
    handle_span: &'a tracing::Span,
    /// False when request affinity pins the upstream (web-search continuity
    /// is account-bound).
    enabled: bool,
    excluded: Vec<Uuid>,
    upstream_attempts: u8,
}

impl<'a> FailoverRouter<'a> {
    /// Routes to the next upstream, or returns `None` when the caller must
    /// relay the current failure. The caller announces the new route.
    fn next(
        &mut self,
        lifecycle: &Lifecycle,
        failed: &RoutedUpstream<'_>,
        reason: FailoverReason,
    ) -> Option<RoutedUpstream<'a>> {
        if !self.enabled || self.upstream_attempts >= MAX_UPSTREAM_ATTEMPTS {
            return None;
        }
        let failed_id = failed.route.upstream_id;
        if !self.excluded.contains(&failed_id) {
            self.excluded.push(failed_id);
        }
        let route_start = Instant::now();
        let (mut candidates, mut selected_cache_matches) =
            build_candidates_with_matches_for_bundles(
                self.view,
                &self.principal.id,
                self.request_kind,
                &self.ctx.cache_breakpoints,
                self.warm_entries,
                Some(self.cache_bundles),
                &*lifecycle.clock,
            );
        candidates.retain(|candidate| !self.excluded.contains(&candidate.upstream_id));
        // The last allowed attempt goes to an API key when one remains, so the
        // attempt cap never strands a request on exhausted OAuth accounts.
        let last_attempt = self.upstream_attempts + 1 == MAX_UPSTREAM_ATTEMPTS;
        if last_attempt
            && candidates
                .iter()
                .any(|candidate| candidate.kind.credential_kind() == CredentialKind::ApiKey)
        {
            candidates
                .retain(|candidate| candidate.kind.credential_kind() == CredentialKind::ApiKey);
        }
        if candidates.is_empty() {
            return None;
        }
        let filters = self
            .router_pipeline
            .filters_for(self.request_kind.provider());
        let mut pipeline_result =
            execute_filter_pipeline(filters, self.ctx, self.principal, candidates);
        if let Some(o) = self.observer {
            for error in pipeline_result.internal_errors.drain(..) {
                o.record_internal_error(error);
            }
        }
        let terminal_decision = lifecycle.select_terminal_upstream(
            self.router_pipeline.terminal.clone(),
            &pipeline_result.candidates,
        );
        let selected = terminal_decision.upstream_id.and_then(|upstream_id| {
            pipeline_result
                .candidates
                .iter()
                .find(|candidate| candidate.upstream_id == upstream_id)
        })?;
        // The subscription filter keeps a lone survivor even when its quota is
        // proven exhausted; on failover that would revive an account already
        // known to be spent, so relay the current failure instead.
        if filters
            .iter()
            .any(|filter| filter.plugin_id() == BUILTIN_SUBSCRIPTION_PREFERENCE_ID)
            && subscription_hard_blocked(self.ctx.canonical_model_id.as_str(), selected)
        {
            return None;
        }
        let upstream_id = selected.upstream_id;
        let record = self
            .view
            .upstreams_snapshot()
            .iter()
            .find(|record| record.id == upstream_id)?;
        let (route, raw_passthrough_base_url) = match route_for_record(record, self.cached) {
            Ok(resolved) => resolved,
            Err(error) => {
                tracing::warn!(
                    upstream = %record.name,
                    %error,
                    "failover target has no upstream route; relaying the current failure"
                );
                return None;
            }
        };
        self.upstream_attempts += 1;
        let routed = RoutedUpstream::new(
            record,
            route,
            raw_passthrough_base_url,
            &pipeline_result,
            terminal_decision,
            selected_cache_matches.remove(&upstream_id),
            duration_to_ms(route_start.elapsed()),
        )
        .with_cache_bundle(self.cache_bundles.for_upstream(record).clone());
        tracing::warn!(
            from_upstream = %failed.record.name,
            from_upstream_id = %failed_id,
            to_upstream = %record.name,
            to_upstream_id = %upstream_id,
            reason = reason.as_str(),
            "failing over to another upstream"
        );
        metrics::counter!(
            "cc_lb_upstream_failover_total",
            "reason" => reason.as_str(),
            "from_kind" => storage_upstream_kind_label(failed.record.kind),
            "to_kind" => storage_upstream_kind_label(record.kind)
        )
        .increment(1);
        self.handle_span
            .record("cc_lb.upstream.name", record.name.as_str());
        self.handle_span
            .record("cc_lb.upstream.id", tracing::field::display(upstream_id));
        Some(routed)
    }
}

/// Start the next attempt: clear the previous attempt's observations (the
/// assembler resets on `UpstreamAttempt` with `attempt_num > 1`), then
/// announce the upstream. The first dispatch stays the TTFB origin.
fn announce_attempt(
    observer: Option<&LifecycleContext>,
    attempt_num: &mut u32,
    upstream_id: Uuid,
    attempt_timings: &mut AttemptTimings,
    first_dispatch_started: Option<Instant>,
) {
    if *attempt_num > 0 {
        attempt_timings.reset_attempt_stages();
        if let Some(o) = observer {
            o.reset_attempt_timings();
            if let Some(dispatch_started) = first_dispatch_started {
                o.set_io_timings(cc_lb_lifecycle::RequestIoTimings {
                    retry_overhead_ms: Some(dispatch_started.elapsed().as_secs_f64() * 1_000.0),
                    ..Default::default()
                });
            }
        }
    }
    *attempt_num += 1;
    if let Some(o) = observer {
        o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::UpstreamAttempt {
            event_id: o.event_id().to_owned(),
            attempt_num: *attempt_num,
            upstream_id,
        });
    }
}

fn is_sse_response(
    headers: &HeaderMap,
    stream_requested: bool,
    capability: ProviderCapability,
) -> bool {
    if !headers.contains_key(CONTENT_TYPE) {
        // ChatGPT's Responses backend can omit this header. An explicit
        // Content-Type, including JSON errors, always keeps its own semantics.
        return stream_requested
            && matches!(
                capability.usage_shape(),
                crate::provider_capability::UsageShape::OpenaiResponses
            );
    }
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value.split(';').next().is_some_and(|media_type| {
                media_type.trim().eq_ignore_ascii_case("text/event-stream")
            })
        })
}

fn to_usage_snapshot(u: &UsageCounts) -> cc_lb_lifecycle::UsageSnapshot {
    cc_lb_lifecycle::UsageSnapshot {
        input_tokens: u.input_tokens,
        output_tokens: u.output_tokens,
        input_image_tokens: u.input_image_tokens,
        output_image_tokens: u.output_image_tokens,
        tool_use_prompt_tokens: u.tool_use_prompt_tokens,
        cache_creation_input_tokens: u.cache_creation_input_tokens,
        cache_creation_input_tokens_5m: u.cache_creation_input_tokens_5m,
        cache_creation_input_tokens_1h: u.cache_creation_input_tokens_1h,
        cache_read_evidence: match u.cache_read_evidence {
            usage_parser::CacheTokenEvidence::Unreported => None,
            usage_parser::CacheTokenEvidence::Zero => {
                Some(cc_lb_request_log::RequestCacheEvidence::Zero)
            }
            usage_parser::CacheTokenEvidence::Positive => {
                Some(cc_lb_request_log::RequestCacheEvidence::Positive)
            }
            usage_parser::CacheTokenEvidence::Invalid => {
                Some(cc_lb_request_log::RequestCacheEvidence::Invalid)
            }
        },
        cache_creation_evidence: match u.cache_creation_evidence {
            usage_parser::CacheTokenEvidence::Unreported => None,
            usage_parser::CacheTokenEvidence::Zero => {
                Some(cc_lb_request_log::RequestCacheEvidence::Zero)
            }
            usage_parser::CacheTokenEvidence::Positive => {
                Some(cc_lb_request_log::RequestCacheEvidence::Positive)
            }
            usage_parser::CacheTokenEvidence::Invalid => {
                Some(cc_lb_request_log::RequestCacheEvidence::Invalid)
            }
        },
        cache_read_input_tokens: u.cache_read_input_tokens,
        cache_write_input_tokens: u.cache_write_input_tokens,
        cache_write_input_tokens_30m: u.cache_write_input_tokens_30m,
        cache_write_input_tokens_12h: u.cache_write_input_tokens_12h,
        thinking_tokens: u.thinking_tokens,
        web_search_requests: u.web_search_requests,
        web_fetch_requests: u.web_fetch_requests,
        service_tier: u.service_tier.clone(),
        inference_geo: u.inference_geo.clone(),
        iterations: u.iterations.clone(),
    }
}

fn principal_kind_lite(kind: DbPrincipalKind) -> PrincipalKindLite {
    match kind {
        DbPrincipalKind::Human => PrincipalKindLite::Human,
        DbPrincipalKind::Machine | DbPrincipalKind::Admin => PrincipalKindLite::Machine,
    }
}

fn db_principal_kind_as_str(kind: DbPrincipalKind) -> &'static str {
    match kind {
        DbPrincipalKind::Machine => "machine",
        DbPrincipalKind::Human => "human",
        DbPrincipalKind::Admin => "admin",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    use async_trait::async_trait;
    use cc_lb_storage_api::{
        SubscriptionQuotaSampleKind, SubscriptionQuotaSource, SubscriptionQuotaStatus,
        SubscriptionQuotaWindow,
        principal::{PrincipalKind as StoragePrincipalKind, PrincipalRecord},
        upstream::{UpstreamKind as StorageRecordKind, UpstreamRecord},
    };
    use http::header::{HeaderName, HeaderValue};

    use super::*;
    use crate::prompt_cache_simulator::V3_TOKEN_ESTIMATE_SOURCE;
    #[test]
    fn openai_replay_identity_namespaces_scope_dimensions() {
        let value = serde_json::json!({
            "model": "gpt-5.6",
            "input": [{"role": "user", "content": "prompt"}],
            "prompt_cache_key": "key-a",
            "conversation": {"id": "conversation-a"},
            "organization": "org-a",
            "processing_region": "us-east"
        });
        let headers = HeaderMap::new();
        let identity = super::openai_replay_identity(
            &value,
            &headers,
            "gpt-5.6",
            Some(cc_lb_storage_api::CacheTtl::Ttl30m),
            Some("prefix-a"),
        )
        .expect("identity");
        for (model, ttl, prefix, organization, region, key, conversation) in [
            (
                "gpt-5.7",
                Some(cc_lb_storage_api::CacheTtl::Ttl30m),
                Some("prefix-a"),
                "org-a",
                "us-east",
                "key-a",
                "conversation-a",
            ),
            (
                "gpt-5.6",
                Some(cc_lb_storage_api::CacheTtl::Ttl12h),
                Some("prefix-a"),
                "org-a",
                "us-east",
                "key-a",
                "conversation-a",
            ),
            (
                "gpt-5.6",
                Some(cc_lb_storage_api::CacheTtl::Ttl30m),
                Some("prefix-b"),
                "org-a",
                "us-east",
                "key-a",
                "conversation-a",
            ),
            (
                "gpt-5.6",
                Some(cc_lb_storage_api::CacheTtl::Ttl30m),
                Some("prefix-a"),
                "org-b",
                "us-east",
                "key-a",
                "conversation-a",
            ),
            (
                "gpt-5.6",
                Some(cc_lb_storage_api::CacheTtl::Ttl30m),
                Some("prefix-a"),
                "org-a",
                "eu-west",
                "key-a",
                "conversation-a",
            ),
            (
                "gpt-5.6",
                Some(cc_lb_storage_api::CacheTtl::Ttl30m),
                Some("prefix-a"),
                "org-a",
                "us-east",
                "key-b",
                "conversation-a",
            ),
            (
                "gpt-5.6",
                Some(cc_lb_storage_api::CacheTtl::Ttl30m),
                Some("prefix-a"),
                "org-a",
                "us-east",
                "key-a",
                "conversation-b",
            ),
        ] {
            let mut scoped = value.clone();
            scoped["organization"] = serde_json::Value::String(organization.to_owned());
            scoped["processing_region"] = serde_json::Value::String(region.to_owned());
            scoped["prompt_cache_key"] = serde_json::Value::String(key.to_owned());
            scoped["conversation"]["id"] = serde_json::Value::String(conversation.to_owned());
            let candidate = super::openai_replay_identity(&scoped, &headers, model, ttl, prefix)
                .expect("scoped identity");
            assert_ne!(identity, candidate);
        }
        let mut delimiter_left = value.clone();
        delimiter_left["organization"] = serde_json::Value::String("acme|region=us".to_owned());
        delimiter_left["processing_region"] = serde_json::Value::String("eu".to_owned());
        let mut delimiter_right = value.clone();
        delimiter_right["organization"] = serde_json::Value::String("acme".to_owned());
        delimiter_right["processing_region"] = serde_json::Value::String("us|region=eu".to_owned());
        assert_ne!(
            super::openai_replay_identity(
                &delimiter_left,
                &headers,
                "gpt-5.6",
                Some(cc_lb_storage_api::CacheTtl::Ttl30m),
                Some("prefix-a"),
            ),
            super::openai_replay_identity(
                &delimiter_right,
                &headers,
                "gpt-5.6",
                Some(cc_lb_storage_api::CacheTtl::Ttl30m),
                Some("prefix-a"),
            )
        );
        let mut prefix_left = value.clone();
        prefix_left["organization"] = serde_json::Value::String("tail".to_owned());
        let mut prefix_right = value.clone();
        prefix_right["organization"] =
            serde_json::Value::String("org-a|organization=tail".to_owned());
        assert_ne!(
            super::openai_replay_identity(
                &prefix_left,
                &headers,
                "gpt-5.6",
                Some(cc_lb_storage_api::CacheTtl::Ttl30m),
                Some("prefix-a|organization=org-a"),
            ),
            super::openai_replay_identity(
                &prefix_right,
                &headers,
                "gpt-5.6",
                Some(cc_lb_storage_api::CacheTtl::Ttl30m),
                Some("prefix-a"),
            )
        );
        let mut region_left = value.clone();
        region_left["processing_region"] =
            serde_json::Value::String("us|prompt_cache_key=key-a".to_owned());
        let mut region_right = value.clone();
        region_right["processing_region"] = serde_json::Value::String("us".to_owned());
        region_right["prompt_cache_key"] =
            serde_json::Value::String("key-a|prompt_cache_key=tail".to_owned());
        region_left["prompt_cache_key"] = serde_json::Value::String("tail".to_owned());
        assert_ne!(
            super::openai_replay_identity(
                &region_left,
                &headers,
                "gpt-5.6",
                Some(cc_lb_storage_api::CacheTtl::Ttl30m),
                Some("prefix-a"),
            ),
            super::openai_replay_identity(
                &region_right,
                &headers,
                "gpt-5.6",
                Some(cc_lb_storage_api::CacheTtl::Ttl30m),
                Some("prefix-a"),
            )
        );
        let mut key_left = value.clone();
        key_left["prompt_cache_key"] =
            serde_json::Value::String("key-a|conversation=left".to_owned());
        key_left["conversation"]["id"] = serde_json::Value::String("right".to_owned());
        let mut key_right = value.clone();
        key_right["prompt_cache_key"] = serde_json::Value::String("key-a".to_owned());
        key_right["conversation"]["id"] =
            serde_json::Value::String("left|conversation=right".to_owned());
        assert_ne!(
            super::openai_replay_identity(
                &key_left,
                &headers,
                "gpt-5.6",
                Some(cc_lb_storage_api::CacheTtl::Ttl30m),
                Some("prefix-a"),
            ),
            super::openai_replay_identity(
                &key_right,
                &headers,
                "gpt-5.6",
                Some(cc_lb_storage_api::CacheTtl::Ttl30m),
                Some("prefix-a"),
            )
        );
        let mut string_conversation = value.clone();
        string_conversation["conversation"] = serde_json::Value::String("conv-a".to_owned());
        let mut string_conversation_other = value.clone();
        string_conversation_other["conversation"] = serde_json::Value::String("conv-b".to_owned());
        assert_ne!(
            super::openai_replay_identity(
                &string_conversation,
                &headers,
                "gpt-5.6",
                Some(cc_lb_storage_api::CacheTtl::Ttl30m),
                Some("prefix-a"),
            ),
            super::openai_replay_identity(
                &string_conversation_other,
                &headers,
                "gpt-5.6",
                Some(cc_lb_storage_api::CacheTtl::Ttl30m),
                Some("prefix-a"),
            )
        );
        let mut object_conversation = value.clone();
        object_conversation["conversation"] = serde_json::json!({"id": "conv-a"});
        assert_ne!(
            super::openai_replay_identity(
                &string_conversation,
                &headers,
                "gpt-5.6",
                Some(cc_lb_storage_api::CacheTtl::Ttl30m),
                Some("prefix-a"),
            ),
            super::openai_replay_identity(
                &object_conversation,
                &headers,
                "gpt-5.6",
                Some(cc_lb_storage_api::CacheTtl::Ttl30m),
                Some("prefix-a"),
            )
        );
    }

    #[test]
    fn request_cache_metadata_uses_openai_responses_ttl_contract_for_keepalive() {
        use cc_lb_storage_api::CacheTtl;
        let input = vec![serde_json::json!({
            "role": "user",
            "content": "x".repeat(5_000)
        })];
        let metadata = |mut body: serde_json::Value| {
            body["model"] = serde_json::Value::String("gpt-5.6".to_owned());
            request_cache_metadata_from_value_with_analysis_for_capability(
                &HeaderMap::new(),
                Some(&body),
                None,
                Some(ProviderCapability::OpenaiResponses),
            )
        };

        for (name, options, expected) in [
            ("omitted", serde_json::json!({}), CacheTtl::Ttl30m),
            (
                "empty options",
                serde_json::json!({"prompt_cache_options": {}}),
                CacheTtl::Ttl30m,
            ),
            (
                "explicit 30m",
                serde_json::json!({"prompt_cache_options": {"ttl": "30m"}}),
                CacheTtl::Ttl30m,
            ),
            (
                "explicit 12h",
                serde_json::json!({"prompt_cache_options": {"ttl": "12h"}}),
                CacheTtl::Ttl12h,
            ),
        ] {
            let mut body = options;
            body["input"] = serde_json::Value::Array(input.clone());
            let result = metadata(body);
            assert_eq!(
                result.cache_keepalive_surface.as_deref(),
                Some("responses"),
                "{name} must enable Responses keep-alive"
            );
            assert_eq!(result.cache_keepalive_ttl, Some(expected), "{name}");
        }

        let mut breakpoint_override = serde_json::json!({
            "prompt_cache_options": {"ttl": "30m"},
            "input": [{
                "role": "user",
                "content": [{
                    "type": "input_text",
                    "text": "x".repeat(5_000),
                    "prompt_cache_breakpoint": {"ttl": "12h"}
                }]
            }]
        });
        let result = metadata(std::mem::take(&mut breakpoint_override));
        assert_eq!(result.cache_keepalive_ttl, Some(CacheTtl::Ttl12h));

        let mut inherited_breakpoint = serde_json::json!({
            "prompt_cache_options": {"ttl": "12h"},
            "input": [{
                "role": "user",
                "content": [{
                    "type": "input_text",
                    "text": "x".repeat(5_000),
                    "prompt_cache_breakpoint": {}
                }]
            }]
        });
        let result = metadata(std::mem::take(&mut inherited_breakpoint));
        assert_eq!(result.cache_keepalive_ttl, Some(CacheTtl::Ttl12h));
        let result = metadata(serde_json::json!({
            "metadata": {"prompt_cache_breakpoint": {"ttl": "12h"}},
            "tools": [{"input_schema": {"prompt_cache_breakpoint": {"ttl": "12h"}}}],
            "input": input.clone()
        }));
        assert_eq!(result.cache_keepalive_surface.as_deref(), Some("responses"));
        assert_eq!(result.cache_keepalive_ttl, Some(CacheTtl::Ttl30m));

        for mut body in [
            serde_json::json!({
                "prompt_cache_options": {"mode": "explicit"},
                "input": input.clone()
            }),
            serde_json::json!({
                "prompt_cache_options": {"ttl": "forever"},
                "input": input.clone()
            }),
            serde_json::json!({
                "prompt_cache_options": {"ttl": "30m"},
                "input": [{
                    "prompt_cache_breakpoint": {"ttl": "forever"},
                    "content": "x".repeat(5_000)
                }]
            }),
            serde_json::json!({
                "prompt_cache_retention": "30m",
                "input": input.clone()
            }),
        ] {
            let result = metadata(std::mem::take(&mut body));
            assert_eq!(result.cache_keepalive_surface, None);
            assert_eq!(result.cache_keepalive_ttl, None);
        }

        let mut unsupported = serde_json::json!({"input": input});
        unsupported["model"] = serde_json::Value::String("gpt-5.5".to_owned());
        let result = request_cache_metadata_from_value_with_analysis_for_capability(
            &HeaderMap::new(),
            Some(&unsupported),
            None,
            Some(ProviderCapability::OpenaiResponses),
        );
        assert_eq!(result.cache_keepalive_surface, None);
        assert_eq!(result.cache_keepalive_ttl, None);
    }

    #[test]
    fn native_observation_tokens_keep_zero_distinct_from_absent() {
        assert_eq!(
            super::native_tokens_for_evidence(cc_lb_request_log::RequestCacheEvidence::Zero, 0,),
            Some(0)
        );
        assert_eq!(
            super::native_tokens_for_evidence(
                cc_lb_request_log::RequestCacheEvidence::Unreported,
                0,
            ),
            None
        );
        assert_eq!(
            super::native_tokens_for_evidence(cc_lb_request_log::RequestCacheEvidence::Invalid, 10,),
            None
        );
    }

    #[test]
    fn operations_filter_subscription_and_api_key_credentials() {
        use StorageRecordKind as K;
        let all_kinds = [
            K::AnthropicApiKey,
            K::AnthropicOauth,
            K::OpenaiApiKey,
            K::OpenaiOauth,
            K::GeminiApiKey,
            K::GeminiOauth,
        ];
        let cases: &[(RequestKind, &[StorageRecordKind])] = &[
            (
                RequestKind::AnthropicMessages,
                &[K::AnthropicApiKey, K::AnthropicOauth],
            ),
            (
                RequestKind::OpenaiResponses,
                &[K::OpenaiApiKey, K::OpenaiOauth],
            ),
            (
                RequestKind::GeminiGenerateContent,
                &[K::GeminiApiKey, K::GeminiOauth],
            ),
            (
                RequestKind::GeminiCountTokens,
                &[K::GeminiApiKey, K::GeminiOauth],
            ),
            (
                RequestKind::AnthropicModels,
                &[K::AnthropicApiKey, K::AnthropicOauth],
            ),
            (RequestKind::AnthropicComplete, &[K::AnthropicApiKey]),
            (RequestKind::OpenaiResponsesInputTokens, &[K::OpenaiApiKey]),
            (
                RequestKind::OpenaiResponsesCompact,
                &[K::OpenaiApiKey, K::OpenaiOauth],
            ),
            (RequestKind::OpenaiChatCompletions, &[K::OpenaiApiKey]),
            (RequestKind::OpenaiCompletions, &[K::OpenaiApiKey]),
            (RequestKind::OpenaiEmbeddings, &[K::OpenaiApiKey]),
            (RequestKind::OpenaiModerations, &[K::OpenaiApiKey]),
            (RequestKind::OpenaiAudio, &[K::OpenaiApiKey]),
            (
                RequestKind::OpenaiImages,
                &[K::OpenaiApiKey, K::OpenaiOauth],
            ),
            (
                RequestKind::OpenaiModels,
                &[K::OpenaiApiKey, K::OpenaiOauth],
            ),
            (RequestKind::OpenaiCodexAuxiliary, &[K::OpenaiOauth]),
            (RequestKind::GeminiEmbedContent, &[K::GeminiApiKey]),
            (RequestKind::GeminiPredict, &[K::GeminiApiKey]),
            (RequestKind::GeminiModels, &[K::GeminiApiKey]),
            (RequestKind::GeminiOpenaiChatCompletions, &[K::GeminiApiKey]),
            (RequestKind::GeminiOpenaiEmbeddings, &[K::GeminiApiKey]),
            (RequestKind::GeminiOpenaiImages, &[K::GeminiApiKey]),
            (RequestKind::GeminiOpenaiModels, &[K::GeminiApiKey]),
            (RequestKind::OpenaiImageVariations, &[K::OpenaiApiKey]),
        ];
        for &(operation, allowed) in cases {
            for kind in all_kinds {
                assert_eq!(
                    operation.matches_upstream(kind),
                    allowed.contains(&kind),
                    "{operation:?} {kind:?}"
                );
            }
        }
    }

    #[test]
    fn openai_oauth_catalog_and_auxiliary_urls_preserve_client_queries() {
        for base in [None, Some("https://gateway.example/codex/")] {
            let root = base
                .unwrap_or("https://chatgpt.com/backend-api/codex/")
                .trim_end_matches('/');
            for (method, suffix) in [
                (Method::POST, "responses"),
                (Method::POST, "responses/compact"),
                (Method::GET, "models"),
                (Method::GET, "models/gpt-test"),
                (Method::POST, "images/generations"),
                (Method::POST, "images/edits"),
                (Method::POST, "alpha/search"),
                (Method::POST, "memories/trace_summarize"),
            ] {
                for query in [
                    None,
                    Some("client_version=1.2.3&trace=a%2Fb"),
                    Some("key=discard&client_version=2"),
                ] {
                    let path = format!("/v1/{suffix}");
                    let url = provider_signed_path(
                        StorageRecordKind::OpenaiOauth,
                        base,
                        method.clone(),
                        &path,
                        query,
                    );
                    let expected_query = query
                        .map(cc_lb_upstream::scrub_query_api_key)
                        .filter(|query| !query.is_empty());
                    assert_eq!(
                        url.as_str(),
                        format!(
                            "{root}/{suffix}{}",
                            expected_query
                                .map(|query| format!("?{query}"))
                                .unwrap_or_default()
                        ),
                        "{path}"
                    );
                }
            }
        }
    }

    #[test]
    fn multipart_operations_use_files_cap_only_on_exact_paths() {
        let config = LifecycleConfig {
            files_body_cap_bytes: 91,
            messages_body_cap_bytes: 17,
            ..LifecycleConfig::default()
        };
        for path in [
            "/v1/audio/transcriptions",
            "/v1/audio/translations",
            "/v1/images/edits",
            "/v1/images/variations",
        ] {
            assert_eq!(body_cap_for_path(&config, path), 91, "{path}");
        }
        for path in [
            "/v1/audio/speech",
            "/v1/images/generations",
            "/v1/images/edits/extra",
            "/v1/chat/completions",
            "/v1/messages",
        ] {
            assert_eq!(body_cap_for_path(&config, path), 17, "{path}");
        }
    }

    fn provider_signed_path(
        kind: StorageRecordKind,
        base_url: Option<&str>,
        method: http::Method,
        path: &str,
        query: Option<&str>,
    ) -> Url {
        let record = UpstreamRecord {
            id: Uuid::from_u128(8_537),
            name: "provider-url-regression".to_owned(),
            kind,
            base_url: base_url.map(|base| Url::parse(base).expect("test base URL parses")),
            enabled: true,
            ..Default::default()
        };
        let principal_view = failure_principal_view("principal-test", default_artifacts());
        let cached = principal_view
            .get("principal-test")
            .expect("test principal exists");
        let (route, _) = route_for_record(&record, cached).expect("provider route resolves");
        let context = RequestContext::builder()
            .request_id("provider-url-regression".to_owned())
            .method(method)
            .path(path.to_owned())
            .query(query.map(str::to_owned))
            .build();
        let principal = Principal {
            id: "principal-test".to_owned(),
            kind: PrincipalKind::InternalKey,
        };
        shape_request(
            route.dialect.as_ref(),
            &context.dialect_shape_context(),
            &route.upstream,
            &principal,
        )
        .expect("provider request shapes")
        .url()
        .clone()
    }

    #[test]
    fn openai_codex_default_http_and_websocket_use_responses_without_v1() {
        for method in [http::Method::POST, http::Method::GET] {
            let url = provider_signed_path(
                StorageRecordKind::OpenaiOauth,
                None,
                method.clone(),
                "/v1/responses",
                Some("trace=a%2Fb&beta=1"),
            );
            assert_eq!(
                url.as_str(),
                "https://chatgpt.com/backend-api/codex/responses?trace=a%2Fb&beta=1",
                "{method}"
            );
        }
    }

    #[test]
    fn openai_codex_custom_http_and_websocket_preserve_base_prefix() {
        for base in [
            "https://gateway.example/codex-proxy",
            "https://gateway.example/codex-proxy/",
            "https://gateway.example/codex-proxy/?stale=1",
        ] {
            for method in [http::Method::POST, http::Method::GET] {
                let url = provider_signed_path(
                    StorageRecordKind::OpenaiOauth,
                    Some(base),
                    method.clone(),
                    "/v1/responses",
                    Some("trace=a%2Fb&beta=1"),
                );
                assert_eq!(
                    url.as_str(),
                    "https://gateway.example/codex-proxy/responses?trace=a%2Fb&beta=1",
                    "{method} {base}"
                );
            }
        }
    }

    #[test]
    fn openai_codex_custom_base_query_is_removed_without_request_query() {
        let url = provider_signed_path(
            StorageRecordKind::OpenaiOauth,
            Some("https://gateway.example/codex-proxy?stale=1"),
            http::Method::POST,
            "/v1/responses",
            None,
        );
        assert_eq!(
            url.as_str(),
            "https://gateway.example/codex-proxy/responses"
        );
    }

    #[test]
    fn provider_signed_paths_outside_codex_responses_are_unchanged() {
        for (kind, base, path, expected) in [
            (
                StorageRecordKind::OpenaiApiKey,
                None,
                "/v1/responses",
                "https://api.openai.com/v1/responses?beta=1",
            ),
            (
                StorageRecordKind::OpenaiApiKey,
                Some("https://gateway.example/old-prefix?stale=1"),
                "/v1/responses",
                "https://gateway.example/v1/responses?beta=1",
            ),
            (
                StorageRecordKind::AnthropicApiKey,
                Some("https://gateway.example/old-prefix?stale=1"),
                "/v1/messages",
                "https://gateway.example/v1/messages?beta=1",
            ),
            (
                StorageRecordKind::GeminiApiKey,
                Some("https://gateway.example/old-prefix?stale=1"),
                "/v1beta/models/gemini-test:generateContent",
                "https://gateway.example/v1beta/models/gemini-test:generateContent?beta=1",
            ),
        ] {
            let url = provider_signed_path(kind, base, http::Method::POST, path, Some("beta=1"));
            assert_eq!(url.as_str(), expected, "{kind:?} {base:?} {path}");
        }
    }

    const TEST_MODEL: &str = "claude-sonnet-4-5-20250929";

    #[test]
    fn sse_event_split_reuses_allocation_and_releases_oversized_remainder() {
        let event = b"event: ping\ndata: {\"type\":\"ping\"}\n\n";
        let mut buffer = BytesMut::with_capacity(SSE_BUFFER_MAX_RETAINED_CAPACITY_BYTES * 2);
        buffer.extend_from_slice(event);
        buffer.extend_from_slice(b"partial");
        let input_ptr = buffer.as_ptr();

        let raw = split_sse_event(&mut buffer, event.len());

        assert_eq!(raw.as_ptr(), input_ptr, "split event must reuse allocation");
        assert_eq!(raw.as_ref(), event);
        assert_eq!(buffer.as_ref(), b"partial");
        assert!(buffer.capacity() <= SSE_BUFFER_MAX_RETAINED_CAPACITY_BYTES);
    }

    #[test]
    fn sse_downstream_boundary_tracker_handles_split_terminators() {
        let mut tracker = SseDownstreamBoundaryTracker::default();

        tracker.observe(b"event: ping\r\n");
        assert!(tracker.partial_event_pending());
        tracker.observe(b"\r");
        assert!(tracker.partial_event_pending());
        tracker.observe(b"\n");
        assert!(!tracker.partial_event_pending());

        tracker.observe(b"data: next\r");
        assert!(tracker.partial_event_pending());
        tracker.observe(b"\r");
        assert!(!tracker.partial_event_pending());

        tracker.observe(b"data: final\n");
        assert!(tracker.partial_event_pending());
        tracker.observe(b"\n");
        assert!(!tracker.partial_event_pending());
    }

    #[test]
    fn semantic_body_conversion_reuses_owned_and_identity_allocations() {
        let owned = vec![1_u8, 2, 3, 4];
        let owned_ptr = owned.as_ptr();
        let compressed_body = Bytes::from_static(b"compressed");

        let decoded = semantic_body_bytes(&compressed_body, std::borrow::Cow::Owned(owned));

        assert_eq!(decoded.as_ptr(), owned_ptr);

        let identity_body = Bytes::from_static(b"identity");
        let identity_ptr = identity_body.as_ptr();
        let identity = semantic_body_bytes(
            &identity_body,
            std::borrow::Cow::Borrowed(identity_body.as_ref()),
        );

        assert_eq!(identity.as_ptr(), identity_ptr);
    }

    #[test]
    fn build_candidates_cache_score() {
        let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000201").unwrap();
        let warm_entries = HashMap::from([(
            upstream_id,
            vec![
                warm_entry_priced("short", TtlClass::Ephemeral5m, 4_100_000_300, 12, 100),
                warm_entry_priced("long", TtlClass::Ephemeral1h, 4_100_003_600, 13, 250),
            ],
        )]);
        let view = cache_score_view(upstream_id);
        let breakpoints = vec![
            cache_breakpoint(0, "long", 250, TtlClass::Ephemeral1h),
            cache_breakpoint(1, "cold", 300, TtlClass::Ephemeral5m),
        ];
        let candidates = build_candidates(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            &breakpoints,
            &warm_entries,
            &cc_lb_clock::SystemClock,
        );
        let score = candidates[0].cache_score.as_ref().expect("cache score");
        assert_eq!(score.predicted_cache_read_tokens, 250);
        assert_eq!(score.predicted_cache_creation_tokens_5m, 50);
        assert_eq!(score.predicted_cache_creation_tokens_1h, 0);
        assert_eq!(score.predicted_uncached_input_tokens, 0);
        assert_eq!(score.predicted_expires_at_unix_secs, Some(4_100_003_600));
        assert_eq!(score.matched_breakpoint_index, Some(0));
        assert_eq!(score.confidence, 1.0);
        assert_eq!(score.ambiguity_reason, None);
    }

    #[test]
    fn build_candidates_cache_score_from_production_parsed_breakpoints() {
        let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000204").unwrap();
        let lorem: String = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ".repeat(300);
        let body = Bytes::from(format!(
            r#"{{"model":"{TEST_MODEL}","system":[{{"type":"text","text":"{lorem}","cache_control":{{"type":"ephemeral","ttl":"1h"}}}}],"messages":[{{"role":"user","content":"hi"}}],"max_tokens":16}}"#
        ));
        let breakpoints = parse_request_cache_breakpoints(&HeaderMap::new(), &body);
        assert_eq!(breakpoints.len(), 1);
        let prefix_hash = breakpoints[0].prefix_hash.clone();
        let prefix_token_count = breakpoints[0].prefix_bytes.get();
        assert!(prefix_token_count >= 1024);
        let warm_entries = HashMap::from([(
            upstream_id,
            vec![warm_entry_priced(
                &prefix_hash,
                TtlClass::Ephemeral1h,
                4_100_003_600,
                12,
                prefix_token_count,
            )],
        )]);
        let view = cache_score_view(upstream_id);
        let candidates = build_candidates(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            &breakpoints,
            &warm_entries,
            &cc_lb_clock::SystemClock,
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
        let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000299").unwrap();
        let warm_entries = HashMap::from([(
            upstream_id,
            vec![
                warm_entry("warm-prefix-a", TtlClass::Ephemeral1h, 4_100_003_600, 12),
                warm_entry("warm-prefix-b", TtlClass::Ephemeral5m, 4_100_000_300, 11),
            ],
        )]);
        let view = cache_score_view(upstream_id);
        let breakpoints = vec![
            cache_breakpoint(0, "warm-prefix-a", 2_400, TtlClass::Ephemeral1h),
            cache_breakpoint(1, "warm-prefix-b", 1_200, TtlClass::Ephemeral5m),
            cache_breakpoint(2, "cold-prefix", 800, TtlClass::Ephemeral5m),
        ];
        for _ in 0..ITERATIONS {
            let _ = build_candidates(
                &view,
                "principal",
                RequestKind::AnthropicMessages,
                &breakpoints,
                &warm_entries,
                &cc_lb_clock::SystemClock,
            );
        }
    }

    #[test]
    fn build_candidates_no_warm_returns_cold_creation_estimate() {
        let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000202").unwrap();
        let view = cache_score_view(upstream_id);
        let breakpoints = vec![cache_breakpoint(0, "cold", 100, TtlClass::Ephemeral5m)];
        let candidates = build_candidates(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            &breakpoints,
            &HashMap::from([(upstream_id, Vec::new())]),
            &cc_lb_clock::SystemClock,
        );
        assert_eq!(
            candidates[0]
                .cache_score
                .as_ref()
                .unwrap()
                .predicted_cache_read_tokens,
            0
        );
        assert_eq!(
            candidates[0]
                .cache_score
                .as_ref()
                .unwrap()
                .predicted_cache_creation_tokens_5m,
            100
        );
    }

    #[test]
    fn build_candidates_considers_all_warm_entries() {
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
        let entries = (0_u32..50)
            .map(|index| WarmCacheEntry {
                prefix_hash: format!("hash-{index}"),
                expires_at_unix_secs: 4_100_000_300,
                ttl_class: TtlClass::Ephemeral5m,
                last_observed_at_unix_secs: 1_700_000_100 + u64::from(50 - index),
                content_block_index: index,
                estimated_prefix_bytes: PrefixBytes::new(u64::from(index + 1)),
                hash_schema_version: HASH_SCHEMA_VERSION,
            })
            .collect::<Vec<_>>();
        let warm_entries = HashMap::from([(upstream_id, entries)]);
        let view = cache_score_view(upstream_id);
        let candidates = build_candidates(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            &breakpoints,
            &warm_entries,
            &cc_lb_clock::SystemClock,
        );
        let score = candidates[0].cache_score.as_ref().expect("cache score");
        assert_eq!(score.predicted_cache_read_tokens, 50);
        assert_eq!(score.matched_breakpoint_index, Some(49));
    }

    #[test]
    fn issue_1002_default_observations_stay_warm_until_provider_ttl() {
        let dispatch = 1_800_000_000;
        let defaults = PromptCacheShadowConfig::default();
        let explicit_grace = PromptCacheShadowConfig {
            grace_margin_secs: 60,
        };
        for (ttl, ttl_secs) in [(TtlClass::Ephemeral5m, 300), (TtlClass::Ephemeral1h, 3_600)] {
            for is_hit in [false, true] {
                for (config_name, config, warm_for) in [
                    ("default", &defaults, ttl_secs),
                    ("explicit_grace", &explicit_grace, ttl_secs - 60),
                ] {
                    let context = PromptCacheObservationContext {
                        upstream_id: Uuid::from_u128(1002),
                        canonical_model_id: TEST_MODEL.to_owned(),
                        cache_breakpoints: vec![cache_breakpoint(3, "cached", 2_400, ttl)],
                        cacheable_breakpoint_prefix_keys: cacheable_prefix_keys(&["cached"]),
                        selected_match: is_hit.then(|| selected_match_hit("cached", 3, ttl, 2_400)),
                        native_cache: None,
                        sink: Arc::new(RecordingPromptCacheObservationSink::new(Vec::new())),
                        grace_margin_secs: config.grace_margin_secs,
                    };
                    let decoded = decode_prompt_cache_observations_pure(
                        &context,
                        PromptCacheUsage {
                            cache_creation_input_tokens: if is_hit { 0 } else { 2_400 },
                            cache_read_input_tokens: if is_hit { 2_400 } else { 0 },
                            read_only_evidence: false,
                            ..PromptCacheUsage::default()
                        },
                        dispatch,
                    );
                    let [observation] = decoded.observations.as_slice() else {
                        panic!("successful cache usage must decode one observation");
                    };
                    let entries = [WarmCacheEntry {
                        prefix_hash: observation.prefix_hash.clone(),
                        expires_at_unix_secs: observation.expires_at_unix_secs,
                        ttl_class: observation.ttl_class,
                        last_observed_at_unix_secs: dispatch,
                        content_block_index: observation.prefix_content_block_index,
                        estimated_prefix_bytes: observation.estimated_prefix_bytes,
                        hash_schema_version: HASH_SCHEMA_VERSION,
                    }];
                    for elapsed in [269, 270, warm_for - 1, warm_for] {
                        let expected_warm = elapsed < warm_for;
                        let matched = resolve_longest_warm_match(
                            &entries,
                            &context.cache_breakpoints,
                            dispatch + elapsed,
                        );
                        assert_eq!(
                            matched.is_some(),
                            expected_warm,
                            "{config_name} {ttl:?} hit={is_hit} at +{elapsed}s",
                        );
                        let Some(score) = build_cache_score_from_match(
                            &context.cache_breakpoints,
                            matched.as_ref(),
                        ) else {
                            panic!("a cacheable request must have a cache routing score");
                        };
                        assert_eq!(
                            score.predicted_cache_read_tokens,
                            if expected_warm { 2_400 } else { 0 },
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn response_decoder_decodes_observations_without_side_effects() {
        let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000221").unwrap();
        let now = 1_800_000_000;
        let cache = Arc::new(
            RecordingPromptCacheObservationSink::new(vec![
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
            cacheable_breakpoint_prefix_keys: cacheable_prefix_keys(&["write"]),
            selected_match: Some(selected_match_hit("hit", 1, TtlClass::Ephemeral1h, 2_400)),
            native_cache: None,
            sink: cache.clone(),
            grace_margin_secs: cache.grace_margin_secs(),
        };

        let decoded = decode_prompt_cache_observations_pure(
            &context,
            PromptCacheUsage {
                cache_creation_input_tokens: 800,
                cache_read_input_tokens: 2_400,
                ..PromptCacheUsage::default()
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
    fn provider_observation_without_lifetime_keeps_expiry_unknown() {
        let sink = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()));
        let context = PromptCacheObservationContext {
            upstream_id: Uuid::from_u128(1_126),
            canonical_model_id: "gpt-5.6".to_owned(),
            cache_breakpoints: Vec::new(),
            cacheable_breakpoint_prefix_keys: HashSet::new(),
            selected_match: None,
            native_cache: Some(cc_lb_request_log::RequestNativeCacheObservation {
                provider: "openai".to_owned(),
                mode: "implicit".to_owned(),
                evidence: cc_lb_request_log::RequestCacheEvidence::Positive,
                cached_tokens: Some(128),
                ..Default::default()
            }),
            sink: sink.clone(),
            grace_margin_secs: 0,
        };

        publish_provider_cache_observation(&context, PromptCacheUsage::default(), 1_800_000_000);

        let records = sink.provider_upserts();
        let [record] = records.as_slice() else {
            panic!("expected one provider observation");
        };
        assert_eq!(record.retention_secs, None);
        assert_eq!(record.expires_at_unix_secs, None);
    }

    #[test]
    fn websocket_native_observation_publication_uses_lifecycle_clock() {
        let sink = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()));
        let lifecycle =
            read_inference_lifecycle(upstream_response(StatusCode::OK, b""), sink.clone(), None);
        let context = PromptCacheObservationContext {
            upstream_id: Uuid::from_u128(1_126),
            canonical_model_id: "gpt-5.6".to_owned(),
            cache_breakpoints: Vec::new(),
            cacheable_breakpoint_prefix_keys: HashSet::new(),
            selected_match: None,
            native_cache: Some(cc_lb_request_log::RequestNativeCacheObservation {
                provider: "openai".to_owned(),
                mode: "implicit".to_owned(),
                ..Default::default()
            }),
            sink: sink.clone(),
            grace_margin_secs: 0,
        };
        let usage = UsageCounts {
            cache_read_input_tokens: 128,
            cache_read_evidence: usage_parser::CacheTokenEvidence::Positive,
            ..UsageCounts::default()
        };

        lifecycle.publish_websocket_provider_cache_observation(&context, &usage);

        let records = sink.provider_upserts();
        let [record] = records.as_slice() else {
            panic!("expected one provider observation");
        };
        assert_eq!(record.upstream_id, context.upstream_id);
        assert_eq!(record.canonical_model_id, context.canonical_model_id);
        assert_eq!(record.observed_cached_tokens, Some(128));
        assert_eq!(record.last_observed_at_unix_secs, 1_800_000_000);
    }

    #[test]
    fn hit_observation_uses_sliding_ttl_from_now() {
        let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000224").unwrap();
        let now = 1_800_000_000;
        let near_expiry = now + 30;
        let cache = Arc::new(
            RecordingPromptCacheObservationSink::new(vec![warm_entry(
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
            cacheable_breakpoint_prefix_keys: HashSet::new(),
            selected_match: Some(selected_match_hit("hit", 0, TtlClass::Ephemeral5m, 2_400)),
            native_cache: None,
            sink: cache.clone(),
            grace_margin_secs: cache.grace_margin_secs(),
        };

        let decoded = decode_prompt_cache_observations_pure(
            &context,
            PromptCacheUsage {
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 2_400,
                ..PromptCacheUsage::default()
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
            RecordingPromptCacheObservationSink::new(vec![warm_entry(
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
            cacheable_breakpoint_prefix_keys: HashSet::new(),
            selected_match: Some(selected_match_hit("hit", 0, TtlClass::Ephemeral5m, 2_400)),
            native_cache: None,
            sink: cache.clone(),
            grace_margin_secs: cache.grace_margin_secs(),
        };

        let decoded = decode_prompt_cache_observations_pure(
            &context,
            PromptCacheUsage {
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 2_400,
                ..PromptCacheUsage::default()
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
        let cache = Arc::new(RecordingPromptCacheObservationSink::new(vec![warm_entry(
            "hit",
            TtlClass::Ephemeral5m,
            now + 120,
            1,
        )]));
        let context = PromptCacheObservationContext {
            upstream_id,
            canonical_model_id: TEST_MODEL.to_owned(),
            cache_breakpoints: vec![cache_breakpoint(0, "hit", 1_200, TtlClass::Ephemeral5m)],
            cacheable_breakpoint_prefix_keys: HashSet::new(),
            selected_match: None,
            native_cache: None,
            sink: cache.clone(),
            grace_margin_secs: cache.grace_margin_secs(),
        };

        let decoded = decode_prompt_cache_observations_pure(
            &context,
            PromptCacheUsage {
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
                ..PromptCacheUsage::default()
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
        let cache = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()));
        let context = PromptCacheObservationContext {
            upstream_id,
            canonical_model_id: TEST_MODEL.to_owned(),
            cache_breakpoints: vec![cache_breakpoint(0, "tiny", 1_023, TtlClass::Ephemeral5m)],
            cacheable_breakpoint_prefix_keys: HashSet::new(),
            selected_match: None,
            native_cache: None,
            sink: cache.clone(),
            grace_margin_secs: cache.grace_margin_secs(),
        };

        let decoded = decode_prompt_cache_observations_pure(
            &context,
            PromptCacheUsage {
                cache_creation_input_tokens: 1_023,
                cache_read_input_tokens: 0,
                ..PromptCacheUsage::default()
            },
            cache.clock_now_unix_secs(),
        );

        assert!(decoded.observations.is_empty());
        assert_eq!(decoded.dropped_below_threshold, 1);
        assert!(cache.upserts().is_empty());
    }

    #[test]
    fn observation_uses_carried_match_under_concurrent_mutation() {
        let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000226").unwrap();
        let now = 1_800_000_000;
        let cache = Arc::new(
            RecordingPromptCacheObservationSink::new(vec![warm_entry(
                "stale-different",
                TtlClass::Ephemeral5m,
                now + 120,
                1,
            )])
            .with_clock_now(now),
        );
        let context = PromptCacheObservationContext {
            upstream_id,
            canonical_model_id: TEST_MODEL.to_owned(),
            cache_breakpoints: vec![
                cache_breakpoint(0, "shallow", 1_200, TtlClass::Ephemeral5m),
                cache_breakpoint(1, "hit", 2_400, TtlClass::Ephemeral1h),
            ],
            cacheable_breakpoint_prefix_keys: HashSet::new(),
            selected_match: Some(selected_match_hit("hit", 1, TtlClass::Ephemeral1h, 2_400)),
            native_cache: None,
            sink: cache.clone(),
            grace_margin_secs: cache.grace_margin_secs(),
        };

        let decoded = decode_prompt_cache_observations_pure(
            &context,
            PromptCacheUsage {
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 2_400,
                ..PromptCacheUsage::default()
            },
            now,
        );

        assert_eq!(decoded.observations.len(), 1);
        assert_eq!(decoded.observations[0].prefix_hash, "hit");
        assert_eq!(decoded.observations[0].ttl_class, TtlClass::Ephemeral1h);
    }

    #[test]
    fn buffered_and_sse_observations_are_equal() {
        let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000227").unwrap();
        let now = 1_800_000_000;
        let cache =
            Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()).with_clock_now(now));
        let context = PromptCacheObservationContext {
            upstream_id,
            canonical_model_id: TEST_MODEL.to_owned(),
            cache_breakpoints: vec![
                cache_breakpoint(0, "hit", 2_400, TtlClass::Ephemeral1h),
                cache_breakpoint(1, "write", 3_200, TtlClass::Ephemeral5m),
            ],
            cacheable_breakpoint_prefix_keys: cacheable_prefix_keys(&["write"]),
            selected_match: Some(selected_match_hit("hit", 0, TtlClass::Ephemeral1h, 2_400)),
            native_cache: None,
            sink: cache.clone(),
            grace_margin_secs: cache.grace_margin_secs(),
        };
        let usage = PromptCacheUsage {
            cache_creation_input_tokens: 800,
            cache_read_input_tokens: 2_400,
            ..PromptCacheUsage::default()
        };

        let buffered = decode_prompt_cache_observations_pure(&context, usage, now);
        let sse = decode_prompt_cache_observations_pure(&context, usage, now);

        assert_eq!(buffered.observations, sse.observations);
        assert_eq!(
            buffered.dropped_below_threshold,
            sse.dropped_below_threshold
        );
    }

    #[test]
    fn refresh_non_breakpoint_nk_hit_on_read() {
        let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000230").unwrap();
        let now = 1_800_000_000;
        let cache =
            Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()).with_clock_now(now));
        let context = PromptCacheObservationContext {
            upstream_id,
            canonical_model_id: TEST_MODEL.to_owned(),
            cache_breakpoints: vec![cache_breakpoint(2, "owner", 5_000, TtlClass::Ephemeral5m)],
            cacheable_breakpoint_prefix_keys: HashSet::new(),
            selected_match: Some(SelectedCacheMatch {
                prefix_hash: "nk".to_owned(),
                content_block_index: 0,
                ttl_class: TtlClass::Ephemeral5m,
                estimated_prefix_bytes: PrefixBytes::new(2_000),
            }),
            native_cache: None,
            sink: cache.clone(),
            grace_margin_secs: cache.grace_margin_secs(),
        };

        let decoded = decode_prompt_cache_observations_pure(
            &context,
            PromptCacheUsage {
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 2_000,
                ..PromptCacheUsage::default()
            },
            now,
        );

        assert_eq!(decoded.observations.len(), 1);
        let hit = &decoded.observations[0];
        assert_eq!(hit.kind, DecodedPromptCacheObservationKind::Hit);
        assert_eq!(
            hit.prefix_hash, "nk",
            "refresh must target the matched N-k prefix, not the owning breakpoint"
        );
        assert_eq!(hit.prefix_content_block_index, 0);
        assert_eq!(hit.estimated_prefix_bytes, PrefixBytes::new(2_000));
    }

    #[test]
    fn hit_refresh_republishes_current_estimator_source() {
        let upstream_id = Uuid::from_u128(76_604);
        let now = 1_800_000_000;
        let cache =
            Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()).with_clock_now(now));
        let context = PromptCacheObservationContext {
            upstream_id,
            canonical_model_id: TEST_MODEL.to_owned(),
            cache_breakpoints: vec![cache_breakpoint(2, "owner", 5_000, TtlClass::Ephemeral5m)],
            cacheable_breakpoint_prefix_keys: HashSet::new(),
            selected_match: Some(selected_match_hit("nk", 0, TtlClass::Ephemeral5m, 2_000)),
            native_cache: None,
            sink: cache.clone(),
            grace_margin_secs: cache.grace_margin_secs(),
        };

        let decoded = decode_prompt_cache_observations_pure(
            &context,
            PromptCacheUsage {
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 2_000,
                ..PromptCacheUsage::default()
            },
            now,
        );
        publish_prompt_cache_observations(&context, &decoded, now);

        let records = cache.upserts();
        assert_eq!(records.len(), 1, "a hit refresh publishes exactly one row");
        let record = records.first().expect("hit refresh record");
        assert_eq!(record.v3_prefix_key, "nk");
        assert_eq!(record.token_estimate_source, PrefixBytes::ESTIMATE_SOURCE);
        assert_eq!(record.estimated_prefix_tokens, 2_000);
        assert_eq!(
            PrefixBytes::from_persisted(
                record.estimated_prefix_tokens,
                &record.token_estimate_source
            ),
            Some(PrefixBytes::new(2_000)),
            "the republished row must decode as the matched bytes"
        );
    }

    #[test]
    fn read_zero_does_not_refresh_predicted_match() {
        let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000231").unwrap();
        let now = 1_800_000_000;
        let cache =
            Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()).with_clock_now(now));
        let context = PromptCacheObservationContext {
            upstream_id,
            canonical_model_id: TEST_MODEL.to_owned(),
            cache_breakpoints: vec![cache_breakpoint(0, "hit", 2_400, TtlClass::Ephemeral5m)],
            cacheable_breakpoint_prefix_keys: cacheable_prefix_keys(&["hit"]),
            selected_match: Some(selected_match_hit("hit", 0, TtlClass::Ephemeral5m, 2_400)),
            native_cache: None,
            sink: cache.clone(),
            grace_margin_secs: cache.grace_margin_secs(),
        };

        let decoded = decode_prompt_cache_observations_pure(
            &context,
            PromptCacheUsage {
                cache_creation_input_tokens: 2_400,
                cache_read_input_tokens: 0,
                ..PromptCacheUsage::default()
            },
            now,
        );

        assert!(
            decoded
                .observations
                .iter()
                .all(|obs| obs.kind != DecodedPromptCacheObservationKind::Hit),
            "cache_read==0 must not refresh the predicted match as a Hit"
        );
        assert!(cache.upserts().is_empty());
    }

    #[test]
    fn read_positive_no_predict_without_eligible_key_stays_unknown() {
        let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000232").unwrap();
        let now = 1_800_000_000;
        let cache =
            Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()).with_clock_now(now));
        let context = PromptCacheObservationContext {
            upstream_id,
            canonical_model_id: TEST_MODEL.to_owned(),
            cache_breakpoints: vec![cache_breakpoint(
                0,
                "unknown-key",
                2_400,
                TtlClass::Ephemeral5m,
            )],
            cacheable_breakpoint_prefix_keys: HashSet::new(),
            selected_match: None,
            native_cache: None,
            sink: cache.clone(),
            grace_margin_secs: cache.grace_margin_secs(),
        };

        let decoded = decode_prompt_cache_observations_pure(
            &context,
            PromptCacheUsage {
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 2_400,
                ..PromptCacheUsage::default()
            },
            now,
        );

        assert!(
            decoded.observations.is_empty(),
            "a positive read with no eligible breakpoint (empty no-eligible-key control) must not fabricate a Hit key"
        );
        assert!(cache.upserts().is_empty());
    }

    #[test]
    fn creation_persists_only_fold_marked_breakpoints() {
        let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000233").unwrap();
        let now = 1_800_000_000;
        let cache =
            Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()).with_clock_now(now));
        let context = PromptCacheObservationContext {
            upstream_id,
            canonical_model_id: TEST_MODEL.to_owned(),
            cache_breakpoints: vec![
                cache_breakpoint(0, "b0", 2_400, TtlClass::Ephemeral5m),
                cache_breakpoint(1, "b1", 2_500, TtlClass::Ephemeral5m),
                cache_breakpoint(2, "b2", 2_600, TtlClass::Ephemeral5m),
                cache_breakpoint(3, "b3", 2_700, TtlClass::Ephemeral5m),
            ],
            cacheable_breakpoint_prefix_keys: cacheable_prefix_keys(&["b0", "b1", "b2", "b3"]),
            selected_match: Some(selected_match_hit("b1", 1, TtlClass::Ephemeral5m, 2_500)),
            native_cache: None,
            sink: cache.clone(),
            grace_margin_secs: cache.grace_margin_secs(),
        };

        let decoded = decode_prompt_cache_observations_pure(
            &context,
            PromptCacheUsage {
                cache_creation_input_tokens: 3_000,
                cache_read_input_tokens: 2_500,
                ..PromptCacheUsage::default()
            },
            now,
        );

        let hits = decoded
            .observations
            .iter()
            .filter(|obs| obs.kind == DecodedPromptCacheObservationKind::Hit)
            .map(|obs| obs.prefix_hash.as_str())
            .collect::<Vec<_>>();
        let writes = decoded
            .observations
            .iter()
            .filter(|obs| obs.kind == DecodedPromptCacheObservationKind::Write)
            .map(|obs| obs.prefix_hash.as_str())
            .collect::<Vec<_>>();

        assert_eq!(hits, vec!["b1"]);
        assert_eq!(writes, vec!["b2", "b3"]);
    }

    #[test]
    fn overlapping_five_minute_hit_persists_one_hour_upgrade() {
        let upstream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000234").unwrap();
        let now = 1_800_000_000;
        let cache =
            Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()).with_clock_now(now));
        let one_hour = cache_breakpoint(10, "shared", 2_400, TtlClass::Ephemeral1h);
        let mut five_minute = cache_breakpoint(15, "later", 3_600, TtlClass::Ephemeral5m);
        five_minute.lookback_prefixes.push(CacheLookbackPrefix {
            prefix_hash: "shared".to_owned(),
            content_block_index: 10,
            lookback_distance: 5,
        });
        let context = PromptCacheObservationContext {
            upstream_id,
            canonical_model_id: TEST_MODEL.to_owned(),
            cache_breakpoints: vec![one_hour, five_minute],
            cacheable_breakpoint_prefix_keys: cacheable_prefix_keys(&["shared", "later"]),
            selected_match: Some(SelectedCacheMatch {
                prefix_hash: "shared".to_owned(),
                content_block_index: 10,
                ttl_class: TtlClass::Ephemeral5m,
                estimated_prefix_bytes: PrefixBytes::new(2_400),
            }),
            native_cache: None,
            sink: cache.clone(),
            grace_margin_secs: cache.grace_margin_secs(),
        };

        let decoded = decode_prompt_cache_observations_pure(
            &context,
            PromptCacheUsage {
                cache_creation_input_tokens: 3_600,
                cache_read_input_tokens: 2_400,
                ..PromptCacheUsage::default()
            },
            now,
        );
        let writes = decoded
            .observations
            .iter()
            .filter(|observation| observation.kind == DecodedPromptCacheObservationKind::Write)
            .map(|observation| (observation.prefix_hash.as_str(), observation.ttl_class))
            .collect::<Vec<_>>();

        assert_eq!(
            writes,
            vec![
                ("shared", TtlClass::Ephemeral1h),
                ("later", TtlClass::Ephemeral5m)
            ]
        );
    }

    fn unpredicted_read_context(
        cache: Arc<RecordingPromptCacheObservationSink>,
    ) -> PromptCacheObservationContext {
        PromptCacheObservationContext {
            upstream_id: Uuid::from_u128(1_003),
            canonical_model_id: TEST_MODEL.to_owned(),
            // Deliberately unsorted: structural depth, not vector order, bytes,
            // or the highest unqualified breakpoint, selects the inferred key.
            cache_breakpoints: vec![
                cache_breakpoint(5, "deep-eligible", 2_800, TtlClass::Ephemeral1h),
                cache_breakpoint(8, "deep-ineligible", 9_600, TtlClass::Ephemeral5m),
                cache_breakpoint(2, "shallow-eligible", 8_400, TtlClass::Ephemeral5m),
            ],
            cacheable_breakpoint_prefix_keys: cacheable_prefix_keys(&[
                "deep-eligible",
                "shallow-eligible",
            ]),
            selected_match: None,
            native_cache: None,
            grace_margin_secs: 30,
            sink: cache,
        }
    }

    #[test]
    fn unpredicted_read_requires_affirmative_zero_creation() {
        let now = 1_800_000_000;
        for (reported, inferred) in [
            (
                r#"{"cache_read_input_tokens":2400,"cache_creation_input_tokens":0}"#,
                true,
            ),
            (
                r#"{"cache_read_input_tokens":2400,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0}}"#,
                true,
            ),
            (
                r#"{"cache_read_input_tokens":2400,"cache_creation_input_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0}}"#,
                true,
            ),
            (r#"{"cache_read_input_tokens":2400}"#, false),
            (
                r#"{"cache_read_input_tokens":2400,"cache_creation_input_tokens":null}"#,
                false,
            ),
            (
                r#"{"cache_read_input_tokens":2400,"cache_creation":null}"#,
                false,
            ),
            (
                r#"{"cache_read_input_tokens":2400,"cache_creation":{}}"#,
                false,
            ),
            (
                r#"{"cache_read_input_tokens":2400,"cache_creation":{"ephemeral_5m_input_tokens":0}}"#,
                false,
            ),
            (
                r#"{"cache_read_input_tokens":2400,"cache_creation":{"ephemeral_1h_input_tokens":0}}"#,
                false,
            ),
            (
                r#"{"cache_read_input_tokens":2400,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":null}}"#,
                false,
            ),
            (
                r#"{"cache_read_input_tokens":2400,"cache_creation_input_tokens":0,"cache_creation":{}}"#,
                false,
            ),
            (
                r#"{"cache_read_input_tokens":2400,"cache_creation_input_tokens":"0"}"#,
                false,
            ),
            (
                r#"{"cache_read_input_tokens":2400,"cache_creation_input_tokens":false}"#,
                false,
            ),
            (
                r#"{"cache_read_input_tokens":2400,"cache_creation_input_tokens":-1}"#,
                false,
            ),
            (
                r#"{"cache_read_input_tokens":2400,"cache_creation_input_tokens":0.5}"#,
                false,
            ),
            (
                r#"{"cache_read_input_tokens":0,"cache_creation_input_tokens":0}"#,
                false,
            ),
            (
                r#"{"cache_read_input_tokens":null,"cache_creation_input_tokens":0}"#,
                false,
            ),
            (r#"{"cache_creation_input_tokens":0}"#, false),
            (
                r#"{"cache_read_input_tokens":"2400","cache_creation_input_tokens":0}"#,
                false,
            ),
            (
                r#"{"cache_read_input_tokens":true,"cache_creation_input_tokens":0}"#,
                false,
            ),
            (
                r#"{"cache_read_input_tokens":-1,"cache_creation_input_tokens":0}"#,
                false,
            ),
            (
                r#"{"cache_read_input_tokens":2.5,"cache_creation_input_tokens":0}"#,
                false,
            ),
        ] {
            let cache = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()));
            let context = unpredicted_read_context(cache.clone());
            let body = format!(r#"{{"usage":{reported}}}"#);
            let usage = usage_parser::observe_non_stream_json_body(body.as_bytes()).usage;
            let decoded = decode_prompt_cache_observations_pure(
                &context,
                PromptCacheUsage::from(&usage),
                now,
            );
            let expected = if inferred {
                vec![DecodedPromptCacheObservation {
                    prefix_hash: "deep-eligible".to_owned(),
                    ttl_class: TtlClass::Ephemeral1h,
                    expires_at_unix_secs: 1_800_003_570,
                    prefix_content_block_index: 5,
                    estimated_prefix_bytes: PrefixBytes::new(2_800),
                    kind: DecodedPromptCacheObservationKind::Hit,
                }]
            } else {
                Vec::new()
            };
            assert_eq!(decoded.observations, expected, "{reported}");
            assert_eq!(decoded.dropped_below_threshold, 0, "{reported}");
            publish_prompt_cache_observations(&context, &decoded, now);
            let records = cache.upserts();
            assert_eq!(records.len(), usize::from(inferred), "{reported}");
            if let Some(record) = records.first() {
                assert_eq!(record.v3_prefix_key, "deep-eligible");
                assert_eq!(record.prefix_content_block_index, 5);
                assert_eq!(record.ttl_class, TtlClass::Ephemeral1h);
                assert_eq!(record.expires_at_unix_secs, 1_800_003_570);
                assert_eq!(record.estimated_prefix_tokens, 2_800);
                assert_eq!(record.token_estimate_source, PrefixBytes::ESTIMATE_SOURCE);
            }
        }
    }

    #[test]
    fn unpredicted_positive_creation_preserves_writes_without_inferred_hits() {
        for reported in [
            r#"{"cache_read_input_tokens":2400,"cache_creation_input_tokens":700}"#,
            r#"{"cache_read_input_tokens":2400,"cache_creation_input_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":700,"ephemeral_1h_input_tokens":0}}"#,
            r#"{"cache_read_input_tokens":2400,"cache_creation_input_tokens":700,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0}}"#,
        ] {
            let cache = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()));
            let context = unpredicted_read_context(cache);
            let body = format!(r#"{{"usage":{reported}}}"#);
            let usage = usage_parser::observe_non_stream_json_body(body.as_bytes()).usage;
            let decoded = decode_prompt_cache_observations_pure(
                &context,
                PromptCacheUsage::from(&usage),
                1_800_000_000,
            );
            assert_eq!(
                decoded.observations,
                vec![
                    DecodedPromptCacheObservation {
                        prefix_hash: "deep-eligible".to_owned(),
                        ttl_class: TtlClass::Ephemeral1h,
                        expires_at_unix_secs: 1_800_003_570,
                        prefix_content_block_index: 5,
                        estimated_prefix_bytes: PrefixBytes::new(2_800),
                        kind: DecodedPromptCacheObservationKind::Write,
                    },
                    DecodedPromptCacheObservation {
                        prefix_hash: "shallow-eligible".to_owned(),
                        ttl_class: TtlClass::Ephemeral5m,
                        expires_at_unix_secs: 1_800_000_270,
                        prefix_content_block_index: 2,
                        estimated_prefix_bytes: PrefixBytes::new(8_400),
                        kind: DecodedPromptCacheObservationKind::Write,
                    },
                ],
                "{reported}",
            );
            assert_eq!(decoded.dropped_below_threshold, 1);
        }
    }

    #[test]
    fn affirmative_unpredicted_evidence_never_replaces_carried_shallow_or_same_key_hit() {
        let usage = usage_parser::observe_non_stream_json_body(
            br#"{"usage":{"cache_read_input_tokens":2400,"cache_creation_input_tokens":0}}"#,
        )
        .usage;
        for (key, index, bytes) in [
            ("lookback-shallow", 1, 1_400),
            ("shallow-eligible", 2, 8_400),
        ] {
            let cache = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()));
            let mut context = unpredicted_read_context(cache.clone());
            context.selected_match =
                Some(selected_match_hit(key, index, TtlClass::Ephemeral1h, bytes));
            let decoded = decode_prompt_cache_observations_pure(
                &context,
                PromptCacheUsage::from(&usage),
                1_800_000_000,
            );
            assert_eq!(
                decoded.observations,
                vec![DecodedPromptCacheObservation {
                    prefix_hash: key.to_owned(),
                    ttl_class: TtlClass::Ephemeral1h,
                    expires_at_unix_secs: 1_800_003_570,
                    prefix_content_block_index: index,
                    estimated_prefix_bytes: PrefixBytes::new(bytes),
                    kind: DecodedPromptCacheObservationKind::Hit,
                }],
            );
            publish_prompt_cache_observations(&context, &decoded, 1_800_000_000);
            let records = cache.upserts();
            assert_eq!(records.len(), 1);
            let record = records.first().expect("carried match record");
            assert_eq!(record.v3_prefix_key, key);
            assert_eq!(record.ttl_class, TtlClass::Ephemeral1h);
            assert_eq!(record.prefix_content_block_index, index);
            assert_eq!(record.estimated_prefix_tokens, bytes);
        }
    }

    /// Independent reference bytes and hash chain for this fixed two-block
    /// request; expectations never come from analyzed/decoded breakpoint data.
    fn read_inference_request_fixture(automatic: bool, large: bool) -> (Value, String, u64) {
        let text = if large {
            "cache evidence ".repeat(3_000)
        } else {
            "tiny".to_owned()
        };
        let mut tail = json!({"type":"text","text":"tail"});
        if !automatic {
            tail["cache_control"] = json!({"type":"ephemeral","ttl":"5m"});
        }
        let first = json!({
            "type":"text","text":text,
            "cache_control":{"type":"ephemeral","ttl":"1h"},
        });
        let mut request = json!({
            "model":TEST_MODEL,
            "messages":[{"role":"user","content":[first.clone(),tail.clone()]}],
            "max_tokens":16,
        });
        if automatic {
            request["cache_control"] = json!({"type":"ephemeral","ttl":"5m"});
        }
        let mut seed = blake3::Hasher::new();
        seed.update(b"cc-lb-cache-v6:seed");
        seed.update(b"claude-sonnet-4-5-20250929");
        let mut previous = seed.finalize();
        for (index, block_text) in [(0, text.as_str()), (1, "tail")] {
            let digest_value = json!({
                "path":format!("messages[0].content[{index}]"),
                "source":"message",
                "value":{"text":block_text,"type":"text"},
            });
            let mut block = blake3::Hasher::new();
            block.update(b"cc-lb-cache-v6:block");
            block.update(&serde_json::to_vec(&digest_value).expect("reference block JSON"));
            let mut prefix = blake3::Hasher::new();
            prefix.update(b"cc-lb-cache-v6:prefix");
            prefix.update(previous.as_bytes());
            prefix.update(block.finalize().as_bytes());
            previous = prefix.finalize();
        }
        let reference_prefix = json!({
            "content_blocks":[
                {"source":"message","value":first},
                {"source":"message","value":tail},
            ],
            "model":"claude-sonnet-4-5-20250929",
        });
        let bytes = serde_json::to_vec(&reference_prefix)
            .expect("reference prefix JSON")
            .len() as u64;
        (request, previous.to_hex().to_string(), bytes)
    }

    #[tokio::test]
    async fn unpredicted_read_infers_deepest_resolved_explicit_and_automatic_boundary() {
        for automatic in [false, true] {
            let (request, expected_key, expected_bytes) =
                read_inference_request_fixture(automatic, true);
            let output = PromptCacheAnalysisExecutor::default()
                .analyze(
                    Arc::new(request.clone()),
                    TEST_MODEL.to_owned(),
                    [0; 32],
                    Some(1_024),
                )
                .await;
            let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
                &HeaderMap::new(),
                Some(&request),
                Some(output),
                None,
            );
            let cache = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()));
            let context = PromptCacheObservationContext {
                upstream_id: Uuid::from_u128(1_003),
                canonical_model_id: TEST_MODEL.to_owned(),
                cache_breakpoints: metadata.plugin_cache_breakpoints(),
                cacheable_breakpoint_prefix_keys: metadata
                    .bundles
                    .official
                    .cacheable_breakpoint_prefix_keys,
                selected_match: None,
                native_cache: None,
                sink: cache.clone(),
                grace_margin_secs: 30,
            };
            let usage = usage_parser::observe_non_stream_json_body(
                br#"{"usage":{"cache_read_input_tokens":2400,"cache_creation_input_tokens":0}}"#,
            )
            .usage;
            let decoded = decode_prompt_cache_observations_pure(
                &context,
                PromptCacheUsage::from(&usage),
                1_800_000_000,
            );
            assert_eq!(
                decoded.observations,
                vec![DecodedPromptCacheObservation {
                    prefix_hash: expected_key.clone(),
                    ttl_class: TtlClass::Ephemeral5m,
                    expires_at_unix_secs: 1_800_000_270,
                    prefix_content_block_index: 1,
                    estimated_prefix_bytes: PrefixBytes::new(expected_bytes),
                    kind: DecodedPromptCacheObservationKind::Hit,
                }],
                "automatic={automatic}",
            );
            publish_prompt_cache_observations(&context, &decoded, 1_800_000_000);
            let records = cache.upserts();
            assert_eq!(records.len(), 1);
            let record = records.first().expect("inferred boundary record");
            assert_eq!(record.v3_prefix_key, expected_key);
            assert_eq!(record.prefix_content_block_index, 1);
            assert_eq!(record.estimated_prefix_tokens, expected_bytes);
            assert_eq!(record.ttl_class, TtlClass::Ephemeral5m);
            assert_eq!(record.expires_at_unix_secs, 1_800_000_270);
        }
    }

    #[tokio::test]
    async fn unpredicted_read_with_empty_or_actual_below_threshold_eligibility_stays_unknown() {
        for automatic in [false, true] {
            let (request, _, _) = read_inference_request_fixture(automatic, false);
            let output = PromptCacheAnalysisExecutor::default()
                .analyze(
                    Arc::new(request.clone()),
                    TEST_MODEL.to_owned(),
                    [0; 32],
                    Some(1_024),
                )
                .await;
            assert!(output.cacheable_breakpoint_prefix_keys.is_empty());
            let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
                &HeaderMap::new(),
                Some(&request),
                Some(output),
                None,
            );
            let cache = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()));
            let mut context = PromptCacheObservationContext {
                upstream_id: Uuid::from_u128(1_003),
                canonical_model_id: TEST_MODEL.to_owned(),
                cache_breakpoints: metadata.plugin_cache_breakpoints(),
                cacheable_breakpoint_prefix_keys: metadata
                    .bundles
                    .official
                    .cacheable_breakpoint_prefix_keys,
                selected_match: None,
                native_cache: None,
                sink: cache.clone(),
                grace_margin_secs: 30,
            };
            let usage = usage_parser::observe_non_stream_json_body(
                br#"{"usage":{"cache_read_input_tokens":2400,"cache_creation_input_tokens":0}}"#,
            )
            .usage;
            for empty_breakpoints in [false, true] {
                if empty_breakpoints {
                    context.cache_breakpoints.clear();
                }
                let decoded = decode_prompt_cache_observations_pure(
                    &context,
                    PromptCacheUsage::from(&usage),
                    1_800_000_000,
                );
                assert!(decoded.observations.is_empty());
                publish_prompt_cache_observations(&context, &decoded, 1_800_000_000);
                assert!(cache.upserts().is_empty());
            }
        }
    }

    struct ObservationSseDialect {
        calls: Arc<AtomicUsize>,
    }

    impl UpstreamDialect for ObservationSseDialect {
        fn shape(
            &self,
            context: &DialectShapeContext,
            upstream: &Upstream,
            principal: &Principal,
            builder: &mut ShapedRequestBuilder,
        ) -> Result<ShapedRequest, DialectError> {
            RawPassthroughDialect {
                base_url: Url::parse("https://api.anthropic.com").expect("test upstream URL"),
                openai_oauth: false,
            }
            .shape(context, upstream, principal, builder)
        }

        fn sse_event_transform_hook(&self) -> Option<&dyn SseEventTransformHook> {
            Some(self)
        }
    }

    impl SseEventTransformHook for ObservationSseDialect {
        fn transform_sse_event(
            &self,
            _request: cc_lb_upstream::TransformSseEventRequest,
        ) -> Result<cc_lb_upstream::TransformSseEventResult, ResponseTransformError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(cc_lb_upstream::TransformSseEventResult::Unchanged)
        }
    }

    fn read_inference_lifecycle(
        upstream_response: Response<Body>,
        cache: Arc<RecordingPromptCacheObservationSink>,
        transform_calls: Option<Arc<AtomicUsize>>,
    ) -> Lifecycle {
        let dialect = transform_calls.map_or(
            cc_lb_control::api_keys::principal_view::DialectCache::Inherit,
            |calls| {
                cc_lb_control::api_keys::principal_view::DialectCache::Explicit(
                    cc_lb_control::api_keys::principal_view::ShapePluginCache {
                        dialect: Arc::new(ObservationSseDialect { calls }),
                    },
                )
            },
        );
        let view = DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(TestSignerFactory))
            .principal_view(failure_principal_view("principal-test", (None, dialect)))
            .upstream_records(vec![upstream_record(Uuid::from_u128(1_003))])
            .prompt_cache_observation_sink(cache)
            .prompt_cache_grace_margin_secs(30)
            .build();
        Lifecycle::new_with_dynamic_view(
            failure_authn(),
            Arc::new(DynamicViewHolder::new(view)),
            Arc::new(QueueDispatch::new(vec![Ok(upstream_response)])),
            LifecycleConfig::default(),
            Arc::new(cc_lb_clock::TestClock::new_at_secs(1_800_000_000)),
        )
    }

    fn assert_inferred_published_record(
        records: &[PromptCacheObservationRecord],
        expected_key: &str,
        expected_bytes: u64,
    ) {
        assert_eq!(records.len(), 1, "publish exactly one deepest boundary");
        let record = records.first().expect("inferred published record");
        assert_eq!(record.upstream_id, Uuid::from_u128(1_003));
        assert_eq!(record.canonical_model_id, TEST_MODEL);
        assert_eq!(record.v3_prefix_key, expected_key);
        assert_eq!(record.prefix_content_block_index, 1);
        assert_eq!(record.ttl_class, TtlClass::Ephemeral5m);
        assert_eq!(
            record.expires_at_unix_secs,
            record.last_observed_at_unix_secs + 270,
        );
        assert_eq!(record.estimated_prefix_tokens, expected_bytes);
        assert_eq!(record.token_estimate_source, PrefixBytes::ESTIMATE_SOURCE);
        assert_eq!(record.hash_schema_version, HASH_SCHEMA_VERSION);
    }

    #[tokio::test]
    async fn buffered_unpredicted_read_publishes_one_resolved_boundary() {
        for automatic in [false, true] {
            let (request, expected_key, expected_bytes) =
                read_inference_request_fixture(automatic, true);
            let cache = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()));
            let lifecycle = read_inference_lifecycle(
                upstream_response(
                    StatusCode::OK,
                    br#"{"id":"msg_read","type":"message","role":"assistant","model":"claude-sonnet-4-5-20250929","content":[],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1,"cache_read_input_tokens":2400,"cache_creation_input_tokens":0}}"#,
                ),
                cache.clone(),
                None,
            );
            let request = failure_request(Bytes::from(
                serde_json::to_vec(&request).expect("request JSON"),
            ));
            let auth = lifecycle
                .authenticate(request.headers())
                .await
                .expect("request authenticates");
            let response = lifecycle
                .handle(request, &auth)
                .await
                .expect("buffered request succeeds");
            assert_eq!(response.status(), StatusCode::OK);
            assert_inferred_published_record(&cache.upserts(), &expected_key, expected_bytes);
            let _ = collect_response_body(response).await;
            assert_inferred_published_record(&cache.upserts(), &expected_key, expected_bytes);
        }
    }

    #[tokio::test]
    async fn native_and_transformed_sse_unpredicted_reads_publish_early_exactly_once() {
        for automatic in [false, true] {
            for transformed in [false, true] {
                for drop_after_start in [false, true] {
                    let (mut request, expected_key, expected_bytes) =
                        read_inference_request_fixture(automatic, true);
                    request["stream"] = Value::Bool(true);
                    let (release, released) = tokio::sync::oneshot::channel::<()>();
                    let stream = async_stream::stream! {
                        yield Ok::<Bytes, Infallible>(Bytes::from_static(
                            b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_read\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-5-20250929\",\"content\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"cache_read_input_tokens\":2400,\"cache_creation\":{\"ephemeral_5m_input_tokens\":0,\"ephemeral_1h_input_tokens\":0}}}}\n\n"
                        ));
                        let _ = released.await;
                        yield Ok::<Bytes, Infallible>(Bytes::from_static(
                            b"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":17,\"cache_read_input_tokens\":2400,\"cache_creation_input_tokens\":0}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
                        ));
                    };
                    let mut upstream = Response::new(Body::from_stream(stream));
                    upstream
                        .headers_mut()
                        .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
                    let cache = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()));
                    let transform_calls = Arc::new(AtomicUsize::new(0));
                    let lifecycle = read_inference_lifecycle(
                        upstream,
                        cache.clone(),
                        transformed.then(|| transform_calls.clone()),
                    );
                    let request = failure_request(Bytes::from(
                        serde_json::to_vec(&request).expect("streaming request JSON"),
                    ));
                    let auth = lifecycle
                        .authenticate(request.headers())
                        .await
                        .expect("request authenticates");
                    let response = lifecycle
                        .handle(request, &auth)
                        .await
                        .expect("streaming request succeeds");
                    assert_eq!(response.status(), StatusCode::OK);
                    assert!(cache.upserts().is_empty(), "body has not been polled yet");
                    let mut body = response.into_body();
                    let first = tokio::time::timeout(Duration::from_secs(5), body.frame())
                        .await
                        .expect("message_start arrives before upstream completion")
                        .expect("message_start frame")
                        .expect("message_start body data");
                    assert!(first.is_data());
                    assert_inferred_published_record(
                        &cache.upserts(),
                        &expected_key,
                        expected_bytes,
                    );
                    if transformed {
                        assert!(transform_calls.load(Ordering::SeqCst) > 0);
                    }
                    if drop_after_start {
                        drop(body);
                        drop(release);
                    } else {
                        release.send(()).expect("release stream completion");
                        let _ = body.collect().await.expect("remaining SSE collects");
                    }
                    assert_inferred_published_record(
                        &cache.upserts(),
                        &expected_key,
                        expected_bytes,
                    );
                }
            }
        }
    }

    #[test]
    fn expiry_anchored_at_start_not_completion() {
        let dispatch_started = Instant::now();
        let dispatch_unix_secs = 1_000_u64;
        let message_start_at = dispatch_started + Duration::from_secs(2);
        let anchor =
            response_start_unix_secs(dispatch_unix_secs, dispatch_started, Some(message_start_at));
        assert_eq!(anchor, 1_002);

        let grace = 60;
        let expires = prompt_cache_observation_expires_at(anchor, TtlClass::Ephemeral5m, grace);
        assert_eq!(expires, 1_002 + 300 - 60);

        let completion_unix = dispatch_unix_secs + 62;
        let completion_expires =
            prompt_cache_observation_expires_at(completion_unix, TtlClass::Ephemeral5m, grace);
        assert!(
            expires < completion_expires,
            "start anchor must not extend expiry by the generation duration"
        );
    }

    #[tokio::test]
    async fn issue_1002_sse_relay_expires_from_dispatch_not_message_start()
    -> Result<(), Box<dyn std::error::Error>> {
        const MESSAGE_START: &[u8] = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"usage\":",
            "{\"input_tokens\":10,\"output_tokens\":1,\"cache_creation_input_tokens\":2400}}}\n\n",
        )
        .as_bytes();
        let upstream_id = Uuid::from_u128(1002);
        let dispatch_unix_secs = 1_800_000_000;
        let principal_view = failure_principal_view("principal-test", default_artifacts());
        let record = upstream_record(upstream_id);
        let cached = principal_view
            .get("principal-test")
            .ok_or("missing principal")?;
        let (route, _) = route_for_record(&record, cached).map_err(|_| "route does not resolve")?;
        let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
        let mut lifecycle = provenance_lifecycle(
            provenance_view(principal_view, Arc::new(TestSignerFactory), vec![record]),
            Arc::new(QueueDispatch::new(Vec::new())),
            &bus,
        );
        let sink = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()));
        let context = PromptCacheObservationContext {
            upstream_id,
            canonical_model_id: TEST_MODEL.to_owned(),
            cache_breakpoints: vec![cache_breakpoint(3, "cached", 2_400, TtlClass::Ephemeral5m)],
            cacheable_breakpoint_prefix_keys: cacheable_prefix_keys(&["cached"]),
            selected_match: None,
            native_cache: None,
            sink: sink.clone(),
            grace_margin_secs: 0,
        };
        let upstream_body = Body::from_stream(async_stream::stream! {
            yield Ok::<_, Infallible>(Bytes::from_static(MESSAGE_START));
            std::future::pending::<()>().await;
        });
        let response = Response::builder()
            .header(CONTENT_TYPE, "text/event-stream")
            .body(upstream_body)?;
        // std::time::Instant is deliberately backdated: Tokio's paused clock
        // does not advance the production relay's monotonic timing source.
        let dispatch_started = Instant::now() - Duration::from_secs(40);
        let event_ctx = RequestEventContext {
            request_id: "issue-1002-relay".to_owned(),
            canonical_model_id: TEST_MODEL.to_owned(),
            upstream_name: format!("upstream-{upstream_id}"),
            requested_model: Some(TEST_MODEL.to_owned()),
            proxy_setup_ms: None,
            stage_timings: AttemptTimings::default(),
        };
        let transform_ctx = ResponseTransformContext {
            principal: Principal {
                id: "principal-test".to_owned(),
                kind: PrincipalKind::InternalKey,
            },
            upstream: route.upstream,
            request_method: http::Method::POST,
            request_path: "/v1/messages".to_owned(),
            dialect: route.dialect,
            gemini_oauth: false,
        };
        let affinity_ctx = ResponseAffinityContext {
            capability: ProviderCapability::AnthropicMessages,
            store: None,
            principal_id: "principal-test".to_owned(),
            upstream_id,
            pending_request_keys: Vec::new(),
            observe_response_keys: false,
            owner_epoch: None,
            ttl_secs: 3_600,
        };
        let response = lifecycle.relay_response(
            response,
            Arc::new(TestSigner),
            StatusCode::OK,
            false,
            None,
            event_ctx.clone(),
            transform_ctx.clone(),
            Some(context.clone()),
            affinity_ctx.clone(),
            None,
            None,
            dispatch_started,
            dispatch_unix_secs,
        );
        let mut body = response.into_body();
        let frame = tokio::time::timeout(Duration::from_secs(1), body.frame())
            .await?
            .ok_or("message_start was not relayed")??;
        let data = frame.into_data().map_err(|_| "expected a data frame")?;
        assert_eq!(data.as_ref(), MESSAGE_START);
        // The upstream is still pending: observation publication and event
        // delivery must not wait for completion or message_stop.
        let records = sink.upserts();
        let [record] = records.as_slice() else {
            panic!("message_start must publish exactly one cache observation");
        };
        assert_eq!(record.expires_at_unix_secs, dispatch_unix_secs + 300);
        assert!(record.last_observed_at_unix_secs >= dispatch_unix_secs + 40);
        drop(body);

        // Buffered responses publish at completion, independently of expiry.
        let completion_unix_secs = dispatch_unix_secs + 90;
        lifecycle.clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(completion_unix_secs));
        const BUFFERED: &[u8] =
            br#"{"usage":{"input_tokens":10,"output_tokens":1,"cache_creation_input_tokens":2400}}"#;
        let response = lifecycle
            .finish_success_response(
                upstream_response(StatusCode::OK, BUFFERED),
                Arc::new(TestSigner),
                false,
                None,
                None,
                Duration::from_secs(90),
                StatusCode::OK,
                event_ctx,
                transform_ctx,
                Some(context),
                affinity_ctx,
                None,
                None,
                dispatch_started,
                dispatch_unix_secs,
            )
            .await;
        assert_eq!(response.into_body().collect().await?.to_bytes(), BUFFERED);
        let records = sink.upserts();
        let [_, buffered_record] = records.as_slice() else {
            panic!("buffered usage must publish one additional cache observation");
        };
        assert_eq!(
            buffered_record.expires_at_unix_secs,
            dispatch_unix_secs + 300
        );
        assert_eq!(
            buffered_record.last_observed_at_unix_secs,
            completion_unix_secs
        );
        Ok(())
    }

    #[test]
    fn sse_without_message_start_falls_back_to_dispatch() {
        let dispatch_started = Instant::now();
        let dispatch_unix_secs = 5_000_u64;
        assert_eq!(
            response_start_unix_secs(dispatch_unix_secs, dispatch_started, None),
            dispatch_unix_secs
        );
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
    fn request_cache_metadata_preserves_raw_requested_service_tier() {
        // Given a request body with a mixed-case tier value.
        let body = Bytes::from_static(
            br#"{"model":"claude-test","service_tier":"Priority-Raw","messages":[]}"#,
        );

        // When request metadata is extracted from the parsed body.
        let metadata = request_cache_metadata(&HeaderMap::new(), &body);

        // Then the pre-request path retains the exact requested value.
        assert_eq!(
            metadata.requested_service_tier.as_deref(),
            Some("Priority-Raw")
        );
    }

    #[test]
    fn gemini_request_aliases_preserve_tier_and_bounded_thinking_metadata() {
        for alias in ["service_tier", "serviceTier"] {
            for tier in ["flex", "priority", "FLEX", "future-tier"] {
                let request = json!({
                    (alias): tier,
                    "generationConfig": {"thinkingConfig": {
                        "thinkingLevel": "LOW",
                        "thinkingBudget": 1024,
                        "includeThoughts": true
                    }},
                    "reasoning_effort": "wrong-provider"
                });
                let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
                    &HeaderMap::new(),
                    Some(&request),
                    None,
                    Some(ProviderCapability::GeminiGenerateContent),
                );
                let limit = LimitRequest::from_value(
                    Some(&request),
                    "gemini-test",
                    Some(ProviderCapability::GeminiGenerateContent),
                    &[],
                    false,
                );
                assert_eq!(metadata.requested_service_tier.as_deref(), Some(tier));
                assert_eq!(limit.service_tier.as_deref(), Some(tier));
                assert_eq!(metadata.thinking_level.as_deref(), Some("LOW"));
                assert_eq!(metadata.thinking_budget_tokens, Some(1024));
                assert_eq!(metadata.reasoning_effort, None);
            }
        }
    }

    #[test]
    fn gemini_service_tier_alias_precedence_and_malformed_fields_are_conservative() {
        for (request, expected) in [
            (
                json!({"service_tier":"priority","serviceTier":"flex"}),
                Some("priority"),
            ),
            (json!({"service_tier":null,"serviceTier":"flex"}), None),
            (json!({"serviceTier":null}), None),
            (json!({"serviceTier":42}), None),
            (json!({"servicetier":"flex"}), None),
        ] {
            let capability = Some(ProviderCapability::GeminiGenerateContent);
            let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
                &HeaderMap::new(),
                Some(&request),
                None,
                capability,
            );
            let limit =
                LimitRequest::from_value(Some(&request), "gemini-test", capability, &[], false);
            assert_eq!(metadata.requested_service_tier.as_deref(), expected);
            assert_eq!(limit.service_tier.as_deref(), expected);
        }
    }
    #[test]
    fn review_1166_gemini_native_snake_case_thinking_aliases_preserve_metadata() {
        let request = json!({
            "generationConfig": {
                "thinkingConfig": {
                    "thinking_level": "LOW",
                    "thinking_budget": 1024
                }
            }
        });
        let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
            &HeaderMap::new(),
            Some(&request),
            None,
            Some(ProviderCapability::GeminiGenerateContent),
        );
        assert_eq!(metadata.thinking_level.as_deref(), Some("LOW"));
        assert_eq!(metadata.thinking_budget_tokens, Some(1024));
        assert_eq!(metadata.reasoning_effort, None);
    }

    #[test]
    fn review_1166_gemini_thinking_alias_bounds_and_camel_precedence_are_conservative() {
        let valid_snake = json!({
            "generationConfig": {
                "thinkingConfig": {
                    "thinking_level": "snake",
                    "thinking_budget": 2048
                }
            }
        });
        let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
            &HeaderMap::new(),
            Some(&valid_snake),
            None,
            Some(ProviderCapability::GeminiGenerateContent),
        );
        assert_eq!(metadata.thinking_level.as_deref(), Some("snake"));
        assert_eq!(metadata.thinking_budget_tokens, Some(2048));

        for (camel_level, snake_level, expected_level) in [
            ("CAMEL", "snake", Some("CAMEL")),
            ("", "snake", None),
            (&"x".repeat(65), "snake", None),
        ] {
            let request = json!({
                "generationConfig": {"thinkingConfig": {
                    "thinkingLevel": camel_level,
                    "thinking_level": snake_level,
                    "thinking_budget": 0
                }}
            });
            let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
                &HeaderMap::new(),
                Some(&request),
                None,
                Some(ProviderCapability::GeminiGenerateContent),
            );
            assert_eq!(metadata.thinking_level.as_deref(), expected_level);
            assert_eq!(metadata.thinking_budget_tokens, Some(0));
        }

        for (camel_budget, snake_budget, expected_budget) in [
            (json!(0), json!(2048), Some(0)),
            (json!(-1), json!(2048), None),
            (json!("2048"), json!(2048), None),
            (json!(9_007_199_254_740_992_u64), json!(2048), None),
            (
                json!(9_007_199_254_740_991_u64),
                json!(2048),
                Some(9_007_199_254_740_991),
            ),
        ] {
            let request = json!({
                "generationConfig": {"thinkingConfig": {
                    "thinking_budget": snake_budget,
                    "thinkingBudget": camel_budget
                }}
            });
            let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
                &HeaderMap::new(),
                Some(&request),
                None,
                Some(ProviderCapability::GeminiGenerateContent),
            );
            assert_eq!(metadata.thinking_budget_tokens, expected_budget);
        }
    }

    #[test]
    fn review_1166_gemini_blank_tiers_are_absent_but_nonblank_raw_values_are_preserved() {
        for request in [
            json!({"serviceTier": ""}),
            json!({"serviceTier": " \t "}),
            json!({"service_tier": null, "serviceTier": "priority"}),
            json!({"service_tier": " \n ", "serviceTier": "priority"}),
            json!({"service_tier": 42, "serviceTier": "priority"}),
        ] {
            let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
                &HeaderMap::new(),
                Some(&request),
                None,
                Some(ProviderCapability::GeminiGenerateContent),
            );
            let limit = LimitRequest::from_value(
                Some(&request),
                "gemini-test",
                Some(ProviderCapability::GeminiGenerateContent),
                &[],
                false,
            );
            assert_eq!(metadata.requested_service_tier, None);
            assert_eq!(limit.service_tier, None);
        }

        for raw in [" flex ", "Priority-Raw"] {
            let request = json!({"serviceTier": raw});
            let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
                &HeaderMap::new(),
                Some(&request),
                None,
                Some(ProviderCapability::GeminiGenerateContent),
            );
            let limit = LimitRequest::from_value(
                Some(&request),
                "gemini-test",
                Some(ProviderCapability::GeminiGenerateContent),
                &[],
                false,
            );
            assert_eq!(metadata.requested_service_tier.as_deref(), Some(raw));
            assert_eq!(limit.service_tier.as_deref(), Some(raw));
        }
    }

    #[test]
    fn review_1166_non_gemini_metadata_stays_provider_scoped() {
        let request = json!({
            "service_tier": "",
            "serviceTier": "priority",
            "generationConfig": {"thinkingConfig": {
                "thinking_level": "LOW",
                "thinking_budget": 1024
            }},
            "reasoning_effort": "medium"
        });
        let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
            &HeaderMap::new(),
            Some(&request),
            None,
            Some(ProviderCapability::OpenaiChatCompletions),
        );
        assert_eq!(metadata.requested_service_tier.as_deref(), Some(""));
        assert_eq!(metadata.thinking_level, None);
        assert_eq!(metadata.thinking_budget_tokens, None);
        assert_eq!(metadata.reasoning_effort.as_deref(), Some("medium"));
    }

    #[test]
    fn gemini_thinking_level_ignores_malformed_and_unbounded_values() {
        for level in [
            json!(null),
            json!(false),
            json!(3),
            json!(""),
            json!("x".repeat(65)),
        ] {
            let request = json!({"generationConfig": {"thinkingConfig": {
                "thinkingLevel": level, "thinkingBudget": 9007199254740992_u64
            }}});
            let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
                &HeaderMap::new(),
                Some(&request),
                None,
                Some(ProviderCapability::GeminiGenerateContent),
            );
            assert_eq!(metadata.thinking_level, None);
            assert_eq!(metadata.thinking_budget_tokens, None);
        }
        for level in [
            "LOW".to_owned(),
            "low".to_owned(),
            "future-level".to_owned(),
            "x".repeat(64),
        ] {
            let request = json!({"generationConfig": {"thinkingConfig": {"thinkingLevel": level}}});
            let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
                &HeaderMap::new(),
                Some(&request),
                None,
                Some(ProviderCapability::GeminiGenerateContent),
            );
            assert_eq!(metadata.thinking_level.as_deref(), Some(level.as_str()));
            assert_eq!(metadata.reasoning_effort, None);
        }
        for capability in [
            ProviderCapability::AnthropicMessages,
            ProviderCapability::OpenaiChatCompletions,
            ProviderCapability::OpenaiResponses,
            ProviderCapability::GeminiOpenaiChatCompletions,
        ] {
            let request = json!({
                "serviceTier": "priority",
                "generationConfig": {"thinkingConfig": {"thinkingLevel": "LOW"}}
            });
            let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
                &HeaderMap::new(),
                Some(&request),
                None,
                Some(capability),
            );
            assert_eq!(metadata.thinking_level, None);
            assert_eq!(metadata.requested_service_tier, None);
        }
    }

    #[test]
    fn request_cache_metadata_preserves_enabled_thinking_budgets_across_badge_boundaries() {
        // Given
        let headers = HeaderMap::new();
        let budgets = [
            1_024, 4_000, 4_001, 12_000, 12_001, 24_000, 24_001, 32_000, 32_001,
        ];

        for budget in budgets {
            let body = Bytes::from(format!(
                r#"{{"thinking":{{"type":"enabled","budget_tokens":{budget}}},"output_config":{{"effort":"max"}}}}"#
            ));

            // When
            let metadata = request_cache_metadata(&headers, &body);

            // Then
            assert_eq!(
                metadata.thinking_budget_tokens,
                Some(budget),
                "enabled budget must remain value-preserving for {budget}"
            );
            assert_eq!(metadata.reasoning_effort.as_deref(), Some("max"));
        }
    }

    #[test]
    fn request_cache_metadata_rejects_inapplicable_or_unsafe_thinking_budgets() {
        // Given
        let headers = HeaderMap::new();
        let cases = [
            (
                "disabled",
                r#"{"thinking":{"type":"disabled","budget_tokens":5000}}"#,
                None,
            ),
            (
                "adaptive",
                r#"{"thinking":{"type":"adaptive"},"output_config":{"effort":"max"}}"#,
                Some("max"),
            ),
            (
                "enabled without budget",
                r#"{"thinking":{"type":"enabled"}}"#,
                None,
            ),
            ("missing thinking", r#"{}"#, None),
            (
                "string budget",
                r#"{"thinking":{"type":"enabled","budget_tokens":"1024"}}"#,
                None,
            ),
            ("null thinking", r#"{"thinking":null}"#, None),
            (
                "one over safe integer maximum",
                r#"{"thinking":{"type":"enabled","budget_tokens":9007199254740992}}"#,
                None,
            ),
            (
                "u64 maximum",
                r#"{"thinking":{"type":"enabled","budget_tokens":18446744073709551615}}"#,
                None,
            ),
            (
                "empty reasoning effort",
                r#"{"output_config":{"effort":""}}"#,
                None,
            ),
            (
                "non-string reasoning effort",
                r#"{"output_config":{"effort":123}}"#,
                None,
            ),
            (
                "reasoning effort over 64 characters",
                r#"{"output_config":{"effort":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}}"#,
                None,
            ),
        ];

        for (case, fixture, expected_reasoning_effort) in cases {
            // When
            let metadata =
                request_cache_metadata(&headers, &Bytes::copy_from_slice(fixture.as_bytes()));

            // Then
            assert_eq!(
                metadata.thinking_budget_tokens, None,
                "{case} must not produce a thinking budget"
            );
            assert_eq!(
                metadata.reasoning_effort.as_deref(),
                expected_reasoning_effort,
                "{case} must preserve only valid reasoning effort"
            );
        }
    }

    #[test]
    fn request_cache_metadata_extracts_http_provider_reasoning_shapes() {
        let cases = [
            (
                "openai chat",
                ProviderCapability::OpenaiChatCompletions,
                json!({
                    "reasoning_effort": "high",
                    "reasoning": {"effort": "wrong-shape"},
                    "generationConfig": {"thinkingConfig": {"thinkingBudget": 99}}
                }),
                None,
                Some("high"),
            ),
            (
                "gemini openai chat",
                ProviderCapability::GeminiOpenaiChatCompletions,
                json!({"reasoning_effort": "high"}),
                None,
                Some("high"),
            ),
            (
                "openai responses",
                ProviderCapability::OpenaiResponses,
                json!({
                    "reasoning": {"effort": "medium"},
                    "reasoning_effort": "wrong-shape",
                    "generationConfig": {"thinkingConfig": {"thinkingBudget": 99}}
                }),
                None,
                Some("medium"),
            ),
            (
                "gemini buffered",
                ProviderCapability::GeminiGenerateContent,
                json!({
                    "generationConfig": {"thinkingConfig": {"thinkingBudget": 8192}},
                    "reasoning_effort": "wrong-shape",
                    "reasoning": {"effort": "wrong-shape"}
                }),
                Some(8192),
                None,
            ),
            (
                "gemini streaming",
                ProviderCapability::GeminiGenerateContent,
                json!({
                    "stream": true,
                    "generationConfig": {"thinkingConfig": {"thinkingBudget": 16384}}
                }),
                Some(16384),
                None,
            ),
            (
                "anthropic",
                ProviderCapability::AnthropicMessages,
                json!({
                    "thinking": {"type": "enabled", "budget_tokens": 18000},
                    "output_config": {"effort": "max"},
                    "reasoning_effort": "wrong-shape",
                    "reasoning": {"effort": "wrong-shape"},
                    "generationConfig": {"thinkingConfig": {"thinkingBudget": 99}}
                }),
                Some(18000),
                Some("max"),
            ),
        ];

        for (case, capability, request, expected_budget, expected_effort) in cases {
            let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
                &HeaderMap::new(),
                Some(&request),
                None,
                Some(capability),
            );
            assert_eq!(
                metadata.thinking_budget_tokens, expected_budget,
                "{case} budget"
            );
            assert_eq!(
                metadata.reasoning_effort.as_deref(),
                expected_effort,
                "{case} effort"
            );
        }
    }

    #[test]
    fn request_cache_metadata_ignores_invalid_http_provider_reasoning_values() {
        let cases = [
            (
                "openai chat null",
                ProviderCapability::OpenaiChatCompletions,
                json!({"reasoning_effort": null}),
            ),
            (
                "openai chat wrong type",
                ProviderCapability::OpenaiChatCompletions,
                json!({"reasoning_effort": 3}),
            ),
            (
                "openai chat empty",
                ProviderCapability::OpenaiChatCompletions,
                json!({"reasoning_effort": ""}),
            ),
            (
                "openai chat overlong",
                ProviderCapability::OpenaiChatCompletions,
                json!({"reasoning_effort": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}),
            ),
            (
                "openai responses empty",
                ProviderCapability::OpenaiResponses,
                json!({"reasoning": {"effort": ""}}),
            ),
            (
                "openai responses overlong",
                ProviderCapability::OpenaiResponses,
                json!({"reasoning": {"effort": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}}),
            ),
            (
                "openai responses wrong type",
                ProviderCapability::OpenaiResponses,
                json!({"reasoning": {"effort": false}}),
            ),
            (
                "gemini string budget",
                ProviderCapability::GeminiGenerateContent,
                json!({"generationConfig": {"thinkingConfig": {"thinkingBudget": "8192"}}}),
            ),
            (
                "gemini negative budget",
                ProviderCapability::GeminiGenerateContent,
                json!({"generationConfig": {"thinkingConfig": {"thinkingBudget": -1}}}),
            ),
            (
                "gemini fractional budget",
                ProviderCapability::GeminiGenerateContent,
                json!({"generationConfig": {"thinkingConfig": {"thinkingBudget": 1.5}}}),
            ),
            (
                "gemini unsafe budget",
                ProviderCapability::GeminiGenerateContent,
                json!({"generationConfig": {"thinkingConfig": {"thinkingBudget": 9007199254740992_u64}}}),
            ),
        ];

        for (case, capability, request) in cases {
            let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
                &HeaderMap::new(),
                Some(&request),
                None,
                Some(capability),
            );
            assert_eq!(metadata.thinking_budget_tokens, None, "{case} budget");
            assert_eq!(metadata.reasoning_effort, None, "{case} effort");
        }
    }

    #[test]
    fn request_cache_metadata_without_capability_preserves_legacy_anthropic_shape() {
        let request = json!({
            "thinking": {"type": "enabled", "budget_tokens": 4096},
            "output_config": {"effort": "low"},
            "reasoning_effort": "wrong-shape",
            "reasoning": {"effort": "wrong-shape"},
            "generationConfig": {"thinkingConfig": {"thinkingBudget": 99}}
        });
        let metadata = request_cache_metadata_from_value_with_analysis_for_capability(
            &HeaderMap::new(),
            Some(&request),
            None,
            None,
        );

        assert_eq!(metadata.thinking_budget_tokens, Some(4096));
        assert_eq!(metadata.reasoning_effort.as_deref(), Some("low"));
    }

    #[test]
    fn request_cache_metadata_prefers_hermes_session_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-hermes-session-id"),
            HeaderValue::from_static("hermes-header-session"),
        );
        headers.insert(
            HeaderName::from_static("x-session-affinity"),
            HeaderValue::from_static("generic-affinity-session"),
        );
        let body = Bytes::from_static(
            br#"{"system":"You are Hermes Agent, an intelligent AI assistant created by Nous Research.","messages":[{"role":"user","content":"hello"}]}"#,
        );

        let metadata = request_cache_metadata(&headers, &body);

        assert_eq!(
            metadata.observed_session_id.as_deref(),
            Some("hermes-header-session")
        );
        assert_eq!(metadata.thread_id.as_deref(), Some("hermes-header-session"));
        assert_eq!(
            metadata.session_id_source.as_deref(),
            Some("x-hermes-session-id")
        );
        assert_eq!(metadata.request_kind.as_deref(), Some("main"));
    }

    #[test]
    fn request_cache_metadata_classifies_hermes_hierarchy_from_tagged_body() {
        let cases = [
            (
                "main",
                r#"{"session_id":"hermes-root-session","tags":["product=hermes-agent","client=hermes-client-v0.19.0","conversation=hermes-root-session"],"system":"You are Hermes Agent, an intelligent AI assistant created by Nous Research.\n\nYou assist users with a wide range of tasks.","messages":[{"role":"user","content":"hello"}],"tools":[{"name":"terminal"}]}"#,
                "main",
            ),
            (
                "subagent",
                r#"{"session_id":"hermes-root-session","tags":["product=hermes-agent","client=hermes-client-v0.19.0","conversation=hermes-root-session"],"system":[{"type":"text","text":"You are a focused subagent working on a specific delegated task.\n\nYOUR TASK:\nReview the auth change"}],"messages":[{"role":"user","content":"Review the auth change"}],"tools":[{"name":"terminal"}]}"#,
                "subagent",
            ),
            (
                "session title",
                r#"{"session_id":"hermes-root-session","tags":["product=hermes-agent","client=hermes-client-v0.19.0","conversation=hermes-root-session"],"system":"You name chat sessions. Given the user's opening message, write a title that lets them find this conversation again in a list.","messages":[{"role":"user","content":"fix the router"}]}"#,
                "session_title",
            ),
            (
                "compaction",
                r#"{"session_id":"hermes-root-session","tags":["product=hermes-agent","client=hermes-client-v0.19.0","conversation=hermes-root-session"],"messages":[{"role":"user","content":"You are a summarization agent creating a context checkpoint.\n\nSummarize the conversation."}]}"#,
                "compaction",
            ),
        ];

        for (case, fixture, request_kind) in cases {
            let metadata = request_cache_metadata(
                &HeaderMap::new(),
                &Bytes::copy_from_slice(fixture.as_bytes()),
            );

            assert_eq!(
                metadata.observed_session_id.as_deref(),
                Some("hermes-root-session"),
                "{case}"
            );
            assert_eq!(
                metadata.thread_id.as_deref(),
                Some("hermes-root-session"),
                "{case}"
            );
            assert_eq!(
                metadata.session_id_source.as_deref(),
                Some("session_id"),
                "{case}"
            );
            assert_eq!(
                metadata.request_kind.as_deref(),
                Some(request_kind),
                "{case}"
            );
            assert_eq!(metadata.parent_session_id, None, "{case}");
            assert_eq!(metadata.client_app, None, "{case}");
        }
    }

    #[test]
    fn request_cache_metadata_uses_hermes_conversation_tag_fallback() {
        let body = Bytes::from_static(
            br#"{"tags":["product=hermes-agent","conversation=hermes-root-fallback"],"system":"You are Hermes Agent, an intelligent AI assistant created by Nous Research.","messages":[{"role":"user","content":"hello"}]}"#,
        );

        let metadata = request_cache_metadata(&HeaderMap::new(), &body);

        assert_eq!(
            metadata.observed_session_id.as_deref(),
            Some("hermes-root-fallback")
        );
        assert_eq!(
            metadata.session_id_source.as_deref(),
            Some("tags.conversation")
        );
        assert_eq!(metadata.request_kind.as_deref(), Some("main"));
    }

    #[test]
    fn request_cache_metadata_rejects_untrusted_hermes_body_session_ids() {
        let oversized_session_id = "x".repeat(MAX_IDENTITY_VALUE_BYTES + 1);
        let cases = [
            serde_json::json!({
                "session_id": "untagged-session",
                "system": "You are Hermes Agent, an intelligent AI assistant created by Nous Research.",
                "messages": [{"role": "user", "content": "hello"}]
            }),
            serde_json::json!({
                "session_id": "wrong-product-session",
                "tags": ["product=hermes-agent-impersonator"],
                "system": "You are Hermes Agent, an intelligent AI assistant created by Nous Research.",
                "messages": [{"role": "user", "content": "hello"}]
            }),
            serde_json::json!({
                "session_id": 123,
                "tags": ["product=hermes-agent"],
                "system": "You are Hermes Agent, an intelligent AI assistant created by Nous Research.",
                "messages": [{"role": "user", "content": "hello"}]
            }),
            serde_json::json!({
                "session_id": oversized_session_id,
                "tags": ["product=hermes-agent"],
                "system": "You are Hermes Agent, an intelligent AI assistant created by Nous Research.",
                "messages": [{"role": "user", "content": "hello"}]
            }),
        ];

        for value in cases {
            let body = Bytes::from(
                serde_json::to_vec(&value).expect("serialize invalid Hermes identity fixture"),
            );
            let metadata = request_cache_metadata(&HeaderMap::new(), &body);

            assert_eq!(metadata.observed_session_id, None);
            assert_eq!(metadata.thread_id, None);
            assert_eq!(metadata.session_id_source, None);
            assert_eq!(metadata.request_kind, None);
        }
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
    fn request_cache_metadata_classifies_opencode_requests_and_parent_session() {
        let cases = [
            (
                "main",
                None,
                r#"{"model":"claude-sonnet-4-5","system":[{"type":"text","text":"You are OpenCode, the best coding agent on the planet.\n\nYou are an interactive CLI tool."}],"messages":[{"role":"user","content":"hello"}],"tools":[{"name":"task"}]}"#,
                "main",
            ),
            (
                "session title",
                None,
                r#"{"model":"claude-haiku-4-5-20251001","system":[{"type":"text","text":"You are a title generator. You output ONLY a thread title. Nothing else.\n\n<task>Generate a title</task>"}],"messages":[{"role":"user","content":"fix the router"}]}"#,
                "session_title",
            ),
            (
                "compaction",
                None,
                r#"{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":[{"type":"text","text":"Here is the conversation so far:\n\n<conversation>\n[User]: fix the router\n</conversation>\n\nCreate a new anchored summary from the conversation history in the <conversation> tags above so another coding agent can continue the work."}]}],"tools":[]}"#,
                "compaction",
            ),
            (
                "subagent",
                Some("ses_opencode_parent"),
                r#"{"model":"claude-sonnet-4-5","system":[{"type":"text","text":"You are OpenCode, the best coding agent on the planet.\n\nYou are an interactive CLI tool."}],"messages":[{"role":"user","content":"Reply with CHILD only."}],"tools":[{"name":"task"}]}"#,
                "subagent",
            ),
        ];

        for (case, parent_session_id, fixture, request_kind) in cases {
            let mut headers = HeaderMap::new();
            headers.insert(
                HeaderName::from_static("x-session-affinity"),
                HeaderValue::from_static("ses_opencode_child_or_main"),
            );
            if let Some(parent_session_id) = parent_session_id {
                headers.insert(
                    HeaderName::from_static("x-parent-session-id"),
                    HeaderValue::from_str(parent_session_id)
                        .expect("valid OpenCode parent session ID"),
                );
            }

            let metadata =
                request_cache_metadata(&headers, &Bytes::copy_from_slice(fixture.as_bytes()));

            assert_eq!(
                metadata.observed_session_id.as_deref(),
                Some("ses_opencode_child_or_main"),
                "{case}"
            );
            assert_eq!(
                metadata.session_id_source.as_deref(),
                Some("x-session-affinity"),
                "{case}"
            );
            assert_eq!(
                metadata.parent_session_id.as_deref(),
                parent_session_id,
                "{case}"
            );
            assert_eq!(
                metadata.request_kind.as_deref(),
                Some(request_kind),
                "{case}"
            );
            assert_eq!(metadata.client_app, None, "{case}");
        }
    }

    #[test]
    fn request_cache_metadata_classifies_senpi_without_inventing_client_app() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-session-affinity"),
            HeaderValue::from_static("senpi-affinity-session"),
        );
        let body = Bytes::from_static(
            br#"{"model":"mock-with-affinity","system":"You are senpi, a coding agent. Your work should be indistinguishable from a careful senior engineer's.\n\n## Intent Gate (EVERY message)","messages":[{"role":"user","content":"Reply exactly SENPI_FAKE_OK"}],"tools":[]}"#,
        );

        let metadata = request_cache_metadata(&headers, &body);

        assert_eq!(
            metadata.observed_session_id.as_deref(),
            Some("senpi-affinity-session")
        );
        assert_eq!(
            metadata.session_id_source.as_deref(),
            Some("x-session-affinity")
        );
        assert_eq!(metadata.request_kind.as_deref(), Some("main"));
        assert_eq!(metadata.client_app, None);
    }

    #[test]
    fn request_cache_metadata_classifies_senpi_auxiliary_requests() {
        let cases = [
            (
                "subagent",
                Some("019d20f2-6f15-7de2-a800-f3e97928a87c"),
                r#"{"system":[{"type":"text","text":"You are senpi, a coding agent. Your work should be indistinguishable from a careful senior engineer's.\n\nYou are a focused code-review subagent."}],"messages":[{"role":"user","content":"Task: Review the auth change"}],"tools":[{"name":"read"}]}"#,
                "subagent",
            ),
            (
                "look at",
                None,
                r#"{"system":[{"type":"text","text":"You analyze attached media for a downstream agent that cannot inspect the attachments directly.\n\nExtract only the information requested by the goal."}],"messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"AA=="}},{"type":"text","text":"Goal:\nRead the label"}]}]}"#,
                "look_at",
            ),
        ];

        for (case, session_id, fixture, request_kind) in cases {
            let mut headers = HeaderMap::new();
            if let Some(session_id) = session_id {
                headers.insert(
                    HeaderName::from_static("x-session-affinity"),
                    HeaderValue::from_str(session_id).expect("valid session ID header"),
                );
            }
            let metadata =
                request_cache_metadata(&headers, &Bytes::copy_from_slice(fixture.as_bytes()));

            assert_eq!(
                metadata.observed_session_id.as_deref(),
                session_id,
                "{case}"
            );
            assert_eq!(
                metadata.request_kind.as_deref(),
                Some(request_kind),
                "{case}"
            );
            assert_eq!(metadata.client_app, None, "{case}");
        }
    }

    #[test]
    fn request_cache_metadata_classifies_omp_requests() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-claude-code-session-id"),
            HeaderValue::from_static("session-123"),
        );

        let cases = [
            (
                "main",
                r#"{"system":[{"type":"text","text":"<system-conventions>rules</system-conventions>\nROLE\n==============\nYou are a helpful assistant the team trusts with load-bearing changes, operating in the Oh My Pi coding harness."}],"messages":[{"role":"user","content":"hello"}]}"#,
                "main",
            ),
            (
                "advisor",
                r#"{"system":[{"type":"text","text":"<system-conventions>rules</system-conventions>\nYou shadow the main agent as a peer programmer:"}],"messages":[{"role":"user","content":"review this"}]}"#,
                "advisor",
            ),
            (
                "recap after reminder",
                r#"{"messages":[{"role":"user","content":"original request"},{"role":"assistant","content":"response"},{"role":"user","content":[{"type":"text","text":"<system-reminder>context</system-reminder>"},{"type":"text","text":"The user stepped away and is coming back. Recap in under 40 words. Lead with the overall goal and current task."}]}]}"#,
                "recap",
            ),
            (
                "compaction after transcript",
                r#"{"messages":[{"role":"user","content":"original request"},{"role":"assistant","content":"response"},{"role":"user","content":"Your task is to create a detailed summary of the conversation so far. Primary Request and Intent: keep this. REMINDER: Do NOT call any tools."}]}"#,
                "compaction",
            ),
            (
                "session title",
                r#"{"system":"Generate a concise, sentence-case title (3-7 words)","messages":[{"role":"user","content":"<session>hello</session> Write the title"}],"output_config":{"format":{"schema":{"properties":{"title":{"type":"string"}}}}}}"#,
                "session_title",
            ),
            (
                "auto thinking",
                r#"{"system":"You are a difficulty classifier for a coding agent. Read the user's request.","messages":[{"role":"user","content":"fix this"}]}"#,
                "auto_thinking",
            ),
            (
                "subagent prompt",
                r#"{"messages":[{"role":"user","content":"Complete the assignment below, thoroughly:\n\nFix the issue"},{"role":"assistant","content":"working"},{"role":"user","content":"follow-up"}]}"#,
                "subagent",
            ),
        ];

        for (case, fixture, request_kind) in cases {
            let metadata =
                request_cache_metadata(&headers, &Bytes::copy_from_slice(fixture.as_bytes()));

            assert_eq!(
                metadata.observed_session_id.as_deref(),
                Some("session-123"),
                "{case}"
            );
            assert_eq!(
                metadata.request_kind.as_deref(),
                Some(request_kind),
                "{case}"
            );
        }
    }

    #[tokio::test]
    async fn explicit_auxiliary_cannot_supersede_pending_legitimate_completion() {
        assert_explicit_auxiliary_completion_ordering(false).await;
    }

    #[tokio::test]
    async fn explicit_auxiliary_detached_completion_does_not_mutate_keepalive() {
        assert_explicit_auxiliary_completion_ordering(true).await;
    }

    async fn assert_explicit_auxiliary_completion_ordering(detached_gate: bool) {
        let body = Bytes::from_static(
            br#"{"model":"claude-test","max_tokens":32,"system":[{"type":"text","text":"cached","cache_control":{"type":"ephemeral","ttl":"5m"}}],"messages":[{"role":"user","content":"hello"}]}"#,
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-claude-code-session-id",
            HeaderValue::from_static("ordered-auxiliary"),
        );
        headers.insert(
            "x-claude-code-request-class",
            HeaderValue::from_static("main"),
        );
        let request_json: Value = serde_json::from_slice(&body).expect("ordered request fixture");
        let mut legitimate = request_cache_metadata(&headers, &body);
        legitimate.request_json = Some(request_json.clone());
        assert_eq!(legitimate.request_kind.as_deref(), Some("main"));
        headers.insert(
            "x-claude-code-request-class",
            HeaderValue::from_static("auxiliary"),
        );
        let mut auxiliary = request_cache_metadata(&headers, &body);
        auxiliary.request_json = Some(request_json);
        assert_eq!(auxiliary.request_kind.as_deref(), Some("unknown"));

        let mut record = principal_record("principal-test");
        record.cache_keepalive = Some(cc_lb_storage_api::CacheKeepaliveConfig {
            enabled: true,
            refresh_lead_time_5m_secs: 30,
            refresh_lead_time_1h_secs: 300,
            max_refreshes_per_session: 12,
            max_total_duration_secs: 14_400,
            snapshot_max_bytes: 524_288,
            min_cache_tokens: 10_000,
            classifier: cc_lb_storage_api::ClassifierConfig::default(),
        });
        let view = DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(TestSignerFactory))
            .principal_view(Arc::new(PrincipalView::from_db(&[record], HashMap::new())))
            .build();
        for cancellation in [false, true] {
            crate::cache_keepalive::LifecycleKeepalive::assert_auxiliary_cannot_supersede_pending_completion(
                Arc::new(DynamicViewHolder::new(Arc::clone(&view))),
                Principal { id: "principal-test".to_owned(), kind: cc_lb_domain::PrincipalKind::InternalKey },
                legitimate.clone(),
                auxiliary.clone(),
                body.clone(),
                cancellation,
                detached_gate,
            ).await;
        }
    }

    #[test]
    fn request_cache_metadata_classifies_claude_code_main_turn() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-claude-code-session-id"),
            HeaderValue::from_static("87b3d2e6-ce49-439d-9e68-0c90138a726a"),
        );
        let body = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "system": [{
                    "type": "text",
                    "text": "\nYou are an interactive agent that helps users with software engineering tasks. Use the instructions below and the tools available to you to assist the user."
                }],
                "messages": [{"role": "user", "content": "QA main turn reply ok"}],
                "tools": [{"name": "Agent"}]
            }))
            .expect("serialize Claude Code main request fixture"),
        );

        let metadata = request_cache_metadata(&headers, &body);

        assert_eq!(metadata.request_kind.as_deref(), Some("main"));
        assert_eq!(
            metadata.observed_session_id.as_deref(),
            Some("87b3d2e6-ce49-439d-9e68-0c90138a726a")
        );
    }

    #[test]
    fn request_cache_metadata_accepts_real_advisor_block_as_omp_provenance() {
        let headers = HeaderMap::new();
        let advisor_system_prefix = concat!(
            "<system-conventions>\n",
            "RFC 2119 applies to MUST, REQUIRED, SHOULD, RECOMMENDED, MAY, OPTIONAL. ",
            "`NEVER` and `AVOID` are aliases for `MUST NOT` and `SHOULD NOT`.\n",
            "</system-conventions>\n\n",
            "You bring a different angle, advocating for the user and for code quality & robustness.\n",
            "You shadow the main agent as a peer programmer:\n",
        );
        let marker_offset = advisor_system_prefix
            .find(crate::request_classification::omp::OMP_ADVISOR_SYSTEM_MARKER)
            .expect("real OMP Advisor prompt contains its marker");
        assert!(marker_offset < crate::request_classification::CLASSIFIER_TEXT_LIMIT);

        // OMP sends advisorSystemPrompt as the first standalone system block;
        // later context, WATCHDOG, shared, and per-advisor instructions are
        // separate blocks. Keep this fixture larger than the bounded scan while
        // preserving the real prefix and marker position.
        let advisor_system = format!(
            "{advisor_system_prefix}{}",
            "x".repeat(crate::request_classification::CLASSIFIER_TEXT_LIMIT)
        );
        assert!(advisor_system.len() > crate::request_classification::CLASSIFIER_TEXT_LIMIT);
        let body = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "system": [{"type": "text", "text": advisor_system}],
                "messages": [{"role": "user", "content": "review this"}],
            }))
            .expect("serialize real-layout OMP Advisor request fixture"),
        );

        let metadata = request_cache_metadata(&headers, &body);

        assert_eq!(metadata.observed_session_id, None);
        assert_eq!(metadata.thread_id, None);
        assert_eq!(metadata.request_kind.as_deref(), Some("advisor"));
    }
    #[test]
    fn request_cache_metadata_extracts_extended_client_session_identities() {
        for (header, value, source) in [
            (
                "x-opencode-session-id",
                "opencode-id",
                "x-opencode-session-id",
            ),
            (
                "x-opencode-session",
                "opencode-session",
                "x-opencode-session",
            ),
            (
                "x-aside-provider-session-id",
                "aside-provider",
                "x-aside-provider-session-id",
            ),
            ("x-aside-session-id", "aside-session", "x-aside-session-id"),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(
                HeaderName::from_bytes(header.as_bytes()).expect("valid header"),
                HeaderValue::from_static(value),
            );
            let metadata = request_cache_metadata(&headers, &Bytes::from_static(br#"{}"#));
            assert_eq!(metadata.observed_session_id.as_deref(), Some(value));
            assert_eq!(metadata.session_id_source.as_deref(), Some(source));
        }

        let mut opencode_headers = HeaderMap::new();
        opencode_headers.insert(
            HeaderName::from_static("x-opencode-session"),
            HeaderValue::from_static("opencode-child"),
        );
        opencode_headers.insert(
            HeaderName::from_static("x-opencode-parent-session-id"),
            HeaderValue::from_static("opencode-parent"),
        );
        let opencode = request_cache_metadata(&opencode_headers, &Bytes::from_static(br#"{}"#));
        assert_eq!(
            opencode.parent_session_id.as_deref(),
            Some("opencode-parent")
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-aside-session-id"),
            HeaderValue::from_static("aside:worker:aside-parent:default"),
        );
        let metadata = request_cache_metadata(&headers, &Bytes::from_static(br#"{}"#));
        assert_eq!(metadata.parent_session_id.as_deref(), Some("aside-parent"));
    }

    #[test]
    fn request_cache_metadata_derives_omp_and_senpi_parent_sessions() {
        for session_id in [
            "omp-parent:side:worker",
            "omp-parent:spec:worker",
            "senpi-parent:btw:worker",
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(
                HeaderName::from_static("x-session-id"),
                HeaderValue::from_str(session_id).expect("valid session ID"),
            );
            let metadata = request_cache_metadata(&headers, &Bytes::from_static(br#"{}"#));
            assert_eq!(
                metadata.parent_session_id.as_deref(),
                Some(session_id.split_once(':').expect("marker").0),
                "{session_id}"
            );
        }
    }

    #[test]
    fn request_cache_metadata_extracts_codex_client_metadata_and_legacy_user_ids() {
        let mut codex_headers = HeaderMap::new();
        codex_headers.insert(
            HeaderName::from_static("x-codex-turn-metadata"),
            HeaderValue::from_static(r#"{"session_id":"codex-session"}"#),
        );
        let codex = request_cache_metadata(&codex_headers, &Bytes::from_static(br#"{}"#));
        assert_eq!(codex.observed_session_id.as_deref(), Some("codex-session"));
        assert_eq!(
            codex.session_id_source.as_deref(),
            Some("x-codex-turn-metadata")
        );

        let client_metadata = request_cache_metadata(
            &HeaderMap::new(),
            &Bytes::from_static(br#"{"client_metadata":{"session_id":"client-session"}}"#),
        );
        assert_eq!(
            client_metadata.observed_session_id.as_deref(),
            Some("client-session")
        );
        assert_eq!(
            client_metadata.session_id_source.as_deref(),
            Some("client_metadata.session_id")
        );

        for user_id in [
            "user_abc_session_legacy-session",
            "87b3d2e6-ce49-439d-9e68-0c90138a726a",
        ] {
            let body = Bytes::from(
                serde_json::to_vec(&json!({"metadata": {"user_id": user_id}}))
                    .expect("serialize metadata"),
            );
            let metadata = request_cache_metadata(&HeaderMap::new(), &body);
            let expected = if user_id.contains("_session_") {
                "legacy-session"
            } else {
                user_id
            };
            assert_eq!(metadata.observed_session_id.as_deref(), Some(expected));
            assert_eq!(
                metadata.session_id_source.as_deref(),
                Some("metadata.user_id")
            );
        }
    }

    #[test]
    fn request_cache_metadata_uses_claude_request_class_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-claude-code-request-class"),
            HeaderValue::from_static("session_title"),
        );
        let metadata = request_cache_metadata(
            &headers,
            &Bytes::from_static(br#"{"messages":[{"role":"user","content":"ordinary"}]}"#),
        );
        assert_eq!(metadata.request_kind.as_deref(), Some("session_title"));
    }

    #[test]
    fn request_cache_metadata_classifies_main_with_large_project_context_block() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-claude-code-session-id"),
            HeaderValue::from_static("session-with-large-context"),
        );
        let body = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "system": [
                    {
                        "type": "text",
                        "text": "x".repeat(crate::request_classification::CLASSIFIER_TEXT_LIMIT + 1),
                    },
                    {
                        "type": "text",
                        "text": "You are a helpful assistant the team trusts with load-bearing changes, operating in the Oh My Pi coding harness.",
                    },
                ],
                "messages": [{"role": "user", "content": "hello"}],
            }))
            .expect("serialize OMP request fixture"),
        );

        let metadata = request_cache_metadata(&headers, &body);

        assert_eq!(metadata.request_kind.as_deref(), Some("main"));
    }

    #[test]
    fn request_cache_metadata_extracts_omp_metadata_session_id() {
        let headers = HeaderMap::new();
        let json_metadata = Bytes::from_static(
            br#"{"metadata":{"user_id":"{\"session_id\":\"session-from-body\",\"account_uuid\":\"account\"}"},"system":"You are a helpful assistant the team trusts with load-bearing changes, operating in the Oh My Pi coding harness.","messages":[{"role":"user","content":"hello"}]}"#,
        );

        let json = request_cache_metadata(&headers, &json_metadata);

        assert_eq!(
            json.observed_session_id.as_deref(),
            Some("session-from-body")
        );
        assert_eq!(json.thread_id.as_deref(), Some("session-from-body"));
        assert_eq!(json.request_kind.as_deref(), Some("main"));
        assert_eq!(json.session_id_source.as_deref(), Some("metadata.user_id"));
        assert_eq!(json.parent_session_id, None);
    }

    #[test]
    fn request_cache_metadata_extracts_claude_code_session_identity() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-claude-code-session-id"),
            HeaderValue::from_static("7f25b3d8-6bf2-4832-94e5-66394f785210"),
        );
        headers.insert(
            HeaderName::from_static("x-claude-code-agent-id"),
            HeaderValue::from_static("a5f4e1590bb7587bc"),
        );
        headers.insert(
            HeaderName::from_static("x-claude-code-parent-agent-id"),
            HeaderValue::from_static("b1c2d3e4f5061728a"),
        );
        headers.insert(
            HeaderName::from_static("x-app"),
            HeaderValue::from_static("cli-bg"),
        );
        let body = Bytes::from_static(
            br#"{"model":"claude-sonnet-4-5","metadata":{"user_id":"{\"device_id\":\"d\",\"account_uuid\":\"acc\",\"session_id\":\"7f25b3d8-6bf2-4832-94e5-66394f785210\",\"parent_session_id\":\"0714858d-c434-4aae-8bc0-5f33cf6cf63d\"}"},"messages":[{"role":"user","content":"hi"}]}"#,
        );

        let metadata = request_cache_metadata(&headers, &body);

        assert_eq!(
            metadata.observed_session_id.as_deref(),
            Some("7f25b3d8-6bf2-4832-94e5-66394f785210")
        );
        assert_eq!(
            metadata.session_id_source.as_deref(),
            Some("x-claude-code-session-id")
        );
        assert_eq!(
            metadata.claude_agent_id.as_deref(),
            Some("a5f4e1590bb7587bc")
        );
        assert_eq!(
            metadata.claude_parent_agent_id.as_deref(),
            Some("b1c2d3e4f5061728a")
        );
        assert_eq!(
            metadata.parent_session_id.as_deref(),
            Some("0714858d-c434-4aae-8bc0-5f33cf6cf63d")
        );
        assert_eq!(metadata.client_app.as_deref(), Some("cli-bg"));
        assert_eq!(metadata.request_kind.as_deref(), Some("subagent"));
    }

    #[test]
    fn request_cache_metadata_reports_metadata_session_source() {
        let headers = HeaderMap::new();
        let body = Bytes::from_static(
            br#"{"metadata":{"user_id":"{\"session_id\":\"session-from-body\",\"parent_session_id\":\"session-parent\"}"},"messages":[]}"#,
        );

        let metadata = request_cache_metadata(&headers, &body);

        assert_eq!(
            metadata.observed_session_id.as_deref(),
            Some("session-from-body")
        );
        assert_eq!(
            metadata.session_id_source.as_deref(),
            Some("metadata.user_id")
        );
        assert_eq!(
            metadata.parent_session_id.as_deref(),
            Some("session-parent")
        );
    }

    #[test]
    fn request_cache_metadata_rejects_oversized_identity_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-claude-code-session-id"),
            HeaderValue::from_static("session-123"),
        );
        headers.insert(
            HeaderName::from_static("x-claude-code-agent-id"),
            HeaderValue::from_bytes(&vec![b'a'; MAX_IDENTITY_VALUE_BYTES + 1])
                .expect("valid oversized header"),
        );

        let metadata = request_cache_metadata(
            &headers,
            &Bytes::from_static(br#"{"messages":[{"role":"user","content":"work"}]}"#),
        );

        assert_eq!(metadata.claude_agent_id, None);
        assert_ne!(metadata.request_kind.as_deref(), Some("subagent"));
    }

    #[test]
    fn request_cache_metadata_prefers_session_header_over_body_metadata() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-session-affinity"),
            HeaderValue::from_static("header-session"),
        );
        let body = Bytes::from_static(
            br#"{"metadata":{"user_id":"{\"session_id\":\"body-session\"}"},"messages":[]}"#,
        );

        let metadata = request_cache_metadata(&headers, &body);

        assert_eq!(
            metadata.observed_session_id.as_deref(),
            Some("header-session")
        );
    }

    #[test]
    fn request_cache_metadata_classifies_subagent_lineage_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-claude-code-agent-id"),
            HeaderValue::from_static("agent-456"),
        );

        let metadata = request_cache_metadata(
            &headers,
            &Bytes::from_static(br#"{"messages":[{"role":"user","content":"work"}]}"#),
        );

        assert_eq!(metadata.request_kind.as_deref(), Some("subagent"));
    }

    #[test]
    fn request_cache_metadata_does_not_fingerprint_without_omp_provenance() {
        let headers = HeaderMap::new();
        let body = Bytes::from_static(
            br#"{"messages":[{"role":"user","content":"The user stepped away and is coming back. Recap in under 40 words."}]}"#,
        );

        let metadata = request_cache_metadata(&headers, &body);

        assert_eq!(metadata.request_kind, None);
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

        assert_eq!(breakpoint.prefix_hash, expected_v3);
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
            plugin_breakpoints[0].prefix_bytes.get(),
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

    struct RecordingPromptCacheObservationSink {
        records: Mutex<Vec<PromptCacheObservationRecord>>,
        provider_records: Mutex<Vec<ProviderPromptCacheObservationRecord>>,
        clock_now: u64,
        grace_secs: u64,
    }
    impl RecordingPromptCacheObservationSink {
        fn new(_entries: Vec<WarmCacheEntry>) -> Self {
            Self {
                records: Mutex::new(Vec::new()),
                provider_records: Mutex::new(Vec::new()),
                clock_now: 0,
                grace_secs: 30,
            }
        }
        fn with_clock_now(mut self, now: u64) -> Self {
            self.clock_now = now;
            self
        }
        fn with_grace_secs(mut self, secs: u64) -> Self {
            self.grace_secs = secs;
            self
        }
        fn upserts(&self) -> Vec<PromptCacheObservationRecord> {
            self.records.lock().expect("records lock").clone()
        }
        fn provider_upserts(&self) -> Vec<ProviderPromptCacheObservationRecord> {
            self.provider_records
                .lock()
                .expect("provider records lock")
                .clone()
        }
        fn clock_now_unix_secs(&self) -> u64 {
            self.clock_now
        }
        fn grace_margin_secs(&self) -> u64 {
            self.grace_secs
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
        fn enqueue_provider(
            &self,
            record: ProviderPromptCacheObservationRecord,
        ) -> Result<(), PromptCacheObservationEnqueueError> {
            self.provider_records
                .lock()
                .expect("provider records lock")
                .push(record);
            Ok(())
        }
    }
    struct TogglePromptCacheStore {
        failed: AtomicBool,
    }
    #[async_trait]
    impl cc_lb_storage_api::PromptCacheObservationStore for TogglePromptCacheStore {
        async fn list_active_for_candidates(
            &self,
            _upstream_ids: &[Uuid],
            _canonical_model_id: &str,
            _v3_prefix_keys: &[String],
            _not_expired_at_unix_secs: u64,
        ) -> cc_lb_storage_api::StorageResult<Vec<PromptCacheObservationRecord>> {
            if self.failed.load(Ordering::Relaxed) {
                Err(cc_lb_storage_api::StorageError::Unavailable {
                    message: "test failure".to_owned(),
                })
            } else {
                Ok(Vec::new())
            }
        }
    }

    #[tokio::test]
    async fn shared_cache_read_failure_is_unknown_not_cold_and_preserves_candidate() {
        let upstream_id = Uuid::from_u128(825);
        let store = Arc::new(TogglePromptCacheStore {
            failed: AtomicBool::new(false),
        });
        let mut view = cache_score_view(upstream_id);
        Arc::get_mut(&mut view)
            .expect("new test view is exclusively owned")
            .prompt_cache_observation_store = Some(store.clone());
        let clock = cc_lb_clock::TestClock::new_at_secs(1_800_000_000);
        let breakpoints = vec![cache_breakpoint(0, "prefix", 100, TtlClass::Ephemeral5m)];
        for failed in [false, true, false] {
            store.failed.store(failed, Ordering::Relaxed);
            let snapshot = fetch_prompt_cache_warm_entries(
                &view,
                "principal",
                RequestKind::AnthropicMessages,
                TEST_MODEL,
                &breakpoints,
                &clock,
            )
            .await;
            let candidates = build_candidates(
                &view,
                "principal",
                RequestKind::AnthropicMessages,
                &breakpoints,
                &snapshot,
                &clock,
            );
            assert_eq!(candidates.len(), 1);
            let candidate = candidates.first().expect("eligible candidate");
            assert_eq!(candidate.upstream_id, upstream_id);
            if failed {
                assert!(candidate.cache_score.is_none(), "failed read is unknown");
            } else {
                let score = candidate.cache_score.as_ref().expect("known-cold score");
                assert_eq!(score.predicted_cache_read_tokens, 0);
                assert_eq!(score.predicted_cache_creation_tokens_5m, 100);
            }
        }
    }

    /// In-memory observation store that mirrors the real backend contract:
    /// rows are filtered by upstream, canonical model, prefix key, and
    /// `expires_at > not_expired_at_unix_secs`, and every lookup is counted.
    struct FakePromptCacheStore {
        records: Mutex<Vec<PromptCacheObservationRecord>>,
        lookups: AtomicUsize,
        requested_keys: Mutex<Vec<String>>,
    }
    impl FakePromptCacheStore {
        fn new(records: Vec<PromptCacheObservationRecord>) -> Self {
            Self {
                records: Mutex::new(records),
                lookups: AtomicUsize::new(0),
                requested_keys: Mutex::new(Vec::new()),
            }
        }
        fn lookup_count(&self) -> usize {
            self.lookups.load(Ordering::Relaxed)
        }
        fn requested_keys(&self) -> Vec<String> {
            self.requested_keys
                .lock()
                .expect("requested keys lock")
                .clone()
        }
    }
    #[async_trait]
    impl cc_lb_storage_api::PromptCacheObservationStore for FakePromptCacheStore {
        async fn list_active_for_candidates(
            &self,
            upstream_ids: &[Uuid],
            canonical_model_id: &str,
            v3_prefix_keys: &[String],
            not_expired_at_unix_secs: u64,
        ) -> cc_lb_storage_api::StorageResult<Vec<PromptCacheObservationRecord>> {
            self.lookups.fetch_add(1, Ordering::Relaxed);
            self.requested_keys
                .lock()
                .expect("requested keys lock")
                .extend(v3_prefix_keys.iter().cloned());
            Ok(self
                .records
                .lock()
                .expect("records lock")
                .iter()
                .filter(|record| upstream_ids.contains(&record.upstream_id))
                .filter(|record| record.canonical_model_id == canonical_model_id)
                .filter(|record| v3_prefix_keys.contains(&record.v3_prefix_key))
                .filter(|record| record.expires_at_unix_secs > not_expired_at_unix_secs)
                .cloned()
                .collect())
        }
    }

    fn observation_record(
        upstream_id: Uuid,
        prefix_key: &str,
        ttl_class: TtlClass,
        expires_at_unix_secs: u64,
    ) -> PromptCacheObservationRecord {
        PromptCacheObservationRecord {
            upstream_id,
            canonical_model_id: TEST_MODEL.to_owned(),
            v3_prefix_key: prefix_key.to_owned(),
            ttl_class,
            expires_at_unix_secs,
            last_observed_at_unix_secs: 1_700_000_000,
            hash_schema_version: HASH_SCHEMA_VERSION,
            prefix_content_block_index: 0,
            estimated_prefix_tokens: 100,
            token_estimate_source: V3_TOKEN_ESTIMATE_SOURCE.to_owned(),
        }
    }

    #[tokio::test]
    async fn shared_warm_lookup_unions_official_and_literal_bundle_keys() {
        let official_id = Uuid::from_u128(1010);
        let custom_id = Uuid::from_u128(1011);
        let mut custom = upstream_record(custom_id);
        custom.base_url = Some("https://proxy.example.com".parse().expect("custom URL"));
        let store = Arc::new(FakePromptCacheStore::new(Vec::new()));
        let view = DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(TestSignerFactory))
            .principal_view(Arc::new(PrincipalView::from_db(
                &[principal_record("principal")],
                HashMap::new(),
            )))
            .upstream_records(vec![upstream_record(official_id), custom])
            .prompt_cache_observation_store(store.clone())
            .build();
        let official = cache_breakpoint(1, "official-union-key", 100, TtlClass::Ephemeral5m);
        let literal = cache_breakpoint(1, "literal-union-key", 200, TtlClass::Ephemeral5m);
        let bundles = PromptCacheBundles {
            official: PromptCacheBundle {
                cache_breakpoints: vec![official],
                cacheable_breakpoint_prefix_keys: HashSet::new(),
                request_breakpoints: Vec::new(),
                cache_prefix_hash: None,
            },
            literal: PromptCacheBundle {
                cache_breakpoints: vec![literal],
                cacheable_breakpoint_prefix_keys: HashSet::new(),
                request_breakpoints: Vec::new(),
                cache_prefix_hash: None,
            },
        };
        let snapshot = fetch_prompt_cache_warm_entries_for_bundles(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            TEST_MODEL,
            &bundles,
            &cc_lb_clock::TestClock::new_at_secs(1_800_000_000),
        )
        .await;
        assert_eq!(snapshot.len(), 2);
        let keys = store.requested_keys();
        assert!(keys.contains(&"official-union-key".to_owned()));
        assert!(keys.contains(&"literal-union-key".to_owned()));
    }

    fn cache_score_view_with_store(
        upstream_id: Uuid,
        store: Arc<dyn cc_lb_storage_api::PromptCacheObservationStore>,
    ) -> Arc<DynamicView> {
        let mut view = cache_score_view(upstream_id);
        Arc::get_mut(&mut view)
            .expect("new test view is exclusively owned")
            .prompt_cache_observation_store = Some(store);
        view
    }

    #[test]
    fn mixed_official_and_custom_candidates_score_only_their_own_bundle() {
        let official_id = Uuid::from_u128(1004);
        let custom_id = Uuid::from_u128(1005);
        let mut custom = upstream_record(custom_id);
        custom.base_url = Some("https://proxy.example.com/v1".parse().expect("custom URL"));
        let view = DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(TestSignerFactory))
            .principal_view(Arc::new(PrincipalView::from_db(
                &[principal_record("principal")],
                HashMap::new(),
            )))
            .upstream_records(vec![upstream_record(official_id), custom])
            .build();
        let official_breakpoints = vec![cache_breakpoint(
            1,
            "official-prefix",
            100,
            TtlClass::Ephemeral5m,
        )];
        let custom_breakpoints = vec![cache_breakpoint(
            1,
            "custom-prefix",
            200,
            TtlClass::Ephemeral5m,
        )];
        let bundles = PromptCacheBundles {
            official: PromptCacheBundle {
                cache_breakpoints: official_breakpoints.clone(),
                cacheable_breakpoint_prefix_keys: cacheable_prefix_keys(&["official-prefix"]),
                request_breakpoints: official_breakpoints
                    .iter()
                    .map(request_cache_breakpoint_from_plugin)
                    .collect(),
                cache_prefix_hash: Some("official-prefix".to_owned()),
            },
            literal: PromptCacheBundle {
                cache_breakpoints: custom_breakpoints.clone(),
                cacheable_breakpoint_prefix_keys: cacheable_prefix_keys(&["custom-prefix"]),
                request_breakpoints: custom_breakpoints
                    .iter()
                    .map(request_cache_breakpoint_from_plugin)
                    .collect(),
                cache_prefix_hash: Some("custom-prefix".to_owned()),
            },
        };
        let now = 1_800_000_000;
        let mut official_warm =
            warm_entry_priced("official-prefix", TtlClass::Ephemeral5m, now + 600, 0, 100);
        official_warm.content_block_index = 1;
        let mut custom_warm =
            warm_entry_priced("custom-prefix", TtlClass::Ephemeral5m, now + 600, 0, 200);
        custom_warm.content_block_index = 1;
        let warm_entries = HashMap::from([
            (official_id, vec![official_warm]),
            (custom_id, vec![custom_warm]),
        ]);
        let candidates = build_candidates_with_matches_for_bundles(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            &official_breakpoints,
            &warm_entries,
            Some(&bundles),
            &cc_lb_clock::TestClock::new_at_secs(now),
        )
        .0;
        let official = candidates
            .iter()
            .find(|candidate| candidate.upstream_id == official_id)
            .and_then(|candidate| candidate.cache_score.as_ref())
            .expect("official score");
        let custom = candidates
            .iter()
            .find(|candidate| candidate.upstream_id == custom_id)
            .and_then(|candidate| candidate.cache_score.as_ref())
            .expect("custom score");
        assert_eq!(
            official.matched_v3_cache_key.as_deref(),
            Some("official-prefix")
        );
        assert_eq!(official.predicted_cache_read_tokens, 100);
        assert_eq!(
            custom.matched_v3_cache_key.as_deref(),
            Some("custom-prefix")
        );
        assert_eq!(custom.predicted_cache_read_tokens, 200);
    }

    #[tokio::test]
    async fn warm_entry_snapshot_is_request_scoped_and_expires_at_selection_time() {
        let upstream_id = Uuid::from_u128(826);
        let now = 1_800_000_000_u64;
        let store = Arc::new(FakePromptCacheStore::new(vec![observation_record(
            upstream_id,
            "prefix-a",
            TtlClass::Ephemeral5m,
            now + 5,
        )]));
        let view = cache_score_view_with_store(upstream_id, store.clone());
        let clock = cc_lb_clock::TestClock::new_at_secs(now);
        let breakpoints = vec![
            cache_breakpoint(0, "prefix-a", 100, TtlClass::Ephemeral5m),
            cache_breakpoint(1, "prefix-b", 200, TtlClass::Ephemeral5m),
        ];

        // The request's snapshot is taken before a second observation commits.
        let snapshot = fetch_prompt_cache_warm_entries(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            TEST_MODEL,
            &breakpoints,
            &clock,
        )
        .await;
        store
            .records
            .lock()
            .expect("records lock")
            .push(observation_record(
                upstream_id,
                "prefix-b",
                TtlClass::Ephemeral5m,
                now + 60,
            ));

        // The saved snapshot must not see the newer committed row, while the
        // next request's read does.
        assert_eq!(snapshot[&upstream_id].len(), 1);
        let next_snapshot = fetch_prompt_cache_warm_entries(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            TEST_MODEL,
            &breakpoints,
            &clock,
        )
        .await;
        assert_eq!(next_snapshot[&upstream_id].len(), 2);

        // Selection on the saved snapshot still warm-routes on prefix-a; the
        // deeper prefix-b row is absent from this request's snapshot.
        let candidates = build_candidates(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            &breakpoints,
            &snapshot,
            &clock,
        );
        let score = candidates[0].cache_score.as_ref().expect("warm score");
        assert_eq!(score.matched_v3_cache_key.as_deref(), Some("prefix-a"));
        assert_eq!(score.predicted_cache_read_tokens, 100);

        // Selection-time expiry re-check: expires_at > now is strict, so the
        // same snapshot is warm one second before expiry and cold at expiry.
        clock.advance_secs(4);
        let candidates = build_candidates(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            &breakpoints,
            &snapshot,
            &clock,
        );
        let score = candidates[0].cache_score.as_ref().expect("still warm");
        assert_eq!(score.predicted_cache_read_tokens, 100);

        clock.advance_secs(1);
        let candidates = build_candidates(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            &breakpoints,
            &snapshot,
            &clock,
        );
        let score = candidates[0].cache_score.as_ref().expect("cold score");
        assert_eq!(score.predicted_cache_read_tokens, 0);
        assert_eq!(score.matched_v3_cache_key, None);
    }

    #[tokio::test]
    async fn empty_request_breakpoints_skip_store_lookup_and_still_build_candidates() {
        let upstream_id = Uuid::from_u128(827);
        let store = Arc::new(FakePromptCacheStore::new(Vec::new()));
        let view = cache_score_view_with_store(upstream_id, store.clone());
        let clock = cc_lb_clock::TestClock::new_at_secs(1_800_000_000);

        // No cache breakpoints: the shared store must not be queried.
        let snapshot = fetch_prompt_cache_warm_entries(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            TEST_MODEL,
            &[],
            &clock,
        )
        .await;
        assert!(snapshot.is_empty());
        assert_eq!(store.lookup_count(), 0);
        let candidates = build_candidates(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            &[],
            &snapshot,
            &clock,
        );
        assert_eq!(candidates.len(), 1);
        assert!(candidates[0].cache_score.is_none());

        // Breakpoints without lookback prefixes produce no candidate keys.
        let mut keyless = cache_breakpoint(0, "prefix", 100, TtlClass::Ephemeral5m);
        keyless.lookback_prefixes.clear();
        let snapshot = fetch_prompt_cache_warm_entries(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            TEST_MODEL,
            &[keyless],
            &clock,
        )
        .await;
        assert!(snapshot.is_empty());
        assert_eq!(store.lookup_count(), 0);

        // An unresolved (empty) canonical model also skips the lookup.
        let breakpoints = vec![cache_breakpoint(0, "prefix", 100, TtlClass::Ephemeral5m)];
        let snapshot = fetch_prompt_cache_warm_entries(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            "",
            &breakpoints,
            &clock,
        )
        .await;
        assert!(snapshot.is_empty());
        assert_eq!(store.lookup_count(), 0);
    }

    #[tokio::test]
    async fn foreign_schema_version_observation_is_known_cold_not_warm() {
        let upstream_id = Uuid::from_u128(828);
        let mut record =
            observation_record(upstream_id, "prefix", TtlClass::Ephemeral5m, 1_800_000_600);
        record.hash_schema_version = HASH_SCHEMA_VERSION + 1;
        let store = Arc::new(FakePromptCacheStore::new(vec![record]));
        let view = cache_score_view_with_store(upstream_id, store.clone());
        let clock = cc_lb_clock::TestClock::new_at_secs(1_800_000_000);
        let breakpoints = vec![cache_breakpoint(0, "prefix", 100, TtlClass::Ephemeral5m)];

        let snapshot = fetch_prompt_cache_warm_entries(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            TEST_MODEL,
            &breakpoints,
            &clock,
        )
        .await;
        // The read succeeded: the upstream is present but the foreign-schema
        // row was dropped, so this is known-cold rather than unknown.
        assert_eq!(snapshot[&upstream_id].len(), 0);
        let candidates = build_candidates(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            &breakpoints,
            &snapshot,
            &clock,
        );
        let score = candidates[0]
            .cache_score
            .as_ref()
            .expect("known-cold score, not unknown");
        assert_eq!(score.predicted_cache_read_tokens, 0);
        assert_eq!(score.predicted_cache_creation_tokens_5m, 100);
    }

    #[tokio::test]
    async fn warm_match_ignores_observation_from_other_estimator_source() {
        let upstream_id = Uuid::from_u128(766);
        let clock = cc_lb_clock::TestClock::new_at_secs(1_800_000_000);
        let breakpoints = vec![cache_breakpoint(0, "prefix", 100, TtlClass::Ephemeral5m)];

        // Positive control: an unexpired row from the current estimator warms
        // the request and its prefix size is subtracted as a predicted read.
        let current =
            observation_record(upstream_id, "prefix", TtlClass::Ephemeral5m, 1_800_000_600);
        let store = Arc::new(FakePromptCacheStore::new(vec![current]));
        let view = cache_score_view_with_store(upstream_id, store.clone());
        let snapshot = fetch_prompt_cache_warm_entries(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            TEST_MODEL,
            &breakpoints,
            &clock,
        )
        .await;
        assert_eq!(snapshot[&upstream_id].len(), 1);
        let candidates = build_candidates(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            &breakpoints,
            &snapshot,
            &clock,
        );
        let score = candidates[0].cache_score.as_ref().expect("warm score");
        assert_eq!(score.matched_v3_cache_key.as_deref(), Some("prefix"));
        assert_eq!(score.predicted_cache_read_tokens, 100);

        // Same schema version, but the size was measured by another
        // estimator: its unit is not comparable, so the row must be ignored
        // and the upstream scored known-cold.
        let mut stale =
            observation_record(upstream_id, "prefix", TtlClass::Ephemeral5m, 1_800_000_600);
        stale.token_estimate_source = "local_tiktoken_v1".to_owned();
        assert_eq!(stale.hash_schema_version, HASH_SCHEMA_VERSION);
        let store = Arc::new(FakePromptCacheStore::new(vec![stale]));
        let view = cache_score_view_with_store(upstream_id, store.clone());
        let snapshot = fetch_prompt_cache_warm_entries(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            TEST_MODEL,
            &breakpoints,
            &clock,
        )
        .await;
        assert_eq!(snapshot[&upstream_id].len(), 0);
        let candidates = build_candidates(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            &breakpoints,
            &snapshot,
            &clock,
        );
        let score = candidates[0]
            .cache_score
            .as_ref()
            .expect("known-cold score, not unknown");
        assert_eq!(score.matched_v3_cache_key, None);
        assert_eq!(score.predicted_cache_read_tokens, 0);
        assert_eq!(score.predicted_cache_creation_tokens_5m, 100);
    }

    #[tokio::test]
    async fn warm_entries_keep_only_current_estimator_rows() {
        let upstream_id = Uuid::from_u128(76_602);
        let clock = cc_lb_clock::TestClock::new_at_secs(1_800_000_000);
        let breakpoints = vec![
            cache_breakpoint(0, "current", 100, TtlClass::Ephemeral5m),
            cache_breakpoint(1, "tiktoken", 200, TtlClass::Ephemeral5m),
            cache_breakpoint(2, "unknown", 300, TtlClass::Ephemeral5m),
            cache_breakpoint(3, "empty", 400, TtlClass::Ephemeral5m),
        ];
        let current =
            observation_record(upstream_id, "current", TtlClass::Ephemeral5m, 1_800_000_600);
        let mut records = vec![current];
        for (prefix_key, source) in [
            ("tiktoken", "local_tiktoken_v1"),
            ("unknown", "unknown"),
            ("empty", ""),
        ] {
            let mut record = observation_record(
                upstream_id,
                prefix_key,
                TtlClass::Ephemeral5m,
                1_800_000_600,
            );
            record.token_estimate_source = source.to_owned();
            records.push(record);
        }
        let store = Arc::new(FakePromptCacheStore::new(records));
        let view = cache_score_view_with_store(upstream_id, store.clone());

        let snapshot = fetch_prompt_cache_warm_entries(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            TEST_MODEL,
            &breakpoints,
            &clock,
        )
        .await;

        assert_eq!(store.lookup_count(), 1);
        let entries = snapshot
            .get(&upstream_id)
            .expect("successful lookup keeps the upstream as known");
        let keys = entries
            .iter()
            .map(|entry| entry.prefix_hash.as_str())
            .collect::<Vec<_>>();
        assert_eq!(keys, vec!["current"]);
        assert_eq!(
            entries.first().map(|entry| entry.estimated_prefix_bytes),
            Some(PrefixBytes::new(100))
        );
    }

    #[tokio::test]
    async fn five_minute_observation_cannot_warm_one_hour_request() {
        let upstream_id = Uuid::from_u128(829);
        let store = Arc::new(FakePromptCacheStore::new(vec![observation_record(
            upstream_id,
            "prefix",
            TtlClass::Ephemeral5m,
            1_800_000_600,
        )]));
        let view = cache_score_view_with_store(upstream_id, store.clone());
        let clock = cc_lb_clock::TestClock::new_at_secs(1_800_000_000);
        let breakpoints = vec![cache_breakpoint(0, "prefix", 100, TtlClass::Ephemeral1h)];

        let snapshot = fetch_prompt_cache_warm_entries(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            TEST_MODEL,
            &breakpoints,
            &clock,
        )
        .await;
        // The store returns the row (TTL is not a store filter); selection
        // rejects it because a 5m observation cannot satisfy a 1h breakpoint.
        assert_eq!(snapshot[&upstream_id].len(), 1);
        let candidates = build_candidates(
            &view,
            "principal",
            RequestKind::AnthropicMessages,
            &breakpoints,
            &snapshot,
            &clock,
        );
        let score = candidates[0].cache_score.as_ref().expect("cold score");
        assert_eq!(score.predicted_cache_read_tokens, 0);
        assert_eq!(score.predicted_cache_creation_tokens_1h, 100);
    }

    fn cache_score_view(upstream_id: Uuid) -> Arc<DynamicView> {
        DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(TestSignerFactory))
            .principal_view(Arc::new(PrincipalView::from_db(
                &[principal_record("principal")],
                HashMap::new(),
            )))
            .upstream_records(vec![upstream_record(upstream_id)])
            .build()
    }
    fn cacheable_prefix_keys(prefixes: &[&str]) -> HashSet<String> {
        prefixes.iter().map(|prefix| (*prefix).to_owned()).collect()
    }

    fn cache_breakpoint(
        index: u32,
        prefix_hash: &str,
        prefix_bytes: u64,
        requested_ttl: TtlClass,
    ) -> CacheBreakpoint {
        CacheBreakpoint {
            block_index: index,
            source: CacheBreakpointSource::Message,
            path: format!("messages[{index}]"),
            message_index: Some(index),
            prefix_hash: prefix_hash.to_owned(),
            prefix_bytes: PrefixBytes::new(prefix_bytes),
            requested_ttl,
            lookback_prefixes: vec![CacheLookbackPrefix {
                prefix_hash: prefix_hash.to_owned(),
                content_block_index: index,
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
        warm_entry_priced(
            prefix_hash,
            ttl_class,
            expires_at_unix_secs,
            observed_offset,
            0,
        )
    }

    fn selected_match_hit(
        prefix_hash: &str,
        block_index: u32,
        ttl_class: TtlClass,
        estimated_prefix_bytes: u64,
    ) -> SelectedCacheMatch {
        SelectedCacheMatch {
            prefix_hash: prefix_hash.to_owned(),
            content_block_index: block_index,
            ttl_class,
            estimated_prefix_bytes: PrefixBytes::new(estimated_prefix_bytes),
        }
    }

    fn warm_entry_priced(
        prefix_hash: &str,
        ttl_class: TtlClass,
        expires_at_unix_secs: u64,
        observed_offset: u64,
        estimated_prefix_bytes: u64,
    ) -> WarmCacheEntry {
        WarmCacheEntry {
            prefix_hash: prefix_hash.to_owned(),
            expires_at_unix_secs,
            ttl_class,
            last_observed_at_unix_secs: 1_700_000_000 + observed_offset,
            content_block_index: 0,
            estimated_prefix_bytes: PrefixBytes::new(estimated_prefix_bytes),
            hash_schema_version: HASH_SCHEMA_VERSION,
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
            deleted_at_unix_secs: None,
            revision: 1,
            created_at_unix_secs: 0,
            updated_at_unix_secs: 0,
            router_terminal_strategy: Default::default(),
            cache_keepalive: None,
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
            _router_chosen_upstream_name: String,
        ) -> Arc<dyn SignerFactory> {
            Arc::new(Self)
        }
    }

    #[async_trait]
    impl SignerFactory for TestSignerFactory {
        async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
            Ok(Arc::new(TestSigner))
        }
    }

    struct TestSigner;

    #[async_trait]
    impl Signer for TestSigner {
        async fn sign(
            &self,
            shaped: ShapedRequest,
            capability: &mut cc_lb_upstream::SigningCapability,
        ) -> Result<SignedRequest, SignerError> {
            Ok(SignedRequest::from_shaped(shaped, capability))
        }

        async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
            RetryDecision::Fail
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
        let mut decoder = UsageDecoder::from_headers(
            &headers,
            decompression_output_budget_bytes(DEFAULT_MESSAGES_CAP_BYTES),
        );
        assert!(decoder.is_active(), "gzip decoder should be active");

        let mut buffer: Vec<u8> = Vec::new();
        let mut usage = UsageCounts::default();
        let mut events_seen = 0_u64;

        for chunk in compressed.chunks(7) {
            let plaintext = decoder.push(chunk).expect("decode chunk");
            buffer.extend_from_slice(&plaintext);
            while let Some(end) = usage_parser::find_sse_event_end(&buffer) {
                let raw = buffer.drain(..end).collect::<Vec<u8>>();
                let event = parse_sse_event(&raw);
                let _ = accumulate_sse_usage(&event, &mut usage);
                events_seen += 1;
            }
        }
        let tail = decoder.finish().expect("decoder finish");
        buffer.extend_from_slice(&tail);
        while let Some(end) = usage_parser::find_sse_event_end(&buffer) {
            let raw = buffer.drain(..end).collect::<Vec<u8>>();
            let event = parse_sse_event(&raw);
            let _ = accumulate_sse_usage(&event, &mut usage);
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

    // ------------------------------------------------------------------
    // Failure-provenance tests: every known internal failure must reach the
    // terminal snapshot with a typed stage/kind/message, and non-fatal
    // diagnostics must accumulate ahead of it.
    // ------------------------------------------------------------------

    struct FailureKeyFixture {
        plaintext: String,
        key_id: String,
        record: StoredApiKeyRecord,
    }

    static FAILURE_KEY_FIXTURE: std::sync::LazyLock<FailureKeyFixture> =
        std::sync::LazyLock::new(|| {
            let generated = cc_lb_control::api_keys::secret::generate_new();
            FailureKeyFixture {
                plaintext: generated.plaintext.expose().to_owned(),
                key_id: generated.key_id,
                record: StoredApiKeyRecord {
                    label: "failure provenance key".to_owned(),
                    verify_hash: generated.verify_hash,
                    secret_salt: generated.secret_salt,
                    status: cc_lb_storage_api::KeyStatus::Active,
                    last_4: generated.last_4,
                    index_hash: generated.index_hash,
                    ..StoredApiKeyRecord::default()
                },
            }
        });

    fn failure_key_fixture() -> &'static FailureKeyFixture {
        &FAILURE_KEY_FIXTURE
    }

    struct InMemoryManagedKeyStore {
        principal_id: String,
    }

    #[async_trait]
    impl cc_lb_storage_api::ManagedKeyStore for InMemoryManagedKeyStore {
        async fn issue(
            &self,
            _principal_id: &str,
            _key_id: &str,
            _params: cc_lb_storage_api::IssueParams,
        ) -> cc_lb_storage_api::StorageResult<StoredApiKeyRecord> {
            Ok(failure_key_fixture().record.clone())
        }

        async fn get(
            &self,
            principal_id: &str,
            key_id: &str,
        ) -> cc_lb_storage_api::StorageResult<Option<StoredApiKeyRecord>> {
            Ok(
                (principal_id == self.principal_id && key_id == failure_key_fixture().key_id)
                    .then(|| failure_key_fixture().record.clone()),
            )
        }

        async fn lookup_by_index_hash(
            &self,
            index_hash: &[u8; 32],
        ) -> cc_lb_storage_api::StorageResult<Option<(String, String, StoredApiKeyRecord)>>
        {
            Ok(
                (index_hash == &failure_key_fixture().record.index_hash).then(|| {
                    (
                        self.principal_id.clone(),
                        failure_key_fixture().key_id.clone(),
                        failure_key_fixture().record.clone(),
                    )
                }),
            )
        }

        async fn list_by_principal(
            &self,
            principal_id: &str,
        ) -> cc_lb_storage_api::StorageResult<Vec<StoredApiKeyRecord>> {
            Ok((principal_id == self.principal_id)
                .then(|| failure_key_fixture().record.clone())
                .into_iter()
                .collect())
        }

        async fn list_all(
            &self,
        ) -> cc_lb_storage_api::StorageResult<Vec<(String, String, StoredApiKeyRecord)>> {
            Ok(vec![(
                self.principal_id.clone(),
                failure_key_fixture().key_id.clone(),
                failure_key_fixture().record.clone(),
            )])
        }

        async fn update(
            &self,
            _principal_id: &str,
            _key_id: &str,
            _mutation: cc_lb_storage_api::ApiKeyMutation,
        ) -> cc_lb_storage_api::StorageResult<()> {
            Ok(())
        }

        async fn revoke_zero_secrets(
            &self,
            _principal_id: &str,
            _key_id: &str,
        ) -> cc_lb_storage_api::StorageResult<()> {
            Ok(())
        }
    }

    fn failure_authn() -> Arc<BuiltinAuthn> {
        let storage: Arc<dyn cc_lb_storage_api::ManagedKeyStore> =
            Arc::new(InMemoryManagedKeyStore {
                principal_id: "principal-test".to_owned(),
            });
        Arc::new(BuiltinAuthn::new(
            Arc::new(cc_lb_control::api_keys::key_store::KeyStore::new(storage)),
            Arc::new(cc_lb_clock::SystemClock),
        ))
    }

    /// Deterministic dispatcher: replays a scripted queue of outcomes.
    struct QueueDispatch {
        calls: AtomicUsize,
        outcomes: Mutex<std::collections::VecDeque<Result<Response<Body>, DispatchError>>>,
    }

    impl QueueDispatch {
        fn new(outcomes: Vec<Result<Response<Body>, DispatchError>>) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                outcomes: Mutex::new(outcomes.into()),
            }
        }

        fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl UpstreamDispatch for QueueDispatch {
        async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.outcomes
                .lock()
                .expect("queue dispatch lock")
                .pop_front()
                .expect("dispatch called more times than scripted")
        }
    }

    fn upstream_response(status: StatusCode, body: &'static [u8]) -> Response<Body> {
        let mut response = Response::new(Body::from(Bytes::from_static(body)));
        *response.status_mut() = status;
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        response
    }

    /// Signer factory whose `build` always fails with a fixed error.
    struct FailingSignerFactory(fn() -> SignerError);

    impl ApiKeyAwareSignerFactory for FailingSignerFactory {
        fn with_router_choice(
            &self,
            _router_chosen_upstream_name: String,
        ) -> Arc<dyn SignerFactory> {
            Arc::new(Self(self.0))
        }
    }

    #[async_trait]
    impl SignerFactory for FailingSignerFactory {
        async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
            Err((self.0)())
        }
    }

    /// Signer whose `sign` always fails; `on_unauthorized` never refreshes.
    struct FailingSigner(fn() -> SignerError);

    #[async_trait]
    impl Signer for FailingSigner {
        async fn sign(
            &self,
            _shaped: ShapedRequest,
            _capability: &mut cc_lb_upstream::SigningCapability,
        ) -> Result<SignedRequest, SignerError> {
            Err((self.0)())
        }

        async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
            RetryDecision::Fail
        }
    }

    /// Signer factory that hands out `FailingSigner`.
    struct FailingSignerFactoryForAttempt(fn() -> SignerError);

    impl ApiKeyAwareSignerFactory for FailingSignerFactoryForAttempt {
        fn with_router_choice(
            &self,
            _router_chosen_upstream_name: String,
        ) -> Arc<dyn SignerFactory> {
            Arc::new(Self(self.0))
        }
    }

    #[async_trait]
    impl SignerFactory for FailingSignerFactoryForAttempt {
        async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
            Ok(Arc::new(FailingSigner(self.0)))
        }
    }

    /// Signer that always asks for a credential refresh on 401.
    struct RefreshingSigner;

    #[async_trait]
    impl Signer for RefreshingSigner {
        async fn sign(
            &self,
            shaped: ShapedRequest,
            capability: &mut cc_lb_upstream::SigningCapability,
        ) -> Result<SignedRequest, SignerError> {
            Ok(SignedRequest::from_shaped(shaped, capability))
        }

        async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
            RetryDecision::Refresh {
                new_signer: Arc::new(TestSigner),
            }
        }
    }

    /// Signer factory that hands out `RefreshingSigner`.
    struct RefreshingSignerFactory;

    impl ApiKeyAwareSignerFactory for RefreshingSignerFactory {
        fn with_router_choice(
            &self,
            _router_chosen_upstream_name: String,
        ) -> Arc<dyn SignerFactory> {
            Arc::new(Self)
        }
    }

    #[async_trait]
    impl SignerFactory for RefreshingSignerFactory {
        async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
            Ok(Arc::new(RefreshingSigner))
        }
    }

    /// Dialect whose shaping always fails, forcing the raw-passthrough fallback.
    struct FailingDialect;

    impl UpstreamDialect for FailingDialect {
        fn shape(
            &self,
            _context: &DialectShapeContext,
            _upstream: &Upstream,
            _principal: &Principal,
            _builder: &mut ShapedRequestBuilder,
        ) -> Result<ShapedRequest, DialectError> {
            Err(DialectError::UnsupportedRequest {
                reason: "injected dialect failure".to_owned(),
            })
        }
    }

    /// Affinity store whose resolve always fails.
    struct FailingAffinityStore;

    #[async_trait]
    impl UpstreamAffinityStore for FailingAffinityStore {
        async fn resolve_upstream_affinities(
            &self,
            _keys: &[UpstreamAffinityKey],
            _now_unix_secs: u64,
            _ttl_secs: u64,
        ) -> cc_lb_storage_api::StorageResult<Vec<UpstreamAffinityBinding>> {
            Err(cc_lb_storage_api::StorageError::Unavailable {
                message: "affinity store down".to_owned(),
            })
        }
        async fn read_upstream_owner_epoch(
            &self,
            _upstream_id: cc_lb_storage_api::UpstreamRecordId,
            _expected_spec_revision: u64,
        ) -> cc_lb_storage_api::StorageResult<Option<u64>> {
            Err(cc_lb_storage_api::StorageError::Unavailable {
                message: "affinity store down".to_owned(),
            })
        }

        async fn bind_upstream_affinities(
            &self,
            _bindings: &[UpstreamAffinityBinding],
            _now_unix_secs: u64,
            _ttl_secs: u64,
        ) -> cc_lb_storage_api::StorageResult<()> {
            Err(cc_lb_storage_api::StorageError::Unavailable {
                message: "affinity store down".to_owned(),
            })
        }

        async fn purge_expired_upstream_affinities(
            &self,
            _now_unix_secs: u64,
            _ttl_secs: u64,
            _batch_size: usize,
        ) -> cc_lb_storage_api::StorageResult<u64> {
            Err(cc_lb_storage_api::StorageError::Unavailable {
                message: "affinity store down".to_owned(),
            })
        }
    }

    /// Filter plugin that returns an upstream id outside the candidate set,
    /// tripping output validation and the invalid-output passthrough.
    struct InvalidOutputFilter;

    impl FilterPlugin for InvalidOutputFilter {
        fn filter(
            &self,
            _ctx: &cc_lb_routing::RoutingContext,
            _principal: &Principal,
            _candidates: &[UpstreamCandidate],
        ) -> Result<FilterOutput, FilterError> {
            Ok(FilterOutput {
                kept_upstream_ids: vec![Uuid::new_v4()],
                reason: "injected invalid output".to_owned(),
                per_candidate_reasons: Vec::new(),
                subscription_preference: None,
            })
        }

        fn plugin_id(&self) -> Uuid {
            Uuid::nil()
        }

        fn plugin_name(&self) -> &str {
            "invalid-output-filter"
        }
    }

    fn failure_principal_view(
        principal_id: &str,
        artifacts: cc_lb_control::api_keys::principal_view::PrincipalRoutingArtifacts,
    ) -> Arc<PrincipalView> {
        let mut chains: HashMap<
            String,
            cc_lb_control::api_keys::principal_view::PrincipalRoutingArtifacts,
        > = HashMap::new();
        chains.insert(principal_id.to_owned(), artifacts);
        Arc::new(PrincipalView::for_tests(
            principal_id,
            true,
            vec!["*".to_owned()],
            Vec::new(),
            chains,
        ))
    }

    fn limited_failure_principal_view(
        limit: cc_lb_storage_api::principal::Limit,
    ) -> Arc<PrincipalView> {
        Arc::new(PrincipalView::for_tests(
            "principal-test",
            true,
            vec!["*".to_owned()],
            vec![limit],
            HashMap::new(),
        ))
    }

    fn upstream_record_with_kind(kind: StorageRecordKind) -> UpstreamRecord {
        UpstreamRecord {
            id: Uuid::new_v4(),
            name: "upstream-test".to_owned(),
            kind,
            enabled: true,
            revision: 1,
            oauth_token_generation: 0,
            created_at_unix_secs: 0,
            updated_at_unix_secs: 0,
            ..UpstreamRecord::default()
        }
    }

    fn default_artifacts() -> cc_lb_control::api_keys::principal_view::PrincipalRoutingArtifacts {
        (
            None,
            cc_lb_control::api_keys::principal_view::DialectCache::Inherit,
        )
    }

    fn provenance_view(
        principal_view: Arc<PrincipalView>,
        signer_factory: Arc<dyn ApiKeyAwareSignerFactory>,
        upstreams: Vec<UpstreamRecord>,
    ) -> Arc<DynamicView> {
        DynamicViewBuilder::new(0)
            .signer_factory(signer_factory)
            .principal_view(principal_view)
            .upstream_records(upstreams)
            .build()
    }

    fn provenance_lifecycle(
        view: Arc<DynamicView>,
        dispatcher: Arc<dyn UpstreamDispatch>,
        bus: &Arc<cc_lb_control::event_bus::InMemoryBus>,
    ) -> Lifecycle {
        Lifecycle::new_with_dynamic_view(
            failure_authn(),
            Arc::new(DynamicViewHolder::new(view)),
            dispatcher,
            LifecycleConfig::default(),
            Arc::new(cc_lb_clock::SystemClock),
        )
        .with_event_bus(bus.clone() as Arc<dyn RequestEventBus>)
    }

    fn failure_request(body: Bytes) -> Request<Bytes> {
        Request::builder()
            .method(http::Method::POST)
            .uri("/v1/messages")
            .header("x-api-key", failure_key_fixture().plaintext.as_str())
            .header("anthropic-version", "2023-06-01")
            .body(body)
            .expect("failure request builds")
    }

    fn subscribe_lifecycle(
        bus: &Arc<cc_lb_control::event_bus::InMemoryBus>,
    ) -> tokio::sync::broadcast::Receiver<cc_lb_lifecycle::LifecycleEvent> {
        bus.subscribe_lifecycle()
    }

    fn drain_events(
        rx: &mut tokio::sync::broadcast::Receiver<cc_lb_lifecycle::LifecycleEvent>,
    ) -> Vec<cc_lb_lifecycle::LifecycleEvent> {
        let mut events = Vec::new();
        loop {
            match rx.try_recv() {
                Ok(event) => events.push(event),
                Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
                Err(tokio::sync::broadcast::error::TryRecvError::Closed) => break,
                Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => continue,
            }
        }
        events
    }

    struct TerminalSnapshot {
        reason: cc_lb_lifecycle::TerminationReason,
        client_status: u16,
        internal_errors: Vec<InternalError>,
        dns_ms: Option<u64>,
        connect_ms: Option<u64>,
        connection_reused: Option<bool>,
    }

    fn terminal_snapshot(events: &[cc_lb_lifecycle::LifecycleEvent]) -> TerminalSnapshot {
        for event in events {
            if let cc_lb_lifecycle::LifecycleEvent::RequestTerminated {
                reason,
                client_status,
                internal_errors,
                dns_ms,
                connect_ms,
                connection_reused,
                ..
            } = event
            {
                return TerminalSnapshot {
                    reason: reason.clone(),
                    client_status: *client_status,
                    internal_errors: internal_errors.clone(),
                    dns_ms: *dns_ms,
                    connect_ms: *connect_ms,
                    connection_reused: *connection_reused,
                };
            }
        }
        panic!("expected RequestTerminated in drained events");
    }

    fn expect_error_code(reason: &cc_lb_lifecycle::TerminationReason, expected: &str) {
        match reason {
            cc_lb_lifecycle::TerminationReason::ErrorCode(code) => {
                assert_eq!(code, expected, "unexpected terminal error code");
            }
            other => panic!("expected ErrorCode({expected}), got {other:?}"),
        }
    }

    async fn collect_response_body(response: Response<Body>) -> (StatusCode, Bytes) {
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("response body collects")
            .to_bytes();
        (status, body)
    }

    #[tokio::test]
    async fn scoped_unsupported_operations_use_provider_errors_and_one_terminal_event() {
        use cc_lb_domain::Provider;
        for (provider, path) in [
            (Provider::Anthropic, "/v1/files"),
            (Provider::Openai, "/v1/responses/resp_1/cancel"),
            (Provider::Gemini, "/v1beta/cachedContents/unknown"),
            (Provider::Anthropic, "/v1/responses"),
        ] {
            let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
            let mut rx = subscribe_lifecycle(&bus);
            let view = provenance_view(
                failure_principal_view("principal-test", default_artifacts()),
                Arc::new(TestSignerFactory),
                Vec::new(),
            );
            let dispatcher = Arc::new(QueueDispatch::new(Vec::new()));
            let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus);
            let mut request = failure_request(Bytes::from_static(b"{}"));
            *request.uri_mut() = path.parse().expect("path URI");
            request
                .extensions_mut()
                .insert(crate::request_context::ProviderScope(provider));
            let auth = lifecycle
                .authenticate(request.headers())
                .await
                .expect("authenticated");
            let response = lifecycle
                .handle(request, &auth)
                .await
                .expect("request handled");
            let (status, body) = collect_response_body(response).await;
            assert_eq!(status, StatusCode::NOT_FOUND);
            let body: Value = serde_json::from_slice(&body).expect("error JSON");
            assert_eq!(body["error"]["type"], "operation_not_supported");
            assert_eq!(
                body["error"]["message"],
                format!("POST {path} is not supported by cc-lb yet")
            );
            match provider {
                Provider::Anthropic => assert_eq!(body["type"], "error"),
                Provider::Openai => {
                    assert_eq!(body["error"]["code"], "operation_not_supported");
                    assert!(body["error"]["param"].is_null());
                    assert!(body.get("type").is_none());
                }
                Provider::Gemini => {
                    assert_eq!(body["error"]["code"], 404);
                    assert_eq!(body["error"]["status"], "NOT_FOUND");
                    assert!(body.get("type").is_none());
                }
            }
            assert_eq!(dispatcher.call_count(), 0);
            let events = drain_events(&mut rx);
            assert_eq!(
                events
                    .iter()
                    .filter(|event| matches!(
                        event,
                        cc_lb_lifecycle::LifecycleEvent::RequestTerminated { .. }
                    ))
                    .count(),
                1,
                "{provider:?} {path}"
            );
            assert_eq!(terminal_snapshot(&events).client_status, 404);
        }
    }

    #[tokio::test]
    async fn negative_generation_bounds_reject_before_provider_dispatch() {
        use cc_lb_domain::Provider;

        for (provider, path, body, upstream_kind) in [
            (
                Provider::Openai,
                "/v1/responses",
                br#"{"model":"gpt-test","max_output_tokens":-1,"input":[]}"#.as_slice(),
                StorageRecordKind::OpenaiApiKey,
            ),
            (
                Provider::Gemini,
                "/v1beta/models/gemini-test:generateContent",
                br#"{"contents":[],"generationConfig":{"maxOutputTokens":-1}}"#.as_slice(),
                StorageRecordKind::GeminiApiKey,
            ),
        ] {
            let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
            let dispatcher = Arc::new(QueueDispatch::new(Vec::new()));
            let view = provenance_view(
                limited_failure_principal_view(cc_lb_storage_api::principal::Limit {
                    kind: cc_lb_storage_api::principal::LimitKind::OutputTokens,
                    window_secs: 60,
                    cap_micros: 100,
                }),
                Arc::new(TestSignerFactory),
                vec![upstream_record_with_kind(upstream_kind)],
            );
            let limit_engine = cc_lb_control::api_keys::limit_engine::LimitEngine::new(
                Arc::new(cc_lb_control::api_keys::concurrent_guard::KeyConcurrencyManager::new()),
                Arc::new(cc_lb_clock::SystemClock),
            );
            let fixture = failure_key_fixture();
            let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus)
                .with_static_limit_subject(
                    limit_engine,
                    "principal-test".to_owned(),
                    fixture.key_id.clone(),
                    fixture.record.clone(),
                );
            let mut request = failure_request(Bytes::from_static(body));
            *request.uri_mut() = path.parse().expect("provider path");
            request
                .extensions_mut()
                .insert(crate::request_context::ProviderScope(provider));
            let auth = lifecycle
                .authenticate(request.headers())
                .await
                .expect("authenticated");
            let response = lifecycle
                .handle(request, &auth)
                .await
                .expect("request handled");
            let (status, body) = collect_response_body(response).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{provider:?}");
            let body: Value = serde_json::from_slice(&body).expect("error JSON");
            assert_eq!(body["error"]["type"], "invalid_request_error");
            assert!(
                body["error"]["message"]
                    .as_str()
                    .is_some_and(|message| message.contains("zero or greater"))
            );
            assert_eq!(dispatcher.call_count(), 0, "{provider:?}");
        }
    }

    #[tokio::test]
    async fn multipart_model_is_read_after_authentication_without_parsing_binary_as_json() {
        let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
        let view = provenance_view(
            failure_principal_view("principal-test", default_artifacts()),
            Arc::new(TestSignerFactory),
            Vec::new(),
        );
        let lifecycle = provenance_lifecycle(view, Arc::new(QueueDispatch::new(Vec::new())), &bus);
        let request = failure_request(Bytes::new());
        let auth = lifecycle
            .authenticate(request.headers())
            .await
            .expect("authenticated");
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("multipart/form-data; boundary=\"catalog-test\""),
        );
        let body = Bytes::from_static(
            b"--catalog-test\r\nContent-Disposition: form-data; name=\"file\"; filename=\"clip.wav\"\r\nContent-Type: audio/wav\r\n\r\n\x00\xffbinary\r\n--catalog-test\r\nContent-Disposition: form-data; name=\"model\"; filename=\"not-a-model\"\r\n\r\nignore-me\r\n--catalog-test\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\ngpt-4o-transcribe\r\n--catalog-test--\r\n"
        );
        assert_eq!(
            multipart_request_model(&auth, &headers, &body)
                .await
                .as_deref(),
            Some("gpt-4o-transcribe")
        );
    }

    /// QA-AUTH-03b: authentication succeeds, then the principal disappears
    /// from the view before `handle` resolves it.
    #[tokio::test]
    async fn post_auth_principal_missing_records_authn_unavailable() {
        let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
        let mut rx = subscribe_lifecycle(&bus);
        let upstream_id = Uuid::new_v4();
        let view_a = provenance_view(
            failure_principal_view("principal-test", default_artifacts()),
            Arc::new(TestSignerFactory),
            vec![upstream_record(upstream_id)],
        );
        let holder = Arc::new(DynamicViewHolder::new(view_a));
        let dispatcher = Arc::new(QueueDispatch::new(Vec::new()));
        let lifecycle = Lifecycle::new_with_dynamic_view(
            failure_authn(),
            holder.clone(),
            dispatcher.clone(),
            LifecycleConfig::default(),
            Arc::new(cc_lb_clock::SystemClock),
        )
        .with_event_bus(bus.clone() as Arc<dyn RequestEventBus>);

        let request = failure_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        ));
        let auth = lifecycle
            .authenticate(request.headers())
            .await
            .expect("request authenticates");
        // Swap the view so the authenticated principal is gone at handle time.
        holder.store(provenance_view(
            failure_principal_view("other-principal", default_artifacts()),
            Arc::new(TestSignerFactory),
            vec![upstream_record(upstream_id)],
        ));

        let response = lifecycle
            .handle(request, &auth)
            .await
            .expect("lifecycle handles request");
        let (status, body) = collect_response_body(response).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            body,
            anthropic_error_body("api_error", "authenticated principal is unavailable"),
            "client response must stay byte-identical"
        );
        assert_eq!(
            dispatcher.call_count(),
            0,
            "no dispatch on principal failure"
        );

        let events = drain_events(&mut rx);
        assert!(
            events.iter().any(|event| matches!(
                event,
                cc_lb_lifecycle::LifecycleEvent::AuthCompleted {
                    result: Err(cc_lb_lifecycle::AuthFailure::PrincipalMissing { .. }),
                    ..
                }
            )),
            "expected AuthCompleted::PrincipalMissing, got {events:?}"
        );
        let terminal = terminal_snapshot(&events);
        assert_eq!(terminal.client_status, 500);
        expect_error_code(&terminal.reason, error_codes::PRINCIPAL_MISSING);
        assert_eq!(terminal.internal_errors.len(), 1);
        assert_eq!(terminal.internal_errors[0].stage, InternalErrorStage::Authn);
        assert_eq!(
            terminal.internal_errors[0].kind,
            InternalErrorKind::Unavailable
        );
        assert_eq!(
            terminal.internal_errors[0].message.as_deref(),
            Some("authenticated principal is unavailable")
        );
    }

    /// QA-ROUTE-02: the resolved router pipeline carries an instantiation
    /// error, so routing cannot run at all.
    #[tokio::test]
    async fn router_pipeline_instantiation_error_records_router_config() {
        let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
        let mut rx = subscribe_lifecycle(&bus);
        let upstream_id = Uuid::new_v4();
        let pipeline = cc_lb_control::api_keys::principal_view::RouterPipelineCache {
            user_filters: Vec::new(),
            terminal: TerminalStrategy::FirstPick,
            instantiation_error: Some(Arc::from("pipeline exploded")),
            provider_filters: None,
        };
        let view = provenance_view(
            failure_principal_view(
                "principal-test",
                (
                    Some(Arc::new(pipeline)),
                    cc_lb_control::api_keys::principal_view::DialectCache::Inherit,
                ),
            ),
            Arc::new(TestSignerFactory),
            vec![upstream_record(upstream_id)],
        );
        let dispatcher = Arc::new(QueueDispatch::new(Vec::new()));
        let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus);

        let request = failure_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        ));
        let auth = lifecycle
            .authenticate(request.headers())
            .await
            .expect("request authenticates");
        let response = lifecycle
            .handle(request, &auth)
            .await
            .expect("lifecycle handles request");
        let (status, _body) = collect_response_body(response).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(dispatcher.call_count(), 0);

        let events = drain_events(&mut rx);
        assert!(
            events.iter().any(|event| matches!(
                event,
                cc_lb_lifecycle::LifecycleEvent::RouteCompleted {
                    result: Err(cc_lb_lifecycle::RouteFailure::RouterPipelineUnavailable),
                    ..
                }
            )),
            "expected RouteCompleted::RouterPipelineUnavailable, got {events:?}"
        );
        let terminal = terminal_snapshot(&events);
        assert_eq!(terminal.client_status, 502);
        expect_error_code(&terminal.reason, error_codes::ROUTER_PIPELINE_UNAVAILABLE);
        assert_eq!(terminal.internal_errors.len(), 1);
        assert_eq!(
            terminal.internal_errors[0].stage,
            InternalErrorStage::Router
        );
        assert_eq!(
            terminal.internal_errors[0].kind,
            InternalErrorKind::ConfigError
        );
        assert_eq!(
            terminal.internal_errors[0].message.as_deref(),
            Some("pipeline exploded")
        );
    }

    /// QA-ROUTE-04: a filter stage fails validation, candidates pass through,
    /// and the non-fatal diagnostic must precede the terminal dispatch cause.
    #[tokio::test]
    async fn filter_invalid_output_history_precedes_terminal_dispatch_cause() {
        let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
        let mut rx = subscribe_lifecycle(&bus);
        let upstream_id = Uuid::new_v4();
        let pipeline = cc_lb_control::api_keys::principal_view::RouterPipelineCache {
            user_filters: vec![Arc::new(InvalidOutputFilter)],
            terminal: TerminalStrategy::FirstPick,
            instantiation_error: None,
            provider_filters: None,
        };
        let view = provenance_view(
            failure_principal_view(
                "principal-test",
                (
                    Some(Arc::new(pipeline)),
                    cc_lb_control::api_keys::principal_view::DialectCache::Inherit,
                ),
            ),
            Arc::new(TestSignerFactory),
            vec![upstream_record(upstream_id)],
        );
        let dispatcher = Arc::new(QueueDispatch::new(vec![Err(DispatchError::Transport {
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "refused",
            )),
        })]));
        let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus);

        let request = failure_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        ));
        let auth = lifecycle
            .authenticate(request.headers())
            .await
            .expect("request authenticates");
        let response = lifecycle
            .handle(request, &auth)
            .await
            .expect("lifecycle handles request");
        let (status, _body) = collect_response_body(response).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(dispatcher.call_count(), 1);

        let events = drain_events(&mut rx);
        let terminal = terminal_snapshot(&events);
        expect_error_code(&terminal.reason, error_codes::UPSTREAM_DISPATCH_FAILED);
        assert_eq!(
            terminal.internal_errors.len(),
            2,
            "non-fatal filter diagnostic and terminal dispatch cause must both persist: {:?}",
            terminal.internal_errors
        );
        assert_eq!(
            terminal.internal_errors[0].stage,
            InternalErrorStage::RouterFilter
        );
        assert_eq!(
            terminal.internal_errors[0].kind,
            InternalErrorKind::InvalidOutput
        );
        assert_eq!(terminal.internal_errors[1].stage, InternalErrorStage::Relay);
    }

    /// QA-SIGN-01: the signer factory cannot build a signer for the route.
    #[tokio::test]
    async fn signer_factory_failure_records_signer_stage() {
        let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
        let mut rx = subscribe_lifecycle(&bus);
        let upstream_id = Uuid::new_v4();
        let view = provenance_view(
            failure_principal_view("principal-test", default_artifacts()),
            Arc::new(FailingSignerFactory(|| SignerError::MissingCredentials {
                reason: "no credential material".to_owned(),
            })),
            vec![upstream_record(upstream_id)],
        );
        let dispatcher = Arc::new(QueueDispatch::new(Vec::new()));
        let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus);

        let request = failure_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        ));
        let auth = lifecycle
            .authenticate(request.headers())
            .await
            .expect("request authenticates");
        let response = lifecycle
            .handle(request, &auth)
            .await
            .expect("lifecycle handles request");
        let (status, body) = collect_response_body(response).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(
            body,
            anthropic_error_body("api_error", "failed to prepare upstream credentials"),
            "client response must stay byte-identical"
        );
        assert_eq!(dispatcher.call_count(), 0);

        let events = drain_events(&mut rx);
        let terminal = terminal_snapshot(&events);
        assert_eq!(terminal.client_status, 502);
        expect_error_code(&terminal.reason, error_codes::SIGNER_FAILED);
        assert_eq!(terminal.internal_errors.len(), 1);
        assert_eq!(
            terminal.internal_errors[0].stage,
            InternalErrorStage::Signer
        );
        assert_eq!(
            terminal.internal_errors[0].kind,
            InternalErrorKind::ConfigError
        );
        assert_eq!(
            terminal.internal_errors[0].message.as_deref(),
            Some("missing credentials: no credential material")
        );
    }

    /// QA-SIGN-02: signing fails inside `attempt` after the factory succeeded.
    #[tokio::test]
    async fn attempt_sign_failure_records_signer_stage() {
        let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
        let mut rx = subscribe_lifecycle(&bus);
        let upstream_id = Uuid::new_v4();
        let view = provenance_view(
            failure_principal_view("principal-test", default_artifacts()),
            Arc::new(FailingSignerFactoryForAttempt(|| {
                SignerError::SigningFailed {
                    reason: "hmac exploded".to_owned(),
                }
            })),
            vec![upstream_record(upstream_id)],
        );
        let dispatcher = Arc::new(QueueDispatch::new(Vec::new()));
        let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus);

        let request = failure_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        ));
        let auth = lifecycle
            .authenticate(request.headers())
            .await
            .expect("request authenticates");
        let response = lifecycle
            .handle(request, &auth)
            .await
            .expect("lifecycle handles request");
        let (status, body) = collect_response_body(response).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(
            body,
            anthropic_error_body("api_error", "failed to sign upstream request"),
            "client response must stay byte-identical"
        );
        assert_eq!(dispatcher.call_count(), 0);

        let events = drain_events(&mut rx);
        let terminal = terminal_snapshot(&events);
        assert_eq!(terminal.client_status, 502);
        expect_error_code(&terminal.reason, error_codes::SIGNER_FAILED);
        assert_eq!(terminal.internal_errors.len(), 1);
        assert_eq!(
            terminal.internal_errors[0].stage,
            InternalErrorStage::Signer
        );
        assert_eq!(
            terminal.internal_errors[0].kind,
            InternalErrorKind::PluginError
        );
        assert_eq!(
            terminal.internal_errors[0].message.as_deref(),
            Some("signing failed: hmac exploded")
        );
    }

    #[test]
    fn sse_completion_follows_each_provider_terminal_event() {
        let message_stop = usage_parser::parse_sse_event(
            b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
        );
        let response_completed = usage_parser::parse_sse_event(
            b"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\"}}\n\n",
        );
        let response_incomplete = usage_parser::parse_sse_event(
            b"event: response.incomplete\ndata: {\"type\":\"response.incomplete\",\"response\":{\"id\":\"resp_1\",\"usage\":{\"input_tokens\":12,\"output_tokens\":7}}}\n\n",
        );
        let response_incomplete_named_without_type = usage_parser::parse_sse_event(
            b"event: response.incomplete\ndata: {\"response\":{\"id\":\"resp_1\",\"usage\":{\"input_tokens\":12,\"output_tokens\":7}}}\n\n",
        );
        let response_incomplete_data_only = usage_parser::parse_sse_event(
            b"data: {\"type\":\"response.incomplete\",\"response\":{\"id\":\"resp_1\",\"usage\":{\"input_tokens\":12,\"output_tokens\":7}}}\n\n",
        );
        let response_incomplete_malformed =
            usage_parser::parse_sse_event(b"event: response.incomplete\ndata: {not-json}\n\n");
        let response_incomplete_empty =
            usage_parser::parse_sse_event(b"event: response.incomplete\ndata:\n\n");
        let response_delta = usage_parser::parse_sse_event(
            b"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n",
        );
        let gemini_chunk = usage_parser::parse_sse_event(
            b"data: {\"candidates\":[{\"finishReason\":\"STOP\",\"content\":{\"parts\":[{\"text\":\"hi\",\"thoughtSignature\":\"sig\"}]}}]}\n\n",
        );

        assert!(sse_event_completes_response(
            ProviderCapability::AnthropicMessages,
            &message_stop,
            false,
        ));
        assert!(!sse_event_completes_response(
            ProviderCapability::AnthropicMessages,
            &response_completed,
            false,
        ));
        assert!(sse_event_completes_response(
            ProviderCapability::OpenaiResponses,
            &response_completed,
            false,
        ));
        assert!(sse_event_completes_response(
            ProviderCapability::OpenaiResponses,
            &response_incomplete,
            false,
        ));
        assert!(sse_event_completes_response(
            ProviderCapability::OpenaiResponses,
            &response_incomplete_named_without_type,
            false,
        ));
        assert!(sse_event_completes_response(
            ProviderCapability::OpenaiResponses,
            &response_incomplete_data_only,
            false,
        ));
        for malformed in [&response_incomplete_malformed, &response_incomplete_empty] {
            assert!(!sse_event_completes_response(
                ProviderCapability::OpenaiResponses,
                malformed,
                false,
            ));
        }
        for (event, expected) in [
            (&response_incomplete_named_without_type, true),
            (&response_incomplete_data_only, true),
            (&response_incomplete_malformed, false),
            (&response_incomplete_empty, false),
        ] {
            let mut semantic_message_stop_seen = false;
            let mut message_stop_at = None;
            observe_sse_terminal_event(
                ProviderCapability::OpenaiResponses,
                event,
                false,
                &mut semantic_message_stop_seen,
                &mut message_stop_at,
                Instant::now,
            );
            assert_eq!(semantic_message_stop_seen, expected);
        }
        assert!(!sse_event_completes_response(
            ProviderCapability::OpenaiResponses,
            &message_stop,
            false,
        ));
        assert!(!sse_event_completes_response(
            ProviderCapability::OpenaiResponses,
            &response_delta,
            false,
        ));
        // Gemini has no terminal event; pending keys bind at clean stream end.
        assert!(!sse_event_completes_response(
            ProviderCapability::GeminiGenerateContent,
            &gemini_chunk,
            false,
        ));
    }

    #[test]
    fn proxy_generated_stream_error_frames_use_selected_provider_envelope() {
        for (capability, forbidden, required) in [
            (
                ProviderCapability::AnthropicMessages,
                "\"code\"",
                "\"type\":\"error\"",
            ),
            (
                ProviderCapability::OpenaiResponses,
                "\"type\":\"error\"",
                "\"error\"",
            ),
            (
                ProviderCapability::GeminiGenerateContent,
                "\"type\":\"error\"",
                "\"error\"",
            ),
        ] {
            let frame = make_stream_error_frame(
                capability,
                UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                "synthetic relay failure",
                false,
            );
            let text = std::str::from_utf8(&frame).expect("SSE frame is UTF-8");
            assert!(text.starts_with("event: error\ndata: "));
            assert!(text.ends_with("\n\n"));
            assert!(text.contains(required), "{text}");
            assert!(!text.contains(forbidden), "{text}");
        }
    }

    #[test]
    fn usage_shape_completion_keeps_provider_and_payload_boundaries() {
        let done = parse_sse_event(b"data: [DONE]\n\n");
        let finished =
            parse_sse_event(b"data: {\"choices\":[{\"index\":0,\"finish_reason\":\"stop\"}]}\n\n");
        let unfinished =
            parse_sse_event(b"data: {\"choices\":[{\"index\":0,\"finish_reason\":null}]}\n\n");
        for capability in [
            ProviderCapability::OpenaiChatCompletions,
            ProviderCapability::OpenaiCompletions,
            ProviderCapability::GeminiOpenaiChatCompletions,
        ] {
            assert!(sse_event_completes_response(capability, &done, false));
            assert!(sse_event_completes_response(capability, &finished, false));
            assert!(!sse_event_completes_response(
                capability,
                &unfinished,
                false
            ));
            assert!(transformed_sse_completes_response(
                capability,
                b"data: [DONE]\n\n",
            ));
            assert!(!transformed_sse_completes_response(
                capability,
                b"data: [DONE]",
            ));
            assert!(!transformed_sse_completes_response(
                capability,
                b"data: {\"choices\":[{\"finish_reason\":null}]}\n\n",
            ));
        }
        for capability in [
            ProviderCapability::AnthropicMessages,
            ProviderCapability::OpenaiResponses,
            ProviderCapability::GeminiGenerateContent,
            ProviderCapability::OpenaiEmbeddings,
            ProviderCapability::GeminiEmbedContent,
        ] {
            assert!(!sse_event_completes_response(capability, &done, false));
            assert!(!sse_event_completes_response(capability, &finished, false));
        }
        for raw in [
            b"event: message_stop\ndata: {bad-json}\n\n".as_slice(),
            b"event: message_stop\n\n",
            b"event: message_stop\ndata: {\"type\":\"message_delta\"}\n\n",
        ] {
            assert!(!sse_event_completes_response(
                ProviderCapability::AnthropicMessages,
                &parse_sse_event(raw),
                true,
            ));
        }
        for raw in [
            b"event: response.completed\ndata: {bad-json}\n\n".as_slice(),
            b"event: response.completed\n\n",
        ] {
            assert!(!sse_event_completes_response(
                ProviderCapability::OpenaiResponses,
                &parse_sse_event(raw),
                false,
            ));
        }
        assert!(sse_event_completes_response(
            ProviderCapability::OpenaiResponses,
            &parse_sse_event(b"event: response.completed\ndata: {}\n\n"),
            false,
        ));
    }

    #[test]
    fn semantic_content_classifier_matches_provider_wire_shapes() {
        let anthropic = parse_sse_event(
            b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"\"}}\n\n",
        );
        assert!(is_semantic_content_frame(
            ProviderCapability::AnthropicMessages,
            &anthropic
        ));

        let chat_text = parse_sse_event(
            b"data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}},{\"delta\":{\"content\":null}}]}\n\n",
        );
        let chat_metadata =
            parse_sse_event(b"data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n");
        let chat_tool = parse_sse_event(
            b"data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"id\":\"x\"}]}}]}\n\n",
        );
        assert!(is_semantic_content_frame(
            ProviderCapability::OpenaiChatCompletions,
            &chat_text
        ));
        assert!(!is_semantic_content_frame(
            ProviderCapability::OpenaiChatCompletions,
            &chat_metadata
        ));
        assert!(!is_semantic_content_frame(
            ProviderCapability::GeminiOpenaiChatCompletions,
            &chat_tool
        ));

        let completion = parse_sse_event(b"data: {\"choices\":[{\"text\":\"legacy\"}]}\n\n");
        assert!(is_semantic_content_frame(
            ProviderCapability::OpenaiCompletions,
            &completion
        ));

        let response_text = parse_sse_event(
            b"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n",
        );
        let response_reasoning = parse_sse_event(
            b"event: response.reasoning.delta\ndata: {\"type\":\"response.reasoning.delta\",\"delta\":\"hidden\"}\n\n",
        );
        assert!(is_semantic_content_frame(
            ProviderCapability::OpenaiResponses,
            &response_text
        ));
        assert!(!is_semantic_content_frame(
            ProviderCapability::OpenaiResponses,
            &response_reasoning
        ));

        let gemini_text = parse_sse_event(
            b"data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"one\"},{\"text\":\"two\"}]}},{\"content\":{\"parts\":[{\"text\":\"three\"}]}}]}\n\n",
        );
        let gemini_oauth_text = parse_sse_event(
            b"data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"wrapped\"}]}}]}}\n\n",
        );
        let gemini_oauth_thought = parse_sse_event(
            b"data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"thought\":true,\"text\":\"hidden\"}]}}]}}\n\n",
        );
        let gemini_thought = parse_sse_event(
            b"data: {\"candidates\":[{\"content\":{\"parts\":[{\"thought\":true,\"text\":\"hidden\"}]}}]}\n\n",
        );
        assert!(is_semantic_content_frame(
            ProviderCapability::GeminiGenerateContent,
            &gemini_text
        ));
        assert!(is_semantic_content_frame(
            ProviderCapability::GeminiGenerateContent,
            &gemini_oauth_text
        ));
        assert!(!is_semantic_content_frame(
            ProviderCapability::GeminiGenerateContent,
            &gemini_oauth_thought
        ));
        assert!(!is_semantic_content_frame(
            ProviderCapability::GeminiGenerateContent,
            &gemini_thought
        ));
        assert!(!is_semantic_content_frame(
            ProviderCapability::GeminiOpenaiChatCompletions,
            &gemini_text
        ));
    }

    /// QA-STOR-01: the affinity store fails while resolving request keys.
    #[tokio::test]
    async fn affinity_store_failure_records_storage_stage() {
        let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
        let mut rx = subscribe_lifecycle(&bus);
        let upstream_id = Uuid::new_v4();
        let view = provenance_view(
            failure_principal_view("principal-test", default_artifacts()),
            Arc::new(TestSignerFactory),
            vec![upstream_record(upstream_id)],
        );
        let dispatcher = Arc::new(QueueDispatch::new(Vec::new()));
        let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus)
            .with_upstream_affinity_store(Arc::new(FailingAffinityStore));

        let request = failure_request(Bytes::from_static(
            br#"{"model":"claude-test","tools":[{"type":"web_search_20250305"}],"messages":[{"role":"user","content":[{"type":"web_search_tool_result","tool_use_id":"srvtoolu_1","content":[{"type":"web_search_result","encrypted_content":"opaque-ciphertext"}]}]}]}"#,
        ));
        let auth = lifecycle
            .authenticate(request.headers())
            .await
            .expect("request authenticates");
        let response = lifecycle
            .handle(request, &auth)
            .await
            .expect("lifecycle handles request");
        let (status, _body) = collect_response_body(response).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(dispatcher.call_count(), 0);

        let events = drain_events(&mut rx);
        let terminal = terminal_snapshot(&events);
        assert_eq!(terminal.client_status, 503);
        expect_error_code(&terminal.reason, error_codes::UPSTREAM_AFFINITY_UNAVAILABLE);
        assert_eq!(terminal.internal_errors.len(), 1);
        assert_eq!(
            terminal.internal_errors[0].stage,
            InternalErrorStage::Storage
        );
        assert_eq!(
            terminal.internal_errors[0].kind,
            InternalErrorKind::Unavailable
        );
        assert_eq!(
            terminal.internal_errors[0].message.as_deref(),
            Some(UPSTREAM_AFFINITY_UNAVAILABLE_MESSAGE)
        );
    }

    /// QA-RELAY-01/02: a typed transport source reaches the terminal snapshot
    /// with a classified relay cause, not a string bucket.
    #[tokio::test]
    async fn transport_failure_records_typed_relay_cause() {
        let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
        let mut rx = subscribe_lifecycle(&bus);
        let upstream_id = Uuid::new_v4();
        let view = provenance_view(
            failure_principal_view("principal-test", default_artifacts()),
            Arc::new(TestSignerFactory),
            vec![upstream_record(upstream_id)],
        );
        let dispatcher = Arc::new(QueueDispatch::new(vec![Err(DispatchError::Transport {
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "connection refused",
            )),
        })]));
        let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus);

        let request = failure_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        ));
        let auth = lifecycle
            .authenticate(request.headers())
            .await
            .expect("request authenticates");
        let response = lifecycle
            .handle(request, &auth)
            .await
            .expect("lifecycle handles request");
        let (status, body) = collect_response_body(response).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(
            body,
            anthropic_error_body("api_error", "upstream request failed"),
            "client response must stay byte-identical"
        );
        assert_eq!(dispatcher.call_count(), 1);

        let events = drain_events(&mut rx);
        let terminal = terminal_snapshot(&events);
        assert_eq!(terminal.client_status, 502);
        expect_error_code(&terminal.reason, error_codes::UPSTREAM_DISPATCH_FAILED);
        assert_eq!(terminal.internal_errors.len(), 1);
        assert_eq!(terminal.internal_errors[0].stage, InternalErrorStage::Relay);
        assert_eq!(
            terminal.internal_errors[0].kind,
            InternalErrorKind::Unavailable
        );
        let message = terminal.internal_errors[0]
            .message
            .as_deref()
            .expect("relay cause message");
        assert!(
            message.starts_with("connection_refused: "),
            "typed tier-2 classification must prefix the chain, got {message}"
        );
        assert!(
            message.contains("connection refused"),
            "typed source chain must be preserved, got {message}"
        );
        // Connection timings are carried on the terminal snapshot even though
        // no UpstreamResponseStarted was emitted; a mock dispatcher leaves
        // them unobserved (None).
    }

    /// QA-RELAY-05: a saturated bulkhead is a relay-stage unavailable failure
    /// with the Retry-After header preserved.
    #[tokio::test]
    async fn bulkhead_full_records_relay_unavailable() {
        let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
        let mut rx = subscribe_lifecycle(&bus);
        let upstream_id = Uuid::new_v4();
        let view = provenance_view(
            failure_principal_view("principal-test", default_artifacts()),
            Arc::new(TestSignerFactory),
            vec![upstream_record(upstream_id)],
        );
        let dispatcher = Arc::new(QueueDispatch::new(vec![Err(DispatchError::BulkheadFull {
            retry_after: Duration::from_secs(3),
        })]));
        let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus);

        let request = failure_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        ));
        let auth = lifecycle
            .authenticate(request.headers())
            .await
            .expect("request authenticates");
        let response = lifecycle
            .handle(request, &auth)
            .await
            .expect("lifecycle handles request");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Some("3"),
            "Retry-After must be preserved"
        );
        let (_status, body) = collect_response_body(response).await;
        assert_eq!(
            body,
            anthropic_error_body("overloaded_error", "upstream bulkhead queue is full")
        );

        let events = drain_events(&mut rx);
        let terminal = terminal_snapshot(&events);
        assert_eq!(terminal.client_status, 503);
        expect_error_code(&terminal.reason, error_codes::UPSTREAM_DISPATCH_FAILED);
        assert_eq!(terminal.internal_errors.len(), 1);
        assert_eq!(terminal.internal_errors[0].stage, InternalErrorStage::Relay);
        assert_eq!(
            terminal.internal_errors[0].kind,
            InternalErrorKind::Unavailable
        );
        assert!(
            terminal.internal_errors[0]
                .message
                .as_deref()
                .is_some_and(|message| message.contains("bulkhead queue full")),
            "bulkhead cause must be preserved: {:?}",
            terminal.internal_errors[0].message
        );
        // A pre-transport rejection never touched the connector: all
        // connection timings must be unobserved.
        assert_eq!(terminal.dns_ms, None);
        assert_eq!(terminal.connect_ms, None);
        assert_eq!(terminal.connection_reused, None);
    }

    /// QA-RETRY-01: a provider 401 refreshes the signer and retries; the 401
    /// itself is not an internal failure, and the second attempt's typed
    /// cause is the only terminal diagnostic.
    #[tokio::test]
    async fn retry_after_401_keeps_provider_error_out_of_internal_errors() {
        let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
        let mut rx = subscribe_lifecycle(&bus);
        let upstream_id = Uuid::new_v4();
        let view = provenance_view(
            failure_principal_view("principal-test", default_artifacts()),
            Arc::new(RefreshingSignerFactory),
            vec![upstream_record(upstream_id)],
        );
        let dispatcher = Arc::new(QueueDispatch::new(vec![
            Ok(upstream_response(
                StatusCode::UNAUTHORIZED,
                br#"{"type":"error","error":{"type":"authentication_error","message":"bad key"}}"#,
            )),
            Err(DispatchError::Transport {
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "connect timed out",
                )),
            }),
        ]));
        let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus);

        let request = failure_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        ));
        let auth = lifecycle
            .authenticate(request.headers())
            .await
            .expect("request authenticates");
        let response = lifecycle
            .handle(request, &auth)
            .await
            .expect("lifecycle handles request");
        let (status, _body) = collect_response_body(response).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(dispatcher.call_count(), 2, "401 must trigger one retry");

        let events = drain_events(&mut rx);
        let attempt_nums: Vec<u32> = events
            .iter()
            .filter_map(|event| match event {
                cc_lb_lifecycle::LifecycleEvent::UpstreamAttempt { attempt_num, .. } => {
                    Some(*attempt_num)
                }
                _ => None,
            })
            .collect();
        assert_eq!(attempt_nums, vec![1, 2], "both attempts must be emitted");
        let terminal = terminal_snapshot(&events);
        expect_error_code(&terminal.reason, error_codes::UPSTREAM_DISPATCH_FAILED);
        assert_eq!(
            terminal.internal_errors.len(),
            1,
            "provider 401 must not enter internal_errors: {:?}",
            terminal.internal_errors
        );
        assert_eq!(terminal.internal_errors[0].stage, InternalErrorStage::Relay);
        assert_eq!(terminal.internal_errors[0].kind, InternalErrorKind::Timeout);
        let message = terminal.internal_errors[0]
            .message
            .as_deref()
            .expect("relay cause message");
        assert!(
            message.starts_with("timeout: "),
            "typed tier-2 classification must prefix the chain, got {message}"
        );
    }

    /// QA-CTRL-09: a dialect shape failure falls back to raw passthrough and
    /// the request still succeeds; the non-fatal shape diagnostic persists.
    #[tokio::test]
    async fn shape_fallback_success_keeps_shape_diagnostic() {
        let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
        let mut rx = subscribe_lifecycle(&bus);
        let upstream_id = Uuid::new_v4();
        let view = provenance_view(
            failure_principal_view(
                "principal-test",
                (
                    None,
                    cc_lb_control::api_keys::principal_view::DialectCache::Explicit(
                        cc_lb_control::api_keys::principal_view::ShapePluginCache {
                            dialect: Arc::new(FailingDialect),
                        },
                    ),
                ),
            ),
            Arc::new(TestSignerFactory),
            vec![upstream_record(upstream_id)],
        );
        let dispatcher = Arc::new(QueueDispatch::new(vec![Ok(upstream_response(
            StatusCode::OK,
            br#"{"id":"msg_1","type":"message","role":"assistant","content":[],"model":"claude-test","stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}"#,
        ))]));
        let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus);

        let request = failure_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        ));
        let auth = lifecycle
            .authenticate(request.headers())
            .await
            .expect("request authenticates");
        let response = lifecycle
            .handle(request, &auth)
            .await
            .expect("lifecycle handles request");
        let (status, _body) = collect_response_body(response).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(dispatcher.call_count(), 1);

        let events = drain_events(&mut rx);
        let terminal = terminal_snapshot(&events);
        assert!(
            matches!(terminal.reason, cc_lb_lifecycle::TerminationReason::Success),
            "fallback success must terminate as success, got {:?}",
            terminal.reason
        );
        assert_eq!(terminal.internal_errors.len(), 1);
        assert_eq!(terminal.internal_errors[0].stage, InternalErrorStage::Shape);
        assert_eq!(terminal.internal_errors[0].kind, InternalErrorKind::Trap);
        assert!(
            terminal.internal_errors[0]
                .message
                .as_deref()
                .is_some_and(|message| message.contains("injected dialect failure")),
            "shape diagnostic must carry the dialect failure: {:?}",
            terminal.internal_errors[0].message
        );
    }

    /// QA-UPSTR-06 (negative gate): a 200 body containing an "error" key must
    /// not be classified as an upstream error.
    #[tokio::test]
    async fn success_body_with_error_key_is_not_upstream_error() {
        let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
        let mut rx = subscribe_lifecycle(&bus);
        let upstream_id = Uuid::new_v4();
        let view = provenance_view(
            failure_principal_view("principal-test", default_artifacts()),
            Arc::new(TestSignerFactory),
            vec![upstream_record(upstream_id)],
        );
        let dispatcher = Arc::new(QueueDispatch::new(vec![Ok(upstream_response(
            StatusCode::OK,
            br#"{"id":"msg_1","type":"message","role":"assistant","content":[],"model":"claude-test","stop_reason":"end_turn","error":{"type":"not_an_error"},"usage":{"input_tokens":1,"output_tokens":1}}"#,
        ))]));
        let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus);

        let request = failure_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        ));
        let auth = lifecycle
            .authenticate(request.headers())
            .await
            .expect("request authenticates");
        let response = lifecycle
            .handle(request, &auth)
            .await
            .expect("lifecycle handles request");
        let (status, _body) = collect_response_body(response).await;
        assert_eq!(status, StatusCode::OK);

        let events = drain_events(&mut rx);
        assert!(
            !events.iter().any(|event| matches!(
                event,
                cc_lb_lifecycle::LifecycleEvent::RequestLogUpstreamErrorObserved { .. }
            )),
            "error-shaped JSON inside a 200 must not emit upstream error observation"
        );
        let terminal = terminal_snapshot(&events);
        assert!(
            matches!(terminal.reason, cc_lb_lifecycle::TerminationReason::Success),
            "200 with error-shaped body must stay success, got {:?}",
            terminal.reason
        );
        assert!(terminal.internal_errors.is_empty());
    }

    /// QA-AUTH-01/05: `reject_unauthenticated`
    /// persists the exact BuiltinAuthError Display literal with the typed
    /// stage/kind.
    #[tokio::test]
    async fn reject_unauthenticated_records_exact_auth_error() {
        for (error, expected_status, expected_kind) in [
            (
                BuiltinAuthError::InvalidFormat,
                StatusCode::UNAUTHORIZED,
                InternalErrorKind::InvalidInput,
            ),
            (
                BuiltinAuthError::Unavailable,
                StatusCode::SERVICE_UNAVAILABLE,
                InternalErrorKind::Unavailable,
            ),
        ] {
            let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
            let mut rx = subscribe_lifecycle(&bus);
            let clock: ClockHandle = Arc::new(cc_lb_clock::SystemClock);
            let observer = LifecycleContext::new(
                "req-authn-reject".to_owned(),
                bus.clone() as Arc<dyn RequestEventBus>,
                &clock,
            );
            observer.mark_authn_reached();
            let response = crate::authn_rail::reject_unauthenticated(&error, Some(&observer));
            assert_eq!(response.status(), expected_status);
            let (_status, body) = collect_response_body(response).await;
            assert_eq!(
                body,
                anthropic_error_body("authentication_error", &error.to_string()),
                "client response must stay byte-identical"
            );
            drop(observer);

            let events = drain_events(&mut rx);
            let started_pos = events
                .iter()
                .position(|event| {
                    matches!(
                        event,
                        cc_lb_lifecycle::LifecycleEvent::RequestStarted { .. }
                    )
                })
                .expect("RequestStarted must be emitted by terminate_failure");
            let auth_pos = events
                .iter()
                .position(|event| {
                    matches!(
                        event,
                        cc_lb_lifecycle::LifecycleEvent::AuthCompleted {
                            result: Err(cc_lb_lifecycle::AuthFailure::AuthenticationFailed { .. }),
                            ..
                        }
                    )
                })
                .expect("expected AuthCompleted::AuthenticationFailed");
            let terminated_pos = events
                .iter()
                .position(|event| {
                    matches!(
                        event,
                        cc_lb_lifecycle::LifecycleEvent::RequestTerminated { .. }
                    )
                })
                .expect("RequestTerminated must be emitted");
            assert!(
                started_pos < auth_pos && auth_pos < terminated_pos,
                "event order must be RequestStarted < AuthCompleted < RequestTerminated: {events:?}"
            );
            let terminal = terminal_snapshot(&events);
            assert_eq!(terminal.client_status, expected_status.as_u16());
            expect_error_code(&terminal.reason, error_codes::AUTHENTICATION_FAILED);
            assert_eq!(terminal.internal_errors.len(), 1);
            assert_eq!(terminal.internal_errors[0].stage, InternalErrorStage::Authn);
            assert_eq!(terminal.internal_errors[0].kind, expected_kind);
            assert_eq!(
                terminal.internal_errors[0].message.as_deref(),
                Some(error.to_string().as_str()),
                "exact BuiltinAuthError Display literal must persist"
            );
        }
    }

    /// QA-CTRL-12: a termination committed before `mark_authn_reached`
    /// publishes nothing — the pre-authn silence invariant.
    #[tokio::test]
    async fn pre_authn_termination_publishes_nothing() {
        let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
        let mut rx = subscribe_lifecycle(&bus);
        let clock: ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        {
            let observer = LifecycleContext::new(
                "req-pre-authn".to_owned(),
                bus.clone() as Arc<dyn RequestEventBus>,
                &clock,
            );
            observer.terminate_failure(InternalFailure {
                status: StatusCode::GATEWAY_TIMEOUT,
                error_code: error_codes::TOWER_TIMEOUT,
                error: InternalError {
                    stage: InternalErrorStage::Relay,
                    kind: InternalErrorKind::Timeout,
                    message: Some("pre-authn timeout".to_owned()),
                },
            });
        }
        let events = drain_events(&mut rx);
        assert!(
            events.is_empty(),
            "pre-authn termination must publish no events, got {events:?}"
        );
    }

    /// QA-CTRL-11: a terminal committed without a prior explicit
    /// `emit_request_started` still emits RequestStarted before
    /// RequestTerminated — the orphan-terminal policy input.
    #[tokio::test]
    async fn orphan_terminal_emits_started_then_terminated() {
        let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
        let mut rx = subscribe_lifecycle(&bus);
        let clock: ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        {
            let observer = LifecycleContext::new(
                "req-orphan".to_owned(),
                bus.clone() as Arc<dyn RequestEventBus>,
                &clock,
            );
            observer.mark_authn_reached();
            observer.terminate_failure(InternalFailure {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                error_code: error_codes::PRINCIPAL_MISSING,
                error: InternalError {
                    stage: InternalErrorStage::Authn,
                    kind: InternalErrorKind::Unavailable,
                    message: Some("orphan terminal".to_owned()),
                },
            });
        }
        let events = drain_events(&mut rx);
        let started_pos = events
            .iter()
            .position(|event| {
                matches!(
                    event,
                    cc_lb_lifecycle::LifecycleEvent::RequestStarted { .. }
                )
            })
            .expect("RequestStarted must be emitted by terminate_failure");
        let terminated_pos = events
            .iter()
            .position(|event| {
                matches!(
                    event,
                    cc_lb_lifecycle::LifecycleEvent::RequestTerminated { .. }
                )
            })
            .expect("RequestTerminated must be emitted");
        assert!(
            started_pos < terminated_pos,
            "RequestStarted must precede RequestTerminated: {events:?}"
        );
        let terminal = terminal_snapshot(&events);
        expect_error_code(&terminal.reason, error_codes::PRINCIPAL_MISSING);
        assert_eq!(terminal.internal_errors.len(), 1);
    }

    /// QA-RELAY-04: a connect timeout is a relay-stage timeout failure with
    /// the typed tier-2 classification preserved.
    #[tokio::test]
    async fn transport_timeout_records_relay_timeout() {
        let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
        let mut rx = subscribe_lifecycle(&bus);
        let upstream_id = Uuid::new_v4();
        let view = provenance_view(
            failure_principal_view("principal-test", default_artifacts()),
            Arc::new(TestSignerFactory),
            vec![upstream_record(upstream_id)],
        );
        let dispatcher = Arc::new(QueueDispatch::new(vec![Err(DispatchError::Transport {
            source: Box::new(std::io::Error::from(std::io::ErrorKind::TimedOut)),
        })]));
        let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus);

        let request = failure_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        ));
        let auth = lifecycle
            .authenticate(request.headers())
            .await
            .expect("request authenticates");
        let response = lifecycle
            .handle(request, &auth)
            .await
            .expect("lifecycle handles request");
        let (status, _body) = collect_response_body(response).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(dispatcher.call_count(), 1);

        let events = drain_events(&mut rx);
        let terminal = terminal_snapshot(&events);
        assert_eq!(terminal.client_status, 502);
        expect_error_code(&terminal.reason, error_codes::UPSTREAM_DISPATCH_FAILED);
        assert_eq!(terminal.internal_errors.len(), 1);
        assert_eq!(terminal.internal_errors[0].stage, InternalErrorStage::Relay);
        assert_eq!(terminal.internal_errors[0].kind, InternalErrorKind::Timeout);
        let message = terminal.internal_errors[0]
            .message
            .as_deref()
            .expect("relay cause message");
        assert!(
            message.starts_with("timeout: "),
            "typed tier-2 classification must prefix the chain, got {message}"
        );
    }

    /// QA-RELAY-06: invalid-URI and request-build dispatch failures are
    /// relay-stage config errors carrying their own reasons.
    #[tokio::test]
    async fn invalid_uri_and_request_build_record_relay_config_error() {
        for (source, expected_fragment) in [
            (
                DispatchError::InvalidUri {
                    reason: "bad scheme".to_owned(),
                },
                "invalid upstream uri: bad scheme",
            ),
            (
                DispatchError::RequestBuild {
                    reason: "header rejected".to_owned(),
                },
                "upstream request build failed: header rejected",
            ),
        ] {
            let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
            let mut rx = subscribe_lifecycle(&bus);
            let upstream_id = Uuid::new_v4();
            let view = provenance_view(
                failure_principal_view("principal-test", default_artifacts()),
                Arc::new(TestSignerFactory),
                vec![upstream_record(upstream_id)],
            );
            let dispatcher = Arc::new(QueueDispatch::new(vec![Err(source)]));
            let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus);

            let request = failure_request(Bytes::from_static(
                br#"{"model":"claude-test","messages":[]}"#,
            ));
            let auth = lifecycle
                .authenticate(request.headers())
                .await
                .expect("request authenticates");
            let response = lifecycle
                .handle(request, &auth)
                .await
                .expect("lifecycle handles request");
            let (status, body) = collect_response_body(response).await;
            assert_eq!(status, StatusCode::BAD_GATEWAY);
            assert_eq!(
                body,
                anthropic_error_body("api_error", "upstream request failed"),
                "client response must stay byte-identical"
            );
            assert_eq!(dispatcher.call_count(), 1);

            let events = drain_events(&mut rx);
            let terminal = terminal_snapshot(&events);
            assert_eq!(terminal.client_status, 502);
            expect_error_code(&terminal.reason, error_codes::UPSTREAM_DISPATCH_FAILED);
            assert_eq!(terminal.internal_errors.len(), 1);
            assert_eq!(terminal.internal_errors[0].stage, InternalErrorStage::Relay);
            assert_eq!(
                terminal.internal_errors[0].kind,
                InternalErrorKind::ConfigError
            );
            assert_eq!(
                terminal.internal_errors[0].message.as_deref(),
                Some(expected_fragment),
                "dispatch variant reason must be preserved"
            );
        }
    }

    /// WS-RED-01: WebSocket setup must honor the resolved terminal strategy
    /// instead of silently falling back to list order.
    #[tokio::test]
    async fn responses_websocket_uses_resolved_terminal_strategy() {
        let pipeline = cc_lb_control::api_keys::principal_view::RouterPipelineCache {
            user_filters: Vec::new(),
            terminal: TerminalStrategy::Random,
            instantiation_error: None,
            provider_filters: None,
        };
        let first_id = Uuid::from_u128(1);
        let second_id = Uuid::from_u128(2);
        let records = vec![
            UpstreamRecord {
                id: first_id,
                name: "first-openai".to_owned(),
                kind: StorageRecordKind::OpenaiApiKey,
                base_url: Some(Url::parse("http://first.invalid/").expect("test URL parses")),
                enabled: true,
                revision: 1,
                ..UpstreamRecord::default()
            },
            UpstreamRecord {
                id: second_id,
                name: "second-openai".to_owned(),
                kind: StorageRecordKind::OpenaiApiKey,
                base_url: Some(Url::parse("http://second.invalid/").expect("test URL parses")),
                enabled: true,
                revision: 1,
                ..UpstreamRecord::default()
            },
        ];
        let view = provenance_view(
            failure_principal_view(
                "principal-test",
                (
                    Some(Arc::new(pipeline)),
                    cc_lb_control::api_keys::principal_view::DialectCache::Inherit,
                ),
            ),
            Arc::new(TestSignerFactory),
            records,
        );
        let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
        let dispatcher = Arc::new(QueueDispatch::new(Vec::new()));
        let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus);
        let mut events = subscribe_lifecycle(&bus);
        let request = failure_request(Bytes::new());
        let auth = lifecycle
            .authenticate(request.headers())
            .await
            .expect("request authenticates");
        let auth_info = lifecycle.websocket_auth_info(&auth);
        assert_eq!(auth_info.principal_id, auth.principal_id());
        assert_eq!(auth_info.key_id.as_deref(), Some(auth.key_id()));
        assert_eq!(auth_info.auth_ms, Some(auth.auth_ms()));
        assert_eq!(auth_info.principal_kind.as_deref(), Some("machine"));

        // Pick a fixed seed whose first Random draw is the second candidate,
        // then install that same seed in the lifecycle's private test state.
        // This keeps the assertion deterministic without adding a test API.
        let seed = (0_u64..1024)
            .find(|seed| {
                let mut rng = StdRng::seed_from_u64(*seed);
                rng.random_range(0..2) == 1
            })
            .expect("a deterministic seed must select the second candidate");
        *lifecycle.rng.lock().expect("terminal rng lock") = StdRng::seed_from_u64(seed);

        let body = Arc::new(json!({"model": "gpt-test", "store": false}));
        let frame = Bytes::from(serde_json::to_vec(body.as_ref()).expect("test frame serializes"));
        let (observer, request_id) = lifecycle.websocket_observer("/v1/responses");
        assert!(
            drain_events(&mut events).is_empty(),
            "allocating an observer is not a turn"
        );
        let turn = lifecycle
            .prepare_websocket_turn(
                &auth,
                WebSocketIngress {
                    headers: request.headers(),
                    frame: &frame,
                    query: None,
                },
                body.clone(),
                None,
                observer,
                &request_id,
            )
            .await
            .expect("an eligible OpenAI upstream should be selected");
        assert_eq!(
            turn.candidates[0].upstream_id, second_id,
            "the resolved Random strategy is honored"
        );
        assert_eq!(
            turn.candidates[1].upstream_id, first_id,
            "unattempted fallback candidates remain available"
        );
        assert!(!turn.store);
        let (url, _) = lifecycle
            .sign_websocket_connection(&auth, request.headers(), &turn.candidates[0], None)
            .await
            .expect("only the selected connection is signed");
        assert_eq!(url.as_str(), "ws://second.invalid/v1/responses");
        let first_events = drain_events(&mut events);
        assert_eq!(
            first_events
                .iter()
                .filter(|event| matches!(
                    event,
                    cc_lb_lifecycle::LifecycleEvent::RequestStarted { .. }
                ))
                .count(),
            1
        );
        assert_eq!(
            first_events
                .iter()
                .filter(|event| matches!(
                    event,
                    cc_lb_lifecycle::LifecycleEvent::AuthCompleted { .. }
                ))
                .count(),
            0,
            "authentication observations belong to the gateway, not turn preparation"
        );
        assert_eq!(
            first_events
                .iter()
                .filter(|event| matches!(
                    event,
                    cc_lb_lifecycle::LifecycleEvent::RouteCompleted { .. }
                ))
                .count(),
            1
        );
        assert_eq!(
            first_events
                .iter()
                .filter(|event| matches!(
                    event,
                    cc_lb_lifecycle::LifecycleEvent::LimitDecision { .. }
                ))
                .count(),
            1
        );
        assert!(first_events.iter().any(|event| matches!(event,
            cc_lb_lifecycle::LifecycleEvent::ParseCompleted { result: Ok(info), .. }
                if info.method == "GET" && info.path == "/v1/responses" && info.stream && info.body_bytes == frame.len() as u64
        )));
        let (observer, next_request_id) = lifecycle.websocket_observer("/v1/responses");
        assert_ne!(request_id, next_request_id);
        let local_body = Arc::new(
            json!({"model": "gpt-test", "store": false, "previous_response_id": "resp_local"}),
        );
        let local_frame =
            Bytes::from(serde_json::to_vec(local_body.as_ref()).expect("test frame serializes"));
        let pinned = lifecycle
            .prepare_websocket_turn(
                &auth,
                WebSocketIngress {
                    headers: request.headers(),
                    frame: &local_frame,
                    query: None,
                },
                local_body.clone(),
                Some(first_id),
                observer,
                &next_request_id,
            )
            .await
            .expect("a locally owned store:false response does not require durable lookup");
        assert_eq!(
            pinned.candidates.len(),
            1,
            "owner-constrained turns cannot fall back"
        );
        assert_eq!(pinned.candidates[0].upstream_id, first_id);
        drain_events(&mut events);
        lifecycle
            .validate_websocket_turn(
                &auth,
                WebSocketIngress {
                    headers: request.headers(),
                    frame: &local_frame,
                    query: None,
                },
                local_body.as_ref(),
                first_id,
            )
            .await
            .expect("retained connection is still eligible");
        assert!(
            drain_events(&mut events).is_empty(),
            "retained validation emits no extra lifecycle events"
        );
        lifecycle
            .bind_websocket_response(&auth, first_id, &json!({"id":"resp_local"}), false)
            .await
            .expect("store:false response ids stay local to the gateway");
        assert!(
            lifecycle
                .bind_websocket_response(
                    &auth,
                    first_id,
                    &json!({"id":"resp_local","conversation":{"id":"conv_durable"}}),
                    false,
                )
                .await
                .is_err(),
            "store:false conversations still require durable affinity storage"
        );
        assert!(
            lifecycle
                .bind_websocket_response(&auth, first_id, &json!({"id":"resp_stored"}), true)
                .await
                .is_err(),
            "stored response state fails closed without durable affinity storage"
        );
        let records = lifecycle.dynamic_view.load().upstreams_snapshot().to_vec();
        lifecycle.dynamic_view.store(provenance_view(
            Arc::new(PrincipalView::for_tests(
                "principal-test",
                false,
                vec!["*".to_owned()],
                Vec::new(),
                HashMap::new(),
            )),
            Arc::new(TestSignerFactory),
            records.clone(),
        ));
        assert!(
            lifecycle
                .validate_websocket_turn(
                    &auth,
                    WebSocketIngress {
                        headers: request.headers(),
                        frame: &frame,
                        query: None,
                    },
                    body.as_ref(),
                    first_id,
                )
                .await
                .is_err(),
            "a retained connection cannot bypass a disabled principal"
        );
        lifecycle.dynamic_view.store(provenance_view(
            Arc::new(PrincipalView::for_tests(
                "principal-test",
                true,
                vec!["other-model".to_owned()],
                Vec::new(),
                HashMap::new(),
            )),
            Arc::new(TestSignerFactory),
            records,
        ));
        let error = lifecycle
            .validate_websocket_turn(
                &auth,
                WebSocketIngress {
                    headers: request.headers(),
                    frame: &frame,
                    query: None,
                },
                body.as_ref(),
                first_id,
            )
            .await
            .expect_err("a retained connection cannot bypass a changed model allowlist");
        assert_eq!(error.status, 403);
        lifecycle.dynamic_view.store(provenance_view(
            failure_principal_view("other-principal", default_artifacts()),
            Arc::new(TestSignerFactory),
            Vec::new(),
        ));
        let missing_info = lifecycle.websocket_auth_info(&auth);
        assert_eq!(missing_info.principal_id, auth.principal_id());
        assert_eq!(missing_info.key_id.as_deref(), Some(auth.key_id()));
        assert_eq!(missing_info.auth_ms, Some(auth.auth_ms()));
        assert_eq!(
            missing_info.principal_kind, None,
            "a missing principal kind is never fabricated"
        );
        assert_eq!(
            dispatcher.call_count(),
            0,
            "WebSocket helpers never use HTTP dispatch"
        );
    }

    struct WebSocketEligibilityFilter {
        allow: AtomicBool,
        seen: Mutex<Vec<Vec<Uuid>>>,
        frame: Bytes,
    }

    impl FilterPlugin for WebSocketEligibilityFilter {
        fn filter(
            &self,
            ctx: &cc_lb_routing::RoutingContext,
            _principal: &Principal,
            candidates: &[UpstreamCandidate],
        ) -> Result<FilterOutput, FilterError> {
            assert_eq!(ctx.method, http::Method::GET);
            assert_eq!(ctx.path, "/v1/responses");
            assert_eq!(ctx.canonical_model_id, "gpt-test");
            assert_eq!(
                ctx.body_bytes, self.frame,
                "filters receive original frame whitespace and key order"
            );
            assert_eq!(
                ctx.body_bytes.as_ptr(),
                self.frame.as_ptr(),
                "routing shares the original frame allocation"
            );
            self.seen.lock().expect("filter observations").push(
                candidates
                    .iter()
                    .map(|candidate| candidate.upstream_id)
                    .collect(),
            );
            Ok(FilterOutput {
                kept_upstream_ids: candidates
                    .iter()
                    .filter(|_| self.allow.load(Ordering::SeqCst))
                    .map(|candidate| candidate.upstream_id)
                    .collect(),
                reason: "current WebSocket eligibility".to_owned(),
                per_candidate_reasons: Vec::new(),
                subscription_preference: None,
            })
        }

        fn plugin_id(&self) -> Uuid {
            Uuid::from_u128(13_084)
        }
        fn plugin_name(&self) -> &str {
            "websocket-eligibility"
        }
    }

    #[tokio::test]
    async fn responses_websocket_applies_filters_after_owner_constraint_and_revalidates() {
        let owner = Uuid::from_u128(71);
        let frame = Bytes::from_static(br#"{ "store" : false, "model" : "gpt-test" }"#);
        let filter = Arc::new(WebSocketEligibilityFilter {
            allow: AtomicBool::new(true),
            seen: Mutex::new(Vec::new()),
            frame: frame.clone(),
        });
        let pipeline = RouterPipelineCache {
            user_filters: vec![filter.clone()],
            terminal: TerminalStrategy::FirstPick,
            instantiation_error: None,
            provider_filters: None,
        };
        let records = [owner, Uuid::from_u128(72)]
            .into_iter()
            .map(|id| UpstreamRecord {
                id,
                name: format!("openai-{id}"),
                kind: StorageRecordKind::OpenaiApiKey,
                enabled: true,
                ..Default::default()
            })
            .collect();
        let view = provenance_view(
            failure_principal_view(
                "principal-test",
                (
                    Some(Arc::new(pipeline)),
                    cc_lb_control::api_keys::principal_view::DialectCache::Inherit,
                ),
            ),
            Arc::new(TestSignerFactory),
            records,
        );
        let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
        let dispatcher = Arc::new(QueueDispatch::new(Vec::new()));
        let lifecycle = provenance_lifecycle(view, dispatcher.clone(), &bus);
        let request = failure_request(Bytes::new());
        let auth = lifecycle
            .authenticate(request.headers())
            .await
            .expect("request authenticates");
        let body =
            Arc::new(serde_json::from_slice::<Value>(&frame).expect("original frame parses"));
        let (observer, request_id) = lifecycle.websocket_observer("/v1/responses");
        let turn = lifecycle
            .prepare_websocket_turn(
                &auth,
                WebSocketIngress {
                    headers: request.headers(),
                    frame: &frame,
                    query: None,
                },
                body.clone(),
                Some(owner),
                observer,
                &request_id,
            )
            .await
            .expect("owner passes the current filters");
        assert_eq!(turn.candidates.len(), 1);
        assert_eq!(
            *filter.seen.lock().expect("filter observations"),
            vec![vec![owner]]
        );
        filter.allow.store(false, Ordering::SeqCst);
        let mut events = subscribe_lifecycle(&bus);
        let error = lifecycle
            .validate_websocket_turn(
                &auth,
                WebSocketIngress {
                    headers: request.headers(),
                    frame: &frame,
                    query: None,
                },
                body.as_ref(),
                owner,
            )
            .await
            .expect_err("a retained connection does not bypass changed filter eligibility");
        assert_eq!(error.code, error_codes::UPSTREAM_AFFINITY_UNAVAILABLE);
        assert!(drain_events(&mut events).is_empty());
        let (observer, request_id) = lifecycle.websocket_observer("/v1/responses");
        assert!(
            lifecycle
                .prepare_websocket_turn(
                    &auth,
                    WebSocketIngress {
                        headers: request.headers(),
                        frame: &frame,
                        query: None,
                    },
                    body,
                    Some(owner),
                    observer,
                    &request_id,
                )
                .await
                .is_err(),
            "an excluded owner cannot fall back"
        );
        let rejected = drain_events(&mut events);
        assert_eq!(
            rejected
                .iter()
                .filter(|event| matches!(
                    event,
                    cc_lb_lifecycle::LifecycleEvent::RequestTerminated { .. }
                ))
                .count(),
            1,
            "a failed create terminates exactly once"
        );
        assert_eq!(dispatcher.call_count(), 0);
    }

    #[tokio::test]
    async fn responses_websocket_signs_native_codex_connection_urls() {
        for (base_url, expected) in [
            (None, "wss://chatgpt.com/backend-api/codex/responses"),
            (
                Some("http://gateway.invalid/codex-proxy/?stale=1"),
                "ws://gateway.invalid/codex-proxy/responses",
            ),
        ] {
            let id = Uuid::from_u128(73);
            let view = provenance_view(
                failure_principal_view("principal-test", default_artifacts()),
                Arc::new(TestSignerFactory),
                vec![UpstreamRecord {
                    id,
                    name: "codex-native".to_owned(),
                    kind: StorageRecordKind::OpenaiOauth,
                    base_url: base_url.map(|value| Url::parse(value).expect("test URL parses")),
                    enabled: true,
                    ..Default::default()
                }],
            );
            let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
            let lifecycle =
                provenance_lifecycle(view, Arc::new(QueueDispatch::new(Vec::new())), &bus);
            let request = failure_request(Bytes::new());
            let auth = lifecycle
                .authenticate(request.headers())
                .await
                .expect("request authenticates");
            let body = Arc::new(json!({"model":"gpt-test","store":false}));
            let frame =
                Bytes::from(serde_json::to_vec(body.as_ref()).expect("test frame serializes"));
            let (observer, request_id) = lifecycle.websocket_observer("/v1/responses");
            let prepared = lifecycle
                .prepare_websocket_turn(
                    &auth,
                    WebSocketIngress {
                        headers: request.headers(),
                        frame: &frame,
                        query: None,
                    },
                    body,
                    None,
                    observer,
                    &request_id,
                )
                .await
                .expect("Codex candidate is eligible");
            for query in [None, Some("trace=a%2Fb&beta=1")] {
                let (url, _) = lifecycle
                    .sign_websocket_connection(
                        &auth,
                        request.headers(),
                        &prepared.candidates[0],
                        query,
                    )
                    .await
                    .expect("Codex connection signs");
                let expected = query.map_or_else(
                    || expected.to_owned(),
                    |query| format!("{expected}?{query}"),
                );
                assert_eq!(url.as_str(), expected);
            }
            let records = lifecycle.dynamic_view.load().upstreams_snapshot().to_vec();
            lifecycle.dynamic_view.store(provenance_view(
                failure_principal_view("principal-test", default_artifacts()),
                Arc::new(FailingSignerFactory(|| SignerError::MissingCredentials {
                    reason: "rotated credential is unavailable".to_owned(),
                })),
                records,
            ));
            let (observer, request_id) = lifecycle.websocket_observer("/v1/responses");
            let turn = lifecycle
                .prepare_websocket_turn(
                    &auth,
                    WebSocketIngress {
                        headers: request.headers(),
                        frame: &frame,
                        query: None,
                    },
                    Arc::new(json!({"model":"gpt-test","store":false})),
                    None,
                    observer,
                    &request_id,
                )
                .await
                .expect("turn preparation does not sign any connection");
            assert!(
                lifecycle
                    .sign_websocket_connection(&auth, request.headers(), &turn.candidates[0], None)
                    .await
                    .is_err(),
                "opening a new connection uses the fresh signer factory, not cached credentials"
            );
        }
    }

    #[tokio::test]
    async fn responses_websocket_authentication_rejection_emits_one_terminal() {
        let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
        let view = provenance_view(
            failure_principal_view("principal-test", default_artifacts()),
            Arc::new(TestSignerFactory),
            Vec::new(),
        );
        let lifecycle = provenance_lifecycle(view, Arc::new(QueueDispatch::new(Vec::new())), &bus);
        let mut events = subscribe_lifecycle(&bus);
        let response = lifecycle.reject_responses_websocket_authentication(
            &BuiltinAuthError::InvalidFormat,
            "/v1/responses",
        );
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let rejected = drain_events(&mut events);
        assert_eq!(
            rejected
                .iter()
                .filter(|event| matches!(
                    event,
                    cc_lb_lifecycle::LifecycleEvent::RequestTerminated { .. }
                ))
                .count(),
            1
        );
        assert_eq!(
            rejected
                .iter()
                .filter(|event| matches!(
                    event,
                    cc_lb_lifecycle::LifecycleEvent::AuthCompleted { result: Err(_), .. }
                ))
                .count(),
            1
        );
        assert!(rejected.iter().any(|event| matches!(
            event,
            cc_lb_lifecycle::LifecycleEvent::RequestStarted {
                event_kind: Some(cc_lb_request_log::RequestEventKind::OpenaiResponses),
                ..
            }
        )));
    }

    #[tokio::test]
    async fn websocket_authentication_rejection_uses_attempted_route_kind() {
        let bus = Arc::new(cc_lb_control::event_bus::InMemoryBus::new());
        let view = provenance_view(
            failure_principal_view("principal-test", default_artifacts()),
            Arc::new(TestSignerFactory),
            Vec::new(),
        );
        let lifecycle = provenance_lifecycle(view, Arc::new(QueueDispatch::new(Vec::new())), &bus);
        let mut events = subscribe_lifecycle(&bus);
        for (path, expected) in [
            (
                "/v1/realtime",
                cc_lb_request_log::RequestEventKind::OpenaiResponses,
            ),
            (
                "/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent",
                cc_lb_request_log::RequestEventKind::GeminiGenerateContent,
            ),
            (
                "/ws/unsupported",
                cc_lb_request_log::RequestEventKind::Other,
            ),
        ] {
            let response = lifecycle
                .reject_responses_websocket_authentication(&BuiltinAuthError::InvalidFormat, path);
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            let rejected = drain_events(&mut events);
            assert!(rejected.iter().any(|event| matches!(
                event,
                cc_lb_lifecycle::LifecycleEvent::RequestStarted {
                    event_kind: Some(kind),
                    ..
                } if *kind == expected
            )));
            assert_eq!(
                rejected
                    .iter()
                    .filter(|event| matches!(
                        event,
                        cc_lb_lifecycle::LifecycleEvent::RequestTerminated { .. }
                    ))
                    .count(),
                1
            );
        }
    }

    #[test]
    fn provider_state_marker_probe_detects_split_keys_and_skips_stateless_events() {
        for stateless in [
            &b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\""[..],
            b"event: ping\ndata: {",
            b"data: xxxxxxxxxxxxxxxx",
            b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"content\":[]}}\n\n",
        ] {
            assert!(!sse_bytes_may_carry_anthropic_provider_state(stateless));
        }
        for marker in ANTHROPIC_PROVIDER_STATE_MARKERS {
            assert!(sse_bytes_may_carry_anthropic_provider_state(marker));
        }
        // A key split across chunks is found inside the probe window that
        // re-scans the tail of the already-forwarded partial event.
        let forwarded = b"data: {\"type\":\"content_block_start\",\"content\":[{\"encrypted_con";
        let mut buffer = forwarded.to_vec();
        buffer.extend_from_slice(b"tent\":\"opaque\"}]}");
        let probe_start = forwarded.len() - ANTHROPIC_PROVIDER_STATE_MARKER_PROBE_BYTES;
        assert!(!sse_bytes_may_carry_anthropic_provider_state(forwarded));
        assert!(sse_bytes_may_carry_anthropic_provider_state(
            &buffer[probe_start..]
        ));
    }
    fn sticky_metadata(
        session_id: &str,
        request_kind: &str,
        claude_agent_id: Option<&str>,
    ) -> RequestCacheMetadata {
        RequestCacheMetadata {
            thread_id: Some(session_id.to_owned()),
            observed_session_id: Some(session_id.to_owned()),
            request_kind: Some(request_kind.to_owned()),
            claude_agent_id: claude_agent_id.map(ToOwned::to_owned),
            ..RequestCacheMetadata::default()
        }
    }

    #[test]
    fn thread_sticky_subagent_latch_seeds_omp_and_senpi_and_rewrites_main() {
        let latch = ThreadStickySubagentLatch::default();
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-session-affinity"),
            HeaderValue::from_static("sticky-thread"),
        );
        let omp = json!({
            "system": "You are omp's trusted coding assistant.",
            "messages": [{"role": "user", "content": "Complete assignment thoroughly:\n\n# Target"}]
        });
        let mut omp_metadata = request_cache_metadata_from_value_with_analysis_for_capability(
            &headers,
            Some(&omp),
            None,
            None,
        );
        assert_eq!(omp_metadata.request_kind.as_deref(), Some("subagent"));
        apply_thread_sticky_subagent_latch(&latch, "principal-a", Some(&omp), &mut omp_metadata);

        let senpi = json!({
            "system": "You are senpi, a coding agent.",
            "messages": [{"role": "user", "content": "Task: Review the auth change"}]
        });
        let mut senpi_metadata = request_cache_metadata_from_value_with_analysis_for_capability(
            &headers,
            Some(&senpi),
            None,
            None,
        );
        assert_eq!(senpi_metadata.request_kind.as_deref(), Some("subagent"));
        apply_thread_sticky_subagent_latch(
            &latch,
            "principal-b",
            Some(&senpi),
            &mut senpi_metadata,
        );

        let ordinary = json!({
            "system": "You are omp's trusted coding assistant.",
            "messages": [{"role": "user", "content": "continue"}]
        });
        let mut ordinary_metadata = request_cache_metadata_from_value_with_analysis_for_capability(
            &headers,
            Some(&ordinary),
            None,
            None,
        );
        apply_thread_sticky_subagent_latch(
            &latch,
            "principal-a",
            Some(&ordinary),
            &mut ordinary_metadata,
        );
        assert_eq!(ordinary_metadata.request_kind.as_deref(), Some("subagent"));

        let mut isolated_metadata = request_cache_metadata_from_value_with_analysis_for_capability(
            &headers,
            Some(&ordinary),
            None,
            None,
        );
        apply_thread_sticky_subagent_latch(
            &latch,
            "principal-c",
            Some(&ordinary),
            &mut isolated_metadata,
        );
        assert_eq!(isolated_metadata.request_kind.as_deref(), Some("main"));
    }

    #[test]
    fn thread_sticky_subagent_latch_preserves_one_shot_and_exclusion_results() {
        let latch = ThreadStickySubagentLatch::default();
        let seed = json!({
            "messages": [{"role": "user", "content": "Complete assignment thoroughly:\n\n# Target"}]
        });
        let mut seed_metadata = sticky_metadata("sticky-thread", "subagent", None);
        apply_thread_sticky_subagent_latch(&latch, "principal-a", Some(&seed), &mut seed_metadata);
        for request_kind in [
            "compaction",
            "recap",
            "session_title",
            "auto_thinking",
            "side",
            "advisor",
            "notification",
            "look_at",
        ] {
            let mut metadata = sticky_metadata("sticky-thread", request_kind, None);
            apply_thread_sticky_subagent_latch(&latch, "principal-a", None, &mut metadata);
            assert_eq!(metadata.request_kind.as_deref(), Some(request_kind));
        }
        let mut explicit_main = sticky_metadata("sticky-thread", "main", None);
        explicit_main.explicit_request_class = Some("main".to_owned());
        apply_thread_sticky_subagent_latch(&latch, "principal-a", None, &mut explicit_main);
        assert_eq!(explicit_main.request_kind.as_deref(), Some("main"));

        let hermes = json!({
            "system": "You are Hermes Agent, an intelligent AI assistant created by Nous Research.",
            "messages": [{"role": "user", "content": "Review the conversation above and consider saving to memory if appropriate."}]
        });
        let mut hermes_metadata = sticky_metadata("hermes-thread", "subagent", None);
        apply_thread_sticky_subagent_latch(
            &latch,
            "principal-a",
            Some(&hermes),
            &mut hermes_metadata,
        );
        let mut hermes_ordinary = sticky_metadata("hermes-thread", "main", None);
        apply_thread_sticky_subagent_latch(
            &latch,
            "principal-a",
            Some(&hermes),
            &mut hermes_ordinary,
        );
        assert_eq!(hermes_ordinary.request_kind.as_deref(), Some("main"));

        let mut claude_seed = sticky_metadata("claude-thread", "subagent", Some("agent-1"));
        apply_thread_sticky_subagent_latch(&latch, "principal-a", Some(&seed), &mut claude_seed);
        let mut claude_ordinary = sticky_metadata("claude-thread", "main", None);
        apply_thread_sticky_subagent_latch(
            &latch,
            "principal-a",
            Some(&seed),
            &mut claude_ordinary,
        );
        assert_eq!(claude_ordinary.request_kind.as_deref(), Some("main"));

        let mut no_thread = RequestCacheMetadata {
            request_kind: Some("subagent".to_owned()),
            ..RequestCacheMetadata::default()
        };
        apply_thread_sticky_subagent_latch(&latch, "principal-a", Some(&seed), &mut no_thread);
        assert!(!latch.observe(
            ThreadStickySubagentKey {
                principal_id: "principal-a".to_owned(),
                observed_session_id: "no-thread".to_owned(),
            },
            false,
        ));
    }

    #[test]
    fn thread_sticky_subagent_latch_evicts_oldest_entry_per_shard() {
        let latch = ThreadStickySubagentLatch::default();
        let first = ThreadStickySubagentKey {
            principal_id: "principal".to_owned(),
            observed_session_id: "thread-0".to_owned(),
        };
        let shard = ThreadStickySubagentLatch::shard_index(&first);
        let mut keys = vec![first];
        let mut index = 1;
        while keys.len() <= MAX_THREAD_STICKY_SUBAGENTS_PER_SHARD {
            let key = ThreadStickySubagentKey {
                principal_id: "principal".to_owned(),
                observed_session_id: format!("thread-{index}"),
            };
            index += 1;
            if ThreadStickySubagentLatch::shard_index(&key) == shard {
                keys.push(key);
            }
        }
        for key in &keys {
            assert!(latch.observe(key.clone(), true));
        }
        assert!(!latch.observe(keys[0].clone(), false));
        assert!(latch.observe(keys[1].clone(), false));
    }
}
