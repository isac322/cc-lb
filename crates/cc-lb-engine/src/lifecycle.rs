use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::fmt::Write as _;
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
    CacheLookbackPrefix, CachePricingSummary, CacheScore, InternalError, InternalErrorKind,
    InternalErrorStage, PrefixBytes, Principal, PrincipalKind, RoutingTrace, StageDecision,
    TerminalDecision, TerminalStrategy, TtlClass, Upstream, UpstreamCandidate,
    UpstreamKind as CandidateUpstreamKind, WarmCacheEntry,
};
use cc_lb_quota::build_subscription_quota_samples;
use cc_lb_quota::rate_limit_headers::{
    parse_anthropic_rate_limit_headers, parse_openai_rate_limit_headers,
};
use cc_lb_request_log::{
    HeaderSnapshot, RequestCacheBreakpoint, RequestCacheBreakpointSource,
    RequestCacheLookbackPrefix,
};
use cc_lb_routing::{FilterError, FilterOutput, FilterPlugin, RouteDecision};
use cc_lb_storage_api::{
    PrincipalKind as DbPrincipalKind, PrincipalKindLite, PromptCacheObservationRecord,
    StoredApiKeyRecord, UpstreamAffinityBinding, UpstreamAffinityKey, UpstreamAffinityStore,
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
use http::header::{CONTENT_LENGTH, CONTENT_TYPE, RETRY_AFTER};
use http::{HeaderMap, HeaderName, HeaderValue, Request, Response, StatusCode};
use http_body::Body as _;
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use rand::{RngExt, SeedableRng, rngs::StdRng};
use serde_json::{Value, json};
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
    anthropic_error_body, anthropic_error_response, anthropic_error_response_with_retry_after,
};
use crate::hop_by_hop::strip_hop_by_hop;
use crate::model_resolution::{cache_threshold_tokens, canonical_model_id};
use crate::prompt_cache_simulator::{
    PromptCacheAnalysisExecutor, PromptCacheAnalysisOutput, PromptCacheAnalysisTimings,
    V3_TOKEN_ESTIMATE_SOURCE, V3PromptCacheAnalysis, V3PromptCacheBlockSource,
    analyze_v3_prompt_cache,
};
use crate::provider_capability::{
    ProviderCapability, body_is_consistency_safe, classify as classify_provider_capability,
};
use crate::request_classification::classify_client_request_kind;
use crate::request_context::RequestContext;
use crate::request_timing::{REQUEST_STAGE_TIMINGS, RequestStageTimings};
use crate::responses_websocket::{
    PreparedWebSocketTurn, WebSocketCandidate, WebSocketError, WebSocketIngress,
};
use crate::sse_error_frame::make_error_frame;
use crate::subscription_quota_events::SubscriptionQuotaSink;
use crate::terminal_observer::{
    InternalFailure, LifecycleContext, StreamErrorClassification, StreamTerminationCause,
    TerminalClassification, UpstreamErrorCode, classify_stream_error, error_codes,
    numeric_attribute,
};
use crate::upstream_affinity::{
    UpstreamAffinityExtractionError, extract_request_affinity_keys, extract_response_affinity_keys,
    extract_response_affinity_keys_from_sse_event, request_observes_response_affinity,
};
use crate::usage_decoder::{UsageDecoder, decode_full_body};
use crate::usage_parser::{self, UsageCounts, accumulate_sse_usage, parse_sse_event};
use cc_lb_clock::{Clock, ClockHandle, unix_millis, unix_secs};
use cc_lb_control::RequestEventBus;
use cc_lb_control::api_keys::builtin_authn::{AuthnSuccess, BuiltinAuthError, BuiltinAuthn};
use cc_lb_control::api_keys::limit_engine::{
    LimitEngine, RejectReason, Reservation as LimitReservation,
};
use cc_lb_control::api_keys::principal_view::{
    PrincipalSpecCached, PrincipalView, RouterPipelineCache,
};
use cc_lb_control::api_keys::types::LimitKind;
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
/// Version 5 adds `thinking`, `redacted_thinking`, and `tool_reference` content blocks
/// to the prefix chain, changing keys for requests that replay them. It also renames
/// the unconditional hash domain tag from `cc-lb-cache-v4:*` to `cc-lb-cache-v5:*`,
/// forcing a clean generational split for every prefix key. Hydration rejects
/// mismatched versions, cold-starting warm state without a storage migration because
/// the persisted schema-version column is an integer.
pub const HASH_SCHEMA_VERSION: u8 = 5;

const DEFAULT_MESSAGES_CAP_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_FILES_CAP_BYTES: usize = 100 * 1024 * 1024;
const DEFAULT_ANTHROPIC_BASE_URL: &str = "https://api.anthropic.com/";
const DEFAULT_OPENAI_BASE_URL: &str = "https://api.openai.com/";
const DEFAULT_OPENAI_CODEX_BASE_URL: &str = "https://chatgpt.com/backend-api/codex";
const DEFAULT_GEMINI_BASE_URL: &str = "https://generativelanguage.googleapis.com/";
const DEFAULT_GEMINI_CODE_ASSIST_BASE_URL: &str = "https://cloudcode-pa.googleapis.com/v1internal";
const DEFAULT_MAX_INPUT_ESTIMATE: i64 = 4000;
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
    error_type: &str,
    message: &str,
    separate_from_partial_event: bool,
) -> Bytes {
    let frame = make_error_frame(error_type, message);
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
}

impl RequestKind {
    fn matches_upstream(self, kind: StorageUpstreamKind) -> bool {
        let (protocol, _) = ids_for(kind);
        matches!(
            (self, protocol),
            (Self::AnthropicMessages, ProtocolId::AnthropicMessages)
                | (Self::OpenaiResponses, ProtocolId::OpenaiResponses)
                | (
                    Self::GeminiGenerateContent,
                    ProtocolId::GeminiGenerateContent
                )
        )
    }
}

impl From<ProviderCapability> for RequestKind {
    fn from(capability: ProviderCapability) -> Self {
        match capability {
            ProviderCapability::AnthropicMessages => Self::AnthropicMessages,
            ProviderCapability::OpenaiResponses => Self::OpenaiResponses,
            ProviderCapability::GeminiGenerateContent => Self::GeminiGenerateContent,
        }
    }
}

fn gemini_model_from_path(path: &str) -> Option<String> {
    let prefix = "/v1beta/models/";
    let model_path = path.strip_prefix(prefix)?;
    let model = model_path
        .strip_suffix(":generateContent")
        .or_else(|| model_path.strip_suffix(":streamGenerateContent"))?;
    (!model.is_empty()).then(|| model.to_owned())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProtocolId {
    AnthropicMessages,
    OpenaiResponses,
    GeminiGenerateContent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TransportId {
    Http,
}

fn ids_for(kind: StorageUpstreamKind) -> (ProtocolId, TransportId) {
    match kind {
        StorageUpstreamKind::AnthropicApiKey | StorageUpstreamKind::AnthropicOauth => {
            (ProtocolId::AnthropicMessages, TransportId::Http)
        }
        StorageUpstreamKind::OpenaiApiKey | StorageUpstreamKind::OpenaiOauth => {
            (ProtocolId::OpenaiResponses, TransportId::Http)
        }
        StorageUpstreamKind::GeminiApiKey | StorageUpstreamKind::GeminiOauth => {
            (ProtocolId::GeminiGenerateContent, TransportId::Http)
        }
    }
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
async fn fetch_prompt_cache_warm_entries(
    view: &DynamicView,
    principal_id: &str,
    request_kind: RequestKind,
    canonical_model: &str,
    request_breakpoints: &[CacheBreakpoint],
    clock: &dyn Clock,
) -> HashMap<Uuid, Vec<WarmCacheEntry>> {
    let mut warm_entries: HashMap<Uuid, Vec<WarmCacheEntry>> = HashMap::new();
    let (Some(store), Some(allowed_upstreams)) = (
        view.prompt_cache_observation_store_opt(),
        view.principal_view.allowed_upstreams(principal_id),
    ) else {
        return warm_entries;
    };
    if canonical_model.is_empty() || request_breakpoints.is_empty() {
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
    for breakpoint in request_breakpoints {
        for prefix in &breakpoint.lookback_prefixes {
            if seen_prefix_keys.insert(prefix.prefix_hash.as_str()) {
                prefix_keys.push(prefix.prefix_hash.clone());
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

pub fn build_candidates(
    view: &DynamicView,
    principal_id: &str,
    request_kind: RequestKind,
    request_breakpoints: &[CacheBreakpoint],
    warm_entries: &HashMap<Uuid, Vec<WarmCacheEntry>>,
    clock: &dyn Clock,
) -> Vec<UpstreamCandidate> {
    build_candidates_with_matches(
        view,
        principal_id,
        request_kind,
        request_breakpoints,
        warm_entries,
        clock,
    )
    .0
}

pub(crate) fn build_candidates_with_matches(
    view: &DynamicView,
    principal_id: &str,
    request_kind: RequestKind,
    request_breakpoints: &[CacheBreakpoint],
    warm_entries: &HashMap<Uuid, Vec<WarmCacheEntry>>,
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
            let cache_score = warm_entries.get(&upstream.id).and_then(|entries| {
                let matched =
                    resolve_longest_warm_match(entries, request_breakpoints, now_unix_secs);
                let score = build_cache_score_from_match(request_breakpoints, matched.as_ref());
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
        CatalogSnapshot, CatalogStatus, PriceCatalog, Pricing, TierRate, UsdPerMillion,
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

fn cache_pricing_summary_for_model(
    catalog: &cc_lb_pricing::PriceCatalog,
    model: &str,
    service_tier: Option<&str>,
) -> CachePricingSummary {
    if model.is_empty() {
        return CachePricingSummary {
            status: "unknown".to_owned(),
            input_micros_per_million: None,
            cache_creation_5m_micros_per_million: None,
            cache_creation_1h_micros_per_million: None,
            cache_read_micros_per_million: None,
        };
    }

    let normalized = cc_lb_pricing::normalize_model_id(model);
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
) -> Option<PromptCacheObservationContext> {
    if canonical_model_id.is_empty() || cache_breakpoints.is_empty() {
        return None;
    }
    let sink = Arc::clone(&view.prompt_cache_observation_sink);
    Some(PromptCacheObservationContext {
        upstream_id,
        canonical_model_id: canonical_model_id.to_owned(),
        cache_breakpoints: cache_breakpoints.to_vec(),
        cacheable_breakpoint_prefix_keys: cacheable_breakpoint_prefix_keys.clone(),
        selected_match,
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
    fn into_response_and_failure(self) -> (Response<Body>, InternalFailure) {
        match self {
            Self::RawPassthrough(source) => {
                let response = anthropic_error_response(
                    StatusCode::BAD_GATEWAY,
                    "api_error",
                    "failed to prepare raw upstream request",
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
                let response = anthropic_error_response(
                    StatusCode::BAD_GATEWAY,
                    "api_error",
                    "failed to sign upstream request",
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
                        anthropic_error_response_with_retry_after(
                            StatusCode::SERVICE_UNAVAILABLE,
                            "overloaded_error",
                            "upstream bulkhead queue is full",
                            retry_after.as_secs().max(1),
                        ),
                        InternalErrorKind::Unavailable,
                        source.to_string(),
                    ),
                    DispatchError::InvalidUri { .. } | DispatchError::RequestBuild { .. } => (
                        anthropic_error_response(
                            StatusCode::BAD_GATEWAY,
                            "api_error",
                            "upstream request failed",
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
                            anthropic_error_response(
                                StatusCode::BAD_GATEWAY,
                                "api_error",
                                "upstream request failed",
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
    pending_keys: Vec<UpstreamAffinityKey>,
}

struct RequestAffinityFailure {
    status: StatusCode,
    error_type: &'static str,
    message: &'static str,
}

impl RequestAffinityFailure {
    fn response(&self) -> Response<Body> {
        anthropic_error_response(self.status, self.error_type, self.message)
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
    ttl_secs: u64,
}

async fn bind_upstream_affinity_keys(
    context: &ResponseAffinityContext,
    keys: &[UpstreamAffinityKey],
    now_unix_secs: u64,
) -> Result<(), ()> {
    if keys.is_empty() {
        return Ok(());
    }
    let Some(store) = context.store.as_ref() else {
        return Err(());
    };
    let mut seen = HashSet::with_capacity(keys.len());
    let mut bindings = Vec::with_capacity(keys.len());
    for key in keys {
        if seen.insert(key) {
            bindings.push(UpstreamAffinityBinding {
                key: key.clone(),
                upstream_id: context.upstream_id,
                observed_at_unix_secs: now_unix_secs,
                expires_at_unix_secs: None,
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

/// Whether a successful SSE event is the provider's terminal success event,
/// before which request-referenced provider state still pending a binding must
/// be persisted. Gemini streams carry no terminal event, so their pending keys
/// are bound once the upstream stream completes cleanly.
fn sse_event_completes_response(
    capability: ProviderCapability,
    event: &usage_parser::ParsedSseEvent<'_>,
    anthropic_message_stop: bool,
) -> bool {
    match capability {
        ProviderCapability::AnthropicMessages => {
            anthropic_message_stop || event.event_name() == Some(b"message_stop")
        }
        ProviderCapability::OpenaiResponses => {
            event.event_type() == Some("response.completed")
                || event.event_name() == Some(b"response.completed")
        }
        ProviderCapability::GeminiGenerateContent => false,
    }
}

/// JSON `type` of the provider's terminal success SSE event, used by the #663
/// accept-with-warning path. Gemini streams have no terminal event.
const fn semantic_terminal_event_type(capability: ProviderCapability) -> Option<&'static str> {
    match capability {
        ProviderCapability::AnthropicMessages => Some("message_stop"),
        ProviderCapability::OpenaiResponses => Some("response.completed"),
        ProviderCapability::GeminiGenerateContent => None,
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
            dynamic_view,
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
        let cache_metadata = request_cache_metadata_from_value(&input.headers, body_value.as_ref());
        let cache_breakpoints = cache_metadata.plugin_cache_breakpoints();

        let request_id = input
            .request_id
            .clone()
            .unwrap_or_else(|| format!("preview-{}", Uuid::new_v4()));
        let ctx = RequestContext::builder()
            .request_id(request_id)
            .thread_id(cache_metadata.thread_id.clone())
            .requested_service_tier(cache_metadata.requested_service_tier.clone())
            .downstream_headers(input.headers)
            .method(http::Method::POST)
            .path("/v1/messages".to_owned())
            .query(None)
            .body_bytes(input.body_bytes)
            .cache_breakpoints(cache_breakpoints)
            .canonical_model_id(cache_metadata.canonical_model_id.clone())
            .cache_pricing(cache_pricing_summary_for_model(
                cc_lb_pricing::global_catalog(),
                &cache_metadata.canonical_model_id,
                cache_metadata.requested_service_tier.as_deref(),
            ))
            .build();
        let principal = Principal {
            id: input.principal_id,
            kind: PrincipalKind::InternalKey,
        };

        let warm_entries = fetch_prompt_cache_warm_entries(
            &view,
            &principal.id,
            RequestKind::AnthropicMessages,
            &ctx.canonical_model_id,
            &ctx.cache_breakpoints,
            &*self.clock,
        )
        .await;
        let candidates = build_candidates(
            &view,
            &principal.id,
            RequestKind::AnthropicMessages,
            &ctx.cache_breakpoints,
            &warm_entries,
            &*self.clock,
        );
        let pipeline_result = execute_filter_pipeline(
            &resolved_pipeline.user_filters,
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
    pub(crate) fn websocket_observer(&self) -> (Option<LifecycleContext>, String) {
        let request_id = next_request_id();
        let observer = self.event_bus.as_ref().map(|bus| {
            let observer = LifecycleContext::new(request_id.clone(), Arc::clone(bus), &self.clock);
            observer.set_event_kind(cc_lb_request_log::RequestEventKind::from_path(
                "/v1/responses",
            ));
            observer
        });
        (observer, request_id)
    }

    pub fn reject_responses_websocket_authentication(
        &self,
        error: &BuiltinAuthError,
    ) -> Response<Body> {
        let (observer, _) = self.websocket_observer();
        if let Some(observer) = observer.as_ref() {
            observer.mark_authn_reached();
        }
        crate::authn_rail::reject_unauthenticated(error, observer.as_ref())
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
        if let Some(observer) = observer.as_ref() {
            observer.mark_authn_reached();
            observer.set_request_span(span.clone());
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
                        cache_breakpoints: Vec::new(),
                        cache_prefix_hash: None,
                        matched_v3_cache_key: None,
                        thread_id: None,
                        observed_session_id: None,
                        request_kind: None,
                        claude_agent_id: None,
                        claude_parent_agent_id: None,
                        parent_session_id: None,
                        client_app: None,
                        session_id_source: None,
                        message_id: None,
                        message_index: None,
                        message_count: None,
                        cache_control_message_indices: Vec::new(),
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
                    &body_view,
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
                        amount: active_limit
                            .as_ref()
                            .map_or(0, |limit| limit.request.max_tokens.max(0) as u64),
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
                reservation: active_limit
                    .as_mut()
                    .and_then(|limit| limit.reservation.take()),
                limit_engine: self.limit_engine.clone(),
                principal_id: auth.principal_id().to_owned(),
                key_id: auth.key_id().to_owned(),
                model,
                store: body.get("store").and_then(Value::as_bool).unwrap_or(true),
                span: span.clone(),
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
        let ctx = RequestContext::builder()
            .request_id(request_id)
            .downstream_headers(downstream_headers)
            .method(http::Method::GET)
            .path("/v1/responses".to_owned())
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
        let mut candidates = build_candidates(
            view,
            auth.principal_id(),
            RequestKind::OpenaiResponses,
            &[],
            &HashMap::new(),
            &*self.clock,
        );
        if let Some(target) = target {
            candidates.retain(|candidate| candidate.upstream_id == target);
        }
        let mut result =
            execute_filter_pipeline(&pipeline.user_filters, &ctx, &principal, candidates);
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
            ttl_secs: self.config.upstream_affinity_ttl.as_secs(),
        };
        bind_upstream_affinity_keys(&context, &keys, unix_secs(self.clock.now()))
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
        let body_view = RequestBodyView::new(&ctx.body_bytes);
        let path_model = gemini_model_from_path(&ctx.path);
        let request_model = body_view.model().or(path_model.clone());
        let request_capability = classify_provider_capability(&ctx.method, &ctx.path);
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
            let response = anthropic_error_response(
                StatusCode::NOT_FOUND,
                "operation_not_supported",
                "operation is not supported by the consistency-safe proxy boundary",
            );
            if let Some(o) = observer.as_ref() {
                o.terminate_local(TerminalClassification::OPERATION_NOT_SUPPORTED);
            }
            return Ok(response);
        };
        if body_view
            .value()
            .is_some_and(|body| !body_is_consistency_safe(request_capability, body))
        {
            let response = anthropic_error_response(
                StatusCode::NOT_FOUND,
                "operation_not_supported",
                "operation is not supported by the consistency-safe proxy boundary",
            );
            if let Some(o) = observer.as_ref() {
                o.terminate_local(TerminalClassification::OPERATION_NOT_SUPPORTED);
            }
            return Ok(response);
        }
        let request_kind = RequestKind::from(request_capability);

        if let Some(response) = body_too_large {
            if let Some(o) = observer.as_ref() {
                o.terminate_body_too_large(body_cap_for_path(&self.config, &ctx.path) as u64);
            }
            return Ok(*response);
        }
        if ctx.path == "/v1/messages" && !ctx.body_bytes.is_empty() && !body_view.is_valid_json() {
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
            return Ok(anthropic_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "request body must be valid JSON",
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
                    },
                    cacheable_breakpoint_prefix_keys: Vec::new(),
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
        let mut cache_metadata = request_cache_metadata_from_value_with_analysis(
            &ctx.downstream_headers,
            body_view.value(),
            prompt_cache_analysis,
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
                    cache_breakpoints: cache_metadata.cache_breakpoints.clone(),
                    cache_prefix_hash: cache_metadata.cache_prefix_hash.clone(),
                    matched_v3_cache_key: cache_metadata.cache_prefix_hash.clone(),
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
                }),
            });
        }
        ctx.cache_breakpoints = cache_metadata.plugin_cache_breakpoints();
        ctx.canonical_model_id = if cache_metadata.canonical_model_id.is_empty() {
            path_model
                .as_deref()
                .map(canonical_model_id)
                .unwrap_or_default()
                .to_owned()
        } else {
            cache_metadata.canonical_model_id.clone()
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
                return Ok(anthropic_error_response(
                    StatusCode::BAD_REQUEST,
                    "invalid_request_error",
                    UPSTREAM_AFFINITY_KEY_LIMIT_MESSAGE,
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
                return Ok(failure.response());
            }
        };
        let track_keepalive_response = self.cache_keepalive_enqueuer.is_some()
            && cached.cache_keepalive().is_some()
            && !cache_metadata.cache_breakpoints.is_empty();
        if track_keepalive_response {
            cache_metadata.request_json = body_view.value().cloned();
        }

        let router_pipeline = cached.resolved_pipeline(None);
        if let Some(error) = router_pipeline.instantiation_error.as_deref() {
            let response = anthropic_error_response(
                StatusCode::BAD_GATEWAY,
                "route_not_configured",
                "router pipeline is unavailable for this request",
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
        let warm_entries = fetch_prompt_cache_warm_entries(
            &view,
            &principal.id,
            request_kind,
            &ctx.canonical_model_id,
            &ctx.cache_breakpoints,
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
        let (mut candidates, mut selected_cache_matches) = build_candidates_with_matches(
            &view,
            &principal.id,
            request_kind,
            &ctx.cache_breakpoints,
            &warm_entries,
            &*self.clock,
        );
        if let Some(affinity) = request_affinity.as_ref() {
            candidates.retain(|candidate| candidate.upstream_id == affinity.target_upstream_id);
            selected_cache_matches
                .retain(|upstream_id, _| *upstream_id == affinity.target_upstream_id);
        }
        let mut pipeline_result =
            execute_filter_pipeline(&router_pipeline.user_filters, &ctx, &principal, candidates);
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
            return Ok(anthropic_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                error_codes::UPSTREAM_AFFINITY_UNAVAILABLE,
                UPSTREAM_AFFINITY_TARGET_INELIGIBLE_MESSAGE,
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
            route_span.record("cc_lb.route.outcome", "error");
            route_span.record("otel.status_code", "ERROR");
            route_span.record("error.type", error_codes::ROUTE_NOT_CONFIGURED);
            let response = anthropic_error_response(
                StatusCode::BAD_GATEWAY,
                "route_not_configured",
                "no upstream route is configured for this request",
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
                let response = anthropic_error_response(
                    StatusCode::BAD_GATEWAY,
                    "route_not_configured",
                    "no upstream route is configured for this request",
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
        );
        emit_route_completed(observer.as_ref(), &routed, request_model.as_deref());

        let limit_reserve_start = Instant::now();
        let mut active_limit = match self
            .reserve_limit(
                &principal_view,
                &ctx,
                &principal,
                routed.record,
                success,
                &body_view,
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
            let decision = if let Some(limit) = active_limit.as_ref() {
                cc_lb_lifecycle::LimitDecisionKind::Reserved {
                    reservation_id: limit
                        .reservation
                        .as_ref()
                        .map(|r| r.id().to_owned())
                        .unwrap_or_default(),
                    amount: limit.request.max_tokens as u64,
                    limit_reserve_ms: Some(limit_reserve_ms),
                }
            } else {
                cc_lb_lifecycle::LimitDecisionKind::Reserved {
                    reservation_id: String::new(),
                    amount: 0,
                    limit_reserve_ms: Some(limit_reserve_ms),
                }
            };
            o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::LimitDecision {
                event_id: o.event_id().to_owned(),
                decision,
            });
        }
        let reserved = active_limit
            .as_mut()
            .and_then(|active_limit| active_limit.reservation.take())
            .map(|reservation| AttemptIntent::from_reservation(reservation).into_reserved());

        // Bounded cross-upstream failover. Only failures that happen before
        // the upstream processed the request (credential preparation, signing,
        // 401, 429) move to another upstream; nothing has reached the client
        // yet, and the reservation above covers every attempt.
        let mut failover = FailoverRouter {
            view: &view,
            cached,
            router_pipeline: &router_pipeline,
            ctx: &ctx,
            principal: &principal,
            warm_entries: &warm_entries,
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
                    let mut response = anthropic_error_response(
                        StatusCode::BAD_GATEWAY,
                        "api_error",
                        "failed to prepare upstream credentials",
                    );
                    self.attach_limit_headers(&mut response, active_limit.as_ref());
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
                    reserved.as_ref(),
                    &ctx,
                    &principal,
                    &routed,
                    signer.clone(),
                    observer.as_ref(),
                    &mut attempt_timings,
                    &mut keepalive_shaped_body,
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
                        active_limit.as_ref(),
                        observer.as_ref(),
                        &attempt_timings,
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
                    return Ok(self.relay_unauthorized(
                        unauthorized,
                        signer.as_ref(),
                        active_limit.as_ref(),
                        observer.as_ref(),
                        &attempt_timings,
                    ));
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
                        reserved.as_ref(),
                        &ctx,
                        &principal,
                        &routed,
                        new_signer.clone(),
                        observer.as_ref(),
                        &mut attempt_timings,
                        &mut keepalive_shaped_body,
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
                            active_limit.as_ref(),
                            observer.as_ref(),
                            &attempt_timings,
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
            &ctx.cache_breakpoints,
            &cache_metadata.cacheable_breakpoint_prefix_keys,
            routed.selected_cache_match.take(),
        );
        let status = response.status();
        let keepalive_shaped_body = if track_keepalive_response {
            keepalive_shaped_body
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
                downstream_headers: ctx.downstream_headers.clone(),
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
            ttl_secs: self.config.upstream_affinity_ttl.as_secs(),
        };
        response = self
            .finish_success_response(
                response,
                signer,
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
        body_view: &RequestBodyView,
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
        let limit_request = body_view.limit_request(&ctx.canonical_model_id);
        let max_input_estimate = DEFAULT_MAX_INPUT_ESTIMATE;
        let cost_estimate = self.limit_cost_estimator.as_ref().and_then(|estimator| {
            estimator.estimate_max(
                &limit_request.model,
                max_input_estimate as u64,
                limit_request.max_tokens.max(0) as u64,
                limit_request.service_tier.as_deref(),
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
        let active_limit = active_limit;
        if active_limit
            .as_ref()
            .is_some_and(|active_limit| active_limit.request.stream)
            || is_sse_response(response.headers())
        {
            let response_status = response.status();
            let mut response = self.relay_response(
                response,
                signer,
                response_status,
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
            self.attach_limit_headers(&mut response, None);
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
        let observation = response_body_json
            .as_ref()
            .map_or_else(usage_parser::NonStreamObservation::default, |value| {
                usage_parser::observe_non_stream_json_value(value)
            });
        let abnormal_stop = response_body_json
            .as_ref()
            .and_then(usage_parser::detect_abnormal_stop_non_stream);
        let usage = observation.usage;
        let canonical_upstream_error = if status.is_client_error() || status.is_server_error() {
            response_body_json
                .as_ref()
                .and_then(usage_parser::observe_http_error_value)
        } else {
            None
        };
        // Provider error text can echo the credential; the parser masks it
        // before its store cap so a secret split by the cap cannot survive.
        // Successful responses never get here.
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
            parts.status = StatusCode::BAD_GATEWAY;
            parts.headers.clear();
            parts.headers.insert(
                CONTENT_TYPE,
                HeaderValue::from_static("application/json; charset=utf-8"),
            );
            anthropic_error_body("api_error", UPSTREAM_BUFFERED_BODY_FAILED_MESSAGE)
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
            let extraction_failed = if let Some(response_json) = response_body_json.as_ref() {
                match extract_response_affinity_keys(
                    response_affinity_context.capability,
                    response_json,
                    &response_affinity_context.principal_id,
                ) {
                    Ok(response_keys) => {
                        affinity_keys.extend(response_keys);
                        false
                    }
                    Err(_) => true,
                }
            } else {
                false
            };
            if extraction_failed
                || bind_upstream_affinity_keys(
                    &response_affinity_context,
                    &affinity_keys,
                    unix_secs(self.clock.now()),
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
                downstream_body =
                    anthropic_error_body("api_error", UPSTREAM_AFFINITY_UNAVAILABLE_MESSAGE);
            }
        }
        let client_status = parts.status;
        if !affinity_bind_failed
            && let (Some(context), Some(response_json)) =
                (keepalive_completion, response_body_json.as_ref())
        {
            crate::cache_keepalive::LifecycleKeepalive::new(
                self.cache_keepalive_enqueuer.clone(),
                Arc::clone(&self.dynamic_view),
            )
            .on_response_completed(
                response_json,
                &context.principal,
                context.accounting_key_id,
                &context.cache_metadata,
                context.upstream_id,
                context.shaped_body,
                &context.downstream_headers,
                first_body_chunk_at.map_or(duration, |anchor| anchor.elapsed()),
            );
        }
        if usage.present
            && status == StatusCode::OK
            && let Some(context) = prompt_cache_observation_context.as_ref()
        {
            let now_unix_secs = unix_secs(self.clock.now());
            let decode = decode_prompt_cache_observations_pure(
                context,
                PromptCacheUsage::from(&usage),
                response_start_unix_secs(dispatch_unix_secs, dispatch_started, None),
            );
            publish_prompt_cache_observations(context, &decode, now_unix_secs);
        }
        // Hand the reservation to the reconcile subscriber for every buffered outcome:
        // it reconciles a measured success and refunds errors on `RequestTerminated`.
        if let (Some(limit_engine), Some(active_limit), Some(response_accounting_guard)) = (
            self.limit_engine.as_ref(),
            active_limit.as_ref(),
            response_accounting_guard,
        ) {
            response_accounting_guard.forget();
            attach_limit_headers_from_engine(
                &mut parts.headers,
                limit_engine.as_ref(),
                &active_limit.subject.key_id,
                &active_limit.subject.principal_id,
            );
        }

        let finalize_ms = if let Some(o) = observer.as_ref() {
            o.set_usage_counts(&usage, true);
            o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::UsageObserved {
                event_id: o.event_id().to_owned(),
                usage: to_usage_snapshot(&usage),
                source: cc_lb_lifecycle::UsageSource::NonStreamBody,
            });
            let stream_result = if body_collect_failed {
                Err(cc_lb_lifecycle::StreamError {
                    error_type: UPSTREAM_RESPONSE_BODY_ERROR_TYPE.to_owned(),
                    error_message: UPSTREAM_BUFFERED_BODY_FAILED_MESSAGE.to_owned(),
                })
            } else if affinity_bind_failed {
                Err(affinity_stream_error())
            } else if let Some(error) = buffered_transform_error.as_ref() {
                Err(cc_lb_lifecycle::StreamError {
                    error_type: "response_transform_error".to_owned(),
                    error_message: error.to_string(),
                })
            } else {
                Ok(cc_lb_lifecycle::StreamSuccess {
                    usage: to_usage_snapshot(&usage),
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
            if status.is_client_error() || status.is_server_error() {
                if upstream_error_type.is_some() || upstream_error_message.is_some() {
                    o.emit_lifecycle(
                        cc_lb_lifecycle::LifecycleEvent::RequestLogUpstreamErrorObserved {
                            event_id: o.event_id().to_owned(),
                            error_type: upstream_error_type.unwrap_or_default(),
                            error_message: upstream_error_message.unwrap_or_default(),
                        },
                    );
                }
                let code = if status.is_client_error() {
                    UpstreamErrorCode::Upstream4xx
                } else {
                    UpstreamErrorCode::Upstream5xx
                };
                o.set_upstream_error(status, code);
            } else if body_collect_failed {
                o.set_upstream_error(StatusCode::BAD_GATEWAY, UpstreamErrorCode::StreamError);
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
        reserved: Option<&Reserved>,
        ctx: &RequestContext,
        principal: &Principal,
        routed: &RoutedUpstream<'_>,
        signer: Arc<dyn Signer>,
        observer: Option<&LifecycleContext>,
        attempt_timings: &mut AttemptTimings,
        shaped_body_out: &mut Option<Bytes>,
    ) -> Result<Response<Body>, AttemptFailure> {
        let attempt_scope = match reserved {
            Some(reserved) => reserved.begin_attempt(),
            None => Scoped::unreserved_for_lifecycle(),
        };
        let response = self
            .attempt(
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
        active_limit: Option<&ActiveLimit>,
        observer: Option<&LifecycleContext>,
        attempt_timings: &AttemptTimings,
    ) -> Response<Body> {
        let (mut response, failure) = failure.into_response_and_failure();
        self.attach_limit_headers(&mut response, active_limit);
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

    /// Terminal relay of an upstream 401 whose signer declined to refresh and
    /// that is not failed over.
    fn relay_unauthorized(
        &self,
        unauthorized: CollectedResponse,
        signer: &dyn Signer,
        active_limit: Option<&ActiveLimit>,
        observer: Option<&LifecycleContext>,
        attempt_timings: &AttemptTimings,
    ) -> Response<Body> {
        if let (Some(o), Some(body)) = (observer, unauthorized.body.as_deref())
            && let Ok(Some(error_body)) = decode_full_body(
                &unauthorized.headers,
                body,
                decompression_output_budget_bytes(self.config.messages_body_cap_bytes),
            )
            && !error_body.is_empty()
        {
            let (error_type, error_message) = usage_parser::observe_http_error_body(&error_body)
                .map(|error| error.into_parts(signer))
                .unwrap_or_default();
            o.emit_lifecycle(
                cc_lb_lifecycle::LifecycleEvent::RequestLogUpstreamErrorObserved {
                    event_id: o.event_id().to_owned(),
                    error_type: error_type.unwrap_or_default(),
                    error_message: match error_message {
                        Some(message) => message,
                        None => scrubbed_upstream_error_body(signer, &error_body, false),
                    },
                },
            );
        }
        let mut response = response_from_collected(unauthorized);
        self.attach_limit_headers(&mut response, active_limit);
        let status = response.status();
        if let Some(o) = observer {
            o.set_attempt_timings(
                attempt_timings.shape_ms,
                attempt_timings.sign_ms,
                attempt_timings.upstream_ttfb_ms,
            );
            let code = if status.is_client_error() {
                UpstreamErrorCode::Upstream4xx
            } else {
                UpstreamErrorCode::Upstream5xx
            };
            o.set_upstream_error(status, code);
            o.finish();
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

        let downstream_headers = {
            strip_hop_by_hop(&mut parts.headers);
            parts.headers
        };
        let ctx = RequestContext::builder()
            .request_id(request_id)
            .thread_id(None)
            .requested_service_tier(None)
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
            url = %signed.url(),
            method = %signed.method(),
            header_count = signed.headers().len(),
            headers = ?signed_header_names,
            body_bytes = signed.body_len(),
            "about to dispatch signed request"
        );

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
        let upstream_is_sse = is_sse_response(&parts.headers);
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
        let keepalive = crate::cache_keepalive::LifecycleKeepalive::new(
            self.cache_keepalive_enqueuer.clone(),
            Arc::clone(&self.dynamic_view),
        );
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
        );
        let completion_observer = Arc::clone(&self.completion_observer);
        let parse_sse_events = !upstream_error_status || upstream_is_sse;
        let stream_body_io_timing = response_body_timing.clone();
        let downstream_poll_timing = response_body_timing;
        let stream = async_stream::stream! {
            let sse_transform_hook = transform_ctx
                .dialect
                .sse_event_transform_hook()
                .or_else(|| {
                    transform_ctx
                        .gemini_oauth
                        .then_some(&GEMINI_OAUTH_RESPONSE_TRANSFORM as &dyn SseEventTransformHook)
                });
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
            // #663: semantic stop only — the SSE `event:` name alone does not
            // count; `usage_update.message_stop` requires the JSON `type`.
            let mut semantic_message_stop_seen = false;
            let mut prompt_cache_observations_published = false;
            let mut keepalive_response = crate::cache_keepalive::StreamingKeepaliveResponse::default();
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
            let mut stream_abnormal_stop: Option<usage_parser::AbnormalStop> = None;
            let mut last_partial_at: Option<Instant> = None;
            let mut last_partial_output_tokens: u64 = 0;
            let mut upstream_error_body: Vec<u8> = Vec::new();
            let mut upstream_error_body_truncated = false;
            let mut upstream_error_body_decode_failed = false;
            let mut parse_sse_events_active = parse_sse_events;
            let mut raw_before_transform_output: Vec<Bytes> = Vec::new();
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
                                });
                                let frame = make_error_frame(
                                    UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                    UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE,
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
                                            });
                                        let frame = make_error_frame(
                                            UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                            UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE,
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
                                            });
                                        if transformed_output_started
                                            || !downstream_stream_is_identity
                                        {
                                            let frame = make_error_frame(
                                                UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                                &error_message,
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
                                                make_response_transform_error_frame(&transform_error);
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
                                let usage_update =
                                    accumulate_sse_usage(&parsed_event, &mut usage);
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
                                if stream_abnormal_stop.is_none()
                                    && let Some(stop) =
                                        usage_parser::detect_abnormal_stop(&parsed_event)
                                {
                                    // Latch only: the upstream-error observation is emitted
                                    // at finalization, where it is known whether a later
                                    // provider/proxy error wins the terminal classification.
                                    stream_abnormal_stop = Some(stop);
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
                                                });
                                            parse_sse_events_active = false;
                                            buffer.clear();
                                            raw_before_transform_output.clear();
                                            let frame = make_error_frame(
                                                UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                                UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE,
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
                                        let frame = make_error_frame(
                                            "api_error",
                                            UPSTREAM_AFFINITY_UNAVAILABLE_MESSAGE,
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
                                keepalive_response.observe(&parsed_event);
                                match event_name {
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
                                            if let Some(o) = observer.as_ref() {
                                                o.set_first_content_delta_ms(duration_to_ms(
                                                    now.saturating_duration_since(relay_start),
                                                ));
                                            }
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
                                            now_unix_secs,
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
                                if usage_update.message_stop
                                    && message_stop_at.is_none()
                                {
                                    message_stop_at = Some(now);
                                }
                                // Strictly JSON-typed: the usage parser's
                                // event-name fallback must not enable #663
                                // accept-with-warning for name-only stops.
                                let semantic_event_type = parsed_event
                                    .value()
                                    .ok()
                                    .flatten()
                                    .and_then(|value| value.get("type"))
                                    .and_then(serde_json::Value::as_str);
                                semantic_message_stop_seen |= semantic_terminal_event_type(
                                    response_affinity_context.capability,
                                )
                                .is_some_and(|terminal| semantic_event_type == Some(terminal));
                                if let Some(o) = observer.as_ref() {
                                    if usage_update.message_start_usage {
                                        o.set_usage_counts(&usage, false);
                                        o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::UsageObserved {
                                            event_id: o.event_id().to_owned(),
                                            usage: to_usage_snapshot(&usage),
                                            source: cc_lb_lifecycle::UsageSource::MessageStart,
                                        });
                                    } else if usage_update.message_stop {
                                        o.set_usage_counts(&usage, true);
                                        o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::UsageObserved {
                                            event_id: o.event_id().to_owned(),
                                            usage: to_usage_snapshot(&usage),
                                            source: cc_lb_lifecycle::UsageSource::MessageStop,
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
                                                        make_response_transform_error_frame(&error);
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
                                                raw_passthrough_current_chunk = true;
                                                for raw in raw_before_transform_output.drain(..) {
                                                    downstream_sse_boundary.observe(&raw);
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
                                                        make_error_frame(
                                                            UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                                            UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE,
                                                        )
                                                    } else {
                                                        make_response_transform_error_frame(&error)
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
                                                raw_passthrough_current_chunk = true;
                                                for raw in raw_before_transform_output.drain(..) {
                                                    downstream_sse_boundary.observe(&raw);
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
                                            });
                                        let frame = make_error_frame(
                                            UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                            UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE,
                                        );
                                        yield Ok::<Bytes, Infallible>(frame);
                                        break 'upstream;
                                    } else {
                                        sse_transform_active = false;
                                        raw_passthrough_current_chunk = true;
                                        Bytes::new()
                                    };
                                    if raw_passthrough_current_chunk {
                                        break;
                                    }
                                    batch_index = batch_index.saturating_add(1);
                                    yield Ok::<Bytes, Infallible>(outgoing);
                                } else if success_sse_affinity_gate {
                                    downstream_sse_boundary.observe(&raw);
                                    batch_index = batch_index.saturating_add(1);
                                    if defer_terminal_raw_chunk {
                                        deferred_terminal_chunks.push(raw);
                                    } else {
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
                                        });
                                    let frame = make_error_frame(
                                        UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                        UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE,
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
                                        let frame = make_response_transform_error_frame(&error);
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
                                    for raw in raw_before_transform_output.drain(..) {
                                        downstream_sse_boundary.observe(&raw);
                                        batch_index = batch_index.saturating_add(1);
                                        yield Ok::<Bytes, Infallible>(raw);
                                    }
                                }
                            }
                            if !sse_transform_active {
                                if !success_sse_affinity_gate && !hold_unsolicited_chunk {
                                    // Chunk fanout stays inline — high-volume, not bus-worthy.
                                    if upstream_is_sse && downstream_stream_is_identity {
                                        downstream_sse_boundary.observe(&data);
                                    }
                                    batch_index = batch_index.saturating_add(1);
                                    if defer_terminal_raw_chunk {
                                        debug_assert!(deferred_terminal_chunks.is_empty());
                                        deferred_terminal_chunks.push(data);
                                    } else {
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
                            let usage_update = accumulate_sse_usage(&parsed_event, &mut usage);
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
                                            });
                                        downstream_drop_guard.mark_upstream_error(
                                            StreamTerminationCause::FramingError,
                                            None,
                                        );
                                        parse_sse_events_active = false;
                                        buffer.clear();
                                        raw_before_transform_output.clear();
                                        let frame = make_error_frame(
                                            UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                            UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE,
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
                                    let frame = make_error_frame(
                                        "api_error",
                                        UPSTREAM_AFFINITY_UNAVAILABLE_MESSAGE,
                                    );
                                    batch_index = batch_index.saturating_add(1);
                                    yield Ok::<Bytes, Infallible>(frame);
                                }
                                break;
                            }
                            keepalive_response.observe(&parsed_event);
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
                            if stream_abnormal_stop.is_none()
                                && let Some(stop) =
                                    usage_parser::detect_abnormal_stop(&parsed_event)
                            {
                                // Latch only: the upstream-error observation is emitted
                                // at finalization, where it is known whether a later
                                // provider/proxy error wins the terminal classification.
                                stream_abnormal_stop = Some(stop);
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
                                            });
                                        downstream_drop_guard.mark_upstream_error(
                                            StreamTerminationCause::FramingError,
                                            None,
                                        );
                                        let frame = make_error_frame(
                                            UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                            UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE,
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
                                        SseTransformOutcome::Emit(bytes) => bytes,
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
                                            let frame = make_error_frame(
                                                UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                                UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE,
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
                                    });
                                downstream_drop_guard.mark_upstream_error(
                                    StreamTerminationCause::FramingError,
                                    None,
                                );
                                let frame = make_error_frame(
                                    UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                    UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE,
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
                                    let frame = make_response_transform_error_frame(&error);
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
                        });
                        downstream_drop_guard.mark_upstream_error(
                            StreamTerminationCause::DecodeError,
                            None,
                        );
                        let frame = make_error_frame(
                            UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                            UPSTREAM_SSE_INSPECTION_FAILED_MESSAGE,
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
                            let frame = make_error_frame(
                                UPSTREAM_RESPONSE_CLIENT_ERROR_TYPE,
                                &error_message,
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
                            let frame = make_response_transform_error_frame(&transform_error);
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
                    let frame = make_error_frame(
                        "api_error",
                        UPSTREAM_AFFINITY_UNAVAILABLE_MESSAGE,
                    );
                    batch_index = batch_index.saturating_add(1);
                    yield Ok::<Bytes, Infallible>(frame);
                }
            }
            downstream_drop_guard.detach_lifecycle_observer();
            stream_body_io_timing.stop();
            let response_body_completed_at = Instant::now();
            if stream_affinity_error.is_none()
                && let Some(context) = keepalive_completion
                && let Some(response_json) = keepalive_response.into_value()
            {
                keepalive.on_response_completed(
                    &response_json,
                    &context.principal,
                    context.accounting_key_id,
                    &context.cache_metadata,
                    context.upstream_id,
                    context.shaped_body,
                    &context.downstream_headers,
                    message_start_at.map_or(relay_start.elapsed(), |anchor| anchor.elapsed()),
                );
            }
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
            // The stream reached its terminal outcome: hand the reservation to the
            // reconcile subscriber, which reconciles a measured success and refunds
            // upstream/affinity/transform errors on `RequestTerminated`. A client
            // cancel drops the stream before this point and refunds via RAII.
            if let Some(response_accounting_guard) = response_accounting_guard {
                response_accounting_guard.forget();
            }
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
            let finalize_ms = if let Some(o) = observer.as_ref() {
                o.set_usage_counts(&usage, message_stop_at.is_some());
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
                        }),
                    });
                } else {
                    o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::StreamCompleted {
                        event_id: o.event_id().to_owned(),
                        result: Ok(cc_lb_lifecycle::StreamSuccess {
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
                    event_ctx.proxy_setup_ms,
                    Some(stream_total_ms),
                    elapsed_ms(first_chunk_at),
                );
                if upstream_error_status
                    && !upstream_error_body_decode_failed
                    && !upstream_error_body.is_empty()
                {
                    let (error_type, message) = usage_parser::observe_http_error_body(&upstream_error_body)
                        .map(|error| error.into_parts(signer.as_ref()))
                        .unwrap_or_default();
                    o.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::RequestLogUpstreamErrorObserved {
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
                    });
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
                } else if let Some(stop) = stream_abnormal_stop.as_ref()
                    && !upstream_error_status
                    && !stream_provider_error_seen
                {
                    o.emit_lifecycle(
                        cc_lb_lifecycle::LifecycleEvent::RequestLogUpstreamErrorObserved {
                            event_id: o.event_id().to_owned(),
                            error_type: stop.stop_reason.clone(),
                            error_message: abnormal_stop_error_message(stop),
                        },
                    );
                    let code = match stop.stop_reason.as_str() {
                        "refusal" => UpstreamErrorCode::Refusal,
                        _ => UpstreamErrorCode::ContextWindowExceeded,
                    };
                    o.set_upstream_error(status, code);
                } else if !upstream_error_status {
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
    parse_anthropic_rate_limit_headers(headers)
        .into_iter()
        .chain(parse_openai_rate_limit_headers(headers))
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
    let openai_oauth_default = openai_oauth && base_url.is_none();
    let base_url = base_url.cloned().unwrap_or_else(|| match upstream {
        Upstream::AnthropicDirect { .. } => default_base_url(StorageUpstreamKind::AnthropicApiKey),
        Upstream::OpenaiDirect { .. } => {
            if openai_oauth_default && ctx.path == "/v1/responses" {
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
            openai_oauth_default,
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
    openai_oauth_default: bool,
}

impl UpstreamDialect for RawPassthroughDialect {
    fn shape(
        &self,
        context: &DialectShapeContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        let url = if self.openai_oauth && context.path == "/v1/responses" {
            let mut url = self.base_url.clone();
            let path = format!("{}/responses", url.path().trim_end_matches('/'));
            url.set_path(&path);
            url.set_query(context.query.as_deref());
            url
        } else {
            let base_url = if self.openai_oauth_default {
                default_base_url(StorageUpstreamKind::OpenaiApiKey)
            } else {
                self.base_url.clone()
            };
            cc_lb_upstream::upstream_request_url(&base_url, &context.path, context.query.as_deref())
        };
        Ok(builder.shaped_request(
            url,
            context.method.clone(),
            context.downstream_headers.clone(),
            context.body_bytes.clone(),
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
/// (we only copy the fields we explicitly whitelist). For Anthropic routes,
/// `anthropic_headers` carries `anthropic-ratelimit-*` and identity slots; for
/// OpenAI routes, it carries `x-ratelimit-*` fields. Downstream subscribers
/// reconstruct a `HeaderMap` and parse into typed observations.
fn header_snapshot_from(headers: &HeaderMap, upstream_kind: StorageUpstreamKind) -> HeaderSnapshot {
    let mut anthropic_headers: std::collections::BTreeMap<String, String> =
        std::collections::BTreeMap::new();
    let is_openai = matches!(
        upstream_kind,
        StorageUpstreamKind::OpenaiApiKey | StorageUpstreamKind::OpenaiOauth
    );
    for (name, value) in headers.iter() {
        let name_lc = name.as_str();
        let Ok(val) = value.to_str() else { continue };
        let should_capture = if is_openai {
            name_lc.starts_with("x-ratelimit-")
        } else {
            name_lc.starts_with("anthropic-ratelimit-")
                || cc_lb_domain::ANTHROPIC_IDENTITY_HEADERS.contains(&name_lc)
        };
        if should_capture {
            anthropic_headers.insert(name_lc.to_owned(), val.to_owned());
        }
    }
    HeaderSnapshot {
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

    fn limit_request(&self, fallback_model: &str) -> LimitRequest {
        LimitRequest::from_value(self.value(), fallback_model)
    }
}

#[derive(Clone, Debug)]
struct LimitRequest {
    model: String,
    max_tokens: i64,
    stream: bool,
    service_tier: Option<String>,
}

impl LimitRequest {
    /// `fallback_model` is the request's canonical model id. It is used only
    /// when the body carries no `model` (e.g. Gemini, where the model lives in
    /// the request path); a body model always wins verbatim.
    fn from_value(value: Option<&Value>, fallback_model: &str) -> Self {
        Self {
            model: value
                .and_then(|v| v.get("model"))
                .and_then(Value::as_str)
                .or_else(|| (!fallback_model.is_empty()).then_some(fallback_model))
                .unwrap_or("unknown")
                .to_owned(),
            max_tokens: value
                .and_then(|v| {
                    v.get("max_tokens")
                        .or_else(|| v.get("max_output_tokens"))
                        .and_then(Value::as_i64)
                        .or_else(|| {
                            v.get("generationConfig")
                                .and_then(|config| config.get("maxOutputTokens"))
                                .and_then(Value::as_i64)
                        })
                })
                .unwrap_or(0),
            stream: value
                .and_then(|v| v.get("stream"))
                .and_then(Value::as_bool)
                .unwrap_or(false),
            service_tier: value
                .and_then(|v| v.get("service_tier"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
        }
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
    GEMINI_OAUTH_RESPONSE_TRANSFORM, ResponseTransformContext, SseTransformOutcome,
    apply_buffered_transform_result, make_response_transform_error_frame,
    sanitize_downstream_response_headers, sanitize_downstream_stream_headers,
    sanitized_response_headers_for_plugin, transform_sse_event_bytes,
    unwrap_gemini_code_assist_body,
};

#[derive(Clone, Default)]
pub(crate) struct RequestCacheMetadata {
    pub(crate) request_json: Option<Value>,
    pub(crate) thread_id: Option<String>,
    pub(crate) observed_session_id: Option<String>,
    pub(crate) request_kind: Option<String>,
    pub(crate) claude_agent_id: Option<String>,
    pub(crate) claude_parent_agent_id: Option<String>,
    pub(crate) parent_session_id: Option<String>,
    pub(crate) client_app: Option<String>,
    pub(crate) session_id_source: Option<String>,
    requested_service_tier: Option<String>,
    message_id: Option<String>,
    message_index: Option<u64>,
    message_count: Option<u64>,
    cache_control_block_count: Option<u64>,
    thinking_budget_tokens: Option<u64>,
    reasoning_effort: Option<String>,
    cache_control_message_indices: Vec<u64>,
    pub(crate) cache_breakpoints: Vec<RequestCacheBreakpoint>,
    cacheable_breakpoint_prefix_keys: HashSet<String>,
    pub(crate) cache_prefix_hash: Option<String>,
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
                // `request_cache_metadata_from_value_with_analysis`).
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
            timings: PromptCacheAnalysisTimings::default(),
        }
    });
    request_cache_metadata_from_value_with_analysis(headers, value, analysis)
}

fn request_cache_metadata_from_value_with_analysis(
    headers: &HeaderMap,
    value: Option<&Value>,
    analysis: Option<PromptCacheAnalysisOutput>,
) -> RequestCacheMetadata {
    let metadata = value.map(metadata_identity).unwrap_or_default();
    let (observed_session_id, session_id_source) =
        match request_session_identity(headers, &metadata, value) {
            Some((session_id, source)) => (Some(session_id), Some(source.to_owned())),
            None => (None, None),
        };
    let thread_id = observed_session_id.clone();
    let claude_agent_id = header_identity(headers, "x-claude-code-agent-id");
    let claude_parent_agent_id = header_identity(headers, "x-claude-code-parent-agent-id");
    let client_app = header_identity(headers, "x-app");
    let parent_session_id = header_identity(headers, "x-parent-session-id")
        .or_else(|| metadata.parent_session_id.clone());
    let request_kind = classify_client_request_kind(
        value,
        observed_session_id.as_deref(),
        parent_session_id.as_deref(),
        claude_agent_id.is_some(),
    )
    .map(str::to_owned);

    let Some(value) = value else {
        return RequestCacheMetadata {
            thread_id,
            observed_session_id,
            request_kind,
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
    let requested_service_tier = value
        .get("service_tier")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);

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
        timings: PromptCacheAnalysisTimings::default(),
    });
    let cacheable_breakpoint_prefix_keys = output
        .cacheable_breakpoint_prefix_keys
        .into_iter()
        .collect();
    let analysis = output.analysis;
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
        })
        .collect::<Vec<_>>();
    let mut cache_control_message_indices = cache_breakpoints
        .iter()
        .filter_map(|breakpoint| breakpoint.message_index)
        .collect::<Vec<_>>();
    cache_control_message_indices.sort_unstable();
    cache_control_message_indices.dedup();

    let cache_control_block_count = cache_breakpoints.len() as u64;
    let thinking_budget_tokens = value
        .get("thinking")
        .filter(|t| t.get("type").and_then(Value::as_str) == Some("enabled"))
        .and_then(|t| t.get("budget_tokens"))
        .and_then(Value::as_u64)
        .filter(|v| *v <= 9_007_199_254_740_991);
    let reasoning_effort = value
        .get("output_config")
        .and_then(|c| c.get("effort"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty() && s.len() <= 64)
        .map(str::to_owned);
    let cache_prefix_hash = cache_breakpoints
        .last()
        .map(|breakpoint| breakpoint.prefix_hash.clone());

    RequestCacheMetadata {
        request_json: None,
        thread_id,
        observed_session_id,
        request_kind,
        claude_agent_id,
        claude_parent_agent_id,
        parent_session_id,
        client_app,
        session_id_source,
        requested_service_tier,
        message_id,
        message_index,
        message_count,
        cache_control_block_count: Some(cache_control_block_count),
        thinking_budget_tokens,
        reasoning_effort,
        cache_control_message_indices,
        cache_breakpoints,
        cacheable_breakpoint_prefix_keys,
        cache_prefix_hash,
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

fn metadata_identity(value: &Value) -> MetadataIdentity {
    let Some(user_id) = value
        .get("metadata")
        .and_then(|metadata| metadata.get("user_id"))
        .and_then(Value::as_str)
    else {
        return MetadataIdentity::default();
    };

    let Ok(parsed) = sonic_rs::from_str::<Value>(user_id) else {
        return MetadataIdentity::default();
    };
    MetadataIdentity {
        session_id: parsed
            .get("session_id")
            .and_then(Value::as_str)
            .and_then(bounded_identity),
        parent_session_id: parsed
            .get("parent_session_id")
            .and_then(Value::as_str)
            .and_then(bounded_identity),
    }
}

fn bounded_identity(value: &str) -> Option<String> {
    (!value.is_empty() && value.len() <= MAX_IDENTITY_VALUE_BYTES).then(|| value.to_owned())
}

fn header_identity(headers: &HeaderMap, name: &str) -> Option<String> {
    header_to_string(headers, name).and_then(|value| bounded_identity(&value))
}

fn request_session_identity(
    headers: &HeaderMap,
    metadata: &MetadataIdentity,
    value: Option<&Value>,
) -> Option<(String, &'static str)> {
    const SESSION_HEADERS: [&str; 5] = [
        "x-claude-code-session-id",
        "x-claude-session-id",
        "x-hermes-session-id",
        "x-session-affinity",
        "x-session-id",
    ];

    SESSION_HEADERS
        .into_iter()
        .find_map(|header| header_identity(headers, header).map(|value| (value, header)))
        .or_else(|| value.and_then(hermes_body_session_identity))
        .or_else(|| {
            metadata
                .session_id
                .clone()
                .map(|session_id| (session_id, "metadata.user_id"))
        })
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
        RejectReason::UnsupportedLimitWindow { window_secs } => (
            StatusCode::SERVICE_UNAVAILABLE,
            "server_error",
            format!("API key limit window {window_secs}s is unsupported"),
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
        | RejectReason::KeyRevoked
        | RejectReason::Expired
        | RejectReason::ModelNotAllowed
        | RejectReason::CostUnavailable
        | RejectReason::UnsupportedLimitWindow { .. }
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

fn upstream_for_record(record: &UpstreamRecord) -> Result<Upstream, String> {
    match record.kind {
        StorageUpstreamKind::AnthropicApiKey | StorageUpstreamKind::AnthropicOauth => {
            Ok(Upstream::AnthropicDirect {
                base_url: record.base_url.clone(),
            })
        }
        StorageUpstreamKind::OpenaiOauth | StorageUpstreamKind::OpenaiApiKey => {
            Ok(Upstream::OpenaiDirect {
                base_url: record.base_url.clone(),
            })
        }
        StorageUpstreamKind::GeminiApiKey | StorageUpstreamKind::GeminiOauth => {
            Ok(Upstream::GeminiDirect {
                base_url: record.base_url.clone(),
            })
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
            route_ms,
        }
    }
}

fn route_for_record(
    record: &UpstreamRecord,
    cached: &PrincipalSpecCached,
) -> Result<(RouteDecision, Option<Url>), String> {
    let raw_passthrough_base_url = record.base_url.clone().or_else(|| {
        (record.kind == StorageUpstreamKind::GeminiOauth).then(|| default_base_url(record.kind))
    });
    let route_dialect: Arc<dyn UpstreamDialect> = Arc::new(RawPassthroughDialect {
        base_url: raw_passthrough_base_url
            .clone()
            .unwrap_or_else(|| default_base_url(record.kind)),
        openai_oauth: record.kind == StorageUpstreamKind::OpenaiOauth,
        openai_oauth_default: record.kind == StorageUpstreamKind::OpenaiOauth
            && record.base_url.is_none(),
    });
    let upstream = upstream_for_record(record)?;
    let dialect = cached.resolved_dialect(&route_dialect).clone();
    Ok((
        RouteDecision {
            upstream_id: record.id,
            upstream,
            dialect,
        },
        raw_passthrough_base_url,
    ))
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
        let (mut candidates, mut selected_cache_matches) = build_candidates_with_matches(
            self.view,
            &self.principal.id,
            self.request_kind,
            &self.ctx.cache_breakpoints,
            self.warm_entries,
            &*lifecycle.clock,
        );
        candidates.retain(|candidate| !self.excluded.contains(&candidate.upstream_id));
        // The last allowed attempt goes to an API key when one remains, so
        // the attempt cap never strands a request on exhausted OAuth accounts.
        if self.request_kind == RequestKind::AnthropicMessages {
            let last_attempt = self.upstream_attempts + 1 == MAX_UPSTREAM_ATTEMPTS;
            if last_attempt
                && candidates
                    .iter()
                    .any(|candidate| candidate.kind == CandidateUpstreamKind::AnthropicApiKey)
            {
                candidates
                    .retain(|candidate| candidate.kind == CandidateUpstreamKind::AnthropicApiKey);
            }
        }
        if candidates.is_empty() {
            return None;
        }
        let mut pipeline_result = execute_filter_pipeline(
            &self.router_pipeline.user_filters,
            self.ctx,
            self.principal,
            candidates,
        );
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
        if self
            .router_pipeline
            .user_filters
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
        );
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

fn to_usage_snapshot(u: &UsageCounts) -> cc_lb_lifecycle::UsageSnapshot {
    cc_lb_lifecycle::UsageSnapshot {
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
    fn anthropic_upstream_kinds_map_to_messages_over_http() {
        let expected = (ProtocolId::AnthropicMessages, TransportId::Http);

        assert_eq!(ids_for(StorageRecordKind::AnthropicApiKey), expected);
        assert_eq!(ids_for(StorageRecordKind::AnthropicOauth), expected);
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
            sink: cache.clone(),
            grace_margin_secs: cache.grace_margin_secs(),
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
            sink: cache.clone(),
            grace_margin_secs: cache.grace_margin_secs(),
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
            sink: cache.clone(),
            grace_margin_secs: cache.grace_margin_secs(),
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
            sink: cache.clone(),
            grace_margin_secs: cache.grace_margin_secs(),
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
        let cache = Arc::new(RecordingPromptCacheObservationSink::new(Vec::new()));
        let context = PromptCacheObservationContext {
            upstream_id,
            canonical_model_id: TEST_MODEL.to_owned(),
            cache_breakpoints: vec![cache_breakpoint(0, "tiny", 1_023, TtlClass::Ephemeral5m)],
            cacheable_breakpoint_prefix_keys: HashSet::new(),
            selected_match: None,
            sink: cache.clone(),
            grace_margin_secs: cache.grace_margin_secs(),
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
            sink: cache.clone(),
            grace_margin_secs: cache.grace_margin_secs(),
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
            sink: cache.clone(),
            grace_margin_secs: cache.grace_margin_secs(),
        };
        let usage = PromptCacheUsage {
            cache_creation_input_tokens: 800,
            cache_read_input_tokens: 2_400,
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
            sink: cache.clone(),
            grace_margin_secs: cache.grace_margin_secs(),
        };

        let decoded = decode_prompt_cache_observations_pure(
            &context,
            PromptCacheUsage {
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 2_000,
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
            sink: cache.clone(),
            grace_margin_secs: cache.grace_margin_secs(),
        };

        let decoded = decode_prompt_cache_observations_pure(
            &context,
            PromptCacheUsage {
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 2_000,
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
            sink: cache.clone(),
            grace_margin_secs: cache.grace_margin_secs(),
        };

        let decoded = decode_prompt_cache_observations_pure(
            &context,
            PromptCacheUsage {
                cache_creation_input_tokens: 2_400,
                cache_read_input_tokens: 0,
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
    fn read_positive_no_predict_records_unknown() {
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
            sink: cache.clone(),
            grace_margin_secs: cache.grace_margin_secs(),
        };

        let decoded = decode_prompt_cache_observations_pure(
            &context,
            PromptCacheUsage {
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 2_400,
            },
            now,
        );

        assert!(
            decoded.observations.is_empty(),
            "a provider read with no predicted match must not fabricate a Hit key"
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
            sink: cache.clone(),
            grace_margin_secs: cache.grace_margin_secs(),
        };

        let decoded = decode_prompt_cache_observations_pure(
            &context,
            PromptCacheUsage {
                cache_creation_input_tokens: 3_000,
                cache_read_input_tokens: 2_500,
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
            sink: cache.clone(),
            grace_margin_secs: cache.grace_margin_secs(),
        };

        let decoded = decode_prompt_cache_observations_pure(
            &context,
            PromptCacheUsage {
                cache_creation_input_tokens: 3_600,
                cache_read_input_tokens: 2_400,
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
        clock_now: u64,
        grace_secs: u64,
    }
    impl RecordingPromptCacheObservationSink {
        fn new(_entries: Vec<WarmCacheEntry>) -> Self {
            Self {
                records: Mutex::new(Vec::new()),
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
    }
    impl FakePromptCacheStore {
        fn new(records: Vec<PromptCacheObservationRecord>) -> Self {
            Self {
                records: Mutex::new(records),
                lookups: AtomicUsize::new(0),
            }
        }
        fn lookup_count(&self) -> usize {
            self.lookups.load(Ordering::Relaxed)
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
        let (observer, request_id) = lifecycle.websocket_observer();
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
        let (observer, next_request_id) = lifecycle.websocket_observer();
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
                        frame: &frame
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
        let (observer, request_id) = lifecycle.websocket_observer();
        let turn = lifecycle
            .prepare_websocket_turn(
                &auth,
                WebSocketIngress {
                    headers: request.headers(),
                    frame: &frame,
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
                },
                body.as_ref(),
                owner,
            )
            .await
            .expect_err("a retained connection does not bypass changed filter eligibility");
        assert_eq!(error.code, error_codes::UPSTREAM_AFFINITY_UNAVAILABLE);
        assert!(drain_events(&mut events).is_empty());
        let (observer, request_id) = lifecycle.websocket_observer();
        assert!(
            lifecycle
                .prepare_websocket_turn(
                    &auth,
                    WebSocketIngress {
                        headers: request.headers(),
                        frame: &frame
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
            let (observer, request_id) = lifecycle.websocket_observer();
            let prepared = lifecycle
                .prepare_websocket_turn(
                    &auth,
                    WebSocketIngress {
                        headers: request.headers(),
                        frame: &frame,
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
            let (observer, request_id) = lifecycle.websocket_observer();
            let turn = lifecycle
                .prepare_websocket_turn(
                    &auth,
                    WebSocketIngress {
                        headers: request.headers(),
                        frame: &frame,
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
        let response =
            lifecycle.reject_responses_websocket_authentication(&BuiltinAuthError::InvalidFormat);
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
}
