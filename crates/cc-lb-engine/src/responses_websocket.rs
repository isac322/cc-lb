//! Responses WebSocket admission and per-response ownership.
//!
//! Transport I/O lives in the server. This single-owner state machine keeps
//! connection-local continuation separate from durable response ownership.
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use cc_lb_control::api_keys::builtin_authn::BuiltinAuthError;
use cc_lb_control::api_keys::limit_engine::{LimitEngine, Reservation};
use cc_lb_lifecycle::{LifecycleEvent, UsageSnapshot, UsageSource};
use cc_lb_storage_api::UpstreamAffinityKey;
use http::{HeaderMap, StatusCode};
use serde_json::Value;
use sonic_rs::JsonValueTrait as _;
use tracing::Span;
use url::Url;
use uuid::Uuid;

use crate::authn_rail::Authenticated;
use crate::completion_observer::{StreamCompletionLog, StreamLatency};
use crate::downstream_stream_drop_guard::record_stream_termination_metric;
use crate::lifecycle::{Lifecycle, PromptCacheObservationContext, WebSocketTurnOwner};
use crate::request_context::RequestContext;
use cc_lb_domain::{CacheBreakpoint, Principal};

fn is_prewarm_body(body: &serde_json::Value) -> bool {
    body.get("generate") == Some(&serde_json::Value::Bool(false))
}
use crate::response_transform::ResponseTransformContext;
use crate::terminal_observer::{
    InternalFailure, LifecycleContext, StreamTerminationCause, StreamTerminationOutcome,
    UpstreamErrorCode, error_codes, record_websocket_terminal_span,
};
use crate::upstream_affinity::{
    input_has_compaction_item, is_openai_invalid_encrypted_content_error_value,
    openai_responses_has_account_owned_reference, strip_openai_reasoning_encrypted_items,
};
use crate::usage_parser::{
    ServiceTierMergePolicy, UsageCounts, observe_non_stream_json_value_for_shape_with_policy,
};
use cc_lb_upstream::{Signer, SseEvent, TransformSseEventRequest, TransformSseEventResult};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WebSocketWireFrame {
    pub bytes: Bytes,
    pub text: bool,
}

#[derive(Clone, Debug, Default)]
struct WebSocketErrorExtra {
    response_id: Option<Box<str>>,
    turn_id: Option<u64>,
    provider_headers: Box<[(String, String)]>,
    raw_provider_frames: Option<Box<[WebSocketWireFrame]>>,
}

#[derive(Clone, Debug)]
pub struct WebSocketError {
    pub status: u16,
    pub code: &'static str,
    // Rare detail fields are boxed so every `Result<_, WebSocketError>` stays small.
    pub native_code: Option<Box<str>>,
    pub message: String,
    pub stream_id: Option<String>,
    pub param: Option<Box<str>>,
    extra: Option<Box<WebSocketErrorExtra>>,
}

impl WebSocketError {
    pub fn new(status: u16, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            param: None,
            native_code: None,
            message: message.into(),
            stream_id: None,
            extra: None,
        }
    }

    pub fn with_native_code(mut self, code: impl Into<String>) -> Self {
        self.native_code = Some(code.into().into_boxed_str());
        self
    }

    pub fn with_param(mut self, param: impl Into<String>) -> Self {
        self.param = Some(param.into().into_boxed_str());
        self
    }

    pub fn with_stream(mut self, stream_id: Option<String>) -> Self {
        self.stream_id = stream_id;
        self
    }

    pub fn with_response_id(mut self, response_id: impl Into<String>) -> Self {
        self.extra.get_or_insert_with(Default::default).response_id =
            Some(response_id.into().into_boxed_str());
        self
    }

    pub fn response_id(&self) -> Option<&str> {
        self.extra
            .as_deref()
            .and_then(|extra| extra.response_id.as_deref())
    }

    pub fn with_turn_id(mut self, turn_id: u64) -> Self {
        self.extra.get_or_insert_with(Default::default).turn_id = Some(turn_id);
        self
    }

    pub fn turn_id(&self) -> Option<u64> {
        self.extra.as_deref().and_then(|extra| extra.turn_id)
    }

    pub(crate) fn provider_headers(&self) -> &[(String, String)] {
        self.extra
            .as_deref()
            .map_or(&[], |extra| &extra.provider_headers)
    }

    /// Retain only bounded, non-sensitive provider response headers.
    pub fn with_provider_headers(mut self, headers: &HeaderMap) -> Self {
        let mut retained = self.extra.as_mut().map_or_else(Vec::new, |extra| {
            std::mem::take(&mut extra.provider_headers).into_vec()
        });
        for (name, value) in headers {
            if retained.len() >= 32 {
                break;
            }
            let name = name.as_str().to_ascii_lowercase();
            let is_codex_quota = name.starts_with("x-codex-primary-")
                || name.starts_with("x-codex-secondary-")
                || name.starts_with("x-codex-credits-")
                || name == "x-codex-active-limit"
                || name == "x-codex-plan-type";
            if name != "retry-after"
                && !name.starts_with("x-ratelimit-")
                && !name.starts_with("anthropic-ratelimit-")
                && !name.starts_with("ratelimit-")
                && !is_codex_quota
            {
                continue;
            }
            let Ok(value) = value.to_str() else {
                continue;
            };
            if value.len() > 1024 {
                continue;
            }
            retained.push((name, value.to_owned()));
        }
        if !retained.is_empty() {
            self.extra
                .get_or_insert_with(Default::default)
                .provider_headers = retained.into_boxed_slice();
        }
        self
    }

    pub fn with_raw_provider_frames(mut self, frames: Vec<WebSocketWireFrame>) -> Self {
        self.extra
            .get_or_insert_with(Default::default)
            .raw_provider_frames = Some(frames.into_boxed_slice());
        self
    }

    pub fn raw_provider_frames(&self) -> Option<&[WebSocketWireFrame]> {
        self.extra
            .as_deref()
            .and_then(|extra| extra.raw_provider_frames.as_deref())
    }

    pub fn to_bytes(&self) -> Bytes {
        if let Some([single]) = self.raw_provider_frames()
            && std::str::from_utf8(&single.bytes).is_ok()
        {
            return single.bytes.clone();
        }
        let code = self.native_code.as_deref().unwrap_or(self.code);
        let mut error = serde_json::json!({
            "type": "error",
            "status": self.status,
            "error": {
                "type": if self.code == "authentication_error" { "authentication_error" }
                    else if self.status == 429 { "rate_limit_error" }
                    else if self.status < 500 { "invalid_request_error" }
                    else { "server_error" },
                "code": code,
                "message": self.message,
                "param": self.param,
            },
        });
        if let Some(response_id) = self.response_id() {
            error["response_id"] = Value::String(response_id.to_owned());
        }
        if let Some(stream_id) = self.stream_id.as_ref() {
            error["stream_id"] = Value::String(stream_id.clone());
        }
        let provider_headers = self.provider_headers();
        if !provider_headers.is_empty() {
            let headers = provider_headers
                .iter()
                .map(|(name, value)| (name.clone(), Value::String(value.clone())))
                .collect::<BTreeMap<_, _>>();
            error["headers"] = serde_json::to_value(headers)
                .expect("serialized provider headers contain only JSON values");
        }
        Bytes::from(serde_json::to_vec(&error).expect("WebSocket error contains only JSON values"))
    }
    pub fn copy_provider_headers(&self, headers: &mut http::HeaderMap) {
        for (name, value) in self.provider_headers() {
            if let (Ok(name), Ok(value)) = (
                http::HeaderName::from_bytes(name.as_bytes()),
                http::HeaderValue::from_str(value),
            ) {
                headers.append(name, value);
            }
        }
    }
}

impl fmt::Display for WebSocketError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for WebSocketError {}

#[derive(Clone, Debug)]
pub struct WebSocketCandidate {
    pub upstream_id: Uuid,
    pub upstream_name: String,
    /// Codex subscription sockets carry one `stream_id` lane each, so a lane
    /// needs its own provider socket. Native sockets multiplex lanes.
    pub socket_per_lane: bool,
    pub is_api_key: bool,
}

/// Borrowed transport inputs shared by one parsed logical turn.
#[derive(Clone, Copy)]
pub(crate) struct WebSocketIngress<'a> {
    pub headers: &'a HeaderMap,
    pub frame: &'a Bytes,
    pub query: Option<&'a str>,
}

#[derive(Clone)]
pub(crate) struct DeferredWebSocketLimitContext {
    pub(crate) ctx: RequestContext,
    pub(crate) principal: Principal,
    pub(crate) cache_breakpoints: Vec<CacheBreakpoint>,
}

pub(crate) enum WsUnknownReasoningPlan {
    None,
    /// Multiple dispatchable upstreams without an active session pin or other
    /// pinning-eligible turns/transports: strip upfront and adjust admission estimate.
    StrippedUpfront {
        body: Arc<Value>,
        frame: Bytes,
        owner_failover: bool,
        requires_api_key: bool,
    },
    /// Resolved or single dispatchable upstream: send verbatim first and retry once
    /// stripping all encrypted reasoning items on HTTP-400 `invalid_encrypted_content`.
    RetryStripAll {
        unknown_keys: Vec<UpstreamAffinityKey>,
    },
    /// Multi-upstream unpinned turn that can narrow to a single OAuth session owner
    /// (or switch between OAuth and API-key transports) before send: defer final
    /// encrypted-reasoning normalization and limit estimation until `validate_candidate_for_send`.
    DeferredUnestablished {
        unknown_keys: Vec<UpstreamAffinityKey>,
    },
    /// Resolved owner with unmatched pending reasoning digests: send verbatim first;
    /// retry once stripping only `pending_digests` on HTTP-400 `invalid_encrypted_content`
    /// before any response frame is emitted.
    RetryStripPending {
        pending_digests: HashSet<[u8; 32]>,
        pending_keys: Vec<UpstreamAffinityKey>,
    },
}

struct DeferredUnestablishedReasoning {
    original_body: Arc<Value>,
    original_frame: Bytes,
    original_bytes: usize,
    stripped_extra_bytes: usize,
    unknown_keys: Vec<UpstreamAffinityKey>,
    currently_stripped: bool,
}

enum WsUnknownReasoningRetry {
    All,
    Pending(HashSet<[u8; 32]>),
}

pub(crate) struct WsOwnerFailoverFallback {
    pub(crate) owner_upstream_id: Uuid,
    pub(crate) candidates: Vec<WebSocketCandidate>,
    pub(crate) requires_api_key: bool,
}

pub(crate) struct PreparedWebSocketTurn {
    pub candidates: Vec<WebSocketCandidate>,
    pub observer: Option<LifecycleContext>,
    pub completion_observer: Arc<crate::completion_observer::CompletionObserver>,
    pub prompt_cache_observation_context: Option<PromptCacheObservationContext>,
    pub reservation: Option<Reservation>,
    pub deferred_limit_context: Option<DeferredWebSocketLimitContext>,
    pub limit_engine: Option<Arc<LimitEngine>>,
    pub principal_id: String,
    pub key_id: String,
    pub model: String,
    pub oauth_transport: bool,
    /// The client frame needed a public-shape transform; provider frames are
    /// filtered for this turn. Codex-shaped turns keep this false.
    pub oauth_compat: bool,
    pub explicit_store: Option<bool>,
    pub store: bool,
    pub span: Span,
    pub route_info: cc_lb_lifecycle::RouteInfo,
    pub setup_timings: cc_lb_lifecycle::RequestSetupTimings,
    pub request_id: String,
    pub unknown_reasoning_plan: WsUnknownReasoningPlan,
    pub has_explicit_affinity: bool,
    pub owner_failover_fallback: Option<WsOwnerFailoverFallback>,
    pub reconciled_body: Option<(Arc<Value>, Bytes)>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ResponsesDispatchTimings {
    pub connection_reused: Option<bool>,
    pub bulkhead_wait_ms: Option<u64>,
    pub dns_ms: Option<u64>,
    pub connect_ms: Option<u64>,
}

impl ResponsesDispatchTimings {
    pub const fn reused() -> Self {
        Self {
            connection_reused: Some(true),
            bulkhead_wait_ms: None,
            dns_ms: None,
            connect_ms: None,
        }
    }
}

pub(crate) struct WebSocketPluginRequest {
    pub frame: Bytes,
    pub headers: HeaderMap,
    pub context: ResponseTransformContext,
    pub oauth_compat: bool,
    pub echo_metadata: Option<Value>,
    pub sent_service_tier: Option<String>,
}

#[derive(Clone, Debug)]
pub struct WebSocketDispatch {
    pub turn_id: u64,
    pub frame: Bytes,
    pub connection_id: Option<u64>,
    pub candidates: Vec<WebSocketCandidate>,
    pub stream_id: Option<String>,
    /// Marks a pass-through control frame for an already-owned response
    /// (for example, `response.interrupt`) rather than a response-creating
    /// or steering-successor turn.
    pub control: bool,
    pub prefer_api_key_on_final_attempt: bool,
    pub has_explicit_affinity: bool,
    /// Marks a failover recovery dispatch whose original upstream failure was
    /// already suppressed after checking pool capacity.
    pub is_failover_recovery: bool,
}

/// Snapshot of in-flight relay dial slots outside `ResponsesWebSocketGateway`'s
/// established connection table, used to verify real slot capacity and
/// dispatch connect budgets before attempting live failover or pre-dispatch recovery.
#[derive(Debug, Default, Clone)]
pub struct WsRelayPoolSnapshot {
    pub pending_connects: usize,
    pub coalescable_upstreams: HashSet<Uuid>,
    pub reserved_connections: HashSet<u64>,
    pub excluded_upstreams: HashSet<Uuid>,
    pub dial_attempts_exhausted: bool,
    pub connect_budget_expired: bool,
}

#[derive(Default)]
pub struct WebSocketEventResult {
    pub forward: bool,
    /// Replacement text frames, in order; `None` retains the upstream frame.
    pub frames: Option<Vec<Bytes>>,
    pub dispatches: Vec<WebSocketDispatch>,
    pub errors: Vec<WebSocketError>,
    pub retire_connections: Vec<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TerminalDelivery {
    None,
    ErrorAwaitingResponseFailed { missing_status: bool },
    ResponseTerminalDelivered,
}

#[derive(Clone, Debug)]
struct ResponseOwner {
    connection_id: u64,
    upstream_id: Uuid,
    turn_id: u64,
    principal_id: String,
    body: Arc<Value>,
    lane: Option<String>,
    oauth_transport: bool,
    oauth_compat: bool,
    echo_metadata: Option<Value>,
    terminal_delivery: TerminalDelivery,
    evicted_for_continuation: bool,
}

fn sent_service_tier_bytes(tier: Option<&str>) -> usize {
    tier.map_or(0, str::len)
}

fn usage_service_tier_bytes(usage: Option<&UsageCounts>) -> usize {
    sent_service_tier_bytes(usage.and_then(|usage| usage.service_tier.as_deref()))
}

fn update_turn_usage_budget(
    used_bytes: &mut usize,
    byte_cap: usize,
    turn: &mut Turn,
    usage: UsageCounts,
    complete: bool,
) -> Result<(), Box<UsageCounts>> {
    let old_tier_bytes = usage_service_tier_bytes(turn.usage.as_ref());
    let new_tier_bytes = usage_service_tier_bytes(Some(&usage));
    if new_tier_bytes > old_tier_bytes {
        let delta = new_tier_bytes - old_tier_bytes;
        if delta > byte_cap.saturating_sub(*used_bytes) {
            return Err(Box::new(usage));
        }
        *used_bytes = (*used_bytes).saturating_add(delta);
        turn.charged_bytes = turn.charged_bytes.saturating_add(delta);
    } else {
        let release = (old_tier_bytes - new_tier_bytes).min(turn.charged_bytes);
        *used_bytes = (*used_bytes).saturating_sub(release);
        turn.charged_bytes = turn.charged_bytes.saturating_sub(release);
    }
    turn.usage = Some(usage);
    turn.usage_complete = complete;
    Ok(())
}

#[derive(Default)]
struct Lane {
    active: Option<u64>,
    queued: VecDeque<u64>,
}

fn duration_to_ms(duration: std::time::Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn ws_semantic_content_delta(frame: &[u8], event_type: &str) -> bool {
    if !matches!(
        event_type,
        "response.output_text.delta"
            | "response.function_call_arguments.delta"
            | "response.reasoning_summary_text.delta"
            | "response.refusal.delta"
    ) {
        return false;
    }
    sonic_rs::get_from_slice(frame, &["delta"])
        .ok()
        .is_some_and(|value| value.as_str().is_some_and(|text| !text.is_empty()))
}

#[derive(Debug)]
struct ResponsesStreamMetrics {
    turn_started: std::time::Instant,
    relay_started: Option<std::time::Instant>,
    send_started: Option<std::time::Instant>,
    proxy_setup_ms: Option<u64>,
    setup_timings: cc_lb_lifecycle::RequestSetupTimings,
    io_timing: crate::body_io_timing::BodyIoTiming,
    bulkhead_wait_ms: Option<u64>,
    dns_ms: Option<u64>,
    connect_ms: Option<u64>,
    connection_reused: Option<bool>,
    shape_ms: Option<u64>,
    sign_ms: Option<u64>,
    upstream_ttfb_ms: Option<u64>,
    upstream_started_emitted: bool,
    first_output_ms: Option<u64>,
    last_output_ms: Option<u64>,
    stream_message_start_ms: Option<u64>,
    stream_content_block_start_ms: Option<u64>,
    stream_first_content_delta_ms: Option<u64>,
    stream_last_content_delta_ms: Option<u64>,
    stream_message_stop_ms: Option<u64>,
    last_activity_heartbeat_at: std::time::Instant,
    event_count: u64,
    content_delta_count: u64,
    ping_count: u64,
    total_bytes: u64,
    provider_session_id: Option<String>,
}

struct ObservedFrameOutcome {
    heartbeat_due: bool,
    first_content_delta_ms: Option<u64>,
}

impl ResponsesStreamMetrics {
    #[cfg(test)]
    fn new() -> Self {
        Self::new_with_setup(
            std::time::Instant::now(),
            None,
            cc_lb_lifecycle::RequestSetupTimings::default(),
        )
    }

    fn new_with_setup(
        turn_started: std::time::Instant,
        proxy_setup_ms: Option<u64>,
        setup_timings: cc_lb_lifecycle::RequestSetupTimings,
    ) -> Self {
        Self {
            turn_started,
            relay_started: None,
            send_started: None,
            proxy_setup_ms,
            setup_timings,
            io_timing: crate::body_io_timing::BodyIoTiming::default(),
            bulkhead_wait_ms: None,
            dns_ms: None,
            connect_ms: None,
            connection_reused: None,
            shape_ms: None,
            sign_ms: None,
            upstream_ttfb_ms: None,
            upstream_started_emitted: false,
            first_output_ms: None,
            last_output_ms: None,
            stream_message_start_ms: None,
            stream_content_block_start_ms: None,
            stream_first_content_delta_ms: None,
            stream_last_content_delta_ms: None,
            stream_message_stop_ms: None,
            last_activity_heartbeat_at: turn_started,
            event_count: 0,
            content_delta_count: 0,
            ping_count: 0,
            total_bytes: 0,
            provider_session_id: None,
        }
    }

    fn record_dispatch_timings(&mut self, timings: ResponsesDispatchTimings) {
        if let Some(reused) = timings.connection_reused {
            self.connection_reused = Some(reused);
        }
        if let Some(wait) = timings.bulkhead_wait_ms {
            self.bulkhead_wait_ms = Some(wait);
        }
        if let Some(dns) = timings.dns_ms {
            self.dns_ms = Some(dns);
        }
        if let Some(connect) = timings.connect_ms {
            self.connect_ms = Some(connect);
        }
    }

    fn mark_sent(&mut self, timings: ResponsesDispatchTimings, send_started: std::time::Instant) {
        self.record_dispatch_timings(timings);
        self.send_started = Some(send_started);
    }

    fn reset_for_retry(&mut self) {
        self.relay_started = None;
        self.send_started = None;
        self.upstream_ttfb_ms = None;
        self.upstream_started_emitted = false;
        self.first_output_ms = None;
        self.last_output_ms = None;
        self.event_count = 0;
        self.total_bytes = 0;
    }

    fn maybe_record_heartbeat(
        &mut self,
        now: std::time::Instant,
        heartbeat_interval_ms: u64,
    ) -> bool {
        if heartbeat_interval_ms > 0
            && duration_to_ms(now.saturating_duration_since(self.last_activity_heartbeat_at))
                >= heartbeat_interval_ms
        {
            self.last_activity_heartbeat_at = now;
            true
        } else {
            false
        }
    }

    fn observe_heartbeat_only(&mut self, heartbeat_interval_ms: u64) -> bool {
        if heartbeat_interval_ms == 0 {
            return false;
        }
        self.maybe_record_heartbeat(std::time::Instant::now(), heartbeat_interval_ms)
    }

    #[cfg(test)]
    fn observe(&mut self, frame: &[u8], heartbeat_interval_ms: u64) -> bool {
        self.observe_at(frame, std::time::Instant::now(), heartbeat_interval_ms)
            .heartbeat_due
    }

    fn observe_at(
        &mut self,
        frame: &[u8],
        now: std::time::Instant,
        heartbeat_interval_ms: u64,
    ) -> ObservedFrameOutcome {
        let relay_start = match self.relay_started {
            Some(start) => start,
            None => {
                self.relay_started = Some(now);
                let first_frame_wait = self
                    .send_started
                    .map(|sent| duration_to_ms(now.saturating_duration_since(sent)))
                    .unwrap_or_else(|| {
                        duration_to_ms(now.saturating_duration_since(self.turn_started))
                    });
                self.upstream_ttfb_ms = Some(
                    self.bulkhead_wait_ms
                        .unwrap_or(0)
                        .saturating_add(self.dns_ms.unwrap_or(0))
                        .saturating_add(self.connect_ms.unwrap_or(0))
                        .saturating_add(first_frame_wait),
                );
                now
            }
        };
        self.io_timing
            .transition_at(Some(crate::body_io_timing::BodyIoPhase::Process), now);
        let elapsed = duration_to_ms(now.saturating_duration_since(relay_start));
        self.first_output_ms.get_or_insert(elapsed);
        self.last_output_ms = Some(elapsed);
        self.event_count = self.event_count.saturating_add(1);
        self.total_bytes = self.total_bytes.saturating_add(frame.len() as u64);

        let mut captured_first_delta = None;
        if let Ok(event_type_val) = sonic_rs::get_from_slice(frame, &["type"])
            && let Some(event_type) = event_type_val.as_str()
        {
            match event_type {
                "response.created" | "response.in_progress" => {
                    self.stream_message_start_ms.get_or_insert(elapsed);
                }
                "response.output_item.added" | "response.content_part.added" => {
                    self.stream_content_block_start_ms.get_or_insert(elapsed);
                }
                "response.completed" | "response.incomplete" | "response.failed" => {
                    if self.stream_message_stop_ms.is_none() {
                        self.stream_message_stop_ms = Some(elapsed);
                    }
                }
                other if ws_semantic_content_delta(frame, other) => {
                    if self.stream_first_content_delta_ms.is_none() {
                        self.stream_first_content_delta_ms = Some(elapsed);
                        captured_first_delta = Some(elapsed);
                    }
                    self.stream_last_content_delta_ms = Some(elapsed);
                    self.content_delta_count = self.content_delta_count.saturating_add(1);
                }
                _ => {}
            }
        }

        if self.provider_session_id.is_none() {
            let bounded = |value: Option<sonic_rs::LazyValue<'_>>| {
                let value = value.and_then(|value| value.as_str().map(str::to_owned))?;
                (!value.is_empty() && value.len() <= 512).then_some(value)
            };
            let response_id = sonic_rs::get_from_slice(frame, &["response", "id"]).ok();
            let fallback_id = sonic_rs::get_from_slice(frame, &["response_id"]).ok();
            self.provider_session_id = bounded(response_id).or_else(|| bounded(fallback_id));
        }
        ObservedFrameOutcome {
            heartbeat_due: self.maybe_record_heartbeat(now, heartbeat_interval_ms),
            first_content_delta_ms: captured_first_delta,
        }
    }

    fn latency(&self, status: u16) -> StreamLatency {
        let stream_total_ms = self
            .relay_started
            .map_or(0, |start| duration_to_ms(start.elapsed()));
        let inter_output_avg_ms = match (self.first_output_ms, self.last_output_ms) {
            (Some(first), Some(last)) if self.event_count > 1 => {
                Some(last.saturating_sub(first) / (self.event_count - 1))
            }
            _ => None,
        };
        let inter_token_avg_ms = match (
            self.stream_first_content_delta_ms,
            self.stream_last_content_delta_ms,
        ) {
            (Some(first), Some(last)) if self.content_delta_count > 1 => {
                Some(last.saturating_sub(first) / (self.content_delta_count - 1))
            }
            _ => None,
        };
        StreamLatency {
            status,
            stream_first_chunk_ms: self.first_output_ms,
            stream_message_start_ms: self.stream_message_start_ms,
            stream_content_block_start_ms: self.stream_content_block_start_ms,
            stream_first_content_delta_ms: self.stream_first_content_delta_ms,
            stream_last_content_delta_ms: self.stream_last_content_delta_ms,
            stream_message_stop_ms: self.stream_message_stop_ms,
            stream_last_chunk_ms: self.last_output_ms,
            stream_first_output_ms: self.first_output_ms,
            stream_last_output_ms: self.last_output_ms,
            stream_event_count: self.event_count,
            stream_inter_output_avg_ms: inter_output_avg_ms,
            provider_session_id: self.provider_session_id.clone(),
            provider_connection_id: None,
            stream_total_ms,
            sse_event_count: 0,
            content_delta_count: self.content_delta_count,
            ping_count: self.ping_count,
            inter_token_avg_ms,
            total_bytes: self.total_bytes,
        }
    }
}

type TransformProviderFrameResult =
    Result<(Option<u64>, Option<Vec<Bytes>>), Box<(Option<u64>, WebSocketError)>>;

const PENDING_ERROR_TERMINAL_WINDOW: std::time::Duration = std::time::Duration::from_secs(2);

struct PendingErrorTerminal {
    turn_id: u64,
    turn: Turn,
    error: Option<WebSocketError>,
    missing_status: bool,
    expires_at: std::time::Instant,
}

struct SuppressedFailoverError {
    error: WebSocketError,
    upstream_id: Option<Uuid>,
    upstream_name: String,
    previous_session_upstream: Option<Uuid>,
    charged_bytes: usize,
}

fn websocket_error_retained_bytes(error: &WebSocketError) -> usize {
    let extra = error.extra.as_deref().map_or(0, |extra| {
        std::mem::size_of::<WebSocketErrorExtra>()
            .saturating_add(extra.response_id.as_deref().map_or(0, str::len))
            .saturating_add(
                extra
                    .provider_headers
                    .iter()
                    .fold(0usize, |acc, (name, value)| {
                        acc.saturating_add(name.len()).saturating_add(value.len())
                    }),
            )
            .saturating_add(extra.raw_provider_frames.as_deref().map_or(0, |frames| {
                frames.iter().fold(0usize, |acc, frame| {
                    acc.saturating_add(std::mem::size_of::<WebSocketWireFrame>())
                        .saturating_add(frame.bytes.len())
                })
            }))
    });
    error
        .message
        .len()
        .saturating_add(error.native_code.as_deref().map_or(0, str::len))
        .saturating_add(error.stream_id.as_deref().map_or(0, str::len))
        .saturating_add(error.param.as_deref().map_or(0, str::len))
        .saturating_add(extra)
}

fn suppressed_failover_retained_bytes(error: &WebSocketError, upstream_name: &str) -> usize {
    std::mem::size_of::<SuppressedFailoverError>()
        .saturating_add(websocket_error_retained_bytes(error))
        .saturating_add(upstream_name.len())
}

struct Turn {
    auth: Authenticated,
    prepared: PreparedWebSocketTurn,
    prepared_signers: HashMap<Uuid, Arc<dyn Signer>>,
    initial_selected_upstream_id: Option<Uuid>,
    candidate_oauth: HashMap<Uuid, bool>,
    body: Arc<Value>,
    frame: Bytes,
    lane: Option<String>,
    connection_id: Option<u64>,
    upstream_id: Option<Uuid>,
    pinned: Option<(u64, Uuid)>,
    response_id: Option<String>,
    previous_response_id: Option<String>,
    steer_parent: Option<String>,
    steer_submissions: usize,
    accepted_steers: HashSet<String>,
    controls: VecDeque<Bytes>,
    sent: bool,
    attempted: bool,
    attempt_num: u32,
    charged_bytes: usize,
    usage: Option<UsageCounts>,
    usage_complete: bool,
    incomplete_reason: Option<Option<String>>,
    plugin_request: Option<(Uuid, WebSocketPluginRequest)>,
    sent_service_tier: Option<String>,
    metrics: ResponsesStreamMetrics,
    echo_metadata: Option<Value>,
    funding_ambiguous: bool,
    response_created: bool,
    provider_frame_forwarded: bool,
    unknown_reasoning_retry: Option<WsUnknownReasoningRetry>,
    unknown_reasoning_retried: bool,
    pending_affinity_keys: Vec<UpstreamAffinityKey>,
    deferred_unestablished: Option<DeferredUnestablishedReasoning>,
    has_any_oauth_candidate: bool,
    all_candidates_oauth: bool,
    admitted_session_upstream: Option<Uuid>,
    owner_failover_replacement: bool,
    owner_failover_requires_api_key: bool,
    owner_failover_fallback: Option<WsOwnerFailoverFallback>,
    live_failover_candidates: Vec<WebSocketCandidate>,
    suppressed_failover_error: Option<SuppressedFailoverError>,
}

impl Turn {
    #[cfg(test)]
    fn sent_service_tier(&self) -> Option<&str> {
        self.sent_service_tier.as_deref()
    }

    fn ensure_credential_eligible(&self, is_api_key: bool) -> Result<(), WebSocketError> {
        if self.owner_failover_requires_api_key && !is_api_key {
            return Err(WebSocketError::new(
                409,
                error_codes::UPSTREAM_AFFINITY_UNAVAILABLE,
                "This turn requires an OpenAI API-key upstream",
            )
            .with_stream(self.lane.clone()));
        }
        Ok(())
    }
}

/// Signed WebSocket connection metadata produced off the relay loop.
pub struct SignedWsConnection {
    pub url: Url,
    pub headers: HeaderMap,
    pub signer: Arc<dyn Signer>,
    pub prepare_signer_ms: f64,
    pub sign_ms: u64,
}

/// Self-contained connection preparation job executed inside a bounded connect
/// task so OAuth refresh and request signing never stall active WebSocket lanes.
pub struct PreparedWsConnectJob {
    lifecycle: Arc<Lifecycle>,
    turn_id: u64,
    candidate: WebSocketCandidate,
    ingress_headers: HeaderMap,
    sign_headers: HeaderMap,
    query: Option<String>,
    frame: Bytes,
    body: Arc<Value>,
    codex_backend: bool,
    reject_exhausted: bool,
    expected_principal_id: String,
    expected_key_id: String,
    reservation_id: Option<String>,
    deferred_limit_context: Option<DeferredWebSocketLimitContext>,
    observer: Option<LifecycleContext>,
}

impl PreparedWsConnectJob {
    pub fn turn_id(&self) -> u64 {
        self.turn_id
    }

    pub fn upstream_id(&self) -> Uuid {
        self.candidate.upstream_id
    }

    pub async fn run(self) -> Result<SignedWsConnection, WebSocketError> {
        let auth = self
            .lifecycle
            .authenticate(&self.ingress_headers)
            .await
            .map_err(|error| {
                WebSocketError::new(
                    error.http_status(),
                    "authentication_error",
                    error.to_string(),
                )
            })?;
        if auth.principal_id() != self.expected_principal_id
            || auth.key_id() != self.expected_key_id
        {
            return Err(WebSocketError::new(
                401,
                "authentication_error",
                "The client identity changed before connection",
            ));
        }
        self.lifecycle
            .validate_websocket_turn_with_policy(
                &auth,
                WebSocketIngress {
                    headers: &self.ingress_headers,
                    frame: &self.frame,
                    query: self.query.as_deref(),
                },
                &self.body,
                self.candidate.upstream_id,
                self.codex_backend,
                self.reject_exhausted,
            )
            .await?;
        if let (Some(deferred_ctx), Some(reservation_id)) = (
            self.deferred_limit_context.as_ref(),
            self.reservation_id.as_deref(),
        ) {
            self.lifecycle
                .resize_websocket_deferred_limit(
                    &auth,
                    &self.ingress_headers,
                    deferred_ctx,
                    self.candidate.upstream_id,
                    &self.body,
                    Some(reservation_id),
                    self.observer.as_ref(),
                )
                .await?;
        }
        let (url, headers, signer, prepare_signer_ms, sign_ms) = self
            .lifecycle
            .sign_websocket_connection_with_signer(
                &auth,
                &self.sign_headers,
                &self.candidate,
                self.query.as_deref(),
            )
            .await?;
        Ok(SignedWsConnection {
            url,
            headers,
            signer,
            prepare_signer_ms,
            sign_ms,
        })
    }
}

/// A session owns no authenticated proof until a fresh client check succeeds.
/// In-flight turns retain their original proof; a later revocation affects new
/// dispatches, not already admitted provider output.
pub struct ResponsesWebSocketGateway {
    lifecycle: Arc<Lifecycle>,
    provider: Option<cc_lb_domain::Provider>,
    codex_backend: bool,
    headers: HeaderMap,
    query: Option<String>,
    byte_cap: usize,
    slot_cap: usize,
    used_bytes: usize,
    control_slots: usize,
    next_turn_id: u64,
    lanes: HashMap<Option<String>, Lane>,
    turns: HashMap<u64, Turn>,
    pending_error_terminals: HashMap<String, PendingErrorTerminal>,
    pending_eviction_errors: Vec<WebSocketError>,
    retired_terminals: VecDeque<(u64, Option<String>, String)>,
    owners: HashMap<String, ResponseOwner>,
    recent: HashMap<(u64, Option<String>), String>,
    connections: HashMap<u64, Uuid>,
    connection_oauth: HashMap<u64, bool>,
    connection_lanes: HashMap<u64, Option<String>>,
    session_upstream: Option<Uuid>,
    session_oauth: bool,
    session_rate_limited_upstreams: HashSet<Uuid>,
    connection_signers: HashMap<u64, Arc<dyn Signer>>,
    successors: HashMap<String, u64>,
    activity_heartbeat_interval_ms: u64,
}
impl ResponsesWebSocketGateway {
    pub fn new(
        lifecycle: Arc<Lifecycle>,
        _authenticated: &Authenticated,
        original_headers: HeaderMap,
        budget_bytes: usize,
        slots: usize,
    ) -> Self {
        Self {
            lifecycle,
            provider: None,
            codex_backend: false,
            headers: original_headers,
            query: None,
            byte_cap: budget_bytes,
            slot_cap: slots,
            used_bytes: 0,
            control_slots: 0,
            next_turn_id: 0,
            lanes: HashMap::new(),
            turns: HashMap::new(),
            pending_error_terminals: HashMap::new(),
            pending_eviction_errors: Vec::new(),
            retired_terminals: VecDeque::new(),
            owners: HashMap::new(),
            recent: HashMap::new(),
            connection_lanes: HashMap::new(),
            session_upstream: None,
            session_oauth: false,
            session_rate_limited_upstreams: HashSet::new(),
            connections: HashMap::new(),
            connection_oauth: HashMap::new(),
            connection_signers: HashMap::new(),
            successors: HashMap::new(),
            activity_heartbeat_interval_ms:
                cc_lb_lifecycle::DEFAULT_STREAM_ACTIVITY_HEARTBEAT_INTERVAL
                    .as_millis()
                    .min(u128::from(u64::MAX)) as u64,
        }
    }

    fn is_session_oauth_effective(&self) -> bool {
        self.session_oauth
            || (!self.turns.is_empty() && self.turns.values().all(|turn| turn.all_candidates_oauth))
    }

    /// Preserve the downstream upgrade query after removing API-key parameters.
    pub fn with_downstream_query(mut self, query: Option<String>) -> Self {
        self.query = query.and_then(|query| match cc_lb_upstream::scrub_query_api_key(&query) {
            std::borrow::Cow::Borrowed(_) => Some(query),
            std::borrow::Cow::Owned(clean) if clean.is_empty() => None,
            std::borrow::Cow::Owned(clean) => Some(clean),
        });
        self
    }

    pub fn with_provider(mut self, provider: Option<cc_lb_domain::Provider>) -> Self {
        self.provider = provider;
        self
    }
    pub fn with_codex_backend(mut self, codex_backend: bool) -> Self {
        self.codex_backend = codex_backend;
        self
    }

    fn successor_for_parent(&self, parent: &str) -> Option<u64> {
        self.successors.get(parent).copied().or_else(|| {
            self.turns.iter().find_map(|(id, turn)| {
                (turn.steer_parent.as_deref() == Some(parent)).then_some(*id)
            })
        })
    }
    pub async fn authenticate(&self) -> Result<Authenticated, BuiltinAuthError> {
        self.lifecycle.authenticate(&self.headers).await
    }

    pub fn reject_authentication(&self, error: &BuiltinAuthError) -> WebSocketError {
        let (observer, _) = self.lifecycle.websocket_observer("/v1/responses");
        if let Some(observer) = observer.as_ref() {
            observer.mark_authn_reached();
        }
        let _ = crate::authn_rail::reject_unauthenticated_for_route(
            error,
            &http::Method::GET,
            "/v1/responses",
            self.provider,
            observer.as_ref(),
        );
        WebSocketError::new(
            error.http_status(),
            "authentication_error",
            error.to_string(),
        )
    }

    pub fn reject_protocol(&self, error: &WebSocketError) {
        let (observer, _) = self.lifecycle.websocket_observer("/v1/responses");
        if let Some(observer) = observer.as_ref() {
            observer.mark_authn_reached();
            observer.set_protocol(cc_lb_request_log::RequestProtocol::Websocket);
            terminate_error(observer, error);
        }
    }

    pub async fn client_message(
        &mut self,
        auth: Authenticated,
        frame: Bytes,
    ) -> Result<Option<WebSocketDispatch>, WebSocketError> {
        let turn_started = std::time::Instant::now();
        let parse_started = std::time::Instant::now();
        let body = parse_client_frame(&auth, &frame, self.byte_cap);
        let json_parse_ms = Some(parse_started.elapsed().as_secs_f64() * 1_000.0);
        let kind_str = body
            .as_ref()
            .ok()
            .and_then(|body| body.get("type").and_then(Value::as_str));
        let repeated_steer = kind_str == Some("response.steer")
            && body.as_ref().ok().is_some_and(|body| {
                body.get("previous_response_id")
                    .and_then(Value::as_str)
                    .is_some_and(|parent| self.successor_for_parent(parent).is_some())
            });
        let is_interrupt = kind_str == Some("response.interrupt");
        let control_without_turn = repeated_steer || is_interrupt;
        let (observer, request_id) = self.lifecycle.websocket_observer("/v1/responses");
        let mut auth_info = observer
            .as_ref()
            .map(|_| self.lifecycle.websocket_auth_info(&auth));
        if !control_without_turn
            && let (Some(observer), Some(info)) = (observer.as_ref(), auth_info.take())
        {
            observe_authenticated_frame(observer, info);
            self.lifecycle
                .record_authentication_completed(&auth, observer);
        }
        let result = match body {
            Ok(body) => {
                self.admit_client(
                    auth,
                    frame,
                    body,
                    if control_without_turn {
                        None
                    } else {
                        observer.clone()
                    },
                    &request_id,
                    turn_started,
                    json_parse_ms,
                )
                .await
            }
            Err(error) => Err(error),
        };
        if let Err(error) = result.as_ref()
            && let Some(observer) = observer.as_ref()
        {
            if let Some(info) = auth_info.take() {
                observe_authenticated_frame(observer, info);
            }
            terminate_error(observer, error);
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    async fn admit_client(
        &mut self,
        auth: Authenticated,
        frame: Bytes,
        body: Value,
        observer: Option<LifecycleContext>,
        request_id: &str,
        turn_started: std::time::Instant,
        json_parse_ms: Option<f64>,
    ) -> Result<Option<WebSocketDispatch>, WebSocketError> {
        let kind = body.get("type").and_then(Value::as_str).ok_or_else(|| {
            WebSocketError::new(
                400,
                "invalid_request_error",
                "WebSocket client message requires a type",
            )
        })?;
        if kind == "response.steer" {
            return self
                .admit_steer(
                    auth,
                    frame,
                    body,
                    observer,
                    request_id,
                    turn_started,
                    json_parse_ms,
                )
                .await;
        }
        if kind == "response.interrupt" {
            return self.admit_interrupt(auth, frame, body);
        }
        if kind != "response.create" {
            let safe_kind = sanitize_client_event_type(kind);
            let message = if safe_kind.is_empty() {
                "Unsupported Responses WebSocket client event".to_owned()
            } else {
                format!("Unsupported Responses WebSocket client event: {safe_kind}")
            };
            return Err(WebSocketError::new(400, "invalid_request_error", message));
        }
        let lane = stream_id(&body)?;
        let previous = body
            .get("previous_response_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let owner = previous
            .as_ref()
            .and_then(|id| self.owners.get(id))
            .filter(|owner| !owner.evicted_for_continuation)
            .cloned();
        let session_oauth = self.is_session_oauth_effective();
        if session_oauth
            && let Some(owner) = owner.as_ref()
            && owner.lane.as_deref() != lane.as_deref()
        {
            return Err(WebSocketError::new(
                400,
                "previous_response_not_found",
                "The previous response belongs to another WebSocket lane",
            )
            .with_stream(lane.clone()));
        }
        if owner.is_none() && session_oauth && previous.as_deref().is_some_and(|id| !id.is_empty())
        {
            return Err(WebSocketError::new(
                400,
                "previous_response_not_found",
                "The previous response is not available on this connection",
            )
            .with_stream(lane.clone()));
        }
        if owner
            .as_ref()
            .is_some_and(|owner| owner.principal_id != auth.principal_id())
        {
            return Err(WebSocketError::new(
                400,
                "previous_response_not_found",
                "The previous response is not owned by this client",
            )
            .with_stream(lane));
        }
        if self.codex_backend
            && let Some(owner_ref) = owner.as_ref()
            && previous.as_deref().is_some_and(|id| !id.is_empty())
            && self
                .lifecycle
                .has_healthy_codex_ws_replacement(
                    &auth,
                    WebSocketIngress {
                        headers: &self.headers,
                        frame: &frame,
                        query: self.query.as_deref(),
                    },
                    &body,
                    owner_ref.upstream_id,
                    &self.session_rate_limited_upstreams,
                    true,
                )
                .await
        {
            self.evict_upstream_for_failover(owner_ref.upstream_id);
            return Err(WebSocketError::new(
                400,
                "previous_response_not_found",
                "The previous response owner is unavailable; replay full history",
            )
            .with_stream(lane));
        }
        let charge = frame
            .len()
            .saturating_add(json_owned_bytes(&body))
            .saturating_add(std::mem::size_of::<Turn>())
            .saturating_add(lane.as_ref().map_or(0, String::len))
            .saturating_add(previous.as_ref().map_or(0, String::len))
            .saturating_add(auth.principal_id().len())
            .saturating_add(auth.key_id().len())
            .saturating_add(
                body.get("model")
                    .and_then(Value::as_str)
                    .map_or(0, str::len),
            )
            .saturating_add(
                body.get("service_tier")
                    .and_then(Value::as_str)
                    .map_or(0, str::len),
            );
        self.check_admission(&lane, charge)?;

        // A required tool result is an explicit create with its own settings.
        // Retire the never-created successor before reserving this explicit one.
        if let Some(parent) = previous.as_ref()
            && let Some(successor) = self.successors.remove(parent)
        {
            let error = WebSocketError::new(
                409,
                "steering_requires_explicit_response",
                "Steering will continue through the explicit response.create",
            );
            self.finish_turn(successor, Some(&error));
        }
        let body = Arc::new(body);
        let routing_owner = match owner.as_ref() {
            Some(owner) if is_prewarm_body(&owner.body) => {
                WebSocketTurnOwner::PrewarmResponse(owner.upstream_id)
            }
            Some(owner) => WebSocketTurnOwner::Response(owner.upstream_id),
            None => self
                .session_oauth
                .then_some(self.session_upstream)
                .flatten()
                .map_or(WebSocketTurnOwner::None, WebSocketTurnOwner::SessionHint),
        };
        let session_unpinned = self.session_upstream.is_none();
        let other_turns_active = !self.turns.is_empty();
        let mut prepared = self
            .lifecycle
            .prepare_websocket_turn_with_surface_and_session(
                &auth,
                WebSocketIngress {
                    headers: &self.headers,
                    frame: &frame,
                    query: self.query.as_deref(),
                },
                body.clone(),
                routing_owner,
                observer,
                request_id,
                self.codex_backend,
                session_unpinned,
                other_turns_active,
                &self.session_rate_limited_upstreams,
            )
            .await
            .map_err(|error| error.with_stream(lane.clone()))?;
        if let Some(prev_session_upstream) = self.session_upstream
            && prepared
                .candidates
                .first()
                .is_some_and(|candidate| candidate.upstream_id != prev_session_upstream)
            && (matches!(
                prepared.unknown_reasoning_plan,
                WsUnknownReasoningPlan::StrippedUpfront {
                    owner_failover: true,
                    ..
                }
            ) || prepared.reconciled_body.is_some()
                || !prepared.has_explicit_affinity)
        {
            self.session_upstream = None;
        }
        let owner_failover_fallback = prepared.owner_failover_fallback.take();
        let (body, frame) = prepared.reconciled_body.take().unwrap_or((body, frame));
        let (
            body,
            frame,
            unknown_reasoning_retry,
            pending_affinity_keys,
            deferred_unestablished,
            owner_failover_replacement,
            owner_failover_requires_api_key,
        ) = match std::mem::replace(
            &mut prepared.unknown_reasoning_plan,
            WsUnknownReasoningPlan::None,
        ) {
            WsUnknownReasoningPlan::None => (body, frame, None, Vec::new(), None, false, false),
            WsUnknownReasoningPlan::StrippedUpfront {
                body: stripped_body,
                frame: stripped_frame,
                owner_failover,
                requires_api_key,
            } => (
                stripped_body,
                stripped_frame,
                None,
                Vec::new(),
                None,
                owner_failover,
                requires_api_key,
            ),
            WsUnknownReasoningPlan::RetryStripAll { unknown_keys } => (
                body,
                frame,
                Some(WsUnknownReasoningRetry::All),
                unknown_keys,
                None,
                false,
                false,
            ),
            WsUnknownReasoningPlan::DeferredUnestablished { unknown_keys } => {
                let original_bytes = frame.len().saturating_add(json_owned_bytes(&body));
                (
                    body.clone(),
                    frame.clone(),
                    None,
                    Vec::new(),
                    Some(DeferredUnestablishedReasoning {
                        original_body: body,
                        original_frame: frame,
                        original_bytes,
                        stripped_extra_bytes: 0,
                        unknown_keys,
                        currently_stripped: false,
                    }),
                    false,
                    false,
                )
            }
            WsUnknownReasoningPlan::RetryStripPending {
                pending_digests,
                pending_keys,
            } => (
                body,
                frame,
                Some(WsUnknownReasoningRetry::Pending(pending_digests)),
                pending_keys,
                None,
                false,
                false,
            ),
        };
        let initial_service_tier = body
            .get("service_tier")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let actual_charge = frame
            .len()
            .saturating_add(json_owned_bytes(&body))
            .saturating_add(std::mem::size_of::<Turn>())
            .saturating_add(lane.as_ref().map_or(0, String::len))
            .saturating_add(previous.as_ref().map_or(0, String::len))
            .saturating_add(auth.principal_id().len())
            .saturating_add(auth.key_id().len())
            .saturating_add(
                body.get("model")
                    .and_then(Value::as_str)
                    .map_or(0, str::len),
            )
            .saturating_add(sent_service_tier_bytes(initial_service_tier.as_deref()));
        let charge = actual_charge;
        if let Some(stream_id) = lane.as_deref() {
            prepared.span.record("cc_lb.responses.stream_id", stream_id);
        }
        let mut setup_timings = prepared.setup_timings;
        if json_parse_ms.is_some() {
            setup_timings.json_parse_ms = json_parse_ms;
        }
        let metrics = ResponsesStreamMetrics::new_with_setup(
            turn_started,
            Some(duration_to_ms(turn_started.elapsed())),
            setup_timings,
        );
        self.next_turn_id = self.next_turn_id.checked_add(1).ok_or_else(|| {
            WebSocketError::new(
                503,
                "overloaded_error",
                "WebSocket turn identifier space is exhausted",
            )
        })?;
        let turn_id = self.next_turn_id;
        let initial_selected_upstream_id = Some(prepared.route_info.upstream_id);
        let candidate_oauth = prepared
            .candidates
            .iter()
            .map(|candidate| (candidate.upstream_id, candidate.socket_per_lane))
            .collect();
        self.used_bytes = self.used_bytes.saturating_add(charge);
        let all_candidates_oauth = !prepared.candidates.is_empty()
            && prepared
                .candidates
                .iter()
                .all(|candidate| candidate.socket_per_lane);
        self.turns.insert(
            turn_id,
            Turn {
                auth,
                prepared,
                prepared_signers: HashMap::new(),
                initial_selected_upstream_id,
                candidate_oauth,
                body,
                frame,
                lane: lane.clone(),
                connection_id: owner.as_ref().map(|owner| owner.connection_id),
                upstream_id: owner.as_ref().map(|owner| owner.upstream_id),
                pinned: owner
                    .as_ref()
                    .map(|owner| (owner.connection_id, owner.upstream_id)),
                response_id: None,
                previous_response_id: previous,
                steer_parent: None,
                attempted: false,
                attempt_num: 0,
                steer_submissions: 0,
                accepted_steers: HashSet::new(),
                sent: false,
                controls: VecDeque::new(),
                charged_bytes: charge,
                usage: None,
                usage_complete: false,
                incomplete_reason: None,
                plugin_request: None,
                sent_service_tier: initial_service_tier,
                echo_metadata: None,
                metrics,
                funding_ambiguous: false,
                response_created: false,
                provider_frame_forwarded: false,
                unknown_reasoning_retry,
                unknown_reasoning_retried: false,
                pending_affinity_keys,
                deferred_unestablished,
                has_any_oauth_candidate: false,
                all_candidates_oauth,
                admitted_session_upstream: self.session_upstream,
                owner_failover_replacement,
                owner_failover_requires_api_key,
                owner_failover_fallback,
                live_failover_candidates: Vec::new(),
                suppressed_failover_error: None,
            },
        );
        if !self.lanes.contains_key(&lane) {
            self.used_bytes = self.used_bytes.saturating_add(lane_bytes(&lane));
        }
        let lane_state = self.lanes.entry(lane).or_default();
        lane_state.queued.push_back(turn_id);
        if lane_state.active.is_none() {
            let next = lane_state
                .queued
                .pop_front()
                .expect("newly queued turn exists");
            lane_state.active = Some(next);
            match self.try_dispatch(next) {
                Ok(dispatched) => Ok(dispatched),
                Err(error) => {
                    self.finish_turn(next, Some(&error));
                    Err(error)
                }
            }
        } else {
            Ok(None)
        }
    }

    fn check_admission(&self, lane: &Option<String>, bytes: usize) -> Result<(), WebSocketError> {
        let lane_bytes = if self.lanes.contains_key(lane) {
            0
        } else {
            std::mem::size_of::<Lane>().saturating_add(lane.as_ref().map_or(0, String::len))
        };
        if self.slot_cap == 0
            || self.turns.len() >= self.slot_cap
            || (!self.lanes.contains_key(lane) && self.lanes.len() >= self.slot_cap)
            || bytes.saturating_add(lane_bytes) > self.byte_cap.saturating_sub(self.used_bytes)
        {
            return Err(WebSocketError::new(
                503,
                "overloaded_error",
                "WebSocket session admission budget is full",
            )
            .with_stream(lane.clone()));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn admit_steer(
        &mut self,
        auth: Authenticated,
        frame: Bytes,
        control: Value,
        observer: Option<LifecycleContext>,
        request_id: &str,
        turn_started: std::time::Instant,
        json_parse_ms: Option<f64>,
    ) -> Result<Option<WebSocketDispatch>, WebSocketError> {
        let parent = control
            .get("previous_response_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                WebSocketError::new(
                    400,
                    "invalid_request_error",
                    "response.steer requires previous_response_id",
                )
            })?
            .to_owned();
        let owner = self
            .owners
            .get(&parent)
            .filter(|owner| !owner.evicted_for_continuation)
            .cloned()
            .ok_or_else(|| {
                WebSocketError::new(
                    400,
                    "response_not_found",
                    "The steering target is not available on this WebSocket connection",
                )
            })?;
        if owner.principal_id != auth.principal_id() {
            return Err(WebSocketError::new(
                400,
                "response_not_found",
                "The steering target is not owned by this client",
            ));
        }
        let lane = owner.lane.clone();
        if let Some(successor) = self.successor_for_parent(&parent) {
            let Some(turn) = self.turns.get_mut(&successor) else {
                return Err(WebSocketError::new(
                    502,
                    "invalid_upstream_response",
                    "Steering successor ownership is unavailable",
                ));
            };
            let charge = queued_control_bytes(&frame);
            if charge > self.byte_cap.saturating_sub(self.used_bytes)
                || (!turn.sent && self.control_slots >= self.slot_cap)
            {
                return Err(WebSocketError::new(
                    503,
                    "overloaded_error",
                    "WebSocket steering budget is full",
                )
                .with_stream(lane));
            }
            turn.steer_submissions = turn.steer_submissions.saturating_add(1);
            // Multiple controls now share this successor and the upstream response
            // frames cannot attribute which control committed it, so clear any
            // earlier control's sent tier fallback.
            if let Some(stale_tier) = turn.sent_service_tier.take() {
                let released = sent_service_tier_bytes(Some(&stale_tier)).min(turn.charged_bytes);
                self.used_bytes = self.used_bytes.saturating_sub(released);
                turn.charged_bytes = turn.charged_bytes.saturating_sub(released);
            }
            if !turn.sent {
                self.control_slots += 1;
                self.used_bytes = self.used_bytes.saturating_add(charge);
                turn.charged_bytes = turn.charged_bytes.saturating_add(charge);
                turn.controls.push_back(frame);
                return Ok(None);
            }
            return Ok(Some(WebSocketDispatch {
                turn_id: successor,
                frame,
                connection_id: Some(owner.connection_id),
                candidates: Vec::new(),
                stream_id: lane,
                control: false,
                prefer_api_key_on_final_attempt: false,
                has_explicit_affinity: false,
                is_failover_recovery: false,
            }));
        }
        let body = owner.body.clone();
        let initial_service_tier = control
            .get("service_tier")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let charge = frame
            .len()
            .saturating_add(std::mem::size_of::<Turn>())
            .saturating_add(parent.len().saturating_mul(3))
            .saturating_add(lane.as_ref().map_or(0, String::len))
            .saturating_add(auth.principal_id().len())
            .saturating_add(auth.key_id().len())
            .saturating_add(sent_service_tier_bytes(initial_service_tier.as_deref()));
        self.check_admission(&lane, charge)?;
        let prepared = self
            .lifecycle
            .prepare_websocket_turn_with_surface(
                &auth,
                WebSocketIngress {
                    headers: &self.headers,
                    frame: &frame,
                    query: self.query.as_deref(),
                },
                body.clone(),
                WebSocketTurnOwner::Response(owner.upstream_id),
                observer,
                request_id,
                self.codex_backend,
            )
            .await
            .map_err(|error| error.with_stream(lane.clone()))?;
        if let Some(stream_id) = lane.as_deref() {
            prepared.span.record("cc_lb.responses.stream_id", stream_id);
        }
        let mut setup_timings = prepared.setup_timings;
        if json_parse_ms.is_some() {
            setup_timings.json_parse_ms = json_parse_ms;
        }
        let metrics = ResponsesStreamMetrics::new_with_setup(
            turn_started,
            Some(duration_to_ms(turn_started.elapsed())),
            setup_timings,
        );
        self.next_turn_id = self.next_turn_id.checked_add(1).ok_or_else(|| {
            WebSocketError::new(
                503,
                "overloaded_error",
                "WebSocket turn identifier space is exhausted",
            )
        })?;
        let turn_id = self.next_turn_id;
        let initial_selected_upstream_id = Some(prepared.route_info.upstream_id);
        let candidate_oauth = prepared
            .candidates
            .iter()
            .map(|candidate| (candidate.upstream_id, candidate.socket_per_lane))
            .collect();
        self.used_bytes = self.used_bytes.saturating_add(charge);
        let all_candidates_oauth = !prepared.candidates.is_empty()
            && prepared
                .candidates
                .iter()
                .all(|candidate| candidate.socket_per_lane);
        self.successors.insert(parent.clone(), turn_id);
        self.turns.insert(
            turn_id,
            Turn {
                auth,
                prepared,
                prepared_signers: HashMap::new(),
                initial_selected_upstream_id,
                candidate_oauth,
                body,
                frame,
                lane: lane.clone(),
                connection_id: Some(owner.connection_id),
                upstream_id: Some(owner.upstream_id),
                response_id: None,
                pinned: Some((owner.connection_id, owner.upstream_id)),
                previous_response_id: Some(parent.clone()),
                steer_parent: Some(parent),
                attempted: false,
                attempt_num: 0,
                steer_submissions: 1,
                accepted_steers: HashSet::new(),
                sent: false,
                controls: VecDeque::new(),
                charged_bytes: charge,
                usage: None,
                usage_complete: false,
                incomplete_reason: None,
                plugin_request: None,
                sent_service_tier: initial_service_tier,
                echo_metadata: None,
                metrics,
                funding_ambiguous: false,
                response_created: false,
                provider_frame_forwarded: false,
                unknown_reasoning_retry: None,
                unknown_reasoning_retried: false,
                pending_affinity_keys: Vec::new(),
                deferred_unestablished: None,
                has_any_oauth_candidate: false,
                all_candidates_oauth,
                admitted_session_upstream: self.session_upstream,
                owner_failover_replacement: false,
                owner_failover_requires_api_key: false,
                owner_failover_fallback: None,
                live_failover_candidates: Vec::new(),
                suppressed_failover_error: None,
            },
        );
        let lane_state = self.lanes.entry(lane).or_default();
        match lane_state.active {
            None => {
                lane_state.active = Some(turn_id);
                Ok(self.dispatch(turn_id))
            }
            Some(active) if active == owner.turn_id => Ok(self.dispatch(turn_id)),
            Some(_) => {
                lane_state.queued.push_back(turn_id);
                Ok(None)
            }
        }
    }

    /// Admit a `response.interrupt` control frame for an already-owned response
    /// without starting a new turn or failing over across connections.
    fn admit_interrupt(
        &self,
        auth: Authenticated,
        frame: Bytes,
        control: Value,
    ) -> Result<Option<WebSocketDispatch>, WebSocketError> {
        let requested_lane = stream_id(&control)?;
        let response_id = control
            .get("response_id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| {
                WebSocketError::new(
                    400,
                    "invalid_request_error",
                    "response.interrupt requires response_id",
                )
                .with_stream(requested_lane.clone())
            })?;
        let owner = self.owners.get(response_id).cloned().ok_or_else(|| {
            WebSocketError::new(
                400,
                "response_not_found",
                "The interrupt target is not available on this WebSocket connection",
            )
            .with_stream(requested_lane.clone())
        })?;
        if requested_lane.is_some() && requested_lane != owner.lane {
            return Err(WebSocketError::new(
                400,
                "response_not_found",
                "The interrupt target belongs to another WebSocket lane",
            )
            .with_stream(requested_lane));
        }
        if owner.principal_id != auth.principal_id() {
            return Err(WebSocketError::new(
                400,
                "response_not_found",
                "The interrupt target is not owned by this client",
            )
            .with_stream(owner.lane));
        }
        if self.connections.get(&owner.connection_id) != Some(&owner.upstream_id) {
            return Err(WebSocketError::new(
                400,
                "response_not_found",
                "The interrupt target connection is no longer available",
            )
            .with_stream(owner.lane));
        }
        Ok(Some(WebSocketDispatch {
            turn_id: owner.turn_id,
            frame,
            connection_id: Some(owner.connection_id),
            candidates: Vec::new(),
            stream_id: owner.lane,
            control: true,
            prefer_api_key_on_final_attempt: false,
            has_explicit_affinity: false,
            is_failover_recovery: false,
        }))
    }

    fn apply_unestablished_reasoning(
        &mut self,
        turn_id: u64,
        pinned_to_single_owner: bool,
    ) -> Result<Option<Bytes>, WebSocketError> {
        let Some((currently_stripped, turn_lane)) = self.turns.get(&turn_id).and_then(|turn| {
            turn.deferred_unestablished
                .as_ref()
                .map(|deferred| (deferred.currently_stripped, turn.lane.clone()))
        }) else {
            return Ok(None);
        };
        if !pinned_to_single_owner {
            if !currently_stripped {
                let (
                    stripped_body,
                    stripped_frame,
                    stripped_bytes,
                    next_tier,
                    next_tier_bytes,
                    old_plugin_and_tier,
                ) = {
                    let Some(turn) = self.turns.get(&turn_id) else {
                        return Ok(None);
                    };
                    let Some(deferred) = turn.deferred_unestablished.as_ref() else {
                        return Ok(None);
                    };
                    let Some(stripped) =
                        strip_openai_reasoning_encrypted_items(&deferred.original_body, None)
                    else {
                        return Ok(None);
                    };
                    let Ok(bytes) = serde_json::to_vec(&stripped) else {
                        return Ok(None);
                    };
                    let next_tier = stripped
                        .get("service_tier")
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                    let next_tier_bytes = sent_service_tier_bytes(next_tier.as_deref());
                    let stripped_body = Arc::new(stripped);
                    let stripped_frame = Bytes::from(bytes);
                    let stripped_bytes = stripped_frame
                        .len()
                        .saturating_add(json_owned_bytes(&stripped_body));
                    let old_plugin_and_tier = turn
                        .plugin_request
                        .as_ref()
                        .map_or(0, |(_, req)| req.frame.len())
                        .saturating_add(sent_service_tier_bytes(turn.sent_service_tier.as_deref()));
                    (
                        stripped_body,
                        stripped_frame,
                        stripped_bytes,
                        next_tier,
                        next_tier_bytes,
                        old_plugin_and_tier,
                    )
                };
                if stripped_bytes.saturating_add(next_tier_bytes)
                    > self
                        .byte_cap
                        .saturating_sub(self.used_bytes)
                        .saturating_add(old_plugin_and_tier)
                {
                    return Err(WebSocketError::new(
                        503,
                        "overloaded_error",
                        "Stripped WebSocket frame exceeds the session budget",
                    )
                    .with_stream(turn_lane));
                }
                let Some(turn) = self.turns.get_mut(&turn_id) else {
                    return Ok(None);
                };
                turn.plugin_request = None;
                let added_charge = stripped_bytes.saturating_add(next_tier_bytes);
                let released_charge = old_plugin_and_tier.min(turn.charged_bytes);
                self.used_bytes = self
                    .used_bytes
                    .saturating_sub(released_charge)
                    .saturating_add(added_charge);
                turn.charged_bytes = turn
                    .charged_bytes
                    .saturating_sub(released_charge)
                    .saturating_add(added_charge);
                turn.body = stripped_body;
                turn.frame = stripped_frame;
                turn.sent_service_tier = next_tier;
                if let Some(deferred) = turn.deferred_unestablished.as_mut() {
                    deferred.currently_stripped = true;
                    deferred.stripped_extra_bytes = stripped_bytes;
                }
            }
            let Some(turn) = self.turns.get_mut(&turn_id) else {
                return Ok(None);
            };
            turn.unknown_reasoning_retry = None;
            turn.pending_affinity_keys.clear();
            return Ok(Some(turn.frame.clone()));
        }
        if currently_stripped {
            let (
                old_plugin_and_tier,
                stripped_extra,
                restored_tier,
                restored_tier_bytes,
                turn_lane,
            ) = {
                let Some(turn) = self.turns.get(&turn_id) else {
                    return Ok(None);
                };
                let old_plugin_and_tier = turn
                    .plugin_request
                    .as_ref()
                    .map_or(0, |(_, req)| req.frame.len())
                    .saturating_add(sent_service_tier_bytes(turn.sent_service_tier.as_deref()));
                let (stripped_extra, restored_tier) = turn
                    .deferred_unestablished
                    .as_ref()
                    .map(|deferred| {
                        (
                            deferred.stripped_extra_bytes,
                            deferred
                                .original_body
                                .get("service_tier")
                                .and_then(Value::as_str)
                                .map(str::to_owned),
                        )
                    })
                    .unwrap_or((0, None));
                let restored_tier_bytes = sent_service_tier_bytes(restored_tier.as_deref());
                (
                    old_plugin_and_tier,
                    stripped_extra,
                    restored_tier,
                    restored_tier_bytes,
                    turn.lane.clone(),
                )
            };
            if restored_tier_bytes
                > self
                    .byte_cap
                    .saturating_sub(self.used_bytes)
                    .saturating_add(old_plugin_and_tier)
                    .saturating_add(stripped_extra)
            {
                return Err(WebSocketError::new(
                    503,
                    "overloaded_error",
                    "Restored WebSocket frame exceeds the session budget",
                )
                .with_stream(turn_lane));
            }
            let Some(turn) = self.turns.get_mut(&turn_id) else {
                return Ok(None);
            };
            turn.plugin_request = None;
            if let Some(deferred) = turn.deferred_unestablished.as_mut() {
                deferred.stripped_extra_bytes = 0;
                turn.body = Arc::clone(&deferred.original_body);
                turn.frame = deferred.original_frame.clone();
                deferred.currently_stripped = false;
            }
            turn.sent_service_tier = restored_tier;
            let total_released = old_plugin_and_tier
                .saturating_add(stripped_extra)
                .min(turn.charged_bytes);
            self.used_bytes = self
                .used_bytes
                .saturating_sub(total_released)
                .saturating_add(restored_tier_bytes);
            turn.charged_bytes = turn
                .charged_bytes
                .saturating_sub(total_released)
                .saturating_add(restored_tier_bytes);
        }
        let Some(turn) = self.turns.get_mut(&turn_id) else {
            return Ok(None);
        };
        if let Some(deferred) = turn.deferred_unestablished.as_ref() {
            turn.unknown_reasoning_retry = Some(WsUnknownReasoningRetry::All);
            turn.pending_affinity_keys = deferred.unknown_keys.clone();
        }
        Ok(Some(turn.frame.clone()))
    }

    /// Resolve any deferred unestablished encrypted-reasoning plan once the
    /// final candidate selection decides between stripping (failover / API-key)
    /// and verbatim preservation with `RetryStripAll` (pinned single owner).
    pub fn finalize_dispatch_reasoning(
        &mut self,
        dispatch: &mut WebSocketDispatch,
        pinned_to_single_owner: bool,
    ) -> Result<(), WebSocketError> {
        if dispatch.control {
            return Ok(());
        }
        if let Some(frame) =
            self.apply_unestablished_reasoning(dispatch.turn_id, pinned_to_single_owner)?
        {
            dispatch.frame = frame;
        }
        Ok(())
    }

    fn try_dispatch(&mut self, turn_id: u64) -> Result<Option<WebSocketDispatch>, WebSocketError> {
        self.dispatch_inner(turn_id, true)
    }

    fn dispatch(&mut self, turn_id: u64) -> Option<WebSocketDispatch> {
        self.dispatch_inner(turn_id, false).ok().flatten()
    }

    fn dispatch_inner(
        &mut self,
        turn_id: u64,
        fail_on_budget: bool,
    ) -> Result<Option<WebSocketDispatch>, WebSocketError> {
        let Some((should_reapply_pin, turn_lane, has_explicit_affinity)) =
            self.turns.get(&turn_id).map(|turn| {
                (
                    self.session_oauth
                        && turn.pinned.is_none()
                        && !turn.prepared.has_explicit_affinity
                        && turn.admitted_session_upstream.is_none()
                        && !turn.owner_failover_replacement
                        && !turn.owner_failover_requires_api_key,
                    turn.lane.clone(),
                    turn.prepared.has_explicit_affinity,
                )
            })
        else {
            return Ok(None);
        };
        let session_owner = should_reapply_pin
            .then_some(self.session_upstream)
            .flatten();
        let owner_fallback = session_owner.and_then(|owner| {
            self.lifecycle
                .websocket_candidate_for_upstream(owner, self.codex_backend)
        });
        let (candidates, current_has_any_oauth_candidate) = {
            let Some(turn) = self.turns.get_mut(&turn_id) else {
                return Ok(None);
            };
            let mut candidates = std::mem::take(&mut turn.prepared.candidates);
            if let Some(owner) = session_owner {
                let existing_pos = candidates
                    .iter()
                    .position(|candidate| candidate.upstream_id == owner);
                let owner_cand = existing_pos
                    .map(|pos| candidates.remove(pos))
                    .or(owner_fallback);
                if let Some(ref owner_cand) = owner_cand {
                    turn.candidate_oauth
                        .insert(owner, owner_cand.socket_per_lane);
                    turn.initial_selected_upstream_id = Some(owner);
                }
                if !is_prewarm_body(&turn.body) {
                    candidates.clear();
                    if let Some(owner_cand) = owner_cand {
                        candidates.push(owner_cand);
                    }
                } else if let Some(owner_cand) = owner_cand {
                    candidates.insert(0, owner_cand);
                }
            }
            let has_any_oauth_candidate = candidates.iter().any(|c| !c.is_api_key);
            turn.has_any_oauth_candidate = has_any_oauth_candidate;
            (candidates, has_any_oauth_candidate)
        };
        let defer_for_other_oauth_owner = current_has_any_oauth_candidate
            && self.session_upstream.is_none()
            && !has_explicit_affinity
            && self.turns.iter().any(|(other_id, other)| {
                *other_id != turn_id
                    && other.lane != turn_lane
                    && self
                        .lanes
                        .get(&other.lane)
                        .is_some_and(|l| l.active == Some(*other_id))
                    && other.upstream_id.is_none()
                    && other.has_any_oauth_candidate
            });
        let live_failover_candidates = {
            let Some(turn) = self.turns.get(&turn_id) else {
                return Ok(None);
            };
            if turn.pinned.is_none() && !turn.prepared.has_explicit_affinity {
                candidates.clone()
            } else {
                Vec::new()
            }
        };
        let reasoning_res = if candidates.len() <= 1 {
            self.apply_unestablished_reasoning(turn_id, true)
        } else if !defer_for_other_oauth_owner {
            self.apply_unestablished_reasoning(turn_id, false)
        } else {
            Ok(None)
        };
        if let Err(error) = reasoning_res
            && fail_on_budget
        {
            return Err(error);
        }
        let Some(turn) = self.turns.get_mut(&turn_id) else {
            return Ok(None);
        };
        if !live_failover_candidates.is_empty() {
            turn.live_failover_candidates = live_failover_candidates;
        }
        let prefer_api_key_on_final_attempt = !self.codex_backend && turn.pinned.is_none();
        Ok(Some(WebSocketDispatch {
            turn_id,
            frame: turn.frame.clone(),
            connection_id: turn.connection_id,
            candidates,
            stream_id: turn.lane.clone(),
            control: false,
            prefer_api_key_on_final_attempt,
            has_explicit_affinity,
            is_failover_recovery: false,
        }))
    }

    fn reject_exhausted_fallback(turn: &Turn, upstream_id: Uuid) -> bool {
        turn.pinned.is_none()
            && (turn.owner_failover_replacement
                || Some(upstream_id) != turn.initial_selected_upstream_id)
    }

    /// Revalidate an unpinned fallback candidate's fresh quota and routing
    /// eligibility before reusing an existing socket or opening a new one.
    pub async fn validate_fallback_candidate(
        &mut self,
        dispatch: &WebSocketDispatch,
        candidate: &WebSocketCandidate,
    ) -> Result<(), WebSocketError> {
        let (turn_frame, turn_body, reject_exhausted, principal_id, key_id, reservation_active) = {
            let turn = self.turns.get(&dispatch.turn_id).ok_or_else(|| {
                WebSocketError::new(
                    409,
                    "response_not_found",
                    "The admitted response is no longer available",
                )
            })?;
            turn.ensure_credential_eligible(candidate.is_api_key)?;
            let reject_exhausted = Self::reject_exhausted_fallback(turn, candidate.upstream_id);
            if !reject_exhausted {
                return Ok(());
            }
            (
                turn.frame.clone(),
                turn.body.clone(),
                reject_exhausted,
                turn.prepared.principal_id.clone(),
                turn.prepared.key_id.clone(),
                turn.prepared
                    .reservation
                    .as_ref()
                    .is_none_or(|reservation| reservation.is_active()),
            )
        };
        let auth = self.authenticate().await.map_err(|error| {
            WebSocketError::new(
                error.http_status(),
                "authentication_error",
                error.to_string(),
            )
        })?;
        if auth.principal_id() != principal_id || auth.key_id() != key_id {
            return Err(WebSocketError::new(
                401,
                "authentication_error",
                "The client identity changed before connection",
            ));
        }
        if !reservation_active {
            return Err(WebSocketError::new(
                503,
                "reservation_expired",
                "The queued response reservation is no longer active",
            ));
        }
        self.lifecycle
            .validate_websocket_turn_with_policy(
                &auth,
                WebSocketIngress {
                    headers: &self.headers,
                    frame: &turn_frame,
                    query: self.query.as_deref(),
                },
                &turn_body,
                candidate.upstream_id,
                self.codex_backend,
                reject_exhausted,
            )
            .await
    }

    /// Return the OAuth upstream currently pinned for this downstream session, if any.
    pub fn session_upstream(&self) -> Option<Uuid> {
        self.session_upstream
    }

    /// Record that `upstream_id` returned a 429/rate-limit rejection during this
    /// WebSocket session so subsequent turns avoid re-selecting it when a healthy
    /// replacement exists.
    pub fn record_session_rate_limited(&mut self, upstream_id: Uuid) {
        self.record_session_rate_limited_with_retry_after(upstream_id, None);
    }

    pub fn record_session_rate_limited_with_retry_after(
        &mut self,
        upstream_id: Uuid,
        retry_after_secs: Option<u64>,
    ) {
        self.session_rate_limited_upstreams.insert(upstream_id);
        self.lifecycle
            .record_ws_rate_limit_cooldown(upstream_id, None, retry_after_secs);
    }

    /// Evict in-session continuation lookups, unsent steer successors, and
    /// session pin state bound to `upstream_id` while preserving live response
    /// attribution for active turns and parked delayed error terminals, and
    /// reclaim `owner_bytes` once owners are unreferenced.
    fn evict_upstream_for_failover(&mut self, upstream_id: Uuid) {
        let mut evicted_ids = Vec::new();
        for (id, owner) in &mut self.owners {
            if owner.upstream_id == upstream_id {
                owner.evicted_for_continuation = true;
                evicted_ids.push(id.clone());
            }
        }
        let evicted_set: HashSet<&str> = evicted_ids.iter().map(String::as_str).collect();
        self.recent
            .retain(|_, resp_id| !evicted_set.contains(resp_id.as_str()));
        let mut pending_unsent_successors = Vec::new();
        for id in &evicted_ids {
            if let Some(successor_id) = self.successors.remove(id)
                && self.turns.get(&successor_id).is_some_and(|turn| !turn.sent)
            {
                pending_unsent_successors.push(successor_id);
            }
        }
        for successor_id in pending_unsent_successors {
            let lane = self
                .turns
                .get(&successor_id)
                .and_then(|turn| turn.lane.clone());
            let error = WebSocketError::new(
                400,
                "previous_response_not_found",
                "The previous response is no longer available",
            )
            .with_stream(lane);
            self.finish_turn(successor_id, Some(&error));
        }
        for id in &evicted_ids {
            self.collect_owner(Some(id));
        }
        self.prune_empty_lanes();
        if self.session_upstream == Some(upstream_id) {
            self.session_upstream = None;
        }
    }

    /// Synchronously prepare plugin request state on the relay loop and return a
    /// self-contained job that performs auth, validation, OAuth token refresh,
    /// and request signing inside the caller's connect task.
    pub fn begin_connection_prepare(
        &mut self,
        dispatch: &WebSocketDispatch,
        candidate: &WebSocketCandidate,
    ) -> Result<PreparedWsConnectJob, WebSocketError> {
        let (
            turn_frame,
            turn_body,
            reject_exhausted,
            expected_principal_id,
            expected_key_id,
            reservation_id,
            deferred_limit_context,
            observer,
        ) = {
            let turn = self.turns.get(&dispatch.turn_id).ok_or_else(|| {
                WebSocketError::new(
                    409,
                    "response_not_found",
                    "The admitted response is no longer available",
                )
            })?;
            turn.ensure_credential_eligible(candidate.is_api_key)?;
            if turn
                .prepared
                .reservation
                .as_ref()
                .is_some_and(|reservation| !reservation.is_active())
            {
                return Err(WebSocketError::new(
                    503,
                    "reservation_expired",
                    "The queued response reservation is no longer active",
                ));
            }
            (
                turn.frame.clone(),
                turn.body.clone(),
                Self::reject_exhausted_fallback(turn, candidate.upstream_id),
                turn.prepared.principal_id.clone(),
                turn.prepared.key_id.clone(),
                turn.prepared
                    .reservation
                    .as_ref()
                    .map(|reservation| reservation.id().to_owned()),
                turn.prepared.deferred_limit_context.clone(),
                turn.prepared.observer.clone(),
            )
        };
        self.prepare_plugin_request(dispatch.turn_id, candidate.upstream_id)?;
        let sign_headers = self
            .turns
            .get(&dispatch.turn_id)
            .and_then(|turn| turn.plugin_request.as_ref())
            .map_or_else(
                || {
                    let mut fallback = self.headers.clone();
                    crate::hop_by_hop::strip_ingress_proxy_headers(&mut fallback);
                    fallback
                },
                |(_, request)| request.headers.clone(),
            );
        Ok(PreparedWsConnectJob {
            lifecycle: self.lifecycle.clone(),
            turn_id: dispatch.turn_id,
            candidate: candidate.clone(),
            ingress_headers: self.headers.clone(),
            sign_headers,
            query: self.query.clone(),
            frame: turn_frame,
            body: turn_body,
            codex_backend: self.codex_backend,
            reject_exhausted,
            expected_principal_id,
            expected_key_id,
            reservation_id,
            deferred_limit_context,
            observer,
        })
    }

    /// Record the signer and signing timings produced off the relay loop.
    pub fn record_prepared_signer(
        &mut self,
        turn_id: u64,
        upstream_id: Uuid,
        signer: Arc<dyn Signer>,
        prepare_signer_ms: f64,
        sign_ms: u64,
    ) {
        if let Some(turn) = self.turns.get_mut(&turn_id) {
            turn.metrics.setup_timings.prepare_signer_ms = Some(prepare_signer_ms);
            turn.metrics.sign_ms = Some(sign_ms);
            turn.prepared_signers.insert(upstream_id, signer);
        }
    }

    pub async fn prepare_connection(
        &mut self,
        dispatch: &WebSocketDispatch,
        candidate: &WebSocketCandidate,
    ) -> Result<(Url, HeaderMap), WebSocketError> {
        let job = self.begin_connection_prepare(dispatch, candidate)?;
        let turn_id = job.turn_id();
        let upstream_id = job.upstream_id();
        let signed = job.run().await?;
        self.record_prepared_signer(
            turn_id,
            upstream_id,
            signed.signer,
            signed.prepare_signer_ms,
            signed.sign_ms,
        );
        Ok((signed.url, signed.headers))
    }

    fn prepare_plugin_request(
        &mut self,
        turn_id: u64,
        upstream_id: Uuid,
    ) -> Result<(), WebSocketError> {
        let (auth, frame, request_id, observer, turn_body_tier, old_charge) = {
            let turn = self.turns.get(&turn_id).ok_or_else(|| {
                WebSocketError::new(
                    409,
                    "response_not_found",
                    "The admitted response is no longer available",
                )
            })?;
            if turn
                .plugin_request
                .as_ref()
                .is_some_and(|(id, _)| *id == upstream_id)
            {
                return Ok(());
            }
            (
                turn.auth.clone(),
                turn.frame.clone(),
                turn.prepared.request_id.clone(),
                turn.prepared.observer.clone(),
                turn.body
                    .get("service_tier")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                turn.plugin_request
                    .as_ref()
                    .map_or(0, |(_, request)| request.frame.len())
                    .saturating_add(sent_service_tier_bytes(turn.sent_service_tier.as_deref())),
            )
        };
        let shape_started = std::time::Instant::now();
        let mut request = self.lifecycle.shape_websocket_frame_with_surface(
            &auth,
            WebSocketIngress {
                headers: &self.headers,
                frame: &frame,
                query: self.query.as_deref(),
            },
            upstream_id,
            &request_id,
            observer.as_ref(),
            self.codex_backend,
        )?;
        let shape_ms = duration_to_ms(shape_started.elapsed());
        let next_sent_service_tier = match request.as_mut() {
            Some(request) => request.sent_service_tier.take(),
            None => turn_body_tier,
        };
        let new_charge = request
            .as_ref()
            .map_or(0, |request| request.frame.len())
            .saturating_add(sent_service_tier_bytes(next_sent_service_tier.as_deref()));
        if new_charge
            > self
                .byte_cap
                .saturating_sub(self.used_bytes)
                .saturating_add(old_charge)
        {
            return Err(WebSocketError::new(
                503,
                "overloaded_error",
                "Shaped WebSocket frame exceeds the session budget",
            ));
        }
        let turn = self.turns.get_mut(&turn_id).expect("turn remains admitted");
        let released = old_charge.min(turn.charged_bytes);
        self.used_bytes = self
            .used_bytes
            .saturating_sub(released)
            .saturating_add(new_charge);
        turn.metrics.shape_ms = Some(shape_ms);
        turn.charged_bytes = turn
            .charged_bytes
            .saturating_sub(released)
            .saturating_add(new_charge);
        turn.prepared.oauth_compat = request.as_ref().is_some_and(|request| request.oauth_compat);
        turn.echo_metadata = request
            .as_ref()
            .and_then(|request| request.echo_metadata.clone());
        turn.sent_service_tier = next_sent_service_tier;
        turn.plugin_request = request.map(|request| (upstream_id, request));
        Ok(())
    }

    /// Shape each create once for the actual connection owner. Steering controls
    /// keep their native wire format; their successor retains the response hook.
    pub fn frame_for_send(
        &mut self,
        dispatch: &WebSocketDispatch,
    ) -> Result<Bytes, WebSocketError> {
        if dispatch.control {
            return Ok(dispatch.frame.clone());
        }
        let (upstream_id, steer) = {
            let turn = self.turns.get(&dispatch.turn_id).ok_or_else(|| {
                WebSocketError::new(
                    409,
                    "response_not_found",
                    "The admitted response is no longer available",
                )
            })?;
            (
                turn.upstream_id.ok_or_else(|| {
                    WebSocketError::new(
                        502,
                        "upstream_unavailable",
                        "No upstream owns this dispatch",
                    )
                })?,
                turn.steer_parent.is_some(),
            )
        };
        if steer {
            return Ok(dispatch.frame.clone());
        }
        self.prepare_plugin_request(dispatch.turn_id, upstream_id)?;
        let turn = self
            .turns
            .get(&dispatch.turn_id)
            .expect("turn remains admitted");
        // OpenAI OAuth frames are always shaped (stream_id removed, public
        // rules applied) by `shape_websocket_frame`.
        Ok(turn.plugin_request.as_ref().map_or_else(
            || dispatch.frame.clone(),
            |(_, request)| request.frame.clone(),
        ))
    }

    /// Report whether `connection_id` is currently owned by `upstream_id`.
    pub fn owns_connection(&self, connection_id: u64, upstream_id: Uuid) -> bool {
        self.connections.get(&connection_id) == Some(&upstream_id)
    }
    pub fn assign_connection(
        &mut self,
        turn_id: u64,
        connection_id: u64,
        upstream_id: Uuid,
        upstream_name: &str,
    ) {
        self.connection_lanes
            .entry(connection_id)
            .or_insert_with(|| self.turns.get(&turn_id).and_then(|turn| turn.lane.clone()));
        let is_oauth = self
            .connection_oauth
            .get(&connection_id)
            .copied()
            .or_else(|| {
                self.turns
                    .get(&turn_id)
                    .and_then(|turn| turn.candidate_oauth.get(&upstream_id).copied())
            })
            .or_else(|| self.lifecycle.is_openai_oauth_upstream(upstream_id))
            .unwrap_or_else(|| {
                self.turns
                    .get(&turn_id)
                    .is_some_and(|turn| turn.prepared.oauth_transport)
            });
        let is_prewarm = self
            .turns
            .get(&turn_id)
            .is_some_and(|turn| is_prewarm_body(&turn.body));
        self.connection_oauth
            .entry(connection_id)
            .or_insert(is_oauth);
        if is_oauth {
            if is_prewarm {
                self.session_upstream.get_or_insert(upstream_id);
            } else {
                self.session_upstream = Some(upstream_id);
            }
            self.session_oauth = true;
        }
        self.connections.entry(connection_id).or_insert(upstream_id);
        if let Some(turn) = self.turns.get_mut(&turn_id) {
            turn.prepared.oauth_transport = is_oauth;
            turn.prepared.store = turn.prepared.explicit_store.unwrap_or(!is_oauth);
            if let Some(signer) = turn.prepared_signers.remove(&upstream_id) {
                self.connection_signers
                    .entry(connection_id)
                    .or_insert(signer);
            }
            turn.connection_id = Some(connection_id);
            turn.upstream_id = Some(upstream_id);
            if let Some(context) = turn.prepared.prompt_cache_observation_context.as_mut() {
                context.upstream_id = upstream_id;
            }
            turn.prepared
                .span
                .record("cc_lb.responses.connection_id", connection_id);
            turn.prepared
                .span
                .record("cc_lb.upstream.id", tracing::field::display(upstream_id));
            if turn.prepared.route_info.upstream_id != upstream_id {
                turn.prepared.route_info.upstream_id = upstream_id;
                turn.prepared.route_info.upstream_name = upstream_name.to_owned();
                turn.prepared.route_info.routing_trace = None;
                turn.prepared.route_info.upstream_kind = None;
                turn.prepared.route_info.route_ms = None;
                turn.prepared.route_info.predicted_cache_read_tokens = None;
                turn.prepared.route_info.matched_v3_cache_key = None;
                turn.prepared.route_info.breakpoint_content_block_index = None;
                turn.prepared.route_info.matched_content_block_index = None;
                turn.prepared.route_info.lookback_distance = None;
                turn.prepared.route_info.predicted_cache_creation_tokens_5m = None;
                turn.prepared.route_info.predicted_cache_creation_tokens_1h = None;
                turn.prepared.route_info.token_estimate_source = None;
                turn.prepared.route_info.cache_value_micros = None;
                turn.prepared.route_info.formula_winner_upstream_id = None;
                turn.prepared.route_info.kept_upstream_id = None;
                turn.prepared.route_info.quota_urgency_5h = None;
                turn.prepared.route_info.quota_urgency_7d = None;
                turn.prepared.route_info.quota_urgency_combined = None;
                turn.prepared.route_info.quota_warning_multiplier = None;
                if let Some(observer) = turn.prepared.observer.as_ref() {
                    observer.emit_lifecycle(LifecycleEvent::RouteCompleted {
                        event_id: observer.event_id().to_owned(),
                        result: Ok(turn.prepared.route_info.clone()),
                        routing_trace: None,
                    });
                }
            }
        }
    }

    /// Read-only pre-send validation for a turn and candidate upstream, suitable
    /// for rechecking fresh quota, auth, and reservation state before committing
    /// connection ownership or session pinning.
    pub async fn validate_candidate_for_send(
        &mut self,
        turn_id: u64,
        upstream_id: Uuid,
        existing_connection_id: Option<u64>,
        has_remaining_candidates: bool,
    ) -> Result<(), WebSocketError> {
        let Some(turn) = self.turns.get(&turn_id) else {
            return Err(WebSocketError::new(
                409,
                "response_not_found",
                "The queued response is no longer available",
            ));
        };
        let auth = self.authenticate().await.map_err(|error| {
            // The already-owned turn reports this authentication failure. Do not
            // construct another observer, which would duplicate the request row.
            WebSocketError::new(
                error.http_status(),
                "authentication_error",
                error.to_string(),
            )
            .with_stream(turn.lane.clone())
        })?;
        if auth.principal_id() != turn.prepared.principal_id
            || auth.key_id() != turn.prepared.key_id
        {
            return Err(WebSocketError::new(
                401,
                "authentication_error",
                "The client identity changed before dispatch",
            )
            .with_stream(turn.lane.clone()));
        }
        if let Some(connection_id) = existing_connection_id
            && self.connections.get(&connection_id) != Some(&upstream_id)
        {
            return Err(WebSocketError::new(
                409,
                "upstream_affinity_unavailable",
                "The selected provider connection is no longer owned by this upstream",
            )
            .with_stream(turn.lane.clone())
            .with_turn_id(turn_id));
        }
        if turn.pinned.is_some_and(|(connection, upstream)| {
            existing_connection_id.is_some_and(|conn| conn != connection) || upstream_id != upstream
        }) {
            return Err(WebSocketError::new(
                409,
                "upstream_affinity_unavailable",
                "A continuation cannot move to another provider connection",
            )
            .with_stream(turn.lane.clone())
            .with_turn_id(turn_id));
        }
        let is_candidate_oauth = turn
            .candidate_oauth
            .get(&upstream_id)
            .copied()
            .or_else(|| self.lifecycle.is_openai_oauth_upstream(upstream_id))
            .unwrap_or(false);
        turn.ensure_credential_eligible(!is_candidate_oauth)?;
        if self.session_oauth
            && is_candidate_oauth
            && !turn.prepared.has_explicit_affinity
            && turn.admitted_session_upstream.is_none()
            && let Some(owner) = self.session_upstream.filter(|owner| *owner != upstream_id)
        {
            if is_prewarm_body(&turn.body) {
                if self
                    .lifecycle
                    .validate_websocket_turn_with_policy(
                        &auth,
                        WebSocketIngress {
                            headers: &self.headers,
                            frame: &turn.frame,
                            query: self.query.as_deref(),
                        },
                        &turn.body,
                        owner,
                        self.codex_backend,
                        true,
                    )
                    .await
                    .is_ok()
                {
                    return Err(WebSocketError::new(
                        409,
                        "upstream_affinity_unavailable",
                        "The session is pinned to another OAuth account",
                    )
                    .with_stream(turn.lane.clone())
                    .with_turn_id(turn_id));
                }
            } else {
                return Err(WebSocketError::new(
                    409,
                    "upstream_affinity_unavailable",
                    "The session is pinned to another OAuth account",
                )
                .with_stream(turn.lane.clone())
                .with_turn_id(turn_id));
            }
        }
        let reject_exhausted = Self::reject_exhausted_fallback(turn, upstream_id)
            || (turn.pinned.is_none() && has_remaining_candidates);
        self.lifecycle
            .validate_websocket_turn_with_policy(
                &auth,
                WebSocketIngress {
                    headers: &self.headers,
                    frame: &turn.frame,
                    query: self.query.as_deref(),
                },
                &turn.body,
                upstream_id,
                self.codex_backend,
                reject_exhausted,
            )
            .await
            .map_err(|error| error.with_stream(turn.lane.clone()))?;
        let turn = self.turns.get(&turn_id).ok_or_else(|| {
            WebSocketError::new(
                409,
                "response_not_found",
                "The admitted response is no longer available",
            )
        })?;
        if turn
            .prepared
            .reservation
            .as_ref()
            .is_some_and(|reservation| !reservation.is_active())
        {
            return Err(WebSocketError::new(
                503,
                "reservation_expired",
                "The queued response reservation is no longer active",
            )
            .with_stream(turn.lane.clone()));
        }
        if let Some((deferred_ctx, turn_body, observer, reservation_id, lane)) = turn
            .prepared
            .deferred_limit_context
            .as_ref()
            .map(|deferred_ctx| {
                (
                    deferred_ctx.clone(),
                    Arc::clone(&turn.body),
                    turn.prepared.observer.clone(),
                    turn.prepared
                        .reservation
                        .as_ref()
                        .map(|reservation| reservation.id().to_owned()),
                    turn.lane.clone(),
                )
            })
        {
            let cache_context = self
                .lifecycle
                .resize_websocket_deferred_limit(
                    &auth,
                    &self.headers,
                    &deferred_ctx,
                    upstream_id,
                    &turn_body,
                    reservation_id.as_deref(),
                    observer.as_ref(),
                )
                .await
                .map_err(|error| error.with_stream(lane))?;
            let turn = self.turns.get_mut(&turn_id).ok_or_else(|| {
                WebSocketError::new(
                    409,
                    "response_not_found",
                    "The admitted response is no longer available",
                )
            })?;
            turn.prepared.prompt_cache_observation_context = cache_context;
        }
        Ok(())
    }

    fn clear_suppressed_failover_error_on_turn(
        used_bytes: &mut usize,
        turn: &mut Turn,
    ) -> Option<SuppressedFailoverError> {
        let suppressed = turn.suppressed_failover_error.take()?;
        let released = suppressed.charged_bytes.min(turn.charged_bytes);
        *used_bytes = used_bytes.saturating_sub(released);
        turn.charged_bytes = turn.charged_bytes.saturating_sub(released);
        Some(suppressed)
    }

    pub fn commit_send_attempt(&mut self, turn_id: u64) {
        if let Some(turn) = self.turns.get_mut(&turn_id)
            && !turn.attempted
            && let Some(upstream_id) = turn.upstream_id
        {
            Self::clear_suppressed_failover_error_on_turn(&mut self.used_bytes, turn);
            turn.attempted = true;
            turn.attempt_num = turn.attempt_num.saturating_add(1);
            turn.prepared.deferred_limit_context = None;
            if let Some(deferred) = turn.deferred_unestablished.take()
                && deferred.currently_stripped
            {
                let released = deferred.original_bytes;
                self.used_bytes = self.used_bytes.saturating_sub(released);
                turn.charged_bytes = turn.charged_bytes.saturating_sub(released);
            }
            if let Some(observer) = turn.prepared.observer.as_ref() {
                observer.emit_lifecycle(LifecycleEvent::UpstreamAttempt {
                    event_id: observer.event_id().to_owned(),
                    attempt_num: turn.attempt_num,
                    upstream_id,
                });
            }
        }
    }

    pub fn mark_failover_replacement_dialed(&mut self, turn_id: u64) {
        if let Some(turn) = self.turns.get_mut(&turn_id) {
            Self::clear_suppressed_failover_error_on_turn(&mut self.used_bytes, turn);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn build_suppressed_live_failover_error(
        &self,
        connection_id: u64,
        kind: &str,
        value: &Value,
        lane: &Option<String>,
        raw_frame: &Bytes,
        raw_text: bool,
        public_frame: Option<&Bytes>,
        transformed_frames: Option<&[Bytes]>,
    ) -> WebSocketError {
        let signer = self.connection_signers.get(&connection_id).map(Arc::as_ref);
        let (classified, _) = classify_terminal_frame(kind, value, lane, signer);
        let mut suppressed =
            classified.unwrap_or_else(|| native_terminal_error_with_signer(value, lane, signer));
        let outbound_frames: Option<Vec<WebSocketWireFrame>> =
            if let Some(frames) = transformed_frames {
                frames
                    .iter()
                    .all(|bytes| std::str::from_utf8(bytes).is_ok())
                    .then(|| {
                        frames
                            .iter()
                            .cloned()
                            .map(|bytes| WebSocketWireFrame { bytes, text: true })
                            .collect()
                    })
            } else if !self.codex_backend
                && let Some(public) = public_frame
            {
                std::str::from_utf8(public).is_ok().then(|| {
                    vec![WebSocketWireFrame {
                        bytes: public.clone(),
                        text: true,
                    }]
                })
            } else {
                (!raw_text || std::str::from_utf8(raw_frame).is_ok()).then(|| {
                    vec![WebSocketWireFrame {
                        bytes: raw_frame.clone(),
                        text: raw_text,
                    }]
                })
            };
        if let Some(frames) = outbound_frames {
            suppressed = suppressed.with_raw_provider_frames(frames);
        }
        suppressed
    }

    fn record_suppressed_failover_error(
        &mut self,
        turn_id: u64,
        failed_upstream_id: Uuid,
        failed_upstream_name: String,
        previous_session_upstream: Option<Uuid>,
        error: WebSocketError,
        charged_bytes: usize,
    ) {
        let previous_session_upstream =
            previous_session_upstream.filter(|_| self.session_upstream.is_none());
        if let Some(turn) = self.turns.get_mut(&turn_id) {
            Self::clear_suppressed_failover_error_on_turn(&mut self.used_bytes, turn);
            self.used_bytes = self.used_bytes.saturating_add(charged_bytes);
            turn.charged_bytes = turn.charged_bytes.saturating_add(charged_bytes);
            turn.suppressed_failover_error = Some(SuppressedFailoverError {
                error,
                upstream_id: Some(failed_upstream_id),
                upstream_name: failed_upstream_name,
                previous_session_upstream,
                charged_bytes,
            });
        }
    }

    pub async fn validate_send(&mut self, turn_id: u64) -> Result<(), WebSocketError> {
        if self
            .turns
            .get(&turn_id)
            .is_some_and(|turn| turn.deferred_unestablished.is_some())
        {
            self.apply_unestablished_reasoning(turn_id, true)?;
        }
        let Some(turn) = self.turns.get(&turn_id) else {
            return Err(WebSocketError::new(
                409,
                "response_not_found",
                "The queued response is no longer available",
            ));
        };
        let upstream_id = turn.upstream_id.ok_or_else(|| {
            WebSocketError::new(
                502,
                "upstream_unavailable",
                "No upstream owns this dispatch",
            )
        })?;
        let connection_id = turn.connection_id.ok_or_else(|| {
            WebSocketError::new(
                409,
                "upstream_affinity_unavailable",
                "The selected provider connection is no longer owned by this upstream",
            )
            .with_stream(turn.lane.clone())
            .with_turn_id(turn_id)
        })?;
        self.validate_candidate_for_send(turn_id, upstream_id, Some(connection_id), false)
            .await?;
        self.commit_send_attempt(turn_id);
        Ok(())
    }

    pub fn record_dispatch_timings(&mut self, turn_id: u64, timings: ResponsesDispatchTimings) {
        if let Some(turn) = self.turns.get_mut(&turn_id) {
            turn.metrics.record_dispatch_timings(timings);
        }
    }

    pub fn sent(&mut self, turn_id: u64) -> Vec<WebSocketDispatch> {
        let connection = self.turns.get(&turn_id).and_then(|turn| turn.connection_id);
        if let Some(connection) = connection
            && self.turns.iter().any(|(id, turn)| {
                *id != turn_id && turn.sent && turn.connection_id == Some(connection)
            })
        {
            for (id, turn) in &mut self.turns {
                if turn.connection_id == Some(connection) && (*id == turn_id || turn.sent) {
                    turn.funding_ambiguous = true;
                    if let Some(observer) = turn.prepared.observer.as_ref() {
                        observer.emit_lifecycle(LifecycleEvent::RequestFundingObserved {
                            event_id: observer.event_id().to_owned(),
                            attempt_num: 1,
                            funding: None,
                            response_id: turn.response_id.clone(),
                            ambiguous: true,
                        });
                    }
                }
            }
        }
        let Some(turn) = self.turns.get_mut(&turn_id) else {
            return Vec::new();
        };
        turn.sent = true;
        let mut ready = Vec::with_capacity(turn.controls.len());
        while let Some(frame) = turn.controls.pop_front() {
            let charge = queued_control_bytes(&frame);
            self.used_bytes = self.used_bytes.saturating_sub(charge);
            turn.charged_bytes = turn.charged_bytes.saturating_sub(charge);
            self.control_slots = self.control_slots.saturating_sub(1);
            ready.push(WebSocketDispatch {
                turn_id,
                frame,
                connection_id: turn.connection_id,
                candidates: Vec::new(),
                stream_id: turn.lane.clone(),
                control: false,
                prefer_api_key_on_final_attempt: false,
                has_explicit_affinity: false,
                is_failover_recovery: false,
            });
        }
        ready
    }

    pub fn sent_with_timings(
        &mut self,
        turn_id: u64,
        timings: ResponsesDispatchTimings,
        send_started: std::time::Instant,
    ) -> Vec<WebSocketDispatch> {
        if let Some(turn) = self.turns.get_mut(&turn_id) {
            turn.metrics.mark_sent(timings, send_started);
        }
        self.sent(turn_id)
    }

    pub fn begin_downstream_send(&mut self, connection_id: u64) {
        let now = std::time::Instant::now();
        for turn in self.turns.values_mut() {
            if turn.connection_id == Some(connection_id)
                && turn.sent
                && turn.metrics.relay_started.is_some()
            {
                turn.metrics.io_timing.transition_at(
                    Some(crate::body_io_timing::BodyIoPhase::DownstreamPollGap),
                    now,
                );
            }
        }
    }

    pub fn finish_downstream_send(&mut self, connection_id: u64) {
        let now = std::time::Instant::now();
        for turn in self.turns.values_mut() {
            if turn.connection_id == Some(connection_id)
                && turn.sent
                && turn.metrics.relay_started.is_some()
            {
                turn.metrics
                    .io_timing
                    .transition_at(Some(crate::body_io_timing::BodyIoPhase::Wait), now);
            }
        }
    }

    fn resolve_provider_turn_id(
        &self,
        connection_id: u64,
        response_id: Option<&str>,
        parent_response_id: Option<&str>,
        lane: Option<&str>,
        is_error: bool,
    ) -> Option<u64> {
        let mut turn_id = response_id
            .and_then(|id| self.owners.get(id))
            .map(|owner| owner.turn_id);
        if turn_id.is_none() && !is_error {
            let active = self
                .lanes
                .iter()
                .find_map(|(candidate, lane_state)| {
                    (candidate.as_deref() == lane).then_some(lane_state.active)
                })
                .flatten();
            turn_id = parent_response_id
                .and_then(|parent| self.successor_for_parent(parent))
                .or_else(|| {
                    active
                        .and_then(|active| self.turns.get(&active))
                        .and_then(|active| active.response_id.as_deref())
                        .and_then(|parent| self.successor_for_parent(parent))
                })
                .or(active);
        }
        if is_error && turn_id.is_none() {
            turn_id = self
                .lanes
                .iter()
                .find_map(|(candidate, lane_state)| {
                    (candidate.as_deref() == lane).then_some(lane_state.active)
                })
                .flatten()
                .filter(|id| {
                    self.turns
                        .get(id)
                        .is_some_and(|turn| turn.connection_id == Some(connection_id) && turn.sent)
                });
        }
        turn_id
    }

    fn observe_provider_metrics(&mut self, connection_id: u64, frame: &Bytes) -> Option<u64> {
        let bounded = |value: Option<sonic_rs::LazyValue<'_>>, max_len| {
            value.and_then(|value| {
                let value = value.as_str()?;
                (!value.is_empty() && value.len() <= max_len).then(|| value.to_owned())
            })
        };
        let lane = bounded(sonic_rs::get_from_slice(frame, &["stream_id"]).ok(), 256);
        let response_id = bounded(
            sonic_rs::get_from_slice(frame, &["response", "id"]).ok(),
            512,
        )
        .or_else(|| bounded(sonic_rs::get_from_slice(frame, &["response_id"]).ok(), 512));
        let parent_response_id = bounded(
            sonic_rs::get_from_slice(frame, &["response", "previous_response_id"]).ok(),
            512,
        )
        .or_else(|| {
            bounded(
                sonic_rs::get_from_slice(frame, &["steer", "previous_response_id"]).ok(),
                512,
            )
        });
        let is_error = sonic_rs::get_from_slice(frame, &["type"])
            .is_ok_and(|value| value.as_str() == Some("error"));
        if let Some(id) = response_id.as_deref()
            && !self.pending_error_terminals.contains_key(id)
            && !self.turns.values().any(|turn| {
                turn.connection_id == Some(connection_id) && turn.response_id.as_deref() == Some(id)
            })
            && self.is_retired_terminal(connection_id, &lane, id)
        {
            return None;
        }
        let turn_id = self.resolve_provider_turn_id(
            connection_id,
            response_id.as_deref(),
            parent_response_id.as_deref(),
            lane.as_deref(),
            is_error,
        )?;
        if let Some(turn) = self.turns.get_mut(&turn_id)
            && turn.connection_id == Some(connection_id)
            && turn.sent
        {
            let now = std::time::Instant::now();
            let outcome = turn
                .metrics
                .observe_at(frame, now, self.activity_heartbeat_interval_ms);
            if let Some(observer) = turn.prepared.observer.as_ref() {
                if !turn.metrics.upstream_started_emitted {
                    turn.metrics.upstream_started_emitted = true;
                    observer.set_attempt_bulkhead_wait_ms(turn.metrics.bulkhead_wait_ms);
                    observer.set_attempt_connection_timings(
                        turn.metrics.dns_ms,
                        turn.metrics.connect_ms,
                        turn.metrics.connection_reused,
                    );
                    observer.set_attempt_timings(
                        turn.metrics.shape_ms,
                        turn.metrics.sign_ms,
                        turn.metrics.upstream_ttfb_ms,
                    );
                    observer.emit_lifecycle(LifecycleEvent::UpstreamResponseStarted {
                        event_id: observer.event_id().to_owned(),
                        status: StatusCode::SWITCHING_PROTOCOLS.as_u16(),
                        headers: Default::default(),
                        bulkhead_wait_ms: turn.metrics.bulkhead_wait_ms,
                        dns_ms: turn.metrics.dns_ms,
                        connect_ms: turn.metrics.connect_ms,
                        connection_reused: turn.metrics.connection_reused,
                        shape_ms: turn.metrics.shape_ms,
                        sign_ms: turn.metrics.sign_ms,
                        upstream_ttfb_ms: turn.metrics.upstream_ttfb_ms,
                        observed_at_unix_millis: None,
                        protocol: Some(cc_lb_request_log::RequestProtocol::Websocket),
                    });
                }
                if let Some(first_delta_ms) = outcome.first_content_delta_ms {
                    observer.set_first_content_delta_ms(first_delta_ms);
                }
                if outcome.heartbeat_due {
                    observer.emit_request_activity();
                }
            }
            if response_id.is_some() && response_id == turn.response_id && lane == turn.lane {
                return Some(turn_id);
            }
        } else if let Some(pending) = response_id
            .as_deref()
            .and_then(|id| self.pending_error_terminals.get_mut(id))
            && pending.turn_id == turn_id
            && pending.turn.connection_id == Some(connection_id)
            && pending.turn.sent
        {
            pending.turn.metrics.observe_at(
                frame,
                std::time::Instant::now(),
                self.activity_heartbeat_interval_ms,
            );
        }
        None
    }

    /// Record provider WebSocket keepalive `Ping` activity for active turns on
    /// `connection_id` so long silent pauses with periodic pings still refresh
    /// lifecycle and reservation idle TTLs.
    pub fn observe_provider_ping(&mut self, connection_id: u64) {
        let heartbeat_interval_ms = self.activity_heartbeat_interval_ms;
        for turn in self.turns.values_mut() {
            if turn.connection_id == Some(connection_id) && turn.sent {
                turn.metrics.ping_count = turn.metrics.ping_count.saturating_add(1);
                if turn.metrics.observe_heartbeat_only(heartbeat_interval_ms)
                    && let Some(observer) = turn.prepared.observer.as_ref()
                {
                    observer.emit_request_activity();
                }
            }
        }
    }

    /// Returns whether any WebSocket turn is still open (in-flight or queued
    /// behind an earlier turn in a serialized lane).
    pub fn has_active_turns(&self) -> bool {
        !self.turns.is_empty()
    }

    /// Emit throttled activity heartbeats for active turns even when neither
    /// upstream frames nor provider `Ping`s arrive during long silent reasoning.
    pub fn observe_active_turns_heartbeat(&mut self) {
        let heartbeat_interval_ms = self.activity_heartbeat_interval_ms;
        for turn in self.turns.values_mut() {
            if turn.metrics.observe_heartbeat_only(heartbeat_interval_ms)
                && let Some(observer) = turn.prepared.observer.as_ref()
            {
                observer.emit_request_activity();
            }
        }
    }

    pub async fn provider_message(
        &mut self,
        connection_id: u64,
        frame: &Bytes,
    ) -> Result<WebSocketEventResult, WebSocketError> {
        self.provider_message_with_pool(connection_id, frame, &WsRelayPoolSnapshot::default())
            .await
    }

    pub async fn provider_message_with_pool(
        &mut self,
        connection_id: u64,
        frame: &Bytes,
        relay_pool: &WsRelayPoolSnapshot,
    ) -> Result<WebSocketEventResult, WebSocketError> {
        self.provider_message_with_pool_and_opcode(connection_id, frame, true, relay_pool)
            .await
    }

    pub async fn provider_message_with_pool_and_opcode(
        &mut self,
        connection_id: u64,
        frame: &Bytes,
        text: bool,
        relay_pool: &WsRelayPoolSnapshot,
    ) -> Result<WebSocketEventResult, WebSocketError> {
        let public_frame = self.oauth_public_frame(connection_id, frame);
        let processing_frame = if self.codex_backend {
            self.native_observation_frame(connection_id, frame)
        } else {
            public_frame.clone().unwrap_or_else(|| frame.clone())
        };
        let quota_turn = self.observe_provider_metrics(connection_id, &processing_frame);
        if public_frame.as_ref().is_some_and(Bytes::is_empty) {
            return Ok(WebSocketEventResult::default());
        }
        let transformed = self.transform_provider_frame(connection_id, &processing_frame);
        let transformed_frames = transformed
            .as_ref()
            .ok()
            .and_then(|(_, frames)| frames.as_deref());
        let mut result = self
            .observe_provider_message(
                connection_id,
                &processing_frame,
                frame,
                text,
                public_frame.as_ref(),
                transformed_frames,
                transformed.as_ref().err().map(|boxed| &boxed.1),
                quota_turn,
                relay_pool,
            )
            .await?;
        let mut pending_errors = self.take_pending_errors();
        if !pending_errors.is_empty() {
            pending_errors.append(&mut result.errors);
            result.errors = pending_errors;
        }
        match transformed {
            Ok((_, Some(frames))) => {
                if result.forward {
                    result.forward = false;
                    result.frames = Some(frames);
                }
            }
            Ok((_, None)) => {
                if result.forward
                    && !self.codex_backend
                    && let Some(public_frame) = public_frame
                {
                    result.forward = false;
                    result.frames = Some(vec![public_frame]);
                }
            }
            Err(boxed) => {
                let (target_turn_id, error) = *boxed;
                result.forward = false;
                if let Some(turn_id) = target_turn_id
                    && let Some(lane) = self.turns.get(&turn_id).map(|turn| turn.lane.clone())
                {
                    self.finish_turn(turn_id, Some(&error));
                    if let Some(dispatch) = self.next_in_lane(&lane) {
                        result.dispatches.push(dispatch);
                    }
                }
                result.errors.push(error);
            }
        }
        Ok(result)
    }
    fn native_observation_frame(&self, connection_id: u64, frame: &Bytes) -> Bytes {
        if frame.windows(11).any(|w| w == b"\"stream_id\"") {
            return frame.clone();
        }
        let Some(lane) = self
            .connection_lanes
            .get(&connection_id)
            .and_then(Option::as_ref)
        else {
            return frame.clone();
        };
        let Ok(encoded) = serde_json::to_vec(lane) else {
            return frame.clone();
        };
        if frame.first() != Some(&b'{') {
            return frame.clone();
        }
        let mut output = Vec::with_capacity(frame.len() + encoded.len() + 14);
        output.extend_from_slice(b"{\"stream_id\":");
        output.extend_from_slice(&encoded);
        if frame.len() > 1 {
            output.push(b',');
            output.extend_from_slice(&frame[1..]);
        } else {
            output.push(b'}');
        }
        Bytes::from(output)
    }

    fn oauth_public_frame(&self, connection_id: u64, frame: &Bytes) -> Option<Bytes> {
        let frame_response_id = sonic_rs::get_from_slice(frame, &["response", "id"])
            .ok()
            .and_then(|value| value.as_str().map(str::to_owned))
            .or_else(|| {
                sonic_rs::get_from_slice(frame, &["response_id"])
                    .ok()
                    .and_then(|value| value.as_str().map(str::to_owned))
            });
        let (oauth_transport, oauth_compat, echo_metadata, lane) = if let Some(pending) =
            frame_response_id
                .as_deref()
                .and_then(|id| self.pending_error_terminals.get(id))
                .filter(|pending| pending.turn.connection_id == Some(connection_id))
        {
            (
                pending.turn.prepared.oauth_transport,
                pending.turn.prepared.oauth_compat,
                pending.turn.echo_metadata.as_ref(),
                pending.turn.lane.clone(),
            )
        } else if let Some(owner) = frame_response_id
            .as_deref()
            .and_then(|id| self.owners.get(id))
            .filter(|owner| {
                owner.connection_id == connection_id
                    && matches!(
                        owner.terminal_delivery,
                        TerminalDelivery::ErrorAwaitingResponseFailed { .. }
                    )
            })
        {
            (
                owner.oauth_transport,
                owner.oauth_compat,
                owner.echo_metadata.as_ref(),
                owner.lane.clone(),
            )
        } else {
            let lane = self.connection_lanes.get(&connection_id)?.clone();
            let turn = self.turns.values().find(|turn| {
                turn.connection_id == Some(connection_id) && turn.sent && turn.lane == lane
            })?;
            (
                turn.prepared.oauth_transport,
                turn.prepared.oauth_compat,
                turn.echo_metadata.as_ref(),
                lane,
            )
        };
        if !oauth_transport || self.codex_backend {
            return None;
        }
        if !oauth_compat {
            return lane
                .as_deref()
                .filter(|_| !frame.windows(11).any(|w| w == b"\"stream_id\""))
                .map(|lane| {
                    let encoded = serde_json::to_vec(lane).expect("lane is JSON");
                    let mut output = Vec::with_capacity(frame.len() + encoded.len() + 14);
                    if frame.first() == Some(&b'{') {
                        output.extend_from_slice(b"{\"stream_id\":");
                        output.extend_from_slice(&encoded);
                        if frame.len() > 1 {
                            output.push(b',');
                            output.extend_from_slice(&frame[1..]);
                        } else {
                            output.push(b'}');
                        }
                        Bytes::from(output)
                    } else {
                        frame.clone()
                    }
                });
        }
        let mut value: Value = sonic_rs::from_slice(frame).ok()?;
        if value.get("type").and_then(Value::as_str) == Some("response.metadata") {
            return Some(Bytes::new());
        }
        let mut changed = false;
        if let Some(lane) = lane.as_ref()
            && value.get("stream_id").is_none()
            && let Some(object) = value.as_object_mut()
        {
            object.insert("stream_id".to_owned(), Value::String(lane.clone()));
            changed = true;
        }
        if let Some(metadata) = echo_metadata
            && value.get("response").is_some_and(Value::is_object)
        {
            crate::openai_compat::response::echo_metadata_into(&mut value, metadata);
            changed = true;
        }
        changed
            .then(|| serde_json::to_vec(&value).ok().map(Bytes::from))
            .flatten()
    }

    fn transform_provider_frame(
        &self,
        connection_id: u64,
        frame: &Bytes,
    ) -> TransformProviderFrameResult {
        let has_turn_hook = self.turns.values().any(|turn| {
            turn.connection_id == Some(connection_id)
                && turn.plugin_request.as_ref().is_some_and(|(_, request)| {
                    request.context.dialect.sse_event_transform_hook().is_some()
                })
        });
        let has_pending_hook = self.pending_error_terminals.values().any(|pending| {
            pending.turn.connection_id == Some(connection_id)
                && pending
                    .turn
                    .plugin_request
                    .as_ref()
                    .is_some_and(|(_, request)| {
                        request.context.dialect.sse_event_transform_hook().is_some()
                    })
        });
        if !has_turn_hook && !has_pending_hook {
            return Ok((None, None));
        }
        let lane = sonic_rs::get_from_slice(frame, &["stream_id"])
            .ok()
            .and_then(|value| value.as_str().map(str::to_owned));
        let response_id = sonic_rs::get_from_slice(frame, &["response_id"])
            .ok()
            .or_else(|| sonic_rs::get_from_slice(frame, &["response", "id"]).ok());
        let response_id_str = response_id.as_ref().and_then(|id| id.as_str());
        let terminal_event = sonic_rs::get_from_slice(frame, &["type"]).is_ok_and(|value| {
            matches!(
                value.as_str(),
                Some("response.completed" | "response.failed" | "response.incomplete" | "error")
            )
        });
        if terminal_event
            && let Some(id) = response_id_str
            && !self.pending_error_terminals.contains_key(id)
            && !self.turns.values().any(|turn| {
                turn.connection_id == Some(connection_id) && turn.response_id.as_deref() == Some(id)
            })
            && self.is_retired_terminal(connection_id, &lane, id)
        {
            return Ok((None, None));
        }
        let owned_turn = response_id_str.and_then(|id| self.owners.get(id));
        let (target_turn_id, turn) = match owned_turn {
            Some(owner) => (
                Some(owner.turn_id),
                self.turns.get(&owner.turn_id).or_else(|| {
                    response_id_str
                        .and_then(|id| self.pending_error_terminals.get(id))
                        .filter(|pending| pending.turn_id == owner.turn_id)
                        .map(|pending| &pending.turn)
                }),
            ),
            None => match response_id_str.and_then(|id| self.pending_error_terminals.get(id)) {
                Some(pending) => (Some(pending.turn_id), Some(&pending.turn)),
                None => match self.lanes.get(&lane).and_then(|lane| lane.active) {
                    Some(id) => (Some(id), self.turns.get(&id)),
                    None => (None, None),
                },
            },
        };
        let Some(turn) = turn.filter(|turn| turn.connection_id == Some(connection_id) && turn.sent)
        else {
            return Ok((target_turn_id, None));
        };
        let Some((_, request)) = turn.plugin_request.as_ref() else {
            return Ok((target_turn_id, None));
        };
        transform_websocket_event(
            &request.context,
            &turn.prepared.request_id,
            &turn.prepared.model,
            frame,
            self.byte_cap,
        )
        .map(|frames| (target_turn_id, frames))
        .map_err(|error| Box::new((target_turn_id, error.with_stream(lane))))
    }

    #[allow(clippy::too_many_arguments)]
    async fn observe_provider_message(
        &mut self,
        connection_id: u64,
        frame: &Bytes,
        raw_frame: &Bytes,
        raw_text: bool,
        public_frame: Option<&Bytes>,
        transformed_frames: Option<&[Bytes]>,
        transform_error: Option<&WebSocketError>,
        quota_turn: Option<u64>,
        relay_pool: &WsRelayPoolSnapshot,
    ) -> Result<WebSocketEventResult, WebSocketError> {
        // A borrowed lazy discriminator is the only work for deltas. It does
        // not construct a Value, copy payload bytes, or record observations.
        let kind = {
            let event_type = sonic_rs::get_from_slice(frame, &["type"]).map_err(|_| {
                WebSocketError::new(
                    502,
                    "invalid_upstream_response",
                    "Upstream WebSocket event has no valid type",
                )
            })?;
            match event_type.as_str() {
                Some("response.created") => "response.created",
                Some("response.queued") => "response.queued",
                Some("response.in_progress") => "response.in_progress",
                Some("response.completed") => "response.completed",
                Some("response.failed") => "response.failed",
                Some("response.incomplete") => "response.incomplete",
                Some("response.output_item.done") => "response.output_item.done",
                Some("error") => "error",
                Some("response.steer.accepted") => "response.steer.accepted",
                Some("response.steer.pending") => "response.steer.pending",
                Some("response.steer.failed") => "response.steer.failed",
                Some("codex.rate_limits") => {
                    if frame.len() <= self.byte_cap
                        && let Some(upstream_id) = self.connections.get(&connection_id).copied()
                    {
                        let matched =
                            quota_turn
                                .and_then(|id| self.turns.get(&id))
                                .filter(|turn| {
                                    !turn.funding_ambiguous
                                        && turn.response_created
                                        && turn.connection_id == Some(connection_id)
                                        && self
                                            .turns
                                            .values()
                                            .filter(|other| {
                                                other.sent
                                                    && other.connection_id == Some(connection_id)
                                            })
                                            .take(2)
                                            .count()
                                            == 1
                                });
                        let evidence = matched.and_then(|turn| {
                            Some((
                                turn.prepared.observer.as_ref()?.event_id(),
                                turn.response_id.as_deref()?,
                            ))
                        });
                        self.lifecycle.observe_websocket_quota_frame(
                            upstream_id,
                            frame.clone(),
                            evidence.map(|(event_id, _)| event_id),
                            evidence.map(|(_, response_id)| response_id),
                        );
                    }
                    return Ok(WebSocketEventResult {
                        forward: true,
                        ..Default::default()
                    });
                }
                Some(_) => {
                    return Ok(WebSocketEventResult {
                        forward: true,
                        ..Default::default()
                    });
                }
                None => {
                    return Err(WebSocketError::new(
                        502,
                        "invalid_upstream_response",
                        "Upstream event type is not a string",
                    ));
                }
            }
        };
        if frame.len() > self.byte_cap {
            return Err(WebSocketError::new(
                502,
                "invalid_upstream_response",
                "Upstream control exceeds the configured message cap",
            ));
        }
        let value: Value = sonic_rs::from_slice(frame).map_err(|_| {
            WebSocketError::new(
                502,
                "invalid_upstream_response",
                "Upstream control is not valid JSON",
            )
        })?;
        let lane = stream_id(&value).map_err(|_| {
            WebSocketError::new(
                502,
                "invalid_upstream_response",
                "Upstream control carries an invalid stream_id",
            )
        })?;
        if kind == "error"
            && value.pointer("/error/code").and_then(Value::as_str)
                == Some("websocket_connection_limit_reached")
        {
            return Err(WebSocketError::new(
                400,
                "websocket_connection_limit_reached",
                value
                    .pointer("/error/message")
                    .and_then(Value::as_str)
                    .unwrap_or("The provider WebSocket reached its connection limit"),
            ));
        }
        if kind.starts_with("response.steer.") {
            return self.steering_event(connection_id, kind, &value, lane);
        }
        let response = value.get("response");
        let response_id = response
            .and_then(|response| response.get("id"))
            .and_then(Value::as_str)
            .or_else(|| value.get("response_id").and_then(Value::as_str));
        let terminal = matches!(
            kind,
            "response.completed" | "response.failed" | "response.incomplete" | "error"
        );
        if terminal && kind != "error" && response_id.is_none() {
            return Err(WebSocketError::new(
                502,
                "invalid_upstream_response",
                "Upstream terminal has no response identity",
            )
            .with_stream(lane));
        }
        if terminal
            && let Some(id) = response_id
            && !self.pending_error_terminals.contains_key(id)
            && !self.turns.values().any(|turn| {
                turn.connection_id == Some(connection_id) && turn.response_id.as_deref() == Some(id)
            })
            && self.is_retired_terminal(connection_id, &lane, id)
        {
            return Ok(WebSocketEventResult::default());
        }
        let parent_response_id = response
            .and_then(|response| response.get("previous_response_id"))
            .and_then(Value::as_str);
        let turn_id = self.resolve_provider_turn_id(
            connection_id,
            response_id,
            parent_response_id,
            lane.as_deref(),
            kind == "error",
        );
        let turn = turn_id.and_then(|id| self.turns.get(&id));
        if turn.is_none()
            && terminal
            && response_id
                .and_then(|id| self.owners.get(id))
                .is_some_and(|owner| {
                    Some(owner.turn_id) == turn_id
                        && owner.connection_id == connection_id
                        && owner.lane == lane
                })
        {
            if kind == "response.failed"
                && let Some(id) = response_id
                && self.pending_error_terminals.contains_key(id)
            {
                // Keep the parked turn registered until affinity binding resolves so
                // cancellation or drain during storage I/O still settles measured usage.
                let is_oauth = self.connection_oauth.get(&connection_id).copied();
                let pending_oauth = self
                    .pending_error_terminals
                    .get(id)
                    .is_some_and(|pending| pending.turn.prepared.oauth_transport);
                let tier_policy = if is_oauth.unwrap_or(pending_oauth) {
                    ServiceTierMergePolicy::OpenaiOauth
                } else {
                    ServiceTierMergePolicy::Standard
                };
                let usage_update =
                    response.and_then(|resp| observed_response_usage(resp, tier_policy));
                let byte_cap = self.byte_cap;
                let mut usage_overflow: Option<(Box<UsageCounts>, bool)> = None;
                let upstream_result = {
                    let pending = self
                        .pending_error_terminals
                        .get_mut(id)
                        .expect("pending error terminal exists");
                    if let Some(oauth) = is_oauth {
                        pending.turn.prepared.oauth_transport = oauth;
                    }
                    if let Some((mut usage, complete)) = usage_update {
                        if is_oauth.unwrap_or(pending.turn.prepared.oauth_transport) {
                            crate::usage_parser::preserve_prior_openai_oauth_service_tier(
                                pending.turn.usage.as_ref(),
                                &mut usage,
                            );
                        }
                        if let Err(overflow_usage) = update_turn_usage_budget(
                            &mut self.used_bytes,
                            byte_cap,
                            &mut pending.turn,
                            usage,
                            complete,
                        ) {
                            usage_overflow = Some((overflow_usage, complete));
                        }
                    }
                    pending.turn.upstream_id.map(|upstream_id| {
                        let stored = pending.turn.prepared.store
                            && response
                                .and_then(|resp| resp.get("store"))
                                .and_then(Value::as_bool)
                                .unwrap_or(true);
                        (pending.turn.auth.clone(), upstream_id, stored)
                    })
                };
                if let Some((overflow_usage, overflow_complete)) = usage_overflow {
                    let mut err = WebSocketError::new(
                        503,
                        "overloaded_error",
                        "WebSocket session buffer capacity exceeded",
                    )
                    .with_stream(lane)
                    .with_response_id(id);
                    if let Some(owner) = self.owners.get_mut(id) {
                        owner.terminal_delivery = TerminalDelivery::ResponseTerminalDelivered;
                    }
                    if let Some(mut pending) = self.pending_error_terminals.remove(id) {
                        err = err.with_turn_id(pending.turn_id);
                        pending.turn.usage = Some(*overflow_usage);
                        pending.turn.usage_complete = overflow_complete;
                        self.settle_turn(pending.turn, Some(&err));
                    }
                    return Err(err);
                }
                if transform_error.is_none()
                    && let Some(resp) = response
                {
                    let (auth, upstream_id, stored) = match upstream_result {
                        Some(tuple) => tuple,
                        None => {
                            let mut err = WebSocketError::new(
                                502,
                                "upstream_unavailable",
                                "Response upstream is unavailable",
                            )
                            .with_stream(lane)
                            .with_response_id(id);
                            if let Some(owner) = self.owners.get_mut(id) {
                                owner.terminal_delivery =
                                    TerminalDelivery::ResponseTerminalDelivered;
                            }
                            if let Some(pending) = self.pending_error_terminals.remove(id) {
                                err = err.with_turn_id(pending.turn_id);
                                self.settle_turn(pending.turn, Some(&err));
                            }
                            return Err(err);
                        }
                    };
                    if let Err(err) = self
                        .lifecycle
                        .bind_websocket_response_with_pending(
                            &auth,
                            upstream_id,
                            resp,
                            None,
                            stored,
                            &[],
                        )
                        .await
                    {
                        let mut err = err.with_stream(lane).with_response_id(id);
                        if let Some(owner) = self.owners.get_mut(id) {
                            owner.terminal_delivery = TerminalDelivery::ResponseTerminalDelivered;
                        }
                        if let Some(pending) = self.pending_error_terminals.remove(id) {
                            err = err.with_turn_id(pending.turn_id);
                            self.settle_turn(pending.turn, Some(&err));
                        }
                        return Err(err);
                    }
                }
                if let Some(owner) = self.owners.get_mut(id) {
                    owner.terminal_delivery = TerminalDelivery::ResponseTerminalDelivered;
                }
                let pending = self
                    .pending_error_terminals
                    .remove(id)
                    .expect("pending error terminal exists");
                let signer = self.connection_signers.get(&connection_id).map(Arc::as_ref);
                let effective_error = if let Some(error) = transform_error {
                    Some(error.clone())
                } else {
                    let (classified, _) = classify_terminal_frame(kind, &value, &lane, signer);
                    match (pending.error, classified) {
                        (Some(prev), Some(next))
                            if prev.native_code.as_deref() == Some("error")
                                && next.native_code.as_deref() != Some("response.failed") =>
                        {
                            Some(next)
                        }
                        (prev, next) => prev.or(next),
                    }
                };
                self.settle_turn(pending.turn, effective_error.as_ref());
                return Ok(WebSocketEventResult {
                    forward: transform_error.is_none(),
                    ..Default::default()
                });
            }
            return Ok(WebSocketEventResult::default());
        }
        if kind == "error"
            && !turn.is_some_and(|turn| {
                turn.connection_id == Some(connection_id) && turn.lane == lane && turn.sent
            })
        {
            return Err(WebSocketError::new(
                502,
                "unattributed_upstream_error",
                "The provider reported a connection-level error",
            ));
        }
        let Some(turn_id) = turn_id else {
            // Never open a second settlement from an unowned or replayed id.
            return Err(WebSocketError::new(
                502,
                "invalid_upstream_response",
                "Upstream event cannot be attributed to an admitted response",
            )
            .with_stream(lane));
        };
        let Some(turn) = turn else {
            return Err(WebSocketError::new(
                502,
                "invalid_upstream_response",
                "Upstream response is already terminal",
            )
            .with_stream(lane));
        };
        if turn.connection_id != Some(connection_id) || turn.lane != lane || !turn.sent {
            return Err(WebSocketError::new(
                502,
                "invalid_upstream_response",
                "Upstream response does not match its connection and lane",
            )
            .with_stream(lane));
        }
        // Only retry before any response identity or lifecycle frame (`response.queued`,
        // `response.in_progress`, `response.created`, or other non-terminal output) has
        // been observed/forwarded to the client. Once a response lifecycle frame arrives,
        // `self.owners`, client-visible response identity, and `pending_affinity_keys`
        // have already committed; retrying after that would emit a second response id
        // on the client stream and leave rejected keys bound. Also require explicit status 400
        // so 429/5xx or statusless errors are never swallowed or replayed.
        if kind == "error"
            && value.get("status").and_then(Value::as_u64) == Some(400)
            && transform_error.is_none()
            && turn.response_id.is_none()
            && !turn.response_created
            && !turn.provider_frame_forwarded
            && !turn.unknown_reasoning_retried
            && turn.unknown_reasoning_retry.is_some()
            && is_openai_invalid_encrypted_content_error_value(&value)
            && turn.upstream_id.is_some()
        {
            let only_digests = match turn.unknown_reasoning_retry.as_ref() {
                Some(WsUnknownReasoningRetry::All) => None,
                Some(WsUnknownReasoningRetry::Pending(digests)) => Some(digests),
                None => None,
            };
            if let Some(stripped_body) =
                strip_openai_reasoning_encrypted_items(&turn.body, only_digests)
                && let Ok(stripped_bytes) = serde_json::to_vec(&stripped_body)
            {
                let stripped_frame = Bytes::from(stripped_bytes);
                let stripped_body = Arc::new(stripped_body);
                let next_tier = stripped_body
                    .get("service_tier")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                let old_dynamic = turn
                    .frame
                    .len()
                    .saturating_add(json_owned_bytes(&turn.body))
                    .saturating_add(
                        turn.plugin_request
                            .as_ref()
                            .map_or(0, |(_, req)| req.frame.len()),
                    )
                    .saturating_add(sent_service_tier_bytes(turn.sent_service_tier.as_deref()));
                let new_dynamic = stripped_frame
                    .len()
                    .saturating_add(json_owned_bytes(&stripped_body))
                    .saturating_add(sent_service_tier_bytes(next_tier.as_deref()));
                if new_dynamic
                    <= self
                        .byte_cap
                        .saturating_sub(self.used_bytes)
                        .saturating_add(old_dynamic)
                {
                    let turn = self.turns.get_mut(&turn_id).expect("turn exists");
                    let released = old_dynamic.min(turn.charged_bytes);
                    self.used_bytes = self
                        .used_bytes
                        .saturating_sub(released)
                        .saturating_add(new_dynamic);
                    turn.charged_bytes = turn
                        .charged_bytes
                        .saturating_sub(released)
                        .saturating_add(new_dynamic);
                    turn.body = stripped_body;
                    turn.frame = stripped_frame.clone();
                    turn.plugin_request = None;
                    turn.sent_service_tier = next_tier;
                    turn.echo_metadata = None;
                    turn.sent = false;
                    turn.attempted = false;
                    turn.metrics.reset_for_retry();
                    turn.unknown_reasoning_retried = true;
                    match turn.unknown_reasoning_retry.take() {
                        Some(WsUnknownReasoningRetry::All) => {
                            turn.pending_affinity_keys.clear();
                        }
                        Some(WsUnknownReasoningRetry::Pending(digests)) => {
                            turn.pending_affinity_keys.retain(|key| {
                                !(key.provider == "openai"
                                    && key.kind
                                        == cc_lb_storage_api::UpstreamAffinityKind::OpenaiResponseId
                                    && digests.contains(&key.value_sha256))
                            });
                        }
                        None => {}
                    }
                    let retry_connection_id = Some(connection_id);
                    let retry_dispatch = WebSocketDispatch {
                        turn_id,
                        frame: stripped_frame,
                        connection_id: retry_connection_id,
                        candidates: Vec::new(),
                        stream_id: lane,
                        control: false,
                        prefer_api_key_on_final_attempt: false,
                        has_explicit_affinity: false,
                        is_failover_recovery: false,
                    };
                    return Ok(WebSocketEventResult {
                        forward: false,
                        frames: None,
                        dispatches: vec![retry_dispatch],
                        errors: Vec::new(),
                        ..Default::default()
                    });
                }
            }
        }
        let rate_limited_upstream = (kind == "error"
            && transform_error.is_none()
            && turn.response_id.is_none()
            && !turn.response_created
            && !turn.provider_frame_forwarded
            && is_openai_rate_or_usage_limit_ws_error(
                &value,
                &lane,
                self.connection_signers.get(&connection_id).map(Arc::as_ref),
            ))
        .then_some(turn.upstream_id)
        .flatten();
        if let Some(failed_upstream_id) = rate_limited_upstream {
            self.session_rate_limited_upstreams
                .insert(failed_upstream_id);
            self.lifecycle
                .record_ws_rate_limit_cooldown(failed_upstream_id, Some(&value), None);
            let previous_session_upstream = self.session_upstream;
            let failed_upstream_name = self
                .turns
                .get(&turn_id)
                .map(|turn| turn.prepared.route_info.upstream_name.clone())
                .unwrap_or_default();
            if let Some((dispatches, recovery_error)) = self
                .try_recover_codex_previous_response_failover(turn_id, failed_upstream_id)
                .await
            {
                let retire_connections = if self.is_connection_idle(connection_id, None) {
                    self.retire_idle_connection(connection_id);
                    vec![connection_id]
                } else {
                    Vec::new()
                };
                return Ok(WebSocketEventResult {
                    forward: false,
                    frames: None,
                    dispatches,
                    errors: vec![recovery_error],
                    retire_connections,
                });
            }
            let suppressed = self.build_suppressed_live_failover_error(
                connection_id,
                kind,
                &value,
                &lane,
                raw_frame,
                raw_text,
                public_frame,
                transformed_frames,
            );
            let suppressed_bytes =
                suppressed_failover_retained_bytes(&suppressed, &failed_upstream_name);
            let retry_dispatch = if let Some(retry_dispatch) = self
                .try_owner_reasoning_failover_dispatch(
                    turn_id,
                    failed_upstream_id,
                    Some(connection_id),
                    relay_pool,
                    suppressed_bytes,
                )
                .await
            {
                Some(retry_dispatch)
            } else {
                self.try_unpinned_live_failover_dispatch(
                    turn_id,
                    failed_upstream_id,
                    Some(connection_id),
                    relay_pool,
                    suppressed_bytes,
                )
                .await
            };
            if let Some(retry_dispatch) = retry_dispatch {
                self.record_suppressed_failover_error(
                    turn_id,
                    failed_upstream_id,
                    failed_upstream_name,
                    previous_session_upstream,
                    suppressed,
                    suppressed_bytes,
                );
                let retire_connections = if self.is_connection_idle(connection_id, None) {
                    self.retire_idle_connection(connection_id);
                    vec![connection_id]
                } else {
                    Vec::new()
                };
                return Ok(WebSocketEventResult {
                    forward: false,
                    frames: None,
                    dispatches: vec![retry_dispatch],
                    errors: Vec::new(),
                    retire_connections,
                });
            }
        }
        let Some(turn) = self.turns.get(&turn_id) else {
            return Ok(WebSocketEventResult::default());
        };
        if let Some(id) = response_id {
            if turn.response_id.as_deref().is_some_and(|bound| bound != id) {
                return Err(WebSocketError::new(
                    502,
                    "invalid_upstream_response",
                    "Upstream changed an admitted response id",
                )
                .with_stream(lane));
            }
            if turn.response_id.is_none() && (kind != "error" || response.is_some()) {
                if !matches!(
                    kind,
                    "response.queued" | "response.created" | "response.in_progress"
                ) {
                    return Err(WebSocketError::new(
                        502,
                        "invalid_upstream_response",
                        "Upstream terminal arrived before response ownership was established",
                    )
                    .with_stream(lane));
                }
                let upstream_id = turn.upstream_id.ok_or_else(|| {
                    WebSocketError::new(
                        502,
                        "upstream_unavailable",
                        "Response upstream is unavailable",
                    )
                })?;
                response.ok_or_else(|| {
                    WebSocketError::new(
                        502,
                        "invalid_upstream_response",
                        "Response identity has no response object",
                    )
                })?;
                let principal_id = turn.auth.principal_id().to_owned();
                let body = turn.body.clone();
                let oauth_transport = turn.prepared.oauth_transport;
                let oauth_compat = turn.prepared.oauth_compat;
                let echo_metadata = turn.echo_metadata.clone();
                let previous = turn.previous_response_id.clone();
                if let Some(turn) = self.turns.get_mut(&turn_id) {
                    turn.previous_response_id = None;
                }
                self.collect_owner(previous.as_deref());
                self.bind_owner(
                    id,
                    ResponseOwner {
                        connection_id,
                        upstream_id,
                        turn_id,
                        principal_id,
                        body,
                        lane: lane.clone(),
                        oauth_transport,
                        oauth_compat,
                        echo_metadata,
                        terminal_delivery: TerminalDelivery::None,
                        evicted_for_continuation: false,
                    },
                    &lane,
                )?;
                let turn = self
                    .turns
                    .get_mut(&turn_id)
                    .expect("admitted turn remains owned during binding");
                turn.response_id = Some(id.to_owned());
                if kind == "response.created" {
                    turn.response_created = true;
                    self.lifecycle.clear_ws_rate_limit_cooldown(upstream_id);
                }
                turn.prepared.span.record("gen_ai.response.id", id);
                if let Some(parent) = turn.steer_parent.as_ref() {
                    self.successors.remove(parent);
                    let lane = self.lanes.entry(lane.clone()).or_default();
                    if lane.active.is_none() || lane.active == Some(turn_id) {
                        lane.active = Some(turn_id);
                    }
                }
            }
        }
        let is_oauth = self.connection_oauth.get(&connection_id).copied();
        let turn_oauth = self
            .turns
            .get(&turn_id)
            .is_some_and(|turn| is_oauth.unwrap_or(turn.prepared.oauth_transport));
        let tier_policy = if turn_oauth {
            ServiceTierMergePolicy::OpenaiOauth
        } else {
            ServiceTierMergePolicy::Standard
        };
        if let Some(response) = response
            && let Some((mut usage, complete)) = observed_response_usage(response, tier_policy)
        {
            let byte_cap = self.byte_cap;
            let (usage_overflow, usage_lane, usage_response_id) = {
                let turn = self.turns.get_mut(&turn_id).expect("bound turn exists");
                if let Some(oauth) = is_oauth {
                    turn.prepared.oauth_transport = oauth;
                }
                if is_oauth.unwrap_or(turn.prepared.oauth_transport) {
                    crate::usage_parser::preserve_prior_openai_oauth_service_tier(
                        turn.usage.as_ref(),
                        &mut usage,
                    );
                }
                let overflow = update_turn_usage_budget(
                    &mut self.used_bytes,
                    byte_cap,
                    turn,
                    usage,
                    terminal && complete,
                )
                .err();
                (overflow, turn.lane.clone(), turn.response_id.clone())
            };
            if let Some(overflow_usage) = usage_overflow {
                let mut err = WebSocketError::new(
                    503,
                    "overloaded_error",
                    "WebSocket session buffer capacity exceeded",
                )
                .with_stream(usage_lane)
                .with_turn_id(turn_id);
                if let Some(id) = usage_response_id {
                    err = err.with_response_id(id);
                }
                if let Some(mut turn) = self.detach_turn(turn_id) {
                    turn.usage = Some(*overflow_usage);
                    turn.usage_complete = terminal && complete;
                    self.settle_turn(turn, Some(&err));
                }
                return Err(err);
            }
        }
        // Later snapshots and item.done can introduce reusable reasoning even
        // when the response id itself belongs only to this live connection.
        if response.is_some() || kind == "response.output_item.done" {
            let (auth, upstream_id, stored, pending_keys) = {
                let turn = self.turns.get_mut(&turn_id).expect("admitted turn exists");
                let upstream_id = turn.upstream_id.ok_or_else(|| {
                    WebSocketError::new(
                        502,
                        "upstream_unavailable",
                        "Response upstream is unavailable",
                    )
                })?;
                let stored = turn.prepared.store
                    && response
                        .and_then(|value| value.get("store"))
                        .and_then(Value::as_bool)
                        .unwrap_or(true);
                // Only bind unverified input `pending_affinity_keys` on accepted terminal frames
                // (`response.completed` or error-free `response.incomplete`), matching
                // `classify_terminal_frame`; `response.queued`, `response.in_progress`,
                // `response.failed`, and error-bearing `response.incomplete` must not bind
                // unverified input reasoning.
                let clean_incomplete = kind == "response.incomplete"
                    && !value
                        .pointer("/response/error")
                        .is_some_and(|error| !error.is_null());
                let pending_keys = if transform_error.is_none()
                    && (kind == "response.completed" || clean_incomplete)
                {
                    std::mem::take(&mut turn.pending_affinity_keys)
                } else {
                    Vec::new()
                };
                (turn.auth.clone(), upstream_id, stored, pending_keys)
            };
            self.lifecycle
                .bind_websocket_response_with_pending(
                    &auth,
                    upstream_id,
                    response.unwrap_or(&Value::Null),
                    (kind == "response.output_item.done")
                        .then(|| value.get("item"))
                        .flatten(),
                    stored,
                    &pending_keys,
                )
                .await
                .map_err(|error| error.with_stream(lane.clone()).with_turn_id(turn_id))?;
        }
        if kind == "response.created"
            && let Some(turn) = self.turns.get_mut(&turn_id)
        {
            turn.response_created = true;
        }
        if !terminal
            && transform_error.is_none()
            && let Some(turn) = self.turns.get_mut(&turn_id)
        {
            turn.provider_frame_forwarded = true;
        }
        let mut result = WebSocketEventResult {
            forward: true,
            ..Default::default()
        };
        if terminal {
            let bound_response_id = self
                .turns
                .get(&turn_id)
                .and_then(|turn| turn.response_id.clone());
            let missing_status = kind == "error"
                && transform_error.is_none()
                && value.get("status").and_then(Value::as_u64).is_none();
            if let Some(resp_id) = bound_response_id.as_deref()
                && let Some(owner) = self.owners.get_mut(resp_id)
            {
                if kind == "error" && transform_error.is_none() {
                    owner.terminal_delivery =
                        TerminalDelivery::ErrorAwaitingResponseFailed { missing_status };
                } else {
                    owner.terminal_delivery = TerminalDelivery::ResponseTerminalDelivered;
                }
            }
            if kind == "error"
                && transform_error.is_none()
                && let Some(resp_id) = bound_response_id
            {
                let signer = self.connection_signers.get(&connection_id).map(Arc::as_ref);
                let (failure, _) = classify_terminal_frame(kind, &value, &lane, signer);
                result.errors.extend(self.park_pending_error_terminal(
                    turn_id,
                    resp_id,
                    failure,
                    missing_status,
                ));
            } else if let Some(error) = transform_error {
                self.finish_turn(turn_id, Some(error));
            } else {
                let signer = self.connection_signers.get(&connection_id).map(Arc::as_ref);
                let (failure, incomplete_reason) =
                    classify_terminal_frame(kind, &value, &lane, signer);
                if failure.is_none()
                    && matches!(kind, "response.completed" | "response.incomplete")
                    && let Some(turn) = self.turns.get(&turn_id)
                    && let Some(upstream_id) = turn.upstream_id
                {
                    self.lifecycle.record_openai_session_hint(
                        turn.auth.principal_id(),
                        &self.headers,
                        &turn.body,
                        upstream_id,
                    );
                }
                if let Some(turn) = self.turns.get_mut(&turn_id) {
                    turn.incomplete_reason = incomplete_reason;
                }
                self.finish_turn(turn_id, failure.as_ref());
            }
            if let Some(dispatch) = self.next_in_lane(&lane) {
                result.dispatches.push(dispatch);
            }
        }
        if let Some(error) = transform_error
            && !terminal
        {
            self.finish_turn(turn_id, Some(error));
        }
        Ok(result)
    }

    pub fn take_pending_errors(&mut self) -> Vec<WebSocketError> {
        std::mem::take(&mut self.pending_eviction_errors)
    }

    fn reclaimable_pending_owner_budget(&self) -> (usize, usize) {
        let mut slots = 0_usize;
        let mut bytes = 0_usize;
        let mut seen_owners = HashSet::new();
        for (id, pending) in &self.pending_error_terminals {
            bytes = bytes.saturating_add(pending.turn.charged_bytes);
            for candidate in [
                Some(id.as_str()),
                pending.turn.response_id.as_deref(),
                pending.turn.previous_response_id.as_deref(),
            ]
            .into_iter()
            .flatten()
            {
                if !seen_owners.insert(candidate) {
                    continue;
                }
                let still_pinned = self.turns.values().any(|turn| {
                    turn.response_id.as_deref() == Some(candidate)
                        || turn.previous_response_id.as_deref() == Some(candidate)
                }) || self.recent.values().any(|recent| recent == candidate);
                if !still_pinned && let Some(owner) = self.owners.get(candidate) {
                    slots = slots.saturating_add(1);
                    bytes = bytes.saturating_add(owner_bytes(candidate, owner));
                }
            }
        }
        (slots, bytes)
    }

    fn bind_owner(
        &mut self,
        id: &str,
        owner: ResponseOwner,
        lane: &Option<String>,
    ) -> Result<(), WebSocketError> {
        if let Some(existing) = self.owners.get(id) {
            if existing.turn_id == owner.turn_id && existing.connection_id == owner.connection_id {
                return Ok(());
            }
            return Err(WebSocketError::new(
                502,
                "invalid_upstream_response",
                "Response id is already owned by another turn",
            )
            .with_stream(lane.clone()));
        }
        let recent_key = (owner.connection_id, lane.clone());
        let old = self.recent.remove(&recent_key);
        self.collect_owner(old.as_deref());
        let cost = owner_bytes(id, &owner);
        let needed_slots = (self.owners.len() + 1).saturating_sub(self.slot_cap);
        let available_bytes = self.byte_cap.saturating_sub(self.used_bytes);
        let needed_bytes = cost.saturating_sub(available_bytes);
        if needed_slots > 0 || needed_bytes > 0 {
            let (reclaimable_slots, reclaimable_bytes) = self.reclaimable_pending_owner_budget();
            if reclaimable_slots < needed_slots || reclaimable_bytes < needed_bytes {
                return Err(WebSocketError::new(
                    503,
                    "overloaded_error",
                    "WebSocket response ownership budget is full",
                )
                .with_stream(lane.clone())
                .with_turn_id(owner.turn_id));
            }
            while self.owners.len() >= self.slot_cap
                || cost > self.byte_cap.saturating_sub(self.used_bytes)
            {
                let Some(emitted) = self.evict_oldest_pending_error_terminal() else {
                    break;
                };
                if let Some(err) = emitted {
                    self.pending_eviction_errors.push(err);
                }
            }
        }
        if self.owners.len() >= self.slot_cap
            || cost > self.byte_cap.saturating_sub(self.used_bytes)
        {
            return Err(WebSocketError::new(
                503,
                "overloaded_error",
                "WebSocket response ownership budget is full",
            )
            .with_stream(lane.clone())
            .with_turn_id(owner.turn_id));
        }
        self.used_bytes = self.used_bytes.saturating_add(cost);
        self.recent.insert(recent_key, id.to_owned());
        self.owners.insert(id.to_owned(), owner);
        Ok(())
    }

    fn collect_owner(&mut self, id: Option<&str>) {
        let Some(id) = id else { return };
        let referenced = self.turns.values().any(|turn| {
            turn.response_id.as_deref() == Some(id)
                || turn.previous_response_id.as_deref() == Some(id)
        }) || self.recent.values().any(|recent| recent == id)
            || self.pending_error_terminals.contains_key(id);
        if !referenced && let Some(owner) = self.owners.remove(id) {
            self.used_bytes = self.used_bytes.saturating_sub(owner_bytes(id, &owner));
        }
    }

    fn steering_event(
        &mut self,
        connection_id: u64,
        kind: &str,
        value: &Value,
        lane: Option<String>,
    ) -> Result<WebSocketEventResult, WebSocketError> {
        let parent = value
            .get("steer")
            .and_then(|steer| steer.get("previous_response_id"))
            .and_then(Value::as_str)
            .ok_or_else(|| {
                WebSocketError::new(
                    502,
                    "invalid_upstream_response",
                    "Steering control has no parent response",
                )
            })?;
        let turn_id = self.successor_for_parent(parent).ok_or_else(|| {
            WebSocketError::new(
                502,
                "invalid_upstream_response",
                "Steering control has no reserved successor",
            )
        })?;
        let turn = self.turns.get_mut(&turn_id).ok_or_else(|| {
            WebSocketError::new(
                502,
                "invalid_upstream_response",
                "Steering successor is unavailable",
            )
        })?;
        if turn.connection_id != Some(connection_id) || turn.lane != lane {
            return Err(WebSocketError::new(
                502,
                "invalid_upstream_response",
                "Steering control changed its owning connection or lane",
            ));
        }
        let steer_id = value
            .get("steer")
            .and_then(|steer| steer.get("id"))
            .and_then(Value::as_str);
        let mut result = WebSocketEventResult {
            forward: true,
            ..Default::default()
        };
        if kind == "response.steer.accepted" {
            if let Some(id) = steer_id {
                let charge = steering_id_bytes(id);
                if !turn.accepted_steers.contains(id) {
                    if self.control_slots >= self.slot_cap
                        || charge > self.byte_cap.saturating_sub(self.used_bytes)
                    {
                        return Err(WebSocketError::new(
                            503,
                            "overloaded_error",
                            "WebSocket steering ownership budget is full",
                        )
                        .with_stream(lane.clone())
                        .with_turn_id(turn_id));
                    }
                    turn.accepted_steers.insert(id.to_owned());
                    self.control_slots += 1;
                    self.used_bytes = self.used_bytes.saturating_add(charge);
                    turn.charged_bytes = turn.charged_bytes.saturating_add(charge);
                }
            }
        } else if kind == "response.steer.failed" {
            if let Some(id) = steer_id
                && turn.accepted_steers.remove(id)
            {
                let charge = steering_id_bytes(id);
                self.control_slots = self.control_slots.saturating_sub(1);
                self.used_bytes = self.used_bytes.saturating_sub(charge);
                turn.charged_bytes = turn.charged_bytes.saturating_sub(charge);
            }
            turn.steer_submissions = turn.steer_submissions.saturating_sub(1);
            if turn.steer_submissions == 0 && turn.response_id.is_none() {
                let error = WebSocketError::new(
                    400,
                    "steering_failed",
                    "The provider did not commit the steering successor",
                )
                .with_stream(lane.clone());
                self.finish_turn(turn_id, Some(&error));
                if let Some(dispatch) = self.next_in_lane(&lane) {
                    result.dispatches.push(dispatch);
                }
            }
        }
        Ok(result)
    }

    fn finish_turn(&mut self, turn_id: u64, error: Option<&WebSocketError>) {
        if let Some(turn) = self.detach_turn(turn_id) {
            self.settle_turn(turn, error);
        }
    }

    fn detach_turn(&mut self, turn_id: u64) -> Option<Turn> {
        let mut turn = self.turns.remove(&turn_id)?;
        self.control_slots = self.control_slots.saturating_sub(
            turn.controls
                .len()
                .saturating_add(turn.accepted_steers.len()),
        );
        turn.controls.clear();
        turn.accepted_steers.clear();
        if let Some(parent) = turn.steer_parent.as_ref()
            && self.successors.get(parent) == Some(&turn_id)
        {
            self.successors.remove(parent);
        }
        let successor = turn
            .response_id
            .as_ref()
            .and_then(|id| self.successor_for_parent(id))
            .filter(|id| {
                self.turns
                    .get(id)
                    .is_some_and(|successor| successor.sent && successor.lane == turn.lane)
            });
        if let Some(lane) = self.lanes.get_mut(&turn.lane) {
            if lane.active == Some(turn_id) {
                lane.active = successor.filter(|successor| !lane.queued.contains(successor));
            }
            lane.queued.retain(|id| *id != turn_id);
        }
        Some(turn)
    }

    fn settle_turn(&mut self, mut turn: Turn, error: Option<&WebSocketError>) {
        self.used_bytes = self.used_bytes.saturating_sub(turn.charged_bytes);
        let complete = error.is_none();
        let (outcome, cause) = match error {
            None => (
                StreamTerminationOutcome::Completed,
                StreamTerminationCause::None,
            ),
            Some(error) if error.code == "client_closed_request" => (
                StreamTerminationOutcome::ClientCancelled,
                StreamTerminationCause::None,
            ),
            Some(error)
                if error.native_code.is_some() || error.code == "upstream_connection_closed" =>
            {
                (
                    StreamTerminationOutcome::UpstreamError,
                    StreamTerminationCause::ProviderError,
                )
            }
            Some(_) => (
                StreamTerminationOutcome::ProxyError,
                StreamTerminationCause::TransportError,
            ),
        };
        record_stream_termination_metric(
            outcome,
            cause,
            resolve_turn_upstream_label(
                outcome,
                turn.upstream_id,
                &turn.prepared.route_info.upstream_name,
                turn.prepared
                    .candidates
                    .first()
                    .map(|candidate| candidate.upstream_name.as_str()),
            ),
            Some(&turn.prepared.model),
        );
        record_websocket_terminal_span(
            &turn.prepared.span,
            outcome,
            cause,
            error.map(|error| error.code),
            error.map(|error| error.message.as_str()),
        );
        turn.metrics.io_timing.stop();
        let finalize_started = std::time::Instant::now();
        let latency = turn
            .metrics
            .latency(StatusCode::SWITCHING_PROTOCOLS.as_u16());
        latency.record_on_span(&turn.prepared.span);
        let is_oauth = turn
            .connection_id
            .and_then(|cid| self.connection_oauth.get(&cid).copied())
            .unwrap_or(turn.prepared.oauth_transport);
        if is_oauth && let Some(usage) = turn.usage.as_mut() {
            crate::usage_parser::apply_openai_oauth_service_tier(
                usage,
                turn.sent_service_tier.as_deref(),
            );
        }
        if let (Some(context), Some(usage)) = (
            turn.prepared.prompt_cache_observation_context.take(),
            turn.usage.as_ref(),
        ) {
            self.lifecycle
                .publish_websocket_provider_cache_observation(&context, usage);
        }
        if let Some(observer) = turn.prepared.observer.as_ref() {
            let usage = turn.usage.take();
            if let Some(usage) = usage.as_ref() {
                observer.set_usage_counts(usage, complete && turn.usage_complete);
                observer.emit_lifecycle(LifecycleEvent::UsageObserved {
                    event_id: observer.event_id().to_owned(),
                    usage: usage_snapshot(usage.clone()),
                    source: UsageSource::NonStreamBody,
                });
            }
            if complete && let Some(incomplete_reason) = turn.incomplete_reason.take() {
                observer.emit_lifecycle(LifecycleEvent::UpstreamStreamDiagnosticsObserved {
                    event_id: observer.event_id().to_owned(),
                    diagnostics: cc_lb_lifecycle::UpstreamStreamDiagnostics {
                        body_bytes: latency.total_bytes,
                        warning_type: Some("response.incomplete".to_owned()),
                        warning_message: incomplete_reason,
                        ..Default::default()
                    },
                });
            }
            observer.set_attempt_bulkhead_wait_ms(turn.metrics.bulkhead_wait_ms);
            observer.set_attempt_connection_timings(
                turn.metrics.dns_ms,
                turn.metrics.connect_ms,
                turn.metrics.connection_reused,
            );
            observer.set_attempt_timings(
                turn.metrics.shape_ms,
                turn.metrics.sign_ms,
                turn.metrics.upstream_ttfb_ms,
            );
            observer.set_setup_timings(turn.metrics.setup_timings);
            observer.set_io_timings(turn.metrics.io_timing.snapshot());
            observer.set_termination_timings(
                turn.metrics.proxy_setup_ms,
                Some(latency.stream_total_ms),
                latency.stream_first_output_ms,
            );
            if let Some(delta_ms) = latency.stream_first_content_delta_ms {
                observer.set_first_content_delta_ms(delta_ms);
            }
            observer.emit_lifecycle(LifecycleEvent::StreamCompleted {
                event_id: observer.event_id().to_owned(),
                result: if complete {
                    Ok(cc_lb_lifecycle::StreamSuccess {
                        usage: usage
                            .as_ref()
                            .map(|usage| usage_snapshot(usage.clone()))
                            .unwrap_or_default(),
                        usage_present: usage.is_some(),
                        sse_event_count: 0,
                        body_bytes: Some(latency.total_bytes),
                        body_chunk_count: Some(latency.stream_event_count),
                        first_body_chunk_ms: latency.stream_first_output_ms,
                        stream_message_start_ms: latency.stream_message_start_ms,
                        stream_content_block_start_ms: latency.stream_content_block_start_ms,
                        stream_first_content_delta_ms: latency.stream_first_content_delta_ms,
                        stream_last_content_delta_ms: latency.stream_last_content_delta_ms,
                        stream_message_stop_ms: latency.stream_message_stop_ms,
                        stream_last_chunk_ms: latency.stream_last_output_ms,
                        stream_total_ms: Some(latency.stream_total_ms),
                        content_delta_count: Some(latency.content_delta_count),
                        ping_count: Some(latency.ping_count),
                        stream_inter_output_avg_ms: latency.stream_inter_output_avg_ms,
                        inter_token_avg_ms: latency.inter_token_avg_ms,
                        provider_session_id: latency.provider_session_id.clone(),
                        provider_connection_id: latency.provider_connection_id.clone(),
                    })
                } else {
                    Err(cc_lb_lifecycle::StreamError {
                        error_type: error
                            .map(|error| {
                                error
                                    .native_code
                                    .as_deref()
                                    .unwrap_or(error.code)
                                    .to_owned()
                            })
                            .unwrap_or_else(|| "responses_stream_incomplete".to_owned()),
                        error_message: error.map(|error| error.message.clone()).unwrap_or_else(
                            || "Responses WebSocket stream ended before completion".to_owned(),
                        ),
                        body_bytes: Some(latency.total_bytes),
                        body_chunk_count: Some(latency.stream_event_count),
                        first_body_chunk_ms: latency.stream_first_output_ms,
                        stream_last_chunk_ms: latency.stream_last_chunk_ms,
                        stream_total_ms: Some(latency.stream_total_ms),
                        ping_count: Some(latency.ping_count),
                        stream_inter_output_avg_ms: latency.stream_inter_output_avg_ms,
                        provider_session_id: latency.provider_session_id.clone(),
                        provider_connection_id: latency.provider_connection_id.clone(),
                        ..Default::default()
                    })
                },
            });
            // The subscriber owns settlement from this point, including an
            // interrupted response with measured usage. Drop must not refund it.
            if let Some(reservation) = turn.prepared.reservation.take() {
                reservation.handoff_after_response();
            }
            match (error, complete) {
                (Some(error), _) if error.code == "client_closed_request" => {
                    observer.set_client_closed(StatusCode::SWITCHING_PROTOCOLS);
                }
                (Some(error), _)
                    if error.native_code.is_some()
                        || error.code == "upstream_connection_closed" =>
                {
                    observer.set_upstream_error(
                        StatusCode::SWITCHING_PROTOCOLS,
                        UpstreamErrorCode::StreamError,
                    );
                }
                (Some(_), _) => observer.set_failure(InternalFailure {
                    status: StatusCode::SWITCHING_PROTOCOLS,
                    error_code: UpstreamErrorCode::StreamError.as_str(),
                    error: cc_lb_domain::InternalError {
                        stage: cc_lb_domain::InternalErrorStage::Relay,
                        kind: cc_lb_domain::InternalErrorKind::Unavailable,
                        message: Some("Responses WebSocket relay failed".to_owned()),
                    },
                }),
                (None, true) => observer.set_success_status(StatusCode::SWITCHING_PROTOCOLS),
                (None, false) => observer.set_upstream_error(
                    StatusCode::SWITCHING_PROTOCOLS,
                    UpstreamErrorCode::StreamError,
                ),
            }
            observer.set_finalize_ms(duration_to_ms(finalize_started.elapsed()));
            observer.finish();
            if let Some(dispatch) = tracing::enabled!(
                target: "cc_lb_engine::lifecycle",
                tracing::Level::DEBUG
            )
            .then(|| tracing::dispatcher::get_default(|dispatch| dispatch.clone()))
            {
                let _ = turn
                    .prepared
                    .completion_observer
                    .enqueue(StreamCompletionLog::new(
                        turn.prepared.request_id.clone(),
                        &turn.prepared.span,
                        latency,
                        dispatch,
                    ));
            }
        } else if let Some(reservation) = turn.prepared.reservation.take() {
            // No event bus exists for this embedded caller. Settle business
            // counters directly; normal server traffic always uses the worker.
            if let (Some(engine), Some(usage)) =
                (turn.prepared.limit_engine.as_ref(), turn.usage.as_ref())
            {
                let cost =
                    cc_lb_pricing::virtual_cost_micros_full_with_context_and_provider_writes(
                        &turn.prepared.model,
                        usage.input_tokens,
                        usage.output_tokens,
                        usage.input_image_tokens.unwrap_or(0),
                        usage.output_image_tokens.unwrap_or(0),
                        usage.cache_creation_input_tokens,
                        usage.cache_creation_input_tokens_5m,
                        usage.cache_creation_input_tokens_1h,
                        usage.cache_read_input_tokens,
                        usage.cache_write_input_tokens,
                        usage.cache_write_input_tokens_30m,
                        usage.cache_write_input_tokens_12h,
                        usage.service_tier.as_deref(),
                    )
                    .unwrap_or_else(cc_lb_pricing::ComputedCostBreakdown::unknown);
                engine.reconcile(
                    reservation,
                    usage.input_tokens,
                    usage.output_tokens,
                    (cost.pricing_status == cc_lb_pricing::PricingStatus::Known)
                        .then_some(cost.total_micros),
                );
            }
        }
        self.collect_owner(turn.previous_response_id.as_deref());
        self.collect_owner(turn.response_id.as_deref());
        if let (Some(connection_id), Some(response_id)) =
            (turn.connection_id, turn.response_id.take())
            && self.connections.contains_key(&connection_id)
        {
            self.record_retired_terminal(connection_id, turn.lane.clone(), response_id);
        }
        // Retain only bounded, connection-local most-recent control metadata.
        // Other stored ids resolve through the existing durable affinity store.
        self.prune_empty_lanes();
    }

    fn retired_terminal_cap(&self) -> usize {
        if self.slot_cap == 0 {
            0
        } else {
            self.slot_cap.saturating_mul(2).clamp(8, 64)
        }
    }

    fn record_retired_terminal(
        &mut self,
        connection_id: u64,
        lane: Option<String>,
        response_id: String,
    ) {
        let cap = self.retired_terminal_cap();
        if cap == 0 || response_id.is_empty() || response_id.len() > 512 {
            return;
        }
        if self.is_retired_terminal(connection_id, &lane, &response_id) {
            return;
        }
        while self.retired_terminals.len() >= cap {
            self.retired_terminals.pop_front();
        }
        self.retired_terminals
            .push_back((connection_id, lane, response_id));
    }

    fn is_retired_terminal(
        &self,
        connection_id: u64,
        lane: &Option<String>,
        response_id: &str,
    ) -> bool {
        let effective_lane = if lane.is_none()
            && !self.codex_backend
            && self
                .connection_oauth
                .get(&connection_id)
                .copied()
                .unwrap_or(false)
        {
            self.connection_lanes.get(&connection_id).unwrap_or(lane)
        } else {
            lane
        };
        self.retired_terminals
            .iter()
            .any(|(conn, retired_lane, retired_id)| {
                *conn == connection_id
                    && retired_lane == effective_lane
                    && retired_id == response_id
            })
    }

    fn synthetic_terminal_error(
        response_id: &str,
        lane: Option<String>,
        error: Option<WebSocketError>,
    ) -> WebSocketError {
        error
            .unwrap_or_else(|| {
                WebSocketError::new(
                    502,
                    "upstream_connection_closed",
                    "Upstream response failed without a terminal event",
                )
            })
            .with_stream(lane)
            .with_response_id(response_id)
    }

    fn terminal_error_for_pending(
        response_id: &str,
        pending: &PendingErrorTerminal,
    ) -> Option<WebSocketError> {
        pending.missing_status.then(|| {
            Self::synthetic_terminal_error(
                response_id,
                pending.turn.lane.clone(),
                pending.error.clone(),
            )
        })
    }

    fn prune_empty_lanes(&mut self) {
        let mut released = 0_usize;
        self.lanes.retain(|id, lane| {
            let keep = lane.active.is_some()
                || !lane.queued.is_empty()
                || self.recent.keys().any(|(_, recent_lane)| recent_lane == id);
            if !keep {
                released = released.saturating_add(lane_bytes(id));
            }
            keep
        });
        self.used_bytes = self.used_bytes.saturating_sub(released);
    }

    fn activate_next_turn(lane: &mut Lane) -> Option<u64> {
        if lane.active.is_some() {
            return None;
        }
        let next = lane.queued.pop_front()?;
        lane.active = Some(next);
        Some(next)
    }

    fn next_in_lane(&mut self, lane_id: &Option<String>) -> Option<WebSocketDispatch> {
        let lane = self.lanes.get_mut(lane_id)?;
        let next = Self::activate_next_turn(lane)?;
        self.dispatch(next)
    }

    async fn try_recover_codex_previous_response_failover(
        &mut self,
        turn_id: u64,
        failed_upstream_id: Uuid,
    ) -> Option<(Vec<WebSocketDispatch>, WebSocketError)> {
        if !self.codex_backend {
            return None;
        }
        let (auth, body, lane) = self.turns.get(&turn_id).and_then(|turn| {
            turn.previous_response_id
                .is_some()
                .then(|| (turn.auth.clone(), Arc::clone(&turn.body), turn.lane.clone()))
        })?;
        let dummy_frame = Bytes::new();
        let has_replacement = self
            .lifecycle
            .has_healthy_codex_ws_replacement(
                &auth,
                WebSocketIngress {
                    headers: &self.headers,
                    frame: &dummy_frame,
                    query: self.query.as_deref(),
                },
                &body,
                failed_upstream_id,
                &self.session_rate_limited_upstreams,
                false,
            )
            .await;
        if !has_replacement || !self.turns.contains_key(&turn_id) {
            return None;
        }
        self.evict_upstream_for_failover(failed_upstream_id);
        tracing::warn!(
            owner_upstream_id = %failed_upstream_id,
            "evicting Codex WebSocket response owner and returning previous_response_not_found for full-history failover"
        );
        let recovery_error = WebSocketError::new(
            400,
            "previous_response_not_found",
            "The previous response owner is unavailable; replay full history",
        )
        .with_stream(lane.clone());
        self.finish_turn(turn_id, Some(&recovery_error));
        let dispatches = self.next_in_lane(&lane).into_iter().collect();
        Some((dispatches, recovery_error))
    }

    fn is_connection_idle(&self, connection_id: u64, ignore_turn_id: Option<u64>) -> bool {
        !self.turns.iter().any(|(id, turn)| {
            Some(*id) != ignore_turn_id && turn.connection_id == Some(connection_id)
        }) && !self
            .owners
            .values()
            .any(|owner| owner.connection_id == connection_id)
            && !self
                .pending_error_terminals
                .values()
                .any(|pending| pending.turn.connection_id == Some(connection_id))
    }

    fn retire_idle_connection(&mut self, connection_id: u64) {
        self.connections.remove(&connection_id);
        self.connection_lanes.remove(&connection_id);
        self.connection_oauth.remove(&connection_id);
        self.connection_signers.remove(&connection_id);
        self.retired_terminals
            .retain(|(conn, _, _)| *conn != connection_id);
        self.recent
            .retain(|(connection, _), _| *connection != connection_id);
        self.prune_empty_lanes();
    }

    fn is_reclaimable_idle_connection(
        &self,
        conn_id: u64,
        uid: Uuid,
        needed_upstream_id: Uuid,
        needed_socket_per_lane: bool,
        needed_lane: &Option<String>,
        reserved_connections: &HashSet<u64>,
    ) -> bool {
        let needed_existing = uid == needed_upstream_id
            && (!needed_socket_per_lane
                || self.connection_lanes.get(&conn_id) == Some(needed_lane));
        !needed_existing
            && !reserved_connections.contains(&conn_id)
            && self.session_upstream != Some(uid)
            && self.is_connection_idle(conn_id, None)
    }

    /// Count idle provider connections that can currently be reclaimed for `needed_upstream_id`
    /// without disturbing reserved sockets or the active session owner.
    pub fn count_reclaimable_idle_connections_for_failover(
        &self,
        needed_upstream_id: Uuid,
        needed_socket_per_lane: bool,
        needed_lane: &Option<String>,
        reserved_connections: &HashSet<u64>,
    ) -> usize {
        self.connections
            .iter()
            .filter(|(conn_id, uid)| {
                self.is_reclaimable_idle_connection(
                    **conn_id,
                    **uid,
                    needed_upstream_id,
                    needed_socket_per_lane,
                    needed_lane,
                    reserved_connections,
                )
            })
            .count()
    }

    /// Reclaim the oldest idle provider connection not needed for `needed_upstream_id`
    /// and not currently pinned as the session owner, freeing a WebSocket slot for failover.
    pub fn reclaim_idle_connection_for_failover(
        &mut self,
        needed_upstream_id: Uuid,
        needed_socket_per_lane: bool,
        needed_lane: &Option<String>,
        reserved_connections: &HashSet<u64>,
    ) -> Option<u64> {
        let retire_id = self
            .connections
            .iter()
            .filter_map(|(conn_id, uid)| {
                self.is_reclaimable_idle_connection(
                    *conn_id,
                    *uid,
                    needed_upstream_id,
                    needed_socket_per_lane,
                    needed_lane,
                    reserved_connections,
                )
                .then_some(*conn_id)
            })
            .min()?;
        self.retire_idle_connection(retire_id);
        Some(retire_id)
    }

    #[allow(clippy::too_many_arguments)]
    fn retain_viable_failover_candidates(
        &self,
        failed_connection_id: Option<u64>,
        failed_upstream_id: Uuid,
        owner_upstream_id: Option<Uuid>,
        turn_id: u64,
        lane: &Option<String>,
        candidates: &mut Vec<WebSocketCandidate>,
        relay_pool: &WsRelayPoolSnapshot,
    ) -> bool {
        let capacity = self.slot_cap.max(1);
        let total_in_use = self
            .connections
            .len()
            .saturating_add(relay_pool.pending_connects);
        let reclaimable_idle_excluding = |protected_conn_id: Option<u64>| {
            self.connections
                .iter()
                .filter(|(conn_id, uid)| {
                    Some(**conn_id) != protected_conn_id
                        && !relay_pool.reserved_connections.contains(conn_id)
                        && self.is_connection_idle(**conn_id, Some(turn_id))
                        && (failed_connection_id == Some(**conn_id)
                            || self.session_upstream != Some(**uid)
                            || **uid == failed_upstream_id
                            || owner_upstream_id == Some(**uid))
                })
                .count()
        };
        let can_open_new_dial = !relay_pool.dial_attempts_exhausted
            && total_in_use.saturating_sub(reclaimable_idle_excluding(None)) < capacity;
        if can_open_new_dial {
            return !candidates.is_empty();
        }
        candidates.retain(|candidate| {
            (!candidate.socket_per_lane
                && relay_pool
                    .coalescable_upstreams
                    .contains(&candidate.upstream_id))
                || self.connections.iter().any(|(conn_id, uid)| {
                    *uid == candidate.upstream_id
                        && (!candidate.socket_per_lane
                            || self.connection_lanes.get(conn_id) == Some(lane))
                        && total_in_use.saturating_sub(reclaimable_idle_excluding(Some(*conn_id)))
                            <= capacity
                })
        });
        !candidates.is_empty()
    }

    async fn try_owner_reasoning_failover_dispatch(
        &mut self,
        turn_id: u64,
        failed_upstream_id: Uuid,
        failed_connection_id: Option<u64>,
        relay_pool: &WsRelayPoolSnapshot,
        extra_retained_bytes: usize,
    ) -> Option<WebSocketDispatch> {
        if relay_pool.connect_budget_expired {
            return None;
        }
        let (
            auth,
            fallback,
            stripped_body,
            stripped_frame,
            next_tier,
            old_dynamic,
            released_deferred_original,
            lane,
        ) = {
            let turn = self.turns.get(&turn_id)?;
            let fallback = turn.owner_failover_fallback.as_ref()?;
            let stripped = strip_openai_reasoning_encrypted_items(&turn.body, None)?;
            let bytes = serde_json::to_vec(&stripped).ok()?;
            let stripped_body = Arc::new(stripped);
            let stripped_frame = Bytes::from(bytes);
            let next_tier = stripped_body
                .get("service_tier")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let old_dynamic = turn
                .frame
                .len()
                .saturating_add(json_owned_bytes(&turn.body))
                .saturating_add(
                    turn.plugin_request
                        .as_ref()
                        .map_or(0, |(_, req)| req.frame.len()),
                )
                .saturating_add(sent_service_tier_bytes(turn.sent_service_tier.as_deref()));
            let released_deferred_original = turn
                .deferred_unestablished
                .as_ref()
                .filter(|deferred| deferred.currently_stripped)
                .map_or(0, |deferred| deferred.original_bytes);
            (
                turn.auth.clone(),
                WsOwnerFailoverFallback {
                    owner_upstream_id: fallback.owner_upstream_id,
                    candidates: fallback.candidates.clone(),
                    requires_api_key: fallback.requires_api_key,
                },
                stripped_body,
                stripped_frame,
                next_tier,
                old_dynamic,
                released_deferred_original,
                turn.lane.clone(),
            )
        };
        let new_dynamic = stripped_frame
            .len()
            .saturating_add(json_owned_bytes(&stripped_body))
            .saturating_add(sent_service_tier_bytes(next_tier.as_deref()));
        let headroom = self
            .byte_cap
            .saturating_sub(self.used_bytes)
            .saturating_add(old_dynamic)
            .saturating_add(released_deferred_original);
        if new_dynamic.saturating_add(extra_retained_bytes) > headroom {
            return None;
        }
        let mut excluded = self.session_rate_limited_upstreams.clone();
        excluded.insert(failed_upstream_id);
        excluded.insert(fallback.owner_upstream_id);
        excluded.extend(relay_pool.excluded_upstreams.iter().copied());
        let mut available = self
            .lifecycle
            .filter_available_ws_owner_failover_candidates(
                &auth,
                WebSocketIngress {
                    headers: &self.headers,
                    frame: &stripped_frame,
                    query: self.query.as_deref(),
                },
                &stripped_body,
                &fallback.candidates,
                self.codex_backend,
                fallback.requires_api_key,
                &excluded,
            )
            .await;
        if !self.retain_viable_failover_candidates(
            failed_connection_id,
            failed_upstream_id,
            Some(fallback.owner_upstream_id),
            turn_id,
            &lane,
            &mut available,
            relay_pool,
        ) {
            return None;
        }
        let released = old_dynamic
            .saturating_add(released_deferred_original)
            .min(self.turns.get(&turn_id)?.charged_bytes);
        let post_wait_headroom = self
            .byte_cap
            .saturating_sub(self.used_bytes)
            .saturating_add(released);
        if new_dynamic.saturating_add(extra_retained_bytes) > post_wait_headroom {
            return None;
        }
        let turn = self.turns.get_mut(&turn_id)?;
        turn.owner_failover_fallback = None;
        let model = turn.prepared.model.clone();
        self.used_bytes = self
            .used_bytes
            .saturating_sub(released)
            .saturating_add(new_dynamic);
        turn.charged_bytes = turn
            .charged_bytes
            .saturating_sub(released)
            .saturating_add(new_dynamic);
        turn.deferred_unestablished = None;
        turn.body = stripped_body;
        turn.frame = stripped_frame.clone();
        turn.plugin_request = None;
        turn.sent_service_tier = next_tier;
        turn.echo_metadata = None;
        turn.unknown_reasoning_retry = None;
        turn.pending_affinity_keys.clear();
        turn.pinned = None;
        turn.connection_id = None;
        turn.upstream_id = None;
        turn.sent = false;
        turn.attempted = false;
        turn.metrics.reset_for_retry();
        turn.admitted_session_upstream = None;
        turn.prepared.has_explicit_affinity = false;
        turn.owner_failover_replacement = true;
        turn.owner_failover_requires_api_key = fallback.requires_api_key;
        turn.initial_selected_upstream_id = Some(available[0].upstream_id);
        turn.candidate_oauth = available
            .iter()
            .map(|candidate| (candidate.upstream_id, candidate.socket_per_lane))
            .collect();
        turn.has_any_oauth_candidate = available.iter().any(|candidate| !candidate.is_api_key);
        turn.all_candidates_oauth =
            !available.is_empty() && available.iter().all(|candidate| candidate.socket_per_lane);
        turn.prepared.candidates = available.clone();
        turn.live_failover_candidates = available.clone();
        if self.session_upstream == Some(failed_upstream_id)
            || self.session_upstream == Some(fallback.owner_upstream_id)
        {
            self.session_upstream = None;
        }
        tracing::warn!(
            owner_upstream_id = %fallback.owner_upstream_id,
            failed_upstream_id = %failed_upstream_id,
            model = %model,
            "failing over WebSocket OpenAI encrypted reasoning turn after stripping owner items on upstream failure"
        );
        Some(WebSocketDispatch {
            turn_id,
            frame: stripped_frame,
            connection_id: None,
            candidates: available,
            stream_id: lane,
            control: false,
            prefer_api_key_on_final_attempt: !self.codex_backend && !fallback.requires_api_key,
            has_explicit_affinity: false,
            is_failover_recovery: true,
        })
    }

    async fn try_unpinned_live_failover_dispatch(
        &mut self,
        turn_id: u64,
        failed_upstream_id: Uuid,
        failed_connection_id: Option<u64>,
        relay_pool: &WsRelayPoolSnapshot,
        extra_retained_bytes: usize,
    ) -> Option<WebSocketDispatch> {
        if relay_pool.connect_budget_expired {
            return None;
        }
        let (
            auth,
            frame,
            body,
            candidates,
            requires_api_key,
            lane,
            released_plugin,
            next_tier_bytes,
        ) = {
            let turn = self.turns.get(&turn_id)?;
            if turn.pinned.is_some()
                || turn.prepared.has_explicit_affinity
                || turn.previous_response_id.is_some()
                || turn.attempt_num >= 3
                || turn.live_failover_candidates.is_empty()
                || turn.unknown_reasoning_retry.is_some()
                || turn.deferred_unestablished.is_some()
                || !turn.pending_affinity_keys.is_empty()
                || openai_responses_has_account_owned_reference(&turn.body)
                || turn
                    .body
                    .get("input")
                    .is_some_and(input_has_compaction_item)
            {
                return None;
            }
            let explicit_store_true = !self.codex_backend
                && turn.body.get("store").and_then(Value::as_bool) == Some(true);
            let failed_is_api_key = turn
                .live_failover_candidates
                .iter()
                .find(|candidate| candidate.upstream_id == failed_upstream_id)
                .map(|candidate| candidate.is_api_key)
                .or_else(|| {
                    failed_connection_id
                        .and_then(|cid| self.connection_oauth.get(&cid).map(|is_oauth| !*is_oauth))
                })
                .or_else(|| {
                    self.lifecycle
                        .websocket_candidate_for_upstream(failed_upstream_id, self.codex_backend)
                        .map(|candidate| candidate.is_api_key)
                })
                .unwrap_or(true);
            let requires_api_key =
                turn.owner_failover_requires_api_key || (explicit_store_true && failed_is_api_key);
            let released_plugin = turn
                .plugin_request
                .as_ref()
                .map_or(0, |(_, req)| req.frame.len())
                .saturating_add(sent_service_tier_bytes(turn.sent_service_tier.as_deref()));
            let next_tier_bytes =
                sent_service_tier_bytes(turn.body.get("service_tier").and_then(Value::as_str));
            (
                turn.auth.clone(),
                turn.frame.clone(),
                Arc::clone(&turn.body),
                turn.live_failover_candidates.clone(),
                requires_api_key,
                turn.lane.clone(),
                released_plugin,
                next_tier_bytes,
            )
        };
        let headroom = self
            .byte_cap
            .saturating_sub(self.used_bytes)
            .saturating_add(released_plugin);
        if next_tier_bytes.saturating_add(extra_retained_bytes) > headroom {
            return None;
        }
        let mut excluded = self.session_rate_limited_upstreams.clone();
        excluded.insert(failed_upstream_id);
        excluded.extend(relay_pool.excluded_upstreams.iter().copied());
        let mut available = self
            .lifecycle
            .filter_available_ws_owner_failover_candidates(
                &auth,
                WebSocketIngress {
                    headers: &self.headers,
                    frame: &frame,
                    query: self.query.as_deref(),
                },
                &body,
                &candidates,
                self.codex_backend,
                requires_api_key,
                &excluded,
            )
            .await;
        if !self.retain_viable_failover_candidates(
            failed_connection_id,
            failed_upstream_id,
            None,
            turn_id,
            &lane,
            &mut available,
            relay_pool,
        ) {
            return None;
        }
        let (old_released, next_tier, new_tier_bytes) = {
            let turn = self.turns.get(&turn_id)?;
            let next_tier = turn
                .body
                .get("service_tier")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let old_released = turn
                .plugin_request
                .as_ref()
                .map_or(0, |(_, req)| req.frame.len())
                .saturating_add(sent_service_tier_bytes(turn.sent_service_tier.as_deref()))
                .min(turn.charged_bytes);
            let new_tier_bytes = sent_service_tier_bytes(next_tier.as_deref());
            (old_released, next_tier, new_tier_bytes)
        };
        if new_tier_bytes.saturating_add(extra_retained_bytes)
            > self
                .byte_cap
                .saturating_sub(self.used_bytes)
                .saturating_add(old_released)
        {
            return None;
        }
        let turn = self.turns.get_mut(&turn_id)?;
        turn.connection_id = None;
        turn.upstream_id = None;
        turn.sent = false;
        turn.attempted = false;
        turn.plugin_request = None;
        let released = old_released.min(turn.charged_bytes);
        self.used_bytes = self
            .used_bytes
            .saturating_sub(released)
            .saturating_add(new_tier_bytes);
        turn.charged_bytes = turn
            .charged_bytes
            .saturating_sub(released)
            .saturating_add(new_tier_bytes);
        turn.sent_service_tier = next_tier;
        turn.echo_metadata = None;
        turn.metrics.reset_for_retry();
        turn.admitted_session_upstream = None;
        turn.owner_failover_replacement = true;
        turn.owner_failover_requires_api_key = requires_api_key;
        turn.candidate_oauth = available
            .iter()
            .map(|candidate| (candidate.upstream_id, candidate.socket_per_lane))
            .collect();
        turn.has_any_oauth_candidate = available.iter().any(|candidate| !candidate.is_api_key);
        turn.all_candidates_oauth =
            !available.is_empty() && available.iter().all(|candidate| candidate.socket_per_lane);
        turn.prepared.candidates = available.clone();
        turn.live_failover_candidates = available.clone();
        if self.session_upstream == Some(failed_upstream_id) {
            self.session_upstream = None;
        }
        Some(WebSocketDispatch {
            turn_id,
            frame,
            connection_id: None,
            candidates: available,
            stream_id: lane,
            control: false,
            prefer_api_key_on_final_attempt: !self.codex_backend && !requires_api_key,
            has_explicit_affinity: false,
            is_failover_recovery: true,
        })
    }

    pub async fn fail_dispatch(
        &mut self,
        turn_id: u64,
        error: &WebSocketError,
    ) -> Vec<WebSocketDispatch> {
        let lane = self.turns.get(&turn_id).map(|turn| turn.lane.clone());
        self.finish_turn(turn_id, Some(error));
        lane.and_then(|lane| self.next_in_lane(&lane))
            .into_iter()
            .collect()
    }

    pub async fn fail_dispatch_with_recovery(
        &mut self,
        turn_id: u64,
        failed_upstream_id: Option<Uuid>,
        error: &WebSocketError,
    ) -> (Vec<WebSocketDispatch>, Option<WebSocketError>) {
        self.fail_dispatch_with_recovery_and_pool(
            turn_id,
            failed_upstream_id,
            error,
            &WsRelayPoolSnapshot::default(),
        )
        .await
    }

    pub async fn fail_dispatch_with_recovery_and_pool(
        &mut self,
        turn_id: u64,
        failed_upstream_id: Option<Uuid>,
        error: &WebSocketError,
        relay_pool: &WsRelayPoolSnapshot,
    ) -> (Vec<WebSocketDispatch>, Option<WebSocketError>) {
        let failed_id = failed_upstream_id.or_else(|| {
            self.turns
                .get(&turn_id)
                .and_then(|turn| turn.upstream_id.or(turn.initial_selected_upstream_id))
        });
        let upstream_failure = (error.status == 429 && error.code != error_codes::LIMIT_REJECTED)
            || (error.status == 401 && error.code != "authentication_error")
            || matches!(
                error.code,
                "upstream_unavailable"
                    | "upstream_connection_closed"
                    | "upstream_signing_failed"
                    | "route_no_upstream_after_filter"
                    | "upstream_affinity_unavailable"
            );
        let recoverable_turn = self.turns.get(&turn_id).is_some_and(|turn| {
            !turn.sent && !turn.response_created && !turn.provider_frame_forwarded
        });
        let not_uncommitted_recovery = self
            .turns
            .get(&turn_id)
            .is_some_and(|turn| turn.suppressed_failover_error.is_none());
        if upstream_failure
            && recoverable_turn
            && not_uncommitted_recovery
            && let Some(failed_id) = failed_id
        {
            if let Some((ready, recovery_error)) = self
                .try_recover_codex_previous_response_failover(turn_id, failed_id)
                .await
            {
                return (ready, Some(recovery_error));
            }
            let previous_session_upstream = self.session_upstream;
            let failed_upstream_name = self
                .turns
                .get(&turn_id)
                .map(|turn| {
                    turn.prepared
                        .candidates
                        .iter()
                        .find(|candidate| candidate.upstream_id == failed_id)
                        .map(|candidate| candidate.upstream_name.clone())
                        .unwrap_or_else(|| turn.prepared.route_info.upstream_name.clone())
                })
                .unwrap_or_default();
            let failed_conn = self.turns.get(&turn_id).and_then(|t| t.connection_id);
            let suppressed_bytes = suppressed_failover_retained_bytes(error, &failed_upstream_name);
            if let Some(retry_dispatch) = self
                .try_owner_reasoning_failover_dispatch(
                    turn_id,
                    failed_id,
                    failed_conn,
                    relay_pool,
                    suppressed_bytes,
                )
                .await
            {
                self.record_suppressed_failover_error(
                    turn_id,
                    failed_id,
                    failed_upstream_name,
                    previous_session_upstream,
                    error.clone(),
                    suppressed_bytes,
                );
                return (vec![retry_dispatch], None);
            }
        }
        if error.code != "authentication_error"
            && let Some(suppressed) = self.turns.get_mut(&turn_id).and_then(|turn| {
                Self::clear_suppressed_failover_error_on_turn(&mut self.used_bytes, turn)
            })
        {
            if self.session_upstream.is_none() {
                self.session_upstream = suppressed.previous_session_upstream;
            }
            if let Some(turn) = self.turns.get_mut(&turn_id) {
                if let Some(uid) = suppressed.upstream_id {
                    turn.upstream_id = Some(uid);
                }
                if !suppressed.upstream_name.is_empty() {
                    turn.prepared.route_info.upstream_name = suppressed.upstream_name;
                }
            }
            let ready = self.fail_dispatch(turn_id, &suppressed.error).await;
            return (ready, Some(suppressed.error));
        }
        let ready = self.fail_dispatch(turn_id, error).await;
        (ready, Some(error.clone()))
    }

    fn evict_oldest_pending_error_terminal(&mut self) -> Option<Option<WebSocketError>> {
        let oldest_id = self
            .pending_error_terminals
            .iter()
            .min_by_key(|(_, pending)| (pending.expires_at, pending.turn_id))
            .map(|(id, _)| id.clone())?;
        let pending = self.pending_error_terminals.remove(&oldest_id)?;
        let emitted = Self::terminal_error_for_pending(&oldest_id, &pending);
        if let Some(owner) = self.owners.get_mut(&oldest_id) {
            owner.terminal_delivery = TerminalDelivery::ResponseTerminalDelivered;
        }
        self.settle_turn(pending.turn, pending.error.as_ref());
        Some(emitted)
    }

    fn park_pending_error_terminal(
        &mut self,
        turn_id: u64,
        resp_id: String,
        error: Option<WebSocketError>,
        missing_status: bool,
    ) -> Vec<WebSocketError> {
        let Some(mut turn) = self.detach_turn(turn_id) else {
            return Vec::new();
        };
        // Lane work has completed and the next queued turn may be dispatched now;
        // release concurrency slots while retaining token/cost liability for
        // trailing `response.failed` usage settlement.
        if let Some(reservation) = turn.prepared.reservation.as_ref() {
            reservation.release_concurrent_guards();
        }
        if self.slot_cap == 0 {
            if let Some(owner) = self.owners.get_mut(&resp_id) {
                owner.terminal_delivery = TerminalDelivery::ResponseTerminalDelivered;
            }
            let emitted = missing_status
                .then(|| Self::synthetic_terminal_error(&resp_id, turn.lane.clone(), error.clone()))
                .into_iter()
                .collect();
            self.settle_turn(turn, error.as_ref());
            return emitted;
        }
        // Release bulky request payload bytes immediately so queued recovery
        // turns are not throttled while awaiting a delayed `response.failed`,
        // retaining only the transform hook context and settlement state.
        let old_payload = turn
            .frame
            .len()
            .saturating_add(json_owned_bytes(&turn.body))
            .saturating_add(
                turn.plugin_request
                    .as_ref()
                    .map_or(0, |(_, request)| request.frame.len()),
            );
        turn.frame = Bytes::new();
        turn.body = Arc::new(Value::Null);
        if let Some((_, ref mut request)) = turn.plugin_request {
            request.frame = Bytes::new();
            request.sent_service_tier = None;
        }
        let new_payload = json_owned_bytes(&turn.body);
        let freed = old_payload
            .saturating_sub(new_payload)
            .min(turn.charged_bytes);
        self.used_bytes = self.used_bytes.saturating_sub(freed);
        turn.charged_bytes = turn.charged_bytes.saturating_sub(freed);
        let mut emitted = Vec::new();
        while self.pending_error_terminals.len() >= self.slot_cap {
            let Some(evicted_error) = self.evict_oldest_pending_error_terminal() else {
                break;
            };
            if let Some(err) = evicted_error {
                emitted.push(err);
            }
        }
        let expires_at = std::time::Instant::now() + PENDING_ERROR_TERMINAL_WINDOW;
        self.pending_error_terminals.insert(
            resp_id,
            PendingErrorTerminal {
                turn_id,
                turn,
                error,
                missing_status,
                expires_at,
            },
        );
        emitted
    }

    pub fn next_pending_error_deadline(&self) -> Option<std::time::Instant> {
        self.pending_error_terminals
            .values()
            .map(|pending| pending.expires_at)
            .min()
    }

    pub fn expire_pending_error_terminals(
        &mut self,
        now: std::time::Instant,
    ) -> Vec<WebSocketError> {
        let mut expired: Vec<_> = self
            .pending_error_terminals
            .iter()
            .filter(|(_, pending)| pending.expires_at <= now)
            .map(|(id, pending)| (pending.expires_at, pending.turn_id, id.clone()))
            .collect();
        expired.sort_by_key(|(expires_at, turn_id, _)| (*expires_at, *turn_id));
        let mut emitted = Vec::new();
        for (_, _, id) in expired {
            if let Some(pending) = self.pending_error_terminals.remove(&id) {
                if let Some(err) = Self::terminal_error_for_pending(&id, &pending) {
                    emitted.push(err);
                }
                if let Some(owner) = self.owners.get_mut(&id) {
                    owner.terminal_delivery = TerminalDelivery::ResponseTerminalDelivered;
                }
                self.settle_turn(pending.turn, pending.error.as_ref());
            }
        }
        emitted
    }

    pub fn connection_closed(&mut self, connection_id: u64, reason: &str) -> WebSocketEventResult {
        self.connections.remove(&connection_id);
        self.connection_lanes.remove(&connection_id);
        self.connection_oauth.remove(&connection_id);
        self.retired_terminals
            .retain(|(conn, _, _)| *conn != connection_id);
        let turn_ids: Vec<_> = self
            .turns
            .iter()
            .filter_map(|(id, turn)| (turn.connection_id == Some(connection_id)).then_some(*id))
            .collect();
        let mut result = WebSocketEventResult::default();
        for id in turn_ids {
            let lane = self.turns.get(&id).map(|turn| turn.lane.clone());
            let error = WebSocketError::new(502, "upstream_connection_closed", reason)
                .with_stream(lane.clone().flatten())
                .with_turn_id(id);
            self.finish_turn(id, Some(&error));
            result.errors.push(error);
        }
        self.connection_signers.remove(&connection_id);
        let dead_owners: Vec<_> = self
            .owners
            .iter()
            .filter_map(|(id, owner)| {
                (owner.connection_id == connection_id).then_some((
                    id.clone(),
                    owner.lane.clone(),
                    owner.terminal_delivery,
                    owner.turn_id,
                ))
            })
            .collect();
        self.recent
            .retain(|(connection, _), _| *connection != connection_id);
        for (id, lane, terminal_delivery, owner_turn_id) in dead_owners {
            if matches!(
                terminal_delivery,
                TerminalDelivery::ErrorAwaitingResponseFailed {
                    missing_status: true
                }
            ) {
                result.errors.push(
                    WebSocketError::new(502, "upstream_connection_closed", reason)
                        .with_stream(lane)
                        .with_response_id(&id)
                        .with_turn_id(owner_turn_id),
                );
            }
            self.collect_owner(Some(&id));
        }
        let pending_ids: Vec<_> = self
            .pending_error_terminals
            .iter()
            .filter(|(_, pending)| pending.turn.connection_id == Some(connection_id))
            .map(|(id, _)| id.clone())
            .collect();
        for id in pending_ids {
            if let Some(pending) = self.pending_error_terminals.remove(&id) {
                if let Some(owner) = self.owners.get_mut(&id) {
                    owner.terminal_delivery = TerminalDelivery::ResponseTerminalDelivered;
                }
                self.settle_turn(pending.turn, pending.error.as_ref());
            }
        }
        if let Some(session_upstream) = self.session_upstream {
            let still_used = self.connections.values().any(|id| *id == session_upstream)
                || self
                    .owners
                    .values()
                    .any(|owner| owner.upstream_id == session_upstream)
                || self
                    .turns
                    .values()
                    .any(|turn| turn.upstream_id == Some(session_upstream))
                || self
                    .pending_error_terminals
                    .values()
                    .any(|pending| pending.turn.upstream_id == Some(session_upstream));
            if !still_used {
                self.session_upstream = None;
            }
        }
        let lanes: Vec<_> = self.lanes.keys().cloned().collect();
        for lane in lanes {
            if let Some(dispatch) = self.next_in_lane(&lane) {
                result.dispatches.push(dispatch);
            }
        }
        self.prune_empty_lanes();
        result
    }

    pub fn drain(&mut self) {
        let error = WebSocketError::new(
            503,
            "draining",
            "The gateway is draining active WebSocket responses",
        );
        self.finish_session(&error);
    }

    pub fn close(&mut self) {
        let error = WebSocketError::new(
            499,
            "client_closed_request",
            "The client WebSocket closed before the response completed",
        );
        self.finish_session(&error);
    }

    fn finish_session(&mut self, error: &WebSocketError) {
        let turn_ids: Vec<_> = self.turns.keys().copied().collect();
        for turn_id in turn_ids {
            self.finish_turn(turn_id, Some(error));
        }
        let pending = std::mem::take(&mut self.pending_error_terminals);
        for (_, item) in pending {
            let effective = item.error.as_ref().unwrap_or(error);
            self.settle_turn(item.turn, Some(effective));
        }
        self.lanes.clear();
        self.pending_eviction_errors.clear();
        self.retired_terminals.clear();
        self.owners.clear();
        self.recent.clear();
        self.connections.clear();
        self.connection_oauth.clear();
        self.connection_signers.clear();
        self.session_upstream = None;
        self.session_oauth = false;
        self.session_rate_limited_upstreams.clear();
        self.successors.clear();
        self.used_bytes = 0;
        self.control_slots = 0;
    }
}

impl Drop for ResponsesWebSocketGateway {
    fn drop(&mut self) {
        self.close();
    }
}

fn transform_websocket_event(
    context: &ResponseTransformContext,
    request_id: &str,
    model: &str,
    frame: &Bytes,
    cap: usize,
) -> Result<Option<Vec<Bytes>>, WebSocketError> {
    let Some(hook) = context.dialect.sse_event_transform_hook() else {
        return Ok(None);
    };
    let error = |message: String| WebSocketError::new(502, "response_transform_error", message);
    let kind = sonic_rs::get_from_slice(frame, &["type"])
        .map_err(|_| error("Upstream event has no valid type".to_owned()))?;
    let name = kind
        .as_str()
        .ok_or_else(|| error("Upstream event type is not a string".to_owned()))?;
    match hook
        .transform_sse_event(TransformSseEventRequest {
            request_id: request_id.to_owned(),
            principal: context.principal.clone(),
            upstream: context.upstream.clone(),
            request_method: context.request_method.clone(),
            request_path: context.request_path.clone(),
            canonical_model_id: crate::model_resolution::canonical_model_id(
                cc_lb_domain::Provider::Openai,
                model,
            )
            .to_owned(),
            response_status: StatusCode::OK,
            response_headers: HeaderMap::new(),
            event: SseEvent {
                event: name.to_owned(),
                data: frame.clone(),
            },
        })
        .map_err(|source| error(source.to_string()))?
    {
        TransformSseEventResult::Unchanged => Ok(None),
        TransformSseEventResult::Drop => Ok(Some(Vec::new())),
        TransformSseEventResult::Replace { events } => {
            let mut frames = Vec::with_capacity(events.len());
            let mut bytes = 0usize;
            for event in events {
                let mut value: Value = sonic_rs::from_slice(&event.data).map_err(|_| {
                    error("Plugin returned an invalid WebSocket JSON event".to_owned())
                })?;
                let object = value.as_object_mut().ok_or_else(|| {
                    error("Plugin returned a non-object WebSocket event".to_owned())
                })?;
                if !event.event.is_empty() {
                    object.insert("type".to_owned(), Value::String(event.event));
                }
                if object.get("type").and_then(Value::as_str).is_none() {
                    return Err(error(
                        "Plugin returned a WebSocket event without a type".to_owned(),
                    ));
                }
                let frame = Bytes::from(
                    serde_json::to_vec(&value).map_err(|source| error(source.to_string()))?,
                );
                bytes = bytes.saturating_add(frame.len());
                if bytes > cap {
                    return Err(error(
                        "Plugin WebSocket output exceeds the message budget".to_owned(),
                    ));
                }
                frames.push(frame);
            }
            Ok(Some(frames))
        }
    }
}

fn resolve_turn_upstream_label<'a>(
    outcome: StreamTerminationOutcome,
    assigned_upstream_id: Option<Uuid>,
    routed_name: &'a str,
    first_candidate_name: Option<&'a str>,
) -> &'a str {
    let candidate =
        if assigned_upstream_id.is_some() || outcome == StreamTerminationOutcome::ClientCancelled {
            Some(routed_name)
        } else {
            first_candidate_name
        };
    candidate
        .filter(|name| !name.is_empty())
        .unwrap_or("unknown")
}

fn extract_incomplete_reason(value: &Value, signer: Option<&dyn Signer>) -> Option<String> {
    let reason = value
        .pointer("/response/incomplete_details/reason")
        .and_then(Value::as_str)
        .filter(|reason| !reason.is_empty())?;
    let scrubbed = signer.map_or(std::borrow::Cow::Borrowed(reason), |signer| {
        signer.scrub_secrets(reason)
    });
    let redacted = cc_lb_observability::RedactionPolicy::default().redact_text(&scrubbed);
    Some(cc_lb_observability::truncate_reason(&redacted))
}

fn classify_terminal_frame(
    kind: &str,
    value: &Value,
    lane: &Option<String>,
    signer: Option<&dyn Signer>,
) -> (Option<WebSocketError>, Option<Option<String>>) {
    match kind {
        "response.completed" => (None, None),
        "response.incomplete" => {
            let has_response_error = value
                .pointer("/response/error")
                .is_some_and(|error| !error.is_null());
            if !has_response_error {
                (None, Some(extract_incomplete_reason(value, signer)))
            } else {
                let mut error = native_terminal_error_with_signer(value, lane, signer);
                if error.native_code.is_none() {
                    error = error.with_native_code("response.incomplete".to_owned());
                }
                (Some(error), None)
            }
        }
        _ => {
            let mut error = native_terminal_error_with_signer(value, lane, signer);
            if error.native_code.is_none() {
                error = error.with_native_code(kind.to_owned());
            }
            (Some(error), None)
        }
    }
}

#[cfg_attr(not(test), allow(dead_code))]
fn native_terminal_error(value: &Value, lane: &Option<String>) -> WebSocketError {
    native_terminal_error_with_signer(value, lane, None)
}

fn is_openai_rate_or_usage_limit_ws_error(
    value: &Value,
    lane: &Option<String>,
    signer: Option<&dyn Signer>,
) -> bool {
    native_terminal_error_with_signer(value, lane, signer).status == 429
}

fn native_terminal_error_with_signer(
    value: &Value,
    lane: &Option<String>,
    signer: Option<&dyn Signer>,
) -> WebSocketError {
    let non_empty_str = |pointer: &str| {
        value
            .pointer(pointer)
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
    };
    let error_type = non_empty_str("/response/error/type")
        .or_else(|| non_empty_str("/error/type"))
        .or_else(|| non_empty_str("/code"));
    let native_code = non_empty_str("/response/error/code")
        .or_else(|| non_empty_str("/response/error/type"))
        .or_else(|| non_empty_str("/error/code"))
        .or_else(|| non_empty_str("/error/type"))
        .or_else(|| non_empty_str("/code"));
    let status = value
        .get("status")
        .or_else(|| value.pointer("/error/status"))
        .and_then(Value::as_u64)
        .and_then(|value| u16::try_from(value).ok())
        .unwrap_or(match (native_code, error_type) {
            (Some("server_is_overloaded" | "service_unavailable_error"), _)
            | (_, Some("service_unavailable_error")) => 503,
            (
                Some(
                    "rate_limit_exceeded"
                    | "rate_limit_error"
                    | "usage_limit_reached"
                    | "usage_limit_exceeded",
                ),
                _,
            )
            | (_, Some("rate_limit_error" | "usage_limit_reached" | "usage_limit_exceeded")) => 429,
            (Some("invalid_request_error"), _) | (_, Some("invalid_request_error")) => 400,
            _ => 502,
        });
    let raw_message = non_empty_str("/response/error/message")
        .or_else(|| non_empty_str("/error/message"))
        .or_else(|| non_empty_str("/message"))
        .unwrap_or("The provider response did not complete successfully");
    let scrubbed = signer.map_or(std::borrow::Cow::Borrowed(raw_message), |signer| {
        signer.scrub_secrets(raw_message)
    });
    let redacted = cc_lb_observability::RedactionPolicy::default().redact_text(&scrubbed);
    let message = cc_lb_observability::truncate_reason(&redacted);
    let mut error = if native_code == Some("previous_response_not_found")
        || message.contains("Invalid `previous_response_id`")
    {
        WebSocketError::new(400, "previous_response_not_found", message).with_stream(lane.clone())
    } else {
        WebSocketError::new(status, "upstream_stream_error", message).with_stream(lane.clone())
    };
    if let Some(code) = native_code {
        error = error.with_native_code(code.to_owned());
    }
    error
}

fn stream_id(value: &Value) -> Result<Option<String>, WebSocketError> {
    match value.get("stream_id") {
        None => Ok(None),
        Some(Value::String(id))
            if !id.is_empty()
                && id.len() <= 256
                && id.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.')
                }) =>
        {
            Ok(Some(id.clone()))
        }
        _ => Err(WebSocketError::new(
            400,
            "invalid_stream_id",
            "stream_id must contain 1-256 letters, numbers, underscores, hyphens, or periods",
        )),
    }
}

fn json_owned_bytes(value: &Value) -> usize {
    let own = std::mem::size_of::<Value>();
    match value {
        Value::String(value) => own.saturating_add(value.len()),
        Value::Array(values) => values.iter().fold(own, |total, value| {
            total.saturating_add(json_owned_bytes(value))
        }),
        Value::Object(values) => values.iter().fold(own, |total, (key, value)| {
            total
                .saturating_add(key.len())
                .saturating_add(json_owned_bytes(value))
                .saturating_add(4 * std::mem::size_of::<usize>())
        }),
        _ => own,
    }
}

fn terminate_error(observer: &LifecycleContext, error: &WebSocketError) {
    observer.terminate_failure(InternalFailure {
        status: StatusCode::from_u16(error.status).unwrap_or(StatusCode::BAD_GATEWAY),
        error_code: error.code,
        error: cc_lb_domain::InternalError {
            stage: if error.code == "authentication_error" {
                cc_lb_domain::InternalErrorStage::Authn
            } else {
                cc_lb_domain::InternalErrorStage::Relay
            },
            kind: if error.status < 500 {
                cc_lb_domain::InternalErrorKind::InvalidInput
            } else {
                cc_lb_domain::InternalErrorKind::Unavailable
            },
            message: Some(error.message.clone()),
        },
    });
}

fn usage_snapshot(usage: UsageCounts) -> UsageSnapshot {
    UsageSnapshot {
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        input_image_tokens: usage.input_image_tokens,
        output_image_tokens: usage.output_image_tokens,
        cache_creation_input_tokens: usage.cache_creation_input_tokens,
        cache_creation_input_tokens_5m: usage.cache_creation_input_tokens_5m,
        cache_creation_input_tokens_1h: usage.cache_creation_input_tokens_1h,
        cache_read_input_tokens: usage.cache_read_input_tokens,
        cache_write_input_tokens: usage.cache_write_input_tokens,
        cache_write_evidence: match usage.cache_write_evidence {
            crate::usage_parser::CacheTokenEvidence::Unreported => None,
            crate::usage_parser::CacheTokenEvidence::Zero => {
                Some(cc_lb_request_log::RequestCacheEvidence::Zero)
            }
            crate::usage_parser::CacheTokenEvidence::Positive => {
                Some(cc_lb_request_log::RequestCacheEvidence::Positive)
            }
            crate::usage_parser::CacheTokenEvidence::Invalid => {
                Some(cc_lb_request_log::RequestCacheEvidence::Invalid)
            }
        },
        cache_write_input_tokens_30m: usage.cache_write_input_tokens_30m,
        cache_write_input_tokens_12h: usage.cache_write_input_tokens_12h,
        cache_read_evidence: match usage.cache_read_evidence {
            crate::usage_parser::CacheTokenEvidence::Unreported => None,
            crate::usage_parser::CacheTokenEvidence::Zero => {
                Some(cc_lb_request_log::RequestCacheEvidence::Zero)
            }
            crate::usage_parser::CacheTokenEvidence::Positive => {
                Some(cc_lb_request_log::RequestCacheEvidence::Positive)
            }
            crate::usage_parser::CacheTokenEvidence::Invalid => {
                Some(cc_lb_request_log::RequestCacheEvidence::Invalid)
            }
        },
        cache_creation_evidence: match usage.cache_creation_evidence {
            crate::usage_parser::CacheTokenEvidence::Unreported => None,
            crate::usage_parser::CacheTokenEvidence::Zero => {
                Some(cc_lb_request_log::RequestCacheEvidence::Zero)
            }
            crate::usage_parser::CacheTokenEvidence::Positive => {
                Some(cc_lb_request_log::RequestCacheEvidence::Positive)
            }
            crate::usage_parser::CacheTokenEvidence::Invalid => {
                Some(cc_lb_request_log::RequestCacheEvidence::Invalid)
            }
        },
        thinking_tokens: usage.thinking_tokens,
        tool_use_prompt_tokens: usage.tool_use_prompt_tokens,
        service_tier: usage.service_tier,
        iterations: usage.iterations,
        pricing_slices: usage.pricing_slices,
    }
}

fn lane_bytes(lane: &Option<String>) -> usize {
    std::mem::size_of::<Lane>().saturating_add(lane.as_ref().map_or(0, String::len))
}

fn owner_bytes(id: &str, owner: &ResponseOwner) -> usize {
    std::mem::size_of::<ResponseOwner>()
        .saturating_add(owner.principal_id.len())
        .saturating_add(std::mem::size_of::<(u64, Option<String>)>())
        .saturating_add(id.len().saturating_mul(3))
        .saturating_add(owner.lane.as_ref().map_or(0, String::len).saturating_mul(2))
        .saturating_add(json_owned_bytes(&owner.body))
        .saturating_add(owner.echo_metadata.as_ref().map_or(0, json_owned_bytes))
}

fn parse_client_frame(
    _authenticated: &Authenticated,
    frame: &Bytes,
    cap: usize,
) -> Result<Value, WebSocketError> {
    if frame.len() > cap {
        return Err(WebSocketError::new(
            413,
            "request_too_large",
            "WebSocket message exceeds the configured body cap",
        ));
    }
    sonic_rs::from_slice(frame).map_err(|_| {
        WebSocketError::new(
            400,
            "invalid_json",
            "WebSocket client message must be valid JSON",
        )
    })
}

fn observe_authenticated_frame(observer: &LifecycleContext, info: cc_lb_lifecycle::AuthInfo) {
    observer.mark_authn_reached();
    observer.set_protocol(cc_lb_request_log::RequestProtocol::Websocket);
    observer.emit_request_started(true);
    observer.emit_lifecycle(LifecycleEvent::AuthCompleted {
        event_id: observer.event_id().to_owned(),
        result: Ok(info),
    });
}

fn queued_control_bytes(frame: &Bytes) -> usize {
    frame.len().saturating_add(std::mem::size_of::<Bytes>())
}

fn steering_id_bytes(id: &str) -> usize {
    id.len()
        .saturating_add(std::mem::size_of::<String>())
        .saturating_add(2 * std::mem::size_of::<usize>())
}

fn sanitize_client_event_type(kind: &str) -> String {
    const MAX_CHARS: usize = 64;
    let redacted = cc_lb_observability::RedactionPolicy::default().redact_text(kind);
    let mut out = String::with_capacity(redacted.len().min(MAX_CHARS + 3));
    for (count, ch) in redacted.chars().enumerate() {
        if count == MAX_CHARS {
            out.push_str("...");
            break;
        }
        out.push(if ch.is_control() { '?' } else { ch });
    }
    out
}

fn observed_response_usage(
    response: &Value,
    policy: ServiceTierMergePolicy,
) -> Option<(UsageCounts, bool)> {
    let reported = response
        .get("usage")
        .or_else(|| {
            response
                .get("response")
                .and_then(|value| value.get("usage"))
        })?
        .as_object()?;
    let input = reported.get("input_tokens").and_then(Value::as_u64);
    let output = reported.get("output_tokens").and_then(Value::as_u64);
    if input.is_none() && output.is_none() {
        return None;
    }
    let mut usage = observe_non_stream_json_value_for_shape_with_policy(
        response,
        crate::provider_capability::UsageShape::OpenaiResponses,
        policy,
    )
    .usage;
    if policy == ServiceTierMergePolicy::Standard
        && let Some(tier) = response
            .get("service_tier")
            .and_then(Value::as_str)
            .or_else(|| {
                response
                    .get("response")
                    .and_then(|value| value.get("service_tier"))
                    .and_then(Value::as_str)
            })
    {
        usage.service_tier = Some(tier.to_owned());
    }
    Some((usage, input.is_some() && output.is_some()))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use cc_lb_control::api_keys::concurrent_guard::KeyConcurrencyManager;
    use std::fmt::Debug;
    use std::sync::{Arc, Mutex};

    use cc_lb_control::api_keys::limit_engine::{ApiKeyUsageHandle, RejectReason};
    use cc_lb_control::api_keys::principal_view::PrincipalView;
    use cc_lb_storage_api::principal::{Limit, LimitKind};
    use cc_lb_storage_api::{ApiKeyUsageBucketQuery, MetaStore, Storage, StoredApiKeyRecord};
    use tracing::{Event, Level, Subscriber};
    use tracing_subscriber::layer::{Context, SubscriberExt as _};
    use tracing_subscriber::{Layer, Registry};

    #[test]
    fn resolve_turn_upstream_label_handles_assigned_queued_and_empty_names() {
        use StreamTerminationOutcome::{ClientCancelled, Completed, ProxyError};
        for (outcome, assigned, routed, candidate, expected) in [
            (
                Completed,
                Some(Uuid::nil()),
                "assigned",
                Some("first"),
                "assigned",
            ),
            (Completed, Some(Uuid::nil()), "reused", None, "reused"),
            (ProxyError, None, "unassigned", Some("queued"), "queued"),
            (ProxyError, None, "openai", None, "unknown"),
            (ClientCancelled, None, "openai", None, "openai"),
            (ClientCancelled, None, "openai", Some("queued"), "openai"),
            (ClientCancelled, None, "", None, "unknown"),
            (ProxyError, None, "unassigned", Some(""), "unknown"),
            (Completed, Some(Uuid::nil()), "", Some("first"), "unknown"),
        ] {
            assert_eq!(
                resolve_turn_upstream_label(outcome, assigned, routed, candidate),
                expected
            );
        }
    }

    #[test]
    fn incomplete_terminal_classification_distinguishes_clean_and_error_payloads() {
        let lane = Some("main".to_owned());
        for response in [
            serde_json::json!({"incomplete_details": {"reason": "max_output_tokens"}}),
            serde_json::json!({"error": null, "incomplete_details": {"reason": "max_output_tokens"}}),
        ] {
            let (failure, reason) = classify_terminal_frame(
                "response.incomplete",
                &serde_json::json!({"response": response}),
                &lane,
                None,
            );
            assert!(failure.is_none());
            assert_eq!(reason, Some(Some("max_output_tokens".to_owned())));
        }
        for (payload, code) in [
            (
                serde_json::json!({"code": "provider_code"}),
                "provider_code",
            ),
            (
                serde_json::json!({"type": "provider_type"}),
                "provider_type",
            ),
            (
                serde_json::json!({"message": "failed"}),
                "response.incomplete",
            ),
            (serde_json::json!({}), "response.incomplete"),
            (serde_json::json!("boom"), "response.incomplete"),
            (serde_json::json!(true), "response.incomplete"),
            (serde_json::json!(0), "response.incomplete"),
        ] {
            let (failure, reason) = classify_terminal_frame(
                "response.incomplete",
                &serde_json::json!({"response": {"error": payload}}),
                &lane,
                None,
            );
            let failure = failure.expect("every non-null error is a provider failure");
            assert_eq!(failure.code, "upstream_stream_error");
            assert_eq!(failure.native_code.as_deref(), Some(code));
            assert_eq!(reason, None);
        }
        let (failure, reason) =
            classify_terminal_frame("response.completed", &serde_json::json!({}), &lane, None);
        assert!(failure.is_none() && reason.is_none());
        for kind in ["response.failed", "error"] {
            let (failure, reason) =
                classify_terminal_frame(kind, &serde_json::json!({}), &lane, None);
            assert!(failure.is_some() && reason.is_none());
        }
    }

    fn terminal_fixture_gateway(
        fixture: &DirectAccountingFixture,
        observer: Option<LifecycleContext>,
    ) -> ResponsesWebSocketGateway {
        let mut gateway = ResponsesWebSocketGateway::new(
            fixture.lifecycle.clone(),
            &fixture.auth,
            HeaderMap::new(),
            16_384,
            8,
        );
        gateway.turns.insert(
            1,
            Turn {
                auth: fixture.auth.clone(),
                prepared_signers: HashMap::new(),
                initial_selected_upstream_id: Some(Uuid::nil()),
                candidate_oauth: HashMap::new(),
                prepared: PreparedWebSocketTurn {
                    oauth_compat: false,
                    oauth_transport: false,
                    explicit_store: Some(false),
                    candidates: Vec::new(),
                    observer,
                    completion_observer: Arc::new(
                        crate::completion_observer::CompletionObserver::new(),
                    ),
                    prompt_cache_observation_context: None,
                    reservation: None,
                    deferred_limit_context: None,
                    limit_engine: None,
                    principal_id: DIRECT_PRINCIPAL.to_owned(),
                    key_id: fixture.record.key_hash_b64.clone(),
                    model: "gpt-test".to_owned(),
                    store: false,
                    span: Span::none(),
                    setup_timings: Default::default(),
                    route_info: serde_json::from_value(serde_json::json!({
                        "upstream_id": Uuid::nil(), "upstream_name": "assigned", "model": "gpt-test"
                    }))
                    .unwrap(),
                    request_id: "terminal-fixture".to_owned(),
                    unknown_reasoning_plan: WsUnknownReasoningPlan::None,
                    has_explicit_affinity: false,
                    owner_failover_fallback: None,
                    reconciled_body: None,
                },
                body: Arc::new(serde_json::json!({})),
                frame: Bytes::new(),
                lane: None,
                connection_id: Some(1),
                upstream_id: Some(Uuid::nil()),
                pinned: None,
                response_id: Some("resp_fixture".to_owned()),
                previous_response_id: None,
                steer_parent: None,
                steer_submissions: 0,
                accepted_steers: HashSet::new(),
                controls: VecDeque::new(),
                sent: true,
                attempted: true,
                attempt_num: 1,
                charged_bytes: 0,
                usage: None,
                usage_complete: false,
                incomplete_reason: None,
                plugin_request: None,
                sent_service_tier: None,
                echo_metadata: None,
                metrics: ResponsesStreamMetrics::new(),
                funding_ambiguous: false,
                response_created: false,
                provider_frame_forwarded: false,
                unknown_reasoning_retry: None,
                unknown_reasoning_retried: false,
                pending_affinity_keys: Vec::new(),
                deferred_unestablished: None,
                has_any_oauth_candidate: false,
                all_candidates_oauth: false,
                admitted_session_upstream: None,
                owner_failover_replacement: false,
                owner_failover_requires_api_key: false,
                owner_failover_fallback: None,
                live_failover_candidates: Vec::new(),
                suppressed_failover_error: None,
            },
        );
        gateway.lanes.insert(
            None,
            Lane {
                active: Some(1),
                ..Default::default()
            },
        );
        gateway
    }

    #[tokio::test]
    async fn oauth_public_frame_uses_normalized_echo_metadata() {
        let fixture = DirectAccountingFixture::new().await;
        let mut gateway = terminal_fixture_gateway(&fixture, None);
        gateway.connection_lanes.insert(1, None);
        let frame = Bytes::from_static(
            br#"{"type":"response.completed","response":{"id":"resp_fixture"}}"#,
        );
        {
            let turn = gateway.turns.get_mut(&1).expect("fixture turn");
            turn.prepared.oauth_transport = true;
            turn.prepared.oauth_compat = true;
            turn.body = Arc::new(serde_json::json!({"metadata":{"raw":"must not echo"}}));
            turn.echo_metadata = None;
        }
        assert!(gateway.oauth_public_frame(1, &frame).is_none());

        gateway
            .turns
            .get_mut(&1)
            .expect("fixture turn")
            .echo_metadata = Some(serde_json::json!({"normalized":"echo"}));
        let output = gateway
            .oauth_public_frame(1, &frame)
            .expect("normalized echo frame");
        let output: Value = sonic_rs::from_slice(&output).expect("JSON frame");
        assert_eq!(
            output["response"]["metadata"],
            serde_json::json!({"normalized":"echo"})
        );
    }

    #[tokio::test]
    async fn extract_incomplete_reason_redacts_and_bounds_utf8() {
        assert_eq!(
            extract_incomplete_reason(&serde_json::json!({}), None),
            None
        );
        for reason in [Value::Null, serde_json::json!(""), serde_json::json!(42)] {
            assert_eq!(
                extract_incomplete_reason(
                    &serde_json::json!({"response": {"incomplete_details": {"reason": reason}}}),
                    None,
                ),
                None
            );
        }
        let extract = |reason: String| {
            extract_incomplete_reason(
                &serde_json::json!({"response": {"incomplete_details": {"reason": reason}}}),
                None,
            )
            .unwrap()
        };
        assert_eq!(extract("a".repeat(600)), "a".repeat(600));
        let unicode = extract("한".repeat(600));
        assert!(unicode.len() <= 1024);
        assert!(unicode.ends_with("...[truncated]"));
        assert!(std::str::from_utf8(unicode.as_bytes()).is_ok());
        let secret = "sk-secret-abcdefghijklmnopqrstuvwxyz123456789";
        let reason = format!("{} Bearer {secret} {}", "x".repeat(990), "z".repeat(200));
        let sanitized = extract(reason.clone());
        assert_eq!(
            sanitized,
            cc_lb_observability::truncate_reason(
                &cc_lb_observability::RedactionPolicy::default().redact_text(&reason)
            )
        );
        assert!(!sanitized.contains("sk-secret"));
        assert!(!sanitized.contains("abcdefghijklmnopqrstuvwxyz"));
        assert!(sanitized.len() <= 1024);
        let fixture = DirectAccountingFixture::new().await;
        let mut gateway = terminal_fixture_gateway(&fixture, None);
        let delta = Bytes::from_static(
            br#"{"type":"response.output_text.delta","delta":"unchanged","response":{"incomplete_details":{"reason":"not terminal"}}}"#
        );
        assert!(
            gateway
                .observe_provider_message(
                    1,
                    &delta,
                    &delta,
                    true,
                    None,
                    None,
                    None,
                    None,
                    &WsRelayPoolSnapshot::default(),
                )
                .await
                .unwrap()
                .forward
        );
        assert_eq!(gateway.turns[&1].incomplete_reason, None);
    }

    #[test]
    fn incomplete_reason_scrubs_exact_secret_before_redaction_and_truncation() {
        use cc_lb_signer_anthropic_key::{ProviderKeyKind, ProviderKeySigner};

        let secret = "opaque_custom_cred_xyz987";
        let signer = ProviderKeySigner::new(ProviderKeyKind::Openai, secret).unwrap();
        let policy = cc_lb_observability::RedactionPolicy::default();
        assert_eq!(policy.redact_text(secret), secret);
        for prefix_len in [0, 990, 1000, 1015] {
            let reason = format!(
                "{}{secret} Bearer sk-secret-abcdefghijklmnopqrstuvwxyz123456789 {}",
                "x".repeat(prefix_len),
                "한".repeat(200)
            );
            let value = serde_json::json!({
                "response": {"incomplete_details": {"reason": reason}}
            });
            let sanitized = extract_incomplete_reason(&value, Some(&signer)).unwrap();
            let expected = cc_lb_observability::truncate_reason(
                &policy.redact_text(&reason.replace(secret, "[REDACTED]")),
            );
            assert_eq!(sanitized, expected, "prefix length {prefix_len}");
            assert!(!sanitized.contains("opaque"));
            assert!(!sanitized.contains("sk-secret"));
            assert!(sanitized.len() <= 1024);
            assert!(std::str::from_utf8(sanitized.as_bytes()).is_ok());
        }
    }

    #[tokio::test]
    async fn assigned_connection_retains_signer_until_connection_or_session_closes() {
        use cc_lb_signer_anthropic_key::{ProviderKeyKind, ProviderKeySigner};

        let fixture = DirectAccountingFixture::new().await;
        let bus = Arc::new(cc_lb_control::InMemoryBus::new());
        let mut events = bus.attach_lifecycle_assembler(16);
        let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let observer = LifecycleContext::new("exact-secret-terminal".to_owned(), bus, &clock);
        observer.mark_authn_reached();
        let mut gateway = terminal_fixture_gateway(&fixture, Some(observer));
        let secret = "opaque_custom_cred_xyz987";
        let signer: Arc<dyn Signer> =
            Arc::new(ProviderKeySigner::new(ProviderKeyKind::Openai, secret).unwrap());
        gateway
            .turns
            .get_mut(&1)
            .unwrap()
            .prepared_signers
            .insert(Uuid::nil(), signer.clone());
        gateway.assign_connection(1, 1, Uuid::nil(), "assigned");
        assert!(Arc::ptr_eq(&gateway.connection_signers[&1], &signer));
        let rotated_signer: Arc<dyn Signer> = Arc::new(
            ProviderKeySigner::new(ProviderKeyKind::Openai, "rotated_custom_cred_abc123").unwrap(),
        );
        gateway
            .turns
            .get_mut(&1)
            .unwrap()
            .prepared_signers
            .insert(Uuid::nil(), rotated_signer);
        gateway.assign_connection(1, 1, Uuid::nil(), "assigned");
        assert!(Arc::ptr_eq(&gateway.connection_signers[&1], &signer));
        assert!(gateway.turns[&1].prepared_signers.is_empty());
        let frame = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "type": "response.incomplete",
                "response": {
                    "id": "resp_fixture",
                    "incomplete_details": {"reason": secret},
                    "usage": {"input_tokens": 12, "output_tokens": 16}
                }
            }))
            .unwrap(),
        );
        let original_frame = frame.clone();
        let result = gateway
            .observe_provider_message(
                1,
                &frame,
                &frame,
                true,
                None,
                None,
                None,
                None,
                &WsRelayPoolSnapshot::default(),
            )
            .await
            .unwrap();
        assert!(result.forward);
        assert!(result.frames.is_none());
        assert!(result.errors.is_empty());
        assert_eq!(frame, original_frame);
        let mut warnings = Vec::new();
        while let Ok(event) = events.try_recv() {
            if let LifecycleEvent::UpstreamStreamDiagnosticsObserved { diagnostics, .. } = event {
                warnings.push(diagnostics.warning_message);
            }
        }
        assert_eq!(warnings, vec![Some("[REDACTED]".to_owned())]);
        assert!(gateway.turns.is_empty());
        assert!(gateway.connection_signers.contains_key(&1));
        gateway.connection_closed(1, "provider closed");
        assert!(gateway.connection_signers.is_empty());
        gateway.connection_signers.insert(2, signer);
        gateway.close();
        assert!(gateway.connection_signers.is_empty());
    }

    #[tokio::test]
    async fn terminal_transform_error_overrides_incomplete_completion() {
        use cc_lb_control::RequestEventBus;
        let fixture = DirectAccountingFixture::new().await;
        let bus = Arc::new(cc_lb_control::InMemoryBus::new());
        let mut events = bus.subscribe_lifecycle();
        let mut assembler_events = bus.attach_lifecycle_assembler(16);
        let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let observer = LifecycleContext::new("transform-terminal".to_owned(), bus, &clock);
        observer.mark_authn_reached();
        let mut gateway = terminal_fixture_gateway(&fixture, Some(observer));
        let frame = Bytes::from_static(
            br#"{"type":"response.incomplete","response":{"id":"resp_fixture","incomplete_details":{"reason":"max_output_tokens"},"usage":{"input_tokens":12,"output_tokens":16}}}"#
        );
        let error = WebSocketError::new(502, "plugin_transform_error", "transform rejected");
        gateway
            .observe_provider_message(
                1,
                &frame,
                &frame,
                true,
                None,
                None,
                Some(&error),
                None,
                &WsRelayPoolSnapshot::default(),
            )
            .await
            .unwrap();
        let mut completions = 0;
        while let Ok(event) = events.try_recv() {
            if let LifecycleEvent::StreamCompleted { result, .. } = event {
                completions += 1;
                assert_eq!(result.unwrap_err().error_type, "plugin_transform_error");
            }
        }
        while let Ok(event) = assembler_events.try_recv() {
            assert!(
                !matches!(
                    event,
                    LifecycleEvent::UpstreamStreamDiagnosticsObserved { .. }
                ),
                "transform failure must not emit incomplete-completion diagnostics"
            );
        }
        assert_eq!(completions, 1);
        assert!(gateway.turns.is_empty());
    }

    pub(crate) static DIRECT_PRICING_LOCK: tokio::sync::Mutex<()> =
        tokio::sync::Mutex::const_new(());
    pub(crate) const DIRECT_PRINCIPAL: &str = "observerless-ws-principal";

    pub(crate) fn install_direct_pricing() {
        use cc_lb_pricing::{
            CatalogSnapshot, CatalogStatus, NativePricingProvider, NativePricingRate, Pricing,
            ProviderNativePricing, TierRate, UsdPerMillion,
        };
        use std::collections::BTreeMap;

        let mut snapshot = CatalogSnapshot::empty_cost_disabled();
        snapshot.status = CatalogStatus::Ok;
        for (model, provider, read_rate) in [
            (
                "ws-openai-known",
                NativePricingProvider::OpenAi,
                Some(100_000),
            ),
            ("ws-openai-unknown", NativePricingProvider::OpenAi, None),
            (
                "ws-gemini-known",
                NativePricingProvider::Gemini,
                Some(100_000),
            ),
            ("ws-gemini-unknown", NativePricingProvider::Gemini, None),
        ] {
            let mut native = ProviderNativePricing::unknown(provider);
            native.ordinary_input = NativePricingRate::known(UsdPerMillion::from_whole_usd(2));
            native.cached_input_read = read_rate.map_or_else(NativePricingRate::unknown, |rate| {
                NativePricingRate::known(UsdPerMillion::from_micros_usd(rate))
            });
            snapshot.models.insert(
                model.to_owned(),
                Pricing {
                    model: model.to_owned(),
                    input_per_million_usd: UsdPerMillion::from_whole_usd(2),
                    output_per_million_usd: UsdPerMillion::from_whole_usd(10),
                    input_image_per_million_usd: None,
                    output_image_per_million_usd: None,
                    by_tier: Default::default(),
                    native: Some(native),
                },
            );
        }
        snapshot.models.insert(
            "ws-openai-priority-known".to_owned(),
            Pricing {
                model: "ws-openai-priority-known".to_owned(),
                input_per_million_usd: UsdPerMillion::from_whole_usd(2),
                output_per_million_usd: UsdPerMillion::from_whole_usd(10),
                input_image_per_million_usd: None,
                output_image_per_million_usd: None,
                by_tier: BTreeMap::from([(
                    "priority".to_owned(),
                    TierRate {
                        input_per_million_usd: UsdPerMillion::from_whole_usd(4),
                        output_per_million_usd: UsdPerMillion::from_whole_usd(20),
                    },
                )]),
                native: None,
            },
        );
        cc_lb_pricing::global_catalog().install_snapshot(snapshot);
    }

    struct UnusedSignerFactory;

    impl cc_lb_upstream::ApiKeyAwareSignerFactory for UnusedSignerFactory {
        fn with_router_choice(&self, _name: String) -> Arc<dyn cc_lb_upstream::SignerFactory> {
            unreachable!("settling an admitted turn does not sign requests")
        }
    }

    pub(crate) struct DirectAccountingFixture {
        pub(crate) engine: Arc<LimitEngine>,
        pub(crate) view: Arc<PrincipalView>,
        pub(crate) record: StoredApiKeyRecord,
        pub(crate) concurrency: Arc<KeyConcurrencyManager>,
        pub(crate) lifecycle: Arc<Lifecycle>,
        pub(crate) auth: Authenticated,
        storage: Arc<dyn Storage>,
        usage_handle: ApiKeyUsageHandle,
    }

    impl DirectAccountingFixture {
        pub(crate) async fn new() -> Self {
            use cc_lb_control::api_keys::builtin_authn::BuiltinAuthn;
            use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
            use cc_lb_control::dynamic_view::{DynamicViewBuilder, DynamicViewHolder};

            let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(1_800_000_000));
            let storage = Arc::new(
                cc_lb_storage_sqlite::open_sqlite("sqlite::memory:", clock.clone())
                    .await
                    .expect("open in-memory websocket accounting database"),
            );
            storage
                .initialize()
                .await
                .expect("migrate accounting database");
            let keys = Arc::new(KeyStore::new(storage.clone()));
            let (record, secret) = keys
                .create(
                    DIRECT_PRINCIPAL,
                    CreateParams {
                        label: "observerless-websocket".to_owned(),
                        description: None,
                        expires_at_unix_secs: None,
                        limit_overrides: Vec::new(),
                    },
                )
                .await
                .expect("issue websocket fixture key");
            let view = Arc::new(PrincipalView::for_tests(
                DIRECT_PRINCIPAL,
                true,
                vec!["*".to_owned()],
                vec![
                    Limit {
                        kind: LimitKind::CostUsd,
                        window_secs: 60,
                        cap_micros: 100_000,
                    },
                    Limit {
                        kind: LimitKind::TotalTokens,
                        window_secs: 60,
                        cap_micros: 100_000,
                    },
                    Limit {
                        kind: LimitKind::Concurrent,
                        window_secs: 0,
                        cap_micros: 1,
                    },
                ],
                HashMap::new(),
            ));
            let authn = Arc::new(BuiltinAuthn::new(keys, clock.clone()));
            let mut headers = HeaderMap::new();
            headers.insert(
                "x-api-key",
                secret.expose().parse().expect("fixture key header"),
            );
            let auth = crate::authenticate_first(&authn, &headers, &view)
                .await
                .expect("authenticate embedded websocket caller");
            let dynamic = DynamicViewBuilder::new(0)
                .signer_factory(Arc::new(UnusedSignerFactory))
                .principal_view(view.clone())
                .build();
            struct UnusedDispatcher;
            #[async_trait::async_trait]
            impl crate::UpstreamDispatch for UnusedDispatcher {
                async fn dispatch(
                    &self,
                    _request: cc_lb_upstream::SignedRequest,
                ) -> Result<http::Response<crate::Body>, crate::DispatchError> {
                    unreachable!("unused dispatcher in websocket accounting test")
                }
            }
            let lifecycle = Arc::new(Lifecycle::new_with_dynamic_view(
                authn,
                Arc::new(DynamicViewHolder::new(dynamic)),
                Arc::new(UnusedDispatcher),
                crate::LifecycleConfig::default(),
                clock.clone(),
            ));
            let concurrency = Arc::new(KeyConcurrencyManager::new());
            let engine = LimitEngine::new(concurrency.clone(), clock);
            let usage_handle = engine
                .start_durable_usage_sync(storage.clone())
                .await
                .expect("start durable embedded accounting");
            Self {
                engine,
                view,
                record,
                concurrency,
                lifecycle,
                auth,
                storage,
                usage_handle,
            }
        }

        pub(crate) fn reserve(&self, model: &str) -> Reservation {
            let reservation = self
                .engine
                .reserve(
                    &self.view,
                    &self.record,
                    DIRECT_PRINCIPAL,
                    model,
                    Some(1_000),
                    1_000,
                    Some(8_010),
                )
                .expect("admit original estimated websocket liability");
            assert_eq!(self.concurrency.current(&self.record.key_hash_b64), 1);
            reservation
        }

        pub(crate) async fn assert_settled(
            self,
            reservation_id: &str,
            model: &str,
            cost: i64,
            input: u64,
            output: u64,
        ) {
            assert_eq!(self.concurrency.current(&self.record.key_hash_b64), 0);
            assert!(
                !self.engine.refund_by_id(reservation_id),
                "finish consumed the reservation"
            );
            assert!(!self.engine.reconcile_by_id(reservation_id, 0, 0, Some(0)));
            // These current-thread tests do not yield between starting durable
            // sync and this oracle, so admission still uses its documented 10%
            // cold allowance. Settlement must preserve the exact balance there.
            let remaining = 10_000 - cost;
            assert!(
                matches!(
                    self.engine.reserve(
                        &self.view,
                        &self.record,
                        DIRECT_PRINCIPAL,
                        model,
                        Some(0),
                        0,
                        Some(remaining + 1)
                    ),
                    Err(RejectReason::CostRateLimit)
                ),
                "one micro above the remaining monetary budget must be rejected"
            );
            let probe = self
                .engine
                .reserve(
                    &self.view,
                    &self.record,
                    DIRECT_PRINCIPAL,
                    model,
                    Some(0),
                    0,
                    Some(remaining),
                )
                .expect("exact remaining budget must be admissible after concurrency release");
            drop(probe);
            let tokens = self
                .engine
                .headers_for(&self.record.key_hash_b64, DIRECT_PRINCIPAL)
                .into_iter()
                .find(|(name, _)| name == "anthropic-ratelimit-tokens-remaining")
                .expect("measured token budget header");
            assert_eq!(tokens.1, (100_000 - input - output).to_string());
            self.usage_handle.shutdown().await;
            let buckets = self
                .storage
                .query_api_key_usage_buckets(&ApiKeyUsageBucketQuery {
                    key_ids: vec![self.record.key_hash_b64.clone()],
                    since_unix_secs: 1_800_000_000,
                    until_unix_secs: 1_800_000_001,
                    exclude_writer_epoch: Uuid::nil(),
                })
                .await
                .expect("read durably settled usage");
            assert_eq!(buckets.len(), 1);
            assert_eq!(buckets[0].usage.requests, 1, "finish settles exactly once");
            assert_eq!(buckets[0].usage.input_tokens, input as i64);
            assert_eq!(buckets[0].usage.output_tokens, output as i64);
            assert_eq!(buckets[0].usage.cost_usd_micros, cost);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn observerless_finish_settles_native_cache_unknown_and_zero_cost() {
        let _guard = DIRECT_PRICING_LOCK.lock().await;
        install_direct_pricing();
        for (model, response, expected_cost, expected_input, expected_output) in [
            (
                "ws-openai-known",
                serde_json::json!({
                    "usage": {
                        "input_tokens": 21_000,
                        "output_tokens": 1,
                        "input_tokens_details": { "cached_tokens": 20_000 }
                    }
                }),
                4_010,
                1_000,
                1,
            ),
            (
                "ws-openai-unknown",
                serde_json::json!({
                    "usage": {
                        "input_tokens": 21_000,
                        "output_tokens": 1,
                        "input_tokens_details": { "cached_tokens": 20_000 }
                    }
                }),
                8_010,
                1_000,
                1,
            ),
            (
                "ws-openai-known",
                serde_json::json!({
                    "usage": {
                        "input_tokens": 0,
                        "output_tokens": 0,
                        "input_tokens_details": { "cached_tokens": 0 }
                    }
                }),
                0,
                0,
                0,
            ),
            (
                "ws-openai-known",
                serde_json::json!({
                    "usage": {
                        "input_tokens": 1_200,
                        "output_tokens": 0,
                        "input_tokens_details": {
                            "cached_tokens": 0,
                            "cache_write_tokens": 300,
                            "cache_write_tokens_details": {
                                "ttl_30m_tokens": 200,
                                "ttl_12h_tokens": 100
                            }
                        }
                    }
                }),
                // 900*2 + 200*(2*5/4) + 100*(2*5/2).
                2_800,
                900,
                0,
            ),
            (
                "ws-openai-known",
                serde_json::json!({
                    "usage": {
                        "input_tokens": 1_200,
                        "output_tokens": 0,
                        "input_tokens_details": {
                            "cached_tokens": 0,
                            "cache_write_tokens": 300
                        }
                    }
                }),
                8_010,
                900,
                0,
            ),
        ] {
            let fixture = DirectAccountingFixture::new().await;
            let reservation = fixture.reserve(model);
            let reservation_id = reservation.id().to_owned();
            let (usage, usage_complete) =
                observed_response_usage(&response, ServiceTierMergePolicy::Standard).unwrap();
            let mut gateway = ResponsesWebSocketGateway::new(
                fixture.lifecycle.clone(),
                &fixture.auth,
                HeaderMap::new(),
                1_024,
                1,
            );
            gateway.turns.insert(
                1,
                Turn {
                    auth: fixture.auth.clone(),
                    prepared_signers: HashMap::new(),
                    initial_selected_upstream_id: None,
                    candidate_oauth: HashMap::new(),
                    prepared: PreparedWebSocketTurn {
                        oauth_compat: false,
                        oauth_transport: false,
                        explicit_store: None,
                        candidates: Vec::new(),
                        observer: None,
                        completion_observer: Arc::new(
                            crate::completion_observer::CompletionObserver::new(),
                        ),
                        prompt_cache_observation_context: None,
                        reservation: Some(reservation),
                        deferred_limit_context: None,
                        limit_engine: Some(fixture.engine.clone()),
                        principal_id: DIRECT_PRINCIPAL.to_owned(),
                        key_id: fixture.record.key_hash_b64.clone(),
                        model: model.to_owned(),
                        store: false,
                        span: Span::none(),
                        setup_timings: Default::default(),
                        route_info: serde_json::from_value(serde_json::json!({
                            "upstream_id": Uuid::nil(), "upstream_name": "embedded", "model": model
                        }))
                        .expect("fixture route info"),
                        request_id: "embedded-turn".to_owned(),
                        unknown_reasoning_plan: WsUnknownReasoningPlan::None,
                        has_explicit_affinity: false,
                        owner_failover_fallback: None,
                        reconciled_body: None,
                    },
                    body: Arc::new(serde_json::json!({})),
                    frame: Bytes::new(),
                    lane: None,
                    connection_id: None,
                    upstream_id: None,
                    pinned: None,
                    response_id: None,
                    previous_response_id: None,
                    steer_parent: None,
                    steer_submissions: 0,
                    accepted_steers: HashSet::new(),
                    controls: VecDeque::new(),
                    sent: true,
                    attempted: true,
                    attempt_num: 1,
                    charged_bytes: 0,
                    usage: Some(usage),
                    usage_complete,
                    incomplete_reason: None,
                    plugin_request: None,
                    sent_service_tier: None,
                    echo_metadata: None,
                    metrics: ResponsesStreamMetrics::new(),
                    funding_ambiguous: false,
                    response_created: false,
                    provider_frame_forwarded: false,
                    unknown_reasoning_retry: None,
                    unknown_reasoning_retried: false,
                    pending_affinity_keys: Vec::new(),
                    deferred_unestablished: None,
                    has_any_oauth_candidate: false,
                    all_candidates_oauth: false,
                    admitted_session_upstream: None,
                    owner_failover_replacement: false,
                    owner_failover_requires_api_key: false,
                    owner_failover_fallback: None,
                    live_failover_candidates: Vec::new(),
                    suppressed_failover_error: None,
                },
            );
            gateway.finish_turn(1, None);
            gateway.finish_turn(1, None);
            assert!(gateway.turns.is_empty());
            drop(gateway);
            fixture
                .assert_settled(
                    &reservation_id,
                    model,
                    expected_cost,
                    expected_input,
                    expected_output,
                )
                .await;
        }
    }

    #[test]
    fn native_cache_write_response_usage_preserves_provider_fields() {
        let (usage, complete) = observed_response_usage(
            &serde_json::json!({
                "usage": {
                    "input_tokens": 1200, "output_tokens": 1,
                    "input_tokens_details": {
                        "cached_tokens": 0,
                        "cache_write_tokens": 300,
                        "cache_write_tokens_details": {
                            "ttl_30m_tokens": 200, "ttl_12h_tokens": 100
                        }
                    }
                }
            }),
            ServiceTierMergePolicy::Standard,
        )
        .expect("complete provider usage");
        assert!(complete);
        assert_eq!(usage.cache_creation_input_tokens, 0);
        assert_eq!(usage.cache_write_input_tokens, Some(300));
        assert_eq!(usage.cache_write_input_tokens_30m, Some(200));
        assert_eq!(usage.cache_write_input_tokens_12h, Some(100));
        assert_eq!(usage.input_tokens, 900);
    }

    #[test]
    fn absent_null_and_empty_usage_remain_unknown_not_zero() {
        for value in [
            serde_json::json!({}),
            serde_json::json!({"usage": null}),
            serde_json::json!({"usage": {}}),
            serde_json::json!({"usage": {"input_tokens": null, "output_tokens": null}}),
        ] {
            assert!(observed_response_usage(&value, ServiceTierMergePolicy::Standard).is_none());
        }
        let (zero, complete) = observed_response_usage(
            &serde_json::json!({"usage": {"input_tokens": 0, "output_tokens": 0}}),
            ServiceTierMergePolicy::Standard,
        )
        .expect("explicit numeric zero is a real observation");
        assert!(zero.present && complete);
        assert_eq!((zero.input_tokens, zero.output_tokens), (0, 0));
        let (partial, complete) = observed_response_usage(
            &serde_json::json!({"usage": {"input_tokens": 7}}),
            ServiceTierMergePolicy::Standard,
        )
        .expect("measured input is retained on interruption");
        assert_eq!(partial.input_tokens, 7);
        assert!(
            !complete,
            "an unreported output count is not a complete zero snapshot"
        );
        let (tiered, _) = observed_response_usage(
            &serde_json::json!({"service_tier": "priority", "usage": {"input_tokens": 7, "output_tokens": 11}}),
            ServiceTierMergePolicy::Standard,
        )
        .unwrap();
        assert_eq!(
            tiered.service_tier.as_deref(),
            Some("priority"),
            "pricing uses the provider's resolved tier"
        );
    }

    #[test]
    fn named_and_default_lanes_remain_distinct() {
        assert_eq!(stream_id(&serde_json::json!({})).unwrap(), None);
        assert_eq!(
            stream_id(&serde_json::json!({"stream_id":"agent.main-1"}))
                .unwrap()
                .as_deref(),
            Some("agent.main-1")
        );
        for value in [
            Value::Null,
            Value::String(String::new()),
            Value::String("a/b".to_owned()),
        ] {
            assert_eq!(
                stream_id(&serde_json::json!({"stream_id":value}))
                    .unwrap_err()
                    .code,
                "invalid_stream_id"
            );
        }
    }

    #[test]
    fn response_failed_uses_native_error_details() {
        let error = native_terminal_error(
            &serde_json::json!({
                "type": "response.failed",
                "status": 200,
                "response": {
                    "error": {
                        "code": "server_error",
                        "message": "native failure"
                    }
                }
            }),
            &Some("main".to_owned()),
        );
        assert_eq!(error.code, "upstream_stream_error");
        assert_eq!(error.native_code.as_deref(), Some("server_error"));
        assert_eq!(error.message, "native failure");
        assert_eq!(error.stream_id.as_deref(), Some("main"));
    }

    #[test]
    fn protocol_error_keeps_provider_error_shape_and_default_lane_absent() {
        let error = WebSocketError::new(503, "overloaded_error", "configured budget is full");
        let value: Value = sonic_rs::from_slice(&error.to_bytes()).unwrap();
        assert_eq!(value["type"], "error");
        assert_eq!(value["status"], 503);
        assert_eq!(value["error"]["code"], "overloaded_error");
        assert!(value.get("stream_id").is_none());
        let value: Value =
            sonic_rs::from_slice(&error.with_stream(Some("main".to_owned())).to_bytes()).unwrap();
        assert_eq!(value["stream_id"], "main");
    }

    #[test]
    fn borrowed_delta_discriminator_does_not_construct_or_rewrite_payload() {
        let raw = Bytes::from_static(
            br#"{"type":"response.output_text.delta","delta":"opaque payload","stream_id":"main"}"#,
        );
        let pointer = raw.as_ptr();
        let event_type = sonic_rs::get_from_slice(&raw, &["type"]).unwrap();
        assert_eq!(event_type.as_str(), Some("response.output_text.delta"));
        assert_eq!(raw.as_ptr(), pointer);
        assert_eq!(
            raw.as_ref(),
            br#"{"type":"response.output_text.delta","delta":"opaque payload","stream_id":"main"}"#
        );
    }

    const UNKNOWN_ALIAS_MESSAGE: &str = "unknown model alias, returning unchanged";

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct CapturedWarning {
        message: String,
        model: Option<String>,
    }

    #[derive(Default)]
    struct EventFields {
        message: Option<String>,
        model: Option<String>,
    }

    impl tracing::field::Visit for EventFields {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn Debug) {
            let value = format!("{value:?}");
            let value = value
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
                .map_or(value.clone(), str::to_owned);
            self.record(field, value);
        }

        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.record(field, value.to_owned());
        }
    }

    impl EventFields {
        fn record(&mut self, field: &tracing::field::Field, value: String) {
            match field.name() {
                "message" => self.message = Some(value),
                "model" => self.model = Some(value),
                _ => {}
            }
        }
    }

    #[derive(Clone)]
    struct WarningLayer {
        warnings: Arc<Mutex<Vec<CapturedWarning>>>,
    }

    impl<S> Layer<S> for WarningLayer
    where
        S: Subscriber,
    {
        fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
            if *event.metadata().level() != Level::WARN {
                return;
            }
            let mut fields = EventFields::default();
            event.record(&mut fields);
            let Some(message) = fields.message else {
                return;
            };
            self.warnings
                .lock()
                .expect("warning capture lock")
                .push(CapturedWarning {
                    message,
                    model: fields.model,
                });
        }
    }

    fn capture_warnings<R>(operation: impl FnOnce() -> R) -> (R, Vec<CapturedWarning>) {
        let warnings = Arc::new(Mutex::new(Vec::new()));
        let layer = WarningLayer {
            warnings: Arc::clone(&warnings),
        };
        let result = tracing::subscriber::with_default(Registry::default().with(layer), operation);
        let warnings = warnings.lock().expect("warning capture lock").clone();
        (result, warnings)
    }

    struct TestWebSocketTransformDialect {
        result: TransformSseEventResult,
        canonical_models: Arc<Mutex<Vec<String>>>,
    }

    impl cc_lb_upstream::UpstreamDialect for TestWebSocketTransformDialect {
        fn shape(
            &self,
            _context: &cc_lb_upstream::DialectShapeContext,
            _upstream: &cc_lb_domain::Upstream,
            _principal: &cc_lb_domain::Principal,
            _builder: &mut cc_lb_upstream::ShapedRequestBuilder,
        ) -> Result<cc_lb_upstream::ShapedRequest, cc_lb_upstream::DialectError> {
            unreachable!("the websocket transform test does not shape a request")
        }

        fn sse_event_transform_hook(&self) -> Option<&dyn cc_lb_upstream::SseEventTransformHook> {
            Some(self)
        }
    }

    impl cc_lb_upstream::SseEventTransformHook for TestWebSocketTransformDialect {
        fn transform_sse_event(
            &self,
            request: cc_lb_upstream::TransformSseEventRequest,
        ) -> Result<TransformSseEventResult, cc_lb_upstream::ResponseTransformError> {
            self.canonical_models
                .lock()
                .expect("canonical model capture lock")
                .push(request.canonical_model_id);
            Ok(self.result.clone())
        }
    }

    fn websocket_transform_context(
        result: TransformSseEventResult,
        canonical_models: Arc<Mutex<Vec<String>>>,
    ) -> ResponseTransformContext {
        ResponseTransformContext {
            principal: cc_lb_domain::Principal {
                id: "principal".to_owned(),
                kind: cc_lb_domain::PrincipalKind::InternalKey,
            },
            upstream: cc_lb_domain::Upstream::OpenaiDirect { base_url: None },
            request_method: http::Method::POST,
            request_path: "/v1/responses".to_owned(),
            dialect: Arc::new(TestWebSocketTransformDialect {
                result,
                canonical_models,
            }),
            gemini_oauth: false,
            vertex: false,
            codex_backend: false,
            vertex_location: None,
        }
    }

    #[test]
    fn websocket_sse_transform_maps_replace_drop_and_event_type() {
        let canonical_models = Arc::new(Mutex::new(Vec::new()));
        let ((replacement, dropped), warnings) = capture_warnings(|| {
            let replacement = transform_websocket_event(
                &websocket_transform_context(
                    TransformSseEventResult::Replace {
                        events: vec![SseEvent {
                            event: "response.output_text.delta".to_owned(),
                            data: Bytes::from_static(br#"{"delta":"rewritten"}"#),
                        }],
                    },
                    Arc::clone(&canonical_models),
                ),
                "request",
                "gpt-test",
                &Bytes::from_static(br#"{"type":"response.output_text.delta","delta":"raw"}"#),
                1024,
            )
            .expect("replacement succeeds")
            .expect("replacement emits frames");
            let dropped = transform_websocket_event(
                &websocket_transform_context(
                    TransformSseEventResult::Drop,
                    Arc::clone(&canonical_models),
                ),
                "request",
                "gpt-test",
                &Bytes::from_static(br#"{"type":"response.completed"}"#),
                1024,
            )
            .expect("drop succeeds");
            (replacement, dropped)
        });
        let value: Value = sonic_rs::from_slice(&replacement[0]).expect("frame is JSON");
        assert_eq!(value["type"], "response.output_text.delta");
        assert_eq!(value["delta"], "rewritten");
        assert_eq!(dropped, Some(Vec::new()));
        assert_eq!(
            canonical_models
                .lock()
                .expect("canonical model capture lock")
                .as_slice(),
            ["gpt-test", "gpt-test"]
        );
        assert_eq!(
            warnings
                .iter()
                .filter(|warning| {
                    warning.message == UNKNOWN_ALIAS_MESSAGE
                        && warning.model.as_deref() == Some("gpt-test")
                })
                .count(),
            0,
            "native OpenAI transform models emitted alias warnings: {warnings:?}"
        );
    }

    #[test]
    fn terminal_transform_failure_advances_named_and_default_lanes() {
        for lane_id in [None, Some("main".to_owned())] {
            let mut lane = Lane {
                active: None,
                queued: VecDeque::from([11, 12]),
            };
            assert_eq!(
                ResponsesWebSocketGateway::activate_next_turn(&mut lane),
                Some(11)
            );
            assert_eq!(lane.active, Some(11));
            lane.active = None;
            assert_eq!(
                ResponsesWebSocketGateway::activate_next_turn(&mut lane),
                Some(12)
            );
            assert_eq!(lane.active, Some(12));
            assert!(lane_id.is_none() || lane_id.as_deref() == Some("main"));
        }
    }

    #[test]
    fn responses_metrics_keep_provider_neutral_timing_and_identity() {
        let mut metrics = ResponsesStreamMetrics::new();
        metrics.observe(br#"{"response":{"id":"resp_123"}}"#, 0);
        metrics.observe(br#"{"type":"response.output_text.delta","delta":"ok"}"#, 0);
        let latency = metrics.latency(101);
        assert_eq!(latency.stream_event_count, 2);
        assert!(latency.stream_first_output_ms.is_some());
        assert!(latency.stream_last_output_ms.is_some());
        assert_eq!(latency.provider_session_id.as_deref(), Some("resp_123"));
        assert_eq!(latency.sse_event_count, 0);
        assert_eq!(latency.content_delta_count, 1);
    }

    #[test]
    fn responses_metrics_bound_provider_response_ids() {
        let oversized = "x".repeat(513);
        let mut metrics = ResponsesStreamMetrics::new();
        let frame = format!(r#"{{"response":{{"id":"{oversized}"}},"response_id":"fallback"}}"#);
        metrics.observe(frame.as_bytes(), 0);
        assert_eq!(
            metrics.provider_session_id.as_deref(),
            Some("fallback"),
            "an oversized primary ID must not block a bounded fallback ID",
        );

        let mut metrics = ResponsesStreamMetrics::new();
        let frame = format!(r#"{{"response_id":"{oversized}"}}"#);
        metrics.observe(frame.as_bytes(), 0);
        assert_eq!(metrics.provider_session_id, None);

        let accepted = "y".repeat(512);
        let mut metrics = ResponsesStreamMetrics::new();
        let frame = format!(r#"{{"response":{{"id":"{accepted}"}}}}"#);
        metrics.observe(frame.as_bytes(), 0);
        assert_eq!(
            metrics.provider_session_id.as_deref(),
            Some(accepted.as_str())
        );
    }

    #[test]
    fn responses_timing_tracks_output_events_and_separated_ttfb_clock() {
        let now = std::time::Instant::now();
        let base = now
            .checked_sub(std::time::Duration::from_millis(500))
            .expect("500ms sub must not underflow");
        let mut metrics = ResponsesStreamMetrics::new_with_setup(
            base,
            Some(2),
            cc_lb_lifecycle::RequestSetupTimings::default(),
        );
        metrics.mark_sent(
            ResponsesDispatchTimings {
                connection_reused: Some(false),
                bulkhead_wait_ms: Some(5),
                dns_ms: Some(10),
                connect_ms: Some(25),
            },
            base + std::time::Duration::from_millis(45),
        );
        let t_first = base + std::time::Duration::from_millis(95);
        metrics.observe_at(
            br#"{"type":"response.created","response":{"id":"resp-native"}}"#,
            t_first,
            0,
        );
        metrics.observe_at(
            br#"{"type":"response.output_item.added"}"#,
            t_first + std::time::Duration::from_millis(4),
            0,
        );
        metrics.observe_at(
            br#"{"type":"response.output_text.delta","delta":"a"}"#,
            t_first + std::time::Duration::from_millis(10),
            0,
        );
        metrics.observe_at(
            br#"{"type":"response.function_call_arguments.delta","delta":"{\"x\":1}"}"#,
            t_first + std::time::Duration::from_millis(20),
            0,
        );
        metrics.observe_at(
            br#"{"type":"response.completed","response":{"id":"resp-native"}}"#,
            t_first + std::time::Duration::from_millis(30),
            0,
        );
        metrics.ping_count = 2;
        let latency = metrics.latency(StatusCode::SWITCHING_PROTOCOLS.as_u16());
        assert_eq!(latency.provider_session_id.as_deref(), Some("resp-native"));
        assert_eq!(latency.stream_event_count, 5);
        assert_eq!(latency.stream_first_output_ms, Some(0));
        assert_eq!(latency.stream_last_output_ms, Some(30));
        assert_eq!(latency.stream_message_start_ms, Some(0));
        assert_eq!(latency.stream_content_block_start_ms, Some(4));
        assert_eq!(latency.stream_first_content_delta_ms, Some(10));
        assert_eq!(latency.stream_last_content_delta_ms, Some(20));
        assert_eq!(latency.stream_message_stop_ms, Some(30));
        assert_eq!(latency.sse_event_count, 0);
        assert_eq!(latency.content_delta_count, 2);
        assert_eq!(latency.ping_count, 2);
        assert_eq!(latency.inter_token_avg_ms, Some(10));
        // bulkhead(5) + dns(10) + connect(25) + wait(95-45=50) == 90ms
        assert_eq!(metrics.upstream_ttfb_ms, Some(90));
        assert!(
            latency.stream_total_ms < duration_to_ms(metrics.turn_started.elapsed()),
            "relay clock ({}) must start on first provider frame, not turn admission ({})",
            latency.stream_total_ms,
            duration_to_ms(metrics.turn_started.elapsed()),
        );
    }

    #[test]
    fn sanitize_client_event_type_redacts_strips_control_and_truncates() {
        assert_eq!(
            sanitize_client_event_type("response.future_event"),
            "response.future_event"
        );
        assert_eq!(sanitize_client_event_type(""), "");
        assert_eq!(
            sanitize_client_event_type("a\nb\rc\td"),
            "a?b?c?d",
            "control characters must be replaced safely"
        );
        let overlong = "x".repeat(70);
        assert_eq!(
            sanitize_client_event_type(&overlong),
            format!("{}...", "x".repeat(64))
        );
        let bearer_secret = sanitize_client_event_type("Bearer super-secret-token-1234567890");
        assert!(
            !bearer_secret.contains("super-secret-token-1234567890"),
            "bearer secret must be redacted: {bearer_secret}"
        );
        assert!(bearer_secret.contains("[REDACTED]"));
        let api_key_secret =
            sanitize_client_event_type("sk-ant-abcdefghijklmnopqrstuvwxyz1234567890");
        assert!(
            !api_key_secret.contains("abcdefghijklmnopqrstuvwxyz"),
            "API key secret must be redacted: {api_key_secret}"
        );
        assert!(api_key_secret.contains("[REDACTED]"));
    }

    #[tokio::test]
    async fn provider_stream_and_ping_emit_throttled_request_activity() {
        use cc_lb_control::RequestEventBus;
        let fixture = DirectAccountingFixture::new().await;
        let bus = Arc::new(cc_lb_control::InMemoryBus::new());
        let mut events = bus.subscribe_lifecycle();
        let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let observer = LifecycleContext::new("ws-activity".to_owned(), bus, &clock);
        observer.mark_authn_reached();
        let expected_event_id = observer.event_id().to_owned();
        let mut gateway = terminal_fixture_gateway(&fixture, Some(observer));

        let aged_65s = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_secs(65))
            .expect("65s sub must not underflow");
        {
            let metrics = &mut gateway.turns.get_mut(&1).unwrap().metrics;
            metrics.turn_started = aged_65s;
            metrics.last_activity_heartbeat_at = aged_65s;
        }
        let created =
            Bytes::from_static(br#"{"type":"response.created","response":{"id":"resp_fixture"}}"#);
        gateway.provider_message(1, &created).await.unwrap();
        // Immediate ping inside the same 60s window must be throttled.
        gateway.observe_provider_ping(1);

        let aged_130s = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_secs(130))
            .expect("130s sub must not underflow");
        {
            let metrics = &mut gateway.turns.get_mut(&1).unwrap().metrics;
            metrics.last_activity_heartbeat_at = aged_130s;
        }
        gateway.observe_provider_ping(1);

        let mut activity_count = 0usize;
        while let Ok(event) = events.try_recv() {
            if let LifecycleEvent::RequestActivity { event_id } = event {
                assert_eq!(event_id, expected_event_id);
                activity_count += 1;
            }
        }
        assert_eq!(
            activity_count, 2,
            "expected one throttled activity from frame and one from later ping"
        );
    }

    #[tokio::test]
    async fn short_single_frame_turn_emits_no_request_activity() {
        use cc_lb_control::RequestEventBus;
        let fixture = DirectAccountingFixture::new().await;
        let bus = Arc::new(cc_lb_control::InMemoryBus::new());
        let mut events = bus.subscribe_lifecycle();
        let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let observer = LifecycleContext::new("ws-short".to_owned(), bus, &clock);
        observer.mark_authn_reached();
        let mut gateway = terminal_fixture_gateway(&fixture, Some(observer));

        let completed = Bytes::from_static(
            br#"{"type":"response.completed","response":{"id":"resp_fixture","usage":{"input_tokens":5,"output_tokens":3}}}"#,
        );
        gateway.provider_message(1, &completed).await.unwrap();
        gateway.observe_provider_ping(1);

        while let Ok(event) = events.try_recv() {
            assert!(
                !matches!(event, LifecycleEvent::RequestActivity { .. }),
                "short turn must not emit RequestActivity before heartbeat interval"
            );
        }
    }

    #[tokio::test]
    async fn silent_active_turn_heartbeat_tracks_turn_lifecycle() {
        use cc_lb_control::RequestEventBus;
        let fixture = DirectAccountingFixture::new().await;
        let bus = Arc::new(cc_lb_control::InMemoryBus::new());
        let mut events = bus.subscribe_lifecycle();
        let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let observer = LifecycleContext::new("ws-silent".to_owned(), bus, &clock);
        observer.mark_authn_reached();
        let expected_event_id = observer.event_id().to_owned();
        let mut gateway = terminal_fixture_gateway(&fixture, Some(observer));
        assert!(gateway.has_active_turns());

        let aged_65s = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_secs(65))
            .expect("65s sub must not underflow");
        {
            let metrics = &mut gateway.turns.get_mut(&1).unwrap().metrics;
            metrics.turn_started = aged_65s;
            metrics.last_activity_heartbeat_at = aged_65s;
        }
        gateway.observe_active_turns_heartbeat();
        // Second immediate tick inside same 60s window must be throttled.
        gateway.observe_active_turns_heartbeat();

        let mut activity_count = 0usize;
        while let Ok(event) = events.try_recv() {
            if let LifecycleEvent::RequestActivity { event_id } = event {
                assert_eq!(event_id, expected_event_id);
                activity_count += 1;
            }
        }
        assert_eq!(activity_count, 1);

        let completed = Bytes::from_static(
            br#"{"type":"response.completed","response":{"id":"resp_fixture","usage":{"input_tokens":5,"output_tokens":3}}}"#,
        );
        gateway.provider_message(1, &completed).await.unwrap();
        assert!(!gateway.has_active_turns());
    }

    #[tokio::test]
    async fn error_followed_by_response_failed_forwards_both_once_and_settles_once() {
        use cc_lb_control::RequestEventBus;

        let fixture = DirectAccountingFixture::new().await;
        let bus = Arc::new(cc_lb_control::InMemoryBus::new());
        let mut events = bus.subscribe_lifecycle();
        let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let observer = LifecycleContext::new("ws-err-failed".to_owned(), bus, &clock);
        observer.mark_authn_reached();
        let mut gateway = terminal_fixture_gateway(&fixture, Some(observer));
        gateway.turns.get_mut(&1).unwrap().response_id = None;

        let created = Bytes::from_static(
            br#"{"type":"response.created","sequence_number":0,"response":{"id":"resp_overload","object":"response","status":"in_progress","store":false,"output":[]}}"#,
        );
        let res_created = gateway.provider_message(1, &created).await.unwrap();
        assert!(res_created.forward);

        let in_progress = Bytes::from_static(
            br#"{"type":"response.in_progress","sequence_number":2,"response":{"id":"resp_overload","object":"response","status":"in_progress","store":false,"output":[]}}"#,
        );
        assert!(
            gateway
                .provider_message(1, &in_progress)
                .await
                .unwrap()
                .forward
        );

        let err_frame = Bytes::from_static(
            br#"{"type":"error","sequence_number":3,"response_id":"resp_overload","error":{"message":"Our servers are currently overloaded. Please try again later.","type":"service_unavailable_error","param":null,"code":"server_is_overloaded","headers":{"retry-after":"360","retry-after-ms":"360000"}}}"#,
        );
        let res_err = gateway.provider_message(1, &err_frame).await.unwrap();
        assert!(res_err.forward, "in-response error frame must forward");
        assert!(!gateway.has_active_turns(), "turn finishes once on error");

        let timing = Bytes::from_static(
            br#"{"type":"responsesapi.websocket_timing","timing_metrics":{"execute_duration_ms":42}}"#,
        );
        assert!(gateway.provider_message(1, &timing).await.unwrap().forward);

        let failed_frame = Bytes::from_static(
            br#"{"type":"response.failed","sequence_number":4,"response":{"id":"resp_overload","object":"response","status":"failed","output":[],"error":{"type":"service_unavailable_error","param":null,"code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later."}}}"#,
        );
        let res_failed = gateway.provider_message(1, &failed_frame).await.unwrap();
        assert!(
            res_failed.forward,
            "follow-up response.failed after in-response error must forward once"
        );
        assert!(res_failed.errors.is_empty());

        let duplicate_failed = gateway.provider_message(1, &failed_frame).await.unwrap();
        assert!(
            !duplicate_failed.forward,
            "duplicate response.failed must remain suppressed"
        );
        let duplicate_err = gateway.provider_message(1, &err_frame).await.unwrap();
        assert!(
            !duplicate_err.forward,
            "duplicate error after response.failed must remain suppressed"
        );

        let mut completed_count = 0usize;
        let mut terminated_count = 0usize;
        while let Ok(event) = events.try_recv() {
            match event {
                LifecycleEvent::StreamCompleted { result, .. } => {
                    completed_count += 1;
                    let stream_err = result.expect_err("overloaded error settles as stream error");
                    assert_eq!(stream_err.error_type, "server_is_overloaded");
                    assert_eq!(
                        stream_err.error_message,
                        "Our servers are currently overloaded. Please try again later."
                    );
                }
                LifecycleEvent::RequestTerminated {
                    reason,
                    internal_errors,
                    ..
                } => {
                    terminated_count += 1;
                    assert_eq!(
                        reason,
                        cc_lb_lifecycle::TerminationReason::ErrorCode(
                            "upstream_stream_error".to_owned()
                        )
                    );
                    assert!(
                        internal_errors.is_empty(),
                        "upstream error must not emit Relay InternalFailure: {internal_errors:?}"
                    );
                }
                _ => {}
            }
        }
        assert_eq!(completed_count, 1, "turn must settle exactly once");
        assert_eq!(terminated_count, 1, "turn must terminate exactly once");
    }

    #[tokio::test]
    async fn statusless_error_followed_by_connection_closed_emits_close_error_without_double_settlement()
     {
        use cc_lb_control::RequestEventBus;

        let fixture = DirectAccountingFixture::new().await;
        let bus = Arc::new(cc_lb_control::InMemoryBus::new());
        let mut events = bus.subscribe_lifecycle();
        let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let observer = LifecycleContext::new("ws-statusless-close".to_owned(), bus, &clock);
        observer.mark_authn_reached();
        let mut gateway = terminal_fixture_gateway(&fixture, Some(observer));
        gateway.turns.get_mut(&1).unwrap().response_id = None;

        let created = Bytes::from_static(
            br#"{"type":"response.created","response":{"id":"resp_statusless","status":"in_progress","store":false}}"#,
        );
        assert!(gateway.provider_message(1, &created).await.unwrap().forward);

        let statusless_err = Bytes::from_static(
            br#"{"type":"error","response_id":"resp_statusless","error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"Overloaded"}}"#,
        );
        assert!(
            gateway
                .provider_message(1, &statusless_err)
                .await
                .unwrap()
                .forward
        );

        let closed = gateway.connection_closed(1, "provider closed early");
        assert_eq!(
            closed.errors.len(),
            1,
            "premature close after statusless error must emit one synthetic close error"
        );
        assert_eq!(closed.errors[0].status, 502);
        assert_eq!(closed.errors[0].code, "upstream_connection_closed");

        let mut terminated = 0usize;
        while let Ok(event) = events.try_recv() {
            if matches!(event, LifecycleEvent::RequestTerminated { .. }) {
                terminated += 1;
            }
        }
        assert_eq!(terminated, 1, "connection_closed must not double-settle");
    }

    #[test]
    fn native_terminal_error_extracts_top_level_error_fields_and_scrubs_secrets() {
        use cc_lb_signer_anthropic_key::{ProviderKeyKind, ProviderKeySigner};

        let overload = native_terminal_error_with_signer(
            &serde_json::json!({
                "type": "error",
                "response_id": "resp_1",
                "error": {
                    "type": "service_unavailable_error",
                    "code": "server_is_overloaded",
                    "message": "Our servers are currently overloaded."
                }
            }),
            &Some("main".to_owned()),
            None,
        );
        assert_eq!(overload.status, 503);
        assert_eq!(overload.code, "upstream_stream_error");
        assert_eq!(
            overload.native_code.as_deref(),
            Some("server_is_overloaded")
        );
        assert_eq!(overload.message, "Our servers are currently overloaded.");
        assert_eq!(overload.stream_id.as_deref(), Some("main"));

        let secret = "opaque_custom_err_secret_123";
        let signer = ProviderKeySigner::new(ProviderKeyKind::Openai, secret).unwrap();
        let scrubbed = native_terminal_error_with_signer(
            &serde_json::json!({
                "type": "error",
                "status": 400,
                "error": {
                    "type": "invalid_request_error",
                    "message": format!("rejected {secret}")
                }
            }),
            &None,
            Some(&signer),
        );
        assert_eq!(scrubbed.status, 400);
        assert_eq!(
            scrubbed.native_code.as_deref(),
            Some("invalid_request_error")
        );
        assert!(!scrubbed.message.contains(secret));
        assert!(scrubbed.message.contains("[REDACTED]"));

        let (bare_err, _) = classify_terminal_frame(
            "error",
            &serde_json::json!({"type": "error", "code": "bad_gateway", "message": "upstream err"}),
            &None,
            None,
        );
        let bare_err = bare_err.expect("bare error produces WebSocketError");
        assert_eq!(bare_err.native_code.as_deref(), Some("bad_gateway"));
        assert_eq!(bare_err.message, "upstream err");

        let (fallback_err, _) =
            classify_terminal_frame("error", &serde_json::json!({"type": "error"}), &None, None);
        assert_eq!(
            fallback_err.unwrap().native_code.as_deref(),
            Some("error"),
            "missing native code must fall back to terminal frame kind"
        );
    }

    #[tokio::test]
    async fn oauth_public_frame_restores_stream_id_and_metadata_on_followup_response_failed() {
        let fixture = DirectAccountingFixture::new().await;
        let mut gateway = terminal_fixture_gateway(&fixture, None);
        gateway.lanes.remove(&None);
        gateway.lanes.insert(
            Some("lane-pub".to_owned()),
            Lane {
                active: Some(1),
                ..Default::default()
            },
        );
        gateway
            .connection_lanes
            .insert(1, Some("lane-pub".to_owned()));
        {
            let turn = gateway.turns.get_mut(&1).expect("fixture turn");
            turn.lane = Some("lane-pub".to_owned());
            turn.response_id = None;
            turn.prepared.oauth_transport = true;
            turn.prepared.oauth_compat = true;
            turn.echo_metadata = Some(serde_json::json!({"trace": "kept"}));
        }

        let created = Bytes::from_static(
            br#"{"type":"response.created","response":{"id":"resp_pub","status":"in_progress"}}"#,
        );
        let res_created = gateway.provider_message(1, &created).await.unwrap();
        let created_out: Value =
            sonic_rs::from_slice(res_created.frames.unwrap().first().unwrap()).unwrap();
        assert_eq!(created_out["stream_id"], "lane-pub");
        assert_eq!(created_out["response"]["metadata"]["trace"], "kept");

        let err_frame = Bytes::from_static(
            br#"{"type":"error","response_id":"resp_pub","error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"overloaded"}}"#,
        );
        let res_err = gateway.provider_message(1, &err_frame).await.unwrap();
        let err_out: Value =
            sonic_rs::from_slice(res_err.frames.unwrap().first().unwrap()).unwrap();
        assert_eq!(err_out["stream_id"], "lane-pub");
        assert!(!gateway.has_active_turns());

        let failed_frame = Bytes::from_static(
            br#"{"type":"response.failed","response":{"id":"resp_pub","status":"failed","error":{"code":"server_is_overloaded","message":"overloaded"}}}"#,
        );
        let res_failed = gateway.provider_message(1, &failed_frame).await.unwrap();
        assert!(!res_failed.forward);
        let failed_frames = res_failed
            .frames
            .expect("public compat rewrites failed frame");
        let failed_out: Value = sonic_rs::from_slice(&failed_frames[0]).unwrap();
        assert_eq!(failed_out["type"], "response.failed");
        assert_eq!(failed_out["stream_id"], "lane-pub");
        assert_eq!(failed_out["response"]["metadata"]["trace"], "kept");
    }

    #[tokio::test]
    async fn connection_closed_clears_session_upstream_when_unused_but_keeps_session_oauth_rejection()
     {
        let fixture = DirectAccountingFixture::new().await;
        let mut gateway = terminal_fixture_gateway(&fixture, None);
        let upstream_a = Uuid::from_u128(0xAA01);
        gateway.session_oauth = true;
        gateway.session_upstream = Some(upstream_a);
        gateway.connections.insert(1, upstream_a);
        gateway.connections.insert(2, upstream_a);
        gateway.connection_lanes.insert(1, Some("a".to_owned()));
        gateway.connection_lanes.insert(2, Some("b".to_owned()));
        gateway.turns.clear();
        gateway.lanes.clear();

        // Closing one of two connections still using upstream_a keeps session_upstream pinned.
        gateway.connection_closed(1, "lane a closed");
        assert_eq!(gateway.session_upstream, Some(upstream_a));
        assert!(gateway.session_oauth);

        // Closing the final connection clears session_upstream while preserving session_oauth.
        gateway.connection_closed(2, "lane b closed");
        assert_eq!(gateway.session_upstream, None);
        assert!(
            gateway.session_oauth,
            "session_oauth must remain true so stale previous_response_id still returns 400"
        );

        let err = gateway
            .client_message(
                fixture.auth.clone(),
                Bytes::from_static(
                    br#"{"type":"response.create","model":"gpt-test","previous_response_id":"resp_dead","store":false,"input":[]}"#,
                ),
            )
            .await
            .expect_err("stale previous_response_id on OAuth WS must still fail closed");
        assert_eq!(err.status, 400);
        assert_eq!(err.code, "previous_response_not_found");
    }

    fn make_synthetic_turn(
        fixture: &DirectAccountingFixture,
        observer: Option<LifecycleContext>,
        lane: Option<String>,
        sent: bool,
        oauth_transport: bool,
        explicit_store: Option<bool>,
        mut plugin_request: Option<(Uuid, WebSocketPluginRequest)>,
    ) -> Turn {
        let sent_service_tier = plugin_request
            .as_mut()
            .and_then(|(_, req)| req.sent_service_tier.take());
        Turn {
            auth: fixture.auth.clone(),
            prepared_signers: HashMap::new(),
            initial_selected_upstream_id: Some(Uuid::nil()),
            candidate_oauth: HashMap::new(),
            prepared: PreparedWebSocketTurn {
                oauth_compat: false,
                oauth_transport,
                explicit_store,
                candidates: Vec::new(),
                observer,
                completion_observer: Arc::new(crate::completion_observer::CompletionObserver::new()),
                prompt_cache_observation_context: None,
                reservation: None,
                deferred_limit_context: None,
                limit_engine: None,
                principal_id: DIRECT_PRINCIPAL.to_owned(),
                key_id: fixture.record.key_hash_b64.clone(),
                model: "gpt-test".to_owned(),
                store: explicit_store.unwrap_or(!oauth_transport),
                span: Span::none(),
                setup_timings: Default::default(),
                route_info: serde_json::from_value(serde_json::json!({
                    "upstream_id": Uuid::nil(), "upstream_name": "assigned", "model": "gpt-test"
                }))
                .unwrap(),
                request_id: "synthetic-turn".to_owned(),
                unknown_reasoning_plan: WsUnknownReasoningPlan::None,
                has_explicit_affinity: false,
                owner_failover_fallback: None,
                reconciled_body: None,
            },
            body: Arc::new(serde_json::json!({})),
            frame: Bytes::new(),
            lane,
            connection_id: sent.then_some(1),
            upstream_id: Some(Uuid::nil()),
            pinned: None,
            response_id: None,
            previous_response_id: None,
            steer_parent: None,
            steer_submissions: 0,
            accepted_steers: HashSet::new(),
            controls: VecDeque::new(),
            sent,
            attempted: sent,
            attempt_num: u32::from(sent),
            charged_bytes: 0,
            usage: None,
            usage_complete: false,
            incomplete_reason: None,
            sent_service_tier,
            plugin_request,
            echo_metadata: None,
            metrics: ResponsesStreamMetrics::new(),
            funding_ambiguous: false,
            response_created: false,
            provider_frame_forwarded: false,
            unknown_reasoning_retry: None,
            unknown_reasoning_retried: false,
            pending_affinity_keys: Vec::new(),
            deferred_unestablished: None,
            has_any_oauth_candidate: false,
            all_candidates_oauth: false,
            admitted_session_upstream: None,
            owner_failover_replacement: false,
            owner_failover_requires_api_key: false,
            owner_failover_fallback: None,
            live_failover_candidates: Vec::new(),
            suppressed_failover_error: None,
        }
    }

    #[tokio::test]
    async fn pending_error_survives_queued_successor_binding_and_settles_response_failed_usage() {
        let fixture = DirectAccountingFixture::new().await;
        let bus = Arc::new(cc_lb_control::InMemoryBus::new());
        let mut events = bus.attach_lifecycle_assembler(32);
        let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);

        let obs_a = LifecycleContext::new("turn-a".to_owned(), bus.clone(), &clock);
        obs_a.mark_authn_reached();
        let event_a = obs_a.event_id().to_owned();
        let obs_b = LifecycleContext::new("turn-b".to_owned(), bus, &clock);
        obs_b.mark_authn_reached();
        let event_b = obs_b.event_id().to_owned();

        let mut gateway = terminal_fixture_gateway(&fixture, Some(obs_a));
        gateway.connections.insert(1, Uuid::nil());
        gateway.connection_lanes.insert(1, None);
        gateway.turns.get_mut(&1).unwrap().response_id = None;

        let mut turn_b =
            make_synthetic_turn(&fixture, Some(obs_b), None, false, false, Some(false), None);
        turn_b.connection_id = None;
        gateway.turns.insert(2, turn_b);
        gateway.lanes.get_mut(&None).unwrap().queued.push_back(2);

        let created_a = Bytes::from_static(
            br#"{"type":"response.created","response":{"id":"resp_a","status":"in_progress","store":false}}"#,
        );
        assert!(
            gateway
                .provider_message(1, &created_a)
                .await
                .unwrap()
                .forward
        );

        // Turn A gets `error` without usage -> parks in `pending_error_terminals` and dispatches Turn B.
        let err_a = Bytes::from_static(
            br#"{"type":"error","response_id":"resp_a","status":503,"error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"Overloaded"}}"#,
        );
        let res_err_a = gateway.provider_message(1, &err_a).await.unwrap();
        assert!(res_err_a.forward);
        assert_eq!(res_err_a.dispatches.len(), 1);
        assert_eq!(res_err_a.dispatches[0].turn_id, 2);

        // Turn B sends and binds `resp_b` BEFORE Turn A's delayed `response.failed` arrives.
        gateway.assign_connection(2, 1, Uuid::nil(), "assigned");
        gateway.sent(2);
        let created_b = Bytes::from_static(
            br#"{"type":"response.created","response":{"id":"resp_b","status":"in_progress","store":false}}"#,
        );
        assert!(
            gateway
                .provider_message(1, &created_b)
                .await
                .unwrap()
                .forward
        );
        assert!(
            gateway.owners.contains_key("resp_a"),
            "binding Turn B must not evict pending Turn A owner"
        );

        // Turn A's delayed `response.failed` arrives carrying measured usage.
        let failed_a = Bytes::from_static(
            br#"{"type":"response.failed","response":{"id":"resp_a","status":"failed","error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"Overloaded"},"usage":{"input_tokens":42,"output_tokens":7}}}"#,
        );
        let res_failed_a = gateway.provider_message(1, &failed_a).await.unwrap();
        assert!(
            res_failed_a.forward,
            "delayed response.failed for Turn A must forward after Turn B bound resp_b"
        );
        assert!(res_failed_a.errors.is_empty());
        assert!(
            !gateway.owners.contains_key("resp_a"),
            "Turn A owner must be collected once response.failed settles"
        );

        // Duplicate `response.failed` and `error` for retired Turn A must suppress cleanly without
        // falling back to active Turn B or closing B's provider connection.
        let dup_failed_a = gateway.provider_message(1, &failed_a).await.unwrap();
        assert!(!dup_failed_a.forward && dup_failed_a.errors.is_empty());
        let dup_err_a = gateway.provider_message(1, &err_a).await.unwrap();
        assert!(!dup_err_a.forward && dup_err_a.errors.is_empty());

        // Turn B completes normally without interference.
        let completed_b = Bytes::from_static(
            br#"{"type":"response.completed","response":{"id":"resp_b","status":"completed","usage":{"input_tokens":10,"output_tokens":2}}}"#,
        );
        let res_completed_b = gateway.provider_message(1, &completed_b).await.unwrap();
        assert!(res_completed_b.forward);

        let mut usage_by_event = HashMap::new();
        let mut completions = HashMap::new();
        while let Ok(event) = events.try_recv() {
            match event {
                LifecycleEvent::UsageObserved {
                    event_id, usage, ..
                } => {
                    usage_by_event.insert(event_id, (usage.input_tokens, usage.output_tokens));
                }
                LifecycleEvent::StreamCompleted {
                    event_id, result, ..
                } => {
                    completions.insert(event_id, result.is_ok());
                }
                _ => {}
            }
        }
        assert_eq!(usage_by_event.get(&event_a), Some(&(42, 7)));
        assert_eq!(usage_by_event.get(&event_b), Some(&(10, 2)));
        assert_eq!(completions.get(&event_a), Some(&false));
        assert_eq!(completions.get(&event_b), Some(&true));
    }

    #[tokio::test]
    async fn pending_error_terminal_expires_after_bounded_window_and_suppresses_late_failed() {
        use cc_lb_control::RequestEventBus;

        let fixture = DirectAccountingFixture::new().await;
        let bus = Arc::new(cc_lb_control::InMemoryBus::new());
        let mut events = bus.subscribe_lifecycle();
        let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let observer = LifecycleContext::new("turn-expire".to_owned(), bus, &clock);
        observer.mark_authn_reached();

        let mut gateway = terminal_fixture_gateway(&fixture, Some(observer));
        gateway.connections.insert(1, Uuid::nil());
        gateway.connection_lanes.insert(1, None);
        gateway.turns.get_mut(&1).unwrap().response_id = None;
        gateway.turns.get_mut(&1).unwrap().charged_bytes = 256;
        gateway.used_bytes = 256;

        let created = Bytes::from_static(
            br#"{"type":"response.created","response":{"id":"resp_exp","status":"in_progress","store":false}}"#,
        );
        assert!(gateway.provider_message(1, &created).await.unwrap().forward);
        let err_frame = Bytes::from_static(
            br#"{"type":"error","response_id":"resp_exp","status":503,"error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"Overloaded"}}"#,
        );
        assert!(
            gateway
                .provider_message(1, &err_frame)
                .await
                .unwrap()
                .forward
        );
        let deadline = gateway
            .next_pending_error_deadline()
            .expect("pending error exposes bounded expiration deadline");

        gateway.expire_pending_error_terminals(deadline + std::time::Duration::from_secs(1));
        assert!(gateway.next_pending_error_deadline().is_none());

        // Late `response.failed` after expiry must be suppressed without double settlement.
        let late_failed = Bytes::from_static(
            br#"{"type":"response.failed","response":{"id":"resp_exp","status":"failed","error":{"code":"server_is_overloaded","message":"Overloaded"}}}"#,
        );
        let res = gateway.provider_message(1, &late_failed).await.unwrap();
        assert!(!res.forward);

        let mut terminated = 0usize;
        while let Ok(event) = events.try_recv() {
            if matches!(event, LifecycleEvent::RequestTerminated { .. }) {
                terminated += 1;
            }
        }
        assert_eq!(terminated, 1);
    }

    #[tokio::test]
    async fn pending_error_terminal_runs_original_hook_and_isolates_successor_hook() {
        struct CountingDialect {
            result: Result<TransformSseEventResult, &'static str>,
            calls: Arc<Mutex<Vec<String>>>,
        }
        impl cc_lb_upstream::UpstreamDialect for CountingDialect {
            fn shape(
                &self,
                _context: &cc_lb_upstream::DialectShapeContext,
                _upstream: &cc_lb_domain::Upstream,
                _principal: &cc_lb_domain::Principal,
                _builder: &mut cc_lb_upstream::ShapedRequestBuilder,
            ) -> Result<cc_lb_upstream::ShapedRequest, cc_lb_upstream::DialectError> {
                unreachable!("not used")
            }

            fn sse_event_transform_hook(
                &self,
            ) -> Option<&dyn cc_lb_upstream::SseEventTransformHook> {
                Some(self)
            }
        }
        impl cc_lb_upstream::SseEventTransformHook for CountingDialect {
            fn transform_sse_event(
                &self,
                request: cc_lb_upstream::TransformSseEventRequest,
            ) -> Result<TransformSseEventResult, cc_lb_upstream::ResponseTransformError>
            {
                self.calls
                    .lock()
                    .expect("hook calls lock")
                    .push(request.event.event);
                match &self.result {
                    Ok(res) => Ok(res.clone()),
                    Err(msg) => Err(cc_lb_upstream::ResponseTransformError::Runtime {
                        reason: (*msg).to_owned(),
                    }),
                }
            }
        }

        let make_plugin_req = |result: Result<TransformSseEventResult, &'static str>,
                               calls: Arc<Mutex<Vec<String>>>| {
            Some((
                Uuid::nil(),
                WebSocketPluginRequest {
                    frame: Bytes::new(),
                    headers: HeaderMap::new(),
                    context: ResponseTransformContext {
                        principal: cc_lb_domain::Principal {
                            id: DIRECT_PRINCIPAL.to_owned(),
                            kind: cc_lb_domain::PrincipalKind::InternalKey,
                        },
                        upstream: cc_lb_domain::Upstream::OpenaiDirect { base_url: None },
                        request_method: http::Method::POST,
                        request_path: "/v1/responses".to_owned(),
                        dialect: Arc::new(CountingDialect { result, calls }),
                        gemini_oauth: false,
                        vertex: false,
                        codex_backend: false,
                        vertex_location: None,
                    },
                    oauth_compat: false,
                    echo_metadata: None,
                    sent_service_tier: None,
                },
            ))
        };

        // Case 1: Original hook `Replace` applies to delayed `response.failed`; successor hook not invoked.
        // Case 2: Original hook `Drop` drops delayed `response.failed`.
        // Case 3: Original hook `Err` settles pending Turn A with error without killing active Turn B.
        for (case_idx, hook_result) in [
            Ok(TransformSseEventResult::Replace {
                events: vec![SseEvent {
                    event: "response.failed".to_owned(),
                    data: Bytes::from_static(br#"{"rewritten":true}"#),
                }],
            }),
            Ok(TransformSseEventResult::Drop),
            Err("hook_rejected_failed"),
        ]
        .into_iter()
        .enumerate()
        {
            let fixture = DirectAccountingFixture::new().await;
            let mut gateway = ResponsesWebSocketGateway::new(
                fixture.lifecycle.clone(),
                &fixture.auth,
                HeaderMap::new(),
                65_536,
                8,
            );
            gateway.connections.insert(1, Uuid::nil());
            gateway.connection_lanes.insert(1, None);
            let calls_a = Arc::new(Mutex::new(Vec::new()));
            let calls_b = Arc::new(Mutex::new(Vec::new()));

            gateway.turns.insert(
                1,
                make_synthetic_turn(
                    &fixture,
                    None,
                    None,
                    true,
                    false,
                    Some(false),
                    make_plugin_req(hook_result, calls_a.clone()),
                ),
            );
            gateway.turns.insert(
                2,
                make_synthetic_turn(
                    &fixture,
                    None,
                    None,
                    false,
                    false,
                    Some(false),
                    make_plugin_req(Ok(TransformSseEventResult::Unchanged), calls_b.clone()),
                ),
            );
            gateway.lanes.insert(
                None,
                Lane {
                    active: Some(1),
                    queued: VecDeque::from([2]),
                },
            );

            let created_a = Bytes::from_static(
                br#"{"type":"response.created","response":{"id":"resp_a","status":"in_progress","store":false}}"#,
            );
            assert!(
                !gateway
                    .provider_message(1, &created_a)
                    .await
                    .unwrap()
                    .errors
                    .is_empty()
                    == (case_idx == 2)
            );
            if case_idx == 2 {
                // `Err` hook would have failed `created_a` above if installed upfront; install hook
                // after `created_a` completes in a clean gateway for Case 3 below.
                continue;
            }
            let err_a = Bytes::from_static(
                br#"{"type":"error","response_id":"resp_a","status":503,"error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"Overloaded"}}"#,
            );
            let res_err = gateway.provider_message(1, &err_a).await.unwrap();
            assert_eq!(res_err.dispatches[0].turn_id, 2);
            gateway.assign_connection(2, 1, Uuid::nil(), "assigned");
            gateway.sent(2);
            let created_b = Bytes::from_static(
                br#"{"type":"response.created","response":{"id":"resp_b","status":"in_progress","store":false}}"#,
            );
            let _ = gateway.provider_message(1, &created_b).await.unwrap();
            let calls_b_before = calls_b.lock().unwrap().len();

            let failed_a = Bytes::from_static(
                br#"{"type":"response.failed","response":{"id":"resp_a","status":"failed"}}"#,
            );
            let res_failed = gateway.provider_message(1, &failed_a).await.unwrap();
            assert_eq!(
                calls_b.lock().unwrap().len(),
                calls_b_before,
                "successor hook must never run for Turn A's delayed response.failed"
            );
            assert!(
                calls_a
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|ev| ev == "response.failed"),
                "original hook must inspect Turn A's delayed response.failed"
            );
            if case_idx == 0 {
                let frames = res_failed
                    .frames
                    .expect("Replace hook emits replaced frame");
                let out: Value = sonic_rs::from_slice(&frames[0]).unwrap();
                assert_eq!(out["rewritten"], true);
            } else {
                assert_eq!(
                    res_failed.frames,
                    Some(Vec::new()),
                    "Drop hook suppresses frame"
                );
            }
        }

        // Verify Case 3 separately where Turn A's hook errors specifically on `response.failed`.
        struct FailOnResponseFailedDialect(Arc<Mutex<Vec<String>>>);
        impl cc_lb_upstream::UpstreamDialect for FailOnResponseFailedDialect {
            fn shape(
                &self,
                _context: &cc_lb_upstream::DialectShapeContext,
                _upstream: &cc_lb_domain::Upstream,
                _principal: &cc_lb_domain::Principal,
                _builder: &mut cc_lb_upstream::ShapedRequestBuilder,
            ) -> Result<cc_lb_upstream::ShapedRequest, cc_lb_upstream::DialectError> {
                unreachable!("not used")
            }

            fn sse_event_transform_hook(
                &self,
            ) -> Option<&dyn cc_lb_upstream::SseEventTransformHook> {
                Some(self)
            }
        }
        impl cc_lb_upstream::SseEventTransformHook for FailOnResponseFailedDialect {
            fn transform_sse_event(
                &self,
                request: cc_lb_upstream::TransformSseEventRequest,
            ) -> Result<TransformSseEventResult, cc_lb_upstream::ResponseTransformError>
            {
                self.0.lock().unwrap().push(request.event.event.clone());
                if request.event.event == "response.failed" {
                    Err(cc_lb_upstream::ResponseTransformError::Runtime {
                        reason: "failed hook".to_owned(),
                    })
                } else {
                    Ok(TransformSseEventResult::Unchanged)
                }
            }
        }

        let fixture = DirectAccountingFixture::new().await;
        let mut gateway = ResponsesWebSocketGateway::new(
            fixture.lifecycle.clone(),
            &fixture.auth,
            HeaderMap::new(),
            65_536,
            8,
        );
        gateway.connections.insert(1, Uuid::nil());
        gateway.connection_lanes.insert(1, None);
        let calls_a = Arc::new(Mutex::new(Vec::new()));
        let calls_b = Arc::new(Mutex::new(Vec::new()));
        let plugin_a = Some((
            Uuid::nil(),
            WebSocketPluginRequest {
                frame: Bytes::new(),
                headers: HeaderMap::new(),
                context: ResponseTransformContext {
                    principal: cc_lb_domain::Principal {
                        id: DIRECT_PRINCIPAL.to_owned(),
                        kind: cc_lb_domain::PrincipalKind::InternalKey,
                    },
                    upstream: cc_lb_domain::Upstream::OpenaiDirect { base_url: None },
                    request_method: http::Method::POST,
                    request_path: "/v1/responses".to_owned(),
                    dialect: Arc::new(FailOnResponseFailedDialect(calls_a.clone())),
                    gemini_oauth: false,
                    vertex: false,
                    codex_backend: false,
                    vertex_location: None,
                },
                oauth_compat: false,
                echo_metadata: None,
                sent_service_tier: None,
            },
        ));
        gateway.turns.insert(
            1,
            make_synthetic_turn(&fixture, None, None, true, false, Some(false), plugin_a),
        );
        gateway.turns.insert(
            2,
            make_synthetic_turn(
                &fixture,
                None,
                None,
                false,
                false,
                Some(false),
                make_plugin_req(Ok(TransformSseEventResult::Unchanged), calls_b),
            ),
        );
        gateway.lanes.insert(
            None,
            Lane {
                active: Some(1),
                queued: VecDeque::from([2]),
            },
        );
        let _ = gateway
            .provider_message(
                1,
                &Bytes::from_static(
                    br#"{"type":"response.created","response":{"id":"resp_a","status":"in_progress","store":false}}"#,
                ),
            )
            .await
            .unwrap();
        let _ = gateway
            .provider_message(
                1,
                &Bytes::from_static(
                    br#"{"type":"error","response_id":"resp_a","status":503,"error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"Overloaded"}}"#,
                ),
            )
            .await
            .unwrap();
        gateway.assign_connection(2, 1, Uuid::nil(), "assigned");
        gateway.sent(2);
        let _ = gateway
            .provider_message(
                1,
                &Bytes::from_static(
                    br#"{"type":"response.created","response":{"id":"resp_b","status":"in_progress","store":false}}"#,
                ),
            )
            .await
            .unwrap();
        let err_res = gateway
            .provider_message(
                1,
                &Bytes::from_static(
                    br#"{"type":"response.failed","response":{"id":"resp_a","status":"failed"}}"#,
                ),
            )
            .await
            .unwrap();
        assert!(!err_res.forward);
        assert_eq!(err_res.errors.len(), 1);
        assert!(
            gateway.turns.contains_key(&2),
            "Turn A response.failed hook error must not terminate active Turn B"
        );
    }

    #[tokio::test]
    async fn assign_connection_derives_credential_kind_and_store_from_connected_candidate() {
        let fixture = DirectAccountingFixture::new().await;
        let oauth_id = Uuid::from_u128(0x0A01);
        let api_key_id = Uuid::from_u128(0xAB01);
        let mut gateway = ResponsesWebSocketGateway::new(
            fixture.lifecycle.clone(),
            &fixture.auth,
            HeaderMap::new(),
            65_536,
            8,
        );

        let mut turn1 = make_synthetic_turn(&fixture, None, None, false, true, None, None);
        turn1.initial_selected_upstream_id = Some(oauth_id);
        turn1.candidate_oauth = HashMap::from([(oauth_id, true), (api_key_id, false)]);
        turn1.prepared.candidates = vec![
            WebSocketCandidate {
                upstream_id: oauth_id,
                upstream_name: "oauth-primary".to_owned(),
                socket_per_lane: true,
                is_api_key: false,
            },
            WebSocketCandidate {
                upstream_id: api_key_id,
                upstream_name: "api-key-fallback".to_owned(),
                socket_per_lane: false,
                is_api_key: true,
            },
        ];
        gateway.turns.insert(1, turn1);
        gateway.lanes.insert(
            None,
            Lane {
                active: Some(1),
                ..Default::default()
            },
        );

        // `dispatch` drains `turn.prepared.candidates`; `assign_connection` must still resolve API key!
        let dispatched = gateway.dispatch(1).unwrap();
        assert!(dispatched.candidates.is_empty() || dispatched.candidates.len() == 2);
        assert!(gateway.turns[&1].prepared.candidates.is_empty());

        gateway.assign_connection(1, 10, api_key_id, "api-key-fallback");
        assert!(
            !gateway.session_oauth,
            "failing over to API-key candidate must not pin session as OAuth"
        );
        assert!(gateway.session_upstream.is_none());
        assert!(!gateway.turns[&1].prepared.oauth_transport);
        assert!(
            gateway.turns[&1].prepared.store,
            "failing over to API-key candidate with omitted store must restore durable store=true"
        );

        // Reusing connection 10 for Turn 2 (also admitted with OAuth primary + explicit_store = None)
        // must also preserve non-OAuth transport and durable store=true.
        let mut turn2 = make_synthetic_turn(&fixture, None, None, false, true, None, None);
        turn2.initial_selected_upstream_id = Some(oauth_id);
        turn2.candidate_oauth = HashMap::from([(oauth_id, true), (api_key_id, false)]);
        gateway.turns.insert(2, turn2);
        gateway.assign_connection(2, 10, api_key_id, "api-key-fallback");
        assert!(!gateway.session_oauth);
        assert!(!gateway.turns[&2].prepared.oauth_transport);
        assert!(gateway.turns[&2].prepared.store);

        // Explicit `store: false` must still be honored on API-key fallback.
        let mut turn3 = make_synthetic_turn(&fixture, None, None, false, true, Some(false), None);
        turn3.candidate_oauth = HashMap::from([(oauth_id, true), (api_key_id, false)]);
        gateway.turns.insert(3, turn3);
        gateway.assign_connection(3, 10, api_key_id, "api-key-fallback");
        assert!(!gateway.turns[&3].prepared.store);
    }

    #[tokio::test]
    async fn statusless_pending_error_expiry_emits_terminal_and_suppresses_late_failed_and_close_duplicate()
     {
        use cc_lb_control::RequestEventBus;

        let fixture = DirectAccountingFixture::new().await;
        let bus = Arc::new(cc_lb_control::InMemoryBus::new());
        let mut events = bus.subscribe_lifecycle();
        let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let observer = LifecycleContext::new("turn-statusless-exp".to_owned(), bus, &clock);
        observer.mark_authn_reached();

        let mut gateway = terminal_fixture_gateway(&fixture, Some(observer));
        gateway.connections.insert(1, Uuid::nil());
        gateway.connection_lanes.insert(1, None);
        gateway.turns.get_mut(&1).unwrap().response_id = None;

        let created = Bytes::from_static(
            br#"{"type":"response.created","response":{"id":"resp_sl_exp","status":"in_progress","store":false}}"#,
        );
        assert!(gateway.provider_message(1, &created).await.unwrap().forward);
        let err_frame = Bytes::from_static(
            br#"{"type":"error","response_id":"resp_sl_exp","error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"Overloaded"}}"#,
        );
        assert!(
            gateway
                .provider_message(1, &err_frame)
                .await
                .unwrap()
                .forward
        );
        let deadline = gateway
            .next_pending_error_deadline()
            .expect("pending error exposes expiration deadline");

        let expired_errors =
            gateway.expire_pending_error_terminals(deadline + std::time::Duration::from_secs(1));
        assert_eq!(
            expired_errors.len(),
            1,
            "statusless pending expiry must emit a status-bearing client terminal"
        );
        assert_eq!(expired_errors[0].status, 503);

        // Late `response.failed` and later `connection_closed` must not double-emit or double-settle.
        let late_failed = Bytes::from_static(
            br#"{"type":"response.failed","response":{"id":"resp_sl_exp","status":"failed","error":{"code":"server_is_overloaded","message":"Overloaded"}}}"#,
        );
        let res = gateway.provider_message(1, &late_failed).await.unwrap();
        assert!(!res.forward);
        let closed = gateway.connection_closed(1, "provider closed");
        assert!(
            closed.errors.is_empty(),
            "connection close after statusless expiry must not emit a duplicate error"
        );

        let mut terminated = 0usize;
        while let Ok(event) = events.try_recv() {
            if matches!(event, LifecycleEvent::RequestTerminated { .. }) {
                terminated += 1;
            }
        }
        assert_eq!(terminated, 1);
    }

    #[tokio::test]
    async fn pending_error_delayed_response_failed_binds_provider_output_and_settles_once_on_affinity_error()
     {
        use cc_lb_control::RequestEventBus;
        use cc_lb_storage_api::upstream_affinity::UpstreamAffinityBinding;
        use cc_lb_storage_api::{
            StorageError, StorageResult, UpstreamAffinityStore, UpstreamRecordId,
        };

        use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

        struct RecordingAffinityStore {
            bindings: Arc<Mutex<Vec<UpstreamAffinityBinding>>>,
            fail_bind: Arc<AtomicBool>,
        }

        #[async_trait::async_trait]
        impl UpstreamAffinityStore for RecordingAffinityStore {
            async fn read_upstream_owner_epoch(
                &self,
                _upstream_id: UpstreamRecordId,
                _expected_spec_revision: u64,
            ) -> StorageResult<Option<u64>> {
                Ok(Some(0))
            }

            async fn resolve_upstream_affinities(
                &self,
                _keys: &[UpstreamAffinityKey],
                _now_unix_secs: u64,
                _ttl_secs: u64,
            ) -> StorageResult<Vec<UpstreamAffinityBinding>> {
                Ok(Vec::new())
            }

            async fn bind_upstream_affinities(
                &self,
                bindings: &[UpstreamAffinityBinding],
                _now_unix_secs: u64,
                _ttl_secs: u64,
            ) -> StorageResult<()> {
                if self.fail_bind.load(AtomicOrdering::SeqCst) {
                    return Err(StorageError::Unavailable {
                        message: "affinity store unavailable".to_owned(),
                    });
                }
                self.bindings.lock().unwrap().extend_from_slice(bindings);
                Ok(())
            }

            async fn purge_expired_upstream_affinities(
                &self,
                _now_unix_secs: u64,
                _ttl_secs: u64,
                _batch_size: usize,
            ) -> StorageResult<u64> {
                Ok(0)
            }
        }

        let make_lifecycle_with_store =
            |lifecycle: Arc<Lifecycle>,
             store: Arc<dyn UpstreamAffinityStore>,
             bus: Arc<cc_lb_control::InMemoryBus>| {
                Arc::new(
                    Arc::try_unwrap(lifecycle)
                        .unwrap_or_else(|_| panic!("single lifecycle owner"))
                        .with_event_bus(bus)
                        .with_upstream_affinity_store(store),
                )
            };

        // Case 1: Delayed `response.failed` binds provider-emitted output before forwarding and
        // excludes unverified request-side pending keys.
        let fixture = DirectAccountingFixture::new().await;
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let store = Arc::new(RecordingAffinityStore {
            bindings: recorded.clone(),
            fail_bind: Arc::new(AtomicBool::new(false)),
        });
        let bus = Arc::new(cc_lb_control::InMemoryBus::new());
        let mut turn = make_synthetic_turn(&fixture, None, None, true, false, Some(true), None);
        turn.pending_affinity_keys.push(UpstreamAffinityKey {
            provider: "openai".to_owned(),
            principal_id: DIRECT_PRINCIPAL.to_owned(),
            kind: cc_lb_storage_api::UpstreamAffinityKind::OpenaiResponseId,
            value_sha256: [0xAB_u8; 32],
        });
        let auth = fixture.auth;
        let lifecycle = make_lifecycle_with_store(fixture.lifecycle, store, bus);
        let _keep_fixture = (
            fixture.storage,
            fixture.usage_handle,
            fixture.engine,
            fixture.view,
            fixture.concurrency,
        );
        let mut gateway =
            ResponsesWebSocketGateway::new(lifecycle, &auth, HeaderMap::new(), 65_536, 8);
        gateway.connections.insert(1, Uuid::nil());
        gateway.connection_lanes.insert(1, None);
        gateway.turns.insert(1, turn);
        gateway.lanes.insert(
            None,
            Lane {
                active: Some(1),
                ..Default::default()
            },
        );

        let created = Bytes::from_static(
            br#"{"type":"response.created","response":{"id":"resp_aff","status":"in_progress","store":true}}"#,
        );
        assert!(gateway.provider_message(1, &created).await.unwrap().forward);
        let err = Bytes::from_static(
            br#"{"type":"error","response_id":"resp_aff","status":503,"error":{"code":"server_is_overloaded","message":"Overloaded"}}"#,
        );
        assert!(gateway.provider_message(1, &err).await.unwrap().forward);
        let failed = Bytes::from_static(
            br#"{"type":"response.failed","response":{"id":"resp_aff","status":"failed","store":true,"output":[{"type":"reasoning","encrypted_content":"gAAAAABprovider_failed_reasoning"}],"error":{"code":"server_is_overloaded","message":"Overloaded"}}}"#,
        );
        let res_failed = gateway.provider_message(1, &failed).await.unwrap();
        assert!(res_failed.forward);
        {
            let stored = recorded.lock().unwrap();
            assert!(
                !stored.is_empty(),
                "provider-owned output from delayed response.failed must bind before forwarding"
            );
            assert!(
                !stored.iter().any(|b| b.key.value_sha256 == [0xAB_u8; 32]),
                "unverified request-side affinity keys must never bind on response.failed"
            );
        }

        // Case 2: Affinity binding error during delayed `response.failed` fails closed and settles once.
        let fixture = DirectAccountingFixture::new().await;
        let fail_bind = Arc::new(AtomicBool::new(false));
        let store = Arc::new(RecordingAffinityStore {
            bindings: Arc::new(Mutex::new(Vec::new())),
            fail_bind: fail_bind.clone(),
        });
        let bus = Arc::new(cc_lb_control::InMemoryBus::new());
        let mut events = bus.subscribe_lifecycle();
        let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let observer = LifecycleContext::new("turn-aff-fail".to_owned(), bus.clone(), &clock);
        observer.mark_authn_reached();
        let turn = make_synthetic_turn(
            &fixture,
            Some(observer),
            None,
            true,
            false,
            Some(true),
            None,
        );
        let auth = fixture.auth;
        let lifecycle = make_lifecycle_with_store(fixture.lifecycle, store, bus);
        let _keep_fixture = (
            fixture.storage,
            fixture.usage_handle,
            fixture.engine,
            fixture.view,
            fixture.concurrency,
        );
        let mut gateway =
            ResponsesWebSocketGateway::new(lifecycle, &auth, HeaderMap::new(), 65_536, 8);
        gateway.connections.insert(1, Uuid::nil());
        gateway.connection_lanes.insert(1, None);
        gateway.turns.insert(1, turn);
        gateway.lanes.insert(
            None,
            Lane {
                active: Some(1),
                ..Default::default()
            },
        );
        assert!(gateway.provider_message(1, &created).await.unwrap().forward);
        assert!(gateway.provider_message(1, &err).await.unwrap().forward);
        fail_bind.store(true, AtomicOrdering::SeqCst);
        let Err(bind_err) = gateway.provider_message(1, &failed).await else {
            panic!("expected affinity binding error");
        };
        assert_eq!(bind_err.code, "upstream_affinity_unavailable");
        assert!(gateway.pending_error_terminals.is_empty());
        let closed = gateway.connection_closed(1, "affinity error");
        assert!(closed.errors.is_empty());

        let mut terminated = 0usize;
        while let Ok(event) = events.try_recv() {
            if matches!(event, LifecycleEvent::RequestTerminated { .. }) {
                terminated += 1;
            }
        }
        assert_eq!(terminated, 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn parking_pending_error_releases_concurrent_limit_while_retaining_token_reservation() {
        let _guard = DIRECT_PRICING_LOCK.lock().await;
        install_direct_pricing();
        let fixture = DirectAccountingFixture::new().await;
        let reservation_a = fixture.reserve("ws-openai-known");
        let reservation_a_id = reservation_a.id().to_owned();
        assert!(
            fixture
                .engine
                .reserve(
                    &fixture.view,
                    &fixture.record,
                    DIRECT_PRINCIPAL,
                    "ws-openai-known",
                    Some(100),
                    100,
                    Some(1_000),
                )
                .is_err(),
            "concurrent=1 cap must block second request while Turn A is active"
        );

        let mut gateway = terminal_fixture_gateway(&fixture, None);
        gateway.connections.insert(1, Uuid::nil());
        gateway.connection_lanes.insert(1, None);
        let turn_a = gateway.turns.get_mut(&1).unwrap();
        turn_a.response_id = None;
        turn_a.prepared.model = "ws-openai-known".to_owned();
        turn_a.prepared.reservation = Some(reservation_a);
        turn_a.prepared.limit_engine = Some(fixture.engine.clone());

        let created = Bytes::from_static(
            br#"{"type":"response.created","response":{"id":"resp_conc","status":"in_progress","store":false}}"#,
        );
        assert!(gateway.provider_message(1, &created).await.unwrap().forward);

        // Status-bearing error parks Turn A and advances the lane -> concurrent slot must be released
        // while token/cost reservation stays active until delayed `response.failed` settles it.
        let err = Bytes::from_static(
            br#"{"type":"error","response_id":"resp_conc","status":503,"error":{"code":"server_is_overloaded","message":"Overloaded"}}"#,
        );
        assert!(gateway.provider_message(1, &err).await.unwrap().forward);
        assert_eq!(fixture.concurrency.current(&fixture.record.key_hash_b64), 0);
        assert!(
            gateway.pending_error_terminals["resp_conc"]
                .turn
                .prepared
                .reservation
                .as_ref()
                .is_some_and(Reservation::is_active),
            "token/cost reservation must stay active while awaiting response.failed usage"
        );

        // A small-cost successor now passes because concurrency=1 was released on park, while
        // reserving a second large cost still fails because Turn A's cost reservation remains held.
        assert!(
            fixture
                .engine
                .reserve(
                    &fixture.view,
                    &fixture.record,
                    DIRECT_PRINCIPAL,
                    "ws-openai-known",
                    Some(100),
                    100,
                    Some(8_010),
                )
                .is_err(),
            "retained cost reservation must still block a full overlapping cost allocation"
        );
        let retry_res = fixture
            .engine
            .reserve(
                &fixture.view,
                &fixture.record,
                DIRECT_PRINCIPAL,
                "ws-openai-known",
                Some(100),
                100,
                Some(1_000),
            )
            .expect("released concurrent limit must allow successor within remaining cost budget");
        assert_eq!(fixture.concurrency.current(&fixture.record.key_hash_b64), 1);
        drop(retry_res);
        assert_eq!(fixture.concurrency.current(&fixture.record.key_hash_b64), 0);

        let failed = Bytes::from_static(
            br#"{"type":"response.failed","response":{"id":"resp_conc","status":"failed","usage":{"input_tokens":21000,"output_tokens":1,"input_tokens_details":{"cached_tokens":20000}}}}"#,
        );
        assert!(gateway.provider_message(1, &failed).await.unwrap().forward);
        fixture
            .assert_settled(&reservation_a_id, "ws-openai-known", 4_010, 1_000, 1)
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pending_error_delayed_response_failed_drop_during_affinity_await_settles_on_close() {
        use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

        use cc_lb_storage_api::upstream_affinity::UpstreamAffinityBinding;
        use cc_lb_storage_api::{StorageResult, UpstreamAffinityStore, UpstreamRecordId};

        let _guard = DIRECT_PRICING_LOCK.lock().await;
        install_direct_pricing();

        struct StallingAffinityStore {
            stall: Arc<AtomicBool>,
            started: Arc<tokio::sync::Notify>,
            blocked: Arc<tokio::sync::Notify>,
        }

        #[async_trait::async_trait]
        impl UpstreamAffinityStore for StallingAffinityStore {
            async fn read_upstream_owner_epoch(
                &self,
                _upstream_id: UpstreamRecordId,
                _expected_spec_revision: u64,
            ) -> StorageResult<Option<u64>> {
                Ok(Some(0))
            }

            async fn resolve_upstream_affinities(
                &self,
                _keys: &[UpstreamAffinityKey],
                _now_unix_secs: u64,
                _ttl_secs: u64,
            ) -> StorageResult<Vec<UpstreamAffinityBinding>> {
                Ok(Vec::new())
            }

            async fn bind_upstream_affinities(
                &self,
                _bindings: &[UpstreamAffinityBinding],
                _now_unix_secs: u64,
                _ttl_secs: u64,
            ) -> StorageResult<()> {
                if self.stall.load(AtomicOrdering::SeqCst) {
                    self.started.notify_one();
                    self.blocked.notified().await;
                }
                Ok(())
            }

            async fn purge_expired_upstream_affinities(
                &self,
                _now_unix_secs: u64,
                _ttl_secs: u64,
                _batch_size: usize,
            ) -> StorageResult<u64> {
                Ok(0)
            }
        }

        let placeholder = DirectAccountingFixture::new().await;
        placeholder.usage_handle.shutdown().await;
        let fixture = DirectAccountingFixture::new().await;
        let reservation = fixture.reserve("ws-openai-known");
        let reservation_id = reservation.id().to_owned();
        let stall = Arc::new(AtomicBool::new(false));
        let started = Arc::new(tokio::sync::Notify::new());
        let blocked = Arc::new(tokio::sync::Notify::new());
        let store = Arc::new(StallingAffinityStore {
            stall: stall.clone(),
            started: started.clone(),
            blocked: blocked.clone(),
        });
        let mut turn = make_synthetic_turn(&fixture, None, None, true, false, Some(true), None);
        turn.response_id = None;
        turn.prepared.model = "ws-openai-known".to_owned();
        turn.prepared.store = true;
        turn.prepared.reservation = Some(reservation);
        turn.prepared.limit_engine = Some(fixture.engine.clone());

        let auth = fixture.auth;
        let lifecycle = Arc::new(
            Arc::try_unwrap(fixture.lifecycle)
                .unwrap_or_else(|_| panic!("single lifecycle owner"))
                .with_upstream_affinity_store(store),
        );
        let mut gateway =
            ResponsesWebSocketGateway::new(lifecycle, &auth, HeaderMap::new(), 65_536, 8);
        gateway.connections.insert(1, Uuid::nil());
        gateway.connection_lanes.insert(1, None);
        gateway.turns.insert(1, turn);
        gateway.lanes.insert(
            None,
            Lane {
                active: Some(1),
                ..Default::default()
            },
        );

        let created = Bytes::from_static(
            br#"{"type":"response.created","response":{"id":"resp_stall","status":"in_progress","store":true}}"#,
        );
        assert!(gateway.provider_message(1, &created).await.unwrap().forward);
        let err = Bytes::from_static(
            br#"{"type":"error","response_id":"resp_stall","status":503,"error":{"code":"server_is_overloaded","message":"Overloaded"}}"#,
        );
        assert!(gateway.provider_message(1, &err).await.unwrap().forward);

        stall.store(true, AtomicOrdering::SeqCst);
        let failed = Bytes::from_static(
            br#"{"type":"response.failed","response":{"id":"resp_stall","status":"failed","store":true,"output":[{"type":"reasoning","encrypted_content":"gAAAAABstall_reasoning"}],"usage":{"input_tokens":21000,"output_tokens":1,"input_tokens_details":{"cached_tokens":20000}},"error":{"code":"server_is_overloaded","message":"Overloaded"}}}"#,
        );
        {
            let mut fut = Box::pin(gateway.provider_message(1, &failed));
            tokio::select! {
                res = &mut fut => panic!("affinity await unexpectedly completed (ok={})", res.is_ok()),
                _ = started.notified() => {}
            }
            // Drop `fut` mid-await (simulating relay cancellation / shutdown), while the parked turn
            // with measured usage stays registered for `close()` -> `finish_session`.
        }

        gateway.close();
        drop(gateway);
        let check_fixture = DirectAccountingFixture {
            engine: fixture.engine,
            view: fixture.view,
            record: fixture.record,
            concurrency: fixture.concurrency,
            lifecycle: placeholder.lifecycle,
            auth,
            storage: fixture.storage,
            usage_handle: fixture.usage_handle,
        };
        check_fixture
            .assert_settled(&reservation_id, "ws-openai-known", 4_010, 1_000, 1)
            .await;
    }

    #[tokio::test]
    async fn statusless_pending_error_expiry_identifies_response_id_after_successor_binds() {
        let fixture = DirectAccountingFixture::new().await;
        let mut gateway = terminal_fixture_gateway(&fixture, None);
        gateway.connections.insert(1, Uuid::nil());
        gateway
            .connection_lanes
            .insert(1, Some("lane-a".to_owned()));
        let turn_a = gateway.turns.get_mut(&1).unwrap();
        turn_a.lane = Some("lane-a".to_owned());
        turn_a.response_id = None;
        gateway.lanes.clear();
        let mut lane_queue = VecDeque::new();
        lane_queue.push_back(2);
        gateway.lanes.insert(
            Some("lane-a".to_owned()),
            Lane {
                active: Some(1),
                queued: lane_queue,
            },
        );
        let mut turn_b = make_synthetic_turn(
            &fixture,
            None,
            Some("lane-a".to_owned()),
            false,
            false,
            Some(false),
            None,
        );
        turn_b.response_id = None;
        turn_b.upstream_id = None;
        turn_b.connection_id = None;
        gateway.turns.insert(2, turn_b);

        let created_a = Bytes::from_static(
            br#"{"type":"response.created","stream_id":"lane-a","response":{"id":"resp_a_id","status":"in_progress","store":false}}"#,
        );
        assert!(
            gateway
                .provider_message(1, &created_a)
                .await
                .unwrap()
                .forward
        );
        let statusless_err = Bytes::from_static(
            br#"{"type":"error","stream_id":"lane-a","response_id":"resp_a_id","error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"Overloaded"}}"#,
        );
        let err_res = gateway.provider_message(1, &statusless_err).await.unwrap();
        assert!(err_res.forward);
        assert_eq!(err_res.dispatches.len(), 1);
        assert_eq!(err_res.dispatches[0].turn_id, 2);

        // Turn B binds and starts on the same lane before A expires.
        gateway.assign_connection(2, 1, Uuid::nil(), "assigned");
        gateway.turns.get_mut(&2).unwrap().sent = true;
        let created_b = Bytes::from_static(
            br#"{"type":"response.created","stream_id":"lane-a","response":{"id":"resp_b_id","status":"in_progress","store":false}}"#,
        );
        assert!(
            gateway
                .provider_message(1, &created_b)
                .await
                .unwrap()
                .forward
        );

        let deadline =
            gateway.next_pending_error_deadline().unwrap() + std::time::Duration::from_secs(1);
        let expired = gateway.expire_pending_error_terminals(deadline);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].response_id(), Some("resp_a_id"));
        assert_eq!(expired[0].stream_id.as_deref(), Some("lane-a"));
        let wire: Value = serde_json::from_slice(&expired[0].to_bytes()).unwrap();
        assert_eq!(wire["response_id"], "resp_a_id");
        assert_eq!(wire["stream_id"], "lane-a");
        assert!(
            gateway.turns.contains_key(&2),
            "active successor Turn B must remain undisturbed"
        );
    }

    #[tokio::test]
    async fn bind_owner_avoids_futile_eviction_and_preserves_evicted_error_on_late_failure() {
        use cc_lb_storage_api::upstream_affinity::UpstreamAffinityBinding;
        use cc_lb_storage_api::{
            StorageError, StorageResult, UpstreamAffinityStore, UpstreamRecordId,
        };

        // Part A: When remaining owners are pinned by `turns`/`recent` (cross-connection),
        // evicting A cannot free enough slots (`slot_cap=1`) -> `bind_owner` must fail upfront
        // with `overloaded_error` without evicting A.
        let fixture = DirectAccountingFixture::new().await;
        let mut gateway = ResponsesWebSocketGateway::new(
            fixture.lifecycle.clone(),
            &fixture.auth,
            HeaderMap::new(),
            65_536,
            1,
        );
        gateway.connections.insert(1, Uuid::nil());
        gateway.connections.insert(2, Uuid::nil());
        gateway.connection_lanes.insert(1, None);
        gateway.connection_lanes.insert(2, None);
        let mut turn_a = make_synthetic_turn(&fixture, None, None, true, false, Some(false), None);
        turn_a.connection_id = Some(1);
        let mut turn_b = make_synthetic_turn(&fixture, None, None, true, false, Some(false), None);
        turn_b.connection_id = Some(2);
        turn_b.response_id = None;
        gateway.turns.insert(1, turn_a);
        gateway.turns.insert(2, turn_b);
        gateway.lanes.insert(
            None,
            Lane {
                active: Some(1),
                ..Default::default()
            },
        );

        let created_a = Bytes::from_static(
            br#"{"type":"response.created","response":{"id":"resp_keep","status":"in_progress","store":false}}"#,
        );
        assert!(
            gateway
                .provider_message(1, &created_a)
                .await
                .unwrap()
                .forward
        );
        let statusless_a = Bytes::from_static(
            br#"{"type":"error","response_id":"resp_keep","error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"Overloaded"}}"#,
        );
        assert!(
            gateway
                .provider_message(1, &statusless_a)
                .await
                .unwrap()
                .forward
        );

        gateway.lanes.get_mut(&None).unwrap().active = Some(2);
        let created_b = Bytes::from_static(
            br#"{"type":"response.created","response":{"id":"resp_reject","status":"in_progress","store":false}}"#,
        );
        let Err(overload) = gateway.provider_message(2, &created_b).await else {
            panic!(
                "expected overloaded_error when eviction cannot free cross-connection owner slot"
            );
        };
        assert_eq!(overload.code, "overloaded_error");
        assert!(
            gateway.pending_error_terminals.contains_key("resp_keep"),
            "A must stay registered when eviction cannot make room for B"
        );
        let failed_a = Bytes::from_static(
            br#"{"type":"response.failed","response":{"id":"resp_keep","status":"failed","error":{"code":"server_is_overloaded","message":"Overloaded"}}}"#,
        );
        assert!(
            gateway
                .provider_message(1, &failed_a)
                .await
                .unwrap()
                .forward
        );

        // Part B: When eviction DOES free A (same recent key, slot_cap=1) and B subsequently
        // fails affinity binding in `observe_provider_message`, A's eviction terminal must be
        // preserved in `take_pending_errors()`.
        struct FailBindStore;
        #[async_trait::async_trait]
        impl UpstreamAffinityStore for FailBindStore {
            async fn read_upstream_owner_epoch(
                &self,
                _upstream_id: UpstreamRecordId,
                _expected_spec_revision: u64,
            ) -> StorageResult<Option<u64>> {
                Ok(Some(0))
            }

            async fn resolve_upstream_affinities(
                &self,
                _keys: &[UpstreamAffinityKey],
                _now_unix_secs: u64,
                _ttl_secs: u64,
            ) -> StorageResult<Vec<UpstreamAffinityBinding>> {
                Ok(Vec::new())
            }

            async fn bind_upstream_affinities(
                &self,
                _bindings: &[UpstreamAffinityBinding],
                _now_unix_secs: u64,
                _ttl_secs: u64,
            ) -> StorageResult<()> {
                Err(StorageError::Unavailable {
                    message: "affinity down".to_owned(),
                })
            }

            async fn purge_expired_upstream_affinities(
                &self,
                _now_unix_secs: u64,
                _ttl_secs: u64,
                _batch_size: usize,
            ) -> StorageResult<u64> {
                Ok(0)
            }
        }

        let fixture = DirectAccountingFixture::new().await;
        let turn_a = make_synthetic_turn(&fixture, None, None, true, false, Some(false), None);
        let mut turn_b = make_synthetic_turn(&fixture, None, None, false, false, Some(true), None);
        turn_b.response_id = None;
        turn_b.upstream_id = None;
        turn_b.connection_id = None;
        let auth = fixture.auth;
        let lifecycle = Arc::new(
            Arc::try_unwrap(fixture.lifecycle)
                .unwrap_or_else(|_| panic!("single lifecycle owner"))
                .with_upstream_affinity_store(Arc::new(FailBindStore)),
        );
        let _keep = (
            fixture.storage,
            fixture.usage_handle,
            fixture.engine,
            fixture.view,
            fixture.concurrency,
        );
        let mut gateway =
            ResponsesWebSocketGateway::new(lifecycle, &auth, HeaderMap::new(), 65_536, 1);
        gateway.connections.insert(1, Uuid::nil());
        gateway.connection_lanes.insert(1, None);
        gateway.turns.insert(1, turn_a);
        gateway.turns.insert(2, turn_b);
        let mut q = VecDeque::new();
        q.push_back(2);
        gateway.lanes.insert(
            None,
            Lane {
                active: Some(1),
                queued: q,
            },
        );
        let created_a2 = Bytes::from_static(
            br#"{"type":"response.created","response":{"id":"resp_evict_me","status":"in_progress","store":false}}"#,
        );
        assert!(
            gateway
                .provider_message(1, &created_a2)
                .await
                .unwrap()
                .forward
        );
        let statusless_a2 = Bytes::from_static(
            br#"{"type":"error","response_id":"resp_evict_me","error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"Overloaded"}}"#,
        );
        assert!(
            gateway
                .provider_message(1, &statusless_a2)
                .await
                .unwrap()
                .forward
        );
        gateway.assign_connection(2, 1, Uuid::nil(), "assigned");
        gateway.turns.get_mut(&2).unwrap().sent = true;
        let created_b2 = Bytes::from_static(
            br#"{"type":"response.created","response":{"id":"resp_b_fails_aff","status":"in_progress","store":true}}"#,
        );
        let Err(aff_err) = gateway.provider_message(1, &created_b2).await else {
            panic!("expected affinity error after evicting resp_evict_me");
        };
        assert_eq!(aff_err.code, "upstream_affinity_unavailable");
        let preserved = gateway.take_pending_errors();
        assert_eq!(preserved.len(), 1);
        assert_eq!(preserved[0].response_id(), Some("resp_evict_me"));
    }

    #[tokio::test]
    async fn public_oauth_retired_terminal_matches_connection_lane_when_omitted_and_rejects_mismatch()
     {
        let fixture = DirectAccountingFixture::new().await;
        let mut gateway = terminal_fixture_gateway(&fixture, None);
        gateway.codex_backend = false;
        gateway.connections.insert(1, Uuid::nil());
        gateway.connection_oauth.insert(1, true);
        gateway
            .connection_lanes
            .insert(1, Some("lane-pub".to_owned()));
        let turn_a = gateway.turns.get_mut(&1).unwrap();
        turn_a.lane = Some("lane-pub".to_owned());
        turn_a.prepared.oauth_transport = true;
        turn_a.response_id = None;
        gateway.lanes.clear();
        let mut q = VecDeque::new();
        q.push_back(2);
        gateway.lanes.insert(
            Some("lane-pub".to_owned()),
            Lane {
                active: Some(1),
                queued: q,
            },
        );
        let mut turn_b = make_synthetic_turn(
            &fixture,
            None,
            Some("lane-pub".to_owned()),
            false,
            true,
            Some(false),
            None,
        );
        turn_b.response_id = None;
        turn_b.upstream_id = None;
        turn_b.connection_id = None;
        gateway.turns.insert(2, turn_b);

        let created_a = Bytes::from_static(
            br#"{"type":"response.created","response":{"id":"resp_pub_a","status":"in_progress","store":false}}"#,
        );
        assert!(
            gateway
                .provider_message(1, &created_a)
                .await
                .unwrap()
                .frames
                .is_some()
        );
        let completed_a = Bytes::from_static(
            br#"{"type":"response.completed","response":{"id":"resp_pub_a","status":"completed"}}"#,
        );
        assert!(
            gateway
                .provider_message(1, &completed_a)
                .await
                .unwrap()
                .frames
                .is_some()
        );

        // Successor B completes on the same lane so the lane is now completely idle and
        // `recent[(1, "lane-pub")]` has moved past `resp_pub_a`.
        gateway.assign_connection(2, 1, Uuid::nil(), "assigned");
        gateway.turns.get_mut(&2).unwrap().sent = true;
        let created_b = Bytes::from_static(
            br#"{"type":"response.created","response":{"id":"resp_pub_b","status":"in_progress","store":false}}"#,
        );
        assert!(
            gateway
                .provider_message(1, &created_b)
                .await
                .unwrap()
                .frames
                .is_some()
        );
        let completed_b = Bytes::from_static(
            br#"{"type":"response.completed","response":{"id":"resp_pub_b","status":"completed"}}"#,
        );
        assert!(
            gateway
                .provider_message(1, &completed_b)
                .await
                .unwrap()
                .frames
                .is_some()
        );

        // Duplicate native terminal for A omits `stream_id`; must match via `connection_lanes[1]`.
        let dup_omitted_lane = Bytes::from_static(
            br#"{"type":"response.failed","response":{"id":"resp_pub_a","status":"failed","error":{"code":"server_is_overloaded","message":"Overloaded"}}}"#,
        );
        let dup_res = gateway
            .provider_message(1, &dup_omitted_lane)
            .await
            .unwrap();
        assert!(!dup_res.forward);
        assert!(dup_res.frames.is_none());
        assert!(dup_res.errors.is_empty());

        // Explicit mismatched `stream_id` must still be rejected.
        let dup_wrong_lane = Bytes::from_static(
            br#"{"type":"response.failed","stream_id":"other-lane","response":{"id":"resp_pub_a","status":"failed","error":{"code":"server_is_overloaded","message":"Overloaded"}}}"#,
        );
        assert!(gateway.provider_message(1, &dup_wrong_lane).await.is_err());
    }

    struct PrewarmMultiUpstreamFixture {
        headers: HeaderMap,
        auth: Authenticated,
        lifecycle: Arc<Lifecycle>,
        dynamic_view: Arc<cc_lb_control::dynamic_view::DynamicViewHolder>,
        upstreams: Vec<cc_lb_storage_api::UpstreamRecord>,
        upstream_a: Uuid,
        upstream_b: Uuid,
        quota_cache: Arc<PrewarmQuotaCache>,
        affinity_store: Arc<PrewarmAffinityStore>,
    }

    #[derive(Default)]
    struct PrewarmQuotaCache {
        snapshots: Mutex<HashMap<Uuid, Vec<cc_lb_domain::SubscriptionQuotaCandidateSnapshot>>>,
    }

    impl cc_lb_control::SubscriptionQuotaCacheLike for PrewarmQuotaCache {
        fn upsert_observation(&self, _record: &cc_lb_storage_api::SubscriptionQuotaSample) {}

        fn snapshot_for_upstream(
            &self,
            upstream_id: Uuid,
            _now_unix_millis: u64,
            _max_staleness_secs: u64,
        ) -> Vec<cc_lb_domain::SubscriptionQuotaCandidateSnapshot> {
            self.snapshots
                .lock()
                .expect("prewarm quota cache lock")
                .get(&upstream_id)
                .cloned()
                .unwrap_or_default()
        }
    }

    #[derive(Default)]
    struct PrewarmAffinityStore {
        bindings: Mutex<
            HashMap<
                UpstreamAffinityKey,
                cc_lb_storage_api::upstream_affinity::UpstreamAffinityBinding,
            >,
        >,
    }

    #[async_trait::async_trait]
    impl cc_lb_storage_api::UpstreamAffinityStore for PrewarmAffinityStore {
        async fn read_upstream_owner_epoch(
            &self,
            _upstream_id: cc_lb_storage_api::UpstreamRecordId,
            _expected_spec_revision: u64,
        ) -> cc_lb_storage_api::StorageResult<Option<u64>> {
            Ok(Some(0))
        }

        async fn resolve_upstream_affinities(
            &self,
            keys: &[UpstreamAffinityKey],
            _now_unix_secs: u64,
            _ttl_secs: u64,
        ) -> cc_lb_storage_api::StorageResult<
            Vec<cc_lb_storage_api::upstream_affinity::UpstreamAffinityBinding>,
        > {
            let bindings = self.bindings.lock().expect("prewarm affinity lock");
            Ok(keys
                .iter()
                .filter_map(|key| bindings.get(key).cloned())
                .collect())
        }

        async fn bind_upstream_affinities(
            &self,
            bindings: &[cc_lb_storage_api::upstream_affinity::UpstreamAffinityBinding],
            _now_unix_secs: u64,
            _ttl_secs: u64,
        ) -> cc_lb_storage_api::StorageResult<()> {
            let mut guard = self.bindings.lock().expect("prewarm affinity lock");
            for binding in bindings {
                guard.insert(binding.key.clone(), binding.clone());
            }
            Ok(())
        }

        async fn purge_expired_upstream_affinities(
            &self,
            _now_unix_secs: u64,
            _ttl_secs: u64,
            _batch_size: usize,
        ) -> cc_lb_storage_api::StorageResult<u64> {
            Ok(0)
        }
    }

    fn prewarm_quota_snapshot(
        window: &str,
        utilization: f64,
        status: &str,
        reset: u64,
    ) -> cc_lb_domain::SubscriptionQuotaCandidateSnapshot {
        cc_lb_domain::SubscriptionQuotaCandidateSnapshot {
            window: window.to_owned(),
            state: cc_lb_domain::SubscriptionQuotaDataState::Fresh,
            source: Some("prewarm-test".to_owned()),
            utilization: Some(utilization),
            status: Some(status.to_owned()),
            resets_at_unix_secs: Some(reset),
            surpassed_threshold: None,
            representative_claim: None,
            disabled_reason: None,
            extra_usage_enabled: None,
            extra_usage_monthly_limit: None,
            extra_usage_used_credits: None,
            credits: None,
            observed_at_unix_millis: Some(1_700_000_000 * 1_000),
            max_staleness_secs: 60,
            fallback_available: None,
            overage_in_use: None,
            overage_period_monthly_utilization: None,
            upgrade_paths: None,
        }
    }

    impl PrewarmMultiUpstreamFixture {
        async fn new() -> Self {
            use cc_lb_control::api_keys::builtin_authn::BuiltinAuthn;
            use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
            use cc_lb_control::api_keys::principal_view::{
                DialectCache, PrincipalRoutingArtifacts, PrincipalView, RouterPipelineCache,
            };
            use cc_lb_control::dynamic_view::{DynamicViewBuilder, DynamicViewHolder};
            use cc_lb_domain::TerminalStrategy;
            use cc_lb_routing::FilterPlugin;
            use cc_lb_storage_api::upstream::{UpstreamKind as StorageRecordKind, UpstreamRecord};
            use cc_lb_upstream::{
                ApiKeyAwareSignerFactory, RetryDecision, ShapedRequest, SignedRequest, Signer,
                SignerError, SignerFactory, SigningCapability, UpstreamError,
            };

            struct DummySignerFactory;
            impl ApiKeyAwareSignerFactory for DummySignerFactory {
                fn with_router_choice(&self, _name: String) -> Arc<dyn SignerFactory> {
                    Arc::new(Self)
                }
            }
            #[async_trait::async_trait]
            impl SignerFactory for DummySignerFactory {
                async fn build(
                    &self,
                    _upstream: &cc_lb_domain::Upstream,
                ) -> Result<Arc<dyn Signer>, SignerError> {
                    Ok(Arc::new(DummySigner))
                }
            }
            struct DummySigner;
            #[async_trait::async_trait]
            impl Signer for DummySigner {
                async fn sign(
                    &self,
                    shaped: ShapedRequest,
                    capability: &mut SigningCapability,
                ) -> Result<SignedRequest, SignerError> {
                    Ok(SignedRequest::from_shaped(shaped, capability))
                }

                async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
                    RetryDecision::Fail
                }
            }
            struct UnusedDispatcher;
            #[async_trait::async_trait]
            impl crate::UpstreamDispatch for UnusedDispatcher {
                async fn dispatch(
                    &self,
                    _request: SignedRequest,
                ) -> Result<http::Response<crate::Body>, crate::DispatchError> {
                    unreachable!("unused dispatcher in websocket prewarm unit test")
                }
            }

            const NOW_SECS: u64 = 1_700_000_000;
            let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(NOW_SECS));
            let storage = Arc::new(
                cc_lb_storage_sqlite::open_sqlite("sqlite::memory:", clock.clone())
                    .await
                    .expect("open in-memory prewarm test database"),
            );
            storage
                .initialize()
                .await
                .expect("migrate prewarm test database");
            let keys = Arc::new(KeyStore::new(storage));
            let (_record, secret) = keys
                .create(
                    DIRECT_PRINCIPAL,
                    CreateParams {
                        label: "prewarm-test".to_owned(),
                        description: None,
                        expires_at_unix_secs: None,
                        limit_overrides: Vec::new(),
                    },
                )
                .await
                .expect("issue prewarm fixture key");
            let filter: Arc<dyn FilterPlugin> = Arc::new(
                crate::builtin_filters::subscription_preference::SubscriptionPreferenceFilter::with_clock(
                    clock.clone(),
                ),
            );
            let pipeline = Arc::new(RouterPipelineCache {
                user_filters: vec![filter],
                terminal: TerminalStrategy::FirstPick,
                instantiation_error: None,
                provider_filters: None,
            });
            let mut chains: HashMap<String, PrincipalRoutingArtifacts> = HashMap::new();
            chains.insert(
                DIRECT_PRINCIPAL.to_owned(),
                (Some(pipeline), DialectCache::Inherit),
            );
            let view = Arc::new(PrincipalView::for_tests(
                DIRECT_PRINCIPAL,
                true,
                vec!["*".to_owned()],
                Vec::new(),
                chains,
            ));
            let authn = Arc::new(BuiltinAuthn::new(keys, clock.clone()));
            let mut headers = HeaderMap::new();
            headers.insert(
                "x-api-key",
                secret.expose().parse().expect("fixture key header"),
            );
            let auth = crate::authenticate_first(&authn, &headers, &view)
                .await
                .expect("authenticate prewarm fixture caller");

            let upstream_a = Uuid::from_u128(0xAAA1);
            let upstream_b = Uuid::from_u128(0xBBB2);
            let upstreams = vec![
                UpstreamRecord {
                    id: upstream_a,
                    name: "oauth-a".to_owned(),
                    kind: StorageRecordKind::OpenaiOauth,
                    enabled: true,
                    ..Default::default()
                },
                UpstreamRecord {
                    id: upstream_b,
                    name: "oauth-b".to_owned(),
                    kind: StorageRecordKind::OpenaiOauth,
                    enabled: true,
                    ..Default::default()
                },
            ];
            let quota_cache = Arc::new(PrewarmQuotaCache::default());
            {
                let mut guard = quota_cache.snapshots.lock().expect("quota cache lock");
                guard.insert(
                    upstream_a,
                    vec![
                        prewarm_quota_snapshot("5h", 0.80, "allowed", NOW_SECS + 10_000),
                        prewarm_quota_snapshot("7d", 0.80, "allowed", NOW_SECS + 100_000),
                    ],
                );
                guard.insert(
                    upstream_b,
                    vec![
                        prewarm_quota_snapshot("5h", 0.10, "allowed", NOW_SECS + 10_000),
                        prewarm_quota_snapshot("7d", 0.10, "allowed", NOW_SECS + 100_000),
                    ],
                );
            }
            let dynamic = DynamicViewBuilder::new(0)
                .signer_factory(Arc::new(DummySignerFactory))
                .principal_view(view)
                .subscription_quota_cache(quota_cache.clone())
                .subscription_quota_routing_max_staleness_secs(60)
                .upstream_records(upstreams.clone())
                .build();
            let dynamic_view = Arc::new(DynamicViewHolder::new(dynamic));
            let affinity_store = Arc::new(PrewarmAffinityStore::default());
            let config = crate::LifecycleConfig {
                unknown_openai_reasoning: cc_lb_domain::UnknownOpenaiReasoningPolicy::Hybrid,
                ..Default::default()
            };
            let lifecycle = Arc::new(
                Lifecycle::new_with_dynamic_view(
                    authn,
                    Arc::clone(&dynamic_view),
                    Arc::new(UnusedDispatcher),
                    config,
                    clock,
                )
                .with_upstream_affinity_store(affinity_store.clone()),
            );
            Self {
                headers,
                auth,
                lifecycle,
                dynamic_view,
                upstreams,
                upstream_a,
                upstream_b,
                quota_cache,
                affinity_store,
            }
        }

        fn disable_upstream(&self, upstream_id: Uuid) {
            let mut updated = self.upstreams.clone();
            for record in &mut updated {
                if record.id == upstream_id {
                    record.enabled = false;
                }
            }
            let current = self.dynamic_view.load();
            self.dynamic_view.store(
                cc_lb_control::dynamic_view::DynamicViewBuilder::from_view(&current)
                    .upstream_records(updated)
                    .build(),
            );
        }

        fn gateway(&self, headers: HeaderMap) -> ResponsesWebSocketGateway {
            ResponsesWebSocketGateway::new(
                self.lifecycle.clone(),
                &self.auth,
                headers,
                1_024 * 1_024,
                16,
            )
            .with_codex_backend(true)
        }

        fn seed_encrypted_content_owner(&self, encrypted_content: &str, owner: Uuid) {
            let body = serde_json::json!({
                "model": "gpt-5",
                "store": false,
                "input": [{
                    "type": "reasoning",
                    "id": "rs_seeded",
                    "summary": [],
                    "content": [],
                    "encrypted_content": encrypted_content
                }]
            });
            let keys = crate::upstream_affinity::extract_request_affinity_keys_with_scope(
                crate::provider_capability::ProviderCapability::OpenaiResponses,
                &body,
                DIRECT_PRINCIPAL,
                crate::upstream_affinity::OpenaiResponseReferenceScope::Durable,
            )
            .expect("affinity keys extract");
            assert!(!keys.is_empty());
            let mut guard = self
                .affinity_store
                .bindings
                .lock()
                .expect("prewarm affinity lock");
            for key in keys {
                guard.insert(
                    key.clone(),
                    cc_lb_storage_api::upstream_affinity::UpstreamAffinityBinding {
                        key,
                        upstream_id: owner,
                        owner_epoch: 0,
                        observed_at_unix_secs: 1_700_000_000,
                        expires_at_unix_secs: None,
                    },
                );
            }
        }
    }

    async fn complete_gateway_turn(
        gateway: &mut ResponsesWebSocketGateway,
        dispatch: &WebSocketDispatch,
        conn_id: u64,
        upstream_id: Uuid,
        upstream_name: &str,
        response_id: &str,
    ) {
        gateway.assign_connection(dispatch.turn_id, conn_id, upstream_id, upstream_name);
        gateway.commit_send_attempt(dispatch.turn_id);
        let _ = gateway.sent_with_timings(
            dispatch.turn_id,
            ResponsesDispatchTimings::reused(),
            std::time::Instant::now(),
        );
        let created = serde_json::to_vec(&serde_json::json!({
            "type": "response.created",
            "response": {"id": response_id, "status": "in_progress", "store": false}
        }))
        .unwrap();
        gateway
            .provider_message(conn_id, &Bytes::from(created))
            .await
            .unwrap();
        let completed = serde_json::to_vec(&serde_json::json!({
            "type": "response.completed",
            "response": {
                "id": response_id,
                "status": "completed",
                "store": false,
                "usage": {"input_tokens": 10, "output_tokens": 5}
            }
        }))
        .unwrap();
        gateway
            .provider_message(conn_id, &Bytes::from(completed))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn prewarm_uses_session_hint_to_route_to_existing_thread_owner() {
        let fixture = PrewarmMultiUpstreamFixture::new().await;

        // Upstream B has lower utilization than A, so unhinted turns choose B.
        // Establish a non-prewarm turn on upstream A with prompt_cache_key "S".
        let mut gateway_owner = fixture.gateway(HeaderMap::new());
        let seed_frame = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "type": "response.create",
                "model": "gpt-5",
                "prompt_cache_key": "session-S",
                "store": false,
                "input": [{"role": "user", "content": "hi"}]
            }))
            .unwrap(),
        );
        let seed_dispatch = gateway_owner
            .client_message(fixture.auth.clone(), seed_frame)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(seed_dispatch.candidates[0].upstream_id, fixture.upstream_b);
        // Simulate failover / assignment to upstream A on the seed turn.
        complete_gateway_turn(
            &mut gateway_owner,
            &seed_dispatch,
            1,
            fixture.upstream_a,
            "oauth-a",
            "resp_seed_a",
        )
        .await;

        // A fresh WebSocket gateway (e.g., subagent / reconnect) sends a prewarm with the same
        // prompt_cache_key; the session hint must steer it proactively to upstream A.
        let mut gateway_prewarm = fixture.gateway(HeaderMap::new());
        let prewarm_frame = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "type": "response.create",
                "model": "gpt-5",
                "generate": false,
                "prompt_cache_key": "session-S",
                "store": false,
                "input": []
            }))
            .unwrap(),
        );
        let prewarm_dispatch = gateway_prewarm
            .client_message(fixture.auth.clone(), prewarm_frame)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            prewarm_dispatch.candidates[0].upstream_id,
            fixture.upstream_a
        );
    }

    #[tokio::test]
    async fn prewarm_on_other_upstream_recovers_via_previous_response_not_found_and_session_hint_yields_to_affinity()
     {
        let fixture = PrewarmMultiUpstreamFixture::new().await;
        let owned_cipher = "gAAAAAB_owned_by_a";
        fixture.seed_encrypted_content_owner(owned_cipher, fixture.upstream_a);

        // Fresh gateway without a session hint sends a prewarm -> routes to B (lower utilization).
        let mut gateway = fixture.gateway(HeaderMap::new());
        let turn1_prewarm = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "type": "response.create",
                "model": "gpt-5",
                "generate": false,
                "prompt_cache_key": "thread-prewarm-mismatch",
                "store": false,
                "input": []
            }))
            .unwrap(),
        );
        let d1 = gateway
            .client_message(fixture.auth.clone(), turn1_prewarm)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(d1.candidates[0].upstream_id, fixture.upstream_b);
        complete_gateway_turn(
            &mut gateway,
            &d1,
            10,
            fixture.upstream_b,
            "oauth-b",
            "resp_prewarm_b",
        )
        .await;
        assert_eq!(gateway.session_upstream, Some(fixture.upstream_b));

        // Turn 2 sends previous_response_id = resp_prewarm_b alongside encrypted_content owned by A.
        // Because the previous owner was a prewarm, it must return previous_response_not_found (400)
        // so Codex retries cleanly without previous_response_id.
        let turn2_conflict = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "type": "response.create",
                "model": "gpt-5",
                "previous_response_id": "resp_prewarm_b",
                "prompt_cache_key": "thread-prewarm-mismatch",
                "store": false,
                "input": [{
                    "type": "reasoning",
                    "id": "rs_seeded",
                    "summary": [],
                    "content": [],
                    "encrypted_content": owned_cipher
                }]
            }))
            .unwrap(),
        );
        let err = gateway
            .client_message(fixture.auth.clone(), turn2_conflict)
            .await
            .expect_err("prewarm owner vs durable affinity conflict must return previous_response_not_found");
        assert_eq!(err.status, 400);
        assert_eq!(err.code, "previous_response_not_found");

        // Turn 3 drops previous_response_id on the SAME gateway (where session_upstream == Some(B));
        // SessionHint(B) must yield to durable affinity A, and assigning A must update session_upstream.
        let turn3_retry = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "type": "response.create",
                "model": "gpt-5",
                "prompt_cache_key": "thread-prewarm-mismatch",
                "store": false,
                "input": [{
                    "type": "reasoning",
                    "id": "rs_seeded",
                    "summary": [],
                    "content": [],
                    "encrypted_content": owned_cipher
                }]
            }))
            .unwrap(),
        );
        let d3 = gateway
            .client_message(fixture.auth.clone(), turn3_retry)
            .await
            .unwrap()
            .unwrap();
        let candidate_ids: Vec<Uuid> = d3.candidates.iter().map(|c| c.upstream_id).collect();
        assert_eq!(candidate_ids, vec![fixture.upstream_a]);

        gateway.assign_connection(d3.turn_id, 11, fixture.upstream_a, "oauth-a");
        assert_eq!(gateway.session_upstream, Some(fixture.upstream_a));
    }

    #[tokio::test]
    async fn non_prewarm_owner_conflict_still_returns_upstream_affinity_unavailable() {
        let fixture = PrewarmMultiUpstreamFixture::new().await;
        let owned_cipher = "gAAAAAB_owned_by_a_real";
        fixture.seed_encrypted_content_owner(owned_cipher, fixture.upstream_a);

        let mut gateway = fixture.gateway(HeaderMap::new());
        let turn1_real = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "type": "response.create",
                "model": "gpt-5",
                "store": false,
                "input": [{"role": "user", "content": "real turn on B"}]
            }))
            .unwrap(),
        );
        let d1 = gateway
            .client_message(fixture.auth.clone(), turn1_real)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(d1.candidates[0].upstream_id, fixture.upstream_b);
        complete_gateway_turn(
            &mut gateway,
            &d1,
            20,
            fixture.upstream_b,
            "oauth-b",
            "resp_real_b",
        )
        .await;

        let turn2_conflict = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "type": "response.create",
                "model": "gpt-5",
                "previous_response_id": "resp_real_b",
                "store": false,
                "input": [{
                    "type": "reasoning",
                    "id": "rs_seeded",
                    "summary": [],
                    "content": [],
                    "encrypted_content": owned_cipher
                }]
            }))
            .unwrap(),
        );
        let err = gateway
            .client_message(fixture.auth.clone(), turn2_conflict)
            .await
            .expect_err("non-prewarm owner vs affinity conflict must still return upstream_affinity_unavailable");
        assert_eq!(err.status, 400);
        assert_eq!(err.code, "upstream_affinity_unavailable");
    }

    #[tokio::test]
    async fn prewarm_ignores_session_hint_when_hinted_upstream_is_hard_blocked() {
        let fixture = PrewarmMultiUpstreamFixture::new().await;

        let mut headers = HeaderMap::new();
        headers.insert("session_id", "blocked-session-S".parse().unwrap());
        fixture.lifecycle.record_openai_session_hint(
            fixture.auth.principal_id(),
            &headers,
            &serde_json::json!({
                "model": "gpt-5",
                "prompt_cache_key": "blocked-hint-key"
            }),
            fixture.upstream_a,
        );

        // Mark upstream A hard-blocked (5h=100% + 7d=100% rejected, credits depleted).
        {
            let mut overage = prewarm_quota_snapshot("overage", 1.0, "rejected", 1_700_600_000);
            overage.utilization = None;
            overage.status = None;
            overage.fallback_available = Some(false);
            overage.credits = Some(cc_lb_domain::SubscriptionQuotaCredits {
                has_credits: Some(false),
                unlimited: Some(false),
                limit_reached: Some(true),
                ..Default::default()
            });
            let mut guard = fixture
                .quota_cache
                .snapshots
                .lock()
                .expect("quota cache lock");
            guard.insert(
                fixture.upstream_a,
                vec![
                    prewarm_quota_snapshot("5h", 1.0, "rejected", 1_700_018_000),
                    prewarm_quota_snapshot("7d", 1.0, "rejected", 1_700_600_000),
                    overage,
                ],
            );
        }

        let mut gateway = fixture.gateway(headers);
        let prewarm_frame = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "type": "response.create",
                "model": "gpt-5",
                "generate": false,
                "prompt_cache_key": "blocked-hint-key",
                "store": false,
                "input": []
            }))
            .unwrap(),
        );
        let dispatch = gateway
            .client_message(fixture.auth.clone(), prewarm_frame)
            .await
            .expect("prewarm must fall back cleanly instead of failing on blocked hint")
            .unwrap();
        assert_eq!(dispatch.candidates[0].upstream_id, fixture.upstream_b);
    }

    #[tokio::test]
    async fn failed_or_interrupted_ws_turn_does_not_record_session_hint() {
        let fixture = PrewarmMultiUpstreamFixture::new().await;

        // 1. A turn that receives response.created on upstream A but terminates with response.failed
        // must NOT persist a session hint pointing future prewarms to failed upstream A.
        let mut gateway_failed = fixture.gateway(HeaderMap::new());
        let failed_turn_frame = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "type": "response.create",
                "model": "gpt-5",
                "prompt_cache_key": "failed-session-S",
                "store": false,
                "input": [{"role": "user", "content": "hi"}]
            }))
            .unwrap(),
        );
        let d_failed = gateway_failed
            .client_message(fixture.auth.clone(), failed_turn_frame)
            .await
            .unwrap()
            .unwrap();
        gateway_failed.assign_connection(d_failed.turn_id, 1, fixture.upstream_a, "oauth-a");
        gateway_failed.commit_send_attempt(d_failed.turn_id);
        let _ = gateway_failed.sent_with_timings(
            d_failed.turn_id,
            ResponsesDispatchTimings::reused(),
            std::time::Instant::now(),
        );
        gateway_failed
            .provider_message(
                1,
                &Bytes::from_static(
                    br#"{"type":"response.created","response":{"id":"resp_fail_a","status":"in_progress","store":false}}"#,
                ),
            )
            .await
            .unwrap();
        gateway_failed
            .provider_message(
                1,
                &Bytes::from_static(
                    br#"{"type":"response.failed","response":{"id":"resp_fail_a","status":"failed","error":{"code":"server_error","message":"boom"}}}"#,
                ),
            )
            .await
            .unwrap();

        let mut gateway_prewarm_after_fail = fixture.gateway(HeaderMap::new());
        let prewarm_after_fail = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "type": "response.create",
                "model": "gpt-5",
                "generate": false,
                "prompt_cache_key": "failed-session-S",
                "store": false,
                "input": []
            }))
            .unwrap(),
        );
        let d_prewarm_fail = gateway_prewarm_after_fail
            .client_message(fixture.auth.clone(), prewarm_after_fail)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(d_prewarm_fail.candidates[0].upstream_id, fixture.upstream_b);

        // 2. A turn that receives response.created on upstream A and disconnects before a clean
        // terminal must also leave the session unhinted.
        let mut gateway_closed = fixture.gateway(HeaderMap::new());
        let closed_turn_frame = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "type": "response.create",
                "model": "gpt-5",
                "prompt_cache_key": "closed-session-S",
                "store": false,
                "input": [{"role": "user", "content": "hi"}]
            }))
            .unwrap(),
        );
        let d_closed = gateway_closed
            .client_message(fixture.auth.clone(), closed_turn_frame)
            .await
            .unwrap()
            .unwrap();
        gateway_closed.assign_connection(d_closed.turn_id, 2, fixture.upstream_a, "oauth-a");
        gateway_closed.commit_send_attempt(d_closed.turn_id);
        let _ = gateway_closed.sent_with_timings(
            d_closed.turn_id,
            ResponsesDispatchTimings::reused(),
            std::time::Instant::now(),
        );
        gateway_closed
            .provider_message(
                2,
                &Bytes::from_static(
                    br#"{"type":"response.created","response":{"id":"resp_close_a","status":"in_progress","store":false}}"#,
                ),
            )
            .await
            .unwrap();
        gateway_closed.connection_closed(2, "upstream reset mid-stream");

        let mut gateway_prewarm_after_close = fixture.gateway(HeaderMap::new());
        let prewarm_after_close = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "type": "response.create",
                "model": "gpt-5",
                "generate": false,
                "prompt_cache_key": "closed-session-S",
                "store": false,
                "input": []
            }))
            .unwrap(),
        );
        let d_prewarm_close = gateway_prewarm_after_close
            .client_message(fixture.auth.clone(), prewarm_after_close)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            d_prewarm_close.candidates[0].upstream_id,
            fixture.upstream_b
        );
    }

    #[tokio::test]
    async fn queued_prewarm_reapplies_session_upstream_when_first_turn_establishes_owner_and_blocks_non_owner_while_healthy()
     {
        let fixture = PrewarmMultiUpstreamFixture::new().await;
        let mut gateway = fixture.gateway(fixture.headers.clone());

        // Turn 1 admits before `session_upstream` is established -> initial route picks B.
        let turn1 = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "type": "response.create",
                "model": "gpt-5",
                "stream_id": "lane-1",
                "store": false,
                "input": [{"role": "user", "content": "first"}]
            }))
            .unwrap(),
        );
        let d1 = gateway
            .client_message(fixture.auth.clone(), turn1)
            .await
            .unwrap()
            .expect("turn 1 dispatches immediately");

        // Turn 2 (`generate: false`) arrives on the same lane while Turn 1 is still active,
        // so it queues with pre-pin candidates [B, A].
        let turn2_prewarm = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "type": "response.create",
                "model": "gpt-5",
                "stream_id": "lane-1",
                "generate": false,
                "store": false,
                "input": []
            }))
            .unwrap(),
        );
        assert!(
            gateway
                .client_message(fixture.auth.clone(), turn2_prewarm)
                .await
                .unwrap()
                .is_none(),
            "second create on busy lane must queue"
        );

        // Turn 1 completes on OAuth upstream A, setting `session_upstream = Some(A)` and popping Turn 2.
        gateway.assign_connection(d1.turn_id, 1, fixture.upstream_a, "oauth-a");
        gateway.commit_send_attempt(d1.turn_id);
        let _ = gateway.sent_with_timings(
            d1.turn_id,
            ResponsesDispatchTimings::reused(),
            std::time::Instant::now(),
        );
        let created = serde_json::to_vec(&serde_json::json!({
            "type": "response.created",
            "stream_id": "lane-1",
            "response": {"id": "resp_owner_a", "status": "in_progress", "store": false}
        }))
        .unwrap();
        let _ = gateway
            .provider_message(1, &Bytes::from(created))
            .await
            .unwrap();
        let completed = serde_json::to_vec(&serde_json::json!({
            "type": "response.completed",
            "stream_id": "lane-1",
            "response": {
                "id": "resp_owner_a",
                "status": "completed",
                "store": false,
                "usage": {"input_tokens": 10, "output_tokens": 5}
            }
        }))
        .unwrap();
        let popped = gateway
            .provider_message(1, &Bytes::from(completed))
            .await
            .unwrap();
        let d2 = popped
            .dispatches
            .into_iter()
            .find(|dispatch| dispatch.turn_id != d1.turn_id && !dispatch.control)
            .expect("completing turn 1 pops queued turn 2");
        assert_eq!(
            d2.candidates[0].upstream_id, fixture.upstream_a,
            "queued generate:false turn must reapply session owner A at index 0"
        );
        let non_owner_err = gateway
            .validate_candidate_for_send(d2.turn_id, fixture.upstream_b, None, false)
            .await
            .expect_err("healthy owner A must prevent queued generate:false from dialing B");
        assert_eq!(non_owner_err.code, "upstream_affinity_unavailable");
        gateway
            .validate_candidate_for_send(d2.turn_id, fixture.upstream_a, None, true)
            .await
            .expect("pinned owner A validates cleanly");
    }

    #[tokio::test]
    async fn queued_prewarm_falls_back_to_alternate_oauth_account_when_session_owner_becomes_disabled()
     {
        let fixture = PrewarmMultiUpstreamFixture::new().await;
        let mut gateway = fixture.gateway(fixture.headers.clone());

        let turn1 = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "type": "response.create",
                "model": "gpt-5",
                "stream_id": "lane-1",
                "store": false,
                "input": [{"role": "user", "content": "first"}]
            }))
            .unwrap(),
        );
        let d1 = gateway
            .client_message(fixture.auth.clone(), turn1)
            .await
            .unwrap()
            .unwrap();
        let turn2_prewarm = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "type": "response.create",
                "model": "gpt-5",
                "stream_id": "lane-1",
                "generate": false,
                "store": false,
                "input": []
            }))
            .unwrap(),
        );
        assert!(
            gateway
                .client_message(fixture.auth.clone(), turn2_prewarm)
                .await
                .unwrap()
                .is_none()
        );

        gateway.assign_connection(d1.turn_id, 1, fixture.upstream_a, "oauth-a");
        gateway.commit_send_attempt(d1.turn_id);
        let _ = gateway.sent_with_timings(
            d1.turn_id,
            ResponsesDispatchTimings::reused(),
            std::time::Instant::now(),
        );
        let created = serde_json::to_vec(&serde_json::json!({
            "type": "response.created",
            "stream_id": "lane-1",
            "response": {"id": "resp_disable_a", "status": "in_progress", "store": false}
        }))
        .unwrap();
        let _ = gateway
            .provider_message(1, &Bytes::from(created))
            .await
            .unwrap();
        let completed = serde_json::to_vec(&serde_json::json!({
            "type": "response.completed",
            "stream_id": "lane-1",
            "response": {
                "id": "resp_disable_a",
                "status": "completed",
                "store": false,
                "usage": {"input_tokens": 10, "output_tokens": 5}
            }
        }))
        .unwrap();
        let popped = gateway
            .provider_message(1, &Bytes::from(completed))
            .await
            .unwrap();
        let d2 = popped
            .dispatches
            .into_iter()
            .find(|dispatch| dispatch.turn_id != d1.turn_id && !dispatch.control)
            .expect("queued turn 2 pops");

        // Now disable session owner A; existing availability fallback lets generate:false use B.
        fixture.disable_upstream(fixture.upstream_a);
        assert!(
            d2.candidates
                .iter()
                .any(|cand| cand.upstream_id == fixture.upstream_b),
            "prewarm dispatch must retain fallback candidate B"
        );
        gateway
            .validate_candidate_for_send(d2.turn_id, fixture.upstream_b, None, false)
            .await
            .expect("disabled owner A must allow prewarm fallback to B");
    }

    #[tokio::test]
    async fn queued_turn_with_explicit_response_affinity_preserves_target_after_session_pin() {
        let fixture = PrewarmMultiUpstreamFixture::new().await;
        let owned_cipher = "gAAAAAB_explicit_b_owner";
        fixture.seed_encrypted_content_owner(owned_cipher, fixture.upstream_b);

        let mut gateway = fixture.gateway(fixture.headers.clone());
        let turn1 = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "type": "response.create",
                "model": "gpt-5",
                "stream_id": "lane-1",
                "store": false,
                "input": [{"role": "user", "content": "first"}]
            }))
            .unwrap(),
        );
        let d1 = gateway
            .client_message(fixture.auth.clone(), turn1)
            .await
            .unwrap()
            .unwrap();

        // Turn 2 has explicit encrypted-content affinity to B while Turn 1 establishes owner A.
        let turn2_affinity = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "type": "response.create",
                "model": "gpt-5",
                "stream_id": "lane-1",
                "store": false,
                "input": [{
                    "type": "reasoning",
                    "id": "rs_queued_b",
                    "summary": [],
                    "content": [],
                    "encrypted_content": owned_cipher
                }]
            }))
            .unwrap(),
        );
        assert!(
            gateway
                .client_message(fixture.auth.clone(), turn2_affinity)
                .await
                .unwrap()
                .is_none()
        );

        gateway.assign_connection(d1.turn_id, 1, fixture.upstream_a, "oauth-a");
        gateway.commit_send_attempt(d1.turn_id);
        let _ = gateway.sent_with_timings(
            d1.turn_id,
            ResponsesDispatchTimings::reused(),
            std::time::Instant::now(),
        );
        let created = serde_json::to_vec(&serde_json::json!({
            "type": "response.created",
            "stream_id": "lane-1",
            "response": {"id": "resp_explicit_a", "status": "in_progress", "store": false}
        }))
        .unwrap();
        let _ = gateway
            .provider_message(1, &Bytes::from(created))
            .await
            .unwrap();
        let completed = serde_json::to_vec(&serde_json::json!({
            "type": "response.completed",
            "stream_id": "lane-1",
            "response": {
                "id": "resp_explicit_a",
                "status": "completed",
                "store": false,
                "usage": {"input_tokens": 10, "output_tokens": 5}
            }
        }))
        .unwrap();
        let popped = gateway
            .provider_message(1, &Bytes::from(completed))
            .await
            .unwrap();
        let d2 = popped
            .dispatches
            .into_iter()
            .find(|dispatch| dispatch.turn_id != d1.turn_id && !dispatch.control)
            .expect("queued explicit-affinity turn pops");
        assert_eq!(
            d2.candidates.len(),
            1,
            "explicit response affinity must keep its target candidate list intact"
        );
        assert_eq!(d2.candidates[0].upstream_id, fixture.upstream_b);
        gateway
            .validate_candidate_for_send(d2.turn_id, fixture.upstream_b, None, false)
            .await
            .expect("explicit affinity to B must not be overridden by session_upstream A");
    }

    #[tokio::test]
    async fn queued_turn_with_unestablished_reasoning_preserves_ciphertext_after_session_pin() {
        let fixture = PrewarmMultiUpstreamFixture::new().await;
        let unknown_cipher = "gAAAAAB_unestablished_queued_cipher";
        let turn_with_reasoning = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "type": "response.create",
                "model": "gpt-5",
                "stream_id": "lane-1",
                "store": false,
                "input": [
                    {
                        "type": "reasoning",
                        "id": "rs_unest",
                        "summary": [],
                        "content": [],
                        "encrypted_content": unknown_cipher
                    },
                    {"role": "user", "content": "hi"}
                ]
            }))
            .unwrap(),
        );

        // Part 1: Unpinned multi-upstream turn without a session pin strips upfront.
        let mut unpinned_gw = fixture.gateway(fixture.headers.clone());
        let d_unpinned = unpinned_gw
            .client_message(fixture.auth.clone(), turn_with_reasoning.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(d_unpinned.candidates.len(), 2);
        let unpinned_text = std::str::from_utf8(&d_unpinned.frame).unwrap();
        assert!(
            !unpinned_text.contains(unknown_cipher),
            "unpinned multi-candidate turn without session owner must strip upfront: {unpinned_text}"
        );

        // Part 2: Turn 2 queued before Turn 1 establishes session_upstream must preserve the
        // original ciphertext verbatim and arm RetryStripAll once pinned to session_upstream A.
        let mut gateway = fixture.gateway(fixture.headers.clone());
        let turn1 = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "type": "response.create",
                "model": "gpt-5",
                "stream_id": "lane-1",
                "store": false,
                "input": [{"role": "user", "content": "first"}]
            }))
            .unwrap(),
        );
        let d1 = gateway
            .client_message(fixture.auth.clone(), turn1)
            .await
            .unwrap()
            .unwrap();
        assert!(
            gateway
                .client_message(fixture.auth.clone(), turn_with_reasoning)
                .await
                .unwrap()
                .is_none()
        );

        gateway.assign_connection(d1.turn_id, 1, fixture.upstream_a, "oauth-a");
        gateway.commit_send_attempt(d1.turn_id);
        let _ = gateway.sent_with_timings(
            d1.turn_id,
            ResponsesDispatchTimings::reused(),
            std::time::Instant::now(),
        );
        let created = serde_json::to_vec(&serde_json::json!({
            "type": "response.created",
            "stream_id": "lane-1",
            "response": {"id": "resp_pin_a", "status": "in_progress", "store": false}
        }))
        .unwrap();
        let _ = gateway
            .provider_message(1, &Bytes::from(created))
            .await
            .unwrap();
        let completed = serde_json::to_vec(&serde_json::json!({
            "type": "response.completed",
            "stream_id": "lane-1",
            "response": {
                "id": "resp_pin_a",
                "status": "completed",
                "store": false,
                "usage": {"input_tokens": 10, "output_tokens": 5}
            }
        }))
        .unwrap();
        let popped = gateway
            .provider_message(1, &Bytes::from(completed))
            .await
            .unwrap();
        let d2 = popped
            .dispatches
            .into_iter()
            .find(|dispatch| dispatch.turn_id != d1.turn_id && !dispatch.control)
            .expect("queued turn 2 pops");
        assert_eq!(d2.candidates.len(), 1);
        assert_eq!(d2.candidates[0].upstream_id, fixture.upstream_a);
        let d2_text = std::str::from_utf8(&d2.frame).unwrap();
        assert!(
            d2_text.contains(unknown_cipher),
            "pinned single-owner queued turn must preserve ciphertext verbatim: {d2_text}"
        );

        // Sending a 400 invalid_encrypted_content triggers RetryStripAll once on the pinned turn.
        gateway.assign_connection(d2.turn_id, 1, fixture.upstream_a, "oauth-a");
        gateway.commit_send_attempt(d2.turn_id);
        let _ = gateway.sent_with_timings(
            d2.turn_id,
            ResponsesDispatchTimings::reused(),
            std::time::Instant::now(),
        );
        let invalid_err = serde_json::to_vec(&serde_json::json!({
            "type": "error",
            "status": 400,
            "error": {
                "type": "invalid_request_error",
                "code": "invalid_encrypted_content",
                "message": "The encrypted content could not be verified."
            }
        }))
        .unwrap();
        let retry = gateway
            .provider_message(1, &Bytes::from(invalid_err))
            .await
            .unwrap();
        assert!(!retry.forward);
        assert_eq!(retry.dispatches.len(), 1);
        let retry_text = std::str::from_utf8(&retry.dispatches[0].frame).unwrap();
        assert!(
            !retry_text.contains(unknown_cipher),
            "RetryStripAll must strip ciphertext on 400 invalid_encrypted_content: {retry_text}"
        );
    }

    #[tokio::test]
    async fn unestablished_reasoning_upfront_strip_uses_stripped_body_for_limit_admission_while_single_owner_keeps_verbatim()
     {
        use cc_lb_control::api_keys::builtin_authn::BuiltinAuthn;
        use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
        use cc_lb_control::api_keys::principal_view::{
            DialectCache, PrincipalView, RouterPipelineCache,
        };
        use cc_lb_control::dynamic_view::{DynamicViewBuilder, DynamicViewHolder};
        use cc_lb_domain::TerminalStrategy;
        use cc_lb_routing::FilterPlugin;
        use cc_lb_storage_api::upstream::{UpstreamKind as StorageRecordKind, UpstreamRecord};
        use cc_lb_upstream::{
            ApiKeyAwareSignerFactory, RetryDecision, ShapedRequest, SignedRequest, Signer,
            SignerError, SignerFactory, SigningCapability, UpstreamError,
        };

        struct DummySignerFactory;
        impl ApiKeyAwareSignerFactory for DummySignerFactory {
            fn with_router_choice(&self, _name: String) -> Arc<dyn SignerFactory> {
                Arc::new(Self)
            }
        }
        #[async_trait::async_trait]
        impl SignerFactory for DummySignerFactory {
            async fn build(
                &self,
                _upstream: &cc_lb_domain::Upstream,
            ) -> Result<Arc<dyn Signer>, SignerError> {
                Ok(Arc::new(DummySigner))
            }
        }
        struct DummySigner;
        #[async_trait::async_trait]
        impl Signer for DummySigner {
            async fn sign(
                &self,
                shaped: ShapedRequest,
                capability: &mut SigningCapability,
            ) -> Result<SignedRequest, SignerError> {
                Ok(SignedRequest::from_shaped(shaped, capability))
            }
            async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
                RetryDecision::Fail
            }
        }
        struct UnusedDispatcher;
        #[async_trait::async_trait]
        impl crate::UpstreamDispatch for UnusedDispatcher {
            async fn dispatch(
                &self,
                _request: SignedRequest,
            ) -> Result<http::Response<crate::Body>, crate::DispatchError> {
                unreachable!("unused dispatcher in limit admission test")
            }
        }

        const NOW_SECS: u64 = 1_700_000_000;
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(NOW_SECS));
        let storage = Arc::new(
            cc_lb_storage_sqlite::open_sqlite("sqlite::memory:", clock.clone())
                .await
                .expect("open in-memory limit test db"),
        );
        storage.initialize().await.expect("migrate limit test db");
        let keys = Arc::new(KeyStore::new(storage));
        let (_record, secret) = keys
            .create(
                DIRECT_PRINCIPAL,
                CreateParams {
                    label: "reasoning-limit-test".to_owned(),
                    description: None,
                    expires_at_unix_secs: None,
                    limit_overrides: Vec::new(),
                },
            )
            .await
            .expect("issue limit fixture key");
        let filter: Arc<dyn FilterPlugin> = Arc::new(
            crate::builtin_filters::subscription_preference::SubscriptionPreferenceFilter::with_clock(
                clock.clone(),
            ),
        );
        let pipeline = Arc::new(RouterPipelineCache {
            user_filters: vec![filter],
            terminal: TerminalStrategy::FirstPick,
            instantiation_error: None,
            provider_filters: None,
        });
        let mut chains = HashMap::new();
        chains.insert(
            DIRECT_PRINCIPAL.to_owned(),
            (Some(pipeline), DialectCache::Inherit),
        );
        let view = Arc::new(PrincipalView::for_tests(
            DIRECT_PRINCIPAL,
            true,
            vec!["*".to_owned()],
            vec![Limit {
                kind: LimitKind::TotalTokens,
                window_secs: 60,
                cap_micros: 4_500,
            }],
            chains,
        ));
        let authn = Arc::new(BuiltinAuthn::new(keys, clock.clone()));
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-api-key",
            secret.expose().parse().expect("fixture key header"),
        );
        let auth = crate::authenticate_first(&authn, &headers, &view)
            .await
            .expect("authenticate limit fixture");

        let upstream_a = Uuid::from_u128(0xAA01);
        let upstream_b = Uuid::from_u128(0xBB02);
        let upstreams = vec![
            UpstreamRecord {
                id: upstream_a,
                name: "oauth-a".to_owned(),
                kind: StorageRecordKind::OpenaiOauth,
                enabled: true,
                ..Default::default()
            },
            UpstreamRecord {
                id: upstream_b,
                name: "oauth-b".to_owned(),
                kind: StorageRecordKind::OpenaiOauth,
                enabled: true,
                ..Default::default()
            },
        ];
        let quota_cache = Arc::new(PrewarmQuotaCache::default());
        {
            let mut guard = quota_cache.snapshots.lock().expect("quota cache lock");
            guard.insert(
                upstream_a,
                vec![
                    prewarm_quota_snapshot("5h", 0.2, "allowed", NOW_SECS + 10_000),
                    prewarm_quota_snapshot("7d", 0.2, "allowed", NOW_SECS + 100_000),
                ],
            );
            guard.insert(
                upstream_b,
                vec![
                    prewarm_quota_snapshot("5h", 0.1, "allowed", NOW_SECS + 10_000),
                    prewarm_quota_snapshot("7d", 0.1, "allowed", NOW_SECS + 100_000),
                ],
            );
        }
        let dynamic = DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(DummySignerFactory))
            .principal_view(view.clone())
            .subscription_quota_cache(quota_cache.clone())
            .subscription_quota_routing_max_staleness_secs(60)
            .upstream_records(upstreams)
            .build();
        let dynamic_view = Arc::new(DynamicViewHolder::new(dynamic));
        let concurrency = Arc::new(KeyConcurrencyManager::new());
        let engine = LimitEngine::new(concurrency, clock.clone());
        let affinity_store = Arc::new(PrewarmAffinityStore::default());
        let config = crate::LifecycleConfig {
            unknown_openai_reasoning: cc_lb_domain::UnknownOpenaiReasoningPolicy::Hybrid,
            ..Default::default()
        };
        let lifecycle = Arc::new(
            Lifecycle::new_with_dynamic_view(
                authn.clone(),
                Arc::clone(&dynamic_view),
                Arc::new(UnusedDispatcher),
                config,
                clock,
            )
            .with_upstream_affinity_store(affinity_store)
            .with_limit_engine(engine, authn),
        );

        let bloated_cipher = format!("gAAAAAB_bloated_{}", "X".repeat(20_000));
        let frame = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "type": "response.create",
                "model": "gpt-5",
                "store": false,
                "max_output_tokens": 16,
                "input": [
                    {
                        "type": "reasoning",
                        "id": "rs_bloated",
                        "summary": [],
                        "content": [],
                        "encrypted_content": bloated_cipher
                    },
                    {"role": "user", "content": "hi"}
                ]
            }))
            .unwrap(),
        );

        // Multi-upstream unpinned turn strips upfront before LimitRequest and passes the 200-token cap.
        let mut multi_gw = ResponsesWebSocketGateway::new(
            lifecycle.clone(),
            &auth,
            headers.clone(),
            1_024 * 1_024,
            16,
        )
        .with_codex_backend(true);
        let dispatch = multi_gw
            .client_message(auth.clone(), frame.clone())
            .await
            .expect("stripped multi-upstream turn must pass tight token limit")
            .expect("dispatch returned");
        assert!(dispatch.candidates.len() > 1);
        let sent_text = std::str::from_utf8(&dispatch.frame).unwrap();
        assert!(!sent_text.contains(&bloated_cipher));

        // Single healthy upstream keeps bloated encrypted_content verbatim (RetryStripAll), so
        // LimitRequest counts the retained encrypted bytes and rejects at 429 `limit_rejected`.
        let single_upstreams = vec![UpstreamRecord {
            id: upstream_a,
            name: "oauth-a".to_owned(),
            kind: StorageRecordKind::OpenaiOauth,
            enabled: true,
            ..Default::default()
        }];
        let current = dynamic_view.load();
        dynamic_view.store(
            DynamicViewBuilder::from_view(&current)
                .upstream_records(single_upstreams)
                .build(),
        );
        let mut single_gw =
            ResponsesWebSocketGateway::new(lifecycle, &auth, headers, 1_024 * 1_024, 16)
                .with_codex_backend(true);
        let err = single_gw
            .client_message(auth, frame)
            .await
            .expect_err("single-owner verbatim body exceeds 200-token limit");
        assert_eq!(err.status, 429);
        assert_eq!(err.code, "limit_rejected");
    }

    async fn setup_deferred_ws_fixture(
        limits: Vec<Limit>,
    ) -> (
        Arc<Lifecycle>,
        crate::Authenticated,
        HeaderMap,
        Uuid,
        Uuid,
        Uuid,
        Arc<std::sync::atomic::AtomicUsize>,
    ) {
        use cc_lb_control::api_keys::builtin_authn::BuiltinAuthn;
        use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
        use cc_lb_control::api_keys::principal_view::{
            DialectCache, PrincipalView, RouterPipelineCache,
        };
        use cc_lb_control::dynamic_view::{DynamicViewBuilder, DynamicViewHolder};
        use cc_lb_domain::TerminalStrategy;
        use cc_lb_storage_api::upstream::{UpstreamKind as StorageRecordKind, UpstreamRecord};
        use cc_lb_upstream::{
            ApiKeyAwareSignerFactory, RetryDecision, ShapedRequest, SignedRequest, Signer,
            SignerError, SignerFactory, SigningCapability, UpstreamError,
        };
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct DummySignerFactory {
            build_calls: Arc<AtomicUsize>,
        }
        impl ApiKeyAwareSignerFactory for DummySignerFactory {
            fn with_router_choice(&self, _name: String) -> Arc<dyn SignerFactory> {
                Arc::new(Self {
                    build_calls: Arc::clone(&self.build_calls),
                })
            }
        }
        #[async_trait::async_trait]
        impl SignerFactory for DummySignerFactory {
            async fn build(
                &self,
                _upstream: &cc_lb_domain::Upstream,
            ) -> Result<Arc<dyn Signer>, SignerError> {
                self.build_calls.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(DummySigner))
            }
        }
        struct DummySigner;
        #[async_trait::async_trait]
        impl Signer for DummySigner {
            async fn sign(
                &self,
                shaped: ShapedRequest,
                capability: &mut SigningCapability,
            ) -> Result<SignedRequest, SignerError> {
                Ok(SignedRequest::from_shaped(shaped, capability))
            }
            async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
                RetryDecision::Fail
            }
        }
        struct UnusedDispatcher;
        #[async_trait::async_trait]
        impl crate::UpstreamDispatch for UnusedDispatcher {
            async fn dispatch(
                &self,
                _request: SignedRequest,
            ) -> Result<http::Response<crate::Body>, crate::DispatchError> {
                unreachable!("unused dispatcher in deferred ws test")
            }
        }

        const NOW_SECS: u64 = 1_700_000_000;
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(NOW_SECS));
        let storage = Arc::new(
            cc_lb_storage_sqlite::open_sqlite("sqlite::memory:", clock.clone())
                .await
                .expect("open in-memory deferred ws test db"),
        );
        storage
            .initialize()
            .await
            .expect("migrate deferred ws test db");
        let keys = Arc::new(KeyStore::new(storage));
        let (_record, secret) = keys
            .create(
                DIRECT_PRINCIPAL,
                CreateParams {
                    label: "deferred-ws-test".to_owned(),
                    description: None,
                    expires_at_unix_secs: None,
                    limit_overrides: Vec::new(),
                },
            )
            .await
            .expect("issue deferred ws test key");
        let pipeline = Arc::new(RouterPipelineCache {
            user_filters: Vec::new(),
            terminal: TerminalStrategy::FirstPick,
            instantiation_error: None,
            provider_filters: None,
        });
        let mut chains = HashMap::new();
        chains.insert(
            DIRECT_PRINCIPAL.to_owned(),
            (Some(pipeline), DialectCache::Inherit),
        );
        let view = Arc::new(PrincipalView::for_tests(
            DIRECT_PRINCIPAL,
            true,
            vec!["*".to_owned()],
            limits,
            chains,
        ));
        let authn = Arc::new(BuiltinAuthn::new(keys, clock.clone()));
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-api-key",
            secret.expose().parse().expect("fixture key header"),
        );
        let auth = crate::authenticate_first(&authn, &headers, &view)
            .await
            .expect("authenticate deferred ws caller");

        let api_key_id = Uuid::from_u128(0x1111);
        let oauth_a_id = Uuid::from_u128(0x2222);
        let oauth_b_id = Uuid::from_u128(0x3333);
        let upstreams = vec![
            UpstreamRecord {
                id: api_key_id,
                name: "api-key-primary".to_owned(),
                kind: StorageRecordKind::OpenaiApiKey,
                enabled: true,
                ..Default::default()
            },
            UpstreamRecord {
                id: oauth_a_id,
                name: "oauth-a".to_owned(),
                kind: StorageRecordKind::OpenaiOauth,
                enabled: true,
                ..Default::default()
            },
            UpstreamRecord {
                id: oauth_b_id,
                name: "oauth-b".to_owned(),
                kind: StorageRecordKind::OpenaiOauth,
                enabled: true,
                ..Default::default()
            },
        ];
        let build_calls = Arc::new(AtomicUsize::new(0));
        let dynamic = DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(DummySignerFactory {
                build_calls: Arc::clone(&build_calls),
            }))
            .principal_view(view)
            .upstream_records(upstreams)
            .build();
        let dynamic_view = Arc::new(DynamicViewHolder::new(dynamic));
        let concurrency = Arc::new(KeyConcurrencyManager::new());
        let engine = LimitEngine::new(concurrency, clock.clone());
        let affinity_store = Arc::new(PrewarmAffinityStore::default());
        let config = crate::LifecycleConfig {
            unknown_openai_reasoning: cc_lb_domain::UnknownOpenaiReasoningPolicy::Hybrid,
            ..Default::default()
        };
        let lifecycle = Arc::new(
            Lifecycle::new_with_dynamic_view(
                authn.clone(),
                Arc::clone(&dynamic_view),
                Arc::new(UnusedDispatcher),
                config,
                clock,
            )
            .with_upstream_affinity_store(affinity_store)
            .with_limit_engine(engine, authn),
        );
        (
            lifecycle,
            auth,
            headers,
            api_key_id,
            oauth_a_id,
            oauth_b_id,
            build_calls,
        )
    }

    #[tokio::test]
    async fn mixed_api_key_then_oauth_fallback_restores_verbatim_reasoning_when_pinned_to_session_owner()
     {
        use std::sync::atomic::Ordering;

        let (lifecycle, auth, headers, api_key_id, oauth_a_id, _oauth_b_id, build_calls) =
            setup_deferred_ws_fixture(vec![Limit {
                kind: LimitKind::TotalTokens,
                window_secs: 60,
                cap_micros: 4_500,
            }])
            .await;

        let bloated_cipher = format!("gAAAAAB_mixed_owner_{}", "Z".repeat(20_000));
        let reasoning_frame = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "type": "response.create",
                "model": "gpt-5",
                "store": false,
                "max_output_tokens": 16,
                "input": [
                    {
                        "type": "reasoning",
                        "id": "rs_mixed",
                        "summary": [],
                        "content": [],
                        "encrypted_content": bloated_cipher
                    },
                    {"role": "user", "content": "hi"}
                ]
            }))
            .unwrap(),
        );

        // Case 1: Mixed [api_key, oauth_a, oauth_b] holds a stripped baseline reservation at
        // admission; targeting api_key first strips encrypted_content and resizes within cap.
        let mut gw = ResponsesWebSocketGateway::new(
            lifecycle.clone(),
            &auth,
            headers.clone(),
            1_024 * 1_024,
            16,
        );
        let mut dispatch = gw
            .client_message(auth.clone(), reasoning_frame.clone())
            .await
            .expect("mixed turn admits with stripped baseline reservation")
            .expect("mixed dispatch returns");
        assert!(dispatch.candidates[0].is_api_key);
        assert!(
            gw.turns
                .get(&dispatch.turn_id)
                .unwrap()
                .prepared
                .reservation
                .is_some(),
            "deferred unestablished turn must still hold an active admission reservation"
        );
        assert!(
            gw.turns
                .get(&dispatch.turn_id)
                .unwrap()
                .prepared
                .deferred_limit_context
                .is_some(),
            "deferred limit context remains active until send commit"
        );
        gw.finalize_dispatch_reasoning(&mut dispatch, false)
            .expect("budget permits stripped copy");
        assert!(
            !std::str::from_utf8(&dispatch.frame)
                .unwrap()
                .contains(&bloated_cipher),
            "API-key attempt with multi-upstream fallback must strip unestablished reasoning"
        );
        gw.validate_candidate_for_send(dispatch.turn_id, api_key_id, None, true)
            .await
            .expect("stripped API-key payload must pass resized limit");
        gw.fail_dispatch(
            dispatch.turn_id,
            &WebSocketError::new(502, "test_cleanup", "cleanup"),
        )
        .await;

        // Case 2: If API-key attempt failed before commit and session pins to OAuth A,
        // finalizing as single-owner restores the original ciphertext verbatim, arms
        // RetryStripAll, and rejects in PreparedWsConnectJob::run() BEFORE signing/dialing.
        let mut fallback_gw =
            ResponsesWebSocketGateway::new(lifecycle, &auth, headers, 1_024 * 1_024, 16);
        let mut d_fallback = fallback_gw
            .client_message(auth, reasoning_frame)
            .await
            .unwrap()
            .unwrap();
        fallback_gw
            .finalize_dispatch_reasoning(&mut d_fallback, false)
            .unwrap();
        assert!(
            !std::str::from_utf8(&d_fallback.frame)
                .unwrap()
                .contains(&bloated_cipher)
        );
        fallback_gw
            .finalize_dispatch_reasoning(&mut d_fallback, true)
            .unwrap();
        assert!(
            std::str::from_utf8(&d_fallback.frame)
                .unwrap()
                .contains(&bloated_cipher),
            "switching to pinned single OAuth owner before commit must restore verbatim ciphertext"
        );
        assert!(matches!(
            fallback_gw
                .turns
                .get(&d_fallback.turn_id)
                .unwrap()
                .unknown_reasoning_retry,
            Some(WsUnknownReasoningRetry::All)
        ));
        let oauth_candidate = d_fallback
            .candidates
            .iter()
            .find(|c| c.upstream_id == oauth_a_id)
            .expect("oauth candidate present")
            .clone();
        let prep_job = fallback_gw
            .begin_connection_prepare(&d_fallback, &oauth_candidate)
            .expect("begin_connection_prepare succeeds");
        let prep_err =
            prep_job.run().await.err().expect(
                "PreparedWsConnectJob::run must reject oversized restored body before signing",
            );
        assert_eq!(prep_err.status, 429);
        assert_eq!(prep_err.code, "limit_rejected");
        assert_eq!(
            build_calls.load(Ordering::SeqCst),
            0,
            "limit resize rejection must prevent signer build and provider connect"
        );
        let err = fallback_gw
            .validate_candidate_for_send(d_fallback.turn_id, oauth_a_id, None, false)
            .await
            .expect_err("deferred limit resize on restored verbatim body must also hit tight cap");
        assert_eq!(err.status, 429);
        assert_eq!(err.code, "limit_rejected");
    }

    #[tokio::test]
    async fn deferred_unestablished_enforces_concurrency_limit_at_admission_before_connect() {
        use std::sync::atomic::Ordering;

        let (lifecycle, auth, headers, _api_key_id, _oauth_a_id, _oauth_b_id, build_calls) =
            setup_deferred_ws_fixture(vec![Limit {
                kind: LimitKind::Concurrent,
                window_secs: 0,
                cap_micros: 1,
            }])
            .await;

        let frame = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "type": "response.create",
                "model": "gpt-5",
                "store": false,
                "max_output_tokens": 16,
                "input": [
                    {
                        "type": "reasoning",
                        "id": "rs_conc",
                        "summary": [],
                        "content": [],
                        "encrypted_content": "gAAAAAB_conc_1"
                    },
                    {"role": "user", "content": "hi"}
                ]
            }))
            .unwrap(),
        );

        let mut gw1 = ResponsesWebSocketGateway::new(
            lifecycle.clone(),
            &auth,
            headers.clone(),
            1_024 * 1_024,
            16,
        );
        let mut gw2 = ResponsesWebSocketGateway::new(lifecycle, &auth, headers, 1_024 * 1_024, 16);

        let d1 = gw1
            .client_message(auth.clone(), frame.clone())
            .await
            .expect("first deferred turn acquires concurrency slot at admission")
            .expect("dispatch returned");
        assert!(
            gw1.turns
                .get(&d1.turn_id)
                .unwrap()
                .prepared
                .reservation
                .is_some()
        );

        let err = gw2
            .client_message(auth.clone(), frame.clone())
            .await
            .expect_err("second turn must reject at admission while first holds concurrency slot");
        assert_eq!(err.status, 429);
        assert_eq!(err.code, "limit_rejected");
        assert_eq!(
            build_calls.load(Ordering::SeqCst),
            0,
            "admission rejection must happen before any signing or socket connect"
        );

        gw1.fail_dispatch(
            d1.turn_id,
            &WebSocketError::new(502, "test_done", "release slot"),
        )
        .await;

        let d2 = gw2
            .client_message(auth, frame)
            .await
            .expect("second turn admits once first releases concurrency slot")
            .expect("dispatch returned");
        gw2.fail_dispatch(
            d2.turn_id,
            &WebSocketError::new(502, "test_done", "cleanup"),
        )
        .await;
    }

    #[tokio::test]
    async fn deferred_unestablished_charges_both_copies_against_byte_cap_and_releases_on_restore_commit_or_cancel()
     {
        let (lifecycle, auth, headers, api_key_id, _oauth_a_id, _oauth_b_id, _build_calls) =
            setup_deferred_ws_fixture(Vec::new()).await;

        let bloated_cipher = format!("gAAAAAB_byte_cap_{}", "X".repeat(20_000));
        let make_frame = |stream_id: &str| {
            Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "type": "response.create",
                    "stream_id": stream_id,
                    "model": "gpt-5",
                    "store": false,
                    "max_output_tokens": 16,
                    "input": [
                        {
                            "type": "reasoning",
                            "id": "rs_cap",
                            "summary": [],
                            "content": [],
                            "encrypted_content": bloated_cipher
                        },
                        {"role": "user", "content": "hi"}
                    ]
                }))
                .unwrap(),
            )
        };

        // Measure exact single-copy, stripped-extra, and dual-copy ledger usage.
        let mut measure_gw = ResponsesWebSocketGateway::new(
            lifecycle.clone(),
            &auth,
            headers.clone(),
            1_024 * 1_024,
            16,
        );
        let peer_frame = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "type": "response.create",
                "stream_id": "lane-peer",
                "model": "gpt-5",
                "store": false,
                "max_output_tokens": 16,
                "input": [{"role": "user", "content": "hello"}]
            }))
            .unwrap(),
        );
        let _peer = measure_gw
            .client_message(auth.clone(), peer_frame.clone())
            .await
            .unwrap()
            .unwrap();
        let peer_bytes = measure_gw.used_bytes;
        let mut d_measure = measure_gw
            .client_message(auth.clone(), make_frame("lane-target"))
            .await
            .unwrap()
            .unwrap();
        // While peer OAuth turn is active, target stays deferred and unstripped at admission.
        let orig_plus_peer = measure_gw.used_bytes;
        let orig_only_used = orig_plus_peer - peer_bytes;
        measure_gw
            .finalize_dispatch_reasoning(&mut d_measure, false)
            .unwrap();
        let both_plus_peer = measure_gw.used_bytes;
        let stripped_delta = both_plus_peer - orig_plus_peer;
        assert!(stripped_delta > 0);
        // Restoring verbatim releases stripped copy, returning to original baseline bytes.
        measure_gw
            .finalize_dispatch_reasoning(&mut d_measure, true)
            .unwrap();
        assert_eq!(measure_gw.used_bytes, orig_plus_peer);

        // Committing stripped attempt releases original copy while keeping stripped copy charged.
        measure_gw
            .finalize_dispatch_reasoning(&mut d_measure, false)
            .unwrap();
        assert_eq!(measure_gw.used_bytes, both_plus_peer);
        let original_copy_bytes = measure_gw
            .turns
            .get(&d_measure.turn_id)
            .unwrap()
            .deferred_unestablished
            .as_ref()
            .unwrap()
            .original_bytes;
        measure_gw.assign_connection(d_measure.turn_id, 1, api_key_id, "api-key-primary");
        measure_gw.commit_send_attempt(d_measure.turn_id);
        assert_eq!(measure_gw.used_bytes, both_plus_peer - original_copy_bytes);

        // Failing/cancelling turn releases all remaining payload bytes.
        measure_gw
            .fail_dispatch(
                d_measure.turn_id,
                &WebSocketError::new(502, "test_cancel", "done"),
            )
            .await;
        assert_eq!(measure_gw.used_bytes, peer_bytes);

        // Tight byte_cap allows original copy at admission (while peer holds OAuth deferral)
        // but rejects finalize_dispatch_reasoning(..., false) with 503 when stripped copy
        // would push dual-copy memory past byte_cap.
        let tight_cap = orig_plus_peer + stripped_delta - 1;
        let mut tight_gw = ResponsesWebSocketGateway::new(
            lifecycle.clone(),
            &auth,
            headers.clone(),
            tight_cap,
            16,
        );
        let _peer2 = tight_gw
            .client_message(auth.clone(), peer_frame)
            .await
            .unwrap()
            .unwrap();
        let mut d_tight = tight_gw
            .client_message(auth.clone(), make_frame("lane-target"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(tight_gw.used_bytes, orig_plus_peer);
        let err = tight_gw
            .finalize_dispatch_reasoning(&mut d_tight, false)
            .expect_err("dual-copy retention exceeding byte_cap must fail closed");
        assert_eq!(err.status, 503);
        assert_eq!(err.code, "overloaded_error");
        assert_eq!(
            tight_gw.used_bytes, orig_plus_peer,
            "failed strip leaves ledger unchanged"
        );
        assert!(
            !tight_gw
                .turns
                .get(&d_tight.turn_id)
                .unwrap()
                .deferred_unestablished
                .as_ref()
                .unwrap()
                .currently_stripped
        );
        tight_gw.fail_dispatch(d_tight.turn_id, &err).await;
        assert_eq!(tight_gw.used_bytes, peer_bytes);

        // Immediate try_dispatch inside client_message also fails closed if single turn
        // + dual stripped copy would exceed byte_cap.
        let single_turn_tight_cap = orig_only_used + stripped_delta - 1;
        let mut single_tight_gw =
            ResponsesWebSocketGateway::new(lifecycle, &auth, headers, single_turn_tight_cap, 16);
        let err = single_tight_gw
            .client_message(auth, make_frame("lane-target"))
            .await
            .expect_err("client_message rejects when immediate dual-copy exceeds byte_cap");
        assert_eq!(err.status, 503);
        assert_eq!(err.code, "overloaded_error");
        assert_eq!(single_tight_gw.used_bytes, 0);
    }

    #[test]
    fn native_terminal_error_maps_usage_limit_codes_to_429() {
        for payload in [
            serde_json::json!({
                "type": "error",
                "error": {
                    "type": "usage_limit_reached",
                    "code": "usage_limit_reached",
                    "message": "limit reached"
                }
            }),
            serde_json::json!({
                "type": "error",
                "error": {
                    "type": "rate_limit_error",
                    "code": "usage_limit_exceeded",
                    "message": "exceeded"
                }
            }),
            serde_json::json!({
                "type": "response.failed",
                "response": {
                    "id": "resp_1",
                    "error": {
                        "code": "usage_limit_reached",
                        "message": "limit reached"
                    }
                }
            }),
        ] {
            let err = native_terminal_error(&payload, &None);
            assert_eq!(err.status, 429, "{payload}");
            assert!(is_openai_rate_or_usage_limit_ws_error(
                &payload, &None, None
            ));
        }

        let unrelated = serde_json::json!({
            "type": "error",
            "error": {"code": "something_else", "message": "boom"}
        });
        assert_eq!(native_terminal_error(&unrelated, &None).status, 502);
        assert!(!is_openai_rate_or_usage_limit_ws_error(
            &unrelated, &None, None
        ));
    }

    #[test]
    fn codex_ws_previous_response_failover_eligibility_contract() {
        let cap = crate::provider_capability::ProviderCapability::OpenaiResponses;
        assert!(
            crate::upstream_affinity::is_openai_codex_ws_previous_response_failover_eligible(
                cap,
                &serde_json::json!({
                    "type": "response.create",
                    "model": "gpt-5.4",
                    "previous_response_id": "resp_1",
                    "store": false,
                    "input": [{"type": "function_call_output", "call_id": "c1", "output": "ok"}]
                })
            )
        );
        for ineligible in [
            serde_json::json!({"type": "response.create", "model": "gpt-5.4", "store": false, "input": []}),
            serde_json::json!({"type": "response.create", "model": "gpt-5.4", "previous_response_id": "resp_1", "store": true, "input": []}),
            serde_json::json!({"type": "response.create", "model": "gpt-5.4", "previous_response_id": "resp_1", "conversation": "conv_1", "store": false, "input": []}),
            serde_json::json!({"type": "response.create", "model": "gpt-5.4", "previous_response_id": "resp_1", "store": false, "input": [{"type": "compaction", "encrypted_content": "c"}]}),
            serde_json::json!({"type": "response.create", "model": "gpt-5.4", "previous_response_id": "resp_1", "store": false, "input": [{"role": "user", "content": [{"type": "input_file", "file_id": "file-1"}]}]}),
            serde_json::json!({"type": "response.create", "model": "gpt-5.4", "previous_response_id": "resp_1", "store": false, "input": [{"type": "item_reference", "id": "item_1"}]}),
        ] {
            assert!(
                !crate::upstream_affinity::is_openai_codex_ws_previous_response_failover_eligible(
                    cap,
                    &ineligible,
                ),
                "{ineligible}"
            );
        }
    }

    #[derive(Default)]
    struct WsQuotaTestCache {
        snapshots: Mutex<HashMap<Uuid, Vec<cc_lb_domain::SubscriptionQuotaCandidateSnapshot>>>,
    }

    impl WsQuotaTestCache {
        fn set_state(&self, upstream_id: Uuid, utilization: f64, status: &str) {
            let make =
                |window: &str, reset_delta: u64| cc_lb_domain::SubscriptionQuotaCandidateSnapshot {
                    window: window.to_owned(),
                    state: cc_lb_domain::SubscriptionQuotaDataState::Fresh,
                    source: Some("test".to_owned()),
                    utilization: Some(utilization),
                    status: Some(status.to_owned()),
                    resets_at_unix_secs: Some(4_100_000_000 + reset_delta),
                    surpassed_threshold: None,
                    representative_claim: None,
                    disabled_reason: None,
                    extra_usage_enabled: None,
                    extra_usage_monthly_limit: None,
                    extra_usage_used_credits: None,
                    credits: None,
                    observed_at_unix_millis: Some(4_100_000_000 * 1000),
                    max_staleness_secs: 60,
                    fallback_available: None,
                    overage_in_use: None,
                    overage_period_monthly_utilization: None,
                    upgrade_paths: None,
                };
            self.snapshots
                .lock()
                .unwrap()
                .insert(upstream_id, vec![make("5h", 1800), make("7d", 86400)]);
        }
    }

    impl cc_lb_control::SubscriptionQuotaCacheLike for WsQuotaTestCache {
        fn upsert_observation(&self, _record: &cc_lb_storage_api::SubscriptionQuotaSample) {}

        fn snapshot_for_upstream(
            &self,
            upstream_id: Uuid,
            _now_unix_millis: u64,
            _max_staleness_secs: u64,
        ) -> Vec<cc_lb_domain::SubscriptionQuotaCandidateSnapshot> {
            self.snapshots
                .lock()
                .unwrap()
                .get(&upstream_id)
                .cloned()
                .unwrap_or_default()
        }
    }

    async fn setup_codex_ws_quota_fixture(
        policy: cc_lb_domain::UnavailableOpenaiReasoningOwnerPolicy,
    ) -> (
        Arc<Lifecycle>,
        Authenticated,
        HeaderMap,
        Uuid,
        Uuid,
        Arc<WsQuotaTestCache>,
        Arc<PrewarmAffinityStore>,
    ) {
        use crate::builtin_filters::subscription_preference::SubscriptionPreferenceFilter;
        use cc_lb_control::api_keys::builtin_authn::BuiltinAuthn;
        use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
        use cc_lb_control::api_keys::principal_view::{
            DialectCache, PrincipalView, RouterPipelineCache,
        };
        use cc_lb_control::dynamic_view::DynamicViewHolder;
        use cc_lb_domain::TerminalStrategy;
        use cc_lb_routing::FilterPlugin;
        use cc_lb_storage_api::upstream::{UpstreamKind as StorageRecordKind, UpstreamRecord};
        use cc_lb_upstream::{
            ApiKeyAwareSignerFactory, RetryDecision, ShapedRequest, SignedRequest, Signer,
            SignerError, SignerFactory, SigningCapability, UpstreamError,
        };

        struct DummySignerFactory;
        impl ApiKeyAwareSignerFactory for DummySignerFactory {
            fn with_router_choice(&self, _name: String) -> Arc<dyn SignerFactory> {
                Arc::new(Self)
            }
        }
        #[async_trait::async_trait]
        impl SignerFactory for DummySignerFactory {
            async fn build(
                &self,
                _upstream: &cc_lb_domain::Upstream,
            ) -> Result<Arc<dyn Signer>, SignerError> {
                Ok(Arc::new(DummySigner))
            }
        }
        struct DummySigner;
        #[async_trait::async_trait]
        impl Signer for DummySigner {
            async fn sign(
                &self,
                shaped: ShapedRequest,
                capability: &mut SigningCapability,
            ) -> Result<SignedRequest, SignerError> {
                Ok(SignedRequest::from_shaped(shaped, capability))
            }
            async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
                RetryDecision::Fail
            }
        }
        struct UnusedDispatcher;
        #[async_trait::async_trait]
        impl crate::UpstreamDispatch for UnusedDispatcher {
            async fn dispatch(
                &self,
                _request: SignedRequest,
            ) -> Result<http::Response<crate::Body>, crate::DispatchError> {
                unreachable!("unused dispatcher in codex ws quota test")
            }
        }

        let quota_cache = Arc::new(WsQuotaTestCache::default());
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(4_100_000_000));
        let storage = Arc::new(
            cc_lb_storage_sqlite::open_sqlite("sqlite::memory:", clock.clone())
                .await
                .expect("open in-memory codex ws quota db"),
        );
        storage
            .initialize()
            .await
            .expect("migrate codex ws quota db");
        let keys = Arc::new(KeyStore::new(storage));
        let (_record, secret) = keys
            .create(
                DIRECT_PRINCIPAL,
                CreateParams {
                    label: "codex-ws-quota-test".to_owned(),
                    description: None,
                    expires_at_unix_secs: None,
                    limit_overrides: Vec::new(),
                },
            )
            .await
            .expect("issue codex ws quota key");
        let filter: Arc<dyn FilterPlugin> =
            Arc::new(SubscriptionPreferenceFilter::with_clock(clock.clone()));
        let pipeline = Arc::new(RouterPipelineCache {
            user_filters: vec![filter],
            terminal: TerminalStrategy::FirstPick,
            instantiation_error: None,
            provider_filters: None,
        });
        let chains = HashMap::from([(
            DIRECT_PRINCIPAL.to_owned(),
            (Some(pipeline), DialectCache::Inherit),
        )]);
        let principal_view = Arc::new(PrincipalView::for_tests(
            DIRECT_PRINCIPAL,
            true,
            vec!["*".to_owned()],
            Vec::new(),
            chains,
        ));
        let authn = Arc::new(BuiltinAuthn::new(keys, clock.clone()));
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-api-key",
            secret.expose().parse().expect("fixture key header"),
        );
        let auth = crate::authenticate_first(&authn, &headers, &principal_view)
            .await
            .expect("authenticate codex ws quota caller");

        let upstream_a = Uuid::from_u128(0xA001);
        let upstream_b = Uuid::from_u128(0xB002);
        let records = vec![
            UpstreamRecord {
                id: upstream_a,
                name: "oauth-a".to_owned(),
                kind: StorageRecordKind::OpenaiOauth,
                base_url: Some(url::Url::parse("https://oauth-a.local/").unwrap()),
                enabled: true,
                oauth_never_refresh: true,
                ..Default::default()
            },
            UpstreamRecord {
                id: upstream_b,
                name: "oauth-b".to_owned(),
                kind: StorageRecordKind::OpenaiOauth,
                base_url: Some(url::Url::parse("https://oauth-b.local/").unwrap()),
                enabled: true,
                oauth_never_refresh: true,
                ..Default::default()
            },
        ];
        let view = cc_lb_control::DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(DummySignerFactory))
            .principal_view(principal_view)
            .subscription_quota_cache(quota_cache.clone())
            .subscription_quota_routing_max_staleness_secs(60)
            .upstream_records(records)
            .build();
        let store = Arc::new(PrewarmAffinityStore::default());
        let lifecycle = Arc::new(
            Lifecycle::new_with_dynamic_view(
                authn.clone(),
                Arc::new(DynamicViewHolder::new(view)),
                Arc::new(UnusedDispatcher),
                crate::LifecycleConfig {
                    unavailable_openai_reasoning_owner: policy,
                    ..Default::default()
                },
                clock,
            )
            .with_upstream_affinity_store(store.clone()),
        );
        (
            lifecycle,
            auth,
            headers,
            upstream_a,
            upstream_b,
            quota_cache,
            store,
        )
    }

    #[tokio::test]
    async fn ws_quota_exhaustion_yields_session_hint_evicts_chained_owner_and_live_429_recovers() {
        let (lifecycle, auth, headers, upstream_a, upstream_b, quota_cache, _store) =
            setup_codex_ws_quota_fixture(
                cc_lb_domain::UnavailableOpenaiReasoningOwnerPolicy::StripAndFailover,
            )
            .await;

        // 1. Pre-dispatch `SessionHint(A)` yields cleanly to healthy B when A is quota-exhausted.
        quota_cache.set_state(upstream_a, 1.0, "rejected");
        quota_cache.set_state(upstream_b, 0.1, "allowed");
        let mut hint_gw = ResponsesWebSocketGateway::new(
            lifecycle.clone(),
            &auth,
            headers.clone(),
            1024 * 1024,
            16,
        )
        .with_codex_backend(true);
        hint_gw.session_upstream = Some(upstream_a);
        hint_gw.session_oauth = true;
        let d_hint = hint_gw
            .client_message(
                auth.clone(),
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "store": false,
                        "input": [{"role": "user", "content": "unchained"}]
                    })
                    .to_string(),
                ),
            )
            .await
            .expect("SessionHint(A) must yield to healthy B when A is exhausted")
            .expect("dispatches");
        assert_eq!(d_hint.candidates[0].upstream_id, upstream_b);
        assert!(hint_gw.session_upstream.is_none());
        hint_gw.close();

        // 2. Codex WS chained `previous_response_id` live pre-response.created 429 `usage_limit_reached`:
        // evicts A's owners + session_upstream, emits 400 `previous_response_not_found`, and replay completes on B.
        quota_cache.set_state(upstream_a, 0.05, "allowed");
        quota_cache.set_state(upstream_b, 0.2, "allowed");
        let mut ws_gw = ResponsesWebSocketGateway::new(
            lifecycle.clone(),
            &auth,
            headers.clone(),
            1024 * 1024,
            16,
        )
        .with_codex_backend(true);
        let turn1 = ws_gw
            .client_message(
                auth.clone(),
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "store": false,
                        "include": ["reasoning.encrypted_content"],
                        "input": [{"role": "user", "content": "turn1"}]
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        let chosen_first = turn1.candidates[0].upstream_id;
        let replacement = if chosen_first == upstream_a {
            upstream_b
        } else {
            upstream_a
        };
        ws_gw.assign_connection(turn1.turn_id, 1, chosen_first, "oauth-first");
        ws_gw.commit_send_attempt(turn1.turn_id);
        ws_gw.sent(turn1.turn_id);
        ws_gw
            .provider_message(
                1,
                &Bytes::from(
                    serde_json::json!({
                        "type": "response.created",
                        "response": {"id": "resp_t1", "object": "response", "status": "in_progress", "output": []}
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap();
        ws_gw
            .provider_message(
                1,
                &Bytes::from(
                    serde_json::json!({
                        "type": "response.completed",
                        "response": {
                            "id": "resp_t1",
                            "object": "response",
                            "status": "completed",
                            "usage": {"input_tokens": 10, "output_tokens": 5},
                            "output": [{
                                "type": "reasoning",
                                "id": "rs_1",
                                "summary": [],
                                "encrypted_content": "gAAAAAB_turn1_cipher"
                            }]
                        }
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap();

        let turn2 = ws_gw
            .client_message(
                auth.clone(),
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "previous_response_id": "resp_t1",
                        "store": false,
                        "input": [{"type": "function_call_output", "call_id": "c1", "output": "ok"}]
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        ws_gw.assign_connection(turn2.turn_id, 1, chosen_first, "oauth-first");
        ws_gw.commit_send_attempt(turn2.turn_id);
        ws_gw.sent(turn2.turn_id);

        // Live pre-response.created `429 usage_limit_reached`: must NOT forward raw 429,
        // must evict owner + session_upstream, and must emit `previous_response_not_found`.
        let recovery_res = ws_gw
            .provider_message(
                1,
                &Bytes::from(
                    serde_json::json!({
                        "type": "error",
                        "error": {
                            "type": "usage_limit_reached",
                            "code": "usage_limit_reached",
                            "message": "The usage limit has been reached"
                        }
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap();
        assert!(!recovery_res.forward);
        assert_eq!(recovery_res.errors.len(), 1);
        assert_eq!(recovery_res.errors[0].status, 400);
        assert_eq!(recovery_res.errors[0].code, "previous_response_not_found");
        assert!(ws_gw.session_upstream.is_none());
        assert!(ws_gw.session_rate_limited_upstreams.contains(&chosen_first));

        // Simulate Codex closing and opening a fresh WS (`session_rate_limited_upstreams` empty):
        // process-wide `WsRateLimitCooldownCache` still excludes `chosen_first`, strips its
        // encrypted reasoning upfront, and routes directly to `replacement`.
        let mut reconnect_gw = ResponsesWebSocketGateway::new(
            lifecycle.clone(),
            &auth,
            headers.clone(),
            1024 * 1024,
            16,
        )
        .with_codex_backend(true);
        let reconnect_replay = reconnect_gw
            .client_message(
                auth.clone(),
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "store": false,
                        "include": ["reasoning.encrypted_content"],
                        "input": [
                            {
                                "type": "reasoning",
                                "id": "rs_reconnect",
                                "summary": [],
                                "encrypted_content": "gAAAAAB_turn1_cipher"
                            },
                            {"type": "function_call_output", "call_id": "c1", "output": "ok"}
                        ]
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reconnect_replay.candidates[0].upstream_id, replacement);
        let parsed_reconnect: Value = serde_json::from_slice(&reconnect_replay.frame).unwrap();
        assert_eq!(parsed_reconnect["input"].as_array().unwrap().len(), 1);
        assert_eq!(parsed_reconnect["input"][0]["type"], "function_call_output");
        reconnect_gw.close();

        // Same-socket replay also strips & routes to replacement.
        let replay = ws_gw
            .client_message(
                auth.clone(),
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "store": false,
                        "include": ["reasoning.encrypted_content"],
                        "input": [
                            {
                                "type": "reasoning",
                                "id": "rs_1",
                                "summary": [],
                                "encrypted_content": "gAAAAAB_turn1_cipher"
                            },
                            {"type": "function_call_output", "call_id": "c1", "output": "ok"}
                        ]
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replay.candidates[0].upstream_id, replacement);
        let parsed_replay: Value = serde_json::from_slice(&replay.frame).unwrap();
        assert_eq!(parsed_replay["input"].as_array().unwrap().len(), 1);
        assert_eq!(parsed_replay["input"][0]["type"], "function_call_output");

        // 3. Post-`response.created` 429 NEVER retries or synthesizes `previous_response_not_found`.
        ws_gw.assign_connection(replay.turn_id, 2, replacement, "oauth-replacement");
        ws_gw.commit_send_attempt(replay.turn_id);
        ws_gw.sent(replay.turn_id);
        ws_gw
            .provider_message(
                2,
                &Bytes::from(
                    serde_json::json!({
                        "type": "response.created",
                        "response": {"id": "resp_r2", "object": "response", "status": "in_progress", "output": []}
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap();
        let post_created_err = ws_gw
            .provider_message(
                2,
                &Bytes::from(
                    serde_json::json!({
                        "type": "error",
                        "error": {"code": "usage_limit_reached", "message": "mid-stream"}
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap();
        assert!(post_created_err.dispatches.is_empty());
        assert!(post_created_err.errors.is_empty());
        assert!(post_created_err.forward);
        ws_gw.close();

        // 4. Negative controls:
        // (a) Public `/v1/responses` (`codex_backend == false`) never synthesizes `previous_response_not_found` on 429.
        let (pub_lifecycle, pub_auth, pub_headers, pub_a, pub_b, pub_quota, _) =
            setup_codex_ws_quota_fixture(
                cc_lb_domain::UnavailableOpenaiReasoningOwnerPolicy::StripAndFailover,
            )
            .await;
        pub_quota.set_state(pub_a, 0.05, "allowed");
        pub_quota.set_state(pub_b, 0.2, "allowed");
        let mut pub_gw =
            ResponsesWebSocketGateway::new(pub_lifecycle, &pub_auth, pub_headers, 1024 * 1024, 16);
        let pub_t1 = pub_gw
            .client_message(
                pub_auth.clone(),
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "store": true,
                        "input": [{"role": "user", "content": "pub1"}]
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        let pub_owner = pub_t1.candidates[0].upstream_id;
        pub_gw.assign_connection(pub_t1.turn_id, 1, pub_owner, "oauth-a");
        pub_gw.commit_send_attempt(pub_t1.turn_id);
        pub_gw.sent(pub_t1.turn_id);
        pub_gw
            .provider_message(
                1,
                &Bytes::from(
                    serde_json::json!({
                        "type": "response.created",
                        "response": {"id": "resp_pub_1", "object": "response", "status": "in_progress", "output": []}
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap();
        pub_gw
            .provider_message(
                1,
                &Bytes::from(
                    serde_json::json!({
                        "type": "response.completed",
                        "response": {"id": "resp_pub_1", "object": "response", "status": "completed", "usage": {"input_tokens": 1, "output_tokens": 1}}
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap();
        let pub_t2 = pub_gw
            .client_message(
                auth.clone(),
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "previous_response_id": "resp_pub_1",
                        "store": false,
                        "input": [{"role": "user", "content": "pub2"}]
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        pub_gw.assign_connection(pub_t2.turn_id, 1, pub_owner, "oauth-a");
        pub_gw.commit_send_attempt(pub_t2.turn_id);
        pub_gw.sent(pub_t2.turn_id);
        let pub_429 = pub_gw
            .provider_message(
                1,
                &Bytes::from(
                    serde_json::json!({
                        "type": "error",
                        "status": 429,
                        "error": {"code": "rate_limit_exceeded", "message": "real 429"}
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap();
        assert!(pub_429.forward);
        assert!(pub_429.dispatches.is_empty());
        assert!(pub_429.errors.is_empty());
        pub_gw.close();

        // (b1) `FailClosed` bound encrypted reasoning owner never fails over on live 429.
        let (fc_lifecycle, fc_auth, fc_headers, fc_a, fc_b, fc_quota, fc_store) =
            setup_codex_ws_quota_fixture(
                cc_lb_domain::UnavailableOpenaiReasoningOwnerPolicy::FailClosed,
            )
            .await;
        fc_quota.set_state(fc_a, 0.05, "allowed");
        fc_quota.set_state(fc_b, 0.2, "allowed");
        let fc_bound_cipher = "gAAAAAB_bound_fc_a";
        fc_store.bindings.lock().unwrap().insert(
            UpstreamAffinityKey {
                principal_id: fc_auth.principal_id().to_owned(),
                provider: "openai".to_owned(),
                kind: cc_lb_storage_api::UpstreamAffinityKind::OpenaiResponseId,
                value_sha256: <sha2::Sha256 as sha2::Digest>::digest(fc_bound_cipher.as_bytes())
                    .into(),
            },
            cc_lb_storage_api::upstream_affinity::UpstreamAffinityBinding {
                key: UpstreamAffinityKey {
                    principal_id: fc_auth.principal_id().to_owned(),
                    provider: "openai".to_owned(),
                    kind: cc_lb_storage_api::UpstreamAffinityKind::OpenaiResponseId,
                    value_sha256: <sha2::Sha256 as sha2::Digest>::digest(
                        fc_bound_cipher.as_bytes(),
                    )
                    .into(),
                },
                upstream_id: fc_a,
                owner_epoch: 0,
                observed_at_unix_secs: 4_100_000_000,
                expires_at_unix_secs: None,
            },
        );
        let mut fc_gw = ResponsesWebSocketGateway::new(
            fc_lifecycle.clone(),
            &fc_auth,
            fc_headers.clone(),
            1024 * 1024,
            16,
        )
        .with_codex_backend(true);
        let fc_turn = fc_gw
            .client_message(
                fc_auth.clone(),
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "store": false,
                        "input": [
                            {"type": "reasoning", "id": "rs_fc", "summary": [], "encrypted_content": fc_bound_cipher},
                            {"role": "user", "content": "u"}
                        ]
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fc_turn.candidates[0].upstream_id, fc_a);
        fc_gw.assign_connection(fc_turn.turn_id, 1, fc_a, "oauth-a");
        fc_gw.commit_send_attempt(fc_turn.turn_id);
        fc_gw.sent(fc_turn.turn_id);
        let fc_res = fc_gw
            .provider_message(
                1,
                &Bytes::from(
                    serde_json::json!({
                        "type": "error",
                        "status": 429,
                        "error": {"code": "rate_limit_exceeded", "message": "429"}
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap();
        assert!(fc_res.forward);
        assert!(fc_res.dispatches.is_empty());
        fc_gw.close();

        // (b2) Unestablished turn with unstripped pending unknown reasoning (`SessionHint(fc_a)`)
        // never uses `try_unpinned_live_failover_dispatch` on live 429.
        let mut fc_gw2 =
            ResponsesWebSocketGateway::new(fc_lifecycle, &fc_auth, fc_headers, 1024 * 1024, 16)
                .with_codex_backend(true);
        fc_gw2.session_upstream = Some(fc_a);
        fc_gw2.session_oauth = true;
        let fc_turn2 = fc_gw2
            .client_message(
                fc_auth,
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "store": false,
                        "input": [
                            {"type": "reasoning", "id": "rs_u", "summary": [], "encrypted_content": "gAAAAAB_unknown"},
                            {"role": "user", "content": "u"}
                        ]
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        let fc_up2 = fc_turn2.candidates[0].upstream_id;
        assert!(
            String::from_utf8_lossy(&fc_turn2.frame).contains("gAAAAAB_unknown"),
            "session-hinted unestablished turn must keep unknown reasoning verbatim"
        );
        fc_gw2.assign_connection(fc_turn2.turn_id, 1, fc_up2, "oauth-a");
        fc_gw2.commit_send_attempt(fc_turn2.turn_id);
        fc_gw2.sent(fc_turn2.turn_id);
        let fc_res2 = fc_gw2
            .provider_message(
                1,
                &Bytes::from(
                    serde_json::json!({
                        "type": "error",
                        "status": 429,
                        "error": {"code": "rate_limit_exceeded", "message": "429"}
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap();
        assert!(fc_res2.forward);
        assert!(fc_res2.dispatches.is_empty());
        fc_gw2.close();
    }

    #[tokio::test]
    async fn ws_pre_dispatch_chained_exhaustion_and_live_encrypted_reasoning_429_fail_over() {
        let (lifecycle, auth, headers, upstream_a, upstream_b, quota_cache, store) =
            setup_codex_ws_quota_fixture(
                cc_lb_domain::UnavailableOpenaiReasoningOwnerPolicy::StripAndFailover,
            )
            .await;

        // 1. Pre-dispatch exhausted Codex WS chained `previous_response_id` returns 400
        // `previous_response_not_found` at admission and evicts A when B is healthy;
        // when B is also exhausted, keeps A verbatim (no false 400 eviction).
        quota_cache.set_state(upstream_a, 0.05, "allowed");
        quota_cache.set_state(upstream_b, 0.2, "allowed");
        let mut chain_gw = ResponsesWebSocketGateway::new(
            lifecycle.clone(),
            &auth,
            headers.clone(),
            1024 * 1024,
            16,
        )
        .with_codex_backend(true);
        let t1 = chain_gw
            .client_message(
                auth.clone(),
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "store": false,
                        "input": [{"role": "user", "content": "pre-chain-1"}]
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        let owner_up = t1.candidates[0].upstream_id;
        let other_up = if owner_up == upstream_a {
            upstream_b
        } else {
            upstream_a
        };
        chain_gw.assign_connection(t1.turn_id, 1, owner_up, "oauth-owner");
        chain_gw.commit_send_attempt(t1.turn_id);
        chain_gw.sent(t1.turn_id);
        chain_gw
            .provider_message(
                1,
                &Bytes::from(
                    serde_json::json!({
                        "type": "response.created",
                        "response": {
                            "id": "resp_pre_chain_1",
                            "object": "response",
                            "status": "in_progress",
                            "output": []
                        }
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap();
        chain_gw
            .provider_message(
                1,
                &Bytes::from(
                    serde_json::json!({
                        "type": "response.completed",
                        "response": {
                            "id": "resp_pre_chain_1",
                            "object": "response",
                            "status": "completed",
                            "usage": {"input_tokens": 5, "output_tokens": 5}
                        }
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap();

        // Exhaust both -> must NOT evict or emit `previous_response_not_found`, admits on owner_up.
        quota_cache.set_state(owner_up, 1.0, "rejected");
        quota_cache.set_state(other_up, 1.0, "rejected");
        let kept_both_exhausted = chain_gw
            .client_message(
                auth.clone(),
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "previous_response_id": "resp_pre_chain_1",
                        "store": false,
                        "input": [{"type": "function_call_output", "call_id": "c_both", "output": "ok"}]
                    })
                    .to_string(),
                ),
            )
            .await
            .expect("must admit verbatim on owner when replacement is also exhausted")
            .expect("dispatches");
        assert_eq!(kept_both_exhausted.candidates[0].upstream_id, owner_up);
        chain_gw
            .fail_dispatch(
                kept_both_exhausted.turn_id,
                &WebSocketError::new(429, "rate_limit_exceeded", "both exhausted"),
            )
            .await;
        // Note: `fail_dispatch` does not record session_rate_limited_upstreams; clear other_up back to healthy.
        quota_cache.set_state(other_up, 0.1, "allowed");
        let evict_err = chain_gw
            .client_message(
                auth.clone(),
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "previous_response_id": "resp_pre_chain_1",
                        "store": false,
                        "input": [{"type": "function_call_output", "call_id": "c_evict", "output": "ok"}]
                    })
                    .to_string(),
                ),
            )
            .await
            .expect_err("exhausted chained owner with healthy B must emit 400 previous_response_not_found");
        assert_eq!(evict_err.status, 400);
        assert_eq!(evict_err.code, "previous_response_not_found");
        assert!(chain_gw.session_upstream.is_none());
        chain_gw.close();

        // 2. Live pre-response.created `429 usage_limit_reached` on encrypted-reasoning-only WS turn:
        // exercises `try_owner_reasoning_failover_dispatch`, strips A's reasoning, and retries onto B.
        quota_cache.set_state(upstream_a, 0.05, "allowed");
        quota_cache.set_state(upstream_b, 0.2, "allowed");
        let owned_cipher = "gAAAAAB_ws_live_reasoning_429";
        let key = UpstreamAffinityKey {
            principal_id: auth.principal_id().to_owned(),
            provider: "openai".to_owned(),
            kind: cc_lb_storage_api::UpstreamAffinityKind::OpenaiResponseId,
            value_sha256: <sha2::Sha256 as sha2::Digest>::digest(owned_cipher.as_bytes()).into(),
        };
        store.bindings.lock().unwrap().insert(
            key,
            cc_lb_storage_api::upstream_affinity::UpstreamAffinityBinding {
                key: UpstreamAffinityKey {
                    principal_id: auth.principal_id().to_owned(),
                    provider: "openai".to_owned(),
                    kind: cc_lb_storage_api::UpstreamAffinityKind::OpenaiResponseId,
                    value_sha256: <sha2::Sha256 as sha2::Digest>::digest(owned_cipher.as_bytes())
                        .into(),
                },
                upstream_id: upstream_a,
                owner_epoch: 0,
                observed_at_unix_secs: 4_100_000_000,
                expires_at_unix_secs: None,
            },
        );
        let mut live_gw =
            ResponsesWebSocketGateway::new(lifecycle, &auth, headers, 1024 * 1024, 16)
                .with_codex_backend(true);
        let pinned_turn = live_gw
            .client_message(
                auth,
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "store": false,
                        "include": ["reasoning.encrypted_content"],
                        "input": [
                            {
                                "type": "reasoning",
                                "id": "rs_live_429",
                                "summary": [],
                                "encrypted_content": owned_cipher
                            },
                            {"role": "user", "content": "follow up"}
                        ]
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pinned_turn.candidates[0].upstream_id, upstream_a);
        live_gw.assign_connection(pinned_turn.turn_id, 1, upstream_a, "oauth-a");
        live_gw.commit_send_attempt(pinned_turn.turn_id);
        live_gw.sent(pinned_turn.turn_id);

        let retry_res = live_gw
            .provider_message(
                1,
                &Bytes::from(
                    serde_json::json!({
                        "type": "error",
                        "error": {
                            "type": "usage_limit_reached",
                            "code": "usage_limit_reached",
                            "message": "The usage limit has been reached"
                        }
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap();
        assert!(!retry_res.forward);
        assert!(retry_res.errors.is_empty());
        assert_eq!(retry_res.dispatches.len(), 1);
        let failover_dispatch = &retry_res.dispatches[0];
        assert_eq!(failover_dispatch.candidates[0].upstream_id, upstream_b);
        let stripped_frame: Value = serde_json::from_slice(&failover_dispatch.frame).unwrap();
        assert_eq!(stripped_frame["input"].as_array().unwrap().len(), 1);
        assert_eq!(stripped_frame["input"][0]["content"], "follow up");
        live_gw.close();
    }

    #[tokio::test]
    async fn multi_owner_openai_reasoning_reconciliation_ws() {
        use cc_lb_control::api_keys::builtin_authn::BuiltinAuthn;
        use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
        use cc_lb_control::api_keys::principal_view::{
            DialectCache, PrincipalView, RouterPipelineCache,
        };
        use cc_lb_control::dynamic_view::{DynamicViewBuilder, DynamicViewHolder};
        use cc_lb_domain::TerminalStrategy;
        use cc_lb_routing::FilterPlugin;
        use cc_lb_storage_api::upstream::{UpstreamKind as StorageRecordKind, UpstreamRecord};
        use cc_lb_upstream::{
            ApiKeyAwareSignerFactory, RetryDecision, ShapedRequest, SignedRequest, Signer,
            SignerError, SignerFactory, SigningCapability, UpstreamError,
        };

        struct DummySignerFactory;
        impl ApiKeyAwareSignerFactory for DummySignerFactory {
            fn with_router_choice(&self, _name: String) -> Arc<dyn SignerFactory> {
                Arc::new(Self)
            }
        }
        #[async_trait::async_trait]
        impl SignerFactory for DummySignerFactory {
            async fn build(
                &self,
                _upstream: &cc_lb_domain::Upstream,
            ) -> Result<Arc<dyn Signer>, SignerError> {
                Ok(Arc::new(DummySigner))
            }
        }
        struct DummySigner;
        #[async_trait::async_trait]
        impl Signer for DummySigner {
            async fn sign(
                &self,
                shaped: ShapedRequest,
                capability: &mut SigningCapability,
            ) -> Result<SignedRequest, SignerError> {
                Ok(SignedRequest::from_shaped(shaped, capability))
            }
            async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
                RetryDecision::Fail
            }
        }
        struct UnusedDispatcher;
        #[async_trait::async_trait]
        impl crate::UpstreamDispatch for UnusedDispatcher {
            async fn dispatch(
                &self,
                _request: SignedRequest,
            ) -> Result<http::Response<crate::Body>, crate::DispatchError> {
                unreachable!("unused dispatcher in multi-owner ws test")
            }
        }

        let quota_cache = Arc::new(WsQuotaTestCache::default());
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(4_100_000_000));
        let storage = Arc::new(
            cc_lb_storage_sqlite::open_sqlite("sqlite::memory:", clock.clone())
                .await
                .unwrap(),
        );
        storage.initialize().await.unwrap();
        let keys = Arc::new(KeyStore::new(storage));
        let (_record, secret) = keys
            .create(
                DIRECT_PRINCIPAL,
                CreateParams {
                    label: "ws-multi-owner".to_owned(),
                    description: None,
                    expires_at_unix_secs: None,
                    limit_overrides: Vec::new(),
                },
            )
            .await
            .unwrap();
        let filter: Arc<dyn FilterPlugin> = Arc::new(
            crate::builtin_filters::subscription_preference::SubscriptionPreferenceFilter::with_clock(
                clock.clone(),
            ),
        );
        let pipeline = Arc::new(RouterPipelineCache {
            user_filters: vec![filter],
            terminal: TerminalStrategy::FirstPick,
            instantiation_error: None,
            provider_filters: None,
        });
        let chains = HashMap::from([(
            DIRECT_PRINCIPAL.to_owned(),
            (Some(pipeline), DialectCache::Inherit),
        )]);
        let principal_view = Arc::new(PrincipalView::for_tests(
            DIRECT_PRINCIPAL,
            true,
            vec!["*".to_owned()],
            Vec::new(),
            chains,
        ));
        let authn = Arc::new(BuiltinAuthn::new(keys, clock.clone()));
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", secret.expose().parse().unwrap());
        let auth = crate::authenticate_first(&authn, &headers, &principal_view)
            .await
            .unwrap();

        let upstream_a = Uuid::parse_str("30000000-0000-0000-0000-000000000701").unwrap();
        let upstream_b = Uuid::parse_str("30000000-0000-0000-0000-000000000702").unwrap();
        let upstream_c = Uuid::parse_str("30000000-0000-0000-0000-000000000703").unwrap();
        let records = vec![
            UpstreamRecord {
                id: upstream_a,
                name: "oauth-a".to_owned(),
                kind: StorageRecordKind::OpenaiOauth,
                base_url: Some(url::Url::parse("https://oauth-a.local/").unwrap()),
                enabled: true,
                oauth_never_refresh: true,
                ..Default::default()
            },
            UpstreamRecord {
                id: upstream_b,
                name: "oauth-b".to_owned(),
                kind: StorageRecordKind::OpenaiOauth,
                base_url: Some(url::Url::parse("https://oauth-b.local/").unwrap()),
                enabled: true,
                oauth_never_refresh: true,
                ..Default::default()
            },
            UpstreamRecord {
                id: upstream_c,
                name: "oauth-c".to_owned(),
                kind: StorageRecordKind::OpenaiOauth,
                base_url: Some(url::Url::parse("https://oauth-c.local/").unwrap()),
                enabled: true,
                oauth_never_refresh: true,
                ..Default::default()
            },
        ];
        let view = DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(DummySignerFactory))
            .principal_view(principal_view)
            .subscription_quota_cache(quota_cache.clone())
            .subscription_quota_routing_max_staleness_secs(60)
            .upstream_records(records)
            .build();
        let store = Arc::new(PrewarmAffinityStore::default());
        let cipher_a = "gAAAAAB_multi_ws_a";
        let cipher_b = "gAAAAAB_multi_ws_b";
        for (cipher, owner) in [(cipher_a, upstream_a), (cipher_b, upstream_b)] {
            store.bindings.lock().unwrap().insert(
                UpstreamAffinityKey {
                    principal_id: auth.principal_id().to_owned(),
                    provider: "openai".to_owned(),
                    kind: cc_lb_storage_api::UpstreamAffinityKind::OpenaiResponseId,
                    value_sha256: <sha2::Sha256 as sha2::Digest>::digest(cipher.as_bytes()).into(),
                },
                cc_lb_storage_api::upstream_affinity::UpstreamAffinityBinding {
                    key: UpstreamAffinityKey {
                        principal_id: auth.principal_id().to_owned(),
                        provider: "openai".to_owned(),
                        kind: cc_lb_storage_api::UpstreamAffinityKind::OpenaiResponseId,
                        value_sha256: <sha2::Sha256 as sha2::Digest>::digest(cipher.as_bytes())
                            .into(),
                    },
                    upstream_id: owner,
                    owner_epoch: 0,
                    observed_at_unix_secs: 4_100_000_000,
                    expires_at_unix_secs: None,
                },
            );
        }
        let view_holder = Arc::new(DynamicViewHolder::new(view));
        let lifecycle = Arc::new(
            Lifecycle::new_with_dynamic_view(
                authn.clone(),
                view_holder.clone(),
                Arc::new(UnusedDispatcher),
                crate::LifecycleConfig::default(),
                clock.clone(),
            )
            .with_upstream_affinity_store(store.clone()),
        );
        let multi_frame = || {
            Bytes::from(
                serde_json::json!({
                    "type": "response.create",
                    "model": "gpt-5.4",
                    "store": false,
                    "include": ["reasoning.encrypted_content"],
                    "input": [
                        {"type": "reasoning", "id": "rs_a", "summary": [], "encrypted_content": cipher_a},
                        {"role": "user", "content": "turn 1"},
                        {"type": "reasoning", "id": "rs_b", "summary": [], "encrypted_content": cipher_b},
                        {"role": "user", "content": "turn 2"}
                    ]
                })
                .to_string(),
            )
        };

        // 1. `[rs_A (exhausted), rs_B (healthy)]`: yields even stale SessionHint(A), routes to B, strips ONLY rs_A, keeps rs_B.
        quota_cache.set_state(upstream_a, 1.0, "rejected");
        quota_cache.set_state(upstream_b, 0.1, "allowed");
        quota_cache.set_state(upstream_c, 0.5, "allowed");
        let mut gw = ResponsesWebSocketGateway::new(
            lifecycle.clone(),
            &auth,
            headers.clone(),
            1024 * 1024,
            16,
        )
        .with_codex_backend(true);
        gw.session_upstream = Some(upstream_a);
        gw.session_oauth = true;
        let d1 = gw
            .client_message(auth.clone(), multi_frame())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(d1.candidates[0].upstream_id, upstream_b);
        assert!(gw.session_upstream.is_none());
        let f1: Value = serde_json::from_slice(&d1.frame).unwrap();
        assert_eq!(f1["input"].as_array().unwrap().len(), 3);
        assert_eq!(f1["input"][1]["encrypted_content"], cipher_b);
        gw.close();

        // 2. `[rs_A (healthy), rs_B (healthy)]`: picks later owner B, strips ONLY rs_A.
        quota_cache.set_state(upstream_a, 0.05, "allowed");
        let mut gw2 = ResponsesWebSocketGateway::new(
            lifecycle.clone(),
            &auth,
            headers.clone(),
            1024 * 1024,
            16,
        )
        .with_codex_backend(true);
        let d2 = gw2
            .client_message(auth.clone(), multi_frame())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(d2.candidates[0].upstream_id, upstream_b);
        let f2: Value = serde_json::from_slice(&d2.frame).unwrap();
        assert_eq!(f2["input"].as_array().unwrap().len(), 3);
        assert_eq!(f2["input"][1]["encrypted_content"], cipher_b);
        gw2.close();

        // 3. `[rs_A (exhausted), rs_B (exhausted)]` with healthy C: strips ALL and routes to C.
        quota_cache.set_state(upstream_a, 1.0, "rejected");
        quota_cache.set_state(upstream_b, 1.0, "rejected");
        quota_cache.set_state(upstream_c, 0.1, "allowed");
        let mut gw3 = ResponsesWebSocketGateway::new(
            lifecycle.clone(),
            &auth,
            headers.clone(),
            1024 * 1024,
            16,
        )
        .with_codex_backend(true);
        let d3 = gw3
            .client_message(auth.clone(), multi_frame())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(d3.candidates[0].upstream_id, upstream_c);
        let f3: Value = serde_json::from_slice(&d3.frame).unwrap();
        assert_eq!(f3["input"].as_array().unwrap().len(), 2);
        gw3.close();

        // 4. `[rs_A (exhausted), rs_B (exhausted), rs_C (exhausted)]`: no healthy replacement -> keeps B (with rs_A stripped) without false 400/409.
        quota_cache.set_state(upstream_c, 1.0, "rejected");
        let mut gw4 = ResponsesWebSocketGateway::new(
            lifecycle.clone(),
            &auth,
            headers.clone(),
            1024 * 1024,
            16,
        )
        .with_codex_backend(true);
        let d4 = gw4
            .client_message(auth.clone(), multi_frame())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(d4.candidates[0].upstream_id, upstream_b);
        let f4: Value = serde_json::from_slice(&d4.frame).unwrap();
        assert_eq!(f4["input"].as_array().unwrap().len(), 3);
        assert_eq!(f4["input"][1]["encrypted_content"], cipher_b);
        gw4.close();

        // 5. `PrewarmResponse(B)` (`generate: false` -> `resp_prewarm_b`) + multi-owner `[rs_A (exhausted), rs_B (healthy)]`:
        // reconciles onto B, keeps `previous_response_id` and `rs_B` verbatim, strips ONLY `rs_A`.
        quota_cache.set_state(upstream_a, 1.0, "rejected");
        quota_cache.set_state(upstream_b, 0.1, "allowed");
        quota_cache.set_state(upstream_c, 0.5, "allowed");
        let mut gw_prewarm_match = ResponsesWebSocketGateway::new(
            lifecycle.clone(),
            &auth,
            headers.clone(),
            1024 * 1024,
            16,
        )
        .with_codex_backend(true);
        let prewarm_b_req = Bytes::from(
            serde_json::json!({
                "type": "response.create",
                "model": "gpt-5.4",
                "generate": false,
                "store": false,
                "input": []
            })
            .to_string(),
        );
        let prewarm_b_dispatch = gw_prewarm_match
            .client_message(auth.clone(), prewarm_b_req)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(prewarm_b_dispatch.candidates[0].upstream_id, upstream_b);
        gw_prewarm_match.assign_connection(prewarm_b_dispatch.turn_id, 1, upstream_b, "oauth-b");
        gw_prewarm_match.commit_send_attempt(prewarm_b_dispatch.turn_id);
        let _ = gw_prewarm_match.sent_with_timings(
            prewarm_b_dispatch.turn_id,
            ResponsesDispatchTimings::reused(),
            std::time::Instant::now(),
        );
        gw_prewarm_match
            .provider_message(
                1,
                &Bytes::from_static(
                    br#"{"type":"response.created","response":{"id":"resp_prewarm_b","status":"in_progress","store":false}}"#,
                ),
            )
            .await
            .unwrap();
        gw_prewarm_match
            .provider_message(
                1,
                &Bytes::from_static(
                    br#"{"type":"response.completed","response":{"id":"resp_prewarm_b","status":"completed","store":false}}"#,
                ),
            )
            .await
            .unwrap();
        let prewarm_multi_req = |prev_id: &str| {
            Bytes::from(
                serde_json::json!({
                    "type": "response.create",
                    "model": "gpt-5.4",
                    "previous_response_id": prev_id,
                    "store": false,
                    "include": ["reasoning.encrypted_content"],
                    "input": [
                        {"type": "reasoning", "id": "rs_a", "summary": [], "encrypted_content": cipher_a},
                        {"role": "user", "content": "turn 1"},
                        {"type": "reasoning", "id": "rs_b", "summary": [], "encrypted_content": cipher_b},
                        {"role": "user", "content": "turn 3 resume"}
                    ]
                })
                .to_string(),
            )
        };
        let d5 = gw_prewarm_match
            .client_message(auth.clone(), prewarm_multi_req("resp_prewarm_b"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(d5.candidates[0].upstream_id, upstream_b);
        let f5: Value = serde_json::from_slice(&d5.frame).unwrap();
        assert_eq!(f5["previous_response_id"], "resp_prewarm_b");
        assert_eq!(f5["input"].as_array().unwrap().len(), 3);
        assert_eq!(f5["input"][1]["encrypted_content"], cipher_b);
        gw_prewarm_match.close();

        // 6. `PrewarmResponse(A)` (`resp_prewarm_a`) + `[rs_A, rs_B (later & healthy)]`:
        // reconciles to B != prewarm_owner A -> returns 400 `previous_response_not_found` so Codex replays without `previous_response_id`.
        quota_cache.set_state(upstream_a, 0.05, "allowed");
        quota_cache.set_state(upstream_b, 0.1, "allowed");
        let mut gw_prewarm_mismatch = ResponsesWebSocketGateway::new(
            lifecycle.clone(),
            &auth,
            headers.clone(),
            1024 * 1024,
            16,
        )
        .with_codex_backend(true);
        let prewarm_a_req = Bytes::from(
            serde_json::json!({
                "type": "response.create",
                "model": "gpt-5.4",
                "generate": false,
                "store": false,
                "input": []
            })
            .to_string(),
        );
        let prewarm_a_dispatch = gw_prewarm_mismatch
            .client_message(auth.clone(), prewarm_a_req)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(prewarm_a_dispatch.candidates[0].upstream_id, upstream_a);
        gw_prewarm_mismatch.assign_connection(prewarm_a_dispatch.turn_id, 2, upstream_a, "oauth-a");
        gw_prewarm_mismatch.commit_send_attempt(prewarm_a_dispatch.turn_id);
        let _ = gw_prewarm_mismatch.sent_with_timings(
            prewarm_a_dispatch.turn_id,
            ResponsesDispatchTimings::reused(),
            std::time::Instant::now(),
        );
        gw_prewarm_mismatch
            .provider_message(
                2,
                &Bytes::from_static(
                    br#"{"type":"response.created","response":{"id":"resp_prewarm_a","status":"in_progress","store":false}}"#,
                ),
            )
            .await
            .unwrap();
        gw_prewarm_mismatch
            .provider_message(
                2,
                &Bytes::from_static(
                    br#"{"type":"response.completed","response":{"id":"resp_prewarm_a","status":"completed","store":false}}"#,
                ),
            )
            .await
            .unwrap();
        let mismatch_err = gw_prewarm_mismatch
            .client_message(auth.clone(), prewarm_multi_req("resp_prewarm_a"))
            .await
            .expect_err("mismatched prewarm owner must return 400 previous_response_not_found");
        assert_eq!(mismatch_err.status, 400);
        assert_eq!(mismatch_err.code, "previous_response_not_found");
        gw_prewarm_mismatch.close();

        // 7. Non-prewarm `WebSocketTurnOwner::Response(B)` negative control -> still rejects 400 "multiple upstreams".
        quota_cache.set_state(upstream_a, 1.0, "rejected");
        quota_cache.set_state(upstream_b, 0.1, "allowed");
        let mut gw_resp_owner = ResponsesWebSocketGateway::new(
            lifecycle.clone(),
            &auth,
            headers.clone(),
            1024 * 1024,
            16,
        )
        .with_codex_backend(true);
        let resp_b_req = Bytes::from(
            serde_json::json!({
                "type": "response.create",
                "model": "gpt-5.4",
                "store": false,
                "input": [{"role": "user", "content": "turn 1"}]
            })
            .to_string(),
        );
        let resp_b_dispatch = gw_resp_owner
            .client_message(auth.clone(), resp_b_req)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(resp_b_dispatch.candidates[0].upstream_id, upstream_b);
        gw_resp_owner.assign_connection(resp_b_dispatch.turn_id, 3, upstream_b, "oauth-b");
        gw_resp_owner.commit_send_attempt(resp_b_dispatch.turn_id);
        let _ = gw_resp_owner.sent_with_timings(
            resp_b_dispatch.turn_id,
            ResponsesDispatchTimings::reused(),
            std::time::Instant::now(),
        );
        gw_resp_owner
            .provider_message(
                3,
                &Bytes::from_static(
                    br#"{"type":"response.created","response":{"id":"resp_non_prewarm_b","status":"in_progress","store":false}}"#,
                ),
            )
            .await
            .unwrap();
        gw_resp_owner
            .provider_message(
                3,
                &Bytes::from_static(
                    br#"{"type":"response.completed","response":{"id":"resp_non_prewarm_b","status":"completed","store":false}}"#,
                ),
            )
            .await
            .unwrap();
        let resp_owner_err = gw_resp_owner
            .client_message(auth.clone(), prewarm_multi_req("resp_non_prewarm_b"))
            .await
            .expect_err("non-prewarm Response owner must reject multi-owner 400");
        assert_eq!(resp_owner_err.status, 400);
        assert_eq!(
            resp_owner_err.code,
            error_codes::UPSTREAM_AFFINITY_UNAVAILABLE
        );
        assert!(
            resp_owner_err.message.contains("multiple upstreams"),
            "{resp_owner_err:?}"
        );
        gw_resp_owner.close();

        // 8. Negative controls: `FailClosed` or `compaction` / unowned `previous_response_id` rejects 400 "opaque request content resolves to multiple upstreams".
        quota_cache.set_state(upstream_a, 0.05, "allowed");
        quota_cache.set_state(upstream_b, 0.1, "allowed");
        for bad in [
            Bytes::from(
                serde_json::json!({
                    "type": "response.create",
                    "model": "gpt-5.4",
                    "previous_response_id": "resp_prev",
                    "store": false,
                    "input": [
                        {"type": "reasoning", "id": "rs_a", "summary": [], "encrypted_content": cipher_a},
                        {"type": "reasoning", "id": "rs_b", "summary": [], "encrypted_content": cipher_b}
                    ]
                })
                .to_string(),
            ),
            Bytes::from(
                serde_json::json!({
                    "type": "response.create",
                    "model": "gpt-5.4",
                    "store": false,
                    "input": [
                        {"type": "compaction", "id": "cmp_1", "encrypted_content": cipher_a},
                        {"type": "reasoning", "id": "rs_b", "summary": [], "encrypted_content": cipher_b}
                    ]
                })
                .to_string(),
            ),
        ] {
            let mut bad_gw = ResponsesWebSocketGateway::new(
                lifecycle.clone(),
                &auth,
                headers.clone(),
                1024 * 1024,
                16,
            )
            .with_codex_backend(true);
            let err = bad_gw
                .client_message(auth.clone(), bad)
                .await
                .expect_err("must reject 400");
            assert_eq!(err.status, 400);
            assert!(err.message.contains("multiple upstreams"), "{err:?}");
            bad_gw.close();
        }
        let fc_lifecycle = Arc::new(
            Lifecycle::new_with_dynamic_view(
                authn,
                view_holder,
                Arc::new(UnusedDispatcher),
                crate::LifecycleConfig {
                    unavailable_openai_reasoning_owner:
                        cc_lb_domain::UnavailableOpenaiReasoningOwnerPolicy::FailClosed,
                    ..Default::default()
                },
                clock,
            )
            .with_upstream_affinity_store(store),
        );
        let mut fc_gw =
            ResponsesWebSocketGateway::new(fc_lifecycle, &auth, headers, 1024 * 1024, 16)
                .with_codex_backend(true);
        let fc_err = fc_gw
            .client_message(auth, multi_frame())
            .await
            .expect_err("FailClosed must reject multi-owner 400");
        assert_eq!(fc_err.status, 400);
        assert!(fc_err.message.contains("multiple upstreams"), "{fc_err:?}");
        fc_gw.close();
    }

    #[test]
    fn ws_rate_limit_cooldown_respects_reset_fields_expiry_and_clear() {
        use crate::lifecycle::{
            DEFAULT_WS_RATE_LIMIT_COOLDOWN_SECS, WsRateLimitCooldownCache,
            ws_rate_limit_cooldown_secs,
        };

        let now = 4_100_000_000_u64;
        assert_eq!(
            ws_rate_limit_cooldown_secs(None, None, now),
            DEFAULT_WS_RATE_LIMIT_COOLDOWN_SECS
        );
        assert_eq!(
            ws_rate_limit_cooldown_secs(
                Some(&serde_json::json!({
                    "type": "error",
                    "error": {"code": "usage_limit_reached", "resets_at": now + 180, "resets_in_seconds": 30}
                })),
                Some(12),
                now,
            ),
            180
        );
        assert_eq!(
            ws_rate_limit_cooldown_secs(
                Some(&serde_json::json!({
                    "type": "response.failed",
                    "response": {"error": {"code": "usage_limit_reached", "resets_in_seconds": 45}}
                })),
                Some(12),
                now,
            ),
            45
        );
        assert_eq!(ws_rate_limit_cooldown_secs(None, Some(90), now), 90);

        let cache = WsRateLimitCooldownCache::default();
        let a = Uuid::from_u128(0xAA01);
        assert!(!cache.has_any());
        cache.record(a, now + 60, now);
        assert!(cache.has_any());
        assert!(cache.is_blocked(a, now + 59));
        assert!(!cache.is_blocked(a, now + 60));

        cache.record(a, now + 120, now);
        assert!(cache.is_blocked(a, now + 100));
        cache.clear(a);
        assert!(!cache.has_any());
        assert!(!cache.is_blocked(a, now + 100));
    }

    #[tokio::test]
    async fn ws_fail_dispatch_recovery_keeps_local_limit_rejected_terminal_and_preserves_retry_after()
     {
        let (lifecycle, auth, headers, upstream_a, upstream_b, quota_cache, store) =
            setup_codex_ws_quota_fixture(
                cc_lb_domain::UnavailableOpenaiReasoningOwnerPolicy::StripAndFailover,
            )
            .await;
        quota_cache.set_state(upstream_a, 0.05, "allowed");
        quota_cache.set_state(upstream_b, 0.1, "allowed");
        let owned_cipher = "gAAAAAB_ws_local_limit_rej";
        store.bindings.lock().unwrap().insert(
            UpstreamAffinityKey {
                principal_id: auth.principal_id().to_owned(),
                provider: "openai".to_owned(),
                kind: cc_lb_storage_api::UpstreamAffinityKind::OpenaiResponseId,
                value_sha256: <sha2::Sha256 as sha2::Digest>::digest(owned_cipher.as_bytes())
                    .into(),
            },
            cc_lb_storage_api::upstream_affinity::UpstreamAffinityBinding {
                key: UpstreamAffinityKey {
                    principal_id: auth.principal_id().to_owned(),
                    provider: "openai".to_owned(),
                    kind: cc_lb_storage_api::UpstreamAffinityKind::OpenaiResponseId,
                    value_sha256: <sha2::Sha256 as sha2::Digest>::digest(owned_cipher.as_bytes())
                        .into(),
                },
                upstream_id: upstream_a,
                owner_epoch: 0,
                observed_at_unix_secs: 4_100_000_000,
                expires_at_unix_secs: None,
            },
        );

        let mut gw =
            ResponsesWebSocketGateway::new(lifecycle.clone(), &auth, headers, 1024 * 1024, 16)
                .with_codex_backend(true);
        let turn = gw
            .client_message(
                auth,
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "store": false,
                        "include": ["reasoning.encrypted_content"],
                        "input": [
                            {
                                "type": "reasoning",
                                "id": "rs_lim",
                                "summary": [],
                                "encrypted_content": owned_cipher
                            },
                            {"role": "user", "content": "hi"}
                        ]
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        let (ready, emitted) = gw
            .fail_dispatch_with_recovery(
                turn.turn_id,
                Some(upstream_a),
                &WebSocketError::new(429, error_codes::LIMIT_REJECTED, "key rate limit exceeded"),
            )
            .await;
        assert!(ready.is_empty());
        assert_eq!(
            emitted.as_ref().map(|e| e.code),
            Some(error_codes::LIMIT_REJECTED)
        );
        assert!(!gw.session_rate_limited_upstreams.contains(&upstream_a));
        assert!(!lifecycle.is_ws_rate_limited(upstream_a, 4_100_000_000));

        // Handshake 429 with `Retry-After: 5` preserves 5s cooldown (not extended to 60s).
        gw.record_session_rate_limited_with_retry_after(upstream_a, Some(5));
        assert!(gw.session_rate_limited_upstreams.contains(&upstream_a));
        assert!(lifecycle.is_ws_rate_limited(upstream_a, 4_100_000_004));
        assert!(!lifecycle.is_ws_rate_limited(upstream_a, 4_100_000_005));
        gw.close();
    }

    #[tokio::test]
    async fn ws_evict_upstream_releases_owner_bytes_and_preserves_pending_error_attribution() {
        let (lifecycle, auth, headers, upstream_a, upstream_b, quota_cache, _store) =
            setup_codex_ws_quota_fixture(
                cc_lb_domain::UnavailableOpenaiReasoningOwnerPolicy::StripAndFailover,
            )
            .await;
        quota_cache.set_state(upstream_a, 0.05, "allowed");
        quota_cache.set_state(upstream_b, 0.1, "allowed");

        let mk_req = |lane: &str, prev: Option<&str>, content: &str| {
            let mut obj = serde_json::json!({
                "type": "response.create",
                "model": "gpt-5.4",
                "stream_id": lane,
                "store": false,
                "input": [{"role": "user", "content": content}]
            });
            if let Some(prev_id) = prev {
                obj.as_object_mut().unwrap().insert(
                    "previous_response_id".to_owned(),
                    serde_json::json!(prev_id),
                );
            }
            Bytes::from(obj.to_string())
        };

        // Part A: Evicting unreferenced owner releases `owner_bytes` so a tight `byte_cap` allows replay
        // that would otherwise be rejected with 503 if `owner_bytes` stayed leaked.
        let sample_frame = mk_req("l1", None, "hello budget");
        let mut gw_budget = ResponsesWebSocketGateway::new(
            lifecycle.clone(),
            &auth,
            headers.clone(),
            1024 * 1024,
            8,
        )
        .with_codex_backend(true);
        let d1 = gw_budget
            .client_message(auth.clone(), sample_frame)
            .await
            .unwrap()
            .unwrap();
        let active_turn_bytes = gw_budget.used_bytes;
        let owner_up = d1.candidates[0].upstream_id;
        gw_budget.assign_connection(d1.turn_id, 1, owner_up, "oauth-a");
        gw_budget.commit_send_attempt(d1.turn_id);
        gw_budget.sent(d1.turn_id);
        gw_budget
            .provider_message(
                1,
                &Bytes::from_static(
                    br#"{"type":"response.created","stream_id":"l1","response":{"id":"resp_evict_budget_1","status":"in_progress","store":false}}"#,
                ),
            )
            .await
            .unwrap();
        gw_budget
            .provider_message(
                1,
                &Bytes::from_static(
                    br#"{"type":"response.completed","stream_id":"l1","response":{"id":"resp_evict_budget_1","status":"completed","store":false,"usage":{"input_tokens":5,"output_tokens":2}}}"#,
                ),
            )
            .await
            .unwrap();
        assert!(gw_budget.owners.contains_key("resp_evict_budget_1"));
        assert!(gw_budget.used_bytes > 0);
        let replay_frame = mk_req("l1", None, "replayed within tight cap");
        gw_budget.byte_cap = active_turn_bytes + 64;
        // Before eviction, retained `owner_bytes` causes 503 on tight cap.
        let err_before = gw_budget
            .client_message(auth.clone(), replay_frame.clone())
            .await
            .expect_err("tight byte_cap rejects while owner_bytes is still retained");
        assert_eq!(err_before.status, 503);
        gw_budget.evict_upstream_for_failover(owner_up);
        assert!(!gw_budget.owners.contains_key("resp_evict_budget_1"));
        assert_eq!(gw_budget.used_bytes, 0);
        // After eviction, `owner_bytes` + empty lane are freed so replay fits comfortably.
        assert!(
            gw_budget
                .client_message(auth.clone(), replay_frame)
                .await
                .is_ok()
        );
        gw_budget.close();

        // Part B: Parked `pending_error_terminals` owner survives eviction for delayed `response.failed`
        // attribution while continuation on it fails `400 previous_response_not_found`, and `owner_bytes`
        // frees when `response.failed` settles.
        let mut gw = ResponsesWebSocketGateway::new(lifecycle, &auth, headers, 1024 * 1024, 8)
            .with_codex_backend(true);
        let t_a1 = gw
            .client_message(auth.clone(), mk_req("l1", None, "turn l1"))
            .await
            .unwrap()
            .unwrap();
        gw.assign_connection(t_a1.turn_id, 1, upstream_a, "oauth-a");
        gw.commit_send_attempt(t_a1.turn_id);
        gw.sent(t_a1.turn_id);
        gw.provider_message(
            1,
            &Bytes::from_static(
                br#"{"type":"response.created","stream_id":"l1","response":{"id":"resp_parked_a","status":"in_progress","store":false}}"#,
            ),
        )
        .await
        .unwrap();
        gw.provider_message(
            1,
            &Bytes::from_static(
                br#"{"type":"error","stream_id":"l1","response_id":"resp_parked_a","status":503,"error":{"code":"server_is_overloaded","message":"overloaded"}}"#,
            ),
        )
        .await
        .unwrap();
        assert!(gw.pending_error_terminals.contains_key("resp_parked_a"));

        // Evict upstream_a across the session: owner stays retained for delayed attribution, but
        // marked `evicted_for_continuation` so continuation lookups return `400 previous_response_not_found`.
        gw.evict_upstream_for_failover(upstream_a);
        assert!(gw.owners.contains_key("resp_parked_a"));
        assert!(gw.owners["resp_parked_a"].evicted_for_continuation);

        let cont_err = gw
            .client_message(
                auth,
                mk_req("l2", Some("resp_parked_a"), "stale continuation"),
            )
            .await
            .expect_err("evicted owner must reject continuation lookup");
        assert_eq!(cont_err.status, 400);
        assert_eq!(cont_err.code, "previous_response_not_found");

        let delayed_res = gw
            .provider_message(
                1,
                &Bytes::from_static(
                    br#"{"type":"response.failed","stream_id":"l1","response":{"id":"resp_parked_a","status":"failed","error":{"code":"server_is_overloaded","message":"overloaded"},"usage":{"input_tokens":12,"output_tokens":3}}}"#,
                ),
            )
            .await
            .expect("delayed response.failed attributes through retained owner");
        assert!(delayed_res.forward);
        assert!(gw.pending_error_terminals.is_empty());
        assert!(!gw.owners.contains_key("resp_parked_a"));
        assert_eq!(gw.used_bytes, 0);
        gw.close();
    }

    #[tokio::test]
    async fn ws_codex_disallowed_model_rejects_403_without_evicting_owners() {
        use crate::builtin_filters::subscription_preference::SubscriptionPreferenceFilter;
        use cc_lb_control::api_keys::builtin_authn::BuiltinAuthn;
        use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
        use cc_lb_control::api_keys::principal_view::{
            DialectCache, PrincipalView, RouterPipelineCache,
        };
        use cc_lb_control::dynamic_view::DynamicViewHolder;
        use cc_lb_domain::TerminalStrategy;
        use cc_lb_routing::FilterPlugin;
        use cc_lb_storage_api::upstream::{UpstreamKind as StorageRecordKind, UpstreamRecord};
        use cc_lb_upstream::{
            ApiKeyAwareSignerFactory, RetryDecision, ShapedRequest, SignedRequest, Signer,
            SignerError, SignerFactory, SigningCapability, UpstreamError,
        };

        struct DummySignerFactory;
        impl ApiKeyAwareSignerFactory for DummySignerFactory {
            fn with_router_choice(&self, _name: String) -> Arc<dyn SignerFactory> {
                Arc::new(Self)
            }
        }
        #[async_trait::async_trait]
        impl SignerFactory for DummySignerFactory {
            async fn build(
                &self,
                _upstream: &cc_lb_domain::Upstream,
            ) -> Result<Arc<dyn Signer>, SignerError> {
                Ok(Arc::new(DummySigner))
            }
        }
        struct DummySigner;
        #[async_trait::async_trait]
        impl Signer for DummySigner {
            async fn sign(
                &self,
                shaped: ShapedRequest,
                capability: &mut SigningCapability,
            ) -> Result<SignedRequest, SignerError> {
                Ok(SignedRequest::from_shaped(shaped, capability))
            }
            async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
                RetryDecision::Fail
            }
        }
        struct UnusedDispatcher;
        #[async_trait::async_trait]
        impl crate::UpstreamDispatch for UnusedDispatcher {
            async fn dispatch(
                &self,
                _request: SignedRequest,
            ) -> Result<http::Response<crate::Body>, crate::DispatchError> {
                unreachable!("unused")
            }
        }

        let quota_cache = Arc::new(WsQuotaTestCache::default());
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(4_100_000_000));
        let storage = Arc::new(
            cc_lb_storage_sqlite::open_sqlite("sqlite::memory:", clock.clone())
                .await
                .unwrap(),
        );
        storage.initialize().await.unwrap();
        let keys = Arc::new(KeyStore::new(storage));
        let (_record, secret) = keys
            .create(
                DIRECT_PRINCIPAL,
                CreateParams {
                    label: "model-authz-ws".to_owned(),
                    description: None,
                    expires_at_unix_secs: None,
                    limit_overrides: Vec::new(),
                },
            )
            .await
            .unwrap();
        let filter: Arc<dyn FilterPlugin> =
            Arc::new(SubscriptionPreferenceFilter::with_clock(clock.clone()));
        let pipeline = Arc::new(RouterPipelineCache {
            user_filters: vec![filter],
            terminal: TerminalStrategy::FirstPick,
            instantiation_error: None,
            provider_filters: None,
        });
        let principal_view = Arc::new(PrincipalView::for_tests(
            DIRECT_PRINCIPAL,
            true,
            vec!["gpt-5.4".to_owned()],
            Vec::new(),
            HashMap::from([(
                DIRECT_PRINCIPAL.to_owned(),
                (Some(pipeline), DialectCache::Inherit),
            )]),
        ));
        let authn = Arc::new(BuiltinAuthn::new(keys, clock.clone()));
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", secret.expose().parse().unwrap());
        let auth = crate::authenticate_first(&authn, &headers, &principal_view)
            .await
            .unwrap();
        let upstream_a = Uuid::from_u128(0xD001);
        let upstream_b = Uuid::from_u128(0xD002);
        quota_cache.set_state(upstream_a, 0.05, "allowed");
        quota_cache.set_state(upstream_b, 0.2, "allowed");
        let view = cc_lb_control::DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(DummySignerFactory))
            .principal_view(principal_view)
            .subscription_quota_cache(quota_cache.clone())
            .subscription_quota_routing_max_staleness_secs(60)
            .upstream_records(vec![
                UpstreamRecord {
                    id: upstream_a,
                    name: "oauth-a".to_owned(),
                    kind: StorageRecordKind::OpenaiOauth,
                    enabled: true,
                    oauth_never_refresh: true,
                    ..Default::default()
                },
                UpstreamRecord {
                    id: upstream_b,
                    name: "oauth-b".to_owned(),
                    kind: StorageRecordKind::OpenaiOauth,
                    enabled: true,
                    oauth_never_refresh: true,
                    ..Default::default()
                },
            ])
            .build();
        let lifecycle = Arc::new(Lifecycle::new_with_dynamic_view(
            authn,
            Arc::new(DynamicViewHolder::new(view)),
            Arc::new(UnusedDispatcher),
            crate::LifecycleConfig::default(),
            clock,
        ));
        let mut gw = ResponsesWebSocketGateway::new(lifecycle, &auth, headers, 1024 * 1024, 8)
            .with_codex_backend(true);
        let d1 = gw
            .client_message(
                auth.clone(),
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "store": false,
                        "input": [{"role": "user", "content": "ok"}]
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        gw.assign_connection(d1.turn_id, 1, upstream_a, "oauth-a");
        gw.commit_send_attempt(d1.turn_id);
        gw.sent(d1.turn_id);
        gw.provider_message(
            1,
            &Bytes::from_static(
                br#"{"type":"response.created","response":{"id":"resp_authz_ok","status":"in_progress","store":false}}"#,
            ),
        )
        .await
        .unwrap();
        gw.provider_message(
            1,
            &Bytes::from_static(
                br#"{"type":"response.completed","response":{"id":"resp_authz_ok","status":"completed","store":false,"usage":{"input_tokens":5,"output_tokens":2}}}"#,
            ),
        )
        .await
        .unwrap();

        quota_cache.set_state(upstream_a, 1.0, "rejected");
        let err = gw
            .client_message(
                auth,
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "forbidden-model",
                        "previous_response_id": "resp_authz_ok",
                        "store": false,
                        "input": [{"role": "user", "content": "forbidden"}]
                    })
                    .to_string(),
                ),
            )
            .await
            .expect_err("disallowed model must return 403 without evicting owners");
        assert_eq!(err.status, 403);
        assert_eq!(err.code, "model_not_allowed");
        assert!(gw.owners.contains_key("resp_authz_ok"));
        assert!(!gw.owners["resp_authz_ok"].evicted_for_continuation);
        gw.close();
    }

    #[tokio::test]
    async fn ws_owner_failover_fallback_retains_no_extra_bytes_and_enforces_byte_cap_lazily() {
        let (lifecycle, auth, headers, upstream_a, upstream_b, quota_cache, store) =
            setup_codex_ws_quota_fixture(
                cc_lb_domain::UnavailableOpenaiReasoningOwnerPolicy::StripAndFailover,
            )
            .await;
        quota_cache.set_state(upstream_a, 0.05, "allowed");
        quota_cache.set_state(upstream_b, 0.1, "allowed");
        let owned_cipher = "gAAAAAB_ws_lazy_fallback";
        store.bindings.lock().unwrap().insert(
            UpstreamAffinityKey {
                principal_id: auth.principal_id().to_owned(),
                provider: "openai".to_owned(),
                kind: cc_lb_storage_api::UpstreamAffinityKind::OpenaiResponseId,
                value_sha256: <sha2::Sha256 as sha2::Digest>::digest(owned_cipher.as_bytes())
                    .into(),
            },
            cc_lb_storage_api::upstream_affinity::UpstreamAffinityBinding {
                key: UpstreamAffinityKey {
                    principal_id: auth.principal_id().to_owned(),
                    provider: "openai".to_owned(),
                    kind: cc_lb_storage_api::UpstreamAffinityKind::OpenaiResponseId,
                    value_sha256: <sha2::Sha256 as sha2::Digest>::digest(owned_cipher.as_bytes())
                        .into(),
                },
                upstream_id: upstream_a,
                owner_epoch: 0,
                observed_at_unix_secs: 4_100_000_000,
                expires_at_unix_secs: None,
            },
        );
        let req = Bytes::from(
            serde_json::json!({
                "type": "response.create",
                "model": "gpt-5.4",
                "store": false,
                "include": ["reasoning.encrypted_content"],
                "input": [
                    {
                        "type": "reasoning",
                        "id": "rs_lazy",
                        "summary": [],
                        "encrypted_content": owned_cipher
                    },
                    {"role": "user", "content": "lazy check"}
                ]
            })
            .to_string(),
        );
        let mut gw = ResponsesWebSocketGateway::new(
            lifecycle.clone(),
            &auth,
            headers.clone(),
            1024 * 1024,
            16,
        )
        .with_codex_backend(true);
        let d = gw
            .client_message(auth.clone(), req.clone())
            .await
            .unwrap()
            .unwrap();
        let turn = &gw.turns[&d.turn_id];
        assert!(turn.owner_failover_fallback.is_some());
        let expected_turn_charge = turn.frame.len()
            + json_owned_bytes(&turn.body)
            + std::mem::size_of::<Turn>()
            + turn.auth.principal_id().len()
            + turn.auth.key_id().len()
            + "gpt-5.4".len();
        assert_eq!(turn.charged_bytes, expected_turn_charge);
        let used_before = gw.used_bytes;

        gw.assign_connection(d.turn_id, 1, upstream_a, "oauth-a");
        gw.commit_send_attempt(d.turn_id);
        gw.sent(d.turn_id);
        let recovered = gw
            .try_owner_reasoning_failover_dispatch(
                d.turn_id,
                upstream_a,
                Some(1),
                &WsRelayPoolSnapshot::default(),
                0,
            )
            .await
            .expect("within budget lazy failover strips & dispatches");
        assert_eq!(recovered.candidates[0].upstream_id, upstream_b);
        assert!(gw.used_bytes < used_before);
        gw.finish_turn(d.turn_id, None);
        gw.prune_empty_lanes();
        assert_eq!(gw.used_bytes, 0);
        gw.close();
    }

    #[tokio::test]
    async fn ws_signing_failure_triggers_owner_reasoning_failover() {
        let (lifecycle, auth, headers, upstream_a, upstream_b, quota_cache, store) =
            setup_codex_ws_quota_fixture(
                cc_lb_domain::UnavailableOpenaiReasoningOwnerPolicy::StripAndFailover,
            )
            .await;
        quota_cache.set_state(upstream_a, 0.05, "allowed");
        quota_cache.set_state(upstream_b, 0.1, "allowed");
        let owned_cipher = "gAAAAAB_ws_sign_fail_a";
        store.bindings.lock().unwrap().insert(
            UpstreamAffinityKey {
                principal_id: auth.principal_id().to_owned(),
                provider: "openai".to_owned(),
                kind: cc_lb_storage_api::UpstreamAffinityKind::OpenaiResponseId,
                value_sha256: <sha2::Sha256 as sha2::Digest>::digest(owned_cipher.as_bytes())
                    .into(),
            },
            cc_lb_storage_api::upstream_affinity::UpstreamAffinityBinding {
                key: UpstreamAffinityKey {
                    principal_id: auth.principal_id().to_owned(),
                    provider: "openai".to_owned(),
                    kind: cc_lb_storage_api::UpstreamAffinityKind::OpenaiResponseId,
                    value_sha256: <sha2::Sha256 as sha2::Digest>::digest(owned_cipher.as_bytes())
                        .into(),
                },
                upstream_id: upstream_a,
                owner_epoch: 0,
                observed_at_unix_secs: 4_100_000_000,
                expires_at_unix_secs: None,
            },
        );
        let mut gw = ResponsesWebSocketGateway::new(lifecycle, &auth, headers, 1024 * 1024, 16)
            .with_codex_backend(true);
        let turn = gw
            .client_message(
                auth,
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "store": false,
                        "include": ["reasoning.encrypted_content"],
                        "input": [
                            {
                                "type": "reasoning",
                                "id": "rs_sign",
                                "summary": [],
                                "encrypted_content": owned_cipher
                            },
                            {"role": "user", "content": "sign retry"}
                        ]
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(turn.candidates[0].upstream_id, upstream_a);
        let (ready, emitted) = gw
            .fail_dispatch_with_recovery(
                turn.turn_id,
                Some(upstream_a),
                &WebSocketError::new(502, "upstream_signing_failed", "signer failed"),
            )
            .await;
        assert!(emitted.is_none());
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].candidates[0].upstream_id, upstream_b);
        let parsed: Value = serde_json::from_slice(&ready[0].frame).unwrap();
        assert_eq!(parsed["input"].as_array().unwrap().len(), 1);
        gw.close();
    }

    #[tokio::test]
    async fn ws_slot_cap_one_retires_idle_socket_on_429_and_relays_when_busy() {
        let (lifecycle, auth, headers, upstream_a, upstream_b, quota_cache, store) =
            setup_codex_ws_quota_fixture(
                cc_lb_domain::UnavailableOpenaiReasoningOwnerPolicy::StripAndFailover,
            )
            .await;
        quota_cache.set_state(upstream_a, 0.05, "allowed");
        quota_cache.set_state(upstream_b, 0.1, "allowed");
        let owned_cipher = "gAAAAAB_ws_slot1_a";
        store.bindings.lock().unwrap().insert(
            UpstreamAffinityKey {
                principal_id: auth.principal_id().to_owned(),
                provider: "openai".to_owned(),
                kind: cc_lb_storage_api::UpstreamAffinityKind::OpenaiResponseId,
                value_sha256: <sha2::Sha256 as sha2::Digest>::digest(owned_cipher.as_bytes())
                    .into(),
            },
            cc_lb_storage_api::upstream_affinity::UpstreamAffinityBinding {
                key: UpstreamAffinityKey {
                    principal_id: auth.principal_id().to_owned(),
                    provider: "openai".to_owned(),
                    kind: cc_lb_storage_api::UpstreamAffinityKind::OpenaiResponseId,
                    value_sha256: <sha2::Sha256 as sha2::Digest>::digest(owned_cipher.as_bytes())
                        .into(),
                },
                upstream_id: upstream_a,
                owner_epoch: 0,
                observed_at_unix_secs: 4_100_000_000,
                expires_at_unix_secs: None,
            },
        );
        let mk_turn = |lane: &str| {
            Bytes::from(
                serde_json::json!({
                    "type": "response.create",
                    "model": "gpt-5.4",
                    "stream_id": lane,
                    "store": false,
                    "include": ["reasoning.encrypted_content"],
                    "input": [
                        {
                            "type": "reasoning",
                            "id": "rs_s1",
                            "summary": [],
                            "encrypted_content": owned_cipher
                        },
                        {"role": "user", "content": "slot1"}
                    ]
                })
                .to_string(),
            )
        };

        // 1. Idle conn_a with slot_cap = 1 retires conn_a and dispatches onto upstream_b.
        let mut gw_idle = ResponsesWebSocketGateway::new(
            lifecycle.clone(),
            &auth,
            headers.clone(),
            1024 * 1024,
            1,
        )
        .with_codex_backend(true);
        let d1 = gw_idle
            .client_message(auth.clone(), mk_turn("l1"))
            .await
            .unwrap()
            .unwrap();
        gw_idle.connections.insert(1, upstream_a);
        gw_idle.assign_connection(d1.turn_id, 1, upstream_a, "oauth-a");
        gw_idle.commit_send_attempt(d1.turn_id);
        gw_idle.sent(d1.turn_id);
        let res1 = gw_idle
            .provider_message(
                1,
                &Bytes::from_static(
                    br#"{"type":"error","stream_id":"l1","error":{"code":"usage_limit_reached","message":"429"}}"#,
                ),
            )
            .await
            .unwrap();
        assert!(!res1.forward);
        assert_eq!(res1.retire_connections, vec![1]);
        assert!(!gw_idle.connections.contains_key(&1));
        assert_eq!(res1.dispatches.len(), 1);
        assert_eq!(res1.dispatches[0].candidates[0].upstream_id, upstream_b);
        gw_idle.close();

        // 2. Busy conn_a (holds a live completed response owner `resp_prior_a`) with slot_cap = 1
        // cannot retire conn_a and has no reusable B socket, so original 429 forwards verbatim
        // instead of failing later with 503. (Fresh fixture so upstream_a is not in Part 1 cooldown.)
        let (busy_lifecycle, busy_auth, busy_headers, busy_a, busy_b, busy_quota, busy_store) =
            setup_codex_ws_quota_fixture(
                cc_lb_domain::UnavailableOpenaiReasoningOwnerPolicy::StripAndFailover,
            )
            .await;
        busy_quota.set_state(busy_a, 0.05, "allowed");
        busy_quota.set_state(busy_b, 0.1, "allowed");
        let mut gw_busy = ResponsesWebSocketGateway::new(
            busy_lifecycle,
            &busy_auth,
            busy_headers,
            1024 * 1024,
            1,
        )
        .with_codex_backend(true);
        let da = gw_busy
            .client_message(
                busy_auth.clone(),
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "stream_id": "l1",
                        "store": false,
                        "input": [{"role": "user", "content": "prior owner turn"}]
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        let prior_up = da.candidates[0].upstream_id;
        let other_up = if prior_up == busy_a { busy_b } else { busy_a };
        gw_busy.connections.insert(1, prior_up);
        gw_busy.assign_connection(da.turn_id, 1, prior_up, "oauth-prior");
        gw_busy.commit_send_attempt(da.turn_id);
        gw_busy.sent(da.turn_id);
        gw_busy
            .provider_message(
                1,
                &Bytes::from_static(
                    br#"{"type":"response.created","stream_id":"l1","response":{"id":"resp_prior_a","status":"in_progress","store":false}}"#,
                ),
            )
            .await
            .unwrap();
        gw_busy
            .provider_message(
                1,
                &Bytes::from_static(
                    br#"{"type":"response.completed","stream_id":"l1","response":{"id":"resp_prior_a","status":"completed","store":false,"usage":{"input_tokens":5,"output_tokens":2}}}"#,
                ),
            )
            .await
            .unwrap();

        // Bind a reasoning ciphertext owned by `prior_up` so the second turn carries `owner_failover_fallback`
        // to `other_up` while running on busy `conn_a = 1`.
        busy_store.bindings.lock().unwrap().insert(
            UpstreamAffinityKey {
                principal_id: busy_auth.principal_id().to_owned(),
                provider: "openai".to_owned(),
                kind: cc_lb_storage_api::UpstreamAffinityKind::OpenaiResponseId,
                value_sha256: <sha2::Sha256 as sha2::Digest>::digest(owned_cipher.as_bytes())
                    .into(),
            },
            cc_lb_storage_api::upstream_affinity::UpstreamAffinityBinding {
                key: UpstreamAffinityKey {
                    principal_id: busy_auth.principal_id().to_owned(),
                    provider: "openai".to_owned(),
                    kind: cc_lb_storage_api::UpstreamAffinityKind::OpenaiResponseId,
                    value_sha256: <sha2::Sha256 as sha2::Digest>::digest(owned_cipher.as_bytes())
                        .into(),
                },
                upstream_id: prior_up,
                owner_epoch: 0,
                observed_at_unix_secs: 4_100_000_000,
                expires_at_unix_secs: None,
            },
        );
        let db = gw_busy
            .client_message(busy_auth, mk_turn("l1"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(db.candidates[0].upstream_id, prior_up);
        assert!(
            gw_busy.turns[&db.turn_id]
                .owner_failover_fallback
                .as_ref()
                .is_some_and(|fb| { fb.candidates.iter().any(|c| c.upstream_id == other_up) })
        );
        gw_busy.assign_connection(db.turn_id, 1, prior_up, "oauth-prior");
        gw_busy.commit_send_attempt(db.turn_id);
        gw_busy.sent(db.turn_id);

        let res_busy = gw_busy
            .provider_message(
                1,
                &Bytes::from_static(
                    br#"{"type":"error","stream_id":"l1","error":{"code":"usage_limit_reached","message":"429"}}"#,
                ),
            )
            .await
            .unwrap();
        assert!(res_busy.forward);
        assert!(res_busy.retire_connections.is_empty());
        assert!(res_busy.dispatches.is_empty());
        gw_busy.close();
    }

    #[tokio::test]
    async fn multi_owner_openai_reasoning_reconciliation_ws_review_cases() {
        use cc_lb_control::api_keys::builtin_authn::BuiltinAuthn;
        use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
        use cc_lb_control::api_keys::principal_view::{
            DialectCache, PrincipalView, RouterPipelineCache,
        };
        use cc_lb_control::dynamic_view::{DynamicViewBuilder, DynamicViewHolder};
        use cc_lb_domain::{Principal, TerminalStrategy, UpstreamCandidate};
        use cc_lb_routing::{FilterError, FilterOutput, FilterPlugin, RoutingContext};
        use cc_lb_storage_api::upstream::{UpstreamKind as StorageRecordKind, UpstreamRecord};
        use cc_lb_upstream::{
            ApiKeyAwareSignerFactory, RetryDecision, ShapedRequest, SignedRequest, Signer,
            SignerError, SignerFactory, SigningCapability, UpstreamError,
        };

        struct DummySignerFactory;
        impl ApiKeyAwareSignerFactory for DummySignerFactory {
            fn with_router_choice(&self, _name: String) -> Arc<dyn SignerFactory> {
                Arc::new(Self)
            }
        }
        #[async_trait::async_trait]
        impl SignerFactory for DummySignerFactory {
            async fn build(
                &self,
                _upstream: &cc_lb_domain::Upstream,
            ) -> Result<Arc<dyn Signer>, SignerError> {
                Ok(Arc::new(DummySigner))
            }
        }
        struct DummySigner;
        #[async_trait::async_trait]
        impl Signer for DummySigner {
            async fn sign(
                &self,
                shaped: ShapedRequest,
                capability: &mut SigningCapability,
            ) -> Result<SignedRequest, SignerError> {
                Ok(SignedRequest::from_shaped(shaped, capability))
            }
            async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
                RetryDecision::Fail
            }
        }
        struct UnusedDispatcher;
        #[async_trait::async_trait]
        impl crate::UpstreamDispatch for UnusedDispatcher {
            async fn dispatch(
                &self,
                _request: SignedRequest,
            ) -> Result<http::Response<crate::Body>, crate::DispatchError> {
                unreachable!("unused")
            }
        }
        struct ExcludeBFilter {
            exclude_enabled: std::sync::atomic::AtomicBool,
            excluded: Uuid,
        }
        impl FilterPlugin for ExcludeBFilter {
            fn filter(
                &self,
                _ctx: &RoutingContext,
                _principal: &Principal,
                candidates: &[UpstreamCandidate],
            ) -> Result<FilterOutput, FilterError> {
                let exclude = self
                    .exclude_enabled
                    .load(std::sync::atomic::Ordering::SeqCst);
                Ok(FilterOutput {
                    kept_upstream_ids: candidates
                        .iter()
                        .filter(|c| !exclude || c.upstream_id != self.excluded)
                        .map(|c| c.upstream_id)
                        .collect(),
                    reason: "exclude-b".to_owned(),
                    per_candidate_reasons: Vec::new(),
                    subscription_preference: None,
                })
            }
            fn plugin_id(&self) -> Uuid {
                Uuid::from_u128(0xE1C10E)
            }
            fn plugin_name(&self) -> &str {
                "exclude-b-ws"
            }
        }

        let quota_cache = Arc::new(WsQuotaTestCache::default());
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(4_100_000_000));
        let storage = Arc::new(
            cc_lb_storage_sqlite::open_sqlite("sqlite::memory:", clock.clone())
                .await
                .unwrap(),
        );
        storage.initialize().await.unwrap();
        let keys = Arc::new(KeyStore::new(storage));
        let (_record, secret) = keys
            .create(
                DIRECT_PRINCIPAL,
                CreateParams {
                    label: "ws-multi-review".to_owned(),
                    description: None,
                    expires_at_unix_secs: None,
                    limit_overrides: Vec::new(),
                },
            )
            .await
            .unwrap();
        let upstream_a = Uuid::parse_str("30000000-0000-0000-0000-000000000801").unwrap();
        let upstream_b = Uuid::parse_str("30000000-0000-0000-0000-000000000802").unwrap();
        let upstream_c = Uuid::parse_str("30000000-0000-0000-0000-000000000803").unwrap();
        let api_key_d = Uuid::parse_str("30000000-0000-0000-0000-000000000804").unwrap();
        let exclude_filter = Arc::new(ExcludeBFilter {
            exclude_enabled: std::sync::atomic::AtomicBool::new(false),
            excluded: upstream_b,
        });
        let sub_filter: Arc<dyn FilterPlugin> = Arc::new(
            crate::builtin_filters::subscription_preference::SubscriptionPreferenceFilter::with_clock(
                clock.clone(),
            ),
        );
        let pipeline = Arc::new(RouterPipelineCache {
            user_filters: vec![exclude_filter.clone(), sub_filter],
            terminal: TerminalStrategy::FirstPick,
            instantiation_error: None,
            provider_filters: None,
        });
        let principal_view = Arc::new(PrincipalView::for_tests(
            DIRECT_PRINCIPAL,
            true,
            vec!["*".to_owned()],
            Vec::new(),
            HashMap::from([(
                DIRECT_PRINCIPAL.to_owned(),
                (Some(pipeline), DialectCache::Inherit),
            )]),
        ));
        let authn = Arc::new(BuiltinAuthn::new(keys, clock.clone()));
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", secret.expose().parse().unwrap());
        let auth = crate::authenticate_first(&authn, &headers, &principal_view)
            .await
            .unwrap();
        let store = Arc::new(PrewarmAffinityStore::default());
        let cipher_a1 = "gAAAAAB_ws_rev_a1";
        let cipher_b = "gAAAAAB_ws_rev_b";
        let cipher_a2 = "gAAAAAB_ws_rev_a2";
        let cipher_api = "gAAAAAB_ws_rev_api";
        for (cipher, owner) in [
            (cipher_a1, upstream_a),
            (cipher_b, upstream_b),
            (cipher_a2, upstream_a),
            (cipher_api, api_key_d),
        ] {
            let key = UpstreamAffinityKey {
                principal_id: auth.principal_id().to_owned(),
                provider: "openai".to_owned(),
                kind: cc_lb_storage_api::UpstreamAffinityKind::OpenaiResponseId,
                value_sha256: <sha2::Sha256 as sha2::Digest>::digest(cipher.as_bytes()).into(),
            };
            store.bindings.lock().unwrap().insert(
                key.clone(),
                cc_lb_storage_api::upstream_affinity::UpstreamAffinityBinding {
                    key,
                    upstream_id: owner,
                    owner_epoch: 0,
                    observed_at_unix_secs: 4_100_000_000,
                    expires_at_unix_secs: None,
                },
            );
        }
        let oauth_recs = vec![
            UpstreamRecord {
                id: upstream_a,
                name: "oauth-a".to_owned(),
                kind: StorageRecordKind::OpenaiOauth,
                enabled: true,
                oauth_never_refresh: true,
                ..Default::default()
            },
            UpstreamRecord {
                id: upstream_b,
                name: "oauth-b".to_owned(),
                kind: StorageRecordKind::OpenaiOauth,
                enabled: true,
                oauth_never_refresh: true,
                ..Default::default()
            },
            UpstreamRecord {
                id: upstream_c,
                name: "oauth-c".to_owned(),
                kind: StorageRecordKind::OpenaiOauth,
                enabled: true,
                oauth_never_refresh: true,
                ..Default::default()
            },
        ];
        let view = DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(DummySignerFactory))
            .principal_view(principal_view.clone())
            .subscription_quota_cache(quota_cache.clone())
            .subscription_quota_routing_max_staleness_secs(60)
            .upstream_records(oauth_recs)
            .build();
        let lifecycle = Arc::new(
            Lifecycle::new_with_dynamic_view(
                authn.clone(),
                Arc::new(DynamicViewHolder::new(view)),
                Arc::new(UnusedDispatcher),
                crate::LifecycleConfig::default(),
                clock.clone(),
            )
            .with_upstream_affinity_store(store.clone()),
        );

        // Finding 8 (WS): `[rs_A, rs_B, rs_A]` with repeated identical ciphertext on A
        // selects A by last occurrence.
        quota_cache.set_state(upstream_a, 0.1, "allowed");
        quota_cache.set_state(upstream_b, 0.1, "allowed");
        quota_cache.set_state(upstream_c, 0.5, "allowed");
        let mut gw8 = ResponsesWebSocketGateway::new(
            lifecycle.clone(),
            &auth,
            headers.clone(),
            1024 * 1024,
            16,
        )
        .with_codex_backend(true);
        let d8 = gw8
            .client_message(
                auth.clone(),
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "store": false,
                        "input": [
                            {"type": "reasoning", "id": "rs_a1", "summary": [], "encrypted_content": cipher_a1},
                            {"role": "user", "content": "t1"},
                            {"type": "reasoning", "id": "rs_b", "summary": [], "encrypted_content": cipher_b},
                            {"role": "user", "content": "t2"},
                            {"type": "reasoning", "id": "rs_a_repeat", "summary": [], "encrypted_content": cipher_a1},
                            {"role": "user", "content": "t3"}
                        ]
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(d8.candidates[0].upstream_id, upstream_a);
        let f8: Value = serde_json::from_slice(&d8.frame).unwrap();
        assert_eq!(f8["input"].as_array().unwrap().len(), 5);
        assert_eq!(f8["input"][0]["encrypted_content"], cipher_a1);
        assert_eq!(f8["input"][3]["encrypted_content"], cipher_a1);
        gw8.close();

        // Finding 9 (WS): `[rs_A, rs_B]` where filter excludes B picks A keeping `rs_A`.
        exclude_filter
            .exclude_enabled
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let mut gw9 = ResponsesWebSocketGateway::new(
            lifecycle.clone(),
            &auth,
            headers.clone(),
            1024 * 1024,
            16,
        )
        .with_codex_backend(true);
        let d9 = gw9
            .client_message(
                auth.clone(),
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "store": false,
                        "input": [
                            {"type": "reasoning", "id": "rs_a1", "summary": [], "encrypted_content": cipher_a1},
                            {"role": "user", "content": "t1"},
                            {"type": "reasoning", "id": "rs_b", "summary": [], "encrypted_content": cipher_b},
                            {"role": "user", "content": "t2"}
                        ]
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(d9.candidates[0].upstream_id, upstream_a);
        let f9: Value = serde_json::from_slice(&d9.frame).unwrap();
        assert_eq!(f9["input"].as_array().unwrap().len(), 3);
        assert_eq!(f9["input"][0]["encrypted_content"], cipher_a1);
        gw9.close();
        exclude_filter
            .exclude_enabled
            .store(false, std::sync::atomic::Ordering::SeqCst);

        // Finding 10 (WS): `PrewarmResponse(A)` + `[rs_A, rs_B]` when ALL A, B, C are exhausted
        // stays on `prewarm_owner` A with `previous_response_id` and `rs_A` verbatim (no false 400).
        let mut gw10 =
            ResponsesWebSocketGateway::new(lifecycle, &auth, headers.clone(), 1024 * 1024, 16)
                .with_codex_backend(true);
        let pw_a = gw10
            .client_message(
                auth.clone(),
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "generate": false,
                        "store": false,
                        "input": []
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        gw10.assign_connection(pw_a.turn_id, 1, upstream_a, "oauth-a");
        gw10.commit_send_attempt(pw_a.turn_id);
        gw10.sent(pw_a.turn_id);
        gw10
            .provider_message(
                1,
                &Bytes::from_static(
                    br#"{"type":"response.created","response":{"id":"resp_pw_all_exh","status":"in_progress","store":false}}"#,
                ),
            )
            .await
            .unwrap();
        gw10
            .provider_message(
                1,
                &Bytes::from_static(
                    br#"{"type":"response.completed","response":{"id":"resp_pw_all_exh","status":"completed","store":false}}"#,
                ),
            )
            .await
            .unwrap();
        quota_cache.set_state(upstream_a, 1.0, "rejected");
        quota_cache.set_state(upstream_b, 1.0, "rejected");
        quota_cache.set_state(upstream_c, 1.0, "rejected");
        let d10 = gw10
            .client_message(
                auth.clone(),
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "previous_response_id": "resp_pw_all_exh",
                        "store": false,
                        "input": [
                            {"type": "reasoning", "id": "rs_a1", "summary": [], "encrypted_content": cipher_a1},
                            {"role": "user", "content": "t1"},
                            {"type": "reasoning", "id": "rs_b", "summary": [], "encrypted_content": cipher_b},
                            {"role": "user", "content": "t2"}
                        ]
                    })
                    .to_string(),
                ),
            )
            .await
            .expect("PrewarmResponse(A) when all exhausted must reconcile to A instead of false 400")
            .unwrap();
        assert_eq!(d10.candidates[0].upstream_id, upstream_a);
        let f10: Value = serde_json::from_slice(&d10.frame).unwrap();
        assert_eq!(f10["previous_response_id"], "resp_pw_all_exh");
        assert_eq!(f10["input"].as_array().unwrap().len(), 3);
        assert_eq!(f10["input"][0]["encrypted_content"], cipher_a1);
        gw10.close();

        // Finding 11 (WS): Non-Codex WS owned by `OpenaiApiKey` with explicit `store: true`
        // must never fail over onto `OpenaiOauth`.
        quota_cache.set_state(upstream_b, 0.1, "allowed");
        let pub_view = DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(DummySignerFactory))
            .principal_view(principal_view)
            .subscription_quota_cache(quota_cache)
            .subscription_quota_routing_max_staleness_secs(60)
            .upstream_records(vec![
                UpstreamRecord {
                    id: api_key_d,
                    name: "api-d".to_owned(),
                    kind: StorageRecordKind::OpenaiApiKey,
                    enabled: false,
                    ..Default::default()
                },
                UpstreamRecord {
                    id: upstream_b,
                    name: "oauth-b".to_owned(),
                    kind: StorageRecordKind::OpenaiOauth,
                    enabled: true,
                    oauth_never_refresh: true,
                    ..Default::default()
                },
            ])
            .build();
        let pub_lifecycle = Arc::new(
            Lifecycle::new_with_dynamic_view(
                authn,
                Arc::new(DynamicViewHolder::new(pub_view)),
                Arc::new(UnusedDispatcher),
                crate::LifecycleConfig::default(),
                clock,
            )
            .with_upstream_affinity_store(store),
        );
        let mut gw11 =
            ResponsesWebSocketGateway::new(pub_lifecycle, &auth, headers, 1024 * 1024, 16);
        let err11 = gw11
            .client_message(
                auth,
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "store": true,
                        "input": [
                            {"type": "reasoning", "id": "rs_api", "summary": [], "encrypted_content": cipher_api},
                            {"role": "user", "content": "store true"}
                        ]
                    })
                    .to_string(),
                ),
            )
            .await
            .expect_err("explicit store:true API-key owner must never downgrade to OAuth on WS");
        assert_eq!(err11.status, 409);
        assert_eq!(err11.code, error_codes::UPSTREAM_AFFINITY_UNAVAILABLE);
        gw11.close();
    }

    #[tokio::test]
    async fn ws_prewarm_owner_with_foreign_multi_owner_ciphertexts_and_fail_closed() {
        use cc_lb_control::api_keys::builtin_authn::BuiltinAuthn;
        use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
        use cc_lb_control::api_keys::principal_view::{
            DialectCache, PrincipalView, RouterPipelineCache,
        };
        use cc_lb_control::dynamic_view::{DynamicViewBuilder, DynamicViewHolder};
        use cc_lb_domain::{TerminalStrategy, UnknownOpenaiReasoningPolicy};
        use cc_lb_routing::FilterPlugin;
        use cc_lb_storage_api::upstream::{UpstreamKind as StorageRecordKind, UpstreamRecord};
        use cc_lb_upstream::{
            ApiKeyAwareSignerFactory, RetryDecision, ShapedRequest, SignedRequest, Signer,
            SignerError, SignerFactory, SigningCapability, UpstreamError,
        };

        struct DummySignerFactory;
        impl ApiKeyAwareSignerFactory for DummySignerFactory {
            fn with_router_choice(&self, _name: String) -> Arc<dyn SignerFactory> {
                Arc::new(Self)
            }
        }
        #[async_trait::async_trait]
        impl SignerFactory for DummySignerFactory {
            async fn build(
                &self,
                _upstream: &cc_lb_domain::Upstream,
            ) -> Result<Arc<dyn Signer>, SignerError> {
                Ok(Arc::new(DummySigner))
            }
        }
        struct DummySigner;
        #[async_trait::async_trait]
        impl Signer for DummySigner {
            async fn sign(
                &self,
                shaped: ShapedRequest,
                capability: &mut SigningCapability,
            ) -> Result<SignedRequest, SignerError> {
                Ok(SignedRequest::from_shaped(shaped, capability))
            }
            async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
                RetryDecision::Fail
            }
        }
        struct UnusedDispatcher;
        #[async_trait::async_trait]
        impl crate::UpstreamDispatch for UnusedDispatcher {
            async fn dispatch(
                &self,
                _request: SignedRequest,
            ) -> Result<http::Response<crate::Body>, crate::DispatchError> {
                unreachable!("unused")
            }
        }

        let quota_cache = Arc::new(WsQuotaTestCache::default());
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(4_100_000_000));
        let storage = Arc::new(
            cc_lb_storage_sqlite::open_sqlite("sqlite::memory:", clock.clone())
                .await
                .unwrap(),
        );
        storage.initialize().await.unwrap();
        let keys = Arc::new(KeyStore::new(storage));
        let (_record, secret) = keys
            .create(
                DIRECT_PRINCIPAL,
                CreateParams {
                    label: "ws-foreign-prewarm-fc".to_owned(),
                    description: None,
                    expires_at_unix_secs: None,
                    limit_overrides: Vec::new(),
                },
            )
            .await
            .unwrap();
        let upstream_a = Uuid::parse_str("30000000-0000-0000-0000-000000000811").unwrap();
        let upstream_b = Uuid::parse_str("30000000-0000-0000-0000-000000000812").unwrap();
        let upstream_c = Uuid::parse_str("30000000-0000-0000-0000-000000000813").unwrap();
        let sub_filter: Arc<dyn FilterPlugin> = Arc::new(
            crate::builtin_filters::subscription_preference::SubscriptionPreferenceFilter::with_clock(
                clock.clone(),
            ),
        );
        let pipeline = Arc::new(RouterPipelineCache {
            user_filters: vec![sub_filter],
            terminal: TerminalStrategy::FirstPick,
            instantiation_error: None,
            provider_filters: None,
        });
        let principal_view = Arc::new(PrincipalView::for_tests(
            DIRECT_PRINCIPAL,
            true,
            vec!["*".to_owned()],
            Vec::new(),
            HashMap::from([(
                DIRECT_PRINCIPAL.to_owned(),
                (Some(pipeline), DialectCache::Inherit),
            )]),
        ));
        let authn = Arc::new(BuiltinAuthn::new(keys, clock.clone()));
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", secret.expose().parse().unwrap());
        let auth = crate::authenticate_first(&authn, &headers, &principal_view)
            .await
            .unwrap();
        let store = Arc::new(PrewarmAffinityStore::default());
        let cipher_a = "gAAAAAB_fp_a";
        let cipher_b = "gAAAAAB_fp_b";
        let cipher_c = "gAAAAAB_fp_c";
        let cipher_unbound = "gAAAAAB_fp_unbound";
        for (cipher, owner) in [
            (cipher_a, upstream_a),
            (cipher_b, upstream_b),
            (cipher_c, upstream_c),
        ] {
            let key = UpstreamAffinityKey {
                principal_id: auth.principal_id().to_owned(),
                provider: "openai".to_owned(),
                kind: cc_lb_storage_api::UpstreamAffinityKind::OpenaiResponseId,
                value_sha256: <sha2::Sha256 as sha2::Digest>::digest(cipher.as_bytes()).into(),
            };
            store.bindings.lock().unwrap().insert(
                key.clone(),
                cc_lb_storage_api::upstream_affinity::UpstreamAffinityBinding {
                    key,
                    upstream_id: owner,
                    owner_epoch: 0,
                    observed_at_unix_secs: 4_100_000_000,
                    expires_at_unix_secs: None,
                },
            );
        }
        let mk_recs = |a_enabled: bool, b_enabled: bool, c_enabled: bool| {
            vec![
                UpstreamRecord {
                    id: upstream_a,
                    name: "oauth-a".to_owned(),
                    kind: StorageRecordKind::OpenaiOauth,
                    enabled: a_enabled,
                    oauth_never_refresh: true,
                    ..Default::default()
                },
                UpstreamRecord {
                    id: upstream_b,
                    name: "oauth-b".to_owned(),
                    kind: StorageRecordKind::OpenaiOauth,
                    enabled: b_enabled,
                    oauth_never_refresh: true,
                    ..Default::default()
                },
                UpstreamRecord {
                    id: upstream_c,
                    name: "oauth-c".to_owned(),
                    kind: StorageRecordKind::OpenaiOauth,
                    enabled: c_enabled,
                    oauth_never_refresh: true,
                    ..Default::default()
                },
            ]
        };

        // Part 1: Prewarm on A while input has only foreign ciphertexts [B, C].
        quota_cache.set_state(upstream_a, 0.1, "allowed");
        quota_cache.set_state(upstream_b, 1.0, "rejected");
        quota_cache.set_state(upstream_c, 1.0, "rejected");
        let view = DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(DummySignerFactory))
            .principal_view(principal_view.clone())
            .subscription_quota_cache(quota_cache.clone())
            .subscription_quota_routing_max_staleness_secs(60)
            .upstream_records(mk_recs(true, true, true))
            .build();
        let lifecycle = Arc::new(
            Lifecycle::new_with_dynamic_view(
                authn.clone(),
                Arc::new(DynamicViewHolder::new(view)),
                Arc::new(UnusedDispatcher),
                crate::LifecycleConfig::default(),
                clock.clone(),
            )
            .with_upstream_affinity_store(store.clone()),
        );
        let mut gw =
            ResponsesWebSocketGateway::new(lifecycle, &auth, headers.clone(), 1024 * 1024, 16)
                .with_codex_backend(true);
        let pw = gw
            .client_message(
                auth.clone(),
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "generate": false,
                        "store": false,
                        "input": []
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pw.candidates[0].upstream_id, upstream_a);
        gw.assign_connection(pw.turn_id, 1, upstream_a, "oauth-a");
        gw.commit_send_attempt(pw.turn_id);
        gw.sent(pw.turn_id);
        gw.provider_message(
            1,
            &Bytes::from_static(
                br#"{"type":"response.created","response":{"id":"resp_pw_foreign","status":"in_progress","store":false}}"#,
            ),
        )
        .await
        .unwrap();
        gw.provider_message(
            1,
            &Bytes::from_static(
                br#"{"type":"response.completed","response":{"id":"resp_pw_foreign","status":"completed","store":false}}"#,
            ),
        )
        .await
        .unwrap();

        let mk_foreign_turn = || {
            Bytes::from(
                serde_json::json!({
                    "type": "response.create",
                    "model": "gpt-5.4",
                    "previous_response_id": "resp_pw_foreign",
                    "store": false,
                    "input": [
                        {"type": "reasoning", "id": "rs_b", "summary": [], "encrypted_content": cipher_b},
                        {"role": "user", "content": "tb"},
                        {"type": "reasoning", "id": "rs_c", "summary": [], "encrypted_content": cipher_c},
                        {"role": "user", "content": "tc"}
                    ]
                })
                .to_string(),
            )
        };

        // Case 1a: A healthy, B & C exhausted -> probes A even though A owns neither B nor C,
        // keeps previous_response_id, strips foreign items (no false 400).
        let d_healthy_a = gw
            .client_message(auth.clone(), mk_foreign_turn())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(d_healthy_a.candidates[0].upstream_id, upstream_a);
        let f_healthy_a: Value = serde_json::from_slice(&d_healthy_a.frame).unwrap();
        assert_eq!(f_healthy_a["previous_response_id"], "resp_pw_foreign");
        assert_eq!(f_healthy_a["input"].as_array().unwrap().len(), 2);
        gw.finish_turn(d_healthy_a.turn_id, None);

        // Case 1b: ALL of A, B, C exhausted -> stays on prewarm_owner A, no false 400.
        quota_cache.set_state(upstream_a, 1.0, "rejected");
        let d_all_exh = gw
            .client_message(auth.clone(), mk_foreign_turn())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(d_all_exh.candidates[0].upstream_id, upstream_a);
        let f_all_exh: Value = serde_json::from_slice(&d_all_exh.frame).unwrap();
        assert_eq!(f_all_exh["previous_response_id"], "resp_pw_foreign");
        assert_eq!(f_all_exh["input"].as_array().unwrap().len(), 2);
        gw.finish_turn(d_all_exh.turn_id, None);

        // Case 1c: A & B exhausted, C healthy -> healthy distinct replacement C triggers 400 previous_response_not_found.
        quota_cache.set_state(upstream_c, 0.1, "allowed");
        let err_c = gw
            .client_message(auth.clone(), mk_foreign_turn())
            .await
            .expect_err("healthy distinct replacement C must trigger previous_response_not_found");
        assert_eq!(err_c.status, 400);
        assert_eq!(err_c.code, "previous_response_not_found");
        gw.close();

        // Part 2: WS FailClosed + unbound ciphertext with A & B exhausted, C healthy stays on B
        // and preserves unbound item instead of stripping everything onto C.
        quota_cache.set_state(upstream_a, 1.0, "rejected");
        quota_cache.set_state(upstream_b, 1.0, "rejected");
        quota_cache.set_state(upstream_c, 0.1, "allowed");
        let fc_view = DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(DummySignerFactory))
            .principal_view(principal_view.clone())
            .subscription_quota_cache(quota_cache.clone())
            .subscription_quota_routing_max_staleness_secs(60)
            .upstream_records(mk_recs(true, true, true))
            .build();
        let fc_lifecycle = Arc::new(
            Lifecycle::new_with_dynamic_view(
                authn.clone(),
                Arc::new(DynamicViewHolder::new(fc_view)),
                Arc::new(UnusedDispatcher),
                crate::LifecycleConfig {
                    unknown_openai_reasoning: UnknownOpenaiReasoningPolicy::FailClosed,
                    ..Default::default()
                },
                clock.clone(),
            )
            .with_upstream_affinity_store(store.clone()),
        );
        let mut gw_fc =
            ResponsesWebSocketGateway::new(fc_lifecycle, &auth, headers.clone(), 1024 * 1024, 16)
                .with_codex_backend(true);
        let d_fc = gw_fc
            .client_message(
                auth.clone(),
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "store": false,
                        "input": [
                            {"type": "reasoning", "id": "rs_a", "summary": [], "encrypted_content": cipher_a},
                            {"type": "reasoning", "id": "rs_b", "summary": [], "encrypted_content": cipher_b},
                            {"type": "reasoning", "id": "rs_unbound", "summary": [], "encrypted_content": cipher_unbound},
                            {"role": "user", "content": "fc"}
                        ]
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(d_fc.candidates[0].upstream_id, upstream_b);
        let f_fc: Value = serde_json::from_slice(&d_fc.frame).unwrap();
        assert_eq!(f_fc["input"].as_array().unwrap().len(), 3);
        assert_eq!(f_fc["input"][0]["encrypted_content"], cipher_b);
        assert_eq!(f_fc["input"][1]["encrypted_content"], cipher_unbound);
        gw_fc.close();
    }

    #[tokio::test]
    async fn ws_unpinned_live_429_respects_account_refs_explicit_store_and_pool_capacity() {
        use cc_lb_control::api_keys::builtin_authn::BuiltinAuthn;
        use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
        use cc_lb_control::api_keys::principal_view::{
            DialectCache, PrincipalView, RouterPipelineCache,
        };
        use cc_lb_control::dynamic_view::{DynamicViewBuilder, DynamicViewHolder};
        use cc_lb_domain::TerminalStrategy;
        use cc_lb_routing::FilterPlugin;
        use cc_lb_storage_api::upstream::{UpstreamKind as StorageRecordKind, UpstreamRecord};
        use cc_lb_upstream::{
            ApiKeyAwareSignerFactory, RetryDecision, ShapedRequest, SignedRequest, Signer,
            SignerError, SignerFactory, SigningCapability, UpstreamError,
        };

        struct DummySignerFactory;
        impl ApiKeyAwareSignerFactory for DummySignerFactory {
            fn with_router_choice(&self, _name: String) -> Arc<dyn SignerFactory> {
                Arc::new(Self)
            }
        }
        #[async_trait::async_trait]
        impl SignerFactory for DummySignerFactory {
            async fn build(
                &self,
                _upstream: &cc_lb_domain::Upstream,
            ) -> Result<Arc<dyn Signer>, SignerError> {
                Ok(Arc::new(DummySigner))
            }
        }
        struct DummySigner;
        #[async_trait::async_trait]
        impl Signer for DummySigner {
            async fn sign(
                &self,
                shaped: ShapedRequest,
                capability: &mut SigningCapability,
            ) -> Result<SignedRequest, SignerError> {
                Ok(SignedRequest::from_shaped(shaped, capability))
            }
            async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
                RetryDecision::Fail
            }
        }
        struct UnusedDispatcher;
        #[async_trait::async_trait]
        impl crate::UpstreamDispatch for UnusedDispatcher {
            async fn dispatch(
                &self,
                _request: SignedRequest,
            ) -> Result<http::Response<crate::Body>, crate::DispatchError> {
                unreachable!("unused")
            }
        }

        let quota_cache = Arc::new(WsQuotaTestCache::default());
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(4_100_000_000));
        let storage = Arc::new(
            cc_lb_storage_sqlite::open_sqlite("sqlite::memory:", clock.clone())
                .await
                .unwrap(),
        );
        storage.initialize().await.unwrap();
        let keys = Arc::new(KeyStore::new(storage));
        let (_record, secret) = keys
            .create(
                DIRECT_PRINCIPAL,
                CreateParams {
                    label: "ws-live-failover-guards".to_owned(),
                    description: None,
                    expires_at_unix_secs: None,
                    limit_overrides: Vec::new(),
                },
            )
            .await
            .unwrap();
        let sub_filter: Arc<dyn FilterPlugin> = Arc::new(
            crate::builtin_filters::subscription_preference::SubscriptionPreferenceFilter::with_clock(
                clock.clone(),
            ),
        );
        let pipeline = Arc::new(RouterPipelineCache {
            user_filters: vec![sub_filter],
            terminal: TerminalStrategy::FirstPick,
            instantiation_error: None,
            provider_filters: None,
        });
        let principal_view = Arc::new(PrincipalView::for_tests(
            DIRECT_PRINCIPAL,
            true,
            vec!["*".to_owned()],
            Vec::new(),
            HashMap::from([(
                DIRECT_PRINCIPAL.to_owned(),
                (Some(pipeline), DialectCache::Inherit),
            )]),
        ));
        let authn = Arc::new(BuiltinAuthn::new(keys, clock.clone()));
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", secret.expose().parse().unwrap());
        let auth = crate::authenticate_first(&authn, &headers, &principal_view)
            .await
            .unwrap();

        let api_a = Uuid::parse_str("30000000-0000-0000-0000-000000000821").unwrap();
        let api_b = Uuid::parse_str("30000000-0000-0000-0000-000000000822").unwrap();
        let oauth_b = Uuid::parse_str("30000000-0000-0000-0000-000000000823").unwrap();
        let api_c = Uuid::parse_str("30000000-0000-0000-0000-000000000824").unwrap();
        quota_cache.set_state(oauth_b, 0.1, "allowed");

        let api_rec = |id: Uuid, name: &str| UpstreamRecord {
            id,
            name: name.to_owned(),
            kind: StorageRecordKind::OpenaiApiKey,
            enabled: true,
            ..Default::default()
        };
        let oauth_rec = |id: Uuid, name: &str| UpstreamRecord {
            id,
            name: name.to_owned(),
            kind: StorageRecordKind::OpenaiOauth,
            enabled: true,
            oauth_never_refresh: true,
            ..Default::default()
        };

        // 1. PRRT_kwDOSkbPEM68webY: account-owned `input_file` file_id never fails over across accounts on live 429.
        let file_view = DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(DummySignerFactory))
            .principal_view(principal_view.clone())
            .subscription_quota_cache(quota_cache.clone())
            .subscription_quota_routing_max_staleness_secs(60)
            .upstream_records(vec![api_rec(api_a, "api-a"), api_rec(api_b, "api-b")])
            .build();
        let file_lc = Arc::new(Lifecycle::new_with_dynamic_view(
            authn.clone(),
            Arc::new(DynamicViewHolder::new(file_view)),
            Arc::new(UnusedDispatcher),
            crate::LifecycleConfig::default(),
            clock.clone(),
        ));
        let mut gw_file =
            ResponsesWebSocketGateway::new(file_lc, &auth, headers.clone(), 1024 * 1024, 16);
        let d_file = gw_file
            .client_message(
                auth.clone(),
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "store": false,
                        "input": [{
                            "role": "user",
                            "content": [{"type": "input_file", "file_id": "file-abc123"}]
                        }]
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        let chosen_file_up = d_file.candidates[0].upstream_id;
        gw_file.connections.insert(1, chosen_file_up);
        gw_file.assign_connection(d_file.turn_id, 1, chosen_file_up, "api");
        gw_file.commit_send_attempt(d_file.turn_id);
        gw_file.sent(d_file.turn_id);
        let res_file = gw_file
            .provider_message(
                1,
                &Bytes::from_static(
                    br#"{"type":"error","error":{"code":"rate_limit_exceeded","message":"429"}}"#,
                ),
            )
            .await
            .unwrap();
        assert!(res_file.forward);
        assert!(res_file.dispatches.is_empty());
        gw_file.close();

        // 2a. PRRT_kwDOSkbPEM68wedN: ordinary `store: true` on API-key A with only OAuth B available must NOT downgrade to OAuth B.
        // Without SubscriptionPreferenceFilter, FirstPick keeps `[api_a, oauth_b]` in order.
        let no_sub_pipeline = Arc::new(RouterPipelineCache {
            user_filters: Vec::new(),
            terminal: TerminalStrategy::FirstPick,
            instantiation_error: None,
            provider_filters: None,
        });
        let first_pick_pv = Arc::new(PrincipalView::for_tests(
            DIRECT_PRINCIPAL,
            true,
            vec!["*".to_owned()],
            Vec::new(),
            HashMap::from([(
                DIRECT_PRINCIPAL.to_owned(),
                (Some(no_sub_pipeline), DialectCache::Inherit),
            )]),
        ));
        let store_no_api_view = DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(DummySignerFactory))
            .principal_view(first_pick_pv.clone())
            .subscription_quota_cache(quota_cache.clone())
            .subscription_quota_routing_max_staleness_secs(60)
            .upstream_records(vec![api_rec(api_a, "api-a"), oauth_rec(oauth_b, "oauth-b")])
            .build();
        let store_no_api_lc = Arc::new(Lifecycle::new_with_dynamic_view(
            authn.clone(),
            Arc::new(DynamicViewHolder::new(store_no_api_view)),
            Arc::new(UnusedDispatcher),
            crate::LifecycleConfig::default(),
            clock.clone(),
        ));
        let mut gw_store_no_api = ResponsesWebSocketGateway::new(
            store_no_api_lc,
            &auth,
            headers.clone(),
            1024 * 1024,
            16,
        );
        let d_store_a = gw_store_no_api
            .client_message(
                auth.clone(),
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "store": true,
                        "input": [{"role": "user", "content": "store true ordinary"}]
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(d_store_a.candidates[0].upstream_id, api_a);
        gw_store_no_api.connections.insert(1, api_a);
        gw_store_no_api.assign_connection(d_store_a.turn_id, 1, api_a, "api-a");
        gw_store_no_api.commit_send_attempt(d_store_a.turn_id);
        gw_store_no_api.sent(d_store_a.turn_id);
        let res_store_no_api = gw_store_no_api
            .provider_message(
                1,
                &Bytes::from_static(
                    br#"{"type":"error","error":{"code":"rate_limit_exceeded","message":"429"}}"#,
                ),
            )
            .await
            .unwrap();
        assert!(res_store_no_api.forward);
        assert!(res_store_no_api.dispatches.is_empty());
        gw_store_no_api.close();

        // 2b. With API-key A, OAuth B, API-key C: live 429 on `store: true` from A skips OAuth B and retries ONLY on API-key C.
        let store_with_c_view = DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(DummySignerFactory))
            .principal_view(first_pick_pv.clone())
            .subscription_quota_cache(quota_cache.clone())
            .subscription_quota_routing_max_staleness_secs(60)
            .upstream_records(vec![
                api_rec(api_a, "api-a"),
                oauth_rec(oauth_b, "oauth-b"),
                api_rec(api_c, "api-c"),
            ])
            .build();
        let store_with_c_lc = Arc::new(Lifecycle::new_with_dynamic_view(
            authn.clone(),
            Arc::new(DynamicViewHolder::new(store_with_c_view)),
            Arc::new(UnusedDispatcher),
            crate::LifecycleConfig::default(),
            clock.clone(),
        ));
        let mut gw_store_c = ResponsesWebSocketGateway::new(
            store_with_c_lc,
            &auth,
            headers.clone(),
            1024 * 1024,
            16,
        );
        let d_store_c = gw_store_c
            .client_message(
                auth.clone(),
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "store": true,
                        "input": [{"role": "user", "content": "store true to api-c"}]
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(d_store_c.candidates[0].upstream_id, api_a);
        gw_store_c.connections.insert(1, api_a);
        gw_store_c.assign_connection(d_store_c.turn_id, 1, api_a, "api-a");
        gw_store_c.commit_send_attempt(d_store_c.turn_id);
        gw_store_c.sent(d_store_c.turn_id);
        let res_store_c = gw_store_c
            .provider_message(
                1,
                &Bytes::from_static(
                    br#"{"type":"error","error":{"code":"rate_limit_exceeded","message":"429"}}"#,
                ),
            )
            .await
            .unwrap();
        assert!(!res_store_c.forward);
        assert_eq!(res_store_c.dispatches.len(), 1);
        assert_eq!(res_store_c.dispatches[0].candidates.len(), 1);
        assert_eq!(res_store_c.dispatches[0].candidates[0].upstream_id, api_c);
        assert!(
            res_store_c.dispatches[0]
                .candidates
                .iter()
                .all(|c| c.is_api_key)
        );
        gw_store_c.close();

        // 3. PRRT_kwDOSkbPEM68vXrv: pending_connects counts toward slot capacity, while coalescable_upstreams permits failover.
        let mk_pool_gw = || async {
            let pool_view = DynamicViewBuilder::new(0)
                .signer_factory(Arc::new(DummySignerFactory))
                .principal_view(first_pick_pv.clone())
                .subscription_quota_cache(quota_cache.clone())
                .subscription_quota_routing_max_staleness_secs(60)
                .upstream_records(vec![api_rec(api_a, "api-a"), api_rec(api_b, "api-b")])
                .build();
            let pool_lc = Arc::new(Lifecycle::new_with_dynamic_view(
                authn.clone(),
                Arc::new(DynamicViewHolder::new(pool_view)),
                Arc::new(UnusedDispatcher),
                crate::LifecycleConfig::default(),
                clock.clone(),
            ));
            let mut gw =
                ResponsesWebSocketGateway::new(pool_lc, &auth, headers.clone(), 1024 * 1024, 2);
            // Seed a completed owner on conn 1 (`api_a`) so conn 1 is NOT idle.
            let seed = gw
                .client_message(
                    auth.clone(),
                    Bytes::from(
                        serde_json::json!({
                            "type": "response.create",
                            "model": "gpt-5.4",
                            "store": false,
                            "input": [{"role": "user", "content": "busy conn 1"}]
                        })
                        .to_string(),
                    ),
                )
                .await
                .unwrap()
                .unwrap();
            gw.connections.insert(1, api_a);
            gw.assign_connection(seed.turn_id, 1, api_a, "api-a");
            gw.commit_send_attempt(seed.turn_id);
            gw.sent(seed.turn_id);
            gw.provider_message(
                1,
                &Bytes::from_static(br#"{"type":"response.created","response":{"id":"resp_busy_pool","status":"in_progress","store":false}}"#),
            )
            .await
            .unwrap();
            gw.provider_message(
                1,
                &Bytes::from_static(br#"{"type":"response.completed","response":{"id":"resp_busy_pool","status":"completed","store":false}}"#),
            )
            .await
            .unwrap();
            let t2 = gw
                .client_message(
                    auth.clone(),
                    Bytes::from(
                        serde_json::json!({
                            "type": "response.create",
                            "model": "gpt-5.4",
                            "store": false,
                            "input": [{"role": "user", "content": "failover pool check"}]
                        })
                        .to_string(),
                    ),
                )
                .await
                .unwrap()
                .unwrap();
            gw.assign_connection(t2.turn_id, 1, api_a, "api-a");
            gw.commit_send_attempt(t2.turn_id);
            gw.sent(t2.turn_id);
            gw
        };

        // 3a: 1 busy conn + 1 pending_connect (no coalescable api_b) at slot_cap = 2 -> forwards 429.
        let mut gw_pending = mk_pool_gw().await;
        let res_pending = gw_pending
            .provider_message_with_pool(
                1,
                &Bytes::from_static(
                    br#"{"type":"error","error":{"code":"rate_limit_exceeded","message":"429"}}"#,
                ),
                &WsRelayPoolSnapshot {
                    pending_connects: 1,
                    coalescable_upstreams: HashSet::new(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(res_pending.forward);
        assert!(res_pending.dispatches.is_empty());
        gw_pending.close();

        // 3b: 1 busy conn + 1 pending_connect that IS coalescing onto api_b -> suppresses 429 and dispatches to api_b.
        let mut gw_coalesce = mk_pool_gw().await;
        let res_coalesce = gw_coalesce
            .provider_message_with_pool(
                1,
                &Bytes::from_static(
                    br#"{"type":"error","error":{"code":"rate_limit_exceeded","message":"429"}}"#,
                ),
                &WsRelayPoolSnapshot {
                    pending_connects: 1,
                    coalescable_upstreams: HashSet::from([api_b]),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(!res_coalesce.forward);
        assert_eq!(res_coalesce.dispatches.len(), 1);
        assert_eq!(res_coalesce.dispatches[0].candidates[0].upstream_id, api_b);
        gw_coalesce.close();
    }

    #[tokio::test]
    async fn ws_public_prewarm_multi_owner_keeps_conflict_error() {
        use cc_lb_control::api_keys::builtin_authn::BuiltinAuthn;
        use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
        use cc_lb_control::api_keys::principal_view::{
            DialectCache, PrincipalView, RouterPipelineCache,
        };
        use cc_lb_control::dynamic_view::{DynamicViewBuilder, DynamicViewHolder};
        use cc_lb_domain::TerminalStrategy;
        use cc_lb_routing::FilterPlugin;
        use cc_lb_storage_api::upstream::{UpstreamKind as StorageRecordKind, UpstreamRecord};
        use cc_lb_upstream::{
            ApiKeyAwareSignerFactory, RetryDecision, ShapedRequest, SignedRequest, Signer,
            SignerError, SignerFactory, SigningCapability, UpstreamError,
        };

        struct DummySignerFactory;
        impl ApiKeyAwareSignerFactory for DummySignerFactory {
            fn with_router_choice(&self, _name: String) -> Arc<dyn SignerFactory> {
                Arc::new(Self)
            }
        }
        #[async_trait::async_trait]
        impl SignerFactory for DummySignerFactory {
            async fn build(
                &self,
                _upstream: &cc_lb_domain::Upstream,
            ) -> Result<Arc<dyn Signer>, SignerError> {
                Ok(Arc::new(DummySigner))
            }
        }
        struct DummySigner;
        #[async_trait::async_trait]
        impl Signer for DummySigner {
            async fn sign(
                &self,
                shaped: ShapedRequest,
                capability: &mut SigningCapability,
            ) -> Result<SignedRequest, SignerError> {
                Ok(SignedRequest::from_shaped(shaped, capability))
            }
            async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
                RetryDecision::Fail
            }
        }
        struct UnusedDispatcher;
        #[async_trait::async_trait]
        impl crate::UpstreamDispatch for UnusedDispatcher {
            async fn dispatch(
                &self,
                _request: SignedRequest,
            ) -> Result<http::Response<crate::Body>, crate::DispatchError> {
                unreachable!("unused")
            }
        }

        let quota_cache = Arc::new(WsQuotaTestCache::default());
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(4_100_000_000));
        let storage = Arc::new(
            cc_lb_storage_sqlite::open_sqlite("sqlite::memory:", clock.clone())
                .await
                .unwrap(),
        );
        storage.initialize().await.unwrap();
        let keys = Arc::new(KeyStore::new(storage));
        let (_record, secret) = keys
            .create(
                DIRECT_PRINCIPAL,
                CreateParams {
                    label: "ws-public-prewarm-multi-owner".to_owned(),
                    description: None,
                    expires_at_unix_secs: None,
                    limit_overrides: Vec::new(),
                },
            )
            .await
            .unwrap();
        let api_a = Uuid::parse_str("30000000-0000-0000-0000-000000000901").unwrap();
        let api_b = Uuid::parse_str("30000000-0000-0000-0000-000000000902").unwrap();
        let sub_filter: Arc<dyn FilterPlugin> = Arc::new(
            crate::builtin_filters::subscription_preference::SubscriptionPreferenceFilter::with_clock(
                clock.clone(),
            ),
        );
        let pipeline = Arc::new(RouterPipelineCache {
            user_filters: vec![sub_filter],
            terminal: TerminalStrategy::FirstPick,
            instantiation_error: None,
            provider_filters: None,
        });
        let principal_view = Arc::new(PrincipalView::for_tests(
            DIRECT_PRINCIPAL,
            true,
            vec!["*".to_owned()],
            Vec::new(),
            HashMap::from([(
                DIRECT_PRINCIPAL.to_owned(),
                (Some(pipeline), DialectCache::Inherit),
            )]),
        ));
        let authn = Arc::new(BuiltinAuthn::new(keys, clock.clone()));
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", secret.expose().parse().unwrap());
        let auth = crate::authenticate_first(&authn, &headers, &principal_view)
            .await
            .unwrap();

        let store = Arc::new(PrewarmAffinityStore::default());
        let cipher_a = "gAAAAAB_pub_pw_a";
        let cipher_b = "gAAAAAB_pub_pw_b";
        for (cipher, owner) in [(cipher_a, api_a), (cipher_b, api_b)] {
            let key = UpstreamAffinityKey {
                principal_id: auth.principal_id().to_owned(),
                provider: "openai".to_owned(),
                kind: cc_lb_storage_api::UpstreamAffinityKind::OpenaiResponseId,
                value_sha256: <sha2::Sha256 as sha2::Digest>::digest(cipher.as_bytes()).into(),
            };
            store.bindings.lock().unwrap().insert(
                key.clone(),
                cc_lb_storage_api::upstream_affinity::UpstreamAffinityBinding {
                    key,
                    upstream_id: owner,
                    owner_epoch: 0,
                    observed_at_unix_secs: 4_100_000_000,
                    expires_at_unix_secs: None,
                },
            );
        }
        quota_cache.set_state(api_a, 0.1, "allowed");
        quota_cache.set_state(api_b, 0.1, "allowed");
        let view = DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(DummySignerFactory))
            .principal_view(principal_view)
            .subscription_quota_cache(quota_cache)
            .subscription_quota_routing_max_staleness_secs(60)
            .upstream_records(vec![
                UpstreamRecord {
                    id: api_a,
                    name: "api-a".to_owned(),
                    kind: StorageRecordKind::OpenaiApiKey,
                    enabled: true,
                    ..Default::default()
                },
                UpstreamRecord {
                    id: api_b,
                    name: "api-b".to_owned(),
                    kind: StorageRecordKind::OpenaiApiKey,
                    enabled: true,
                    ..Default::default()
                },
            ])
            .build();
        let lifecycle = Arc::new(
            Lifecycle::new_with_dynamic_view(
                authn,
                Arc::new(DynamicViewHolder::new(view)),
                Arc::new(UnusedDispatcher),
                crate::LifecycleConfig::default(),
                clock,
            )
            .with_upstream_affinity_store(store),
        );

        let mut gw = ResponsesWebSocketGateway::new(lifecycle, &auth, headers, 1024 * 1024, 16);
        let prewarm = gw
            .client_message(
                auth.clone(),
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "generate": false,
                        "store": false,
                        "input": []
                    })
                    .to_string(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(prewarm.candidates[0].upstream_id, api_a);
        gw.assign_connection(prewarm.turn_id, 1, api_a, "api-a");
        gw.commit_send_attempt(prewarm.turn_id);
        gw.sent(prewarm.turn_id);
        gw.provider_message(
            1,
            &Bytes::from_static(
                br#"{"type":"response.created","response":{"id":"resp_pub_pw","status":"in_progress","store":false}}"#,
            ),
        )
        .await
        .unwrap();
        gw.provider_message(
            1,
            &Bytes::from_static(
                br#"{"type":"response.completed","response":{"id":"resp_pub_pw","status":"completed","store":false}}"#,
            ),
        )
        .await
        .unwrap();

        let err = gw
            .client_message(
                auth,
                Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "previous_response_id": "resp_pub_pw",
                        "store": false,
                        "input": [
                            {"type": "reasoning", "id": "rs_a", "summary": [], "encrypted_content": cipher_a},
                            {"role": "user", "content": "t1"},
                            {"type": "reasoning", "id": "rs_b", "summary": [], "encrypted_content": cipher_b},
                            {"role": "user", "content": "t2"}
                        ]
                    })
                    .to_string(),
                ),
            )
            .await
            .expect_err("public WS PrewarmResponse multi-owner conflict must keep upstream_affinity_unavailable");
        assert_eq!(err.status, 400);
        assert_eq!(err.code, error_codes::UPSTREAM_AFFINITY_UNAVAILABLE);
        gw.close();
    }

    #[tokio::test]
    async fn ws_suppressed_failover_error_budget_drop_and_opcode() {
        struct DropTransformDialect;

        impl cc_lb_upstream::UpstreamDialect for DropTransformDialect {
            fn shape(
                &self,
                _context: &cc_lb_upstream::DialectShapeContext,
                _upstream: &cc_lb_domain::Upstream,
                _principal: &cc_lb_domain::Principal,
                _builder: &mut cc_lb_upstream::ShapedRequestBuilder,
            ) -> Result<cc_lb_upstream::ShapedRequest, cc_lb_upstream::DialectError> {
                unreachable!("not shaping request")
            }

            fn sse_event_transform_hook(
                &self,
            ) -> Option<&dyn cc_lb_upstream::SseEventTransformHook> {
                Some(self)
            }
        }

        impl cc_lb_upstream::SseEventTransformHook for DropTransformDialect {
            fn transform_sse_event(
                &self,
                _request: cc_lb_upstream::TransformSseEventRequest,
            ) -> Result<TransformSseEventResult, cc_lb_upstream::ResponseTransformError>
            {
                Ok(TransformSseEventResult::Drop)
            }
        }

        let (lifecycle, auth, headers, upstream_a, upstream_b, quota_cache, _store) =
            setup_codex_ws_quota_fixture(
                cc_lb_domain::UnavailableOpenaiReasoningOwnerPolicy::StripAndFailover,
            )
            .await;
        quota_cache.set_state(upstream_a, 0.05, "allowed");
        quota_cache.set_state(upstream_b, 0.1, "allowed");

        let req = Bytes::from_static(
            br#"{"type":"response.create","model":"gpt-5.4","store":false,"input":[{"role":"user","content":"budget check"}]}"#,
        );
        let error_frame = Bytes::from(
            serde_json::json!({
                "type": "error",
                "error": {
                    "type": "rate_limit_error",
                    "code": "rate_limit_exceeded",
                    "message": format!("quota exhausted {}", "X".repeat(256)),
                }
            })
            .to_string(),
        );
        let pool = WsRelayPoolSnapshot::default();

        // Part A: Suppressed failover error charges `used_bytes` + `turn.charged_bytes`
        // and releases on both `mark_failover_replacement_dialed` and `commit_send_attempt`,
        // while tightening `byte_cap` below `used_bytes + suppressed_bytes` declines failover
        // and forwards the original error frame.
        let mut gw = ResponsesWebSocketGateway::new(
            lifecycle.clone(),
            &auth,
            headers.clone(),
            1024 * 1024,
            16,
        )
        .with_codex_backend(true);
        let d1 = gw
            .client_message(auth.clone(), req.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(d1.candidates[0].upstream_id, upstream_a);
        let used_before = gw.used_bytes;
        let charge_before = gw.turns[&d1.turn_id].charged_bytes;
        gw.assign_connection(d1.turn_id, 1, upstream_a, "oauth-a");
        gw.commit_send_attempt(d1.turn_id);
        gw.sent(d1.turn_id);

        let res1 = gw
            .provider_message_with_pool_and_opcode(1, &error_frame, false, &pool)
            .await
            .unwrap();
        assert!(!res1.forward);
        assert_eq!(res1.dispatches.len(), 1);
        let suppressed_charge = gw.used_bytes.checked_sub(used_before).expect("charged");
        assert!(suppressed_charge > error_frame.len());
        assert_eq!(
            gw.turns[&d1.turn_id].charged_bytes,
            charge_before + suppressed_charge
        );
        assert_eq!(
            gw.turns[&d1.turn_id]
                .suppressed_failover_error
                .as_ref()
                .and_then(|s| s.error.raw_provider_frames()),
            Some(
                [WebSocketWireFrame {
                    bytes: error_frame.clone(),
                    text: false,
                }]
                .as_slice()
            )
        );

        gw.mark_failover_replacement_dialed(d1.turn_id);
        assert!(gw.turns[&d1.turn_id].suppressed_failover_error.is_none());
        assert_eq!(gw.used_bytes, used_before);
        assert_eq!(gw.turns[&d1.turn_id].charged_bytes, charge_before);
        gw.finish_turn(d1.turn_id, None);
        gw.prune_empty_lanes();
        assert_eq!(gw.used_bytes, 0);

        // Verify `commit_send_attempt` releases suppressed bytes immediately when reusing a socket
        // without calling `mark_failover_replacement_dialed`.
        let (
            lifecycle_commit,
            auth_commit,
            headers_commit,
            commit_a,
            commit_b,
            commit_quota,
            _commit_store,
        ) = setup_codex_ws_quota_fixture(
            cc_lb_domain::UnavailableOpenaiReasoningOwnerPolicy::StripAndFailover,
        )
        .await;
        commit_quota.set_state(commit_a, 0.05, "allowed");
        commit_quota.set_state(commit_b, 0.1, "allowed");
        let mut gw_commit = ResponsesWebSocketGateway::new(
            lifecycle_commit,
            &auth_commit,
            headers_commit,
            1024 * 1024,
            16,
        )
        .with_codex_backend(true);
        let d_commit = gw_commit
            .client_message(auth_commit.clone(), req.clone())
            .await
            .unwrap()
            .unwrap();
        let commit_used_before = gw_commit.used_bytes;
        let commit_charge_before = gw_commit.turns[&d_commit.turn_id].charged_bytes;
        gw_commit.assign_connection(d_commit.turn_id, 1, commit_a, "oauth-a");
        gw_commit.commit_send_attempt(d_commit.turn_id);
        gw_commit.sent(d_commit.turn_id);

        let commit_res = gw_commit
            .provider_message_with_pool_and_opcode(1, &error_frame, true, &pool)
            .await
            .unwrap();
        assert!(!commit_res.forward);
        assert_eq!(commit_res.dispatches.len(), 1);
        assert_eq!(gw_commit.used_bytes, commit_used_before + suppressed_charge);
        assert_eq!(
            gw_commit.turns[&d_commit.turn_id].charged_bytes,
            commit_charge_before + suppressed_charge
        );
        assert!(
            gw_commit.turns[&d_commit.turn_id]
                .suppressed_failover_error
                .is_some()
        );

        gw_commit.assign_connection(d_commit.turn_id, 2, commit_b, "oauth-b");
        gw_commit.commit_send_attempt(d_commit.turn_id);
        assert_eq!(gw_commit.used_bytes, commit_used_before);
        assert_eq!(
            gw_commit.turns[&d_commit.turn_id].charged_bytes,
            commit_charge_before
        );
        assert!(
            gw_commit.turns[&d_commit.turn_id]
                .suppressed_failover_error
                .is_none()
        );
        gw_commit.finish_turn(d_commit.turn_id, None);
        gw_commit.prune_empty_lanes();
        assert_eq!(gw_commit.used_bytes, 0);
        gw_commit.close();

        // With `byte_cap` 1 byte below `used_before + suppressed_charge`, failover is rejected
        // and the original 429 error frame is forwarded directly instead of suppressed.
        let (lifecycle_cap, auth_cap, headers_cap, cap_a, cap_b, cap_quota, _cap_store) =
            setup_codex_ws_quota_fixture(
                cc_lb_domain::UnavailableOpenaiReasoningOwnerPolicy::StripAndFailover,
            )
            .await;
        cap_quota.set_state(cap_a, 0.05, "allowed");
        cap_quota.set_state(cap_b, 0.1, "allowed");
        let mut gw_cap =
            ResponsesWebSocketGateway::new(lifecycle_cap, &auth_cap, headers_cap, 1024 * 1024, 16)
                .with_codex_backend(true);
        let d_cap = gw_cap
            .client_message(auth_cap.clone(), req.clone())
            .await
            .unwrap()
            .unwrap();
        gw_cap.assign_connection(d_cap.turn_id, 1, cap_a, "oauth-a");
        gw_cap.commit_send_attempt(d_cap.turn_id);
        gw_cap.sent(d_cap.turn_id);
        gw_cap.byte_cap = gw_cap.used_bytes + suppressed_charge - 1;
        let cap_res = gw_cap
            .provider_message_with_pool_and_opcode(1, &error_frame, true, &pool)
            .await
            .unwrap();
        assert!(cap_res.forward);
        assert!(cap_res.dispatches.is_empty());
        assert!(gw_cap.turns.is_empty());
        assert_eq!(gw_cap.used_bytes, 0);
        gw_cap.close();

        // Part B: `TransformSseEventResult::Drop` preserves empty `raw_provider_frames`
        // across uncommitted failover restoration, releases `plugin_request` bytes on
        // failover, and settles `used_bytes` to zero when restoration fails.
        let (lifecycle_drop, auth_drop, headers_drop, drop_a, drop_b, drop_quota, _drop_store) =
            setup_codex_ws_quota_fixture(
                cc_lb_domain::UnavailableOpenaiReasoningOwnerPolicy::StripAndFailover,
            )
            .await;
        drop_quota.set_state(drop_a, 0.05, "allowed");
        drop_quota.set_state(drop_b, 0.1, "allowed");
        let mut gw_drop = ResponsesWebSocketGateway::new(
            lifecycle_drop,
            &auth_drop,
            headers_drop,
            1024 * 1024,
            16,
        )
        .with_codex_backend(true);
        let d_drop = gw_drop
            .client_message(auth_drop.clone(), req)
            .await
            .unwrap()
            .unwrap();
        gw_drop.assign_connection(d_drop.turn_id, 1, drop_a, "oauth-a");
        let plugin_req_bytes = Bytes::from_static(b"plugin-req");
        let extra_plugin_charge = plugin_req_bytes.len();
        if let Some(turn) = gw_drop.turns.get_mut(&d_drop.turn_id) {
            gw_drop.used_bytes = gw_drop.used_bytes.saturating_add(extra_plugin_charge);
            turn.charged_bytes = turn.charged_bytes.saturating_add(extra_plugin_charge);
            turn.plugin_request = Some((
                drop_a,
                WebSocketPluginRequest {
                    frame: plugin_req_bytes,
                    headers: HeaderMap::new(),
                    context: ResponseTransformContext {
                        principal: cc_lb_domain::Principal {
                            id: auth_drop.principal_id().to_owned(),
                            kind: cc_lb_domain::PrincipalKind::InternalKey,
                        },
                        upstream: cc_lb_domain::Upstream::OpenaiDirect { base_url: None },
                        request_method: http::Method::POST,
                        request_path: "/v1/responses".to_owned(),
                        dialect: Arc::new(DropTransformDialect),
                        gemini_oauth: false,
                        vertex: false,
                        codex_backend: true,
                        vertex_location: None,
                    },
                    oauth_compat: false,
                    echo_metadata: None,
                    sent_service_tier: None,
                },
            ));
        }
        gw_drop.commit_send_attempt(d_drop.turn_id);
        gw_drop.sent(d_drop.turn_id);

        let drop_res = gw_drop
            .provider_message_with_pool_and_opcode(1, &error_frame, false, &pool)
            .await
            .unwrap();
        assert!(!drop_res.forward);
        assert!(drop_res.frames.is_none());
        assert_eq!(drop_res.dispatches.len(), 1);
        assert_eq!(
            gw_drop.turns[&d_drop.turn_id]
                .suppressed_failover_error
                .as_ref()
                .and_then(|s| s.error.raw_provider_frames()),
            Some(&[][..])
        );
        assert!(gw_drop.turns[&d_drop.turn_id].plugin_request.is_none());

        let (ready, restored) = gw_drop
            .fail_dispatch_with_recovery_and_pool(
                d_drop.turn_id,
                Some(drop_b),
                &WebSocketError::new(502, "upstream_unavailable", "breaker open"),
                &pool,
            )
            .await;
        assert!(ready.is_empty());
        let restored = restored.expect("restored dropped 429 error");
        assert_eq!(restored.raw_provider_frames(), Some(&[][..]));
        assert!(gw_drop.turns.is_empty());
        assert_eq!(gw_drop.used_bytes, 0);
        gw_drop.close();
        gw.close();
    }

    #[tokio::test]
    async fn oauth_ws_resolves_sent_service_tier_while_api_key_keeps_upstream_tier() {
        use cc_lb_control::RequestEventBus;

        let fixture = DirectAccountingFixture::new().await;
        let bus = Arc::new(cc_lb_control::InMemoryBus::new());
        let mut events = bus.subscribe_lifecycle();
        let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);

        for (label, is_oauth, req_tier, upstream_tier, expected_tier) in [
            (
                "oauth-priority",
                true,
                Some("priority"),
                "default",
                "priority",
            ),
            (
                "oauth-ultrafast",
                true,
                Some("ultrafast"),
                "default",
                "ultrafast",
            ),
            ("oauth-default", true, Some("default"), "default", "default"),
            (
                "oauth-coerced-flex",
                true,
                Some("flex"),
                "default",
                "default",
            ),
            (
                "apikey-upstream-wins",
                false,
                Some("priority"),
                "default",
                "default",
            ),
        ] {
            let obs = LifecycleContext::new(label.to_owned(), bus.clone(), &clock);
            obs.mark_authn_reached();
            let event_id = obs.event_id().to_owned();

            let mut gw = ResponsesWebSocketGateway::new(
                fixture.lifecycle.clone(),
                &fixture.auth,
                HeaderMap::new(),
                65_536,
                8,
            );
            let upstream_id = Uuid::new_v4();
            gw.connection_oauth.insert(1, is_oauth);
            gw.connections.insert(1, upstream_id);
            gw.connection_lanes.insert(1, None);
            let mut turn =
                make_synthetic_turn(&fixture, Some(obs), None, true, is_oauth, Some(false), None);
            turn.upstream_id = Some(upstream_id);
            let sent_for_turn = if is_oauth && req_tier == Some("flex") {
                Some("default".to_owned())
            } else {
                req_tier.map(str::to_owned)
            };
            turn.body = Arc::new(match req_tier {
                Some(tier) => {
                    serde_json::json!({ "type": "response.create", "model": "gpt-test", "service_tier": tier })
                }
                None => serde_json::json!({ "type": "response.create", "model": "gpt-test" }),
            });
            turn.sent_service_tier = sent_for_turn;
            turn.candidate_oauth.insert(upstream_id, is_oauth);
            gw.turns.insert(1, turn);
            gw.lanes.insert(
                None,
                Lane {
                    active: Some(1),
                    ..Default::default()
                },
            );

            let created = Bytes::from(
                serde_json::json!({
                    "type": "response.created",
                    "response": { "id": format!("resp_{label}"), "object": "response", "status": "in_progress", "store": false, "service_tier": "auto" }
                })
                .to_string(),
            );
            let completed = Bytes::from(
                serde_json::json!({
                    "type": "response.completed",
                    "response": {
                        "id": format!("resp_{label}"),
                        "object": "response",
                        "status": "completed",
                        "store": false,
                        "service_tier": upstream_tier,
                        "usage": { "input_tokens": 10, "output_tokens": 5, "total_tokens": 15 }
                    }
                })
                .to_string(),
            );
            let pool = WsRelayPoolSnapshot::default();
            let r1 = gw
                .provider_message_with_pool_and_opcode(1, &created, false, &pool)
                .await
                .unwrap();
            assert!(r1.forward);
            let r2 = gw
                .provider_message_with_pool_and_opcode(1, &completed, false, &pool)
                .await
                .unwrap();
            assert!(r2.forward);
            // Forwarded payload to the client remains untouched (`frames: None` retains raw frame).
            assert!(r2.frames.is_none(), "case={label}");
            gw.close();

            let mut observed_tier = None;
            while let Ok(ev) = events.try_recv() {
                if let LifecycleEvent::UsageObserved {
                    event_id: ev_id,
                    usage,
                    ..
                } = ev
                    && ev_id == event_id
                {
                    observed_tier = usage.service_tier;
                }
            }
            assert_eq!(
                observed_tier.as_deref(),
                Some(expected_tier),
                "case={label}"
            );
        }
    }

    #[tokio::test]
    async fn oauth_ws_retains_shaped_tier_omission_across_parked_pending_error_settlement() {
        use cc_lb_control::api_keys::builtin_authn::BuiltinAuthn;
        use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
        use cc_lb_control::api_keys::principal_view::{
            DialectCache, PrincipalView, ShapePluginCache,
        };
        use cc_lb_control::dynamic_view::{DynamicViewBuilder, DynamicViewHolder};
        use cc_lb_control::event_bus::{InMemoryBus, RequestEventBus};

        struct StripServiceTierDialect;
        impl cc_lb_upstream::UpstreamDialect for StripServiceTierDialect {
            fn shape(
                &self,
                context: &cc_lb_upstream::DialectShapeContext,
                _upstream: &cc_lb_domain::Upstream,
                _principal: &cc_lb_domain::Principal,
                builder: &mut cc_lb_upstream::ShapedRequestBuilder,
            ) -> Result<cc_lb_upstream::ShapedRequest, cc_lb_upstream::DialectError> {
                let mut body: Value = serde_json::from_slice(&context.body_bytes).map_err(|e| {
                    cc_lb_upstream::DialectError::UnsupportedRequest {
                        reason: e.to_string(),
                    }
                })?;
                if let Some(obj) = body.as_object_mut() {
                    obj.remove("service_tier");
                }
                let stripped = Bytes::from(serde_json::to_vec(&body).map_err(|e| {
                    cc_lb_upstream::DialectError::UnsupportedRequest {
                        reason: e.to_string(),
                    }
                })?);
                let url = url::Url::parse("https://chatgpt.com/backend-api/codex/responses")
                    .expect("valid test URL");
                Ok(builder.shaped_request(
                    url,
                    context.method.clone(),
                    context.downstream_headers.clone(),
                    stripped,
                ))
            }
        }
        struct UnusedWsDispatcher;
        #[async_trait::async_trait]
        impl crate::UpstreamDispatch for UnusedWsDispatcher {
            async fn dispatch(
                &self,
                _request: cc_lb_upstream::SignedRequest,
            ) -> Result<http::Response<crate::Body>, crate::DispatchError> {
                unreachable!("websocket test does not dispatch HTTP")
            }
        }

        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(1_800_000_000));
        let storage = Arc::new(
            cc_lb_storage_sqlite::open_sqlite("sqlite::memory:", clock.clone())
                .await
                .expect("open in-memory db"),
        );
        storage.initialize().await.expect("migrate db");
        let keys = Arc::new(KeyStore::new(storage.clone()));
        let (_rec, secret) = keys
            .create(
                DIRECT_PRINCIPAL,
                CreateParams {
                    label: "ws-shaped-omission".to_owned(),
                    description: None,
                    expires_at_unix_secs: None,
                    limit_overrides: Vec::new(),
                },
            )
            .await
            .expect("create test key");
        let upstream_id = Uuid::new_v4();
        let record = cc_lb_storage_api::upstream::UpstreamRecord {
            id: upstream_id,
            name: "oauth-shaped-ws".to_owned(),
            kind: cc_lb_storage_api::upstream::UpstreamKind::OpenaiOauth,
            enabled: true,
            base_url: Some(
                url::Url::parse("https://chatgpt.com/backend-api/codex").expect("valid test URL"),
            ),
            ..Default::default()
        };

        let mut chains = HashMap::new();
        chains.insert(
            DIRECT_PRINCIPAL.to_owned(),
            (
                None,
                DialectCache::Explicit(ShapePluginCache {
                    dialect: Arc::new(StripServiceTierDialect),
                }),
            ),
        );
        let pview = Arc::new(PrincipalView::for_tests(
            DIRECT_PRINCIPAL,
            true,
            vec!["*".to_owned()],
            Vec::new(),
            chains,
        ));
        let authn = Arc::new(BuiltinAuthn::new(keys, clock.clone()));
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-api-key",
            secret.expose().parse().expect("valid auth header"),
        );
        let auth = crate::authenticate_first(&authn, &headers, &pview)
            .await
            .expect("authenticate");
        let bus = Arc::new(InMemoryBus::new());
        let mut events = bus.subscribe_lifecycle();
        let dynamic = DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(UnusedSignerFactory))
            .principal_view(pview)
            .upstream_records(vec![record])
            .build();
        let lifecycle_bus: Arc<dyn RequestEventBus> = bus.clone();
        let lifecycle = Arc::new(
            Lifecycle::new_with_dynamic_view(
                authn,
                Arc::new(DynamicViewHolder::new(dynamic)),
                Arc::new(UnusedWsDispatcher),
                crate::LifecycleConfig::default(),
                clock.clone(),
            )
            .with_event_bus(lifecycle_bus.clone()),
        );

        let obs = LifecycleContext::new(
            "ws-shaped-omission-evt".to_owned(),
            lifecycle_bus,
            &(clock as cc_lb_clock::ClockHandle),
        );
        obs.mark_authn_reached();
        let event_id = obs.event_id().to_owned();

        let mut gw =
            ResponsesWebSocketGateway::new(lifecycle.clone(), &auth, HeaderMap::new(), 65_536, 8);
        gw.connection_oauth.insert(1, true);
        gw.connections.insert(1, upstream_id);
        gw.connection_lanes.insert(1, None);
        let mut turn = Turn {
            body: Arc::new(serde_json::json!({
                "type": "response.create",
                "model": "gpt-test",
                "service_tier": "priority"
            })),
            frame: Bytes::from_static(
                br#"{"type":"response.create","model":"gpt-test","service_tier":"priority"}"#,
            ),
            upstream_id: Some(upstream_id),
            connection_id: Some(1),
            sent: true,
            attempted: true,
            attempt_num: 1,
            sent_service_tier: Some("priority".to_owned()),
            ..make_synthetic_turn(
                &DirectAccountingFixture::new().await,
                Some(obs),
                None,
                true,
                true,
                Some(false),
                None,
            )
        };
        turn.candidate_oauth.insert(upstream_id, true);
        gw.turns.insert(1, turn);
        gw.lanes.insert(
            None,
            Lane {
                active: Some(1),
                ..Default::default()
            },
        );

        gw.prepare_plugin_request(1, upstream_id)
            .expect("shape strips service_tier");
        assert_eq!(gw.turns[&1].sent_service_tier(), None);

        let pool = WsRelayPoolSnapshot::default();
        let created = Bytes::from_static(
            br#"{"type":"response.created","response":{"id":"resp_parked_omit","object":"response","status":"in_progress","store":false,"service_tier":"auto"}}"#,
        );
        gw.provider_message_with_pool_and_opcode(1, &created, false, &pool)
            .await
            .unwrap();
        let err_frame = Bytes::from_static(
            br#"{"type":"error","error":{"type":"server_error","code":"upstream_error","message":"boom"}}"#,
        );
        gw.provider_message_with_pool_and_opcode(1, &err_frame, false, &pool)
            .await
            .unwrap();
        assert!(gw.pending_error_terminals.contains_key("resp_parked_omit"));
        assert_eq!(
            gw.pending_error_terminals["resp_parked_omit"]
                .turn
                .sent_service_tier(),
            None,
            "park_pending_error_terminal clearing request.frame must keep shaped None omission"
        );

        let delayed_failed = Bytes::from_static(
            br#"{"type":"response.failed","response":{"id":"resp_parked_omit","object":"response","status":"failed","store":false,"service_tier":"default","error":{"code":"upstream_error","message":"boom"},"usage":{"input_tokens":10,"output_tokens":5,"total_tokens":15}}}"#,
        );
        gw.provider_message_with_pool_and_opcode(1, &delayed_failed, false, &pool)
            .await
            .unwrap();
        gw.close();

        let mut observed_tier = None;
        while let Ok(ev) = events.try_recv() {
            if let LifecycleEvent::UsageObserved {
                event_id: ev_id,
                usage,
                ..
            } = ev
                && ev_id == event_id
            {
                observed_tier = usage.service_tier;
            }
        }
        assert_eq!(
            observed_tier.as_deref(),
            Some("default"),
            "delayed settlement must keep backend default instead of reverting to stripped ingress priority"
        );
    }

    #[tokio::test]
    async fn oauth_ws_steer_successor_leaves_sent_tier_unset_unless_control_specifies_one() {
        use cc_lb_control::api_keys::builtin_authn::BuiltinAuthn;
        use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
        use cc_lb_control::api_keys::principal_view::{
            DialectCache, PrincipalView, ShapePluginCache,
        };
        use cc_lb_control::dynamic_view::{DynamicViewBuilder, DynamicViewHolder};
        use cc_lb_control::event_bus::{InMemoryBus, RequestEventBus};
        use cc_lb_domain::{Principal, Upstream};
        use cc_lb_upstream::{
            DialectError, DialectShapeContext, ShapedRequest, ShapedRequestBuilder, UpstreamDialect,
        };

        struct SteerTierDialect {
            replacement: Option<&'static str>,
        }
        impl UpstreamDialect for SteerTierDialect {
            fn shape(
                &self,
                context: &DialectShapeContext,
                _upstream: &Upstream,
                _principal: &Principal,
                builder: &mut ShapedRequestBuilder,
            ) -> Result<ShapedRequest, DialectError> {
                let mut body: Value = serde_json::from_slice(&context.body_bytes).map_err(|e| {
                    DialectError::UnsupportedRequest {
                        reason: e.to_string(),
                    }
                })?;
                if let Some(obj) = body.as_object_mut() {
                    match self.replacement {
                        Some(tier) => {
                            obj.insert("service_tier".to_owned(), Value::String(tier.to_owned()));
                        }
                        None => {
                            obj.remove("service_tier");
                        }
                    }
                }
                let rewritten = Bytes::from(serde_json::to_vec(&body).map_err(|e| {
                    DialectError::UnsupportedRequest {
                        reason: e.to_string(),
                    }
                })?);
                let url = url::Url::parse("https://chatgpt.com/backend-api/codex/responses")
                    .expect("valid test URL");
                Ok(builder.shaped_request(
                    url,
                    context.method.clone(),
                    context.downstream_headers.clone(),
                    rewritten,
                ))
            }
        }
        struct UnusedWsDispatcher;
        #[async_trait::async_trait]
        impl crate::UpstreamDispatch for UnusedWsDispatcher {
            async fn dispatch(
                &self,
                _request: cc_lb_upstream::SignedRequest,
            ) -> Result<http::Response<crate::Body>, crate::DispatchError> {
                unreachable!("websocket test does not dispatch HTTP")
            }
        }

        for (case, parent_replacement, steer_tier, child_upstream_tier, expected_child_tier) in [
            (
                "no_steer_tier_child_default",
                Some("priority"),
                None,
                "default",
                "default",
            ),
            (
                "no_steer_tier_child_measured_priority",
                Some("priority"),
                None,
                "priority",
                "priority",
            ),
            (
                "explicit_steer_tier_priority_child_default",
                Some("default"),
                Some("priority"),
                "default",
                "priority",
            ),
        ] {
            let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(1_800_000_000));
            let storage = Arc::new(
                cc_lb_storage_sqlite::open_sqlite("sqlite::memory:", clock.clone())
                    .await
                    .expect("open in-memory db"),
            );
            storage.initialize().await.expect("migrate db");
            let keys = Arc::new(KeyStore::new(storage.clone()));
            let (_rec, secret) = keys
                .create(
                    DIRECT_PRINCIPAL,
                    CreateParams {
                        label: format!("ws-steer-{case}"),
                        description: None,
                        expires_at_unix_secs: None,
                        limit_overrides: Vec::new(),
                    },
                )
                .await
                .expect("create test key");
            let upstream_id = Uuid::new_v4();
            let record = cc_lb_storage_api::upstream::UpstreamRecord {
                id: upstream_id,
                name: format!("oauth-steer-{case}"),
                kind: cc_lb_storage_api::upstream::UpstreamKind::OpenaiOauth,
                enabled: true,
                base_url: Some(
                    url::Url::parse("https://chatgpt.com/backend-api/codex")
                        .expect("valid test URL"),
                ),
                ..Default::default()
            };

            let mut chains = HashMap::new();
            chains.insert(
                DIRECT_PRINCIPAL.to_owned(),
                (
                    None,
                    DialectCache::Explicit(ShapePluginCache {
                        dialect: Arc::new(SteerTierDialect {
                            replacement: parent_replacement,
                        }),
                    }),
                ),
            );
            let pview = Arc::new(PrincipalView::for_tests(
                DIRECT_PRINCIPAL,
                true,
                vec!["*".to_owned()],
                Vec::new(),
                chains,
            ));
            let authn = Arc::new(BuiltinAuthn::new(keys, clock.clone()));
            let mut headers = HeaderMap::new();
            headers.insert(
                "x-api-key",
                secret.expose().parse().expect("valid auth header"),
            );
            let auth = crate::authenticate_first(&authn, &headers, &pview)
                .await
                .expect("authenticate");
            let bus = Arc::new(InMemoryBus::new());
            let mut events = bus.subscribe_lifecycle();
            let dynamic = DynamicViewBuilder::new(0)
                .signer_factory(Arc::new(UnusedSignerFactory))
                .principal_view(pview)
                .upstream_records(vec![record])
                .build();
            let lifecycle_bus: Arc<dyn RequestEventBus> = bus.clone();
            let lifecycle = Arc::new(
                Lifecycle::new_with_dynamic_view(
                    authn,
                    Arc::new(DynamicViewHolder::new(dynamic)),
                    Arc::new(UnusedWsDispatcher),
                    crate::LifecycleConfig::default(),
                    clock.clone(),
                )
                .with_event_bus(lifecycle_bus.clone()),
            );

            let mut gw = ResponsesWebSocketGateway::new(
                lifecycle.clone(),
                &auth,
                HeaderMap::new(),
                65_536,
                8,
            );
            gw.connection_oauth.insert(1, true);
            gw.connections.insert(1, upstream_id);
            gw.connection_lanes.insert(1, None);
            let parent_obs = LifecycleContext::new(
                format!("ws-steer-parent-{case}"),
                lifecycle_bus.clone(),
                &(clock.clone() as cc_lb_clock::ClockHandle),
            );
            parent_obs.mark_authn_reached();
            let mut parent_turn = Turn {
                body: Arc::new(serde_json::json!({
                    "type": "response.create",
                    "model": "gpt-test",
                    "service_tier": parent_replacement.unwrap_or("default")
                })),
                frame: Bytes::from_static(
                    br#"{"type":"response.create","model":"gpt-test","service_tier":"priority"}"#,
                ),
                upstream_id: Some(upstream_id),
                connection_id: Some(1),
                sent: true,
                attempted: true,
                attempt_num: 1,
                sent_service_tier: parent_replacement.map(str::to_owned),
                ..make_synthetic_turn(
                    &DirectAccountingFixture::new().await,
                    Some(parent_obs),
                    None,
                    true,
                    true,
                    Some(false),
                    None,
                )
            };
            parent_turn.candidate_oauth.insert(upstream_id, true);
            gw.turns.insert(1, parent_turn);
            gw.next_turn_id = 1;
            gw.lanes.insert(
                None,
                Lane {
                    active: Some(1),
                    ..Default::default()
                },
            );

            gw.prepare_plugin_request(1, upstream_id)
                .expect("shape parent turn");
            let pool = WsRelayPoolSnapshot::default();
            let parent_id = format!("resp_steer_parent_{case}");
            let parent_created = Bytes::from(
                serde_json::json!({
                    "type": "response.created",
                    "response": { "id": parent_id, "object": "response", "status": "in_progress", "store": false, "service_tier": "auto" }
                })
                .to_string(),
            );
            gw.provider_message_with_pool_and_opcode(1, &parent_created, false, &pool)
                .await
                .unwrap();

            let mut steer_json = serde_json::json!({
                "type": "response.steer",
                "previous_response_id": parent_id,
                "input": "steer update"
            });
            if let Some(tier) = steer_tier {
                steer_json
                    .as_object_mut()
                    .expect("steer json object")
                    .insert("service_tier".to_owned(), serde_json::json!(tier));
            }
            let steer_frame = Bytes::from(steer_json.to_string());
            let steer_dispatch = gw
                .client_message(auth.clone(), steer_frame)
                .await
                .expect("admit steer")
                .expect("steer dispatch");
            let steer_event_id = gw.turns[&steer_dispatch.turn_id]
                .prepared
                .observer
                .as_ref()
                .expect("steer turn has observer")
                .event_id()
                .to_owned();
            let _ = gw.frame_for_send(&steer_dispatch).expect("steer frame");
            gw.turns.get_mut(&steer_dispatch.turn_id).unwrap().sent = true;

            let parent_incomplete = Bytes::from(
                serde_json::json!({
                    "type": "response.incomplete",
                    "response": {
                        "id": parent_id,
                        "object": "response",
                        "status": "incomplete",
                        "store": false,
                        "service_tier": "default",
                        "incomplete_details": { "reason": "steered" },
                        "usage": { "input_tokens": 5, "output_tokens": 2, "total_tokens": 7 }
                    }
                })
                .to_string(),
            );
            gw.provider_message_with_pool_and_opcode(1, &parent_incomplete, false, &pool)
                .await
                .unwrap();

            let child_id = format!("resp_steer_child_{case}");
            let child_created = Bytes::from(
                serde_json::json!({
                    "type": "response.created",
                    "response": { "id": child_id, "previous_response_id": parent_id, "object": "response", "status": "in_progress", "store": false, "service_tier": "auto" }
                })
                .to_string(),
            );
            gw.provider_message_with_pool_and_opcode(1, &child_created, false, &pool)
                .await
                .unwrap();

            let child_completed = Bytes::from(
                serde_json::json!({
                    "type": "response.completed",
                    "response": {
                        "id": child_id,
                        "object": "response",
                        "status": "completed",
                        "store": false,
                        "service_tier": child_upstream_tier,
                        "usage": { "input_tokens": 10, "output_tokens": 5, "total_tokens": 15 }
                    }
                })
                .to_string(),
            );
            gw.provider_message_with_pool_and_opcode(1, &child_completed, false, &pool)
                .await
                .unwrap();
            gw.close();

            let mut observed_tier = None;
            while let Ok(ev) = events.try_recv() {
                if let LifecycleEvent::UsageObserved {
                    event_id: ev_id,
                    usage,
                    ..
                } = ev
                    && ev_id == steer_event_id
                {
                    observed_tier = usage.service_tier;
                }
            }
            assert_eq!(
                observed_tier.as_deref(),
                Some(expected_child_tier),
                "case={case}: steering successor must not fabricate parent tier when steer control omits service_tier"
            );
        }
    }

    #[tokio::test]
    async fn oauth_ws_preserves_measured_priority_across_later_default_completed_frame() {
        use cc_lb_control::RequestEventBus;

        let fixture = DirectAccountingFixture::new().await;
        let bus = Arc::new(cc_lb_control::InMemoryBus::new());
        let mut events = bus.subscribe_lifecycle();
        let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let upstream_id = Uuid::new_v4();
        let mut gw = ResponsesWebSocketGateway::new(
            fixture.lifecycle.clone(),
            &fixture.auth,
            HeaderMap::new(),
            65_536,
            8,
        );
        gw.connection_oauth.insert(1, true);
        gw.connections.insert(1, upstream_id);
        gw.connection_lanes.insert(1, None);
        let obs = LifecycleContext::new("ws-measured-priority-evt".to_owned(), bus, &clock);
        obs.mark_authn_reached();
        let event_id = obs.event_id().to_owned();
        let mut turn = Turn {
            body: Arc::new(serde_json::json!({
                "type": "response.create",
                "model": "gpt-test",
                "service_tier": "default"
            })),
            frame: Bytes::from_static(
                br#"{"type":"response.create","model":"gpt-test","service_tier":"default"}"#,
            ),
            upstream_id: Some(upstream_id),
            connection_id: Some(1),
            sent: true,
            attempted: true,
            attempt_num: 1,
            sent_service_tier: Some("default".to_owned()),
            ..make_synthetic_turn(&fixture, Some(obs), None, true, true, Some(false), None)
        };
        turn.candidate_oauth.insert(upstream_id, true);
        gw.turns.insert(1, turn);
        gw.next_turn_id = 1;
        gw.lanes.insert(
            None,
            Lane {
                active: Some(1),
                ..Default::default()
            },
        );

        let pool = WsRelayPoolSnapshot::default();
        for payload in [
            serde_json::json!({
                "type": "response.created",
                "response": { "id": "resp_measured_prio", "object": "response", "status": "in_progress", "store": false, "service_tier": "auto" }
            }),
            serde_json::json!({
                "type": "response.in_progress",
                "response": {
                    "id": "resp_measured_prio",
                    "object": "response",
                    "status": "in_progress",
                    "store": false,
                    "service_tier": "priority",
                    "usage": { "input_tokens": 10, "output_tokens": 2, "total_tokens": 12 }
                }
            }),
            serde_json::json!({
                "type": "response.completed",
                "response": {
                    "id": "resp_measured_prio",
                    "object": "response",
                    "status": "completed",
                    "store": false,
                    "service_tier": "default",
                    "usage": { "input_tokens": 10, "output_tokens": 8, "total_tokens": 18 }
                }
            }),
        ] {
            let frame = Bytes::from(payload.to_string());
            gw.provider_message_with_pool_and_opcode(1, &frame, false, &pool)
                .await
                .unwrap();
        }
        gw.close();

        let mut observed_tier = None;
        while let Ok(ev) = events.try_recv() {
            if let LifecycleEvent::UsageObserved {
                event_id: ev_id,
                usage,
                ..
            } = ev
                && ev_id == event_id
            {
                observed_tier = usage.service_tier;
            }
        }
        assert_eq!(
            observed_tier.as_deref(),
            Some("priority"),
            "earlier measured priority tier must survive later OAuth default placeholder frame"
        );
    }

    #[tokio::test]
    async fn oauth_ws_envelope_priority_overrides_nested_default_in_same_frame() {
        use cc_lb_control::RequestEventBus;

        let fixture = DirectAccountingFixture::new().await;
        let bus = Arc::new(cc_lb_control::InMemoryBus::new());
        let mut events = bus.subscribe_lifecycle();
        let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);

        for (label, is_oauth, expected_tier) in [
            ("oauth-same-frame", true, "priority"),
            ("apikey-same-frame", false, "priority"),
        ] {
            let upstream_id = Uuid::new_v4();
            let mut gw = ResponsesWebSocketGateway::new(
                fixture.lifecycle.clone(),
                &fixture.auth,
                HeaderMap::new(),
                65_536,
                8,
            );
            gw.connection_oauth.insert(1, is_oauth);
            gw.connections.insert(1, upstream_id);
            gw.connection_lanes.insert(1, None);
            let obs = LifecycleContext::new(label.to_owned(), bus.clone(), &clock);
            obs.mark_authn_reached();
            let event_id = obs.event_id().to_owned();
            let mut turn = Turn {
                body: Arc::new(serde_json::json!({
                    "type": "response.create",
                    "model": "gpt-test"
                })),
                frame: Bytes::from_static(br#"{"type":"response.create","model":"gpt-test"}"#),
                upstream_id: Some(upstream_id),
                connection_id: Some(1),
                sent: true,
                attempted: true,
                attempt_num: 1,
                sent_service_tier: None,
                ..make_synthetic_turn(&fixture, Some(obs), None, true, is_oauth, Some(false), None)
            };
            turn.candidate_oauth.insert(upstream_id, is_oauth);
            gw.turns.insert(1, turn);
            gw.next_turn_id = 1;
            gw.lanes.insert(
                None,
                Lane {
                    active: Some(1),
                    ..Default::default()
                },
            );

            let pool = WsRelayPoolSnapshot::default();
            let resp_id = format!("resp_same_{label}");
            let created = Bytes::from(
                serde_json::json!({
                    "type": "response.created",
                    "response": { "id": resp_id, "object": "response", "status": "in_progress", "store": false }
                })
                .to_string(),
            );
            let completed = Bytes::from(
                serde_json::json!({
                    "type": "response.completed",
                    "response": {
                        "id": resp_id,
                        "object": "response",
                        "status": "completed",
                        "store": false,
                        "service_tier": "priority",
                        "usage": { "input_tokens": 10, "output_tokens": 5, "total_tokens": 15, "service_tier": "default" }
                    }
                })
                .to_string(),
            );
            gw.provider_message_with_pool_and_opcode(1, &created, false, &pool)
                .await
                .unwrap();
            gw.provider_message_with_pool_and_opcode(1, &completed, false, &pool)
                .await
                .unwrap();
            gw.close();

            let mut observed_tier = None;
            while let Ok(ev) = events.try_recv() {
                if let LifecycleEvent::UsageObserved {
                    event_id: ev_id,
                    usage,
                    ..
                } = ev
                    && ev_id == event_id
                {
                    observed_tier = usage.service_tier;
                }
            }
            assert_eq!(
                observed_tier.as_deref(),
                Some(expected_tier),
                "case={label}"
            );
        }
    }

    #[tokio::test]
    async fn oauth_ws_sent_service_tier_is_bounded_and_charged_through_parked_settlement() {
        use cc_lb_control::api_keys::builtin_authn::BuiltinAuthn;
        use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
        use cc_lb_control::api_keys::principal_view::{
            DialectCache, PrincipalView, ShapePluginCache,
        };
        use cc_lb_control::dynamic_view::{DynamicViewBuilder, DynamicViewHolder};
        use cc_lb_control::event_bus::{InMemoryBus, RequestEventBus};
        use cc_lb_domain::{Principal, Upstream};
        use cc_lb_upstream::{
            DialectError, DialectShapeContext, ShapedRequest, ShapedRequestBuilder, UpstreamDialect,
        };

        struct OversizedTierDialect {
            tier: String,
        }
        impl UpstreamDialect for OversizedTierDialect {
            fn shape(
                &self,
                context: &DialectShapeContext,
                _upstream: &Upstream,
                _principal: &Principal,
                builder: &mut ShapedRequestBuilder,
            ) -> Result<ShapedRequest, DialectError> {
                let shaped = Bytes::from(
                    serde_json::json!({
                        "type": "response.create",
                        "model": "gpt-test",
                        "service_tier": self.tier,
                    })
                    .to_string(),
                );
                let url = url::Url::parse("https://chatgpt.com/backend-api/codex/responses")
                    .expect("valid test URL");
                Ok(builder.shaped_request(
                    url,
                    context.method.clone(),
                    context.downstream_headers.clone(),
                    shaped,
                ))
            }
        }
        struct UnusedWsDispatcher;
        #[async_trait::async_trait]
        impl crate::UpstreamDispatch for UnusedWsDispatcher {
            async fn dispatch(
                &self,
                _request: cc_lb_upstream::SignedRequest,
            ) -> Result<http::Response<crate::Body>, crate::DispatchError> {
                unreachable!("websocket test does not dispatch HTTP")
            }
        }

        let long_tier = "p".repeat(64);
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(1_800_000_000));
        let storage = Arc::new(
            cc_lb_storage_sqlite::open_sqlite("sqlite::memory:", clock.clone())
                .await
                .expect("open in-memory db"),
        );
        storage.initialize().await.expect("migrate db");
        let keys = Arc::new(KeyStore::new(storage.clone()));
        let (_rec, secret) = keys
            .create(
                DIRECT_PRINCIPAL,
                CreateParams {
                    label: "ws-tier-budget".to_owned(),
                    description: None,
                    expires_at_unix_secs: None,
                    limit_overrides: Vec::new(),
                },
            )
            .await
            .expect("create test key");
        let upstream_id = Uuid::new_v4();
        let record = cc_lb_storage_api::upstream::UpstreamRecord {
            id: upstream_id,
            name: "oauth-tier-budget".to_owned(),
            kind: cc_lb_storage_api::upstream::UpstreamKind::OpenaiOauth,
            enabled: true,
            base_url: Some(
                url::Url::parse("https://chatgpt.com/backend-api/codex").expect("valid test URL"),
            ),
            ..Default::default()
        };

        let mut chains = HashMap::new();
        chains.insert(
            DIRECT_PRINCIPAL.to_owned(),
            (
                None,
                DialectCache::Explicit(ShapePluginCache {
                    dialect: Arc::new(OversizedTierDialect {
                        tier: long_tier.clone(),
                    }),
                }),
            ),
        );
        let pview = Arc::new(PrincipalView::for_tests(
            DIRECT_PRINCIPAL,
            true,
            vec!["*".to_owned()],
            Vec::new(),
            chains,
        ));
        let authn = Arc::new(BuiltinAuthn::new(keys, clock.clone()));
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-api-key",
            secret.expose().parse().expect("valid auth header"),
        );
        let auth = crate::authenticate_first(&authn, &headers, &pview)
            .await
            .expect("authenticate");
        let bus = Arc::new(InMemoryBus::new());
        let mut events = bus.subscribe_lifecycle();
        let dynamic = DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(UnusedSignerFactory))
            .principal_view(pview)
            .upstream_records(vec![record])
            .build();
        let lifecycle_bus: Arc<dyn RequestEventBus> = bus.clone();
        let lifecycle = Arc::new(
            Lifecycle::new_with_dynamic_view(
                authn,
                Arc::new(DynamicViewHolder::new(dynamic)),
                Arc::new(UnusedWsDispatcher),
                crate::LifecycleConfig::default(),
                clock.clone(),
            )
            .with_event_bus(lifecycle_bus.clone()),
        );

        let create_frame = Bytes::from_static(
            br#"{"type":"response.create","model":"gpt-test","input":"hi","store":false}"#,
        );

        // Full gateway first: measure actual shaped frame bytes + retained tier charge, then
        // verify a tight cap allowing the shaped frame alone rejects the extra tier string.
        let mut gw =
            ResponsesWebSocketGateway::new(lifecycle.clone(), &auth, HeaderMap::new(), 65_536, 8);
        gw.connection_oauth.insert(1, true);
        gw.connections.insert(1, upstream_id);
        gw.connection_lanes.insert(1, None);
        let dispatch = gw
            .client_message(auth.clone(), create_frame.clone())
            .await
            .expect("admit")
            .expect("dispatch");
        let event_id = gw.turns[&dispatch.turn_id]
            .prepared
            .observer
            .as_ref()
            .expect("turn has observer")
            .event_id()
            .to_owned();
        let pre_shape_used = gw.used_bytes;
        gw.prepare_plugin_request(dispatch.turn_id, upstream_id)
            .expect("shape with generous cap");
        let shaped_req = &gw.turns[&dispatch.turn_id]
            .plugin_request
            .as_ref()
            .expect("plugin request recorded")
            .1;
        let shaped_frame_len = shaped_req.frame.len();
        assert!(
            shaped_req.sent_service_tier.is_none(),
            "plugin_request must not retain a second uncharged tier copy"
        );
        assert_eq!(
            gw.turns[&dispatch.turn_id].sent_service_tier(),
            Some(long_tier.as_str())
        );
        assert_eq!(
            gw.used_bytes,
            pre_shape_used + shaped_frame_len + long_tier.len(),
            "shaped frame and retained tier string must both be charged"
        );

        // Tight cap allows initial frame + shaped frame bytes, but NOT the extra 64-byte tier string.
        let mut tight_gw = ResponsesWebSocketGateway::new(
            lifecycle.clone(),
            &auth,
            HeaderMap::new(),
            pre_shape_used + shaped_frame_len + 10,
            8,
        );
        let tight_dispatch = tight_gw
            .client_message(auth.clone(), create_frame)
            .await
            .expect("admit tight")
            .expect("dispatch");
        assert_eq!(tight_gw.used_bytes, pre_shape_used);
        let err = tight_gw
            .prepare_plugin_request(tight_dispatch.turn_id, upstream_id)
            .expect_err("shaped tier string must enforce session byte budget");
        assert_eq!(err.status, 503);
        assert_eq!(err.code, "overloaded_error");
        assert_eq!(
            tight_gw.used_bytes, pre_shape_used,
            "budget rejection must not alter used_bytes"
        );
        tight_gw.close();

        {
            let turn = gw.turns.get_mut(&dispatch.turn_id).unwrap();
            turn.connection_id = Some(1);
            turn.upstream_id = Some(upstream_id);
        }
        let _ = gw.frame_for_send(&dispatch).expect("send frame");
        gw.turns.get_mut(&dispatch.turn_id).unwrap().sent = true;
        gw.lanes.insert(
            None,
            Lane {
                active: Some(dispatch.turn_id),
                ..Default::default()
            },
        );

        let pool = WsRelayPoolSnapshot::default();
        let created = Bytes::from_static(
            br#"{"type":"response.created","response":{"id":"resp_parked_budget","object":"response","status":"in_progress","store":false,"service_tier":"auto"}}"#,
        );
        gw.provider_message_with_pool_and_opcode(1, &created, false, &pool)
            .await
            .unwrap();
        let pre_park_turn_charged = gw.turns[&dispatch.turn_id].charged_bytes;
        let non_turn_used = gw.used_bytes - pre_park_turn_charged;
        let error_frame = Bytes::from_static(
            br#"{"type":"error","response":{"id":"resp_parked_budget","object":"response","status":"failed","store":false},"error":{"code":"upstream_error","message":"boom"}}"#,
        );
        gw.provider_message_with_pool_and_opcode(1, &error_frame, false, &pool)
            .await
            .unwrap();
        assert!(
            gw.pending_error_terminals
                .contains_key("resp_parked_budget")
        );
        let parked_charged = gw.pending_error_terminals["resp_parked_budget"]
            .turn
            .charged_bytes;
        assert!(
            parked_charged < pre_park_turn_charged && parked_charged >= long_tier.len(),
            "retained tier string must remain charged while frame/body payload is freed on park (pre={pre_park_turn_charged}, parked={parked_charged})"
        );
        assert_eq!(
            gw.used_bytes,
            parked_charged + non_turn_used,
            "gateway used_bytes must keep tracking parked turn + tier string plus bound session state"
        );

        let delayed_failed = Bytes::from_static(
            br#"{"type":"response.failed","response":{"id":"resp_parked_budget","object":"response","status":"failed","store":false,"service_tier":"default","error":{"code":"upstream_error","message":"boom"},"usage":{"input_tokens":10,"output_tokens":5,"total_tokens":15}}}"#,
        );
        gw.provider_message_with_pool_and_opcode(1, &delayed_failed, false, &pool)
            .await
            .unwrap();
        assert!(
            !gw.pending_error_terminals
                .contains_key("resp_parked_budget")
        );
        assert_eq!(
            gw.used_bytes, non_turn_used,
            "settling parked terminal must release parked turn and charged tier bytes completely"
        );
        gw.close();
        assert_eq!(gw.used_bytes, 0);

        let mut observed_tier = None;
        while let Ok(ev) = events.try_recv() {
            if let LifecycleEvent::UsageObserved {
                event_id: ev_id,
                usage,
                ..
            } = ev
                && ev_id == event_id
            {
                observed_tier = usage.service_tier;
            }
        }
        assert_eq!(observed_tier.as_deref(), Some(long_tier.as_str()));
    }

    #[test]
    fn api_key_ws_observed_response_usage_prefers_response_level_tier_over_nested_default() {
        let response_priority_nested_default = serde_json::json!({
            "service_tier": "priority",
            "usage": {
                "input_tokens": 10,
                "output_tokens": 5,
                "service_tier": "default"
            }
        });
        let (std_usage, _) = observed_response_usage(
            &response_priority_nested_default,
            ServiceTierMergePolicy::Standard,
        )
        .unwrap();
        assert_eq!(std_usage.service_tier.as_deref(), Some("priority"));
        let (oauth_usage, _) = observed_response_usage(
            &response_priority_nested_default,
            ServiceTierMergePolicy::OpenaiOauth,
        )
        .unwrap();
        assert_eq!(oauth_usage.service_tier.as_deref(), Some("priority"));

        let response_default_nested_priority = serde_json::json!({
            "service_tier": "default",
            "usage": {
                "input_tokens": 10,
                "output_tokens": 5,
                "service_tier": "priority"
            }
        });
        let (std_explicit_default, _) = observed_response_usage(
            &response_default_nested_priority,
            ServiceTierMergePolicy::Standard,
        )
        .unwrap();
        assert_eq!(
            std_explicit_default.service_tier.as_deref(),
            Some("default")
        );
        let (oauth_placeholder, _) = observed_response_usage(
            &response_default_nested_priority,
            ServiceTierMergePolicy::OpenaiOauth,
        )
        .unwrap();
        assert_eq!(oauth_placeholder.service_tier.as_deref(), Some("priority"));
    }

    #[tokio::test]
    async fn oauth_ws_second_steer_control_reusing_successor_clears_prior_sent_tier() {
        use cc_lb_control::api_keys::builtin_authn::BuiltinAuthn;
        use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
        use cc_lb_control::api_keys::principal_view::PrincipalView;
        use cc_lb_control::dynamic_view::{DynamicViewBuilder, DynamicViewHolder};
        use cc_lb_control::event_bus::{InMemoryBus, RequestEventBus};

        struct UnusedWsDispatcher;
        #[async_trait::async_trait]
        impl crate::UpstreamDispatch for UnusedWsDispatcher {
            async fn dispatch(
                &self,
                _request: cc_lb_upstream::SignedRequest,
            ) -> Result<http::Response<crate::Body>, crate::DispatchError> {
                unreachable!("websocket test does not dispatch HTTP")
            }
        }

        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(1_800_000_000));
        let storage = Arc::new(
            cc_lb_storage_sqlite::open_sqlite("sqlite::memory:", clock.clone())
                .await
                .expect("open in-memory db"),
        );
        storage.initialize().await.expect("migrate db");
        let keys = Arc::new(KeyStore::new(storage.clone()));
        let (_rec, secret) = keys
            .create(
                DIRECT_PRINCIPAL,
                CreateParams {
                    label: "ws-steer-reuse".to_owned(),
                    description: None,
                    expires_at_unix_secs: None,
                    limit_overrides: Vec::new(),
                },
            )
            .await
            .expect("create test key");
        let upstream_id = Uuid::new_v4();
        let record = cc_lb_storage_api::upstream::UpstreamRecord {
            id: upstream_id,
            name: "oauth-steer-reuse".to_owned(),
            kind: cc_lb_storage_api::upstream::UpstreamKind::OpenaiOauth,
            enabled: true,
            base_url: Some(
                url::Url::parse("https://chatgpt.com/backend-api/codex").expect("valid test URL"),
            ),
            ..Default::default()
        };
        let pview = Arc::new(PrincipalView::for_tests(
            DIRECT_PRINCIPAL,
            true,
            vec!["*".to_owned()],
            Vec::new(),
            HashMap::new(),
        ));
        let authn = Arc::new(BuiltinAuthn::new(keys, clock.clone()));
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-api-key",
            secret.expose().parse().expect("valid auth header"),
        );
        let auth = crate::authenticate_first(&authn, &headers, &pview)
            .await
            .expect("authenticate");
        let bus = Arc::new(InMemoryBus::new());
        let mut events = bus.subscribe_lifecycle();
        let dynamic = DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(UnusedSignerFactory))
            .principal_view(pview)
            .upstream_records(vec![record])
            .build();
        let lifecycle = Arc::new(
            Lifecycle::new_with_dynamic_view(
                authn,
                Arc::new(DynamicViewHolder::new(dynamic)),
                Arc::new(UnusedWsDispatcher),
                crate::LifecycleConfig::default(),
                clock,
            )
            .with_event_bus(bus as Arc<dyn RequestEventBus>),
        );
        let mut gw = ResponsesWebSocketGateway::new(lifecycle, &auth, HeaderMap::new(), 1 << 20, 8);
        let parent_id = "resp_parent_steer_reuse";
        gw.owners.insert(
            parent_id.to_owned(),
            ResponseOwner {
                connection_id: 1,
                upstream_id,
                turn_id: 99,
                principal_id: auth.principal_id().to_owned(),
                lane: None,
                body: Arc::new(serde_json::json!({
                    "type": "response.create",
                    "model": "gpt-6.2-sol",
                    "service_tier": "priority",
                })),
                oauth_transport: true,
                oauth_compat: false,
                echo_metadata: None,
                terminal_delivery: TerminalDelivery::ResponseTerminalDelivered,
                evicted_for_continuation: false,
            },
        );
        gw.connections.insert(1, upstream_id);
        gw.connection_oauth.insert(1, true);
        let owner_charge = owner_bytes(parent_id, gw.owners.get(parent_id).unwrap());
        gw.used_bytes = owner_charge;

        let first_steer = Bytes::from_static(
            br#"{"type":"response.steer","previous_response_id":"resp_parent_steer_reuse","service_tier":"priority","input":[]}"#,
        );
        let first_dispatch = gw
            .client_message(auth.clone(), first_steer)
            .await
            .expect("first steer admits")
            .expect("dispatches immediately");
        let successor_event_id = gw.turns[&first_dispatch.turn_id]
            .prepared
            .observer
            .as_ref()
            .expect("steer turn has observer")
            .event_id()
            .to_owned();
        assert_eq!(
            gw.turns[&first_dispatch.turn_id].sent_service_tier(),
            Some("priority")
        );

        let second_steer = Bytes::from_static(
            br#"{"type":"response.steer","previous_response_id":"resp_parent_steer_reuse","input":[]}"#,
        );
        let queued = gw
            .client_message(auth, second_steer)
            .await
            .expect("second steer queues on same successor");
        assert!(queued.is_none());
        assert_eq!(
            gw.turns[&first_dispatch.turn_id].sent_service_tier(),
            None,
            "sharing a successor across controls must clear any stale sent tier fallback"
        );

        {
            let turn = gw.turns.get_mut(&first_dispatch.turn_id).unwrap();
            turn.sent = true;
            turn.connection_id = Some(1);
            turn.upstream_id = Some(upstream_id);
        }
        gw.lanes.insert(
            None,
            Lane {
                active: Some(first_dispatch.turn_id),
                ..Default::default()
            },
        );
        let pool = WsRelayPoolSnapshot::default();
        let failed_first = Bytes::from_static(
            br#"{"type":"response.steer.failed","steer":{"id":"steer_1","previous_response_id":"resp_parent_steer_reuse"}}"#,
        );
        let res = gw
            .provider_message_with_pool_and_opcode(1, &failed_first, false, &pool)
            .await
            .expect("handles steer.failed");
        assert!(res.forward);
        assert!(
            gw.turns.contains_key(&first_dispatch.turn_id),
            "second steer remains active on successor"
        );

        let created = Bytes::from_static(
            br#"{"type":"response.created","response":{"id":"resp_steer_child_2","object":"response","status":"in_progress","service_tier":"auto"}}"#,
        );
        gw.provider_message_with_pool_and_opcode(1, &created, false, &pool)
            .await
            .unwrap();
        let completed = Bytes::from_static(
            br#"{"type":"response.completed","response":{"id":"resp_steer_child_2","object":"response","status":"completed","service_tier":"default","usage":{"input_tokens":10,"output_tokens":5,"total_tokens":15}}}"#,
        );
        gw.provider_message_with_pool_and_opcode(1, &completed, false, &pool)
            .await
            .unwrap();
        assert!(!gw.owners.contains_key(parent_id));
        let child_owner = gw
            .owners
            .get("resp_steer_child_2")
            .expect("child owner bound");
        assert_eq!(
            gw.used_bytes,
            owner_bytes("resp_steer_child_2", child_owner)
        );

        let mut recorded_tier = None;
        while let Ok(ev) = events.try_recv() {
            if let LifecycleEvent::UsageObserved {
                event_id: ev_id,
                usage,
                ..
            } = ev
                && ev_id == successor_event_id
            {
                recorded_tier = usage.service_tier;
            }
        }
        assert_eq!(
            recorded_tier.as_deref(),
            Some("default"),
            "second steer without service_tier must not inherit rejected first steer's priority"
        );
    }

    #[tokio::test]
    async fn ws_usage_tier_budget_and_combined_failover_headroom_are_enforced() {
        use cc_lb_control::api_keys::builtin_authn::BuiltinAuthn;
        use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
        use cc_lb_control::api_keys::principal_view::PrincipalView;
        use cc_lb_control::dynamic_view::{DynamicViewBuilder, DynamicViewHolder};
        use cc_lb_control::event_bus::{InMemoryBus, RequestEventBus};

        struct UnusedWsDispatcher;
        #[async_trait::async_trait]
        impl crate::UpstreamDispatch for UnusedWsDispatcher {
            async fn dispatch(
                &self,
                _request: cc_lb_upstream::SignedRequest,
            ) -> Result<http::Response<crate::Body>, crate::DispatchError> {
                unreachable!("websocket test does not dispatch HTTP")
            }
        }

        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(1_800_000_000));
        let storage = Arc::new(
            cc_lb_storage_sqlite::open_sqlite("sqlite::memory:", clock.clone())
                .await
                .expect("open in-memory db"),
        );
        storage.initialize().await.expect("migrate db");
        let keys = Arc::new(KeyStore::new(storage.clone()));
        let (_rec, secret) = keys
            .create(
                DIRECT_PRINCIPAL,
                CreateParams {
                    label: "ws-usage-budget".to_owned(),
                    description: None,
                    expires_at_unix_secs: None,
                    limit_overrides: Vec::new(),
                },
            )
            .await
            .expect("create test key");
        let upstream_a_id = Uuid::new_v4();
        let upstream_b_id = Uuid::new_v4();
        let record_a = cc_lb_storage_api::upstream::UpstreamRecord {
            id: upstream_a_id,
            name: "oauth-usage-a".to_owned(),
            kind: cc_lb_storage_api::upstream::UpstreamKind::OpenaiOauth,
            enabled: true,
            base_url: Some(
                url::Url::parse("https://chatgpt.com/backend-api/codex").expect("valid test URL"),
            ),
            ..Default::default()
        };
        let record_b = cc_lb_storage_api::upstream::UpstreamRecord {
            id: upstream_b_id,
            name: "oauth-usage-b".to_owned(),
            kind: cc_lb_storage_api::upstream::UpstreamKind::OpenaiOauth,
            enabled: true,
            base_url: Some(
                url::Url::parse("https://chatgpt.com/backend-api/codex").expect("valid test URL"),
            ),
            ..Default::default()
        };
        let pview = Arc::new(PrincipalView::for_tests(
            DIRECT_PRINCIPAL,
            true,
            vec!["*".to_owned()],
            Vec::new(),
            HashMap::new(),
        ));
        let authn = Arc::new(BuiltinAuthn::new(keys, clock.clone()));
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-api-key",
            secret.expose().parse().expect("valid auth header"),
        );
        let auth = crate::authenticate_first(&authn, &headers, &pview)
            .await
            .expect("authenticate");
        let bus = Arc::new(InMemoryBus::new());
        let mut events = bus.subscribe_lifecycle();
        let dynamic = DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(UnusedSignerFactory))
            .principal_view(pview)
            .upstream_records(vec![record_a, record_b])
            .build();
        let lifecycle = Arc::new(
            Lifecycle::new_with_dynamic_view(
                authn,
                Arc::new(DynamicViewHolder::new(dynamic)),
                Arc::new(UnusedWsDispatcher),
                crate::LifecycleConfig::default(),
                clock,
            )
            .with_event_bus(bus as Arc<dyn RequestEventBus>),
        );

        let mut gw =
            ResponsesWebSocketGateway::new(lifecycle.clone(), &auth, HeaderMap::new(), 1 << 20, 8);
        let create = Bytes::from_static(
            br#"{"type":"response.create","model":"gpt-6.2-sol","store":false,"service_tier":"priority","input":[]}"#,
        );
        let dispatch = gw
            .client_message(auth.clone(), create)
            .await
            .unwrap()
            .unwrap();
        let upstream_usage_tier = format!("custom_{}", "t".repeat(120));
        let in_progress = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "type": "response.in_progress",
                "response": {
                    "id": "resp_usage_budget",
                    "object": "response",
                    "status": "in_progress",
                    "store": false,
                    "service_tier": upstream_usage_tier,
                    "usage": {"input_tokens": 5, "output_tokens": 1}
                }
            }))
            .unwrap(),
        );
        gw.connections.insert(1, upstream_a_id);
        gw.connection_oauth.insert(1, true);
        {
            let turn = gw.turns.get_mut(&dispatch.turn_id).unwrap();
            turn.sent = true;
            turn.connection_id = Some(1);
            turn.upstream_id = Some(upstream_a_id);
        }
        gw.lanes.insert(
            None,
            Lane {
                active: Some(dispatch.turn_id),
                ..Default::default()
            },
        );
        let pool = WsRelayPoolSnapshot::default();
        let pre_usage_charged = gw.turns[&dispatch.turn_id].charged_bytes;
        gw.provider_message_with_pool_and_opcode(1, &in_progress, false, &pool)
            .await
            .unwrap();
        let with_usage_charged = gw.turns[&dispatch.turn_id].charged_bytes;
        assert_eq!(
            with_usage_charged,
            pre_usage_charged + upstream_usage_tier.len(),
            "upstream-reported usage service_tier must be accounted in turn.charged_bytes"
        );
        let non_turn_used = gw.used_bytes - with_usage_charged;

        // Tighten byte_cap below an even longer usage tier update -> 503 + clean settle_turn.
        let longer_tier = format!("longer_{}", "x".repeat(200));
        gw.byte_cap = gw.used_bytes + 8;
        let overflow_completed = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "type": "response.completed",
                "response": {
                    "id": "resp_usage_budget",
                    "object": "response",
                    "status": "completed",
                    "store": false,
                    "service_tier": longer_tier,
                    "usage": {"input_tokens": 5, "output_tokens": 2}
                }
            }))
            .unwrap(),
        );
        let Err(err) = gw
            .provider_message_with_pool_and_opcode(1, &overflow_completed, false, &pool)
            .await
        else {
            panic!("usage tier exceeding byte_cap must fail closed");
        };
        assert_eq!(err.code, "overloaded_error");
        assert_eq!(gw.used_bytes, non_turn_used);
        while events.try_recv().is_ok() {}

        // Combined failover headroom check in try_unpinned_live_failover_dispatch:
        // each of extra_retained_bytes and new_tier_bytes fits individually, but their sum exceeds headroom.
        let mut gw_failover =
            ResponsesWebSocketGateway::new(lifecycle, &auth, HeaderMap::new(), 1 << 20, 8);
        let failover_tier = format!("tier_{}", "p".repeat(60));
        let failover_frame = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "type": "response.create",
                "model": "gpt-6.2-sol",
                "store": false,
                "service_tier": failover_tier,
                "input": []
            }))
            .unwrap(),
        );
        let d2 = gw_failover
            .client_message(auth, failover_frame)
            .await
            .unwrap()
            .unwrap();
        let initial_charge = gw_failover.turns[&d2.turn_id].charged_bytes;
        let plugin_tier_released =
            sent_service_tier_bytes(gw_failover.turns[&d2.turn_id].sent_service_tier());
        {
            let turn = gw_failover.turns.get_mut(&d2.turn_id).unwrap();
            turn.live_failover_candidates = vec![WebSocketCandidate {
                upstream_id: upstream_b_id,
                upstream_name: "oauth-usage-b".to_owned(),
                is_api_key: false,
                socket_per_lane: true,
            }];
            turn.plugin_request = None;
        }
        gw_failover.byte_cap = gw_failover.used_bytes - plugin_tier_released + 70;
        let denied = gw_failover
            .try_unpinned_live_failover_dispatch(d2.turn_id, upstream_a_id, Some(1), &pool, 50)
            .await;
        assert!(
            denied.is_none(),
            "failover must decline when extra_retained_bytes + restored tier exceed headroom"
        );
        assert_eq!(
            gw_failover.turns[&d2.turn_id].charged_bytes, initial_charge,
            "declined failover must not mutate turn charge"
        );
        gw_failover.finish_turn(d2.turn_id, None);
        assert_eq!(gw_failover.used_bytes, 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ws_first_usage_report_with_oversized_tier_settles_counters_and_reconciles_reservation()
    {
        let _guard = DIRECT_PRICING_LOCK.lock().await;
        install_direct_pricing();

        let oversized_tier = format!("oversized_{}", "z".repeat(200));
        let pool = WsRelayPoolSnapshot::default();

        // Case A: Live turn whose very first usage report arrives on `response.completed`
        // with an oversized `service_tier` still reconciles its reservation with the measured counters.
        {
            let fixture = DirectAccountingFixture::new().await;
            let reservation = fixture.reserve("ws-openai-known");
            let reservation_id = reservation.id().to_owned();
            let expected_cost =
                cc_lb_pricing::virtual_cost_micros_full_with_context_and_provider_writes(
                    "ws-openai-known",
                    1_000,
                    500,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    None,
                    None,
                    None,
                    None,
                )
                .unwrap()
                .total_micros;
            let mut gw = ResponsesWebSocketGateway::new(
                fixture.lifecycle.clone(),
                &fixture.auth,
                HeaderMap::new(),
                65_536,
                8,
            );
            let mut turn =
                make_synthetic_turn(&fixture, None, None, true, false, Some(false), None);
            let padded_body = serde_json::json!({
                "type": "response.create",
                "model": "ws-openai-known",
                "store": false,
                "input": "x".repeat(600),
            });
            turn.frame = Bytes::from(serde_json::to_vec(&padded_body).unwrap());
            turn.body = Arc::new(padded_body);
            turn.prepared.model = "ws-openai-known".to_owned();
            turn.prepared.reservation = Some(reservation);
            turn.prepared.limit_engine = Some(fixture.engine.clone());
            turn.upstream_id = Some(Uuid::nil());
            turn.connection_id = Some(1);
            let frame_charge = turn
                .frame
                .len()
                .saturating_add(json_owned_bytes(&turn.body));
            turn.charged_bytes = frame_charge;
            gw.turns.insert(1, turn);
            gw.next_turn_id = 1;
            gw.used_bytes = frame_charge;
            gw.lanes.insert(
                None,
                Lane {
                    active: Some(1),
                    ..Default::default()
                },
            );
            gw.connections.insert(1, Uuid::nil());
            gw.connection_oauth.insert(1, false);
            let created = Bytes::from_static(
                br#"{"type":"response.created","response":{"id":"resp_live_first_overflow","object":"response","status":"in_progress","store":false}}"#,
            );
            gw.provider_message_with_pool_and_opcode(1, &created, false, &pool)
                .await
                .unwrap();
            gw.byte_cap = gw.used_bytes + 8;

            let completed = Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "type": "response.completed",
                    "response": {
                        "id": "resp_live_first_overflow",
                        "object": "response",
                        "status": "completed",
                        "store": false,
                        "service_tier": oversized_tier,
                        "usage": {"input_tokens": 1_000, "output_tokens": 500, "total_tokens": 1_500}
                    }
                }))
                .unwrap(),
            );
            let Err(err) = gw
                .provider_message_with_pool_and_opcode(1, &completed, false, &pool)
                .await
            else {
                panic!("oversized live usage tier must fail closed");
            };
            assert_eq!(err.code, "overloaded_error");
            let non_turn_used = owner_bytes(
                "resp_live_first_overflow",
                gw.owners
                    .get("resp_live_first_overflow")
                    .expect("owner retained for continuation"),
            );
            assert_eq!(gw.used_bytes, non_turn_used);
            gw.connection_closed(1, "cleanup");
            assert_eq!(gw.used_bytes, 0);
            fixture
                .assert_settled(
                    &reservation_id,
                    "ws-openai-known",
                    expected_cost,
                    1_000,
                    500,
                )
                .await;
        }

        // Case B: Parked PendingErrorTerminal whose first usage arrives in delayed `response.failed`
        // with an oversized `service_tier` still settles newly measured counters and labels the error.
        {
            let fixture = DirectAccountingFixture::new().await;
            let reservation = fixture.reserve("ws-openai-known");
            let reservation_id = reservation.id().to_owned();
            let expected_cost =
                cc_lb_pricing::virtual_cost_micros_full_with_context_and_provider_writes(
                    "ws-openai-known",
                    800,
                    400,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    None,
                    None,
                    None,
                    None,
                )
                .unwrap()
                .total_micros;
            let mut gw = ResponsesWebSocketGateway::new(
                fixture.lifecycle.clone(),
                &fixture.auth,
                HeaderMap::new(),
                65_536,
                8,
            );
            let mut turn =
                make_synthetic_turn(&fixture, None, None, true, false, Some(false), None);
            let padded_body = serde_json::json!({
                "type": "response.create",
                "model": "ws-openai-known",
                "store": false,
                "input": "x".repeat(600),
            });
            turn.frame = Bytes::from(serde_json::to_vec(&padded_body).unwrap());
            turn.body = Arc::new(padded_body);
            turn.prepared.model = "ws-openai-known".to_owned();
            turn.prepared.reservation = Some(reservation);
            turn.prepared.limit_engine = Some(fixture.engine.clone());
            turn.upstream_id = Some(Uuid::nil());
            turn.connection_id = Some(1);
            let frame_charge = turn
                .frame
                .len()
                .saturating_add(json_owned_bytes(&turn.body));
            turn.charged_bytes = frame_charge;
            gw.turns.insert(1, turn);
            gw.next_turn_id = 1;
            gw.used_bytes = frame_charge;
            gw.lanes.insert(
                None,
                Lane {
                    active: Some(1),
                    ..Default::default()
                },
            );
            gw.connections.insert(1, Uuid::nil());
            gw.connection_oauth.insert(1, false);

            let created = Bytes::from_static(
                br#"{"type":"response.created","response":{"id":"resp_parked_first_usage","object":"response","status":"in_progress","store":false}}"#,
            );
            gw.provider_message_with_pool_and_opcode(1, &created, false, &pool)
                .await
                .unwrap();
            let park_error = Bytes::from_static(
                br#"{"type":"error","response":{"id":"resp_parked_first_usage","object":"response","status":"failed","store":false},"error":{"code":"upstream_error","message":"boom"}}"#,
            );
            gw.provider_message_with_pool_and_opcode(1, &park_error, false, &pool)
                .await
                .unwrap();
            assert!(
                gw.pending_error_terminals
                    .contains_key("resp_parked_first_usage")
            );
            gw.byte_cap = gw.used_bytes + 8;

            let delayed_failed = Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "type": "response.failed",
                    "response": {
                        "id": "resp_parked_first_usage",
                        "object": "response",
                        "status": "failed",
                        "store": false,
                        "service_tier": oversized_tier,
                        "error": {"code": "upstream_error", "message": "boom"},
                        "usage": {"input_tokens": 800, "output_tokens": 400, "total_tokens": 1_200}
                    }
                }))
                .unwrap(),
            );
            let Err(err) = gw
                .provider_message_with_pool_and_opcode(1, &delayed_failed, false, &pool)
                .await
            else {
                panic!("oversized parked usage tier must fail closed");
            };
            assert_eq!(err.code, "overloaded_error");
            assert_eq!(err.response_id(), Some("resp_parked_first_usage"));
            assert!(
                !gw.pending_error_terminals
                    .contains_key("resp_parked_first_usage")
            );
            let non_turn_used = owner_bytes(
                "resp_parked_first_usage",
                gw.owners
                    .get("resp_parked_first_usage")
                    .expect("owner retained"),
            );
            assert_eq!(gw.used_bytes, non_turn_used);
            gw.connection_closed(1, "cleanup");
            assert_eq!(gw.used_bytes, 0);
            fixture
                .assert_settled(&reservation_id, "ws-openai-known", expected_cost, 800, 400)
                .await;
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ws_usage_budget_overflow_preserves_observed_priority_tier_for_immediate_settlement() {
        let _guard = DIRECT_PRICING_LOCK.lock().await;
        install_direct_pricing();

        let expected_base_cost =
            cc_lb_pricing::virtual_cost_micros_full_with_context_and_provider_writes(
                "ws-openai-priority-known",
                500,
                200,
                0,
                0,
                0,
                0,
                0,
                0,
                None,
                None,
                None,
                None,
            )
            .unwrap()
            .total_micros;
        let expected_priority_cost =
            cc_lb_pricing::virtual_cost_micros_full_with_context_and_provider_writes(
                "ws-openai-priority-known",
                500,
                200,
                0,
                0,
                0,
                0,
                0,
                0,
                None,
                None,
                None,
                Some("priority"),
            )
            .unwrap()
            .total_micros;
        assert!(expected_priority_cost > expected_base_cost);

        let pool = WsRelayPoolSnapshot::default();

        // Case A: Embedded live turn settles with observed `"priority"` multiplier when headroom is < 7 bytes.
        {
            let fixture = DirectAccountingFixture::new().await;
            let reservation = fixture.reserve("ws-openai-priority-known");
            let reservation_id = reservation.id().to_owned();
            let mut gw = ResponsesWebSocketGateway::new(
                fixture.lifecycle.clone(),
                &fixture.auth,
                HeaderMap::new(),
                65_536,
                8,
            );
            let mut turn =
                make_synthetic_turn(&fixture, None, None, true, false, Some(false), None);
            let padded_body = serde_json::json!({
                "type": "response.create",
                "model": "ws-openai-priority-known",
                "store": false,
                "input": "x".repeat(600),
            });
            turn.frame = Bytes::from(serde_json::to_vec(&padded_body).unwrap());
            turn.body = Arc::new(padded_body);
            turn.prepared.model = "ws-openai-priority-known".to_owned();
            turn.prepared.reservation = Some(reservation);
            turn.prepared.limit_engine = Some(fixture.engine.clone());
            turn.upstream_id = Some(Uuid::nil());
            turn.connection_id = Some(1);
            let frame_charge = turn
                .frame
                .len()
                .saturating_add(json_owned_bytes(&turn.body));
            turn.charged_bytes = frame_charge;
            gw.turns.insert(1, turn);
            gw.next_turn_id = 1;
            gw.used_bytes = frame_charge;
            gw.lanes.insert(
                None,
                Lane {
                    active: Some(1),
                    ..Default::default()
                },
            );
            gw.connections.insert(1, Uuid::nil());
            gw.connection_oauth.insert(1, false);

            let created = Bytes::from_static(
                br#"{"type":"response.created","response":{"id":"resp_live_priority_overflow","object":"response","status":"in_progress","store":false}}"#,
            );
            gw.provider_message_with_pool_and_opcode(1, &created, false, &pool)
                .await
                .unwrap();
            gw.byte_cap = gw.used_bytes + 4;

            let completed = Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "type": "response.completed",
                    "response": {
                        "id": "resp_live_priority_overflow",
                        "object": "response",
                        "status": "completed",
                        "store": false,
                        "service_tier": "priority",
                        "usage": {"input_tokens": 500, "output_tokens": 200, "total_tokens": 700}
                    }
                }))
                .unwrap(),
            );
            let Err(err) = gw
                .provider_message_with_pool_and_opcode(1, &completed, false, &pool)
                .await
            else {
                panic!("priority tier exceeding 4B remaining headroom must overflow");
            };
            assert_eq!(err.code, "overloaded_error");
            assert_eq!(err.turn_id(), Some(1));
            assert_eq!(err.response_id(), Some("resp_live_priority_overflow"));
            let non_turn_used = owner_bytes(
                "resp_live_priority_overflow",
                gw.owners
                    .get("resp_live_priority_overflow")
                    .expect("owner retained for continuation"),
            );
            assert_eq!(gw.used_bytes, non_turn_used);
            gw.connection_closed(1, "cleanup");
            assert_eq!(gw.used_bytes, 0);
            fixture
                .assert_settled(
                    &reservation_id,
                    "ws-openai-priority-known",
                    expected_priority_cost,
                    500,
                    200,
                )
                .await;
        }

        // Case B: Event-based OAuth turn preserves `sent_service_tier` fallback when placeholder `"default"` overflows 4B headroom.
        {
            use cc_lb_control::RequestEventBus as _;

            let fixture = DirectAccountingFixture::new().await;
            let bus = Arc::new(cc_lb_control::InMemoryBus::new());
            let mut events = bus.subscribe_lifecycle();
            let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
            let observer =
                LifecycleContext::new("turn-oauth-priority-overflow".to_owned(), bus, &clock);
            observer.mark_authn_reached();
            let mut turn = make_synthetic_turn(
                &fixture,
                Some(observer),
                None,
                true,
                true,
                Some(false),
                None,
            );
            let padded_body = serde_json::json!({
                "type": "response.create",
                "model": "ws-openai-priority-known",
                "store": false,
                "service_tier": "priority",
                "input": "x".repeat(600),
            });
            turn.frame = Bytes::from(serde_json::to_vec(&padded_body).unwrap());
            turn.body = Arc::new(padded_body);
            turn.sent_service_tier = Some("priority".to_owned());
            turn.prepared.model = "ws-openai-priority-known".to_owned();
            turn.upstream_id = Some(Uuid::nil());
            turn.connection_id = Some(1);
            let frame_charge = turn
                .frame
                .len()
                .saturating_add(json_owned_bytes(&turn.body))
                .saturating_add(sent_service_tier_bytes(turn.sent_service_tier.as_deref()));
            turn.charged_bytes = frame_charge;
            let mut gw = ResponsesWebSocketGateway::new(
                fixture.lifecycle.clone(),
                &fixture.auth,
                HeaderMap::new(),
                65_536,
                8,
            );
            gw.turns.insert(1, turn);
            gw.next_turn_id = 1;
            gw.used_bytes = frame_charge;
            gw.lanes.insert(
                None,
                Lane {
                    active: Some(1),
                    ..Default::default()
                },
            );
            gw.connections.insert(1, Uuid::nil());
            gw.connection_oauth.insert(1, true);
            let created = Bytes::from_static(
                br#"{"type":"response.created","response":{"id":"resp_obs_priority_overflow","object":"response","status":"in_progress","store":false,"service_tier":"auto"}}"#,
            );
            gw.provider_message_with_pool_and_opcode(1, &created, false, &pool)
                .await
                .unwrap();
            gw.byte_cap = gw.used_bytes + 4;

            let completed = Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "type": "response.completed",
                    "response": {
                        "id": "resp_obs_priority_overflow",
                        "object": "response",
                        "status": "completed",
                        "store": false,
                        "service_tier": "default",
                        "usage": {"input_tokens": 500, "output_tokens": 200, "total_tokens": 700}
                    }
                }))
                .unwrap(),
            );
            let Err(err) = gw
                .provider_message_with_pool_and_opcode(1, &completed, false, &pool)
                .await
            else {
                panic!("placeholder default exceeding 4B headroom must overflow");
            };
            assert_eq!(err.code, "overloaded_error");
            assert_eq!(err.turn_id(), Some(1));
            gw.connection_closed(1, "cleanup");
            assert_eq!(gw.used_bytes, 0);

            let mut observed_tier = None;
            while let Ok(event) = events.try_recv() {
                if let LifecycleEvent::UsageObserved { usage, .. } = event {
                    observed_tier = usage.service_tier;
                }
            }
            assert_eq!(observed_tier.as_deref(), Some("priority"));
        }

        // Case C: Parked `PendingErrorTerminal` settles with observed `"priority"` multiplier when headroom is < 7 bytes.
        {
            let fixture = DirectAccountingFixture::new().await;
            let reservation = fixture.reserve("ws-openai-priority-known");
            let reservation_id = reservation.id().to_owned();
            let mut gw = ResponsesWebSocketGateway::new(
                fixture.lifecycle.clone(),
                &fixture.auth,
                HeaderMap::new(),
                65_536,
                8,
            );
            let mut turn =
                make_synthetic_turn(&fixture, None, None, true, false, Some(false), None);
            let padded_body = serde_json::json!({
                "type": "response.create",
                "model": "ws-openai-priority-known",
                "store": false,
                "input": "x".repeat(600),
            });
            turn.frame = Bytes::from(serde_json::to_vec(&padded_body).unwrap());
            turn.body = Arc::new(padded_body);
            turn.prepared.model = "ws-openai-priority-known".to_owned();
            turn.prepared.reservation = Some(reservation);
            turn.prepared.limit_engine = Some(fixture.engine.clone());
            turn.upstream_id = Some(Uuid::nil());
            turn.connection_id = Some(1);
            let frame_charge = turn
                .frame
                .len()
                .saturating_add(json_owned_bytes(&turn.body));
            turn.charged_bytes = frame_charge;
            gw.turns.insert(1, turn);
            gw.next_turn_id = 1;
            gw.used_bytes = frame_charge;
            gw.lanes.insert(
                None,
                Lane {
                    active: Some(1),
                    ..Default::default()
                },
            );
            gw.connections.insert(1, Uuid::nil());
            gw.connection_oauth.insert(1, false);

            let created = Bytes::from_static(
                br#"{"type":"response.created","response":{"id":"resp_parked_priority_overflow","object":"response","status":"in_progress","store":false}}"#,
            );
            gw.provider_message_with_pool_and_opcode(1, &created, false, &pool)
                .await
                .unwrap();
            let park_error = Bytes::from_static(
                br#"{"type":"error","response":{"id":"resp_parked_priority_overflow","object":"response","status":"failed","store":false},"error":{"code":"upstream_error","message":"boom"}}"#,
            );
            gw.provider_message_with_pool_and_opcode(1, &park_error, false, &pool)
                .await
                .unwrap();
            gw.byte_cap = gw.used_bytes + 4;

            let delayed_failed = Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "type": "response.failed",
                    "response": {
                        "id": "resp_parked_priority_overflow",
                        "object": "response",
                        "status": "failed",
                        "store": false,
                        "service_tier": "priority",
                        "error": {"code": "upstream_error", "message": "boom"},
                        "usage": {"input_tokens": 500, "output_tokens": 200, "total_tokens": 700}
                    }
                }))
                .unwrap(),
            );
            let Err(err) = gw
                .provider_message_with_pool_and_opcode(1, &delayed_failed, false, &pool)
                .await
            else {
                panic!("parked priority overflow must fail closed");
            };
            assert_eq!(err.code, "overloaded_error");
            assert_eq!(err.turn_id(), Some(1));
            assert_eq!(err.response_id(), Some("resp_parked_priority_overflow"));
            let non_turn_used = owner_bytes(
                "resp_parked_priority_overflow",
                gw.owners
                    .get("resp_parked_priority_overflow")
                    .expect("owner retained"),
            );
            assert_eq!(gw.used_bytes, non_turn_used);
            gw.connection_closed(1, "cleanup");
            assert_eq!(gw.used_bytes, 0);
            fixture
                .assert_settled(
                    &reservation_id,
                    "ws-openai-priority-known",
                    expected_priority_cost,
                    500,
                    200,
                )
                .await;
        }
    }
}
