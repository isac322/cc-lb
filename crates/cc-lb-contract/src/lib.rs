//! Contract types and traits shared by cc-lb runtime layers.
//!
//! This crate owns the event vocabulary, DTOs, and hook traits exchanged between
//! the data-plane engine, control-plane subscribers, storage adapters, and
//! dashboard/API projections. It has no storage or engine runtime dependency.
//!
//! # Design goals
//!
//! - **Types + trait definitions.** No storage, engine runtime, or request
//!   execution. Owns serde-ready DTOs, hook traits, and channel endpoint types.
//! - **`#[non_exhaustive]`.** Adding a new event variant or extending an
//!   existing struct field must not require a major version bump of the
//!   consumer crates.
//! - **Result-based stage variants.** Each pipeline stage emits
//!   `Result<StageInfo, StageFailure>` so consumers can pattern-match on
//!   success/failure explicitly instead of guessing from a status code.
//!
//! # Sequence (successful non-stream request)
//!
//! ```text
//! RequestStarted
//!   → ParseCompleted(Ok)
//!   → AuthCompleted(Ok)
//!   → RouteCompleted(Ok)
//!   → LimitDecision(Reserved)
//!   → UpstreamAttempt
//!   → UpstreamResponseStarted
//!   → UsageObserved(source=NonStreamBody)
//!   → RequestTerminated(reason=Success, status=200)
//! ```
//!
//! # Sequence (streaming response with mid-stream error)
//!
//! ```text
//! RequestStarted
//!   → ParseCompleted(Ok)
//!   → AuthCompleted(Ok)
//!   → RouteCompleted(Ok)
//!   → LimitDecision(Reserved)
//!   → UpstreamAttempt
//!   → UpstreamResponseStarted
//!   → UsageObserved(source=MessageStart)
//!   → UsageObserved(source=MessageDelta) × N
//!   → StreamCompleted(Err(...))
//!   → RequestTerminated(reason=ErrorCode("upstream_stream_error"), status=200)
//! ```
//!
//! # See also
//!
//! - `docs/rfc/0002-event-driven-lifecycle.md` §Event vocabulary
//! - `cc_lb_engine::LifecycleContext` (producer)
//! - `cc_lb_contract::RequestEventBus::publish_lifecycle` (transport trait)

#![deny(missing_debug_implementations)]

pub mod event_bus;

pub use event_bus::{
    BusReceiver, DEFAULT_LIFECYCLE_BROADCAST_CAPACITY, FinalRequestEventUpdate,
    LifecycleBusReceiver, RequestEventBus, RequestEventPartial, RequestEventPhase,
    RequestEventUpdate,
};

use std::collections::BTreeMap;

use cc_lb_plugin_api::{InternalError, RoutingTrace, types::TtlClass};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use uuid::Uuid;

pub trait AuditSink: Send + Sync {
    fn sink_audit(&self, entry: AuditEntry);
}

pub trait EngineMetricsHook: Send + Sync {
    fn record_cache_hit(&self, upstream: &str, model: &str);

    fn record_cache_miss(&self, upstream: &str, model: &str);

    fn record_cache_observation_dropped(&self, reason: &str);

    fn record_dropped_events_by(&self, reason: &str, count: u64);

    fn record_routing_tier_selection(&self, tier: &str, upstream: &str, principal_id: &str);
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoopMetricsHook;

impl EngineMetricsHook for NoopMetricsHook {
    fn record_cache_hit(&self, _upstream: &str, _model: &str) {}

    fn record_cache_miss(&self, _upstream: &str, _model: &str) {}

    fn record_cache_observation_dropped(&self, _reason: &str) {}

    fn record_dropped_events_by(&self, _reason: &str, _count: u64) {}

    fn record_routing_tier_selection(&self, _tier: &str, _upstream: &str, _principal_id: &str) {}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaIdentity {
    pub id: Uuid,
}

pub trait ReplicaIdentityProvider: Send + Sync {
    fn replica_identity(&self) -> Option<ReplicaIdentity>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKindLite {
    Human,
    #[default]
    Machine,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RequestEvent {
    #[serde(default, skip_serializing_if = "is_zero")]
    pub ts: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ts_ms: Option<u64>,
    pub principal_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_id: Option<String>,
    pub principal_kind: Option<String>,
    pub upstream: Option<RequestEventUpstream>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_name: Option<String>,
    pub model: Option<String>,
    pub status: u16,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_creation_input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_creation_input_tokens_5m: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_creation_input_tokens_1h: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_read_input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_state: Option<RequestCacheState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_index: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_control_block_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cache_control_message_indices: Vec<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cache_breakpoints: Vec<RequestCacheBreakpoint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_prefix_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_usd_micros: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_input_micros: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_output_micros: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_cache_creation_5m_micros: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_cache_creation_1h_micros: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_cache_read_micros: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit_reserve_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bulkhead_wait_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dns_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connect_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_reused: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit_reconcile_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observability_post_ms: Option<u64>,
    pub duration_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_setup_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shape_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sign_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_ttfb_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_body_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_body_chunk_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_chunk_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_message_start_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_content_block_start_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_first_content_delta_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_last_content_delta_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_message_stop_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_last_chunk_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_total_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sse_event_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_delta_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ping_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inter_token_avg_ms: Option<u64>,
    pub error_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing_trace: Option<RoutingTrace>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub internal_errors: Vec<InternalError>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub web_search_requests: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub web_fetch_requests: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inference_geo: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_error_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_error_message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iterations: Option<JsonValue>,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestEventUpstream {
    AnthropicDirect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestCacheState {
    Hit,
    Write,
    Refresh,
    Miss,
    None,
    Unknown,
}

impl RequestCacheState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Write => "write",
            Self::Refresh => "refresh",
            Self::Miss => "miss",
            Self::None => "none",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestCacheBreakpoint {
    pub block_index: u64,
    pub source: RequestCacheBreakpointSource,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_index: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl: Option<String>,
    pub prefix_hash: String,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub prefix_token_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestCacheBreakpointSource {
    System,
    Tools,
    Message,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AuditEntry {
    pub ts: u64,
    pub request_id: String,
    pub principal_id: String,
    pub route: String,
    pub upstream: String,
    pub model: Option<String>,
    pub status: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
    pub duration_ms: u64,
    pub agent_label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_usd_micros: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit_violation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin_action: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<JsonValue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum LimitKind {
    #[default]
    Requests,
    InputTokens,
    OutputTokens,
    TotalTokens,
    CostUsd,
    Concurrent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Limit {
    pub kind: LimitKind,
    pub window_secs: u64,
    pub cap_micros: i64,
}

impl Limit {
    pub fn is_subset_of(&self, parent: &Limit) -> bool {
        self.kind == parent.kind
            && self.window_secs == parent.window_secs
            && self.cap_micros <= parent.cap_micros
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum KeyStatus {
    #[default]
    Active,
    Disabled,
    Revoked,
}

/// Application-generated unique identifier for a single request lifecycle.
///
/// Same value the `LifecycleContext` (successor of `TerminalObserver`)
/// generates as a UUID v7 string. Used as the DB row uniqueness key and
/// for cross-subscriber correlation.
pub type EventId = String;

/// Fixed vocabulary of events emitted during a single request's lifecycle.
///
/// See the crate-level documentation for the expected sequences.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LifecycleEvent {
    /// Handler entry; emitted once per request before any pipeline work.
    RequestStarted {
        event_id: EventId,
        request_id: String,
        ts_ms: u64,
        stream: bool,
    },
    /// Body parsing and shape validation completed.
    ParseCompleted {
        event_id: EventId,
        result: Result<ParseInfo, ParseFailure>,
    },
    /// Authentication (bearer token, principal lookup) completed.
    AuthCompleted {
        event_id: EventId,
        result: Result<AuthInfo, AuthFailure>,
    },
    AuthenticationCompleted {
        event_id: EventId,
        principal_id: String,
        principal_kind: PrincipalKindLite,
    },
    /// Router selection completed.
    RouteCompleted {
        event_id: EventId,
        result: Result<RouteInfo, RouteFailure>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        routing_trace: Option<RoutingTrace>,
    },
    /// Rate-limit / quota decision.
    LimitDecision {
        event_id: EventId,
        decision: LimitDecisionKind,
    },
    /// An attempt to dispatch to an upstream (may be retried).
    UpstreamAttempt {
        event_id: EventId,
        attempt_num: u32,
        upstream_id: Uuid,
    },
    /// Upstream returned response headers (may be pre-body).
    UpstreamResponseStarted {
        event_id: EventId,
        status: u16,
        #[serde(default, skip_serializing_if = "HeaderSnapshot::is_empty")]
        headers: HeaderSnapshot,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bulkhead_wait_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        dns_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        connect_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        connection_reused: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        shape_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sign_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        upstream_ttfb_ms: Option<u64>,
    },
    ProviderErrorObserved {
        event_id: EventId,
        code: String,
        message: String,
        source: String,
    },
    /// Usage counts were observed from an SSE frame or non-stream body.
    UsageObserved {
        event_id: EventId,
        usage: UsageSnapshot,
        source: UsageSource,
    },
    /// The streaming (or non-streaming) response body completed.
    StreamCompleted {
        event_id: EventId,
        result: Result<StreamSuccess, StreamError>,
    },
    /// Terminal event; exactly one per request unless the process is
    /// killed with SIGKILL. See PR #222 observation guarantee.
    RequestTerminated {
        event_id: EventId,
        reason: TerminationReason,
        client_status: u16,
        duration_ms: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit_reconcile_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        observability_post_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        proxy_setup_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        upstream_body_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        first_body_chunk_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        internal_errors: Vec<InternalError>,
    },
    /// Emitted by the pricing subscriber after cost is computed from usage
    /// counts, model, and upstream kind. The assembler merges these micros
    /// into the request row's cost columns.
    Priced {
        event_id: EventId,
        cost: CostBreakdown,
    },
    /// Emitted by the cache-observation subscriber when the final
    /// prompt-cache state can be derived from the usage counters plus
    /// parse-time cache metadata. The assembler merges this into the
    /// request row's `cache_state` column.
    CacheObserved {
        event_id: EventId,
        cache_state: RequestCacheState,
    },
    /// Emitted by request producers after prompt-cache observations are decoded.
    /// The prompt-cache observation subscriber owns cache writes, sink enqueue,
    /// and drop metric side effects for these records.
    PromptCacheObservationsProduced {
        event_id: EventId,
        upstream_id: Uuid,
        canonical_model_id: String,
        observations: Vec<PromptCacheObservationWire>,
        dropped_below_threshold: u32,
        dropped_aborted: u32,
    },
}

impl LifecycleEvent {
    /// Extract the correlation ID.
    pub fn event_id(&self) -> &EventId {
        match self {
            Self::RequestStarted { event_id, .. }
            | Self::ParseCompleted { event_id, .. }
            | Self::AuthCompleted { event_id, .. }
            | Self::AuthenticationCompleted { event_id, .. }
            | Self::RouteCompleted { event_id, .. }
            | Self::LimitDecision { event_id, .. }
            | Self::UpstreamAttempt { event_id, .. }
            | Self::UpstreamResponseStarted { event_id, .. }
            | Self::ProviderErrorObserved { event_id, .. }
            | Self::UsageObserved { event_id, .. }
            | Self::StreamCompleted { event_id, .. }
            | Self::RequestTerminated { event_id, .. }
            | Self::Priced { event_id, .. }
            | Self::CacheObserved { event_id, .. }
            | Self::PromptCacheObservationsProduced { event_id, .. } => event_id,
        }
    }

    /// Static label suitable for Prometheus metric cardinality.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::RequestStarted { .. } => "request_started",
            Self::ParseCompleted { .. } => "parse_completed",
            Self::AuthCompleted { .. } => "auth_completed",
            Self::AuthenticationCompleted { .. } => "authentication_completed",
            Self::RouteCompleted { .. } => "route_completed",
            Self::LimitDecision { .. } => "limit_decision",
            Self::UpstreamAttempt { .. } => "upstream_attempt",
            Self::UpstreamResponseStarted { .. } => "upstream_response_started",
            Self::ProviderErrorObserved { .. } => "provider_error_observed",
            Self::UsageObserved { .. } => "usage_observed",
            Self::StreamCompleted { .. } => "stream_completed",
            Self::RequestTerminated { .. } => "request_terminated",
            Self::Priced { .. } => "priced",
            Self::CacheObserved { .. } => "cache_observed",
            Self::PromptCacheObservationsProduced { .. } => "prompt_cache_observations_produced",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PromptCacheObservationWire {
    pub prefix_hash: String,
    pub ttl_class: TtlClass,
    pub expires_at_unix_secs: u64,
    pub kind: PromptCacheObservationKindWire,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PromptCacheObservationKindWire {
    Hit,
    Write,
}

/// Sanitized subset of upstream response headers carried by
/// `LifecycleEvent::UpstreamResponseStarted` (RFC-0002 §237-241).
///
/// Producers MUST NOT copy `Authorization` or similar credential-bearing
/// headers. The `anthropic_headers` map is a flat pass-through of every
/// header whose lowercased name starts with `anthropic-ratelimit-` OR
/// exactly matches one of the fixed Anthropic identity slots (see
/// `ANTHROPIC_IDENTITY_HEADERS`). Downstream subscribers parse these into
/// typed rate-limit and subscription-quota observations.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeaderSnapshot {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_encoding: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// `Retry-After` header. Anthropic sets this on 429/503.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after: Option<String>,
    /// All headers matching `anthropic-ratelimit-*` (any window/field
    /// combination — the vocabulary is open-ended, so we pass through
    /// raw values keyed by their lowercased header name) plus the fixed
    /// identity slots (`anthropic-organization-id`, etc.). Downstream
    /// subscribers reconstruct a `HeaderMap` and hand it to the existing
    /// `parse_anthropic_rate_limit_headers` / `parse_anthropic_unified_headers`
    /// parsers in `cc_lb_engine::rate_limit_headers`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub anthropic_headers: BTreeMap<String, String>,
}

/// Anthropic identity header slots that MUST be projected verbatim into
/// `HeaderSnapshot::anthropic_headers` in addition to the
/// `anthropic-ratelimit-*` prefix match. Producers use this list to
/// build the map without duplicating string constants.
pub const ANTHROPIC_IDENTITY_HEADERS: &[&str] =
    &["anthropic-organization-id", "anthropic-account-uuid"];

impl HeaderSnapshot {
    pub fn is_empty(&self) -> bool {
        self.content_type.is_none()
            && self.content_encoding.is_none()
            && self.request_id.is_none()
            && self.retry_after.is_none()
            && self.anthropic_headers.is_empty()
    }
}

/// Cost breakdown in micro-USD, produced by the pricing subscriber.
///
/// Field semantics mirror `RequestEvent.cost_*_micros` for direct assembler
/// merge.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct CostBreakdown {
    pub total_micros: Option<i64>,
    pub input_micros: Option<i64>,
    pub output_micros: Option<i64>,
    pub cache_creation_5m_micros: Option<i64>,
    pub cache_creation_1h_micros: Option<i64>,
    pub cache_read_micros: Option<i64>,
}

// ---------------------------------------------------------------------------
// Stage payloads: Parse
// ---------------------------------------------------------------------------

/// Information extracted by the body parser.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ParseInfo {
    pub path: String,
    /// `messages`, `models`, etc.
    pub method: String,
    /// Requested model, if the body specified one.
    pub model: Option<String>,
    /// Whether the client asked for a streaming response (`stream: true`).
    pub stream: bool,
    /// Byte length of the raw request body.
    pub body_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_control_block_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cache_breakpoints: Vec<RequestCacheBreakpoint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_prefix_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_index: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cache_control_message_indices: Vec<u64>,
}

/// Reason the body parser rejected the request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ParseFailure {
    /// Request body exceeded the configured cap.
    BodyTooLarge {
        limit_bytes: u64,
    },
    InvalidJson,
}

// ---------------------------------------------------------------------------
// Stage payloads: Auth
// ---------------------------------------------------------------------------

/// Result of successful authentication.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AuthInfo {
    pub principal_id: String,
    pub key_id: Option<String>,
    pub principal_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_ms: Option<u64>,
}

/// Reason authentication failed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AuthFailure {
    /// Token was missing, malformed, or did not match any known key.
    AuthenticationFailed {
        http_status: u16,
        /// Coarse reason label used by the api-key metrics subscriber to
        /// tag `cclb_key_auth_failures_total`. `None` when the caller
        /// cannot classify the failure.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    /// Token matched a key but the principal record was not found.
    PrincipalMissing { principal_id: String },
}

// ---------------------------------------------------------------------------
// Stage payloads: Route
// ---------------------------------------------------------------------------

/// Route selected by the router.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RouteInfo {
    pub upstream_id: Uuid,
    pub upstream_name: String,
    pub model: Option<String>,
    /// Pricing bucket for the chosen upstream. Values used by the pricing
    /// subscriber to select the correct cost model: `"anthropic_key"` or
    /// `"anthropic_oauth"`. `None` means the pricing default (Anthropic key).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing_trace: Option<RoutingTrace>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicted_cache_read_tokens: Option<u32>,
}

/// Reason routing failed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RouteFailure {
    /// Router pipeline could not be instantiated for this principal.
    RouterPipelineUnavailable,
    /// The filter chain returned zero candidates after evaluation.
    RouteNoUpstreamAfterFilter,
    /// No upstream is configured for the requested route.
    RouteNotConfigured,
}

// ---------------------------------------------------------------------------
// Stage payloads: LimitDecision
// ---------------------------------------------------------------------------

/// Whether the limit engine reserved capacity or rejected the request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "outcome", rename_all = "snake_case")]
#[non_exhaustive]
pub enum LimitDecisionKind {
    Reserved {
        reservation_id: String,
        amount: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit_reserve_ms: Option<u64>,
    },
    Rejected {
        reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subject: Option<LimitSubject>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        request_summary: Option<LimitRequestSummary>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        route_summary: Option<RouteSummary>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit_violation: Option<String>,
    },
}

/// Identity carried by `LimitDecisionKind::Rejected` for downstream audit
/// subscribers to reconstruct the legacy `AuditEntry`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LimitSubject {
    pub principal_id: String,
    pub key_id: String,
}

/// Request shape summary carried by `LimitDecisionKind::Rejected`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LimitRequestSummary {
    pub model: String,
    pub path: String,
    pub method: String,
}

/// Route summary carried by `LimitDecisionKind::Rejected`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RouteSummary {
    pub upstream_name: String,
}

// ---------------------------------------------------------------------------
// Stage payloads: Usage
// ---------------------------------------------------------------------------

/// Compact snapshot of the Anthropic usage counters observed so far.
///
/// Mirrors the fields in `cc_lb_engine::usage_parser::UsageCounts` that are
/// safe to broadcast to subscribers.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct UsageSnapshot {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_creation_input_tokens: u64,
    pub cache_creation_input_tokens_5m: u64,
    pub cache_creation_input_tokens_1h: u64,
    pub cache_read_input_tokens: u64,
    pub thinking_tokens: u64,
    pub web_search_requests: u64,
    pub web_fetch_requests: u64,
    pub service_tier: Option<String>,
    pub inference_geo: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iterations: Option<JsonValue>,
}

/// Which SSE frame produced the update.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum UsageSource {
    /// First-frame authoritative snapshot (`message_start`).
    MessageStart,
    /// Cumulative update (`message_delta.usage.*`).
    MessageDelta,
    /// Terminator (`message_stop`) boundary.
    MessageStop,
    /// `content_block_delta.thinking_delta.estimated_tokens` — deltas summed.
    ContentBlockDelta,
    /// Non-streaming JSON body top-level `usage.*`.
    NonStreamBody,
}

// ---------------------------------------------------------------------------
// Stage payloads: StreamCompleted
// ---------------------------------------------------------------------------

/// Stream ended normally.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct StreamSuccess {
    pub usage: UsageSnapshot,
    /// Number of SSE events observed on the tap.
    pub sse_event_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_chunk_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_body_chunk_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_message_start_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_content_block_start_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_first_content_delta_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_last_content_delta_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_message_stop_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_last_chunk_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_total_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_delta_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ping_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inter_token_avg_ms: Option<u64>,
}

/// Stream terminated on an error before completion (e.g. mid-stream
/// `event: error` payload from Anthropic).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamError {
    pub error_type: String,
    pub error_message: String,
}

// ---------------------------------------------------------------------------
// Stage payloads: RequestTerminated
// ---------------------------------------------------------------------------

/// Why the request terminated.
///
/// String `error_code` mirrors the pre-existing catalog in
/// `cc_lb_engine::terminal_observer::error_codes` so downstream consumers keep
/// working during the shadow-mode migration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum TerminationReason {
    /// Successful 2xx response.
    Success,
    /// Request was rejected or upstream returned an error. The string
    /// matches an entry in the `error_codes` catalog.
    ErrorCode(String),
    /// `LifecycleContext` was dropped without an explicit termination
    /// signal (usually an in-flight cancellation).
    Dropped,
}

impl TerminationReason {
    /// Static-label form for metrics without payload cardinality.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::ErrorCode(_) => "error_code",
            Self::Dropped => "dropped",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_event_id() -> EventId {
        "01978c00-0000-7000-8000-000000000000".to_owned()
    }

    #[test]
    fn request_started_roundtrip() {
        let event = LifecycleEvent::RequestStarted {
            event_id: sample_event_id(),
            request_id: "req-123".to_owned(),
            ts_ms: 1_730_000_000_000,
            stream: true,
        };
        let json = serde_json::to_string(&event).expect("serialize");
        let restored: LifecycleEvent = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(event, restored);
    }

    #[test]
    fn kind_labels_cover_every_variant() {
        // Anti-regression: if a new variant is added without updating kind()
        // the compile-checked match in kind() will fail. This runtime test
        // additionally locks the label strings that Prometheus depends on.
        let labels = [
            LifecycleEvent::RequestStarted {
                event_id: sample_event_id(),
                request_id: "r".into(),
                ts_ms: 0,
                stream: false,
            }
            .kind(),
            LifecycleEvent::ParseCompleted {
                event_id: sample_event_id(),
                result: Err(ParseFailure::BodyTooLarge { limit_bytes: 1 }),
            }
            .kind(),
            LifecycleEvent::AuthCompleted {
                event_id: sample_event_id(),
                result: Err(AuthFailure::AuthenticationFailed {
                    http_status: 401,
                    reason: None,
                }),
            }
            .kind(),
            LifecycleEvent::AuthenticationCompleted {
                event_id: sample_event_id(),
                principal_id: "principal".into(),
                principal_kind: PrincipalKindLite::Machine,
            }
            .kind(),
            LifecycleEvent::RouteCompleted {
                event_id: sample_event_id(),
                result: Err(RouteFailure::RouteNotConfigured),
                routing_trace: None,
            }
            .kind(),
            LifecycleEvent::LimitDecision {
                event_id: sample_event_id(),
                decision: LimitDecisionKind::Rejected {
                    reason: "quota".into(),
                    subject: None,
                    request_summary: None,
                    route_summary: None,
                    limit_violation: None,
                },
            }
            .kind(),
            LifecycleEvent::UpstreamAttempt {
                event_id: sample_event_id(),
                attempt_num: 1,
                upstream_id: Uuid::nil(),
            }
            .kind(),
            LifecycleEvent::UpstreamResponseStarted {
                event_id: sample_event_id(),
                status: 200,
                headers: HeaderSnapshot::default(),
                bulkhead_wait_ms: None,
                dns_ms: None,
                connect_ms: None,
                connection_reused: None,
                shape_ms: None,
                sign_ms: None,
                upstream_ttfb_ms: None,
            }
            .kind(),
            LifecycleEvent::ProviderErrorObserved {
                event_id: sample_event_id(),
                code: "provider_error".into(),
                message: "redacted".into(),
                source: "provider".into(),
            }
            .kind(),
            LifecycleEvent::UsageObserved {
                event_id: sample_event_id(),
                usage: UsageSnapshot::default(),
                source: UsageSource::MessageStart,
            }
            .kind(),
            LifecycleEvent::StreamCompleted {
                event_id: sample_event_id(),
                result: Ok(StreamSuccess::default()),
            }
            .kind(),
            LifecycleEvent::RequestTerminated {
                event_id: sample_event_id(),
                reason: TerminationReason::Success,
                client_status: 200,
                duration_ms: 1,
                limit_reconcile_ms: None,
                observability_post_ms: None,
                proxy_setup_ms: None,
                upstream_body_ms: None,
                first_body_chunk_ms: None,
                internal_errors: Vec::new(),
            }
            .kind(),
            LifecycleEvent::Priced {
                event_id: sample_event_id(),
                cost: CostBreakdown::default(),
            }
            .kind(),
            LifecycleEvent::CacheObserved {
                event_id: sample_event_id(),
                cache_state: RequestCacheState::Unknown,
            }
            .kind(),
            LifecycleEvent::PromptCacheObservationsProduced {
                event_id: sample_event_id(),
                upstream_id: Uuid::nil(),
                canonical_model_id: "model".to_owned(),
                observations: Vec::new(),
                dropped_below_threshold: 0,
                dropped_aborted: 0,
            }
            .kind(),
        ];
        assert_eq!(
            labels,
            [
                "request_started",
                "parse_completed",
                "auth_completed",
                "authentication_completed",
                "route_completed",
                "limit_decision",
                "upstream_attempt",
                "upstream_response_started",
                "provider_error_observed",
                "usage_observed",
                "stream_completed",
                "request_terminated",
                "priced",
                "cache_observed",
                "prompt_cache_observations_produced",
            ],
        );
    }

    #[test]
    fn event_id_accessor_returns_stable_reference() {
        let id = sample_event_id();
        let event = LifecycleEvent::RequestTerminated {
            event_id: id.clone(),
            reason: TerminationReason::Dropped,
            client_status: 0,
            duration_ms: 0,
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            upstream_body_ms: None,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
        };
        assert_eq!(event.event_id(), &id);
    }

    #[test]
    fn termination_reason_kind_is_stable() {
        assert_eq!(TerminationReason::Success.kind(), "success");
        assert_eq!(
            TerminationReason::ErrorCode("upstream_4xx".into()).kind(),
            "error_code",
        );
        assert_eq!(TerminationReason::Dropped.kind(), "dropped");
    }
}
