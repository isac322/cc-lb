//! Lifecycle event vocabulary for the cc-lb request pipeline.
//!
//! This crate defines [`LifecycleEvent`], the fixed set of typed events every
//! request emits over the [`RequestEventBus`](../cc-lb-core/index.html)
//! lifecycle stream. Subscribers (Admin SSE, Pricing, CacheObservation,
//! ObservabilityHook adapter, RequestEventAssembler) correlate events by
//! [`EventId`] and materialise the persisted `request_events_v1` row.
//!
//! # Design goals
//!
//! - **Types-only crate.** No runtime, no I/O, no async. Purely serde-ready
//!   structs and enums. Both `cc-lb-core` (producer) and `cc-lb-admin`
//!   (SSE consumer) depend on it without pulling in async or observability.
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
//! - `cc_lb_core::LifecycleContext` (producer)
//! - `cc_lb_core::event_bus::RequestEventBus::publish_lifecycle` (transport)

#![deny(missing_debug_implementations)]

use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
    /// Router selection completed.
    RouteCompleted {
        event_id: EventId,
        result: Result<RouteInfo, RouteFailure>,
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
    UpstreamResponseStarted { event_id: EventId, status: u16 },
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
    },
    /// Emitted by the Phase-5 pricing subscriber after cost is computed
    /// from usage counts + model + upstream kind. Advisory: the assembler
    /// merges these micros into the shadow row.
    Priced {
        event_id: EventId,
        cost: CostBreakdown,
    },
    /// Emitted by the Phase-5 cache observation subscriber when the final
    /// prompt-cache state can be derived from the usage counters plus
    /// parse-time cache metadata. Advisory: the assembler merges this into
    /// the shadow row's `cache_state` column.
    CacheObserved {
        event_id: EventId,
        cache_state: RequestCacheStateLite,
    },
}

impl LifecycleEvent {
    /// Extract the correlation ID.
    pub fn event_id(&self) -> &EventId {
        match self {
            Self::RequestStarted { event_id, .. }
            | Self::ParseCompleted { event_id, .. }
            | Self::AuthCompleted { event_id, .. }
            | Self::RouteCompleted { event_id, .. }
            | Self::LimitDecision { event_id, .. }
            | Self::UpstreamAttempt { event_id, .. }
            | Self::UpstreamResponseStarted { event_id, .. }
            | Self::UsageObserved { event_id, .. }
            | Self::StreamCompleted { event_id, .. }
            | Self::RequestTerminated { event_id, .. }
            | Self::Priced { event_id, .. }
            | Self::CacheObserved { event_id, .. } => event_id,
        }
    }

    /// Static label suitable for Prometheus metric cardinality.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::RequestStarted { .. } => "request_started",
            Self::ParseCompleted { .. } => "parse_completed",
            Self::AuthCompleted { .. } => "auth_completed",
            Self::RouteCompleted { .. } => "route_completed",
            Self::LimitDecision { .. } => "limit_decision",
            Self::UpstreamAttempt { .. } => "upstream_attempt",
            Self::UpstreamResponseStarted { .. } => "upstream_response_started",
            Self::UsageObserved { .. } => "usage_observed",
            Self::StreamCompleted { .. } => "stream_completed",
            Self::RequestTerminated { .. } => "request_terminated",
            Self::Priced { .. } => "priced",
            Self::CacheObserved { .. } => "cache_observed",
        }
    }
}

/// Sanitized subset of upstream response headers carried by
/// `LifecycleEvent::UpstreamResponseStarted` (RFC-0002 §237-241).
/// Only compact, non-sensitive header slots are included; producers MUST
/// NOT copy `Authorization` or similar credential-bearing headers.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeaderSnapshot {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ratelimit_requests_remaining: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ratelimit_tokens_remaining: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ratelimit_reset: Option<String>,
}

impl HeaderSnapshot {
    pub fn is_empty(&self) -> bool {
        self.content_type.is_none()
            && self.request_id.is_none()
            && self.ratelimit_requests_remaining.is_none()
            && self.ratelimit_tokens_remaining.is_none()
            && self.ratelimit_reset.is_none()
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
    pub cache_breakpoints: Vec<CacheBreakpointLite>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_prefix_hash: Option<String>,
}

/// Snake-case string mirror of `cc_lb_storage_api::types::RequestCacheState`.
/// Duplicated in `cc-lb-lifecycle` so this crate stays free of `cc-lb-storage-api`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestCacheStateLite {
    Hit,
    Write,
    Refresh,
    Miss,
    None,
    Unknown,
}

impl RequestCacheStateLite {
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

/// Snake-case string mirror of `cc_lb_storage_api::types::RequestCacheBreakpointSource`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheBreakpointSourceLite {
    System,
    Tools,
    Message,
}

/// Lightweight mirror of `cc_lb_storage_api::types::RequestCacheBreakpoint`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheBreakpointLite {
    pub block_index: u64,
    pub source: CacheBreakpointSourceLite,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_index: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl: Option<String>,
    pub prefix_hash: String,
    #[serde(default, skip_serializing_if = "cache_breakpoint_token_count_is_zero")]
    pub prefix_token_count: u64,
}

fn cache_breakpoint_token_count_is_zero(value: &u64) -> bool {
    *value == 0
}

/// Reason the body parser rejected the request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ParseFailure {
    /// Request body exceeded the configured cap.
    BodyTooLarge { limit_bytes: u64 },
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
}

/// Reason authentication failed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AuthFailure {
    /// Token was missing, malformed, or did not match any known key.
    AuthenticationFailed { http_status: u16 },
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
    Reserved { reservation_id: String, amount: u64 },
    Rejected { reason: String },
}

// ---------------------------------------------------------------------------
// Stage payloads: Usage
// ---------------------------------------------------------------------------

/// Compact snapshot of the Anthropic usage counters observed so far.
///
/// Mirrors the fields in `cc_lb_core::usage_parser::UsageCounts` that are
/// safe to broadcast to subscribers.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamSuccess {
    pub usage: UsageSnapshot,
    /// Number of SSE events observed on the tap.
    pub sse_event_count: u64,
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
/// `cc_lb_core::terminal_observer::error_codes` so downstream consumers keep
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
                result: Err(AuthFailure::AuthenticationFailed { http_status: 401 }),
            }
            .kind(),
            LifecycleEvent::RouteCompleted {
                event_id: sample_event_id(),
                result: Err(RouteFailure::RouteNotConfigured),
            }
            .kind(),
            LifecycleEvent::LimitDecision {
                event_id: sample_event_id(),
                decision: LimitDecisionKind::Rejected {
                    reason: "quota".into(),
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
                result: Ok(StreamSuccess {
                    usage: UsageSnapshot::default(),
                    sse_event_count: 0,
                }),
            }
            .kind(),
            LifecycleEvent::RequestTerminated {
                event_id: sample_event_id(),
                reason: TerminationReason::Success,
                client_status: 200,
                duration_ms: 1,
            }
            .kind(),
            LifecycleEvent::Priced {
                event_id: sample_event_id(),
                cost: CostBreakdown::default(),
            }
            .kind(),
            LifecycleEvent::CacheObserved {
                event_id: sample_event_id(),
                cache_state: RequestCacheStateLite::Unknown,
            }
            .kind(),
        ];
        assert_eq!(
            labels,
            [
                "request_started",
                "parse_completed",
                "auth_completed",
                "route_completed",
                "limit_decision",
                "upstream_attempt",
                "upstream_response_started",
                "usage_observed",
                "stream_completed",
                "request_terminated",
                "priced",
                "cache_observed",
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
