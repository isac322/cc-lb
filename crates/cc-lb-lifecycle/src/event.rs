use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    AuthFailure, AuthInfo, LimitDecisionKind, ParseFailure, ParseInfo, PromptCacheObservationWire,
    RouteFailure, RouteInfo, StreamError, StreamSuccess, TerminationReason, UsageSnapshot,
    UsageSource,
};

/// Application-generated unique identifier for a single request lifecycle.
///
/// Same value the `LifecycleContext` (successor of `TerminalObserver`)
/// generates as a UUID v7 string. Used as the DB row uniqueness key and
/// for cross-subscriber correlation.
pub type EventId = String;

/// Optional request-scoped setup timings. `serde(flatten)` keeps the eight
/// fields at the top level of the terminal lifecycle event while allowing
/// producers and fixtures to pass them as one value.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RequestSetupTimings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub json_parse_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_structure_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_token_key_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_count_lookup_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_tokenizer_queue_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_serialize_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_tokenize_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prepare_signer_ms: Option<f64>,
}

/// Optional request/response I/O observations collected across the request
/// lifecycle. Producers record only finite, nonnegative values. These are
/// nested under `io_timings` on the terminal lifecycle event; persisted
/// request-log DTOs expose the individual fields directly.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RequestIoTimings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_body_first_chunk_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_body_receive_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_body_wait_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_body_process_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_body_chunk_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_body_wait_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_body_process_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_body_downstream_poll_gap_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_overhead_ms: Option<f64>,
}

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
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source_kind: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source_ref_id: Option<String>,
        /// Endpoint classification resolved at the earliest request boundary
        /// (ingress path via `cc_lb_request_log::RequestEventKind::from_path`,
        /// or `Renewal` for scheduler keepalive publishes). `None` means the
        /// producer did not classify the request.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_kind: Option<cc_lb_request_log::RequestEventKind>,
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
        principal_kind: cc_lb_domain::PrincipalKindLite,
    },
    /// Router selection completed.
    RouteCompleted {
        event_id: EventId,
        result: Result<RouteInfo, RouteFailure>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        routing_trace: Option<cc_lb_domain::RoutingTrace>,
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
        #[serde(
            default,
            skip_serializing_if = "cc_lb_request_log::HeaderSnapshot::is_empty"
        )]
        headers: cc_lb_request_log::HeaderSnapshot,
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
    RequestLogUpstreamErrorObserved {
        event_id: EventId,
        error_type: String,
        error_message: String,
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
        request_body_read_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        request_body_bytes: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit_reconcile_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        observability_post_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        proxy_setup_ms: Option<u64>,
        #[serde(default, flatten)]
        setup_timings: RequestSetupTimings,
        #[serde(default)]
        io_timings: RequestIoTimings,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        upstream_body_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        first_body_chunk_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        finalize_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        internal_errors: Vec<cc_lb_domain::InternalError>,
        /// Endpoint classification mirrored from `RequestStarted` so the
        /// terminal event still categorizes the row when the start event is
        /// lost on the writer channel. `None` means the producer did not
        /// classify the request.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_kind: Option<cc_lb_request_log::RequestEventKind>,
    },
    /// Emitted by the pricing subscriber after cost is computed from usage
    /// counts, model, and upstream kind. The assembler merges these micros
    /// into the request row's cost columns.
    Priced {
        event_id: EventId,
        cost: cc_lb_request_log::CostBreakdown,
    },
    /// Emitted by the cache-observation subscriber when the final
    /// prompt-cache state can be derived from the usage counters plus
    /// parse-time cache metadata. The assembler merges this into the
    /// request row's `cache_state` column.
    CacheObserved {
        event_id: EventId,
        cache_state: cc_lb_request_log::RequestCacheState,
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
            | Self::RequestLogUpstreamErrorObserved { event_id, .. }
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
            Self::RequestLogUpstreamErrorObserved { .. } => "request_log_upstream_error_observed",
            Self::UsageObserved { .. } => "usage_observed",
            Self::StreamCompleted { .. } => "stream_completed",
            Self::RequestTerminated { .. } => "request_terminated",
            Self::Priced { .. } => "priced",
            Self::CacheObserved { .. } => "cache_observed",
            Self::PromptCacheObservationsProduced { .. } => "prompt_cache_observations_produced",
        }
    }
}
