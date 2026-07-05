use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackendKind {
    Postgres,
    Sqlite,
}

impl BackendKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::Sqlite => "sqlite",
        }
    }
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
    pub payload: Option<Value>,
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
    /// handle entry → attempt() entry (auth + route + ctx).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_setup_ms: Option<u64>,
    /// shape_request execution (dialect + wasm shape plugin).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shape_ms: Option<u64>,
    /// sign_request execution (signer + OAuth refresh).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sign_ms: Option<u64>,
    /// dispatcher.dispatch() resolves when response HEADERS arrive (true TTFB).
    /// Includes TCP/TLS connect, request send, network RTT, upstream processing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_ttfb_ms: Option<u64>,
    /// body.collect() time after headers (response body download).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_body_ms: Option<u64>,
    /// Non-stream: time from headers to first body byte. Stream: time from relay start to first chunk.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_body_chunk_ms: Option<u64>,
    /// Non-stream: number of body chunks; stream: number of stream frames received.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_chunk_count: Option<u64>,
    /// Response body byte count (non-stream collect or stream total bytes relayed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_bytes: Option<u64>,
    /// SSE only: time from relay start to `event: message_start`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_message_start_ms: Option<u64>,
    /// SSE only: time from relay start to first `event: content_block_start`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_content_block_start_ms: Option<u64>,
    /// SSE only: time from relay start to first `event: content_block_delta` (= TTFT).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_first_content_delta_ms: Option<u64>,
    /// SSE only: time from relay start to last `event: content_block_delta`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_last_content_delta_ms: Option<u64>,
    /// SSE only: time from relay start to `event: message_stop`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_message_stop_ms: Option<u64>,
    /// SSE only: time from relay start to last byte received.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_last_chunk_ms: Option<u64>,
    /// SSE only: total relay duration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_total_ms: Option<u64>,
    /// SSE only: total parsed SSE events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sse_event_count: Option<u64>,
    /// SSE only: count of `content_block_delta` events (≈ token chunks).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_delta_count: Option<u64>,
    /// SSE only: count of `ping` keepalive events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ping_count: Option<u64>,
    /// SSE only: (last_content_delta − first_content_delta) / (content_delta_count − 1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inter_token_avg_ms: Option<u64>,
    pub error_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing_trace: Option<cc_lb_plugin_api::RoutingTrace>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub internal_errors: Vec<cc_lb_plugin_api::InternalError>,
    /// Application-generated unique row identifier (UUID v7).
    ///
    /// Used as the DB uniqueness key in `request_events_v1`. Distinct from
    /// [`request_id`](Self::request_id), which is a client-visible correlation
    /// header forwarded to/from Anthropic and exposed to PDK plugins, and
    /// therefore intentionally NOT guaranteed unique.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    /// Output tokens spent on extended-thinking content (subset of
    /// [`output_tokens`](Self::output_tokens)).
    /// Source: `usage.output_tokens_details.thinking_tokens`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking_tokens: Option<u64>,
    /// Anthropic web search tool invocations (billed separately from tokens).
    /// Source: `usage.server_tool_use.web_search_requests`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub web_search_requests: Option<u64>,
    /// Anthropic web fetch tool invocations (billed separately from tokens).
    /// Source: `usage.server_tool_use.web_fetch_requests`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub web_fetch_requests: Option<u64>,
    /// Service tier reported by Anthropic
    /// (`standard` / `priority` / `batch`). Source: `usage.service_tier`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<String>,
    /// Geographic region of inference. Source: `usage.inference_geo`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inference_geo: Option<String>,
    /// Upstream stream `error` event type (e.g. `overloaded_error`).
    /// Set when Anthropic emits a mid-stream `event: error`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_error_type: Option<String>,
    /// Upstream stream `error` event message (sanitized + truncated to a
    /// safe length).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_error_message: Option<String>,
    /// Opaque preservation of the beta `usage.iterations[]` array (server-side
    /// fallback `fallback-credit-2026-06-01`). Stored only in `payload`; no
    /// top-level column. Per-iteration model + token breakdown is preserved
    /// verbatim for forward compatibility.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iterations: Option<Value>,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

/// In-flight snapshot of an active request, emitted by the lifecycle event
/// assembler while the request is still executing (before finalization).
///
/// The dashboard uses partials to render live request rows that update as the
/// request progresses (streaming, incremental usage, live cost estimate).
/// Partials are **memory-only**: they are not persisted to storage. Only the
/// final [`RequestEvent`] row is durable.
///
/// Fields are a strict subset of [`RequestEvent`] limited to what is
/// meaningful before the request finalizes. Terminal-only fields (final
/// `status`, `duration_ms`, `error_code`, `routing_trace`, `internal_errors`,
/// `iterations`, `cache_breakpoints`, and detailed stream timing) are
/// intentionally excluded to keep partial payloads compact.
///
/// See `.omo/plans/dashboard-live-tail-redesign.md` §3.4 for the field
/// classification rationale.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RequestEventPartial {
    pub event_id: String,
    pub request_id: String,
    pub ts: u64,
    pub ts_ms: u64,
    /// Millisecond timestamp of the most recent lifecycle event merged into
    /// this snapshot. Client uses this for orphan-eviction and freshness UX.
    pub last_update_ms: u64,
    /// Milliseconds since `ts_ms`. Rendered as the running duration in the
    /// dashboard until finalization replaces it with the exact `duration_ms`.
    pub elapsed_ms: u64,
    pub stream: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_id: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream: Option<RequestEventUpstream>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// HTTP status observed from upstream response headers, if received.
    /// Distinct from final client-visible `status` on [`RequestEvent`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_response_status: Option<u16>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
    pub cache_control_block_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_prefix_hash: Option<String>,

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
    pub shape_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sign_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_ttfb_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_body_chunk_ms: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RequestEventStreamFilters {
    pub principal_id: Option<String>,
    pub model: Option<String>,
    pub upstream: Option<RequestEventUpstream>,
    pub upstream_id: Option<Uuid>,
    pub status_class: Option<StatusClass>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusClass {
    TwoXx,
    ThreeXx,
    FourXx,
    FiveXx,
}

impl StatusClass {
    pub fn matches(self, status: u16) -> bool {
        match self {
            Self::TwoXx => (200..=299).contains(&status),
            Self::ThreeXx => (300..=399).contains(&status),
            Self::FourXx => (400..=499).contains(&status),
            Self::FiveXx => (500..=599).contains(&status),
        }
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BucketKind {
    Requests,
    InputTokens,
    OutputTokens,
}

impl BucketKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Requests => "requests",
            Self::InputTokens => "input_tokens",
            Self::OutputTokens => "output_tokens",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalLimitKind {
    Requests,
    Tokens,
    InputTokens,
    OutputTokens,
}

impl PrincipalLimitKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Requests => "requests",
            Self::Tokens => "tokens",
            Self::InputTokens => "input_tokens",
            Self::OutputTokens => "output_tokens",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalLimitIdentityKind {
    Account,
    Credential,
    Unobserved,
}

impl PrincipalLimitIdentityKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Account => "account",
            Self::Credential => "credential",
            Self::Unobserved => "unobserved",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrincipalLimitState {
    pub principal_id: String,
    pub identity_kind: PrincipalLimitIdentityKind,
    pub identity_value: Option<String>,
    pub account_observed: bool,
    pub window: String,
    pub kind: PrincipalLimitKind,
    pub limit: Option<u64>,
    pub remaining: Option<u64>,
    pub reset: Option<String>,
    pub observed_at_unix_secs: u64,
    pub stored_at_unix_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ConfigDraftState {
    pub draft: Option<Value>,
    pub revision: u64,
    pub last_validated_revision: Option<u64>,
    pub last_validation_error: Option<String>,
    pub saved_at_unix_secs: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistorySummary {
    pub upstreams: usize,
    pub principals: usize,
    pub plugin_count: usize,
    pub tls_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub revision: u64,
    pub config_toml: String,
    pub applied_at_unix_secs: u64,
    pub summary: HistorySummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredHistoryEntry {
    pub config_toml: String,
    pub applied_at_unix_secs: u64,
    pub summary: HistorySummary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageRollupResolution {
    Minute,
    Hour,
}

impl UsageRollupResolution {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Minute => "minute",
            Self::Hour => "hour",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct UsageRollupKey {
    pub resolution: UsageRollupResolution,
    pub bucket_start: u64,
    pub principal: String,
    pub upstream_id: Uuid,
    pub upstream_name: String,
    pub model: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageRollup {
    pub resolution: UsageRollupResolution,
    pub bucket_start: u64,
    pub principal: String,
    pub upstream_id: Uuid,
    pub upstream_name: String,
    pub model: String,
    pub request_count: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    #[serde(default)]
    pub cache_creation_input_tokens: u64,
    #[serde(default)]
    pub cache_read_input_tokens: u64,
    pub error_count: u64,
    pub latency_count: u64,
    pub latency_ms_sum: u64,
    pub latency_ms_min: Option<u64>,
    pub latency_ms_max: Option<u64>,
    #[serde(default)]
    pub proxy_setup_ms_count: u64,
    #[serde(default)]
    pub proxy_setup_ms_sum: u64,
    #[serde(default)]
    pub shape_ms_count: u64,
    #[serde(default)]
    pub shape_ms_sum: u64,
    #[serde(default)]
    pub sign_ms_count: u64,
    #[serde(default)]
    pub sign_ms_sum: u64,
    #[serde(default)]
    pub upstream_ttfb_ms_count: u64,
    #[serde(default)]
    pub upstream_ttfb_ms_sum: u64,
    #[serde(default)]
    pub upstream_body_ms_count: u64,
    #[serde(default)]
    pub upstream_body_ms_sum: u64,
    pub virtual_cost_micros: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageRollupRun {
    pub processed_events: u64,
    pub updated_rollups: u64,
    pub checkpoint: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OAuthCredentials {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at: u64,
    pub scopes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnthropicApiKeyCredential {
    pub anthropic_api_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IssuedKey {
    pub key_id: String,
    pub plaintext: String,
    pub issued_at_unix_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiKeyRecord {
    pub key_id: String,
    pub label: Option<String>,
    pub issued_at_unix_secs: u64,
    pub revoked_at_unix_secs: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum UpstreamKind {
    #[default]
    AnthropicKey,
    AnthropicOAuth,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Limit {
    pub kind: LimitKind,
    pub window_secs: u64,
    pub cap_micros: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum KeyStatus {
    #[default]
    Active,
    Disabled,
    Revoked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKindLite {
    Human,
    #[default]
    Machine,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StoredApiKeyRecord {
    pub label: String,
    pub issued_at_unix_secs: u64,
    pub revoked_at_unix_secs: Option<u64>,
    pub key_hash_b64: String,
    pub verify_hash: [u8; 32],
    pub secret_salt: [u8; 16],
    pub upstream_kind: UpstreamKind,
    pub limit_overrides: Vec<Limit>,
    pub status: KeyStatus,
    pub expires_at_unix_secs: Option<u64>,
    pub last_4: String,
    pub description: Option<String>,
    pub principal_kind: PrincipalKindLite,
    pub index_hash: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredApiKeyRecordV1 {
    label: String,
    issued_at_unix_secs: u64,
    revoked_at_unix_secs: Option<u64>,
    key_hash_b64: String,
    verify_hash: [u8; 32],
    secret_salt: [u8; 16],
    upstream_kind: UpstreamKind,
    limit_overrides: Vec<Limit>,
    status: KeyStatus,
    expires_at_unix_secs: Option<u64>,
    last_4: String,
    description: Option<String>,
    principal_kind: PrincipalKindLite,
    index_hash: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredApiKeyRecordV0 {
    label: String,
    issued_at_unix_secs: u64,
    revoked_at_unix_secs: Option<u64>,
    key_hash_b64: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum StoredApiKeyRecordWire {
    V0(StoredApiKeyRecordV0),
    V1(StoredApiKeyRecordV1),
}

impl From<&StoredApiKeyRecord> for StoredApiKeyRecordV1 {
    fn from(value: &StoredApiKeyRecord) -> Self {
        Self {
            label: value.label.clone(),
            issued_at_unix_secs: value.issued_at_unix_secs,
            revoked_at_unix_secs: value.revoked_at_unix_secs,
            key_hash_b64: value.key_hash_b64.clone(),
            verify_hash: value.verify_hash,
            secret_salt: value.secret_salt,
            upstream_kind: value.upstream_kind,
            limit_overrides: value.limit_overrides.clone(),
            status: value.status,
            expires_at_unix_secs: value.expires_at_unix_secs,
            last_4: value.last_4.clone(),
            description: value.description.clone(),
            principal_kind: value.principal_kind,
            index_hash: value.index_hash,
        }
    }
}

impl Serialize for StoredApiKeyRecord {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        StoredApiKeyRecordWire::V1(StoredApiKeyRecordV1::from(self)).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for StoredApiKeyRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match StoredApiKeyRecordWire::deserialize(deserializer)? {
            StoredApiKeyRecordWire::V0(value) => Ok(Self {
                label: value.label,
                issued_at_unix_secs: value.issued_at_unix_secs,
                revoked_at_unix_secs: value.revoked_at_unix_secs,
                key_hash_b64: value.key_hash_b64,
                ..Default::default()
            }),
            StoredApiKeyRecordWire::V1(value) => Ok(Self {
                label: value.label,
                issued_at_unix_secs: value.issued_at_unix_secs,
                revoked_at_unix_secs: value.revoked_at_unix_secs,
                key_hash_b64: value.key_hash_b64,
                verify_hash: value.verify_hash,
                secret_salt: value.secret_salt,
                upstream_kind: value.upstream_kind,
                limit_overrides: value.limit_overrides,
                status: value.status,
                expires_at_unix_secs: value.expires_at_unix_secs,
                last_4: value.last_4,
                description: value.description,
                principal_kind: value.principal_kind,
                index_hash: value.index_hash,
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IssueParams {
    pub label: String,
    pub description: Option<String>,
    pub upstream_kind: UpstreamKind,
    pub expires_at_unix_secs: Option<u64>,
    pub limit_overrides: Vec<Limit>,
    pub secret_salt: [u8; 16],
    pub verify_hash: [u8; 32],
    pub last_4: String,
    pub principal_kind: PrincipalKindLite,
    pub index_hash: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ApiKeyMutation {
    pub label: Option<String>,
    pub description: Option<Option<String>>,
    pub expires_at_unix_secs: Option<Option<u64>>,
    pub limit_overrides: Option<Vec<Limit>>,
    pub status: Option<KeyStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceCatalogSnapshotRecord {
    pub json_bytes: Vec<u8>,
    pub fetched_at_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_event_serde_round_trips_new_optional_fields() {
        let original = RequestEvent {
            request_id: "req_round_trip".to_owned(),
            cache_creation_input_tokens: Some(700),
            cache_creation_input_tokens_5m: Some(400),
            cache_creation_input_tokens_1h: Some(300),
            cache_read_input_tokens: Some(200),
            cost_usd_micros: Some(987_654),
            cost_input_micros: Some(369_000),
            cost_output_micros: Some(675_000),
            cost_cache_creation_5m_micros: Some(150_000),
            cost_cache_creation_1h_micros: Some(180_000),
            cost_cache_read_micros: Some(60_000),
            ..Default::default()
        };

        let bytes = serde_json::to_vec(&original).expect("serialize");
        let parsed: RequestEvent = serde_json::from_slice(&bytes).expect("deserialize");

        assert_eq!(parsed, original);
    }

    #[test]
    fn request_event_serde_old_json_missing_new_fields_yields_none() {
        let old_json = serde_json::json!({
            "ts": 1_700_000_000u64,
            "request_id": "req_old",
            "principal_id": null,
            "principal_kind": null,
            "upstream": null,
            "model": null,
            "status": 200u16,
            "input_tokens": null,
            "output_tokens": null,
            "cache_creation_input_tokens": 1600u64,
            "cache_read_input_tokens": 0u64,
            "cost_usd_micros": 12_345i64,
            "duration_ms": 100u64,
        });

        let parsed: RequestEvent = serde_json::from_value(old_json).expect("deserialize old shape");

        assert_eq!(parsed.cache_creation_input_tokens, Some(1600));
        assert_eq!(parsed.cache_creation_input_tokens_5m, None);
        assert_eq!(parsed.cache_creation_input_tokens_1h, None);
        assert_eq!(parsed.cost_input_micros, None);
        assert_eq!(parsed.cost_output_micros, None);
        assert_eq!(parsed.cost_cache_creation_5m_micros, None);
        assert_eq!(parsed.cost_cache_creation_1h_micros, None);
        assert_eq!(parsed.cost_cache_read_micros, None);
    }

    #[test]
    fn request_event_serializes_new_latency_fields_round_trip() {
        let event = RequestEvent {
            auth_ms: Some(50),
            route_ms: Some(30),
            limit_reserve_ms: Some(10),
            bulkhead_wait_ms: Some(3),
            dns_ms: Some(12),
            connect_ms: Some(85),
            connection_reused: Some(false),
            limit_reconcile_ms: Some(15),
            observability_post_ms: Some(20),
            ..RequestEvent::default()
        };
        let json = serde_json::to_string(&event).expect("serializes");
        let decoded: RequestEvent = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(decoded.auth_ms, Some(50));
        assert_eq!(decoded.route_ms, Some(30));
        assert_eq!(decoded.limit_reserve_ms, Some(10));
        assert_eq!(decoded.bulkhead_wait_ms, Some(3));
        assert_eq!(decoded.dns_ms, Some(12));
        assert_eq!(decoded.connect_ms, Some(85));
        assert_eq!(decoded.connection_reused, Some(false));
        assert_eq!(decoded.limit_reconcile_ms, Some(15));
        assert_eq!(decoded.observability_post_ms, Some(20));
    }

    #[test]
    fn request_event_decodes_old_event_without_new_fields_yields_none() {
        let old_json =
            r#"{"ts": 1700000000, "request_id": "test", "status": 200, "duration_ms": 100}"#;
        let decoded: RequestEvent = serde_json::from_str(old_json).expect("deserializes old shape");
        assert_eq!(decoded.auth_ms, None);
        assert_eq!(decoded.route_ms, None);
        assert_eq!(decoded.limit_reserve_ms, None);
        assert_eq!(decoded.bulkhead_wait_ms, None);
        assert_eq!(decoded.dns_ms, None);
        assert_eq!(decoded.connect_ms, None);
        assert_eq!(decoded.connection_reused, None);
        assert_eq!(decoded.limit_reconcile_ms, None);
        assert_eq!(decoded.observability_post_ms, None);
    }

    #[test]
    fn request_event_serde_round_trips_terminal_observation_fields() {
        let iterations = serde_json::json!([
            {
                "type": "message",
                "input_tokens": 100,
                "output_tokens": 200,
                "model": "claude-x"
            }
        ]);
        let original = RequestEvent {
            request_id: "req_t1".to_owned(),
            event_id: Some("0193f76b-1ab2-7a4d-8a3c-44ab3c5e1f0a".to_owned()),
            thinking_tokens: Some(64),
            web_search_requests: Some(3),
            web_fetch_requests: Some(1),
            service_tier: Some("priority".to_owned()),
            inference_geo: Some("us-east".to_owned()),
            upstream_error_type: Some("overloaded_error".to_owned()),
            upstream_error_message: Some("upstream overloaded; retry".to_owned()),
            iterations: Some(iterations.clone()),
            ..Default::default()
        };

        let bytes = serde_json::to_vec(&original).expect("serialize");
        let parsed: RequestEvent = serde_json::from_slice(&bytes).expect("deserialize");
        assert_eq!(parsed, original);

        // Legacy payload without any of the new fields still deserializes with None.
        let legacy = serde_json::json!({
            "ts": 1_700_000_000u64,
            "request_id": "req_legacy",
            "status": 200u16,
            "duration_ms": 100u64,
        });
        let parsed_legacy: RequestEvent =
            serde_json::from_value(legacy).expect("deserialize legacy");
        assert_eq!(parsed_legacy.event_id, None);
        assert_eq!(parsed_legacy.thinking_tokens, None);
        assert_eq!(parsed_legacy.web_search_requests, None);
        assert_eq!(parsed_legacy.web_fetch_requests, None);
        assert_eq!(parsed_legacy.service_tier, None);
        assert_eq!(parsed_legacy.inference_geo, None);
        assert_eq!(parsed_legacy.upstream_error_type, None);
        assert_eq!(parsed_legacy.upstream_error_message, None);
        assert_eq!(parsed_legacy.iterations, None);
    }

    #[test]
    fn request_event_warm_pool_invariant_preserved_through_round_trip() {
        let event = RequestEvent {
            connection_reused: Some(true),
            dns_ms: None,
            connect_ms: None,
            ..RequestEvent::default()
        };
        let json = serde_json::to_string(&event).expect("serializes");
        assert!(!json.contains("dns_ms"));
        assert!(!json.contains("connect_ms"));
        let decoded: RequestEvent = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(decoded.connection_reused, Some(true));
        assert_eq!(decoded.dns_ms, None);
        assert_eq!(decoded.connect_ms, None);
    }
}
