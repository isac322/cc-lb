use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{RequestEvent, RequestEventUpstream};

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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
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
    /// Session identifier discovered during parsing. This non-terminal field is
    /// broadcast only in memory and is never persisted as a partial snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
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
    pub thinking_budget_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_prefix_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_v3_cache_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota_urgency_5h: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota_urgency_7d: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota_urgency_combined: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota_warning_multiplier: Option<f64>,

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FinalRequestEventUpdate {
    pub event: RequestEvent,
    pub cursor: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RequestEventPhase {
    Partial,
    Final,
}

impl RequestEventPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Partial => "partial",
            Self::Final => "final",
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "phase", content = "payload", rename_all = "snake_case")]
pub enum RequestEventUpdate {
    Partial(RequestEventPartial),
    Final(FinalRequestEventUpdate),
}

impl RequestEventUpdate {
    pub fn partial(snapshot: RequestEventPartial) -> Self {
        Self::Partial(snapshot)
    }

    pub fn final_(event: RequestEvent, cursor: u64) -> Self {
        Self::Final(FinalRequestEventUpdate { event, cursor })
    }

    pub fn phase(&self) -> RequestEventPhase {
        match self {
            Self::Partial(_) => RequestEventPhase::Partial,
            Self::Final(_) => RequestEventPhase::Final,
        }
    }

    pub fn is_final(&self) -> bool {
        matches!(self, Self::Final(_))
    }

    pub fn event_id(&self) -> &str {
        match self {
            Self::Partial(snapshot) => &snapshot.event_id,
            Self::Final(FinalRequestEventUpdate { event, .. }) => {
                event.event_id.as_deref().unwrap_or("")
            }
        }
    }
}
