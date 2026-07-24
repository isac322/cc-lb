use serde::Serialize;
use uuid::Uuid;

use crate::storage_types_common::RequestEventStreamFilters;
use cc_lb_request_log::RequestEventUpstream;

/// Slim projection of a [`cc_lb_request_log::RequestEvent`] limited to the
/// fields the admin request-event LIST view displays. Backends select and
/// decode only these columns for a list page instead of the full row
/// (including large `payload`/body columns), so paginated/polled list reads
/// stop paying the full-event decode cost. The single-item DETAIL path still
/// returns the full event byte-identically via `RequestEventStore::get_request_event`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RequestEventListItem {
    pub ts: u64,
    pub ts_ms: Option<u64>,
    pub request_id: String,
    pub event_id: Option<String>,
    pub source_kind: Option<String>,
    pub principal_id: Option<String>,
    pub upstream: Option<RequestEventUpstream>,
    pub upstream_id: Option<Uuid>,
    pub upstream_name: Option<String>,
    pub thread_id: Option<String>,
    pub claude_agent_id: Option<String>,
    pub claude_parent_agent_id: Option<String>,
    pub claude_auxiliary_kind: Option<String>,
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
    pub thinking_budget_tokens: Option<u64>,
    pub thinking_tokens: Option<u64>,
    pub service_tier: Option<String>,
    pub status: u16,
    pub error_code: Option<String>,
    pub upstream_error_type: Option<String>,
    pub upstream_error_message: Option<String>,
    pub duration_ms: u64,
    pub auth_ms: Option<u64>,
    pub route_ms: Option<u64>,
    pub limit_reserve_ms: Option<u64>,
    pub bulkhead_wait_ms: Option<u64>,
    pub dns_ms: Option<u64>,
    pub connect_ms: Option<u64>,
    pub connection_reused: Option<bool>,
    pub limit_reconcile_ms: Option<u64>,
    pub observability_post_ms: Option<u64>,
    pub proxy_setup_ms: Option<u64>,
    pub shape_ms: Option<u64>,
    pub sign_ms: Option<u64>,
    pub upstream_ttfb_ms: Option<u64>,
    pub upstream_body_ms: Option<u64>,
    pub stream_first_content_delta_ms: Option<u64>,
    pub stream_last_content_delta_ms: Option<u64>,
    pub inter_token_avg_ms: Option<u64>,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cache_creation_input_tokens: Option<u64>,
    pub cache_creation_input_tokens_5m: Option<u64>,
    pub cache_creation_input_tokens_1h: Option<u64>,
    pub cache_read_input_tokens: Option<u64>,
    pub cost_usd_micros: Option<i64>,
    pub cost_input_micros: Option<i64>,
    pub cost_output_micros: Option<i64>,
    pub cost_cache_creation_5m_micros: Option<i64>,
    pub cost_cache_creation_1h_micros: Option<i64>,
    pub cost_cache_read_micros: Option<i64>,
}

/// Cursor + limit + filter parameters for a request-event list page,
/// evaluated entirely in SQL by backend implementations of
/// `RequestEventStore::list_request_events`. Mirrors the semantics of
/// `cc_lb_admin::events::RecentEventsParams` / `before_recent_cursor` /
/// `compare_recent_events_desc` exactly: results are ordered by
/// `(ts_ms ?? ts*1000, event_id ?? request_id)` descending, and
/// `until_ts_ms`/`until_event_id` form an exclusive compound cursor into
/// that same ordering.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RequestEventListQuery {
    pub since_unix_secs: u64,
    pub until_unix_secs: u64,
    pub until_ts_ms: Option<u64>,
    pub until_event_id: Option<String>,
    pub limit: usize,
    pub filters: RequestEventStreamFilters,
    /// Source-kind filter for the list view:
    /// - `None` excludes `renewal` rows (the default admin request-log view)
    /// - `Some("all")` includes every source kind
    /// - `Some(kind)` returns only rows whose `source_kind` equals `kind`
    pub source_kind: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestEventKeyLastUsedQuery {
    pub principal_id: String,
    pub since_unix_secs: u64,
    pub until_unix_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestEventKeyLastUsed {
    pub key_id: String,
    pub last_used_at_unix_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestEventKeyUsageQuery {
    pub principal_id: String,
    pub key_id: String,
    pub range_start_ms: u64,
    pub range_end_ms: u64,
    pub step_ms: u64,
    pub bucket_count: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RequestEventKeyUsageBucket {
    pub bucket_start_unix_secs: u64,
    pub request_count: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cost_usd_micros: i64,
}
