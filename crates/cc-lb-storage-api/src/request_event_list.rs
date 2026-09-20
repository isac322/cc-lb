use serde::Serialize;
use uuid::Uuid;

use crate::storage_types_common::RequestEventStreamFilters;
use cc_lb_request_log::{RequestEventKind, RequestEventUpstream};

/// Slim projection of a [`cc_lb_request_log::RequestEvent`] limited to the
/// fields the admin request-event LIST view displays. Backends may populate
/// these fields from materialized columns or decode the stored payload once
/// after selecting a bounded page; the DETAIL path still returns the full
/// event byte-identically via `RequestEventStore::get_request_event`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RequestEventListItem {
    pub ts: u64,
    pub ts_ms: Option<u64>,
    pub request_id: String,
    pub event_id: Option<String>,
    pub source_kind: Option<String>,
    /// Endpoint classification; `None` on rows persisted before the
    /// `event_kind` column existed (historical `unclassified`).
    pub event_kind: Option<RequestEventKind>,
    pub principal_id: Option<String>,
    pub upstream: Option<RequestEventUpstream>,
    pub upstream_id: Option<Uuid>,
    pub upstream_name: Option<String>,
    pub thread_id: Option<String>,
    pub observed_session_id: Option<String>,
    pub request_kind: Option<String>,
    pub claude_agent_id: Option<String>,
    pub claude_parent_agent_id: Option<String>,
    pub parent_session_id: Option<String>,
    pub client_app: Option<String>,
    pub session_id_source: Option<String>,
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
    pub request_body_read_ms: Option<u64>,
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
    pub request_body_bytes: Option<u64>,
    pub auth_ms: Option<u64>,
    pub route_ms: Option<u64>,
    pub limit_reserve_ms: Option<u64>,
    pub json_parse_ms: Option<f64>,
    pub cache_structure_ms: Option<f64>,
    pub cache_token_key_ms: Option<f64>,
    pub cache_count_lookup_ms: Option<f64>,
    pub cache_tokenizer_queue_ms: Option<f64>,
    pub cache_serialize_ms: Option<f64>,
    pub cache_tokenize_ms: Option<f64>,
    pub prepare_signer_ms: Option<f64>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_body_wait_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_body_process_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_body_downstream_poll_gap_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_overhead_ms: Option<f64>,
    pub finalize_ms: Option<u64>,
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
    /// - `None` excludes `renewal` rows (the default admin request-log view),
    ///   unless `filters.event_kind` is set — an explicit endpoint-kind filter
    ///   bypasses only this implicit exclusion
    /// - `Some("all")` includes every source kind
    /// - `Some(kind)` returns only rows whose `source_kind` equals `kind`
    ///   (conjunctive with `filters.event_kind` when both are supplied)
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestEventPrincipalCostQuery {
    pub since_unix_secs: u64,
    pub until_unix_secs: u64,
    pub bucket_width_secs: u64,
    pub upstream_id: Option<Uuid>,
    /// Normalized usage-rollup principal keys whose component costs should be returned.
    /// An empty selection returns no rows.
    pub principal_keys: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RequestEventPrincipalCostBucket {
    pub principal: String,
    pub bucket_start_unix_secs: u64,
    pub total_cost_micros: u64,
    pub component_costs_recorded: bool,
    pub cost_input_micros: u64,
    pub cost_output_micros: u64,
    pub cost_cache_creation_5m_micros: u64,
    pub cost_cache_creation_1h_micros: u64,
    pub cost_cache_read_micros: u64,
}

/// Describes a histogram over the same inclusive, second-based time window as
/// [`RequestEventListQuery`]. Storage backends apply
/// `ts >= since_unix_secs AND ts <= until_unix_secs`, then use `list_ts_ms`
/// only to assign each matching row to a bucket.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RequestEventHistogramQuery {
    pub since_unix_secs: u64,
    pub until_unix_secs: u64,
    pub bucket_ms: u64,
    pub bucket_count: u64,
    pub filters: RequestEventStreamFilters,
    /// Same semantics as [`RequestEventListQuery::source_kind`]:
    /// `None` excludes `renewal`, `Some("all")` includes everything, and
    /// `Some(kind)` returns only that kind.
    pub source_kind: Option<String>,
}

/// One point on the histogram's zero-filled, continuous bucket axis.
///
/// `bucket_start_unix_secs` is present for every bucket in the requested
/// range, including buckets with no matching events. `error_count` includes
/// rows where `list_status >= 500` or where `list_status` is 2xx and
/// `error_code = 'upstream_stream_error'`. This matches the admin UI's danger
/// tone in `crates/cc-lb-admin/web/src/lib/format.ts`; 4xx rows are excluded.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct RequestEventHistogramBucket {
    pub bucket_start_unix_secs: u64,
    pub total_count: u64,
    pub error_count: u64,
}

#[cfg(test)]
mod tests {
    use super::RequestEventListItem;

    fn empty_list_item() -> RequestEventListItem {
        RequestEventListItem {
            ts: 0,
            ts_ms: None,
            request_id: String::new(),
            event_id: None,
            source_kind: None,
            event_kind: None,
            principal_id: None,
            upstream: None,
            upstream_id: None,
            upstream_name: None,
            thread_id: None,
            observed_session_id: None,
            request_kind: None,
            claude_agent_id: None,
            claude_parent_agent_id: None,
            parent_session_id: None,
            client_app: None,
            session_id_source: None,
            model: None,
            reasoning_effort: None,
            thinking_budget_tokens: None,
            thinking_tokens: None,
            service_tier: None,
            status: 0,
            error_code: None,
            upstream_error_type: None,
            upstream_error_message: None,
            duration_ms: 0,
            request_body_read_ms: None,
            request_body_first_chunk_ms: None,
            request_body_receive_ms: None,
            request_body_wait_ms: None,
            request_body_process_ms: None,
            request_body_chunk_count: None,
            request_body_bytes: None,
            auth_ms: None,
            route_ms: None,
            limit_reserve_ms: None,
            json_parse_ms: None,
            cache_structure_ms: None,
            cache_token_key_ms: None,
            cache_count_lookup_ms: None,
            cache_tokenizer_queue_ms: None,
            cache_serialize_ms: None,
            cache_tokenize_ms: None,
            prepare_signer_ms: None,
            bulkhead_wait_ms: None,
            dns_ms: None,
            connect_ms: None,
            connection_reused: None,
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            shape_ms: None,
            sign_ms: None,
            upstream_ttfb_ms: None,
            upstream_body_ms: None,
            response_body_wait_ms: None,
            response_body_process_ms: None,
            response_body_downstream_poll_gap_ms: None,
            retry_overhead_ms: None,
            finalize_ms: None,
            stream_first_content_delta_ms: None,
            stream_last_content_delta_ms: None,
            inter_token_avg_ms: None,
            input_tokens: None,
            output_tokens: None,
            cache_creation_input_tokens: None,
            cache_creation_input_tokens_5m: None,
            cache_creation_input_tokens_1h: None,
            cache_read_input_tokens: None,
            cost_usd_micros: None,
            cost_input_micros: None,
            cost_output_micros: None,
            cost_cache_creation_5m_micros: None,
            cost_cache_creation_1h_micros: None,
            cost_cache_read_micros: None,
        }
    }

    #[test]
    fn request_event_list_item_preserves_measured_zero_and_omits_missing_io_timings() {
        let measured = RequestEventListItem {
            request_body_first_chunk_ms: Some(0.0),
            request_body_receive_ms: Some(1.25),
            request_body_wait_ms: Some(1.0),
            request_body_process_ms: Some(0.25),
            request_body_chunk_count: Some(0),
            response_body_wait_ms: Some(2.5),
            response_body_process_ms: Some(0.5),
            response_body_downstream_poll_gap_ms: Some(0.0),
            retry_overhead_ms: Some(0.0),
            ..empty_list_item()
        };
        let measured_json =
            serde_json::to_value(measured).expect("serialize measured list I/O timings");
        assert_eq!(measured_json["request_body_first_chunk_ms"], 0.0);
        assert_eq!(measured_json["request_body_chunk_count"], 0);
        assert_eq!(measured_json["response_body_downstream_poll_gap_ms"], 0.0);
        assert_eq!(measured_json["retry_overhead_ms"], 0.0);

        let missing_json =
            serde_json::to_value(empty_list_item()).expect("serialize missing list I/O timings");
        for field in [
            "request_body_first_chunk_ms",
            "request_body_receive_ms",
            "request_body_wait_ms",
            "request_body_process_ms",
            "request_body_chunk_count",
            "response_body_wait_ms",
            "response_body_process_ms",
            "response_body_downstream_poll_gap_ms",
            "retry_overhead_ms",
        ] {
            assert!(missing_json.get(field).is_none(), "{field} must be omitted");
        }
    }
}
