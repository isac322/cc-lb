use cc_lb_storage_api::{
    RequestEvent, RequestEventListItem, RequestEventListQuery, RequestEventUpstream, StatusClass,
    StorageResult, model_filter_like_pattern,
};
use sqlx::FromRow;

use crate::SqliteStorage;
use crate::map_sqlx_error;

use super::request_event_list_row::list_row_to_item;
use super::request_events::{u64_to_i64, u64_to_i64_upper, usize_to_i64};

const LIST_REQUEST_EVENTS_SQL: &str = "\
SELECT \
    ts, \
    list_ts_ms AS ts_ms, \
    request_id, \
    event_id, \
    source_kind, \
    principal_id, \
    upstream_id, \
    upstream_name, \
    thread_id, \
    observed_session_id, \
    request_kind, \
    claude_agent_id, \
    claude_parent_agent_id, \
    parent_session_id, \
    client_app, \
    session_id_source, \
    model, \
    reasoning_effort, \
    thinking_budget_tokens, \
    thinking_tokens, \
    service_tier, \
    list_upstream AS upstream, \
    list_status AS status, \
    list_duration_ms AS duration_ms, \
    error_code, \
    upstream_error_type, \
    upstream_error_message, \
    list_auth_ms AS auth_ms, \
    list_route_ms AS route_ms, \
    list_limit_reserve_ms AS limit_reserve_ms, \
    list_request_body_read_ms AS request_body_read_ms, \
    list_request_body_bytes AS request_body_bytes, \
    list_request_body_first_chunk_ms AS request_body_first_chunk_ms, \
    list_request_body_receive_ms AS request_body_receive_ms, \
    list_request_body_wait_ms AS request_body_wait_ms, \
    list_request_body_process_ms AS request_body_process_ms, \
    list_request_body_chunk_count AS request_body_chunk_count, \
    list_response_body_wait_ms AS response_body_wait_ms, \
    list_response_body_process_ms AS response_body_process_ms, \
    list_response_body_downstream_poll_gap_ms AS response_body_downstream_poll_gap_ms, \
    list_retry_overhead_ms AS retry_overhead_ms, \
    list_json_parse_ms AS json_parse_ms, \
    list_cache_structure_ms AS cache_structure_ms, \
    list_cache_token_key_ms AS cache_token_key_ms, \
    list_cache_count_lookup_ms AS cache_count_lookup_ms, \
    list_cache_tokenizer_queue_ms AS cache_tokenizer_queue_ms, \
    list_cache_serialize_ms AS cache_serialize_ms, \
    list_cache_tokenize_ms AS cache_tokenize_ms, \
    list_prepare_signer_ms AS prepare_signer_ms, \
    list_bulkhead_wait_ms AS bulkhead_wait_ms, \
    list_dns_ms AS dns_ms, \
    list_connect_ms AS connect_ms, \
    list_connection_reused AS connection_reused, \
    list_limit_reconcile_ms AS limit_reconcile_ms, \
    list_observability_post_ms AS observability_post_ms, \
    list_finalize_ms AS finalize_ms, \
    list_proxy_setup_ms AS proxy_setup_ms, \
    list_shape_ms AS shape_ms, \
    list_sign_ms AS sign_ms, \
    list_upstream_ttfb_ms AS upstream_ttfb_ms, \
    list_upstream_body_ms AS upstream_body_ms, \
    list_stream_first_content_delta_ms AS stream_first_content_delta_ms, \
    list_stream_last_content_delta_ms AS stream_last_content_delta_ms, \
    list_inter_token_avg_ms AS inter_token_avg_ms, \
    input_tokens, \
    output_tokens, \
    cache_creation_input_tokens, \
    cache_creation_input_tokens_5m, \
    cache_creation_input_tokens_1h, \
    cache_read_input_tokens, \
    list_cost_usd_micros AS cost_usd_micros, \
    list_cost_input_micros AS cost_input_micros, \
    list_cost_output_micros AS cost_output_micros, \
    list_cost_cache_creation_5m_micros AS cost_cache_creation_5m_micros, \
    list_cost_cache_creation_1h_micros AS cost_cache_creation_1h_micros, \
    list_cost_cache_read_micros AS cost_cache_read_micros \
FROM request_events_v1 \
WHERE ts >= ?1 AND ts <= ?2 \
  AND (?3 IS NULL OR principal_id = ?3) \
  AND (?4 IS NULL OR lower(model) LIKE ?4 ESCAPE '\\') \
  AND (?5 IS NULL OR upstream_id = ?5) \
  AND (?14 IS NULL OR thread_id = ?14) \
  AND (?6 IS NULL OR list_upstream = ?6) \
  AND (?7 IS NULL OR list_status BETWEEN ?7 AND ?8) \
  AND ( \
        ?12 = 1 \
     OR (?13 IS NOT NULL AND source_kind = ?13) \
     OR (?13 IS NULL AND (source_kind IS NULL OR source_kind <> 'renewal')) \
  ) \
  AND ( \
        ?9 IS NULL \
     OR list_ts_ms < ?9 \
     OR (list_ts_ms = ?9 \
         AND ?10 IS NOT NULL AND list_event_key < ?10) \
  ) \
ORDER BY list_ts_ms DESC, \
         list_event_key DESC, \
         id DESC \
LIMIT ?11";

#[derive(FromRow)]
pub(super) struct ListRow {
    pub(super) ts: i64,
    pub(super) ts_ms: Option<i64>,
    pub(super) request_id: String,
    pub(super) event_id: Option<String>,
    pub(super) source_kind: Option<String>,
    pub(super) principal_id: Option<String>,
    pub(super) upstream_id: Option<String>,
    pub(super) upstream_name: Option<String>,
    pub(super) thread_id: Option<String>,
    pub(super) observed_session_id: Option<String>,
    pub(super) request_kind: Option<String>,
    pub(super) claude_agent_id: Option<String>,
    pub(super) claude_parent_agent_id: Option<String>,
    pub(super) parent_session_id: Option<String>,
    pub(super) client_app: Option<String>,
    pub(super) session_id_source: Option<String>,
    pub(super) model: Option<String>,
    pub(super) reasoning_effort: Option<String>,
    pub(super) thinking_budget_tokens: Option<i64>,
    pub(super) thinking_tokens: Option<i64>,
    pub(super) service_tier: Option<String>,
    pub(super) upstream: Option<String>,
    pub(super) status: Option<i64>,
    pub(super) duration_ms: Option<i64>,
    pub(super) error_code: Option<String>,
    pub(super) upstream_error_type: Option<String>,
    pub(super) upstream_error_message: Option<String>,
    pub(super) auth_ms: Option<i64>,
    pub(super) route_ms: Option<i64>,
    pub(super) limit_reserve_ms: Option<i64>,
    pub(super) request_body_read_ms: Option<i64>,
    pub(super) request_body_bytes: Option<i64>,
    pub(super) request_body_first_chunk_ms: Option<f64>,
    pub(super) request_body_receive_ms: Option<f64>,
    pub(super) request_body_wait_ms: Option<f64>,
    pub(super) request_body_process_ms: Option<f64>,
    pub(super) request_body_chunk_count: Option<i64>,
    pub(super) response_body_wait_ms: Option<f64>,
    pub(super) response_body_process_ms: Option<f64>,
    pub(super) response_body_downstream_poll_gap_ms: Option<f64>,
    pub(super) retry_overhead_ms: Option<f64>,
    pub(super) json_parse_ms: Option<f64>,
    pub(super) cache_structure_ms: Option<f64>,
    pub(super) cache_token_key_ms: Option<f64>,
    pub(super) cache_count_lookup_ms: Option<f64>,
    pub(super) cache_tokenizer_queue_ms: Option<f64>,
    pub(super) cache_serialize_ms: Option<f64>,
    pub(super) cache_tokenize_ms: Option<f64>,
    pub(super) prepare_signer_ms: Option<f64>,
    pub(super) bulkhead_wait_ms: Option<i64>,
    pub(super) dns_ms: Option<i64>,
    pub(super) connect_ms: Option<i64>,
    pub(super) connection_reused: Option<i64>,
    pub(super) limit_reconcile_ms: Option<i64>,
    pub(super) observability_post_ms: Option<i64>,
    pub(super) finalize_ms: Option<i64>,
    pub(super) proxy_setup_ms: Option<i64>,
    pub(super) shape_ms: Option<i64>,
    pub(super) sign_ms: Option<i64>,
    pub(super) upstream_ttfb_ms: Option<i64>,
    pub(super) upstream_body_ms: Option<i64>,
    pub(super) stream_first_content_delta_ms: Option<i64>,
    pub(super) stream_last_content_delta_ms: Option<i64>,
    pub(super) inter_token_avg_ms: Option<i64>,
    pub(super) input_tokens: Option<i64>,
    pub(super) output_tokens: Option<i64>,
    pub(super) cache_creation_input_tokens: Option<i64>,
    pub(super) cache_creation_input_tokens_5m: Option<i64>,
    pub(super) cache_creation_input_tokens_1h: Option<i64>,
    pub(super) cache_read_input_tokens: Option<i64>,
    pub(super) cost_usd_micros: Option<i64>,
    pub(super) cost_input_micros: Option<i64>,
    pub(super) cost_output_micros: Option<i64>,
    pub(super) cost_cache_creation_5m_micros: Option<i64>,
    pub(super) cost_cache_creation_1h_micros: Option<i64>,
    pub(super) cost_cache_read_micros: Option<i64>,
}

pub(super) async fn list_request_events(
    storage: &SqliteStorage,
    query: &RequestEventListQuery,
) -> StorageResult<Vec<RequestEventListItem>> {
    if query.limit == 0 || query.until_unix_secs < query.since_unix_secs {
        return Ok(Vec::new());
    }

    let (status_min, status_max) = query
        .filters
        .status_class
        .map(status_class_range)
        .map_or((None, None), |(min, max)| (Some(min), Some(max)));
    let (source_kind_all, source_kind_exact) = source_kind_filter(query.source_kind.as_deref());

    let rows = sqlx::query_as::<_, ListRow>(LIST_REQUEST_EVENTS_SQL)
        .bind(u64_to_i64(
            query.since_unix_secs,
            "request event list since",
        )?)
        .bind(u64_to_i64_upper(query.until_unix_secs))
        .bind(query.filters.principal_id.as_deref())
        .bind(
            query
                .filters
                .model
                .as_deref()
                .map(model_filter_like_pattern),
        )
        .bind(query.filters.upstream_id.map(|id| id.to_string()))
        .bind(query.filters.upstream.map(upstream_as_str))
        .bind(status_min)
        .bind(status_max)
        .bind(
            query
                .until_ts_ms
                .map(|value| u64_to_i64(value, "request event list until_ts_ms"))
                .transpose()?,
        )
        .bind(query.until_event_id.as_deref())
        .bind(usize_to_i64(query.limit, "request event list limit")?)
        .bind(i64::from(source_kind_all))
        .bind(source_kind_exact)
        .bind(query.filters.thread_id.as_deref())
        .fetch_all(storage.pool())
        .await
        .map_err(map_sqlx_error)?;

    rows.into_iter().map(list_row_to_item).collect()
}

pub(super) async fn get_request_event(
    storage: &SqliteStorage,
    event_id: &str,
) -> StorageResult<Option<RequestEvent>> {
    let payload =
        sqlx::query_scalar::<_, String>("SELECT payload FROM request_events_v1 WHERE event_id = ?")
            .bind(event_id)
            .fetch_optional(storage.pool())
            .await
            .map_err(map_sqlx_error)?;

    payload
        .map(|payload| serde_json::from_str(&payload).map_err(Into::into))
        .transpose()
}

pub(super) fn upstream_as_str(upstream: RequestEventUpstream) -> &'static str {
    match upstream {
        RequestEventUpstream::AnthropicDirect => "anthropic_direct",
    }
}

pub(super) fn status_class_range(class: StatusClass) -> (i64, i64) {
    match class {
        StatusClass::TwoXx => (200, 299),
        StatusClass::ThreeXx => (300, 399),
        StatusClass::FourXx => (400, 499),
        StatusClass::FiveXx => (500, 599),
    }
}

pub(super) fn source_kind_filter(source_kind: Option<&str>) -> (bool, Option<&str>) {
    match source_kind {
        Some("all") => (true, None),
        Some(kind) => (false, Some(kind)),
        None => (false, None),
    }
}
