use cc_lb_storage_api::{
    RequestEvent, RequestEventListItem, RequestEventListQuery, RequestEventUpstream, StatusClass,
    StorageResult,
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
    principal_id, \
    upstream_id, \
    upstream_name, \
    thread_id, \
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
    list_bulkhead_wait_ms AS bulkhead_wait_ms, \
    list_dns_ms AS dns_ms, \
    list_connect_ms AS connect_ms, \
    list_connection_reused AS connection_reused, \
    list_limit_reconcile_ms AS limit_reconcile_ms, \
    list_observability_post_ms AS observability_post_ms, \
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
  AND (?4 IS NULL OR model = ?4) \
  AND (?5 IS NULL OR upstream_id = ?5) \
  AND (?6 IS NULL OR list_upstream = ?6) \
  AND (?7 IS NULL OR list_status BETWEEN ?7 AND ?8) \
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
    pub(super) principal_id: Option<String>,
    pub(super) upstream_id: Option<String>,
    pub(super) upstream_name: Option<String>,
    pub(super) thread_id: Option<String>,
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
    pub(super) bulkhead_wait_ms: Option<i64>,
    pub(super) dns_ms: Option<i64>,
    pub(super) connect_ms: Option<i64>,
    pub(super) connection_reused: Option<i64>,
    pub(super) limit_reconcile_ms: Option<i64>,
    pub(super) observability_post_ms: Option<i64>,
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

    let rows = sqlx::query_as::<_, ListRow>(LIST_REQUEST_EVENTS_SQL)
        .bind(u64_to_i64(
            query.since_unix_secs,
            "request event list since",
        )?)
        .bind(u64_to_i64_upper(query.until_unix_secs))
        .bind(query.filters.principal_id.as_deref())
        .bind(query.filters.model.as_deref())
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

fn upstream_as_str(upstream: RequestEventUpstream) -> &'static str {
    match upstream {
        RequestEventUpstream::AnthropicDirect => "anthropic_direct",
    }
}

fn status_class_range(class: StatusClass) -> (i64, i64) {
    match class {
        StatusClass::TwoXx => (200, 299),
        StatusClass::ThreeXx => (300, 399),
        StatusClass::FourXx => (400, 499),
        StatusClass::FiveXx => (500, 599),
    }
}
