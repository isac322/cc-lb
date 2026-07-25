use cc_lb_storage_api::{
    RequestEvent, RequestEventListItem, RequestEventListQuery, RequestEventUpstream, StatusClass,
    StorageResult,
};
use sqlx::FromRow;

use crate::adapter::{
    PostgresStorage, u64_to_i64, unix_secs_to_datetime_lower, unix_secs_to_datetime_upper,
};
use crate::error_map::map_sqlx_error;

use super::request_event_list_row::list_row_to_item;

const LIST_REQUEST_EVENTS_SQL: &str = "\
SELECT \
    ts_secs, \
    list_ts_ms AS ts_ms, \
    COALESCE(payload_jsonb ->> 'request_id', '') AS request_id, \
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
    (payload_jsonb ->> 'duration_ms')::bigint AS duration_ms, \
    error_code, \
    upstream_error_type, \
    upstream_error_message, \
    (payload_jsonb ->> 'auth_ms')::bigint AS auth_ms, \
    (payload_jsonb ->> 'route_ms')::bigint AS route_ms, \
    (payload_jsonb ->> 'limit_reserve_ms')::bigint AS limit_reserve_ms, \
    (payload_jsonb ->> 'bulkhead_wait_ms')::bigint AS bulkhead_wait_ms, \
    (payload_jsonb ->> 'dns_ms')::bigint AS dns_ms, \
    (payload_jsonb ->> 'connect_ms')::bigint AS connect_ms, \
    (payload_jsonb ->> 'connection_reused')::boolean AS connection_reused, \
    (payload_jsonb ->> 'limit_reconcile_ms')::bigint AS limit_reconcile_ms, \
    (payload_jsonb ->> 'observability_post_ms')::bigint AS observability_post_ms, \
    (payload_jsonb ->> 'proxy_setup_ms')::bigint AS proxy_setup_ms, \
    (payload_jsonb ->> 'shape_ms')::bigint AS shape_ms, \
    (payload_jsonb ->> 'sign_ms')::bigint AS sign_ms, \
    (payload_jsonb ->> 'upstream_ttfb_ms')::bigint AS upstream_ttfb_ms, \
    (payload_jsonb ->> 'upstream_body_ms')::bigint AS upstream_body_ms, \
    (payload_jsonb ->> 'stream_first_content_delta_ms')::bigint AS stream_first_content_delta_ms, \
    (payload_jsonb ->> 'stream_last_content_delta_ms')::bigint AS stream_last_content_delta_ms, \
    (payload_jsonb ->> 'inter_token_avg_ms')::bigint AS inter_token_avg_ms, \
    input_tokens, \
    output_tokens, \
    cache_creation_input_tokens, \
    cache_creation_input_tokens_5m, \
    cache_creation_input_tokens_1h, \
    cache_read_input_tokens, \
    (payload_jsonb ->> 'cost_usd_micros')::bigint AS cost_usd_micros, \
    (payload_jsonb ->> 'cost_input_micros')::bigint AS cost_input_micros, \
    (payload_jsonb ->> 'cost_output_micros')::bigint AS cost_output_micros, \
    (payload_jsonb ->> 'cost_cache_creation_5m_micros')::bigint AS cost_cache_creation_5m_micros, \
    (payload_jsonb ->> 'cost_cache_creation_1h_micros')::bigint AS cost_cache_creation_1h_micros, \
    (payload_jsonb ->> 'cost_cache_read_micros')::bigint AS cost_cache_read_micros \
FROM ( \
    SELECT \
        candidates.*, \
        convert_from(candidates.payload, 'UTF8')::jsonb AS payload_jsonb \
    FROM ( \
        SELECT \
            EXTRACT(EPOCH FROM r.ts)::bigint AS ts_secs, \
            r.payload, \
            r.list_ts_ms, \
            r.list_event_key, \
            r.list_upstream, \
            r.list_status, \
            r.event_id, \
            r.source_kind, \
            r.principal_id, \
            r.upstream_id, \
            r.upstream_name, \
            r.thread_id, \
            r.observed_session_id, \
            r.request_kind, \
            r.claude_agent_id, \
            r.claude_parent_agent_id, \
            r.parent_session_id, \
            r.client_app, \
            r.session_id_source, \
            r.model, \
            r.reasoning_effort, \
            r.thinking_budget_tokens, \
            r.thinking_tokens, \
            r.service_tier, \
            r.error_code, \
            r.upstream_error_type, \
            r.upstream_error_message, \
            r.input_tokens, \
            r.output_tokens, \
            r.cache_creation_input_tokens, \
            r.cache_creation_input_tokens_5m, \
            r.cache_creation_input_tokens_1h, \
            r.cache_read_input_tokens \
        FROM request_events_v1 r \
        WHERE r.ts >= $1 AND ($2::timestamptz IS NULL OR r.ts <= $2) \
          AND ($3::text IS NULL OR r.principal_id = $3) \
          AND ($4::text IS NULL OR r.model = $4) \
          AND ($5::uuid IS NULL OR r.upstream_id = $5) \
          AND ($14::text IS NULL OR r.thread_id = $14) \
          AND ($6::text IS NULL OR r.list_upstream = $6) \
          AND ($7::int IS NULL OR r.list_status BETWEEN $7 AND $8) \
          AND ( \
                $12::int = 1 \
             OR ($13::text IS NOT NULL AND r.source_kind = $13) \
             OR ($13::text IS NULL AND (r.source_kind IS NULL OR r.source_kind <> 'renewal')) \
          ) \
          AND ( \
                $9::bigint IS NULL \
             OR r.list_ts_ms < $9 \
             OR (r.list_ts_ms = $9 \
                 AND $10::text IS NOT NULL \
                 AND r.list_event_key < $10) \
          ) \
        ORDER BY r.list_ts_ms DESC, r.list_event_key DESC \
        LIMIT $11 \
    ) candidates \
) parsed \
ORDER BY list_ts_ms DESC, list_event_key DESC";

#[derive(FromRow)]
pub(super) struct ListRow {
    pub(super) ts_secs: i64,
    pub(super) ts_ms: Option<i64>,
    pub(super) request_id: String,
    pub(super) event_id: Option<String>,
    pub(super) source_kind: Option<String>,
    pub(super) principal_id: Option<String>,
    pub(super) upstream_id: Option<uuid::Uuid>,
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
    pub(super) status: Option<i32>,
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
    pub(super) connection_reused: Option<bool>,
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
    storage: &PostgresStorage,
    query: &RequestEventListQuery,
) -> StorageResult<Vec<RequestEventListItem>> {
    if query.limit == 0 || query.until_unix_secs < query.since_unix_secs {
        return Ok(Vec::new());
    }
    let Some(since) =
        unix_secs_to_datetime_lower(query.since_unix_secs, "request event list since")?
    else {
        return Ok(Vec::new());
    };
    let until = unix_secs_to_datetime_upper(query.until_unix_secs, "request event list until")?;

    let (status_min, status_max) = query
        .filters
        .status_class
        .map(status_class_range)
        .map_or((None, None), |(min, max)| (Some(min), Some(max)));
    let (source_kind_all, source_kind_exact) = source_kind_filter(query.source_kind.as_deref());

    let rows = sqlx::query_as::<_, ListRow>(LIST_REQUEST_EVENTS_SQL)
        .bind(since)
        .bind(until)
        .bind(query.filters.principal_id.as_deref())
        .bind(query.filters.model.as_deref())
        .bind(query.filters.upstream_id)
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
        .bind(u64_to_i64(query.limit as u64, "request event list limit")?)
        .bind(i32::from(source_kind_all))
        .bind(source_kind_exact)
        .bind(query.filters.thread_id.as_deref())
        .fetch_all(&storage.pool)
        .await
        .map_err(map_sqlx_error)?;

    rows.into_iter().map(list_row_to_item).collect()
}

pub(super) async fn get_request_event(
    storage: &PostgresStorage,
    event_id: &str,
) -> StorageResult<Option<RequestEvent>> {
    let payload = sqlx::query_scalar::<_, Vec<u8>>(
        "SELECT payload FROM request_events_v1 WHERE event_id = $1",
    )
    .bind(event_id)
    .fetch_optional(&storage.pool)
    .await
    .map_err(map_sqlx_error)?;

    payload
        .map(|payload| serde_json::from_slice(&payload).map_err(Into::into))
        .transpose()
}

fn upstream_as_str(upstream: RequestEventUpstream) -> &'static str {
    match upstream {
        RequestEventUpstream::AnthropicDirect => "anthropic_direct",
    }
}

fn status_class_range(class: StatusClass) -> (i32, i32) {
    match class {
        StatusClass::TwoXx => (200, 299),
        StatusClass::ThreeXx => (300, 399),
        StatusClass::FourXx => (400, 499),
        StatusClass::FiveXx => (500, 599),
    }
}

fn source_kind_filter(source_kind: Option<&str>) -> (bool, Option<&str>) {
    match source_kind {
        Some("all") => (true, None),
        Some(kind) => (false, Some(kind)),
        None => (false, None),
    }
}
