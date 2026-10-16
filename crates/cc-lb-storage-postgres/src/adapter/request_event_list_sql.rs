use cc_lb_storage_api::{
    RequestEvent, RequestEventKind, RequestEventListItem, RequestEventListQuery, StatusClass,
    StorageError, StorageResult, model_filter_like_patterns,
};
use sqlx::{FromRow, Postgres, QueryBuilder};

use crate::adapter::{
    PostgresStorage, u64_to_i64, unix_secs_to_datetime_lower, unix_secs_to_datetime_upper,
};
use crate::error_map::map_sqlx_error;

use super::request_event_list_row::list_row_to_item;

const LIST_REQUEST_EVENTS_SELECT: &str = "\
SELECT \
    EXTRACT(EPOCH FROM r.ts)::bigint AS ts_secs, \
    r.request_id AS request_id, \
    r.duration_ms AS duration_ms, \
    r.request_body_read_ms AS request_body_read_ms, \
    r.request_body_bytes AS request_body_bytes, \
    r.request_body_first_chunk_ms AS request_body_first_chunk_ms, \
    r.request_body_receive_ms AS request_body_receive_ms, \
    r.request_body_wait_ms AS request_body_wait_ms, \
    r.request_body_process_ms AS request_body_process_ms, \
    r.request_body_chunk_count AS request_body_chunk_count, \
    r.response_body_wait_ms AS response_body_wait_ms, \
    r.response_body_process_ms AS response_body_process_ms, \
    r.response_body_downstream_poll_gap_ms AS response_body_downstream_poll_gap_ms, \
    r.retry_overhead_ms AS retry_overhead_ms, \
    r.auth_ms AS auth_ms, \
    r.route_ms AS route_ms, \
    r.limit_reserve_ms AS limit_reserve_ms, \
    r.json_parse_ms AS json_parse_ms, \
    r.cache_structure_ms AS cache_structure_ms, \
    r.cache_token_key_ms AS cache_token_key_ms, \
    r.cache_count_lookup_ms AS cache_count_lookup_ms, \
    r.cache_tokenizer_queue_ms AS cache_tokenizer_queue_ms, \
    r.cache_serialize_ms AS cache_serialize_ms, \
    r.cache_tokenize_ms AS cache_tokenize_ms, \
    r.prepare_signer_ms AS prepare_signer_ms, \
    r.bulkhead_wait_ms AS bulkhead_wait_ms, \
    r.dns_ms AS dns_ms, \
    r.connect_ms AS connect_ms, \
    r.connection_reused AS connection_reused, \
    r.finalize_ms AS finalize_ms, \
    r.proxy_setup_ms AS proxy_setup_ms, \
    r.shape_ms AS shape_ms, \
    r.sign_ms AS sign_ms, \
    r.upstream_ttfb_ms AS upstream_ttfb_ms, \
    r.upstream_body_ms AS upstream_body_ms, \
    r.stream_first_content_delta_ms AS stream_first_content_delta_ms, \
    r.stream_last_content_delta_ms AS stream_last_content_delta_ms, \
    r.inter_token_avg_ms AS inter_token_avg_ms, \
    r.list_cost_usd_micros AS cost_usd_micros, \
    r.list_cost_input_micros AS cost_input_micros, \
    r.list_cost_output_micros AS cost_output_micros, \
    r.list_cost_cache_creation_5m_micros AS cost_cache_creation_5m_micros, \
    r.list_cost_cache_creation_1h_micros AS cost_cache_creation_1h_micros, \
    r.list_cost_cache_read_micros AS cost_cache_read_micros, \
    r.list_ts_ms AS ts_ms, \
    r.event_id, \
    r.source_kind, \
    r.event_kind, \
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
    r.list_status AS status, \
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
WHERE r.ts >= ";

#[derive(FromRow)]
pub(super) struct ListRow {
    pub(super) ts_secs: i64,
    pub(super) request_id: Option<String>,
    pub(super) duration_ms: Option<i64>,
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
    pub(super) auth_ms: Option<i64>,
    pub(super) route_ms: Option<i64>,
    pub(super) limit_reserve_ms: Option<i64>,
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
    pub(super) connection_reused: Option<bool>,
    pub(super) finalize_ms: Option<i64>,
    pub(super) proxy_setup_ms: Option<i64>,
    pub(super) shape_ms: Option<i64>,
    pub(super) sign_ms: Option<i64>,
    pub(super) upstream_ttfb_ms: Option<i64>,
    pub(super) upstream_body_ms: Option<i64>,
    pub(super) stream_first_content_delta_ms: Option<i64>,
    pub(super) stream_last_content_delta_ms: Option<i64>,
    pub(super) inter_token_avg_ms: Option<i64>,
    pub(super) cost_usd_micros: Option<i64>,
    pub(super) cost_input_micros: Option<i64>,
    pub(super) cost_output_micros: Option<i64>,
    pub(super) cost_cache_creation_5m_micros: Option<i64>,
    pub(super) cost_cache_creation_1h_micros: Option<i64>,
    pub(super) cost_cache_read_micros: Option<i64>,
    pub(super) ts_ms: Option<i64>,
    pub(super) event_id: Option<String>,
    pub(super) source_kind: Option<String>,
    pub(super) event_kind: Option<String>,
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
    pub(super) status: Option<i32>,
    pub(super) error_code: Option<String>,
    pub(super) upstream_error_type: Option<String>,
    pub(super) upstream_error_message: Option<String>,
    pub(super) input_tokens: Option<i64>,
    pub(super) output_tokens: Option<i64>,
    pub(super) cache_creation_input_tokens: Option<i64>,
    pub(super) cache_creation_input_tokens_5m: Option<i64>,
    pub(super) cache_creation_input_tokens_1h: Option<i64>,
    pub(super) cache_read_input_tokens: Option<i64>,
}

pub(super) async fn list_request_events(
    storage: &PostgresStorage,
    query: &RequestEventListQuery,
) -> StorageResult<Vec<RequestEventListItem>> {
    let Some(mut builder) = list_request_events_query("", query)? else {
        return Ok(Vec::new());
    };

    let rows = builder
        .build_query_as::<ListRow>()
        .fetch_all(&storage.pool)
        .await
        .map_err(map_sqlx_error)?;

    rows.into_iter().map(list_row_to_item).collect()
}

/// Builds the list statement behind `prefix` (empty for execution,
/// `EXPLAIN …` for plan inspection), so plan tests see the exact production
/// SQL. `None` means the page is empty without querying.
pub(super) fn list_request_events_query(
    prefix: &str,
    query: &RequestEventListQuery,
) -> StorageResult<Option<QueryBuilder<Postgres>>> {
    if query.limit == 0 || query.until_unix_secs < query.since_unix_secs {
        return Ok(None);
    }
    let Some(since) =
        unix_secs_to_datetime_lower(query.since_unix_secs, "request event list since")?
    else {
        return Ok(None);
    };
    let until = unix_secs_to_datetime_upper(query.until_unix_secs, "request event list until")?;

    let mut builder =
        QueryBuilder::<Postgres>::new(format!("{prefix}{LIST_REQUEST_EVENTS_SELECT}"));
    builder.push_bind(since);

    if let Some(until) = until {
        builder.push(" AND r.ts <= ");
        builder.push_bind(until);
    }
    if let Some(principal_id) = query.filters.principal_id.as_deref() {
        builder.push(" AND r.principal_id = ");
        builder.push_bind(principal_id);
    }
    if let Some([prefix_pattern, contains_pattern]) = query
        .filters
        .model
        .as_deref()
        .and_then(model_filter_like_patterns)
    {
        builder.push(" AND (lower(r.model) LIKE ");
        builder.push_bind(prefix_pattern);
        builder.push(" ESCAPE '\\' OR lower(r.model) LIKE ");
        builder.push_bind(contains_pattern);
        builder.push(" ESCAPE '\\')");
    }
    if let Some(upstream_id) = query.filters.upstream_id {
        builder.push(" AND r.upstream_id = ");
        builder.push_bind(upstream_id);
    }
    if let Some(thread_id) = query.filters.thread_id.as_deref() {
        builder.push(" AND r.thread_id = ");
        builder.push_bind(thread_id);
    }
    if let Some(status_class) = query.filters.status_class {
        let (status_min, status_max) = status_class_range(status_class);
        builder.push(" AND r.list_status BETWEEN ");
        builder.push_bind(status_min);
        builder.push(" AND ");
        builder.push_bind(status_max);
    }
    if query.filters.errors_only {
        builder.push(" AND (r.list_status >= 400 OR r.error_code IS NOT NULL)");
    }

    match query.source_kind.as_deref() {
        Some("all") => {}
        Some(source_kind) => {
            builder.push(" AND r.source_kind = ");
            builder.push_bind(source_kind);
        }
        // An explicit event_kind filter bypasses only this implicit default
        // renewal exclusion; an explicit source_kind stays conjunctive.
        None if query.filters.event_kind.is_none() => {
            builder.push(" AND (r.source_kind IS NULL OR r.source_kind <> 'renewal')");
        }
        None => {}
    }

    if let Some(event_kind) = query.filters.event_kind {
        builder.push(
            " AND CASE WHEN r.source_kind = 'renewal' THEN 'renewal' \
             ELSE COALESCE(r.event_kind, 'unclassified') END = ",
        );
        builder.push_bind(event_kind.as_str());
    }

    if let Some(until_ts_ms) = query.until_ts_ms {
        let until_ts_ms = u64_to_i64(until_ts_ms, "request event list until_ts_ms")?;
        builder.push(" AND (r.list_ts_ms < ");
        builder.push_bind(until_ts_ms);
        if let Some(until_event_id) = query.until_event_id.as_deref() {
            builder.push(" OR (r.list_ts_ms = ");
            builder.push_bind(until_ts_ms);
            builder.push(" AND r.list_event_key < ");
            builder.push_bind(until_event_id);
            builder.push(")");
        }
        builder.push(")");
    }

    builder.push(" ORDER BY r.list_ts_ms DESC, r.list_event_key DESC LIMIT ");
    builder.push_bind(u64_to_i64(query.limit as u64, "request event list limit")?);
    Ok(Some(builder))
}

pub(super) async fn get_request_event(
    storage: &PostgresStorage,
    event_id: &str,
) -> StorageResult<Option<RequestEvent>> {
    let rows = sqlx::query(super::request_events::row::GET_SQL)
        .bind(event_id)
        .fetch_optional(&storage.pool)
        .await
        .map_err(map_sqlx_error)?;
    rows.map(|row| super::request_events::row::from_row(&row))
        .transpose()
}

pub(super) fn status_class_range(class: StatusClass) -> (i32, i32) {
    match class {
        StatusClass::TwoXx => (200, 299),
        StatusClass::ThreeXx => (300, 399),
        StatusClass::FourXx => (400, 499),
        StatusClass::FiveXx => (500, 599),
    }
}

pub(super) fn parse_event_kind(value: &str) -> StorageResult<RequestEventKind> {
    value.parse().map_err(|_| StorageError::Corrupted {
        message: format!("request event unknown event_kind value: {value}"),
    })
}
