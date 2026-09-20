use cc_lb_storage_api::{
    RequestEvent, RequestEventKind, RequestEventListItem, RequestEventListQuery,
    RequestEventUpstream, StatusClass, StorageError, StorageResult, model_filter_like_pattern,
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
    r.payload, \
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
    r.list_upstream AS upstream, \
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
    pub(super) payload: Option<Vec<u8>>,
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
    pub(super) upstream: Option<String>,
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
    if query.limit == 0 || query.until_unix_secs < query.since_unix_secs {
        return Ok(Vec::new());
    }
    let Some(since) =
        unix_secs_to_datetime_lower(query.since_unix_secs, "request event list since")?
    else {
        return Ok(Vec::new());
    };
    let until = unix_secs_to_datetime_upper(query.until_unix_secs, "request event list until")?;

    let mut builder = QueryBuilder::<Postgres>::new(LIST_REQUEST_EVENTS_SELECT);
    builder.push_bind(since);

    if let Some(until) = until {
        builder.push(" AND r.ts <= ");
        builder.push_bind(until);
    }
    if let Some(principal_id) = query.filters.principal_id.as_deref() {
        builder.push(" AND r.principal_id = ");
        builder.push_bind(principal_id);
    }
    if let Some(model) = query.filters.model.as_deref() {
        builder.push(" AND lower(r.model) LIKE ");
        builder.push_bind(model_filter_like_pattern(model));
        builder.push(" ESCAPE '\\'");
    }
    if let Some(upstream_id) = query.filters.upstream_id {
        builder.push(" AND r.upstream_id = ");
        builder.push_bind(upstream_id);
    }
    if let Some(thread_id) = query.filters.thread_id.as_deref() {
        builder.push(" AND r.thread_id = ");
        builder.push_bind(thread_id);
    }
    if let Some(upstream) = query.filters.upstream {
        builder.push(" AND r.list_upstream = ");
        builder.push_bind(upstream_as_str(upstream));
    }
    if let Some(status_class) = query.filters.status_class {
        let (status_min, status_max) = status_class_range(status_class);
        builder.push(" AND r.list_status BETWEEN ");
        builder.push_bind(status_min);
        builder.push(" AND ");
        builder.push_bind(status_max);
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

    let rows = builder
        .build_query_as::<ListRow>()
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

pub(super) fn upstream_as_str(upstream: RequestEventUpstream) -> &'static str {
    match upstream {
        RequestEventUpstream::AnthropicDirect => "anthropic_direct",
    }
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
