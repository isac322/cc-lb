use cc_lb_storage_api::{
    RequestEvent, RequestEventListItem, RequestEventListQuery, StatusClass, StorageError,
    StorageResult, model_filter_like_patterns,
};
use sqlx::FromRow;

use crate::SqliteStorage;
use crate::map_sqlx_error;

use super::request_event_list_row::list_row_to_item;
use super::request_events::{u64_to_i64, u64_to_i64_upper, usize_to_i64};

const LIST_REQUEST_EVENTS_SQL_HEAD: &str = "\
SELECT \
    ts, \
    list_ts_ms AS ts_ms, \
    request_id, \
    event_id, \
    source_kind, \
    event_kind, \
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
    thinking_level, \
    thinking_budget_tokens, \
    thinking_tokens, \
    service_tier, \
    list_status AS status, \
    list_duration_ms AS duration_ms, \
    error_code, \
    upstream_error_type, \
    upstream_error_message, \
    upstream_error_code, \
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
    cache_write_input_tokens, \
    cache_write_input_tokens_30m, \
    cache_write_input_tokens_12h, \
    list_cost_usd_micros AS cost_usd_micros, \
    list_cost_input_micros AS cost_input_micros, \
    list_cost_output_micros AS cost_output_micros, \
    list_cost_cache_creation_5m_micros AS cost_cache_creation_5m_micros, \
    list_cost_cache_creation_1h_micros AS cost_cache_creation_1h_micros, \
    list_cost_cache_read_micros AS cost_cache_read_micros, \
    list_cost_cache_write_micros AS cost_cache_write_micros, \
    list_cost_cache_write_30m_micros AS cost_cache_write_30m_micros, \
    list_cost_cache_write_12h_micros AS cost_cache_write_12h_micros \
FROM request_events_v1 \
WHERE ";

const LIST_REQUEST_EVENTS_SQL_BOUNDED_TIME: &str = "ts >= ?1 AND ts <= ?2";
const LIST_REQUEST_EVENTS_SQL_UNBOUNDED_TIME: &str = "+ts >= ?1 AND +ts <= ?2";
const LIST_REQUEST_EVENTS_SQL_FILTERS: &str = "\
  AND (?3 IS NULL OR principal_id = ?3) \
  AND (?4 IS NULL OR lower(model) LIKE ?4 ESCAPE '\\' OR lower(model) LIKE ?16 ESCAPE '\\') \
  AND (?5 IS NULL OR upstream_id = ?5) \
  AND (?17 IS NULL OR upstream_id IN (SELECT CAST(value AS TEXT) FROM json_each(?17))) \
  AND (?13 IS NULL OR thread_id = ?13) \
  AND (?6 IS NULL OR list_status BETWEEN ?6 AND ?7) \
  AND (?15 = 0 OR list_status >= 400 OR error_code IS NOT NULL) \
  AND ( \
        ?11 = 1 \
     OR (?12 IS NOT NULL AND source_kind = ?12) \
     OR (?12 IS NULL AND ?14 IS NOT NULL) \
     OR (?12 IS NULL AND (source_kind IS NULL OR source_kind <> 'renewal')) \
  )";

// Emitted only when `event_kind` is `Some`: a direct equality against the
// effective kind so the query can seek
// `request_events_v1_event_kind_list_order_idx`. The `?14 IS NULL OR ...`
// optional form cannot use that index, so the clause is omitted entirely when
// no kind filter is set. `?14` stays bound in both branches because the
// source_kind clause above still references it.
const LIST_REQUEST_EVENTS_EVENT_KIND_FILTER: &str = " \
  AND CASE WHEN source_kind = 'renewal' THEN 'renewal' \
           ELSE COALESCE(event_kind, 'unclassified') END IN \
      (SELECT value FROM json_each(?14)) ";

const LIST_REQUEST_EVENTS_SQL_TAIL: &str = "\
  AND ( \
        ?8 IS NULL \
     OR list_ts_ms < ?8 \
     OR (list_ts_ms = ?8 \
         AND ?9 IS NOT NULL AND list_event_key < ?9) \
  ) \
ORDER BY list_ts_ms DESC, \
         list_event_key DESC, \
         id DESC \
LIMIT ?10";

#[derive(FromRow)]
pub(super) struct ListRow {
    pub(super) ts: i64,
    pub(super) ts_ms: Option<i64>,
    pub(super) request_id: String,
    pub(super) event_id: Option<String>,
    pub(super) source_kind: Option<String>,
    pub(super) event_kind: Option<String>,
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
    pub(super) thinking_level: Option<String>,
    pub(super) thinking_budget_tokens: Option<i64>,
    pub(super) thinking_tokens: Option<i64>,
    pub(super) service_tier: Option<String>,
    pub(super) status: Option<i64>,
    pub(super) duration_ms: Option<i64>,
    pub(super) error_code: Option<String>,
    pub(super) upstream_error_type: Option<String>,
    pub(super) upstream_error_message: Option<String>,
    pub(super) upstream_error_code: Option<String>,
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
    pub(super) cache_write_input_tokens: Option<i64>,
    pub(super) cache_write_input_tokens_30m: Option<i64>,
    pub(super) cache_write_input_tokens_12h: Option<i64>,
    pub(super) cost_usd_micros: Option<i64>,
    pub(super) cost_input_micros: Option<i64>,
    pub(super) cost_output_micros: Option<i64>,
    pub(super) cost_cache_creation_5m_micros: Option<i64>,
    pub(super) cost_cache_creation_1h_micros: Option<i64>,
    pub(super) cost_cache_read_micros: Option<i64>,
    pub(super) cost_cache_write_micros: Option<i64>,
    pub(super) cost_cache_write_30m_micros: Option<i64>,
    pub(super) cost_cache_write_12h_micros: Option<i64>,
}

pub(super) async fn list_request_events(
    storage: &SqliteStorage,
    query: &RequestEventListQuery,
) -> StorageResult<Vec<RequestEventListItem>> {
    if query.limit == 0 || query.until_unix_secs < query.since_unix_secs {
        return Ok(Vec::new());
    }

    let rows = bind_list_query(list_request_events_sql(query), query)?
        .fetch_all(storage.pool())
        .await
        .map_err(map_sqlx_error)?;

    rows.into_iter().map(list_row_to_item).collect()
}

fn bind_list_query<'q>(
    sql: &'static str,
    query: &'q RequestEventListQuery,
) -> StorageResult<sqlx::query::QueryAs<'q, sqlx::Sqlite, ListRow, sqlx::sqlite::SqliteArguments>> {
    let (status_min, status_max) = query
        .filters
        .status_class
        .map(status_class_range)
        .map_or((None, None), |(min, max)| (Some(min), Some(max)));
    let (source_kind_all, source_kind_exact) = source_kind_filter(query.source_kind.as_deref());
    // Model predicate mirrors the histogram disjunction documented on
    // `model_filter_like_patterns`: `?4` carries the literal-prefix arm and
    // `?16` the `claude-`-namespaced contains arm. An absent/blank/bare
    // `claude-` filter binds NULL to both and disables the clause.
    let [model_prefix_pattern, model_contains_pattern] = query
        .filters
        .model
        .as_deref()
        .and_then(model_filter_like_patterns)
        .map_or([None, None], |[prefix, contains]| {
            [Some(prefix), Some(contains)]
        });
    let event_kinds_json = query
        .filters
        .event_kinds
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| StorageError::Fatal {
            message: format!("request event kind filter serialization failed: {error}"),
        })?;
    let provider_upstream_ids_json = query
        .filters
        .provider_upstream_ids
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| StorageError::Fatal {
            message: format!("request event provider filter serialization failed: {error}"),
        })?;
    Ok(sqlx::query_as::<_, ListRow>(sql)
        .bind(u64_to_i64(
            query.since_unix_secs,
            "request event list since",
        )?)
        .bind(u64_to_i64_upper(query.until_unix_secs))
        .bind(query.filters.principal_id.as_deref())
        .bind(model_prefix_pattern)
        .bind(query.filters.upstream_id.map(|id| id.to_string()))
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
        .bind(event_kinds_json)
        .bind(i64::from(query.filters.errors_only))
        // Parameter `?16`: second arm of the model-pattern disjunction.
        .bind(model_contains_pattern)
        // Parameter `?17`: provider-qualified upstream ids as a JSON array.
        .bind(provider_upstream_ids_json))
}
fn list_request_events_sql(query: &RequestEventListQuery) -> &'static str {
    static SQL: std::sync::LazyLock<[[String; 2]; 2]> = std::sync::LazyLock::new(|| {
        [
            LIST_REQUEST_EVENTS_SQL_BOUNDED_TIME,
            LIST_REQUEST_EVENTS_SQL_UNBOUNDED_TIME,
        ]
        .map(|time_predicate| {
            ["", LIST_REQUEST_EVENTS_EVENT_KIND_FILTER].map(|event_kind_predicate| {
                [
                    LIST_REQUEST_EVENTS_SQL_HEAD,
                    time_predicate,
                    " ",
                    LIST_REQUEST_EVENTS_SQL_FILTERS,
                    event_kind_predicate,
                    LIST_REQUEST_EVENTS_SQL_TAIL,
                ]
                .concat()
            })
        })
    });
    // With no effective time bound, a ts skip-scan can materialize and sort
    // the whole log instead of reading one ordered page. Bounded windows
    // keep ts indexable so narrow historical/future reads still seek.
    let unbounded = query.since_unix_secs == 0 && query.until_unix_secs >= i64::MAX as u64;
    SQL[usize::from(unbounded)][usize::from(query.filters.event_kinds.is_some())].as_str()
}

pub(super) async fn get_request_event(
    storage: &SqliteStorage,
    event_id: &str,
) -> StorageResult<Option<RequestEvent>> {
    static SQL: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
        format!(
            "SELECT {} FROM request_events_v1 WHERE event_id = ?",
            super::request_event_row::REQUEST_EVENT_COLUMNS,
        )
    });
    let row = sqlx::query(SQL.as_str())
        .bind(event_id)
        .fetch_optional(storage.pool())
        .await
        .map_err(map_sqlx_error)?;

    row.as_ref()
        .map(super::request_event_row::row_to_request_event)
        .transpose()
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cc_lb_storage_api::{MetaStore, RequestEventKind, RequestEventStreamFilters};
    use sqlx::{AssertSqlSafe, Execute, Row};

    use super::*;

    async fn explain_list(storage: &SqliteStorage, query: &RequestEventListQuery) -> Vec<String> {
        let mut statement = bind_list_query(list_request_events_sql(query), query)
            .expect("build production list query");
        let sql = format!("EXPLAIN QUERY PLAN {}", list_request_events_sql(query));
        let arguments = statement
            .take_arguments()
            .expect("encode list arguments")
            .expect("list arguments");
        sqlx::query_with(AssertSqlSafe(sql), arguments)
            .fetch_all(storage.pool())
            .await
            .expect("explain production list query")
            .into_iter()
            .map(|row| row.get::<String, _>("detail"))
            .collect()
    }

    fn list_query(
        source_kind: Option<&str>,
        event_kind: Option<RequestEventKind>,
        until_ts_ms: Option<u64>,
        until_event_id: Option<&str>,
    ) -> RequestEventListQuery {
        timed_list_query(
            0,
            u64::MAX,
            source_kind,
            event_kind,
            until_ts_ms,
            until_event_id,
        )
    }

    fn timed_list_query(
        since_unix_secs: u64,
        until_unix_secs: u64,
        source_kind: Option<&str>,
        event_kind: Option<RequestEventKind>,
        until_ts_ms: Option<u64>,
        until_event_id: Option<&str>,
    ) -> RequestEventListQuery {
        RequestEventListQuery {
            since_unix_secs,
            until_unix_secs,
            until_ts_ms,
            until_event_id: until_event_id.map(str::to_owned),
            limit: 100,
            filters: RequestEventStreamFilters {
                event_kinds: event_kind.map(|kind| vec![kind]),
                ..Default::default()
            },
            source_kind: source_kind.map(str::to_owned),
        }
    }

    #[tokio::test]
    async fn analyzed_request_event_list_plans_preserve_order_without_skip_scan() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let database_url = format!("sqlite://{}", temp_dir.path().join("plan.sqlite").display());
        let storage = crate::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .expect("open sqlite");
        storage.initialize().await.expect("initialize sqlite");
        sqlx::query(
            "WITH RECURSIVE seq(n) AS ( \
                 SELECT 0 \
                 UNION ALL \
                 SELECT n + 1 FROM seq WHERE n < 9999 \
             ) \
             INSERT INTO request_events_v1 \
                 (request_id, ts, event_id, principal_id, key_id, model, \
                  list_ts_ms, list_event_key, list_status, source_kind, event_kind) \
             SELECT printf('plan-request-%06d', n), \
                    1900000000 + n, \
                    printf('plan-event-%06d', n), \
                    'principal-single', \
                    NULL, \
                    'claude-sonnet-4-5', \
                    (1900000000 + n) * 1000, \
                    printf('plan-event-%06d', n), \
                    200, \
                    'proxy', \
                    'messages' \
             FROM seq",
        )
        .execute(storage.pool())
        .await
        .expect("seed 10000 request events");
        sqlx::query(
            "INSERT INTO request_events_v1 \
             (request_id, ts, event_id, principal_id, key_id, model, \
              list_ts_ms, list_event_key, list_status, source_kind, event_kind) \
             VALUES \
                ('plan-renewal-1', 1900400000, 'plan-renewal-1', 'principal-single', NULL, \
                 'claude-sonnet-4-5', 1900400000000, 'plan-renewal-1', 200, 'renewal', 'renewal'), \
                ('plan-renewal-2', 1900400001, 'plan-renewal-2', 'principal-single', NULL, \
                 'claude-sonnet-4-5', 1900400001000, 'plan-renewal-2', 200, 'renewal', 'renewal')",
        )
        .execute(storage.pool())
        .await
        .expect("seed renewal request events");
        // Approximate ANALYZE mirrors the production CLI's PRAGMA optimize
        // path, which leaves STAT1-only statistics instead of full STAT4
        // samples: https://www.sqlite.org/lang_analyze.html#approximate_analyze_for_large_databases
        let mut connection = storage
            .pool()
            .acquire()
            .await
            .expect("acquire analyze connection");
        sqlx::query("PRAGMA analysis_limit = 1000")
            .execute(&mut *connection)
            .await
            .expect("set approximate analyze limit");
        sqlx::query("ANALYZE")
            .execute(&mut *connection)
            .await
            .expect("analyze");
        drop(connection);
        storage.pool().close().await;
        let storage =
            crate::open_sqlite_existing(&database_url, Arc::new(cc_lb_clock::SystemClock))
                .await
                .expect("reopen analyzed sqlite");

        let cases = [
            ("default", list_query(None, None, None, None)),
            (
                "messages",
                list_query(None, Some(RequestEventKind::Messages), None, None),
            ),
            ("all", list_query(Some("all"), None, None, None)),
            (
                "renewal-conjunctive",
                list_query(Some("renewal"), Some(RequestEventKind::Renewal), None, None),
            ),
            (
                "cursor-continuation",
                list_query(
                    Some("all"),
                    None,
                    Some(1_900_005_000_000),
                    Some("plan-event-005000"),
                ),
            ),
            (
                "saturated-i64-max",
                timed_list_query(0, i64::MAX as u64, None, None, None, None),
            ),
        ];
        for (name, query) in cases {
            let details = explain_list(&storage, &query).await;
            assert!(
                details
                    .iter()
                    .all(|detail| !detail.contains("USE TEMP B-TREE FOR ORDER BY")),
                "{name} list plan must preserve index ordering: {details:?}"
            );
            assert!(
                details
                    .iter()
                    .all(|detail| !detail.contains("principal_key_ts_idx")),
                "{name} list plan must not skip-scan principal/key/ts: {details:?}"
            );
        }
        let bounded_cases = [
            (
                "bounded-narrow-old",
                timed_list_query(1_900_000_000, 1_900_000_010, None, None, None, None),
            ),
            (
                "bounded-future",
                timed_list_query(2_000_000_000, 2_000_000_010, None, None, None, None),
            ),
            (
                "sentinel-max-minus-one",
                timed_list_query(0, i64::MAX as u64 - 1, None, None, None, None),
            ),
            (
                "bounded-since-one",
                timed_list_query(1, u64::MAX, None, None, None, None),
            ),
        ];
        for (name, query) in bounded_cases {
            let details = explain_list(&storage, &query).await;
            assert!(
                details
                    .iter()
                    .any(|detail| detail.contains("principal_key_ts_idx")),
                "{name} must retain the bounded ts plan variant: {details:?}"
            );
        }
    }
}
