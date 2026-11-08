use async_trait::async_trait;
use cc_lb_storage_api::{
    CacheKeepaliveDecisionRow, CacheKeepaliveEvidenceRow, CacheKeepaliveProjectionStore,
    CacheKeepaliveTurnRow, RequestEvent, RequestEventExportPage, RequestEventHistogramBucket,
    RequestEventHistogramQuery, RequestEventKeyLastUsed, RequestEventKeyLastUsedQuery,
    RequestEventListItem, RequestEventListQuery, RequestEventProjections, RequestEventStore,
    RequestEventStreamFilters, StorageError, StorageResult, model_filter_like_patterns,
};
use sqlx::{AssertSqlSafe, Row};
use std::time::Instant;

use super::request_event_list_sql;
use super::request_event_row::{REQUEST_EVENT_COLUMNS, row_to_request_event};
use crate::{SqliteStorage, map_sqlx_error};

const KEY_SEQUENCE_SCALE: u64 = 1_000_000;

#[async_trait]
impl CacheKeepaliveProjectionStore for SqliteStorage {
    async fn append_cache_keepalive_decision(
        &self,
        decision: &CacheKeepaliveDecisionRow,
    ) -> StorageResult<()> {
        let mut tx = self.begin_immediate().await?;
        insert_keepalive_decision_in_tx(&mut tx, decision).await?;
        tx.commit().await.map_err(map_sqlx_error)
    }
}

#[async_trait]
impl RequestEventStore for SqliteStorage {
    async fn append_request_event(&self, event: &RequestEvent) -> StorageResult<u64> {
        let event_id = storage_event_id(event)?;
        let mut tx = self.begin_immediate().await?;
        let inserted_id = insert_request_event_in_tx(&mut tx, event, event_id).await?;
        let id = match inserted_id {
            Some(id) => id,
            None => select_existing_event_id_in_tx(&mut tx, event_id).await?,
        };
        tx.commit().await.map_err(map_sqlx_error)?;
        i64_to_u64(id, "request event cursor")
    }

    async fn append_request_event_with_projections(
        &self,
        event: &RequestEvent,
        projections: &RequestEventProjections,
    ) -> StorageResult<u64> {
        let event_id = storage_event_id(event)?;
        let mut tx = self.begin_immediate().await?;
        let inserted_id = insert_request_event_in_tx(&mut tx, event, event_id).await?;
        let id = match inserted_id {
            Some(id) => id,
            None => select_existing_event_id_in_tx(&mut tx, event_id).await?,
        };
        if let Some(turn) = projections.turn.as_ref() {
            insert_keepalive_turn_in_tx(&mut tx, turn).await?;
        }
        insert_keepalive_decision_in_tx(&mut tx, &projections.decision).await?;
        for evidence in &projections.evidence {
            insert_keepalive_evidence_in_tx(&mut tx, evidence).await?;
        }
        tx.commit().await.map_err(map_sqlx_error)?;
        i64_to_u64(id, "request event cursor")
    }
    async fn append_request_event_with_evidence(
        &self,
        event: &RequestEvent,
        evidence: &[CacheKeepaliveEvidenceRow],
    ) -> StorageResult<u64> {
        let event_id = storage_event_id(event)?;
        let mut tx = self.begin_immediate().await?;
        let inserted_id = insert_request_event_in_tx(&mut tx, event, event_id).await?;
        let id = match inserted_id {
            Some(id) => id,
            None => select_existing_event_id_in_tx(&mut tx, event_id).await?,
        };
        insert_filtered_keepalive_evidence_in_tx(&mut tx, evidence).await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        i64_to_u64(id, "request event cursor")
    }

    async fn prune_request_events_before(
        &self,
        cutoff_ms_x_1m: u64,
        batch_size: usize,
    ) -> StorageResult<u64> {
        if batch_size == 0 {
            return Ok(0);
        }

        let cutoff_secs = (cutoff_ms_x_1m / KEY_SEQUENCE_SCALE) / 1_000;
        let result = sqlx::query(
            "DELETE FROM request_events_v1 \
             WHERE id IN ( \
                 SELECT id FROM request_events_v1 \
                 WHERE ts < ? \
                 ORDER BY id ASC \
                 LIMIT ? \
             )",
        )
        .bind(u64_to_i64(
            cutoff_secs,
            "request event prune before cutoff",
        )?)
        .bind(u64_to_i64(
            batch_size as u64,
            "request event prune before batch size",
        )?)
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        Ok(result.rows_affected())
    }

    async fn current_request_event_cursor(&self) -> StorageResult<u64> {
        let cursor =
            sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(id), 0) FROM request_events_v1")
                .fetch_one(self.pool())
                .await
                .map_err(map_sqlx_error)?;
        i64_to_u64(cursor, "request event cursor")
    }

    async fn query_request_events_between_cursors(
        &self,
        after: u64,
        until: u64,
        limit: usize,
        filters: &RequestEventStreamFilters,
    ) -> StorageResult<Vec<(u64, RequestEvent)>> {
        if limit == 0 || until <= after {
            return Ok(Vec::new());
        }
        static SQL: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
            format!(
                "SELECT {REQUEST_EVENT_COLUMNS} FROM request_events_v1 \
                     WHERE id > ?1 AND id <= ?2 \
                       AND (?3 IS NULL OR principal_id = ?3) \
                       AND (?4 IS NULL OR thread_id = ?4) \
                       AND (?5 IS NULL OR lower(model) LIKE ?5 ESCAPE '\\' \
                            OR lower(model) LIKE ?6 ESCAPE '\\') \
                       AND (?7 IS NULL OR upstream_id = ?7) \
                       AND (?8 IS NULL OR list_status BETWEEN ?8 AND ?9) \
                       AND (?10 = 0 OR list_status >= 400 OR error_code IS NOT NULL) \
                       AND (?11 IS NULL OR CASE \
                            WHEN source_kind = 'renewal' THEN 'renewal' \
                            ELSE COALESCE(event_kind, 'unclassified') END IN \
                            (SELECT value FROM json_each(?11))) \
                       AND (?12 IS NULL OR source_kind = ?12) \
                       AND (?13 = 0 OR source_kind IS NULL OR source_kind <> 'renewal') \
                       AND (?14 IS NULL OR upstream_id IN \
                            (SELECT CAST(value AS TEXT) FROM json_each(?14))) \
                     ORDER BY id ASC LIMIT ?15"
            )
        });
        let (status_min, status_max) = filters
            .status_class
            .map(request_event_list_sql::status_class_range)
            .map_or((None, None), |(min, max)| (Some(min), Some(max)));
        let model_patterns = filters
            .model
            .as_deref()
            .and_then(model_filter_like_patterns);
        let event_kinds_json = filters
            .event_kinds
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| StorageError::Fatal {
                message: format!("request event kind filter serialization failed: {error}"),
            })?;
        let provider_upstream_ids_json = filters
            .provider_upstream_ids
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| StorageError::Fatal {
                message: format!("request event provider filter serialization failed: {error}"),
            })?;
        let rows = sqlx::query(SQL.as_str())
            .bind(u64_to_i64(after, "request event cursor after")?)
            .bind(u64_to_i64(until, "request event cursor until")?)
            .bind(filters.principal_id.as_deref())
            .bind(filters.thread_id.as_deref())
            .bind(model_patterns.as_ref().map(|patterns| patterns[0].as_str()))
            .bind(model_patterns.as_ref().map(|patterns| patterns[1].as_str()))
            .bind(filters.upstream_id.map(|id| id.to_string()))
            .bind(status_min)
            .bind(status_max)
            .bind(i64::from(filters.errors_only))
            .bind(event_kinds_json)
            .bind(filters.source_kind.as_deref())
            .bind(i64::from(filters.exclude_renewal))
            .bind(provider_upstream_ids_json)
            .bind(usize_to_i64(limit.min(500), "request event cursor limit")?)
            .fetch_all(self.pool())
            .await
            .map_err(map_sqlx_error)?;

        rows.into_iter()
            .map(|row| {
                let cursor = i64_to_u64(
                    row.try_get("id").map_err(map_sqlx_error)?,
                    "request event cursor",
                )?;
                Ok((cursor, row_to_request_event(&row)?))
            })
            .collect()
    }

    async fn list_request_events(
        &self,
        query: &RequestEventListQuery,
    ) -> StorageResult<Vec<RequestEventListItem>> {
        let start = Instant::now();
        let result = request_event_list_sql::list_request_events(self, query).await;
        record_storage_operation("request_event_list", start, &result);
        self.record_pool_metrics();
        result
    }

    async fn request_event_key_last_used(
        &self,
        query: &RequestEventKeyLastUsedQuery,
    ) -> StorageResult<Vec<RequestEventKeyLastUsed>> {
        let start = Instant::now();
        let result = request_event_key_last_used(self, query).await;
        record_storage_operation("request_event_key_last_used", start, &result);
        self.record_pool_metrics();
        result
    }

    async fn request_event_histogram(
        &self,
        query: &RequestEventHistogramQuery,
    ) -> StorageResult<Vec<RequestEventHistogramBucket>> {
        let start = Instant::now();
        let result = request_event_histogram(self, query).await;
        record_storage_operation("request_event_histogram", start, &result);
        self.record_pool_metrics();
        result
    }

    async fn get_request_event(&self, event_id: &str) -> StorageResult<Option<RequestEvent>> {
        request_event_list_sql::get_request_event(self, event_id).await
    }

    /// Writers serialize on `BEGIN IMMEDIATE`, so ids commit in allocation
    /// order and the visible maximum is already stable.
    async fn request_event_stable_seq_ceiling(
        &self,
        max_wait: std::time::Duration,
    ) -> StorageResult<u64> {
        let _ = max_wait;
        self.current_request_event_cursor().await
    }

    async fn request_event_seq_bounds(
        &self,
        from_ts_ms: u64,
        to_ts_ms: u64,
    ) -> StorageResult<Option<(u64, u64)>> {
        if to_ts_ms <= from_ts_ms {
            return Ok(None);
        }
        let (min_id, max_id) = sqlx::query_as::<_, (Option<i64>, Option<i64>)>(
            "SELECT MIN(id), MAX(id) FROM request_events_v1 \
             WHERE list_ts_ms >= ? AND list_ts_ms < ?",
        )
        .bind(u64_to_i64(from_ts_ms, "request event seq bounds from")?)
        .bind(u64_to_i64_upper(to_ts_ms))
        .fetch_one(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        match (min_id, max_id) {
            (Some(min_id), Some(max_id)) => Ok(Some((
                i64_to_u64(min_id, "request event seq bounds min")?,
                i64_to_u64(max_id, "request event seq bounds max")?,
            ))),
            _ => Ok(None),
        }
    }

    async fn export_request_events(
        &self,
        page: &RequestEventExportPage,
    ) -> StorageResult<Vec<(u64, RequestEvent)>> {
        if page.limit == 0 || page.until_seq <= page.after_seq {
            return Ok(Vec::new());
        }
        static SQL: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
            format!(
                "SELECT {REQUEST_EVENT_COLUMNS} FROM request_events_v1 \
                     WHERE id > ? AND id <= ? \
                       AND (? IS NULL OR list_ts_ms >= ?) \
                       AND (? IS NULL OR list_ts_ms < ?) \
                     ORDER BY id ASC LIMIT ?"
            )
        });
        let rows = sqlx::query(SQL.as_str())
            .bind(u64_to_i64(
                page.after_seq,
                "request event export after_seq",
            )?)
            .bind(u64_to_i64_upper(page.until_seq))
            .bind(option_u64_to_i64(
                page.from_ts_ms,
                "request event export from_ts_ms",
            )?)
            .bind(option_u64_to_i64(
                page.from_ts_ms,
                "request event export from_ts_ms",
            )?)
            .bind(page.to_ts_ms.map(u64_to_i64_upper))
            .bind(page.to_ts_ms.map(u64_to_i64_upper))
            .bind(usize_to_i64(page.limit, "request event export limit")?)
            .fetch_all(self.pool())
            .await
            .map_err(map_sqlx_error)?;

        rows.into_iter()
            .map(|row| {
                let cursor = i64_to_u64(
                    row.try_get("id").map_err(map_sqlx_error)?,
                    "request event cursor",
                )?;
                Ok((cursor, row_to_request_event(&row)?))
            })
            .collect()
    }

    async fn request_event_day_counts(
        &self,
        from_ts_ms: u64,
        to_ts_ms: u64,
    ) -> StorageResult<Vec<(u64, u64)>> {
        if to_ts_ms <= from_ts_ms {
            return Ok(Vec::new());
        }
        let rows = sqlx::query_as::<_, (i64, i64)>(
            "SELECT list_ts_ms / 86400000 AS utc_day, COUNT(*) FROM request_events_v1 \
             WHERE list_ts_ms >= ? AND list_ts_ms < ? \
             GROUP BY 1 ORDER BY 1",
        )
        .bind(u64_to_i64(from_ts_ms, "request event day counts from")?)
        .bind(u64_to_i64_upper(to_ts_ms))
        .fetch_all(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        rows.into_iter()
            .map(|(day, count)| {
                Ok((
                    i64_to_u64(day, "request event day index")?,
                    i64_to_u64(count, "request event day count")?,
                ))
            })
            .collect()
    }
}

async fn request_event_key_last_used(
    storage: &SqliteStorage,
    query: &RequestEventKeyLastUsedQuery,
) -> StorageResult<Vec<RequestEventKeyLastUsed>> {
    if query.until_unix_secs < query.since_unix_secs {
        return Ok(Vec::new());
    }

    let rows = sqlx::query_as::<_, (String, i64)>(
        "SELECT key_id, MAX(ts) AS last_used_at_unix_secs \
         FROM request_events_v1 \
         WHERE principal_id = ? \
           AND key_id IS NOT NULL \
           AND key_id <> '' \
           AND ts >= ? \
           AND ts <= ? \
         GROUP BY key_id",
    )
    .bind(query.principal_id.as_str())
    .bind(u64_to_i64(
        query.since_unix_secs,
        "request event key last-used since",
    )?)
    .bind(u64_to_i64_upper(query.until_unix_secs))
    .fetch_all(storage.pool())
    .await
    .map_err(map_sqlx_error)?;

    rows.into_iter()
        .map(|(key_id, last_used_at_unix_secs)| {
            Ok(RequestEventKeyLastUsed {
                key_id,
                last_used_at_unix_secs: i64_to_u64(
                    last_used_at_unix_secs,
                    "request event key last-used timestamp",
                )?,
            })
        })
        .collect()
}

/// The histogram filters its window on `ts` (seconds), exactly like the list
/// query, so the two always count the same rows. That predicate alone cannot
/// use an index: `request_events_v1` has no `ts`-leading index, only
/// `list_ts_ms`-leading ones. The extra `list_ts_ms` bound in the query exists
/// solely to give SQLite a seekable leading-column range; it is deliberately
/// WIDER than the `ts` window so it can never decide membership even if a
/// writer ever sets `ts` and `ts_ms` from separate clock reads.
///
/// Do not tighten this to the exact window.
const HISTOGRAM_INDEX_HINT_SLACK_MS: u64 = 60_000;

const HISTOGRAM_SQL_HEAD: &str = "SELECT MIN(MAX((list_ts_ms - ?1) / ?2, 0), ?3) AS bucket_index, \
        COUNT(*) AS total_count, \
        COUNT(CASE WHEN list_status >= 500 \
            OR (list_status BETWEEN 200 AND 299 \
                AND error_code = 'upstream_stream_error') \
            THEN 1 END) AS error_count \
 FROM request_events_v1 \
 WHERE ts >= ?4 AND ts <= ?5 \
   AND list_ts_ms >= ?14 AND list_ts_ms < ?15 \
   AND (?6 IS NULL OR principal_id = ?6) \
   AND (?7 IS NULL OR lower(model) LIKE ?7 ESCAPE '\\' OR lower(model) LIKE ?18 ESCAPE '\\') \
   AND (?8 IS NULL OR upstream_id = ?8) \
   AND (?19 IS NULL OR upstream_id IN (SELECT CAST(value AS TEXT) FROM json_each(?19))) \
   AND (?13 IS NULL OR thread_id = ?13) \
   AND (?9 IS NULL OR list_status BETWEEN ?9 AND ?10) \
   AND (?17 = 0 OR list_status >= 400 OR error_code IS NOT NULL) \
   AND ( \
         ?11 = 1 \
      OR (?12 IS NOT NULL AND source_kind = ?12) \
      OR (?12 IS NULL AND ?16 IS NOT NULL) \
      OR (?12 IS NULL AND (source_kind IS NULL OR source_kind <> 'renewal')) \
   )";

// Emitted only when `event_kinds` is `Some`: a direct IN predicate against the
// effective kind.
const HISTOGRAM_EVENT_KIND_FILTER: &str = " \
   AND CASE WHEN source_kind = 'renewal' THEN 'renewal' \
            ELSE COALESCE(event_kind, 'unclassified') END IN \
       (SELECT value FROM json_each(?16))";

const HISTOGRAM_SQL_TAIL: &str = " \
 GROUP BY bucket_index";

async fn request_event_histogram(
    storage: &SqliteStorage,
    query: &RequestEventHistogramQuery,
) -> StorageResult<Vec<RequestEventHistogramBucket>> {
    if query.bucket_count == 0
        || query.bucket_ms == 0
        || query.until_unix_secs < query.since_unix_secs
    {
        return Ok(Vec::new());
    }

    let bucket_count = usize::try_from(query.bucket_count).map_err(|_| StorageError::Fatal {
        message: "request event histogram bucket_count cannot be represented as usize".to_owned(),
    })?;
    let range_start_ms = query.since_unix_secs.saturating_mul(1_000);
    let mut buckets = vec![RequestEventHistogramBucket::default(); bucket_count];
    for (index, bucket) in buckets.iter_mut().enumerate() {
        let bucket_start_ms =
            range_start_ms.saturating_add((index as u64).saturating_mul(query.bucket_ms));
        bucket.bucket_start_unix_secs = bucket_start_ms / 1_000;
    }

    let (status_min, status_max) = query
        .filters
        .status_class
        .map(request_event_list_sql::status_class_range)
        .map_or((None, None), |(min, max)| (Some(min), Some(max)));
    let (source_kind_all, source_kind_exact) =
        request_event_list_sql::source_kind_filter(query.source_kind.as_deref());

    // The model predicate is the disjunction documented on
    // `model_filter_like_patterns`: `?7` carries the literal-prefix arm and
    // `?18` the `claude-`-namespaced contains arm. `None` (absent filter)
    // binds NULL to both and disables the clause via `?7 IS NULL`.
    let [model_prefix_pattern, model_contains_pattern] = query
        .filters
        .model
        .as_deref()
        .and_then(model_filter_like_patterns)
        .map_or([None, None], |[prefix, contains]| {
            [Some(prefix), Some(contains)]
        });

    static SQL: std::sync::LazyLock<(String, String)> = std::sync::LazyLock::new(|| {
        (
            [HISTOGRAM_SQL_HEAD, HISTOGRAM_SQL_TAIL].concat(),
            [
                HISTOGRAM_SQL_HEAD,
                HISTOGRAM_EVENT_KIND_FILTER,
                HISTOGRAM_SQL_TAIL,
            ]
            .concat(),
        )
    });
    let sql = if query.filters.event_kinds.is_some() {
        SQL.1.as_str()
    } else {
        SQL.0.as_str()
    };

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
    let rows = sqlx::query_as::<_, (i64, i64, i64)>(sql)
        .bind(u64_to_i64(
            range_start_ms,
            "request event histogram range start",
        )?)
        .bind(u64_to_i64(
            query.bucket_ms,
            "request event histogram bucket width",
        )?)
        .bind(u64_to_i64(
            query.bucket_count.saturating_sub(1),
            "request event histogram last bucket",
        )?)
        .bind(u64_to_i64(
            query.since_unix_secs,
            "request event histogram since",
        )?)
        .bind(u64_to_i64_upper(query.until_unix_secs))
        .bind(query.filters.principal_id.as_deref())
        .bind(model_prefix_pattern)
        .bind(query.filters.upstream_id.map(|id| id.to_string()))
        .bind(status_min)
        .bind(status_max)
        .bind(i64::from(source_kind_all))
        .bind(source_kind_exact)
        .bind(query.filters.thread_id.as_deref())
        .bind(u64_to_i64(
            range_start_ms.saturating_sub(HISTOGRAM_INDEX_HINT_SLACK_MS),
            "request event histogram index hint lower bound",
        )?)
        .bind(u64_to_i64_upper(
            query
                .until_unix_secs
                .saturating_add(1)
                .saturating_mul(1_000)
                .saturating_add(HISTOGRAM_INDEX_HINT_SLACK_MS),
        ))
        .bind(event_kinds_json)
        .bind(i64::from(query.filters.errors_only))
        .bind(model_contains_pattern)
        .bind(provider_upstream_ids_json)
        .fetch_all(storage.pool())
        .await
        .map_err(map_sqlx_error)?;

    for (bucket_index, total_count, error_count) in rows {
        let bucket_index = usize::try_from(bucket_index).map_err(|_| StorageError::Corrupted {
            message: "request event histogram bucket index is negative".to_owned(),
        })?;
        let Some(bucket) = buckets.get_mut(bucket_index) else {
            return Err(StorageError::Corrupted {
                message: "request event histogram bucket index is out of range".to_owned(),
            });
        };
        bucket.total_count = i64_to_u64(total_count, "request event histogram total count")?;
        bucket.error_count = i64_to_u64(error_count, "request event histogram error count")?;
    }

    Ok(buckets)
}

fn record_storage_operation<T>(operation: &'static str, start: Instant, result: &StorageResult<T>) {
    let status = if result.is_ok() { "ok" } else { "error" };
    metrics::histogram!(
        "cc_lb_storage_operation_duration_seconds",
        "store" => "sqlite",
        "operation" => operation,
        "status" => status
    )
    .record(start.elapsed().as_secs_f64());
    if result.is_err() {
        metrics::counter!(
            "cc_lb_storage_operation_errors_total",
            "store" => "sqlite",
            "operation" => operation
        )
        .increment(1);
    }
}

async fn insert_request_event_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    event: &RequestEvent,
    event_id: &str,
) -> StorageResult<Option<i64>> {
    let ts_ms = storage_ts_ms(event)?;
    let ts_secs = ts_ms / 1_000;
    sqlx::query_scalar::<_, i64>(concat!(
        "INSERT INTO request_events_v1 (",
        "request_id, ts, source_kind, source_ref_id, upstream_id, principal_id, ",
        "created_at, key_id, model, upstream_name, thread_id, input_tokens, ",
        "output_tokens, cache_creation_input_tokens, cache_read_input_tokens, ",
        "event_id, error_code, upstream_error_type, upstream_error_message, ",
        "thinking_tokens, tool_use_prompt_tokens, service_tier, cache_creation_input_tokens_5m, ",
        "cache_creation_input_tokens_1h, token_estimate_source, ",
        "thinking_budget_tokens, reasoning_effort, list_ts_ms, ",
        "list_event_key, list_status, list_duration_ms, list_auth_ms, ",
        "list_route_ms, list_limit_reserve_ms, list_json_parse_ms, ",
        "list_cache_structure_ms, list_cache_token_key_ms, ",
        "list_cache_count_lookup_ms, list_cache_tokenizer_queue_ms, ",
        "list_cache_serialize_ms, list_cache_tokenize_ms, ",
        "list_prepare_signer_ms, list_bulkhead_wait_ms, list_dns_ms, ",
        "list_connect_ms, list_connection_reused, list_proxy_setup_ms, ",
        "list_shape_ms, list_sign_ms, list_upstream_ttfb_ms, ",
        "list_upstream_body_ms, list_stream_first_content_delta_ms, ",
        "list_stream_last_content_delta_ms, list_inter_token_avg_ms, ",
        "list_cost_usd_micros, list_cost_input_micros, list_cost_output_micros, ",
        "list_cost_cache_creation_5m_micros, list_cost_cache_creation_1h_micros, ",
        "list_cost_cache_read_micros, observed_session_id, request_kind, ",
        "claude_agent_id, claude_parent_agent_id, parent_session_id, client_app, ",
        "session_id_source, list_request_body_read_ms, list_request_body_bytes, ",
        "list_finalize_ms, list_request_body_first_chunk_ms, ",
        "list_request_body_receive_ms, list_request_body_wait_ms, ",
        "list_request_body_process_ms, list_request_body_chunk_count, ",
        "list_response_body_wait_ms, list_response_body_process_ms, ",
        "list_response_body_downstream_poll_gap_ms, list_retry_overhead_ms, ",
        "event_kind, principal_kind, body_chunk_count, body_bytes, ",
        "stream_message_start_ms, stream_content_block_start_ms, ",
        "stream_message_stop_ms, stream_last_chunk_ms, stream_total_ms, ",
        "sse_event_count, content_delta_count, ping_count, upstream_http_version, ",
        "upstream_request_id, upstream_content_encoding, upstream_content_length, ",
        "upstream_body_bytes, upstream_body_end, upstream_body_error_cause, ",
        "upstream_body_error_io_kind, upstream_body_error_h2_reason, ",
        "upstream_stream_warning_type, upstream_stream_warning_message",
        ") VALUES (",
        "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ",
        "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ",
        "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ",
        "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ",
        "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ",
        "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ",
        "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ",
        "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ",
        "?, ?, ?, ?, ?",
        ") ON CONFLICT(event_id) WHERE event_id IS NOT NULL DO NOTHING ",
        "RETURNING id",
    ))
    .bind(&event.request_id)
    .bind(u64_to_i64(ts_secs, "request event ts")?)
    .bind(event.source_kind.as_deref())
    .bind(event.source_ref_id.as_deref())
    .bind(event.upstream_id.map(|id| id.to_string()))
    .bind(event.principal_id.as_deref())
    .bind(u64_to_i64(ts_secs, "request event created_at")?)
    .bind(event.key_id.as_deref())
    .bind(event.model.as_deref())
    .bind(event.upstream_name.as_deref())
    .bind(event.thread_id.as_deref())
    .bind(option_u64_to_i64(
        event.input_tokens,
        "request event input_tokens",
    )?)
    .bind(option_u64_to_i64(
        event.output_tokens,
        "request event output_tokens",
    )?)
    .bind(option_u64_to_i64(
        event.cache_creation_input_tokens,
        "request event cache_creation_input_tokens",
    )?)
    .bind(option_u64_to_i64(
        event.cache_read_input_tokens,
        "request event cache_read_input_tokens",
    )?)
    .bind(event_id)
    .bind(event.error_code.as_deref())
    .bind(event.upstream_error_type.as_deref())
    .bind(event.upstream_error_message.as_deref())
    .bind(option_u64_to_i64(
        event.thinking_tokens,
        "request event thinking_tokens",
    )?)
    .bind(option_u64_to_i64(
        event.tool_use_prompt_tokens,
        "request event tool_use_prompt_tokens",
    )?)
    .bind(event.service_tier.as_deref())
    .bind(option_u64_to_i64(
        event.cache_creation_input_tokens_5m,
        "request event cache_creation_input_tokens_5m",
    )?)
    .bind(option_u64_to_i64(
        event.cache_creation_input_tokens_1h,
        "request event cache_creation_input_tokens_1h",
    )?)
    .bind(event.token_estimate_source.as_deref())
    .bind(option_u64_to_i64(
        event.thinking_budget_tokens,
        "thinking_budget_tokens",
    )?)
    .bind(event.reasoning_effort.as_deref())
    .bind(u64_to_i64(ts_ms, "request event list_ts_ms")?)
    .bind(event_id)
    .bind(i64::from(event.status))
    .bind(u64_to_i64(
        event.duration_ms,
        "request event list_duration_ms",
    )?)
    .bind(option_u64_to_i64(
        event.auth_ms,
        "request event list_auth_ms",
    )?)
    .bind(option_u64_to_i64(
        event.route_ms,
        "request event list_route_ms",
    )?)
    .bind(option_u64_to_i64(
        event.limit_reserve_ms,
        "request event list_limit_reserve_ms",
    )?)
    .bind(event.json_parse_ms)
    .bind(event.cache_structure_ms)
    .bind(event.cache_token_key_ms)
    .bind(event.cache_count_lookup_ms)
    .bind(event.cache_tokenizer_queue_ms)
    .bind(event.cache_serialize_ms)
    .bind(event.cache_tokenize_ms)
    .bind(event.prepare_signer_ms)
    .bind(option_u64_to_i64(
        event.bulkhead_wait_ms,
        "request event list_bulkhead_wait_ms",
    )?)
    .bind(option_u64_to_i64(
        event.dns_ms,
        "request event list_dns_ms",
    )?)
    .bind(option_u64_to_i64(
        event.connect_ms,
        "request event list_connect_ms",
    )?)
    .bind(event.connection_reused.map(i64::from))
    .bind(option_u64_to_i64(
        event.proxy_setup_ms,
        "request event list_proxy_setup_ms",
    )?)
    .bind(option_u64_to_i64(
        event.shape_ms,
        "request event list_shape_ms",
    )?)
    .bind(option_u64_to_i64(
        event.sign_ms,
        "request event list_sign_ms",
    )?)
    .bind(option_u64_to_i64(
        event.upstream_ttfb_ms,
        "request event list_upstream_ttfb_ms",
    )?)
    .bind(option_u64_to_i64(
        event.upstream_body_ms,
        "request event list_upstream_body_ms",
    )?)
    .bind(option_u64_to_i64(
        event.stream_first_content_delta_ms,
        "request event list_stream_first_content_delta_ms",
    )?)
    .bind(option_u64_to_i64(
        event.stream_last_content_delta_ms,
        "request event list_stream_last_content_delta_ms",
    )?)
    .bind(option_u64_to_i64(
        event.inter_token_avg_ms,
        "request event list_inter_token_avg_ms",
    )?)
    .bind(event.cost_usd_micros)
    .bind(event.cost_input_micros)
    .bind(event.cost_output_micros)
    .bind(event.cost_cache_creation_5m_micros)
    .bind(event.cost_cache_creation_1h_micros)
    .bind(event.cost_cache_read_micros)
    .bind(event.observed_session_id.as_deref())
    .bind(event.request_kind.as_deref())
    .bind(event.claude_agent_id.as_deref())
    .bind(event.claude_parent_agent_id.as_deref())
    .bind(event.parent_session_id.as_deref())
    .bind(event.client_app.as_deref())
    .bind(event.session_id_source.as_deref())
    .bind(option_u64_to_i64(
        event.request_body_read_ms,
        "request event list_request_body_read_ms",
    )?)
    .bind(option_u64_to_i64(
        event.request_body_bytes,
        "request event list_request_body_bytes",
    )?)
    .bind(option_u64_to_i64(
        event.finalize_ms,
        "request event list_finalize_ms",
    )?)
    .bind(event.request_body_first_chunk_ms)
    .bind(event.request_body_receive_ms)
    .bind(event.request_body_wait_ms)
    .bind(event.request_body_process_ms)
    .bind(option_u64_to_i64(
        event.request_body_chunk_count,
        "request event list_request_body_chunk_count",
    )?)
    .bind(event.response_body_wait_ms)
    .bind(event.response_body_process_ms)
    .bind(event.response_body_downstream_poll_gap_ms)
    .bind(event.retry_overhead_ms)
    .bind(event.event_kind.map(|kind| kind.as_str()))
    .bind(event.principal_kind.as_deref())
    .bind(option_u64_to_i64(
        event.body_chunk_count,
        "request event body_chunk_count",
    )?)
    .bind(option_u64_to_i64(
        event.body_bytes,
        "request event body_bytes",
    )?)
    .bind(option_u64_to_i64(
        event.stream_message_start_ms,
        "request event stream_message_start_ms",
    )?)
    .bind(option_u64_to_i64(
        event.stream_content_block_start_ms,
        "request event stream_content_block_start_ms",
    )?)
    .bind(option_u64_to_i64(
        event.stream_message_stop_ms,
        "request event stream_message_stop_ms",
    )?)
    .bind(option_u64_to_i64(
        event.stream_last_chunk_ms,
        "request event stream_last_chunk_ms",
    )?)
    .bind(option_u64_to_i64(
        event.stream_total_ms,
        "request event stream_total_ms",
    )?)
    .bind(option_u64_to_i64(
        event.sse_event_count,
        "request event sse_event_count",
    )?)
    .bind(option_u64_to_i64(
        event.content_delta_count,
        "request event content_delta_count",
    )?)
    .bind(option_u64_to_i64(
        event.ping_count,
        "request event ping_count",
    )?)
    .bind(event.upstream_http_version.as_deref())
    .bind(event.upstream_request_id.as_deref())
    .bind(event.upstream_content_encoding.as_deref())
    .bind(option_u64_to_i64(
        event.upstream_content_length,
        "request event upstream_content_length",
    )?)
    .bind(option_u64_to_i64(
        event.upstream_body_bytes,
        "request event upstream_body_bytes",
    )?)
    .bind(event.upstream_body_end.as_deref())
    .bind(event.upstream_body_error_cause.as_deref())
    .bind(event.upstream_body_error_io_kind.as_deref())
    .bind(event.upstream_body_error_h2_reason.as_deref())
    .bind(event.upstream_stream_warning_type.as_deref())
    .bind(event.upstream_stream_warning_message.as_deref())
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx_error)
}

async fn select_existing_event_id_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    event_id: &str,
) -> StorageResult<i64> {
    sqlx::query_scalar::<_, i64>("SELECT id FROM request_events_v1 WHERE event_id = ?")
        .bind(event_id)
        .fetch_one(&mut **tx)
        .await
        .map_err(map_sqlx_error)
}

async fn insert_keepalive_turn_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    row: &CacheKeepaliveTurnRow,
) -> StorageResult<()> {
    sqlx::query(
        "INSERT INTO cache_keepalive_turns \
         (source_ref_id, session_key_hash, principal_id, accounting_key_id, upstream_id, cycle_id, model, input_tokens, output_tokens, cache_creation_input_tokens, cache_creation_input_tokens_5m, cache_creation_input_tokens_1h, cache_read_input_tokens, cost_micros, hit_miss, ts) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(source_ref_id) DO NOTHING",
    )
    .bind(&row.source_ref_id)
    .bind(&row.session_key_hash)
    .bind(&row.principal_id)
    .bind(row.accounting_key_id.as_deref())
    .bind(row.upstream_id.to_string())
    .bind(row.cycle_id.map(|id| id.to_string()))
    .bind(&row.model)
    .bind(u64_to_i64(row.input_tokens, "cache keepalive input_tokens")?)
    .bind(u64_to_i64(row.output_tokens, "cache keepalive output_tokens")?)
    .bind(u64_to_i64(
        row.cache_creation_input_tokens,
        "cache keepalive cache_creation_input_tokens",
    )?)
    .bind(u64_to_i64(
        row.cache_creation_input_tokens_5m,
        "cache keepalive cache_creation_input_tokens_5m",
    )?)
    .bind(u64_to_i64(
        row.cache_creation_input_tokens_1h,
        "cache keepalive cache_creation_input_tokens_1h",
    )?)
    .bind(u64_to_i64(
        row.cache_read_input_tokens,
        "cache keepalive cache_read_input_tokens",
    )?)
    .bind(row.cost_micros)
    .bind(&row.hit_miss)
    .bind(u64_to_i64(row.ts, "cache keepalive ts")?)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

async fn insert_keepalive_decision_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    row: &CacheKeepaliveDecisionRow,
) -> StorageResult<()> {
    let config_snapshot = row
        .config_snapshot
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    sqlx::query(
        "INSERT INTO cache_keepalive_decisions \
         (source_ref_id, principal_id, session_key_hash, upstream_id, decision, reason, error, generation, ttl, config_snapshot, last_message_at_ms, ts) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(source_ref_id) DO NOTHING",
    )
    .bind(&row.source_ref_id)
    .bind(&row.principal_id)
    .bind(row.session_key_hash.as_deref())
    .bind(row.upstream_id.to_string())
    .bind(&row.decision)
    .bind(&row.reason)
    .bind(row.error.as_deref())
    .bind(u64_to_i64(row.generation, "cache keepalive generation")?)
    .bind(cache_keepalive_ttl_to_db(row.ttl))
    .bind(config_snapshot)
    .bind(u64_to_i64(
        row.last_message_at_ms,
        "cache keepalive decision last_message_at_ms",
    )?)
    .bind(u64_to_i64(row.ts, "cache keepalive decision ts")?)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

async fn insert_filtered_keepalive_evidence_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    evidence: &[CacheKeepaliveEvidenceRow],
) -> StorageResult<()> {
    for row in evidence
        .iter()
        .filter(|row| row.evidence_kind != "follow_up")
    {
        insert_keepalive_evidence_in_tx(tx, row).await?;
    }
    let mut candidates = evidence
        .iter()
        .filter(|row| {
            row.evidence_kind == "follow_up"
                && row.cache_read_input_tokens.is_some_and(|tokens| tokens > 0)
                && row.prefix_hash.is_some()
        })
        .collect::<Vec<_>>();
    candidates.sort_unstable_by(|a, b| {
        a.request_start_ms
            .cmp(&b.request_start_ms)
            .then_with(|| a.evidence_id.cmp(&b.evidence_id))
    });
    if candidates.is_empty() {
        return Ok(());
    }
    let principals = candidates
        .iter()
        .map(|row| row.principal_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let upstreams = candidates
        .iter()
        .map(|row| row.upstream_id.to_string())
        .collect::<std::collections::BTreeSet<_>>();
    let min_request_start_ms = candidates
        .iter()
        .map(|row| row.request_start_ms)
        .min()
        .unwrap_or(0);
    let min_renewal_start_ms = min_request_start_ms.saturating_sub(3_600_000);
    let max_request_start_ms = candidates
        .iter()
        .map(|row| row.request_start_ms)
        .max()
        .unwrap_or(0);
    let principal_values = principals.iter().cloned().collect::<Vec<_>>();
    let upstream_values = upstreams.iter().cloned().collect::<Vec<_>>();
    let running_sql = format!(
        "SELECT DISTINCT principal_id, upstream_id
         FROM cache_keepalive_sessions
         WHERE status = 'active' AND enqueue_state = 'running'
           AND principal_id IN ({}) AND upstream_id IN ({})",
        std::iter::repeat_n("?", principals.len())
            .collect::<Vec<_>>()
            .join(", "),
        std::iter::repeat_n("?", upstreams.len())
            .collect::<Vec<_>>()
            .join(", "),
    );
    let mut running_query = sqlx::query(AssertSqlSafe(running_sql));
    for value in &principal_values {
        running_query = running_query.bind(value);
    }
    for value in &upstream_values {
        running_query = running_query.bind(value);
    }
    let running_pairs = running_query
        .fetch_all(&mut **tx)
        .await
        .map_err(map_sqlx_error)?
        .into_iter()
        .map(|row| {
            Ok((
                row.try_get::<String, _>("principal_id")
                    .map_err(map_sqlx_error)?,
                row.try_get::<String, _>("upstream_id")
                    .map_err(map_sqlx_error)?,
            ))
        })
        .collect::<StorageResult<std::collections::HashSet<_>>>()?;
    let recent_sql = format!(
        "SELECT DISTINCT cycle_id
         FROM cache_keepalive_evidence
         WHERE evidence_kind = 'renewal'
           AND cycle_id IS NOT NULL
           AND prefix_policy_version IS NOT NULL
           AND prefix_schema_version IS NOT NULL
           AND principal_id IN ({})
           AND upstream_id IN ({})
           AND request_start_ms >= ?
           AND request_start_ms <= ?",
        std::iter::repeat_n("?", principals.len())
            .collect::<Vec<_>>()
            .join(", "),
        std::iter::repeat_n("?", upstreams.len())
            .collect::<Vec<_>>()
            .join(", "),
    );
    let mut recent_query = sqlx::query(AssertSqlSafe(recent_sql));
    for value in &principal_values {
        recent_query = recent_query.bind(value);
    }
    for value in &upstream_values {
        recent_query = recent_query.bind(value);
    }
    let recent_renewals = recent_query
        .bind(u64_to_i64(
            min_renewal_start_ms,
            "cache keepalive evidence request_start_ms",
        )?)
        .bind(u64_to_i64(
            max_request_start_ms,
            "cache keepalive evidence request_start_ms",
        )?)
        .fetch_all(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
    let cycle_ids = recent_renewals
        .into_iter()
        .map(|row| {
            row.try_get::<Option<String>, _>("cycle_id")
                .map_err(map_sqlx_error)
        })
        .collect::<StorageResult<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    if running_pairs.is_empty() && cycle_ids.is_empty() {
        return Ok(());
    }
    let mut existing = Vec::new();
    if !cycle_ids.is_empty() {
        let renewal_sql = format!(
            "SELECT evidence_id, cycle_id, evidence_kind, request_start_ms, prefix_hash,
                    prefix_policy_version, prefix_schema_version, principal_id, upstream_id,
                    model, ttl, cache_read_input_tokens
             FROM cache_keepalive_evidence
             WHERE evidence_kind = 'renewal'
               AND cycle_id IN ({})
               AND request_start_ms <= ?",
            std::iter::repeat_n("?", cycle_ids.len())
                .collect::<Vec<_>>()
                .join(", "),
        );
        let mut renewal_query = sqlx::query(AssertSqlSafe(renewal_sql));
        for value in &cycle_ids {
            renewal_query = renewal_query.bind(value);
        }
        let renewal_rows = renewal_query
            .bind(u64_to_i64(
                max_request_start_ms,
                "cache keepalive evidence request_start_ms",
            )?)
            .fetch_all(&mut **tx)
            .await
            .map_err(map_sqlx_error)?;
        let min_cycle_renewal_start_ms = renewal_rows
            .iter()
            .filter_map(|row| row.try_get::<i64, _>("request_start_ms").ok())
            .min()
            .unwrap_or(0);
        existing.extend(renewal_rows);
        let follow_up_sql = format!(
            "SELECT evidence_id, cycle_id, evidence_kind, request_start_ms, prefix_hash,
                    prefix_policy_version, prefix_schema_version, principal_id, upstream_id,
                    model, ttl, cache_read_input_tokens
             FROM cache_keepalive_evidence
             WHERE evidence_kind = 'follow_up'
               AND principal_id IN ({})
               AND upstream_id IN ({})
               AND request_start_ms >= ?
               AND request_start_ms <= ?",
            std::iter::repeat_n("?", principals.len())
                .collect::<Vec<_>>()
                .join(", "),
            std::iter::repeat_n("?", upstreams.len())
                .collect::<Vec<_>>()
                .join(", "),
        );
        let mut follow_up_query = sqlx::query(AssertSqlSafe(follow_up_sql));
        for value in &principal_values {
            follow_up_query = follow_up_query.bind(value);
        }
        for value in &upstream_values {
            follow_up_query = follow_up_query.bind(value);
        }
        let follow_up_rows = follow_up_query
            .bind(min_cycle_renewal_start_ms)
            .bind(u64_to_i64(
                max_request_start_ms,
                "cache keepalive evidence request_start_ms",
            )?)
            .fetch_all(&mut **tx)
            .await
            .map_err(map_sqlx_error)?;
        existing.extend(follow_up_rows);
    }

    let mut accepted_cycles = std::collections::HashSet::new();
    for candidate in candidates {
        let candidate_start = u64_to_i64(
            candidate.request_start_ms,
            "cache keepalive evidence request_start_ms",
        )?;
        let running = running_pairs.contains(&(
            candidate.principal_id.clone(),
            candidate.upstream_id.to_string(),
        ));
        let mut qualifying_cycles = std::collections::HashSet::new();
        if !running {
            for renewal in &existing {
                if renewal
                    .try_get::<String, _>("evidence_kind")
                    .map_err(map_sqlx_error)?
                    != "renewal"
                {
                    continue;
                }
                let Some(cycle_id) = renewal
                    .try_get::<Option<String>, _>("cycle_id")
                    .map_err(map_sqlx_error)?
                    .and_then(|value| uuid::Uuid::parse_str(&value).ok())
                else {
                    continue;
                };
                if renewal
                    .try_get::<Option<String>, _>("prefix_policy_version")
                    .map_err(map_sqlx_error)?
                    .is_none()
                    || renewal
                        .try_get::<Option<String>, _>("prefix_schema_version")
                        .map_err(map_sqlx_error)?
                        .is_none()
                    || renewal
                        .try_get::<String, _>("principal_id")
                        .map_err(map_sqlx_error)?
                        != candidate.principal_id
                    || renewal
                        .try_get::<String, _>("upstream_id")
                        .map_err(map_sqlx_error)?
                        != candidate.upstream_id.to_string()
                    || renewal
                        .try_get::<String, _>("model")
                        .map_err(map_sqlx_error)?
                        != candidate.model
                    || renewal
                        .try_get::<Option<String>, _>("prefix_hash")
                        .map_err(map_sqlx_error)?
                        .as_deref()
                        != candidate.prefix_hash.as_deref()
                {
                    continue;
                }
                let renewal_start = renewal
                    .try_get::<i64, _>("request_start_ms")
                    .map_err(map_sqlx_error)?;
                let ttl_ms = match renewal
                    .try_get::<String, _>("ttl")
                    .map_err(map_sqlx_error)?
                    .as_str()
                {
                    "1h" => 60 * 60 * 1_000,
                    _ => 300_000,
                };
                if candidate_start <= renewal_start
                    || candidate_start > renewal_start.saturating_add(ttl_ms)
                {
                    continue;
                }
                qualifying_cycles.insert(cycle_id);
            }
        }

        let mut accepted = running;
        if !running {
            for cycle_id in qualifying_cycles.iter().copied() {
                if accepted_cycles.contains(&cycle_id) {
                    continue;
                }
                // A persisted follow-up blocks this candidate if it qualifies
                // against any renewal in the same cycle, not only the renewal
                // that first matched the candidate.
                let earlier_follow_up = existing.iter().any(|follow_up| {
                    let Ok(kind) = follow_up.try_get::<String, _>("evidence_kind") else {
                        return false;
                    };
                    if kind != "follow_up"
                        || !follow_up
                            .try_get::<Option<i64>, _>("cache_read_input_tokens")
                            .ok()
                            .flatten()
                            .is_some_and(|tokens| tokens > 0)
                        || follow_up
                            .try_get::<Option<String>, _>("prefix_hash")
                            .ok()
                            .flatten()
                            .is_none()
                    {
                        return false;
                    }
                    let Ok(follow_up_start) = follow_up.try_get::<i64, _>("request_start_ms")
                    else {
                        return false;
                    };
                    let Ok(follow_up_id) = follow_up.try_get::<String, _>("evidence_id") else {
                        return false;
                    };
                    if !(follow_up_start < candidate_start
                        || (follow_up_start == candidate_start
                            && follow_up_id.as_str() < candidate.evidence_id.as_str()))
                    {
                        return false;
                    }
                    existing.iter().any(|renewal| {
                        let Ok(kind) = renewal.try_get::<String, _>("evidence_kind") else {
                            return false;
                        };
                        if kind != "renewal"
                            || renewal
                                .try_get::<Option<String>, _>("cycle_id")
                                .ok()
                                .flatten()
                                .and_then(|value| uuid::Uuid::parse_str(&value).ok())
                                != Some(cycle_id)
                            || renewal
                                .try_get::<Option<String>, _>("prefix_policy_version")
                                .ok()
                                .flatten()
                                .is_none()
                            || renewal
                                .try_get::<Option<String>, _>("prefix_schema_version")
                                .ok()
                                .flatten()
                                .is_none()
                        {
                            return false;
                        }
                        let Ok(renewal_start) = renewal.try_get::<i64, _>("request_start_ms")
                        else {
                            return false;
                        };
                        let Ok(ttl) = renewal.try_get::<String, _>("ttl") else {
                            return false;
                        };
                        let ttl_ms = if ttl == "1h" { 3_600_000 } else { 300_000 };
                        follow_up_start > renewal_start
                            && follow_up_start <= renewal_start.saturating_add(ttl_ms)
                            && follow_up.try_get::<String, _>("principal_id").ok()
                                == renewal.try_get::<String, _>("principal_id").ok()
                            && follow_up.try_get::<String, _>("upstream_id").ok()
                                == renewal.try_get::<String, _>("upstream_id").ok()
                            && follow_up.try_get::<String, _>("model").ok()
                                == renewal.try_get::<String, _>("model").ok()
                            && follow_up.try_get::<Option<String>, _>("prefix_hash").ok()
                                == renewal.try_get::<Option<String>, _>("prefix_hash").ok()
                    })
                });
                if !earlier_follow_up {
                    accepted = true;
                    break;
                }
            }
        }
        if accepted {
            insert_keepalive_evidence_in_tx(tx, candidate).await?;
            if !running {
                accepted_cycles.extend(qualifying_cycles);
            }
        }
    }
    Ok(())
}

async fn insert_keepalive_evidence_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    row: &CacheKeepaliveEvidenceRow,
) -> StorageResult<()> {
    sqlx::query(
        "INSERT INTO cache_keepalive_evidence \
         (evidence_id, cycle_id, evidence_kind, request_start_ms, prefix_fingerprint, prefix_hash, prefix_policy_version, prefix_schema_version,
          principal_id, upstream_id, model, ttl, hit_miss, cache_read_input_tokens, cost_micros, request_ref, event_ref, created_at_ms)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(evidence_id) DO NOTHING",
    )
    .bind(&row.evidence_id)
    .bind(row.cycle_id.map(|id| id.to_string()))
    .bind(&row.evidence_kind)
    .bind(u64_to_i64(
        row.request_start_ms,
        "cache keepalive evidence request_start_ms",
    )?)
    .bind(row.prefix_fingerprint.as_deref())
    .bind(row.prefix_hash.as_deref())
    .bind(row.prefix_policy_version.as_deref())
    .bind(row.prefix_schema_version.as_deref())
    .bind(&row.principal_id)
    .bind(row.upstream_id.to_string())
    .bind(&row.model)
    .bind(cache_keepalive_ttl_to_db(row.ttl))
    .bind(row.hit_miss.as_deref())
    .bind(
        row.cache_read_input_tokens
            .map(|value| u64_to_i64(value, "cache keepalive evidence cache_read_input_tokens"))
            .transpose()?,
    )
    .bind(row.cost_micros)
    .bind(row.request_ref.as_deref())
    .bind(row.event_ref.as_deref())
    .bind(u64_to_i64(
        row.request_start_ms,
        "cache keepalive evidence created_at_ms",
    )?)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

fn cache_keepalive_ttl_to_db(ttl: cc_lb_storage_api::CacheTtl) -> &'static str {
    match ttl {
        cc_lb_storage_api::CacheTtl::Ttl5m => "5m",
        cc_lb_storage_api::CacheTtl::Ttl1h => "1h",
        cc_lb_storage_api::CacheTtl::Ttl30m => "30m",
        cc_lb_storage_api::CacheTtl::Ttl12h => "12h",
    }
}

fn storage_event_id(event: &RequestEvent) -> StorageResult<&str> {
    event
        .event_id
        .as_deref()
        .ok_or_else(|| StorageError::InvalidInput {
            field: "request_event.event_id".to_owned(),
            reason: "must be set".to_owned(),
        })
}

fn storage_ts_ms(event: &RequestEvent) -> StorageResult<u64> {
    event.ts_ms.ok_or_else(|| StorageError::InvalidInput {
        field: "request_event.ts_ms".to_owned(),
        reason: "must be set".to_owned(),
    })
}

fn option_u64_to_i64(value: Option<u64>, field: &str) -> StorageResult<Option<i64>> {
    value.map(|value| u64_to_i64(value, field)).transpose()
}

pub(super) fn u64_to_i64(value: u64, field: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| StorageError::Fatal {
        message: format!("{field} cannot be represented as sqlite INTEGER"),
    })
}

pub(super) fn i64_to_u64(value: i64, field: &str) -> StorageResult<u64> {
    u64::try_from(value).map_err(|_| StorageError::Corrupted {
        message: format!("{field} is negative"),
    })
}

pub(super) fn u64_to_i64_upper(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

pub(super) fn usize_to_i64(value: usize, field: &str) -> StorageResult<i64> {
    i64::try_from(value).or_else(|_| {
        if value == usize::MAX {
            Ok(i64::MAX)
        } else {
            Err(StorageError::Fatal {
                message: format!("{field} cannot be represented as sqlite INTEGER"),
            })
        }
    })
}
