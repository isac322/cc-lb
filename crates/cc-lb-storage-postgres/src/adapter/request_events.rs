use async_trait::async_trait;
use cc_lb_storage_api::{
    CacheKeepaliveDecisionRow, CacheKeepaliveProjectionStore, CacheKeepaliveTurnRow, RequestEvent,
    RequestEventHistogramBucket, RequestEventHistogramQuery, RequestEventKeyLastUsed,
    RequestEventKeyLastUsedQuery, RequestEventKeyUsageBucket, RequestEventKeyUsageQuery,
    RequestEventListItem, RequestEventListQuery, RequestEventPrincipalCostBucket,
    RequestEventPrincipalCostQuery, RequestEventProjections, RequestEventStore,
    RequestEventStreamFilters, StorageError, StorageResult, model_filter_like_pattern,
    model_filter_matches, normalize_usage_rollup_dimension,
};
use sqlx::{AssertSqlSafe, Postgres, QueryBuilder};
use std::collections::BTreeMap;

use super::request_event_list_sql::{self, status_class_range, upstream_as_str};
use crate::{
    adapter::{
        PostgresStorage, i64_to_u64, u64_to_i64, unix_secs_to_datetime,
        unix_secs_to_datetime_lower, unix_secs_to_datetime_upper,
    },
    error_map::map_sqlx_error,
};

const KEY_SEQUENCE_SCALE: u64 = 1_000_000;

#[async_trait]
impl CacheKeepaliveProjectionStore for PostgresStorage {
    async fn append_cache_keepalive_decision(
        &self,
        decision: &CacheKeepaliveDecisionRow,
    ) -> StorageResult<()> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        insert_keepalive_decision_in_tx(&mut tx, decision).await?;
        tx.commit().await.map_err(map_sqlx_error)
    }
}

#[async_trait]
impl RequestEventStore for PostgresStorage {
    async fn append_request_event(&self, event: &RequestEvent) -> StorageResult<u64> {
        let event_id = storage_event_id(event);
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let inserted_seq = insert_request_event_in_tx(&mut tx, event, &event_id).await?;
        let seq = match inserted_seq {
            Some(seq) => seq,
            None => select_existing_event_id_in_tx(&mut tx, &event_id).await?,
        };
        tx.commit().await.map_err(map_sqlx_error)?;
        i64_to_u64(seq, "request event cursor")
    }

    async fn append_request_event_with_projections(
        &self,
        event: &RequestEvent,
        projections: &RequestEventProjections,
    ) -> StorageResult<u64> {
        let event_id = storage_event_id(event);
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let inserted_seq = insert_request_event_in_tx(&mut tx, event, &event_id).await?;
        let seq = match inserted_seq {
            Some(seq) => seq,
            None => select_existing_event_id_in_tx(&mut tx, &event_id).await?,
        };
        if let Some(turn) = projections.turn.as_ref() {
            insert_keepalive_turn_in_tx(&mut tx, turn).await?;
        }
        insert_keepalive_decision_in_tx(&mut tx, &projections.decision).await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        i64_to_u64(seq, "request event cursor")
    }

    async fn query_request_events(
        &self,
        since: u64,
        until: u64,
        limit: usize,
    ) -> StorageResult<Vec<RequestEvent>> {
        if limit == 0 || until < since {
            return Ok(Vec::new());
        }
        let Some(since) = unix_secs_to_datetime_lower(since, "request event since")? else {
            return Ok(Vec::new());
        };
        let until = unix_secs_to_datetime_upper(until, "request event until")?;

        let rows = sqlx::query_as::<_, (Option<String>, Option<String>, Vec<u8>)>(
            "SELECT source_kind, source_ref_id, payload FROM request_events_v1              WHERE ts >= $1 AND ($2::timestamptz IS NULL OR ts <= $2)              ORDER BY seq ASC LIMIT $3",
        )
        .bind(since)
        .bind(until)
        .bind(u64_to_i64(limit as u64, "request event limit")?)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        rows.into_iter()
            .map(|(source_kind, source_ref_id, payload)| {
                request_event_from_storage(&payload, source_kind, source_ref_id)
            })
            .collect()
    }

    async fn query_recent_request_events(
        &self,
        since: u64,
        until: u64,
        limit: usize,
    ) -> StorageResult<Vec<RequestEvent>> {
        if limit == 0 || until < since {
            return Ok(Vec::new());
        }
        let Some(since) = unix_secs_to_datetime_lower(since, "request event since")? else {
            return Ok(Vec::new());
        };
        let until = unix_secs_to_datetime_upper(until, "request event until")?;

        let rows = sqlx::query_as::<_, (Option<String>, Option<String>, Vec<u8>)>(
            "SELECT source_kind, source_ref_id, payload FROM request_events_v1              WHERE ts >= $1 AND ($2::timestamptz IS NULL OR ts <= $2)              ORDER BY seq DESC LIMIT $3",
        )
        .bind(since)
        .bind(until)
        .bind(u64_to_i64(limit as u64, "request event limit")?)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        rows.into_iter()
            .map(|(source_kind, source_ref_id, payload)| {
                request_event_from_storage(&payload, source_kind, source_ref_id)
            })
            .collect()
    }

    async fn prune_request_events_before(
        &self,
        cutoff_ms_x_1m: u64,
        batch_size: usize,
    ) -> StorageResult<u64> {
        if batch_size == 0 {
            return Ok(0);
        }

        let cutoff_ms = cutoff_ms_x_1m / KEY_SEQUENCE_SCALE;
        let result = sqlx::query(
            "DELETE FROM request_events_v1              WHERE seq IN (                 SELECT seq FROM request_events_v1                 WHERE list_ts_ms < $1                 ORDER BY seq ASC                 LIMIT $2             )",
        )
        .bind(u64_to_i64(
            cutoff_ms,
            "request event prune before cutoff",
        )?)
        .bind(u64_to_i64(
            batch_size as u64,
            "request event prune before batch size",
        )?)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        Ok(result.rows_affected())
    }

    /// Returns the highest `seq` that is safe to treat as "current" for a
    /// subsequent `query_request_events_between_cursors` call.
    ///
    /// Uses the same `pg_snapshot_xmin` horizon as that query. A plain
    /// `MAX(seq)` is unsound here: `BIGSERIAL` values are allocated by
    /// `nextval()` before commit, so a transaction with a lower xid can
    /// commit *after* one with a higher xid whose row already counts toward
    /// `MAX(seq)`. If callers advance a "last seen" bookmark to that
    /// uncapped `MAX(seq)`, the lower-xid row can still be behind
    /// `pg_snapshot_xmin` at query time, be excluded by the between-cursors
    /// filter, and then be permanently skipped once the bookmark moves past
    /// it. Capping the max to rows already below the xmin horizon keeps the
    /// two queries consistent: anything counted here is guaranteed to be
    /// returned by a between-cursors call using an equal-or-later snapshot,
    /// because `pg_snapshot_xmin` never decreases over time.
    async fn current_request_event_cursor(&self) -> StorageResult<u64> {
        let cursor = sqlx::query_scalar::<_, i64>(include_str!("current_request_event_cursor.sql"))
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_error)?
            .unwrap_or(0);
        i64_to_u64(cursor, "request event cursor")
    }

    /// Query committed request events in cursor order without skipping rows whose
    /// `BIGSERIAL` value was allocated by a transaction that has not become visible yet.
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

        let rows = sqlx::query_as::<_, (i64, Option<String>, Option<String>, Vec<u8>)>(
            "SELECT seq, source_kind, source_ref_id, payload FROM request_events_v1 \
             WHERE seq > $1 AND seq <= $2 \
               AND COALESCE(tx_id, '0'::xid8) < pg_snapshot_xmin(pg_current_snapshot()) \
             ORDER BY seq ASC LIMIT $3",
        )
        .bind(u64_to_i64(after, "request event cursor after")?)
        .bind(u64_to_i64(until, "request event cursor until")?)
        .bind(u64_to_i64(
            limit.min(500) as u64,
            "request event cursor limit",
        )?)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        rows.into_iter()
            .map(|(seq, source_kind, source_ref_id, payload)| {
                let cursor = i64_to_u64(seq, "request event cursor")?;
                let event = request_event_from_storage(&payload, source_kind, source_ref_id)?;
                Ok((cursor, event))
            })
            .filter(|result| match result {
                Ok((_, event)) => request_event_matches_filters(event, filters),
                Err(_) => true,
            })
            .collect()
    }

    async fn list_request_events(
        &self,
        query: &RequestEventListQuery,
    ) -> StorageResult<Vec<RequestEventListItem>> {
        request_event_list_sql::list_request_events(self, query).await
    }

    async fn request_event_key_last_used(
        &self,
        query: &RequestEventKeyLastUsedQuery,
    ) -> StorageResult<Vec<RequestEventKeyLastUsed>> {
        request_event_key_last_used(self, query).await
    }

    async fn request_event_key_usage(
        &self,
        query: &RequestEventKeyUsageQuery,
    ) -> StorageResult<Vec<RequestEventKeyUsageBucket>> {
        request_event_key_usage(self, query).await
    }

    async fn request_event_principal_costs(
        &self,
        query: &RequestEventPrincipalCostQuery,
    ) -> StorageResult<Vec<RequestEventPrincipalCostBucket>> {
        request_event_principal_costs(self, query).await
    }

    async fn request_event_histogram(
        &self,
        query: &RequestEventHistogramQuery,
    ) -> StorageResult<Vec<RequestEventHistogramBucket>> {
        request_event_histogram(self, query).await
    }

    async fn get_request_event(&self, event_id: &str) -> StorageResult<Option<RequestEvent>> {
        request_event_list_sql::get_request_event(self, event_id).await
    }
}

async fn request_event_key_last_used(
    storage: &PostgresStorage,
    query: &RequestEventKeyLastUsedQuery,
) -> StorageResult<Vec<RequestEventKeyLastUsed>> {
    if query.until_unix_secs < query.since_unix_secs {
        return Ok(Vec::new());
    }
    let Some(since) =
        unix_secs_to_datetime_lower(query.since_unix_secs, "request event key last-used since")?
    else {
        return Ok(Vec::new());
    };
    let until =
        unix_secs_to_datetime_upper(query.until_unix_secs, "request event key last-used until")?;

    let rows = sqlx::query_as::<_, (String, i64)>(
        "SELECT key_id, EXTRACT(EPOCH FROM MAX(ts))::bigint AS last_used_at_unix_secs \
         FROM request_events_v1 \
         WHERE principal_id = $1 \
           AND key_id IS NOT NULL \
           AND key_id <> '' \
           AND ts >= $2 \
           AND ($3::timestamptz IS NULL OR ts <= $3) \
         GROUP BY key_id",
    )
    .bind(query.principal_id.as_str())
    .bind(since)
    .bind(until)
    .fetch_all(&storage.pool)
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

async fn request_event_key_usage(
    storage: &PostgresStorage,
    query: &RequestEventKeyUsageQuery,
) -> StorageResult<Vec<RequestEventKeyUsageBucket>> {
    if query.bucket_count == 0 || query.step_ms == 0 || query.range_end_ms < query.range_start_ms {
        return Ok(Vec::new());
    }

    let bucket_count = usize::try_from(query.bucket_count).map_err(|_| StorageError::Fatal {
        message: "request event key usage bucket_count cannot be represented as usize".to_owned(),
    })?;
    let mut buckets = vec![RequestEventKeyUsageBucket::default(); bucket_count];
    for (index, bucket) in buckets.iter_mut().enumerate() {
        let bucket_start_ms = query
            .range_start_ms
            .saturating_add((index as u64).saturating_mul(query.step_ms));
        bucket.bucket_start_unix_secs = bucket_start_ms / 1_000;
    }

    let rows = sqlx::query_as::<_, (i64, i64, i64, i64, i64)>(
        "SELECT LEAST(((list_ts_ms - $1) / $2), $3)::bigint AS bucket_index, \
                COUNT(*)::bigint AS request_count, \
                COALESCE(SUM(COALESCE(input_tokens, 0) \
                    + COALESCE(cache_creation_input_tokens, 0) \
                    + COALESCE(cache_read_input_tokens, 0)), 0)::bigint AS input_tokens, \
                COALESCE(SUM(COALESCE(output_tokens, 0)), 0)::bigint AS output_tokens, \
                COALESCE(SUM(COALESCE(list_cost_usd_micros, 0)), 0)::bigint AS cost_usd_micros \
         FROM request_events_v1 \
         WHERE principal_id = $4 \
           AND key_id = $5 \
           AND list_ts_ms BETWEEN $6 AND $7 \
         GROUP BY bucket_index",
    )
    .bind(u64_to_i64(
        query.range_start_ms,
        "request event key usage range start",
    )?)
    .bind(u64_to_i64(query.step_ms, "request event key usage step")?)
    .bind(u64_to_i64(
        query.bucket_count.saturating_sub(1),
        "request event key usage last bucket",
    )?)
    .bind(query.principal_id.as_str())
    .bind(query.key_id.as_str())
    .bind(u64_to_i64(
        query.range_start_ms,
        "request event key usage lower bound",
    )?)
    .bind(u64_to_i64(
        query.range_end_ms,
        "request event key usage upper bound",
    )?)
    .fetch_all(&storage.pool)
    .await
    .map_err(map_sqlx_error)?;

    for (bucket_index, request_count, input_tokens, output_tokens, cost_usd_micros) in rows {
        let bucket_index = usize::try_from(bucket_index).map_err(|_| StorageError::Corrupted {
            message: "request event key usage bucket index is negative".to_owned(),
        })?;
        let Some(bucket) = buckets.get_mut(bucket_index) else {
            return Err(StorageError::Corrupted {
                message: "request event key usage bucket index is out of range".to_owned(),
            });
        };
        bucket.request_count = i64_to_u64(request_count, "request event key usage request count")?;
        bucket.input_tokens = i64_to_u64(input_tokens, "request event key usage input tokens")?;
        bucket.output_tokens = i64_to_u64(output_tokens, "request event key usage output tokens")?;
        bucket.cost_usd_micros = cost_usd_micros;
    }

    Ok(buckets)
}

fn is_canonical_uuid_principal(value: &str) -> bool {
    value.len() == 36 && uuid::Uuid::parse_str(value).is_ok()
}

type PrincipalCostRow = (Option<String>, i64, i64, bool, i64, i64, i64, i64, i64);

const PRINCIPAL_COST_AGGREGATE_PREFIX: &str = "SELECT principal_id, bucket_index, \
        COALESCE(SUM(GREATEST(COALESCE(list_cost_usd_micros, 0), 0)), 0)::bigint \
            AS total_cost_micros, \
        BOOL_OR(list_cost_input_micros IS NOT NULL \
            OR list_cost_output_micros IS NOT NULL \
            OR list_cost_cache_creation_5m_micros IS NOT NULL \
            OR list_cost_cache_creation_1h_micros IS NOT NULL \
            OR list_cost_cache_read_micros IS NOT NULL) \
            AS component_costs_recorded, \
        COALESCE(SUM(GREATEST(COALESCE(list_cost_input_micros, 0), 0)), 0)::bigint \
            AS cost_input_micros, \
        COALESCE(SUM(GREATEST(COALESCE(list_cost_output_micros, 0), 0)), 0)::bigint \
            AS cost_output_micros, \
        COALESCE(SUM(GREATEST(COALESCE(list_cost_cache_creation_5m_micros, 0), 0)), 0)::bigint \
            AS cost_cache_creation_5m_micros, \
        COALESCE(SUM(GREATEST(COALESCE(list_cost_cache_creation_1h_micros, 0), 0)), 0)::bigint \
            AS cost_cache_creation_1h_micros, \
        COALESCE(SUM(GREATEST(COALESCE(list_cost_cache_read_micros, 0), 0)), 0)::bigint \
            AS cost_cache_read_micros \
    FROM (";

const PRINCIPAL_COST_AGGREGATE_SUFFIX: &str = ") matched \
    GROUP BY principal_id, bucket_index \
    ORDER BY bucket_index ASC, principal_id ASC";

const UUID_PRINCIPAL_COST_SOURCE_SQL: &str = include_str!("request_event_principal_cost_uuid.sql");
const FILTERED_UUID_PRINCIPAL_COST_SOURCE_SQL: &str =
    include_str!("request_event_principal_cost_uuid_filtered.sql");
const NON_UUID_PRINCIPAL_COST_SOURCE_SQL: &str =
    include_str!("request_event_principal_cost_normalized.sql");
const FILTERED_NON_UUID_PRINCIPAL_COST_SOURCE_SQL: &str =
    include_str!("request_event_principal_cost_normalized_filtered.sql");

async fn fetch_principal_cost_rows(
    storage: &PostgresStorage,
    source_sql: &'static str,
    range_start_ms: i64,
    bucket_width_ms: i64,
    range_end_ms: i64,
    principal_keys: &Vec<String>,
) -> StorageResult<Vec<PrincipalCostRow>> {
    sqlx::query_as::<_, PrincipalCostRow>(AssertSqlSafe(format!(
        "{PRINCIPAL_COST_AGGREGATE_PREFIX}{source_sql}{PRINCIPAL_COST_AGGREGATE_SUFFIX}"
    )))
    .bind(range_start_ms)
    .bind(bucket_width_ms)
    .bind(range_end_ms)
    .bind(principal_keys)
    .fetch_all(&storage.pool)
    .await
    .map_err(map_sqlx_error)
}

async fn fetch_filtered_principal_cost_rows(
    storage: &PostgresStorage,
    source_sql: &'static str,
    range_start_ms: i64,
    bucket_width_ms: i64,
    range_end_ms: i64,
    upstream_id: uuid::Uuid,
    principal_keys: &Vec<String>,
) -> StorageResult<Vec<PrincipalCostRow>> {
    sqlx::query_as::<_, PrincipalCostRow>(AssertSqlSafe(format!(
        "{PRINCIPAL_COST_AGGREGATE_PREFIX}{source_sql}{PRINCIPAL_COST_AGGREGATE_SUFFIX}"
    )))
    .bind(range_start_ms)
    .bind(bucket_width_ms)
    .bind(range_end_ms)
    .bind(upstream_id)
    .bind(principal_keys)
    .fetch_all(&storage.pool)
    .await
    .map_err(map_sqlx_error)
}

async fn request_event_principal_costs(
    storage: &PostgresStorage,
    query: &RequestEventPrincipalCostQuery,
) -> StorageResult<Vec<RequestEventPrincipalCostBucket>> {
    if query.bucket_width_secs == 0
        || query.until_unix_secs <= query.since_unix_secs
        || query.principal_keys.is_empty()
    {
        return Ok(Vec::new());
    }

    let range_start_ms = seconds_to_millis(
        query.since_unix_secs,
        "request event principal cost range start",
    )?;
    let range_end_ms = seconds_to_millis(
        query.until_unix_secs,
        "request event principal cost range end",
    )?;
    let bucket_width_ms = seconds_to_millis(
        query.bucket_width_secs,
        "request event principal cost bucket width",
    )?;
    let uuid_keys = query
        .principal_keys
        .iter()
        .filter(|key| is_canonical_uuid_principal(key))
        .cloned()
        .collect::<Vec<_>>();
    let mut rows = Vec::<PrincipalCostRow>::new();

    if !uuid_keys.is_empty() {
        rows.extend(match query.upstream_id {
            Some(upstream_id) => {
                fetch_filtered_principal_cost_rows(
                    storage,
                    FILTERED_UUID_PRINCIPAL_COST_SOURCE_SQL,
                    range_start_ms,
                    bucket_width_ms,
                    range_end_ms,
                    upstream_id,
                    &uuid_keys,
                )
                .await?
            }
            None => {
                fetch_principal_cost_rows(
                    storage,
                    UUID_PRINCIPAL_COST_SOURCE_SQL,
                    range_start_ms,
                    bucket_width_ms,
                    range_end_ms,
                    &uuid_keys,
                )
                .await?
            }
        });
    }
    rows.extend(match query.upstream_id {
        Some(upstream_id) => {
            fetch_filtered_principal_cost_rows(
                storage,
                FILTERED_NON_UUID_PRINCIPAL_COST_SOURCE_SQL,
                range_start_ms,
                bucket_width_ms,
                range_end_ms,
                upstream_id,
                &query.principal_keys,
            )
            .await?
        }
        None => {
            fetch_principal_cost_rows(
                storage,
                NON_UUID_PRINCIPAL_COST_SOURCE_SQL,
                range_start_ms,
                bucket_width_ms,
                range_end_ms,
                &query.principal_keys,
            )
            .await?
        }
    });

    let mut buckets = BTreeMap::<(String, u64), RequestEventPrincipalCostBucket>::new();
    for (
        principal_id,
        bucket_index,
        total_cost_micros,
        component_costs_recorded,
        cost_input_micros,
        cost_output_micros,
        cost_cache_creation_5m_micros,
        cost_cache_creation_1h_micros,
        cost_cache_read_micros,
    ) in rows
    {
        let bucket_index = i64_to_u64(bucket_index, "request event principal cost bucket index")?;
        let bucket_offset = bucket_index
            .checked_mul(query.bucket_width_secs)
            .ok_or_else(|| StorageError::Corrupted {
                message: "request event principal cost bucket offset overflowed".to_owned(),
            })?;
        let bucket_start_unix_secs = query
            .since_unix_secs
            .checked_add(bucket_offset)
            .filter(|bucket_start| *bucket_start < query.until_unix_secs)
            .ok_or_else(|| StorageError::Corrupted {
                message: "request event principal cost bucket is out of range".to_owned(),
            })?;
        let principal = normalize_usage_rollup_dimension(principal_id.as_deref());
        let bucket = buckets
            .entry((principal.clone(), bucket_start_unix_secs))
            .or_insert_with(|| RequestEventPrincipalCostBucket {
                principal,
                bucket_start_unix_secs,
                ..RequestEventPrincipalCostBucket::default()
            });

        add_principal_cost(
            &mut bucket.total_cost_micros,
            total_cost_micros,
            "request event principal total cost",
        )?;
        bucket.component_costs_recorded |= component_costs_recorded;
        add_principal_cost(
            &mut bucket.cost_input_micros,
            cost_input_micros,
            "request event principal input cost",
        )?;
        add_principal_cost(
            &mut bucket.cost_output_micros,
            cost_output_micros,
            "request event principal output cost",
        )?;
        add_principal_cost(
            &mut bucket.cost_cache_creation_5m_micros,
            cost_cache_creation_5m_micros,
            "request event principal cache creation 5m cost",
        )?;
        add_principal_cost(
            &mut bucket.cost_cache_creation_1h_micros,
            cost_cache_creation_1h_micros,
            "request event principal cache creation 1h cost",
        )?;
        add_principal_cost(
            &mut bucket.cost_cache_read_micros,
            cost_cache_read_micros,
            "request event principal cache read cost",
        )?;
    }

    Ok(buckets.into_values().collect())
}

/// See the SQLite adapter: the `list_ts_ms` bound in the histogram query is a
/// widened index hint, never a membership filter.
const HISTOGRAM_INDEX_HINT_SLACK_MS: u64 = 60_000;

async fn request_event_histogram(
    storage: &PostgresStorage,
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

    let Some(since) =
        unix_secs_to_datetime_lower(query.since_unix_secs, "request event histogram since")?
    else {
        return Ok(Vec::new());
    };
    let until =
        unix_secs_to_datetime_upper(query.until_unix_secs, "request event histogram until")?;

    let mut builder = QueryBuilder::<Postgres>::new("SELECT LEAST(GREATEST((r.list_ts_ms - ");
    builder.push_bind(u64_to_i64(
        range_start_ms,
        "request event histogram range start",
    )?);
    builder.push(") / ");
    builder.push_bind(u64_to_i64(
        query.bucket_ms,
        "request event histogram bucket size",
    )?);
    builder.push(", 0), ");
    builder.push_bind(u64_to_i64(
        query.bucket_count.saturating_sub(1),
        "request event histogram last bucket",
    )?);
    builder.push(
        ")::bigint AS bucket_index, \
         COUNT(*) AS total_count, \
         COUNT(CASE WHEN r.list_status >= 500 \
             OR (r.list_status BETWEEN 200 AND 299 \
                 AND r.error_code = 'upstream_stream_error') \
             THEN 1 END) AS error_count \
         FROM request_events_v1 r \
         WHERE r.ts >= ",
    );
    builder.push_bind(since);

    if let Some(until) = until {
        builder.push(" AND r.ts <= ");
        builder.push_bind(until);
    }
    // Widened `list_ts_ms` bound so the planner can range-scan a
    // `list_ts_ms`-leading index; `r.ts` above stays the authoritative window.
    // Deliberately a superset — do not tighten it to the exact range.
    builder.push(" AND r.list_ts_ms >= ");
    builder.push_bind(u64_to_i64(
        range_start_ms.saturating_sub(HISTOGRAM_INDEX_HINT_SLACK_MS),
        "request event histogram index hint lower bound",
    )?);
    builder.push(" AND r.list_ts_ms < ");
    builder.push_bind(u64_to_i64(
        query
            .until_unix_secs
            .saturating_add(1)
            .saturating_mul(1_000)
            .saturating_add(HISTOGRAM_INDEX_HINT_SLACK_MS)
            .min(i64::MAX as u64),
        "request event histogram index hint upper bound",
    )?);
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
        None => {
            builder.push(" AND (r.source_kind IS NULL OR r.source_kind <> 'renewal')");
        }
    }

    builder.push(" GROUP BY bucket_index");

    let rows = builder
        .build_query_as::<(i64, i64, i64)>()
        .fetch_all(&storage.pool)
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

fn request_event_from_storage(
    payload: &[u8],
    source_kind: Option<String>,
    source_ref_id: Option<String>,
) -> StorageResult<RequestEvent> {
    let mut event = serde_json::from_slice::<RequestEvent>(payload)?;
    if source_kind.is_some() {
        event.source_kind = source_kind;
    }
    if source_ref_id.is_some() {
        event.source_ref_id = source_ref_id;
    }
    Ok(event)
}

async fn insert_request_event_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    event: &RequestEvent,
    event_id: &str,
) -> StorageResult<Option<i64>> {
    let payload = serde_json::to_vec(event)?;
    let cache_breakpoints = serde_json::to_value(&event.cache_breakpoints)?;
    sqlx::query_scalar::<_, i64>(
            "INSERT INTO request_events_v1 \
              (ts, source_kind, source_ref_id, principal_id, upstream_id, key_id, model, upstream_name, cache_state, thread_id, message_id, \
               message_index, message_count, cache_control_block_count, cache_breakpoints, cache_prefix_hash, \
                input_tokens, output_tokens, cache_creation_input_tokens, cache_read_input_tokens, \
                 event_id, error_code, upstream_error_type, upstream_error_message, \
                 thinking_tokens, web_search_requests, web_fetch_requests, \
                  service_tier, inference_geo, \
                  cache_creation_input_tokens_5m, cache_creation_input_tokens_1h, \
                  matched_v3_cache_key, breakpoint_content_block_index, matched_content_block_index, lookback_distance, \
                  predicted_cache_read_tokens, predicted_cache_creation_tokens_5m, predicted_cache_creation_tokens_1h, \
                   token_estimate_source, cache_value_micros, formula_winner_upstream_id, kept_upstream_id, \
                   quota_urgency_5h, quota_urgency_7d, quota_urgency_combined, quota_warning_multiplier, \
                   lineage_would_have_predicted_read_tokens, lineage_would_have_picked_upstream_id, \
                   thinking_budget_tokens, reasoning_effort, \
                   observed_session_id, request_kind, \
                   list_ts_ms, list_event_key, list_upstream, list_status, \
                   list_cost_usd_micros, list_cost_input_micros, list_cost_output_micros, \
                   list_cost_cache_creation_5m_micros, list_cost_cache_creation_1h_micros, list_cost_cache_read_micros, \
                   list_cost_components_materialized, payload, claude_agent_id, claude_parent_agent_id, parent_session_id, client_app, session_id_source, created_at) \
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,$55,$56,$57,$58,$59,$60,$61,$62,$63,$64,$65,$66,$67,$68,$69,NOW()) \
               ON CONFLICT(event_id) WHERE event_id IS NOT NULL DO NOTHING \
               RETURNING seq",
        )
        .bind(unix_secs_to_datetime(event.ts, "request event ts")?)
        .bind(event.source_kind.as_deref())
        .bind(event.source_ref_id.as_deref())
        .bind(event.principal_id.as_deref())
        .bind(event.upstream_id)
        .bind(event.key_id.as_deref())
        .bind(event.model.as_deref())
        .bind(event.upstream_name.as_deref())
        .bind(event.cache_state.map(|state| state.as_str()))
        .bind(event.thread_id.as_deref())
        .bind(event.message_id.as_deref())
        .bind(
            event
                .message_index
                .map(|value| u64_to_i64(value, "request event message_index"))
                .transpose()?,
        )
        .bind(
            event
                .message_count
                .map(|value| u64_to_i64(value, "request event message_count"))
                .transpose()?,
        )
        .bind(
            event
                .cache_control_block_count
                .map(|value| u64_to_i64(value, "request event cache_control_block_count"))
                .transpose()?,
        )
        .bind(cache_breakpoints)
        .bind(event.cache_prefix_hash.as_deref())
        .bind(
            event
                .input_tokens
                .map(|value| u64_to_i64(value, "request event input_tokens"))
                .transpose()?,
        )
        .bind(
            event
                .output_tokens
                .map(|value| u64_to_i64(value, "request event output_tokens"))
                .transpose()?,
        )
        .bind(
            event
                .cache_creation_input_tokens
                .map(|value| u64_to_i64(value, "request event cache_creation_input_tokens"))
                .transpose()?,
        )
        .bind(
            event
                .cache_read_input_tokens
                .map(|value| u64_to_i64(value, "request event cache_read_input_tokens"))
                .transpose()?,
        )
        .bind(event_id)
        .bind(event.error_code.as_deref())
        .bind(event.upstream_error_type.as_deref())
        .bind(event.upstream_error_message.as_deref())
        .bind(
            event
                .thinking_tokens
                .map(|value| u64_to_i64(value, "request event thinking_tokens"))
                .transpose()?,
        )
        .bind(
            event
                .web_search_requests
                .map(|value| u64_to_i64(value, "request event web_search_requests"))
                .transpose()?,
        )
        .bind(
            event
                .web_fetch_requests
                .map(|value| u64_to_i64(value, "request event web_fetch_requests"))
                .transpose()?,
        )
        .bind(event.service_tier.as_deref())
        .bind(event.inference_geo.as_deref())
        .bind(
            event
                .cache_creation_input_tokens_5m
                .map(|value| u64_to_i64(value, "request event cache_creation_input_tokens_5m"))
                .transpose()?,
        )
        .bind(
            event
                .cache_creation_input_tokens_1h
                .map(|value| u64_to_i64(value, "request event cache_creation_input_tokens_1h"))
                .transpose()?,
        )
        .bind(event.matched_v3_cache_key.as_deref())
        .bind(
            event
                .breakpoint_content_block_index
                .map(|value| u64_to_i64(value, "request event breakpoint_content_block_index"))
                .transpose()?,
        )
        .bind(
            event
                .matched_content_block_index
                .map(|value| u64_to_i64(value, "request event matched_content_block_index"))
                .transpose()?,
        )
        .bind(
            event
                .lookback_distance
                .map(|value| u64_to_i64(value, "request event lookback_distance"))
                .transpose()?,
        )
        .bind(
            event
                .predicted_cache_read_tokens
                .map(|value| u64_to_i64(value, "request event predicted_cache_read_tokens"))
                .transpose()?,
        )
        .bind(
            event
                .predicted_cache_creation_tokens_5m
                .map(|value| u64_to_i64(value, "request event predicted_cache_creation_tokens_5m"))
                .transpose()?,
        )
        .bind(
            event
                .predicted_cache_creation_tokens_1h
                .map(|value| u64_to_i64(value, "request event predicted_cache_creation_tokens_1h"))
                .transpose()?,
        )
        .bind(event.token_estimate_source.as_deref())
        .bind(event.cache_value_micros)
        .bind(event.formula_winner_upstream_id)
        .bind(event.kept_upstream_id)
        .bind(event.quota_urgency_5h)
        .bind(event.quota_urgency_7d)
        .bind(event.quota_urgency_combined)
        .bind(event.quota_warning_multiplier)
        .bind(
            event
                .lineage_would_have_predicted_read_tokens
                .map(|value| u64_to_i64(value, "request event lineage_would_have_predicted_read_tokens"))
                .transpose()?,
        )
        .bind(event.lineage_would_have_picked_upstream_id)
        .bind(
            event
                .thinking_budget_tokens
                .map(|value| u64_to_i64(value, "request event thinking_budget_tokens"))
                .transpose()?,
        )
        .bind(event.reasoning_effort.as_deref())
        .bind(event.observed_session_id.as_deref())
        .bind(event.request_kind.as_deref())
        .bind(u64_to_i64(
            event
                .ts_ms
                .unwrap_or_else(|| event.ts.saturating_mul(1_000)),
            "request event list_ts_ms",
        )?)
        .bind(event_id)
        .bind(event.upstream.map(|upstream| match upstream {
            cc_lb_storage_api::RequestEventUpstream::AnthropicDirect => "anthropic_direct",
        }))
        .bind(i32::from(event.status))
        .bind(event.cost_usd_micros)
        .bind(event.cost_input_micros)
        .bind(event.cost_output_micros)
        .bind(event.cost_cache_creation_5m_micros)
        .bind(event.cost_cache_creation_1h_micros)
        .bind(event.cost_cache_read_micros)
        .bind(true)
        .bind(payload)
        .bind(event.claude_agent_id.as_deref())
        .bind(event.claude_parent_agent_id.as_deref())
        .bind(event.parent_session_id.as_deref())
        .bind(event.client_app.as_deref())
        .bind(event.session_id_source.as_deref())
        .fetch_optional(&mut **tx)
        .await
        .map_err(map_sqlx_error)
}
async fn select_existing_event_id_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    event_id: &str,
) -> StorageResult<i64> {
    sqlx::query_scalar::<_, i64>("SELECT seq FROM request_events_v1 WHERE event_id = $1")
        .bind(event_id)
        .fetch_one(&mut **tx)
        .await
        .map_err(map_sqlx_error)
}

async fn insert_keepalive_turn_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    row: &CacheKeepaliveTurnRow,
) -> StorageResult<()> {
    sqlx::query(
        "INSERT INTO cache_keepalive_turns \
         (source_ref_id, session_key_hash, principal_id, accounting_key_id, upstream_id, model, input_tokens, output_tokens, cache_creation_input_tokens, cache_creation_input_tokens_5m, cache_creation_input_tokens_1h, cache_read_input_tokens, cost_micros, hit_miss, ts) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15) \
         ON CONFLICT(source_ref_id) DO NOTHING",
    )
    .bind(&row.source_ref_id)
    .bind(&row.session_key_hash)
    .bind(&row.principal_id)
    .bind(row.accounting_key_id.as_deref())
    .bind(row.upstream_id)
    .bind(&row.model)
    .bind(u64_to_i64(row.input_tokens, "cache keepalive input_tokens")?)
    .bind(u64_to_i64(row.output_tokens, "cache keepalive output_tokens")?)
    .bind(u64_to_i64(row.cache_creation_input_tokens, "cache keepalive cache_creation_input_tokens")?)
    .bind(u64_to_i64(row.cache_creation_input_tokens_5m, "cache keepalive cache_creation_input_tokens_5m")?)
    .bind(u64_to_i64(row.cache_creation_input_tokens_1h, "cache keepalive cache_creation_input_tokens_1h")?)
    .bind(u64_to_i64(row.cache_read_input_tokens, "cache keepalive cache_read_input_tokens")?)
    .bind(row.cost_micros)
    .bind(&row.hit_miss)
    .bind(u64_to_i64(row.ts, "cache keepalive ts")?)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

async fn insert_keepalive_decision_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
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
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) \
         ON CONFLICT(source_ref_id) DO NOTHING",
    )
    .bind(&row.source_ref_id)
    .bind(&row.principal_id)
    .bind(row.session_key_hash.as_deref())
    .bind(row.upstream_id)
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

fn cache_keepalive_ttl_to_db(ttl: cc_lb_storage_api::CacheTtl) -> &'static str {
    match ttl {
        cc_lb_storage_api::CacheTtl::Ttl5m => "5m",
        cc_lb_storage_api::CacheTtl::Ttl1h => "1h",
    }
}

fn storage_event_id(event: &RequestEvent) -> String {
    event
        .event_id
        .clone()
        .unwrap_or_else(|| format!("{}-legacy-live-{}", event.ts, uuid::Uuid::now_v7()))
}

fn request_event_matches_filters(
    event: &RequestEvent,
    filters: &RequestEventStreamFilters,
) -> bool {
    if let Some(principal_id) = filters.principal_id.as_deref()
        && event.principal_id.as_deref() != Some(principal_id)
    {
        return false;
    }
    if let Some(thread_id) = filters.thread_id.as_deref()
        && event.thread_id.as_deref() != Some(thread_id)
    {
        return false;
    }
    if let Some(model) = filters.model.as_deref()
        && !model_filter_matches(model, event.model.as_deref())
    {
        return false;
    }
    if let Some(upstream) = filters.upstream
        && event.upstream != Some(upstream)
    {
        return false;
    }
    if let Some(upstream_id) = filters.upstream_id
        && event.upstream_id != Some(upstream_id)
    {
        return false;
    }
    if let Some(status_class) = filters.status_class
        && !status_class.matches(event.status)
    {
        return false;
    }
    true
}

impl PostgresStorage {
    pub async fn prune_request_events(&self, older_than: u64) -> StorageResult<u64> {
        let result = sqlx::query("DELETE FROM request_events_v1 WHERE ts < $1")
            .bind(unix_secs_to_datetime(
                older_than,
                "request event prune cutoff",
            )?)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        Ok(result.rows_affected())
    }
}

fn seconds_to_millis(value: u64, field: &str) -> StorageResult<i64> {
    let value = value
        .checked_mul(1_000)
        .ok_or_else(|| StorageError::Fatal {
            message: format!("{field} cannot be represented as milliseconds"),
        })?;
    u64_to_i64(value, field)
}

fn add_principal_cost(target: &mut u64, value: i64, field: &str) -> StorageResult<()> {
    let value = i64_to_u64(value, field)?;
    *target = target
        .checked_add(value)
        .ok_or_else(|| StorageError::Corrupted {
            message: format!("{field} aggregate overflowed"),
        })?;
    Ok(())
}
