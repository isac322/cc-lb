use std::collections::{BTreeMap, HashMap};

use async_trait::async_trait;
use cc_lb_storage_api::{
    OverviewExcludedErrorBucket, RequestEvent, StorageError, StorageResult, UsageRollup,
    UsageRollupResolution, UsageRollupRun, UsageRollupStore, UsageTokenInterval,
    UsageTokenIntervalStore, UsageTokenIntervalSum, normalize_usage_rollup_dimension,
};
use sqlx::{AssertSqlSafe, Row, Sqlite, Transaction, sqlite::SqliteRow};
use uuid::Uuid;

use crate::{SqliteStorage, map_sqlx_error};

const CHECKPOINT_ID: &str = "high_water";
const COST_BACKFILL_UPPER_ID: &str = "cost_components_backfill_upper";
const COST_BACKFILL_CURSOR_ID: &str = "cost_components_backfill_cursor";
const GEMINI_RECONCILIATION_ID: &str = "canonicalize_gemini_model_identity";
const ROLLUP_BATCH_LIMIT: i64 = 10_000;
const MINUTE_SECS: u64 = 60;
const HOUR_SECS: u64 = 60 * 60;
const TOKEN_INTERVAL_BATCH_SIZE: usize = 100;

async fn cleanup_gemini_reconciliation_markers(
    tx: &mut Transaction<'_, Sqlite>,
) -> StorageResult<()> {
    sqlx::query("DELETE FROM usage_rollup_reconciliation_markers_v1")
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RollupKey {
    resolution: UsageRollupResolution,
    bucket_start: u64,
    principal: String,
    upstream_id: Uuid,
    upstream_name: String,
    model: String,
}

#[derive(Debug, Default, Clone)]
struct RollupDelta {
    request_count: u64,
    input_tokens: u64,
    output_tokens: u64,
    cache_creation_input_tokens: u64,
    cache_read_input_tokens: u64,
    cache_write_input_tokens: Option<u64>,
    cache_write_input_tokens_30m: Option<u64>,
    cache_write_input_tokens_12h: Option<u64>,
    error_count: u64,
    latency_count: u64,
    latency_ms_sum: u64,
    latency_ms_min: Option<u64>,
    latency_ms_max: Option<u64>,
    proxy_setup_ms_count: u64,
    proxy_setup_ms_sum: u64,
    shape_ms_count: u64,
    shape_ms_sum: u64,
    sign_ms_count: u64,
    sign_ms_sum: u64,
    upstream_ttfb_ms_count: u64,
    upstream_ttfb_ms_sum: u64,
    upstream_body_ms_count: u64,
    upstream_body_ms_sum: u64,
    virtual_cost_micros: u64,
    cost_input_micros: u64,
    cost_output_micros: u64,
    cost_cache_creation_5m_micros: u64,
    cost_cache_creation_1h_micros: u64,
    cost_cache_read_micros: u64,
    cost_cache_write_30m_micros: u64,
    cost_cache_write_12h_micros: u64,
    cost_components_event_count: u64,
}

impl RollupDelta {
    fn add_event(&mut self, event: &RequestEvent) {
        self.request_count = self.request_count.saturating_add(1);
        self.input_tokens = self
            .input_tokens
            .saturating_add(event.input_tokens.unwrap_or(0));
        self.output_tokens = self
            .output_tokens
            .saturating_add(event.output_tokens.unwrap_or(0));
        self.cache_creation_input_tokens = self
            .cache_creation_input_tokens
            .saturating_add(event.cache_creation_input_tokens.unwrap_or(0));
        self.cache_read_input_tokens = self
            .cache_read_input_tokens
            .saturating_add(event.cache_read_input_tokens.unwrap_or(0));
        add_optional(
            &mut self.cache_write_input_tokens,
            event.cache_write_input_tokens,
        );
        add_optional(
            &mut self.cache_write_input_tokens_30m,
            event.cache_write_input_tokens_30m,
        );
        add_optional(
            &mut self.cache_write_input_tokens_12h,
            event.cache_write_input_tokens_12h,
        );
        if event.status >= 400 {
            self.error_count = self.error_count.saturating_add(1);
        }
        self.latency_count = self.latency_count.saturating_add(1);
        self.latency_ms_sum = self.latency_ms_sum.saturating_add(event.duration_ms);
        self.latency_ms_min = min_option(self.latency_ms_min, Some(event.duration_ms));
        self.latency_ms_max = max_option(self.latency_ms_max, Some(event.duration_ms));
        if let Some(value) = event.proxy_setup_ms {
            self.proxy_setup_ms_count = self.proxy_setup_ms_count.saturating_add(1);
            self.proxy_setup_ms_sum = self.proxy_setup_ms_sum.saturating_add(value);
        }
        if let Some(value) = event.shape_ms {
            self.shape_ms_count = self.shape_ms_count.saturating_add(1);
            self.shape_ms_sum = self.shape_ms_sum.saturating_add(value);
        }
        if let Some(value) = event.sign_ms {
            self.sign_ms_count = self.sign_ms_count.saturating_add(1);
            self.sign_ms_sum = self.sign_ms_sum.saturating_add(value);
        }
        if let Some(value) = event.upstream_ttfb_ms {
            self.upstream_ttfb_ms_count = self.upstream_ttfb_ms_count.saturating_add(1);
            self.upstream_ttfb_ms_sum = self.upstream_ttfb_ms_sum.saturating_add(value);
        }
        if let Some(value) = event.upstream_body_ms {
            self.upstream_body_ms_count = self.upstream_body_ms_count.saturating_add(1);
            self.upstream_body_ms_sum = self.upstream_body_ms_sum.saturating_add(value);
        }
        self.virtual_cost_micros = self
            .virtual_cost_micros
            .saturating_add(event.cost_usd_micros.unwrap_or(0).max(0) as u64);
        self.add_cost_components(event);
    }

    fn add_cost_components(&mut self, event: &RequestEvent) {
        if event.cost_input_micros.is_some()
            || event.cost_output_micros.is_some()
            || event.cost_cache_creation_5m_micros.is_some()
            || event.cost_cache_creation_1h_micros.is_some()
            || event.cost_cache_read_micros.is_some()
            || event.cost_cache_write_30m_micros.is_some()
            || event.cost_cache_write_12h_micros.is_some()
        {
            self.cost_components_event_count = self.cost_components_event_count.saturating_add(1);
            self.cost_input_micros = self
                .cost_input_micros
                .saturating_add(event.cost_input_micros.unwrap_or(0).max(0) as u64);
            self.cost_output_micros = self
                .cost_output_micros
                .saturating_add(event.cost_output_micros.unwrap_or(0).max(0) as u64);
            self.cost_cache_creation_5m_micros = self
                .cost_cache_creation_5m_micros
                .saturating_add(event.cost_cache_creation_5m_micros.unwrap_or(0).max(0) as u64);
            self.cost_cache_creation_1h_micros = self
                .cost_cache_creation_1h_micros
                .saturating_add(event.cost_cache_creation_1h_micros.unwrap_or(0).max(0) as u64);
            self.cost_cache_read_micros = self
                .cost_cache_read_micros
                .saturating_add(event.cost_cache_read_micros.unwrap_or(0).max(0) as u64);
            self.cost_cache_write_30m_micros = self
                .cost_cache_write_30m_micros
                .saturating_add(event.cost_cache_write_30m_micros.unwrap_or(0).max(0) as u64);
            self.cost_cache_write_12h_micros = self
                .cost_cache_write_12h_micros
                .saturating_add(event.cost_cache_write_12h_micros.unwrap_or(0).max(0) as u64);
        }
    }
}

#[derive(Debug, Clone)]
struct UpstreamIdentity {
    id: Uuid,
    name: String,
}

#[async_trait]
impl UsageRollupStore for SqliteStorage {
    async fn prune_usage_rollups_before(
        &self,
        minute_cutoff_unix_secs: u64,
        hour_cutoff_unix_secs: u64,
        batch_size: usize,
    ) -> StorageResult<u64> {
        if batch_size == 0 {
            return Ok(0);
        }
        let minute_cutoff = u64_to_i64(minute_cutoff_unix_secs, "rollup minute cutoff")?;
        let hour_cutoff = u64_to_i64(hour_cutoff_unix_secs, "rollup hour cutoff")?;
        let limit = super::request_events::usize_to_i64(batch_size, "rollup prune batch size")?;
        let mut tx = self.begin_immediate().await?;
        let removed = sqlx::query(
            "DELETE FROM usage_rollups_v2 WHERE rowid IN ( \
             SELECT rowid FROM usage_rollups_v2 \
             WHERE (resolution = 'minute' AND bucket_start_unix_secs < ?) \
                OR (resolution = 'hour' AND bucket_start_unix_secs < ?) \
             ORDER BY bucket_start_unix_secs, rowid LIMIT ?)",
        )
        .bind(minute_cutoff)
        .bind(hour_cutoff)
        .bind(limit)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?
        .rows_affected();
        let remaining = limit - removed as i64;
        let excluded_removed = if remaining > 0 {
            sqlx::query(
                "DELETE FROM overview_excluded_error_rollups_v1 WHERE rowid IN ( \
                 SELECT rowid FROM overview_excluded_error_rollups_v1 \
                 WHERE (resolution = 'minute' AND bucket_start_unix_secs < ?) \
                    OR (resolution = 'hour' AND bucket_start_unix_secs < ?) \
                 ORDER BY bucket_start_unix_secs, rowid LIMIT ?)",
            )
            .bind(minute_cutoff)
            .bind(hour_cutoff)
            .bind(remaining)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?
            .rows_affected()
        } else {
            0
        };
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(removed + excluded_removed)
    }

    async fn rollup_usage_once(&self) -> StorageResult<UsageRollupRun> {
        rollup_usage_once_inner(self).await
    }

    async fn query_usage_rollups_in_range(
        &self,
        resolution: UsageRollupResolution,
        window_start_unix_secs: u64,
        window_end_unix_secs: u64,
    ) -> StorageResult<Vec<UsageRollup>> {
        if window_end_unix_secs < window_start_unix_secs {
            return Ok(Vec::new());
        }

        let rows = sqlx::query(
            "SELECT resolution, bucket_start_unix_secs, principal_id, upstream_id, upstream_name, model, \
              request_count, input_tokens, output_tokens, cache_creation_input_tokens, \
              cache_read_input_tokens, cache_write_input_tokens, cache_write_input_tokens_30m, \
              cache_write_input_tokens_12h, error_count, latency_count, latency_ms_sum, \
              latency_ms_min, latency_ms_max, proxy_setup_ms_count, proxy_setup_ms_sum, \
              shape_ms_count, shape_ms_sum, sign_ms_count, sign_ms_sum, \
              upstream_ttfb_ms_count, upstream_ttfb_ms_sum, upstream_body_ms_count, \
              upstream_body_ms_sum, virtual_cost_micros, cost_input_micros, cost_output_micros, \
              cost_cache_creation_5m_micros, cost_cache_creation_1h_micros, \
              cost_cache_read_micros, cost_cache_write_30m_micros, cost_cache_write_12h_micros, \
              cost_components_event_count \
              FROM usage_rollups_v2 AS rollup \
              WHERE rollup.resolution = ? AND rollup.bucket_start_unix_secs >= ? AND rollup.bucket_start_unix_secs < ? \
              ORDER BY bucket_start_unix_secs ASC",
        )
        .bind(resolution.as_str())
        .bind(u64_to_i64(window_start_unix_secs, "usage rollup range start")?)
        .bind(u64_to_i64(window_end_unix_secs, "usage rollup range end")?)
        .fetch_all(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        rows.into_iter().map(row_to_usage_rollup).collect()
    }

    async fn query_overview_excluded_error_buckets_in_range(
        &self,
        resolution: UsageRollupResolution,
        window_start_unix_secs: u64,
        window_end_unix_secs: u64,
    ) -> StorageResult<Vec<OverviewExcludedErrorBucket>> {
        if window_end_unix_secs < window_start_unix_secs {
            return Ok(Vec::new());
        }
        let rows = sqlx::query_as::<_, (i64, i64)>(
            "SELECT bucket_start_unix_secs, error_count \
             FROM overview_excluded_error_rollups_v1 \
             WHERE resolution = ? AND bucket_start_unix_secs >= ? AND bucket_start_unix_secs < ? \
             ORDER BY bucket_start_unix_secs ASC",
        )
        .bind(resolution.as_str())
        .bind(u64_to_i64(
            window_start_unix_secs,
            "overview excluded error range start",
        )?)
        .bind(u64_to_i64(
            window_end_unix_secs,
            "overview excluded error range end",
        )?)
        .fetch_all(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        rows.into_iter()
            .map(|(bucket_start, error_count)| {
                Ok(OverviewExcludedErrorBucket {
                    bucket_start: i64_to_u64(bucket_start, "overview excluded error bucket")?,
                    error_count: i64_to_u64(error_count, "overview excluded error count")?,
                })
            })
            .collect()
    }

    async fn usage_rollup_checkpoint(&self) -> StorageResult<Option<u64>> {
        read_checkpoint(self.pool()).await
    }
}

#[async_trait]
impl UsageTokenIntervalStore for SqliteStorage {
    async fn sum_usage_tokens_for_intervals(
        &self,
        intervals: &[UsageTokenInterval],
    ) -> StorageResult<Vec<UsageTokenIntervalSum>> {
        let mut sums = Vec::with_capacity(intervals.len());
        for (batch_index, batch) in intervals.chunks(TOKEN_INTERVAL_BATCH_SIZE).enumerate() {
            let values = std::iter::repeat_n("(?, ?, ?, ?, ?)", batch.len())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "WITH intervals(ordinal, interval_id, upstream_id, start_unix_secs, end_unix_secs) \
                 AS (VALUES {values}) \
                 SELECT intervals.interval_id AS interval_id, \
                        COALESCE(SUM(rollup.input_tokens + rollup.output_tokens + \
                                     rollup.cache_creation_input_tokens + \
                                     rollup.cache_read_input_tokens), 0) AS tokens \
                 FROM intervals \
                 LEFT JOIN usage_rollups_v2 rollup \
                   ON rollup.upstream_id = intervals.upstream_id \
                  AND rollup.resolution = 'minute' \
                  AND rollup.bucket_start_unix_secs >= intervals.start_unix_secs \
                  AND rollup.bucket_start_unix_secs <= intervals.end_unix_secs \
                 GROUP BY intervals.ordinal, intervals.interval_id \
                 ORDER BY intervals.ordinal ASC"
            );
            let mut query = sqlx::query(AssertSqlSafe(sql));
            for (interval_index, interval) in batch.iter().enumerate() {
                let ordinal = batch_index
                    .checked_mul(TOKEN_INTERVAL_BATCH_SIZE)
                    .and_then(|value| value.checked_add(interval_index))
                    .and_then(|value| i64::try_from(value).ok())
                    .ok_or_else(|| StorageError::Fatal {
                        message:
                            "usage token interval ordinal cannot be represented as sqlite INTEGER"
                                .to_owned(),
                    })?;
                query = query
                    .bind(ordinal)
                    .bind(u64_to_i64(interval.interval_id, "usage token interval id")?)
                    .bind(interval.upstream_id.to_string())
                    .bind(u64_to_i64(
                        interval.start_unix_secs,
                        "usage token interval start",
                    )?)
                    .bind(u64_to_i64(
                        interval.end_unix_secs,
                        "usage token interval end",
                    )?);
            }
            let rows = query.fetch_all(self.pool()).await.map_err(map_sqlx_error)?;
            for row in rows {
                sums.push(UsageTokenIntervalSum {
                    interval_id: i64_to_u64(
                        row.try_get("interval_id").map_err(map_sqlx_error)?,
                        "usage token interval id",
                    )?,
                    tokens: i64_to_u64(
                        row.try_get("tokens").map_err(map_sqlx_error)?,
                        "usage token interval sum",
                    )?,
                });
            }
        }
        Ok(sums)
    }
}
async fn merge_stale_gemini_rollups(
    tx: &mut Transaction<'_, Sqlite>,
    nested: bool,
    rollup_upper_bound: i64,
) -> StorageResult<()> {
    let source_rows = sqlx::query(
        "SELECT rollup.resolution, rollup.bucket_start_unix_secs, rollup.principal_id, \
         rollup.upstream_id, rollup.model \
         FROM usage_rollups_v2 AS rollup \
         JOIN upstream_spec_v1 AS upstream ON upstream.id = rollup.upstream_id \
         WHERE upstream.kind IN ('gemini_api_key', 'gemini_oauth') \
           AND rollup.rowid <= ? \
           AND length(rollup.model) > 7 \
           AND ( \
             (? = 0 \
              AND rollup.model LIKE 'models\\_%' ESCAPE '\\' \
              AND rollup.model NOT LIKE 'models\\_models\\_%' ESCAPE '\\' \
              AND rollup.model NOT LIKE 'models\\_tunedModels\\_%' ESCAPE '\\') \
             OR \
             (? = 1 \
              AND rollup.model LIKE 'models\\_models\\_%' ESCAPE '\\' \
              AND rollup.model NOT LIKE 'models\\_models\\_tunedModels\\_%' ESCAPE '\\') \
           ) \
           AND NOT EXISTS ( \
             SELECT 1 FROM usage_rollup_reconciliation_markers_v1 AS marker \
             WHERE marker.resolution = rollup.resolution \
               AND marker.bucket_start_unix_secs = rollup.bucket_start_unix_secs \
               AND marker.principal_id = rollup.principal_id \
               AND marker.upstream_id = rollup.upstream_id \
               AND marker.marker_model = rollup.model \
           ) \
         ORDER BY rollup.resolution, rollup.bucket_start_unix_secs, rollup.principal_id, \
                  rollup.upstream_id, rollup.model \
         LIMIT ?",
    )
    .bind(rollup_upper_bound)
    .bind(if nested { 1_i64 } else { 0_i64 })
    .bind(if nested { 1_i64 } else { 0_i64 })
    .bind(ROLLUP_BATCH_LIMIT)
    .fetch_all(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;

    for source in source_rows {
        let resolution: String = source.try_get("resolution").map_err(map_sqlx_error)?;
        let bucket_start: i64 = source
            .try_get("bucket_start_unix_secs")
            .map_err(map_sqlx_error)?;
        let principal: String = source.try_get("principal_id").map_err(map_sqlx_error)?;
        let upstream_id: String = source.try_get("upstream_id").map_err(map_sqlx_error)?;
        let model: String = source.try_get("model").map_err(map_sqlx_error)?;

        sqlx::query(
            "INSERT INTO usage_rollups_v2 \
             (resolution, bucket_start_unix_secs, principal_id, upstream_id, upstream_name, model, \
              request_count, input_tokens, output_tokens, cache_creation_input_tokens, \
              cache_read_input_tokens, cache_write_input_tokens, cache_write_input_tokens_30m, \
              cache_write_input_tokens_12h, error_count, latency_count, latency_ms_sum, \
              latency_ms_min, latency_ms_max, proxy_setup_ms_count, proxy_setup_ms_sum, \
              shape_ms_count, shape_ms_sum, sign_ms_count, sign_ms_sum, \
              upstream_ttfb_ms_count, upstream_ttfb_ms_sum, upstream_body_ms_count, \
              upstream_body_ms_sum, virtual_cost_micros, cost_input_micros, cost_output_micros, \
              cost_cache_creation_5m_micros, cost_cache_creation_1h_micros, \
              cost_cache_read_micros, cost_cache_write_30m_micros, cost_cache_write_12h_micros, \
              cost_components_event_count, updated_at) \
             SELECT resolution, bucket_start_unix_secs, principal_id, upstream_id, upstream_name, \
                    substr(model, 8), request_count, input_tokens, output_tokens, \
                    cache_creation_input_tokens, cache_read_input_tokens, cache_write_input_tokens, \
                    cache_write_input_tokens_30m, cache_write_input_tokens_12h, error_count, \
                    latency_count, latency_ms_sum, latency_ms_min, latency_ms_max, \
                    proxy_setup_ms_count, proxy_setup_ms_sum, shape_ms_count, shape_ms_sum, \
                    sign_ms_count, sign_ms_sum, upstream_ttfb_ms_count, upstream_ttfb_ms_sum, \
                    upstream_body_ms_count, upstream_body_ms_sum, virtual_cost_micros, \
                    cost_input_micros, cost_output_micros, cost_cache_creation_5m_micros, \
                    cost_cache_creation_1h_micros, cost_cache_read_micros, \
                    cost_cache_write_30m_micros, cost_cache_write_12h_micros, \
                    cost_components_event_count, updated_at \
             FROM usage_rollups_v2 \
             WHERE resolution = ? AND bucket_start_unix_secs = ? AND principal_id = ? \
               AND upstream_id = ? AND model = ? \
             ON CONFLICT (resolution, bucket_start_unix_secs, principal_id, upstream_id, model) \
             DO UPDATE SET \
                 upstream_name = excluded.upstream_name, \
                 request_count = usage_rollups_v2.request_count + excluded.request_count, \
                 input_tokens = usage_rollups_v2.input_tokens + excluded.input_tokens, \
                 output_tokens = usage_rollups_v2.output_tokens + excluded.output_tokens, \
                 cache_creation_input_tokens = usage_rollups_v2.cache_creation_input_tokens + excluded.cache_creation_input_tokens, \
                 cache_read_input_tokens = usage_rollups_v2.cache_read_input_tokens + excluded.cache_read_input_tokens, \
                 cache_write_input_tokens = CASE \
                     WHEN usage_rollups_v2.cache_write_input_tokens IS NULL THEN excluded.cache_write_input_tokens \
                     WHEN excluded.cache_write_input_tokens IS NULL THEN usage_rollups_v2.cache_write_input_tokens \
                     ELSE usage_rollups_v2.cache_write_input_tokens + excluded.cache_write_input_tokens END, \
                 cache_write_input_tokens_30m = CASE \
                     WHEN usage_rollups_v2.cache_write_input_tokens_30m IS NULL THEN excluded.cache_write_input_tokens_30m \
                     WHEN excluded.cache_write_input_tokens_30m IS NULL THEN usage_rollups_v2.cache_write_input_tokens_30m \
                     ELSE usage_rollups_v2.cache_write_input_tokens_30m + excluded.cache_write_input_tokens_30m END, \
                 cache_write_input_tokens_12h = CASE \
                     WHEN usage_rollups_v2.cache_write_input_tokens_12h IS NULL THEN excluded.cache_write_input_tokens_12h \
                     WHEN excluded.cache_write_input_tokens_12h IS NULL THEN usage_rollups_v2.cache_write_input_tokens_12h \
                     ELSE usage_rollups_v2.cache_write_input_tokens_12h + excluded.cache_write_input_tokens_12h END, \
                 error_count = usage_rollups_v2.error_count + excluded.error_count, \
                 latency_count = usage_rollups_v2.latency_count + excluded.latency_count, \
                 latency_ms_sum = usage_rollups_v2.latency_ms_sum + excluded.latency_ms_sum, \
                 latency_ms_min = CASE \
                     WHEN usage_rollups_v2.latency_ms_min IS NULL THEN excluded.latency_ms_min \
                     WHEN excluded.latency_ms_min IS NULL THEN usage_rollups_v2.latency_ms_min \
                     ELSE MIN(usage_rollups_v2.latency_ms_min, excluded.latency_ms_min) END, \
                 latency_ms_max = CASE \
                     WHEN usage_rollups_v2.latency_ms_max IS NULL THEN excluded.latency_ms_max \
                     WHEN excluded.latency_ms_max IS NULL THEN usage_rollups_v2.latency_ms_max \
                     ELSE MAX(usage_rollups_v2.latency_ms_max, excluded.latency_ms_max) END, \
                 proxy_setup_ms_count = usage_rollups_v2.proxy_setup_ms_count + excluded.proxy_setup_ms_count, \
                 proxy_setup_ms_sum = usage_rollups_v2.proxy_setup_ms_sum + excluded.proxy_setup_ms_sum, \
                 shape_ms_count = usage_rollups_v2.shape_ms_count + excluded.shape_ms_count, \
                 shape_ms_sum = usage_rollups_v2.shape_ms_sum + excluded.shape_ms_sum, \
                 sign_ms_count = usage_rollups_v2.sign_ms_count + excluded.sign_ms_count, \
                 sign_ms_sum = usage_rollups_v2.sign_ms_sum + excluded.sign_ms_sum, \
                 upstream_ttfb_ms_count = usage_rollups_v2.upstream_ttfb_ms_count + excluded.upstream_ttfb_ms_count, \
                 upstream_ttfb_ms_sum = usage_rollups_v2.upstream_ttfb_ms_sum + excluded.upstream_ttfb_ms_sum, \
                 upstream_body_ms_count = usage_rollups_v2.upstream_body_ms_count + excluded.upstream_body_ms_count, \
                 upstream_body_ms_sum = usage_rollups_v2.upstream_body_ms_sum + excluded.upstream_body_ms_sum, \
                 virtual_cost_micros = usage_rollups_v2.virtual_cost_micros + excluded.virtual_cost_micros, \
                 cost_input_micros = usage_rollups_v2.cost_input_micros + excluded.cost_input_micros, \
                 cost_output_micros = usage_rollups_v2.cost_output_micros + excluded.cost_output_micros, \
                 cost_cache_creation_5m_micros = usage_rollups_v2.cost_cache_creation_5m_micros + excluded.cost_cache_creation_5m_micros, \
                 cost_cache_creation_1h_micros = usage_rollups_v2.cost_cache_creation_1h_micros + excluded.cost_cache_creation_1h_micros, \
                 cost_cache_read_micros = usage_rollups_v2.cost_cache_read_micros + excluded.cost_cache_read_micros, \
                 cost_cache_write_30m_micros = usage_rollups_v2.cost_cache_write_30m_micros + excluded.cost_cache_write_30m_micros, \
                 cost_cache_write_12h_micros = usage_rollups_v2.cost_cache_write_12h_micros + excluded.cost_cache_write_12h_micros, \
                 cost_components_event_count = usage_rollups_v2.cost_components_event_count + excluded.cost_components_event_count, \
                 updated_at = MAX(usage_rollups_v2.updated_at, excluded.updated_at)",
        )
        .bind(&resolution)
        .bind(bucket_start)
        .bind(&principal)
        .bind(&upstream_id)
        .bind(&model)
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query(
            "INSERT INTO usage_rollup_reconciliation_markers_v1 \
             (resolution, bucket_start_unix_secs, principal_id, upstream_id, marker_model) \
             VALUES (?, ?, ?, ?, substr(?, 8)) \
             ON CONFLICT DO NOTHING",
        )
        .bind(&resolution)
        .bind(bucket_start)
        .bind(&principal)
        .bind(&upstream_id)
        .bind(&model)
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query(
            "DELETE FROM usage_rollups_v2 \
             WHERE resolution = ? AND bucket_start_unix_secs = ? AND principal_id = ? \
               AND upstream_id = ? AND model = ?",
        )
        .bind(&resolution)
        .bind(bucket_start)
        .bind(&principal)
        .bind(&upstream_id)
        .bind(&model)
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
    }
    Ok(())
}

async fn reconcile_gemini_model_identity(
    storage: &SqliteStorage,
) -> StorageResult<Option<UsageRollupRun>> {
    let state = sqlx::query_as::<_, (String, i64, i64, i64)>(
        "SELECT status, cursor, event_upper_bound, rollup_upper_bound \
         FROM usage_rollup_reconciliations_v1 WHERE id = ?",
    )
    .bind(GEMINI_RECONCILIATION_ID)
    .fetch_optional(storage.pool())
    .await
    .map_err(map_sqlx_error)?;
    let Some((status, _, _, _)) = state else {
        return Ok(None);
    };
    if status == "complete" {
        return Ok(None);
    }

    let mut tx = storage.begin_immediate().await?;
    let (status, event_cursor, event_upper_bound, rollup_upper_bound) =
        sqlx::query_as::<_, (String, i64, i64, i64)>(
            "SELECT status, cursor, event_upper_bound, rollup_upper_bound \
             FROM usage_rollup_reconciliations_v1 WHERE id = ?",
        )
        .bind(GEMINI_RECONCILIATION_ID)
        .fetch_one(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
    if status == "complete" {
        tx.rollback().await.map_err(map_sqlx_error)?;
        return Ok(None);
    }

    if status == "pending" {
        let has_event_work = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(SELECT 1 FROM request_events_v1 AS events \
             JOIN upstream_spec_v1 AS upstream ON upstream.id = events.upstream_id \
             WHERE upstream.kind IN ('gemini_api_key', 'gemini_oauth') \
               AND events.model LIKE 'models/%' \
               AND events.model NOT LIKE 'models/models/%' \
               AND events.model NOT LIKE 'models/tunedModels/%')",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        let has_rollup_work = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(SELECT 1 FROM usage_rollups_v2 AS rollup \
             JOIN upstream_spec_v1 AS upstream ON upstream.id = rollup.upstream_id \
             WHERE upstream.kind IN ('gemini_api_key', 'gemini_oauth') \
               AND length(rollup.model) > 7 \
               AND rollup.model LIKE 'models\\_%' ESCAPE '\\' \
               AND rollup.model NOT LIKE 'models\\_models\\_%' ESCAPE '\\' \
               AND rollup.model NOT LIKE 'models\\_tunedModels\\_%' ESCAPE '\\' \
               AND NOT EXISTS ( \
                 SELECT 1 FROM usage_rollup_reconciliation_markers_v1 AS marker \
                 WHERE marker.resolution = rollup.resolution \
                   AND marker.bucket_start_unix_secs = rollup.bucket_start_unix_secs \
                   AND marker.principal_id = rollup.principal_id \
                   AND marker.upstream_id = rollup.upstream_id \
                   AND marker.marker_model = rollup.model \
               )",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        let event_upper_bound =
            sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(id), 0) FROM request_events_v1")
                .fetch_one(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
        let rollup_upper_bound =
            sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(rowid), 0) FROM usage_rollups_v2")
                .fetch_one(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
        let has_nested_work = if has_event_work == 0 && has_rollup_work == 0 {
            sqlx::query_scalar::<_, i64>(
                "SELECT EXISTS(SELECT 1 FROM request_events_v1 AS events \
                 JOIN upstream_spec_v1 AS upstream ON upstream.id = events.upstream_id \
                 WHERE upstream.kind IN ('gemini_api_key', 'gemini_oauth') \
                   AND events.id <= ? \
                   AND events.model LIKE 'models/models/%' \
                   AND events.model NOT LIKE 'models/models/tunedModels/%') \
                 OR EXISTS(SELECT 1 FROM usage_rollups_v2 AS rollup \
                 JOIN upstream_spec_v1 AS upstream ON upstream.id = rollup.upstream_id \
                 WHERE upstream.kind IN ('gemini_api_key', 'gemini_oauth') \
                   AND rollup.rowid <= ? \
                   AND rollup.model LIKE 'models\\_models\\_%' ESCAPE '\\' \
                   AND rollup.model NOT LIKE 'models\\_models\\_tunedModels\\_%' ESCAPE '\\' \
                   AND NOT EXISTS ( \
                     SELECT 1 FROM usage_rollup_reconciliation_markers_v1 AS marker \
                     WHERE marker.resolution = rollup.resolution \
                       AND marker.bucket_start_unix_secs = rollup.bucket_start_unix_secs \
                       AND marker.principal_id = rollup.principal_id \
                       AND marker.upstream_id = rollup.upstream_id \
                       AND marker.marker_model = rollup.model \
                   )",
            )
            .bind(event_upper_bound)
            .bind(rollup_upper_bound)
            .fetch_one(&mut *tx)
            .await
            .map_err(map_sqlx_error)?
        } else {
            0
        };
        let no_reconciliation_work =
            has_event_work == 0 && has_rollup_work == 0 && has_nested_work == 0;
        sqlx::query(
            "UPDATE usage_rollup_reconciliations_v1 \
             SET cursor = 0, event_upper_bound = ?, rollup_upper_bound = ?, \
                 status = CASE WHEN ? <> 0 THEN 'rewriting' \
                               WHEN ? <> 0 THEN 'rewriting' \
                               WHEN ? <> 0 THEN 'rewriting_nested' \
                               ELSE 'complete' END \
             WHERE id = ?",
        )
        .bind(event_upper_bound)
        .bind(rollup_upper_bound)
        .bind(has_event_work)
        .bind(has_rollup_work)
        .bind(has_nested_work)
        .bind(GEMINI_RECONCILIATION_ID)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        if has_event_work == 0 && has_rollup_work != 0 {
            merge_stale_gemini_rollups(&mut tx, false, rollup_upper_bound).await?;
            let remaining = sqlx::query_scalar::<_, i64>(
                "SELECT EXISTS(SELECT 1 FROM usage_rollups_v2 AS rollup \
                 JOIN upstream_spec_v1 AS upstream ON upstream.id = rollup.upstream_id \
                 WHERE upstream.kind IN ('gemini_api_key', 'gemini_oauth') \
                   AND rollup.rowid <= ? \
                   AND length(rollup.model) > 7 \
                   AND rollup.model LIKE 'models\\_%' ESCAPE '\\' \
                   AND rollup.model NOT LIKE 'models\\_tunedModels\\_%' ESCAPE '\\' \
                   AND NOT EXISTS ( \
                     SELECT 1 FROM usage_rollup_reconciliation_markers_v1 AS marker \
                     WHERE marker.resolution = rollup.resolution \
                       AND marker.bucket_start_unix_secs = rollup.bucket_start_unix_secs \
                       AND marker.principal_id = rollup.principal_id \
                       AND marker.upstream_id = rollup.upstream_id \
                       AND marker.marker_model = rollup.model \
                   )",
            )
            .bind(rollup_upper_bound)
            .fetch_one(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
            if remaining == 0 {
                sqlx::query(
                    "UPDATE usage_rollup_reconciliations_v1 SET cursor = 0, status = 'rewriting_nested' \
                     WHERE id = ?",
                )
                .bind(GEMINI_RECONCILIATION_ID)
                .execute(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
            }
        }
        if no_reconciliation_work {
            cleanup_gemini_reconciliation_markers(&mut tx).await?;
        }
        tx.commit().await.map_err(map_sqlx_error)?;
        if no_reconciliation_work {
            return Ok(None);
        }
        return Ok(Some(UsageRollupRun {
            processed_events: 0,
            updated_rollups: 0,
            checkpoint: None,
        }));
    }

    if status == "rewriting_nested" {
        let ids = sqlx::query_scalar::<_, i64>(
            "SELECT events.id \
             FROM request_events_v1 AS events \
             JOIN upstream_spec_v1 AS upstream ON upstream.id = events.upstream_id \
             WHERE upstream.kind IN ('gemini_api_key', 'gemini_oauth') \
               AND events.id > ? \
               AND events.id <= ? \
               AND events.model LIKE 'models/models/%' \
               AND events.model NOT LIKE 'models/models/tunedModels/%' \
             ORDER BY events.id ASC LIMIT ?",
        )
        .bind(event_cursor)
        .bind(event_upper_bound)
        .bind(ROLLUP_BATCH_LIMIT)
        .fetch_all(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        let nested_event_cursor = ids.last().copied().unwrap_or(event_cursor);
        if !ids.is_empty() {
            let placeholders = std::iter::repeat_n("?", ids.len())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "UPDATE request_events_v1 SET model = substr(model, 8) \
                 WHERE id IN ({placeholders}) AND id > ? AND id <= ?"
            );
            let mut query = sqlx::query(AssertSqlSafe(sql));
            for id in &ids {
                query = query.bind(id);
            }
            query = query.bind(event_cursor).bind(event_upper_bound);
            query.execute(&mut *tx).await.map_err(map_sqlx_error)?;
            sqlx::query(
                "UPDATE usage_rollup_reconciliations_v1 SET cursor = ? \
                 WHERE id = ?",
            )
            .bind(nested_event_cursor)
            .bind(GEMINI_RECONCILIATION_ID)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        }
        merge_stale_gemini_rollups(&mut tx, true, rollup_upper_bound).await?;
        let has_nested_work = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(SELECT 1 FROM request_events_v1 AS events \
             JOIN upstream_spec_v1 AS upstream ON upstream.id = events.upstream_id \
             WHERE upstream.kind IN ('gemini_api_key', 'gemini_oauth') \
               AND events.id > ? \
               AND events.id <= ? \
               AND events.model LIKE 'models/models/%' \
               AND events.model NOT LIKE 'models/models/tunedModels/%') \
             OR EXISTS(SELECT 1 FROM usage_rollups_v2 AS rollup \
             JOIN upstream_spec_v1 AS upstream ON upstream.id = rollup.upstream_id \
             WHERE upstream.kind IN ('gemini_api_key', 'gemini_oauth') \
               AND rollup.rowid <= ? \
               AND rollup.model LIKE 'models\\_models\\_%' ESCAPE '\\' \
                   AND rollup.model NOT LIKE 'models\\_models\\_tunedModels\\_%' ESCAPE '\\' \
                   AND NOT EXISTS ( \
                     SELECT 1 FROM usage_rollup_reconciliation_markers_v1 AS marker \
                     WHERE marker.resolution = rollup.resolution \
                       AND marker.bucket_start_unix_secs = rollup.bucket_start_unix_secs \
                       AND marker.principal_id = rollup.principal_id \
                       AND marker.upstream_id = rollup.upstream_id \
                   AND marker.marker_model = rollup.model \
                   )",
        )
        .bind(nested_event_cursor)
        .bind(event_upper_bound)
        .bind(rollup_upper_bound)
        .fetch_one(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        if has_nested_work == 0 {
            cleanup_gemini_reconciliation_markers(&mut tx).await?;
            sqlx::query(
                "UPDATE usage_rollup_reconciliations_v1 SET status = 'complete' \
                 WHERE id = ?",
            )
            .bind(GEMINI_RECONCILIATION_ID)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        }
        tx.commit().await.map_err(map_sqlx_error)?;
        return Ok(Some(UsageRollupRun {
            processed_events: 0,
            updated_rollups: 0,
            checkpoint: None,
        }));
    }

    let ids = sqlx::query_scalar::<_, i64>(
        "SELECT events.id \
         FROM request_events_v1 AS events \
         JOIN upstream_spec_v1 AS upstream ON upstream.id = events.upstream_id \
         WHERE upstream.kind IN ('gemini_api_key', 'gemini_oauth') \
           AND events.id <= ? \
           AND events.model LIKE 'models/%' \
           AND events.model NOT LIKE 'models/models/%' \
           AND events.model NOT LIKE 'models/tunedModels/%' \
         ORDER BY events.id ASC LIMIT ?",
    )
    .bind(event_upper_bound)
    .bind(ROLLUP_BATCH_LIMIT)
    .fetch_all(&mut *tx)
    .await
    .map_err(map_sqlx_error)?;
    if ids.is_empty() {
        merge_stale_gemini_rollups(&mut tx, false, rollup_upper_bound).await?;
    } else {
        let placeholders = std::iter::repeat_n("?", ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "UPDATE request_events_v1 SET model = substr(model, 8) \
             WHERE id IN ({placeholders}) AND id <= ?"
        );
        let mut query = sqlx::query(AssertSqlSafe(sql));
        for id in &ids {
            query = query.bind(id);
        }
        query = query.bind(event_upper_bound);
        query.execute(&mut *tx).await.map_err(map_sqlx_error)?;
        merge_stale_gemini_rollups(&mut tx, false, rollup_upper_bound).await?;
    }
    let has_event_work = sqlx::query_scalar::<_, i64>(
        "SELECT EXISTS(SELECT 1 FROM request_events_v1 AS events \
         JOIN upstream_spec_v1 AS upstream ON upstream.id = events.upstream_id \
         WHERE upstream.kind IN ('gemini_api_key', 'gemini_oauth') \
           AND events.id <= ? \
           AND events.model LIKE 'models/%' \
           AND events.model NOT LIKE 'models/models/%' \
           AND events.model NOT LIKE 'models/tunedModels/%')",
    )
    .bind(event_upper_bound)
    .fetch_one(&mut *tx)
    .await
    .map_err(map_sqlx_error)?;
    let has_rollup_work = sqlx::query_scalar::<_, i64>(
        "SELECT EXISTS(SELECT 1 FROM usage_rollups_v2 AS rollup \
         JOIN upstream_spec_v1 AS upstream ON upstream.id = rollup.upstream_id \
         WHERE upstream.kind IN ('gemini_api_key', 'gemini_oauth') \
           AND rollup.rowid <= ? \
           AND length(rollup.model) > 7 \
           AND rollup.model LIKE 'models\\_%' ESCAPE '\\' \
           AND rollup.model NOT LIKE 'models\\_models\\_%' ESCAPE '\\' \
           AND rollup.model NOT LIKE 'models\\_tunedModels\\_%' ESCAPE '\\' \
           AND NOT EXISTS ( \
             SELECT 1 FROM usage_rollup_reconciliation_markers_v1 AS marker \
             WHERE marker.resolution = rollup.resolution \
               AND marker.bucket_start_unix_secs = rollup.bucket_start_unix_secs \
               AND marker.principal_id = rollup.principal_id \
               AND marker.upstream_id = rollup.upstream_id \
               AND marker.marker_model = rollup.model \
           )",
    )
    .bind(rollup_upper_bound)
    .fetch_one(&mut *tx)
    .await
    .map_err(map_sqlx_error)?;
    if has_event_work == 0 && has_rollup_work == 0 {
        sqlx::query(
            "UPDATE usage_rollup_reconciliations_v1 SET status = 'rewriting_nested' \
             WHERE id = ?",
        )
        .bind(GEMINI_RECONCILIATION_ID)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
    }
    tx.commit().await.map_err(map_sqlx_error)?;
    Ok(Some(UsageRollupRun {
        processed_events: 0,
        updated_rollups: 0,
        checkpoint: None,
    }))
}

async fn rollup_usage_once_inner(storage: &SqliteStorage) -> StorageResult<UsageRollupRun> {
    if let Some(run) = reconcile_gemini_model_identity(storage).await? {
        return Ok(run);
    }
    let pool = storage.pool();
    let previous_checkpoint = read_checkpoint(pool).await?.unwrap_or(0);
    let backfill_upper = read_named_checkpoint(pool, COST_BACKFILL_UPPER_ID).await?;
    let backfill_cursor = read_named_checkpoint(pool, COST_BACKFILL_CURSOR_ID).await?;
    let target_backfill_upper = backfill_upper.unwrap_or(previous_checkpoint);
    let current_backfill_cursor = backfill_cursor.unwrap_or(0);
    let needs_backfill_init = backfill_upper.is_none();
    let needs_backfill_batch = current_backfill_cursor < target_backfill_upper;

    let rows = sqlx::query(
        "SELECT id, ts, list_ts_ms, source_kind, event_kind, principal_id, upstream_id, \
         upstream_name, model, list_status, error_code, input_tokens, output_tokens, \
         cache_creation_input_tokens, cache_read_input_tokens, cache_write_input_tokens, \
         cache_write_input_tokens_30m, cache_write_input_tokens_12h, list_cost_usd_micros, \
         list_cost_input_micros, list_cost_output_micros, \
         list_cost_cache_creation_5m_micros, list_cost_cache_creation_1h_micros, \
         list_cost_cache_read_micros, list_cost_cache_write_30m_micros, \
         list_cost_cache_write_12h_micros, list_duration_ms, list_proxy_setup_ms, \
         list_shape_ms, list_sign_ms, list_upstream_ttfb_ms, list_upstream_body_ms \
         FROM request_events_v1 WHERE id > ? ORDER BY id ASC LIMIT ?",
    )
    .bind(u64_to_i64(previous_checkpoint, "usage rollup checkpoint")?)
    .bind(ROLLUP_BATCH_LIMIT)
    .fetch_all(pool)
    .await
    .map_err(map_sqlx_error)?;

    let backfill_rows = if needs_backfill_batch {
        sqlx::query(
            "SELECT id, ts, list_ts_ms, source_kind, event_kind, principal_id, upstream_id, \
             upstream_name, model, list_status, error_code, input_tokens, output_tokens, \
             cache_creation_input_tokens, cache_read_input_tokens, cache_write_input_tokens, \
             cache_write_input_tokens_30m, cache_write_input_tokens_12h, list_cost_usd_micros, \
             list_cost_input_micros, list_cost_output_micros, \
             list_cost_cache_creation_5m_micros, list_cost_cache_creation_1h_micros, \
             list_cost_cache_read_micros, list_cost_cache_write_30m_micros, \
             list_cost_cache_write_12h_micros, list_duration_ms, list_proxy_setup_ms, \
             list_shape_ms, list_sign_ms, list_upstream_ttfb_ms, list_upstream_body_ms \
             FROM request_events_v1 WHERE id > ? AND id <= ? ORDER BY id ASC LIMIT ?",
        )
        .bind(u64_to_i64(current_backfill_cursor, "cost backfill cursor")?)
        .bind(u64_to_i64(target_backfill_upper, "cost backfill upper")?)
        .bind(ROLLUP_BATCH_LIMIT)
        .fetch_all(pool)
        .await
        .map_err(map_sqlx_error)?
    } else {
        Vec::new()
    };

    if rows.is_empty() && !needs_backfill_init && !needs_backfill_batch {
        return Ok(UsageRollupRun {
            processed_events: 0,
            updated_rollups: 0,
            checkpoint: Some(previous_checkpoint),
        });
    }

    let upstreams = load_upstream_identities(pool).await?;
    let mut max_id = previous_checkpoint;
    let mut deltas: BTreeMap<RollupKey, RollupDelta> = BTreeMap::new();
    let mut overview_excluded_error_deltas: BTreeMap<(UsageRollupResolution, u64), u64> =
        BTreeMap::new();
    let mut backfill_deltas: BTreeMap<RollupKey, RollupDelta> = BTreeMap::new();

    for row in &rows {
        let id = i64_to_u64(
            row.try_get("id").map_err(map_sqlx_error)?,
            "request event id",
        )?;
        let table_upstream_id = row
            .try_get::<Option<String>, _>("upstream_id")
            .map_err(map_sqlx_error)?
            .as_deref()
            .map(|value| parse_uuid(value, "request event upstream_id"))
            .transpose()?;
        let event = rollup_event_from_row(row, table_upstream_id)?;
        let upstream = resolve_upstream_identity(&event, &upstreams);

        max_id = max_id.max(id);
        for resolution in [UsageRollupResolution::Minute, UsageRollupResolution::Hour] {
            let key = rollup_key(&event, &upstream, resolution);
            if is_overview_excluded_status(event.status) {
                let count = overview_excluded_error_deltas
                    .entry((resolution, key.bucket_start))
                    .or_default();
                *count = count.saturating_add(1);
            }
            deltas.entry(key).or_default().add_event(&event);
        }
    }

    let mut next_backfill_cursor = current_backfill_cursor;
    if needs_backfill_batch {
        next_backfill_cursor = if backfill_rows.len() < ROLLUP_BATCH_LIMIT as usize {
            target_backfill_upper
        } else {
            i64_to_u64(
                backfill_rows
                    .last()
                    .ok_or_else(|| StorageError::Corrupted {
                        message: "empty cost backfill batch reached full batch limit".to_owned(),
                    })?
                    .try_get("id")
                    .map_err(map_sqlx_error)?,
                "cost backfill cursor",
            )?
        };
        for row in &backfill_rows {
            let table_upstream_id = row
                .try_get::<Option<String>, _>("upstream_id")
                .map_err(map_sqlx_error)?
                .as_deref()
                .map(|value| parse_uuid(value, "request event upstream_id"))
                .transpose()?;
            let event = rollup_event_from_row(row, table_upstream_id)?;
            let upstream = resolve_upstream_identity(&event, &upstreams);
            for resolution in [UsageRollupResolution::Minute, UsageRollupResolution::Hour] {
                let key = rollup_key(&event, &upstream, resolution);
                backfill_deltas
                    .entry(key)
                    .or_default()
                    .add_cost_components(&event);
            }
        }
    }

    let updated_rollups = u64::try_from(deltas.len().saturating_add(backfill_deltas.len()))
        .map_err(|_| StorageError::Fatal {
            message: "usage rollup aggregate count cannot be represented as u64".to_owned(),
        })?;

    let mut tx = storage.begin_immediate().await?;
    let current_checkpoint = read_checkpoint(&mut *tx).await?.unwrap_or(0);
    let current_backfill_upper = read_named_checkpoint(&mut *tx, COST_BACKFILL_UPPER_ID).await?;
    let current_backfill_cursor = read_named_checkpoint(&mut *tx, COST_BACKFILL_CURSOR_ID).await?;
    if current_checkpoint != previous_checkpoint
        || current_backfill_upper != backfill_upper
        || current_backfill_cursor != backfill_cursor
    {
        return Ok(UsageRollupRun {
            processed_events: 0,
            updated_rollups: 0,
            checkpoint: Some(current_checkpoint),
        });
    }

    for (key, delta) in deltas {
        upsert_rollup_delta(&mut tx, &key, &delta).await?;
    }
    for (key, delta) in backfill_deltas {
        if delta.cost_components_event_count == 0 {
            continue;
        }
        sqlx::query(
            "UPDATE usage_rollups_v2 SET \
             cost_input_micros = cost_input_micros + ?, \
             cost_output_micros = cost_output_micros + ?, \
             cost_cache_creation_5m_micros = cost_cache_creation_5m_micros + ?, \
             cost_cache_creation_1h_micros = cost_cache_creation_1h_micros + ?, \
             cost_cache_read_micros = cost_cache_read_micros + ?, \
             cost_cache_write_30m_micros = cost_cache_write_30m_micros + ?, \
             cost_cache_write_12h_micros = cost_cache_write_12h_micros + ?, \
             cost_components_event_count = cost_components_event_count + ?, \
             updated_at = unixepoch() \
             WHERE resolution = ? AND bucket_start_unix_secs = ? AND principal_id = ? \
               AND upstream_id = ? AND model = ?",
        )
        .bind(u64_to_i64(
            delta.cost_input_micros,
            "rollup cost_input_micros",
        )?)
        .bind(u64_to_i64(
            delta.cost_output_micros,
            "rollup cost_output_micros",
        )?)
        .bind(u64_to_i64(
            delta.cost_cache_creation_5m_micros,
            "rollup cost_cache_creation_5m_micros",
        )?)
        .bind(u64_to_i64(
            delta.cost_cache_creation_1h_micros,
            "rollup cost_cache_creation_1h_micros",
        )?)
        .bind(u64_to_i64(
            delta.cost_cache_read_micros,
            "rollup cost_cache_read_micros",
        )?)
        .bind(u64_to_i64(
            delta.cost_cache_write_30m_micros,
            "rollup cost_cache_write_30m_micros",
        )?)
        .bind(u64_to_i64(
            delta.cost_cache_write_12h_micros,
            "rollup cost_cache_write_12h_micros",
        )?)
        .bind(u64_to_i64(
            delta.cost_components_event_count,
            "rollup cost_components_event_count",
        )?)
        .bind(key.resolution.as_str())
        .bind(u64_to_i64(key.bucket_start, "usage rollup bucket_start")?)
        .bind(&key.principal)
        .bind(key.upstream_id.to_string())
        .bind(&key.model)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
    }
    for ((resolution, bucket_start), error_count) in overview_excluded_error_deltas {
        sqlx::query(
            "INSERT INTO overview_excluded_error_rollups_v1 \
             (resolution, bucket_start_unix_secs, error_count, updated_at) \
             VALUES (?, ?, ?, unixepoch()) \
             ON CONFLICT (resolution, bucket_start_unix_secs) DO UPDATE SET \
                 error_count = overview_excluded_error_rollups_v1.error_count + excluded.error_count, \
                 updated_at = unixepoch()",
        )
        .bind(resolution.as_str())
        .bind(u64_to_i64(
            bucket_start,
            "overview excluded error bucket",
        )?)
        .bind(u64_to_i64(
            error_count,
            "overview excluded error count",
        )?)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
    }

    if needs_backfill_init {
        persist_named_checkpoint_in_tx(&mut tx, COST_BACKFILL_UPPER_ID, target_backfill_upper)
            .await?;
        persist_named_checkpoint_in_tx(
            &mut tx,
            COST_BACKFILL_CURSOR_ID,
            if needs_backfill_batch {
                next_backfill_cursor
            } else {
                0
            },
        )
        .await?;
    } else if needs_backfill_batch {
        persist_named_checkpoint_in_tx(&mut tx, COST_BACKFILL_CURSOR_ID, next_backfill_cursor)
            .await?;
    }
    if !rows.is_empty() {
        persist_checkpoint_in_tx(&mut tx, max_id).await?;
    }
    tx.commit().await.map_err(map_sqlx_error)?;
    Ok(UsageRollupRun {
        processed_events: rows.len() as u64,
        updated_rollups,
        checkpoint: Some(max_id),
    })
}
fn rollup_key(
    event: &RequestEvent,
    upstream: &UpstreamIdentity,
    resolution: UsageRollupResolution,
) -> RollupKey {
    RollupKey {
        resolution,
        bucket_start: bucket_start(resolution, event_ts_secs(event)),
        principal: normalize_usage_rollup_dimension(event.principal_id.as_deref()),
        upstream_id: upstream.id,
        upstream_name: normalize_usage_rollup_dimension(Some(&upstream.name)),
        model: normalize_usage_rollup_dimension(event.model.as_deref()),
    }
}

async fn upsert_rollup_delta(
    tx: &mut Transaction<'_, Sqlite>,
    key: &RollupKey,
    delta: &RollupDelta,
) -> StorageResult<()> {
    sqlx::query(
        "INSERT INTO usage_rollups_v2 \
          (resolution, bucket_start_unix_secs, principal_id, upstream_id, upstream_name, model, \
           request_count, input_tokens, output_tokens, cache_creation_input_tokens, \
           cache_read_input_tokens, cache_write_input_tokens, cache_write_input_tokens_30m, \
           cache_write_input_tokens_12h, error_count, latency_count, latency_ms_sum, \
           latency_ms_min, latency_ms_max, proxy_setup_ms_count, proxy_setup_ms_sum, \
           shape_ms_count, shape_ms_sum, sign_ms_count, sign_ms_sum, \
           upstream_ttfb_ms_count, upstream_ttfb_ms_sum, upstream_body_ms_count, \
           upstream_body_ms_sum, virtual_cost_micros, cost_input_micros, cost_output_micros, \
           cost_cache_creation_5m_micros, cost_cache_creation_1h_micros, \
           cost_cache_read_micros, cost_cache_write_30m_micros, cost_cache_write_12h_micros, \
           cost_components_event_count, updated_at) \
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, unixepoch()) \
          ON CONFLICT (resolution, bucket_start_unix_secs, principal_id, upstream_id, model) DO UPDATE \
          SET upstream_name = excluded.upstream_name, \
              request_count = usage_rollups_v2.request_count + excluded.request_count, \
              input_tokens = usage_rollups_v2.input_tokens + excluded.input_tokens, \
              output_tokens = usage_rollups_v2.output_tokens + excluded.output_tokens, \
              cache_creation_input_tokens = usage_rollups_v2.cache_creation_input_tokens + excluded.cache_creation_input_tokens, \
              cache_read_input_tokens = usage_rollups_v2.cache_read_input_tokens + excluded.cache_read_input_tokens, \
              cache_write_input_tokens = CASE \
                  WHEN usage_rollups_v2.cache_write_input_tokens IS NULL THEN excluded.cache_write_input_tokens \
                  WHEN excluded.cache_write_input_tokens IS NULL THEN usage_rollups_v2.cache_write_input_tokens \
                  ELSE usage_rollups_v2.cache_write_input_tokens + excluded.cache_write_input_tokens \
              END, \
              cache_write_input_tokens_30m = CASE \
                  WHEN usage_rollups_v2.cache_write_input_tokens_30m IS NULL THEN excluded.cache_write_input_tokens_30m \
                  WHEN excluded.cache_write_input_tokens_30m IS NULL THEN usage_rollups_v2.cache_write_input_tokens_30m \
                  ELSE usage_rollups_v2.cache_write_input_tokens_30m + excluded.cache_write_input_tokens_30m \
              END, \
              cache_write_input_tokens_12h = CASE \
                  WHEN usage_rollups_v2.cache_write_input_tokens_12h IS NULL THEN excluded.cache_write_input_tokens_12h \
                  WHEN excluded.cache_write_input_tokens_12h IS NULL THEN usage_rollups_v2.cache_write_input_tokens_12h \
                  ELSE usage_rollups_v2.cache_write_input_tokens_12h + excluded.cache_write_input_tokens_12h \
              END, \
              error_count = usage_rollups_v2.error_count + excluded.error_count, \
              latency_count = usage_rollups_v2.latency_count + excluded.latency_count, \
              latency_ms_sum = usage_rollups_v2.latency_ms_sum + excluded.latency_ms_sum, \
              latency_ms_min = CASE \
                  WHEN usage_rollups_v2.latency_ms_min IS NULL THEN excluded.latency_ms_min \
                  WHEN excluded.latency_ms_min IS NULL THEN usage_rollups_v2.latency_ms_min \
                  ELSE MIN(usage_rollups_v2.latency_ms_min, excluded.latency_ms_min) \
              END, \
              latency_ms_max = CASE \
                  WHEN usage_rollups_v2.latency_ms_max IS NULL THEN excluded.latency_ms_max \
                  WHEN excluded.latency_ms_max IS NULL THEN usage_rollups_v2.latency_ms_max \
                  ELSE MAX(usage_rollups_v2.latency_ms_max, excluded.latency_ms_max) \
              END, \
              proxy_setup_ms_count = usage_rollups_v2.proxy_setup_ms_count + excluded.proxy_setup_ms_count, \
              proxy_setup_ms_sum = usage_rollups_v2.proxy_setup_ms_sum + excluded.proxy_setup_ms_sum, \
              shape_ms_count = usage_rollups_v2.shape_ms_count + excluded.shape_ms_count, \
              shape_ms_sum = usage_rollups_v2.shape_ms_sum + excluded.shape_ms_sum, \
              sign_ms_count = usage_rollups_v2.sign_ms_count + excluded.sign_ms_count, \
              sign_ms_sum = usage_rollups_v2.sign_ms_sum + excluded.sign_ms_sum, \
              upstream_ttfb_ms_count = usage_rollups_v2.upstream_ttfb_ms_count + excluded.upstream_ttfb_ms_count, \
              upstream_ttfb_ms_sum = usage_rollups_v2.upstream_ttfb_ms_sum + excluded.upstream_ttfb_ms_sum, \
              upstream_body_ms_count = usage_rollups_v2.upstream_body_ms_count + excluded.upstream_body_ms_count, \
              upstream_body_ms_sum = usage_rollups_v2.upstream_body_ms_sum + excluded.upstream_body_ms_sum, \
              virtual_cost_micros = usage_rollups_v2.virtual_cost_micros + excluded.virtual_cost_micros, \
              cost_input_micros = usage_rollups_v2.cost_input_micros + excluded.cost_input_micros, \
              cost_output_micros = usage_rollups_v2.cost_output_micros + excluded.cost_output_micros, \
              cost_cache_creation_5m_micros = usage_rollups_v2.cost_cache_creation_5m_micros + excluded.cost_cache_creation_5m_micros, \
              cost_cache_creation_1h_micros = usage_rollups_v2.cost_cache_creation_1h_micros + excluded.cost_cache_creation_1h_micros, \
              cost_cache_read_micros = usage_rollups_v2.cost_cache_read_micros + excluded.cost_cache_read_micros, \
              cost_cache_write_30m_micros = usage_rollups_v2.cost_cache_write_30m_micros + excluded.cost_cache_write_30m_micros, \
              cost_cache_write_12h_micros = usage_rollups_v2.cost_cache_write_12h_micros + excluded.cost_cache_write_12h_micros, \
              cost_components_event_count = usage_rollups_v2.cost_components_event_count + excluded.cost_components_event_count, \
              updated_at = unixepoch()",
    )
    .bind(key.resolution.as_str())
    .bind(u64_to_i64(key.bucket_start, "usage rollup bucket_start")?)
    .bind(&key.principal)
    .bind(key.upstream_id.to_string())
    .bind(&key.upstream_name)
    .bind(&key.model)
    .bind(u64_to_i64(delta.request_count, "rollup request_count")?)
    .bind(u64_to_i64(delta.input_tokens, "rollup input_tokens")?)
    .bind(u64_to_i64(delta.output_tokens, "rollup output_tokens")?)
    .bind(u64_to_i64(
        delta.cache_creation_input_tokens,
        "rollup cache_creation_input_tokens",
    )?)
    .bind(u64_to_i64(delta.cache_read_input_tokens, "rollup cache_read_input_tokens")?)
    .bind(option_u64_to_i64(
        delta.cache_write_input_tokens,
        "rollup cache_write_input_tokens",
    )?)
    .bind(option_u64_to_i64(
        delta.cache_write_input_tokens_30m,
        "rollup cache_write_input_tokens_30m",
    )?)
    .bind(option_u64_to_i64(
        delta.cache_write_input_tokens_12h,
        "rollup cache_write_input_tokens_12h",
    )?)
    .bind(u64_to_i64(delta.error_count, "rollup error_count")?)
    .bind(u64_to_i64(delta.latency_count, "rollup latency_count")?)
    .bind(u64_to_i64(delta.latency_ms_sum, "rollup latency_ms_sum")?)
    .bind(option_u64_to_i64(delta.latency_ms_min, "rollup latency_ms_min")?)
    .bind(option_u64_to_i64(delta.latency_ms_max, "rollup latency_ms_max")?)
    .bind(u64_to_i64(delta.proxy_setup_ms_count, "rollup proxy_setup_ms_count")?)
    .bind(u64_to_i64(delta.proxy_setup_ms_sum, "rollup proxy_setup_ms_sum")?)
    .bind(u64_to_i64(delta.shape_ms_count, "rollup shape_ms_count")?)
    .bind(u64_to_i64(delta.shape_ms_sum, "rollup shape_ms_sum")?)
    .bind(u64_to_i64(delta.sign_ms_count, "rollup sign_ms_count")?)
    .bind(u64_to_i64(delta.sign_ms_sum, "rollup sign_ms_sum")?)
    .bind(u64_to_i64(delta.upstream_ttfb_ms_count, "rollup upstream_ttfb_ms_count")?)
    .bind(u64_to_i64(delta.upstream_ttfb_ms_sum, "rollup upstream_ttfb_ms_sum")?)
    .bind(u64_to_i64(delta.upstream_body_ms_count, "rollup upstream_body_ms_count")?)
    .bind(u64_to_i64(delta.upstream_body_ms_sum, "rollup upstream_body_ms_sum")?)
    .bind(u64_to_i64(delta.virtual_cost_micros, "rollup virtual_cost_micros")?)
    .bind(u64_to_i64(delta.cost_input_micros, "rollup cost_input_micros")?)
    .bind(u64_to_i64(delta.cost_output_micros, "rollup cost_output_micros")?)
    .bind(u64_to_i64(
        delta.cost_cache_creation_5m_micros,
        "rollup cost_cache_creation_5m_micros",
    )?)
    .bind(u64_to_i64(
        delta.cost_cache_creation_1h_micros,
        "rollup cost_cache_creation_1h_micros",
    )?)
    .bind(u64_to_i64(delta.cost_cache_read_micros, "rollup cost_cache_read_micros")?)
    .bind(u64_to_i64(
        delta.cost_cache_write_30m_micros,
        "rollup cost_cache_write_30m_micros",
    )?)
    .bind(u64_to_i64(
        delta.cost_cache_write_12h_micros,
        "rollup cost_cache_write_12h_micros",
    )?)
    .bind(u64_to_i64(
        delta.cost_components_event_count,
        "rollup cost_components_event_count",
    )?)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

fn rollup_event_from_row(
    row: &SqliteRow,
    upstream_id: Option<Uuid>,
) -> StorageResult<RequestEvent> {
    let optional_u64 = |column: &str| -> StorageResult<Option<u64>> {
        row.try_get::<Option<i64>, _>(column)
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, column))
            .transpose()
    };
    Ok(RequestEvent {
        ts: i64_to_u64(
            row.try_get("ts").map_err(map_sqlx_error)?,
            "request event ts",
        )?,
        ts_ms: optional_u64("list_ts_ms")?,
        source_kind: row.try_get("source_kind").map_err(map_sqlx_error)?,
        event_kind: row
            .try_get::<Option<String>, _>("event_kind")
            .map_err(map_sqlx_error)?
            .map(|value| {
                value.parse().map_err(|_| StorageError::Corrupted {
                    message: format!("request event unknown event_kind value: {value}"),
                })
            })
            .transpose()?,
        principal_id: row.try_get("principal_id").map_err(map_sqlx_error)?,
        upstream_id,
        upstream_name: row.try_get("upstream_name").map_err(map_sqlx_error)?,
        model: row.try_get("model").map_err(map_sqlx_error)?,
        status: u16::try_from(
            row.try_get::<i64, _>("list_status")
                .map_err(map_sqlx_error)?,
        )
        .map_err(|_| StorageError::Corrupted {
            message: "request event rollup status out of range".to_owned(),
        })?,
        error_code: row.try_get("error_code").map_err(map_sqlx_error)?,
        input_tokens: optional_u64("input_tokens")?,
        output_tokens: optional_u64("output_tokens")?,
        cache_creation_input_tokens: optional_u64("cache_creation_input_tokens")?,
        cache_read_input_tokens: optional_u64("cache_read_input_tokens")?,
        cache_write_input_tokens: optional_u64("cache_write_input_tokens")?,
        cache_write_input_tokens_30m: optional_u64("cache_write_input_tokens_30m")?,
        cache_write_input_tokens_12h: optional_u64("cache_write_input_tokens_12h")?,
        cost_usd_micros: row
            .try_get("list_cost_usd_micros")
            .map_err(map_sqlx_error)?,
        cost_input_micros: row
            .try_get("list_cost_input_micros")
            .map_err(map_sqlx_error)?,
        cost_output_micros: row
            .try_get("list_cost_output_micros")
            .map_err(map_sqlx_error)?,
        cost_cache_creation_5m_micros: row
            .try_get("list_cost_cache_creation_5m_micros")
            .map_err(map_sqlx_error)?,
        cost_cache_creation_1h_micros: row
            .try_get("list_cost_cache_creation_1h_micros")
            .map_err(map_sqlx_error)?,
        cost_cache_read_micros: row
            .try_get("list_cost_cache_read_micros")
            .map_err(map_sqlx_error)?,
        cost_cache_write_30m_micros: row
            .try_get("list_cost_cache_write_30m_micros")
            .map_err(map_sqlx_error)?,
        cost_cache_write_12h_micros: row
            .try_get("list_cost_cache_write_12h_micros")
            .map_err(map_sqlx_error)?,
        duration_ms: optional_u64("list_duration_ms")?.unwrap_or(0),
        proxy_setup_ms: optional_u64("list_proxy_setup_ms")?,
        shape_ms: optional_u64("list_shape_ms")?,
        sign_ms: optional_u64("list_sign_ms")?,
        upstream_ttfb_ms: optional_u64("list_upstream_ttfb_ms")?,
        upstream_body_ms: optional_u64("list_upstream_body_ms")?,
        ..RequestEvent::default()
    })
}

fn is_overview_excluded_status(status: u16) -> bool {
    matches!(status, 401 | 403 | 404)
}

async fn read_checkpoint<'e, E>(executor: E) -> StorageResult<Option<u64>>
where
    E: sqlx::Executor<'e, Database = Sqlite>,
{
    read_named_checkpoint(executor, CHECKPOINT_ID).await
}

async fn read_named_checkpoint<'e, E>(executor: E, id: &str) -> StorageResult<Option<u64>>
where
    E: sqlx::Executor<'e, Database = Sqlite>,
{
    let value =
        sqlx::query_scalar::<_, i64>("SELECT value FROM usage_rollup_checkpoints_v1 WHERE id = ?")
            .bind(id)
            .fetch_optional(executor)
            .await
            .map_err(map_sqlx_error)?;

    value
        .map(|value| i64_to_u64(value, "usage rollup checkpoint"))
        .transpose()
}

async fn persist_checkpoint_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    checkpoint: u64,
) -> StorageResult<()> {
    persist_named_checkpoint_in_tx(tx, CHECKPOINT_ID, checkpoint).await
}

async fn persist_named_checkpoint_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    id: &str,
    checkpoint: u64,
) -> StorageResult<()> {
    sqlx::query(
        "INSERT INTO usage_rollup_checkpoints_v1 (id, value) VALUES (?, ?) \
         ON CONFLICT(id) DO UPDATE SET value = excluded.value \
         WHERE usage_rollup_checkpoints_v1.value < excluded.value",
    )
    .bind(id)
    .bind(u64_to_i64(checkpoint, "usage rollup checkpoint")?)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

async fn load_upstream_identities<'e, E>(
    executor: E,
) -> StorageResult<HashMap<String, UpstreamIdentity>>
where
    E: sqlx::Executor<'e, Database = Sqlite>,
{
    let rows = sqlx::query("SELECT id, name FROM upstream_spec_v1 WHERE deleted_at IS NULL")
        .fetch_all(executor)
        .await
        .map_err(map_sqlx_error)?;
    let mut upstreams = HashMap::new();
    for row in rows {
        let id: String = row.try_get("id").map_err(map_sqlx_error)?;
        let identity = UpstreamIdentity {
            id: parse_uuid(&id, "upstream id")?,
            name: row.try_get("name").map_err(map_sqlx_error)?,
        };
        upstreams.insert(identity.id.to_string(), identity.clone());
        upstreams.insert(identity.name.clone(), identity);
    }
    Ok(upstreams)
}

fn resolve_upstream_identity(
    event: &RequestEvent,
    upstreams: &HashMap<String, UpstreamIdentity>,
) -> UpstreamIdentity {
    if let Some(upstream_id) = event.upstream_id {
        if let Some(name) = event.upstream_name.clone() {
            return UpstreamIdentity {
                id: upstream_id,
                name,
            };
        }
        if let Some(identity) = upstreams.get(&upstream_id.to_string()) {
            return identity.clone();
        }
        return UpstreamIdentity {
            id: upstream_id,
            name: event_upstream_name(event),
        };
    }
    let name = event_upstream_name(event);
    upstreams.get(&name).cloned().unwrap_or(UpstreamIdentity {
        id: Uuid::nil(),
        name,
    })
}

fn row_to_usage_rollup(row: SqliteRow) -> StorageResult<UsageRollup> {
    let resolution = usage_rollup_resolution_from_str(
        &row.try_get::<String, _>("resolution")
            .map_err(map_sqlx_error)?,
    )?;
    let bucket_start = row
        .try_get::<i64, _>("bucket_start_unix_secs")
        .map_err(map_sqlx_error)?;
    let upstream_id: String = row.try_get("upstream_id").map_err(map_sqlx_error)?;

    Ok(UsageRollup {
        resolution,
        bucket_start: i64_to_u64(bucket_start, "usage rollup bucket_start")?,
        principal: row.try_get("principal_id").map_err(map_sqlx_error)?,
        upstream_id: parse_uuid(&upstream_id, "usage rollup upstream_id")?,
        upstream_name: row.try_get("upstream_name").map_err(map_sqlx_error)?,
        model: row.try_get("model").map_err(map_sqlx_error)?,
        request_count: i64_to_u64(
            row.try_get("request_count").map_err(map_sqlx_error)?,
            "rollup request_count",
        )?,
        input_tokens: i64_to_u64(
            row.try_get("input_tokens").map_err(map_sqlx_error)?,
            "rollup input_tokens",
        )?,
        output_tokens: i64_to_u64(
            row.try_get("output_tokens").map_err(map_sqlx_error)?,
            "rollup output_tokens",
        )?,
        cache_creation_input_tokens: i64_to_u64(
            row.try_get("cache_creation_input_tokens")
                .map_err(map_sqlx_error)?,
            "rollup cache_creation_input_tokens",
        )?,
        cache_read_input_tokens: i64_to_u64(
            row.try_get("cache_read_input_tokens")
                .map_err(map_sqlx_error)?,
            "rollup cache_read_input_tokens",
        )?,
        cache_write_input_tokens: row
            .try_get::<Option<i64>, _>("cache_write_input_tokens")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "rollup cache_write_input_tokens"))
            .transpose()?,
        cache_write_input_tokens_30m: row
            .try_get::<Option<i64>, _>("cache_write_input_tokens_30m")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "rollup cache_write_input_tokens_30m"))
            .transpose()?,
        cache_write_input_tokens_12h: row
            .try_get::<Option<i64>, _>("cache_write_input_tokens_12h")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "rollup cache_write_input_tokens_12h"))
            .transpose()?,
        error_count: i64_to_u64(
            row.try_get("error_count").map_err(map_sqlx_error)?,
            "rollup error_count",
        )?,
        latency_count: i64_to_u64(
            row.try_get("latency_count").map_err(map_sqlx_error)?,
            "rollup latency_count",
        )?,
        latency_ms_sum: i64_to_u64(
            row.try_get("latency_ms_sum").map_err(map_sqlx_error)?,
            "rollup latency_ms_sum",
        )?,
        latency_ms_min: row
            .try_get::<Option<i64>, _>("latency_ms_min")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "rollup latency_ms_min"))
            .transpose()?,
        latency_ms_max: row
            .try_get::<Option<i64>, _>("latency_ms_max")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "rollup latency_ms_max"))
            .transpose()?,
        proxy_setup_ms_count: i64_to_u64(
            row.try_get("proxy_setup_ms_count")
                .map_err(map_sqlx_error)?,
            "rollup proxy_setup_ms_count",
        )?,
        proxy_setup_ms_sum: i64_to_u64(
            row.try_get("proxy_setup_ms_sum").map_err(map_sqlx_error)?,
            "rollup proxy_setup_ms_sum",
        )?,
        shape_ms_count: i64_to_u64(
            row.try_get("shape_ms_count").map_err(map_sqlx_error)?,
            "rollup shape_ms_count",
        )?,
        shape_ms_sum: i64_to_u64(
            row.try_get("shape_ms_sum").map_err(map_sqlx_error)?,
            "rollup shape_ms_sum",
        )?,
        sign_ms_count: i64_to_u64(
            row.try_get("sign_ms_count").map_err(map_sqlx_error)?,
            "rollup sign_ms_count",
        )?,
        sign_ms_sum: i64_to_u64(
            row.try_get("sign_ms_sum").map_err(map_sqlx_error)?,
            "rollup sign_ms_sum",
        )?,
        upstream_ttfb_ms_count: i64_to_u64(
            row.try_get("upstream_ttfb_ms_count")
                .map_err(map_sqlx_error)?,
            "rollup upstream_ttfb_ms_count",
        )?,
        upstream_ttfb_ms_sum: i64_to_u64(
            row.try_get("upstream_ttfb_ms_sum")
                .map_err(map_sqlx_error)?,
            "rollup upstream_ttfb_ms_sum",
        )?,
        upstream_body_ms_count: i64_to_u64(
            row.try_get("upstream_body_ms_count")
                .map_err(map_sqlx_error)?,
            "rollup upstream_body_ms_count",
        )?,
        upstream_body_ms_sum: i64_to_u64(
            row.try_get("upstream_body_ms_sum")
                .map_err(map_sqlx_error)?,
            "rollup upstream_body_ms_sum",
        )?,
        virtual_cost_micros: i64_to_u64(
            row.try_get("virtual_cost_micros").map_err(map_sqlx_error)?,
            "rollup virtual_cost_micros",
        )?,
        cost_input_micros: i64_to_u64(
            row.try_get("cost_input_micros").map_err(map_sqlx_error)?,
            "rollup cost_input_micros",
        )?,
        cost_output_micros: i64_to_u64(
            row.try_get("cost_output_micros").map_err(map_sqlx_error)?,
            "rollup cost_output_micros",
        )?,
        cost_cache_creation_5m_micros: i64_to_u64(
            row.try_get("cost_cache_creation_5m_micros")
                .map_err(map_sqlx_error)?,
            "rollup cost_cache_creation_5m_micros",
        )?,
        cost_cache_creation_1h_micros: i64_to_u64(
            row.try_get("cost_cache_creation_1h_micros")
                .map_err(map_sqlx_error)?,
            "rollup cost_cache_creation_1h_micros",
        )?,
        cost_cache_read_micros: i64_to_u64(
            row.try_get("cost_cache_read_micros")
                .map_err(map_sqlx_error)?,
            "rollup cost_cache_read_micros",
        )?,
        cost_cache_write_30m_micros: i64_to_u64(
            row.try_get("cost_cache_write_30m_micros")
                .map_err(map_sqlx_error)?,
            "rollup cost_cache_write_30m_micros",
        )?,
        cost_cache_write_12h_micros: i64_to_u64(
            row.try_get("cost_cache_write_12h_micros")
                .map_err(map_sqlx_error)?,
            "rollup cost_cache_write_12h_micros",
        )?,
        cost_components_event_count: i64_to_u64(
            row.try_get("cost_components_event_count")
                .map_err(map_sqlx_error)?,
            "rollup cost_components_event_count",
        )?,
    })
}

fn usage_rollup_resolution_from_str(value: &str) -> StorageResult<UsageRollupResolution> {
    match value {
        "minute" => Ok(UsageRollupResolution::Minute),
        "hour" => Ok(UsageRollupResolution::Hour),
        other => Err(StorageError::Corrupted {
            message: format!("unknown usage rollup resolution {other}"),
        }),
    }
}

fn bucket_start(resolution: UsageRollupResolution, ts: u64) -> u64 {
    let width = match resolution {
        UsageRollupResolution::Minute => MINUTE_SECS,
        UsageRollupResolution::Hour => HOUR_SECS,
    };
    ts - (ts % width)
}

fn event_ts_secs(event: &RequestEvent) -> u64 {
    event.ts_ms.map(|ts_ms| ts_ms / 1000).unwrap_or(event.ts)
}

fn event_upstream_name(event: &RequestEvent) -> String {
    event
        .upstream_name
        .as_deref()
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| normalize_usage_rollup_dimension(None))
}

fn option_u64_to_i64(value: Option<u64>, field: &str) -> StorageResult<Option<i64>> {
    value.map(|value| u64_to_i64(value, field)).transpose()
}

fn parse_uuid(value: &str, field: &str) -> StorageResult<Uuid> {
    Uuid::parse_str(value).map_err(|error| StorageError::Corrupted {
        message: format!("invalid {field} {value}: {error}"),
    })
}

fn u64_to_i64(value: u64, field: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| StorageError::Fatal {
        message: format!("{field} cannot be represented as sqlite INTEGER"),
    })
}

fn i64_to_u64(value: i64, field: &str) -> StorageResult<u64> {
    u64::try_from(value).map_err(|_| StorageError::Corrupted {
        message: format!("{field} is negative in sqlite storage"),
    })
}

fn add_optional(target: &mut Option<u64>, value: Option<u64>) {
    if let Some(value) = value {
        *target = Some(target.unwrap_or_default().saturating_add(value));
    }
}

fn min_option(current: Option<u64>, next: Option<u64>) -> Option<u64> {
    match (current, next) {
        (Some(current), Some(next)) => Some(current.min(next)),
        (Some(current), None) => Some(current),
        (None, Some(next)) => Some(next),
        (None, None) => None,
    }
}

fn max_option(current: Option<u64>, next: Option<u64>) -> Option<u64> {
    match (current, next) {
        (Some(current), Some(next)) => Some(current.max(next)),
        (Some(current), None) => Some(current),
        (None, Some(next)) => Some(next),
        (None, None) => None,
    }
}
