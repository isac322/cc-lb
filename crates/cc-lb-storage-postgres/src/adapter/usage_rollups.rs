use std::collections::BTreeMap;

use async_trait::async_trait;
use cc_lb_storage_api::{
    StorageError, StorageResult, UsageRollup, UsageRollupResolution, UsageRollupRun,
    UsageRollupStore,
};
use chrono::{DateTime, Utc};
use sqlx::{Row, postgres::PgRow};

use crate::{
    adapter::{PostgresStorage, datetime_to_unix_secs, i64_to_u64, unix_secs_to_datetime},
    error_map::map_sqlx_error,
};

const CHECKPOINT_ID: &str = "high_water";
const ROLLUP_BATCH_LIMIT: i64 = 10_000;
const MINUTE_SECS: u64 = 60;
const HOUR_SECS: u64 = 60 * 60;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RollupKey {
    resolution: UsageRollupResolution,
    bucket_start: u64,
    principal: String,
    upstream: String,
    model: String,
}

#[async_trait]
impl UsageRollupStore for PostgresStorage {
    async fn rollup_usage_once(&self) -> StorageResult<UsageRollupRun> {
        rollup_usage_once_inner(self).await
    }

    async fn query_usage_rollups(&self) -> StorageResult<Vec<UsageRollup>> {
        let rows = sqlx::query(
            "SELECT resolution, bucket_start, principal_id, upstream, model, value              FROM usage_rollups_v1 ORDER BY bucket_start ASC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        rows.into_iter().map(row_to_usage_rollup).collect()
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
            "SELECT resolution, bucket_start, principal_id, upstream, model, value              FROM usage_rollups_v1              WHERE resolution = $1 AND bucket_start BETWEEN $2 AND $3              ORDER BY bucket_start ASC",
        )
        .bind(resolution.as_str())
        .bind(unix_secs_to_datetime(
            window_start_unix_secs,
            "usage rollup range start",
        )?)
        .bind(unix_secs_to_datetime(
            window_end_unix_secs,
            "usage rollup range end",
        )?)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        rows.into_iter().map(row_to_usage_rollup).collect()
    }

    async fn usage_rollup_checkpoint(&self) -> StorageResult<Option<u64>> {
        let checkpoint = sqlx::query_scalar::<_, i64>(
            "SELECT value FROM usage_rollup_checkpoints_v1 WHERE id = $1",
        )
        .bind(CHECKPOINT_ID)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        checkpoint
            .map(|value| i64_to_u64(value, "usage rollup checkpoint"))
            .transpose()
    }

    async fn advance_rollup_checkpoint_and_persist(
        &self,
        _run: &UsageRollupRun,
    ) -> StorageResult<()> {
        rollup_usage_once_inner(self).await.map(|_| ())
    }
}

async fn rollup_usage_once_inner(storage: &PostgresStorage) -> StorageResult<UsageRollupRun> {
    let mut tx = storage.pool.begin().await.map_err(map_sqlx_error)?;
    let previous_checkpoint =
        sqlx::query_scalar::<_, i64>("SELECT value FROM usage_rollup_checkpoints_v1 WHERE id = $1")
            .bind(CHECKPOINT_ID)
            .fetch_optional(&mut *tx)
            .await
            .map_err(map_sqlx_error)?
            .unwrap_or(0);

    let rows = sqlx::query(
        "SELECT seq, ts, principal_id, payload FROM request_events_v1          WHERE seq > $1 ORDER BY seq ASC LIMIT $2",
    )
    .bind(previous_checkpoint)
    .bind(ROLLUP_BATCH_LIMIT)
    .fetch_all(&mut *tx)
    .await
    .map_err(map_sqlx_error)?;

    if rows.is_empty() {
        tx.commit().await.map_err(map_sqlx_error)?;
        return Ok(UsageRollupRun {
            processed_events: 0,
            updated_rollups: 0,
            checkpoint: Some(i64_to_u64(previous_checkpoint, "usage rollup checkpoint")?),
        });
    }

    let mut max_seq = previous_checkpoint;
    let mut aggregates: BTreeMap<RollupKey, i64> = BTreeMap::new();
    for row in &rows {
        let seq = row.try_get::<i64, _>("seq").map_err(map_sqlx_error)?;
        let ts = row
            .try_get::<DateTime<Utc>, _>("ts")
            .map_err(map_sqlx_error)?;
        let principal = row
            .try_get::<Option<String>, _>("principal_id")
            .map_err(map_sqlx_error)?
            .unwrap_or_default();
        let _payload = row
            .try_get::<Vec<u8>, _>("payload")
            .map_err(map_sqlx_error)?;

        max_seq = max_seq.max(seq);
        for resolution in [UsageRollupResolution::Minute, UsageRollupResolution::Hour] {
            let key = RollupKey {
                resolution,
                bucket_start: bucket_start(
                    resolution,
                    datetime_to_unix_secs(ts, "request event ts")?,
                ),
                principal: principal.clone(),
                upstream: String::new(),
                model: String::new(),
            };
            *aggregates.entry(key).or_default() += 1;
        }
    }

    let updated_rollups = u64::try_from(aggregates.len()).map_err(|_| StorageError::Fatal {
        message: "usage rollup aggregate count cannot be represented as u64".to_owned(),
    })?;

    for (key, value) in aggregates {
        sqlx::query(
            "INSERT INTO usage_rollups_v1              (resolution, bucket_start, principal_id, upstream, model, value, updated_at)              VALUES ($1, $2, $3, $4, $5, $6, NOW())              ON CONFLICT (resolution, bucket_start, principal_id, upstream, model) DO UPDATE              SET value = usage_rollups_v1.value + EXCLUDED.value, updated_at = NOW()",
        )
        .bind(key.resolution.as_str())
        .bind(unix_secs_to_datetime(
            key.bucket_start,
            "usage rollup bucket_start",
        )?)
        .bind(&key.principal)
        .bind(&key.upstream)
        .bind(&key.model)
        .bind(value)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
    }

    sqlx::query(
        "INSERT INTO usage_rollup_checkpoints_v1 (id, value) VALUES ($1, $2)          ON CONFLICT (id) DO UPDATE SET value = $2          WHERE usage_rollup_checkpoints_v1.value < $2",
    )
    .bind(CHECKPOINT_ID)
    .bind(max_seq)
    .execute(&mut *tx)
    .await
    .map_err(map_sqlx_error)?;

    tx.commit().await.map_err(map_sqlx_error)?;
    Ok(UsageRollupRun {
        processed_events: rows.len() as u64,
        updated_rollups,
        checkpoint: Some(i64_to_u64(max_seq, "usage rollup checkpoint")?),
    })
}

fn row_to_usage_rollup(row: PgRow) -> StorageResult<UsageRollup> {
    let resolution = usage_rollup_resolution_from_str(
        &row.try_get::<String, _>("resolution")
            .map_err(map_sqlx_error)?,
    )?;
    let bucket_start = row
        .try_get::<DateTime<Utc>, _>("bucket_start")
        .map_err(map_sqlx_error)?;
    let value = row.try_get::<i64, _>("value").map_err(map_sqlx_error)?;

    Ok(UsageRollup {
        resolution,
        bucket_start: datetime_to_unix_secs(bucket_start, "usage rollup bucket_start")?,
        principal: row.try_get("principal_id").map_err(map_sqlx_error)?,
        upstream: row.try_get("upstream").map_err(map_sqlx_error)?,
        model: row.try_get("model").map_err(map_sqlx_error)?,
        request_count: i64_to_u64(value, "usage rollup value")?,
        input_tokens: 0,
        output_tokens: 0,
        error_count: 0,
        latency_count: 0,
        latency_ms_sum: 0,
        latency_ms_min: None,
        latency_ms_max: None,
        virtual_cost_micros: 0,
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
