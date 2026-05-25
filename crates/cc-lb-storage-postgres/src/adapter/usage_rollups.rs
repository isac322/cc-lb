use std::collections::BTreeMap;

use async_trait::async_trait;
use cc_lb_pricing::{virtual_cost_micros_full, UpstreamKind};
use cc_lb_storage_api::{
    RequestEvent, RequestEventUpstream, StorageError, StorageResult, UsageRollup,
    UsageRollupResolution, UsageRollupRun, UsageRollupStore,
};
use chrono::{DateTime, Utc};
use sqlx::{Row, postgres::PgRow};

use crate::{
    adapter::{
        PostgresStorage, datetime_to_unix_secs, i64_to_u64, u64_to_i64, unix_secs_to_datetime,
    },
    error_map::map_sqlx_error,
};

const CHECKPOINT_ID: &str = "high_water";
const ROLLUP_BATCH_LIMIT: i64 = 10_000;
const MINUTE_SECS: u64 = 60;
const HOUR_SECS: u64 = 60 * 60;
const UNKNOWN_DIMENSION: &str = "unknown";
const MAX_DIMENSION_CHARS: usize = 64;
/// Postgres advisory lock key for serializing usage_rollup runs across instances.
/// 64-bit constant chosen to be globally unique within this codebase.
const USAGE_ROLLUP_LOCK_KEY: i64 = 0x_CC1B_0001_0010_0001_u64 as i64;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RollupKey {
    resolution: UsageRollupResolution,
    bucket_start: u64,
    principal: String,
    upstream: String,
    model: String,
}

#[derive(Debug, Default, Clone)]
struct RollupDelta {
    request_count: u64,
    input_tokens: u64,
    output_tokens: u64,
    error_count: u64,
    latency_count: u64,
    latency_ms_sum: u64,
    latency_ms_min: Option<u64>,
    latency_ms_max: Option<u64>,
    virtual_cost_micros: u64,
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
        if event.status >= 400 {
            self.error_count = self.error_count.saturating_add(1);
        }
        self.latency_count = self.latency_count.saturating_add(1);
        self.latency_ms_sum = self.latency_ms_sum.saturating_add(event.duration_ms);
        self.latency_ms_min = min_option(self.latency_ms_min, Some(event.duration_ms));
        self.latency_ms_max = max_option(self.latency_ms_max, Some(event.duration_ms));
        if let Some(model) = event.model.as_deref() {
            let upstream_kind = event.upstream.map(|u| match u {
                RequestEventUpstream::AnthropicDirect | RequestEventUpstream::CustomAnthropicSpec => {
                    UpstreamKind::AnthropicKey
                }
            });
            let estimate = virtual_cost_micros_full(
                model,
                event.input_tokens.unwrap_or(0),
                event.output_tokens.unwrap_or(0),
                0,
                0,
                upstream_kind,
            );
            self.virtual_cost_micros = self
                .virtual_cost_micros
                .saturating_add(estimate.micros_usd.unwrap_or(0));
        }
    }
}

#[async_trait]
impl UsageRollupStore for PostgresStorage {
    async fn rollup_usage_once(&self) -> StorageResult<UsageRollupRun> {
        rollup_usage_once_inner(self).await
    }

    async fn query_usage_rollups(&self) -> StorageResult<Vec<UsageRollup>> {
        let rows = sqlx::query(
            "SELECT resolution, bucket_start, principal_id, upstream, model, request_count, \
              input_tokens, output_tokens, error_count, latency_count, latency_ms_sum, \
              latency_ms_min, latency_ms_max, virtual_cost_micros \
              FROM usage_rollups_v1 ORDER BY bucket_start ASC",
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
            "SELECT resolution, bucket_start, principal_id, upstream, model, request_count, \
              input_tokens, output_tokens, error_count, latency_count, latency_ms_sum, \
              latency_ms_min, latency_ms_max, virtual_cost_micros \
              FROM usage_rollups_v1 \
              WHERE resolution = $1 AND bucket_start BETWEEN $2 AND $3 \
              ORDER BY bucket_start ASC",
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

    // Multi-instance serialization: only one cc-lb-server may run a rollup pass at a time.
    // pg_try_advisory_xact_lock returns false if another transaction already holds the
    // lock; the lock is released automatically when this transaction commits or rolls back.
    let locked = sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_xact_lock($1)")
        .bind(USAGE_ROLLUP_LOCK_KEY)
        .fetch_one(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;

    if !locked {
        // Another instance is already processing this batch — skip cleanly.
        let checkpoint = sqlx::query_scalar::<_, i64>(
            "SELECT value FROM usage_rollup_checkpoints_v1 WHERE id = $1",
        )
        .bind(CHECKPOINT_ID)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx_error)?
        .unwrap_or(0);
        tx.rollback().await.map_err(map_sqlx_error)?;
        return Ok(UsageRollupRun {
            processed_events: 0,
            updated_rollups: 0,
            checkpoint: Some(i64_to_u64(checkpoint, "usage rollup checkpoint")?),
        });
    }

    let previous_checkpoint =
        sqlx::query_scalar::<_, i64>("SELECT value FROM usage_rollup_checkpoints_v1 WHERE id = $1")
            .bind(CHECKPOINT_ID)
            .fetch_optional(&mut *tx)
            .await
            .map_err(map_sqlx_error)?
            .unwrap_or(0);

    let rows = sqlx::query(
        "SELECT seq, payload FROM request_events_v1 \
          WHERE seq > $1 ORDER BY seq ASC LIMIT $2",
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
    let mut deltas: BTreeMap<RollupKey, RollupDelta> = BTreeMap::new();
    for row in &rows {
        let seq = row.try_get::<i64, _>("seq").map_err(map_sqlx_error)?;
        let payload: Vec<u8> = row.try_get("payload").map_err(map_sqlx_error)?;
        let event: RequestEvent = serde_json::from_slice(&payload)?;

        max_seq = max_seq.max(seq);
        for resolution in [UsageRollupResolution::Minute, UsageRollupResolution::Hour] {
            let key = RollupKey {
                resolution,
                bucket_start: bucket_start(resolution, event.ts),
                principal: normalize_dimension(event.principal_id.as_deref()),
                upstream: event
                    .upstream
                    .map(upstream_dimension)
                    .unwrap_or_else(|| UNKNOWN_DIMENSION.to_owned()),
                model: normalize_dimension(event.model.as_deref()),
            };
            deltas.entry(key).or_default().add_event(&event);
        }
    }

    let updated_rollups = u64::try_from(deltas.len()).map_err(|_| StorageError::Fatal {
        message: "usage rollup aggregate count cannot be represented as u64".to_owned(),
    })?;

    for (key, delta) in deltas {
        sqlx::query(
            "INSERT INTO usage_rollups_v1 \
              (resolution, bucket_start, principal_id, upstream, model, \
               request_count, input_tokens, output_tokens, error_count, \
               latency_count, latency_ms_sum, latency_ms_min, latency_ms_max, \
               virtual_cost_micros, updated_at) \
              VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW()) \
              ON CONFLICT (resolution, bucket_start, principal_id, upstream, model) DO UPDATE \
              SET request_count = usage_rollups_v1.request_count + EXCLUDED.request_count, \
                  input_tokens = usage_rollups_v1.input_tokens + EXCLUDED.input_tokens, \
                  output_tokens = usage_rollups_v1.output_tokens + EXCLUDED.output_tokens, \
                  error_count = usage_rollups_v1.error_count + EXCLUDED.error_count, \
                  latency_count = usage_rollups_v1.latency_count + EXCLUDED.latency_count, \
                  latency_ms_sum = usage_rollups_v1.latency_ms_sum + EXCLUDED.latency_ms_sum, \
                  latency_ms_min = CASE \
                      WHEN usage_rollups_v1.latency_ms_min IS NULL THEN EXCLUDED.latency_ms_min \
                      WHEN EXCLUDED.latency_ms_min IS NULL THEN usage_rollups_v1.latency_ms_min \
                      ELSE LEAST(usage_rollups_v1.latency_ms_min, EXCLUDED.latency_ms_min) \
                  END, \
                  latency_ms_max = CASE \
                      WHEN usage_rollups_v1.latency_ms_max IS NULL THEN EXCLUDED.latency_ms_max \
                      WHEN EXCLUDED.latency_ms_max IS NULL THEN usage_rollups_v1.latency_ms_max \
                      ELSE GREATEST(usage_rollups_v1.latency_ms_max, EXCLUDED.latency_ms_max) \
                  END, \
                  virtual_cost_micros = usage_rollups_v1.virtual_cost_micros + EXCLUDED.virtual_cost_micros, \
                  updated_at = NOW()",
        )
        .bind(key.resolution.as_str())
        .bind(unix_secs_to_datetime(
            key.bucket_start,
            "usage rollup bucket_start",
        )?)
        .bind(&key.principal)
        .bind(&key.upstream)
        .bind(&key.model)
        .bind(u64_to_i64(delta.request_count, "rollup request_count")?)
        .bind(u64_to_i64(delta.input_tokens, "rollup input_tokens")?)
        .bind(u64_to_i64(delta.output_tokens, "rollup output_tokens")?)
        .bind(u64_to_i64(delta.error_count, "rollup error_count")?)
        .bind(u64_to_i64(delta.latency_count, "rollup latency_count")?)
        .bind(u64_to_i64(delta.latency_ms_sum, "rollup latency_ms_sum")?)
        .bind(
            delta
                .latency_ms_min
                .map(|value| u64_to_i64(value, "rollup latency_ms_min"))
                .transpose()?,
        )
        .bind(
            delta
                .latency_ms_max
                .map(|value| u64_to_i64(value, "rollup latency_ms_max"))
                .transpose()?,
        )
        .bind(u64_to_i64(
            delta.virtual_cost_micros,
            "rollup virtual_cost_micros",
        )?)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
    }

    sqlx::query(
        "INSERT INTO usage_rollup_checkpoints_v1 (id, value) VALUES ($1, $2) \
          ON CONFLICT (id) DO UPDATE SET value = $2 \
          WHERE usage_rollup_checkpoints_v1.value < $2",
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

    let request_count = row
        .try_get::<i64, _>("request_count")
        .map_err(map_sqlx_error)?;
    let input_tokens = row
        .try_get::<i64, _>("input_tokens")
        .map_err(map_sqlx_error)?;
    let output_tokens = row
        .try_get::<i64, _>("output_tokens")
        .map_err(map_sqlx_error)?;
    let error_count = row
        .try_get::<i64, _>("error_count")
        .map_err(map_sqlx_error)?;
    let latency_count = row
        .try_get::<i64, _>("latency_count")
        .map_err(map_sqlx_error)?;
    let latency_ms_sum = row
        .try_get::<i64, _>("latency_ms_sum")
        .map_err(map_sqlx_error)?;
    let latency_ms_min: Option<i64> = row.try_get("latency_ms_min").map_err(map_sqlx_error)?;
    let latency_ms_max: Option<i64> = row.try_get("latency_ms_max").map_err(map_sqlx_error)?;
    let virtual_cost_micros = row
        .try_get::<i64, _>("virtual_cost_micros")
        .map_err(map_sqlx_error)?;

    Ok(UsageRollup {
        resolution,
        bucket_start: datetime_to_unix_secs(bucket_start, "usage rollup bucket_start")?,
        principal: row.try_get("principal_id").map_err(map_sqlx_error)?,
        upstream: row.try_get("upstream").map_err(map_sqlx_error)?,
        model: row.try_get("model").map_err(map_sqlx_error)?,
        request_count: i64_to_u64(request_count, "rollup request_count")?,
        input_tokens: i64_to_u64(input_tokens, "rollup input_tokens")?,
        output_tokens: i64_to_u64(output_tokens, "rollup output_tokens")?,
        error_count: i64_to_u64(error_count, "rollup error_count")?,
        latency_count: i64_to_u64(latency_count, "rollup latency_count")?,
        latency_ms_sum: i64_to_u64(latency_ms_sum, "rollup latency_ms_sum")?,
        latency_ms_min: latency_ms_min
            .map(|value| i64_to_u64(value, "rollup latency_ms_min"))
            .transpose()?,
        latency_ms_max: latency_ms_max
            .map(|value| i64_to_u64(value, "rollup latency_ms_max"))
            .transpose()?,
        virtual_cost_micros: i64_to_u64(virtual_cost_micros, "rollup virtual_cost_micros")?,
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

fn normalize_dimension(value: Option<&str>) -> String {
    let Some(value) = value else {
        return UNKNOWN_DIMENSION.to_owned();
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return UNKNOWN_DIMENSION.to_owned();
    }

    let mut normalized = String::new();
    for ch in trimmed.chars().take(MAX_DIMENSION_CHARS) {
        if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | ':' | '@') {
            normalized.push(ch);
        } else {
            normalized.push('_');
        }
    }
    if normalized.is_empty() {
        UNKNOWN_DIMENSION.to_owned()
    } else {
        normalized
    }
}

fn upstream_dimension(upstream: RequestEventUpstream) -> String {
    match upstream {
        RequestEventUpstream::AnthropicDirect => "anthropic_direct",
        RequestEventUpstream::CustomAnthropicSpec => "custom_anthropic_spec",
    }
    .to_owned()
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
