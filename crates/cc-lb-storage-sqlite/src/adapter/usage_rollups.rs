use std::collections::BTreeMap;

use async_trait::async_trait;
use cc_lb_storage_api::{
    RequestEvent, StorageError, StorageResult, UsageRollup, UsageRollupResolution, UsageRollupRun,
    UsageRollupStore,
};
use sqlx::Row;
use uuid::Uuid;

use crate::{SqliteStorage, map_sqlx_error};

const CHECKPOINT_KEY: &str = "usage_rollup_checkpoint_high_water";
const MINUTE_SECS: u64 = 60;
const HOUR_SECS: u64 = 60 * 60;
const UNKNOWN_DIMENSION: &str = "unknown";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RollupKey {
    principal_id: String,
    window_start: u64,
}

#[derive(Debug, Clone, Default)]
struct RollupDelta {
    input_tokens: u64,
    output_tokens: u64,
    request_count: u64,
    cost_usd_micros: u64,
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
        self.cost_usd_micros = self
            .cost_usd_micros
            .saturating_add(event.cost_usd_micros.unwrap_or(0).max(0) as u64);
    }

    fn add_rollup(&mut self, rollup: &BasicRollup) {
        self.request_count = self.request_count.saturating_add(rollup.request_count);
        self.input_tokens = self.input_tokens.saturating_add(rollup.input_tokens);
        self.output_tokens = self.output_tokens.saturating_add(rollup.output_tokens);
        self.cost_usd_micros = self.cost_usd_micros.saturating_add(rollup.cost_usd_micros);
    }
}

#[derive(Debug, Clone)]
struct BasicRollup {
    principal_id: String,
    window_start: u64,
    input_tokens: u64,
    output_tokens: u64,
    request_count: u64,
    cost_usd_micros: u64,
}

#[async_trait]
impl UsageRollupStore for SqliteStorage {
    async fn rollup_usage_once(&self) -> StorageResult<UsageRollupRun> {
        let previous_checkpoint = self.usage_rollup_checkpoint().await?.unwrap_or(0);
        let rows = sqlx::query(
            "SELECT id, payload FROM request_events_v1 WHERE id > ? ORDER BY id ASC LIMIT 10000",
        )
        .bind(u64_to_i64(previous_checkpoint, "usage rollup checkpoint")?)
        .fetch_all(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        if rows.is_empty() {
            return Ok(UsageRollupRun {
                processed_events: 0,
                updated_rollups: 0,
                checkpoint: Some(previous_checkpoint),
            });
        }

        let mut max_id = previous_checkpoint;
        let mut deltas = BTreeMap::<RollupKey, RollupDelta>::new();
        for row in rows {
            let id = i64_to_u64(
                row.try_get("id").map_err(map_sqlx_error)?,
                "request event id",
            )?;
            let payload: String = row.try_get("payload").map_err(map_sqlx_error)?;
            let event: RequestEvent = serde_json::from_str(&payload)?;
            max_id = max_id.max(id);

            let key = RollupKey {
                principal_id: normalize_dimension(event.principal_id.as_deref()),
                window_start: bucket_start(UsageRollupResolution::Minute, event_ts_secs(&event)),
            };
            deltas.entry(key).or_default().add_event(&event);
        }

        let updated_rollups = u64::try_from(deltas.len()).map_err(|_| StorageError::Fatal {
            message: "usage rollup aggregate count cannot be represented as u64".to_owned(),
        })?;

        for (key, delta) in &deltas {
            sqlx::query(
                "INSERT INTO usage_rollups_v2 \
                 (principal_id, window_start, input_tokens, output_tokens, request_count, cost_usd_micros) \
                 VALUES (?, ?, ?, ?, ?, ?) \
                 ON CONFLICT(principal_id, window_start) DO UPDATE SET \
                     input_tokens = usage_rollups_v2.input_tokens + excluded.input_tokens, \
                     output_tokens = usage_rollups_v2.output_tokens + excluded.output_tokens, \
                     request_count = usage_rollups_v2.request_count + excluded.request_count, \
                     cost_usd_micros = usage_rollups_v2.cost_usd_micros + excluded.cost_usd_micros",
            )
            .bind(&key.principal_id)
            .bind(u64_to_i64(key.window_start, "usage rollup window_start")?)
            .bind(u64_to_i64(delta.input_tokens, "usage rollup input_tokens")?)
            .bind(u64_to_i64(delta.output_tokens, "usage rollup output_tokens")?)
            .bind(u64_to_i64(delta.request_count, "usage rollup request_count")?)
            .bind(u64_to_i64(delta.cost_usd_micros, "usage rollup cost_usd_micros")?)
            .execute(self.pool())
            .await
            .map_err(map_sqlx_error)?;
        }

        persist_checkpoint(self, max_id).await?;

        Ok(UsageRollupRun {
            processed_events: max_id.saturating_sub(previous_checkpoint).min(
                deltas
                    .values()
                    .map(|delta| delta.request_count)
                    .sum::<u64>(),
            ),
            updated_rollups,
            checkpoint: Some(max_id),
        })
    }

    async fn query_usage_rollups(&self) -> StorageResult<Vec<UsageRollup>> {
        let minute_rows = load_basic_rollups(self, None, None).await?;
        let mut rollups = minute_rows
            .iter()
            .map(|row| row_to_usage_rollup(row, UsageRollupResolution::Minute))
            .collect::<Vec<_>>();
        rollups.extend(hour_rollups(&minute_rows));
        Ok(rollups)
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

        let rows = load_basic_rollups(
            self,
            Some(window_start_unix_secs),
            Some(window_end_unix_secs),
        )
        .await?;

        let rollups = match resolution {
            UsageRollupResolution::Minute => rows
                .iter()
                .map(|row| row_to_usage_rollup(row, UsageRollupResolution::Minute))
                .collect(),
            UsageRollupResolution::Hour => hour_rollups(&rows),
        };
        Ok(rollups)
    }

    async fn usage_rollup_checkpoint(&self) -> StorageResult<Option<u64>> {
        let value = sqlx::query_scalar::<_, String>("SELECT value FROM meta_v1 WHERE key = ?")
            .bind(CHECKPOINT_KEY)
            .fetch_optional(self.pool())
            .await
            .map_err(map_sqlx_error)?;

        value
            .map(|value| parse_u64(&value, "usage rollup checkpoint"))
            .transpose()
    }

    async fn advance_rollup_checkpoint_and_persist(
        &self,
        run: &UsageRollupRun,
    ) -> StorageResult<()> {
        if let Some(checkpoint) = run.checkpoint {
            persist_checkpoint(self, checkpoint).await?;
        }
        Ok(())
    }
}

async fn persist_checkpoint(storage: &SqliteStorage, checkpoint: u64) -> StorageResult<()> {
    sqlx::query(
        "INSERT INTO meta_v1 (key, value) VALUES (?, ?) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value \
         WHERE CAST(meta_v1.value AS INTEGER) < CAST(excluded.value AS INTEGER)",
    )
    .bind(CHECKPOINT_KEY)
    .bind(checkpoint.to_string())
    .execute(storage.pool())
    .await
    .map_err(map_sqlx_error)?;

    Ok(())
}

async fn load_basic_rollups(
    storage: &SqliteStorage,
    window_start: Option<u64>,
    window_end: Option<u64>,
) -> StorageResult<Vec<BasicRollup>> {
    let rows = sqlx::query(
        "SELECT principal_id, window_start, input_tokens, output_tokens, request_count, cost_usd_micros \
         FROM usage_rollups_v2 \
         WHERE (? IS NULL OR window_start >= ?) AND (? IS NULL OR window_start < ?) \
         ORDER BY window_start ASC, principal_id ASC",
    )
    .bind(window_start.map(|value| u64_to_i64(value, "usage rollup range start")).transpose()?)
    .bind(window_start.map(|value| u64_to_i64(value, "usage rollup range start")).transpose()?)
    .bind(window_end.map(u64_to_i64_upper))
    .bind(window_end.map(u64_to_i64_upper))
    .fetch_all(storage.pool())
    .await
    .map_err(map_sqlx_error)?;

    rows.into_iter()
        .map(|row| {
            Ok(BasicRollup {
                principal_id: row.try_get("principal_id").map_err(map_sqlx_error)?,
                window_start: i64_to_u64(
                    row.try_get("window_start").map_err(map_sqlx_error)?,
                    "usage rollup window_start",
                )?,
                input_tokens: i64_to_u64(
                    row.try_get("input_tokens").map_err(map_sqlx_error)?,
                    "usage rollup input_tokens",
                )?,
                output_tokens: i64_to_u64(
                    row.try_get("output_tokens").map_err(map_sqlx_error)?,
                    "usage rollup output_tokens",
                )?,
                request_count: i64_to_u64(
                    row.try_get("request_count").map_err(map_sqlx_error)?,
                    "usage rollup request_count",
                )?,
                cost_usd_micros: i64_to_u64(
                    row.try_get("cost_usd_micros").map_err(map_sqlx_error)?,
                    "usage rollup cost_usd_micros",
                )?,
            })
        })
        .collect()
}

fn hour_rollups(rows: &[BasicRollup]) -> Vec<UsageRollup> {
    let mut deltas = BTreeMap::<RollupKey, RollupDelta>::new();
    for row in rows {
        let key = RollupKey {
            principal_id: row.principal_id.clone(),
            window_start: bucket_start(UsageRollupResolution::Hour, row.window_start),
        };
        deltas.entry(key).or_default().add_rollup(row);
    }

    deltas
        .into_iter()
        .map(|(key, delta)| {
            usage_rollup_from_parts(
                UsageRollupResolution::Hour,
                key.window_start,
                key.principal_id,
                delta,
            )
        })
        .collect()
}

fn row_to_usage_rollup(row: &BasicRollup, resolution: UsageRollupResolution) -> UsageRollup {
    usage_rollup_from_parts(
        resolution,
        row.window_start,
        row.principal_id.clone(),
        RollupDelta {
            input_tokens: row.input_tokens,
            output_tokens: row.output_tokens,
            request_count: row.request_count,
            cost_usd_micros: row.cost_usd_micros,
        },
    )
}

fn usage_rollup_from_parts(
    resolution: UsageRollupResolution,
    bucket_start: u64,
    principal: String,
    delta: RollupDelta,
) -> UsageRollup {
    UsageRollup {
        resolution,
        bucket_start,
        principal,
        upstream_id: Uuid::nil(),
        upstream_name: UNKNOWN_DIMENSION.to_owned(),
        model: UNKNOWN_DIMENSION.to_owned(),
        request_count: delta.request_count,
        input_tokens: delta.input_tokens,
        output_tokens: delta.output_tokens,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
        error_count: 0,
        latency_count: 0,
        latency_ms_sum: 0,
        latency_ms_min: None,
        latency_ms_max: None,
        proxy_setup_ms_count: 0,
        proxy_setup_ms_sum: 0,
        shape_ms_count: 0,
        shape_ms_sum: 0,
        sign_ms_count: 0,
        sign_ms_sum: 0,
        upstream_ttfb_ms_count: 0,
        upstream_ttfb_ms_sum: 0,
        upstream_body_ms_count: 0,
        upstream_body_ms_sum: 0,
        virtual_cost_micros: delta.cost_usd_micros,
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
    event.ts_ms.map(|ts_ms| ts_ms / 1_000).unwrap_or(event.ts)
}

fn normalize_dimension(value: Option<&str>) -> String {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(UNKNOWN_DIMENSION)
        .to_owned()
}

fn parse_u64(value: &str, field: &str) -> StorageResult<u64> {
    value.parse::<u64>().map_err(|_| StorageError::Corrupted {
        message: format!("invalid {field} value {value}"),
    })
}

fn u64_to_i64(value: u64, field: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| StorageError::Fatal {
        message: format!("{field} cannot be represented as sqlite INTEGER"),
    })
}

fn u64_to_i64_upper(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn i64_to_u64(value: i64, field: &str) -> StorageResult<u64> {
    u64::try_from(value).map_err(|_| StorageError::Corrupted {
        message: format!("{field} is negative in sqlite storage"),
    })
}
