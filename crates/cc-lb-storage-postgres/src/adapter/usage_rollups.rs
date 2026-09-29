use std::collections::{BTreeMap, HashMap};

use async_trait::async_trait;
use cc_lb_storage_api::{
    OverviewExcludedErrorBucket, RequestEvent, StorageError, StorageResult, UsageRollup,
    UsageRollupResolution, UsageRollupRun, UsageRollupStore, normalize_usage_rollup_dimension,
};
use sqlx::{Postgres, Row, Transaction, postgres::PgRow};
use uuid::Uuid;

use crate::{
    adapter::{PostgresStorage, i64_to_u64, u64_to_i64},
    error_map::map_sqlx_error,
};

const CHECKPOINT_ID: &str = "high_water";
const ROLLUP_BATCH_LIMIT: i64 = 10_000;
const MINUTE_SECS: u64 = 60;
const HOUR_SECS: u64 = 60 * 60;
const USAGE_ROLLUP_LOCK_KEY: i64 = 0x_CC1B_0001_0010_0001_u64 as i64;

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
    }
}

#[derive(Debug, Clone)]
struct UpstreamIdentity {
    id: Uuid,
    name: String,
}

#[async_trait]
impl UsageRollupStore for PostgresStorage {
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
              cache_read_input_tokens, error_count, latency_count, latency_ms_sum, \
              latency_ms_min, latency_ms_max, proxy_setup_ms_count, proxy_setup_ms_sum, \
              shape_ms_count, shape_ms_sum, sign_ms_count, sign_ms_sum, \
              upstream_ttfb_ms_count, upstream_ttfb_ms_sum, upstream_body_ms_count, \
              upstream_body_ms_sum, virtual_cost_micros \
              FROM usage_rollups_v2 \
              WHERE resolution = $1 AND bucket_start_unix_secs >= $2 AND bucket_start_unix_secs < $3 \
              ORDER BY bucket_start_unix_secs ASC",
        )
        .bind(resolution.as_str())
        .bind(u64_to_i64(window_start_unix_secs, "usage rollup range start")?)
        .bind(u64_to_i64(window_end_unix_secs, "usage rollup range end")?)
        .fetch_all(&self.pool)
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
             WHERE resolution = $1 AND bucket_start_unix_secs >= $2 AND bucket_start_unix_secs < $3 \
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
        .fetch_all(&self.pool)
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
}

async fn rollup_usage_once_inner(storage: &PostgresStorage) -> StorageResult<UsageRollupRun> {
    let mut tx = storage.pool.begin().await.map_err(map_sqlx_error)?;

    let locked = sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_xact_lock($1)")
        .bind(USAGE_ROLLUP_LOCK_KEY)
        .fetch_one(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;

    if !locked {
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

    let (previous_seq, previous_xid) = sqlx::query_as::<_, (i64, String)>(
        "SELECT value, value_xid::text FROM usage_rollup_checkpoints_v1 WHERE id = $1",
    )
    .bind(CHECKPOINT_ID)
    .fetch_optional(&mut *tx)
    .await
    .map_err(map_sqlx_error)?
    .unwrap_or_else(|| (0, "0".to_owned()));

    // Page by the composite (tx_id, seq) cursor below the snapshot xmin horizon,
    // never by seq alone: a lower seq can COMMIT after a higher seq (BIGSERIAL is
    // assigned at INSERT, visible at COMMIT), so a seq high-water-mark would skip
    // it forever. xmin never advances past an in-flight txn, so tx_id order does not.
    let rows = sqlx::query(
        "SELECT seq, tx_id::text AS tx_id_text, payload, upstream_id FROM request_events_v1 \
          WHERE (tx_id, seq) > ($1::xid8, $2) \
            AND tx_id < pg_snapshot_xmin(pg_current_snapshot()) \
          ORDER BY tx_id ASC, seq ASC LIMIT $3",
    )
    .bind(&previous_xid)
    .bind(previous_seq)
    .bind(ROLLUP_BATCH_LIMIT)
    .fetch_all(&mut *tx)
    .await
    .map_err(map_sqlx_error)?;

    if rows.is_empty() {
        tx.commit().await.map_err(map_sqlx_error)?;
        return Ok(UsageRollupRun {
            processed_events: 0,
            updated_rollups: 0,
            checkpoint: Some(i64_to_u64(previous_seq, "usage rollup checkpoint")?),
        });
    }

    let upstreams = load_upstream_identities(&mut tx).await?;
    // Rows arrive in (tx_id, seq) order, so the final row is the new cursor.
    let mut cursor_seq = previous_seq;
    let mut cursor_xid = previous_xid;
    let mut deltas: BTreeMap<RollupKey, RollupDelta> = BTreeMap::new();
    let mut overview_excluded_error_deltas: BTreeMap<(UsageRollupResolution, u64), u64> =
        BTreeMap::new();
    for row in &rows {
        let seq = row.try_get::<i64, _>("seq").map_err(map_sqlx_error)?;
        let tx_id_text: String = row.try_get("tx_id_text").map_err(map_sqlx_error)?;
        let payload: Vec<u8> = row.try_get("payload").map_err(map_sqlx_error)?;
        let table_upstream_id: Option<Uuid> = row.try_get("upstream_id").map_err(map_sqlx_error)?;
        let event: RequestEvent = serde_json::from_slice(&payload)?;
        let upstream = resolve_upstream_identity(&event, table_upstream_id, &upstreams);

        cursor_seq = seq;
        cursor_xid = tx_id_text;
        for resolution in [UsageRollupResolution::Minute, UsageRollupResolution::Hour] {
            let key = RollupKey {
                resolution,
                bucket_start: bucket_start(resolution, event_ts_secs(&event)),
                principal: normalize_usage_rollup_dimension(event.principal_id.as_deref()),
                upstream_id: upstream.id,
                upstream_name: normalize_usage_rollup_dimension(Some(&upstream.name)),
                model: normalize_usage_rollup_dimension(event.model.as_deref()),
            };
            if is_overview_excluded_status(event.status) {
                let count = overview_excluded_error_deltas
                    .entry((resolution, key.bucket_start))
                    .or_default();
                *count = count.saturating_add(1);
            }
            deltas.entry(key).or_default().add_event(&event);
        }
    }

    let updated_rollups = u64::try_from(deltas.len()).map_err(|_| StorageError::Fatal {
        message: "usage rollup aggregate count cannot be represented as u64".to_owned(),
    })?;

    for (key, delta) in deltas {
        sqlx::query(
            "INSERT INTO usage_rollups_v2 \
              (resolution, bucket_start_unix_secs, principal_id, upstream_id, upstream_name, model, \
               request_count, input_tokens, output_tokens, cache_creation_input_tokens, \
               cache_read_input_tokens, error_count, latency_count, latency_ms_sum, \
               latency_ms_min, latency_ms_max, proxy_setup_ms_count, proxy_setup_ms_sum, \
               shape_ms_count, shape_ms_sum, sign_ms_count, sign_ms_sum, \
               upstream_ttfb_ms_count, upstream_ttfb_ms_sum, upstream_body_ms_count, \
               upstream_body_ms_sum, virtual_cost_micros, updated_at) \
              VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, \
                      $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, NOW()) \
              ON CONFLICT (resolution, bucket_start_unix_secs, principal_id, upstream_id, model) DO UPDATE \
              SET upstream_name = EXCLUDED.upstream_name, \
                  request_count = usage_rollups_v2.request_count + EXCLUDED.request_count, \
                  input_tokens = usage_rollups_v2.input_tokens + EXCLUDED.input_tokens, \
                  output_tokens = usage_rollups_v2.output_tokens + EXCLUDED.output_tokens, \
                  cache_creation_input_tokens = usage_rollups_v2.cache_creation_input_tokens + EXCLUDED.cache_creation_input_tokens, \
                  cache_read_input_tokens = usage_rollups_v2.cache_read_input_tokens + EXCLUDED.cache_read_input_tokens, \
                  error_count = usage_rollups_v2.error_count + EXCLUDED.error_count, \
                  latency_count = usage_rollups_v2.latency_count + EXCLUDED.latency_count, \
                  latency_ms_sum = usage_rollups_v2.latency_ms_sum + EXCLUDED.latency_ms_sum, \
                  latency_ms_min = CASE \
                      WHEN usage_rollups_v2.latency_ms_min IS NULL THEN EXCLUDED.latency_ms_min \
                      WHEN EXCLUDED.latency_ms_min IS NULL THEN usage_rollups_v2.latency_ms_min \
                      ELSE LEAST(usage_rollups_v2.latency_ms_min, EXCLUDED.latency_ms_min) \
                  END, \
                  latency_ms_max = CASE \
                      WHEN usage_rollups_v2.latency_ms_max IS NULL THEN EXCLUDED.latency_ms_max \
                      WHEN EXCLUDED.latency_ms_max IS NULL THEN usage_rollups_v2.latency_ms_max \
                      ELSE GREATEST(usage_rollups_v2.latency_ms_max, EXCLUDED.latency_ms_max) \
                  END, \
                  proxy_setup_ms_count = usage_rollups_v2.proxy_setup_ms_count + EXCLUDED.proxy_setup_ms_count, \
                  proxy_setup_ms_sum = usage_rollups_v2.proxy_setup_ms_sum + EXCLUDED.proxy_setup_ms_sum, \
                  shape_ms_count = usage_rollups_v2.shape_ms_count + EXCLUDED.shape_ms_count, \
                  shape_ms_sum = usage_rollups_v2.shape_ms_sum + EXCLUDED.shape_ms_sum, \
                  sign_ms_count = usage_rollups_v2.sign_ms_count + EXCLUDED.sign_ms_count, \
                  sign_ms_sum = usage_rollups_v2.sign_ms_sum + EXCLUDED.sign_ms_sum, \
                  upstream_ttfb_ms_count = usage_rollups_v2.upstream_ttfb_ms_count + EXCLUDED.upstream_ttfb_ms_count, \
                  upstream_ttfb_ms_sum = usage_rollups_v2.upstream_ttfb_ms_sum + EXCLUDED.upstream_ttfb_ms_sum, \
                  upstream_body_ms_count = usage_rollups_v2.upstream_body_ms_count + EXCLUDED.upstream_body_ms_count, \
                  upstream_body_ms_sum = usage_rollups_v2.upstream_body_ms_sum + EXCLUDED.upstream_body_ms_sum, \
                  virtual_cost_micros = usage_rollups_v2.virtual_cost_micros + EXCLUDED.virtual_cost_micros, \
                  updated_at = NOW()",
        )
        .bind(key.resolution.as_str())
        .bind(u64_to_i64(key.bucket_start, "usage rollup bucket_start")?)
        .bind(&key.principal)
        .bind(key.upstream_id)
        .bind(&key.upstream_name)
        .bind(&key.model)
        .bind(u64_to_i64(delta.request_count, "rollup request_count")?)
        .bind(u64_to_i64(delta.input_tokens, "rollup input_tokens")?)
        .bind(u64_to_i64(delta.output_tokens, "rollup output_tokens")?)
        .bind(u64_to_i64(delta.cache_creation_input_tokens, "rollup cache_creation_input_tokens")?)
        .bind(u64_to_i64(delta.cache_read_input_tokens, "rollup cache_read_input_tokens")?)
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
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
    }
    for ((resolution, bucket_start), error_count) in overview_excluded_error_deltas {
        sqlx::query(
            "INSERT INTO overview_excluded_error_rollups_v1 \
             (resolution, bucket_start_unix_secs, error_count, updated_at) \
             VALUES ($1, $2, $3, NOW()) \
             ON CONFLICT (resolution, bucket_start_unix_secs) DO UPDATE SET \
                 error_count = overview_excluded_error_rollups_v1.error_count + EXCLUDED.error_count, \
                 updated_at = NOW()",
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

    sqlx::query(
        "INSERT INTO usage_rollup_checkpoints_v1 (id, value, value_xid) VALUES ($1, $2, $3::xid8) \
          ON CONFLICT (id) DO UPDATE SET value = $2, value_xid = $3::xid8 \
          WHERE (usage_rollup_checkpoints_v1.value_xid, usage_rollup_checkpoints_v1.value) < ($3::xid8, $2)",
    )
    .bind(CHECKPOINT_ID)
    .bind(cursor_seq)
    .bind(&cursor_xid)
    .execute(&mut *tx)
    .await
    .map_err(map_sqlx_error)?;

    tx.commit().await.map_err(map_sqlx_error)?;
    Ok(UsageRollupRun {
        processed_events: rows.len() as u64,
        updated_rollups,
        checkpoint: Some(i64_to_u64(cursor_seq, "usage rollup checkpoint")?),
    })
}

fn is_overview_excluded_status(status: u16) -> bool {
    matches!(status, 401 | 403 | 404)
}

async fn load_upstream_identities(
    tx: &mut Transaction<'_, Postgres>,
) -> StorageResult<HashMap<Uuid, UpstreamIdentity>> {
    let rows = sqlx::query("SELECT id, name FROM upstream_spec_v1 WHERE deleted_at IS NULL")
        .fetch_all(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
    let mut upstreams = HashMap::new();
    for row in rows {
        let identity = UpstreamIdentity {
            id: row.try_get("id").map_err(map_sqlx_error)?,
            name: row.try_get("name").map_err(map_sqlx_error)?,
        };
        upstreams.insert(identity.id, identity);
    }
    Ok(upstreams)
}

fn resolve_upstream_identity(
    event: &RequestEvent,
    table_upstream_id: Option<Uuid>,
    upstreams: &HashMap<Uuid, UpstreamIdentity>,
) -> UpstreamIdentity {
    if let Some(upstream_id) = event.upstream_id.or(table_upstream_id) {
        if let Some(name) = event.upstream_name.clone() {
            return UpstreamIdentity {
                id: upstream_id,
                name,
            };
        }
        if let Some(identity) = upstreams.get(&upstream_id) {
            return identity.clone();
        }
        return UpstreamIdentity {
            id: upstream_id,
            name: event_upstream_name(event),
        };
    }
    UpstreamIdentity {
        id: Uuid::nil(),
        name: event_upstream_name(event),
    }
}

fn row_to_usage_rollup(row: PgRow) -> StorageResult<UsageRollup> {
    let resolution = usage_rollup_resolution_from_str(
        &row.try_get::<String, _>("resolution")
            .map_err(map_sqlx_error)?,
    )?;
    let bucket_start = row
        .try_get::<i64, _>("bucket_start_unix_secs")
        .map_err(map_sqlx_error)?;

    Ok(UsageRollup {
        resolution,
        bucket_start: i64_to_u64(bucket_start, "usage rollup bucket_start")?,
        principal: row.try_get("principal_id").map_err(map_sqlx_error)?,
        upstream_id: row.try_get("upstream_id").map_err(map_sqlx_error)?,
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
        .clone()
        .unwrap_or_else(|| normalize_usage_rollup_dimension(None))
}

fn option_u64_to_i64(value: Option<u64>, field: &str) -> StorageResult<Option<i64>> {
    value.map(|value| u64_to_i64(value, field)).transpose()
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
