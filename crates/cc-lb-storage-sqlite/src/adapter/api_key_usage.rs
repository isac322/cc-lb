use async_trait::async_trait;
use cc_lb_clock::unix_secs;
use cc_lb_storage_api::{
    ApiKeyConcurrencyHold, ApiKeyConcurrencyHoldStore, ApiKeyUsage, ApiKeyUsageBucket,
    ApiKeyUsageBucketDelta, ApiKeyUsageBucketKey, ApiKeyUsageBucketQuery, ApiKeyUsageBucketStore,
    ApiKeyUsageCompactionRun, ApiKeyUsageFlush, ApiKeyUsageFlushResult, StorageError,
    StorageResult,
};
use sqlx::{QueryBuilder, Row, Sqlite};
use uuid::Uuid;

use crate::{SqliteStorage, map_sqlx_error};

const RETIRED_WRITER_EPOCH: &str = "00000000-0000-0000-0000-000000000000";

#[async_trait]
impl ApiKeyUsageBucketStore for SqliteStorage {
    async fn register_api_key_usage_writer(
        &self,
        writer_epoch: Uuid,
        lease_until_unix_secs: u64,
    ) -> StorageResult<()> {
        sqlx::query(
            "INSERT INTO api_key_usage_writers_v1 (writer_epoch, lease_until_unix_secs, last_flush_id) VALUES (?, ?, NULL) ON CONFLICT(writer_epoch) DO UPDATE SET lease_until_unix_secs = excluded.lease_until_unix_secs, last_flush_id = NULL",
        )
        .bind(writer_epoch.to_string())
        .bind(u64_to_i64(lease_until_unix_secs, "API-key usage writer lease")?)
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn flush_api_key_usage(
        &self,
        flush: &ApiKeyUsageFlush,
    ) -> StorageResult<ApiKeyUsageFlushResult> {
        let mut tx = self.begin_immediate().await?;
        let writer_epoch = flush.writer_epoch.to_string();
        let row = sqlx::query(
            "SELECT lease_until_unix_secs, last_flush_id FROM api_key_usage_writers_v1 WHERE writer_epoch = ?",
        )
        .bind(&writer_epoch)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        let Some(row) = row else {
            tx.rollback().await.map_err(map_sqlx_error)?;
            return Ok(ApiKeyUsageFlushResult::LeaseLost);
        };
        let lease_until = i64_to_u64(
            row.try_get("lease_until_unix_secs")
                .map_err(map_sqlx_error)?,
            "API-key usage writer lease",
        )?;
        if lease_until < unix_secs(self.clock().now()) {
            tx.rollback().await.map_err(map_sqlx_error)?;
            return Ok(ApiKeyUsageFlushResult::LeaseLost);
        }
        let last_flush_id: Option<String> = row.try_get("last_flush_id").map_err(map_sqlx_error)?;
        if last_flush_id.as_deref() == Some(flush.flush_id.to_string().as_str()) {
            tx.rollback().await.map_err(map_sqlx_error)?;
            return Ok(ApiKeyUsageFlushResult::AlreadyApplied);
        }

        for delta in &flush.deltas {
            validate_delta(delta)?;
            sqlx::query(
                "INSERT INTO api_key_usage_buckets_v1 (writer_epoch, key_id, bucket_width_secs, bucket_start_unix_secs, requests, input_tokens, output_tokens, cost_usd_micros) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(writer_epoch, key_id, bucket_width_secs, bucket_start_unix_secs) DO UPDATE SET requests = requests + excluded.requests, input_tokens = input_tokens + excluded.input_tokens, output_tokens = output_tokens + excluded.output_tokens, cost_usd_micros = cost_usd_micros + excluded.cost_usd_micros",
            )
            .bind(&writer_epoch)
            .bind(&delta.key.key_id)
            .bind(u64_to_i64(delta.key.bucket_width_secs, "API-key usage bucket width")?)
            .bind(u64_to_i64(delta.key.bucket_start_unix_secs, "API-key usage bucket start")?)
            .bind(delta.usage.requests)
            .bind(delta.usage.input_tokens)
            .bind(delta.usage.output_tokens)
            .bind(delta.usage.cost_usd_micros)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        }
        sqlx::query(
            "UPDATE api_key_usage_writers_v1 SET lease_until_unix_secs = ?, last_flush_id = ? WHERE writer_epoch = ?",
        )
        .bind(u64_to_i64(flush.lease_until_unix_secs, "API-key usage writer lease")?)
        .bind(flush.flush_id.to_string())
        .bind(&writer_epoch)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(ApiKeyUsageFlushResult::Applied)
    }

    async fn query_api_key_usage_buckets(
        &self,
        query: &ApiKeyUsageBucketQuery,
    ) -> StorageResult<Vec<ApiKeyUsageBucket>> {
        if query.key_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut builder = QueryBuilder::<Sqlite>::new(
            "SELECT key_id, bucket_width_secs, bucket_start_unix_secs, SUM(requests) AS requests, SUM(input_tokens) AS input_tokens, SUM(output_tokens) AS output_tokens, SUM(cost_usd_micros) AS cost_usd_micros FROM api_key_usage_buckets_v1 WHERE writer_epoch <> ",
        );
        builder.push_bind(query.exclude_writer_epoch.to_string());
        builder.push(" AND bucket_start_unix_secs + bucket_width_secs > ");
        builder.push_bind(u64_to_i64(
            query.since_unix_secs,
            "API-key usage query start",
        )?);
        builder.push(" AND bucket_start_unix_secs <= ");
        builder.push_bind(u64_to_i64(
            query.until_unix_secs,
            "API-key usage query end",
        )?);
        builder.push(" AND key_id IN (");
        let mut separated = builder.separated(", ");
        for key_id in &query.key_ids {
            separated.push_bind(key_id);
        }
        separated.push_unseparated(") GROUP BY key_id, bucket_width_secs, bucket_start_unix_secs ORDER BY key_id, bucket_width_secs, bucket_start_unix_secs");
        let rows = builder
            .build()
            .fetch_all(self.pool())
            .await
            .map_err(map_sqlx_error)?;
        rows.into_iter().map(row_to_bucket).collect()
    }

    async fn compact_api_key_usage_buckets(
        &self,
        writer_inactive_after_secs: u64,
        retain_for_secs: u64,
        batch_size: usize,
    ) -> StorageResult<ApiKeyUsageCompactionRun> {
        if batch_size == 0 {
            return Ok(ApiKeyUsageCompactionRun {
                folded_rows: 0,
                pruned_rows: 0,
            });
        }
        let now = unix_secs(self.clock().now());
        let inactive_before = now.saturating_sub(writer_inactive_after_secs);
        let retain_since = now.saturating_sub(retain_for_secs);
        let mut tx = self.begin_immediate().await?;
        let stale = sqlx::query(
            "SELECT b.writer_epoch, b.key_id, b.bucket_width_secs, b.bucket_start_unix_secs, b.requests, b.input_tokens, b.output_tokens, b.cost_usd_micros FROM api_key_usage_buckets_v1 b JOIN api_key_usage_writers_v1 w ON w.writer_epoch = b.writer_epoch WHERE b.writer_epoch <> ? AND w.lease_until_unix_secs < ? ORDER BY b.writer_epoch, b.key_id, b.bucket_width_secs, b.bucket_start_unix_secs LIMIT ?",
        )
        .bind(RETIRED_WRITER_EPOCH)
        .bind(u64_to_i64(inactive_before, "API-key usage inactive cutoff")?)
        .bind(i64::try_from(batch_size).unwrap_or(i64::MAX))
        .fetch_all(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        for row in &stale {
            sqlx::query(
                "INSERT INTO api_key_usage_buckets_v1 (writer_epoch, key_id, bucket_width_secs, bucket_start_unix_secs, requests, input_tokens, output_tokens, cost_usd_micros) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(writer_epoch, key_id, bucket_width_secs, bucket_start_unix_secs) DO UPDATE SET requests = requests + excluded.requests, input_tokens = input_tokens + excluded.input_tokens, output_tokens = output_tokens + excluded.output_tokens, cost_usd_micros = cost_usd_micros + excluded.cost_usd_micros",
            )
            .bind(RETIRED_WRITER_EPOCH)
            .bind(row.try_get::<String, _>("key_id").map_err(map_sqlx_error)?)
            .bind(row.try_get::<i64, _>("bucket_width_secs").map_err(map_sqlx_error)?)
            .bind(row.try_get::<i64, _>("bucket_start_unix_secs").map_err(map_sqlx_error)?)
            .bind(row.try_get::<i64, _>("requests").map_err(map_sqlx_error)?)
            .bind(row.try_get::<i64, _>("input_tokens").map_err(map_sqlx_error)?)
            .bind(row.try_get::<i64, _>("output_tokens").map_err(map_sqlx_error)?)
            .bind(row.try_get::<i64, _>("cost_usd_micros").map_err(map_sqlx_error)?)
            .execute(&mut *tx).await.map_err(map_sqlx_error)?;
            sqlx::query("DELETE FROM api_key_usage_buckets_v1 WHERE writer_epoch = ? AND key_id = ? AND bucket_width_secs = ? AND bucket_start_unix_secs = ?")
                .bind(row.try_get::<String, _>("writer_epoch").map_err(map_sqlx_error)?)
                .bind(row.try_get::<String, _>("key_id").map_err(map_sqlx_error)?)
                .bind(row.try_get::<i64, _>("bucket_width_secs").map_err(map_sqlx_error)?)
                .bind(row.try_get::<i64, _>("bucket_start_unix_secs").map_err(map_sqlx_error)?)
                .execute(&mut *tx).await.map_err(map_sqlx_error)?;
        }
        sqlx::query("DELETE FROM api_key_usage_writers_v1 WHERE writer_epoch <> ? AND lease_until_unix_secs < ? AND NOT EXISTS (SELECT 1 FROM api_key_usage_buckets_v1 b WHERE b.writer_epoch = api_key_usage_writers_v1.writer_epoch)")
            .bind(RETIRED_WRITER_EPOCH)
            .bind(u64_to_i64(inactive_before, "API-key usage inactive cutoff")?)
            .execute(&mut *tx).await.map_err(map_sqlx_error)?;
        let pruned = sqlx::query("DELETE FROM api_key_usage_buckets_v1 WHERE rowid IN (SELECT rowid FROM api_key_usage_buckets_v1 WHERE bucket_start_unix_secs + bucket_width_secs <= ? ORDER BY bucket_start_unix_secs LIMIT ?)")
            .bind(u64_to_i64(retain_since, "API-key usage retention cutoff")?)
            .bind(i64::try_from(batch_size).unwrap_or(i64::MAX))
            .execute(&mut *tx).await.map_err(map_sqlx_error)?.rows_affected();
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(ApiKeyUsageCompactionRun {
            folded_rows: stale.len() as u64,
            pruned_rows: pruned,
        })
    }
}

/// No-op hold store: SQLite is single-instance, so the `Concurrent` limit
/// stays on the in-process `KeyConcurrencyManager` counter and the engine
/// never consults this path (gated on `BackendKind::Postgres`).
#[async_trait]
impl ApiKeyConcurrencyHoldStore for SqliteStorage {
    async fn insert_api_key_concurrency_hold(
        &self,
        hold: &ApiKeyConcurrencyHold,
    ) -> StorageResult<()> {
        let _ = hold;
        Ok(())
    }

    async fn delete_api_key_concurrency_hold(&self, hold_id: Uuid) -> StorageResult<()> {
        let _ = hold_id;
        Ok(())
    }

    async fn count_api_key_concurrency_holds(
        &self,
        key_id: &str,
        now_unix_secs: u64,
        max_age_secs: u64,
    ) -> StorageResult<u64> {
        let _ = (key_id, now_unix_secs, max_age_secs);
        Ok(0)
    }

    async fn delete_expired_api_key_concurrency_holds(
        &self,
        now_unix_secs: u64,
        max_age_secs: u64,
    ) -> StorageResult<u64> {
        let _ = (now_unix_secs, max_age_secs);
        Ok(0)
    }
}

fn validate_delta(delta: &ApiKeyUsageBucketDelta) -> StorageResult<()> {
    if delta.key.key_id.is_empty() || delta.key.bucket_width_secs == 0 {
        return Err(StorageError::InvalidInput {
            field: "api_key_usage_bucket".to_owned(),
            reason: "key_id must be non-empty and bucket_width_secs must be positive".to_owned(),
        });
    }
    Ok(())
}

fn row_to_bucket(row: sqlx::sqlite::SqliteRow) -> StorageResult<ApiKeyUsageBucket> {
    Ok(ApiKeyUsageBucket {
        key: ApiKeyUsageBucketKey {
            key_id: row.try_get("key_id").map_err(map_sqlx_error)?,
            bucket_width_secs: i64_to_u64(
                row.try_get("bucket_width_secs").map_err(map_sqlx_error)?,
                "API-key usage bucket width",
            )?,
            bucket_start_unix_secs: i64_to_u64(
                row.try_get("bucket_start_unix_secs")
                    .map_err(map_sqlx_error)?,
                "API-key usage bucket start",
            )?,
        },
        usage: ApiKeyUsage {
            requests: row.try_get("requests").map_err(map_sqlx_error)?,
            input_tokens: row.try_get("input_tokens").map_err(map_sqlx_error)?,
            output_tokens: row.try_get("output_tokens").map_err(map_sqlx_error)?,
            cost_usd_micros: row.try_get("cost_usd_micros").map_err(map_sqlx_error)?,
        },
    })
}

fn u64_to_i64(value: u64, field: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| StorageError::InvalidInput {
        field: field.to_owned(),
        reason: "exceeds SQLite INTEGER".to_owned(),
    })
}

fn i64_to_u64(value: i64, field: &str) -> StorageResult<u64> {
    u64::try_from(value).map_err(|_| StorageError::Corrupted {
        message: format!("{field} is negative"),
    })
}
