use std::collections::HashSet;

use async_trait::async_trait;
use cc_lb_clock::unix_secs;
use cc_lb_storage_api::{
    ApiKeyConcurrencyHold, ApiKeyConcurrencyHoldStore, ApiKeyUsage, ApiKeyUsageBucket,
    ApiKeyUsageBucketDelta, ApiKeyUsageBucketKey, ApiKeyUsageBucketQuery, ApiKeyUsageBucketStore,
    ApiKeyUsageCompactionRun, ApiKeyUsageFlush, ApiKeyUsageFlushResult, StorageError,
    StorageResult,
};
use sqlx::{Postgres, QueryBuilder, Row};
use uuid::Uuid;

use crate::{
    adapter::{PostgresStorage, i64_to_u64, u64_to_i64},
    error_map::map_sqlx_error,
};

const RETIRED_WRITER_EPOCH: Uuid = Uuid::nil();
const MAX_FLUSH_MARKER_PRUNE_BATCHES: usize = 256;

#[async_trait]
impl ApiKeyUsageBucketStore for PostgresStorage {
    async fn register_api_key_usage_writer(
        &self,
        writer_epoch: Uuid,
        lease_until_unix_secs: u64,
    ) -> StorageResult<()> {
        sqlx::query("INSERT INTO api_key_usage_writers_v1 (writer_epoch, lease_until_unix_secs, last_flush_id) VALUES ($1, $2, NULL) ON CONFLICT(writer_epoch) DO UPDATE SET lease_until_unix_secs = excluded.lease_until_unix_secs, last_flush_id = NULL")
            .bind(writer_epoch).bind(u64_to_i64(lease_until_unix_secs, "API-key usage writer lease")?)
            .execute(&self.pool).await.map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn flush_api_key_usage(
        &self,
        flush: &ApiKeyUsageFlush,
    ) -> StorageResult<ApiKeyUsageFlushResult> {
        let now = unix_secs(self.clock.now());
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        if !flush.deltas.is_empty() {
            let already_recorded =
                sqlx::query("SELECT 1 FROM api_key_usage_flushes_v1 WHERE flush_id = $1 LIMIT 1")
                    .bind(flush.flush_id)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(map_sqlx_error)?
                    .is_some();
            if already_recorded {
                tx.rollback().await.map_err(map_sqlx_error)?;
                return Ok(ApiKeyUsageFlushResult::AlreadyApplied);
            }
        }

        let row = sqlx::query("SELECT lease_until_unix_secs, last_flush_id FROM api_key_usage_writers_v1 WHERE writer_epoch = $1 FOR UPDATE")
            .bind(flush.writer_epoch).fetch_optional(&mut *tx).await.map_err(map_sqlx_error)?;
        let Some(row) = row else {
            tx.rollback().await.map_err(map_sqlx_error)?;
            return Ok(ApiKeyUsageFlushResult::LeaseLost);
        };
        let last_flush_id: Option<Uuid> = row.try_get("last_flush_id").map_err(map_sqlx_error)?;
        if last_flush_id == Some(flush.flush_id) {
            tx.rollback().await.map_err(map_sqlx_error)?;
            return Ok(ApiKeyUsageFlushResult::AlreadyApplied);
        }
        let lease_until = i64_to_u64(
            row.try_get("lease_until_unix_secs")
                .map_err(map_sqlx_error)?,
            "API-key usage writer lease",
        )?;
        if lease_until < now {
            tx.rollback().await.map_err(map_sqlx_error)?;
            return Ok(ApiKeyUsageFlushResult::LeaseLost);
        }
        if !flush.deltas.is_empty() {
            let claimed = sqlx::query(
                "INSERT INTO api_key_usage_flushes_v1 (flush_id, applied_at_unix_secs) VALUES ($1, $2) ON CONFLICT(flush_id) DO NOTHING RETURNING flush_id",
            )
            .bind(flush.flush_id)
            .bind(u64_to_i64(now, "API-key usage flush timestamp")?)
            .fetch_optional(&mut *tx)
            .await
            .map_err(map_sqlx_error)?
            .is_some();
            if !claimed {
                tx.rollback().await.map_err(map_sqlx_error)?;
                return Ok(ApiKeyUsageFlushResult::AlreadyApplied);
            }
            let mut has_duplicate_key = false;
            let mut seen_keys = HashSet::with_capacity(flush.deltas.len());
            let mut prepared = Vec::with_capacity(flush.deltas.len());
            for delta in &flush.deltas {
                validate_delta(delta)?;
                let bucket_width_secs =
                    u64_to_i64(delta.key.bucket_width_secs, "API-key usage bucket width")?;
                let bucket_start_unix_secs = u64_to_i64(
                    delta.key.bucket_start_unix_secs,
                    "API-key usage bucket start",
                )?;
                if !seen_keys.insert((
                    delta.key.key_id.as_str(),
                    bucket_width_secs,
                    bucket_start_unix_secs,
                )) {
                    has_duplicate_key = true;
                }
                prepared.push((delta, bucket_width_secs, bucket_start_unix_secs));
            }
            if has_duplicate_key {
                // Retain input-order sequential upserts when a flush contains duplicate bucket
                // keys so signed BIGINT arithmetic preserves intermediate prefix bounds against
                // the stored row.
                for (delta, bucket_width_secs, bucket_start_unix_secs) in prepared {
                    sqlx::query(
                        "INSERT INTO api_key_usage_buckets_v1 (writer_epoch, key_id, bucket_width_secs, bucket_start_unix_secs, requests, input_tokens, output_tokens, cost_usd_micros) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT(writer_epoch, key_id, bucket_width_secs, bucket_start_unix_secs) DO UPDATE SET requests = api_key_usage_buckets_v1.requests + excluded.requests, input_tokens = api_key_usage_buckets_v1.input_tokens + excluded.input_tokens, output_tokens = api_key_usage_buckets_v1.output_tokens + excluded.output_tokens, cost_usd_micros = api_key_usage_buckets_v1.cost_usd_micros + excluded.cost_usd_micros",
                    )
                    .bind(flush.writer_epoch)
                    .bind(&delta.key.key_id)
                    .bind(bucket_width_secs)
                    .bind(bucket_start_unix_secs)
                    .bind(delta.usage.requests)
                    .bind(delta.usage.input_tokens)
                    .bind(delta.usage.output_tokens)
                    .bind(delta.usage.cost_usd_micros)
                    .execute(&mut *tx)
                    .await
                    .map_err(map_sqlx_error)?;
                }
            } else {
                let mut key_ids = Vec::with_capacity(prepared.len());
                let mut bucket_widths = Vec::with_capacity(prepared.len());
                let mut bucket_starts = Vec::with_capacity(prepared.len());
                let mut requests = Vec::with_capacity(prepared.len());
                let mut input_tokens = Vec::with_capacity(prepared.len());
                let mut output_tokens = Vec::with_capacity(prepared.len());
                let mut cost_usd_micros = Vec::with_capacity(prepared.len());
                for (delta, bucket_width_secs, bucket_start_unix_secs) in prepared {
                    key_ids.push(delta.key.key_id.as_str());
                    bucket_widths.push(bucket_width_secs);
                    bucket_starts.push(bucket_start_unix_secs);
                    requests.push(delta.usage.requests);
                    input_tokens.push(delta.usage.input_tokens);
                    output_tokens.push(delta.usage.output_tokens);
                    cost_usd_micros.push(delta.usage.cost_usd_micros);
                }
                sqlx::query(
                    "INSERT INTO api_key_usage_buckets_v1 \
                     (writer_epoch, key_id, bucket_width_secs, bucket_start_unix_secs, requests, input_tokens, output_tokens, cost_usd_micros) \
                     SELECT $1, t.key_id, t.bucket_width_secs, t.bucket_start_unix_secs, t.requests, t.input_tokens, t.output_tokens, t.cost_usd_micros \
                     FROM UNNEST($2::text[], $3::bigint[], $4::bigint[], $5::bigint[], $6::bigint[], $7::bigint[], $8::bigint[]) \
                       AS t(key_id, bucket_width_secs, bucket_start_unix_secs, requests, input_tokens, output_tokens, cost_usd_micros) \
                     ON CONFLICT(writer_epoch, key_id, bucket_width_secs, bucket_start_unix_secs) DO UPDATE SET \
                       requests = api_key_usage_buckets_v1.requests + excluded.requests, \
                       input_tokens = api_key_usage_buckets_v1.input_tokens + excluded.input_tokens, \
                       output_tokens = api_key_usage_buckets_v1.output_tokens + excluded.output_tokens, \
                       cost_usd_micros = api_key_usage_buckets_v1.cost_usd_micros + excluded.cost_usd_micros",
                )
                .bind(flush.writer_epoch)
                .bind(&key_ids)
                .bind(&bucket_widths)
                .bind(&bucket_starts)
                .bind(&requests)
                .bind(&input_tokens)
                .bind(&output_tokens)
                .bind(&cost_usd_micros)
                .execute(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
            }
        }
        sqlx::query("UPDATE api_key_usage_writers_v1 SET lease_until_unix_secs = $1, last_flush_id = $2 WHERE writer_epoch = $3")
            .bind(u64_to_i64(flush.lease_until_unix_secs, "API-key usage writer lease")?)
            .bind(flush.flush_id).bind(flush.writer_epoch)
            .execute(&mut *tx).await.map_err(map_sqlx_error)?;
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
        let mut builder = QueryBuilder::<Postgres>::new(
            "SELECT key_id, bucket_width_secs, bucket_start_unix_secs, SUM(requests)::BIGINT AS requests, SUM(input_tokens)::BIGINT AS input_tokens, SUM(output_tokens)::BIGINT AS output_tokens, SUM(cost_usd_micros)::BIGINT AS cost_usd_micros FROM api_key_usage_buckets_v1 WHERE writer_epoch <> ",
        );
        builder.push_bind(query.exclude_writer_epoch);
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
        builder
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx_error)?
            .into_iter()
            .map(row_to_bucket)
            .collect()
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
        let now = unix_secs(self.clock.now());
        let inactive_before = now.saturating_sub(writer_inactive_after_secs);
        let retain_since = now.saturating_sub(retain_for_secs);
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let stale = sqlx::query("SELECT b.writer_epoch, b.key_id, b.bucket_width_secs, b.bucket_start_unix_secs, b.requests, b.input_tokens, b.output_tokens, b.cost_usd_micros FROM api_key_usage_buckets_v1 b JOIN api_key_usage_writers_v1 w ON w.writer_epoch = b.writer_epoch WHERE b.writer_epoch <> $1 AND w.lease_until_unix_secs < $2 ORDER BY b.writer_epoch, b.key_id, b.bucket_width_secs, b.bucket_start_unix_secs LIMIT $3 FOR UPDATE OF b SKIP LOCKED")
            .bind(RETIRED_WRITER_EPOCH).bind(u64_to_i64(inactive_before, "API-key usage inactive cutoff")?)
            .bind(i64::try_from(batch_size).unwrap_or(i64::MAX)).fetch_all(&mut *tx).await.map_err(map_sqlx_error)?;
        for row in &stale {
            let source_epoch: Uuid = row.try_get("writer_epoch").map_err(map_sqlx_error)?;
            let key_id: String = row.try_get("key_id").map_err(map_sqlx_error)?;
            let width: i64 = row.try_get("bucket_width_secs").map_err(map_sqlx_error)?;
            let start: i64 = row
                .try_get("bucket_start_unix_secs")
                .map_err(map_sqlx_error)?;
            sqlx::query("INSERT INTO api_key_usage_buckets_v1 (writer_epoch, key_id, bucket_width_secs, bucket_start_unix_secs, requests, input_tokens, output_tokens, cost_usd_micros) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT(writer_epoch, key_id, bucket_width_secs, bucket_start_unix_secs) DO UPDATE SET requests = api_key_usage_buckets_v1.requests + excluded.requests, input_tokens = api_key_usage_buckets_v1.input_tokens + excluded.input_tokens, output_tokens = api_key_usage_buckets_v1.output_tokens + excluded.output_tokens, cost_usd_micros = api_key_usage_buckets_v1.cost_usd_micros + excluded.cost_usd_micros")
                .bind(RETIRED_WRITER_EPOCH).bind(&key_id).bind(width).bind(start)
                .bind(row.try_get::<i64, _>("requests").map_err(map_sqlx_error)?)
                .bind(row.try_get::<i64, _>("input_tokens").map_err(map_sqlx_error)?)
                .bind(row.try_get::<i64, _>("output_tokens").map_err(map_sqlx_error)?)
                .bind(row.try_get::<i64, _>("cost_usd_micros").map_err(map_sqlx_error)?)
                .execute(&mut *tx).await.map_err(map_sqlx_error)?;
            sqlx::query("DELETE FROM api_key_usage_buckets_v1 WHERE writer_epoch = $1 AND key_id = $2 AND bucket_width_secs = $3 AND bucket_start_unix_secs = $4")
                .bind(source_epoch).bind(&key_id).bind(width).bind(start).execute(&mut *tx).await.map_err(map_sqlx_error)?;
        }
        sqlx::query("DELETE FROM api_key_usage_writers_v1 w WHERE writer_epoch <> $1 AND lease_until_unix_secs < $2 AND NOT EXISTS (SELECT 1 FROM api_key_usage_buckets_v1 b WHERE b.writer_epoch = w.writer_epoch)")
            .bind(RETIRED_WRITER_EPOCH).bind(u64_to_i64(inactive_before, "API-key usage inactive cutoff")?)
            .execute(&mut *tx).await.map_err(map_sqlx_error)?;
        let pruned = sqlx::query("DELETE FROM api_key_usage_buckets_v1 WHERE ctid IN (SELECT ctid FROM api_key_usage_buckets_v1 WHERE bucket_start_unix_secs + bucket_width_secs <= $1 ORDER BY bucket_start_unix_secs LIMIT $2 FOR UPDATE SKIP LOCKED)")
            .bind(u64_to_i64(retain_since, "API-key usage retention cutoff")?)
            .bind(i64::try_from(batch_size).unwrap_or(i64::MAX)).execute(&mut *tx).await.map_err(map_sqlx_error)?.rows_affected();
        tx.commit().await.map_err(map_sqlx_error)?;
        let marker_chunk_size = batch_size.max(1);
        for _ in 0..MAX_FLUSH_MARKER_PRUNE_BATCHES {
            let removed = self
                .prune_expired_api_key_usage_flush_markers(retain_for_secs, marker_chunk_size)
                .await?;
            if removed < u64::try_from(marker_chunk_size).unwrap_or(u64::MAX) {
                break;
            }
        }
        Ok(ApiKeyUsageCompactionRun {
            folded_rows: stale.len() as u64,
            pruned_rows: pruned,
        })
    }

    async fn prune_expired_api_key_usage_flush_markers(
        &self,
        retain_for_secs: u64,
        batch_size: usize,
    ) -> StorageResult<u64> {
        if batch_size == 0 {
            return Ok(0);
        }
        let now = unix_secs(self.clock.now());
        let retain_since = now.saturating_sub(retain_for_secs);
        let retain_since_i64 = u64_to_i64(retain_since, "API-key usage retention cutoff")?;
        let limit_i64 = i64::try_from(batch_size).unwrap_or(i64::MAX);
        let removed = sqlx::query(
            "DELETE FROM api_key_usage_flushes_v1 WHERE ctid IN (SELECT ctid FROM api_key_usage_flushes_v1 WHERE applied_at_unix_secs <= $1 ORDER BY applied_at_unix_secs LIMIT $2 FOR UPDATE SKIP LOCKED)",
        )
        .bind(retain_since_i64)
        .bind(limit_i64)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?
        .rows_affected();
        Ok(removed)
    }
}

#[async_trait]
impl ApiKeyConcurrencyHoldStore for PostgresStorage {
    async fn insert_api_key_concurrency_hold(
        &self,
        hold: &ApiKeyConcurrencyHold,
    ) -> StorageResult<()> {
        sqlx::query("INSERT INTO api_key_concurrency_holds_v1 (hold_id, key_id, writer_epoch, acquired_at_unix_secs) VALUES ($1, $2, $3, $4) ON CONFLICT(hold_id) DO NOTHING")
            .bind(hold.hold_id)
            .bind(&hold.key_id)
            .bind(hold.writer_epoch)
            .bind(u64_to_i64(hold.acquired_at_unix_secs, "API-key concurrency hold acquired_at")?)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn delete_api_key_concurrency_hold(&self, hold_id: Uuid) -> StorageResult<()> {
        sqlx::query("DELETE FROM api_key_concurrency_holds_v1 WHERE hold_id = $1")
            .bind(hold_id)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn delete_uncertain_api_key_concurrency_hold(
        &self,
        hold_id: Uuid,
        key_id: &str,
    ) -> StorageResult<()> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        // Force READ COMMITTED so taking the advisory lock does not freeze an
        // earlier REPEATABLE READ snapshot if the session/role defaults stricter.
        sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(key_id)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        sqlx::query("DELETE FROM api_key_concurrency_holds_v1 WHERE hold_id = $1")
            .bind(hold_id)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn count_api_key_concurrency_holds(
        &self,
        key_id: &str,
        now_unix_secs: u64,
        max_age_secs: u64,
    ) -> StorageResult<u64> {
        let cutoff = now_unix_secs.saturating_sub(max_age_secs);
        let row = sqlx::query("SELECT COUNT(*)::BIGINT AS live FROM api_key_concurrency_holds_v1 WHERE key_id = $1 AND acquired_at_unix_secs > $2")
            .bind(key_id)
            .bind(u64_to_i64(cutoff, "API-key concurrency hold expiry cutoff")?)
            .fetch_one(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        i64_to_u64(
            row.try_get::<i64, _>("live").map_err(map_sqlx_error)?,
            "API-key concurrency hold count",
        )
    }

    async fn delete_expired_api_key_concurrency_holds(
        &self,
        _now_unix_secs: u64,
        max_age_secs: u64,
    ) -> StorageResult<u64> {
        let max_age = i64::try_from(max_age_secs).unwrap_or(i64::MAX);
        let deleted = sqlx::query(
            "DELETE FROM api_key_concurrency_holds_v1 WHERE acquired_at_unix_secs <= (EXTRACT(EPOCH FROM NOW())::BIGINT - $1)",
        )
        .bind(max_age)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?
        .rows_affected();
        Ok(deleted)
    }

    async fn refresh_api_key_concurrency_holds(
        &self,
        hold_ids: &[Uuid],
        _now_unix_secs: u64,
    ) -> StorageResult<u64> {
        if hold_ids.is_empty() {
            return Ok(0);
        }
        let rows_affected = sqlx::query(
            "UPDATE api_key_concurrency_holds_v1 SET acquired_at_unix_secs = EXTRACT(EPOCH FROM NOW())::BIGINT WHERE hold_id = ANY($1::uuid[])",
        )
        .bind(hold_ids)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?
        .rows_affected();
        Ok(rows_affected)
    }

    async fn try_acquire_api_key_concurrency_hold(
        &self,
        hold_id: Uuid,
        key_id: &str,
        writer_epoch: Uuid,
        cap: u32,
        max_age_secs: u64,
    ) -> StorageResult<bool> {
        if cap == 0 {
            return Ok(false);
        }
        let max_age = i64::try_from(max_age_secs).unwrap_or(i64::MAX);
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        // Force READ COMMITTED so taking the advisory lock does not freeze an
        // earlier REPEATABLE READ snapshot if the session/role defaults stricter.
        sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(key_id)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        let row = sqlx::query(
            "SELECT COUNT(*)::BIGINT AS live FROM api_key_concurrency_holds_v1 WHERE key_id = $1 AND acquired_at_unix_secs > (EXTRACT(EPOCH FROM NOW())::BIGINT - $2)",
        )
        .bind(key_id)
        .bind(max_age)
        .fetch_one(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        let live = row.try_get::<i64, _>("live").map_err(map_sqlx_error)?;
        if live >= i64::from(cap) {
            tx.rollback().await.map_err(map_sqlx_error)?;
            return Ok(false);
        }
        sqlx::query(
            "INSERT INTO api_key_concurrency_holds_v1 (hold_id, key_id, writer_epoch, acquired_at_unix_secs) VALUES ($1, $2, $3, EXTRACT(EPOCH FROM NOW())::BIGINT) ON CONFLICT(hold_id) DO NOTHING",
        )
        .bind(hold_id)
        .bind(key_id)
        .bind(writer_epoch)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(true)
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

fn row_to_bucket(row: sqlx::postgres::PgRow) -> StorageResult<ApiKeyUsageBucket> {
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
