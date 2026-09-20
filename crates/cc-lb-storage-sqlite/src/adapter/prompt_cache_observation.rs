use async_trait::async_trait;
use cc_lb_domain::TtlClass;
use cc_lb_storage_api::{
    PromptCacheObservationRecord, PromptCacheObservationStore, StorageError, StorageResult,
};
use sqlx::Row;
use uuid::Uuid;

use crate::{SqliteStorage, map_sqlx_error};

/// Bounds how many rows one purge DELETE removes so a large expired backlog cannot
/// hold the single SQLite write lock long enough to stall request-path writers.
const PURGE_BATCH_SIZE: i64 = 1_000;
// Bound work per call so a continuous influx of already-expired rows cannot pin
// the purge in an unbounded loop; the scheduler re-runs the job to finish later.
const PURGE_MAX_BATCHES: usize = 1_024;

#[async_trait]
impl PromptCacheObservationStore for SqliteStorage {
    async fn upsert_observation(&self, record: &PromptCacheObservationRecord) -> StorageResult<()> {
        // Monotonic winner update: the row is only overwritten when the incoming
        // (expires_at, last_observed_at) pair is strictly newer, so a delayed or
        // replayed write cannot regress a fresher observation. The WHERE guard
        // keeps the update atomic — the winner's metadata is written whole, never
        // spliced with the loser's fields.
        sqlx::query(
            "INSERT INTO prompt_cache_observations \
             (upstream_id, canonical_model_id, v3_prefix_key, ttl_class, expires_at, last_observed_at, hash_schema_version, prefix_content_block_index, estimated_prefix_tokens, token_estimate_source) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(upstream_id, canonical_model_id, v3_prefix_key, ttl_class) DO UPDATE SET \
             expires_at = excluded.expires_at, \
             last_observed_at = excluded.last_observed_at, \
             hash_schema_version = excluded.hash_schema_version, \
             prefix_content_block_index = excluded.prefix_content_block_index, \
             estimated_prefix_tokens = excluded.estimated_prefix_tokens, \
             token_estimate_source = excluded.token_estimate_source \
             WHERE excluded.expires_at > prompt_cache_observations.expires_at \
                OR (excluded.expires_at = prompt_cache_observations.expires_at \
                    AND excluded.last_observed_at > prompt_cache_observations.last_observed_at)",
        )
        .bind(record.upstream_id.to_string())
        .bind(&record.canonical_model_id)
        .bind(&record.v3_prefix_key)
        .bind(ttl_class_to_db(record.ttl_class))
        .bind(u64_to_i64(record.expires_at_unix_secs, "prompt cache expires_at_unix_secs")?)
        .bind(u64_to_i64(record.last_observed_at_unix_secs, "prompt cache last_observed_at_unix_secs")?)
        .bind(i64::from(record.hash_schema_version))
        .bind(i64::from(record.prefix_content_block_index))
        .bind(u64_to_i64(record.estimated_prefix_tokens, "prompt cache estimated_prefix_tokens")?)
        .bind(&record.token_estimate_source)
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn list_active_for_candidates(
        &self,
        upstream_ids: &[Uuid],
        canonical_model_id: &str,
        v3_prefix_keys: &[String],
        not_expired_at_unix_secs: u64,
    ) -> StorageResult<Vec<PromptCacheObservationRecord>> {
        if upstream_ids.is_empty() || v3_prefix_keys.is_empty() {
            return Ok(Vec::new());
        }
        // Bound JSON arrays keep the whole candidate set in two binds, so the
        // lookup stays a single statement with one snapshot regardless of input
        // size — no IN-list bind-limit chunking. json_each is the same JSON1
        // path request_events uses for bound principal lists.
        let rows = sqlx::query(
            "SELECT upstream_id, canonical_model_id, v3_prefix_key, ttl_class, expires_at, last_observed_at, hash_schema_version, prefix_content_block_index, estimated_prefix_tokens, token_estimate_source \
             FROM prompt_cache_observations \
             WHERE upstream_id IN (SELECT CAST(value AS TEXT) FROM json_each(?1)) \
               AND canonical_model_id = ?2 \
               AND v3_prefix_key IN (SELECT value FROM json_each(?3)) \
               AND expires_at > ?4 \
             ORDER BY upstream_id, v3_prefix_key, ttl_class",
        )
        .bind(serde_json::to_string(upstream_ids)?)
        .bind(canonical_model_id)
        .bind(serde_json::to_string(v3_prefix_keys)?)
        .bind(u64_to_i64(
            not_expired_at_unix_secs,
            "prompt cache not_expired_at_unix_secs",
        )?)
        .fetch_all(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        rows.into_iter().map(row_to_record).collect()
    }

    async fn purge_expired_before(&self, ts_unix_secs: u64) -> StorageResult<u64> {
        let cutoff = u64_to_i64(ts_unix_secs, "prompt cache purge cutoff unix secs")?;
        let mut total_removed = 0u64;
        for _ in 0..PURGE_MAX_BATCHES {
            let removed = sqlx::query(
                "DELETE FROM prompt_cache_observations \
                 WHERE expires_at < ? \
                   AND rowid IN ( \
                       SELECT rowid FROM prompt_cache_observations \
                       WHERE expires_at < ? LIMIT ? \
                   )",
            )
            .bind(cutoff)
            .bind(cutoff)
            .bind(PURGE_BATCH_SIZE)
            .execute(self.pool())
            .await
            .map_err(map_sqlx_error)?
            .rows_affected();
            total_removed = total_removed.saturating_add(removed);
            if removed < PURGE_BATCH_SIZE as u64 {
                break;
            }
        }
        Ok(total_removed)
    }

    async fn count(&self) -> StorageResult<u64> {
        let count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM prompt_cache_observations")
            .fetch_one(self.pool())
            .await
            .map_err(map_sqlx_error)?;
        i64_to_u64(count, "prompt cache observation count")
    }
}

fn row_to_record(row: sqlx::sqlite::SqliteRow) -> StorageResult<PromptCacheObservationRecord> {
    let upstream_id = row
        .try_get::<String, _>("upstream_id")
        .map_err(map_sqlx_error)?;
    Ok(PromptCacheObservationRecord {
        upstream_id: uuid_from_db(&upstream_id, "prompt cache upstream_id")?,
        canonical_model_id: row.try_get("canonical_model_id").map_err(map_sqlx_error)?,
        v3_prefix_key: row.try_get("v3_prefix_key").map_err(map_sqlx_error)?,
        ttl_class: ttl_class_from_db(
            &row.try_get::<String, _>("ttl_class")
                .map_err(map_sqlx_error)?,
        )?,
        expires_at_unix_secs: i64_to_u64(
            row.try_get("expires_at").map_err(map_sqlx_error)?,
            "prompt cache expires_at",
        )?,
        last_observed_at_unix_secs: i64_to_u64(
            row.try_get("last_observed_at").map_err(map_sqlx_error)?,
            "prompt cache last_observed_at",
        )?,
        hash_schema_version: u8::try_from(
            row.try_get::<i64, _>("hash_schema_version")
                .map_err(map_sqlx_error)?,
        )
        .map_err(|_| StorageError::Corrupted {
            message: "invalid prompt cache hash_schema_version".to_owned(),
        })?,
        prefix_content_block_index: u32::try_from(
            row.try_get::<i64, _>("prefix_content_block_index")
                .map_err(map_sqlx_error)?,
        )
        .map_err(|_| StorageError::Corrupted {
            message: "invalid prompt cache prefix_content_block_index".to_owned(),
        })?,
        estimated_prefix_tokens: i64_to_u64(
            row.try_get("estimated_prefix_tokens")
                .map_err(map_sqlx_error)?,
            "prompt cache estimated_prefix_tokens",
        )?,
        token_estimate_source: row
            .try_get("token_estimate_source")
            .map_err(map_sqlx_error)?,
    })
}

fn ttl_class_to_db(ttl_class: TtlClass) -> &'static str {
    match ttl_class {
        TtlClass::Ephemeral5m => "0",
        TtlClass::Ephemeral1h => "1",
    }
}

fn ttl_class_from_db(value: &str) -> StorageResult<TtlClass> {
    match value {
        "0" => Ok(TtlClass::Ephemeral5m),
        "1" => Ok(TtlClass::Ephemeral1h),
        value => Err(StorageError::Corrupted {
            message: format!("invalid prompt cache ttl_class {value}"),
        }),
    }
}

fn uuid_from_db(value: &str, field: &str) -> StorageResult<Uuid> {
    Uuid::parse_str(value).map_err(|error| StorageError::Corrupted {
        message: format!("invalid {field}: {error}"),
    })
}

fn u64_to_i64(value: u64, field: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| StorageError::InvalidInput {
        field: field.to_owned(),
        reason: "value exceeds i64::MAX".to_owned(),
    })
}

fn i64_to_u64(value: i64, field: &str) -> StorageResult<u64> {
    u64::try_from(value).map_err(|_| StorageError::Corrupted {
        message: format!("negative {field} value {value}"),
    })
}
