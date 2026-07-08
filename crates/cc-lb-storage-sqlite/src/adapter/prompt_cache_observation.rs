use async_trait::async_trait;
use cc_lb_storage_api::{
    PromptCacheObservationRecord, PromptCacheObservationStore, StorageError, StorageResult,
    TtlClass,
};
use sqlx::Row;
use uuid::Uuid;

use crate::{SqliteStorage, map_sqlx_error};

#[async_trait]
impl PromptCacheObservationStore for SqliteStorage {
    async fn upsert_observation(&self, record: &PromptCacheObservationRecord) -> StorageResult<()> {
        sqlx::query(
            "INSERT INTO prompt_cache_observations \
             (upstream_id, canonical_model_id, prefix_hash, ttl_class, expires_at, last_observed_at, hash_schema_version) \
             VALUES (?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(upstream_id, canonical_model_id, prefix_hash, ttl_class) DO UPDATE SET \
             expires_at = excluded.expires_at, \
             last_observed_at = excluded.last_observed_at, \
             hash_schema_version = excluded.hash_schema_version",
        )
        .bind(record.upstream_id.to_string())
        .bind(&record.canonical_model_id)
        .bind(&record.prefix_hash)
        .bind(ttl_class_to_db(record.ttl_class))
        .bind(u64_to_i64(record.expires_at_unix_secs, "prompt cache expires_at_unix_secs")?)
        .bind(u64_to_i64(record.last_observed_at_unix_secs, "prompt cache last_observed_at_unix_secs")?)
        .bind(i64::from(record.hash_schema_version))
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn list_active_for_upstream(
        &self,
        upstream_id: Uuid,
        not_expired_at_unix_secs: u64,
    ) -> StorageResult<Vec<PromptCacheObservationRecord>> {
        let rows = sqlx::query(
            "SELECT upstream_id, canonical_model_id, prefix_hash, ttl_class, expires_at, last_observed_at, hash_schema_version \
             FROM prompt_cache_observations \
             WHERE upstream_id = ? AND expires_at > ? \
             ORDER BY prefix_hash, ttl_class",
        )
        .bind(upstream_id.to_string())
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
        let result = sqlx::query("DELETE FROM prompt_cache_observations WHERE expires_at < ?")
            .bind(u64_to_i64(
                ts_unix_secs,
                "prompt cache purge cutoff unix secs",
            )?)
            .execute(self.pool())
            .await
            .map_err(map_sqlx_error)?;
        Ok(result.rows_affected())
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
        prefix_hash: row.try_get("prefix_hash").map_err(map_sqlx_error)?,
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
