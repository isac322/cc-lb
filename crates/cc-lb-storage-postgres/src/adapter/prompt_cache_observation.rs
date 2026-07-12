use async_trait::async_trait;
use cc_lb_domain::TtlClass;
use cc_lb_storage_api::{
    PromptCacheObservationRecord, PromptCacheObservationStore, StorageError, StorageResult,
};
use sqlx::{Row, postgres::PgRow};

use crate::{
    adapter::{PostgresStorage, i64_to_u64, u64_to_i64},
    error_map::map_sqlx_error,
};

#[async_trait]
impl PromptCacheObservationStore for PostgresStorage {
    async fn upsert_observation(&self, record: &PromptCacheObservationRecord) -> StorageResult<()> {
        sqlx::query(
            "INSERT INTO prompt_cache_observations (upstream_id, canonical_model_id, v3_prefix_key, ttl_class, expires_at, last_observed_at, hash_schema_version, prefix_content_block_index, estimated_prefix_tokens, token_estimate_source, last_provider_cache_read_tokens, last_provider_cache_creation_tokens) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) ON CONFLICT (upstream_id, canonical_model_id, v3_prefix_key, ttl_class) DO UPDATE SET expires_at = EXCLUDED.expires_at, last_observed_at = EXCLUDED.last_observed_at, hash_schema_version = EXCLUDED.hash_schema_version, prefix_content_block_index = EXCLUDED.prefix_content_block_index, estimated_prefix_tokens = EXCLUDED.estimated_prefix_tokens, token_estimate_source = EXCLUDED.token_estimate_source, last_provider_cache_read_tokens = EXCLUDED.last_provider_cache_read_tokens, last_provider_cache_creation_tokens = EXCLUDED.last_provider_cache_creation_tokens",
        )
        .bind(record.upstream_id)
        .bind(&record.canonical_model_id)
        .bind(&record.v3_prefix_key)
        .bind(ttl_class_to_db(record.ttl_class))
        .bind(u64_to_i64(
            record.expires_at_unix_secs,
            "prompt cache expires_at_unix_secs",
        )?)
        .bind(u64_to_i64(
            record.last_observed_at_unix_secs,
            "prompt cache last_observed_at_unix_secs",
        )?)
        .bind(i16::from(record.hash_schema_version))
        .bind(i64::from(record.prefix_content_block_index))
        .bind(u64_to_i64(
            record.estimated_prefix_tokens,
            "prompt cache estimated_prefix_tokens",
        )?)
        .bind(&record.token_estimate_source)
        .bind(
            record
                .last_provider_cache_read_tokens
                .map(|value| u64_to_i64(value, "prompt cache last_provider_cache_read_tokens"))
                .transpose()?,
        )
        .bind(
            record
                .last_provider_cache_creation_tokens
                .map(|value| u64_to_i64(value, "prompt cache last_provider_cache_creation_tokens"))
                .transpose()?,
        )
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn list_active_for_upstream(
        &self,
        upstream_id: uuid::Uuid,
        not_expired_at_unix_secs: u64,
    ) -> StorageResult<Vec<PromptCacheObservationRecord>> {
        let rows = sqlx::query(
            "SELECT upstream_id, canonical_model_id, v3_prefix_key, ttl_class, expires_at, last_observed_at, hash_schema_version, prefix_content_block_index, estimated_prefix_tokens, token_estimate_source, last_provider_cache_read_tokens, last_provider_cache_creation_tokens FROM prompt_cache_observations WHERE upstream_id = $1 AND expires_at > $2 ORDER BY v3_prefix_key, ttl_class",
        )
        .bind(upstream_id)
        .bind(u64_to_i64(
            not_expired_at_unix_secs,
            "prompt cache not_expired_at_unix_secs",
        )?)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        rows.into_iter().map(row_to_record).collect()
    }

    async fn list_active_for_upstream_keys(
        &self,
        upstream_id: uuid::Uuid,
        not_expired_at_unix_secs: u64,
        v3_prefix_keys: &[String],
    ) -> StorageResult<Vec<PromptCacheObservationRecord>> {
        if v3_prefix_keys.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(
            "SELECT upstream_id, canonical_model_id, v3_prefix_key, ttl_class, expires_at, last_observed_at, hash_schema_version, prefix_content_block_index, estimated_prefix_tokens, token_estimate_source, last_provider_cache_read_tokens, last_provider_cache_creation_tokens FROM prompt_cache_observations WHERE upstream_id = $1 AND expires_at > $2 AND v3_prefix_key = ANY($3) ORDER BY v3_prefix_key, ttl_class",
        )
        .bind(upstream_id)
        .bind(u64_to_i64(
            not_expired_at_unix_secs,
            "prompt cache not_expired_at_unix_secs",
        )?)
        .bind(v3_prefix_keys)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        rows.into_iter().map(row_to_record).collect()
    }

    async fn purge_expired_before(&self, ts_unix_secs: u64) -> StorageResult<u64> {
        let result = sqlx::query("DELETE FROM prompt_cache_observations WHERE expires_at < $1")
            .bind(u64_to_i64(
                ts_unix_secs,
                "prompt cache purge cutoff unix secs",
            )?)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        Ok(result.rows_affected())
    }

    async fn count(&self) -> StorageResult<u64> {
        let count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM prompt_cache_observations")
            .fetch_one(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        i64_to_u64(count, "prompt cache observation count")
    }
}

fn row_to_record(row: PgRow) -> StorageResult<PromptCacheObservationRecord> {
    Ok(PromptCacheObservationRecord {
        upstream_id: row.try_get("upstream_id").map_err(map_sqlx_error)?,
        canonical_model_id: row.try_get("canonical_model_id").map_err(map_sqlx_error)?,
        v3_prefix_key: row.try_get("v3_prefix_key").map_err(map_sqlx_error)?,
        ttl_class: ttl_class_from_db(row.try_get("ttl_class").map_err(map_sqlx_error)?)?,
        expires_at_unix_secs: i64_to_u64(
            row.try_get("expires_at").map_err(map_sqlx_error)?,
            "prompt cache expires_at",
        )?,
        last_observed_at_unix_secs: i64_to_u64(
            row.try_get("last_observed_at").map_err(map_sqlx_error)?,
            "prompt cache last_observed_at",
        )?,
        hash_schema_version: hash_schema_version_from_db(
            row.try_get("hash_schema_version").map_err(map_sqlx_error)?,
        )?,
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
        last_provider_cache_read_tokens: row
            .try_get::<Option<i64>, _>("last_provider_cache_read_tokens")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "prompt cache last_provider_cache_read_tokens"))
            .transpose()?,
        last_provider_cache_creation_tokens: row
            .try_get::<Option<i64>, _>("last_provider_cache_creation_tokens")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "prompt cache last_provider_cache_creation_tokens"))
            .transpose()?,
    })
}

fn ttl_class_to_db(ttl_class: TtlClass) -> i16 {
    match ttl_class {
        TtlClass::Ephemeral5m => 0,
        TtlClass::Ephemeral1h => 1,
    }
}

fn ttl_class_from_db(value: i16) -> StorageResult<TtlClass> {
    match value {
        0 => Ok(TtlClass::Ephemeral5m),
        1 => Ok(TtlClass::Ephemeral1h),
        value => Err(StorageError::Corrupted {
            message: format!("invalid prompt cache ttl_class {value}"),
        }),
    }
}

fn hash_schema_version_from_db(value: i16) -> StorageResult<u8> {
    u8::try_from(value).map_err(|_| StorageError::Corrupted {
        message: format!("invalid prompt cache hash_schema_version {value}"),
    })
}
