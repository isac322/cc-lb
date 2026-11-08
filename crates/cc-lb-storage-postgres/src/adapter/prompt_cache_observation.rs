use async_trait::async_trait;
use cc_lb_domain::TtlClass;
use cc_lb_storage_api::{
    PromptCacheObservationRecord, PromptCacheObservationStore,
    ProviderPromptCacheObservationRecord, StorageError, StorageResult,
};
use sqlx::{Row, postgres::PgRow};

use crate::{
    adapter::{PostgresStorage, i64_to_u64, u64_to_i64},
    error_map::map_sqlx_error,
};

/// Bounds how many rows one purge DELETE removes so a large expired backlog cannot
/// hold a long row-lock / WAL burst that stalls request-path writers.
const PURGE_BATCH_SIZE: i64 = 1_000;
// Bound work per call so a continuous influx of already-expired rows cannot pin
// the purge in an unbounded loop; the scheduler re-runs the job to finish later.
const PURGE_MAX_BATCHES: usize = 1_024;

#[async_trait]
impl PromptCacheObservationStore for PostgresStorage {
    async fn upsert_observation(&self, record: &PromptCacheObservationRecord) -> StorageResult<()> {
        // Monotonic winner update: the row is only overwritten when the incoming
        // (expires_at, last_observed_at) pair is strictly newer, so a delayed or
        // replayed write cannot regress a fresher observation. The WHERE guard
        // keeps the update atomic — the winner's metadata is written whole, never
        // spliced with the loser's fields.
        sqlx::query(
            "INSERT INTO prompt_cache_observations (upstream_id, canonical_model_id, v3_prefix_key, ttl_class, expires_at, last_observed_at, hash_schema_version, prefix_content_block_index, estimated_prefix_tokens, token_estimate_source) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT (upstream_id, canonical_model_id, v3_prefix_key, ttl_class) DO UPDATE SET expires_at = EXCLUDED.expires_at, last_observed_at = EXCLUDED.last_observed_at, hash_schema_version = EXCLUDED.hash_schema_version, prefix_content_block_index = EXCLUDED.prefix_content_block_index, estimated_prefix_tokens = EXCLUDED.estimated_prefix_tokens, token_estimate_source = EXCLUDED.token_estimate_source WHERE EXCLUDED.expires_at > prompt_cache_observations.expires_at OR (EXCLUDED.expires_at = prompt_cache_observations.expires_at AND EXCLUDED.last_observed_at > prompt_cache_observations.last_observed_at)",
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
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        Ok(())
    }
    async fn upsert_provider_observation(
        &self,
        record: &ProviderPromptCacheObservationRecord,
    ) -> StorageResult<()> {
        sqlx::query(
            "INSERT INTO provider_native_cache_observations (upstream_id, provider, canonical_model_id, mode, isolation_scope, identity_hash, boundary, evidence_source, observed_cached_tokens, observed_write_tokens, retention_secs, expires_at, last_observed_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) ON CONFLICT (upstream_id, provider, canonical_model_id, mode, isolation_scope, identity_hash, boundary) DO UPDATE SET evidence_source = EXCLUDED.evidence_source, observed_cached_tokens = EXCLUDED.observed_cached_tokens, observed_write_tokens = EXCLUDED.observed_write_tokens, retention_secs = EXCLUDED.retention_secs, expires_at = EXCLUDED.expires_at, last_observed_at = EXCLUDED.last_observed_at WHERE EXCLUDED.last_observed_at > provider_native_cache_observations.last_observed_at",
        )
        .bind(record.upstream_id)
        .bind(&record.provider)
        .bind(&record.canonical_model_id)
        .bind(&record.mode)
        .bind(&record.isolation_scope)
        .bind(&record.identity_hash)
        .bind(&record.boundary)
        .bind(&record.evidence_source)
        .bind(
            record
                .observed_cached_tokens
                .map(|value| u64_to_i64(value, "native cached tokens"))
                .transpose()?,
        )
        .bind(
            record
                .observed_write_tokens
                .map(|value| u64_to_i64(value, "native write tokens"))
                .transpose()?,
        )
        .bind(
            record
                .retention_secs
                .map(|value| u64_to_i64(value, "native retention"))
                .transpose()?,
        )
        .bind(
            record
                .expires_at_unix_secs
                .map(|value| u64_to_i64(value, "native expiry"))
                .transpose()?,
        )
        .bind(u64_to_i64(
            record.last_observed_at_unix_secs,
            "native observed_at",
        )?)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn list_active_provider_observations(
        &self,
        upstream_id: uuid::Uuid,
        provider: &str,
        canonical_model_id: &str,
        mode: &str,
        isolation_scope: &str,
        identity_hash: &str,
        not_expired_at_unix_secs: u64,
    ) -> StorageResult<Vec<ProviderPromptCacheObservationRecord>> {
        let rows = sqlx::query(
            "SELECT upstream_id, provider, canonical_model_id, mode, isolation_scope, identity_hash, boundary, evidence_source, observed_cached_tokens, observed_write_tokens, retention_secs, expires_at, last_observed_at FROM provider_native_cache_observations WHERE upstream_id = $1 AND provider = $2 AND canonical_model_id = $3 AND mode = $4 AND isolation_scope = $5 AND identity_hash = $6 AND (expires_at IS NULL OR expires_at > $7) ORDER BY boundary",
        )
        .bind(upstream_id)
        .bind(provider)
        .bind(canonical_model_id)
        .bind(mode)
        .bind(isolation_scope)
        .bind(identity_hash)
        .bind(u64_to_i64(not_expired_at_unix_secs, "native lookup cutoff")?)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        rows.into_iter()
            .map(|row| {
                Ok(ProviderPromptCacheObservationRecord {
                    upstream_id: row.try_get("upstream_id").map_err(map_sqlx_error)?,
                    provider: row.try_get("provider").map_err(map_sqlx_error)?,
                    canonical_model_id: row.try_get("canonical_model_id").map_err(map_sqlx_error)?,
                    mode: row.try_get("mode").map_err(map_sqlx_error)?,
                    isolation_scope: row.try_get("isolation_scope").map_err(map_sqlx_error)?,
                    identity_hash: row.try_get("identity_hash").map_err(map_sqlx_error)?,
                    boundary: row.try_get("boundary").map_err(map_sqlx_error)?,
                    evidence_source: row.try_get("evidence_source").map_err(map_sqlx_error)?,
                    observed_cached_tokens: row
                        .try_get::<Option<i64>, _>("observed_cached_tokens")
                        .map_err(map_sqlx_error)?
                        .map(|value| i64_to_u64(value, "native cached tokens"))
                        .transpose()?,
                    observed_write_tokens: row
                        .try_get::<Option<i64>, _>("observed_write_tokens")
                        .map_err(map_sqlx_error)?
                        .map(|value| i64_to_u64(value, "native write tokens"))
                        .transpose()?,
                    retention_secs: row
                        .try_get::<Option<i64>, _>("retention_secs")
                        .map_err(map_sqlx_error)?
                        .map(|value| i64_to_u64(value, "native retention"))
                        .transpose()?,
                    expires_at_unix_secs: row
                        .try_get::<Option<i64>, _>("expires_at")
                        .map_err(map_sqlx_error)?
                        .map(|value| i64_to_u64(value, "native expiry"))
                        .transpose()?,
                    last_observed_at_unix_secs: i64_to_u64(
                        row.try_get("last_observed_at").map_err(map_sqlx_error)?,
                        "native observed_at",
                    )?,
                })
            })
            .collect()
    }

    async fn list_active_for_candidates(
        &self,
        upstream_ids: &[uuid::Uuid],
        canonical_model_id: &str,
        v3_prefix_keys: &[String],
        not_expired_at_unix_secs: u64,
    ) -> StorageResult<Vec<PromptCacheObservationRecord>> {
        if upstream_ids.is_empty() || v3_prefix_keys.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(
            "SELECT upstream_id, canonical_model_id, v3_prefix_key, ttl_class, expires_at, last_observed_at, hash_schema_version, prefix_content_block_index, estimated_prefix_tokens, token_estimate_source FROM prompt_cache_observations WHERE upstream_id = ANY($1) AND canonical_model_id = $2 AND v3_prefix_key = ANY($3) AND expires_at > $4 ORDER BY upstream_id, v3_prefix_key, ttl_class",
        )
        .bind(upstream_ids)
        .bind(canonical_model_id)
        .bind(v3_prefix_keys)
        .bind(u64_to_i64(
            not_expired_at_unix_secs,
            "prompt cache not_expired_at_unix_secs",
        )?)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        rows.into_iter().map(row_to_record).collect()
    }

    async fn purge_expired_before(&self, ts_unix_secs: u64) -> StorageResult<u64> {
        let cutoff = u64_to_i64(ts_unix_secs, "prompt cache purge cutoff unix secs")?;
        let mut total_removed = 0u64;
        for _ in 0..PURGE_MAX_BATCHES {
            // Break on how many expired rows were found, not how many were
            // removed. A concurrent upsert can bump expires_at on a selected
            // ctid so the outer predicate skips it; keying the break on the
            // removed count could stop while other expired rows still remain.
            let row = sqlx::query(
                "WITH candidates AS MATERIALIZED ( \
                     SELECT ctid FROM prompt_cache_observations \
                     WHERE expires_at < $1 LIMIT $2 \
                 ), deleted AS ( \
                     DELETE FROM prompt_cache_observations \
                     WHERE ctid IN (SELECT ctid FROM candidates) AND expires_at < $1 \
                     RETURNING 1 \
                 ) \
                 SELECT \
                     (SELECT count(*) FROM candidates) AS candidate_count, \
                     (SELECT count(*) FROM deleted) AS deleted_count",
            )
            .bind(cutoff)
            .bind(PURGE_BATCH_SIZE)
            .fetch_one(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
            let candidate_count: i64 = row.try_get("candidate_count").map_err(map_sqlx_error)?;
            let deleted_count: i64 = row.try_get("deleted_count").map_err(map_sqlx_error)?;
            total_removed = total_removed.saturating_add(i64_to_u64(
                deleted_count,
                "prompt cache purge deleted count",
            )?);
            if candidate_count < PURGE_BATCH_SIZE {
                break;
            }
        }
        Ok(total_removed)
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
