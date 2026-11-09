use async_trait::async_trait;
use cc_lb_domain::TtlClass;
use cc_lb_storage_api::{
    PromptCacheObservationRecord, PromptCacheObservationStore,
    ProviderPromptCacheObservationRecord, StorageError, StorageResult,
    prompt_cache_observation::MAX_PROVIDER_NATIVE_CANDIDATES,
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
    async fn upsert_provider_observation(
        &self,
        record: &ProviderPromptCacheObservationRecord,
    ) -> StorageResult<()> {
        sqlx::query(
            "INSERT INTO provider_native_cache_observations \
             (upstream_id, provider, canonical_model_id, mode, isolation_scope, identity_hash, boundary, evidence_source, observed_cached_tokens, observed_write_tokens, retention_secs, expires_at, last_observed_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(upstream_id, provider, canonical_model_id, mode, isolation_scope, identity_hash, boundary) DO UPDATE SET \
             evidence_source = excluded.evidence_source, \
             observed_cached_tokens = excluded.observed_cached_tokens, \
             observed_write_tokens = excluded.observed_write_tokens, \
             retention_secs = excluded.retention_secs, \
             expires_at = excluded.expires_at, last_observed_at = excluded.last_observed_at \
             WHERE excluded.last_observed_at > provider_native_cache_observations.last_observed_at",
        )
        .bind(record.upstream_id.to_string())
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
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn list_active_provider_observations(
        &self,
        upstream_id: Uuid,
        provider: &str,
        canonical_model_id: &str,
        mode: &str,
        isolation_scope: &str,
        identity_hash: &str,
        not_expired_at_unix_secs: u64,
    ) -> StorageResult<Vec<ProviderPromptCacheObservationRecord>> {
        let rows = sqlx::query(
            "SELECT upstream_id, provider, canonical_model_id, mode, isolation_scope, identity_hash, boundary, evidence_source, observed_cached_tokens, observed_write_tokens, retention_secs, expires_at, last_observed_at \
             FROM provider_native_cache_observations WHERE upstream_id = ? AND provider = ? AND canonical_model_id = ? AND mode = ? AND isolation_scope = ? AND identity_hash = ? \
             AND expires_at IS NOT NULL AND expires_at > ? ORDER BY boundary",
        )
        .bind(upstream_id.to_string())
        .bind(provider)
        .bind(canonical_model_id)
        .bind(mode)
        .bind(isolation_scope)
        .bind(identity_hash)
        .bind(u64_to_i64(not_expired_at_unix_secs, "native lookup cutoff")?)
        .fetch_all(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        rows.into_iter()
            .map(|row| {
                Ok(ProviderPromptCacheObservationRecord {
                    upstream_id: uuid_from_db(
                        &row.try_get::<String, _>("upstream_id")
                            .map_err(map_sqlx_error)?,
                        "native upstream_id",
                    )?,
                    provider: row.try_get("provider").map_err(map_sqlx_error)?,
                    canonical_model_id: row
                        .try_get("canonical_model_id")
                        .map_err(map_sqlx_error)?,
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

    async fn list_active_provider_observations_for_candidates(
        &self,
        upstream_ids: &[Uuid],
        provider: &str,
        canonical_model_id: &str,
        mode: &str,
        isolation_scope: &str,
        identity_hash: &str,
        required_retention_secs: Option<u64>,
        not_expired_at_unix_secs: u64,
    ) -> StorageResult<Vec<ProviderPromptCacheObservationRecord>> {
        if upstream_ids.is_empty() || identity_hash.is_empty() {
            return Ok(Vec::new());
        }
        if upstream_ids.len() > MAX_PROVIDER_NATIVE_CANDIDATES {
            return Err(StorageError::InvalidInput {
                field: "upstream_ids".to_owned(),
                reason: format!(
                    "provider native candidate lookup supports at most {MAX_PROVIDER_NATIVE_CANDIDATES} upstreams"
                ),
            });
        }
        let rows = sqlx::query(
            "SELECT upstream_id, provider, canonical_model_id, mode, isolation_scope, identity_hash, boundary, evidence_source, observed_cached_tokens, observed_write_tokens, retention_secs, expires_at, last_observed_at \
             FROM provider_native_cache_observations \
             WHERE upstream_id IN (SELECT CAST(value AS TEXT) FROM json_each(?1)) \
               AND provider = ?2 \
               AND canonical_model_id = ?3 \
               AND mode = ?4 \
               AND isolation_scope = ?5 \
               AND identity_hash = ?6 \
               AND retention_secs IS ?7 \
               AND expires_at IS NOT NULL \
               AND expires_at > ?8 \
             ORDER BY upstream_id, boundary",
        )
        .bind(serde_json::to_string(upstream_ids)?)
        .bind(provider)
        .bind(canonical_model_id)
        .bind(mode)
        .bind(isolation_scope)
        .bind(identity_hash)
        .bind(
            required_retention_secs
                .map(|value| u64_to_i64(value, "native required retention"))
                .transpose()?,
        )
        .bind(u64_to_i64(
            not_expired_at_unix_secs,
            "native lookup cutoff",
        )?)
        .fetch_all(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        rows.into_iter()
            .map(|row| {
                Ok(ProviderPromptCacheObservationRecord {
                    upstream_id: uuid_from_db(
                        &row.try_get::<String, _>("upstream_id")
                            .map_err(map_sqlx_error)?,
                        "native upstream_id",
                    )?,
                    provider: row.try_get("provider").map_err(map_sqlx_error)?,
                    canonical_model_id: row
                        .try_get("canonical_model_id")
                        .map_err(map_sqlx_error)?,
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

            let provider_removed = sqlx::query(
                "DELETE FROM provider_native_cache_observations \
                 WHERE expires_at < ? \
                   AND rowid IN ( \
                       SELECT rowid FROM provider_native_cache_observations \
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
            total_removed = total_removed.saturating_add(provider_removed);
            if removed < PURGE_BATCH_SIZE as u64 && provider_removed < PURGE_BATCH_SIZE as u64 {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cc_lb_clock::SystemClock;
    use cc_lb_storage_api::{MetaStore, PromptCacheObservationStore};

    use super::*;

    #[tokio::test]
    async fn provider_candidate_lookup_requires_exact_scope_identity_and_retention() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let database_url = format!(
            "sqlite://{}",
            temp_dir
                .path()
                .join("provider-observations.sqlite")
                .display()
        );
        let storage = crate::open_sqlite(&database_url, Arc::new(SystemClock))
            .await
            .expect("open sqlite");
        storage.initialize().await.expect("initialize sqlite");

        let candidate_a = Uuid::from_u128(1);
        let candidate_b = Uuid::from_u128(2);
        let base = |upstream_id: Uuid, boundary: &str| ProviderPromptCacheObservationRecord {
            upstream_id,
            provider: "openai".to_owned(),
            canonical_model_id: "gpt-5.6".to_owned(),
            mode: "implicit".to_owned(),
            isolation_scope: "org:one;region:us-east".to_owned(),
            identity_hash: "sha256:key".to_owned(),
            boundary: boundary.to_owned(),
            evidence_source: "provider".to_owned(),
            observed_cached_tokens: Some(128),
            observed_write_tokens: None,
            retention_secs: Some(1_800),
            expires_at_unix_secs: Some(500),
            last_observed_at_unix_secs: 100,
        };

        let matching_a = base(candidate_a, "z");
        let matching_b = base(candidate_b, "a");
        storage
            .upsert_provider_observation(&matching_a)
            .await
            .expect("upsert matching candidate a");
        storage
            .upsert_provider_observation(&matching_b)
            .await
            .expect("upsert matching candidate b");

        for (provider, model, mode, scope, identity, retention, boundary) in [
            (
                "azure",
                "gpt-5.6",
                "implicit",
                "org:one;region:us-east",
                "sha256:key",
                Some(1_800),
                "provider-mismatch",
            ),
            (
                "openai",
                "gpt-5.5",
                "implicit",
                "org:one;region:us-east",
                "sha256:key",
                Some(1_800),
                "model-mismatch",
            ),
            (
                "openai",
                "gpt-5.6",
                "explicit",
                "org:one;region:us-east",
                "sha256:key",
                Some(1_800),
                "mode-mismatch",
            ),
            (
                "openai",
                "gpt-5.6",
                "implicit",
                "org:two;region:us-east",
                "sha256:key",
                Some(1_800),
                "scope-mismatch",
            ),
            (
                "openai",
                "gpt-5.6",
                "implicit",
                "org:one;region:us-east",
                "sha256:other",
                Some(1_800),
                "identity-mismatch",
            ),
            (
                "openai",
                "gpt-5.6",
                "implicit",
                "org:one;region:us-east",
                "sha256:key",
                Some(3_600),
                "retention-mismatch",
            ),
        ] {
            let mut record = base(candidate_a, boundary);
            record.provider = provider.to_owned();
            record.canonical_model_id = model.to_owned();
            record.mode = mode.to_owned();
            record.isolation_scope = scope.to_owned();
            record.identity_hash = identity.to_owned();
            record.retention_secs = retention;
            storage
                .upsert_provider_observation(&record)
                .await
                .expect("upsert mismatch");
        }

        let active = storage
            .list_active_provider_observations_for_candidates(
                &[candidate_b, candidate_a],
                "openai",
                "gpt-5.6",
                "implicit",
                "org:one;region:us-east",
                "sha256:key",
                Some(1_800),
                200,
            )
            .await
            .expect("lookup candidates");
        assert_eq!(active, vec![matching_a, matching_b]);

        assert!(
            storage
                .list_active_provider_observations_for_candidates(
                    &[],
                    "openai",
                    "gpt-5.6",
                    "implicit",
                    "org:one;region:us-east",
                    "sha256:key",
                    Some(1_800),
                    200,
                )
                .await
                .expect("empty candidates")
                .is_empty()
        );
        assert!(
            storage
                .list_active_provider_observations_for_candidates(
                    &[candidate_a],
                    "openai",
                    "gpt-5.6",
                    "implicit",
                    "org:one;region:us-east",
                    "",
                    Some(1_800),
                    200,
                )
                .await
                .expect("empty identity")
                .is_empty()
        );
    }
}
