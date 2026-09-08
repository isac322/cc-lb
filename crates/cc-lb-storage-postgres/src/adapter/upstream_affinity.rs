use std::{
    collections::{HashMap, hash_map::Entry},
    time::Instant,
};

use async_trait::async_trait;
use cc_lb_storage_api::{
    StorageError, StorageResult, UpstreamAffinityBinding, UpstreamAffinityKey,
    UpstreamAffinityKind, UpstreamAffinityStore,
};
use sqlx::{Postgres, QueryBuilder, Row};

use crate::{
    adapter::{PostgresStorage, i64_to_u64, u64_to_i64},
    error_map::map_sqlx_error,
};

// Bound each transaction statement's row and array-encoding work.
const BINDINGS_PER_QUERY: usize = 128;
const RESOLVE_KEYS_PER_QUERY: usize = 1_000;

const BIND_UPSTREAM_AFFINITIES_SQL: &str = "\
    INSERT INTO upstream_affinity_v1 \
      (principal_id, provider, kind, value_sha256, upstream_id, observed_at_unix_secs, expires_at_unix_secs) \
    SELECT principal_id, provider, kind, value_sha256, upstream_id, observed_at_unix_secs, expires_at_unix_secs \
    FROM UNNEST( \
      $1::text[], $2::text[], $3::text[], $4::bytea[], $5::uuid[], $6::bigint[], $7::bigint[] \
    ) AS input(principal_id, provider, kind, value_sha256, upstream_id, observed_at_unix_secs, expires_at_unix_secs) \
    ON CONFLICT(principal_id, provider, kind, value_sha256) DO UPDATE SET \
      upstream_id = CASE WHEN \
        (upstream_affinity_v1.expires_at_unix_secs IS NOT NULL AND upstream_affinity_v1.expires_at_unix_secs <= $8) \
        OR ($9::bigint IS NOT NULL AND upstream_affinity_v1.observed_at_unix_secs <= $9) \
        THEN EXCLUDED.upstream_id ELSE upstream_affinity_v1.upstream_id END, \
      observed_at_unix_secs = CASE WHEN \
        (upstream_affinity_v1.expires_at_unix_secs IS NOT NULL AND upstream_affinity_v1.expires_at_unix_secs <= $8) \
        OR ($9::bigint IS NOT NULL AND upstream_affinity_v1.observed_at_unix_secs <= $9) \
        THEN EXCLUDED.observed_at_unix_secs \
        ELSE GREATEST(upstream_affinity_v1.observed_at_unix_secs, EXCLUDED.observed_at_unix_secs) END, \
      expires_at_unix_secs = CASE WHEN \
        (upstream_affinity_v1.expires_at_unix_secs IS NOT NULL AND upstream_affinity_v1.expires_at_unix_secs <= $8) \
        OR ($9::bigint IS NOT NULL AND upstream_affinity_v1.observed_at_unix_secs <= $9) \
        THEN EXCLUDED.expires_at_unix_secs \
        WHEN upstream_affinity_v1.expires_at_unix_secs IS NULL OR EXCLUDED.expires_at_unix_secs IS NULL THEN NULL \
        ELSE GREATEST(upstream_affinity_v1.expires_at_unix_secs, EXCLUDED.expires_at_unix_secs) END \
    WHERE upstream_affinity_v1.upstream_id = EXCLUDED.upstream_id \
       OR (upstream_affinity_v1.expires_at_unix_secs IS NOT NULL AND upstream_affinity_v1.expires_at_unix_secs <= $8) \
       OR ($9::bigint IS NOT NULL AND upstream_affinity_v1.observed_at_unix_secs <= $9)";

#[async_trait]
impl UpstreamAffinityStore for PostgresStorage {
    async fn resolve_upstream_affinities(
        &self,
        keys: &[UpstreamAffinityKey],
        now_unix_secs: u64,
        ttl_secs: u64,
    ) -> StorageResult<Vec<UpstreamAffinityBinding>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let start = Instant::now();
        let result = async {
            record_affinity_batch_keys("upstream_affinity_resolve", keys.len());

            let now = u64_to_i64(now_unix_secs, "upstream affinity resolve time")?;
            let cutoff = retention_cutoff(now_unix_secs, ttl_secs)
                .map(|value| u64_to_i64(value, "upstream affinity retention cutoff"))
                .transpose()?;
            let mut bindings = Vec::new();
            for chunk in keys.chunks(RESOLVE_KEYS_PER_QUERY) {
                let mut query = QueryBuilder::<Postgres>::new(
                    "SELECT principal_id, provider, kind, value_sha256, upstream_id, observed_at_unix_secs, expires_at_unix_secs \
                     FROM upstream_affinity_v1 \
                     WHERE (expires_at_unix_secs IS NULL OR expires_at_unix_secs > ",
                );
                query
                    .push_bind(now)
                    .push(") AND (")
                    .push_bind(cutoff)
                    .push("::bigint IS NULL OR observed_at_unix_secs > ")
                    .push_bind(cutoff)
                    .push(") AND (");
                for (index, key) in chunk.iter().enumerate() {
                    if index > 0 {
                        query.push(" OR ");
                    }
                    query
                        .push("(principal_id = ")
                        .push_bind(&key.principal_id)
                        .push(" AND provider = ")
                        .push_bind(&key.provider)
                        .push(" AND kind = ")
                        .push_bind(key.kind.as_str())
                        .push(" AND value_sha256 = ")
                        .push_bind(key.value_sha256.as_slice())
                        .push(")");
                }
                query.push(") ORDER BY principal_id, provider, kind, value_sha256");

                let rows = query
                    .build()
                    .fetch_all(&self.pool)
                    .await
                    .map_err(map_sqlx_error)?;
                bindings.reserve(rows.len());
                for row in rows {
                    bindings.push(row_to_binding(row)?);
                }
            }
            Ok(bindings)
        }
        .await;
        record_storage_operation("upstream_affinity_resolve", start, &result);
        result
    }

    async fn bind_upstream_affinities(
        &self,
        bindings: &[UpstreamAffinityBinding],
        now_unix_secs: u64,
        ttl_secs: u64,
    ) -> StorageResult<()> {
        if bindings.is_empty() {
            return Ok(());
        }

        let start = Instant::now();
        let result = async {
            record_affinity_batch_keys("upstream_affinity_bind", bindings.len());

            let bindings = prepare_bindings(bindings)?;
            let now = u64_to_i64(now_unix_secs, "upstream affinity bind time")?;
            let cutoff = retention_cutoff(now_unix_secs, ttl_secs)
                .map(|value| u64_to_i64(value, "upstream affinity retention cutoff"))
                .transpose()?;
            let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
            let chunk_capacity = bindings.len().min(BINDINGS_PER_QUERY);
            let mut principal_ids = Vec::<&str>::with_capacity(chunk_capacity);
            let mut providers = Vec::<&str>::with_capacity(chunk_capacity);
            let mut kinds = Vec::<&str>::with_capacity(chunk_capacity);
            let mut value_sha256s = Vec::<&[u8]>::with_capacity(chunk_capacity);
            let mut upstream_ids = Vec::with_capacity(chunk_capacity);
            let mut observed_at_unix_secs = Vec::with_capacity(chunk_capacity);
            let mut expires_at_unix_secs = Vec::with_capacity(chunk_capacity);

            for chunk in bindings.chunks(BINDINGS_PER_QUERY) {
                principal_ids.clear();
                providers.clear();
                kinds.clear();
                value_sha256s.clear();
                upstream_ids.clear();
                observed_at_unix_secs.clear();
                expires_at_unix_secs.clear();
                for binding in chunk {
                    principal_ids.push(binding.key.principal_id.as_str());
                    providers.push(binding.key.provider.as_str());
                    kinds.push(binding.key.kind.as_str());
                    value_sha256s.push(binding.key.value_sha256.as_slice());
                    upstream_ids.push(binding.upstream_id);
                    observed_at_unix_secs.push(binding.observed_at_unix_secs);
                    expires_at_unix_secs.push(binding.expires_at_unix_secs);
                }

                let result = sqlx::query(BIND_UPSTREAM_AFFINITIES_SQL)
                    .bind(&principal_ids)
                    .bind(&providers)
                    .bind(&kinds)
                    .bind(&value_sha256s)
                    .bind(&upstream_ids)
                    .bind(&observed_at_unix_secs)
                    .bind(&expires_at_unix_secs)
                    .bind(now)
                    .bind(cutoff)
                    .execute(&mut *tx)
                    .await
                    .map_err(map_sqlx_error)?;
                if result.rows_affected() != chunk.len() as u64 {
                    tx.rollback().await.map_err(map_sqlx_error)?;
                    return Err(StorageError::Conflict {
                        message: "upstream affinity is already bound to a different upstream"
                            .to_owned(),
                    });
                }
            }
            tx.commit().await.map_err(map_sqlx_error)
        }
        .await;
        record_storage_operation("upstream_affinity_bind", start, &result);
        result
    }

    async fn purge_expired_upstream_affinities(
        &self,
        now_unix_secs: u64,
        ttl_secs: u64,
        batch_size: usize,
    ) -> StorageResult<u64> {
        if batch_size == 0 {
            return Ok(0);
        }

        let start = Instant::now();
        let result = async {
            let now = u64_to_i64(now_unix_secs, "upstream affinity purge time")?;
            let limit = i64::try_from(batch_size).unwrap_or(i64::MAX);
            let cutoff = retention_cutoff(now_unix_secs, ttl_secs)
                .map(|value| u64_to_i64(value, "upstream affinity retention cutoff"))
                .transpose()?;
            let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;

            let mut deleted = sqlx::query(
                "WITH candidates AS ( \
                   SELECT ctid \
                   FROM upstream_affinity_v1 \
                   WHERE expires_at_unix_secs IS NOT NULL AND expires_at_unix_secs <= $1 \
                   ORDER BY expires_at_unix_secs \
                   LIMIT $2 FOR UPDATE SKIP LOCKED \
                 ) \
                 DELETE FROM upstream_affinity_v1 AS target \
                 WHERE target.ctid = ANY(ARRAY(SELECT ctid FROM candidates)) \
                   AND target.expires_at_unix_secs IS NOT NULL \
                   AND target.expires_at_unix_secs <= $1",
            )
            .bind(now)
            .bind(limit)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?
            .rows_affected();

            if let Some(cutoff) = cutoff {
                let remaining = batch_size.saturating_sub(deleted as usize);
                if remaining > 0 {
                    deleted += sqlx::query(
                        "WITH candidates AS ( \
                           SELECT ctid \
                           FROM upstream_affinity_v1 \
                           WHERE observed_at_unix_secs <= $1 \
                           ORDER BY observed_at_unix_secs \
                           LIMIT $2 FOR UPDATE SKIP LOCKED \
                         ) \
                         DELETE FROM upstream_affinity_v1 AS target \
                         WHERE target.ctid = ANY(ARRAY(SELECT ctid FROM candidates)) \
                           AND target.observed_at_unix_secs <= $1",
                    )
                    .bind(cutoff)
                    .bind(i64::try_from(remaining).unwrap_or(i64::MAX))
                    .execute(&mut *tx)
                    .await
                    .map_err(map_sqlx_error)?
                    .rows_affected();
                }
            }

            tx.commit().await.map_err(map_sqlx_error)?;
            Ok(deleted)
        }
        .await;
        record_storage_operation("upstream_affinity_purge", start, &result);
        result
    }
}

fn record_storage_operation<T>(operation: &'static str, start: Instant, result: &StorageResult<T>) {
    let status = if result.is_ok() { "ok" } else { "error" };
    metrics::histogram!(
        "cc_lb_storage_operation_duration_seconds",
        "store" => "postgres",
        "operation" => operation,
        "status" => status
    )
    .record(start.elapsed().as_secs_f64());
    if result.is_err() {
        metrics::counter!(
            "cc_lb_storage_operation_errors_total",
            "store" => "postgres",
            "operation" => operation
        )
        .increment(1);
    }
}

fn record_affinity_batch_keys(operation: &'static str, count: usize) {
    metrics::histogram!(
        "cc_lb_upstream_affinity_batch_keys",
        "store" => "postgres",
        "operation" => operation
    )
    .record(count as f64);
}

fn retention_cutoff(now_unix_secs: u64, ttl_secs: u64) -> Option<u64> {
    now_unix_secs.checked_sub(ttl_secs)
}

struct PreparedAffinityBinding<'a> {
    key: &'a UpstreamAffinityKey,
    upstream_id: uuid::Uuid,
    observed_at_unix_secs: i64,
    expires_at_unix_secs: Option<i64>,
}

fn prepare_bindings(
    bindings: &[UpstreamAffinityBinding],
) -> StorageResult<Vec<PreparedAffinityBinding<'_>>> {
    let mut prepared = Vec::<PreparedAffinityBinding<'_>>::with_capacity(bindings.len());
    let mut indexes = HashMap::<&UpstreamAffinityKey, usize>::with_capacity(bindings.len());

    for binding in bindings {
        let observed_at_unix_secs = u64_to_i64(
            binding.observed_at_unix_secs,
            "upstream affinity observed_at_unix_secs",
        )?;
        let expires_at_unix_secs = binding
            .expires_at_unix_secs
            .map(|value| u64_to_i64(value, "upstream affinity expires_at_unix_secs"))
            .transpose()?;

        match indexes.entry(&binding.key) {
            Entry::Vacant(entry) => {
                entry.insert(prepared.len());
                prepared.push(PreparedAffinityBinding {
                    key: &binding.key,
                    upstream_id: binding.upstream_id,
                    observed_at_unix_secs,
                    expires_at_unix_secs,
                });
            }
            Entry::Occupied(entry) => {
                let existing = &mut prepared[*entry.get()];
                if existing.upstream_id != binding.upstream_id {
                    return Err(StorageError::Conflict {
                        message: "upstream affinity is already bound to a different upstream"
                            .to_owned(),
                    });
                }
                existing.observed_at_unix_secs =
                    existing.observed_at_unix_secs.max(observed_at_unix_secs);
                existing.expires_at_unix_secs =
                    match (existing.expires_at_unix_secs, expires_at_unix_secs) {
                        (Some(existing), Some(incoming)) => Some(existing.max(incoming)),
                        _ => None,
                    };
            }
        }
    }

    prepared.sort_unstable_by(|left, right| {
        left.key
            .principal_id
            .cmp(&right.key.principal_id)
            .then_with(|| left.key.provider.cmp(&right.key.provider))
            .then_with(|| left.key.kind.as_str().cmp(right.key.kind.as_str()))
            .then_with(|| left.key.value_sha256.cmp(&right.key.value_sha256))
    });

    Ok(prepared)
}

fn row_to_binding(row: sqlx::postgres::PgRow) -> StorageResult<UpstreamAffinityBinding> {
    let digest: Vec<u8> = row.try_get("value_sha256").map_err(map_sqlx_error)?;
    Ok(UpstreamAffinityBinding {
        key: UpstreamAffinityKey {
            principal_id: row.try_get("principal_id").map_err(map_sqlx_error)?,
            provider: row.try_get("provider").map_err(map_sqlx_error)?,
            kind: affinity_kind_from_db(
                &row.try_get::<String, _>("kind").map_err(map_sqlx_error)?,
            )?,
            value_sha256: digest
                .try_into()
                .map_err(|value: Vec<u8>| StorageError::Corrupted {
                    message: format!(
                        "upstream affinity value_sha256 has {} bytes instead of 32",
                        value.len()
                    ),
                })?,
        },
        upstream_id: row.try_get("upstream_id").map_err(map_sqlx_error)?,
        observed_at_unix_secs: i64_to_u64(
            row.try_get("observed_at_unix_secs")
                .map_err(map_sqlx_error)?,
            "upstream affinity observed_at_unix_secs",
        )?,
        expires_at_unix_secs: row
            .try_get::<Option<i64>, _>("expires_at_unix_secs")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "upstream affinity expires_at_unix_secs"))
            .transpose()?,
    })
}

fn affinity_kind_from_db(value: &str) -> StorageResult<UpstreamAffinityKind> {
    match value {
        "anthropic_web_search_encrypted_content" => {
            Ok(UpstreamAffinityKind::AnthropicWebSearchEncryptedContent)
        }
        value => Err(StorageError::Corrupted {
            message: format!("invalid upstream affinity kind {value}"),
        }),
    }
}
