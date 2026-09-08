use std::{
    collections::{HashMap, hash_map::Entry},
    time::Instant,
};

use async_trait::async_trait;
use cc_lb_storage_api::{
    StorageError, StorageResult, UpstreamAffinityBinding, UpstreamAffinityKey,
    UpstreamAffinityKind, UpstreamAffinityStore,
};
use sqlx::{QueryBuilder, Row, Sqlite};

use crate::{SqliteStorage, map_sqlx_error};

// Each row uses seven bind parameters. 128 rows plus the two policy parameters
// stay below SQLite's historical 999-variable limit while amortizing the
// transaction's statement awaits.
const BINDINGS_PER_QUERY: usize = 128;
const RESOLVE_KEYS_PER_QUERY: usize = 200;

#[async_trait]
impl UpstreamAffinityStore for SqliteStorage {
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
                let mut query = QueryBuilder::<Sqlite>::new(
                    "SELECT principal_id, provider, kind, value_sha256, upstream_id, observed_at_unix_secs, expires_at_unix_secs \
                     FROM upstream_affinity_v1 \
                     WHERE (expires_at_unix_secs IS NULL OR expires_at_unix_secs > ",
                );
                query
                    .push_bind(now)
                    .push(") AND (")
                    .push_bind(cutoff)
                    .push(" IS NULL OR observed_at_unix_secs > ")
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
                    .fetch_all(self.pool())
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
            let mut tx = self.begin_immediate().await?;
            for chunk in bindings.chunks(BINDINGS_PER_QUERY) {
                let mut query = QueryBuilder::<Sqlite>::new(
                    "WITH policy(now_unix_secs, cutoff_unix_secs) AS (VALUES (",
                );
                query
                    .push_bind(now)
                    .push(", ")
                    .push_bind(cutoff)
                    .push(
                        ")) \
                         INSERT INTO upstream_affinity_v1 \
                         (principal_id, provider, kind, value_sha256, upstream_id, observed_at_unix_secs, expires_at_unix_secs) ",
                    );
                query.push_values(chunk, |mut values, binding| {
                    values
                        .push_bind(&binding.key.principal_id)
                        .push_bind(&binding.key.provider)
                        .push_bind(binding.key.kind.as_str())
                        .push_bind(binding.key.value_sha256.as_slice())
                        .push_bind(binding.upstream_id.to_string())
                        .push_bind(binding.observed_at_unix_secs)
                        .push_bind(binding.expires_at_unix_secs);
                });
                query.push(
                    " ON CONFLICT(principal_id, provider, kind, value_sha256) DO UPDATE SET \
                       upstream_id = CASE WHEN \
                         (upstream_affinity_v1.expires_at_unix_secs IS NOT NULL AND upstream_affinity_v1.expires_at_unix_secs <= (SELECT now_unix_secs FROM policy)) \
                         OR ((SELECT cutoff_unix_secs FROM policy) IS NOT NULL AND upstream_affinity_v1.observed_at_unix_secs <= (SELECT cutoff_unix_secs FROM policy)) \
                         THEN excluded.upstream_id ELSE upstream_affinity_v1.upstream_id END, \
                       observed_at_unix_secs = CASE WHEN \
                         (upstream_affinity_v1.expires_at_unix_secs IS NOT NULL AND upstream_affinity_v1.expires_at_unix_secs <= (SELECT now_unix_secs FROM policy)) \
                         OR ((SELECT cutoff_unix_secs FROM policy) IS NOT NULL AND upstream_affinity_v1.observed_at_unix_secs <= (SELECT cutoff_unix_secs FROM policy)) \
                         THEN excluded.observed_at_unix_secs \
                         ELSE MAX(upstream_affinity_v1.observed_at_unix_secs, excluded.observed_at_unix_secs) END, \
                       expires_at_unix_secs = CASE WHEN \
                         (upstream_affinity_v1.expires_at_unix_secs IS NOT NULL AND upstream_affinity_v1.expires_at_unix_secs <= (SELECT now_unix_secs FROM policy)) \
                         OR ((SELECT cutoff_unix_secs FROM policy) IS NOT NULL AND upstream_affinity_v1.observed_at_unix_secs <= (SELECT cutoff_unix_secs FROM policy)) \
                         THEN excluded.expires_at_unix_secs \
                         WHEN upstream_affinity_v1.expires_at_unix_secs IS NULL OR excluded.expires_at_unix_secs IS NULL THEN NULL \
                         ELSE MAX(upstream_affinity_v1.expires_at_unix_secs, excluded.expires_at_unix_secs) END \
                     WHERE upstream_affinity_v1.upstream_id = excluded.upstream_id \
                        OR (upstream_affinity_v1.expires_at_unix_secs IS NOT NULL AND upstream_affinity_v1.expires_at_unix_secs <= (SELECT now_unix_secs FROM policy)) \
                        OR ((SELECT cutoff_unix_secs FROM policy) IS NOT NULL AND upstream_affinity_v1.observed_at_unix_secs <= (SELECT cutoff_unix_secs FROM policy))",
                );

                let result = query
                    .build()
                    .persistent(chunk.len() == BINDINGS_PER_QUERY || chunk.len() == 1)
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
            let mut tx = self.begin_immediate().await?;

            let mut deleted = sqlx::query(
                "DELETE FROM upstream_affinity_v1 \
                 WHERE rowid IN ( \
                   SELECT rowid FROM upstream_affinity_v1 INDEXED BY upstream_affinity_v1_expires_at_idx \
                   WHERE expires_at_unix_secs IS NOT NULL AND expires_at_unix_secs <= ? \
                   ORDER BY expires_at_unix_secs LIMIT ? \
                 ) \
                 AND expires_at_unix_secs IS NOT NULL AND expires_at_unix_secs <= ?",
            )
            .bind(now)
            .bind(limit)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?
            .rows_affected();

            if let Some(cutoff) = cutoff {
                let remaining = batch_size.saturating_sub(deleted as usize);
                if remaining > 0 {
                    deleted += sqlx::query(
                        "DELETE FROM upstream_affinity_v1 \
                         WHERE rowid IN ( \
                           SELECT rowid FROM upstream_affinity_v1 INDEXED BY upstream_affinity_v1_observed_at_idx \
                           WHERE observed_at_unix_secs <= ? \
                           ORDER BY observed_at_unix_secs LIMIT ? \
                         ) \
                         AND observed_at_unix_secs <= ?",
                    )
                    .bind(cutoff)
                    .bind(i64::try_from(remaining).unwrap_or(i64::MAX))
                    .bind(cutoff)
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
        "store" => "sqlite",
        "operation" => operation,
        "status" => status
    )
    .record(start.elapsed().as_secs_f64());
    if result.is_err() {
        metrics::counter!(
            "cc_lb_storage_operation_errors_total",
            "store" => "sqlite",
            "operation" => operation
        )
        .increment(1);
    }
}

fn record_affinity_batch_keys(operation: &'static str, count: usize) {
    metrics::histogram!(
        "cc_lb_upstream_affinity_batch_keys",
        "store" => "sqlite",
        "operation" => operation
    )
    .record(count as f64);
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
        let expires_at_unix_secs = option_u64_to_i64(
            binding.expires_at_unix_secs,
            "upstream affinity expires_at_unix_secs",
        )?;

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

fn row_to_binding(row: sqlx::sqlite::SqliteRow) -> StorageResult<UpstreamAffinityBinding> {
    let digest: Vec<u8> = row.try_get("value_sha256").map_err(map_sqlx_error)?;
    let upstream_id: String = row.try_get("upstream_id").map_err(map_sqlx_error)?;
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
        upstream_id: uuid::Uuid::parse_str(&upstream_id).map_err(|error| {
            StorageError::Corrupted {
                message: format!("invalid upstream affinity upstream_id: {error}"),
            }
        })?,
        observed_at_unix_secs: i64_to_u64(
            row.try_get("observed_at_unix_secs")
                .map_err(map_sqlx_error)?,
            "upstream affinity observed_at_unix_secs",
        )?,
        expires_at_unix_secs: option_i64_to_u64(
            row.try_get("expires_at_unix_secs")
                .map_err(map_sqlx_error)?,
            "upstream affinity expires_at_unix_secs",
        )?,
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

fn retention_cutoff(now_unix_secs: u64, ttl_secs: u64) -> Option<u64> {
    now_unix_secs.checked_sub(ttl_secs)
}

fn u64_to_i64(value: u64, field: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| StorageError::InvalidInput {
        field: field.to_owned(),
        reason: "value exceeds i64::MAX".to_owned(),
    })
}

fn option_u64_to_i64(value: Option<u64>, field: &str) -> StorageResult<Option<i64>> {
    value.map(|value| u64_to_i64(value, field)).transpose()
}

fn i64_to_u64(value: i64, field: &str) -> StorageResult<u64> {
    u64::try_from(value).map_err(|_| StorageError::Corrupted {
        message: format!("negative {field} value {value}"),
    })
}

fn option_i64_to_u64(value: Option<i64>, field: &str) -> StorageResult<Option<u64>> {
    value.map(|value| i64_to_u64(value, field)).transpose()
}
