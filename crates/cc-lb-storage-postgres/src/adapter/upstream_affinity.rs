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

const RESOLVE_KEYS_PER_QUERY: usize = 1_000;

#[async_trait]
impl UpstreamAffinityStore for PostgresStorage {
    async fn resolve_upstream_affinities(
        &self,
        keys: &[UpstreamAffinityKey],
        now_unix_secs: u64,
    ) -> StorageResult<Vec<UpstreamAffinityBinding>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let now = u64_to_i64(now_unix_secs, "upstream affinity resolve time")?;
        let mut bindings = Vec::new();
        for chunk in keys.chunks(RESOLVE_KEYS_PER_QUERY) {
            let mut query = QueryBuilder::<Postgres>::new(
                "SELECT principal_id, provider, kind, value_sha256, upstream_id, observed_at_unix_secs, expires_at_unix_secs \
                 FROM upstream_affinity_v1 \
                 WHERE (expires_at_unix_secs IS NULL OR expires_at_unix_secs > ",
            );
            query.push_bind(now).push(") AND (");
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

    async fn bind_upstream_affinities(
        &self,
        bindings: &[UpstreamAffinityBinding],
    ) -> StorageResult<()> {
        if bindings.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        for binding in bindings {
            let stored_upstream_id: Option<uuid::Uuid> = sqlx::query_scalar(
                "INSERT INTO upstream_affinity_v1 \
                 (principal_id, provider, kind, value_sha256, upstream_id, observed_at_unix_secs, expires_at_unix_secs) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7) \
                 ON CONFLICT(principal_id, provider, kind, value_sha256) DO UPDATE SET \
                   observed_at_unix_secs = GREATEST(upstream_affinity_v1.observed_at_unix_secs, EXCLUDED.observed_at_unix_secs), \
                   expires_at_unix_secs = CASE \
                     WHEN upstream_affinity_v1.expires_at_unix_secs IS NULL OR EXCLUDED.expires_at_unix_secs IS NULL THEN NULL \
                     ELSE GREATEST(upstream_affinity_v1.expires_at_unix_secs, EXCLUDED.expires_at_unix_secs) \
                   END \
                 WHERE upstream_affinity_v1.upstream_id = EXCLUDED.upstream_id \
                 RETURNING upstream_id",
            )
            .bind(&binding.key.principal_id)
            .bind(&binding.key.provider)
            .bind(binding.key.kind.as_str())
            .bind(binding.key.value_sha256.as_slice())
            .bind(binding.upstream_id)
            .bind(u64_to_i64(
                binding.observed_at_unix_secs,
                "upstream affinity observed_at_unix_secs",
            )?)
            .bind(
                binding
                    .expires_at_unix_secs
                    .map(|value| {
                        u64_to_i64(value, "upstream affinity expires_at_unix_secs")
                    })
                    .transpose()?,
            )
            .fetch_optional(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;

            if stored_upstream_id.is_none() {
                let existing_upstream_id: Option<uuid::Uuid> = sqlx::query_scalar(
                    "SELECT upstream_id FROM upstream_affinity_v1 \
                     WHERE principal_id = $1 AND provider = $2 AND kind = $3 AND value_sha256 = $4 \
                     FOR UPDATE",
                )
                .bind(&binding.key.principal_id)
                .bind(&binding.key.provider)
                .bind(binding.key.kind.as_str())
                .bind(binding.key.value_sha256.as_slice())
                .fetch_optional(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
                tx.rollback().await.map_err(map_sqlx_error)?;
                return match existing_upstream_id {
                    Some(_) => Err(StorageError::Conflict {
                        message: "upstream affinity is already bound to a different upstream"
                            .to_owned(),
                    }),
                    None => Err(StorageError::Fatal {
                        message: "upstream affinity conflict row disappeared".to_owned(),
                    }),
                };
            }
        }
        tx.commit().await.map_err(map_sqlx_error)
    }
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
