use async_trait::async_trait;
use cc_lb_storage_api::{AnthropicCompatibilityKvStore, CompatibilityKvRecord, StorageResult};
use sqlx::{Row, postgres::PgRow};

use crate::{
    adapter::{PostgresStorage, i64_to_u64, u64_to_i64},
    error_map::map_sqlx_error,
};

#[async_trait]
impl AnthropicCompatibilityKvStore for PostgresStorage {
    async fn put_compatibility_kv_value(
        &self,
        key: &str,
        value: &str,
        observed_at_unix_secs: u64,
        source_url: Option<&str>,
    ) -> StorageResult<()> {
        sqlx::query(
            "INSERT INTO anthropic_compatibility_kv_v1 \
             (key, value, last_updated_at_unix_secs, last_attempt_at_unix_secs, last_error, source_url) \
             VALUES ($1, $2, $3, $3, NULL, $4) \
             ON CONFLICT (key) DO UPDATE SET \
             value = EXCLUDED.value, \
             last_updated_at_unix_secs = EXCLUDED.last_updated_at_unix_secs, \
             last_attempt_at_unix_secs = EXCLUDED.last_attempt_at_unix_secs, \
             last_error = NULL, \
             source_url = EXCLUDED.source_url",
        )
        .bind(key)
        .bind(value)
        .bind(u64_to_i64(
            observed_at_unix_secs,
            "anthropic compatibility kv observed_at_unix_secs",
        )?)
        .bind(source_url)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn put_compatibility_kv_failure(
        &self,
        key: &str,
        attempted_at_unix_secs: u64,
        error: &str,
    ) -> StorageResult<()> {
        sqlx::query(
            "UPDATE anthropic_compatibility_kv_v1 \
             SET last_attempt_at_unix_secs = $2, last_error = $3 \
             WHERE key = $1",
        )
        .bind(key)
        .bind(u64_to_i64(
            attempted_at_unix_secs,
            "anthropic compatibility kv attempted_at_unix_secs",
        )?)
        .bind(error)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn get_compatibility_kv(
        &self,
        key: &str,
    ) -> StorageResult<Option<CompatibilityKvRecord>> {
        let row = sqlx::query(
            "SELECT key, value, last_updated_at_unix_secs, last_attempt_at_unix_secs, last_error, source_url \
             FROM anthropic_compatibility_kv_v1 WHERE key = $1",
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        row.map(row_to_record).transpose()
    }

    async fn list_compatibility_kv(&self) -> StorageResult<Vec<CompatibilityKvRecord>> {
        let rows = sqlx::query(
            "SELECT key, value, last_updated_at_unix_secs, last_attempt_at_unix_secs, last_error, source_url \
             FROM anthropic_compatibility_kv_v1 ORDER BY key ASC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        rows.into_iter().map(row_to_record).collect()
    }
}

fn row_to_record(row: PgRow) -> StorageResult<CompatibilityKvRecord> {
    Ok(CompatibilityKvRecord {
        key: row.try_get("key").map_err(map_sqlx_error)?,
        value: row.try_get("value").map_err(map_sqlx_error)?,
        last_updated_at_unix_secs: i64_to_u64(
            row.try_get("last_updated_at_unix_secs")
                .map_err(map_sqlx_error)?,
            "anthropic compatibility kv last_updated_at_unix_secs",
        )?,
        last_attempt_at_unix_secs: i64_to_u64(
            row.try_get("last_attempt_at_unix_secs")
                .map_err(map_sqlx_error)?,
            "anthropic compatibility kv last_attempt_at_unix_secs",
        )?,
        last_error: row.try_get("last_error").map_err(map_sqlx_error)?,
        source_url: row.try_get("source_url").map_err(map_sqlx_error)?,
    })
}
