use async_trait::async_trait;
use cc_lb_storage_api::{
    AnthropicCompatibilityKvStore, CompatibilityKvRecord, StorageError, StorageResult,
};
use serde::{Deserialize, Serialize};
use sqlx::Row;

use crate::{SqliteStorage, map_sqlx_error};

#[async_trait]
impl AnthropicCompatibilityKvStore for SqliteStorage {
    async fn put_compatibility_kv_value(
        &self,
        key: &str,
        value: &str,
        observed_at_unix_secs: u64,
        source_url: Option<&str>,
    ) -> StorageResult<()> {
        u64_to_i64(
            observed_at_unix_secs,
            "anthropic compatibility kv observed_at_unix_secs",
        )?;
        let record = CompatibilityKvRecord {
            key: key.to_owned(),
            value: value.to_owned(),
            last_updated_at_unix_secs: observed_at_unix_secs,
            last_attempt_at_unix_secs: observed_at_unix_secs,
            last_error: None,
            source_url: source_url.map(str::to_owned),
        };
        put_record(self, &record).await
    }

    async fn put_compatibility_kv_failure(
        &self,
        key: &str,
        attempted_at_unix_secs: u64,
        error: &str,
    ) -> StorageResult<()> {
        u64_to_i64(
            attempted_at_unix_secs,
            "anthropic compatibility kv attempted_at_unix_secs",
        )?;
        let Some(mut record) = self.get_compatibility_kv(key).await? else {
            return Ok(());
        };
        record.last_attempt_at_unix_secs = attempted_at_unix_secs;
        record.last_error = Some(error.to_owned());
        put_record(self, &record).await
    }

    async fn get_compatibility_kv(
        &self,
        key: &str,
    ) -> StorageResult<Option<CompatibilityKvRecord>> {
        let row = sqlx::query("SELECT key, value FROM anthropic_compatibility_kv_v1 WHERE key = ?")
            .bind(key)
            .fetch_optional(self.pool())
            .await
            .map_err(map_sqlx_error)?;

        row.map(row_to_record).transpose()
    }

    async fn list_compatibility_kv(&self) -> StorageResult<Vec<CompatibilityKvRecord>> {
        let rows =
            sqlx::query("SELECT key, value FROM anthropic_compatibility_kv_v1 ORDER BY key ASC")
                .fetch_all(self.pool())
                .await
                .map_err(map_sqlx_error)?;

        rows.into_iter().map(row_to_record).collect()
    }
}

async fn put_record(storage: &SqliteStorage, record: &CompatibilityKvRecord) -> StorageResult<()> {
    let payload = serde_json::to_string(&CompatibilityKvPayload::from(record))?;
    sqlx::query(
        "INSERT INTO anthropic_compatibility_kv_v1 (key, value) VALUES (?, ?) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
    )
    .bind(&record.key)
    .bind(payload)
    .execute(storage.pool())
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

fn row_to_record(row: sqlx::sqlite::SqliteRow) -> StorageResult<CompatibilityKvRecord> {
    let key: String = row.try_get("key").map_err(map_sqlx_error)?;
    let payload: String = row.try_get("value").map_err(map_sqlx_error)?;
    let payload: CompatibilityKvPayload = serde_json::from_str(&payload)?;
    Ok(CompatibilityKvRecord {
        key,
        value: payload.value,
        last_updated_at_unix_secs: payload.last_updated_at_unix_secs,
        last_attempt_at_unix_secs: payload.last_attempt_at_unix_secs,
        last_error: payload.last_error,
        source_url: payload.source_url,
    })
}

#[derive(Debug, Serialize, Deserialize)]
struct CompatibilityKvPayload {
    value: String,
    last_updated_at_unix_secs: u64,
    last_attempt_at_unix_secs: u64,
    last_error: Option<String>,
    source_url: Option<String>,
}

impl From<&CompatibilityKvRecord> for CompatibilityKvPayload {
    fn from(record: &CompatibilityKvRecord) -> Self {
        Self {
            value: record.value.clone(),
            last_updated_at_unix_secs: record.last_updated_at_unix_secs,
            last_attempt_at_unix_secs: record.last_attempt_at_unix_secs,
            last_error: record.last_error.clone(),
            source_url: record.source_url.clone(),
        }
    }
}

fn u64_to_i64(value: u64, field: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| StorageError::InvalidInput {
        field: field.to_owned(),
        reason: "value exceeds i64::MAX".to_owned(),
    })
}
