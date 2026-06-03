use async_trait::async_trait;
use cc_lb_storage_api::{AnthropicCompatibilityKvStore, CompatibilityKvRecord, StorageResult};
use redb::{ReadableDatabase, ReadableTable};
use serde::{Deserialize, Serialize};

use crate::adapter::error_map::{map_join_err, map_redb_err};
use crate::{ANTHROPIC_COMPATIBILITY_KV_V1, RedbStorage, StorageError};

/// Stored row layout. `key` is the redb table primary key so it is not
/// duplicated in the value blob; reads hydrate it from the table key
/// path. This keeps the on-disk shape compatible with prior PR #51
/// schema variants that did not carry `key` in the payload.
#[derive(Serialize, Deserialize)]
struct StoredCompatRow {
    value: String,
    last_updated_at_unix_secs: u64,
    last_attempt_at_unix_secs: u64,
    #[serde(default)]
    last_error: Option<String>,
    #[serde(default)]
    source_url: Option<String>,
}

impl StoredCompatRow {
    fn into_record(self, key: String) -> CompatibilityKvRecord {
        CompatibilityKvRecord {
            key,
            value: self.value,
            last_updated_at_unix_secs: self.last_updated_at_unix_secs,
            last_attempt_at_unix_secs: self.last_attempt_at_unix_secs,
            last_error: self.last_error,
            source_url: self.source_url,
        }
    }
}

#[async_trait]
impl AnthropicCompatibilityKvStore for RedbStorage {
    async fn put_compatibility_kv_value(
        &self,
        key: &str,
        value: &str,
        observed_at_unix_secs: u64,
        source_url: Option<&str>,
    ) -> StorageResult<()> {
        let storage = self.clone();
        let key = key.to_owned();
        let value = value.to_owned();
        let source_url = source_url.map(str::to_owned);
        tokio::task::spawn_blocking(move || {
            put_compatibility_kv_value_sync(&storage, key, value, observed_at_unix_secs, source_url)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn put_compatibility_kv_failure(
        &self,
        key: &str,
        attempted_at_unix_secs: u64,
        error: &str,
    ) -> StorageResult<()> {
        let storage = self.clone();
        let key = key.to_owned();
        let error = error.to_owned();
        tokio::task::spawn_blocking(move || {
            put_compatibility_kv_failure_sync(&storage, key, attempted_at_unix_secs, error)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn get_compatibility_kv(
        &self,
        key: &str,
    ) -> StorageResult<Option<CompatibilityKvRecord>> {
        let storage = self.clone();
        let key = key.to_owned();
        tokio::task::spawn_blocking(move || get_compatibility_kv_sync(&storage, &key))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn list_compatibility_kv(&self) -> StorageResult<Vec<CompatibilityKvRecord>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || list_compatibility_kv_sync(&storage))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }
}

fn put_compatibility_kv_value_sync(
    storage: &RedbStorage,
    key: String,
    value: String,
    observed_at_unix_secs: u64,
    source_url: Option<String>,
) -> Result<(), StorageError> {
    let write_txn = storage.db.begin_write()?;
    {
        let mut table = write_txn.open_table(ANTHROPIC_COMPATIBILITY_KV_V1)?;
        let row = StoredCompatRow {
            value,
            last_updated_at_unix_secs: observed_at_unix_secs,
            last_attempt_at_unix_secs: observed_at_unix_secs,
            last_error: None,
            source_url,
        };
        let bytes = serde_json::to_vec(&row)?;
        table.insert(key.as_str(), bytes.as_slice())?;
    }
    write_txn.commit()?;
    Ok(())
}

fn put_compatibility_kv_failure_sync(
    storage: &RedbStorage,
    key: String,
    attempted_at_unix_secs: u64,
    error: String,
) -> Result<(), StorageError> {
    let write_txn = storage.db.begin_write()?;
    {
        let mut table = write_txn.open_table(ANTHROPIC_COMPATIBILITY_KV_V1)?;
        let Some(existing) = table.get(key.as_str())? else {
            drop(table);
            write_txn.commit()?;
            return Ok(());
        };
        let mut row: StoredCompatRow = serde_json::from_slice(existing.value())?;
        row.last_attempt_at_unix_secs = attempted_at_unix_secs;
        row.last_error = Some(error);
        drop(existing);
        let bytes = serde_json::to_vec(&row)?;
        table.insert(key.as_str(), bytes.as_slice())?;
    }
    write_txn.commit()?;
    Ok(())
}

fn get_compatibility_kv_sync(
    storage: &RedbStorage,
    key: &str,
) -> Result<Option<CompatibilityKvRecord>, StorageError> {
    let read_txn = storage.db.begin_read()?;
    let table = read_txn.open_table(ANTHROPIC_COMPATIBILITY_KV_V1)?;
    table
        .get(key)?
        .map(|value| {
            let row: StoredCompatRow = serde_json::from_slice(value.value())?;
            Ok::<_, StorageError>(row.into_record(key.to_owned()))
        })
        .transpose()
}

fn list_compatibility_kv_sync(
    storage: &RedbStorage,
) -> Result<Vec<CompatibilityKvRecord>, StorageError> {
    let read_txn = storage.db.begin_read()?;
    let table = read_txn.open_table(ANTHROPIC_COMPATIBILITY_KV_V1)?;
    let mut records = Vec::new();
    for row in table.iter()? {
        let (key_handle, value) = row?;
        let row: StoredCompatRow = serde_json::from_slice(value.value())?;
        let key = key_handle.value().to_owned();
        records.push(row.into_record(key));
    }
    records.sort_by(|left: &CompatibilityKvRecord, right| left.key.cmp(&right.key));
    Ok(records)
}
