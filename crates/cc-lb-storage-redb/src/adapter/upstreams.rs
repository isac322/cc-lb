use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use cc_lb_aead::EncryptedOAuthTokens;
use cc_lb_storage_api::{
    StorageError as ApiStorageError, StorageResult, UpstreamCreate, UpstreamRecord, UpstreamStore,
    UpstreamUpdate, validate_identifier,
};
use chrono::{DateTime, Utc};
use redb::{ReadableDatabase, ReadableTable};
use uuid::Uuid;

use crate::{RedbStorage, UPSTREAMS_V2, UPSTREAMS_V2_BY_NAME};

use crate::error_map::{map_join_err, map_redb_err};

#[async_trait]
impl UpstreamStore for RedbStorage {
    async fn create(&self, create: UpstreamCreate) -> StorageResult<UpstreamRecord> {
        validate_identifier("name", &create.name)?;
        let storage = self.clone();
        tokio::task::spawn_blocking(move || create_sync(&storage, create))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn get_by_name(&self, name: &str) -> StorageResult<Option<UpstreamRecord>> {
        validate_identifier("name", name)?;
        let storage = self.clone();
        let name = name.to_owned();
        tokio::task::spawn_blocking(move || get_by_name_sync(&storage, &name))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn get_by_id(&self, id: Uuid) -> StorageResult<Option<UpstreamRecord>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || get_by_id_sync(&storage, id))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn list(&self, after: Option<Uuid>, limit: usize) -> StorageResult<Vec<UpstreamRecord>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || list_sync(&storage, after, limit))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn update(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: UpstreamUpdate,
    ) -> StorageResult<UpstreamRecord> {
        if let Some(name) = &update.name {
            validate_identifier("name", name)?;
        }
        let storage = self.clone();
        tokio::task::spawn_blocking(move || update_sync(&storage, id, expected_revision, update))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn set_enabled(
        &self,
        id: Uuid,
        expected_revision: u64,
        enabled: bool,
    ) -> StorageResult<UpstreamRecord> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || {
            mutate_revision_sync(&storage, id, Some(expected_revision), |record| {
                record.enabled = enabled;
                Ok(())
            })
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn store_oauth_tokens(
        &self,
        id: Uuid,
        expected_revision: u64,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || {
            mutate_revision_sync(&storage, id, Some(expected_revision), |record| {
                record.oauth_credentials = Some(tokens);
                Ok(())
            })
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn claim_refresh_lease(
        &self,
        id: Uuid,
        holder: Uuid,
        ttl_secs: u64,
    ) -> StorageResult<bool> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || {
            claim_refresh_lease_sync(&storage, id, holder, ttl_secs)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn complete_refresh(
        &self,
        id: Uuid,
        holder: Uuid,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || {
            mutate_revision_sync(&storage, id, None, |record| {
                if record.refresh_lease_holder != Some(holder) {
                    return Err(conflict("refresh lease holder mismatch"));
                }
                record.oauth_credentials = Some(tokens);
                record.refresh_lease_holder = None;
                record.refresh_lease_until_unix_secs = None;
                Ok(())
            })
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn release_lease_on_failure(
        &self,
        id: Uuid,
        holder: Uuid,
        reason: String,
    ) -> StorageResult<()> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || {
            release_lease_on_failure_sync(&storage, id, holder, reason)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn set_last_apply_error(&self, id: Uuid, error: Option<String>) -> StorageResult<()> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || set_last_apply_error_sync(&storage, id, error))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn soft_delete(&self, id: Uuid, expected_revision: u64) -> StorageResult<()> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || soft_delete_sync(&storage, id, expected_revision))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn hard_delete(&self, id: Uuid) -> StorageResult<()> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || hard_delete_sync(&storage, id))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn claim_warmup_lease(
        &self,
        id: Uuid,
        holder: &str,
        ttl_secs: i64,
    ) -> StorageResult<bool> {
        let storage = self.clone();
        let holder = holder.to_owned();
        tokio::task::spawn_blocking(move || {
            claim_warmup_lease_sync(&storage, id, &holder, ttl_secs)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn write_warmup_cycle_key(
        &self,
        id: Uuid,
        holder: &str,
        new_cycle_key: i64,
        next_warmup_at: Option<DateTime<Utc>>,
    ) -> StorageResult<bool> {
        let storage = self.clone();
        let holder = holder.to_owned();
        tokio::task::spawn_blocking(move || {
            write_warmup_cycle_key_sync(&storage, id, &holder, new_cycle_key, next_warmup_at)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn release_warmup_lease(&self, id: Uuid, holder: &str) -> StorageResult<bool> {
        let storage = self.clone();
        let holder = holder.to_owned();
        tokio::task::spawn_blocking(move || release_warmup_lease_sync(&storage, id, &holder))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn warmup_now_unix_secs(&self) -> StorageResult<i64> {
        Ok(i64::try_from(now_unix_secs()).unwrap_or(i64::MAX))
    }

    async fn write_warmup_next_at(
        &self,
        id: Uuid,
        holder: &str,
        next_warmup_at: DateTime<Utc>,
    ) -> StorageResult<bool> {
        let storage = self.clone();
        let holder = holder.to_owned();
        tokio::task::spawn_blocking(move || {
            write_warmup_next_at_sync(&storage, id, &holder, next_warmup_at)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn clear_warmup_dialect_plugin(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> StorageResult<Option<UpstreamRecord>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || {
            clear_warmup_dialect_plugin_sync(&storage, id, expected_revision)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }
}

fn create_sync(
    storage: &RedbStorage,
    create: UpstreamCreate,
) -> Result<UpstreamRecord, crate::StorageError> {
    let now = now_unix_secs();
    let record = UpstreamRecord {
        id: Uuid::new_v4(),
        name: create.name,
        kind: create.kind,
        base_url: create.base_url,
        enabled: true,
        oauth_credentials: None,
        api_key_ciphertext: create.api_key_ciphertext,
        refresh_lease_holder: None,
        refresh_lease_until_unix_secs: None,
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs: None,
        revision: 1,
        created_at_unix_secs: now,
        updated_at_unix_secs: now,
        warmup_enabled: create.warmup_enabled,
        next_warmup_at: create.next_warmup_at,
        last_warmup_cycle_key: create.last_warmup_cycle_key,
        warmup_lease_holder: create.warmup_lease_holder,
        warmup_lease_until_unix_secs: create.warmup_lease_until_unix_secs,
        warmup_dialect_plugin: create.warmup_dialect_plugin,
    };

    let write_txn = storage.db.begin_write()?;
    {
        let id = record.id.as_bytes();
        let mut by_id = write_txn.open_table(UPSTREAMS_V2)?;
        let mut by_name = write_txn.open_table(UPSTREAMS_V2_BY_NAME)?;
        if by_name.get(record.name.as_str())?.is_some() {
            return Err(crate::StorageError::UpstreamConflict(
                "upstream name already exists".to_owned(),
            ));
        }
        by_id.insert(id.as_slice(), serde_json::to_vec(&record)?.as_slice())?;
        by_name.insert(record.name.as_str(), id.as_slice())?;
    }
    write_txn.commit()?;
    Ok(record)
}

fn get_by_name_sync(
    storage: &RedbStorage,
    name: &str,
) -> Result<Option<UpstreamRecord>, crate::StorageError> {
    let read_txn = storage.db.begin_read()?;
    let by_name = read_txn.open_table(UPSTREAMS_V2_BY_NAME)?;
    let Some(id) = by_name.get(name)?.map(|value| value.value().to_vec()) else {
        return Ok(None);
    };
    let by_id = read_txn.open_table(UPSTREAMS_V2)?;
    by_id
        .get(id.as_slice())?
        .map(|stored| serde_json::from_slice(stored.value()).map_err(Into::into))
        .transpose()
}

fn get_by_id_sync(
    storage: &RedbStorage,
    id: Uuid,
) -> Result<Option<UpstreamRecord>, crate::StorageError> {
    let read_txn = storage.db.begin_read()?;
    let table = read_txn.open_table(UPSTREAMS_V2)?;
    table
        .get(id.as_bytes().as_slice())?
        .map(|stored| serde_json::from_slice(stored.value()).map_err(Into::into))
        .transpose()
}

fn list_sync(
    storage: &RedbStorage,
    after: Option<Uuid>,
    limit: usize,
) -> Result<Vec<UpstreamRecord>, crate::StorageError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let read_txn = storage.db.begin_read()?;
    let table = read_txn.open_table(UPSTREAMS_V2)?;
    let mut records = Vec::new();
    for row in table.iter()? {
        let (_, value) = row?;
        let record: UpstreamRecord = serde_json::from_slice(value.value())?;
        if record.deleted_at_unix_secs.is_none()
            && after.is_none_or(|after| record.id.as_bytes() > after.as_bytes())
        {
            records.push(record);
        }
    }
    records.sort_by_key(|record| record.id);
    records.truncate(limit);
    Ok(records)
}

fn update_sync(
    storage: &RedbStorage,
    id: Uuid,
    expected_revision: u64,
    update: UpstreamUpdate,
) -> Result<UpstreamRecord, crate::StorageError> {
    mutate_revision_sync(storage, id, Some(expected_revision), |record| {
        if let Some(name) = update.name {
            record.name = name;
        }
        if update.base_url.is_some() {
            record.base_url = update.base_url;
        }
        if update.api_key_ciphertext.is_some() {
            record.api_key_ciphertext = update.api_key_ciphertext;
        }
        if let Some(warmup_enabled) = update.warmup_enabled {
            record.warmup_enabled = warmup_enabled;
        }
        if update.next_warmup_at.is_some() {
            record.next_warmup_at = update.next_warmup_at;
        }
        if update.last_warmup_cycle_key.is_some() {
            record.last_warmup_cycle_key = update.last_warmup_cycle_key;
        }
        if update.warmup_lease_holder.is_some() {
            record.warmup_lease_holder = update.warmup_lease_holder;
        }
        if update.warmup_lease_until_unix_secs.is_some() {
            record.warmup_lease_until_unix_secs = update.warmup_lease_until_unix_secs;
        }
        if let Some(warmup_dialect_plugin) = update.warmup_dialect_plugin {
            record.warmup_dialect_plugin = Some(warmup_dialect_plugin);
        }
        Ok(())
    })
}

fn claim_refresh_lease_sync(
    storage: &RedbStorage,
    id: Uuid,
    holder: Uuid,
    ttl_secs: u64,
) -> Result<bool, crate::StorageError> {
    let write_txn = storage.db.begin_write()?;
    let claimed = {
        let mut table = write_txn.open_table(UPSTREAMS_V2)?;
        let mut record: UpstreamRecord = {
            let Some(stored) = table.get(id.as_bytes().as_slice())? else {
                return Ok(false);
            };
            let stored = stored.value().to_vec();
            serde_json::from_slice(&stored)?
        };
        let now = now_unix_secs();
        if record
            .refresh_lease_until_unix_secs
            .is_some_and(|until| until > now)
            && record.refresh_lease_holder != Some(holder)
        {
            false
        } else {
            record.refresh_lease_holder = Some(holder);
            record.refresh_lease_until_unix_secs = Some(now.saturating_add(ttl_secs));
            record.updated_at_unix_secs = now;
            record.revision = record.revision.saturating_add(1);
            table.insert(
                id.as_bytes().as_slice(),
                serde_json::to_vec(&record)?.as_slice(),
            )?;
            true
        }
    };
    write_txn.commit()?;
    Ok(claimed)
}

fn release_lease_on_failure_sync(
    storage: &RedbStorage,
    id: Uuid,
    holder: Uuid,
    reason: String,
) -> Result<(), crate::StorageError> {
    let write_txn = storage.db.begin_write()?;
    {
        let mut table = write_txn.open_table(UPSTREAMS_V2)?;
        let mut record: UpstreamRecord = {
            let Some(stored) = table.get(id.as_bytes().as_slice())? else {
                return Ok(());
            };
            let stored = stored.value().to_vec();
            serde_json::from_slice(&stored)?
        };
        if record.refresh_lease_holder == Some(holder) {
            let now = now_unix_secs();
            record.last_apply_error = Some(reason);
            record.last_apply_at_unix_secs = Some(now);
            record.refresh_lease_holder = None;
            record.refresh_lease_until_unix_secs = None;
            record.updated_at_unix_secs = now;
            record.revision = record.revision.saturating_add(1);
            table.insert(
                id.as_bytes().as_slice(),
                serde_json::to_vec(&record)?.as_slice(),
            )?;
        }
    }
    write_txn.commit()?;
    Ok(())
}

fn set_last_apply_error_sync(
    storage: &RedbStorage,
    id: Uuid,
    error: Option<String>,
) -> Result<(), crate::StorageError> {
    let write_txn = storage.db.begin_write()?;
    let mut record = None;
    {
        let table = write_txn.open_table(UPSTREAMS_V2)?;
        if let Some(guard) = table.get(id.as_bytes().as_slice())? {
            let stored = guard.value().to_vec();
            let mut rec: UpstreamRecord = serde_json::from_slice(&stored)?;
            rec.last_apply_error = error;
            rec.last_apply_at_unix_secs = Some(now_unix_secs());
            rec.updated_at_unix_secs = rec.last_apply_at_unix_secs.unwrap_or_default();
            rec.revision = rec.revision.saturating_add(1);
            record = Some(rec);
        }
    }
    if let Some(record) = record {
        let mut table = write_txn.open_table(UPSTREAMS_V2)?;
        table.insert(
            id.as_bytes().as_slice(),
            serde_json::to_vec(&record)?.as_slice(),
        )?;
    }
    write_txn.commit()?;
    Ok(())
}

fn hard_delete_sync(storage: &RedbStorage, id: Uuid) -> Result<(), crate::StorageError> {
    let write_txn = storage.db.begin_write()?;
    let mut record_to_delete = None;
    {
        let by_id = write_txn.open_table(UPSTREAMS_V2)?;
        if let Some(guard) = by_id.get(id.as_bytes().as_slice())? {
            let stored = guard.value().to_vec();
            let record: UpstreamRecord = serde_json::from_slice(&stored)?;
            record_to_delete = Some(record);
        }
    }
    if let Some(record) = record_to_delete {
        let mut by_id = write_txn.open_table(UPSTREAMS_V2)?;
        by_id.remove(id.as_bytes().as_slice())?;
        let mut by_name = write_txn.open_table(UPSTREAMS_V2_BY_NAME)?;
        by_name.remove(record.name.as_str())?;
    }
    write_txn.commit()?;
    Ok(())
}

fn soft_delete_sync(
    storage: &RedbStorage,
    id: Uuid,
    expected_revision: u64,
) -> Result<(), crate::StorageError> {
    let write_txn = storage.db.begin_write()?;
    {
        let mut by_id = write_txn.open_table(UPSTREAMS_V2)?;
        let stored = by_id
            .get(id.as_bytes().as_slice())?
            .ok_or(crate::StorageError::UpstreamNotFound)?
            .value()
            .to_vec();
        let mut record: UpstreamRecord = serde_json::from_slice(&stored)?;
        if record.revision != expected_revision {
            return Err(crate::StorageError::UpstreamConflict(
                "stale upstream revision".to_owned(),
            ));
        }
        let name = record.name.clone();
        record.deleted_at_unix_secs = Some(now_unix_secs());
        record.revision = record.revision.saturating_add(1);
        record.updated_at_unix_secs = now_unix_secs();
        by_id.insert(
            id.as_bytes().as_slice(),
            serde_json::to_vec(&record)?.as_slice(),
        )?;
        let mut by_name = write_txn.open_table(UPSTREAMS_V2_BY_NAME)?;
        by_name.remove(name.as_str())?;
    }
    write_txn.commit()?;
    Ok(())
}

fn mutate_revision_sync<F>(
    storage: &RedbStorage,
    id: Uuid,
    expected_revision: Option<u64>,
    mutate: F,
) -> Result<UpstreamRecord, crate::StorageError>
where
    F: FnOnce(&mut UpstreamRecord) -> Result<(), ApiStorageError>,
{
    let write_txn = storage.db.begin_write()?;
    let record = {
        let mut by_id = write_txn.open_table(UPSTREAMS_V2)?;
        let mut record: UpstreamRecord = {
            let Some(stored) = by_id.get(id.as_bytes().as_slice())? else {
                return Err(crate::StorageError::UpstreamNotFound);
            };
            let stored = stored.value().to_vec();
            serde_json::from_slice(&stored)?
        };
        if let Some(expected) = expected_revision
            && record.revision != expected
        {
            return Err(crate::StorageError::UpstreamConflict(
                "stale upstream revision".to_owned(),
            ));
        }
        let old_name = record.name.clone();
        mutate(&mut record).map_err(api_error_as_redb)?;
        record.revision = record.revision.saturating_add(1);
        record.updated_at_unix_secs = now_unix_secs();
        by_id.insert(
            id.as_bytes().as_slice(),
            serde_json::to_vec(&record)?.as_slice(),
        )?;
        if old_name != record.name {
            let mut by_name = write_txn.open_table(UPSTREAMS_V2_BY_NAME)?;
            if by_name.get(record.name.as_str())?.is_some() {
                return Err(crate::StorageError::UpstreamConflict(
                    "upstream name already exists".to_owned(),
                ));
            }
            by_name.remove(old_name.as_str())?;
            by_name.insert(record.name.as_str(), id.as_bytes().as_slice())?;
        }
        record
    };
    write_txn.commit()?;
    Ok(record)
}

fn api_error_as_redb(error: ApiStorageError) -> crate::StorageError {
    crate::StorageError::UpstreamConflict(error.to_string())
}

fn conflict(message: impl Into<String>) -> ApiStorageError {
    ApiStorageError::Conflict {
        message: message.into(),
    }
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn claim_warmup_lease_sync(
    storage: &RedbStorage,
    id: Uuid,
    holder: &str,
    ttl_secs: i64,
) -> Result<bool, crate::StorageError> {
    let write_txn = storage.db.begin_write()?;
    let claimed = {
        let mut table = write_txn.open_table(UPSTREAMS_V2)?;
        let mut record: UpstreamRecord = {
            let Some(stored) = table.get(id.as_bytes().as_slice())? else {
                return Ok(false);
            };
            let stored = stored.value().to_vec();
            serde_json::from_slice(&stored)?
        };
        let now = now_unix_secs();
        let now_i64 = i64::try_from(now).unwrap_or(i64::MAX);
        if record
            .warmup_lease_until_unix_secs
            .is_some_and(|until| until > now_i64)
            && record.warmup_lease_holder.as_deref() != Some(holder)
        {
            false
        } else {
            record.warmup_lease_holder = Some(holder.to_owned());
            record.warmup_lease_until_unix_secs = Some(now_i64.saturating_add(ttl_secs));
            record.updated_at_unix_secs = now;
            record.revision = record.revision.saturating_add(1);
            table.insert(
                id.as_bytes().as_slice(),
                serde_json::to_vec(&record)?.as_slice(),
            )?;
            true
        }
    };
    write_txn.commit()?;
    Ok(claimed)
}

fn write_warmup_cycle_key_sync(
    storage: &RedbStorage,
    id: Uuid,
    holder: &str,
    new_cycle_key: i64,
    next_warmup_at: Option<DateTime<Utc>>,
) -> Result<bool, crate::StorageError> {
    let write_txn = storage.db.begin_write()?;
    let updated = {
        let mut table = write_txn.open_table(UPSTREAMS_V2)?;
        let mut record: UpstreamRecord = {
            let Some(stored) = table.get(id.as_bytes().as_slice())? else {
                return Ok(false);
            };
            let stored = stored.value().to_vec();
            serde_json::from_slice(&stored)?
        };
        let now = now_unix_secs();
        let now_i64 = i64::try_from(now).unwrap_or(i64::MAX);
        // Holder match is required; on success the cycle key and lease clear happen atomically.
        if record.deleted_at_unix_secs.is_some()
            || record.warmup_lease_holder.as_deref() != Some(holder)
            || record
                .warmup_lease_until_unix_secs
                .is_none_or(|until| until <= now_i64)
            || record.last_warmup_cycle_key == Some(new_cycle_key)
        {
            false
        } else {
            record.last_warmup_cycle_key = Some(new_cycle_key);
            record.next_warmup_at = next_warmup_at;
            record.warmup_lease_holder = None;
            record.warmup_lease_until_unix_secs = None;
            record.updated_at_unix_secs = now;
            record.revision = record.revision.saturating_add(1);
            table.insert(
                id.as_bytes().as_slice(),
                serde_json::to_vec(&record)?.as_slice(),
            )?;
            true
        }
    };
    write_txn.commit()?;
    Ok(updated)
}

fn release_warmup_lease_sync(
    storage: &RedbStorage,
    id: Uuid,
    holder: &str,
) -> Result<bool, crate::StorageError> {
    let write_txn = storage.db.begin_write()?;
    let released = {
        let mut table = write_txn.open_table(UPSTREAMS_V2)?;
        let mut record: UpstreamRecord = {
            let Some(stored) = table.get(id.as_bytes().as_slice())? else {
                return Ok(false);
            };
            let stored = stored.value().to_vec();
            serde_json::from_slice(&stored)?
        };
        if record.deleted_at_unix_secs.is_some()
            || record.warmup_lease_holder.as_deref() != Some(holder)
        {
            false
        } else {
            record.warmup_lease_holder = None;
            record.warmup_lease_until_unix_secs = None;
            record.updated_at_unix_secs = now_unix_secs();
            record.revision = record.revision.saturating_add(1);
            table.insert(
                id.as_bytes().as_slice(),
                serde_json::to_vec(&record)?.as_slice(),
            )?;
            true
        }
    };
    write_txn.commit()?;
    Ok(released)
}

fn write_warmup_next_at_sync(
    storage: &RedbStorage,
    id: Uuid,
    holder: &str,
    next_warmup_at: DateTime<Utc>,
) -> Result<bool, crate::StorageError> {
    let write_txn = storage.db.begin_write()?;
    let updated = {
        let mut table = write_txn.open_table(UPSTREAMS_V2)?;
        let mut record: UpstreamRecord = {
            let Some(stored) = table.get(id.as_bytes().as_slice())? else {
                return Ok(false);
            };
            let stored = stored.value().to_vec();
            serde_json::from_slice(&stored)?
        };
        let now = now_unix_secs();
        let now_i64 = i64::try_from(now).unwrap_or(i64::MAX);
        if record.deleted_at_unix_secs.is_some()
            || record.warmup_lease_holder.as_deref() != Some(holder)
            || record
                .warmup_lease_until_unix_secs
                .is_none_or(|until| until <= now_i64)
        {
            false
        } else {
            record.next_warmup_at = Some(next_warmup_at);
            record.updated_at_unix_secs = now;
            record.revision = record.revision.saturating_add(1);
            table.insert(
                id.as_bytes().as_slice(),
                serde_json::to_vec(&record)?.as_slice(),
            )?;
            true
        }
    };
    write_txn.commit()?;
    Ok(updated)
}

fn clear_warmup_dialect_plugin_sync(
    storage: &RedbStorage,
    id: Uuid,
    expected_revision: u64,
) -> Result<Option<UpstreamRecord>, crate::StorageError> {
    let write_txn = storage.db.begin_write()?;
    let updated = {
        let mut table = write_txn.open_table(UPSTREAMS_V2)?;
        let mut record: UpstreamRecord = {
            let Some(stored) = table.get(id.as_bytes().as_slice())? else {
                return Ok(None);
            };
            let stored = stored.value().to_vec();
            serde_json::from_slice(&stored)?
        };
        if record.deleted_at_unix_secs.is_some() || record.revision != expected_revision {
            return Ok(None);
        }
        record.warmup_dialect_plugin = None;
        record.revision += 1;
        record.updated_at_unix_secs = now_unix_secs();
        let bytes = serde_json::to_vec(&record)?;
        table.insert(id.as_bytes().as_slice(), bytes.as_slice())?;
        Some(record)
    };
    write_txn.commit()?;
    Ok(updated)
}

#[cfg(test)]
mod tests {
    use std::future::Future;

    use super::*;
    use cc_lb_storage_api::upstream::UpstreamKind;
    use chrono::TimeZone;

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    fn run_async(future: impl Future<Output = TestResult>) -> TestResult {
        tokio::runtime::Runtime::new()?.block_on(future)
    }

    fn temp_storage() -> TestResult<(tempfile::TempDir, RedbStorage)> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("upstreams.redb");
        let storage = RedbStorage::open(&path, [29; 32])?;
        Ok((dir, storage))
    }

    async fn create_warmup_upstream(storage: &RedbStorage) -> TestResult<UpstreamRecord> {
        let record = storage
            .create(UpstreamCreate {
                name: format!("upstream-{}", Uuid::new_v4().simple()),
                kind: UpstreamKind::AnthropicOauth,
                base_url: None,
                api_key_ciphertext: None,
                warmup_enabled: true,
                next_warmup_at: None,
                last_warmup_cycle_key: None,
                warmup_lease_holder: None,
                warmup_lease_until_unix_secs: None,
                warmup_dialect_plugin: None,
            })
            .await?;
        Ok(record)
    }

    #[test]
    fn claim_warmup_lease_succeeds_when_free() -> TestResult {
        run_async(async {
            let (_dir, storage) = temp_storage()?;
            let record = create_warmup_upstream(&storage).await?;

            assert!(
                storage
                    .claim_warmup_lease(record.id, "replica-a", 60)
                    .await?
            );

            let stored = storage.get_by_id(record.id).await?.expect("upstream");
            assert_eq!(stored.warmup_lease_holder.as_deref(), Some("replica-a"));
            assert!(stored.warmup_lease_until_unix_secs.is_some_and(|until| until
                > i64::try_from(now_unix_secs()).unwrap_or(i64::MAX)));
            Ok(())
        })
    }

    #[test]
    fn claim_warmup_lease_fails_when_held_by_other() -> TestResult {
        run_async(async {
            let (_dir, storage) = temp_storage()?;
            let record = create_warmup_upstream(&storage).await?;

            assert!(
                storage
                    .claim_warmup_lease(record.id, "replica-a", 60)
                    .await?
            );
            assert!(
                !storage
                    .claim_warmup_lease(record.id, "replica-b", 60)
                    .await?
            );

            let stored = storage.get_by_id(record.id).await?.expect("upstream");
            assert_eq!(stored.warmup_lease_holder.as_deref(), Some("replica-a"));
            Ok(())
        })
    }

    #[test]
    fn write_warmup_cycle_key_fails_with_wrong_holder() -> TestResult {
        run_async(async {
            let (_dir, storage) = temp_storage()?;
            let record = create_warmup_upstream(&storage).await?;
            let next_warmup_at = Utc.timestamp_opt(1_700_018_030, 0).single();

            assert!(
                storage
                    .claim_warmup_lease(record.id, "replica-a", 60)
                    .await?
            );
            assert!(
                !storage
                    .write_warmup_cycle_key(record.id, "replica-b", 1_700_000_000, next_warmup_at)
                    .await?
            );

            let stored = storage.get_by_id(record.id).await?.expect("upstream");
            assert_eq!(stored.last_warmup_cycle_key, None);
            assert_eq!(stored.next_warmup_at, None);
            Ok(())
        })
    }

    #[test]
    fn write_warmup_cycle_key_fails_when_lease_expired() -> TestResult {
        run_async(async {
            let (_dir, storage) = temp_storage()?;
            let record = create_warmup_upstream(&storage).await?;
            let next_warmup_at = Utc.timestamp_opt(1_700_018_030, 0).single();

            assert!(
                storage
                    .claim_warmup_lease(record.id, "replica-a", 0)
                    .await?
            );
            assert!(
                !storage
                    .write_warmup_cycle_key(record.id, "replica-a", 1_700_000_000, next_warmup_at)
                    .await?
            );

            let stored = storage.get_by_id(record.id).await?.expect("upstream");
            assert_eq!(stored.last_warmup_cycle_key, None);
            assert_eq!(stored.next_warmup_at, None);
            Ok(())
        })
    }

    #[test]
    fn write_warmup_cycle_key_fails_when_cycle_key_unchanged() -> TestResult {
        run_async(async {
            let (_dir, storage) = temp_storage()?;
            let record = create_warmup_upstream(&storage).await?;
            let first_next_warmup_at = Utc.timestamp_opt(1_700_018_030, 0).single();
            let second_next_warmup_at = Utc.timestamp_opt(1_700_018_060, 0).single();

            assert!(
                storage
                    .claim_warmup_lease(record.id, "replica-a", 60)
                    .await?
            );
            assert!(
                storage
                    .write_warmup_cycle_key(
                        record.id,
                        "replica-a",
                        1_700_000_000,
                        first_next_warmup_at,
                    )
                    .await?
            );
            let stored = storage.get_by_id(record.id).await?.expect("upstream");
            assert_eq!(stored.warmup_lease_holder, None);
            assert_eq!(stored.warmup_lease_until_unix_secs, None);
            assert!(
                storage
                    .claim_warmup_lease(record.id, "replica-a", 60)
                    .await?
            );
            assert!(
                !storage
                    .write_warmup_cycle_key(
                        record.id,
                        "replica-a",
                        1_700_000_000,
                        second_next_warmup_at,
                    )
                    .await?
            );

            let stored = storage.get_by_id(record.id).await?.expect("upstream");
            assert_eq!(stored.last_warmup_cycle_key, Some(1_700_000_000));
            assert_eq!(stored.next_warmup_at, first_next_warmup_at);
            assert_eq!(stored.warmup_lease_holder.as_deref(), Some("replica-a"));
            Ok(())
        })
    }

    #[test]
    fn write_warmup_cycle_key_succeeds_when_all_predicates_hold() -> TestResult {
        run_async(async {
            let (_dir, storage) = temp_storage()?;
            let record = create_warmup_upstream(&storage).await?;
            let next_warmup_at = Utc.timestamp_opt(1_700_018_030, 0).single();

            assert!(
                storage
                    .claim_warmup_lease(record.id, "replica-a", 60)
                    .await?
            );
            assert!(
                storage
                    .write_warmup_cycle_key(record.id, "replica-a", 1_700_000_000, next_warmup_at)
                    .await?
            );

            let stored = storage.get_by_id(record.id).await?.expect("upstream");
            assert_eq!(stored.last_warmup_cycle_key, Some(1_700_000_000));
            assert_eq!(stored.next_warmup_at, next_warmup_at);
            assert_eq!(stored.warmup_lease_holder, None);
            assert_eq!(stored.warmup_lease_until_unix_secs, None);
            Ok(())
        })
    }
}
