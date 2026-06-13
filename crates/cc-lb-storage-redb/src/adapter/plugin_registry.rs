use std::collections::HashSet;

use async_trait::async_trait;
use cc_lb_storage_api::{
    MAX_WASM_BLOB_BYTES, PluginChainConflictReason, PluginChainEntry, PluginChainEntryInput,
    PluginChainEntryUpdate, PluginRegistryStore, PluginSlot, StorageError as ApiStorageError,
    StorageResult, WasmBlob, WasmBlobRecord, WasmRegistryEntry, WasmRegistryEntryInput,
    sparse_order, validate_identifier,
};
use redb::{ReadableDatabase, ReadableTable};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    PLUGIN_CHAINS_V2, PRINCIPALS_V2, RedbStorage, StorageError, WASM_BLOBS_V2, WASM_REGISTRY_V2,
};

use crate::error_map::{map_join_err, map_redb_err};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredWasmBlob {
    bytes: Vec<u8>,
    size_bytes: u64,
    parse_validated_at_unix_secs: u64,
    refcount: i64,
}

#[async_trait]
impl PluginRegistryStore for RedbStorage {
    async fn persist_wasm_upload(
        &self,
        blob: WasmBlob,
        entry: WasmRegistryEntryInput,
    ) -> StorageResult<(WasmRegistryEntry, bool)> {
        validate_upload(&blob, &entry)?;
        let storage = self.clone();
        tokio::task::spawn_blocking(move || storage.persist_wasm_upload_sync(blob, entry))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn get_blob_bytes(&self, sha256: [u8; 32]) -> StorageResult<Option<Vec<u8>>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || storage.get_blob_bytes_sync(sha256))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn get_blob(&self, sha256: [u8; 32]) -> StorageResult<Option<WasmBlobRecord>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || storage.get_blob_sync(sha256))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn list_orphan_blobs(&self) -> StorageResult<Vec<[u8; 32]>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || storage.list_orphan_blobs_sync())
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn list_registry(
        &self,
        after: Option<Uuid>,
        limit: usize,
    ) -> StorageResult<Vec<WasmRegistryEntry>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || storage.list_registry_sync(after, limit))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn get_registry_entry_by_sha(
        &self,
        sha256: [u8; 32],
    ) -> StorageResult<Option<WasmRegistryEntry>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || storage.get_registry_entry_by_sha_sync(sha256))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn get_registry_entry_by_id(&self, id: Uuid) -> StorageResult<Option<WasmRegistryEntry>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || storage.get_registry_entry_by_id_sync(id))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn update_registry_label(
        &self,
        id: Uuid,
        expected_revision: u64,
        label: Option<String>,
    ) -> StorageResult<WasmRegistryEntry> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || {
            storage.update_registry_label_sync(id, expected_revision, label)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn update_supported_slots(
        &self,
        id: Uuid,
        supported_slots: Vec<PluginSlot>,
    ) -> StorageResult<()> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || storage.update_supported_slots_sync(id, supported_slots))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn delete_registry_entry(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> StorageResult<Option<WasmRegistryEntry>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || {
            storage.delete_registry_entry_sync(id, expected_revision)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn decrement_blob_refcount_or_delete(&self, sha256: [u8; 32]) -> StorageResult<bool> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || storage.decrement_blob_refcount_or_delete_sync(sha256))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn insert_chain_entry(
        &self,
        entry: PluginChainEntryInput,
    ) -> StorageResult<PluginChainEntry> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || storage.insert_chain_entry_sync(entry))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn list_chain_for_principal(
        &self,
        principal_id: Uuid,
        slot: PluginSlot,
    ) -> StorageResult<Vec<PluginChainEntry>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || {
            storage.list_chain_for_principal_sync(principal_id, slot)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn update_chain_entry(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: PluginChainEntryUpdate,
    ) -> StorageResult<Option<PluginChainEntry>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || {
            storage.update_chain_entry_sync(id, expected_revision, update)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn reorder_chain(
        &self,
        principal_id: Uuid,
        slot: PluginSlot,
        new_orders: Vec<(Uuid, i64, u64)>,
    ) -> StorageResult<Vec<PluginChainEntry>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || {
            storage.reorder_chain_sync(principal_id, slot, new_orders)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn delete_chain_entry(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> StorageResult<Option<PluginChainEntry>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || storage.delete_chain_entry_sync(id, expected_revision))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn rebalance_chain(
        &self,
        principal_id: Uuid,
        slot: PluginSlot,
    ) -> StorageResult<Vec<PluginChainEntry>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || storage.rebalance_chain_sync(principal_id, slot))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }
}

impl RedbStorage {
    fn persist_wasm_upload_sync(
        &self,
        blob: WasmBlob,
        input: WasmRegistryEntryInput,
    ) -> Result<(WasmRegistryEntry, bool), StorageError> {
        let write_txn = self.db.begin_write()?;
        let (sha_match, name_conflict) =
            scan_registry_for_upload(&write_txn, blob.sha256, &input.name)?;
        if let Some(existing) = sha_match {
            if existing.name == input.name
                && existing.original_filename == input.original_filename
                && existing.label == input.label
            {
                self_heal_blob_if_missing(&write_txn, &blob, existing.id)?;
                write_txn.commit()?;
                return Ok((existing, true));
            }
            return Err(StorageError::PluginRegistryConflict {
                message: "sha256 already registered for a different wasm entry".to_owned(),
            });
        }
        if name_conflict {
            return Err(StorageError::PluginRegistryConflict {
                message: format!("plugin registry name already exists: {}", input.name),
            });
        }
        let stored_blob = StoredWasmBlob {
            bytes: blob.bytes,
            size_bytes: blob.size_bytes,
            parse_validated_at_unix_secs: blob.parse_validated_at_unix_secs,
            refcount: 0,
        };
        {
            let mut blobs = write_txn.open_table(WASM_BLOBS_V2)?;
            let payload = serde_json::to_vec(&stored_blob)?;
            blobs.insert(blob.sha256.as_slice(), payload.as_slice())?;
        }
        let record = WasmRegistryEntry {
            id: Uuid::new_v4(),
            sha256: blob.sha256,
            name: input.name,
            original_filename: input.original_filename,
            label: input.label,
            uploaded_at_unix_secs: input.uploaded_at_unix_secs,
            uploaded_by_admin_id: input.uploaded_by_admin_id,
            refcount: 0,
            revision: 0,
            kind: "filter".to_owned(),
            wire_version: cc_lb_storage_api::BUILTIN_CACHE_AFFINITY_WIRE_VERSION,
            is_builtin: false,
            metadata: None,
            supported_slots: input.supported_slots,
        };
        {
            let mut registry = write_txn.open_table(WASM_REGISTRY_V2)?;
            let payload = serde_json::to_vec(&record)?;
            registry.insert(record.id.as_bytes().as_slice(), payload.as_slice())?;
        }
        write_txn.commit()?;
        Ok((record, false))
    }

    fn get_blob_bytes_sync(&self, sha256: [u8; 32]) -> Result<Option<Vec<u8>>, StorageError> {
        let read_txn = self.db.begin_read()?;
        let blobs = read_txn.open_table(WASM_BLOBS_V2)?;
        blobs
            .get(sha256.as_slice())?
            .map(|value| {
                serde_json::from_slice::<StoredWasmBlob>(value.value())
                    .map(|blob| blob.bytes)
                    .map_err(StorageError::from)
            })
            .transpose()
    }

    fn get_blob_sync(&self, sha256: [u8; 32]) -> Result<Option<WasmBlobRecord>, StorageError> {
        let read_txn = self.db.begin_read()?;
        let blobs = read_txn.open_table(WASM_BLOBS_V2)?;
        Ok(blobs
            .get(sha256.as_slice())?
            .map(|_| WasmBlobRecord { sha256 }))
    }

    fn list_orphan_blobs_sync(&self) -> Result<Vec<[u8; 32]>, StorageError> {
        let read_txn = self.db.begin_read()?;
        let blobs = read_txn.open_table(WASM_BLOBS_V2)?;
        let registry = read_txn.open_table(WASM_REGISTRY_V2)?;
        let mut blob_shas = HashSet::new();
        for row in blobs.iter()? {
            let (key, _) = row?;
            let sha = sha_from_key(key.value())?;
            blob_shas.insert(sha);
        }
        for row in registry.iter()? {
            let (_, value) = row?;
            let entry: WasmRegistryEntry = serde_json::from_slice(value.value())?;
            blob_shas.remove(&entry.sha256);
        }
        let mut orphaned = blob_shas.into_iter().collect::<Vec<_>>();
        orphaned.sort();
        Ok(orphaned)
    }

    fn list_registry_sync(
        &self,
        after: Option<Uuid>,
        limit: usize,
    ) -> Result<Vec<WasmRegistryEntry>, StorageError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let read_txn = self.db.begin_read()?;
        let registry = read_txn.open_table(WASM_REGISTRY_V2)?;
        let blobs = read_txn.open_table(WASM_BLOBS_V2)?;
        let mut entries = Vec::new();
        for row in registry.iter()? {
            let (_, value) = row?;
            let mut entry: WasmRegistryEntry = serde_json::from_slice(value.value())?;
            entry.refcount = blob_refcount(&blobs, entry.sha256)?;
            entries.push(entry);
        }
        entries.sort_by_key(|entry| entry.id);
        let skip = after
            .and_then(|id| {
                entries
                    .iter()
                    .position(|entry| entry.id == id)
                    .map(|idx| idx + 1)
            })
            .unwrap_or(0);
        Ok(entries.into_iter().skip(skip).take(limit).collect())
    }

    fn get_registry_entry_by_sha_sync(
        &self,
        sha256: [u8; 32],
    ) -> Result<Option<WasmRegistryEntry>, StorageError> {
        let read_txn = self.db.begin_read()?;
        registry_by_sha_read(&read_txn, sha256)
    }

    fn get_registry_entry_by_id_sync(
        &self,
        id: Uuid,
    ) -> Result<Option<WasmRegistryEntry>, StorageError> {
        let read_txn = self.db.begin_read()?;
        let registry = read_txn.open_table(WASM_REGISTRY_V2)?;
        let blobs = read_txn.open_table(WASM_BLOBS_V2)?;
        registry
            .get(id.as_bytes().as_slice())?
            .map(|value| {
                let mut entry: WasmRegistryEntry = serde_json::from_slice(value.value())?;
                entry.refcount = blob_refcount(&blobs, entry.sha256)?;
                Ok(entry)
            })
            .transpose()
    }

    fn update_registry_label_sync(
        &self,
        id: Uuid,
        expected_revision: u64,
        label: Option<String>,
    ) -> Result<WasmRegistryEntry, StorageError> {
        let write_txn = self.db.begin_write()?;
        let mut found = None;
        {
            let registry = write_txn.open_table(WASM_REGISTRY_V2)?;
            let blobs = write_txn.open_table(WASM_BLOBS_V2)?;
            for row in registry.iter()? {
                let (_key, value) = row?;
                let mut entry: WasmRegistryEntry = serde_json::from_slice(value.value())?;
                if entry.id == id {
                    if entry.revision != expected_revision {
                        return Err(StorageError::StalePluginRegistryRevision {
                            current: entry.revision,
                        });
                    }
                    entry.label = label;
                    entry.revision = entry
                        .revision
                        .checked_add(1)
                        .ok_or(StorageError::PluginRegistryRevisionOverflow)?;
                    entry.refcount = blob_refcount_from_write(&blobs, entry.sha256)?;
                    found = Some(entry);
                    break;
                }
            }
        }
        let Some(entry) = found else {
            return Err(StorageError::PluginRegistryConflict {
                message: "unknown plugin registry entry".to_owned(),
            });
        };
        {
            let mut registry = write_txn.open_table(WASM_REGISTRY_V2)?;
            let payload = serde_json::to_vec(&entry)?;
            registry.insert(entry.id.as_bytes().as_slice(), payload.as_slice())?;
        }
        write_txn.commit()?;
        Ok(entry)
    }

    fn update_supported_slots_sync(
        &self,
        id: Uuid,
        supported_slots: Vec<PluginSlot>,
    ) -> Result<(), StorageError> {
        let write_txn = self.db.begin_write()?;
        let mut updated = None;
        {
            let registry = write_txn.open_table(WASM_REGISTRY_V2)?;
            for row in registry.iter()? {
                let (_key, value) = row?;
                let mut entry: WasmRegistryEntry = serde_json::from_slice(value.value())?;
                if entry.id == id {
                    entry.supported_slots = supported_slots.clone();
                    updated = Some(entry);
                    break;
                }
            }
        }
        let Some(entry) = updated else {
            return Ok(());
        };
        {
            let mut registry = write_txn.open_table(WASM_REGISTRY_V2)?;
            let payload = serde_json::to_vec(&entry)?;
            registry.insert(entry.id.as_bytes().as_slice(), payload.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    fn delete_registry_entry_sync(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> Result<Option<WasmRegistryEntry>, StorageError> {
        let write_txn = self.db.begin_write()?;
        let deleted = {
            let registry = write_txn.open_table(WASM_REGISTRY_V2)?;
            let Some(value) = registry.get(id.as_bytes().as_slice())? else {
                return Ok(None);
            };
            let mut entry: WasmRegistryEntry = serde_json::from_slice(value.value())?;
            drop(value);
            drop(registry);
            if entry.revision != expected_revision {
                return Err(StorageError::StalePluginRegistryRevision {
                    current: entry.revision,
                });
            }
            if registry_is_referenced_by_chain(&write_txn, id)? {
                return Err(StorageError::PluginRegistryReferenced { id: id.to_string() });
            }
            let mut blobs = write_txn.open_table(WASM_BLOBS_V2)?;
            entry.refcount = blob_refcount_from_write(&blobs, entry.sha256)?;
            blobs.remove(entry.sha256.as_slice())?;
            drop(blobs);
            let mut registry = write_txn.open_table(WASM_REGISTRY_V2)?;
            registry.remove(id.as_bytes().as_slice())?;
            entry
        };
        write_txn.commit()?;
        Ok(Some(deleted))
    }

    fn decrement_blob_refcount_or_delete_sync(
        &self,
        sha256: [u8; 32],
    ) -> Result<bool, StorageError> {
        let write_txn = self.db.begin_write()?;
        let removed = {
            let mut blobs = write_txn.open_table(WASM_BLOBS_V2)?;
            let Some(value) = blobs.get(sha256.as_slice())? else {
                drop(blobs);
                write_txn.commit()?;
                return Ok(false);
            };
            let blob: StoredWasmBlob = serde_json::from_slice(value.value())?;
            drop(value);
            if blob.refcount > 0 {
                false
            } else {
                let registry = write_txn.open_table(WASM_REGISTRY_V2)?;
                let mut referenced = false;
                for row in registry.iter()? {
                    let (_, value) = row?;
                    let entry: WasmRegistryEntry = serde_json::from_slice(value.value())?;
                    if entry.sha256 == sha256 {
                        referenced = true;
                        break;
                    }
                }
                drop(registry);
                if referenced {
                    false
                } else {
                    blobs.remove(sha256.as_slice())?.is_some()
                }
            }
        };
        write_txn.commit()?;
        Ok(removed)
    }

    fn insert_chain_entry_sync(
        &self,
        input: PluginChainEntryInput,
    ) -> Result<PluginChainEntry, StorageError> {
        let write_txn = self.db.begin_write()?;
        if !principal_exists(&write_txn, input.principal_id)? {
            return Err(StorageError::PrincipalNotFound {
                id: input.principal_id.to_string(),
            });
        }
        if is_singleton_slot(input.slot)
            && let Some(existing_entry_id) =
                existing_chain_entry_for_slot(&write_txn, input.principal_id, input.slot)?
        {
            return Err(StorageError::PluginChainConflict {
                reason: PluginChainConflictReason::SlotIsSingleton { existing_entry_id },
            });
        }
        let registry = registry_by_id(&write_txn, input.wasm_registry_id)?.ok_or_else(|| {
            StorageError::PluginRegistryConflict {
                message: "unknown plugin registry entry".to_owned(),
            }
        })?;
        increment_blob(&write_txn, registry.sha256)?;
        let record = PluginChainEntry {
            id: Uuid::new_v4(),
            principal_id: input.principal_id,
            slot: input.slot,
            order: input.order,
            wasm_registry_id: input.wasm_registry_id,
            config: input.config,
            sse_per_event: input.sse_per_event,
            batched_events_per_flush: input.batched_events_per_flush,
            batched_flush_ms: input.batched_flush_ms,
            revision: 0,
            wire_version: input.wire_version,
        };
        put_chain(&write_txn, &record)?;
        write_txn.commit()?;
        Ok(record)
    }

    fn list_chain_for_principal_sync(
        &self,
        principal_id: Uuid,
        slot: PluginSlot,
    ) -> Result<Vec<PluginChainEntry>, StorageError> {
        let read_txn = self.db.begin_read()?;
        let chains = read_txn.open_table(PLUGIN_CHAINS_V2)?;
        let mut entries = Vec::new();
        for row in chains.iter()? {
            let (_, value) = row?;
            let entry: PluginChainEntry = serde_json::from_slice(value.value())?;
            if entry.principal_id == principal_id && entry.slot == slot {
                entries.push(entry);
            }
        }
        entries.sort_by_key(|entry| (entry.order, entry.id));
        Ok(entries)
    }

    fn update_chain_entry_sync(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: PluginChainEntryUpdate,
    ) -> Result<Option<PluginChainEntry>, StorageError> {
        if update.config.is_none()
            && update.sse_per_event.is_none()
            && update.batched_events_per_flush.is_none()
            && update.batched_flush_ms.is_none()
        {
            return Err(StorageError::InvalidInput {
                field: "plugin_chain_update".to_owned(),
                reason: "empty_update".to_owned(),
            });
        }
        let write_txn = self.db.begin_write()?;
        let Some(mut entry) = chain_by_id(&write_txn, id)? else {
            return Ok(None);
        };
        if entry.revision != expected_revision {
            return Err(StorageError::StalePluginChainRevision {
                current: entry.revision,
            });
        }
        if let Some(value) = update.config {
            entry.config = value;
        }
        if let Some(value) = update.sse_per_event {
            entry.sse_per_event = value;
        }
        if let Some(value) = update.batched_events_per_flush {
            entry.batched_events_per_flush = value;
        }
        if let Some(value) = update.batched_flush_ms {
            entry.batched_flush_ms = value;
        }
        entry.revision = entry
            .revision
            .checked_add(1)
            .ok_or(StorageError::PluginChainRevisionOverflow)?;
        put_chain(&write_txn, &entry)?;
        write_txn.commit()?;
        Ok(Some(entry))
    }

    fn reorder_chain_sync(
        &self,
        principal_id: Uuid,
        slot: PluginSlot,
        new_orders: Vec<(Uuid, i64, u64)>,
    ) -> Result<Vec<PluginChainEntry>, StorageError> {
        let write_txn = self.db.begin_write()?;
        let mut entries = chain_for_principal_from_write(&write_txn, principal_id, slot)?;
        for (id, order, expected_revision) in new_orders {
            let Some(entry) = entries.iter_mut().find(|entry| entry.id == id) else {
                return Err(StorageError::PluginRegistryConflict {
                    message: "unknown plugin chain entry".to_owned(),
                });
            };
            if entry.revision != expected_revision {
                return Err(StorageError::StalePluginChainRevision {
                    current: entry.revision,
                });
            }
            entry.order = order;
            entry.revision = entry
                .revision
                .checked_add(1)
                .ok_or(StorageError::PluginChainRevisionOverflow)?;
        }
        validate_chain_orders(&entries)?;
        for entry in &entries {
            put_chain(&write_txn, entry)?;
        }
        write_txn.commit()?;
        entries.sort_by_key(|entry| (entry.order, entry.id));
        Ok(entries)
    }

    fn delete_chain_entry_sync(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> Result<Option<PluginChainEntry>, StorageError> {
        let write_txn = self.db.begin_write()?;
        let Some(entry) = chain_by_id(&write_txn, id)? else {
            return Ok(None);
        };
        if entry.revision != expected_revision {
            return Err(StorageError::StalePluginChainRevision {
                current: entry.revision,
            });
        }
        let registry = registry_by_id(&write_txn, entry.wasm_registry_id)?.ok_or_else(|| {
            StorageError::PluginRegistryConflict {
                message: "missing plugin registry entry".to_owned(),
            }
        })?;
        decrement_blob(&write_txn, registry.sha256)?;
        {
            let mut chains = write_txn.open_table(PLUGIN_CHAINS_V2)?;
            chains.remove(id.as_bytes().as_slice())?;
        }
        write_txn.commit()?;
        Ok(Some(entry))
    }

    fn rebalance_chain_sync(
        &self,
        principal_id: Uuid,
        slot: PluginSlot,
    ) -> Result<Vec<PluginChainEntry>, StorageError> {
        let mut entries = self.list_chain_for_principal_sync(principal_id, slot)?;
        let new_orders = sparse_order::rebalance(vec![0; entries.len()]);
        let orders = entries
            .iter()
            .zip(new_orders)
            .map(|(entry, order)| (entry.id, order, entry.revision))
            .collect();
        self.reorder_chain_sync(principal_id, slot, orders)?;
        entries = self.list_chain_for_principal_sync(principal_id, slot)?;
        Ok(entries)
    }
}

fn validate_upload(blob: &WasmBlob, entry: &WasmRegistryEntryInput) -> StorageResult<()> {
    validate_identifier("plugin.name", &entry.name)?;
    if blob.size_bytes > MAX_WASM_BLOB_BYTES || blob.bytes.len() as u64 != blob.size_bytes {
        return Err(ApiStorageError::InvalidInput {
            field: "wasm_blob.bytes".to_owned(),
            reason: "blob must be <= 32 MiB and size_bytes must match bytes length".to_owned(),
        });
    }
    Ok(())
}

fn scan_registry_for_upload(
    write_txn: &redb::WriteTransaction,
    sha256: [u8; 32],
    name: &str,
) -> Result<(Option<WasmRegistryEntry>, bool), StorageError> {
    let registry = write_txn.open_table(WASM_REGISTRY_V2)?;
    let blobs = write_txn.open_table(WASM_BLOBS_V2)?;
    let mut sha_match = None;
    let mut name_conflict = false;
    for row in registry.iter()? {
        let (_, value) = row?;
        let mut entry: WasmRegistryEntry = serde_json::from_slice(value.value())?;
        if entry.sha256 == sha256 {
            entry.refcount = blob_refcount_from_write(&blobs, sha256)?;
            sha_match = Some(entry);
        } else if entry.name == name {
            name_conflict = true;
        }
        if sha_match.is_some() && name_conflict {
            break;
        }
    }
    Ok((sha_match, name_conflict))
}

fn sha_from_key(key: &[u8]) -> Result<[u8; 32], StorageError> {
    <[u8; 32]>::try_from(key).map_err(|_| StorageError::PluginRegistryConflict {
        message: "wasm blob sha256 key must be 32 bytes".to_owned(),
    })
}

fn registry_by_sha_read(
    read_txn: &redb::ReadTransaction,
    sha256: [u8; 32],
) -> Result<Option<WasmRegistryEntry>, StorageError> {
    let registry = read_txn.open_table(WASM_REGISTRY_V2)?;
    let blobs = read_txn.open_table(WASM_BLOBS_V2)?;
    for row in registry.iter()? {
        let (_, value) = row?;
        let mut entry: WasmRegistryEntry = serde_json::from_slice(value.value())?;
        if entry.sha256 == sha256 {
            entry.refcount = blob_refcount(&blobs, sha256)?;
            return Ok(Some(entry));
        }
    }
    Ok(None)
}

fn registry_is_referenced_by_chain(
    write_txn: &redb::WriteTransaction,
    registry_id: Uuid,
) -> Result<bool, StorageError> {
    let chains = write_txn.open_table(PLUGIN_CHAINS_V2)?;
    for row in chains.iter()? {
        let (_, value) = row?;
        let entry: PluginChainEntry = serde_json::from_slice(value.value())?;
        if entry.wasm_registry_id == registry_id {
            return Ok(true);
        }
    }
    Ok(false)
}

fn principal_exists(
    write_txn: &redb::WriteTransaction,
    principal_id: Uuid,
) -> Result<bool, StorageError> {
    let principals = write_txn.open_table(PRINCIPALS_V2)?;
    Ok(principals
        .get(principal_id.as_bytes().as_slice())?
        .is_some())
}

fn is_singleton_slot(slot: PluginSlot) -> bool {
    matches!(slot, PluginSlot::Shape)
}

fn existing_chain_entry_for_slot(
    write_txn: &redb::WriteTransaction,
    principal_id: Uuid,
    slot: PluginSlot,
) -> Result<Option<Uuid>, StorageError> {
    let chains = write_txn.open_table(PLUGIN_CHAINS_V2)?;
    for row in chains.iter()? {
        let (_, value) = row?;
        let entry: PluginChainEntry = serde_json::from_slice(value.value())?;
        if entry.principal_id == principal_id && entry.slot == slot {
            return Ok(Some(entry.id));
        }
    }
    Ok(None)
}

fn registry_by_id(
    write_txn: &redb::WriteTransaction,
    id: Uuid,
) -> Result<Option<WasmRegistryEntry>, StorageError> {
    let registry = write_txn.open_table(WASM_REGISTRY_V2)?;
    registry
        .get(id.as_bytes().as_slice())?
        .map(|value| serde_json::from_slice(value.value()).map_err(StorageError::from))
        .transpose()
}

fn chain_for_principal_from_write(
    write_txn: &redb::WriteTransaction,
    principal_id: Uuid,
    slot: PluginSlot,
) -> Result<Vec<PluginChainEntry>, StorageError> {
    let chains = write_txn.open_table(PLUGIN_CHAINS_V2)?;
    let mut entries = Vec::new();
    for row in chains.iter()? {
        let (_, value) = row?;
        let entry: PluginChainEntry = serde_json::from_slice(value.value())?;
        if entry.principal_id == principal_id && entry.slot == slot {
            entries.push(entry);
        }
    }
    Ok(entries)
}

fn validate_chain_orders(entries: &[PluginChainEntry]) -> Result<(), StorageError> {
    let mut ordered = entries.to_vec();
    ordered.sort_by_key(|entry| (entry.order, entry.id));
    let mut seen = HashSet::new();
    for entry in &ordered {
        if !seen.insert(entry.order) {
            return Err(StorageError::PluginRegistryConflict {
                message: "invalid_order_duplicate".to_owned(),
            });
        }
    }
    for pair in ordered.windows(2) {
        if pair[1].order - pair[0].order < 2 {
            return Err(StorageError::PluginRegistryConflict {
                message: "invalid_order_gap_below_2".to_owned(),
            });
        }
    }
    Ok(())
}

fn chain_by_id(
    write_txn: &redb::WriteTransaction,
    id: Uuid,
) -> Result<Option<PluginChainEntry>, StorageError> {
    let chains = write_txn.open_table(PLUGIN_CHAINS_V2)?;
    chains
        .get(id.as_bytes().as_slice())?
        .map(|value| serde_json::from_slice(value.value()).map_err(StorageError::from))
        .transpose()
}

fn put_chain(
    write_txn: &redb::WriteTransaction,
    entry: &PluginChainEntry,
) -> Result<(), StorageError> {
    let mut chains = write_txn.open_table(PLUGIN_CHAINS_V2)?;
    let payload = serde_json::to_vec(entry)?;
    chains.insert(entry.id.as_bytes().as_slice(), payload.as_slice())?;
    Ok(())
}

fn blob_refcount(
    table: &redb::ReadOnlyTable<&[u8], &[u8]>,
    sha256: [u8; 32],
) -> Result<i64, StorageError> {
    Ok(table
        .get(sha256.as_slice())?
        .map(|value| serde_json::from_slice::<StoredWasmBlob>(value.value()))
        .transpose()?
        .map(|blob| blob.refcount)
        .unwrap_or(0))
}

fn blob_refcount_from_write(
    table: &redb::Table<'_, &[u8], &[u8]>,
    sha256: [u8; 32],
) -> Result<i64, StorageError> {
    Ok(table
        .get(sha256.as_slice())?
        .map(|value| serde_json::from_slice::<StoredWasmBlob>(value.value()))
        .transpose()?
        .map(|blob| blob.refcount)
        .unwrap_or(0))
}

fn self_heal_blob_if_missing(
    write_txn: &redb::WriteTransaction,
    blob: &WasmBlob,
    registry_id: Uuid,
) -> Result<(), StorageError> {
    {
        let blobs = write_txn.open_table(WASM_BLOBS_V2)?;
        if blobs.get(blob.sha256.as_slice())?.is_some() {
            return Ok(());
        }
    }
    let refcount = {
        let chains = write_txn.open_table(PLUGIN_CHAINS_V2)?;
        let mut count: i64 = 0;
        for row in chains.iter()? {
            let (_, value) = row?;
            let entry: PluginChainEntry = serde_json::from_slice(value.value())?;
            if entry.wasm_registry_id == registry_id {
                count += 1;
            }
        }
        count
    };
    let stored = StoredWasmBlob {
        bytes: blob.bytes.clone(),
        size_bytes: blob.size_bytes,
        parse_validated_at_unix_secs: blob.parse_validated_at_unix_secs,
        refcount,
    };
    let mut blobs = write_txn.open_table(WASM_BLOBS_V2)?;
    blobs.insert(
        blob.sha256.as_slice(),
        serde_json::to_vec(&stored)?.as_slice(),
    )?;
    Ok(())
}

fn increment_blob(
    write_txn: &redb::WriteTransaction,
    sha256: [u8; 32],
) -> Result<(), StorageError> {
    let mut blobs = write_txn.open_table(WASM_BLOBS_V2)?;
    let mut blob: StoredWasmBlob = {
        let Some(value) = blobs.get(sha256.as_slice())? else {
            return Err(StorageError::PluginRegistryConflict {
                message: "missing wasm blob".to_owned(),
            });
        };
        serde_json::from_slice(value.value())?
    };
    blob.refcount += 1;
    let payload = serde_json::to_vec(&blob)?;
    blobs.insert(sha256.as_slice(), payload.as_slice())?;
    Ok(())
}

// Decrement chain refcount on the blob row. Never removes the row: blob
// lifetime is owned by the registry entry. Returns true iff refcount reached 0.
fn decrement_blob(
    write_txn: &redb::WriteTransaction,
    sha256: [u8; 32],
) -> Result<bool, StorageError> {
    let mut blobs = write_txn.open_table(WASM_BLOBS_V2)?;
    let mut blob: StoredWasmBlob = {
        let Some(value) = blobs.get(sha256.as_slice())? else {
            return Ok(false);
        };
        serde_json::from_slice(value.value())?
    };
    if blob.refcount > 0 {
        blob.refcount -= 1;
    }
    let became_orphan = blob.refcount == 0;
    let payload = serde_json::to_vec(&blob)?;
    blobs.insert(sha256.as_slice(), payload.as_slice())?;
    Ok(became_orphan)
}
