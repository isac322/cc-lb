use cc_lb_storage_api::{
    MAX_WASM_BLOB_BYTES, PluginChainEntry, PluginChainEntryInput, PluginChainEntryUpdate,
    PluginSlot, WasmBlob, WasmRegistryEntry, WasmRegistryEntryInput, sparse_order,
};
use redb::{ReadableDatabase, ReadableTable};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{PLUGIN_CHAINS_V2, RedbStorage, StorageError, WASM_BLOBS_V2, WASM_REGISTRY_V2};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredWasmBlob {
    sha256: [u8; 32],
    bytes: Vec<u8>,
    size_bytes: u64,
    parse_validated_at_unix_secs: u64,
    refcount: i64,
}

impl RedbStorage {
    pub(crate) fn persist_wasm_upload_sync(
        &self,
        blob: WasmBlob,
        input: WasmRegistryEntryInput,
    ) -> Result<WasmRegistryEntry, StorageError> {
        if blob.size_bytes > MAX_WASM_BLOB_BYTES || blob.bytes.len() as u64 > MAX_WASM_BLOB_BYTES {
            return Err(StorageError::PluginRegistryConflict {
                message: format!("wasm blob exceeds 32 MiB: {} bytes", blob.size_bytes),
            });
        }

        let write_txn = self.db.begin_write()?;
        let existing_by_sha = {
            let registry = write_txn.open_table(WASM_REGISTRY_V2)?;
            read_registry(&registry)?
                .into_iter()
                .find(|entry| entry.sha256 == blob.sha256)
        };
        if let Some(existing) = existing_by_sha {
            if existing.name == input.name
                && existing.original_filename == input.original_filename
                && existing.label == input.label
                && existing.uploaded_by_admin_id == input.uploaded_by_admin_id
            {
                return Ok(existing);
            }
            return Err(StorageError::PluginRegistryConflict {
                message: "sha256 already registered for a different wasm entry".to_owned(),
            });
        }
        {
            let registry = write_txn.open_table(WASM_REGISTRY_V2)?;
            if read_registry(&registry)?
                .into_iter()
                .any(|entry| entry.name == input.name)
            {
                return Err(StorageError::PluginRegistryConflict {
                    message: format!("plugin registry name already exists: {}", input.name),
                });
            }
        }

        let entry = WasmRegistryEntry {
            id: Uuid::new_v4(),
            sha256: blob.sha256,
            name: input.name,
            original_filename: input.original_filename,
            label: input.label,
            uploaded_at_unix_secs: input.uploaded_at_unix_secs,
            uploaded_by_admin_id: input.uploaded_by_admin_id,
            refcount: 0,
            revision: 0,
        };
        let stored_blob = StoredWasmBlob {
            sha256: blob.sha256,
            bytes: blob.bytes,
            size_bytes: blob.size_bytes,
            parse_validated_at_unix_secs: blob.parse_validated_at_unix_secs,
            refcount: 0,
        };
        {
            let mut blobs = write_txn.open_table(WASM_BLOBS_V2)?;
            blobs.insert(
                blob.sha256.as_slice(),
                serde_json::to_vec(&stored_blob)?.as_slice(),
            )?;
        }
        {
            let mut registry = write_txn.open_table(WASM_REGISTRY_V2)?;
            registry.insert(
                entry.id.as_bytes().as_slice(),
                serde_json::to_vec(&entry)?.as_slice(),
            )?;
        }
        write_txn.commit()?;
        Ok(entry)
    }

    pub(crate) fn get_blob_bytes_sync(
        &self,
        sha256: [u8; 32],
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let read_txn = self.db.begin_read()?;
        let blobs = read_txn.open_table(WASM_BLOBS_V2)?;
        blobs
            .get(sha256.as_slice())?
            .map(|stored| {
                serde_json::from_slice::<StoredWasmBlob>(stored.value())
                    .map(|blob| blob.bytes)
                    .map_err(StorageError::from)
            })
            .transpose()
    }

    pub(crate) fn list_registry_sync(
        &self,
        after: Option<Uuid>,
        limit: usize,
    ) -> Result<Vec<WasmRegistryEntry>, StorageError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let read_txn = self.db.begin_read()?;
        let registry = read_txn.open_table(WASM_REGISTRY_V2)?;
        let mut entries = read_registry(&registry)?;
        entries.sort_by(|left, right| left.name.cmp(&right.name).then(left.id.cmp(&right.id)));
        let start = after
            .and_then(|id| {
                entries
                    .iter()
                    .position(|entry| entry.id == id)
                    .map(|idx| idx + 1)
            })
            .unwrap_or(0);
        Ok(entries.into_iter().skip(start).take(limit).collect())
    }

    pub(crate) fn get_registry_entry_by_sha_sync(
        &self,
        sha256: [u8; 32],
    ) -> Result<Option<WasmRegistryEntry>, StorageError> {
        let read_txn = self.db.begin_read()?;
        let registry = read_txn.open_table(WASM_REGISTRY_V2)?;
        Ok(read_registry(&registry)?
            .into_iter()
            .find(|entry| entry.sha256 == sha256))
    }

    pub(crate) fn decrement_blob_refcount_or_delete_sync(
        &self,
        sha256: [u8; 32],
    ) -> Result<bool, StorageError> {
        let write_txn = self.db.begin_write()?;
        let Some(mut blob) = read_blob_from_txn(&write_txn, sha256)? else {
            return Ok(false);
        };
        if blob.refcount <= 1 {
            let mut blobs = write_txn.open_table(WASM_BLOBS_V2)?;
            blobs.remove(sha256.as_slice())?;
        } else {
            blob.refcount -= 1;
            let mut blobs = write_txn.open_table(WASM_BLOBS_V2)?;
            blobs.insert(sha256.as_slice(), serde_json::to_vec(&blob)?.as_slice())?;
        }
        write_txn.commit()?;
        Ok(true)
    }

    pub(crate) fn insert_chain_entry_sync(
        &self,
        input: PluginChainEntryInput,
    ) -> Result<PluginChainEntry, StorageError> {
        let write_txn = self.db.begin_write()?;
        let registry = read_registry_entry_from_txn(&write_txn, input.wasm_registry_id)?
            .ok_or_else(|| StorageError::PluginRegistryConflict {
                message: format!("unknown plugin registry entry {}", input.wasm_registry_id),
            })?;
        let entry = PluginChainEntry {
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
        };
        increment_blob_refcount_in_txn(&write_txn, registry.sha256)?;
        {
            let mut chains = write_txn.open_table(PLUGIN_CHAINS_V2)?;
            chains.insert(
                entry.id.as_bytes().as_slice(),
                serde_json::to_vec(&entry)?.as_slice(),
            )?;
        }
        write_txn.commit()?;
        Ok(entry)
    }

    pub(crate) fn list_chain_for_principal_sync(
        &self,
        principal_id: Uuid,
        slot: PluginSlot,
    ) -> Result<Vec<PluginChainEntry>, StorageError> {
        let read_txn = self.db.begin_read()?;
        let chains = read_txn.open_table(PLUGIN_CHAINS_V2)?;
        let mut entries: Vec<_> = read_chains(&chains)?
            .into_iter()
            .filter(|entry| entry.principal_id == principal_id && entry.slot == slot)
            .collect();
        entries.sort_by_key(|entry| (entry.order, entry.id));
        Ok(entries)
    }

    pub(crate) fn update_chain_entry_sync(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: PluginChainEntryUpdate,
    ) -> Result<Option<PluginChainEntry>, StorageError> {
        self.mutate_chain(id, Some(expected_revision), |entry| {
            if let Some(config) = update.config {
                entry.config = config;
            }
            if let Some(sse_per_event) = update.sse_per_event {
                entry.sse_per_event = sse_per_event;
            }
            if let Some(value) = update.batched_events_per_flush {
                entry.batched_events_per_flush = value;
            }
            if let Some(value) = update.batched_flush_ms {
                entry.batched_flush_ms = value;
            }
            Ok(())
        })
    }

    pub(crate) fn reorder_chain_sync(
        &self,
        principal_id: Uuid,
        slot: PluginSlot,
        new_orders: Vec<(Uuid, i64)>,
    ) -> Result<Vec<PluginChainEntry>, StorageError> {
        if sparse_order::needs_rebalance(
            &new_orders
                .iter()
                .map(|(_, order)| *order)
                .collect::<Vec<_>>(),
        ) {
            return Err(StorageError::PluginRegistryConflict {
                message: "plugin chain order gaps need rebalance".to_owned(),
            });
        }
        let write_txn = self.db.begin_write()?;
        let mut chains = read_chains(&write_txn.open_table(PLUGIN_CHAINS_V2)?)?;
        for (id, order) in new_orders {
            let Some(entry) = chains.iter_mut().find(|entry| entry.id == id) else {
                return Err(StorageError::PluginRegistryConflict {
                    message: format!("unknown plugin chain entry {id}"),
                });
            };
            if entry.principal_id != principal_id || entry.slot != slot {
                return Err(StorageError::PluginRegistryConflict {
                    message: "plugin chain reorder entry outside principal/slot".to_owned(),
                });
            }
            entry.order = order;
            entry.revision = entry
                .revision
                .checked_add(1)
                .ok_or(StorageError::PluginChainRevisionOverflow)?;
        }
        let selected = write_chains_and_select(&write_txn, chains, principal_id, slot)?;
        write_txn.commit()?;
        Ok(selected)
    }

    pub(crate) fn delete_chain_entry_sync(&self, id: Uuid) -> Result<bool, StorageError> {
        let write_txn = self.db.begin_write()?;
        let Some(entry) = read_chain_entry_from_txn(&write_txn, id)? else {
            return Ok(false);
        };
        let registry = read_registry_entry_from_txn(&write_txn, entry.wasm_registry_id)?
            .ok_or_else(|| StorageError::PluginRegistryConflict {
                message: format!("missing registry entry {}", entry.wasm_registry_id),
            })?;
        decrement_blob_refcount_in_txn(&write_txn, registry.sha256)?;
        {
            let mut chains = write_txn.open_table(PLUGIN_CHAINS_V2)?;
            chains.remove(id.as_bytes().as_slice())?;
        }
        write_txn.commit()?;
        Ok(true)
    }

    pub(crate) fn rebalance_chain_sync(
        &self,
        principal_id: Uuid,
        slot: PluginSlot,
    ) -> Result<Vec<PluginChainEntry>, StorageError> {
        let write_txn = self.db.begin_write()?;
        let mut chains = read_chains(&write_txn.open_table(PLUGIN_CHAINS_V2)?)?;
        let mut selected_indices: Vec<_> = chains
            .iter()
            .enumerate()
            .filter_map(|(idx, entry)| {
                (entry.principal_id == principal_id && entry.slot == slot).then_some(idx)
            })
            .collect();
        selected_indices.sort_by_key(|idx| (chains[*idx].order, chains[*idx].id));
        let new_orders = sparse_order::rebalance(vec![0; selected_indices.len()]);
        for (idx, order) in selected_indices.into_iter().zip(new_orders) {
            chains[idx].order = order;
            chains[idx].revision = chains[idx]
                .revision
                .checked_add(1)
                .ok_or(StorageError::PluginChainRevisionOverflow)?;
        }
        let selected = write_chains_and_select(&write_txn, chains, principal_id, slot)?;
        write_txn.commit()?;
        Ok(selected)
    }

    fn mutate_chain<F>(
        &self,
        id: Uuid,
        expected_revision: Option<u64>,
        mutate: F,
    ) -> Result<Option<PluginChainEntry>, StorageError>
    where
        F: FnOnce(&mut PluginChainEntry) -> Result<(), StorageError>,
    {
        let write_txn = self.db.begin_write()?;
        let Some(mut entry) = read_chain_entry_from_txn(&write_txn, id)? else {
            return Ok(None);
        };
        if let Some(expected_revision) = expected_revision {
            if entry.revision != expected_revision {
                return Err(StorageError::StalePluginChainRevision {
                    current: entry.revision,
                });
            }
        }
        mutate(&mut entry)?;
        entry.revision = entry
            .revision
            .checked_add(1)
            .ok_or(StorageError::PluginChainRevisionOverflow)?;
        {
            let mut chains = write_txn.open_table(PLUGIN_CHAINS_V2)?;
            chains.insert(
                id.as_bytes().as_slice(),
                serde_json::to_vec(&entry)?.as_slice(),
            )?;
        }
        write_txn.commit()?;
        Ok(Some(entry))
    }
}

fn read_registry(
    table: &redb::Table<'_, &[u8], &[u8]>,
) -> Result<Vec<WasmRegistryEntry>, StorageError> {
    let mut entries = Vec::new();
    for row in table.iter()? {
        let (_, value) = row?;
        entries.push(serde_json::from_slice(value.value())?);
    }
    Ok(entries)
}

fn read_chains(
    table: &redb::Table<'_, &[u8], &[u8]>,
) -> Result<Vec<PluginChainEntry>, StorageError> {
    let mut entries = Vec::new();
    for row in table.iter()? {
        let (_, value) = row?;
        entries.push(serde_json::from_slice(value.value())?);
    }
    Ok(entries)
}

fn read_registry_entry_from_txn(
    txn: &redb::WriteTransaction,
    id: Uuid,
) -> Result<Option<WasmRegistryEntry>, StorageError> {
    let registry = txn.open_table(WASM_REGISTRY_V2)?;
    registry
        .get(id.as_bytes().as_slice())?
        .map(|stored| serde_json::from_slice(stored.value()).map_err(StorageError::from))
        .transpose()
}

fn read_chain_entry_from_txn(
    txn: &redb::WriteTransaction,
    id: Uuid,
) -> Result<Option<PluginChainEntry>, StorageError> {
    let chains = txn.open_table(PLUGIN_CHAINS_V2)?;
    chains
        .get(id.as_bytes().as_slice())?
        .map(|stored| serde_json::from_slice(stored.value()).map_err(StorageError::from))
        .transpose()
}

fn read_blob_from_txn(
    txn: &redb::WriteTransaction,
    sha256: [u8; 32],
) -> Result<Option<StoredWasmBlob>, StorageError> {
    let blobs = txn.open_table(WASM_BLOBS_V2)?;
    blobs
        .get(sha256.as_slice())?
        .map(|stored| serde_json::from_slice(stored.value()).map_err(StorageError::from))
        .transpose()
}

fn increment_blob_refcount_in_txn(
    txn: &redb::WriteTransaction,
    sha256: [u8; 32],
) -> Result<(), StorageError> {
    let mut blob =
        read_blob_from_txn(txn, sha256)?.ok_or_else(|| StorageError::PluginRegistryConflict {
            message: "missing wasm blob".to_owned(),
        })?;
    blob.refcount += 1;
    let mut blobs = txn.open_table(WASM_BLOBS_V2)?;
    blobs.insert(sha256.as_slice(), serde_json::to_vec(&blob)?.as_slice())?;
    Ok(())
}

fn decrement_blob_refcount_in_txn(
    txn: &redb::WriteTransaction,
    sha256: [u8; 32],
) -> Result<(), StorageError> {
    let Some(mut blob) = read_blob_from_txn(txn, sha256)? else {
        return Ok(());
    };
    let mut blobs = txn.open_table(WASM_BLOBS_V2)?;
    if blob.refcount <= 1 {
        blobs.remove(sha256.as_slice())?;
    } else {
        blob.refcount -= 1;
        blobs.insert(sha256.as_slice(), serde_json::to_vec(&blob)?.as_slice())?;
    }
    Ok(())
}

fn write_chains_and_select(
    txn: &redb::WriteTransaction,
    chains: Vec<PluginChainEntry>,
    principal_id: Uuid,
    slot: PluginSlot,
) -> Result<Vec<PluginChainEntry>, StorageError> {
    let mut selected = Vec::new();
    let mut table = txn.open_table(PLUGIN_CHAINS_V2)?;
    for entry in chains {
        if entry.principal_id == principal_id && entry.slot == slot {
            selected.push(entry.clone());
        }
        table.insert(
            entry.id.as_bytes().as_slice(),
            serde_json::to_vec(&entry)?.as_slice(),
        )?;
    }
    selected.sort_by_key(|entry| (entry.order, entry.id));
    Ok(selected)
}
