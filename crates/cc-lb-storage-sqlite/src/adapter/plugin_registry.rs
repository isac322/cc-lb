use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use cc_lb_storage_api::{
    AugmentedMetadata, BUILTIN_CACHE_AFFINITY_ID, BUILTIN_CACHE_AFFINITY_NAME,
    BUILTIN_CACHE_AFFINITY_SHA256, MAX_WASM_BLOB_BYTES, PluginBlobRepo, PluginChainConflictReason,
    PluginChainEntry, PluginChainEntryInput, PluginChainEntryUpdate, PluginRegistryRecord,
    PluginRegistryRepo, PluginRegistryStatus, PluginRegistryStore, PluginSlot, RepoError,
    StorageError, StorageResult, WasmBlob, WasmBlobRecord, WasmRegistryEntry,
    WasmRegistryEntryInput, default_wire_version, sparse_order, validate_identifier,
};
use serde_json::Value;
use sqlx::{Row, Sqlite, Transaction, sqlite::SqliteRow};
use uuid::Uuid;

use crate::{SqliteStorage, map_sqlx_error};

const SHUTDOWN_MARKER_KEY: &str = "shutdown";
const LIST_REGISTRY_SQL: &str = "SELECT wasm_registry_v2.*, (SELECT COUNT(*) FROM plugin_chains_v2 WHERE wasm_sha256 = wasm_registry_v2.sha256) AS refcount FROM wasm_registry_v2 WHERE (? IS NULL OR wasm_registry_v2.sha256 > ?) ORDER BY wasm_registry_v2.sha256 ASC LIMIT ?";
const GET_REGISTRY_BY_SHA_SQL: &str = "SELECT wasm_registry_v2.*, (SELECT COUNT(*) FROM plugin_chains_v2 WHERE wasm_sha256 = wasm_registry_v2.sha256) AS refcount FROM wasm_registry_v2 WHERE wasm_registry_v2.sha256 = ?";

#[async_trait]
impl PluginRegistryStore for SqliteStorage {
    async fn persist_wasm_upload(
        &self,
        blob: WasmBlob,
        input: WasmRegistryEntryInput,
    ) -> StorageResult<(WasmRegistryEntry, bool)> {
        validate_identifier("plugin.name", &input.name)?;
        validate_blob(&blob)?;

        let mut tx = self.pool().begin().await.map_err(map_sqlx_error)?;
        let now = unix_secs_to_i64(blob.parse_validated_at_unix_secs, "wasm_blob.created_at")?;
        let blob_insert = sqlx::query(
            "INSERT INTO wasm_blobs_v2 (sha256, bytes, created_at) VALUES (?, ?, ?) ON CONFLICT(sha256) DO NOTHING",
        )
        .bind(blob.sha256.as_slice())
        .bind(blob.bytes.as_slice())
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlite_error)?;

        let metadata = metadata_from_input(&input)?;
        let registry_insert = sqlx::query(
            "INSERT INTO wasm_registry_v2 (sha256, plugin_name, plugin_version, abi_envelope, augmented_metadata, host_offer_hash, handshake_schema_version, last_handshake_at, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?) ON CONFLICT(sha256) DO NOTHING",
        )
        .bind(blob.sha256.as_slice())
        .bind(&input.name)
        .bind(&input.original_filename)
        .bind(i64::from(input.wire_version))
        .bind(serde_json::to_string(&metadata)?)
        .bind(blob.sha256.as_slice())
        .bind(1_i64)
        .bind(unix_secs_to_i64(input.uploaded_at_unix_secs, "wasm_registry.uploaded_at")?)
        .bind(unix_secs_to_i64(input.uploaded_at_unix_secs, "wasm_registry.created_at")?)
        .bind(unix_secs_to_i64(input.uploaded_at_unix_secs, "wasm_registry.updated_at")?)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlite_error)?;

        let existed = registry_insert.rows_affected() == 0;
        if existed {
            let existing = registry_by_sha_in_tx(&mut tx, blob.sha256)
                .await?
                .ok_or_else(|| StorageError::Fatal {
                    message: "wasm registry row missing after upload".to_owned(),
                })?;
            if !same_wasm_entry_metadata(&existing, &input) {
                return Err(StorageError::Conflict {
                    message: "sha256 already registered for a different wasm entry".to_owned(),
                });
            }
        } else {
            let id = Uuid::new_v4();
            put_marker_in_tx(&mut tx, &registry_id_key(blob.sha256), &id.to_string()).await?;
            put_marker_in_tx(&mut tx, &registry_sha_key(id), &sha_hex(&blob.sha256)).await?;
            put_marker_in_tx(
                &mut tx,
                &registry_label_key(blob.sha256),
                &optional_string(&input.label)?,
            )
            .await?;
            put_marker_in_tx(
                &mut tx,
                &registry_uploaded_by_key(blob.sha256),
                &input.uploaded_by_admin_id.to_string(),
            )
            .await?;
            put_marker_in_tx(&mut tx, &registry_revision_key(blob.sha256), "0").await?;
            put_marker_in_tx(
                &mut tx,
                &registry_wire_version_key(blob.sha256),
                &input.wire_version.to_string(),
            )
            .await?;
            put_marker_in_tx(
                &mut tx,
                &registry_supported_slots_key(blob.sha256),
                &slots_to_json(&input.supported_slots)?,
            )
            .await?;
        }

        let entry = registry_by_sha_in_tx(&mut tx, blob.sha256)
            .await?
            .ok_or_else(|| StorageError::Fatal {
                message: "wasm registry row missing after upload".to_owned(),
            })?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok((entry, existed && blob_insert.rows_affected() == 0))
    }

    async fn get_blob(&self, sha256: [u8; 32]) -> StorageResult<Option<WasmBlobRecord>> {
        let row =
            sqlx::query_scalar::<_, Vec<u8>>("SELECT sha256 FROM wasm_blobs_v2 WHERE sha256 = ?")
                .bind(sha256.as_slice())
                .fetch_optional(self.pool())
                .await
                .map_err(map_sqlx_error)?;

        row.map(|bytes| {
            Ok(WasmBlobRecord {
                sha256: sha_to_array(&bytes)?,
            })
        })
        .transpose()
    }

    async fn get_blob_bytes(&self, sha256: [u8; 32]) -> StorageResult<Option<Vec<u8>>> {
        sqlx::query_scalar::<_, Vec<u8>>("SELECT bytes FROM wasm_blobs_v2 WHERE sha256 = ?")
            .bind(sha256.as_slice())
            .fetch_optional(self.pool())
            .await
            .map_err(map_sqlx_error)
    }

    async fn list_orphan_blobs(&self) -> StorageResult<Vec<[u8; 32]>> {
        let rows = sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT sha256 FROM wasm_blobs_v2 WHERE sha256 NOT IN (SELECT sha256 FROM wasm_registry_v2) ORDER BY sha256 ASC",
        )
        .fetch_all(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        rows.into_iter().map(|row| sha_to_array(&row)).collect()
    }

    async fn list_registry(
        &self,
        after: Option<Uuid>,
        limit: usize,
    ) -> StorageResult<Vec<WasmRegistryEntry>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let after_sha = match after {
            Some(id) => self.sha_for_registry_id(id).await?,
            None => None,
        }
        .map(|sha| sha.to_vec());
        let rows = sqlx::query(LIST_REGISTRY_SQL)
            .bind(after_sha.clone())
            .bind(after_sha)
            .bind(usize_to_i64(limit, "plugin_registry.limit")?)
            .fetch_all(self.pool())
            .await
            .map_err(map_sqlx_error)?;
        let mut entries = Vec::with_capacity(rows.len());
        for row in rows {
            entries.push(self.registry_from_row(row).await?);
        }
        Ok(entries)
    }

    async fn get_registry_entry_by_sha(
        &self,
        sha256: [u8; 32],
    ) -> StorageResult<Option<WasmRegistryEntry>> {
        let row = sqlx::query(GET_REGISTRY_BY_SHA_SQL)
            .bind(sha256.as_slice())
            .fetch_optional(self.pool())
            .await
            .map_err(map_sqlx_error)?;
        match row {
            Some(row) => Ok(Some(self.registry_from_row(row).await?)),
            None => Ok(None),
        }
    }

    async fn get_registry_entry_by_id(&self, id: Uuid) -> StorageResult<Option<WasmRegistryEntry>> {
        let Some(sha256) = self.sha_for_registry_id(id).await? else {
            return Ok(None);
        };
        self.get_registry_entry_by_sha(sha256).await
    }

    async fn update_registry_label(
        &self,
        id: Uuid,
        expected_revision: u64,
        label: Option<String>,
    ) -> StorageResult<WasmRegistryEntry> {
        let mut tx = self.pool().begin().await.map_err(map_sqlx_error)?;
        let sha256 = sha_for_registry_id_in_tx(&mut tx, id)
            .await?
            .ok_or_else(|| conflict("unknown plugin registry entry"))?;
        let current = registry_revision_in_tx(&mut tx, sha256).await?;
        if current != expected_revision {
            return Err(StorageError::StalePluginRegistryRevision { current });
        }
        put_marker_in_tx(
            &mut tx,
            &registry_label_key(sha256),
            &optional_string(&label)?,
        )
        .await?;
        put_marker_in_tx(
            &mut tx,
            &registry_revision_key(sha256),
            &(current + 1).to_string(),
        )
        .await?;
        sqlx::query("UPDATE wasm_registry_v2 SET updated_at = unixepoch() WHERE sha256 = ?")
            .bind(sha256.as_slice())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)?;
        self.get_registry_entry_by_sha(sha256)
            .await?
            .ok_or_else(|| conflict("updated plugin registry row disappeared"))
    }

    async fn update_supported_slots(
        &self,
        id: Uuid,
        supported_slots: Vec<PluginSlot>,
    ) -> StorageResult<()> {
        let sha256 = self
            .sha_for_registry_id(id)
            .await?
            .ok_or_else(|| conflict("unknown plugin registry entry"))?;
        sqlx::query(
            "INSERT INTO plugin_registry_marker_v1 (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .bind(registry_supported_slots_key(sha256))
        .bind(slots_to_json(&supported_slots)?)
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn update_wire_version(&self, id: Uuid, wire_version: u8) -> StorageResult<()> {
        let sha256 = self
            .sha_for_registry_id(id)
            .await?
            .ok_or_else(|| conflict("unknown plugin registry entry"))?;
        sqlx::query(
            "INSERT INTO plugin_registry_marker_v1 (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .bind(registry_wire_version_key(sha256))
        .bind(wire_version.to_string())
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn delete_registry_entry(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> StorageResult<Option<WasmRegistryEntry>> {
        let mut tx = self.pool().begin().await.map_err(map_sqlx_error)?;
        let Some(sha256) = sha_for_registry_id_in_tx(&mut tx, id).await? else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(None);
        };
        let entry = registry_by_sha_in_tx(&mut tx, sha256)
            .await?
            .ok_or_else(|| conflict("unknown plugin registry entry"))?;
        if entry.revision != expected_revision {
            return Err(StorageError::StalePluginRegistryRevision {
                current: entry.revision,
            });
        }
        if let Some(chain_id) = sqlx::query_scalar::<_, String>(
            "SELECT id FROM plugin_chains_v2 WHERE wasm_sha256 = ? LIMIT 1",
        )
        .bind(sha256.as_slice())
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx_error)?
        {
            return Err(StorageError::PluginRegistryReferenced { id: chain_id });
        }
        if let Some(upstream_id) = warmup_reference_in_tx(&mut tx, id).await? {
            return Err(StorageError::PluginRegistryReferenced {
                id: upstream_id.to_string(),
            });
        }
        sqlx::query("DELETE FROM wasm_registry_v2 WHERE sha256 = ?")
            .bind(sha256.as_slice())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        sqlx::query("DELETE FROM wasm_blobs_v2 WHERE sha256 = ?")
            .bind(sha256.as_slice())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        delete_registry_markers_in_tx(&mut tx, sha256, id).await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(Some(entry))
    }

    async fn decrement_blob_refcount_or_delete(&self, sha256: [u8; 32]) -> StorageResult<bool> {
        let mut tx = self.pool().begin().await.map_err(map_sqlx_error)?;
        let referenced: Option<Vec<u8>> =
            sqlx::query_scalar("SELECT sha256 FROM wasm_registry_v2 WHERE sha256 = ? LIMIT 1")
                .bind(sha256.as_slice())
                .fetch_optional(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
        let removed = if referenced.is_some() {
            false
        } else {
            sqlx::query("DELETE FROM wasm_blobs_v2 WHERE sha256 = ?")
                .bind(sha256.as_slice())
                .execute(&mut *tx)
                .await
                .map_err(map_sqlx_error)?
                .rows_affected()
                > 0
        };
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(removed)
    }

    async fn insert_chain_entry(
        &self,
        input: PluginChainEntryInput,
    ) -> StorageResult<PluginChainEntry> {
        let sha256 = self
            .sha_for_registry_id(input.wasm_registry_id)
            .await?
            .ok_or_else(|| StorageError::PluginRegistryConflict {
                message: "unknown plugin registry entry".to_owned(),
            })?;
        let principal_exists: Option<i64> =
            sqlx::query_scalar("SELECT 1 FROM principals_v1 WHERE id = ?")
                .bind(input.principal_id.to_string())
                .fetch_optional(self.pool())
                .await
                .map_err(map_sqlx_error)?;
        if principal_exists.is_none() {
            return Err(StorageError::PrincipalNotFound {
                id: input.principal_id.to_string(),
            });
        }
        if is_singleton_slot(input.slot)
            && let Some(existing_entry_id) = sqlx::query_scalar::<_, String>(
                "SELECT id FROM plugin_chains_v2 WHERE principal_id = ? AND slot = ? ORDER BY id ASC LIMIT 1",
            )
            .bind(input.principal_id.to_string())
            .bind(input.slot.as_str())
            .fetch_optional(self.pool())
            .await
            .map_err(map_sqlx_error)?
        {
            return Err(StorageError::PluginChainConflict {
                reason: PluginChainConflictReason::SlotIsSingleton {
                    existing_entry_id: parse_uuid(&existing_entry_id, "plugin_chain.id")?,
                },
            });
        }
        let id = Uuid::new_v4();
        let config = chain_config_to_json(&input)?;
        let row = sqlx::query(
            "INSERT INTO plugin_chains_v2 (id, principal_id, slot, wasm_sha256, order_index, config, revision, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, 0, unixepoch(), unixepoch()) RETURNING *",
        )
        .bind(id.to_string())
        .bind(input.principal_id.to_string())
        .bind(input.slot.as_str())
        .bind(sha256.as_slice())
        .bind(input.order)
        .bind(config)
        .fetch_one(self.pool())
        .await
        .map_err(map_sqlite_error)?;
        let entry = chain_from_row(row)?;
        Ok(entry)
    }

    async fn list_chain_for_principal(
        &self,
        principal_id: Uuid,
        slot: PluginSlot,
    ) -> StorageResult<Vec<PluginChainEntry>> {
        let rows = sqlx::query(
            "SELECT * FROM plugin_chains_v2 WHERE principal_id = ? AND slot = ? ORDER BY order_index ASC, id ASC",
        )
        .bind(principal_id.to_string())
        .bind(slot.as_str())
        .fetch_all(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        rows.into_iter().map(chain_from_row).collect()
    }

    async fn update_chain_entry(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: PluginChainEntryUpdate,
    ) -> StorageResult<Option<PluginChainEntry>> {
        if update.config.is_none()
            && update.sse_per_event.is_none()
            && update.batched_events_per_flush.is_none()
            && update.batched_flush_ms.is_none()
        {
            return Err(StorageError::InvalidInput {
                field: "plugin_chain.update".to_owned(),
                reason: "empty_update".to_owned(),
            });
        }
        let mut current = match self.get_chain_by_id(id).await? {
            Some(current) => current,
            None => return Ok(None),
        };
        if current.revision != expected_revision {
            return Err(StorageError::StalePluginChainRevision {
                current: current.revision,
            });
        }
        if let Some(value) = update.config {
            current.config = value;
        }
        if let Some(value) = update.sse_per_event {
            current.sse_per_event = value;
        }
        if let Some(value) = update.batched_events_per_flush {
            current.batched_events_per_flush = value;
        }
        if let Some(value) = update.batched_flush_ms {
            current.batched_flush_ms = value;
        }
        let config = serde_json::to_string(&StoredChainConfig::from_entry(&current))?;
        let row = sqlx::query(
            "UPDATE plugin_chains_v2 SET config = ?, revision = revision + 1, updated_at = unixepoch() WHERE id = ? AND revision = ? RETURNING *",
        )
        .bind(config)
        .bind(id.to_string())
        .bind(u64_to_i64(expected_revision, "plugin_chain.revision")?)
        .fetch_optional(self.pool())
        .await
        .map_err(map_sqlite_error)?;
        row.map(chain_from_row).transpose()
    }

    async fn reorder_chain(
        &self,
        principal_id: Uuid,
        slot: PluginSlot,
        new_orders: Vec<(Uuid, i64, u64)>,
    ) -> StorageResult<Vec<PluginChainEntry>> {
        let mut tx = self.pool().begin().await.map_err(map_sqlx_error)?;
        let mut staged_orders = HashMap::with_capacity(new_orders.len());
        let mut seen_ids = HashSet::with_capacity(new_orders.len());
        for (id, order, expected_revision) in &new_orders {
            if !seen_ids.insert(*id) {
                return Err(StorageError::Conflict {
                    message: "duplicate_plugin_chain_entry".to_owned(),
                });
            }
            let row = sqlx::query("SELECT * FROM plugin_chains_v2 WHERE id = ?")
                .bind(id.to_string())
                .fetch_optional(&mut *tx)
                .await
                .map_err(map_sqlx_error)?
                .ok_or_else(|| StorageError::Conflict {
                    message: "unknown plugin chain entry".to_owned(),
                })?;
            let entry = chain_from_row(row)?;
            if entry.principal_id != principal_id || entry.slot != slot {
                return Err(StorageError::Conflict {
                    message: "plugin chain entry is not in requested chain".to_owned(),
                });
            }
            if entry.revision != *expected_revision {
                return Err(StorageError::StalePluginChainRevision {
                    current: entry.revision,
                });
            }
            staged_orders.insert(*id, *order);
        }

        let rows = sqlx::query(
            "SELECT * FROM plugin_chains_v2 WHERE principal_id = ? AND slot = ? ORDER BY order_index ASC, id ASC",
        )
        .bind(principal_id.to_string())
        .bind(slot.as_str())
        .fetch_all(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        let mut entries = rows
            .into_iter()
            .map(chain_from_row)
            .collect::<StorageResult<Vec<_>>>()?;
        for entry in &mut entries {
            if let Some(order) = staged_orders.get(&entry.id) {
                entry.order = *order;
            }
        }
        entries.sort_by_key(|entry| (entry.order, entry.id));
        if entries
            .windows(2)
            .any(|pair| pair[1].order.checked_sub(pair[0].order).unwrap_or(i64::MAX) < 2)
        {
            return Err(StorageError::PluginChainConflict {
                reason: PluginChainConflictReason::InvalidOrderGap,
            });
        }
        for (id, order, expected_revision) in new_orders {
            sqlx::query(
                "UPDATE plugin_chains_v2 SET order_index = ?, revision = revision + 1, updated_at = unixepoch() WHERE id = ? AND revision = ?",
            )
            .bind(order)
            .bind(id.to_string())
            .bind(u64_to_i64(expected_revision, "plugin_chain.revision")?)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlite_error)?;
        }
        let rows = sqlx::query(
            "SELECT * FROM plugin_chains_v2 WHERE principal_id = ? AND slot = ? ORDER BY order_index ASC, id ASC",
        )
        .bind(principal_id.to_string())
        .bind(slot.as_str())
        .fetch_all(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)?;
        rows.into_iter().map(chain_from_row).collect()
    }

    async fn delete_chain_entry(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> StorageResult<Option<PluginChainEntry>> {
        let Some(entry) = self.get_chain_by_id(id).await? else {
            return Ok(None);
        };
        if entry.revision != expected_revision {
            return Err(StorageError::StalePluginChainRevision {
                current: entry.revision,
            });
        }
        let result = sqlx::query("DELETE FROM plugin_chains_v2 WHERE id = ? AND revision = ?")
            .bind(id.to_string())
            .bind(u64_to_i64(expected_revision, "plugin_chain.revision")?)
            .execute(self.pool())
            .await
            .map_err(map_sqlite_error)?;
        if result.rows_affected() == 0 {
            return Err(StorageError::StalePluginChainRevision {
                current: entry.revision,
            });
        }
        Ok(Some(entry))
    }

    async fn rebalance_chain(
        &self,
        principal_id: Uuid,
        slot: PluginSlot,
    ) -> StorageResult<Vec<PluginChainEntry>> {
        let entries = self.list_chain_for_principal(principal_id, slot).await?;
        let orders = sparse_order::rebalance(vec![0; entries.len()]);
        self.reorder_chain(
            principal_id,
            slot,
            entries
                .into_iter()
                .zip(orders)
                .map(|(entry, order)| (entry.id, order, entry.revision))
                .collect(),
        )
        .await
    }
}

#[async_trait]
impl PluginRegistryRepo for SqliteStorage {
    async fn upsert_record(&self, record: &PluginRegistryRecord) -> Result<(), RepoError> {
        sqlx::query("INSERT INTO wasm_blobs_v2 (sha256, bytes, created_at) VALUES (?, x'', unixepoch()) ON CONFLICT(sha256) DO NOTHING")
            .bind(record.sha256.as_slice())
            .execute(self.pool())
            .await
            .map_err(map_sqlite_error)?;
        sqlx::query(
            "INSERT INTO wasm_registry_v2 (sha256, plugin_name, plugin_version, abi_envelope, augmented_metadata, host_offer_hash, handshake_schema_version, last_handshake_at, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, unixepoch(), unixepoch()) ON CONFLICT(sha256) DO UPDATE SET plugin_name = excluded.plugin_name, plugin_version = excluded.plugin_version, abi_envelope = excluded.abi_envelope, augmented_metadata = excluded.augmented_metadata, host_offer_hash = excluded.host_offer_hash, handshake_schema_version = excluded.handshake_schema_version, last_handshake_at = excluded.last_handshake_at, status = excluded.status, updated_at = unixepoch()",
        )
        .bind(record.sha256.as_slice())
        .bind(&record.plugin_name)
        .bind(&record.plugin_version)
        .bind(i64::from(record.abi_envelope))
        .bind(serde_json::to_string(&record.augmented_metadata)?)
        .bind(record.host_offer_hash.as_slice())
        .bind(i64::from(record.handshake_schema_version))
        .bind(record.last_handshake_at)
        .bind(status_as_str(record.status))
        .execute(self.pool())
        .await
        .map_err(map_sqlite_error)?;
        Ok(())
    }

    async fn get_by_sha256(
        &self,
        sha256: &[u8; 32],
    ) -> Result<Option<PluginRegistryRecord>, RepoError> {
        let row = sqlx::query("SELECT * FROM wasm_registry_v2 WHERE sha256 = ?")
            .bind(sha256.as_slice())
            .fetch_optional(self.pool())
            .await
            .map_err(map_sqlx_error)?;
        row.map(record_from_row).transpose()
    }

    async fn list_active(&self) -> Result<Vec<PluginRegistryRecord>, RepoError> {
        let rows = sqlx::query(
            "SELECT * FROM wasm_registry_v2 WHERE status = 'active' ORDER BY plugin_name ASC, sha256 ASC",
        )
        .fetch_all(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        rows.into_iter().map(record_from_row).collect()
    }

    async fn set_status(
        &self,
        sha256: &[u8; 32],
        status: PluginRegistryStatus,
    ) -> Result<(), RepoError> {
        sqlx::query(
            "UPDATE wasm_registry_v2 SET status = ?, updated_at = unixepoch() WHERE sha256 = ?",
        )
        .bind(status_as_str(status))
        .bind(sha256.as_slice())
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn delete_by_sha256(&self, sha256: &[u8; 32]) -> Result<(), RepoError> {
        sqlx::query("DELETE FROM wasm_registry_v2 WHERE sha256 = ?")
            .bind(sha256.as_slice())
            .execute(self.pool())
            .await
            .map_err(map_sqlite_error)?;
        Ok(())
    }

    async fn count(&self) -> Result<usize, RepoError> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM wasm_registry_v2")
            .fetch_one(self.pool())
            .await
            .map_err(map_sqlx_error)?;
        usize::try_from(count).map_err(|_| StorageError::Corrupted {
            message: "wasm_registry_v2 count cannot fit usize".to_owned(),
        })
    }

    async fn get_shutdown_marker(&self) -> Result<Option<i64>, RepoError> {
        let value: Option<String> =
            sqlx::query_scalar("SELECT value FROM plugin_registry_marker_v1 WHERE key = ?")
                .bind(SHUTDOWN_MARKER_KEY)
                .fetch_optional(self.pool())
                .await
                .map_err(map_sqlx_error)?;
        value
            .map(|value| parse_i64(&value, "shutdown marker"))
            .transpose()
    }

    async fn set_shutdown_marker(&self, unix_secs: i64) -> Result<(), RepoError> {
        sqlx::query(
            "INSERT INTO plugin_registry_marker_v1 (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .bind(SHUTDOWN_MARKER_KEY)
        .bind(unix_secs.to_string())
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn clear_shutdown_marker(&self) -> Result<(), RepoError> {
        sqlx::query("DELETE FROM plugin_registry_marker_v1 WHERE key = ?")
            .bind(SHUTDOWN_MARKER_KEY)
            .execute(self.pool())
            .await
            .map_err(map_sqlx_error)?;
        Ok(())
    }
}

#[async_trait]
impl PluginBlobRepo for SqliteStorage {
    async fn put_blob(&self, sha256: &[u8; 32], bytes: &[u8]) -> Result<(), RepoError> {
        sqlx::query(
            "INSERT INTO wasm_blobs_v2 (sha256, bytes, created_at) VALUES (?, ?, unixepoch()) ON CONFLICT(sha256) DO UPDATE SET bytes = excluded.bytes",
        )
        .bind(sha256.as_slice())
        .bind(bytes)
        .execute(self.pool())
        .await
        .map_err(map_sqlite_error)?;
        Ok(())
    }

    async fn get_blob(&self, sha256: &[u8; 32]) -> Result<Option<Vec<u8>>, RepoError> {
        sqlx::query_scalar("SELECT bytes FROM wasm_blobs_v2 WHERE sha256 = ?")
            .bind(sha256.as_slice())
            .fetch_optional(self.pool())
            .await
            .map_err(map_sqlx_error)
    }

    async fn delete_blob(&self, sha256: &[u8; 32]) -> Result<(), RepoError> {
        sqlx::query("DELETE FROM wasm_blobs_v2 WHERE sha256 = ?")
            .bind(sha256.as_slice())
            .execute(self.pool())
            .await
            .map_err(map_sqlite_error)?;
        Ok(())
    }

    async fn list_blob_keys(&self) -> Result<Vec<[u8; 32]>, RepoError> {
        let rows = sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT sha256 FROM wasm_blobs_v2 ORDER BY sha256 ASC",
        )
        .fetch_all(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        rows.into_iter().map(|row| sha_to_array(&row)).collect()
    }
}

impl SqliteStorage {
    async fn sha_for_registry_id(&self, id: Uuid) -> StorageResult<Option<[u8; 32]>> {
        if id == BUILTIN_CACHE_AFFINITY_ID {
            return Ok(Some(BUILTIN_CACHE_AFFINITY_SHA256));
        }
        let value: Option<String> =
            sqlx::query_scalar("SELECT value FROM plugin_registry_marker_v1 WHERE key = ?")
                .bind(registry_sha_key(id))
                .fetch_optional(self.pool())
                .await
                .map_err(map_sqlx_error)?;
        value.map(|value| sha_from_hex(&value)).transpose()
    }

    async fn registry_from_row(&self, row: SqliteRow) -> StorageResult<WasmRegistryEntry> {
        let sha256 = row_sha(&row)?;
        let markers = registry_markers_for_pool(self.pool(), sha256).await?;
        registry_from_row_with_markers(row, &markers)
    }

    async fn get_chain_by_id(&self, id: Uuid) -> StorageResult<Option<PluginChainEntry>> {
        let row = sqlx::query("SELECT * FROM plugin_chains_v2 WHERE id = ?")
            .bind(id.to_string())
            .fetch_optional(self.pool())
            .await
            .map_err(map_sqlx_error)?;
        row.map(chain_from_row).transpose()
    }
}

async fn registry_by_sha_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    sha256: [u8; 32],
) -> StorageResult<Option<WasmRegistryEntry>> {
    let row = sqlx::query(GET_REGISTRY_BY_SHA_SQL)
        .bind(sha256.as_slice())
        .fetch_optional(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
    match row {
        Some(row) => {
            let sha256 = row_sha(&row)?;
            let markers = registry_markers_for_tx(tx, sha256).await?;
            Ok(Some(registry_from_row_with_markers(row, &markers)?))
        }
        None => Ok(None),
    }
}

async fn registry_markers_for_pool(
    pool: &sqlx::SqlitePool,
    sha256: [u8; 32],
) -> StorageResult<HashMap<String, String>> {
    let keys = registry_marker_keys(sha256);
    let mut markers = HashMap::with_capacity(keys.len());
    for key in keys {
        if let Some(value) = sqlx::query_scalar::<_, String>(
            "SELECT value FROM plugin_registry_marker_v1 WHERE key = ?",
        )
        .bind(&key)
        .fetch_optional(pool)
        .await
        .map_err(map_sqlx_error)?
        {
            markers.insert(key, value);
        }
    }
    Ok(markers)
}

async fn registry_markers_for_tx(
    tx: &mut Transaction<'_, Sqlite>,
    sha256: [u8; 32],
) -> StorageResult<HashMap<String, String>> {
    let keys = registry_marker_keys(sha256);
    let mut markers = HashMap::with_capacity(keys.len());
    for key in keys {
        if let Some(value) = sqlx::query_scalar::<_, String>(
            "SELECT value FROM plugin_registry_marker_v1 WHERE key = ?",
        )
        .bind(&key)
        .fetch_optional(&mut **tx)
        .await
        .map_err(map_sqlx_error)?
        {
            markers.insert(key, value);
        }
    }
    Ok(markers)
}

fn registry_marker_keys(sha256: [u8; 32]) -> Vec<String> {
    vec![
        registry_id_key(sha256),
        registry_label_key(sha256),
        registry_uploaded_by_key(sha256),
        registry_revision_key(sha256),
        registry_wire_version_key(sha256),
        registry_supported_slots_key(sha256),
    ]
}

fn registry_from_row_with_markers(
    row: SqliteRow,
    markers: &HashMap<String, String>,
) -> StorageResult<WasmRegistryEntry> {
    let sha256 = row_sha(&row)?;
    let refcount = row.try_get::<i64, _>("refcount").map_err(map_sqlx_error)?;
    if sha256 == BUILTIN_CACHE_AFFINITY_SHA256 {
        return Ok(WasmRegistryEntry::builtin_cache_affinity(refcount));
    }
    let id = markers
        .get(&registry_id_key(sha256))
        .cloned()
        .map(|value| parse_uuid(&value, "wasm_registry.id"))
        .transpose()?
        .unwrap_or_else(|| uuid_from_sha(sha256));
    let label = markers
        .get(&registry_label_key(sha256))
        .cloned()
        .map(|value| serde_json::from_str::<Option<String>>(&value))
        .transpose()?
        .unwrap_or(None);
    let uploaded_by = markers
        .get(&registry_uploaded_by_key(sha256))
        .cloned()
        .map(|value| parse_uuid(&value, "wasm_registry.uploaded_by_admin_id"))
        .transpose()?
        .unwrap_or_else(Uuid::nil);
    let revision = markers
        .get(&registry_revision_key(sha256))
        .cloned()
        .map(|value| parse_u64(&value, "wasm_registry.revision"))
        .transpose()?
        .unwrap_or(0);
    let wire_version = markers
        .get(&registry_wire_version_key(sha256))
        .cloned()
        .map(|value| parse_u8(&value, "wasm_registry.wire_version"))
        .transpose()?
        .unwrap_or_else(default_wire_version);
    let supported_slots = markers
        .get(&registry_supported_slots_key(sha256))
        .cloned()
        .map(|value| slots_from_json(&value))
        .transpose()?
        .unwrap_or_default();
    Ok(WasmRegistryEntry {
        id,
        sha256,
        name: row.try_get("plugin_name").map_err(map_sqlx_error)?,
        original_filename: row.try_get("plugin_version").map_err(map_sqlx_error)?,
        label,
        uploaded_at_unix_secs: i64_to_u64(
            row.try_get("last_handshake_at").map_err(map_sqlx_error)?,
            "wasm_registry.uploaded_at",
        )?,
        uploaded_by_admin_id: uploaded_by,
        refcount,
        revision,
        kind: "filter".to_owned(),
        wire_version,
        is_builtin: false,
        metadata: None,
        supported_slots,
    })
}

fn row_sha(row: &SqliteRow) -> StorageResult<[u8; 32]> {
    sha_to_array(
        &row.try_get::<Vec<u8>, _>("sha256")
            .map_err(map_sqlx_error)?,
    )
}

fn record_from_row(row: SqliteRow) -> Result<PluginRegistryRecord, RepoError> {
    let metadata_json: String = row.try_get("augmented_metadata").map_err(map_sqlx_error)?;
    Ok(PluginRegistryRecord {
        sha256: sha_to_array(
            &row.try_get::<Vec<u8>, _>("sha256")
                .map_err(map_sqlx_error)?,
        )?,
        plugin_name: row.try_get("plugin_name").map_err(map_sqlx_error)?,
        plugin_version: row.try_get("plugin_version").map_err(map_sqlx_error)?,
        abi_envelope: u32_from_i64(
            row.try_get("abi_envelope").map_err(map_sqlx_error)?,
            "abi_envelope",
        )?,
        augmented_metadata: serde_json::from_str::<AugmentedMetadata>(&metadata_json)?,
        host_offer_hash: sha_to_array(
            &row.try_get::<Vec<u8>, _>("host_offer_hash")
                .map_err(map_sqlx_error)?,
        )?,
        handshake_schema_version: u32_from_i64(
            row.try_get("handshake_schema_version")
                .map_err(map_sqlx_error)?,
            "handshake_schema_version",
        )?,
        last_handshake_at: row.try_get("last_handshake_at").map_err(map_sqlx_error)?,
        status: parse_status(&row.try_get::<String, _>("status").map_err(map_sqlx_error)?)?,
    })
}

#[derive(serde::Serialize, serde::Deserialize)]
struct StoredChainConfig {
    config: Value,
    sse_per_event: bool,
    batched_events_per_flush: u32,
    batched_flush_ms: u64,
    wire_version: Option<u8>,
    wasm_registry_id: Uuid,
}

impl StoredChainConfig {
    fn from_entry(entry: &PluginChainEntry) -> Self {
        Self {
            config: entry.config.clone(),
            sse_per_event: entry.sse_per_event,
            batched_events_per_flush: entry.batched_events_per_flush,
            batched_flush_ms: entry.batched_flush_ms,
            wire_version: entry.wire_version,
            wasm_registry_id: entry.wasm_registry_id,
        }
    }
}

fn chain_config_to_json(input: &PluginChainEntryInput) -> StorageResult<String> {
    serde_json::to_string(&StoredChainConfig {
        config: input.config.clone(),
        sse_per_event: input.sse_per_event,
        batched_events_per_flush: input.batched_events_per_flush,
        batched_flush_ms: input.batched_flush_ms,
        wire_version: input.wire_version,
        wasm_registry_id: input.wasm_registry_id,
    })
    .map_err(StorageError::from)
}

fn chain_from_row(row: SqliteRow) -> StorageResult<PluginChainEntry> {
    let id = row.try_get::<String, _>("id").map_err(map_sqlx_error)?;
    let principal_id = row
        .try_get::<String, _>("principal_id")
        .map_err(map_sqlx_error)?;
    let config_text: Option<String> = row.try_get("config").map_err(map_sqlx_error)?;
    let stored: StoredChainConfig = config_text
        .as_deref()
        .map(serde_json::from_str)
        .transpose()?
        .unwrap_or(StoredChainConfig {
            config: Value::Null,
            sse_per_event: false,
            batched_events_per_flush: 0,
            batched_flush_ms: 0,
            wire_version: None,
            wasm_registry_id: Uuid::nil(),
        });
    Ok(PluginChainEntry {
        id: parse_uuid(&id, "plugin_chain.id")?,
        principal_id: parse_uuid(&principal_id, "plugin_chain.principal_id")?,
        slot: PluginSlot::parse(
            row.try_get::<String, _>("slot")
                .map_err(map_sqlx_error)?
                .as_str(),
        )
        .ok_or_else(|| StorageError::Corrupted {
            message: "invalid plugin slot".to_owned(),
        })?,
        order: row.try_get("order_index").map_err(map_sqlx_error)?,
        wasm_registry_id: stored.wasm_registry_id,
        config: stored.config,
        sse_per_event: stored.sse_per_event,
        batched_events_per_flush: stored.batched_events_per_flush,
        batched_flush_ms: stored.batched_flush_ms,
        revision: i64_to_u64(
            row.try_get("revision").map_err(map_sqlx_error)?,
            "plugin_chain.revision",
        )?,
        wire_version: stored.wire_version,
    })
}

async fn put_marker_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    key: &str,
    value: &str,
) -> StorageResult<()> {
    sqlx::query(
        "INSERT INTO plugin_registry_marker_v1 (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
    )
    .bind(key)
    .bind(value)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

async fn sha_for_registry_id_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    id: Uuid,
) -> StorageResult<Option<[u8; 32]>> {
    if id == BUILTIN_CACHE_AFFINITY_ID {
        return Ok(Some(BUILTIN_CACHE_AFFINITY_SHA256));
    }
    let value: Option<String> =
        sqlx::query_scalar("SELECT value FROM plugin_registry_marker_v1 WHERE key = ?")
            .bind(registry_sha_key(id))
            .fetch_optional(&mut **tx)
            .await
            .map_err(map_sqlx_error)?;
    value.map(|value| sha_from_hex(&value)).transpose()
}

async fn registry_revision_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    sha256: [u8; 32],
) -> StorageResult<u64> {
    let value: Option<String> =
        sqlx::query_scalar("SELECT value FROM plugin_registry_marker_v1 WHERE key = ?")
            .bind(registry_revision_key(sha256))
            .fetch_optional(&mut **tx)
            .await
            .map_err(map_sqlx_error)?;
    value
        .map(|value| parse_u64(&value, "wasm_registry.revision"))
        .transpose()
        .map(|value| value.unwrap_or(0))
}

async fn warmup_reference_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    registry_id: Uuid,
) -> StorageResult<Option<Uuid>> {
    let rows = sqlx::query("SELECT id, warmup_dialect_plugin FROM upstream_spec_v1 WHERE deleted_at IS NULL AND warmup_dialect_plugin IS NOT NULL")
        .fetch_all(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
    for row in rows {
        let Some(plugin_json) = row
            .try_get::<Option<String>, _>("warmup_dialect_plugin")
            .map_err(map_sqlx_error)?
        else {
            continue;
        };
        if plugin_json.contains(&registry_id.to_string()) {
            let id = row.try_get::<String, _>("id").map_err(map_sqlx_error)?;
            return Ok(Some(parse_uuid(&id, "upstream.id")?));
        }
    }
    Ok(None)
}

async fn delete_registry_markers_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    sha256: [u8; 32],
    id: Uuid,
) -> StorageResult<()> {
    for key in [
        registry_id_key(sha256),
        registry_sha_key(id),
        registry_label_key(sha256),
        registry_uploaded_by_key(sha256),
        registry_revision_key(sha256),
        registry_wire_version_key(sha256),
        registry_supported_slots_key(sha256),
    ] {
        sqlx::query("DELETE FROM plugin_registry_marker_v1 WHERE key = ?")
            .bind(key)
            .execute(&mut **tx)
            .await
            .map_err(map_sqlx_error)?;
    }
    Ok(())
}

fn validate_blob(blob: &WasmBlob) -> StorageResult<()> {
    if blob.size_bytes > MAX_WASM_BLOB_BYTES || blob.bytes.len() as u64 != blob.size_bytes {
        return Err(StorageError::InvalidInput {
            field: "wasm_blob.bytes".to_owned(),
            reason: "blob must be <= 32 MiB and size_bytes must match bytes length".to_owned(),
        });
    }
    Ok(())
}

fn metadata_from_input(input: &WasmRegistryEntryInput) -> StorageResult<AugmentedMetadata> {
    serde_json::from_value(serde_json::json!({
        "identity": {
            "magic": [204, 27, 112, 16, 0, 1, 0, 0],
            "abi_envelope": input.wire_version,
            "plugin_name": input.name,
            "plugin_version": input.original_filename,
        },
        "negotiated_functions": {},
        "negotiated_capabilities": [],
        "handshake_completed_at": input.uploaded_at_unix_secs as i64,
        "self_check_passed": true,
        "self_check_completed_at": input.uploaded_at_unix_secs as i64,
        "expires_at": input.uploaded_at_unix_secs as i64,
    }))
    .map_err(StorageError::from)
}

fn same_wasm_entry_metadata(existing: &WasmRegistryEntry, input: &WasmRegistryEntryInput) -> bool {
    existing.name == input.name
        && existing.original_filename == input.original_filename
        && existing.label == input.label
}

fn is_singleton_slot(slot: PluginSlot) -> bool {
    matches!(slot, PluginSlot::Shape)
}

fn conflict(message: &str) -> StorageError {
    StorageError::Conflict {
        message: message.to_owned(),
    }
}

fn map_sqlite_error(error: sqlx::Error) -> StorageError {
    if error
        .as_database_error()
        .is_some_and(|database_error| database_error.is_unique_violation())
    {
        return StorageError::Conflict {
            message: error.to_string(),
        };
    }
    map_sqlx_error(error)
}

fn status_as_str(status: PluginRegistryStatus) -> &'static str {
    match status {
        PluginRegistryStatus::Active => "active",
        PluginRegistryStatus::Disabled => "disabled",
    }
}

fn parse_status(value: &str) -> Result<PluginRegistryStatus, RepoError> {
    match value {
        "active" => Ok(PluginRegistryStatus::Active),
        "disabled" => Ok(PluginRegistryStatus::Disabled),
        value => Err(StorageError::Corrupted {
            message: format!("invalid plugin registry status {value}"),
        }),
    }
}

fn sha_to_array(bytes: &[u8]) -> StorageResult<[u8; 32]> {
    <[u8; 32]>::try_from(bytes).map_err(|_| StorageError::Corrupted {
        message: "sha256 must be 32 bytes".to_owned(),
    })
}

fn u32_from_i64(value: i64, field: &str) -> StorageResult<u32> {
    u32::try_from(value).map_err(|_| StorageError::Corrupted {
        message: format!("{field} is outside u32 range"),
    })
}

fn usize_to_i64(value: usize, field: &str) -> StorageResult<i64> {
    if value == usize::MAX {
        return Ok(i64::MAX);
    }
    i64::try_from(value).map_err(|_| StorageError::Fatal {
        message: format!("{field} cannot be represented as sqlite integer"),
    })
}

fn u64_to_i64(value: u64, field: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| StorageError::Fatal {
        message: format!("{field} cannot be represented as sqlite integer"),
    })
}

fn unix_secs_to_i64(value: u64, field: &str) -> StorageResult<i64> {
    u64_to_i64(value, field)
}

fn i64_to_u64(value: i64, field: &str) -> StorageResult<u64> {
    u64::try_from(value).map_err(|_| StorageError::Corrupted {
        message: format!("{field} is negative"),
    })
}

fn parse_i64(value: &str, field: &str) -> StorageResult<i64> {
    value.parse().map_err(|_| StorageError::Corrupted {
        message: format!("invalid {field}"),
    })
}

fn parse_u64(value: &str, field: &str) -> StorageResult<u64> {
    value.parse().map_err(|_| StorageError::Corrupted {
        message: format!("invalid {field}"),
    })
}

fn parse_u8(value: &str, field: &str) -> StorageResult<u8> {
    value.parse().map_err(|_| StorageError::Corrupted {
        message: format!("invalid {field}"),
    })
}

fn parse_uuid(value: &str, field: &str) -> StorageResult<Uuid> {
    Uuid::parse_str(value).map_err(|error| StorageError::Corrupted {
        message: format!("invalid {field} {value}: {error}"),
    })
}

fn optional_string(value: &Option<String>) -> StorageResult<String> {
    serde_json::to_string(value).map_err(StorageError::from)
}

fn slots_to_json(slots: &[PluginSlot]) -> StorageResult<String> {
    serde_json::to_string(&slots.iter().map(|slot| slot.as_str()).collect::<Vec<_>>())
        .map_err(StorageError::from)
}

fn slots_from_json(value: &str) -> StorageResult<Vec<PluginSlot>> {
    let slots = serde_json::from_str::<Vec<String>>(value)?;
    Ok(slots
        .into_iter()
        .filter_map(|slot| PluginSlot::parse(&slot))
        .collect())
}

fn registry_id_key(sha256: [u8; 32]) -> String {
    format!("wasm_registry:{}:id", sha_hex(&sha256))
}

fn registry_sha_key(id: Uuid) -> String {
    format!("wasm_registry_id:{id}:sha256")
}

fn registry_label_key(sha256: [u8; 32]) -> String {
    format!("wasm_registry:{}:label", sha_hex(&sha256))
}

fn registry_uploaded_by_key(sha256: [u8; 32]) -> String {
    format!("wasm_registry:{}:uploaded_by", sha_hex(&sha256))
}

fn registry_revision_key(sha256: [u8; 32]) -> String {
    format!("wasm_registry:{}:revision", sha_hex(&sha256))
}

fn registry_wire_version_key(sha256: [u8; 32]) -> String {
    format!("wasm_registry:{}:wire_version", sha_hex(&sha256))
}

fn registry_supported_slots_key(sha256: [u8; 32]) -> String {
    format!("wasm_registry:{}:supported_slots", sha_hex(&sha256))
}

fn sha_hex(sha256: &[u8; 32]) -> String {
    sha256.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn sha_from_hex(value: &str) -> StorageResult<[u8; 32]> {
    if value.len() != 64 {
        return Err(StorageError::Corrupted {
            message: "sha256 hex must be 64 chars".to_owned(),
        });
    }
    let mut out = [0_u8; 32];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(chunk).map_err(|_| StorageError::Corrupted {
            message: "sha256 hex is not utf8".to_owned(),
        })?;
        out[index] = u8::from_str_radix(text, 16).map_err(|_| StorageError::Corrupted {
            message: "sha256 hex is invalid".to_owned(),
        })?;
    }
    Ok(out)
}

fn uuid_from_sha(sha256: [u8; 32]) -> Uuid {
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&sha256[..16]);
    Uuid::from_bytes(bytes)
}

#[allow(dead_code)]
fn builtin_name() -> &'static str {
    BUILTIN_CACHE_AFFINITY_NAME
}
