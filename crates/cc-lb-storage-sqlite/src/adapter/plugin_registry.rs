use std::collections::{BTreeMap, HashMap, HashSet};

use async_trait::async_trait;
use cc_lb_plugin_wire::metadata::HookMetadata;
use cc_lb_storage_api::{
    BUILTIN_SUBSCRIPTION_PREFERENCE_ID, BUILTIN_SUBSCRIPTION_PREFERENCE_SHA256,
    MAX_WASM_BLOB_BYTES, PluginBlobRepo, PluginChainConflictReason, PluginChainEntry,
    PluginChainEntryInput, PluginChainEntryUpdate, PluginRegistryStore, PluginSlotKind, RepoError,
    StorageError, StorageResult, WasmBlob, WasmBlobRecord, WasmRegistryCascadeDelete,
    WasmRegistryEntry, WasmRegistryEntryInput, WasmRegistryReference,
    WasmRegistryReferenceFingerprint, WasmRegistryReferences, sparse_order, validate_identifier,
};
use serde_json::Value;
use sqlx::{QueryBuilder, Row, Sqlite, Transaction, sqlite::SqliteRow};
use uuid::Uuid;

use crate::{SqliteStorage, map_sqlx_error};

const LIST_REGISTRY_SQL: &str = "SELECT r.*, (SELECT COUNT(*) FROM plugin_chains_v2 c WHERE c.wasm_registry_id = r.id) + (SELECT COUNT(*) FROM upstream_spec_v1 u WHERE u.deleted_at IS NULL AND json_extract(u.warmup_dialect_plugin, '$.wasm_registry_id') = r.id) AS refcount FROM wasm_registry_v2 r WHERE (? IS NULL OR r.id > ?) ORDER BY r.id ASC LIMIT ?";
const GET_REGISTRY_BY_SHA_SQL: &str = "SELECT r.*, (SELECT COUNT(*) FROM plugin_chains_v2 c WHERE c.wasm_registry_id = r.id) + (SELECT COUNT(*) FROM upstream_spec_v1 u WHERE u.deleted_at IS NULL AND json_extract(u.warmup_dialect_plugin, '$.wasm_registry_id') = r.id) AS refcount FROM wasm_registry_v2 r WHERE r.sha256 = ?";
const GET_REGISTRY_BY_NAME_SQL: &str = "SELECT r.*, (SELECT COUNT(*) FROM plugin_chains_v2 c WHERE c.wasm_registry_id = r.id) + (SELECT COUNT(*) FROM upstream_spec_v1 u WHERE u.deleted_at IS NULL AND json_extract(u.warmup_dialect_plugin, '$.wasm_registry_id') = r.id) AS refcount FROM wasm_registry_v2 r WHERE r.plugin_name = ?";
const GET_REGISTRY_BY_ID_SQL: &str = "SELECT r.*, (SELECT COUNT(*) FROM plugin_chains_v2 c WHERE c.wasm_registry_id = r.id) + (SELECT COUNT(*) FROM upstream_spec_v1 u WHERE u.deleted_at IS NULL AND json_extract(u.warmup_dialect_plugin, '$.wasm_registry_id') = r.id) AS refcount FROM wasm_registry_v2 r WHERE r.id = ?";

#[async_trait]
impl PluginRegistryStore for SqliteStorage {
    async fn persist_wasm_upload(
        &self,
        blob: WasmBlob,
        input: WasmRegistryEntryInput,
    ) -> StorageResult<(WasmRegistryEntry, bool)> {
        validate_identifier("plugin.name", &input.name)?;
        validate_blob(&blob)?;

        let mut tx = self.begin_immediate().await?;
        let now = unix_secs_to_i64(blob.parse_validated_at_unix_secs, "wasm_blob.created_at")?;
        let blob_insert = sqlx::query(
            "INSERT INTO wasm_blobs_v2 (sha256, bytes, size_bytes, parse_validated_at, created_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT(sha256) DO NOTHING",
        )
        .bind(blob.sha256.as_slice())
        .bind(blob.bytes.as_slice())
        .bind(u64_to_i64(blob.size_bytes, "wasm_blob.size_bytes")?)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlite_error)?;

        let id = Uuid::new_v4();
        let registry_insert = sqlx::query(
            "INSERT INTO wasm_registry_v2 (id, sha256, plugin_name, plugin_version, original_filename, label, uploaded_by_admin_id, revision, description, usage, hook_metadata, supported_slots, schema_hash, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, 'active', ?, ?) ON CONFLICT(sha256) DO NOTHING",
        )
        .bind(id.to_string())
        .bind(blob.sha256.as_slice())
        .bind(&input.name)
        .bind(&input.version)
        .bind(&input.original_filename)
        .bind(input.label.as_deref())
        .bind(input.uploaded_by_admin_id.to_string())
        .bind(&input.description)
        .bind(&input.usage)
        .bind(hook_metadata_to_json(&input.hook_metadata)?)
        .bind(slots_to_json(&input.supported_slots)?)
        .bind(input.schema_hash.as_ref().map(|h| h.as_slice()))
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
        let after_id = after.map(|id| id.to_string());
        let rows = sqlx::query(LIST_REGISTRY_SQL)
            .bind(after_id.clone())
            .bind(after_id)
            .bind(usize_to_i64(limit, "plugin_registry.limit")?)
            .fetch_all(self.pool())
            .await
            .map_err(map_sqlx_error)?;
        let mut entries = Vec::with_capacity(rows.len());
        for row in rows {
            entries.push(registry_from_row(row)?);
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
            Some(row) => Ok(Some(registry_from_row(row)?)),
            None => Ok(None),
        }
    }

    async fn get_registry_entry_by_name(
        &self,
        name: &str,
    ) -> StorageResult<Option<WasmRegistryEntry>> {
        let row = sqlx::query(GET_REGISTRY_BY_NAME_SQL)
            .bind(name)
            .fetch_optional(self.pool())
            .await
            .map_err(map_sqlx_error)?;
        row.map(registry_from_row).transpose()
    }

    async fn get_registry_entry_by_id(&self, id: Uuid) -> StorageResult<Option<WasmRegistryEntry>> {
        let row = sqlx::query(GET_REGISTRY_BY_ID_SQL)
            .bind(id.to_string())
            .fetch_optional(self.pool())
            .await
            .map_err(map_sqlx_error)?;
        row.map(registry_from_row).transpose()
    }

    async fn replace_wasm_entry(
        &self,
        blob: WasmBlob,
        input: WasmRegistryEntryInput,
        expected_revision: u64,
    ) -> StorageResult<WasmRegistryEntry> {
        validate_identifier("plugin.name", &input.name)?;
        validate_blob(&blob)?;

        let mut tx = self.begin_immediate().await?;
        let current = registry_by_name_in_tx(&mut tx, &input.name)
            .await?
            .ok_or_else(|| conflict("unknown plugin registry entry"))?;
        if current.revision != expected_revision {
            return Err(StorageError::StalePluginRegistryRevision {
                current: current.revision,
            });
        }

        let validated_at =
            unix_secs_to_i64(blob.parse_validated_at_unix_secs, "wasm_blob.created_at")?;
        sqlx::query(
            "INSERT INTO wasm_blobs_v2 (sha256, bytes, size_bytes, parse_validated_at, created_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT(sha256) DO NOTHING",
        )
        .bind(blob.sha256.as_slice())
        .bind(blob.bytes.as_slice())
        .bind(u64_to_i64(blob.size_bytes, "wasm_blob.size_bytes")?)
        .bind(validated_at)
        .bind(validated_at)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlite_error)?;

        let uploaded_at =
            unix_secs_to_i64(input.uploaded_at_unix_secs, "wasm_registry.uploaded_at")?;
        let result = sqlx::query(
            "UPDATE wasm_registry_v2 SET sha256 = ?, plugin_version = ?, original_filename = ?, uploaded_by_admin_id = ?, revision = revision + 1, description = ?, usage = ?, hook_metadata = ?, supported_slots = ?, schema_hash = ?, created_at = ?, updated_at = ? WHERE id = ? AND revision = ?",
        )
        .bind(blob.sha256.as_slice())
        .bind(&input.version)
        .bind(&input.original_filename)
        .bind(input.uploaded_by_admin_id.to_string())
        .bind(&input.description)
        .bind(&input.usage)
        .bind(hook_metadata_to_json(&input.hook_metadata)?)
        .bind(slots_to_json(&input.supported_slots)?)
        .bind(input.schema_hash.as_ref().map(|hash| hash.as_slice()))
        .bind(uploaded_at)
        .bind(uploaded_at)
        .bind(current.id.to_string())
        .bind(u64_to_i64(expected_revision, "wasm_registry.revision")?)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlite_error)?;
        if result.rows_affected() == 0 {
            return Err(StorageError::StalePluginRegistryRevision {
                current: current.revision,
            });
        }

        sqlx::query(
            "DELETE FROM wasm_blobs_v2 WHERE sha256 = ? AND NOT EXISTS (SELECT 1 FROM wasm_registry_v2 WHERE sha256 = ?)",
        )
        .bind(current.sha256.as_slice())
        .bind(current.sha256.as_slice())
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;

        let entry = registry_by_id_in_tx(&mut tx, current.id)
            .await?
            .ok_or_else(|| conflict("updated plugin registry row disappeared"))?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(entry)
    }

    async fn list_registry_references(&self, id: Uuid) -> StorageResult<WasmRegistryReferences> {
        let mut tx = self.begin_immediate().await?;
        let references = registry_references_in_tx(&mut tx, id).await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(references)
    }

    async fn cascade_delete_registry_entry(
        &self,
        id: Uuid,
        expected_revision: u64,
        expected_references: WasmRegistryReferenceFingerprint,
    ) -> StorageResult<Option<WasmRegistryCascadeDelete>> {
        let mut tx = self.begin_immediate().await?;
        let Some(entry) = registry_by_id_in_tx(&mut tx, id).await? else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(None);
        };
        if entry.revision != expected_revision {
            return Err(StorageError::StalePluginRegistryRevision {
                current: entry.revision,
            });
        }
        let references = registry_references_in_tx(&mut tx, id).await?;
        if references.fingerprint != expected_references {
            return Err(StorageError::StalePluginRegistryReferences);
        }

        sqlx::query("DELETE FROM plugin_chains_v2 WHERE wasm_registry_id = ?")
            .bind(id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        sqlx::query(
            "UPDATE upstream_spec_v1 SET warmup_dialect_plugin = NULL, spec_revision = spec_revision + 1, updated_at = unixepoch() WHERE deleted_at IS NULL AND json_extract(warmup_dialect_plugin, '$.wasm_registry_id') = ?",
        )
        .bind(id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        sqlx::query("DELETE FROM wasm_registry_v2 WHERE id = ? AND revision = ?")
            .bind(id.to_string())
            .bind(u64_to_i64(expected_revision, "wasm_registry.revision")?)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        sqlx::query(
            "DELETE FROM wasm_blobs_v2 WHERE sha256 = ? AND NOT EXISTS (SELECT 1 FROM wasm_registry_v2 WHERE sha256 = ?)",
        )
        .bind(entry.sha256.as_slice())
        .bind(entry.sha256.as_slice())
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(Some(WasmRegistryCascadeDelete {
            entry,
            references: references.references,
        }))
    }

    async fn update_registry_label(
        &self,
        id: Uuid,
        expected_revision: u64,
        label: Option<String>,
    ) -> StorageResult<WasmRegistryEntry> {
        let mut tx = self.begin_immediate().await?;
        let current = registry_revision_in_tx(&mut tx, id).await?;
        if current != expected_revision {
            return Err(StorageError::StalePluginRegistryRevision { current });
        }
        sqlx::query("UPDATE wasm_registry_v2 SET label = ?, revision = revision + 1, updated_at = unixepoch() WHERE id = ? AND revision = ?")
            .bind(label.as_deref())
            .bind(id.to_string())
            .bind(u64_to_i64(expected_revision, "wasm_registry.revision")?)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)?;
        self.get_registry_entry_by_id(id)
            .await?
            .ok_or_else(|| conflict("updated plugin registry row disappeared"))
    }

    async fn update_supported_slots(
        &self,
        id: Uuid,
        supported_slots: Vec<PluginSlotKind>,
    ) -> StorageResult<()> {
        sqlx::query("UPDATE wasm_registry_v2 SET supported_slots = ? WHERE id = ?")
            .bind(slots_to_json(&supported_slots)?)
            .bind(id.to_string())
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
        let mut tx = self.begin_immediate().await?;
        let Some(entry) = registry_by_id_in_tx(&mut tx, id).await? else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(None);
        };
        if entry.revision != expected_revision {
            return Err(StorageError::StalePluginRegistryRevision {
                current: entry.revision,
            });
        }
        let references = registry_references_in_tx(&mut tx, id).await?;
        if let Some(reference) = references.references.first() {
            let reference_id = match reference {
                WasmRegistryReference::PluginChain { chain_entry_id, .. } => *chain_entry_id,
                WasmRegistryReference::UpstreamWarmupDialect { upstream_id, .. } => *upstream_id,
            };
            return Err(StorageError::PluginRegistryReferenced {
                id: reference_id.to_string(),
            });
        }
        sqlx::query("DELETE FROM wasm_registry_v2 WHERE id = ?")
            .bind(id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        sqlx::query(
            "DELETE FROM wasm_blobs_v2 WHERE sha256 = ? AND NOT EXISTS (SELECT 1 FROM wasm_registry_v2 WHERE sha256 = ?)",
        )
        .bind(entry.sha256.as_slice())
        .bind(entry.sha256.as_slice())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(Some(entry))
    }

    async fn decrement_blob_refcount_or_delete(&self, sha256: [u8; 32]) -> StorageResult<bool> {
        let mut tx = self.begin_immediate().await?;
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
        let mut tx = self.begin_immediate().await?;
        if sha_for_registry_id_in_tx(&mut tx, input.wasm_registry_id)
            .await?
            .is_none()
        {
            return Err(StorageError::PluginRegistryConflict {
                message: "unknown plugin registry entry".to_owned(),
            });
        }
        let principal_exists: Option<i64> =
            sqlx::query_scalar("SELECT 1 FROM principals_v1 WHERE id = ? AND deleted_at IS NULL")
                .bind(input.principal_id.to_string())
                .fetch_optional(&mut *tx)
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
            .fetch_optional(&mut *tx)
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
        let row = sqlx::query(
            "INSERT INTO plugin_chains_v2 (id, principal_id, slot, wasm_registry_id, order_value, config, revision, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, 0, unixepoch(), unixepoch()) RETURNING *",
        )
        .bind(id.to_string())
        .bind(input.principal_id.to_string())
        .bind(input.slot.as_str())
        .bind(input.wasm_registry_id.to_string())
        .bind(input.order)
        .bind(serde_json::to_string(&input.config)?)
        .fetch_one(&mut *tx)
        .await
        .map_err(map_sqlite_error)?;
        tx.commit().await.map_err(map_sqlx_error)?;
        let entry = chain_from_row(row)?;
        Ok(entry)
    }

    async fn list_chain_for_principal(
        &self,
        principal_id: Uuid,
        slot: PluginSlotKind,
    ) -> StorageResult<Vec<PluginChainEntry>> {
        let rows = sqlx::query(
            "SELECT * FROM plugin_chains_v2 WHERE principal_id = ? AND slot = ? ORDER BY order_value ASC, id ASC",
        )
        .bind(principal_id.to_string())
        .bind(slot.as_str())
        .fetch_all(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        rows.into_iter().map(chain_from_row).collect()
    }

    async fn list_chains_for_principals(
        &self,
        principal_ids: &[Uuid],
        slots: &[PluginSlotKind],
    ) -> StorageResult<Vec<PluginChainEntry>> {
        if principal_ids.is_empty() || slots.is_empty() {
            return Ok(Vec::new());
        }

        let mut query =
            QueryBuilder::<Sqlite>::new("SELECT * FROM plugin_chains_v2 WHERE principal_id IN (");
        {
            let mut principal_bindings = query.separated(", ");
            for principal_id in principal_ids {
                principal_bindings.push_bind(principal_id.to_string());
            }
        }
        query.push(") AND slot IN (");
        {
            let mut slot_bindings = query.separated(", ");
            for slot in slots {
                slot_bindings.push_bind(slot.as_str());
            }
        }
        query.push(") ORDER BY principal_id ASC, slot ASC, order_value ASC, id ASC");

        let rows = query
            .build()
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
        let Some(config) = update.config else {
            return Err(StorageError::InvalidInput {
                field: "plugin_chain.update".to_owned(),
                reason: "empty_update".to_owned(),
            });
        };
        let current = match self.get_chain_by_id(id).await? {
            Some(current) => current,
            None => return Ok(None),
        };
        if current.revision != expected_revision {
            return Err(StorageError::StalePluginChainRevision {
                current: current.revision,
            });
        }
        let row = sqlx::query(
            "UPDATE plugin_chains_v2 SET config = ?, revision = revision + 1, updated_at = unixepoch() WHERE id = ? AND revision = ? RETURNING *",
        )
        .bind(serde_json::to_string(&config)?)
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
        slot: PluginSlotKind,
        new_orders: Vec<(Uuid, i64, u64)>,
    ) -> StorageResult<Vec<PluginChainEntry>> {
        let mut tx = self.begin_immediate().await?;
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
            "SELECT * FROM plugin_chains_v2 WHERE principal_id = ? AND slot = ? ORDER BY order_value ASC, id ASC",
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
                "UPDATE plugin_chains_v2 SET order_value = ?, revision = revision + 1, updated_at = unixepoch() WHERE id = ? AND revision = ?",
            )
            .bind(order)
            .bind(id.to_string())
            .bind(u64_to_i64(expected_revision, "plugin_chain.revision")?)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlite_error)?;
        }
        let rows = sqlx::query(
            "SELECT * FROM plugin_chains_v2 WHERE principal_id = ? AND slot = ? ORDER BY order_value ASC, id ASC",
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
        let mut tx = self.begin_immediate().await?;
        let result = sqlx::query("DELETE FROM plugin_chains_v2 WHERE id = ? AND revision = ?")
            .bind(id.to_string())
            .bind(u64_to_i64(expected_revision, "plugin_chain.revision")?)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlite_error)?;
        if result.rows_affected() == 0 {
            return Err(StorageError::StalePluginChainRevision {
                current: entry.revision,
            });
        }
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(Some(entry))
    }

    async fn rebalance_chain(
        &self,
        principal_id: Uuid,
        slot: PluginSlotKind,
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
impl PluginBlobRepo for SqliteStorage {
    async fn put_blob(&self, sha256: &[u8; 32], bytes: &[u8]) -> Result<(), RepoError> {
        sqlx::query(
            "INSERT INTO wasm_blobs_v2 (sha256, bytes, size_bytes, parse_validated_at, created_at) VALUES (?, ?, ?, NULL, unixepoch()) ON CONFLICT(sha256) DO UPDATE SET bytes = excluded.bytes, size_bytes = excluded.size_bytes",
        )
        .bind(sha256.as_slice())
        .bind(bytes)
        .bind(usize_to_i64(bytes.len(), "wasm_blob.size_bytes")?)
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
    async fn get_chain_by_id(&self, id: Uuid) -> StorageResult<Option<PluginChainEntry>> {
        let row = sqlx::query("SELECT * FROM plugin_chains_v2 WHERE id = ?")
            .bind(id.to_string())
            .fetch_optional(self.pool())
            .await
            .map_err(map_sqlx_error)?;
        row.map(chain_from_row).transpose()
    }
}

async fn registry_by_id_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    id: Uuid,
) -> StorageResult<Option<WasmRegistryEntry>> {
    let row = sqlx::query(GET_REGISTRY_BY_ID_SQL)
        .bind(id.to_string())
        .fetch_optional(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
    row.map(registry_from_row).transpose()
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
    row.map(registry_from_row).transpose()
}

async fn registry_by_name_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    name: &str,
) -> StorageResult<Option<WasmRegistryEntry>> {
    let row = sqlx::query(GET_REGISTRY_BY_NAME_SQL)
        .bind(name)
        .fetch_optional(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
    row.map(registry_from_row).transpose()
}

fn registry_from_row(row: SqliteRow) -> StorageResult<WasmRegistryEntry> {
    let sha256 = row_sha(&row)?;
    let refcount = row.try_get::<i64, _>("refcount").map_err(map_sqlx_error)?;
    let id = parse_uuid(
        &row.try_get::<String, _>("id").map_err(map_sqlx_error)?,
        "wasm_registry.id",
    )?;
    if id == BUILTIN_SUBSCRIPTION_PREFERENCE_ID || sha256 == BUILTIN_SUBSCRIPTION_PREFERENCE_SHA256
    {
        return Ok(WasmRegistryEntry::builtin_subscription_preference(refcount));
    }
    let uploaded_by = parse_uuid(
        &row.try_get::<String, _>("uploaded_by_admin_id")
            .map_err(map_sqlx_error)?,
        "wasm_registry.uploaded_by_admin_id",
    )?;
    let revision = i64_to_u64(
        row.try_get("revision").map_err(map_sqlx_error)?,
        "wasm_registry.revision",
    )?;
    let supported_slots = slots_from_json(
        &row.try_get::<String, _>("supported_slots")
            .map_err(map_sqlx_error)?,
    )?;
    let schema_hash = row
        .try_get::<Option<Vec<u8>>, _>("schema_hash")
        .map_err(map_sqlx_error)?
        .map(|bytes| sha_to_array(&bytes))
        .transpose()?;
    Ok(WasmRegistryEntry {
        id,
        sha256,
        name: row.try_get("plugin_name").map_err(map_sqlx_error)?,
        version: row.try_get("plugin_version").map_err(map_sqlx_error)?,
        original_filename: row.try_get("original_filename").map_err(map_sqlx_error)?,
        label: row.try_get("label").map_err(map_sqlx_error)?,
        uploaded_at_unix_secs: i64_to_u64(
            row.try_get("created_at").map_err(map_sqlx_error)?,
            "wasm_registry.uploaded_at",
        )?,
        uploaded_by_admin_id: uploaded_by,
        refcount,
        revision,
        kind: "filter".to_owned(),
        description: row.try_get("description").map_err(map_sqlx_error)?,
        usage: row.try_get("usage").map_err(map_sqlx_error)?,
        hook_metadata: hook_metadata_from_json(
            &row.try_get::<String, _>("hook_metadata")
                .map_err(map_sqlx_error)?,
        )?,
        is_builtin: false,
        metadata: None,
        supported_slots,
        schema_hash,
    })
}

fn row_sha(row: &SqliteRow) -> StorageResult<[u8; 32]> {
    sha_to_array(
        &row.try_get::<Vec<u8>, _>("sha256")
            .map_err(map_sqlx_error)?,
    )
}

fn chain_from_row(row: SqliteRow) -> StorageResult<PluginChainEntry> {
    let id = row.try_get::<String, _>("id").map_err(map_sqlx_error)?;
    let principal_id = row
        .try_get::<String, _>("principal_id")
        .map_err(map_sqlx_error)?;
    let config_text: String = row.try_get("config").map_err(map_sqlx_error)?;
    let wasm_registry_id = row
        .try_get::<String, _>("wasm_registry_id")
        .map_err(map_sqlx_error)?;
    Ok(PluginChainEntry {
        id: parse_uuid(&id, "plugin_chain.id")?,
        principal_id: parse_uuid(&principal_id, "plugin_chain.principal_id")?,
        slot: PluginSlotKind::parse(
            row.try_get::<String, _>("slot")
                .map_err(map_sqlx_error)?
                .as_str(),
        )
        .ok_or_else(|| StorageError::Corrupted {
            message: "invalid plugin slot".to_owned(),
        })?,
        order: row.try_get("order_value").map_err(map_sqlx_error)?,
        wasm_registry_id: parse_uuid(&wasm_registry_id, "plugin_chain.wasm_registry_id")?,
        config: serde_json::from_str::<Value>(&config_text)?,
        revision: i64_to_u64(
            row.try_get("revision").map_err(map_sqlx_error)?,
            "plugin_chain.revision",
        )?,
    })
}

async fn sha_for_registry_id_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    id: Uuid,
) -> StorageResult<Option<[u8; 32]>> {
    if id == BUILTIN_SUBSCRIPTION_PREFERENCE_ID {
        return Ok(Some(BUILTIN_SUBSCRIPTION_PREFERENCE_SHA256));
    }
    let value: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT sha256 FROM wasm_registry_v2 WHERE id = ?")
            .bind(id.to_string())
            .fetch_optional(&mut **tx)
            .await
            .map_err(map_sqlx_error)?;
    value.map(|value| sha_to_array(&value)).transpose()
}

async fn registry_revision_in_tx(tx: &mut Transaction<'_, Sqlite>, id: Uuid) -> StorageResult<u64> {
    let value: Option<i64> =
        sqlx::query_scalar("SELECT revision FROM wasm_registry_v2 WHERE id = ?")
            .bind(id.to_string())
            .fetch_optional(&mut **tx)
            .await
            .map_err(map_sqlx_error)?;
    value
        .map(|value| i64_to_u64(value, "wasm_registry.revision"))
        .transpose()
        .map(|value| value.unwrap_or(0))
}

async fn registry_references_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    registry_id: Uuid,
) -> StorageResult<WasmRegistryReferences> {
    let chain_rows = sqlx::query(
        "SELECT c.id AS chain_entry_id, c.principal_id, p.name AS principal_name, c.slot, c.revision FROM plugin_chains_v2 c JOIN principals_v1 p ON p.id = c.principal_id WHERE c.wasm_registry_id = ? ORDER BY c.id ASC",
    )
    .bind(registry_id.to_string())
    .fetch_all(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    let mut references = Vec::with_capacity(chain_rows.len());
    for row in chain_rows {
        let slot = row.try_get::<String, _>("slot").map_err(map_sqlx_error)?;
        references.push(WasmRegistryReference::PluginChain {
            chain_entry_id: parse_uuid(
                &row.try_get::<String, _>("chain_entry_id")
                    .map_err(map_sqlx_error)?,
                "plugin_chain.id",
            )?,
            principal_id: parse_uuid(
                &row.try_get::<String, _>("principal_id")
                    .map_err(map_sqlx_error)?,
                "plugin_chain.principal_id",
            )?,
            principal_name: row.try_get("principal_name").map_err(map_sqlx_error)?,
            slot: PluginSlotKind::parse(&slot).ok_or_else(|| StorageError::Corrupted {
                message: "invalid plugin slot".to_owned(),
            })?,
            revision: i64_to_u64(
                row.try_get("revision").map_err(map_sqlx_error)?,
                "plugin_chain.revision",
            )?,
        });
    }

    let upstream_rows = sqlx::query(
        "SELECT id, name, spec_revision AS revision FROM upstream_spec_v1 WHERE deleted_at IS NULL AND json_extract(warmup_dialect_plugin, '$.wasm_registry_id') = ? ORDER BY id ASC",
    )
    .bind(registry_id.to_string())
        .fetch_all(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
    references.reserve(upstream_rows.len());
    for row in upstream_rows {
        references.push(WasmRegistryReference::UpstreamWarmupDialect {
            upstream_id: parse_uuid(
                &row.try_get::<String, _>("id").map_err(map_sqlx_error)?,
                "upstream.id",
            )?,
            upstream_name: row.try_get("name").map_err(map_sqlx_error)?,
            revision: i64_to_u64(
                row.try_get("revision").map_err(map_sqlx_error)?,
                "upstream.spec_revision",
            )?,
        });
    }
    Ok(WasmRegistryReferences::from_references(references))
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

fn same_wasm_entry_metadata(existing: &WasmRegistryEntry, input: &WasmRegistryEntryInput) -> bool {
    let schema_hash_ok = match (existing.schema_hash, input.schema_hash) {
        (Some(a), Some(b)) => a == b,
        _ => true,
    };
    existing.name == input.name
        && existing.version == input.version
        && existing.original_filename == input.original_filename
        && existing.label == input.label
        && existing.description == input.description
        && existing.usage == input.usage
        && existing.hook_metadata == input.hook_metadata
        && existing.supported_slots == input.supported_slots
        && schema_hash_ok
}

fn is_singleton_slot(slot: PluginSlotKind) -> bool {
    matches!(slot, PluginSlotKind::Shape)
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

fn sha_to_array(bytes: &[u8]) -> StorageResult<[u8; 32]> {
    <[u8; 32]>::try_from(bytes).map_err(|_| StorageError::Corrupted {
        message: "sha256 must be 32 bytes".to_owned(),
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

fn parse_uuid(value: &str, field: &str) -> StorageResult<Uuid> {
    Uuid::parse_str(value).map_err(|error| StorageError::Corrupted {
        message: format!("invalid {field} {value}: {error}"),
    })
}

fn slots_to_json(slots: &[PluginSlotKind]) -> StorageResult<String> {
    serde_json::to_string(&slots.iter().map(|slot| slot.as_str()).collect::<Vec<_>>())
        .map_err(StorageError::from)
}

fn hook_metadata_to_json(metadata: &BTreeMap<String, HookMetadata>) -> StorageResult<String> {
    serde_json::to_string(metadata).map_err(StorageError::from)
}

fn hook_metadata_from_json(value: &str) -> StorageResult<BTreeMap<String, HookMetadata>> {
    serde_json::from_str(value).map_err(StorageError::from)
}

fn slots_from_json(value: &str) -> StorageResult<Vec<PluginSlotKind>> {
    let slots = serde_json::from_str::<Vec<String>>(value)?;
    Ok(slots
        .into_iter()
        .filter_map(|slot| PluginSlotKind::parse(&slot))
        .collect())
}
