use std::collections::{BTreeMap, HashMap, HashSet};

use async_trait::async_trait;
use cc_lb_plugin_wire::metadata::HookMetadata;
use cc_lb_storage_api::{
    BUILTIN_SUBSCRIPTION_PREFERENCE_ID, MAX_WASM_BLOB_BYTES, PluginChainConflictReason,
    PluginChainEntry, PluginChainEntryInput, PluginChainEntryUpdate, PluginRegistryStore,
    PluginSlotKind, StorageError, StorageResult, WasmBlob, WasmBlobRecord,
    WasmRegistryCascadeDelete, WasmRegistryEntry, WasmRegistryEntryInput, WasmRegistryReference,
    WasmRegistryReferenceFingerprint, WasmRegistryReferences, sparse_order, validate_identifier,
};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::Row;
use uuid::Uuid;

use crate::{
    adapter::{
        PostgresStorage, conflict, datetime_to_unix_secs, i64_to_u64, retry, u64_to_i64,
        unix_secs_to_datetime,
    },
    error_map::map_sqlx_error,
};

#[async_trait]
impl PluginRegistryStore for PostgresStorage {
    async fn persist_wasm_upload(
        &self,
        blob: WasmBlob,
        input: WasmRegistryEntryInput,
    ) -> StorageResult<(WasmRegistryEntry, bool)> {
        validate_identifier("plugin.name", &input.name)?;
        if blob.size_bytes > MAX_WASM_BLOB_BYTES || blob.bytes.len() as u64 > MAX_WASM_BLOB_BYTES {
            return Err(StorageError::InvalidInput {
                field: "wasm_blob.bytes".to_owned(),
                reason: format!("blob exceeds 32 MiB: {} bytes", blob.size_bytes),
            });
        }
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let id = Uuid::new_v4();
        let uploaded_at =
            unix_secs_to_datetime(input.uploaded_at_unix_secs, "wasm_registry.uploaded_at")?;
        let blob_was_inserted = insert_blob_in_tx(&mut tx, &blob).await?;
        let inserted_registry =
            insert_registry_in_tx(&mut tx, id, blob.sha256, &input, uploaded_at).await?;
        let (entry, existed) = match (inserted_registry, blob_was_inserted) {
            // Fresh registry row. The blob may be new, or it may be an orphan blob
            // row that is now gaining its registry owner; both are new uploads.
            (Some(entry), true) | (Some(entry), false) => (entry, false),
            // Registry row already existed while the blob row was missing. The blob
            // INSERT healed the zombie state, and the existing registry row is safe
            // to return because the FK now has its target row again.
            (None, true) => {
                let entry = self
                    .registry_by_sha_in_tx(&mut tx, blob.sha256)
                    .await?
                    .ok_or_else(|| StorageError::Fatal {
                        message: "wasm registry row missing after upload".to_owned(),
                    })?;
                if !same_wasm_entry_metadata(&entry, &input) {
                    return Err(StorageError::Conflict {
                        message: "sha256 already registered for a different wasm entry".to_owned(),
                    });
                }
                (entry, true)
            }
            // Registry and blob both already existed. Under READ COMMITTED, this
            // SELECT sees the row that ON CONFLICT waited on before DO NOTHING.
            (None, false) => {
                let entry = self
                    .registry_by_sha_in_tx(&mut tx, blob.sha256)
                    .await?
                    .ok_or_else(|| StorageError::Fatal {
                        message: "wasm registry row missing after upload".to_owned(),
                    })?;
                if !same_wasm_entry_metadata(&entry, &input) {
                    return Err(StorageError::Conflict {
                        message: "sha256 already registered for a different wasm entry".to_owned(),
                    });
                }
                (entry, true)
            }
        };
        if !existed {
            sqlx::query("SELECT pg_notify('cclb_plugin_changed', $1)")
                .bind(entry.id.to_string())
                .execute(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
        }
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok((entry, existed))
    }

    async fn get_blob_bytes(&self, sha256: [u8; 32]) -> StorageResult<Option<Vec<u8>>> {
        sqlx::query_scalar::<_, Vec<u8>>("SELECT bytes FROM wasm_blobs_v2 WHERE sha256 = $1")
            .bind(sha256.as_slice())
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_error)
    }

    async fn get_blob(&self, sha256: [u8; 32]) -> StorageResult<Option<WasmBlobRecord>> {
        let row =
            sqlx::query_scalar::<_, Vec<u8>>("SELECT sha256 FROM wasm_blobs_v2 WHERE sha256 = $1")
                .bind(sha256.as_slice())
                .fetch_optional(&self.pool)
                .await
                .map_err(map_sqlx_error)?;
        row.map(|bytes| {
            Ok(WasmBlobRecord {
                sha256: sha_to_array(&bytes)?,
            })
        })
        .transpose()
    }

    async fn list_orphan_blobs(&self) -> StorageResult<Vec<[u8; 32]>> {
        let rows = sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT sha256 FROM wasm_blobs_v2 WHERE sha256 NOT IN (SELECT sha256 FROM wasm_registry_v2) ORDER BY sha256 ASC",
        )
        .fetch_all(&self.pool)
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
        let rows = sqlx::query("SELECT r.*, (SELECT COUNT(*) FROM plugin_chains_v2 c WHERE c.wasm_registry_id = r.id) + (SELECT COUNT(*) FROM upstream_spec_v1 u WHERE u.deleted_at IS NULL AND (u.warmup_dialect_plugin->>'wasm_registry_id')::uuid = r.id) AS refcount FROM wasm_registry_v2 r WHERE ($1::uuid IS NULL OR r.id > $1) ORDER BY r.id ASC LIMIT $2")
            .bind(after).bind(u64_to_i64(limit as u64, "plugin_registry.limit")?).fetch_all(&self.pool).await.map_err(map_sqlx_error)?;
        rows.into_iter().map(registry_from_row).collect()
    }

    async fn get_registry_entry_by_sha(
        &self,
        sha256: [u8; 32],
    ) -> StorageResult<Option<WasmRegistryEntry>> {
        let row = sqlx::query("SELECT r.*, (SELECT COUNT(*) FROM plugin_chains_v2 c WHERE c.wasm_registry_id = r.id) + (SELECT COUNT(*) FROM upstream_spec_v1 u WHERE u.deleted_at IS NULL AND (u.warmup_dialect_plugin->>'wasm_registry_id')::uuid = r.id) AS refcount FROM wasm_registry_v2 r WHERE r.sha256 = $1")
            .bind(sha256.as_slice()).fetch_optional(&self.pool).await.map_err(map_sqlx_error)?;
        row.map(registry_from_row).transpose()
    }

    async fn get_registry_entry_by_name(
        &self,
        name: &str,
    ) -> StorageResult<Option<WasmRegistryEntry>> {
        let row = sqlx::query("SELECT r.*, (SELECT COUNT(*) FROM plugin_chains_v2 c WHERE c.wasm_registry_id = r.id) + (SELECT COUNT(*) FROM upstream_spec_v1 u WHERE u.deleted_at IS NULL AND (u.warmup_dialect_plugin->>'wasm_registry_id')::uuid = r.id) AS refcount FROM wasm_registry_v2 r WHERE r.name = $1")
            .bind(name)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        row.map(registry_from_row).transpose()
    }

    async fn get_registry_entry_by_id(&self, id: Uuid) -> StorageResult<Option<WasmRegistryEntry>> {
        let row = sqlx::query("SELECT r.*, (SELECT COUNT(*) FROM plugin_chains_v2 c WHERE c.wasm_registry_id = r.id) + (SELECT COUNT(*) FROM upstream_spec_v1 u WHERE u.deleted_at IS NULL AND (u.warmup_dialect_plugin->>'wasm_registry_id')::uuid = r.id) AS refcount FROM wasm_registry_v2 r WHERE r.id = $1")
            .bind(id).fetch_optional(&self.pool).await.map_err(map_sqlx_error)?;
        row.map(registry_from_row).transpose()
    }

    async fn replace_wasm_entry(
        &self,
        blob: WasmBlob,
        input: WasmRegistryEntryInput,
        expected_revision: u64,
    ) -> StorageResult<WasmRegistryEntry> {
        validate_identifier("plugin.name", &input.name)?;
        if blob.size_bytes > MAX_WASM_BLOB_BYTES || blob.bytes.len() as u64 > MAX_WASM_BLOB_BYTES {
            return Err(StorageError::InvalidInput {
                field: "wasm_blob.bytes".to_owned(),
                reason: format!("blob exceeds 32 MiB: {} bytes", blob.size_bytes),
            });
        }

        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let current = registry_by_name_in_tx(&mut tx, &input.name)
            .await?
            .ok_or_else(|| conflict("unknown plugin registry entry"))?;
        if current.revision != expected_revision {
            return Err(StorageError::StalePluginRegistryRevision {
                current: current.revision,
            });
        }

        insert_blob_in_tx(&mut tx, &blob).await?;
        let supported_slots: Vec<String> = input
            .supported_slots
            .iter()
            .map(|slot| slot.as_str().to_owned())
            .collect();
        let uploaded_at =
            unix_secs_to_datetime(input.uploaded_at_unix_secs, "wasm_registry.uploaded_at")?;
        let result = sqlx::query("UPDATE wasm_registry_v2 SET sha256 = $1, plugin_version = $2, original_filename = $3, uploaded_at = $4, uploaded_by_admin_id = $5, revision = revision + 1, description = $6, usage = $7, hook_metadata = $8, supported_slots = $9, schema_hash = $10 WHERE id = $11 AND revision = $12")
            .bind(blob.sha256.as_slice())
            .bind(&input.version)
            .bind(&input.original_filename)
            .bind(uploaded_at)
            .bind(input.uploaded_by_admin_id)
            .bind(&input.description)
            .bind(&input.usage)
            .bind(hook_metadata_to_json(&input.hook_metadata)?)
            .bind(&supported_slots)
            .bind(input.schema_hash.as_ref().map(|hash| hash.as_slice()))
            .bind(current.id)
            .bind(u64_to_i64(expected_revision, "wasm_registry.revision")?)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        if result.rows_affected() == 0 {
            return Err(StorageError::StalePluginRegistryRevision {
                current: current.revision,
            });
        }

        sqlx::query("DELETE FROM wasm_blobs_v2 b WHERE b.sha256 = $1 AND NOT EXISTS (SELECT 1 FROM wasm_registry_v2 r WHERE r.sha256 = b.sha256)")
            .bind(current.sha256.as_slice())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        let entry = registry_by_id_in_tx(&mut tx, current.id)
            .await?
            .ok_or_else(|| conflict("updated plugin registry row disappeared"))?;
        sqlx::query("SELECT pg_notify('cclb_plugin_changed', $1)")
            .bind(entry.id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(entry)
    }

    async fn list_registry_references(&self, id: Uuid) -> StorageResult<WasmRegistryReferences> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
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
        let mut tx = begin_repeatable_read(&self.pool).await?;
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

        sqlx::query("DELETE FROM plugin_chains_v2 WHERE wasm_registry_id = $1")
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        sqlx::query("UPDATE upstream_spec_v1 SET warmup_dialect_plugin = NULL, spec_revision = spec_revision + 1, updated_at = NOW() WHERE deleted_at IS NULL AND (warmup_dialect_plugin->>'wasm_registry_id')::uuid = $1")
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        let result = sqlx::query("DELETE FROM wasm_registry_v2 WHERE id = $1 AND revision = $2")
            .bind(id)
            .bind(u64_to_i64(expected_revision, "wasm_registry.revision")?)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        if result.rows_affected() == 0 {
            return Err(StorageError::StalePluginRegistryRevision {
                current: entry.revision,
            });
        }
        sqlx::query("DELETE FROM wasm_blobs_v2 b WHERE b.sha256 = $1 AND NOT EXISTS (SELECT 1 FROM wasm_registry_v2 r WHERE r.sha256 = b.sha256)")
            .bind(entry.sha256.as_slice())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        notify_cascade_reference_changes(&mut tx, &references.references).await?;
        sqlx::query("SELECT pg_notify('cclb_plugin_changed', $1)")
            .bind(id.to_string())
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
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let current_revision: Option<i64> =
            sqlx::query_scalar("SELECT revision FROM wasm_registry_v2 WHERE id = $1 FOR UPDATE")
                .bind(id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
        let current_revision =
            current_revision.ok_or_else(|| conflict("unknown plugin registry entry"))?;
        let current = i64_to_u64(current_revision, "wasm_registry.revision")?;
        if current != expected_revision {
            return Err(StorageError::StalePluginRegistryRevision { current });
        }
        let row = sqlx::query("UPDATE wasm_registry_v2 SET label=$1, revision=revision+1 WHERE id=$2 AND revision=$3 RETURNING *")
            .bind(label).bind(id).bind(u64_to_i64(expected_revision, "wasm_registry.revision")?)
            .fetch_optional(&mut *tx).await.map_err(map_sqlx_error)?;
        let row = row.ok_or_else(|| conflict("updated plugin registry row disappeared"))?;
        sqlx::query("SELECT pg_notify('cclb_plugin_changed', $1)")
            .bind(id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)?;
        self.get_registry_entry_by_id(row.try_get("id").map_err(map_sqlx_error)?)
            .await?
            .ok_or_else(|| conflict("updated plugin registry row disappeared"))
    }

    async fn update_supported_slots(
        &self,
        id: Uuid,
        supported_slots: Vec<PluginSlotKind>,
    ) -> StorageResult<()> {
        let slots: Vec<String> = supported_slots
            .iter()
            .map(|slot| slot.as_str().to_owned())
            .collect();
        sqlx::query("UPDATE wasm_registry_v2 SET supported_slots = $1 WHERE id = $2")
            .bind(&slots)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn delete_registry_entry(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> StorageResult<Option<WasmRegistryEntry>> {
        retry::with_retry_serialization(&retry::RetryPolicy::default(), || {
            delete_registry_entry_once(&self.pool, id, expected_revision)
        })
        .await
    }

    async fn decrement_blob_refcount_or_delete(&self, sha256: [u8; 32]) -> StorageResult<bool> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let referenced: Option<Vec<u8>> =
            sqlx::query_scalar("SELECT sha256 FROM wasm_registry_v2 WHERE sha256 = $1 LIMIT 1")
                .bind(sha256.as_slice())
                .fetch_optional(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
        let removed = if referenced.is_some() {
            false
        } else {
            let result = sqlx::query("DELETE FROM wasm_blobs_v2 WHERE sha256 = $1")
                .bind(sha256.as_slice())
                .execute(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
            result.rows_affected() > 0
        };
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(removed)
    }

    async fn insert_chain_entry(
        &self,
        input: PluginChainEntryInput,
    ) -> StorageResult<PluginChainEntry> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let sha: Vec<u8> =
            sqlx::query_scalar("SELECT sha256 FROM wasm_registry_v2 WHERE id = $1 FOR UPDATE")
                .bind(input.wasm_registry_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(map_sqlx_error)?
                .ok_or_else(|| StorageError::PluginRegistryConflict {
                    message: "unknown plugin registry entry".to_owned(),
                })?;
        let blob_present: Option<Vec<u8>> =
            sqlx::query_scalar("SELECT sha256 FROM wasm_blobs_v2 WHERE sha256 = $1")
                .bind(&sha)
                .fetch_optional(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
        if blob_present.is_none() {
            return Err(StorageError::PluginRegistryConflict {
                message: "missing wasm blob".to_owned(),
            });
        }
        let principal_exists: Option<Uuid> = sqlx::query_scalar(
            "SELECT id FROM principals_v1 WHERE id = $1 AND deleted_at IS NULL FOR UPDATE",
        )
        .bind(input.principal_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        if principal_exists.is_none() {
            return Err(StorageError::PrincipalNotFound {
                id: input.principal_id.to_string(),
            });
        }
        if is_singleton_slot(input.slot)
            && let Some(existing_entry_id) = sqlx::query_scalar::<_, Uuid>(
                "SELECT id FROM plugin_chains_v2 WHERE principal_id = $1 AND slot = $2 ORDER BY id ASC LIMIT 1",
            )
            .bind(input.principal_id)
            .bind(input.slot.as_str())
            .fetch_optional(&mut *tx)
            .await
            .map_err(map_sqlx_error)?
        {
            return Err(StorageError::PluginChainConflict {
                reason: PluginChainConflictReason::SlotIsSingleton { existing_entry_id },
            });
        }
        let id = Uuid::new_v4();
        let row = sqlx::query("INSERT INTO plugin_chains_v2 (id, principal_id, slot, order_value, wasm_registry_id, config, revision) VALUES ($1,$2,$3,$4,$5,$6,0) RETURNING *")
            .bind(id).bind(input.principal_id).bind(input.slot.as_str()).bind(input.order).bind(input.wasm_registry_id).bind(input.config)
            .fetch_one(&mut *tx).await.map_err(map_sqlx_error)?;
        sqlx::query("SELECT pg_notify('cclb_plugin_chain_changed', $1)")
            .bind(input.principal_id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)?;
        chain_from_row(row)
    }

    async fn list_chain_for_principal(
        &self,
        principal_id: Uuid,
        slot: PluginSlotKind,
    ) -> StorageResult<Vec<PluginChainEntry>> {
        let rows = sqlx::query("SELECT * FROM plugin_chains_v2 WHERE principal_id = $1 AND slot = $2 ORDER BY order_value ASC, id ASC")
            .bind(principal_id).bind(slot.as_str()).fetch_all(&self.pool).await.map_err(map_sqlx_error)?;
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

        let slot_names = slots
            .iter()
            .map(|slot| slot.as_str().to_owned())
            .collect::<Vec<_>>();
        let rows = sqlx::query(
            "SELECT * FROM plugin_chains_v2 WHERE principal_id = ANY($1) AND slot = ANY($2) ORDER BY principal_id ASC, slot ASC, order_value ASC, id ASC",
        )
        .bind(principal_ids.to_vec())
        .bind(slot_names)
        .fetch_all(&self.pool)
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
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let Some(current) = self.get_chain_in_tx(&mut tx, id).await? else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(None);
        };
        if current.revision != expected_revision {
            return Err(StorageError::StalePluginChainRevision {
                current: current.revision,
            });
        }
        let row = sqlx::query("UPDATE plugin_chains_v2 SET config=$3, revision=revision+1 WHERE id=$1 AND revision=$2 RETURNING *")
            .bind(id).bind(u64_to_i64(expected_revision, "plugin_chain.revision")?).bind(config)
            .fetch_one(&mut *tx).await.map_err(map_sqlx_error)?;
        let entry = chain_from_row(row)?;
        sqlx::query("SELECT pg_notify('cclb_plugin_chain_changed', $1)")
            .bind(entry.principal_id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(Some(entry))
    }

    async fn reorder_chain(
        &self,
        principal_id: Uuid,
        slot: PluginSlotKind,
        new_orders: Vec<(Uuid, i64, u64)>,
    ) -> StorageResult<Vec<PluginChainEntry>> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let mut staged_orders = HashMap::with_capacity(new_orders.len());
        let mut staged_ids = Vec::with_capacity(new_orders.len());
        let mut staged_order_values = Vec::with_capacity(new_orders.len());
        let mut staged_revisions = Vec::with_capacity(new_orders.len());
        let mut seen_ids = HashSet::with_capacity(new_orders.len());
        for (id, order, expected_revision) in &new_orders {
            if !seen_ids.insert(*id) {
                return Err(StorageError::Conflict {
                    message: "duplicate_plugin_chain_entry".to_owned(),
                });
            }
            let row = sqlx::query("SELECT * FROM plugin_chains_v2 WHERE id = $1 FOR UPDATE")
                .bind(*id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
            let Some(row) = row else {
                return Err(StorageError::Conflict {
                    message: "unknown plugin chain entry".to_owned(),
                });
            };
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
            staged_ids.push(*id);
            staged_order_values.push(*order);
            staged_revisions.push(u64_to_i64(*expected_revision, "plugin_chain.revision")?);
        }
        let rows = sqlx::query("SELECT * FROM plugin_chains_v2 WHERE principal_id = $1 AND slot = $2 ORDER BY order_value ASC, id ASC FOR UPDATE")
            .bind(principal_id)
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
        if !staged_ids.is_empty() {
            let result = sqlx::query("UPDATE plugin_chains_v2 AS existing SET order_value = staged.order_value, revision = existing.revision + 1 FROM (SELECT * FROM UNNEST($1::uuid[], $2::bigint[], $3::bigint[]) AS staged(id, order_value, revision)) AS staged WHERE existing.id = staged.id AND existing.principal_id = $4 AND existing.slot = $5 AND existing.revision = staged.revision")
                .bind(&staged_ids)
                .bind(&staged_order_values)
                .bind(&staged_revisions)
                .bind(principal_id)
                .bind(slot.as_str())
                .execute(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
            if result.rows_affected() != staged_ids.len() as u64 {
                return Err(StorageError::Conflict {
                    message: "stale or missing plugin chain revision".to_owned(),
                });
            }
            sqlx::query("SELECT pg_notify('cclb_plugin_chain_changed', $1)")
                .bind(principal_id.to_string())
                .execute(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
        }
        let rows = sqlx::query("SELECT * FROM plugin_chains_v2 WHERE principal_id = $1 AND slot = $2 ORDER BY order_value ASC, id ASC")
            .bind(principal_id)
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
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let Some(entry) = self.get_chain_in_tx(&mut tx, id).await? else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(None);
        };
        if entry.revision != expected_revision {
            return Err(StorageError::StalePluginChainRevision {
                current: entry.revision,
            });
        }
        let result = sqlx::query("DELETE FROM plugin_chains_v2 WHERE id = $1 AND revision = $2")
            .bind(id)
            .bind(u64_to_i64(expected_revision, "plugin_chain.revision")?)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        if result.rows_affected() == 0 {
            return Err(StorageError::StalePluginChainRevision {
                current: entry.revision,
            });
        }
        sqlx::query("SELECT pg_notify('cclb_plugin_chain_changed', $1)")
            .bind(entry.principal_id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
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

impl PostgresStorage {
    async fn registry_by_sha_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        sha256: [u8; 32],
    ) -> StorageResult<Option<WasmRegistryEntry>> {
        let row = sqlx::query("SELECT r.*, (SELECT COUNT(*) FROM plugin_chains_v2 c WHERE c.wasm_registry_id = r.id) + (SELECT COUNT(*) FROM upstream_spec_v1 u WHERE u.deleted_at IS NULL AND (u.warmup_dialect_plugin->>'wasm_registry_id')::uuid = r.id) AS refcount FROM wasm_registry_v2 r WHERE r.sha256 = $1 FOR UPDATE")
            .bind(sha256.as_slice())
            .fetch_optional(&mut **tx)
            .await
            .map_err(map_sqlx_error)?;
        row.map(registry_from_row).transpose()
    }

    async fn get_chain_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        id: Uuid,
    ) -> StorageResult<Option<PluginChainEntry>> {
        let row = sqlx::query("SELECT * FROM plugin_chains_v2 WHERE id = $1 FOR UPDATE")
            .bind(id)
            .fetch_optional(&mut **tx)
            .await
            .map_err(map_sqlx_error)?;
        row.map(chain_from_row).transpose()
    }
}

async fn registry_by_name_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    name: &str,
) -> StorageResult<Option<WasmRegistryEntry>> {
    let row = sqlx::query("SELECT r.*, (SELECT COUNT(*) FROM plugin_chains_v2 c WHERE c.wasm_registry_id = r.id) + (SELECT COUNT(*) FROM upstream_spec_v1 u WHERE u.deleted_at IS NULL AND (u.warmup_dialect_plugin->>'wasm_registry_id')::uuid = r.id) AS refcount FROM wasm_registry_v2 r WHERE r.name = $1 FOR UPDATE")
        .bind(name)
        .fetch_optional(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
    row.map(registry_from_row).transpose()
}

async fn registry_by_id_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    id: Uuid,
) -> StorageResult<Option<WasmRegistryEntry>> {
    let row = sqlx::query("SELECT r.*, (SELECT COUNT(*) FROM plugin_chains_v2 c WHERE c.wasm_registry_id = r.id) + (SELECT COUNT(*) FROM upstream_spec_v1 u WHERE u.deleted_at IS NULL AND (u.warmup_dialect_plugin->>'wasm_registry_id')::uuid = r.id) AS refcount FROM wasm_registry_v2 r WHERE r.id = $1 FOR UPDATE")
        .bind(id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
    row.map(registry_from_row).transpose()
}

async fn registry_references_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    registry_id: Uuid,
) -> StorageResult<WasmRegistryReferences> {
    let chain_rows = sqlx::query("SELECT c.id AS chain_entry_id, c.principal_id, p.name AS principal_name, c.slot, c.revision FROM plugin_chains_v2 c JOIN principals_v1 p ON p.id = c.principal_id WHERE c.wasm_registry_id = $1 ORDER BY c.id ASC FOR UPDATE OF c")
        .bind(registry_id)
        .fetch_all(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
    let mut references = Vec::with_capacity(chain_rows.len());
    for row in chain_rows {
        let slot = row.try_get::<String, _>("slot").map_err(map_sqlx_error)?;
        references.push(WasmRegistryReference::PluginChain {
            chain_entry_id: row.try_get("chain_entry_id").map_err(map_sqlx_error)?,
            principal_id: row.try_get("principal_id").map_err(map_sqlx_error)?,
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

    let upstream_rows = sqlx::query("SELECT id, name, spec_revision AS revision FROM upstream_spec_v1 WHERE deleted_at IS NULL AND (warmup_dialect_plugin->>'wasm_registry_id')::uuid = $1 ORDER BY id ASC FOR UPDATE")
        .bind(registry_id)
        .fetch_all(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
    references.reserve(upstream_rows.len());
    for row in upstream_rows {
        references.push(WasmRegistryReference::UpstreamWarmupDialect {
            upstream_id: row.try_get("id").map_err(map_sqlx_error)?,
            upstream_name: row.try_get("name").map_err(map_sqlx_error)?,
            revision: i64_to_u64(
                row.try_get("revision").map_err(map_sqlx_error)?,
                "upstream.spec_revision",
            )?,
        });
    }
    Ok(WasmRegistryReferences::from_references(references))
}

async fn notify_cascade_reference_changes(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    references: &[WasmRegistryReference],
) -> StorageResult<()> {
    let mut principals = HashSet::new();
    for reference in references {
        match reference {
            WasmRegistryReference::PluginChain { principal_id, .. } => {
                principals.insert(*principal_id);
            }
            WasmRegistryReference::UpstreamWarmupDialect { upstream_id, .. } => {
                sqlx::query("SELECT pg_notify('cclb_upstream_changed', $1)")
                    .bind(upstream_id.to_string())
                    .execute(&mut **tx)
                    .await
                    .map_err(map_sqlx_error)?;
            }
        }
    }
    for principal_id in principals {
        sqlx::query("SELECT pg_notify('cclb_plugin_chain_changed', $1)")
            .bind(principal_id.to_string())
            .execute(&mut **tx)
            .await
            .map_err(map_sqlx_error)?;
    }
    Ok(())
}

async fn insert_blob_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    blob: &WasmBlob,
) -> StorageResult<bool> {
    let validated_at = unix_secs_to_datetime(
        blob.parse_validated_at_unix_secs,
        "wasm_blob.parse_validated_at",
    )?;
    let inserted = sqlx::query_scalar::<_, Vec<u8>>("INSERT INTO wasm_blobs_v2 (sha256, bytes, size_bytes, parse_validated_at, created_at) VALUES ($1, $2, $3, $4, NOW()) ON CONFLICT (sha256) DO NOTHING RETURNING sha256")
        .bind(blob.sha256.as_slice())
        .bind(blob.bytes.as_slice())
        .bind(u64_to_i64(blob.size_bytes, "wasm_blob.size_bytes")?)
        .bind(validated_at)
        .fetch_optional(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
    Ok(inserted.is_some())
}

async fn insert_registry_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    id: Uuid,
    sha256: [u8; 32],
    input: &WasmRegistryEntryInput,
    uploaded_at: DateTime<Utc>,
) -> StorageResult<Option<WasmRegistryEntry>> {
    let supported_slots: Vec<String> = input
        .supported_slots
        .iter()
        .map(|slot| slot.as_str().to_owned())
        .collect();
    let row = sqlx::query("INSERT INTO wasm_registry_v2 (id, sha256, name, plugin_version, original_filename, label, uploaded_at, uploaded_by_admin_id, revision, description, usage, hook_metadata, supported_slots, schema_hash) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0, $9, $10, $11, $12, $13) ON CONFLICT (sha256) DO NOTHING RETURNING id, sha256, name, plugin_version, original_filename, label, uploaded_at, uploaded_by_admin_id, 0::BIGINT AS refcount, revision, description, usage, hook_metadata, supported_slots, schema_hash")
        .bind(id)
        .bind(sha256.as_slice())
        .bind(&input.name)
        .bind(&input.version)
        .bind(&input.original_filename)
        .bind(&input.label)
        .bind(uploaded_at)
        .bind(input.uploaded_by_admin_id)
        .bind(&input.description)
        .bind(&input.usage)
        .bind(hook_metadata_to_json(&input.hook_metadata)?)
        .bind(&supported_slots)
        .bind(input.schema_hash.as_ref().map(|h| h.as_slice()))
        .fetch_optional(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
    row.map(registry_from_row).transpose()
}

async fn delete_registry_entry_once(
    pool: &sqlx::PgPool,
    id: Uuid,
    expected_revision: u64,
) -> StorageResult<Option<WasmRegistryEntry>> {
    let mut tx = begin_repeatable_read(pool).await?;
    let row = sqlx::query("SELECT r.*, (SELECT COUNT(*) FROM plugin_chains_v2 c WHERE c.wasm_registry_id = r.id) + (SELECT COUNT(*) FROM upstream_spec_v1 u WHERE u.deleted_at IS NULL AND (u.warmup_dialect_plugin->>'wasm_registry_id')::uuid = r.id) AS refcount FROM wasm_registry_v2 r WHERE r.id = $1 FOR UPDATE")
        .bind(id).fetch_optional(&mut *tx).await.map_err(map_sqlx_error)?;
    let Some(row) = row else {
        tx.commit().await.map_err(map_sqlx_error)?;
        return Ok(None);
    };
    let entry = registry_from_row(row)?;
    if entry.revision != expected_revision {
        return Err(StorageError::StalePluginRegistryRevision {
            current: entry.revision,
        });
    }
    if let Some(chain_id) = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM plugin_chains_v2 WHERE wasm_registry_id = $1 LIMIT 1",
    )
    .bind(id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(map_sqlx_error)?
    {
        return Err(StorageError::PluginRegistryReferenced {
            id: chain_id.to_string(),
        });
    }
    if let Some(upstream_id) = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM upstream_spec_v1 WHERE deleted_at IS NULL AND (warmup_dialect_plugin->>'wasm_registry_id')::uuid = $1 LIMIT 1",
    )
    .bind(id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(map_sqlx_error)?
    {
        return Err(StorageError::PluginRegistryReferenced {
            id: upstream_id.to_string(),
        });
    }
    let result = sqlx::query("DELETE FROM wasm_registry_v2 WHERE id = $1 AND revision = $2")
        .bind(id)
        .bind(u64_to_i64(expected_revision, "wasm_registry.revision")?)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
    if result.rows_affected() == 0 {
        return Err(StorageError::StalePluginRegistryRevision {
            current: entry.revision,
        });
    }
    sqlx::query("DELETE FROM wasm_blobs_v2 b WHERE b.sha256 = $1 AND NOT EXISTS (SELECT 1 FROM wasm_registry_v2 r WHERE r.sha256 = b.sha256)")
        .bind(entry.sha256.as_slice())
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
    sqlx::query("SELECT pg_notify('cclb_plugin_changed', $1)")
        .bind(id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
    tx.commit().await.map_err(map_sqlx_error)?;
    Ok(Some(entry))
}

async fn begin_repeatable_read(
    pool: &sqlx::PgPool,
) -> StorageResult<sqlx::Transaction<'_, sqlx::Postgres>> {
    let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
    Ok(tx)
}

fn registry_from_row(row: sqlx::postgres::PgRow) -> StorageResult<WasmRegistryEntry> {
    let id = row.try_get("id").map_err(map_sqlx_error)?;
    let refcount = row.try_get("refcount").map_err(map_sqlx_error)?;
    if id == BUILTIN_SUBSCRIPTION_PREFERENCE_ID {
        return Ok(WasmRegistryEntry::builtin_subscription_preference(refcount));
    }

    let uploaded_at: DateTime<Utc> = row.try_get("uploaded_at").map_err(map_sqlx_error)?;
    Ok(WasmRegistryEntry {
        id,
        sha256: sha_to_array(
            &row.try_get::<Vec<u8>, _>("sha256")
                .map_err(map_sqlx_error)?,
        )?,
        name: row.try_get("name").map_err(map_sqlx_error)?,
        version: row.try_get("plugin_version").map_err(map_sqlx_error)?,
        original_filename: row.try_get("original_filename").map_err(map_sqlx_error)?,
        label: row.try_get("label").map_err(map_sqlx_error)?,
        uploaded_at_unix_secs: datetime_to_unix_secs(uploaded_at, "wasm_registry.uploaded_at")?,
        uploaded_by_admin_id: row
            .try_get("uploaded_by_admin_id")
            .map_err(map_sqlx_error)?,
        refcount,
        revision: i64_to_u64(
            row.try_get("revision").map_err(map_sqlx_error)?,
            "wasm_registry.revision",
        )?,
        kind: "filter".to_owned(),
        description: row.try_get("description").map_err(map_sqlx_error)?,
        usage: row.try_get("usage").map_err(map_sqlx_error)?,
        hook_metadata: hook_metadata_from_json(
            &row.try_get::<String, _>("hook_metadata")
                .map_err(map_sqlx_error)?,
        )?,
        is_builtin: false,
        metadata: None,
        supported_slots: row
            .try_get::<Vec<String>, _>("supported_slots")
            .map_err(map_sqlx_error)?
            .into_iter()
            .filter_map(|s| PluginSlotKind::parse(&s))
            .collect(),
        schema_hash: row
            .try_get::<Option<Vec<u8>>, _>("schema_hash")
            .map_err(map_sqlx_error)?
            .map(|bytes| sha_to_array(&bytes))
            .transpose()?,
    })
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

fn chain_from_row(row: sqlx::postgres::PgRow) -> StorageResult<PluginChainEntry> {
    Ok(PluginChainEntry {
        id: row.try_get("id").map_err(map_sqlx_error)?,
        principal_id: row.try_get("principal_id").map_err(map_sqlx_error)?,
        slot: PluginSlotKind::parse(
            row.try_get::<String, _>("slot")
                .map_err(map_sqlx_error)?
                .as_str(),
        )
        .ok_or_else(|| StorageError::Corrupted {
            message: "invalid plugin slot".to_owned(),
        })?,
        order: row.try_get("order_value").map_err(map_sqlx_error)?,
        wasm_registry_id: row.try_get("wasm_registry_id").map_err(map_sqlx_error)?,
        config: row.try_get::<Value, _>("config").map_err(map_sqlx_error)?,
        revision: i64_to_u64(
            row.try_get("revision").map_err(map_sqlx_error)?,
            "plugin_chain.revision",
        )?,
    })
}

fn hook_metadata_to_json(metadata: &BTreeMap<String, HookMetadata>) -> StorageResult<String> {
    serde_json::to_string(metadata).map_err(StorageError::from)
}

fn hook_metadata_from_json(value: &str) -> StorageResult<BTreeMap<String, HookMetadata>> {
    serde_json::from_str(value).map_err(StorageError::from)
}

fn sha_to_array(bytes: &[u8]) -> StorageResult<[u8; 32]> {
    <[u8; 32]>::try_from(bytes).map_err(|_| StorageError::Corrupted {
        message: "sha256 must be 32 bytes".to_owned(),
    })
}
