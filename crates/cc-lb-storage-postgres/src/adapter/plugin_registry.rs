use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use cc_lb_storage_api::{
    MAX_WASM_BLOB_BYTES, PluginChainEntry, PluginChainEntryInput, PluginChainEntryUpdate,
    PluginRegistryStore, PluginSlot, StorageError, StorageResult, WasmBlob, WasmBlobRecord,
    WasmRegistryEntry, WasmRegistryEntryInput, sparse_order, validate_identifier,
};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::Row;
use uuid::Uuid;

use crate::{
    adapter::{
        PostgresStorage, conflict, datetime_to_unix_secs, i64_to_u64, u64_to_i64,
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
        let mut tx = begin_repeatable_read(&self.pool).await?;
        if let Some(existing) = self.registry_by_sha_in_tx(&mut tx, blob.sha256).await? {
            insert_missing_blob_in_tx(&mut tx, &blob, existing.id).await?;
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok((existing, true));
        }
        let id = Uuid::new_v4();
        let uploaded_at =
            unix_secs_to_datetime(input.uploaded_at_unix_secs, "wasm_registry.uploaded_at")?;
        insert_blob_in_tx(&mut tx, &blob, 0).await?;
        sqlx::query("INSERT INTO wasm_registry_v2 (id, sha256, name, original_filename, label, uploaded_at, uploaded_by_admin_id, revision) VALUES ($1, $2, $3, $4, $5, $6, $7, 0)")
            .bind(id).bind(blob.sha256.as_slice()).bind(input.name).bind(input.original_filename).bind(input.label).bind(uploaded_at).bind(input.uploaded_by_admin_id)
            .execute(&mut *tx).await.map_err(map_sqlx_error)?;
        let row = sqlx::query("SELECT r.*, COALESCE((SELECT COUNT(*) FROM plugin_chains_v2 c WHERE c.wasm_registry_id = r.id), 0)::BIGINT AS refcount FROM wasm_registry_v2 r WHERE r.id = $1")
            .bind(id).fetch_one(&mut *tx).await.map_err(map_sqlx_error)?;
        sqlx::query("SELECT pg_notify('cclb_plugin_changed', $1)")
            .bind(id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)?;
        registry_from_row(row).map(|entry| (entry, false))
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
        let rows = sqlx::query("SELECT r.*, b.refcount FROM wasm_registry_v2 r JOIN wasm_blobs_v2 b ON b.sha256 = r.sha256 WHERE ($1::uuid IS NULL OR r.id > $1) ORDER BY r.id ASC LIMIT $2")
            .bind(after).bind(u64_to_i64(limit as u64, "plugin_registry.limit")?).fetch_all(&self.pool).await.map_err(map_sqlx_error)?;
        rows.into_iter().map(registry_from_row).collect()
    }

    async fn get_registry_entry_by_sha(
        &self,
        sha256: [u8; 32],
    ) -> StorageResult<Option<WasmRegistryEntry>> {
        let row = sqlx::query("SELECT r.*, b.refcount FROM wasm_registry_v2 r JOIN wasm_blobs_v2 b ON b.sha256 = r.sha256 WHERE r.sha256 = $1")
            .bind(sha256.as_slice()).fetch_optional(&self.pool).await.map_err(map_sqlx_error)?;
        row.map(registry_from_row).transpose()
    }

    async fn get_registry_entry_by_id(&self, id: Uuid) -> StorageResult<Option<WasmRegistryEntry>> {
        let row = sqlx::query("SELECT r.*, b.refcount FROM wasm_registry_v2 r JOIN wasm_blobs_v2 b ON b.sha256 = r.sha256 WHERE r.id = $1")
            .bind(id).fetch_optional(&self.pool).await.map_err(map_sqlx_error)?;
        row.map(registry_from_row).transpose()
    }

    async fn update_registry_label(
        &self,
        id: Uuid,
        expected_revision: u64,
        label: Option<String>,
    ) -> StorageResult<WasmRegistryEntry> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let row = sqlx::query("UPDATE wasm_registry_v2 SET label=$1, revision=revision+1 WHERE id=$2 AND revision=$3 RETURNING *")
            .bind(label).bind(id).bind(u64_to_i64(expected_revision, "wasm_registry.revision")?)
            .fetch_optional(&mut *tx).await.map_err(map_sqlx_error)?;
        let row = match row {
            Some(row) => row,
            None => return Err(conflict("stale or missing plugin registry revision")),
        };
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

    async fn delete_registry_entry(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> StorageResult<Option<WasmRegistryEntry>> {
        let mut tx = begin_repeatable_read(&self.pool).await?;
        let row = sqlx::query("SELECT r.*, COALESCE((SELECT COUNT(*) FROM plugin_chains_v2 c WHERE c.wasm_registry_id = r.id), 0)::BIGINT AS refcount FROM wasm_registry_v2 r WHERE r.id = $1 FOR UPDATE")
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
        sqlx::query("DELETE FROM wasm_blobs_v2 WHERE sha256 = $1")
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

    async fn decrement_blob_refcount_or_delete(&self, sha256: [u8; 32]) -> StorageResult<bool> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let changed = decrement_blob_in_tx(&mut tx, sha256).await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(changed)
    }

    async fn insert_chain_entry(
        &self,
        input: PluginChainEntryInput,
    ) -> StorageResult<PluginChainEntry> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let sha: Vec<u8> = sqlx::query_scalar(
            "SELECT sha256 FROM wasm_registry_v2 WHERE id = $1 FOR UPDATE",
        )
        .bind(input.wasm_registry_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx_error)?
        .ok_or_else(|| StorageError::PluginRegistryConflict {
            message: "unknown plugin registry entry".to_owned(),
        })?;
        let blob_refcount: Option<i64> = sqlx::query_scalar(
            "SELECT refcount FROM wasm_blobs_v2 WHERE sha256 = $1 FOR UPDATE",
        )
        .bind(&sha)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        if blob_refcount.is_none() {
            return Err(StorageError::PluginRegistryConflict {
                message: "missing wasm blob".to_owned(),
            });
        }
        let principal_exists: Option<i64> =
            sqlx::query_scalar("SELECT 1 FROM principals_v1 WHERE id = $1")
                .bind(input.principal_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
        if principal_exists.is_none() {
            return Err(StorageError::PrincipalNotFound {
                id: input.principal_id.to_string(),
            });
        }
        sqlx::query("UPDATE wasm_blobs_v2 SET refcount = refcount + 1 WHERE sha256 = $1")
            .bind(&sha)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        let id = Uuid::new_v4();
        let row = sqlx::query("INSERT INTO plugin_chains_v2 (id, principal_id, slot, order_value, wasm_registry_id, config, sse_per_event, batched_events_per_flush, batched_flush_ms, revision) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,0) RETURNING *")
            .bind(id).bind(input.principal_id).bind(input.slot.as_str()).bind(input.order).bind(input.wasm_registry_id).bind(input.config).bind(input.sse_per_event).bind(i32::try_from(input.batched_events_per_flush).map_err(|_| StorageError::Fatal { message: "batched_events_per_flush exceeds i32".to_owned() })?).bind(u64_to_i64(input.batched_flush_ms, "plugin_chain.batched_flush_ms")?)
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
        slot: PluginSlot,
    ) -> StorageResult<Vec<PluginChainEntry>> {
        let rows = sqlx::query("SELECT * FROM plugin_chains_v2 WHERE principal_id = $1 AND slot = $2 ORDER BY order_value ASC, id ASC")
            .bind(principal_id).bind(slot.as_str()).fetch_all(&self.pool).await.map_err(map_sqlx_error)?;
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
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let Some(mut current) = self.get_chain_in_tx(&mut tx, id).await? else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(None);
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
        let row = sqlx::query("UPDATE plugin_chains_v2 SET config=$3, sse_per_event=$4, batched_events_per_flush=$5, batched_flush_ms=$6, revision=revision+1 WHERE id=$1 AND revision=$2 RETURNING *")
            .bind(id).bind(u64_to_i64(expected_revision, "plugin_chain.revision")?).bind(current.config).bind(current.sse_per_event).bind(i32::try_from(current.batched_events_per_flush).map_err(|_| StorageError::Fatal { message: "batched_events_per_flush exceeds i32".to_owned() })?).bind(u64_to_i64(current.batched_flush_ms, "plugin_chain.batched_flush_ms")?)
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
        slot: PluginSlot,
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
                return Err(StorageError::PluginChainConflict {
                    message: "duplicate_plugin_chain_entry".to_owned(),
                });
            }
            let row = sqlx::query("SELECT * FROM plugin_chains_v2 WHERE id = $1 FOR UPDATE")
                .bind(*id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
            let Some(row) = row else {
                return Err(StorageError::PluginChainConflict {
                    message: "unknown plugin chain entry".to_owned(),
                });
            };
            let entry = chain_from_row(row)?;
            if entry.principal_id != principal_id || entry.slot != slot {
                return Err(StorageError::PluginChainConflict {
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
        if entries.windows(2).any(|pair| {
            pair[1]
                .order
                .checked_sub(pair[0].order)
                .unwrap_or(i64::MAX)
                < 2
        }) {
            return Err(StorageError::PluginChainConflict {
                message: "invalid_order_gap_below_2".to_owned(),
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
                return Err(StorageError::PluginChainConflict {
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
        let sha =
            sqlx::query_scalar::<_, Vec<u8>>("SELECT sha256 FROM wasm_registry_v2 WHERE id = $1")
                .bind(entry.wasm_registry_id)
                .fetch_one(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
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
        decrement_blob_in_tx(&mut tx, sha_to_array(&sha)?).await?;
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

impl PostgresStorage {
    async fn registry_by_sha_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        sha256: [u8; 32],
    ) -> StorageResult<Option<WasmRegistryEntry>> {
        let row = sqlx::query("SELECT r.*, COALESCE((SELECT COUNT(*) FROM plugin_chains_v2 c WHERE c.wasm_registry_id = r.id), 0)::BIGINT AS refcount FROM wasm_registry_v2 r WHERE r.sha256 = $1 FOR UPDATE")
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

async fn insert_missing_blob_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    blob: &WasmBlob,
    registry_id: Uuid,
) -> StorageResult<()> {
    let exists: Option<i64> =
        sqlx::query_scalar("SELECT 1 FROM wasm_blobs_v2 WHERE sha256 = $1 FOR UPDATE")
            .bind(blob.sha256.as_slice())
            .fetch_optional(&mut **tx)
            .await
            .map_err(map_sqlx_error)?;
    if exists.is_some() {
        return Ok(());
    }
    let refcount: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM plugin_chains_v2 WHERE wasm_registry_id = $1")
            .bind(registry_id)
            .fetch_one(&mut **tx)
            .await
            .map_err(map_sqlx_error)?;
    insert_blob_in_tx(tx, blob, refcount).await
}

async fn insert_blob_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    blob: &WasmBlob,
    refcount: i64,
) -> StorageResult<()> {
    let validated_at = unix_secs_to_datetime(
        blob.parse_validated_at_unix_secs,
        "wasm_blob.parse_validated_at",
    )?;
    sqlx::query("INSERT INTO wasm_blobs_v2 (sha256, bytes, size_bytes, parse_validated_at, refcount, created_at) VALUES ($1, $2, $3, $4, $5, NOW())")
        .bind(blob.sha256.as_slice())
        .bind(blob.bytes.as_slice())
        .bind(u64_to_i64(blob.size_bytes, "wasm_blob.size_bytes")?)
        .bind(validated_at)
        .bind(refcount)
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
    Ok(())
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

async fn decrement_blob_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sha256: [u8; 32],
) -> StorageResult<bool> {
    let refcount: Option<i64> =
        sqlx::query_scalar("SELECT refcount FROM wasm_blobs_v2 WHERE sha256 = $1 FOR UPDATE")
            .bind(sha256.as_slice())
            .fetch_optional(&mut **tx)
            .await
            .map_err(map_sqlx_error)?;
    let Some(refcount) = refcount else {
        return Ok(false);
    };
    if refcount == 0 {
        return Ok(false);
    }
    sqlx::query("UPDATE wasm_blobs_v2 SET refcount = GREATEST(refcount - 1, 0) WHERE sha256 = $1")
        .bind(sha256.as_slice())
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
    Ok(true)
}

fn registry_from_row(row: sqlx::postgres::PgRow) -> StorageResult<WasmRegistryEntry> {
    let uploaded_at: DateTime<Utc> = row.try_get("uploaded_at").map_err(map_sqlx_error)?;
    Ok(WasmRegistryEntry {
        id: row.try_get("id").map_err(map_sqlx_error)?,
        sha256: sha_to_array(
            &row.try_get::<Vec<u8>, _>("sha256")
                .map_err(map_sqlx_error)?,
        )?,
        name: row.try_get("name").map_err(map_sqlx_error)?,
        original_filename: row.try_get("original_filename").map_err(map_sqlx_error)?,
        label: row.try_get("label").map_err(map_sqlx_error)?,
        uploaded_at_unix_secs: datetime_to_unix_secs(uploaded_at, "wasm_registry.uploaded_at")?,
        uploaded_by_admin_id: row
            .try_get("uploaded_by_admin_id")
            .map_err(map_sqlx_error)?,
        refcount: row.try_get("refcount").map_err(map_sqlx_error)?,
        revision: i64_to_u64(
            row.try_get("revision").map_err(map_sqlx_error)?,
            "wasm_registry.revision",
        )?,
    })
}

fn chain_from_row(row: sqlx::postgres::PgRow) -> StorageResult<PluginChainEntry> {
    Ok(PluginChainEntry {
        id: row.try_get("id").map_err(map_sqlx_error)?,
        principal_id: row.try_get("principal_id").map_err(map_sqlx_error)?,
        slot: PluginSlot::parse(
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
        sse_per_event: row.try_get("sse_per_event").map_err(map_sqlx_error)?,
        batched_events_per_flush: u32::try_from(
            row.try_get::<i32, _>("batched_events_per_flush")
                .map_err(map_sqlx_error)?,
        )
        .map_err(|_| StorageError::Corrupted {
            message: "negative batched_events_per_flush".to_owned(),
        })?,
        batched_flush_ms: i64_to_u64(
            row.try_get("batched_flush_ms").map_err(map_sqlx_error)?,
            "plugin_chain.batched_flush_ms",
        )?,
        revision: i64_to_u64(
            row.try_get("revision").map_err(map_sqlx_error)?,
            "plugin_chain.revision",
        )?,
    })
}

fn sha_to_array(bytes: &[u8]) -> StorageResult<[u8; 32]> {
    <[u8; 32]>::try_from(bytes).map_err(|_| StorageError::Corrupted {
        message: "sha256 must be 32 bytes".to_owned(),
    })
}
