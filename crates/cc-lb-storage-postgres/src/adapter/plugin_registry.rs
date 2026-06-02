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
        // TODO: Oracle Medium 11 — wire in W3b.
        validate_identifier("plugin.name", &input.name)?;
        if blob.size_bytes > MAX_WASM_BLOB_BYTES || blob.bytes.len() as u64 > MAX_WASM_BLOB_BYTES {
            return Err(StorageError::InvalidInput {
                field: "wasm_blob.bytes".to_owned(),
                reason: format!("blob exceeds 32 MiB: {} bytes", blob.size_bytes),
            });
        }
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        if let Some(existing) = self.registry_by_sha_in_tx(&mut tx, blob.sha256).await? {
            if existing.name == input.name
                && existing.original_filename == input.original_filename
                && existing.label == input.label
                && existing.uploaded_by_admin_id == input.uploaded_by_admin_id
            {
                tx.commit().await.map_err(map_sqlx_error)?;
                return Ok((existing, false));
            }
            return Err(conflict(
                "sha256 already registered for a different wasm entry",
            ));
        }
        let id = Uuid::new_v4();
        let validated_at = unix_secs_to_datetime(
            blob.parse_validated_at_unix_secs,
            "wasm_blob.parse_validated_at",
        )?;
        let uploaded_at =
            unix_secs_to_datetime(input.uploaded_at_unix_secs, "wasm_registry.uploaded_at")?;
        sqlx::query("INSERT INTO wasm_blobs_v2 (sha256, bytes, size_bytes, parse_validated_at, refcount, created_at) VALUES ($1, $2, $3, $4, 0, NOW())")
            .bind(blob.sha256.as_slice()).bind(blob.bytes).bind(u64_to_i64(blob.size_bytes, "wasm_blob.size_bytes")?).bind(validated_at)
            .execute(&mut *tx).await.map_err(map_sqlx_error)?;
        sqlx::query("INSERT INTO wasm_registry_v2 (id, sha256, name, original_filename, label, uploaded_at, uploaded_by_admin_id, revision) VALUES ($1, $2, $3, $4, $5, $6, $7, 0)")
            .bind(id).bind(blob.sha256.as_slice()).bind(input.name).bind(input.original_filename).bind(input.label).bind(uploaded_at).bind(input.uploaded_by_admin_id)
            .execute(&mut *tx).await.map_err(map_sqlx_error)?;
        let row = sqlx::query("SELECT r.*, b.refcount FROM wasm_registry_v2 r JOIN wasm_blobs_v2 b ON b.sha256 = r.sha256 WHERE r.id = $1")
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
        let sha: Vec<u8> = sqlx::query_scalar("SELECT sha256 FROM wasm_registry_v2 WHERE id = $1")
            .bind(input.wasm_registry_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(map_sqlx_error)?
            .ok_or_else(|| conflict("unknown plugin registry entry"))?;
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
        let Some(mut current) = self.get_chain(id).await? else {
            return Ok(None);
        };
        if current.revision != expected_revision {
            return Err(conflict(format!(
                "stale plugin chain revision; current revision is {}",
                current.revision
            )));
        }
        if let Some(v) = update.config {
            current.config = v;
        }
        if let Some(v) = update.sse_per_event {
            current.sse_per_event = v;
        }
        if let Some(v) = update.batched_events_per_flush {
            current.batched_events_per_flush = v;
        }
        if let Some(v) = update.batched_flush_ms {
            current.batched_flush_ms = v;
        }
        let row = sqlx::query("UPDATE plugin_chains_v2 SET config=$3, sse_per_event=$4, batched_events_per_flush=$5, batched_flush_ms=$6, revision=revision+1 WHERE id=$1 AND revision=$2 RETURNING *")
            .bind(id).bind(u64_to_i64(expected_revision, "plugin_chain.revision")?).bind(current.config).bind(current.sse_per_event).bind(i32::try_from(current.batched_events_per_flush).map_err(|_| StorageError::Fatal { message: "batched_events_per_flush exceeds i32".to_owned() })?).bind(u64_to_i64(current.batched_flush_ms, "plugin_chain.batched_flush_ms")?)
            .fetch_one(&self.pool).await.map_err(map_sqlx_error)?;
        let entry = chain_from_row(row)?;
        self.notify_chain(entry.principal_id).await?;
        Ok(Some(entry))
    }

    async fn reorder_chain(
        &self,
        principal_id: Uuid,
        slot: PluginSlot,
        new_orders: Vec<(Uuid, i64, u64)>,
    ) -> StorageResult<Vec<PluginChainEntry>> {
        if sparse_order::needs_rebalance(
            &new_orders
                .iter()
                .map(|(_, order, _)| *order)
                .collect::<Vec<_>>(),
        ) {
            return Err(conflict("plugin chain order gaps need rebalance"));
        }
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        for (id, order, expected_revision) in new_orders {
            let result = sqlx::query("UPDATE plugin_chains_v2 SET order_value=$5, revision=revision+1 WHERE id=$1 AND principal_id=$2 AND slot=$3 AND revision=$4")
                .bind(id).bind(principal_id).bind(slot.as_str()).bind(u64_to_i64(expected_revision, "plugin_chain.revision")?).bind(order).execute(&mut *tx).await.map_err(map_sqlx_error)?;
            if result.rows_affected() == 0 {
                return Err(conflict("stale or missing plugin chain revision"));
            }
        }
        sqlx::query("SELECT pg_notify('cclb_plugin_chain_changed', $1)")
            .bind(principal_id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)?;
        self.list_chain_for_principal(principal_id, slot).await
    }

    async fn delete_chain_entry(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> StorageResult<Option<PluginChainEntry>> {
        // TODO: Oracle High 3 — wire in W3b.
        let _ = expected_revision;
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let Some(entry) = self.get_chain_in_tx(&mut tx, id).await? else {
            return Ok(None);
        };
        let sha =
            sqlx::query_scalar::<_, Vec<u8>>("SELECT sha256 FROM wasm_registry_v2 WHERE id = $1")
                .bind(entry.wasm_registry_id)
                .fetch_one(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
        sqlx::query("DELETE FROM plugin_chains_v2 WHERE id = $1")
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
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
        let row = sqlx::query("SELECT r.*, b.refcount FROM wasm_registry_v2 r JOIN wasm_blobs_v2 b ON b.sha256 = r.sha256 WHERE r.sha256 = $1").bind(sha256.as_slice()).fetch_optional(&mut **tx).await.map_err(map_sqlx_error)?;
        row.map(registry_from_row).transpose()
    }

    async fn get_chain(&self, id: Uuid) -> StorageResult<Option<PluginChainEntry>> {
        let row = sqlx::query("SELECT * FROM plugin_chains_v2 WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        row.map(chain_from_row).transpose()
    }

    async fn get_chain_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        id: Uuid,
    ) -> StorageResult<Option<PluginChainEntry>> {
        let row = sqlx::query("SELECT * FROM plugin_chains_v2 WHERE id = $1")
            .bind(id)
            .fetch_optional(&mut **tx)
            .await
            .map_err(map_sqlx_error)?;
        row.map(chain_from_row).transpose()
    }

    async fn notify_chain(&self, principal_id: Uuid) -> StorageResult<()> {
        sqlx::query("SELECT pg_notify('cclb_plugin_chain_changed', $1)")
            .bind(principal_id.to_string())
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        Ok(())
    }
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
    if refcount <= 1 {
        sqlx::query("DELETE FROM wasm_blobs_v2 WHERE sha256 = $1")
            .bind(sha256.as_slice())
            .execute(&mut **tx)
            .await
            .map_err(map_sqlx_error)?;
    } else {
        sqlx::query("UPDATE wasm_blobs_v2 SET refcount = refcount - 1 WHERE sha256 = $1")
            .bind(sha256.as_slice())
            .execute(&mut **tx)
            .await
            .map_err(map_sqlx_error)?;
    }
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
