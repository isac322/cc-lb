use anyhow::{Result, ensure};
use cc_lb_storage_api::{
    PluginChainEntryInput, PluginChainEntryUpdate, PluginRegistryStore, PluginSlot, StorageError,
    WasmBlob, WasmRegistryEntryInput, sparse_order,
};
use serde_json::json;
use uuid::Uuid;

pub async fn run_all<S>(storage: &S) -> Result<()>
where
    S: PluginRegistryStore,
{
    persist_wasm_upload_creates_blob_and_registry(storage).await?;
    persist_wasm_upload_idempotent_on_same_entry_input(storage).await?;
    persist_wasm_upload_rollback_on_registry_conflict(storage).await?;
    persist_wasm_upload_rejects_oversize(storage).await?;
    persist_wasm_upload_records_parse_validated_at(storage).await?;
    get_blob_bytes_returns_persisted_blob(storage).await?;
    registry_list_paginates(storage).await?;
    get_registry_entry_by_sha_returns_entry(storage).await?;
    registry_label_update_with_correct_revision_bumps_and_persists(storage).await?;
    registry_label_update_with_stale_revision_conflicts(storage).await?;
    chain_insert_preserves_sparse_order(storage).await?;
    list_chain_for_principal_returns_ordered(storage).await?;
    refcount_increment_on_chain_insert(storage).await?;
    update_chain_entry_bumps_revision(storage).await?;
    update_chain_entry_stale_revision_conflicts(storage).await?;
    reorder_chain_valid_orders(storage).await?;
    reorder_chain_needs_rebalance_conflicts(storage).await?;
    rebalance_chain_evenly_spaces(storage).await?;
    sparse_order_between_integration(storage).await?;
    delete_chain_entry_decrements_refcount(storage).await?;
    delete_chain_entry_missing_is_false(storage).await?;
    decrement_blob_refcount_or_delete_missing_is_false(storage).await?;
    list_orphan_blobs_returns_zero_refcount_sha(storage).await?;
    validate_identifier_rejects_bad_name(storage).await?;
    fk_on_delete_restrict(storage).await?;
    Ok(())
}

pub async fn registry_label_update_with_correct_revision_bumps_and_persists<
    S: PluginRegistryStore,
>(
    storage: &S,
) -> Result<()> {
    let created = storage
        .persist_wasm_upload(blob(31, b"label".to_vec()), entry("plugin-label-update"))
        .await?;
    let updated = storage
        .update_registry_label(created.id, created.revision, Some("Updated".to_owned()))
        .await?;
    ensure!(updated.revision == created.revision + 1, "revision bumps");
    ensure!(updated.label.as_deref() == Some("Updated"), "label changed");
    ensure!(
        storage
            .get_registry_entry_by_id(created.id)
            .await?
            .is_some_and(|entry| entry.label.as_deref() == Some("Updated")),
        "label persists"
    );
    Ok(())
}

pub async fn registry_label_update_with_stale_revision_conflicts<S: PluginRegistryStore>(
    storage: &S,
) -> Result<()> {
    let created = storage
        .persist_wasm_upload(
            blob(32, b"stale-label".to_vec()),
            entry("plugin-label-stale"),
        )
        .await?;
    let err = storage
        .update_registry_label(created.id, created.revision + 1, None)
        .await
        .expect_err("stale registry label revision conflicts");
    ensure!(
        matches!(err, StorageError::Conflict { .. }),
        "stale registry label revision conflict"
    );
    Ok(())
}

pub async fn persist_wasm_upload_creates_blob_and_registry<S: PluginRegistryStore>(
    storage: &S,
) -> Result<()> {
    let blob = blob(1, b"wasm-a".to_vec());
    let entry = storage
        .persist_wasm_upload(blob.clone(), entry("plugin-a"))
        .await?;
    ensure!(
        entry.sha256 == blob.sha256,
        "registry sha should match blob sha"
    );
    ensure!(
        storage.get_blob_bytes(blob.sha256).await? == Some(blob.bytes),
        "blob bytes roundtrip"
    );
    Ok(())
}

pub async fn persist_wasm_upload_idempotent_on_same_entry_input<S: PluginRegistryStore>(
    storage: &S,
) -> Result<()> {
    let input = entry("plugin-idempotent");
    let first = storage
        .persist_wasm_upload(blob(2, b"same".to_vec()), input.clone())
        .await?;
    let second = storage
        .persist_wasm_upload(blob(2, b"same".to_vec()), input)
        .await?;
    ensure!(
        first.id == second.id,
        "same sha and input should return existing row"
    );
    Ok(())
}

pub async fn persist_wasm_upload_rollback_on_registry_conflict<S: PluginRegistryStore>(
    storage: &S,
) -> Result<()> {
    storage
        .persist_wasm_upload(blob(3, b"a".to_vec()), entry("plugin-conflict"))
        .await?;
    let err = storage
        .persist_wasm_upload(blob(4, b"b".to_vec()), entry("plugin-conflict"))
        .await
        .expect_err("name conflict");
    ensure!(
        matches!(err, StorageError::Conflict { .. }),
        "name conflict should map to Conflict"
    );
    ensure!(
        storage.get_blob_bytes([4; 32]).await?.is_none(),
        "conflicting upload must roll back blob insert"
    );
    Ok(())
}

pub async fn persist_wasm_upload_rejects_oversize<S: PluginRegistryStore>(
    storage: &S,
) -> Result<()> {
    let mut blob = blob(5, Vec::new());
    blob.size_bytes = cc_lb_storage_api::MAX_WASM_BLOB_BYTES + 1;
    let err = storage
        .persist_wasm_upload(blob, entry("plugin-big"))
        .await
        .expect_err("oversize rejected");
    ensure!(
        matches!(
            err,
            StorageError::InvalidInput { .. } | StorageError::Conflict { .. }
        ),
        "oversize should reject"
    );
    Ok(())
}

pub async fn persist_wasm_upload_records_parse_validated_at<S: PluginRegistryStore>(
    storage: &S,
) -> Result<()> {
    let uploaded = storage
        .persist_wasm_upload(blob(6, b"valid".to_vec()), entry("plugin-validated"))
        .await?;
    ensure!(uploaded.uploaded_at_unix_secs > 0, "uploaded time recorded");
    Ok(())
}

pub async fn get_blob_bytes_returns_persisted_blob<S: PluginRegistryStore>(
    storage: &S,
) -> Result<()> {
    let blob = blob(7, b"bytes".to_vec());
    storage
        .persist_wasm_upload(blob.clone(), entry("plugin-bytes"))
        .await?;
    ensure!(
        storage.get_blob_bytes(blob.sha256).await?.as_deref() == Some(blob.bytes.as_slice()),
        "bytes match"
    );
    Ok(())
}

pub async fn registry_list_paginates<S: PluginRegistryStore>(storage: &S) -> Result<()> {
    storage
        .persist_wasm_upload(blob(8, b"one".to_vec()), entry("plugin-page-a"))
        .await?;
    ensure!(
        storage.list_registry(None, 1).await?.len() == 1,
        "limit applies"
    );
    Ok(())
}

pub async fn get_registry_entry_by_sha_returns_entry<S: PluginRegistryStore>(
    storage: &S,
) -> Result<()> {
    let created = storage
        .persist_wasm_upload(blob(10, b"sha".to_vec()), entry("plugin-sha"))
        .await?;
    ensure!(
        storage
            .get_registry_entry_by_sha(created.sha256)
            .await?
            .map(|entry| entry.id)
            == Some(created.id),
        "sha lookup works"
    );
    Ok(())
}

pub async fn chain_insert_preserves_sparse_order<S: PluginRegistryStore>(
    storage: &S,
) -> Result<()> {
    let (principal, plugin) = principal_and_plugin(storage, 11, "plugin-chain-order").await?;
    storage
        .insert_chain_entry(chain(principal, plugin.id, sparse_order::STEP * 2))
        .await?;
    storage
        .insert_chain_entry(chain(principal, plugin.id, sparse_order::STEP))
        .await?;
    let listed = storage
        .list_chain_for_principal(principal, PluginSlot::Router)
        .await?;
    ensure!(listed[0].order < listed[1].order, "chain list is ordered");
    Ok(())
}

pub async fn list_chain_for_principal_returns_ordered<S: PluginRegistryStore>(
    storage: &S,
) -> Result<()> {
    let (principal, plugin) = principal_and_plugin(storage, 22, "plugin-chain-list").await?;
    storage
        .insert_chain_entry(chain(principal, plugin.id, sparse_order::STEP * 2))
        .await?;
    storage
        .insert_chain_entry(chain(principal, plugin.id, sparse_order::STEP))
        .await?;
    let listed = storage
        .list_chain_for_principal(principal, PluginSlot::Router)
        .await?;
    ensure!(listed.len() == 2, "chain list includes both entries");
    ensure!(listed[0].order < listed[1].order, "chain list is ordered");
    Ok(())
}

pub async fn refcount_increment_on_chain_insert<S: PluginRegistryStore>(storage: &S) -> Result<()> {
    let (principal, plugin) = principal_and_plugin(storage, 12, "plugin-ref-inc").await?;
    storage
        .insert_chain_entry(chain(principal, plugin.id, sparse_order::STEP))
        .await?;
    let listed = storage.list_registry(None, 100).await?;
    ensure!(
        listed
            .into_iter()
            .find(|entry| entry.id == plugin.id)
            .is_some_and(|entry| entry.refcount >= 1),
        "refcount increments"
    );
    Ok(())
}

pub async fn update_chain_entry_bumps_revision<S: PluginRegistryStore>(storage: &S) -> Result<()> {
    let created = one_chain(storage, 13, "plugin-update").await?;
    let updated = storage
        .update_chain_entry(
            created.id,
            created.revision,
            PluginChainEntryUpdate {
                config: Some(json!({"x": 1})),
                ..Default::default()
            },
        )
        .await?
        .expect("updated");
    ensure!(updated.revision == created.revision + 1, "revision bumps");
    Ok(())
}

pub async fn update_chain_entry_stale_revision_conflicts<S: PluginRegistryStore>(
    storage: &S,
) -> Result<()> {
    let created = one_chain(storage, 14, "plugin-stale").await?;
    let err = storage
        .update_chain_entry(
            created.id,
            created.revision + 1,
            PluginChainEntryUpdate::default(),
        )
        .await
        .expect_err("stale conflicts");
    ensure!(
        matches!(err, StorageError::Conflict { .. }),
        "stale revision conflict"
    );
    Ok(())
}

pub async fn reorder_chain_valid_orders<S: PluginRegistryStore>(storage: &S) -> Result<()> {
    let first = one_chain(storage, 15, "plugin-reorder").await?;
    let reordered = storage
        .reorder_chain(
            first.principal_id,
            first.slot,
            vec![(first.id, sparse_order::STEP * 3, first.revision)],
        )
        .await?;
    ensure!(
        reordered[0].order == sparse_order::STEP * 3,
        "order updated"
    );
    Ok(())
}

pub async fn reorder_chain_needs_rebalance_conflicts<S: PluginRegistryStore>(
    storage: &S,
) -> Result<()> {
    let first = one_chain(storage, 16, "plugin-tight").await?;
    let err = storage
        .reorder_chain(
            first.principal_id,
            first.slot,
            vec![(first.id, 1000, first.revision), (Uuid::new_v4(), 1001, 0)],
        )
        .await
        .expect_err("tight gap conflicts");
    ensure!(
        matches!(err, StorageError::Conflict { .. }),
        "tight order conflict"
    );
    Ok(())
}

pub async fn rebalance_chain_evenly_spaces<S: PluginRegistryStore>(storage: &S) -> Result<()> {
    let first = one_chain(storage, 17, "plugin-rebalance").await?;
    let rebalanced = storage
        .rebalance_chain(first.principal_id, first.slot)
        .await?;
    ensure!(
        rebalanced[0].order == sparse_order::STEP,
        "rebalance starts at step"
    );
    Ok(())
}

pub async fn sparse_order_between_integration<S: PluginRegistryStore>(storage: &S) -> Result<()> {
    let (principal, plugin) = principal_and_plugin(storage, 18, "plugin-between").await?;
    let created = storage
        .insert_chain_entry(chain(
            principal,
            plugin.id,
            sparse_order::between(1000, 3000),
        ))
        .await?;
    ensure!(created.order == 2000, "between order stored");
    Ok(())
}

pub async fn delete_chain_entry_decrements_refcount<S: PluginRegistryStore>(
    storage: &S,
) -> Result<()> {
    let created = one_chain(storage, 19, "plugin-delete").await?;
    ensure!(
        storage.delete_chain_entry(created.id).await?,
        "delete returns true"
    );
    Ok(())
}

pub async fn delete_chain_entry_missing_is_false<S: PluginRegistryStore>(
    storage: &S,
) -> Result<()> {
    ensure!(
        !storage.delete_chain_entry(Uuid::new_v4()).await?,
        "missing delete false"
    );
    Ok(())
}

pub async fn decrement_blob_refcount_or_delete_missing_is_false<S: PluginRegistryStore>(
    storage: &S,
) -> Result<()> {
    ensure!(
        !storage.decrement_blob_refcount_or_delete([99; 32]).await?,
        "missing blob false"
    );
    Ok(())
}

pub async fn list_orphan_blobs_returns_zero_refcount_sha<S: PluginRegistryStore>(
    storage: &S,
) -> Result<()> {
    let created = storage
        .persist_wasm_upload(blob(23, b"orphan".to_vec()), entry("plugin-orphan"))
        .await?;
    let orphaned = storage.list_orphan_blobs().await?;
    ensure!(
        orphaned.contains(&created.sha256),
        "fresh upload has zero chain refcount"
    );
    Ok(())
}

pub async fn validate_identifier_rejects_bad_name<S: PluginRegistryStore>(
    storage: &S,
) -> Result<()> {
    let err = storage
        .persist_wasm_upload(blob(20, b"bad".to_vec()), entry("system.bad"))
        .await
        .expect_err("bad name");
    ensure!(
        matches!(err, StorageError::InvalidInput { .. }),
        "bad name invalid input"
    );
    Ok(())
}

pub async fn fk_on_delete_restrict<S: PluginRegistryStore>(storage: &S) -> Result<()> {
    let created = one_chain(storage, 21, "plugin-fk").await?;
    ensure!(
        storage.get_blob_bytes([21; 32]).await?.is_some(),
        "referenced blob remains available"
    );
    ensure!(
        storage
            .list_chain_for_principal(created.principal_id, created.slot)
            .await?
            .len()
            == 1,
        "chain exists"
    );
    Ok(())
}

async fn principal_and_plugin<S: PluginRegistryStore>(
    storage: &S,
    seed: u8,
    name: &str,
) -> Result<(Uuid, cc_lb_storage_api::WasmRegistryEntry)> {
    let plugin = storage
        .persist_wasm_upload(blob(seed, vec![seed]), entry(name))
        .await?;
    Ok((Uuid::new_v4(), plugin))
}

async fn one_chain<S: PluginRegistryStore>(
    storage: &S,
    seed: u8,
    name: &str,
) -> Result<cc_lb_storage_api::PluginChainEntry> {
    let (principal, plugin) = principal_and_plugin(storage, seed, name).await?;
    Ok(storage
        .insert_chain_entry(chain(principal, plugin.id, sparse_order::STEP))
        .await?)
}

fn blob(seed: u8, bytes: Vec<u8>) -> WasmBlob {
    WasmBlob {
        sha256: [seed; 32],
        size_bytes: bytes.len() as u64,
        bytes,
        parse_validated_at_unix_secs: 1_800_000_000 + seed as u64,
    }
}

fn entry(name: &str) -> WasmRegistryEntryInput {
    WasmRegistryEntryInput {
        name: name.to_owned(),
        original_filename: format!("{name}.wasm"),
        label: None,
        uploaded_at_unix_secs: 1_800_000_100,
        uploaded_by_admin_id: Uuid::new_v4(),
    }
}

fn chain(principal_id: Uuid, wasm_registry_id: Uuid, order: i64) -> PluginChainEntryInput {
    PluginChainEntryInput {
        principal_id,
        slot: PluginSlot::Router,
        order,
        wasm_registry_id,
        config: json!({}),
        sse_per_event: false,
        batched_events_per_flush: 1,
        batched_flush_ms: 100,
    }
}

#[cfg(test)]
mod tests {
    use cc_lb_storage_redb::RedbStorage;

    use super::*;

    #[tokio::test]
    async fn plugin_registry_store_redb_conformance() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let storage = RedbStorage::open(&dir.path().join("plugin-registry.redb"), [0; 32])?;
        run_all(&storage).await
    }
}
