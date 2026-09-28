use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_plugin_wire::metadata::HookMetadata;
use cc_lb_storage_api::{
    BUILTIN_SUBSCRIPTION_PREFERENCE_ID, BUILTIN_SUBSCRIPTION_PREFERENCE_NAME,
    BUILTIN_SUBSCRIPTION_PREFERENCE_SHA256, PluginChainConflictReason, PluginChainEntryInput,
    PluginChainEntryUpdate, PluginRegistryStore, PluginSlotKind, PrincipalStore, StorageError,
    UpstreamCreate, UpstreamStore, UpstreamWarmupDialectPlugin, WasmBlob, WasmRegistryEntryInput,
    default_wire_version,
    principal::{Limit, LimitKind, PrincipalCreate, PrincipalKind},
    sparse_order,
};
use serde_json::json;
use uuid::Uuid;

use crate::harness::{ConformanceBackend, with_conformance_fixture};

const BASE_TS: u64 = 1_900_000_000;
const REFCOUNT_CONCURRENCY_PLUGIN_COUNT: usize = 5;
const REFCOUNT_CONCURRENCY_INITIAL_CHAINS: usize = 3;
const REFCOUNT_CONCURRENCY_TASKS: usize = 8;
const REFCOUNT_CONCURRENCY_OPS_PER_TASK: usize = 16;

pub async fn run_all<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: PluginRegistryStore + PrincipalStore + UpstreamStore,
{
    with_conformance_fixture(backend, |storage| async move {
        run_all_on_storage(storage.as_ref()).await
    })
    .await
}

pub async fn run_all_on_storage<S>(storage: &S) -> Result<()>
where
    S: PluginRegistryStore + PrincipalStore + UpstreamStore + 'static,
{
    persist_wasm_upload_creates_blob_and_registry(storage).await?;
    persist_wasm_upload_idempotent_on_same_entry_input(storage).await?;
    persist_wasm_upload_rollback_on_registry_conflict(storage).await?;
    persist_wasm_upload_rejects_oversize(storage).await?;
    persist_wasm_upload_records_parse_validated_at(storage).await?;
    get_blob_bytes_returns_persisted_blob(storage).await?;
    registry_list_paginates(storage).await?;
    get_registry_entry_by_sha_returns_entry(storage).await?;
    get_registry_entry_by_name_returns_entry(storage).await?;
    refcount_counts_chain_and_warmup_references_on_storage(storage).await?;
    replace_wasm_entry_preserves_id_and_references_on_storage(storage).await?;
    replace_wasm_entry_with_stale_revision_conflicts_on_storage(storage).await?;
    list_registry_references_returns_chain_and_warmup_on_storage(storage).await?;
    cascade_delete_registry_entry_removes_chain_warmup_and_blob_on_storage(storage).await?;
    cascade_delete_registry_entry_rejects_changed_fingerprint_on_storage(storage).await?;
    registry_by_id_returns_seeded_builtin_subscription_preference_on_storage(storage).await?;
    created_principal_has_builtin_subscription_preference_chain_entry_on_storage(storage).await?;
    registry_label_update_with_correct_revision_bumps_and_persists(storage).await?;
    registry_label_update_with_stale_revision_conflicts(storage).await?;
    chain_insert_preserves_sparse_order(storage).await?;
    list_chain_for_principal_returns_ordered(storage).await?;
    list_chains_for_principals_returns_ordered_and_filtered(storage).await?;
    router_multi_entry_ordered_on_storage(storage).await?;
    insert_chain_entry_rejects_duplicate_for_shape_slot_on_storage(storage).await?;
    router_reorder_preserves_invariants_on_storage(storage).await?;
    shape_singleton_preserved_on_storage(storage).await?;
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
    list_orphan_blobs_returns_blobs_without_registry_on_storage(storage).await?;
    chain_delete_keeps_blob_for_reinsert_on_storage(storage).await?;
    delete_registry_rejects_while_chain_refed_on_storage(storage).await?;
    delete_registry_removes_blob_atomically_on_storage(storage).await?;
    persist_wasm_upload_heals_missing_blob_on_storage(storage).await?;
    decrement_blob_refcount_or_delete_skips_registry_backed_on_storage(storage).await?;
    insert_chain_entry_rejects_unknown_principal_on_storage(storage).await?;
    insert_chain_entry_rejects_soft_deleted_principal_on_storage(storage).await?;
    reorder_chain_rejects_final_chain_gap_on_storage(storage).await?;
    update_chain_entry_rejects_no_op_on_storage(storage).await?;
    upload_returns_existed_flag_on_storage(storage).await?;
    same_sha_metadata_mismatch_conflicts_on_storage(storage).await?;
    validate_identifier_rejects_bad_name(storage).await?;
    fk_on_delete_restrict(storage).await?;
    Ok(())
}

macro_rules! plugin_registry_scenario {
    ($name:ident, $inner:ident) => {
        pub async fn $name<B>(backend: Arc<B>) -> Result<()>
        where
            B: ConformanceBackend,
            B::Storage: PluginRegistryStore + PrincipalStore,
        {
            with_conformance_fixture(
                backend,
                |storage| async move { $inner(storage.as_ref()).await },
            )
            .await
        }
    };
}

macro_rules! plugin_registry_upstream_scenario {
    ($name:ident, $inner:ident) => {
        pub async fn $name<B>(backend: Arc<B>) -> Result<()>
        where
            B: ConformanceBackend,
            B::Storage: PluginRegistryStore + PrincipalStore + UpstreamStore,
        {
            with_conformance_fixture(
                backend,
                |storage| async move { $inner(storage.as_ref()).await },
            )
            .await
        }
    };
}

plugin_registry_upstream_scenario!(
    refcount_counts_chain_and_warmup_references,
    refcount_counts_chain_and_warmup_references_on_storage
);
plugin_registry_upstream_scenario!(
    replace_wasm_entry_preserves_id_and_references,
    replace_wasm_entry_preserves_id_and_references_on_storage
);
plugin_registry_upstream_scenario!(
    replace_wasm_entry_with_stale_revision_conflicts,
    replace_wasm_entry_with_stale_revision_conflicts_on_storage
);
plugin_registry_upstream_scenario!(
    list_registry_references_returns_chain_and_warmup,
    list_registry_references_returns_chain_and_warmup_on_storage
);
plugin_registry_upstream_scenario!(
    cascade_delete_registry_entry_removes_chain_warmup_and_blob,
    cascade_delete_registry_entry_removes_chain_warmup_and_blob_on_storage
);
plugin_registry_upstream_scenario!(
    cascade_delete_registry_entry_rejects_changed_fingerprint,
    cascade_delete_registry_entry_rejects_changed_fingerprint_on_storage
);

pub async fn registry_label_update_with_correct_revision_bumps_and_persists<
    S: PluginRegistryStore,
>(
    storage: &S,
) -> Result<()> {
    let (created, _) = storage
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
    let (created, _) = storage
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
        matches!(err, StorageError::StalePluginRegistryRevision { .. }),
        "stale registry label revision conflict"
    );
    Ok(())
}

pub async fn persist_wasm_upload_creates_blob_and_registry<S: PluginRegistryStore>(
    storage: &S,
) -> Result<()> {
    let blob = blob(1, b"wasm-a".to_vec());
    let (entry, _) = storage
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
    let (first, _) = storage
        .persist_wasm_upload(blob(2, b"same".to_vec()), input.clone())
        .await?;
    let (second, _) = storage
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
    let (uploaded, _) = storage
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
    let (created, _) = storage
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

pub async fn get_registry_entry_by_name_returns_entry<S: PluginRegistryStore>(
    storage: &S,
) -> Result<()> {
    let (created, _) = storage
        .persist_wasm_upload(blob(52, b"name".to_vec()), entry("plugin-name"))
        .await?;
    let found = storage.get_registry_entry_by_name("plugin-name").await?;
    ensure!(
        found.map(|entry| entry.id) == Some(created.id),
        "name lookup returns the registry entry"
    );
    Ok(())
}

async fn refcount_counts_chain_and_warmup_references_on_storage<
    S: PluginRegistryStore + PrincipalStore + UpstreamStore,
>(
    storage: &S,
) -> Result<()> {
    let (principal, plugin) = principal_and_plugin(storage, 44, "plugin-live-refcount").await?;
    storage
        .insert_chain_entry(chain(principal, plugin.id, sparse_order::STEP))
        .await?;
    UpstreamStore::create(
        storage,
        warmup_upstream("plugin-live-refcount-upstream", plugin.id),
    )
    .await?;

    let entry = storage
        .get_registry_entry_by_id(plugin.id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("live refcount registry entry is present"))?;
    ensure!(
        entry.refcount == 2,
        "chain and warmup references are counted"
    );
    Ok(())
}

async fn replace_wasm_entry_preserves_id_and_references_on_storage<
    S: PluginRegistryStore + PrincipalStore,
>(
    storage: &S,
) -> Result<()> {
    let mut initial = entry("plugin-replace");
    initial.label = Some("Operator label".to_owned());
    let (created, _) = storage
        .persist_wasm_upload(blob(45, b"replace-original".to_vec()), initial)
        .await?;
    let principal = PrincipalStore::create(storage, principal_create(45), BASE_TS + 45).await?;
    let chain_entry = storage
        .insert_chain_entry(chain(principal.id, created.id, sparse_order::STEP))
        .await?;

    let mut replacement = entry("plugin-replace");
    replacement.version = Some("2.0.0".to_owned());
    replacement.original_filename = "plugin-replace-v2.wasm".to_owned();
    replacement.label = Some("Ignored replacement label".to_owned());
    replacement.description = "replacement description".to_owned();
    replacement.usage = "replacement usage".to_owned();
    replacement.uploaded_at_unix_secs += 1;
    let updated = storage
        .replace_wasm_entry(
            blob(46, b"replace-updated".to_vec()),
            replacement,
            created.revision,
        )
        .await?;

    ensure!(
        updated.id == created.id,
        "replacement preserves registry id"
    );
    ensure!(updated.sha256 == [46; 32], "replacement updates sha");
    ensure!(
        updated.version.as_deref() == Some("2.0.0"),
        "replacement updates version"
    );
    ensure!(
        updated.label.as_deref() == Some("Operator label"),
        "replacement preserves operator label"
    );
    ensure!(updated.refcount == 1, "replacement preserves references");
    ensure!(
        storage
            .list_chain_for_principal(principal.id, PluginSlotKind::Router)
            .await?
            .into_iter()
            .any(|entry| entry.id == chain_entry.id && entry.wasm_registry_id == created.id),
        "chain retains stable registry id"
    );
    ensure!(
        storage.get_blob(created.sha256).await?.is_none(),
        "unreferenced replacement blob is removed"
    );
    Ok(())
}

async fn replace_wasm_entry_with_stale_revision_conflicts_on_storage<S: PluginRegistryStore>(
    storage: &S,
) -> Result<()> {
    let (created, _) = storage
        .persist_wasm_upload(
            blob(47, b"replace-stale".to_vec()),
            entry("plugin-replace-stale"),
        )
        .await?;
    let err = storage
        .replace_wasm_entry(
            blob(48, b"replace-stale-update".to_vec()),
            entry("plugin-replace-stale"),
            created.revision + 1,
        )
        .await
        .expect_err("stale replacement revision conflicts");
    ensure!(
        matches!(err, StorageError::StalePluginRegistryRevision { .. }),
        "stale replacement returns typed registry conflict"
    );
    Ok(())
}

async fn list_registry_references_returns_chain_and_warmup_on_storage<
    S: PluginRegistryStore + PrincipalStore + UpstreamStore,
>(
    storage: &S,
) -> Result<()> {
    let (principal, plugin) = principal_and_plugin(storage, 49, "plugin-reference-list").await?;
    let chain_entry = storage
        .insert_chain_entry(chain(principal, plugin.id, sparse_order::STEP))
        .await?;
    let upstream = UpstreamStore::create(
        storage,
        warmup_upstream("plugin-reference-list-upstream", plugin.id),
    )
    .await?;

    let references = storage.list_registry_references(plugin.id).await?;
    ensure!(
        references.references.len() == 2,
        "all references are listed"
    );
    ensure!(
        references.references.iter().any(|reference| matches!(
            reference,
            cc_lb_storage_api::WasmRegistryReference::PluginChain {
                chain_entry_id,
                principal_id,
                principal_name,
                revision,
                ..
            } if *chain_entry_id == chain_entry.id
                && *principal_id == principal
                && principal_name == "plugin-principal-0049"
                && *revision == chain_entry.revision
        )),
        "chain reference includes ids, name, and revision"
    );
    ensure!(
        references.references.iter().any(|reference| matches!(
            reference,
            cc_lb_storage_api::WasmRegistryReference::UpstreamWarmupDialect {
                upstream_id,
                upstream_name,
                revision,
            } if *upstream_id == upstream.id
                && upstream_name == "plugin-reference-list-upstream"
                && *revision == upstream.revision
        )),
        "warmup reference includes id, name, and revision"
    );
    Ok(())
}

async fn cascade_delete_registry_entry_removes_chain_warmup_and_blob_on_storage<
    S: PluginRegistryStore + PrincipalStore + UpstreamStore,
>(
    storage: &S,
) -> Result<()> {
    let (principal, plugin) = principal_and_plugin(storage, 50, "plugin-cascade-delete").await?;
    storage
        .insert_chain_entry(chain(principal, plugin.id, sparse_order::STEP))
        .await?;
    let upstream = UpstreamStore::create(
        storage,
        warmup_upstream("plugin-cascade-delete-upstream", plugin.id),
    )
    .await?;
    let references = storage.list_registry_references(plugin.id).await?;
    let deleted = storage
        .cascade_delete_registry_entry(plugin.id, plugin.revision, references.fingerprint)
        .await?
        .ok_or_else(|| anyhow::anyhow!("cascade delete returns registry entry"))?;

    ensure!(
        deleted.entry.id == plugin.id,
        "cascade returns deleted entry"
    );
    ensure!(deleted.references.len() == 2, "cascade returns references");
    let router_entries = storage
        .list_chain_for_principal(principal, PluginSlotKind::Router)
        .await?;
    ensure!(
        router_entries
            .iter()
            .all(|entry| entry.wasm_registry_id != plugin.id),
        "cascade removes the deleted plugin's chain reference"
    );
    let updated_upstream = UpstreamStore::get_by_id(storage, upstream.id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("cascade upstream remains present"))?;
    ensure!(
        updated_upstream.warmup_dialect_plugin.is_none(),
        "cascade clears warmup reference"
    );
    ensure!(
        updated_upstream.revision == upstream.revision + 1,
        "cascade bumps upstream revision"
    );
    ensure!(
        storage.get_registry_entry_by_id(plugin.id).await?.is_none(),
        "cascade removes registry row"
    );
    ensure!(
        storage.get_blob(plugin.sha256).await?.is_none(),
        "cascade removes orphaned blob"
    );
    Ok(())
}

async fn cascade_delete_registry_entry_rejects_changed_fingerprint_on_storage<
    S: PluginRegistryStore + PrincipalStore,
>(
    storage: &S,
) -> Result<()> {
    let (principal, plugin) = principal_and_plugin(storage, 51, "plugin-cascade-stale").await?;
    let references = storage.list_registry_references(plugin.id).await?;
    let chain_entry = storage
        .insert_chain_entry(chain(principal, plugin.id, sparse_order::STEP))
        .await?;
    let err = storage
        .cascade_delete_registry_entry(plugin.id, plugin.revision, references.fingerprint)
        .await
        .expect_err("changed references block cascade delete");
    ensure!(
        matches!(err, StorageError::StalePluginRegistryReferences),
        "changed reference fingerprint returns typed conflict"
    );
    ensure!(
        storage.get_registry_entry_by_id(plugin.id).await?.is_some(),
        "stale cascade leaves registry row"
    );
    ensure!(
        storage
            .list_chain_for_principal(principal, PluginSlotKind::Router)
            .await?
            .into_iter()
            .any(|entry| entry.id == chain_entry.id),
        "stale cascade leaves references"
    );
    Ok(())
}

plugin_registry_scenario!(
    registry_by_id_returns_seeded_builtin_subscription_preference,
    registry_by_id_returns_seeded_builtin_subscription_preference_on_storage
);

pub async fn registry_by_id_returns_seeded_builtin_subscription_preference_on_storage<
    S: PluginRegistryStore,
>(
    storage: &S,
) -> Result<()> {
    let entry = storage
        .get_registry_entry_by_id(BUILTIN_SUBSCRIPTION_PREFERENCE_ID)
        .await?
        .expect("builtin subscription-preference registry row is seeded");
    ensure!(
        entry.id == BUILTIN_SUBSCRIPTION_PREFERENCE_ID,
        "builtin id matches"
    );
    ensure!(
        entry.sha256 == BUILTIN_SUBSCRIPTION_PREFERENCE_SHA256,
        "builtin sha matches"
    );
    ensure!(
        entry.name == BUILTIN_SUBSCRIPTION_PREFERENCE_NAME,
        "builtin name matches"
    );
    ensure!(
        entry.original_filename == "builtin://subscription-preference",
        "builtin filename matches"
    );
    ensure!(
        entry.uploaded_by_admin_id == Uuid::nil(),
        "builtin uploader is nil"
    );
    ensure!(
        entry
            .hook_metadata
            .get("filter")
            .is_some_and(|hook| hook.wire_version == default_wire_version()),
        "builtin filter hook wire version matches"
    );
    ensure!(entry.is_builtin, "builtin flag is persisted");
    ensure!(entry.metadata.is_some(), "builtin metadata is persisted");
    ensure!(
        storage
            .get_blob_bytes(BUILTIN_SUBSCRIPTION_PREFERENCE_SHA256)
            .await?
            .is_some_and(|bytes| bytes.is_empty()),
        "builtin zero-hash blob row is seeded"
    );
    Ok(())
}

plugin_registry_scenario!(
    created_principal_has_builtin_subscription_preference_chain_entry,
    created_principal_has_builtin_subscription_preference_chain_entry_on_storage
);

pub async fn created_principal_has_builtin_subscription_preference_chain_entry_on_storage<
    S: PluginRegistryStore + PrincipalStore,
>(
    storage: &S,
) -> Result<()> {
    let refcount_before = storage
        .get_registry_entry_by_id(BUILTIN_SUBSCRIPTION_PREFERENCE_ID)
        .await?
        .expect("builtin is registered before principal creation")
        .refcount;
    let principal = PrincipalStore::create(storage, principal_create(42), BASE_TS + 42).await?;
    let listed = storage
        .list_chain_for_principal(principal.id, PluginSlotKind::Router)
        .await?;
    ensure!(
        listed.len() == 1,
        "new principal has one router chain entry"
    );
    ensure!(
        listed[0].wasm_registry_id == BUILTIN_SUBSCRIPTION_PREFERENCE_ID,
        "new principal chain references builtin subscription-preference"
    );
    let registry = storage
        .get_registry_entry_by_id(BUILTIN_SUBSCRIPTION_PREFERENCE_ID)
        .await?
        .expect("builtin remains registered after principal creation");
    ensure!(
        registry.refcount == refcount_before + 1,
        "builtin refcount increments for the seeded entry"
    );
    Ok(())
}

pub async fn chain_insert_preserves_sparse_order<S: PluginRegistryStore + PrincipalStore>(
    storage: &S,
) -> Result<()> {
    let (principal, plugin) = principal_and_plugin(storage, 11, "plugin-chain-order").await?;
    storage
        .insert_chain_entry(chain_with_slot(
            principal,
            PluginSlotKind::Router,
            plugin.id,
            sparse_order::STEP * 2,
        ))
        .await?;
    storage
        .insert_chain_entry(chain_with_slot(
            principal,
            PluginSlotKind::Router,
            plugin.id,
            sparse_order::STEP,
        ))
        .await?;
    let listed = storage
        .list_chain_for_principal(principal, PluginSlotKind::Router)
        .await?;
    let custom_entries: Vec<_> = listed
        .iter()
        .filter(|entry| entry.wasm_registry_id == plugin.id)
        .collect();
    ensure!(
        custom_entries[0].order < custom_entries[1].order,
        "chain list is ordered"
    );
    Ok(())
}

pub async fn list_chain_for_principal_returns_ordered<S: PluginRegistryStore + PrincipalStore>(
    storage: &S,
) -> Result<()> {
    let (principal, plugin) = principal_and_plugin(storage, 22, "plugin-chain-list").await?;
    storage
        .insert_chain_entry(chain_with_slot(
            principal,
            PluginSlotKind::Router,
            plugin.id,
            sparse_order::STEP * 2,
        ))
        .await?;
    storage
        .insert_chain_entry(chain_with_slot(
            principal,
            PluginSlotKind::Router,
            plugin.id,
            sparse_order::STEP,
        ))
        .await?;
    let listed = storage
        .list_chain_for_principal(principal, PluginSlotKind::Router)
        .await?;
    let custom_entries: Vec<_> = listed
        .iter()
        .filter(|entry| entry.wasm_registry_id == plugin.id)
        .collect();
    ensure!(
        custom_entries.len() == 2,
        "chain list includes both entries"
    );
    ensure!(
        custom_entries[0].order < custom_entries[1].order,
        "chain list is ordered"
    );
    Ok(())
}

pub async fn list_chains_for_principals_returns_ordered_and_filtered<
    S: PluginRegistryStore + PrincipalStore,
>(
    storage: &S,
) -> Result<()> {
    let (first_principal, first_plugin) =
        principal_and_plugin(storage, 160, "plugin-chain-batch-first").await?;
    let (second_principal, second_plugin) =
        principal_and_plugin(storage, 161, "plugin-chain-batch-second").await?;
    let (ignored_principal, ignored_plugin) =
        principal_and_plugin(storage, 162, "plugin-chain-batch-ignored").await?;

    let first_late = storage
        .insert_chain_entry(chain_with_slot(
            first_principal,
            PluginSlotKind::Router,
            first_plugin.id,
            sparse_order::STEP * 2,
        ))
        .await?;
    let first_early = storage
        .insert_chain_entry(chain_with_slot(
            first_principal,
            PluginSlotKind::Router,
            first_plugin.id,
            sparse_order::STEP,
        ))
        .await?;
    let second_router = storage
        .insert_chain_entry(chain_with_slot(
            second_principal,
            PluginSlotKind::Router,
            second_plugin.id,
            sparse_order::STEP,
        ))
        .await?;
    storage
        .insert_chain_entry(chain_with_slot(
            ignored_principal,
            PluginSlotKind::Router,
            ignored_plugin.id,
            sparse_order::STEP,
        ))
        .await?;

    let listed = storage
        .list_chains_for_principals(
            &[first_principal, second_principal],
            &[PluginSlotKind::Router],
        )
        .await?;

    let actual_groups: std::collections::BTreeMap<_, Vec<_>> =
        listed
            .into_iter()
            .fold(std::collections::BTreeMap::new(), |mut groups, entry| {
                groups
                    .entry((entry.principal_id, entry.slot))
                    .or_default()
                    .push(entry.id);
                groups
            });
    let first_custom_entries: Vec<_> = actual_groups
        .get(&(first_principal, PluginSlotKind::Router))
        .into_iter()
        .flatten()
        .copied()
        .filter(|id| *id == first_early.id || *id == first_late.id)
        .collect();
    ensure!(
        first_custom_entries == vec![first_early.id, first_late.id],
        "batched chain list orders router entries"
    );
    ensure!(
        actual_groups
            .get(&(second_principal, PluginSlotKind::Router))
            .is_some_and(|entries| entries.contains(&second_router.id)),
        "batched chain list includes the requested principal's custom router entry"
    );
    ensure!(
        actual_groups
            .keys()
            .all(|(principal_id, _)| *principal_id != ignored_principal),
        "batched chain list excludes unrequested principals"
    );

    let empty = storage
        .list_chains_for_principals(&[], &[PluginSlotKind::Router])
        .await?;
    ensure!(empty.is_empty(), "empty principal filter returns no chains");

    let empty = storage
        .list_chains_for_principals(&[first_principal], &[])
        .await?;
    ensure!(empty.is_empty(), "empty slot filter returns no chains");
    Ok(())
}

plugin_registry_scenario!(
    router_multi_entry_ordered,
    router_multi_entry_ordered_on_storage
);

async fn router_multi_entry_ordered_on_storage<S: PluginRegistryStore + PrincipalStore>(
    storage: &S,
) -> Result<()> {
    let (principal, plugin) = principal_and_plugin(storage, 37, "plugin-router-multi").await?;
    let first = storage
        .insert_chain_entry(chain_with_slot(
            principal,
            PluginSlotKind::Router,
            plugin.id,
            sparse_order::STEP,
        ))
        .await?;
    let second = storage
        .insert_chain_entry(chain_with_slot(
            principal,
            PluginSlotKind::Router,
            plugin.id,
            sparse_order::STEP * 2,
        ))
        .await?;
    let listed = storage
        .list_chain_for_principal(principal, PluginSlotKind::Router)
        .await?;
    let custom_entries: Vec<_> = listed
        .iter()
        .filter(|entry| entry.wasm_registry_id == plugin.id)
        .collect();
    ensure!(
        custom_entries.len() == 2,
        "router slot allows multiple custom entries"
    );
    ensure!(
        custom_entries[0].order < custom_entries[1].order,
        "router entries are properly ordered"
    );
    ensure!(
        custom_entries[0].id == first.id && custom_entries[1].id == second.id,
        "custom entries appear in insertion order"
    );
    Ok(())
}

plugin_registry_scenario!(
    router_reorder_preserves_invariants,
    router_reorder_preserves_invariants_on_storage
);

async fn router_reorder_preserves_invariants_on_storage<S: PluginRegistryStore + PrincipalStore>(
    storage: &S,
) -> Result<()> {
    let (principal, plugin) = principal_and_plugin(storage, 40, "plugin-router-reorder").await?;
    let first = storage
        .insert_chain_entry(chain_with_slot(
            principal,
            PluginSlotKind::Router,
            plugin.id,
            100,
        ))
        .await?;
    let second = storage
        .insert_chain_entry(chain_with_slot(
            principal,
            PluginSlotKind::Router,
            plugin.id,
            200,
        ))
        .await?;
    let entries = storage
        .list_chain_for_principal(principal, PluginSlotKind::Router)
        .await?;
    let builtin = entries
        .iter()
        .find(|entry| entry.wasm_registry_id == BUILTIN_SUBSCRIPTION_PREFERENCE_ID)
        .expect("principal has a built-in router entry");
    let reordered = storage
        .reorder_chain(
            principal,
            PluginSlotKind::Router,
            vec![
                (builtin.id, builtin.order, builtin.revision),
                (first.id, 150, first.revision),
                (second.id, 250, second.revision),
            ],
        )
        .await?;
    let custom_entries: Vec<_> = reordered
        .iter()
        .filter(|entry| entry.wasm_registry_id == plugin.id)
        .collect();
    ensure!(
        custom_entries[0].order == 150 && custom_entries[1].order == 250,
        "custom router entries can be reordered"
    );
    ensure!(
        custom_entries[0].order < custom_entries[1].order,
        "reordering preserves custom-entry order"
    );
    Ok(())
}

plugin_registry_scenario!(
    shape_singleton_preserved,
    shape_singleton_preserved_on_storage
);

async fn shape_singleton_preserved_on_storage<S: PluginRegistryStore + PrincipalStore>(
    storage: &S,
) -> Result<()> {
    let (principal, plugin) =
        principal_and_plugin(storage, 41, "plugin-shape-still-singleton").await?;
    let first = storage
        .insert_chain_entry(chain_with_slot(
            principal,
            PluginSlotKind::Shape,
            plugin.id,
            sparse_order::STEP,
        ))
        .await?;
    let err = storage
        .insert_chain_entry(chain_with_slot(
            principal,
            PluginSlotKind::Shape,
            plugin.id,
            sparse_order::STEP * 2,
        ))
        .await
        .expect_err("shape slot must remain singleton");
    ensure!(
        matches!(
            err,
            StorageError::PluginChainConflict {
                reason: PluginChainConflictReason::SlotIsSingleton { existing_entry_id }
            } if existing_entry_id == first.id
        ),
        "shape slot singleton constraint preserved"
    );
    Ok(())
}

plugin_registry_scenario!(
    insert_chain_entry_rejects_duplicate_for_shape_slot,
    insert_chain_entry_rejects_duplicate_for_shape_slot_on_storage
);

async fn insert_chain_entry_rejects_duplicate_for_shape_slot_on_storage<
    S: PluginRegistryStore + PrincipalStore,
>(
    storage: &S,
) -> Result<()> {
    let (principal, plugin) = principal_and_plugin(storage, 38, "plugin-shape-singleton").await?;
    let first = storage
        .insert_chain_entry(chain_with_slot(
            principal,
            PluginSlotKind::Shape,
            plugin.id,
            sparse_order::STEP,
        ))
        .await?;
    let err = storage
        .insert_chain_entry(chain_with_slot(
            principal,
            PluginSlotKind::Shape,
            plugin.id,
            sparse_order::STEP * 2,
        ))
        .await
        .expect_err("duplicate shape slot insert conflicts");
    ensure!(
        matches!(
            err,
            StorageError::PluginChainConflict {
                reason: PluginChainConflictReason::SlotIsSingleton { existing_entry_id }
            } if existing_entry_id == first.id
        ),
        "duplicate shape slot returns typed singleton conflict"
    );
    Ok(())
}

pub async fn refcount_increment_on_chain_insert<S: PluginRegistryStore + PrincipalStore>(
    storage: &S,
) -> Result<()> {
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

pub async fn refcount_under_concurrency<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: PluginRegistryStore + PrincipalStore,
{
    with_conformance_fixture(backend, |storage| async move {
        refcount_under_concurrency_on_storage(storage).await
    })
    .await
}

async fn refcount_under_concurrency_on_storage<S>(storage: Arc<S>) -> Result<()>
where
    S: PluginRegistryStore + PrincipalStore + 'static,
{
    let mut registry_ids = Vec::with_capacity(REFCOUNT_CONCURRENCY_PLUGIN_COUNT);
    let mut principal_ids =
        Vec::with_capacity(REFCOUNT_CONCURRENCY_PLUGIN_COUNT + REFCOUNT_CONCURRENCY_TASKS);

    for plugin_index in 0..REFCOUNT_CONCURRENCY_PLUGIN_COUNT {
        let seed = 80 + plugin_index as u8;
        let principal = PrincipalStore::create(
            storage.as_ref(),
            principal_create(seed),
            BASE_TS + seed as u64,
        )
        .await?;
        let (plugin, _) = storage
            .persist_wasm_upload(
                blob(seed, vec![seed]),
                entry(&format!("plugin-ref-concurrent-{plugin_index}")),
            )
            .await?;

        principal_ids.push(principal.id);
        registry_ids.push(plugin.id);

        for chain_index in 0..REFCOUNT_CONCURRENCY_INITIAL_CHAINS {
            storage
                .insert_chain_entry(chain_with_slot(
                    principal.id,
                    PluginSlotKind::Router,
                    plugin.id,
                    sparse_order::STEP * (chain_index as i64 + 1),
                ))
                .await?;
        }
    }

    let task_principal_offset = principal_ids.len();
    for task_index in 0..REFCOUNT_CONCURRENCY_TASKS {
        let seed = 90 + task_index as u8;
        let principal = PrincipalStore::create(
            storage.as_ref(),
            principal_create(seed),
            BASE_TS + seed as u64,
        )
        .await?;
        principal_ids.push(principal.id);
    }

    let mut handles = Vec::with_capacity(REFCOUNT_CONCURRENCY_TASKS);
    for task_index in 0..REFCOUNT_CONCURRENCY_TASKS {
        let task_storage = Arc::clone(&storage);
        let task_registry_ids = registry_ids.clone();
        let task_principal_id = principal_ids[task_principal_offset + task_index];
        handles.push(tokio::spawn(async move {
            for op_index in 0..REFCOUNT_CONCURRENCY_OPS_PER_TASK {
                let plugin_index =
                    pseudo_random_index(task_index, op_index, REFCOUNT_CONCURRENCY_PLUGIN_COUNT);
                let order = sparse_order::STEP
                    * (100 + (task_index * REFCOUNT_CONCURRENCY_OPS_PER_TASK + op_index) as i64);
                let inserted = task_storage
                    .insert_chain_entry(chain_with_slot(
                        task_principal_id,
                        PluginSlotKind::Router,
                        task_registry_ids[plugin_index],
                        order,
                    ))
                    .await?;
                ensure!(
                    task_storage
                        .delete_chain_entry(inserted.id, inserted.revision)
                        .await?
                        .is_some(),
                    "concurrent chain delete returns the just-inserted entry"
                );
            }
            Ok::<(), anyhow::Error>(())
        }));
    }

    for handle in handles {
        handle.await??;
    }

    let listed = storage.list_registry(None, 100).await?;
    for registry_id in registry_ids {
        let reported = listed
            .iter()
            .find(|entry| entry.id == registry_id)
            .map(|entry| entry.refcount);
        let expected =
            control_refcount_from_chains(storage.as_ref(), &principal_ids, registry_id).await?;
        ensure!(
            reported == Some(expected),
            "registry refcount should match chain reference count for {registry_id}: reported={reported:?}, expected={expected}"
        );
    }

    Ok(())
}

fn pseudo_random_index(task_index: usize, op_index: usize, len: usize) -> usize {
    (task_index.wrapping_mul(17) + op_index.wrapping_mul(31) + 7) % len
}

async fn control_refcount_from_chains<S: PluginRegistryStore>(
    storage: &S,
    principal_ids: &[Uuid],
    registry_id: Uuid,
) -> Result<i64> {
    let mut refcount = 0;
    for principal_id in principal_ids {
        for slot in [PluginSlotKind::Router, PluginSlotKind::Shape] {
            refcount += storage
                .list_chain_for_principal(*principal_id, slot)
                .await?
                .into_iter()
                .filter(|entry| entry.wasm_registry_id == registry_id)
                .count() as i64;
        }
    }
    Ok(refcount)
}

pub async fn update_chain_entry_bumps_revision<S: PluginRegistryStore + PrincipalStore>(
    storage: &S,
) -> Result<()> {
    let created = one_chain(storage, 13, "plugin-update").await?;
    let updated = storage
        .update_chain_entry(
            created.id,
            created.revision,
            PluginChainEntryUpdate {
                config: Some(json!({"x": 1})),
            },
        )
        .await?
        .expect("updated");
    ensure!(updated.revision == created.revision + 1, "revision bumps");
    Ok(())
}

pub async fn update_chain_entry_stale_revision_conflicts<
    S: PluginRegistryStore + PrincipalStore,
>(
    storage: &S,
) -> Result<()> {
    let created = one_chain(storage, 14, "plugin-stale").await?;
    let err = storage
        .update_chain_entry(
            created.id,
            created.revision + 1,
            PluginChainEntryUpdate {
                config: Some(json!({"x": 1})),
            },
        )
        .await
        .expect_err("stale conflicts");
    ensure!(
        matches!(err, StorageError::StalePluginChainRevision { .. }),
        "stale revision conflict"
    );
    Ok(())
}

pub async fn reorder_chain_valid_orders<S: PluginRegistryStore + PrincipalStore>(
    storage: &S,
) -> Result<()> {
    let first = one_chain(storage, 15, "plugin-reorder").await?;
    let expected_order = sparse_order::STEP * 3;
    let entries = storage
        .list_chain_for_principal(first.principal_id, first.slot)
        .await?;
    let reordered = storage
        .reorder_chain(
            first.principal_id,
            first.slot,
            entries
                .into_iter()
                .map(|entry| {
                    (
                        entry.id,
                        if entry.id == first.id {
                            expected_order
                        } else {
                            entry.order
                        },
                        entry.revision,
                    )
                })
                .collect(),
        )
        .await?;
    ensure!(
        reordered
            .iter()
            .any(|entry| entry.id == first.id && entry.order == expected_order),
        "order updated"
    );
    Ok(())
}

pub async fn reorder_chain_needs_rebalance_conflicts<S: PluginRegistryStore + PrincipalStore>(
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

pub async fn rebalance_chain_evenly_spaces<S: PluginRegistryStore + PrincipalStore>(
    storage: &S,
) -> Result<()> {
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

pub async fn sparse_order_between_integration<S: PluginRegistryStore + PrincipalStore>(
    storage: &S,
) -> Result<()> {
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

pub async fn delete_chain_entry_decrements_refcount<S: PluginRegistryStore + PrincipalStore>(
    storage: &S,
) -> Result<()> {
    let created = one_chain(storage, 19, "plugin-delete").await?;
    ensure!(
        storage
            .delete_chain_entry(created.id, created.revision)
            .await?
            .is_some(),
        "delete returns entry"
    );
    Ok(())
}

pub async fn delete_chain_entry_missing_is_false<S: PluginRegistryStore>(
    storage: &S,
) -> Result<()> {
    ensure!(
        storage
            .delete_chain_entry(Uuid::new_v4(), 0)
            .await?
            .is_none(),
        "missing delete none"
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

// RED until W3a
plugin_registry_scenario!(
    list_orphan_blobs_returns_blobs_without_registry,
    list_orphan_blobs_returns_blobs_without_registry_on_storage
);

async fn list_orphan_blobs_returns_blobs_without_registry_on_storage<
    S: PluginRegistryStore + PrincipalStore + 'static,
>(
    storage: &S,
) -> Result<()> {
    let (created, _) = storage
        .persist_wasm_upload(blob(23, b"orphan".to_vec()), entry("plugin-orphan"))
        .await?;
    ensure!(
        storage.list_orphan_blobs().await?.is_empty(),
        "registry-backed blobs are not orphans"
    );

    let deleted = storage
        .delete_registry_entry(created.id, created.revision)
        .await?;
    ensure!(deleted.is_some(), "registry entry deletes");
    ensure!(
        storage.get_blob(created.sha256).await?.is_none(),
        "deleting an unreferenced registry entry removes its blob row"
    );

    let orphan_blob = blob(24, b"manual-orphan".to_vec());
    if insert_orphan_blob_if_exposed(storage, &orphan_blob).await? {
        let orphaned = storage.list_orphan_blobs().await?;
        ensure!(
            orphaned.contains(&orphan_blob.sha256),
            "unregistered blob rows are reported as orphans"
        );
    }
    Ok(())
}

// RED until W3a
plugin_registry_scenario!(
    chain_delete_keeps_blob_for_reinsert,
    chain_delete_keeps_blob_for_reinsert_on_storage
);

async fn chain_delete_keeps_blob_for_reinsert_on_storage<
    S: PluginRegistryStore + PrincipalStore,
>(
    storage: &S,
) -> Result<()> {
    let (principal, plugin) = principal_and_plugin(storage, 25, "plugin-chain-reinsert").await?;
    let created = storage
        .insert_chain_entry(chain(principal, plugin.id, sparse_order::STEP))
        .await?;
    ensure!(
        storage.get_blob(plugin.sha256).await?.is_some(),
        "chain insert leaves blob readable"
    );

    let deleted = storage
        .delete_chain_entry(created.id, created.revision)
        .await?;
    ensure!(deleted.is_some(), "chain delete returns the deleted entry");
    ensure!(
        storage.get_blob(plugin.sha256).await?.is_some(),
        "chain delete keeps registry-owned blob readable"
    );

    storage
        .insert_chain_entry(chain(principal, plugin.id, sparse_order::STEP * 2))
        .await?;
    Ok(())
}

// RED until W3a
plugin_registry_scenario!(
    delete_registry_rejects_while_chain_refed,
    delete_registry_rejects_while_chain_refed_on_storage
);

async fn delete_registry_rejects_while_chain_refed_on_storage<
    S: PluginRegistryStore + PrincipalStore,
>(
    storage: &S,
) -> Result<()> {
    // NOTE: Phase-3 partially green; W3a hardening still needed for chain-scan/atomicity.
    let (principal, plugin) = principal_and_plugin(storage, 26, "plugin-registry-refed").await?;
    storage
        .insert_chain_entry(chain(principal, plugin.id, sparse_order::STEP))
        .await?;

    let err = storage
        .delete_registry_entry(plugin.id, plugin.revision)
        .await
        .expect_err("registry delete must reject while a chain references it");
    let message = err.to_string();
    ensure!(
        message.contains("referenced")
            || message.contains("foreign key")
            || message.contains(&plugin.id.to_string()),
        "referenced registry delete reports the registry id or FK reference"
    );
    ensure!(
        storage.get_registry_entry_by_id(plugin.id).await?.is_some(),
        "referenced registry row remains after rejected delete"
    );
    ensure!(
        storage.get_blob(plugin.sha256).await?.is_some(),
        "referenced blob row remains after rejected registry delete"
    );
    Ok(())
}

// RED until W3a
plugin_registry_scenario!(
    delete_registry_removes_blob_atomically,
    delete_registry_removes_blob_atomically_on_storage
);

async fn delete_registry_removes_blob_atomically_on_storage<S: PluginRegistryStore>(
    storage: &S,
) -> Result<()> {
    // NOTE: Phase-3 partially green; W3a hardening still needed for chain-scan/atomicity.
    let (created, _) = storage
        .persist_wasm_upload(
            blob(27, b"atomic-delete".to_vec()),
            entry("plugin-atomic-delete"),
        )
        .await?;
    let deleted = storage
        .delete_registry_entry(created.id, created.revision)
        .await?;
    ensure!(deleted.is_some(), "registry delete returns the deleted row");
    ensure!(
        storage.get_blob(created.sha256).await?.is_none(),
        "registry delete atomically removes the blob row"
    );
    ensure!(
        storage
            .get_registry_entry_by_id(created.id)
            .await?
            .is_none(),
        "registry delete atomically removes the registry row"
    );
    Ok(())
}

// RED until W3a
plugin_registry_scenario!(
    persist_wasm_upload_heals_missing_blob,
    persist_wasm_upload_heals_missing_blob_on_storage
);

async fn persist_wasm_upload_heals_missing_blob_on_storage<S: PluginRegistryStore + 'static>(
    storage: &S,
) -> Result<()> {
    // NOTE: Phase-3 partially green; W3a hardening still needed for chain-scan/atomicity.
    // Postgres enforces wasm_registry_v2.sha256 -> wasm_blobs_v2.sha256 with ON DELETE RESTRICT,
    // which prevents the zombie state this scenario exercises. Skip on postgres.
    #[cfg(feature = "postgres")]
    {
        if (storage as &dyn std::any::Any)
            .downcast_ref::<cc_lb_storage_postgres::PostgresStorage>()
            .is_some()
        {
            return Ok(());
        }
    }
    let wasm = blob(28, b"heal-missing-blob".to_vec());
    let input = entry("plugin-heal-missing-blob");
    let (created, _) = storage
        .persist_wasm_upload(wasm.clone(), input.clone())
        .await?;

    if !delete_blob_row_if_exposed(storage, created.sha256).await? {
        let chain_entry = storage
            .insert_chain_entry(chain(Uuid::new_v4(), created.id, sparse_order::STEP))
            .await?;
        storage
            .delete_chain_entry(chain_entry.id, chain_entry.revision)
            .await?;
    }
    ensure!(
        storage.get_blob(created.sha256).await?.is_none(),
        "test setup removes only the blob row"
    );

    let (healed, existed) = storage.persist_wasm_upload(wasm, input).await?;
    ensure!(
        existed,
        "re-upload of a zombie registry row reports existed"
    );
    ensure!(
        healed.id == created.id,
        "healed upload returns the existing registry id"
    );
    ensure!(
        storage.get_blob(created.sha256).await?.is_some(),
        "re-upload heals the missing blob row"
    );
    Ok(())
}

// RED until W3a
plugin_registry_scenario!(
    decrement_blob_refcount_or_delete_skips_registry_backed,
    decrement_blob_refcount_or_delete_skips_registry_backed_on_storage
);

async fn decrement_blob_refcount_or_delete_skips_registry_backed_on_storage<
    S: PluginRegistryStore + PrincipalStore,
>(
    storage: &S,
) -> Result<()> {
    // NOTE: Phase-3 partially green; W3a hardening still needed for chain-scan/atomicity.
    let (principal, plugin) = principal_and_plugin(storage, 29, "plugin-skip-backed").await?;
    let created = storage
        .insert_chain_entry(chain(principal, plugin.id, sparse_order::STEP))
        .await?;
    storage
        .delete_chain_entry(created.id, created.revision)
        .await?;

    let removed = storage
        .decrement_blob_refcount_or_delete(plugin.sha256)
        .await?;
    ensure!(
        !removed,
        "registry-backed zero-refcount blob is not deleted"
    );
    ensure!(
        storage.get_registry_entry_by_id(plugin.id).await?.is_some(),
        "registry row still references the sha"
    );
    ensure!(
        storage.get_blob(plugin.sha256).await?.is_some(),
        "registry-backed blob row remains"
    );
    Ok(())
}

// RED until W3a
plugin_registry_scenario!(
    insert_chain_entry_rejects_unknown_principal,
    insert_chain_entry_rejects_unknown_principal_on_storage
);

async fn insert_chain_entry_rejects_unknown_principal_on_storage<S: PluginRegistryStore>(
    storage: &S,
) -> Result<()> {
    let (plugin, _) = storage
        .persist_wasm_upload(
            blob(30, b"unknown-principal".to_vec()),
            entry("plugin-unknown-principal"),
        )
        .await?;
    let err = storage
        .insert_chain_entry(chain(Uuid::new_v4(), plugin.id, sparse_order::STEP))
        .await
        .expect_err("chain insert must reject an unknown principal");
    let message = err.to_string();
    ensure!(
        matches!(err, StorageError::InvalidInput { .. }) || message.contains("principal"),
        "unknown principal is reported as invalid input or conflict"
    );
    Ok(())
}

async fn insert_chain_entry_rejects_soft_deleted_principal_on_storage<
    S: PluginRegistryStore + PrincipalStore,
>(
    storage: &S,
) -> Result<()> {
    let (principal, plugin) =
        principal_and_plugin(storage, 43, "plugin-soft-deleted-principal").await?;
    PrincipalStore::soft_delete(storage, principal, 0, BASE_TS + 43)
        .await?
        .expect("principal should soft delete");

    let err = storage
        .insert_chain_entry(chain(principal, plugin.id, sparse_order::STEP))
        .await
        .expect_err("chain insert must reject a soft-deleted principal");
    ensure!(
        matches!(err, StorageError::PrincipalNotFound { .. }),
        "soft-deleted principal is rejected as not found"
    );
    Ok(())
}

// RED until W3a
plugin_registry_scenario!(
    reorder_chain_rejects_final_chain_gap,
    reorder_chain_rejects_final_chain_gap_on_storage
);

async fn reorder_chain_rejects_final_chain_gap_on_storage<
    S: PluginRegistryStore + PrincipalStore,
>(
    storage: &S,
) -> Result<()> {
    let (principal, plugin) = principal_and_plugin(storage, 33, "plugin-final-gap").await?;
    let first = storage
        .insert_chain_entry(chain_with_slot(
            principal,
            PluginSlotKind::Router,
            plugin.id,
            100,
        ))
        .await?;
    let second = storage
        .insert_chain_entry(chain_with_slot(
            principal,
            PluginSlotKind::Router,
            plugin.id,
            200,
        ))
        .await?;
    storage
        .insert_chain_entry(chain_with_slot(
            principal,
            PluginSlotKind::Router,
            plugin.id,
            300,
        ))
        .await?;

    let err = storage
        .reorder_chain(
            principal,
            PluginSlotKind::Router,
            vec![(second.id, first.order - 1, second.revision)],
        )
        .await
        .expect_err("final chain with pairwise gap below 2 is invalid");
    ensure!(
        matches!(
            err,
            StorageError::PluginChainConflict {
                reason: PluginChainConflictReason::InvalidOrderGap
            }
        ),
        "final chain gap conflict returns typed InvalidOrderGap reason"
    );
    Ok(())
}

// RED until W3a
plugin_registry_scenario!(
    update_chain_entry_rejects_no_op,
    update_chain_entry_rejects_no_op_on_storage
);

async fn update_chain_entry_rejects_no_op_on_storage<S: PluginRegistryStore + PrincipalStore>(
    storage: &S,
) -> Result<()> {
    let created = one_chain(storage, 34, "plugin-no-op").await?;
    let err = storage
        .update_chain_entry(
            created.id,
            created.revision,
            PluginChainEntryUpdate::default(),
        )
        .await
        .expect_err("no-op chain updates are invalid");
    ensure!(
        matches!(err, StorageError::InvalidInput { .. }),
        "no-op chain update reports InvalidInput"
    );
    let persisted = storage
        .list_chain_for_principal(created.principal_id, created.slot)
        .await?
        .into_iter()
        .find(|entry| entry.id == created.id)
        .expect("chain entry remains after rejected no-op update");
    ensure!(
        persisted.revision == created.revision,
        "rejected no-op update does not bump revision"
    );
    Ok(())
}

// RED until W3a
plugin_registry_scenario!(
    upload_returns_existed_flag,
    upload_returns_existed_flag_on_storage
);

async fn upload_returns_existed_flag_on_storage<S: PluginRegistryStore>(storage: &S) -> Result<()> {
    let wasm = blob(35, b"upload-existed".to_vec());
    let (first, first_existed) = storage
        .persist_wasm_upload(wasm.clone(), entry("plugin-upload-existed"))
        .await?;
    ensure!(!first_existed, "first upload reports existed=false");
    let uploaded_at = first.uploaded_at_unix_secs;

    let (second, second_existed) = storage
        .persist_wasm_upload(wasm, entry("plugin-upload-existed"))
        .await?;
    ensure!(second_existed, "second upload reports existed=true");
    ensure!(
        second.id == first.id,
        "second upload returns the existing id"
    );
    ensure!(
        second.uploaded_at_unix_secs == uploaded_at,
        "second upload preserves original uploaded_at"
    );

    let err = storage
        .persist_wasm_upload(
            blob(35, b"upload-existed".to_vec()),
            entry("plugin-upload-renamed"),
        )
        .await
        .expect_err("same sha with different metadata conflicts");
    ensure!(
        matches!(
            err,
            StorageError::Conflict { ref message }
                if message == "sha256 already registered for a different wasm entry"
        ),
        "same sha with different metadata returns the strict conflict"
    );
    Ok(())
}

plugin_registry_scenario!(
    same_sha_metadata_mismatch_conflicts,
    same_sha_metadata_mismatch_conflicts_on_storage
);

async fn same_sha_metadata_mismatch_conflicts_on_storage<S: PluginRegistryStore>(
    storage: &S,
) -> Result<()> {
    let wasm = blob(36, b"same-sha-metadata".to_vec());
    storage
        .persist_wasm_upload(wasm.clone(), entry("plugin-same-sha-a"))
        .await?;

    let err = storage
        .persist_wasm_upload(wasm, entry("plugin-same-sha-b"))
        .await
        .expect_err("same sha with a different name conflicts");
    ensure!(
        matches!(
            err,
            StorageError::Conflict { ref message }
                if message == "sha256 already registered for a different wasm entry"
        ),
        "same sha metadata mismatch returns the strict conflict"
    );
    Ok(())
}

async fn insert_orphan_blob_if_exposed<S: PluginRegistryStore + 'static>(
    storage: &S,
    blob: &WasmBlob,
) -> Result<bool> {
    #[cfg(feature = "postgres")]
    {
        if let Some(postgres) = (storage as &dyn std::any::Any)
            .downcast_ref::<cc_lb_storage_postgres::PostgresStorage>()
        {
            sqlx::query("INSERT INTO wasm_blobs_v2 (sha256, bytes, size_bytes, parse_validated_at, created_at) VALUES ($1, $2, $3, NOW(), NOW()) ON CONFLICT (sha256) DO NOTHING")
                .bind(blob.sha256.as_slice())
                .bind(blob.bytes.as_slice())
                .bind(i64::try_from(blob.size_bytes)?)
                .execute(postgres.pool())
                .await?;
            return Ok(true);
        }
    }
    let _ = (storage, blob);
    Ok(false)
}

async fn delete_blob_row_if_exposed<S: PluginRegistryStore + 'static>(
    storage: &S,
    sha256: [u8; 32],
) -> Result<bool> {
    #[cfg(feature = "postgres")]
    {
        if let Some(postgres) = (storage as &dyn std::any::Any)
            .downcast_ref::<cc_lb_storage_postgres::PostgresStorage>()
        {
            let result = sqlx::query("DELETE FROM wasm_blobs_v2 WHERE sha256 = $1")
                .bind(sha256.as_slice())
                .execute(postgres.pool())
                .await?;
            return Ok(result.rows_affected() > 0);
        }
    }
    let _ = (storage, sha256);
    Ok(false)
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

pub async fn fk_on_delete_restrict<S: PluginRegistryStore + PrincipalStore>(
    storage: &S,
) -> Result<()> {
    let created = one_chain(storage, 21, "plugin-fk").await?;
    ensure!(
        storage.get_blob_bytes([21; 32]).await?.is_some(),
        "referenced blob remains available"
    );
    ensure!(
        storage
            .list_chain_for_principal(created.principal_id, created.slot)
            .await?
            .iter()
            .any(|entry| entry.id == created.id),
        "custom chain exists"
    );
    Ok(())
}

async fn principal_and_plugin<S: PluginRegistryStore + PrincipalStore>(
    storage: &S,
    seed: u8,
    name: &str,
) -> Result<(Uuid, cc_lb_storage_api::WasmRegistryEntry)> {
    let principal =
        PrincipalStore::create(storage, principal_create(seed), BASE_TS + seed as u64).await?;
    let (plugin, _) = storage
        .persist_wasm_upload(blob(seed, vec![seed]), entry(name))
        .await?;
    Ok((principal.id, plugin))
}

async fn one_chain<S: PluginRegistryStore + PrincipalStore>(
    storage: &S,
    seed: u8,
    name: &str,
) -> Result<cc_lb_storage_api::PluginChainEntry> {
    let (principal, plugin) = principal_and_plugin(storage, seed, name).await?;
    Ok(storage
        .insert_chain_entry(chain(principal, plugin.id, sparse_order::STEP))
        .await?)
}

fn principal_create(seed: u8) -> PrincipalCreate {
    PrincipalCreate {
        name: format!("plugin-principal-{seed:04}"),
        kind: PrincipalKind::Machine,
        allowed_models: vec!["claude-sonnet-*".to_owned()],
        allowed_upstreams: vec![],
        default_limits: vec![Limit {
            kind: LimitKind::Requests,
            window_secs: 60,
            cap_micros: 100,
        }],
        cache_keepalive: None,
    }
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
        schema_hash: None,
        name: name.to_owned(),
        version: None,
        original_filename: format!("{name}.wasm"),
        label: None,
        uploaded_at_unix_secs: 1_800_000_100,
        uploaded_by_admin_id: Uuid::new_v4(),
        description: format!("{name} description"),
        usage: format!("{name} usage"),
        hook_metadata: filter_hook_metadata(),
        supported_slots: Vec::new(),
    }
}

fn chain(principal_id: Uuid, wasm_registry_id: Uuid, order: i64) -> PluginChainEntryInput {
    chain_with_slot(
        principal_id,
        PluginSlotKind::Router,
        wasm_registry_id,
        order,
    )
}

fn chain_with_slot(
    principal_id: Uuid,
    slot: PluginSlotKind,
    wasm_registry_id: Uuid,
    order: i64,
) -> PluginChainEntryInput {
    PluginChainEntryInput {
        principal_id,
        slot,
        order,
        wasm_registry_id,
        config: json!({}),
    }
}

fn warmup_upstream(name: &str, wasm_registry_id: Uuid) -> UpstreamCreate {
    UpstreamCreate {
        name: name.to_owned(),
        kind: cc_lb_storage_api::upstream::UpstreamKind::AnthropicApiKey,
        warmup_enabled: true,
        warmup_dialect_plugin: Some(UpstreamWarmupDialectPlugin {
            wasm_registry_id,
            config: json!({}),
            wire_version: None,
        }),
        ..Default::default()
    }
}

fn filter_hook_metadata() -> BTreeMap<String, HookMetadata> {
    BTreeMap::from([(
        "filter".to_owned(),
        HookMetadata {
            wire_version: default_wire_version(),
            description: "filter hook".to_owned(),
            usage: "called by router".to_owned(),
            mode: Default::default(),
        },
    )])
}
