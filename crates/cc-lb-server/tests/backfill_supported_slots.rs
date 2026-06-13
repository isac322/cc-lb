use std::sync::Arc;

use cc_lb_server::app::backfill_supported_slots;
use cc_lb_storage_api::{
    BUILTIN_CACHE_AFFINITY_ID, PluginRegistryStore, PluginSlot, WasmBlob, WasmRegistryEntry,
    WasmRegistryEntryInput,
};
use cc_lb_storage_redb::Storage;
use tempfile::TempDir;
use uuid::Uuid;

#[tokio::test]
async fn backfill_populates_builtin_cache_affinity_with_router_slot() {
    let (_dir, storage) = open_storage();
    overwrite_builtin_with_empty_slots(&storage).await;

    backfill_supported_slots(storage.as_ref()).await;

    let entry = storage
        .get_registry_entry_by_id(BUILTIN_CACHE_AFFINITY_ID)
        .await
        .unwrap()
        .expect("builtin entry exists");
    assert_eq!(entry.supported_slots, vec![PluginSlot::Router]);
}

#[tokio::test]
async fn backfill_skips_entries_already_populated_and_is_idempotent() {
    let (_dir, storage) = open_storage();
    let entry = seed_legacy_entry(&storage, 9, "already-populated").await;
    storage
        .update_supported_slots(entry.id, vec![PluginSlot::Shape])
        .await
        .unwrap();

    backfill_supported_slots(storage.as_ref()).await;
    backfill_supported_slots(storage.as_ref()).await;

    let after = storage
        .get_registry_entry_by_id(entry.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.supported_slots, vec![PluginSlot::Shape]);
}

#[tokio::test]
async fn backfill_tolerates_corrupt_blob_without_panicking() {
    let (_dir, storage) = open_storage();
    let entry = seed_legacy_entry(&storage, 11, "corrupt-blob").await;
    assert!(entry.supported_slots.is_empty());

    backfill_supported_slots(storage.as_ref()).await;

    let after = storage
        .get_registry_entry_by_id(entry.id)
        .await
        .unwrap()
        .unwrap();
    assert!(after.supported_slots.is_empty());
}

fn open_storage() -> (TempDir, Arc<Storage>) {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(&dir.path().join("backfill.redb"), [42; 32]).unwrap());
    (dir, storage)
}

async fn overwrite_builtin_with_empty_slots(storage: &Arc<Storage>) {
    storage
        .update_supported_slots(BUILTIN_CACHE_AFFINITY_ID, Vec::new())
        .await
        .unwrap();
    let entry = storage
        .get_registry_entry_by_id(BUILTIN_CACHE_AFFINITY_ID)
        .await
        .unwrap()
        .expect("builtin entry exists");
    assert!(entry.supported_slots.is_empty());
}

async fn seed_legacy_entry(storage: &Arc<Storage>, seed: u8, name: &str) -> WasmRegistryEntry {
    let (entry, _) = storage
        .persist_wasm_upload(
            WasmBlob {
                sha256: [seed; 32],
                bytes: vec![seed; 16],
                size_bytes: 16,
                parse_validated_at_unix_secs: 1_800_000_000,
            },
            WasmRegistryEntryInput {
                name: name.to_owned(),
                original_filename: format!("{name}.wasm"),
                label: None,
                uploaded_at_unix_secs: 1_800_000_000,
                uploaded_by_admin_id: Uuid::new_v4(),
                supported_slots: Vec::new(),
            },
        )
        .await
        .unwrap();
    entry
}
