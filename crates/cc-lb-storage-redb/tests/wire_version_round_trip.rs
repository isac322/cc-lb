use anyhow::Result;
use cc_lb_storage_api::{PluginRegistryStore, PluginSlot, WasmBlob, WasmRegistryEntryInput};
use cc_lb_storage_redb::RedbStorage;
use uuid::Uuid;

#[tokio::test]
async fn wire_version_round_trip() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("wire_version_round_trip.redb");
    let storage = RedbStorage::open(&path, [53; 32])?;

    let bytes = b"wire-version-plugin".to_vec();
    let blob = WasmBlob {
        sha256: [2; 32],
        size_bytes: bytes.len() as u64,
        bytes,
        parse_validated_at_unix_secs: 1_800_000_000,
    };
    let input = WasmRegistryEntryInput {
        name: "wire-version-plugin".to_owned(),
        original_filename: "wire-version-plugin.wasm".to_owned(),
        label: Some("Wire version plugin".to_owned()),
        uploaded_at_unix_secs: 1_800_000_100,
        uploaded_by_admin_id: Uuid::new_v4(),
        wire_version: 2,
        supported_slots: vec![PluginSlot::Router],
    };

    let (entry, existed) = storage.persist_wasm_upload(blob, input).await?;
    assert!(!existed);
    assert_eq!(entry.wire_version, 2);

    let by_id = storage
        .get_registry_entry_by_id(entry.id)
        .await?
        .expect("entry should be readable by id");
    assert_eq!(by_id.wire_version, 2);

    storage.update_wire_version(entry.id, 3).await?;
    let updated = storage
        .get_registry_entry_by_id(entry.id)
        .await?
        .expect("entry should be readable after update");
    assert_eq!(updated.wire_version, 3);
    assert_eq!(updated.revision, entry.revision + 1);

    Ok(())
}
