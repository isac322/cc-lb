use cc_lb_storage_api::{
    BUILTIN_SUBSCRIPTION_PREFERENCE_ID, PluginMetadata, PluginSlotKind, WasmRegistryEntry,
};
use serde_json::json;
use uuid::Uuid;

#[test]
fn plugin_metadata_roundtrips_with_default_examples() {
    let value = json!({
        "purpose": "purpose",
        "keeps": "keeps",
        "drops": "drops",
        "empty_behavior": "empty behavior"
    });

    let metadata: PluginMetadata = serde_json::from_value(value).unwrap();

    assert_eq!(metadata.examples, Vec::<String>::new());
    assert_eq!(
        serde_json::from_value::<PluginMetadata>(serde_json::to_value(&metadata).unwrap()).unwrap(),
        metadata
    );
}

#[test]
fn wasm_registry_entry_roundtrips_with_metadata() {
    let entry = uploaded_entry(Some(PluginMetadata {
        purpose: "purpose".to_owned(),
        keeps: "keeps".to_owned(),
        drops: "drops".to_owned(),
        empty_behavior: "empty behavior".to_owned(),
        examples: vec!["example".to_owned()],
    }));

    let roundtripped: WasmRegistryEntry =
        serde_json::from_value(serde_json::to_value(&entry).unwrap()).unwrap();

    assert_eq!(roundtripped, entry);
    assert_eq!(roundtripped.metadata.unwrap().examples, vec!["example"]);
}

#[test]
fn wasm_registry_entry_defaults_missing_metadata_to_none() {
    let value = serde_json::to_value(uploaded_entry(None)).unwrap();

    assert!(value.get("metadata").is_none());
    let entry: WasmRegistryEntry = serde_json::from_value(value).unwrap();
    assert_eq!(entry.metadata, None);
}

#[test]
fn legacy_wasm_registry_entry_defaults_supported_slots_to_empty() {
    let sha: [u8; 32] = [1; 32];
    let value = json!({
        "id": Uuid::new_v4(),
        "sha256": sha,
        "name": "legacy",
        "original_filename": "legacy.wasm",
        "label": null,
        "uploaded_at_unix_secs": 1_800_000_000u64,
        "uploaded_by_admin_id": Uuid::new_v4(),
        "refcount": 0u64,
        "revision": 0u64,
        "kind": "filter",
        "wire_version": 1,
        "is_builtin": false
    });

    let entry: WasmRegistryEntry = serde_json::from_value(value).unwrap();
    assert!(entry.supported_slots.is_empty());
}

#[test]
fn wasm_registry_entry_round_trips_with_supported_slots() {
    let mut entry = uploaded_entry(None);
    entry.supported_slots = vec![PluginSlotKind::Router, PluginSlotKind::Shape];

    let serialized = serde_json::to_value(&entry).unwrap();
    let roundtripped: WasmRegistryEntry = serde_json::from_value(serialized).unwrap();

    assert_eq!(roundtripped, entry);
}

#[test]
fn builtin_subscription_preference_entry_synthesizes_metadata() {
    let entry = WasmRegistryEntry::builtin_subscription_preference(2);
    let metadata = entry.metadata.expect("builtin metadata is present");

    assert_eq!(entry.id, BUILTIN_SUBSCRIPTION_PREFERENCE_ID);
    assert_eq!(entry.supported_slots, vec![PluginSlotKind::Router]);
    assert_eq!(
        metadata.purpose,
        "Prefer subscription/OAuth upstreams while quota appears alive; use API-key upstreams only when subscription candidates are exhausted."
    );
    assert!(metadata.keeps.contains("OAuth candidates"));
    assert!(metadata.drops.contains("API-key candidates"));
}

fn uploaded_entry(metadata: Option<PluginMetadata>) -> WasmRegistryEntry {
    WasmRegistryEntry {
        schema_hash: None,
        id: Uuid::new_v4(),
        sha256: [1; 32],
        name: "uploaded".to_owned(),
        version: None,
        original_filename: "uploaded.wasm".to_owned(),
        label: None,
        uploaded_at_unix_secs: 1_800_000_000,
        uploaded_by_admin_id: Uuid::new_v4(),
        refcount: 0,
        revision: 0,
        kind: "filter".to_owned(),
        description: "uploaded description".to_owned(),
        usage: "uploaded usage".to_owned(),
        hook_metadata: Default::default(),
        is_builtin: false,
        metadata,
        supported_slots: Vec::new(),
    }
}
