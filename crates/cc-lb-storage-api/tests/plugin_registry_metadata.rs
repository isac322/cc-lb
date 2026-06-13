use cc_lb_storage_api::{
    BUILTIN_CACHE_AFFINITY_ID, PluginMetadata, PluginSlot, WasmRegistryEntry,
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
fn builtin_cache_affinity_entry_synthesizes_metadata() {
    let entry = WasmRegistryEntry::builtin_cache_affinity(2);
    let metadata = entry.metadata.expect("builtin metadata is present");

    assert_eq!(entry.id, BUILTIN_CACHE_AFFINITY_ID);
    assert_eq!(
        metadata.purpose,
        "Prefer upstreams whose prompt cache is already warm for this request."
    );
    assert_eq!(
        metadata.keeps,
        "Candidates with a positive prefill_cache_score (the upstream has already cached the prefix)."
    );
    assert_eq!(
        metadata.drops,
        "Candidates with zero cache score — only when at least one candidate is a cache hit; otherwise nothing is dropped."
    );
    assert_eq!(
        metadata.empty_behavior,
        "Never drops everything. Falls back to passing all candidates through when no cache hit exists."
    );
    assert_eq!(
        metadata.examples,
        vec![
            "5 candidates, 2 with positive cache score → keep the 2 hits.",
            "5 candidates, all with zero cache score → pass all 5 through.",
            "Exactly 1 candidate → no change.",
        ]
    );
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
    entry.supported_slots = vec![PluginSlot::Router, PluginSlot::Shape];

    let serialized = serde_json::to_value(&entry).unwrap();
    let roundtripped: WasmRegistryEntry = serde_json::from_value(serialized).unwrap();

    assert_eq!(roundtripped, entry);
}

#[test]
fn builtin_cache_affinity_entry_advertises_router_slot() {
    let entry = WasmRegistryEntry::builtin_cache_affinity(0);
    assert_eq!(entry.supported_slots, vec![PluginSlot::Router]);
}

fn uploaded_entry(metadata: Option<PluginMetadata>) -> WasmRegistryEntry {
    WasmRegistryEntry {
        id: Uuid::new_v4(),
        sha256: [1; 32],
        name: "uploaded".to_owned(),
        original_filename: "uploaded.wasm".to_owned(),
        label: None,
        uploaded_at_unix_secs: 1_800_000_000,
        uploaded_by_admin_id: Uuid::new_v4(),
        refcount: 0,
        revision: 0,
        kind: "filter".to_owned(),
        wire_version: 3,
        is_builtin: false,
        metadata,
        supported_slots: Vec::new(),
    }
}
