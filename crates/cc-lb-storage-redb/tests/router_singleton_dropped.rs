use cc_lb_storage_api::{
    PluginChainEntryInput, PluginRegistryStore, PluginSlot, PrincipalStore, WasmBlob,
    WasmRegistryEntryInput, PrincipalCreate, PrincipalKind,
    principal::{Limit, LimitKind},
};
use cc_lb_storage_redb::RedbStorage;
use serde_json::json;
use uuid::Uuid;

async fn setup_storage() -> anyhow::Result<RedbStorage> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("router_singleton_dropped.redb");
    let storage = RedbStorage::open(&path, [50; 32])?;
    Ok(storage)
}

async fn create_test_principal(
    storage: &RedbStorage,
) -> anyhow::Result<Uuid> {
    let principal_create = PrincipalCreate {
        name: "test-principal".to_string(),
        kind: PrincipalKind::Human,
        allowed_models: vec![],
        allowed_upstreams: vec![],
        default_limits: vec![
            Limit {
                kind: LimitKind::Requests,
                window_secs: 60,
                cap_micros: 1_000_000,
            },
        ],
    };
    let principal = storage.create(principal_create, 1000).await?;
    Ok(principal.id)
}

async fn upload_wasm_plugin(
    storage: &RedbStorage,
    name: &str,
) -> anyhow::Result<Uuid> {
    let blob = WasmBlob {
        sha256: [1; 32],
        bytes: b"wasm code".to_vec(),
        size_bytes: 9,
        parse_validated_at_unix_secs: 1000,
    };
    let entry = WasmRegistryEntryInput {
        name: name.to_string(),
        original_filename: "test.wasm".to_string(),
        label: Some("test".to_string()),
        uploaded_at_unix_secs: 1000,
        uploaded_by_admin_id: Uuid::nil(),
    };
    let (reg_entry, _) = storage.persist_wasm_upload(blob, entry).await?;
    Ok(reg_entry.id)
}

#[tokio::test]
async fn router_allows_multiple_entries_per_principal() -> anyhow::Result<()> {
    let storage = setup_storage().await?;
    let principal_id = create_test_principal(&storage).await?;
    let plugin_id = upload_wasm_plugin(&storage, "router_multi_test").await?;

    // Create first Router entry
    let entry1_input = PluginChainEntryInput {
        principal_id,
        slot: PluginSlot::Router,
        order: 100,
        wasm_registry_id: plugin_id,
        config: json!({"mode": "route1"}),
        sse_per_event: false,
        batched_events_per_flush: 100,
        batched_flush_ms: 1000,
        wire_version: None,
    };
    let entry1 = storage.insert_chain_entry(entry1_input).await?;
    assert_eq!(entry1.slot, PluginSlot::Router);

    // Create second Router entry for same principal - should succeed
    let entry2_input = PluginChainEntryInput {
        principal_id,
        slot: PluginSlot::Router,
        order: 200,
        wasm_registry_id: plugin_id,
        config: json!({"mode": "route2"}),
        sse_per_event: false,
        batched_events_per_flush: 100,
        batched_flush_ms: 1000,
        wire_version: None,
    };
    let entry2 = storage.insert_chain_entry(entry2_input).await?;
    assert_eq!(entry2.slot, PluginSlot::Router);
    assert_ne!(entry1.id, entry2.id);

    // Verify both entries exist for the Router slot
    let entries = storage.list_chain_for_principal(principal_id, PluginSlot::Router).await?;
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().any(|e| e.id == entry1.id));
    assert!(entries.iter().any(|e| e.id == entry2.id));

    Ok(())
}

#[tokio::test]
async fn shape_remains_singleton() -> anyhow::Result<()> {
    let storage = setup_storage().await?;
    let principal_id = create_test_principal(&storage).await?;
    let plugin_id = upload_wasm_plugin(&storage, "shape_singleton_test").await?;

    // Create first Shape entry
    let entry1_input = PluginChainEntryInput {
        principal_id,
        slot: PluginSlot::Shape,
        order: 100,
        wasm_registry_id: plugin_id,
        config: json!({}),
        sse_per_event: false,
        batched_events_per_flush: 100,
        batched_flush_ms: 1000,
        wire_version: None,
    };
    let entry1 = storage.insert_chain_entry(entry1_input).await?;
    assert_eq!(entry1.slot, PluginSlot::Shape);

    // Try to create second Shape entry for same principal - should fail
    let entry2_input = PluginChainEntryInput {
        principal_id,
        slot: PluginSlot::Shape,
        order: 200,
        wasm_registry_id: plugin_id,
        config: json!({}),
        sse_per_event: false,
        batched_events_per_flush: 100,
        batched_flush_ms: 1000,
        wire_version: None,
    };
    let result = storage.insert_chain_entry(entry2_input).await;

    // Should get a conflict error about singleton slot
    assert!(result.is_err());
    match result.unwrap_err() {
        cc_lb_storage_api::StorageError::PluginChainConflict {
            reason: cc_lb_storage_api::PluginChainConflictReason::SlotIsSingleton { .. },
        } => {},
        other => panic!("expected SlotIsSingleton error, got: {:?}", other),
    }

    // Verify only one Shape entry exists
    let entries = storage.list_chain_for_principal(principal_id, PluginSlot::Shape).await?;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].id, entry1.id);

    Ok(())
}

#[tokio::test]
async fn router_and_shape_coexist_independently() -> anyhow::Result<()> {
    let storage = setup_storage().await?;
    let principal_id = create_test_principal(&storage).await?;
    let plugin_id = upload_wasm_plugin(&storage, "coexist_test").await?;

    // Insert multiple routers
    for i in 0..3 {
        let entry_input = PluginChainEntryInput {
            principal_id,
            slot: PluginSlot::Router,
            order: (i + 1) as i64 * 100,
            wasm_registry_id: plugin_id,
            config: json!({"id": i}),
            sse_per_event: false,
            batched_events_per_flush: 100,
            batched_flush_ms: 1000,
            wire_version: None,
        };
        storage.insert_chain_entry(entry_input).await?;
    }

    // Insert one shape
    let shape_input = PluginChainEntryInput {
        principal_id,
        slot: PluginSlot::Shape,
        order: 500,
        wasm_registry_id: plugin_id,
        config: json!({}),
        sse_per_event: false,
        batched_events_per_flush: 100,
        batched_flush_ms: 1000,
        wire_version: None,
    };
    storage.insert_chain_entry(shape_input).await?;

    // Verify 3 routers exist
    let routers = storage.list_chain_for_principal(principal_id, PluginSlot::Router).await?;
    assert_eq!(routers.len(), 3);

    // Verify 1 shape exists
    let shapes = storage.list_chain_for_principal(principal_id, PluginSlot::Shape).await?;
    assert_eq!(shapes.len(), 1);

    // Verify observability hook slot is also not enforced as singleton
    let obs_input = PluginChainEntryInput {
        principal_id,
        slot: PluginSlot::ObservabilityHook,
        order: 100,
        wasm_registry_id: plugin_id,
        config: json!({}),
        sse_per_event: false,
        batched_events_per_flush: 100,
        batched_flush_ms: 1000,
        wire_version: None,
    };
    storage.insert_chain_entry(obs_input.clone()).await?;
    storage.insert_chain_entry(obs_input).await?;

    let obs_hooks = storage.list_chain_for_principal(principal_id, PluginSlot::ObservabilityHook).await?;
    assert_eq!(obs_hooks.len(), 2);

    Ok(())
}
