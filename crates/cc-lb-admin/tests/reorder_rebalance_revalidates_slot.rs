mod admin_test_common;

use axum::http::StatusCode;
use cc_lb_storage_api::{
    PluginChainEntry, PluginChainEntryInput, PluginRegistryStore, PluginSlot, PrincipalCreate,
    PrincipalKind, PrincipalStore, WasmBlob, WasmRegistryEntry, WasmRegistryEntryInput,
    sparse_order,
};
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
async fn reorder_chain_revalidates_slot_drift() {
    let server = admin_test_common::spawn_admin_server();
    let principal_id = seed_principal(&server.storage, "principal-reorder-slot-drift").await;
    let registry = seed_registry_with_slots(
        &server.storage,
        31,
        "shape-drifted-to-router-reorder",
        vec![PluginSlot::Shape],
    )
    .await;
    let chain = seed_chain_with_slot(
        &server.storage,
        principal_id,
        PluginSlot::Shape,
        registry.id,
        sparse_order::STEP,
    )
    .await;
    server
        .storage
        .update_supported_slots(registry.id, vec![PluginSlot::Router])
        .await
        .unwrap();

    let (status, _, body) = server
        .client
        .post_json(
            &format!("/admin/v1/principals/{principal_id}/plugin-chain/reorder"),
            json!({ "entries": [
                { "id": chain.id, "order": sparse_order::STEP, "expected_revision": chain.revision }
            ] }),
        )
        .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        body,
        json!({
            "error": "chain_drift_detected",
            "principal_id": principal_id,
            "wasm_registry_id": registry.id,
            "chain_slot": "shape",
            "supported_slots": ["router"],
        })
    );
}

#[tokio::test]
async fn rebalance_chain_revalidates_slot_drift() {
    let server = admin_test_common::spawn_admin_server();
    let principal_id = seed_principal(&server.storage, "principal-rebalance-slot-drift").await;
    let registry = seed_registry_with_slots(
        &server.storage,
        32,
        "shape-drifted-to-router-rebalance",
        vec![PluginSlot::Shape],
    )
    .await;
    seed_chain_with_slot(
        &server.storage,
        principal_id,
        PluginSlot::Shape,
        registry.id,
        sparse_order::STEP,
    )
    .await;
    server
        .storage
        .update_supported_slots(registry.id, vec![PluginSlot::Router])
        .await
        .unwrap();

    let (status, _, body) = server
        .client
        .json(
            "POST",
            &format!("/admin/v1/principals/{principal_id}/plugin-chain/rebalance?slot=Shape"),
            None,
            &[],
        )
        .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        body,
        json!({
            "error": "chain_drift_detected",
            "principal_id": principal_id,
            "wasm_registry_id": registry.id,
            "chain_slot": "shape",
            "supported_slots": ["router"],
        })
    );
}

async fn seed_principal(storage: &cc_lb_storage_redb::Storage, name: &str) -> Uuid {
    storage
        .create(
            PrincipalCreate {
                name: name.to_owned(),
                kind: PrincipalKind::Machine,
                allowed_models: Vec::new(),
                allowed_upstreams: Vec::new(),
                default_limits: Vec::new(),
            },
            1_800_000_000,
        )
        .await
        .unwrap()
        .id
}

async fn seed_registry_with_slots(
    storage: &cc_lb_storage_redb::Storage,
    seed: u8,
    name: &str,
    slots: Vec<PluginSlot>,
) -> WasmRegistryEntry {
    let (entry, _) = storage
        .persist_wasm_upload(
            WasmBlob {
                sha256: [seed; 32],
                bytes: vec![seed; seed as usize],
                size_bytes: seed as u64,
                parse_validated_at_unix_secs: 1_800_000_000,
            },
            WasmRegistryEntryInput {
                name: name.to_owned(),
                original_filename: format!("{name}.wasm"),
                label: None,
                uploaded_at_unix_secs: 1_800_000_000,
                uploaded_by_admin_id: Uuid::new_v4(),
                wire_version: 1,
                supported_slots: slots,
            },
        )
        .await
        .unwrap();
    entry
}

async fn seed_chain_with_slot(
    storage: &cc_lb_storage_redb::Storage,
    principal_id: Uuid,
    slot: PluginSlot,
    wasm_registry_id: Uuid,
    order: i64,
) -> PluginChainEntry {
    storage
        .insert_chain_entry(PluginChainEntryInput {
            principal_id,
            slot,
            order,
            wasm_registry_id,
            config: json!({}),
            sse_per_event: false,
            batched_events_per_flush: 1,
            batched_flush_ms: 100,
            wire_version: None,
        })
        .await
        .unwrap()
}
