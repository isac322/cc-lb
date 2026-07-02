mod config_admin_common;

use axum::http::StatusCode;
use cc_lb_config::Config;
use cc_lb_storage_api::{
    BUILTIN_CACHE_AFFINITY_ID, PluginRegistryStore, PluginSlot, PrincipalCreate, PrincipalKind,
    PrincipalStore, WasmBlob, WasmRegistryEntryInput, default_wire_version,
};
use config_admin_common::{app, authed_json, temp_storage, test_state};
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
async fn insert_chain_rejects_wire_version_above_registry_entry() {
    let (_dir, storage) = temp_storage().await;
    let principal_id = seed_principal(&storage, "principal-wire-too-new").await;
    let entry = seed_registry_with_wire_version(&storage, 41, "wire-v1-plugin", 1).await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, _, body, _) = authed_json(
        app,
        "POST",
        &format!("/admin/v1/principals/{principal_id}/plugin-chain"),
        Some(json!({
            "slot": "Router",
            "wasm_registry_id": entry.id,
            "wire_version": 2
        })),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body,
        json!({
            "error": "unsupported_wire_version",
            "plugin_name": "wire-v1-plugin",
            "requested": 2,
            "max_supported": 1
        })
    );
}

#[tokio::test]
async fn insert_chain_accepts_wire_version_equal_to_registry_entry() {
    let (_dir, storage) = temp_storage().await;
    let principal_id = seed_principal(&storage, "principal-wire-equal").await;
    let entry = seed_registry_with_wire_version(&storage, 42, "wire-v1-plugin-equal", 1).await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, _, body, _) = authed_json(
        app,
        "POST",
        &format!("/admin/v1/principals/{principal_id}/plugin-chain"),
        Some(json!({
            "slot": "Router",
            "wasm_registry_id": entry.id,
            "wire_version": 1
        })),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["wire_version"], 1);
}

#[tokio::test]
async fn insert_chain_accepts_unspecified_wire_version() {
    let (_dir, storage) = temp_storage().await;
    let principal_id = seed_principal(&storage, "principal-wire-unspecified").await;
    let entry = seed_registry_with_wire_version(&storage, 43, "wire-default-plugin", 1).await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, _, body, _) = authed_json(
        app,
        "POST",
        &format!("/admin/v1/principals/{principal_id}/plugin-chain"),
        Some(json!({
            "slot": "Router",
            "wasm_registry_id": entry.id
        })),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    assert!(
        body.get("wire_version")
            .is_none_or(serde_json::Value::is_null)
    );
}

#[tokio::test]
async fn insert_chain_accepts_builtin_cache_affinity_wire_version() {
    let (_dir, storage) = temp_storage().await;
    let principal_id = seed_principal(&storage, "principal-wire-builtin").await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, _, body, _) = authed_json(
        app,
        "POST",
        &format!("/admin/v1/principals/{principal_id}/plugin-chain"),
        Some(json!({
            "slot": "Router",
            "wasm_registry_id": BUILTIN_CACHE_AFFINITY_ID,
            "wire_version": default_wire_version()
        })),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(
        body["wasm_registry_id"],
        BUILTIN_CACHE_AFFINITY_ID.to_string()
    );
    assert_eq!(body["wire_version"], default_wire_version());
}

async fn seed_principal(storage: &cc_lb_storage_sqlite::SqliteStorage, name: &str) -> Uuid {
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

async fn seed_registry_with_wire_version(
    storage: &cc_lb_storage_sqlite::SqliteStorage,
    seed: u8,
    name: &str,
    wire_version: u8,
) -> cc_lb_storage_api::WasmRegistryEntry {
    let (entry, _) = storage
        .persist_wasm_upload(
            WasmBlob {
                sha256: [seed; 32],
                bytes: vec![seed; seed as usize],
                size_bytes: seed as u64,
                parse_validated_at_unix_secs: 1_800_000_000,
            },
            WasmRegistryEntryInput {
                schema_hash: None,
                name: name.to_owned(),
                original_filename: format!("{name}.wasm"),
                label: None,
                uploaded_at_unix_secs: 1_800_000_000,
                uploaded_by_admin_id: Uuid::new_v4(),
                wire_version,
                supported_slots: vec![PluginSlot::Router],
            },
        )
        .await
        .unwrap();
    entry
}
