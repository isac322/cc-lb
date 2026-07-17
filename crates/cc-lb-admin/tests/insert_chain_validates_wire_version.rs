use crate::config_admin_common;

use axum::http::StatusCode;
use cc_lb_config::Config;
use cc_lb_storage_api::{
    BUILTIN_SUBSCRIPTION_PREFERENCE_ID, PluginRegistryStore, PluginSlotKind, PrincipalCreate,
    PrincipalKind, PrincipalStore, WasmBlob, WasmRegistryEntryInput,
};
use config_admin_common::{app, authed_json, temp_storage, test_state};
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
async fn insert_chain_rejects_unsupported_slot_from_registry_metadata() {
    let (_dir, storage) = temp_storage().await;
    let principal_id = seed_principal(&storage, "principal-wire-too-new").await;
    let entry = seed_registry_with_wire_version(&storage, 41, "wire-v1-plugin", 1).await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, _, body, _) = authed_json(
        app,
        "POST",
        &format!("/admin/v1/principals/{principal_id}/plugin-chain"),
        Some(json!({
            "slot": "Shape",
            "wasm_registry_id": entry.id,
        })),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "unsupported_slot");
}

#[tokio::test]
async fn insert_chain_accepts_registry_entry() {
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
async fn insert_chain_accepts_unspecified_metadata() {
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
async fn insert_chain_accepts_builtin_subscription_preference() {
    let (_dir, storage) = temp_storage().await;
    let principal_id = seed_principal(&storage, "principal-wire-builtin").await;
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, _, body, _) = authed_json(
        app,
        "POST",
        &format!("/admin/v1/principals/{principal_id}/plugin-chain"),
        Some(json!({
            "slot": "Router",
            "wasm_registry_id": BUILTIN_SUBSCRIPTION_PREFERENCE_ID
        })),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(
        body["wasm_registry_id"],
        BUILTIN_SUBSCRIPTION_PREFERENCE_ID.to_string()
    );
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
                cache_keepalive: None,
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
                version: None,
                original_filename: format!("{name}.wasm"),
                label: None,
                uploaded_at_unix_secs: 1_800_000_000,
                uploaded_by_admin_id: Uuid::new_v4(),
                description: format!("{name} description"),
                usage: format!("wire v{wire_version} fixture"),
                hook_metadata: Default::default(),
                supported_slots: vec![PluginSlotKind::Router],
            },
        )
        .await
        .unwrap();
    entry
}
