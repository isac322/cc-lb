use crate::config_admin_common;

use axum::http::StatusCode;
use cc_lb_config::Config;
use cc_lb_storage_api::{PluginRegistryStore, PluginSlotKind, WasmBlob, WasmRegistryEntryInput};
use config_admin_common::{app, authed_json, temp_storage, test_state};
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
async fn export_supported_slots_serializes_registry_entry_slots_as_snake_case() {
    let (_dir, storage) = temp_storage().await;
    storage
        .persist_wasm_upload(
            WasmBlob {
                sha256: [42; 32],
                bytes: vec![42; 42],
                size_bytes: 42,
                parse_validated_at_unix_secs: 1_800_000_000,
            },
            WasmRegistryEntryInput {
                schema_hash: None,
                name: "plugin-slots".to_owned(),
                original_filename: "plugin-slots.wasm".to_owned(),
                label: Some("slot fixture".to_owned()),
                uploaded_at_unix_secs: 1_800_000_000,
                uploaded_by_admin_id: Uuid::new_v4(),
                description: "slot fixture".to_owned(),
                usage: "test fixture".to_owned(),
                hook_metadata: Default::default(),
                supported_slots: vec![
                    PluginSlotKind::Router,
                    PluginSlotKind::Shape,
                    PluginSlotKind::ObservabilityHook,
                ],
            },
        )
        .await
        .unwrap();

    let (status, _, body, _) = authed_json(
        app(test_state(Config::default(), Some(storage))),
        "GET",
        "/admin/v1/export",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let plugin = body["plugins"]["registry"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["name"] == "plugin-slots")
        .expect("uploaded plugin is included in export registry");
    assert_eq!(
        plugin["supported_slots"],
        json!(["router", "shape", "observability_hook"])
    );
}
