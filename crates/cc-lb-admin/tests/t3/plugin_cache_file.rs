use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use cc_lb_config::Config;
use cc_lb_storage_api::{PluginRegistryStore, PluginSlotKind, WasmBlob, WasmRegistryEntryInput};
use http_body_util::BodyExt;
use tower::ServiceExt;
use uuid::Uuid;

use crate::config_admin_common::{TOKEN, app, sqlite_temp_storage, test_state};

#[tokio::test]
async fn t3__registry_delete_when_unreferenced_deletes_blob_and_cache_file() {
    let (dir, storage) = sqlite_temp_storage().await;
    let entry = seed_registry(&storage).await;
    let data_dir = dir.path().join("data");
    let cache_dir = data_dir.join("plugins/wasm/cache");
    std::fs::create_dir_all(&cache_dir).unwrap();
    let cache_path = cache_dir.join(format!("{}.wasm", hex_sha256(entry.sha256)));
    std::fs::write(&cache_path, b"cached wasm").unwrap();
    let mut config = Config::default();
    config.runtime.data_dir = Some(data_dir);
    let app = app(test_state(config, Some(storage.clone())));

    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/admin/v1/plugins/registry/{}", entry.id))
                .header("Authorization", format!("Bearer {TOKEN}"))
                .header("If-Match", "W/\"0\"")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let _body = response.into_body().collect().await.unwrap().to_bytes();

    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(
        storage
            .get_blob_bytes(entry.sha256)
            .await
            .unwrap()
            .is_none()
    );
    assert!(!cache_path.exists());
}

async fn seed_registry(
    storage: &Arc<cc_lb_storage_sqlite::SqliteStorage>,
) -> cc_lb_storage_api::WasmRegistryEntry {
    let (entry, _) = storage
        .persist_wasm_upload(
            WasmBlob {
                sha256: [6; 32],
                bytes: vec![6; 6],
                size_bytes: 6,
                parse_validated_at_unix_secs: 1_800_000_000,
            },
            WasmRegistryEntryInput {
                schema_hash: None,
                name: "plugin-delete-registry".to_owned(),
                version: None,
                original_filename: "plugin-delete-registry.wasm".to_owned(),
                label: None,
                uploaded_at_unix_secs: 1_800_000_000,
                uploaded_by_admin_id: Uuid::from_u128(6),
                description: "plugin-delete-registry description".to_owned(),
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
    entry
}

fn hex_sha256(sha256: [u8; 32]) -> String {
    sha256.iter().map(|byte| format!("{byte:02x}")).collect()
}
