use std::sync::Arc;

use cc_lb_server::app::backfill_wire_version;
use cc_lb_storage_api::{
    BackendKind, MetaStore, PluginRegistryStore, WasmBlob, WasmRegistryEntry,
    WasmRegistryEntryInput,
};
use cc_lb_storage_sqlite::SqliteStorage as Storage;
use serde_json::json;
use tempfile::TempDir;
use uuid::Uuid;

const ENVELOPE_VERSION_V1: u32 = 1;

#[tokio::test]
async fn backfill_updates_legacy_entry_from_handshake_chosen_wire_version() {
    let (_dir, storage) = open_storage().await;
    let entry = seed_filter_plugin(&storage, "legacy-filter", 1).await;
    assert_eq!(entry.wire_version, 1);

    backfill_wire_version(storage.as_ref()).await;

    let after = storage
        .get_registry_entry_by_id(entry.id)
        .await
        .unwrap()
        .expect("registry entry exists");
    assert_eq!(after.wire_version, 3);
}

#[tokio::test]
async fn backfill_skips_entries_with_non_default_wire_version() {
    let (_dir, storage) = open_storage().await;
    let entry = seed_filter_plugin(&storage, "already-negotiated-filter", 2).await;

    backfill_wire_version(storage.as_ref()).await;

    let after = storage
        .get_registry_entry_by_id(entry.id)
        .await
        .unwrap()
        .expect("registry entry exists");
    assert_eq!(after.wire_version, 2);
}

async fn open_storage() -> (TempDir, Arc<Storage>) {
    let dir = tempfile::tempdir().unwrap();
    let database_url = format!("sqlite://{}", dir.path().join("backfill.sqlite").display());
    let storage = Arc::new(
        cc_lb_storage_sqlite::open_sqlite(
            &database_url,
            Arc::new(cc_lb_core::SystemClock),
        )
            .await
            .unwrap(),
    );
    storage.initialize(BackendKind::Sqlite).await.unwrap();
    (dir, storage)
}

async fn seed_filter_plugin(
    storage: &Arc<Storage>,
    name: &str,
    wire_version: u8,
) -> WasmRegistryEntry {
    let bytes = handshake_filter_wasm();
    let sha256 = sha256_of(&bytes);
    let (entry, _) = storage
        .persist_wasm_upload(
            WasmBlob {
                sha256,
                size_bytes: bytes.len() as u64,
                bytes,
                parse_validated_at_unix_secs: 1_800_000_000,
            },
            WasmRegistryEntryInput {
                name: name.to_owned(),
                original_filename: format!("{name}.wasm"),
                label: None,
                uploaded_at_unix_secs: 1_800_000_000,
                uploaded_by_admin_id: Uuid::new_v4(),
                wire_version,
                supported_slots: Vec::new(),
            },
        )
        .await
        .unwrap();
    entry
}

fn handshake_filter_wasm() -> Vec<u8> {
    let accept = json!({
        "handshake_schema_version": cc_lb_plugin_wire::handshake::HANDSHAKE_SCHEMA_VERSION_V1,
        "envelope_version": ENVELOPE_VERSION_V1,
        "chosen_versions": { "filter": 3 },
        "plugin_supported": { "filter": [1, 2, 3] },
        "implemented_functions": ["filter"],
        "required_capabilities": [],
    })
    .to_string();

    handshake_module(&accept, &["filter"])
}

fn handshake_module(output: &str, extra_exports: &[&str]) -> Vec<u8> {
    let output_helper = bytes_helper("handshake_out", output.as_bytes());
    let mut exports = String::new();
    for export in extra_exports {
        exports.push_str(&format!(
            r#"
(func (export "{export}") (result i32)
  (i32.const 0))
"#
        ));
    }

    let wat = format!(
        r#"
(module
  (import "extism:host/env" "alloc" (func $alloc (param i64) (result i64)))
  (import "extism:host/env" "store_u8" (func $store_u8 (param i64 i32)))
  (import "extism:host/env" "output_set" (func $output_set (param i64 i64)))
  {output_helper}
  (func (export "cc_lb_handshake") (result i32)
    (call $output_set (call $handshake_out) (i64.const {len}))
    (i32.const 0))
  {exports}
)
"#,
        len = output.len()
    );
    wat::parse_str(&wat).expect("handshake wat parses")
}

fn bytes_helper(name: &str, bytes: &[u8]) -> String {
    let mut stores = String::new();
    for (index, byte) in bytes.iter().enumerate() {
        stores.push_str(&format!(
            "  (call $store_u8 (i64.add (local.get $ptr) (i64.const {index})) (i32.const {byte}))\n"
        ));
    }
    format!(
        r#"
(func ${name} (result i64)
  (local $ptr i64)
  (local.set $ptr (call $alloc (i64.const {len})))
{stores}  (local.get $ptr))
"#,
        len = bytes.len()
    )
}

fn sha256_of(bytes: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes).into()
}
