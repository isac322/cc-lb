use std::path::Path;
use std::process::{Command, Output};

use cc_lb_storage_api::{
    BackendKind, MetaStore, PluginChainEntry, PluginChainEntryInput, PluginRegistryStore,
    PluginSlot, WasmBlob, WasmRegistryEntry, WasmRegistryEntryInput,
    principal::{Limit, LimitKind, PrincipalCreate, PrincipalKind, PrincipalStore},
};
use cc_lb_storage_sqlite::SqliteStorage;
use serde_json::{Value, json};
use tempfile::TempDir;
use uuid::Uuid;

#[tokio::test]
async fn list_abandoned_chain_entries_empty_storage_outputs_empty_report() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("storage.sqlite");
    let storage = open_sqlite_storage(&path).await?;
    drop(storage);

    let output = run_doctor(&path)?;

    assert_success_json(output, json!({ "abandoned_chain_entries": [] }));
    Ok(())
}

#[tokio::test]
async fn list_abandoned_chain_entries_reports_empty_supported_slots() -> anyhow::Result<()> {
    let (dir, storage) = open_storage().await?;
    let principal_id = create_principal(&storage, "doctor-abandoned-principal").await?;
    let registry_entry =
        upload_plugin(&storage, "doctor-abandoned-plugin", [11; 32], Vec::new()).await?;
    let chain_entry = insert_router_chain(&storage, principal_id, registry_entry.id, 300).await?;
    let path = storage_path(&dir);
    drop(storage);

    let output = run_doctor(&path)?;

    assert_success_json(
        output,
        json!({
            "abandoned_chain_entries": [{
                "principal_id": principal_id.to_string(),
                "wasm_registry_id": registry_entry.id.to_string(),
                "wasm_registry_name": registry_entry.name,
                "slot": "router",
                "order": chain_entry.order,
            }]
        }),
    );
    Ok(())
}

#[tokio::test]
async fn list_abandoned_chain_entries_omits_healthy_chain_entries() -> anyhow::Result<()> {
    let (dir, storage) = open_storage().await?;
    let principal_id = create_principal(&storage, "doctor-healthy-principal").await?;
    let registry_entry = upload_plugin(
        &storage,
        "doctor-healthy-plugin",
        [12; 32],
        vec![PluginSlot::Router],
    )
    .await?;
    insert_router_chain(&storage, principal_id, registry_entry.id, 500).await?;
    let path = storage_path(&dir);
    drop(storage);

    let output = run_doctor(&path)?;

    assert_success_json(output, json!({ "abandoned_chain_entries": [] }));
    Ok(())
}

async fn open_storage() -> anyhow::Result<(TempDir, SqliteStorage)> {
    let dir = tempfile::tempdir()?;
    let path = storage_path(&dir);
    let storage = open_sqlite_storage(&path).await?;
    Ok((dir, storage))
}

fn storage_path(dir: &TempDir) -> std::path::PathBuf {
    dir.path().join("storage.sqlite")
}

async fn open_sqlite_storage(path: &Path) -> anyhow::Result<SqliteStorage> {
    let database_url = format!("sqlite://{}", path.display());
    let storage = cc_lb_storage_sqlite::open_sqlite(
        &database_url,
        std::sync::Arc::new(cc_lb_core::SystemClock),
    )
    .await?;
    storage.initialize(BackendKind::Sqlite).await?;
    Ok(storage)
}

async fn create_principal(storage: &SqliteStorage, name: &str) -> anyhow::Result<Uuid> {
    let principal = storage
        .create(
            PrincipalCreate {
                name: name.to_owned(),
                kind: PrincipalKind::Human,
                allowed_models: Vec::new(),
                allowed_upstreams: Vec::new(),
                default_limits: vec![Limit {
                    kind: LimitKind::Requests,
                    window_secs: 60,
                    cap_micros: 1_000_000,
                }],
            },
            1_800_000_000,
        )
        .await?;
    Ok(principal.id)
}

async fn upload_plugin(
    storage: &SqliteStorage,
    name: &str,
    sha256: [u8; 32],
    supported_slots: Vec<PluginSlot>,
) -> anyhow::Result<WasmRegistryEntry> {
    let bytes = format!("{name}-bytes").into_bytes();
    let (entry, existed) = storage
        .persist_wasm_upload(
            WasmBlob {
                sha256,
                size_bytes: bytes.len() as u64,
                bytes,
                parse_validated_at_unix_secs: 1_800_000_000,
            },
            WasmRegistryEntryInput {
                schema_hash: None,
                name: name.to_owned(),
                original_filename: format!("{name}.wasm"),
                label: None,
                uploaded_at_unix_secs: 1_800_000_000,
                uploaded_by_admin_id: Uuid::new_v4(),
                wire_version: 1,
                supported_slots,
            },
        )
        .await?;
    assert!(!existed);
    Ok(entry)
}

async fn insert_router_chain(
    storage: &SqliteStorage,
    principal_id: Uuid,
    wasm_registry_id: Uuid,
    order: i64,
) -> anyhow::Result<PluginChainEntry> {
    let entry = storage
        .insert_chain_entry(PluginChainEntryInput {
            principal_id,
            slot: PluginSlot::Router,
            order,
            wasm_registry_id,
            config: json!({}),
            sse_per_event: false,
            batched_events_per_flush: 100,
            batched_flush_ms: 1_000,
            wire_version: None,
        })
        .await?;
    Ok(entry)
}

fn run_doctor(storage_path: &Path) -> anyhow::Result<Output> {
    Ok(Command::new(env!("CARGO_BIN_EXE_cc-lb"))
        .args(["doctor", "list-abandoned-chain-entries"])
        .env("CC_LB_STORAGE_PATH", storage_path)
        .output()?)
}

fn assert_success_json(output: Output, expected: Value) {
    assert!(
        output.status.success(),
        "doctor exited with status {:?}\nstdout: {}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "stderr should be empty: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let actual: Value = serde_json::from_slice(&output.stdout).expect("stdout is JSON");
    assert_eq!(actual, expected);
}
