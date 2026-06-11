use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use cc_lb_plugin_wire::handshake::{HANDSHAKE_SCHEMA_VERSION_V1, HandshakeAccept, HandshakeOffer};
use cc_lb_plugin_wire::identity::{CC_LB_PLUGIN_MAGIC, CC_LB_PLUGIN_SECTION_NAME};
use cc_lb_plugin_wire::self_check::{SelfCheckResponse, SelfCheckStatus};
use cc_lb_runtime_extism::handshake::{HandshakeExecutionError, build_offer};
use cc_lb_runtime_extism::registry::{PluginRegistry, RegistryLifecycle};
use cc_lb_runtime_extism::self_check::SelfCheckExecutionError;
use cc_lb_server::startup_handshake::bridge_legacy_wasm_registry;
use cc_lb_storage_api::{PluginRegistryStore, WasmBlob, WasmRegistryEntryInput};
use cc_lb_storage_redb::{RedbPluginBlobRepo, RedbPluginRegistryRepo, Storage as RedbStorage};
use serde_json::json;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use uuid::Uuid;

#[tokio::test]
async fn bridge_registers_legacy_uploads_missing_from_plugin_registry() {
    let (dir, storage, plugin_registry, lifecycle) = setup();
    let wasm = plugin_wasm("legacy-shape", "0.1.0");
    let sha = sha256(&wasm);
    seed_legacy_upload(&storage, &wasm, "legacy-shape").await;

    let report = bridge_legacy_wasm_registry(&plugin_registry, storage.as_ref()).await;

    assert_eq!(report.scanned, 1);
    assert_eq!(report.bridged, 1);
    assert_eq!(report.already_present, 0);
    assert_eq!(report.orphan, 0);
    assert!(report.failed.is_empty());
    assert_eq!(lifecycle.handshake_count(), 1);
    let record = plugin_registry
        .get_by_sha256(&sha)
        .await
        .expect("registry query succeeds")
        .expect("bridged record is present");
    assert_eq!(record.plugin_name, "legacy-shape");
    assert_eq!(record.plugin_version, "0.1.0");
    drop(dir);
}

#[tokio::test]
async fn second_bridge_call_is_idempotent_no_extra_handshake() {
    let (dir, storage, plugin_registry, lifecycle) = setup();
    let wasm = plugin_wasm("legacy-shape", "0.1.0");
    seed_legacy_upload(&storage, &wasm, "legacy-shape").await;
    bridge_legacy_wasm_registry(&plugin_registry, storage.as_ref()).await;
    assert_eq!(lifecycle.handshake_count(), 1);

    let report = bridge_legacy_wasm_registry(&plugin_registry, storage.as_ref()).await;

    assert_eq!(report.scanned, 1);
    assert_eq!(report.bridged, 0);
    assert_eq!(report.already_present, 1);
    assert_eq!(lifecycle.handshake_count(), 1);
    drop(dir);
}

#[tokio::test]
async fn empty_legacy_registry_produces_empty_report() {
    let (dir, storage, plugin_registry, lifecycle) = setup();

    let report = bridge_legacy_wasm_registry(&plugin_registry, storage.as_ref()).await;

    assert_eq!(report.scanned, 0);
    assert_eq!(report.bridged, 0);
    assert_eq!(lifecycle.handshake_count(), 0);
    drop(dir);
}

fn setup() -> (
    TempDir,
    Arc<dyn PluginRegistryStore>,
    PluginRegistry,
    Arc<CountingLifecycle>,
) {
    let dir = TempDir::new().expect("tempdir");
    let storage =
        RedbStorage::open(&dir.path().join("legacy-bridge.redb"), [9; 32]).expect("redb opens");
    let registry_repo =
        Arc::new(RedbPluginRegistryRepo::new(storage.clone()).expect("registry repo"));
    let blob_repo = Arc::new(RedbPluginBlobRepo::new(storage.clone()).expect("blob repo"));
    let lifecycle = Arc::new(CountingLifecycle::default());
    let registry = PluginRegistry::new_with_lifecycle(
        registry_repo,
        blob_repo,
        build_offer(&BTreeSet::new()),
        lifecycle.clone(),
    )
    .expect("plugin registry builds");
    let storage_dyn: Arc<dyn PluginRegistryStore> = Arc::new(storage);
    (dir, storage_dyn, registry, lifecycle)
}

async fn seed_legacy_upload(storage: &Arc<dyn PluginRegistryStore>, wasm: &[u8], name: &str) {
    let sha = sha256(wasm);
    let size = wasm.len() as u64;
    storage
        .persist_wasm_upload(
            WasmBlob {
                sha256: sha,
                bytes: wasm.to_vec(),
                size_bytes: size,
                parse_validated_at_unix_secs: 1_800_000_000,
            },
            WasmRegistryEntryInput {
                name: name.to_owned(),
                original_filename: format!("{name}.wasm"),
                label: None,
                uploaded_at_unix_secs: 1_800_000_000,
                uploaded_by_admin_id: Uuid::new_v4(),
            },
        )
        .await
        .expect("legacy upload persists");
}

#[derive(Default)]
struct CountingLifecycle {
    handshakes: AtomicUsize,
    self_checks: AtomicUsize,
}

impl CountingLifecycle {
    fn handshake_count(&self) -> usize {
        self.handshakes.load(Ordering::SeqCst)
    }
}

impl RegistryLifecycle for CountingLifecycle {
    fn execute_handshake(
        &self,
        _plugin_bytes: &[u8],
        offer: &HandshakeOffer,
    ) -> Result<HandshakeAccept, HandshakeExecutionError> {
        self.handshakes.fetch_add(1, Ordering::SeqCst);
        let accept = HandshakeAccept {
            handshake_schema_version: HANDSHAKE_SCHEMA_VERSION_V1,
            envelope_version: 1,
            chosen_versions: BTreeMap::from([("shape".to_owned(), 1)]),
            plugin_supported: BTreeMap::from([("shape".to_owned(), vec![1])]),
            implemented_functions: BTreeSet::from(["shape".to_owned()]),
            required_capabilities: BTreeSet::new(),
        };
        accept.validate_against_offer(offer)?;
        Ok(accept)
    }

    fn execute_self_check(
        &self,
        _plugin_bytes: &[u8],
    ) -> Result<SelfCheckResponse, SelfCheckExecutionError> {
        self.self_checks.fetch_add(1, Ordering::SeqCst);
        Ok(SelfCheckResponse {
            status: SelfCheckStatus::Success,
            failures: Vec::new(),
            completed_at: 1,
        })
    }
}

fn plugin_wasm(plugin_name: &str, plugin_version: &str) -> Vec<u8> {
    let payload = json!({
        "magic": CC_LB_PLUGIN_MAGIC,
        "abi_envelope": 1,
        "plugin_name": plugin_name,
        "plugin_version": plugin_version,
    })
    .to_string();
    wasm_with_custom_section(CC_LB_PLUGIN_SECTION_NAME, payload.as_bytes())
}

fn wasm_with_custom_section(name: &str, data: &[u8]) -> Vec<u8> {
    let mut wasm = Vec::from([0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]);
    wasm.push(0);
    let mut payload = Vec::new();
    encode_u32(name.len() as u32, &mut payload);
    payload.extend_from_slice(name.as_bytes());
    payload.extend_from_slice(data);
    encode_u32(payload.len() as u32, &mut wasm);
    wasm.extend_from_slice(&payload);
    wasm
}

fn encode_u32(mut value: u32, output: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        output.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}
