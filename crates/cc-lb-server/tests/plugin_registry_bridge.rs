use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use cc_lb_plugin_wire::augmented_metadata::AugmentedMetadata;
use cc_lb_plugin_wire::handshake::{HANDSHAKE_SCHEMA_VERSION_V1, HandshakeAccept, HandshakeOffer};
use cc_lb_plugin_wire::identity::{CC_LB_PLUGIN_MAGIC, CC_LB_PLUGIN_SECTION_NAME, PluginIdentity};
use cc_lb_plugin_wire::self_check::{SelfCheckResponse, SelfCheckStatus};
use cc_lb_runtime_extism::handshake::{HandshakeExecutionError, build_offer};
use cc_lb_runtime_extism::registry::{PluginRegistry, RegistryLifecycle};
use cc_lb_runtime_extism::self_check::SelfCheckExecutionError;
use cc_lb_server::dynamic_view_builder::bridged_metadata;
use cc_lb_storage_api::{
    PluginBlobRepo, PluginRegistryRecord, PluginRegistryRepo, PluginRegistryStatus, RepoError,
};
use parking_lot::Mutex;
use serde_json::json;
use tokio::sync::Mutex as TokioMutex;

struct StubRepo {
    records: Mutex<BTreeMap<[u8; 32], PluginRegistryRecord>>,
}

impl StubRepo {
    fn new() -> Self {
        Self {
            records: Mutex::new(BTreeMap::new()),
        }
    }

    fn insert(&self, record: PluginRegistryRecord) {
        self.records.lock().insert(record.sha256, record);
    }
}

#[async_trait]
impl PluginRegistryRepo for StubRepo {
    async fn upsert_record(&self, record: &PluginRegistryRecord) -> Result<(), RepoError> {
        self.records.lock().insert(record.sha256, record.clone());
        Ok(())
    }
    async fn get_by_sha256(
        &self,
        sha256: &[u8; 32],
    ) -> Result<Option<PluginRegistryRecord>, RepoError> {
        Ok(self.records.lock().get(sha256).cloned())
    }
    async fn list_active(&self) -> Result<Vec<PluginRegistryRecord>, RepoError> {
        Ok(self.records.lock().values().cloned().collect())
    }
    async fn set_status(
        &self,
        _sha256: &[u8; 32],
        _status: PluginRegistryStatus,
    ) -> Result<(), RepoError> {
        Ok(())
    }
    async fn delete_by_sha256(&self, _sha256: &[u8; 32]) -> Result<(), RepoError> {
        Ok(())
    }
    async fn count(&self) -> Result<usize, RepoError> {
        Ok(self.records.lock().len())
    }
    async fn get_shutdown_marker(&self) -> Result<Option<i64>, RepoError> {
        Ok(None)
    }
    async fn set_shutdown_marker(&self, _unix_secs: i64) -> Result<(), RepoError> {
        Ok(())
    }
    async fn clear_shutdown_marker(&self) -> Result<(), RepoError> {
        Ok(())
    }
}

fn sample_record(sha256: [u8; 32], plugin_version: &str) -> PluginRegistryRecord {
    PluginRegistryRecord {
        sha256,
        plugin_name: "round-robin".to_owned(),
        plugin_version: plugin_version.to_owned(),
        abi_envelope: 1,
        augmented_metadata: AugmentedMetadata {
            identity: PluginIdentity {
                magic: CC_LB_PLUGIN_MAGIC,
                abi_envelope: 1,
                plugin_name: "round-robin".to_owned(),
                plugin_version: plugin_version.to_owned(),
            },
            negotiated_functions: BTreeMap::from([("route".to_owned(), 1_u32)]),
            negotiated_capabilities: BTreeSet::new(),
            handshake_completed_at: 100,
            self_check_passed: true,
            self_check_completed_at: 100,
            expires_at: 200,
        },
        host_offer_hash: [9_u8; 32],
        handshake_schema_version: 1,
        last_handshake_at: 100,
        status: PluginRegistryStatus::Active,
    }
}

#[tokio::test]
async fn bridge_injects_augmented_metadata_when_record_present() {
    let sha = [1_u8; 32];
    let stub = Arc::new(StubRepo::new());
    stub.insert(sample_record(sha, "0.1.0"));
    let repo: Arc<dyn PluginRegistryRepo> = stub;

    let metadata = bridged_metadata(Some(&repo), sha).await;

    let value = metadata
        .get("augmented_metadata")
        .expect("bridge must inject augmented_metadata when PluginRegistryRecord exists");
    let parsed: AugmentedMetadata =
        serde_json::from_value(value.clone()).expect("round-trip serde");
    assert_eq!(parsed.identity.plugin_version, "0.1.0");
    assert_ne!(
        parsed.identity.plugin_version, "legacy",
        "bridge must NOT produce the legacy_dispatch_metadata sentinel",
    );
    assert_eq!(parsed.negotiated_functions.get("route").copied(), Some(1));
}

#[tokio::test]
async fn bridge_falls_back_when_repo_absent() {
    let metadata = bridged_metadata(None, [2_u8; 32]).await;
    assert!(
        metadata.is_empty(),
        "no repo MUST yield empty metadata so PluginSlot falls back to legacy_dispatch_metadata",
    );
}

#[tokio::test]
async fn bridge_falls_back_when_record_missing() {
    let repo: Arc<dyn PluginRegistryRepo> = Arc::new(StubRepo::new());
    let metadata = bridged_metadata(Some(&repo), [3_u8; 32]).await;
    assert!(
        metadata.is_empty(),
        "missing PluginRegistryRecord MUST yield empty metadata so PluginSlot falls back to legacy_dispatch_metadata",
    );
}

#[tokio::test]
async fn end_to_end_register_then_bridge_round_trip() {
    let registry_repo: Arc<MemoryRegistryRepo> = Arc::new(MemoryRegistryRepo::default());
    let blob_repo: Arc<MemoryBlobRepo> = Arc::new(MemoryBlobRepo::default());
    let registry_dyn: Arc<dyn PluginRegistryRepo> = registry_repo.clone();
    let blob_dyn: Arc<dyn PluginBlobRepo> = blob_repo.clone();
    let registry = PluginRegistry::new_with_lifecycle(
        registry_dyn.clone(),
        blob_dyn,
        build_offer(&BTreeSet::new()),
        Arc::new(StubLifecycle),
    )
    .expect("registry builds");

    let wasm = plugin_wasm("e2e-bridge-plugin", "9.9.9");
    let record = registry
        .register_plugin(&wasm)
        .await
        .expect("real PluginRegistry::register_plugin succeeds");
    assert_eq!(record.plugin_version, "9.9.9");

    let bridged = bridged_metadata(Some(&registry_dyn), record.sha256).await;
    let value = bridged.get("augmented_metadata").expect(
        "bridge MUST inject augmented_metadata after PluginRegistry::register_plugin populates the repo",
    );

    let parsed: AugmentedMetadata = serde_json::from_value(value.clone())
        .expect("bridge value MUST round-trip through serde to AugmentedMetadata");
    assert_eq!(parsed.identity.plugin_version, "9.9.9");
    assert_ne!(
        parsed.identity.plugin_version, "legacy",
        "bridge MUST carry the handshake-derived version, not the legacy sentinel",
    );

    let cached = registry
        .get_metadata(&record.sha256)
        .expect("PluginRegistry::get_metadata is populated");
    assert_eq!(
        parsed, *cached,
        "bridge metadata MUST equal the in-process cached AugmentedMetadata",
    );

    let mut manifest_metadata: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    manifest_metadata.extend(bridged);
    let injected = manifest_metadata
        .get("augmented_metadata")
        .expect("PluginManifest.metadata receives the bridged key");
    let manifest_round_trip: AugmentedMetadata =
        serde_json::from_value(injected.clone()).expect("PluginManifest-style round-trip succeeds");
    assert_eq!(manifest_round_trip, *cached);
}

struct StubLifecycle;

impl RegistryLifecycle for StubLifecycle {
    fn execute_handshake(
        &self,
        _plugin_bytes: &[u8],
        offer: &HandshakeOffer,
    ) -> Result<HandshakeAccept, HandshakeExecutionError> {
        let accept = HandshakeAccept {
            handshake_schema_version: HANDSHAKE_SCHEMA_VERSION_V1,
            envelope_version: 1,
            chosen_versions: BTreeMap::from([("route".to_owned(), 1)]),
            plugin_supported: BTreeMap::from([("route".to_owned(), vec![1])]),
            implemented_functions: BTreeSet::from(["route".to_owned()]),
            required_capabilities: BTreeSet::new(),
        };
        accept.validate_against_offer(offer)?;
        Ok(accept)
    }

    fn execute_self_check(
        &self,
        _plugin_bytes: &[u8],
    ) -> Result<SelfCheckResponse, SelfCheckExecutionError> {
        Ok(SelfCheckResponse {
            status: SelfCheckStatus::Success,
            failures: Vec::new(),
            completed_at: 1,
        })
    }
}

#[derive(Default)]
struct MemoryRegistryRepo {
    records: TokioMutex<BTreeMap<[u8; 32], PluginRegistryRecord>>,
}

#[async_trait]
impl PluginRegistryRepo for MemoryRegistryRepo {
    async fn upsert_record(&self, record: &PluginRegistryRecord) -> Result<(), RepoError> {
        self.records
            .lock()
            .await
            .insert(record.sha256, record.clone());
        Ok(())
    }
    async fn get_by_sha256(
        &self,
        sha256: &[u8; 32],
    ) -> Result<Option<PluginRegistryRecord>, RepoError> {
        Ok(self.records.lock().await.get(sha256).cloned())
    }
    async fn list_active(&self) -> Result<Vec<PluginRegistryRecord>, RepoError> {
        Ok(self
            .records
            .lock()
            .await
            .values()
            .filter(|r| r.status == PluginRegistryStatus::Active)
            .cloned()
            .collect())
    }
    async fn set_status(
        &self,
        sha256: &[u8; 32],
        status: PluginRegistryStatus,
    ) -> Result<(), RepoError> {
        if let Some(record) = self.records.lock().await.get_mut(sha256) {
            record.status = status;
        }
        Ok(())
    }
    async fn delete_by_sha256(&self, sha256: &[u8; 32]) -> Result<(), RepoError> {
        self.records.lock().await.remove(sha256);
        Ok(())
    }
    async fn count(&self) -> Result<usize, RepoError> {
        Ok(self.records.lock().await.len())
    }
    async fn get_shutdown_marker(&self) -> Result<Option<i64>, RepoError> {
        Ok(None)
    }
    async fn set_shutdown_marker(&self, _unix_secs: i64) -> Result<(), RepoError> {
        Ok(())
    }
    async fn clear_shutdown_marker(&self) -> Result<(), RepoError> {
        Ok(())
    }
}

#[derive(Default)]
struct MemoryBlobRepo {
    blobs: TokioMutex<BTreeMap<[u8; 32], Vec<u8>>>,
}

#[async_trait]
impl PluginBlobRepo for MemoryBlobRepo {
    async fn put_blob(&self, sha256: &[u8; 32], bytes: &[u8]) -> Result<(), RepoError> {
        self.blobs.lock().await.insert(*sha256, bytes.to_vec());
        Ok(())
    }
    async fn get_blob(&self, sha256: &[u8; 32]) -> Result<Option<Vec<u8>>, RepoError> {
        Ok(self.blobs.lock().await.get(sha256).cloned())
    }
    async fn delete_blob(&self, sha256: &[u8; 32]) -> Result<(), RepoError> {
        self.blobs.lock().await.remove(sha256);
        Ok(())
    }
    async fn list_blob_keys(&self) -> Result<Vec<[u8; 32]>, RepoError> {
        Ok(self.blobs.lock().await.keys().copied().collect())
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
