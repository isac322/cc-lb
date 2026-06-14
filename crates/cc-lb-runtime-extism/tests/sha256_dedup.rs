use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use cc_lb_plugin_wire::handshake::{HANDSHAKE_SCHEMA_VERSION_V1, HandshakeAccept, HandshakeOffer};
use cc_lb_plugin_wire::identity::{CC_LB_PLUGIN_MAGIC, CC_LB_PLUGIN_SECTION_NAME};
use cc_lb_plugin_wire::self_check::{SelfCheckResponse, SelfCheckStatus};
use cc_lb_runtime_extism::handshake::HandshakeExecutionError;
use cc_lb_runtime_extism::registry::{PluginRegistry, RegistryLifecycle};
use cc_lb_runtime_extism::self_check::SelfCheckExecutionError;
use cc_lb_storage_api::{
    PluginBlobRepo, PluginRegistryRecord, PluginRegistryRepo, PluginRegistryStatus, RepoError,
};
use ring::digest::{SHA256, digest};
use serde_json::json;
use tokio::sync::Mutex;

#[tokio::test]
async fn same_wasm_bytes_registered_to_ten_slots_share_one_metadata_arc() {
    let repos = Repos::default();
    let lifecycle = Arc::new(CountingLifecycle::default());
    let registry = registry(&repos, lifecycle.clone());
    let wasm = plugin_wasm("dedup-plugin", "1.0.0");
    let sha = sha256(&wasm);
    let mut slot_metadata = Vec::new();

    for slot_index in 0..10 {
        registry
            .register_plugin(&wasm)
            .await
            .unwrap_or_else(|error| panic!("slot {slot_index} registration succeeds: {error}"));
        slot_metadata.push(
            registry
                .get_metadata(&sha)
                .unwrap_or_else(|| panic!("slot {slot_index} metadata is cached")),
        );
    }

    assert_eq!(lifecycle.handshake_count(), 1);
    assert_eq!(slot_metadata.len(), 10);
    for metadata in &slot_metadata[1..] {
        assert!(Arc::ptr_eq(&slot_metadata[0], metadata));
    }
}

#[tokio::test]
async fn different_wasm_bytes_execute_independent_handshakes() {
    let repos = Repos::default();
    let lifecycle = Arc::new(CountingLifecycle::default());
    let registry = registry(&repos, lifecycle.clone());
    let mut metadata = Vec::new();

    for index in 0..3 {
        let wasm = plugin_wasm(&format!("dedup-{index}"), "1.0.0");
        let sha = sha256(&wasm);
        registry
            .register_plugin(&wasm)
            .await
            .unwrap_or_else(|error| panic!("plugin {index} registration succeeds: {error}"));
        metadata.push(
            registry
                .get_metadata(&sha)
                .unwrap_or_else(|| panic!("plugin {index} metadata is cached")),
        );
    }

    assert_eq!(lifecycle.handshake_count(), 3);
    assert!(!Arc::ptr_eq(&metadata[0], &metadata[1]));
    assert!(!Arc::ptr_eq(&metadata[0], &metadata[2]));
    assert!(!Arc::ptr_eq(&metadata[1], &metadata[2]));
}

#[tokio::test]
async fn startup_load_populates_cache_without_new_handshakes() {
    let repos = Repos::default();
    let seed_lifecycle = Arc::new(CountingLifecycle::default());
    let seed_registry = registry(&repos, seed_lifecycle.clone());
    let wasm = plugin_wasm("startup-dedup", "1.0.0");
    let sha = sha256(&wasm);
    seed_registry
        .register_plugin(&wasm)
        .await
        .expect("seed registration succeeds");
    let seed_metadata = seed_registry
        .get_metadata(&sha)
        .expect("seed metadata is cached");
    assert_eq!(seed_lifecycle.handshake_count(), 1);

    let startup_lifecycle = Arc::new(CountingLifecycle::default());
    let startup_registry = registry(&repos, startup_lifecycle.clone());

    let loaded = startup_registry
        .load_from_db_at_startup()
        .await
        .expect("startup load succeeds");

    assert_eq!(loaded, 1);
    assert_eq!(startup_lifecycle.handshake_count(), 0);
    assert_eq!(startup_lifecycle.self_check_count(), 0);
    let startup_metadata = startup_registry
        .get_metadata(&sha)
        .expect("startup metadata is cached");
    assert_eq!(*startup_metadata, *seed_metadata);
}

fn registry(repos: &Repos, lifecycle: Arc<CountingLifecycle>) -> PluginRegistry {
    PluginRegistry::new_with_lifecycle(
        repos.registry.clone(),
        repos.blobs.clone(),
        cc_lb_runtime_extism::handshake::build_offer(&BTreeSet::new()),
        lifecycle,
    )
    .expect("registry builds")
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

    fn self_check_count(&self) -> usize {
        self.self_checks.load(Ordering::SeqCst)
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
        _supported_slots: &[cc_lb_storage_api::PluginSlot],
    ) -> Result<SelfCheckResponse, SelfCheckExecutionError> {
        self.self_checks.fetch_add(1, Ordering::SeqCst);
        Ok(SelfCheckResponse {
            status: SelfCheckStatus::Success,
            failures: Vec::new(),
            completed_at: 1,
        })
    }
}

#[derive(Default)]
struct Repos {
    registry: Arc<MemoryRegistryRepo>,
    blobs: Arc<MemoryBlobRepo>,
}

#[derive(Default)]
struct MemoryRegistryRepo {
    records: Mutex<BTreeMap<[u8; 32], PluginRegistryRecord>>,
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
            .filter(|record| record.status == PluginRegistryStatus::Active)
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
    blobs: Mutex<BTreeMap<[u8; 32], Vec<u8>>>,
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

fn sha256(bytes: &[u8]) -> [u8; 32] {
    digest(&SHA256, bytes)
        .as_ref()
        .try_into()
        .expect("sha256 is 32 bytes")
}
