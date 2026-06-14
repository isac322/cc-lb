use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use cc_lb_plugin_wire::augmented_metadata::{AugmentedMetadata, AugmentedMetadataError};
use cc_lb_plugin_wire::handshake::{CanonicalError, HandshakeAccept, HandshakeOffer};
use cc_lb_plugin_wire::limits::SKIP_HANDSHAKE_IF_FRESH_TTL_SECS;
use cc_lb_plugin_wire::self_check::SelfCheckResponse;
use cc_lb_storage_api::{
    PluginBlobRepo, PluginRegistryRecord, PluginRegistryRepo, PluginRegistryStatus, PluginSlot,
    RepoError,
};
use dashmap::DashMap;
use ring::digest::{SHA256, digest};
use thiserror::Error;

use cc_lb_runtime_protocol::handshake::{HandshakeExecutionError, execute_handshake, slot_set_from_handshake};
use cc_lb_runtime_protocol::identity::{IdentityReadError, read_identity};
use cc_lb_runtime_protocol::self_check::{SelfCheckExecutionError, execute_self_check};

pub struct PluginRegistry {
    registry_repo: Arc<dyn PluginRegistryRepo>,
    blob_repo: Arc<dyn PluginBlobRepo>,
    in_process_cache: Arc<DashMap<[u8; 32], Arc<AugmentedMetadata>>>,
    host_offer: HandshakeOffer,
    host_offer_hash: [u8; 32],
    lifecycle: Arc<dyn RegistryLifecycle>,
}

#[doc(hidden)]
pub trait RegistryLifecycle: Send + Sync {
    fn execute_handshake(
        &self,
        plugin_bytes: &[u8],
        offer: &HandshakeOffer,
    ) -> Result<HandshakeAccept, HandshakeExecutionError>;

    fn execute_self_check(
        &self,
        plugin_bytes: &[u8],
        supported_slots: &[PluginSlot],
    ) -> Result<SelfCheckResponse, SelfCheckExecutionError>;
}

struct ExtismRegistryLifecycle;

impl RegistryLifecycle for ExtismRegistryLifecycle {
    fn execute_handshake(
        &self,
        plugin_bytes: &[u8],
        offer: &HandshakeOffer,
    ) -> Result<HandshakeAccept, HandshakeExecutionError> {
        execute_handshake(plugin_bytes, offer)
    }

    fn execute_self_check(
        &self,
        plugin_bytes: &[u8],
        supported_slots: &[PluginSlot],
    ) -> Result<SelfCheckResponse, SelfCheckExecutionError> {
        execute_self_check(plugin_bytes, supported_slots)
    }
}

impl Clone for PluginRegistry {
    fn clone(&self) -> Self {
        Self {
            registry_repo: self.registry_repo.clone(),
            blob_repo: self.blob_repo.clone(),
            in_process_cache: self.in_process_cache.clone(),
            host_offer: self.host_offer.clone(),
            host_offer_hash: self.host_offer_hash,
            lifecycle: self.lifecycle.clone(),
        }
    }
}

impl PluginRegistry {
    pub fn new(
        registry_repo: Arc<dyn PluginRegistryRepo>,
        blob_repo: Arc<dyn PluginBlobRepo>,
        host_offer: HandshakeOffer,
    ) -> Result<Self, RegistryError> {
        Self::new_with_lifecycle(
            registry_repo,
            blob_repo,
            host_offer,
            Arc::new(ExtismRegistryLifecycle),
        )
    }

    #[doc(hidden)]
    pub fn new_with_lifecycle(
        registry_repo: Arc<dyn PluginRegistryRepo>,
        blob_repo: Arc<dyn PluginBlobRepo>,
        host_offer: HandshakeOffer,
        lifecycle: Arc<dyn RegistryLifecycle>,
    ) -> Result<Self, RegistryError> {
        host_offer.validate()?;
        let host_offer_hash = host_offer.canonical_hash()?;
        Ok(Self {
            registry_repo,
            blob_repo,
            in_process_cache: Arc::new(DashMap::new()),
            host_offer,
            host_offer_hash,
            lifecycle,
        })
    }

    pub fn host_offer_hash(&self) -> [u8; 32] {
        self.host_offer_hash
    }

    pub async fn register_plugin(
        &self,
        wasm_bytes: &[u8],
    ) -> Result<PluginRegistryRecord, RegistryError> {
        let sha256 = sha256(wasm_bytes);
        let identity = read_identity(wasm_bytes)?;

        if let Some(existing) = self.get_existing_record(&sha256).await? {
            reject_name_mismatch(&sha256, &existing.plugin_name, &identity.plugin_name)?;
            if existing.status == PluginRegistryStatus::Active
                && existing.host_offer_hash == self.host_offer_hash
            {
                self.get_or_insert_cached_metadata(sha256, existing.augmented_metadata.clone());
                return Ok(existing);
            }
        }

        let record = self.verify_and_build_record(sha256, wasm_bytes).await?;
        self.blob_repo
            .put_blob(&sha256, wasm_bytes)
            .await
            .map_err(|source| RegistryError::BlobRepo { source })?;
        self.registry_repo
            .upsert_record(&record)
            .await
            .map_err(|source| RegistryError::RegistryRepo { source })?;
        self.replace_cached_metadata(sha256, record.augmented_metadata.clone());
        Ok(record)
    }

    pub async fn re_handshake_by_sha256(
        &self,
        requested_sha256: &[u8; 32],
    ) -> Result<PluginRegistryRecord, RegistryError> {
        let wasm_bytes = self
            .blob_repo
            .get_blob(requested_sha256)
            .await
            .map_err(|source| RegistryError::BlobRepo { source })?
            .ok_or(RegistryError::BlobMissing {
                sha256: *requested_sha256,
            })?;
        let identity = read_identity(&wasm_bytes)?;

        if let Some(existing) = self.get_existing_record(requested_sha256).await? {
            reject_name_mismatch(
                requested_sha256,
                &existing.plugin_name,
                &identity.plugin_name,
            )?;
        }

        let actual_sha256 = sha256(&wasm_bytes);
        if actual_sha256 != *requested_sha256 {
            return Err(RegistryError::BlobSha256Mismatch {
                expected: *requested_sha256,
                actual: actual_sha256,
            });
        }

        let record = self
            .verify_and_build_record(*requested_sha256, &wasm_bytes)
            .await?;
        self.registry_repo
            .upsert_record(&record)
            .await
            .map_err(|source| RegistryError::RegistryRepo { source })?;
        self.replace_cached_metadata(*requested_sha256, record.augmented_metadata.clone());
        Ok(record)
    }

    pub async fn delete_plugin(&self, sha256: &[u8; 32]) -> Result<(), RegistryError> {
        self.registry_repo
            .delete_by_sha256(sha256)
            .await
            .map_err(|source| RegistryError::RegistryRepo { source })?;
        self.blob_repo
            .delete_blob(sha256)
            .await
            .map_err(|source| RegistryError::BlobRepo { source })?;
        self.in_process_cache.remove(sha256);
        Ok(())
    }

    pub async fn list_active(&self) -> Result<Vec<PluginRegistryRecord>, RegistryError> {
        self.registry_repo
            .list_active()
            .await
            .map_err(|source| RegistryError::RegistryRepo { source })
    }

    pub async fn get_by_sha256(
        &self,
        sha256: &[u8; 32],
    ) -> Result<Option<PluginRegistryRecord>, RegistryError> {
        self.registry_repo
            .get_by_sha256(sha256)
            .await
            .map_err(|source| RegistryError::RegistryRepo { source })
    }

    pub async fn load_from_db_at_startup(&self) -> Result<usize, RegistryError> {
        let records = self
            .registry_repo
            .list_active()
            .await
            .map_err(|source| RegistryError::RegistryRepo { source })?;
        let mut loaded = 0;
        for record in records {
            if record.host_offer_hash != self.host_offer_hash {
                continue;
            }
            self.replace_cached_metadata(record.sha256, record.augmented_metadata);
            loaded += 1;
        }
        Ok(loaded)
    }

    pub fn get_metadata(&self, sha256: &[u8; 32]) -> Option<Arc<AugmentedMetadata>> {
        self.in_process_cache
            .get(sha256)
            .map(|metadata| Arc::clone(metadata.value()))
    }

    pub fn load_record_into_cache(&self, record: &PluginRegistryRecord) {
        self.replace_cached_metadata(record.sha256, record.augmented_metadata.clone());
    }

    #[allow(clippy::collapsible_if)]
    fn get_or_insert_cached_metadata(
        &self,
        sha256: [u8; 32],
        metadata: AugmentedMetadata,
    ) -> Arc<AugmentedMetadata> {
        if let Some(existing) = self.in_process_cache.get(&sha256) {
            if existing.value().as_ref() == &metadata {
                return Arc::clone(existing.value());
            }
        }
        let metadata = Arc::new(metadata);
        self.in_process_cache.insert(sha256, Arc::clone(&metadata));
        metadata
    }

    fn replace_cached_metadata(
        &self,
        sha256: [u8; 32],
        metadata: AugmentedMetadata,
    ) -> Arc<AugmentedMetadata> {
        let metadata = Arc::new(metadata);
        self.in_process_cache.insert(sha256, Arc::clone(&metadata));
        metadata
    }

    async fn get_existing_record(
        &self,
        sha256: &[u8; 32],
    ) -> Result<Option<PluginRegistryRecord>, RegistryError> {
        self.get_by_sha256(sha256).await
    }

    async fn verify_and_build_record(
        &self,
        sha256: [u8; 32],
        wasm_bytes: &[u8],
    ) -> Result<PluginRegistryRecord, RegistryError> {
        let identity = read_identity(wasm_bytes)?;
        let accept = self
            .lifecycle
            .execute_handshake(wasm_bytes, &self.host_offer)?;
        let handshake_completed_at = unix_now()?;
        let supported_slots = slot_set_from_handshake(&accept.implemented_functions);
        let self_check = self
            .lifecycle
            .execute_self_check(wasm_bytes, &supported_slots)?;

        let augmented_metadata = AugmentedMetadata::from_handshake_and_self_check(
            identity.clone(),
            accept.chosen_versions,
            accept.required_capabilities,
            handshake_completed_at,
            true,
            self_check.completed_at,
            SKIP_HANDSHAKE_IF_FRESH_TTL_SECS,
        )?;

        Ok(PluginRegistryRecord {
            sha256,
            plugin_name: identity.plugin_name,
            plugin_version: identity.plugin_version,
            abi_envelope: identity.abi_envelope,
            augmented_metadata,
            host_offer_hash: self.host_offer_hash,
            handshake_schema_version: self.host_offer.handshake_schema_version,
            last_handshake_at: handshake_completed_at,
            status: PluginRegistryStatus::Active,
        })
    }
}

fn reject_name_mismatch(
    sha256: &[u8; 32],
    existing: &str,
    actual: &str,
) -> Result<(), RegistryError> {
    if existing == actual {
        return Ok(());
    }
    Err(RegistryError::PluginNameMismatch {
        sha256: *sha256,
        existing: existing.to_owned(),
        actual: actual.to_owned(),
    })
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    digest(&SHA256, bytes)
        .as_ref()
        .try_into()
        .expect("sha256 is 32 bytes")
}

fn unix_now() -> Result<i64, RegistryError> {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|source| RegistryError::Clock {
            reason: source.to_string(),
        })?
        .as_secs();
    i64::try_from(seconds).map_err(|source| RegistryError::Clock {
        reason: source.to_string(),
    })
}

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("host offer validation failed: {0}")]
    HostOfferValidation(#[from] cc_lb_plugin_wire::handshake::HandshakeError),
    #[error("host offer canonical hash failed: {0}")]
    HostOfferHash(#[from] CanonicalError),
    #[error("identity verification failed: {0}")]
    Identity(#[from] IdentityReadError),
    #[error("handshake verification failed: {0}")]
    Handshake(#[from] HandshakeExecutionError),
    #[error("self-check verification failed: {0}")]
    SelfCheck(#[from] SelfCheckExecutionError),
    #[error("augmented metadata validation failed: {0}")]
    Metadata(#[from] AugmentedMetadataError),
    #[error("plugin registry repository failed: {source}")]
    RegistryRepo { source: RepoError },
    #[error("plugin blob repository failed: {source}")]
    BlobRepo { source: RepoError },
    #[error("plugin blob missing for sha256 {}", hex_sha256(sha256))]
    BlobMissing { sha256: [u8; 32] },
    #[error(
        "plugin blob sha256 mismatch: expected {}, actual {}",
        hex_sha256(expected),
        hex_sha256(actual)
    )]
    BlobSha256Mismatch {
        expected: [u8; 32],
        actual: [u8; 32],
    },
    #[error(
        "sha256 {} is already registered as plugin '{existing}', not '{actual}'",
        hex_sha256(sha256)
    )]
    PluginNameMismatch {
        sha256: [u8; 32],
        existing: String,
        actual: String,
    },
    #[error("clock failed: {reason}")]
    Clock { reason: String },
}

fn hex_sha256(sha256: &[u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in sha256 {
        use std::fmt::Write;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use cc_lb_plugin_wire::handshake::{HANDSHAKE_SCHEMA_VERSION_V1, HandshakeAccept};
    use cc_lb_plugin_wire::identity::{CC_LB_PLUGIN_MAGIC, CC_LB_PLUGIN_SECTION_NAME};
    use serde_json::json;
    use tokio::sync::Mutex;

    use super::*;

    #[tokio::test]
    async fn full_register() {
        let repos = Repos::default();
        let registry = registry(repos.registry.clone(), repos.blobs.clone());
        let wasm = plugin_wasm("test-plugin", "1.0.0");
        let sha = sha256(&wasm);

        let record = registry
            .register_plugin(&wasm)
            .await
            .expect("plugin registers");

        assert_eq!(record.sha256, sha);
        assert_eq!(record.plugin_name, "test-plugin");
        assert_eq!(record.plugin_version, "1.0.0");
        assert_eq!(record.host_offer_hash, registry.host_offer_hash());
        assert_eq!(
            record.augmented_metadata.identity.plugin_name,
            "test-plugin"
        );
        assert_eq!(
            record.augmented_metadata.negotiated_functions.get("shape"),
            Some(&1)
        );
        assert!(registry.get_metadata(&sha).is_some());
        assert_eq!(repos.blobs.puts.load(Ordering::SeqCst), 1);
        assert_eq!(repos.registry.upserts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn sha256_dedup_single_handshake() {
        let repos = Repos::default();
        let registry = registry(repos.registry.clone(), repos.blobs.clone());
        let wasm = plugin_wasm("test-plugin", "1.0.0");

        registry
            .register_plugin(&wasm)
            .await
            .expect("first registration succeeds");
        registry
            .register_plugin(&wasm)
            .await
            .expect("second registration dedups");

        assert_eq!(repos.registry.upserts.load(Ordering::SeqCst), 1);
        assert_eq!(repos.blobs.puts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn name_mismatch_rejected() {
        let repos = Repos::default();
        let registry = registry(repos.registry.clone(), repos.blobs.clone());
        let wasm = plugin_wasm("test-plugin", "1.0.0");
        let sha = sha256(&wasm);
        let mut record = registry
            .register_plugin(&wasm)
            .await
            .expect("plugin registers");
        record.plugin_name = "other-plugin".to_owned();
        repos
            .registry
            .upsert_record(&record)
            .await
            .expect("seed mismatch");

        let error = registry
            .register_plugin(&wasm)
            .await
            .expect_err("mismatched name is rejected");

        match error {
            RegistryError::PluginNameMismatch {
                sha256,
                existing,
                actual,
            } => {
                assert_eq!(sha256, sha);
                assert_eq!(existing, "other-plugin");
                assert_eq!(actual, "test-plugin");
            }
            other => panic!("expected plugin name mismatch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn host_offer_hash_mismatch_skipped() {
        let repos = Repos::default();
        let registry = registry(repos.registry.clone(), repos.blobs.clone());
        let wasm = plugin_wasm("test-plugin", "1.0.0");
        let mut record = registry
            .register_plugin(&wasm)
            .await
            .expect("plugin registers");
        record.host_offer_hash = [9; 32];
        repos
            .registry
            .upsert_record(&record)
            .await
            .expect("seed stale hash");
        registry.in_process_cache.clear();

        let loaded = registry
            .load_from_db_at_startup()
            .await
            .expect("startup load succeeds");

        assert_eq!(loaded, 0);
        assert!(registry.get_metadata(&record.sha256).is_none());
    }

    #[tokio::test]
    async fn re_handshake_by_sha256_fetches_blob_and_updates_cache() {
        let repos = Repos::default();
        let registry = registry(repos.registry.clone(), repos.blobs.clone());
        let wasm = plugin_wasm("test-plugin", "1.0.0");
        let sha = sha256(&wasm);
        repos
            .blobs
            .put_blob(&sha, &wasm)
            .await
            .expect("blob seeded");

        let record = registry
            .re_handshake_by_sha256(&sha)
            .await
            .expect("re-handshake succeeds");

        assert_eq!(record.sha256, sha);
        assert!(registry.get_metadata(&sha).is_some());
        assert_eq!(repos.registry.upserts.load(Ordering::SeqCst), 1);
    }

    fn registry(
        registry_repo: Arc<MemoryRegistryRepo>,
        blob_repo: Arc<MemoryBlobRepo>,
    ) -> PluginRegistry {
        PluginRegistry::new(
            registry_repo,
            blob_repo,
            cc_lb_runtime_protocol::handshake::build_offer(&BTreeSet::new()),
        )
        .expect("registry builds")
    }

    #[derive(Default)]
    struct Repos {
        registry: Arc<MemoryRegistryRepo>,
        blobs: Arc<MemoryBlobRepo>,
    }

    #[derive(Default)]
    struct MemoryRegistryRepo {
        records: Mutex<BTreeMap<[u8; 32], PluginRegistryRecord>>,
        upserts: AtomicUsize,
    }

    #[async_trait]
    impl PluginRegistryRepo for MemoryRegistryRepo {
        async fn upsert_record(&self, record: &PluginRegistryRecord) -> Result<(), RepoError> {
            self.upserts.fetch_add(1, Ordering::SeqCst);
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
        puts: AtomicUsize,
    }

    #[async_trait]
    impl PluginBlobRepo for MemoryBlobRepo {
        async fn put_blob(&self, sha256: &[u8; 32], bytes: &[u8]) -> Result<(), RepoError> {
            self.puts.fetch_add(1, Ordering::SeqCst);
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
        let accept = HandshakeAccept {
            handshake_schema_version: HANDSHAKE_SCHEMA_VERSION_V1,
            envelope_version: 1,
            chosen_versions: BTreeMap::from([("shape".to_owned(), 1)]),
            plugin_supported: BTreeMap::from([("shape".to_owned(), vec![1])]),
            implemented_functions: BTreeSet::from(["shape".to_owned()]),
            required_capabilities: BTreeSet::new(),
        };
        let handshake_output = serde_json::to_string(&accept).expect("accept serializes");
        let self_check_output = json!({
            "status": "success",
            "failures": [],
            "completed_at": 1,
        })
        .to_string();
        let wat = format!(
            r#"
(module
  (import "extism:host/env" "alloc" (func $alloc (param i64) (result i64)))
  (import "extism:host/env" "store_u8" (func $store_u8 (param i64 i32)))
  (import "extism:host/env" "output_set" (func $output_set (param i64 i64)))
  {handshake_helper}
  {self_check_helper}
  (func (export "cc_lb_handshake") (result i32)
    (call $output_set (call $handshake_out) (i64.const {handshake_len}))
    (i32.const 0))
  (func (export "cc_lb_self_check") (result i32)
    (call $output_set (call $self_check_out) (i64.const {self_check_len}))
    (i32.const 0))
  (func (export "shape") (result i32)
    (i32.const 0))
)
"#,
            handshake_helper = bytes_helper("handshake_out", handshake_output.as_bytes()),
            self_check_helper = bytes_helper("self_check_out", self_check_output.as_bytes()),
            handshake_len = handshake_output.len(),
            self_check_len = self_check_output.len(),
        );
        let mut wasm = wat::parse_str(&wat).expect("wat parses");
        append_identity_section(&mut wasm, plugin_name, plugin_version);
        wasm
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

    fn append_identity_section(wasm: &mut Vec<u8>, plugin_name: &str, plugin_version: &str) {
        let payload = json!({
            "magic": CC_LB_PLUGIN_MAGIC,
            "abi_envelope": 1,
            "plugin_name": plugin_name,
            "plugin_version": plugin_version,
        })
        .to_string();
        wasm.push(0);
        let mut section = Vec::new();
        encode_u32(CC_LB_PLUGIN_SECTION_NAME.len() as u32, &mut section);
        section.extend_from_slice(CC_LB_PLUGIN_SECTION_NAME.as_bytes());
        section.extend_from_slice(payload.as_bytes());
        encode_u32(section.len() as u32, wasm);
        wasm.extend_from_slice(&section);
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
}
