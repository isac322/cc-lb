use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use cc_lb_plugin_wire::augmented_metadata::AugmentedMetadata;
use cc_lb_plugin_wire::identity::{CC_LB_PLUGIN_MAGIC, PluginIdentity};
use cc_lb_server::dynamic_view_builder::bridged_metadata;
use cc_lb_storage_api::{
    PluginRegistryRecord, PluginRegistryRepo, PluginRegistryStatus, RepoError,
};
use parking_lot::Mutex;

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
