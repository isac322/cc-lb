#![allow(ambiguous_glob_reexports)]

pub mod anthropic_compatibility_kv;
pub mod error;
pub mod organization_metadata;
pub mod plugin_registry;
pub mod pool_quota_history;
pub mod principal;
pub mod prompt_cache_observation;
pub mod runtime_change_notifier;
pub mod sparse_order;
pub mod traits;
pub mod types;
pub mod upstream;
pub mod upstream_rate_limit;
pub mod upstream_subscription_metadata;
pub mod upstream_subscription_quota;
pub mod validation;
pub mod warmup_attempts;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub use anthropic_compatibility_kv::*;
pub use cc_lb_plugin_wire::augmented_metadata::AugmentedMetadata;
pub use error::{PluginChainConflictReason, StorageError, StorageResult};
pub use organization_metadata::*;
pub use plugin_registry::*;
pub use pool_quota_history::*;
pub use principal::*;
pub use prompt_cache_observation::{
    PromptCacheObservationRecord, PromptCacheObservationStore, TtlClass,
};
pub use runtime_change_notifier::*;
pub use traits::{
    ApiKeyStore, AuditStore, CURRENT_CONTRACT_VERSION, ConfigStore, ManagedKeyStore, MetaStore,
    OAuthCredentialStore, PriceCatalogCache, RequestEventStore, Storage, UsageRollupStore,
};
pub use types::*;
pub use upstream::*;
pub use upstream_rate_limit::*;
pub use upstream_subscription_metadata::*;
pub use upstream_subscription_quota::*;
pub use uuid::Uuid as UpstreamRecordId;
pub use validation::validate_identifier;
pub use warmup_attempts::*;

pub type RepoError = StorageError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginRegistryStatus {
    Active,
    Disabled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginRegistryRecord {
    pub sha256: [u8; 32],
    pub plugin_name: String,
    pub plugin_version: String,
    pub abi_envelope: u32,
    pub augmented_metadata: AugmentedMetadata,
    pub host_offer_hash: [u8; 32],
    pub handshake_schema_version: u32,
    pub last_handshake_at: i64,
    pub status: PluginRegistryStatus,
}

#[async_trait]
pub trait PluginRegistryRepo: Send + Sync {
    async fn upsert_record(&self, record: &PluginRegistryRecord) -> Result<(), RepoError>;

    async fn get_by_sha256(
        &self,
        sha256: &[u8; 32],
    ) -> Result<Option<PluginRegistryRecord>, RepoError>;

    async fn list_active(&self) -> Result<Vec<PluginRegistryRecord>, RepoError>;

    async fn set_status(
        &self,
        sha256: &[u8; 32],
        status: PluginRegistryStatus,
    ) -> Result<(), RepoError>;

    async fn delete_by_sha256(&self, sha256: &[u8; 32]) -> Result<(), RepoError>;

    async fn count(&self) -> Result<usize, RepoError>;

    async fn get_shutdown_marker(&self) -> Result<Option<i64>, RepoError>;

    async fn set_shutdown_marker(&self, unix_secs: i64) -> Result<(), RepoError>;

    async fn clear_shutdown_marker(&self) -> Result<(), RepoError>;
}

#[async_trait]
pub trait PluginBlobRepo: Send + Sync {
    async fn put_blob(&self, sha256: &[u8; 32], bytes: &[u8]) -> Result<(), RepoError>;

    async fn get_blob(&self, sha256: &[u8; 32]) -> Result<Option<Vec<u8>>, RepoError>;

    async fn delete_blob(&self, sha256: &[u8; 32]) -> Result<(), RepoError>;

    async fn list_blob_keys(&self) -> Result<Vec<[u8; 32]>, RepoError>;
}
