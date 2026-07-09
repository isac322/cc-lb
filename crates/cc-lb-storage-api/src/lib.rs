#![allow(ambiguous_glob_reexports)]

pub mod anthropic_compatibility_kv;
pub mod error;
pub mod organization_metadata;
pub mod plan_tiers;
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
pub mod upstream_subscription_quota_checkpoint;
pub mod validation;
pub mod warmup_attempts;

use async_trait::async_trait;

pub use anthropic_compatibility_kv::*;

pub use error::{PluginChainConflictReason, StorageError, StorageResult};
pub use organization_metadata::*;
pub use plan_tiers::*;
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
pub use upstream_subscription_quota_checkpoint::*;
pub use uuid::Uuid as UpstreamRecordId;
pub use validation::{validate_identifier, validate_sse_batching_knobs};
pub use warmup_attempts::*;

pub type RepoError = StorageError;

#[async_trait]
pub trait PluginBlobRepo: Send + Sync {
    async fn put_blob(&self, sha256: &[u8; 32], bytes: &[u8]) -> Result<(), RepoError>;

    async fn get_blob(&self, sha256: &[u8; 32]) -> Result<Option<Vec<u8>>, RepoError>;

    async fn delete_blob(&self, sha256: &[u8; 32]) -> Result<(), RepoError>;

    async fn list_blob_keys(&self) -> Result<Vec<[u8; 32]>, RepoError>;
}
