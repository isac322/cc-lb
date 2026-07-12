#![allow(ambiguous_glob_reexports)]

pub mod anthropic_compatibility_kv;
pub mod audit;
pub mod cache_keepalive;
pub mod cache_keepalive_sessions;
pub mod error;
pub mod limits;
pub mod organization_metadata;
pub mod plan_tiers;
pub mod plugin_registry;
mod plugin_registry_models;
pub mod plugin_slot_kind;
pub mod pool_quota_history;
pub mod principal;
pub mod prompt_cache_observation;
pub mod runtime_change_notifier;
pub mod sparse_order;
mod storage_types_common;
mod storage_types_keys;
pub mod traits;
pub mod types;
pub mod upstream;
pub mod upstream_rate_limit;
pub mod upstream_subscription_metadata;
pub mod upstream_subscription_quota;
pub mod upstream_subscription_quota_checkpoint;
pub mod usage_pruner;
pub mod validation;
pub mod warmup_attempts;

use async_trait::async_trait;

pub use anthropic_compatibility_kv::*;
pub use audit::*;
pub use cache_keepalive::{
    CacheKeepaliveConfig, CacheTtl, ClassifierConfig, JudgeResponseFormat, LlmJudgeConfig,
};
pub use cache_keepalive_sessions::*;

pub use error::{PluginChainConflictReason, StorageError, StorageResult};
pub use limits::*;
pub use organization_metadata::*;
pub use plan_tiers::*;
pub use plugin_registry::*;
pub use plugin_slot_kind::*;
pub use pool_quota_history::*;
pub use principal::*;
pub use prompt_cache_observation::{PromptCacheObservationRecord, PromptCacheObservationStore};
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
pub use validation::validate_identifier;
pub use warmup_attempts::*;

pub type RepoError = StorageError;

#[async_trait]
pub trait PluginBlobRepo: Send + Sync {
    async fn put_blob(&self, sha256: &[u8; 32], bytes: &[u8]) -> Result<(), RepoError>;

    async fn get_blob(&self, sha256: &[u8; 32]) -> Result<Option<Vec<u8>>, RepoError>;

    async fn delete_blob(&self, sha256: &[u8; 32]) -> Result<(), RepoError>;

    async fn list_blob_keys(&self) -> Result<Vec<[u8; 32]>, RepoError>;
}
