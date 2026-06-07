//! Storage API records observed prompt-cache prefixes per upstream/model.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{StorageError, StorageResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TtlClass {
    #[default]
    Ephemeral5m,
    Ephemeral1h,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptCacheObservationRecord {
    pub upstream_id: Uuid,
    pub canonical_model_id: String,
    pub prefix_hash: String,
    pub ttl_class: TtlClass,
    pub expires_at_unix_secs: u64,
    pub last_observed_at_unix_secs: u64,
    pub hash_schema_version: u8,
}

#[async_trait]
pub trait PromptCacheObservationStore: Send + Sync + 'static {
    async fn upsert_observation(
        &self,
        _record: &PromptCacheObservationRecord,
    ) -> StorageResult<()> {
        Err(StorageError::Unavailable {
            message: "PromptCacheObservationStore::upsert_observation is not implemented"
                .to_owned(),
        })
    }

    async fn list_active_for_upstream(
        &self,
        _upstream_id: Uuid,
        _not_expired_at_unix_secs: u64,
    ) -> StorageResult<Vec<PromptCacheObservationRecord>> {
        Ok(Vec::new())
    }

    async fn purge_expired_before(&self, _ts_unix_secs: u64) -> StorageResult<u64> {
        Ok(0)
    }

    async fn count(&self) -> StorageResult<u64> {
        Ok(0)
    }
}

impl<T> PromptCacheObservationStore for T where
    T: crate::traits::AuditStore
        + crate::plugin_registry::PluginRegistryStore
        + crate::principal::PrincipalStore
        + crate::upstream::UpstreamStore
        + crate::traits::RequestEventStore
        + crate::traits::QuotaStore
        + crate::traits::LimitStateStore
        + crate::upstream_rate_limit::UpstreamRateLimitStateStore
        + crate::upstream_subscription_quota::UpstreamSubscriptionQuotaStore
        + crate::upstream_subscription_metadata::UpstreamSubscriptionMetadataStore
        + crate::organization_metadata::OrganizationMetadataStore
        + crate::anthropic_compatibility_kv::AnthropicCompatibilityKvStore
        + crate::traits::UsageRollupStore
        + crate::traits::OAuthCredentialStore
        + crate::traits::ApiKeyStore
        + crate::traits::PriceCatalogCache
        + crate::traits::ConfigStore
        + crate::traits::MetaStore
        + crate::RuntimeChangeNotifier
        + crate::PluginRegistryRepo
        + crate::PluginBlobRepo
        + Send
        + Sync
        + 'static
{
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record_with_ttl(ttl_class: TtlClass) -> PromptCacheObservationRecord {
        PromptCacheObservationRecord {
            upstream_id: Uuid::from_u128(0x1234_5678_90ab_cdef_1234_5678_90ab_cdef),
            canonical_model_id: "claude-sonnet-4-5-20250929".to_owned(),
            prefix_hash: "sha256:abc123".to_owned(),
            ttl_class,
            expires_at_unix_secs: 1_800,
            last_observed_at_unix_secs: 1_500,
            hash_schema_version: 1,
        }
    }

    #[test]
    fn prompt_cache_observation_record_roundtrips_for_ephemeral_5m() {
        let record = record_with_ttl(TtlClass::Ephemeral5m);

        let json = serde_json::to_string(&record).unwrap();
        let decoded: PromptCacheObservationRecord = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded, record);
    }

    #[test]
    fn prompt_cache_observation_record_roundtrips_for_ephemeral_1h() {
        let record = record_with_ttl(TtlClass::Ephemeral1h);

        let json = serde_json::to_string(&record).unwrap();
        let decoded: PromptCacheObservationRecord = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded, record);
    }

    #[test]
    fn ttl_class_defaults_to_ephemeral_5m() {
        assert_eq!(TtlClass::default(), TtlClass::Ephemeral5m);
    }
}
