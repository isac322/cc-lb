//! Storage API records observed prompt-cache prefixes per upstream/model.

use async_trait::async_trait;
use cc_lb_domain::TtlClass;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{StorageError, StorageResult};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptCacheObservationRecord {
    pub upstream_id: Uuid,
    pub canonical_model_id: String,
    pub v3_prefix_key: String,
    pub ttl_class: TtlClass,
    pub expires_at_unix_secs: u64,
    pub last_observed_at_unix_secs: u64,
    pub hash_schema_version: u8,
    pub prefix_content_block_index: u32,
    pub estimated_prefix_tokens: u64,
    pub token_estimate_source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_provider_cache_read_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_provider_cache_creation_tokens: Option<u64>,
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

    async fn list_active_for_upstream_keys(
        &self,
        upstream_id: Uuid,
        not_expired_at_unix_secs: u64,
        _v3_prefix_keys: &[String],
    ) -> StorageResult<Vec<PromptCacheObservationRecord>> {
        self.list_active_for_upstream(upstream_id, not_expired_at_unix_secs)
            .await
    }

    async fn purge_expired_before(&self, _ts_unix_secs: u64) -> StorageResult<u64> {
        Ok(0)
    }

    async fn count(&self) -> StorageResult<u64> {
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cc_lb_engine::lifecycle::HASH_SCHEMA_VERSION;

    fn record_with_ttl(ttl_class: TtlClass) -> PromptCacheObservationRecord {
        PromptCacheObservationRecord {
            upstream_id: Uuid::from_u128(0x1234_5678_90ab_cdef_1234_5678_90ab_cdef),
            canonical_model_id: "claude-sonnet-4-5-20250929".to_owned(),
            v3_prefix_key: "v3:abc123".to_owned(),
            ttl_class,
            expires_at_unix_secs: 1_800,
            last_observed_at_unix_secs: 1_500,
            hash_schema_version: HASH_SCHEMA_VERSION,
            prefix_content_block_index: 7,
            estimated_prefix_tokens: 12_345,
            token_estimate_source: "local_tiktoken_v1".to_owned(),
            last_provider_cache_read_tokens: Some(12_000),
            last_provider_cache_creation_tokens: Some(345),
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
