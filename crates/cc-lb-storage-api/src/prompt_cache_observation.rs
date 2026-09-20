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
}

#[async_trait]
pub trait PromptCacheObservationStore: Send + Sync + 'static {
    /// Atomically records an observation. Writes are monotonic: the winner is
    /// the record with the lexicographically greater `(expires_at,
    /// last_observed_at)` pair, so a stale write can never regress a fresher
    /// row. On a full tie the stored row is kept whole — winner metadata is
    /// never spliced with loser fields — and replaying an identical record is a
    /// no-op.
    async fn upsert_observation(
        &self,
        _record: &PromptCacheObservationRecord,
    ) -> StorageResult<()> {
        Err(StorageError::Unavailable {
            message: "PromptCacheObservationStore::upsert_observation is not implemented"
                .to_owned(),
        })
    }

    /// Batch lookup of active observations for one request's routing candidates:
    /// every `upstream_id` × `v3_prefix_key` pair under one `canonical_model_id`
    /// whose `expires_at` is still after `not_expired_at_unix_secs`.
    ///
    /// Results are deterministically ordered by `(upstream_id, v3_prefix_key,
    /// ttl_class)` under each backend's own collation.
    ///
    /// Empty `upstream_ids` or `v3_prefix_keys` MUST return `Ok(vec![])` without
    /// issuing a query. The default implementation reports `Unavailable` rather
    /// than silently returning an empty snapshot, so an unimplemented backend
    /// cannot masquerade as "no observations".
    async fn list_active_for_candidates(
        &self,
        _upstream_ids: &[Uuid],
        _canonical_model_id: &str,
        _v3_prefix_keys: &[String],
        _not_expired_at_unix_secs: u64,
    ) -> StorageResult<Vec<PromptCacheObservationRecord>> {
        Err(StorageError::Unavailable {
            message: "PromptCacheObservationStore::list_active_for_candidates is not implemented"
                .to_owned(),
        })
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
