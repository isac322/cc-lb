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
    /// Independent full TTL, before routing grace; legacy rows have no deadline.
    #[serde(default)]
    pub protection_expires_at_unix_secs: Option<u64>,
    pub last_observed_at_unix_secs: u64,
    pub hash_schema_version: u8,
    pub prefix_content_block_index: u32,
    /// Prefix size in the unit named by `token_estimate_source`; mirrors the
    /// DB column. Decode only via `cc_lb_domain::PrefixBytes::from_persisted`.
    pub estimated_prefix_tokens: u64,
    /// Estimator identity of `estimated_prefix_tokens`.
    pub token_estimate_source: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptCacheProtection {
    pub upstream_id: Uuid,
    /// None represents account-wide evidence with an encrypted/unknown model.
    pub canonical_model_id: Option<String>,
    pub protection_expires_at_unix_secs: u64,
}

#[async_trait]
pub trait PromptCacheObservationStore: Send + Sync + 'static {
    /// Atomically records an observation. Routing fields retain the whole winner
    /// with the lexicographically greater `(expires_at, last_observed_at)` pair;
    /// ties keep the stored routing metadata without mixing incoming fields.
    /// Independently, every write maximizes the effective full-TTL protection
    /// deadline, including writes that lose the routing comparison. Legacy null
    /// deadlines conservatively derive from `last_observed_at + TTL`. Replaying
    /// an identical observation cannot regress either routing or protection.
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

    /// Compact protection, independent of prefix and hash schema. Includes model-wide
    /// observations and account-wide keepalive renewal anchors whose model is encrypted.
    /// The latter have no model and apply across requested models.
    /// An empty result means the scoped keys were read successfully, not unavailable.
    async fn list_active_protection(
        &self,
        _upstream_ids: &[Uuid],
        _canonical_model_id: &str,
        _now_unix_secs: u64,
    ) -> StorageResult<Vec<PromptCacheProtection>> {
        Err(StorageError::Unavailable {
            message: "PromptCacheObservationStore::list_active_protection is not implemented"
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
            protection_expires_at_unix_secs: None,
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
