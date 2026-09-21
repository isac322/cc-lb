use std::sync::Arc;

use async_trait::async_trait;
use cc_lb_domain::{CacheScore, SubscriptionQuotaCandidateSnapshot};
use cc_lb_storage_api::SubscriptionQuotaSample;
use cc_lb_storage_api::types::{ApiKeyMutation, PrincipalLimitState, StoredApiKeyRecord};
use uuid::Uuid;

use crate::api_keys::key_store::KeyStore;
use crate::api_keys::key_store::{CreateParams, KeyStoreError};
use crate::api_keys::limit_engine::LimitEngine;
use crate::api_keys::limit_engine::{IdentityFilter, PrincipalLimitsSnapshot};
use crate::api_keys::principal_view::PrincipalView;
use crate::api_keys::secret::RedactedSecret;
use crate::audit_writer::AuditWriterSink;
use crate::dynamic_view::{DynamicView, DynamicViewHolder};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PromptCacheThreadUsage {
    pub cache_read_input_tokens: u64,
    pub cache_creation_input_tokens_5m: u64,
    pub cache_creation_input_tokens_1h: u64,
}

/// Diagnostic thread-usage tracker. This is a separate lineage from the
/// shared prompt-cache observation store: it records per-thread provider
/// cache usage for counterfactual diagnostics only and is never a warmth
/// authority for routing.
pub trait PromptCacheThreadUsageTrackerLike: Send + Sync {
    fn thread_usage_score(
        &self,
        _upstream_id: Uuid,
        _canonical_model: &str,
        _thread_id: &str,
        _now_unix_secs: u64,
    ) -> Option<CacheScore> {
        None
    }

    fn record_thread_usage(
        &self,
        _upstream_id: Uuid,
        _canonical_model: &str,
        _thread_id: &str,
        _usage: PromptCacheThreadUsage,
        _now_unix_secs: u64,
    ) {
    }
}

pub trait SubscriptionQuotaCacheLike: Send + Sync {
    fn upsert_observation(&self, record: &SubscriptionQuotaSample);

    fn snapshot_for_upstream(
        &self,
        upstream_id: Uuid,
        now_unix_millis: u64,
        max_staleness_secs: u64,
    ) -> Vec<SubscriptionQuotaCandidateSnapshot>;
}

pub trait PromptCacheObservationSinkLike: Send + Sync {
    fn enqueue(
        &self,
        record: cc_lb_storage_api::PromptCacheObservationRecord,
    ) -> Result<(), PromptCacheObservationEnqueueError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PromptCacheObservationEnqueueError {
    ChannelFull,
    ChannelClosed,
}

#[derive(Debug, Default)]
pub struct NoopSubscriptionQuotaCache;

impl SubscriptionQuotaCacheLike for NoopSubscriptionQuotaCache {
    fn upsert_observation(&self, _record: &SubscriptionQuotaSample) {}

    fn snapshot_for_upstream(
        &self,
        _upstream_id: Uuid,
        _now_unix_millis: u64,
        _max_staleness_secs: u64,
    ) -> Vec<SubscriptionQuotaCandidateSnapshot> {
        Vec::new()
    }
}

#[async_trait]
pub trait ManagedKeyControl: Send + Sync {
    async fn create_key(
        &self,
        principal_id: &str,
        params: CreateParams,
    ) -> Result<(StoredApiKeyRecord, RedactedSecret), KeyStoreError>;

    async fn get_key(
        &self,
        principal_id: &str,
        key_id: &str,
    ) -> Result<Option<StoredApiKeyRecord>, KeyStoreError>;

    async fn list_keys_by_principal(
        &self,
        principal_id: &str,
    ) -> Result<Vec<StoredApiKeyRecord>, KeyStoreError>;

    async fn list_all_keys(
        &self,
    ) -> Result<Vec<(String, String, StoredApiKeyRecord)>, KeyStoreError>;

    async fn disable_key(&self, principal_id: &str, key_id: &str) -> Result<(), KeyStoreError>;

    async fn enable_key(&self, principal_id: &str, key_id: &str) -> Result<(), KeyStoreError>;

    async fn revoke_key(&self, principal_id: &str, key_id: &str) -> Result<(), KeyStoreError>;

    async fn patch_key(
        &self,
        principal_id: &str,
        key_id: &str,
        mutation: ApiKeyMutation,
    ) -> Result<(), KeyStoreError>;
}

pub trait LimitControl: Send + Sync {
    fn snapshot_for_principal(
        &self,
        view: &PrincipalView,
        principal_id: &str,
        identity_filter: IdentityFilter,
    ) -> PrincipalLimitsSnapshot;

    fn record_principal_limit_state(&self, state: &PrincipalLimitState);
}

pub trait DynamicViewControl: Send + Sync {
    fn load_dynamic_view(&self) -> Arc<DynamicView>;

    fn store_dynamic_view(&self, view: Arc<DynamicView>);

    fn try_store_dynamic_view_if_newer(&self, view: Arc<DynamicView>) -> bool;

    fn dynamic_view_generation(&self) -> u64;
}

#[async_trait]
pub trait RuntimeStatusControl: Send + Sync {
    async fn runtime_status(&self) -> Result<serde_json::Value, RuntimeStatusError>;
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeStatusError {
    #[error("runtime status unavailable")]
    Unavailable,
    #[error("runtime status failed: {0}")]
    Failed(String),
}

pub trait SubscriptionQuotaSampleControl: Send + Sync {
    fn upsert_observation(&self, record: &SubscriptionQuotaSample);

    fn snapshot_for_upstream(
        &self,
        upstream_id: Uuid,
        now_unix_millis: u64,
        max_staleness_secs: u64,
    ) -> Vec<SubscriptionQuotaCandidateSnapshot>;
}

#[async_trait]
impl ManagedKeyControl for KeyStore {
    async fn create_key(
        &self,
        principal_id: &str,
        params: CreateParams,
    ) -> Result<(StoredApiKeyRecord, RedactedSecret), KeyStoreError> {
        self.create(principal_id, params).await
    }

    async fn get_key(
        &self,
        principal_id: &str,
        key_id: &str,
    ) -> Result<Option<StoredApiKeyRecord>, KeyStoreError> {
        self.get(principal_id, key_id).await
    }

    async fn list_keys_by_principal(
        &self,
        principal_id: &str,
    ) -> Result<Vec<StoredApiKeyRecord>, KeyStoreError> {
        self.list_by_principal(principal_id).await
    }

    async fn list_all_keys(
        &self,
    ) -> Result<Vec<(String, String, StoredApiKeyRecord)>, KeyStoreError> {
        self.list_all().await
    }

    async fn disable_key(&self, principal_id: &str, key_id: &str) -> Result<(), KeyStoreError> {
        self.disable(principal_id, key_id).await
    }

    async fn enable_key(&self, principal_id: &str, key_id: &str) -> Result<(), KeyStoreError> {
        self.enable(principal_id, key_id).await
    }

    async fn revoke_key(&self, principal_id: &str, key_id: &str) -> Result<(), KeyStoreError> {
        self.revoke(principal_id, key_id).await
    }

    async fn patch_key(
        &self,
        principal_id: &str,
        key_id: &str,
        mutation: ApiKeyMutation,
    ) -> Result<(), KeyStoreError> {
        self.patch(principal_id, key_id, mutation).await
    }
}

impl LimitControl for LimitEngine {
    fn snapshot_for_principal(
        &self,
        view: &PrincipalView,
        principal_id: &str,
        identity_filter: IdentityFilter,
    ) -> PrincipalLimitsSnapshot {
        LimitEngine::snapshot_for_principal(self, view, principal_id, identity_filter)
    }

    fn record_principal_limit_state(&self, state: &PrincipalLimitState) {
        LimitEngine::record_principal_limit_state(self, state);
    }
}

impl DynamicViewControl for DynamicViewHolder {
    fn load_dynamic_view(&self) -> Arc<DynamicView> {
        self.load()
    }

    fn store_dynamic_view(&self, view: Arc<DynamicView>) {
        self.store(view);
    }

    fn try_store_dynamic_view_if_newer(&self, view: Arc<DynamicView>) -> bool {
        self.try_store_if_newer(view)
    }

    fn dynamic_view_generation(&self) -> u64 {
        self.generation()
    }
}

impl SubscriptionQuotaSampleControl for NoopSubscriptionQuotaCache {
    fn upsert_observation(&self, record: &SubscriptionQuotaSample) {
        SubscriptionQuotaCacheLike::upsert_observation(self, record);
    }

    fn snapshot_for_upstream(
        &self,
        upstream_id: Uuid,
        now_unix_millis: u64,
        max_staleness_secs: u64,
    ) -> Vec<SubscriptionQuotaCandidateSnapshot> {
        SubscriptionQuotaCacheLike::snapshot_for_upstream(
            self,
            upstream_id,
            now_unix_millis,
            max_staleness_secs,
        )
    }
}

impl cc_lb_storage_api::AuditSink for AuditWriterSink {
    fn sink_audit(&self, entry: cc_lb_storage_api::AuditEntry) {
        let _ = self.try_enqueue(entry);
    }
}
