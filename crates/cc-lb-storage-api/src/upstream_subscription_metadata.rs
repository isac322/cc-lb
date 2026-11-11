use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::StorageResult;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpstreamSubscriptionMetadataRecord {
    pub upstream_id: Uuid,
    pub organization_uuid: Option<String>,
    pub organization_role: Option<String>,
    pub workspace_role: Option<String>,
    pub observed_at_unix_millis: i64,
    pub last_error: Option<String>,
    pub raw_roles: Option<String>,
    pub raw_bootstrap: Option<String>,
}

#[async_trait]
pub trait UpstreamSubscriptionMetadataStore: Send + Sync {
    async fn put_upstream_subscription_metadata(
        &self,
        record: &UpstreamSubscriptionMetadataRecord,
    ) -> StorageResult<()>;

    /// Atomically store an OAuth observation only while the upstream is active,
    /// its token generation matches, and its metadata equals the snapshot read
    /// before enrichment. `None` requires an absent metadata row.
    /// A missing, deleted, or changed upstream/snapshot returns `Ok(false)`.
    async fn compare_and_put_upstream_subscription_metadata(
        &self,
        record: &UpstreamSubscriptionMetadataRecord,
        expected_oauth_token_generation: u64,
        expected_metadata: Option<&UpstreamSubscriptionMetadataRecord>,
    ) -> StorageResult<bool>;

    async fn get_upstream_subscription_metadata(
        &self,
        upstream_id: Uuid,
    ) -> StorageResult<Option<UpstreamSubscriptionMetadataRecord>>;

    async fn list_upstream_subscription_metadata(
        &self,
    ) -> StorageResult<Vec<UpstreamSubscriptionMetadataRecord>>;
}
