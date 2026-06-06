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

    async fn get_upstream_subscription_metadata(
        &self,
        upstream_id: Uuid,
    ) -> StorageResult<Option<UpstreamSubscriptionMetadataRecord>>;

    async fn list_upstream_subscription_metadata(
        &self,
    ) -> StorageResult<Vec<UpstreamSubscriptionMetadataRecord>>;
}
