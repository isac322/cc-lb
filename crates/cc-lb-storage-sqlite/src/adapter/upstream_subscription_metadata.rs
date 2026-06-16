use async_trait::async_trait;
use cc_lb_storage_api::{
    StorageResult, UpstreamSubscriptionMetadataRecord, UpstreamSubscriptionMetadataStore,
};
use uuid::Uuid;

use crate::SqliteStorage;

#[async_trait]
impl UpstreamSubscriptionMetadataStore for SqliteStorage {
    async fn put_upstream_subscription_metadata(
        &self,
        _record: &UpstreamSubscriptionMetadataRecord,
    ) -> StorageResult<()> {
        unimplemented!()
    }

    async fn get_upstream_subscription_metadata(
        &self,
        _upstream_id: Uuid,
    ) -> StorageResult<Option<UpstreamSubscriptionMetadataRecord>> {
        unimplemented!()
    }

    async fn list_upstream_subscription_metadata(
        &self,
    ) -> StorageResult<Vec<UpstreamSubscriptionMetadataRecord>> {
        unimplemented!()
    }
}
