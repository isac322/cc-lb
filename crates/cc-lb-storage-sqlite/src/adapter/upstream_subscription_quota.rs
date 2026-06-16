use async_trait::async_trait;
use cc_lb_storage_api::{
    StorageResult, SubscriptionQuotaLatestRecord, SubscriptionQuotaObservationRecord,
    SubscriptionQuotaSeries, SubscriptionQuotaSeriesQuery, UpstreamSubscriptionQuotaStore,
};
use uuid::Uuid;

use crate::SqliteStorage;

#[async_trait]
impl UpstreamSubscriptionQuotaStore for SqliteStorage {
    async fn put_subscription_quota_batch(
        &self,
        _records: &[SubscriptionQuotaObservationRecord],
    ) -> StorageResult<()> {
        unimplemented!()
    }

    async fn put_subscription_quota(
        &self,
        _record: &SubscriptionQuotaObservationRecord,
    ) -> StorageResult<()> {
        unimplemented!()
    }

    async fn list_latest_subscription_quota_for_upstreams(
        &self,
        _upstream_ids: &[Uuid],
    ) -> StorageResult<Vec<SubscriptionQuotaLatestRecord>> {
        unimplemented!()
    }

    async fn list_subscription_quota_series(
        &self,
        _query: SubscriptionQuotaSeriesQuery,
    ) -> StorageResult<Vec<SubscriptionQuotaSeries>> {
        unimplemented!()
    }

    async fn delete_subscription_quota_before(
        &self,
        _cutoff_unix_millis: u64,
        _batch_size: u32,
    ) -> StorageResult<u64> {
        unimplemented!()
    }
}
