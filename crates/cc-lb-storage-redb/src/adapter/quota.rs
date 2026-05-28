use async_trait::async_trait;
use cc_lb_storage_api::{
    QuotaStore, StorageError as ApiStorageError, StorageResult, types::BucketKind as ApiBucketKind,
};

use crate::RedbStorage;

fn quota_removed() -> ApiStorageError {
    ApiStorageError::Fatal {
        message: "legacy quota store removed; use limit engine".to_owned(),
    }
}

#[async_trait]
impl QuotaStore for RedbStorage {
    async fn incr_quota(
        &self,
        _principal_id: &str,
        _window_start: u64,
        _kind: ApiBucketKind,
        _amount: u64,
    ) -> StorageResult<u64> {
        Err(quota_removed())
    }

    async fn try_incr_quota(
        &self,
        _principal_id: &str,
        _window_start: u64,
        _kind: ApiBucketKind,
        _amount: u64,
        _capacity: u64,
    ) -> StorageResult<Option<u64>> {
        Err(quota_removed())
    }

    async fn get_quota(
        &self,
        _principal_id: &str,
        _window_start: u64,
        _kind: ApiBucketKind,
    ) -> StorageResult<u64> {
        Err(quota_removed())
    }

    async fn adjust_quota(
        &self,
        _principal_id: &str,
        _window_start: u64,
        _kind: ApiBucketKind,
        _delta: i64,
    ) -> StorageResult<u64> {
        Err(quota_removed())
    }

    async fn sweep_old_quotas(&self, _older_than_window_start: u64) -> StorageResult<u64> {
        Ok(0)
    }
}
