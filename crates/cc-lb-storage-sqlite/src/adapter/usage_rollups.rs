use async_trait::async_trait;
use cc_lb_storage_api::{
    BucketKind, LimitStateStore, PrincipalLimitIdentityKind, PrincipalLimitKind,
    PrincipalLimitState, QuotaStore, StorageResult, UsageRollup, UsageRollupResolution,
    UsageRollupRun, UsageRollupStore,
};

use crate::SqliteStorage;

#[async_trait]
impl UsageRollupStore for SqliteStorage {
    async fn rollup_usage_once(&self) -> StorageResult<UsageRollupRun> {
        unimplemented!()
    }

    async fn query_usage_rollups(&self) -> StorageResult<Vec<UsageRollup>> {
        unimplemented!()
    }

    async fn query_usage_rollups_in_range(
        &self,
        _resolution: UsageRollupResolution,
        _window_start_unix_secs: u64,
        _window_end_unix_secs: u64,
    ) -> StorageResult<Vec<UsageRollup>> {
        unimplemented!()
    }

    async fn usage_rollup_checkpoint(&self) -> StorageResult<Option<u64>> {
        unimplemented!()
    }

    async fn advance_rollup_checkpoint_and_persist(
        &self,
        _run: &UsageRollupRun,
    ) -> StorageResult<()> {
        unimplemented!()
    }
}

#[async_trait]
impl QuotaStore for SqliteStorage {
    async fn incr_quota(
        &self,
        _principal_id: &str,
        _window_start: u64,
        _kind: BucketKind,
        _amount: u64,
    ) -> StorageResult<u64> {
        unimplemented!()
    }

    async fn try_incr_quota(
        &self,
        _principal_id: &str,
        _window_start: u64,
        _kind: BucketKind,
        _amount: u64,
        _capacity: u64,
    ) -> StorageResult<Option<u64>> {
        unimplemented!()
    }

    async fn get_quota(
        &self,
        _principal_id: &str,
        _window_start: u64,
        _kind: BucketKind,
    ) -> StorageResult<u64> {
        unimplemented!()
    }

    async fn adjust_quota(
        &self,
        _principal_id: &str,
        _window_start: u64,
        _kind: BucketKind,
        _delta: i64,
    ) -> StorageResult<u64> {
        unimplemented!()
    }

    async fn sweep_old_quotas(&self, _older_than_window_start: u64) -> StorageResult<u64> {
        unimplemented!()
    }
}

#[async_trait]
impl LimitStateStore for SqliteStorage {
    async fn put_principal_limit_state(&self, _state: &PrincipalLimitState) -> StorageResult<()> {
        unimplemented!()
    }

    async fn get_principal_limit_state(
        &self,
        _principal_id: &str,
        _identity_kind: PrincipalLimitIdentityKind,
        _identity_value: Option<&str>,
        _window: &str,
        _kind: PrincipalLimitKind,
    ) -> StorageResult<Option<PrincipalLimitState>> {
        unimplemented!()
    }

    async fn list_principal_limit_states(
        &self,
        _principal_id: &str,
    ) -> StorageResult<Vec<PrincipalLimitState>> {
        unimplemented!()
    }
}
