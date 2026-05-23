use async_trait::async_trait;
use cc_lb_storage_api::{types::BucketKind as ApiBucketKind, QuotaStore, StorageResult};

use crate::{BucketKind as RedbBucketKind, RedbStorage};

use super::error_map::{map_join_err, map_redb_err};

#[async_trait]
impl QuotaStore for RedbStorage {
    async fn incr_quota(
        &self,
        principal_id: &str,
        window_start: u64,
        kind: ApiBucketKind,
        amount: u64,
    ) -> StorageResult<u64> {
        let storage = self.clone();
        let principal_id = principal_id.to_owned();
        let kind = to_redb_bucket_kind(kind);

        tokio::task::spawn_blocking(move || {
            RedbStorage::incr_quota(&storage, &principal_id, window_start, kind, amount)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn try_incr_quota(
        &self,
        principal_id: &str,
        window_start: u64,
        kind: ApiBucketKind,
        amount: u64,
        capacity: u64,
    ) -> StorageResult<Option<u64>> {
        let storage = self.clone();
        let principal_id = principal_id.to_owned();
        let kind = to_redb_bucket_kind(kind);

        tokio::task::spawn_blocking(move || {
            RedbStorage::try_incr_quota(
                &storage,
                &principal_id,
                window_start,
                kind,
                amount,
                capacity,
            )
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn get_quota(
        &self,
        principal_id: &str,
        window_start: u64,
        kind: ApiBucketKind,
    ) -> StorageResult<u64> {
        let storage = self.clone();
        let principal_id = principal_id.to_owned();
        let kind = to_redb_bucket_kind(kind);

        tokio::task::spawn_blocking(move || {
            RedbStorage::get_quota(&storage, &principal_id, window_start, kind)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn adjust_quota(
        &self,
        principal_id: &str,
        window_start: u64,
        kind: ApiBucketKind,
        delta: i64,
    ) -> StorageResult<u64> {
        let storage = self.clone();
        let principal_id = principal_id.to_owned();
        let kind = to_redb_bucket_kind(kind);

        tokio::task::spawn_blocking(move || {
            RedbStorage::adjust_quota(&storage, &principal_id, window_start, kind, delta)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn sweep_old_quotas(&self, older_than_window_start: u64) -> StorageResult<u64> {
        let storage = self.clone();

        tokio::task::spawn_blocking(move || {
            RedbStorage::sweep_old_quotas(&storage, older_than_window_start)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }
}

fn to_redb_bucket_kind(kind: ApiBucketKind) -> RedbBucketKind {
    match kind {
        ApiBucketKind::Requests => RedbBucketKind::Requests,
        ApiBucketKind::InputTokens => RedbBucketKind::InputTokens,
        ApiBucketKind::OutputTokens => RedbBucketKind::OutputTokens,
    }
}
