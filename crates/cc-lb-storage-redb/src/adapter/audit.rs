use async_trait::async_trait;
use cc_lb_storage_api::{AuditStore, StorageResult, types::AuditEntry};

use crate::RedbStorage;

use super::error_map::{map_join_err, map_redb_err};

#[async_trait]
impl AuditStore for RedbStorage {
    async fn append_audit(&self, entry: &AuditEntry) -> StorageResult<()> {
        let storage = self.clone();
        let entry = entry.clone();

        tokio::task::spawn_blocking(move || RedbStorage::append_audit(&storage, &entry))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn query_audit(
        &self,
        principal_id: Option<&str>,
        since: u64,
        until: u64,
        limit: usize,
    ) -> StorageResult<Vec<AuditEntry>> {
        let storage = self.clone();
        let principal_id = principal_id.map(str::to_owned);

        tokio::task::spawn_blocking(move || {
            RedbStorage::query_audit(&storage, principal_id.as_deref(), since, until, limit)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn prune_audit(&self, older_than: u64) -> StorageResult<u64> {
        let storage = self.clone();

        tokio::task::spawn_blocking(move || RedbStorage::prune_audit(&storage, older_than))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn prune_audit_before(
        &self,
        cutoff_ts_x_1m: u64,
        batch_size: usize,
    ) -> StorageResult<u64> {
        let storage = self.clone();

        tokio::task::spawn_blocking(move || {
            RedbStorage::prune_audit_before(&storage, cutoff_ts_x_1m, batch_size)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }
}
