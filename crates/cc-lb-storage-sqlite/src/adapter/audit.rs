use async_trait::async_trait;
use cc_lb_storage_api::{AuditEntry, AuditStore, StorageResult};

use crate::SqliteStorage;

#[async_trait]
impl AuditStore for SqliteStorage {
    async fn append_audit(&self, _entry: &AuditEntry) -> StorageResult<()> {
        unimplemented!()
    }

    async fn append_audit_entries(&self, _entries: &[AuditEntry]) -> StorageResult<()> {
        unimplemented!()
    }

    async fn query_audit(
        &self,
        _principal_id: Option<&str>,
        _since: u64,
        _until: u64,
        _limit: usize,
    ) -> StorageResult<Vec<AuditEntry>> {
        unimplemented!()
    }

    async fn prune_audit(&self, _older_than: u64) -> StorageResult<u64> {
        unimplemented!()
    }

    async fn prune_audit_before(
        &self,
        _cutoff_ts_x_1m: u64,
        _batch_size: usize,
    ) -> StorageResult<u64> {
        unimplemented!()
    }
}
