use async_trait::async_trait;
use cc_lb_storage_api::{
    PrincipalCreate, PrincipalRecord, PrincipalStore, PrincipalUpdate, StorageResult,
};
use uuid::Uuid;

use crate::SqliteStorage;

#[async_trait]
impl PrincipalStore for SqliteStorage {
    async fn create(
        &self,
        _input: PrincipalCreate,
        _now_unix_secs: u64,
    ) -> StorageResult<PrincipalRecord> {
        unimplemented!()
    }

    async fn get_by_id(&self, _id: Uuid) -> StorageResult<Option<PrincipalRecord>> {
        unimplemented!()
    }

    async fn get_by_name(&self, _name: &str) -> StorageResult<Option<PrincipalRecord>> {
        unimplemented!()
    }

    async fn list(
        &self,
        _offset: usize,
        _limit: usize,
        _include_deleted: bool,
    ) -> StorageResult<Vec<PrincipalRecord>> {
        unimplemented!()
    }

    async fn update(
        &self,
        _id: Uuid,
        _expected_revision: u64,
        _update: PrincipalUpdate,
        _now_unix_secs: u64,
    ) -> StorageResult<Option<PrincipalRecord>> {
        unimplemented!()
    }

    async fn set_enabled(
        &self,
        _id: Uuid,
        _expected_revision: u64,
        _enabled: bool,
        _now_unix_secs: u64,
    ) -> StorageResult<Option<PrincipalRecord>> {
        unimplemented!()
    }

    async fn soft_delete(
        &self,
        _id: Uuid,
        _expected_revision: u64,
        _now_unix_secs: u64,
    ) -> StorageResult<Option<PrincipalRecord>> {
        unimplemented!()
    }

    async fn hard_delete(&self, _id: Uuid) -> StorageResult<bool> {
        unimplemented!()
    }

    async fn set_last_apply_error(
        &self,
        _id: Uuid,
        _expected_revision: u64,
        _error: Option<String>,
        _applied_at_unix_secs: u64,
    ) -> StorageResult<Option<PrincipalRecord>> {
        unimplemented!()
    }
}
