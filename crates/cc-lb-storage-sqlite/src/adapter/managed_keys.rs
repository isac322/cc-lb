use async_trait::async_trait;
use cc_lb_storage_api::{
    ApiKeyMutation, IssueParams, ManagedKeyStore, StorageResult, StoredApiKeyRecord,
};

use crate::SqliteStorage;

#[async_trait]
impl ManagedKeyStore for SqliteStorage {
    async fn issue(
        &self,
        _principal_id: &str,
        _key_id: &str,
        _params: IssueParams,
    ) -> StorageResult<StoredApiKeyRecord> {
        unimplemented!()
    }

    async fn get(
        &self,
        _principal_id: &str,
        _key_id: &str,
    ) -> StorageResult<Option<StoredApiKeyRecord>> {
        unimplemented!()
    }

    async fn lookup_by_index_hash(
        &self,
        _index_hash: &[u8; 32],
    ) -> StorageResult<Option<(String, String, StoredApiKeyRecord)>> {
        unimplemented!()
    }

    async fn list_by_principal(
        &self,
        _principal_id: &str,
    ) -> StorageResult<Vec<StoredApiKeyRecord>> {
        unimplemented!()
    }

    async fn list_all(&self) -> StorageResult<Vec<(String, String, StoredApiKeyRecord)>> {
        unimplemented!()
    }

    async fn update(
        &self,
        _principal_id: &str,
        _key_id: &str,
        _mutation: ApiKeyMutation,
    ) -> StorageResult<()> {
        unimplemented!()
    }

    async fn revoke_zero_secrets(&self, _principal_id: &str, _key_id: &str) -> StorageResult<()> {
        unimplemented!()
    }
}
