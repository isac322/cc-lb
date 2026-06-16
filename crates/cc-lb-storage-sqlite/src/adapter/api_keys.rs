use async_trait::async_trait;
use cc_lb_storage_api::{ApiKeyStore, StorageResult};

use crate::SqliteStorage;

#[async_trait]
impl ApiKeyStore for SqliteStorage {
    async fn put_api_key_ciphertext(
        &self,
        _principal_id: &str,
        _key_id: &str,
        _ciphertext: &[u8],
    ) -> StorageResult<()> {
        unimplemented!()
    }

    async fn get_api_key_ciphertext(
        &self,
        _principal_id: &str,
        _key_id: &str,
    ) -> StorageResult<Option<Vec<u8>>> {
        unimplemented!()
    }

    async fn list_api_key_ciphertexts(
        &self,
        _principal_id: &str,
    ) -> StorageResult<Vec<(String, Vec<u8>)>> {
        unimplemented!()
    }

    async fn revoke_api_key(
        &self,
        _principal_id: &str,
        _key_id: &str,
        _revoked_ciphertext: &[u8],
    ) -> StorageResult<bool> {
        unimplemented!()
    }
}
