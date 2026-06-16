use async_trait::async_trait;
use cc_lb_storage_api::{OAuthCredentialStore, StorageResult};

use crate::SqliteStorage;

#[async_trait]
impl OAuthCredentialStore for SqliteStorage {
    async fn put_oauth_ciphertext(
        &self,
        _principal_id: &str,
        _provider: &str,
        _ciphertext: &[u8],
    ) -> StorageResult<()> {
        unimplemented!()
    }

    async fn get_oauth_ciphertext(
        &self,
        _principal_id: &str,
        _provider: &str,
    ) -> StorageResult<Option<Vec<u8>>> {
        unimplemented!()
    }

    async fn delete_oauth(&self, _principal_id: &str, _provider: &str) -> StorageResult<bool> {
        unimplemented!()
    }

    async fn put_anthropic_api_key_ciphertext(
        &self,
        _storage_key: &str,
        _ciphertext: &[u8],
    ) -> StorageResult<()> {
        unimplemented!()
    }

    async fn get_anthropic_api_key_ciphertext(
        &self,
        _storage_key: &str,
    ) -> StorageResult<Option<Vec<u8>>> {
        unimplemented!()
    }
}
