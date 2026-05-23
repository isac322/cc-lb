use async_trait::async_trait;
use cc_lb_storage_api::{ApiKeyStore, OAuthCredentialStore, StorageResult};

use crate::Storage;

use super::error_map::{map_join_err, map_redb_err};

#[async_trait]
impl OAuthCredentialStore for Storage {
    async fn put_oauth_ciphertext(
        &self,
        principal_id: &str,
        provider: &str,
        ciphertext: &[u8],
    ) -> StorageResult<()> {
        let storage = self.clone();
        let principal_id = principal_id.to_owned();
        let provider = provider.to_owned();
        let ciphertext = ciphertext.to_vec();

        tokio::task::spawn_blocking(move || {
            Storage::put_oauth_ciphertext(&storage, &principal_id, &provider, &ciphertext)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn get_oauth_ciphertext(
        &self,
        principal_id: &str,
        provider: &str,
    ) -> StorageResult<Option<Vec<u8>>> {
        let storage = self.clone();
        let principal_id = principal_id.to_owned();
        let provider = provider.to_owned();

        tokio::task::spawn_blocking(move || {
            Storage::get_oauth_ciphertext(&storage, &principal_id, &provider)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn delete_oauth(&self, principal_id: &str, provider: &str) -> StorageResult<bool> {
        let storage = self.clone();
        let principal_id = principal_id.to_owned();
        let provider = provider.to_owned();

        tokio::task::spawn_blocking(move || {
            Storage::delete_oauth_ciphertext(&storage, &principal_id, &provider)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn put_anthropic_api_key_ciphertext(
        &self,
        storage_key: &str,
        ciphertext: &[u8],
    ) -> StorageResult<()> {
        let storage = self.clone();
        let storage_key = storage_key.to_owned();
        let ciphertext = ciphertext.to_vec();

        tokio::task::spawn_blocking(move || {
            Storage::put_anthropic_api_key_ciphertext(&storage, &storage_key, &ciphertext)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn get_anthropic_api_key_ciphertext(
        &self,
        storage_key: &str,
    ) -> StorageResult<Option<Vec<u8>>> {
        let storage = self.clone();
        let storage_key = storage_key.to_owned();

        tokio::task::spawn_blocking(move || {
            Storage::get_anthropic_api_key_ciphertext(&storage, &storage_key)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }
}

#[async_trait]
impl ApiKeyStore for Storage {
    async fn put_api_key_ciphertext(
        &self,
        principal_id: &str,
        key_id: &str,
        ciphertext: &[u8],
    ) -> StorageResult<()> {
        let storage = self.clone();
        let principal_id = principal_id.to_owned();
        let key_id = key_id.to_owned();
        let ciphertext = ciphertext.to_vec();

        tokio::task::spawn_blocking(move || {
            Storage::put_api_key_ciphertext(&storage, &principal_id, &key_id, &ciphertext)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn get_api_key_ciphertext(
        &self,
        principal_id: &str,
        key_id: &str,
    ) -> StorageResult<Option<Vec<u8>>> {
        let storage = self.clone();
        let principal_id = principal_id.to_owned();
        let key_id = key_id.to_owned();

        tokio::task::spawn_blocking(move || {
            Storage::get_api_key_ciphertext(&storage, &principal_id, &key_id)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn list_api_key_ciphertexts(
        &self,
        principal_id: &str,
    ) -> StorageResult<Vec<(String, Vec<u8>)>> {
        let storage = self.clone();
        let principal_id = principal_id.to_owned();

        tokio::task::spawn_blocking(move || {
            Storage::list_api_key_ciphertexts(&storage, &principal_id)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn revoke_api_key(
        &self,
        principal_id: &str,
        key_id: &str,
        revoked_ciphertext: &[u8],
    ) -> StorageResult<bool> {
        let storage = self.clone();
        let principal_id = principal_id.to_owned();
        let key_id = key_id.to_owned();
        let revoked_ciphertext = revoked_ciphertext.to_vec();

        tokio::task::spawn_blocking(move || {
            Storage::revoke_api_key_ciphertext(
                &storage,
                &principal_id,
                &key_id,
                &revoked_ciphertext,
            )
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }
}
