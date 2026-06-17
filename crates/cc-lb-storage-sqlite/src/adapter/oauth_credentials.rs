use async_trait::async_trait;
use cc_lb_storage_api::{OAuthCredentialStore, StorageResult};
use sqlx::Row;

use crate::{SqliteStorage, map_sqlx_error};

const ANTHROPIC_API_KEY_CLIENT_ID: &str = "anthropic_api_key";

#[async_trait]
impl OAuthCredentialStore for SqliteStorage {
    async fn put_oauth_ciphertext(
        &self,
        principal_id: &str,
        provider: &str,
        ciphertext: &[u8],
    ) -> StorageResult<()> {
        sqlx::query(
            "INSERT INTO oauth_credentials_v1 (principal_id, client_id, client_secret_ciphertext, auth_url, token_url, redirect_uri, scopes, created_at, updated_at) VALUES (?, ?, ?, '', '', '', '', unixepoch(), unixepoch()) ON CONFLICT(principal_id) DO UPDATE SET client_id = excluded.client_id, client_secret_ciphertext = excluded.client_secret_ciphertext, updated_at = excluded.updated_at",
        )
        .bind(principal_id)
        .bind(provider)
        .bind(ciphertext)
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        Ok(())
    }

    async fn get_oauth_ciphertext(
        &self,
        principal_id: &str,
        provider: &str,
    ) -> StorageResult<Option<Vec<u8>>> {
        get_oauth_ciphertext_by_client_id(self, principal_id, provider).await
    }

    async fn delete_oauth(&self, principal_id: &str, provider: &str) -> StorageResult<bool> {
        let result = sqlx::query(
            "DELETE FROM oauth_credentials_v1 WHERE principal_id = ? AND client_id = ?",
        )
        .bind(principal_id)
        .bind(provider)
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        Ok(result.rows_affected() == 1)
    }

    async fn put_anthropic_api_key_ciphertext(
        &self,
        storage_key: &str,
        ciphertext: &[u8],
    ) -> StorageResult<()> {
        sqlx::query(
            "INSERT INTO oauth_credentials_v1 (principal_id, client_id, client_secret_ciphertext, auth_url, token_url, redirect_uri, scopes, created_at, updated_at) VALUES (?, ?, ?, '', '', '', '', unixepoch(), unixepoch()) ON CONFLICT(principal_id) DO UPDATE SET client_id = excluded.client_id, client_secret_ciphertext = excluded.client_secret_ciphertext, updated_at = excluded.updated_at",
        )
        .bind(storage_key)
        .bind(ANTHROPIC_API_KEY_CLIENT_ID)
        .bind(ciphertext)
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        Ok(())
    }

    async fn get_anthropic_api_key_ciphertext(
        &self,
        storage_key: &str,
    ) -> StorageResult<Option<Vec<u8>>> {
        get_oauth_ciphertext_by_client_id(self, storage_key, ANTHROPIC_API_KEY_CLIENT_ID).await
    }
}

async fn get_oauth_ciphertext_by_client_id(
    storage: &SqliteStorage,
    principal_id: &str,
    client_id: &str,
) -> StorageResult<Option<Vec<u8>>> {
    let row = match sqlx::query(
        "SELECT client_secret_ciphertext FROM oauth_credentials_v1 WHERE principal_id = ? AND client_id = ?",
    )
    .bind(principal_id)
    .bind(client_id)
    .fetch_one(storage.pool())
    .await
    {
        Ok(row) => row,
        Err(sqlx::Error::RowNotFound) => return Ok(None),
        Err(error) => return Err(map_sqlx_error(error)),
    };

    row.try_get::<Vec<u8>, _>("client_secret_ciphertext")
        .map(Some)
        .map_err(map_sqlx_error)
}
