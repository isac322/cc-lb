use async_trait::async_trait;
use cc_lb_storage_api::{OAuthCredentialStore, StorageResult};
use sqlx::Row;

use crate::{adapter::PostgresStorage, error_map::map_sqlx_error};

#[async_trait]
impl OAuthCredentialStore for PostgresStorage {
    async fn put_oauth_ciphertext(
        &self,
        principal_id: &str,
        provider: &str,
        ciphertext: &[u8],
    ) -> StorageResult<()> {
        sqlx::query(
            "INSERT INTO oauth_credentials_v1 (principal_id, provider, ciphertext, revision, created_at, updated_at) VALUES ($1, $2, $3, 0, NOW(), NOW()) ON CONFLICT (principal_id, provider) DO UPDATE SET ciphertext = $3, updated_at = NOW(), revision = oauth_credentials_v1.revision + 1",
        )
        .bind(principal_id)
        .bind(provider)
        .bind(ciphertext)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        Ok(())
    }

    async fn get_oauth_ciphertext(
        &self,
        principal_id: &str,
        provider: &str,
    ) -> StorageResult<Option<Vec<u8>>> {
        let row = match sqlx::query(
            "SELECT ciphertext FROM oauth_credentials_v1 WHERE principal_id = $1 AND provider = $2",
        )
        .bind(principal_id)
        .bind(provider)
        .fetch_one(&self.pool)
        .await
        {
            Ok(row) => row,
            Err(sqlx::Error::RowNotFound) => return Ok(None),
            Err(error) => return Err(map_sqlx_error(error)),
        };

        row.try_get::<Vec<u8>, _>("ciphertext")
            .map(Some)
            .map_err(map_sqlx_error)
    }

    async fn delete_oauth(&self, principal_id: &str, provider: &str) -> StorageResult<bool> {
        let result = sqlx::query(
            "DELETE FROM oauth_credentials_v1 WHERE principal_id = $1 AND provider = $2",
        )
        .bind(principal_id)
        .bind(provider)
        .execute(&self.pool)
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
            "INSERT INTO anthropic_api_keys_v1 (storage_key, ciphertext, created_at, updated_at) VALUES ($1, $2, NOW(), NOW()) ON CONFLICT (storage_key) DO UPDATE SET ciphertext = $2, updated_at = NOW()",
        )
        .bind(storage_key)
        .bind(ciphertext)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        Ok(())
    }

    async fn get_anthropic_api_key_ciphertext(
        &self,
        storage_key: &str,
    ) -> StorageResult<Option<Vec<u8>>> {
        let row = match sqlx::query(
            "SELECT ciphertext FROM anthropic_api_keys_v1 WHERE storage_key = $1",
        )
        .bind(storage_key)
        .fetch_one(&self.pool)
        .await
        {
            Ok(row) => row,
            Err(sqlx::Error::RowNotFound) => return Ok(None),
            Err(error) => return Err(map_sqlx_error(error)),
        };

        row.try_get::<Vec<u8>, _>("ciphertext")
            .map(Some)
            .map_err(map_sqlx_error)
    }
}
