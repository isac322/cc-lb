use async_trait::async_trait;
use cc_lb_storage_api::{ApiKeyStore, StorageResult};
use sqlx::Row;

use crate::{adapter::PostgresStorage, error_map::map_sqlx_error};

#[async_trait]
impl ApiKeyStore for PostgresStorage {
    async fn put_api_key_ciphertext(
        &self,
        principal_id: &str,
        key_id: &str,
        ciphertext: &[u8],
    ) -> StorageResult<()> {
        sqlx::query(
            "INSERT INTO api_keys_v1 (principal_id, key_id, ciphertext, revision, created_at, updated_at) VALUES ($1, $2, $3, 0, NOW(), NOW()) ON CONFLICT (principal_id, key_id) DO UPDATE SET ciphertext = $3, updated_at = NOW(), revision = api_keys_v1.revision + 1",
        )
        .bind(principal_id)
        .bind(key_id)
        .bind(ciphertext)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        Ok(())
    }

    async fn get_api_key_ciphertext(
        &self,
        principal_id: &str,
        key_id: &str,
    ) -> StorageResult<Option<Vec<u8>>> {
        let row = match sqlx::query(
            "SELECT ciphertext FROM api_keys_v1 WHERE principal_id = $1 AND key_id = $2",
        )
        .bind(principal_id)
        .bind(key_id)
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

    async fn list_api_key_ciphertexts(
        &self,
        principal_id: &str,
    ) -> StorageResult<Vec<(String, Vec<u8>)>> {
        let rows = sqlx::query(
            "SELECT key_id, ciphertext FROM api_keys_v1 WHERE principal_id = $1 ORDER BY key_id ASC",
        )
        .bind(principal_id)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        rows.into_iter()
            .map(|row| {
                Ok((
                    row.try_get::<String, _>("key_id").map_err(map_sqlx_error)?,
                    row.try_get::<Vec<u8>, _>("ciphertext")
                        .map_err(map_sqlx_error)?,
                ))
            })
            .collect()
    }

    async fn revoke_api_key(
        &self,
        principal_id: &str,
        key_id: &str,
        revoked_ciphertext: &[u8],
    ) -> StorageResult<bool> {
        let result = sqlx::query(
            "UPDATE api_keys_v1 SET ciphertext = $3, updated_at = NOW(), revision = revision + 1 WHERE principal_id = $1 AND key_id = $2",
        )
        .bind(principal_id)
        .bind(key_id)
        .bind(revoked_ciphertext)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        Ok(result.rows_affected() == 1)
    }
}
