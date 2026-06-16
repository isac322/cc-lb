use async_trait::async_trait;
use cc_lb_storage_api::{ApiKeyStore, StorageResult};
use sqlx::Row;

use crate::{SqliteStorage, map_sqlx_error};

#[async_trait]
impl ApiKeyStore for SqliteStorage {
    async fn put_api_key_ciphertext(
        &self,
        principal_id: &str,
        key_id: &str,
        ciphertext: &[u8],
    ) -> StorageResult<()> {
        sqlx::query(
            "INSERT INTO api_keys_v1 (id, principal_id, name, secret_hash, created_at, expires_at, status) VALUES (?, ?, ?, ?, unixepoch(), NULL, 'active') ON CONFLICT(id) DO UPDATE SET principal_id = excluded.principal_id, name = excluded.name, secret_hash = excluded.secret_hash, status = 'active'",
        )
        .bind(key_id)
        .bind(principal_id)
        .bind(key_id)
        .bind(ciphertext)
        .execute(self.pool())
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
            "SELECT secret_hash FROM api_keys_v1 WHERE principal_id = ? AND id = ?",
        )
        .bind(principal_id)
        .bind(key_id)
        .fetch_one(self.pool())
        .await
        {
            Ok(row) => row,
            Err(sqlx::Error::RowNotFound) => return Ok(None),
            Err(error) => return Err(map_sqlx_error(error)),
        };

        row.try_get::<Vec<u8>, _>("secret_hash")
            .map(Some)
            .map_err(map_sqlx_error)
    }

    async fn list_api_key_ciphertexts(
        &self,
        principal_id: &str,
    ) -> StorageResult<Vec<(String, Vec<u8>)>> {
        let rows = sqlx::query(
            "SELECT id, secret_hash FROM api_keys_v1 WHERE principal_id = ? ORDER BY id ASC",
        )
        .bind(principal_id)
        .fetch_all(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        rows.into_iter()
            .map(|row| {
                Ok((
                    row.try_get::<String, _>("id").map_err(map_sqlx_error)?,
                    row.try_get::<Vec<u8>, _>("secret_hash")
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
            "UPDATE api_keys_v1 SET secret_hash = ?, status = 'revoked' WHERE principal_id = ? AND id = ?",
        )
        .bind(revoked_ciphertext)
        .bind(principal_id)
        .bind(key_id)
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        Ok(result.rows_affected() == 1)
    }
}
