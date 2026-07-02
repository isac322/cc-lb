use async_trait::async_trait;
use cc_lb_storage_api::{PluginBlobRepo, RepoError, StorageError};
use sqlx::PgPool;

use crate::{adapter::PostgresStorage, error_map::map_sqlx_error};

#[derive(Debug, Clone)]
pub struct PostgresPluginBlobRepo {
    pool: PgPool,
}

impl PostgresPluginBlobRepo {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn from_storage(storage: &PostgresStorage) -> Self {
        Self::new(storage.pool.clone())
    }
}

#[async_trait]
impl PluginBlobRepo for PostgresPluginBlobRepo {
    async fn put_blob(&self, sha256: &[u8; 32], bytes: &[u8]) -> Result<(), RepoError> {
        sqlx::query(
            "INSERT INTO plugin_registry_blobs (sha256, bytes) VALUES ($1, $2)
             ON CONFLICT (sha256) DO UPDATE SET bytes = EXCLUDED.bytes",
        )
        .bind(sha256.as_slice())
        .bind(bytes)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        Ok(())
    }

    async fn get_blob(&self, sha256: &[u8; 32]) -> Result<Option<Vec<u8>>, RepoError> {
        sqlx::query_scalar("SELECT bytes FROM plugin_registry_blobs WHERE sha256 = $1")
            .bind(sha256.as_slice())
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_error)
    }

    async fn delete_blob(&self, sha256: &[u8; 32]) -> Result<(), RepoError> {
        sqlx::query("DELETE FROM plugin_registry_blobs WHERE sha256 = $1")
            .bind(sha256.as_slice())
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_error)?;

        Ok(())
    }

    async fn list_blob_keys(&self) -> Result<Vec<[u8; 32]>, RepoError> {
        let rows = sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT sha256 FROM plugin_registry_blobs ORDER BY sha256 ASC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        rows.into_iter().map(|row| sha_to_array(&row)).collect()
    }
}

fn sha_to_array(bytes: &[u8]) -> Result<[u8; 32], RepoError> {
    <[u8; 32]>::try_from(bytes).map_err(|_| StorageError::Corrupted {
        message: "sha256 must be 32 bytes".to_owned(),
    })
}

#[async_trait]
impl PluginBlobRepo for PostgresStorage {
    async fn put_blob(&self, sha256: &[u8; 32], bytes: &[u8]) -> Result<(), RepoError> {
        PostgresPluginBlobRepo::from_storage(self)
            .put_blob(sha256, bytes)
            .await
    }

    async fn get_blob(&self, sha256: &[u8; 32]) -> Result<Option<Vec<u8>>, RepoError> {
        PostgresPluginBlobRepo::from_storage(self)
            .get_blob(sha256)
            .await
    }

    async fn delete_blob(&self, sha256: &[u8; 32]) -> Result<(), RepoError> {
        PostgresPluginBlobRepo::from_storage(self)
            .delete_blob(sha256)
            .await
    }

    async fn list_blob_keys(&self) -> Result<Vec<[u8; 32]>, RepoError> {
        PostgresPluginBlobRepo::from_storage(self)
            .list_blob_keys()
            .await
    }
}
