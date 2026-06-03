use async_trait::async_trait;
use cc_lb_storage_api::{
    AugmentedMetadata, PluginBlobRepo, PluginRegistryRecord, PluginRegistryRepo,
    PluginRegistryStatus, RepoError, StorageError,
};
use sqlx::{PgPool, Row, postgres::PgRow};

use crate::{adapter::PostgresStorage, error_map::map_sqlx_error};

const SHUTDOWN_MARKER_KEY: &str = "shutdown";

#[derive(Debug, Clone)]
pub struct PostgresPluginRegistryRepo {
    pool: PgPool,
}

impl PostgresPluginRegistryRepo {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn from_storage(storage: &PostgresStorage) -> Self {
        Self::new(storage.pool.clone())
    }
}

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
impl PluginRegistryRepo for PostgresPluginRegistryRepo {
    async fn upsert_record(&self, record: &PluginRegistryRecord) -> Result<(), RepoError> {
        let metadata = serde_json::to_vec(&record.augmented_metadata)?;
        sqlx::query(
            "INSERT INTO plugin_registry (
                sha256,
                plugin_name,
                plugin_version,
                abi_envelope,
                augmented_metadata,
                host_offer_hash,
                handshake_schema_version,
                last_handshake_at,
                status
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (sha256) DO UPDATE SET
                plugin_name = EXCLUDED.plugin_name,
                plugin_version = EXCLUDED.plugin_version,
                abi_envelope = EXCLUDED.abi_envelope,
                augmented_metadata = EXCLUDED.augmented_metadata,
                host_offer_hash = EXCLUDED.host_offer_hash,
                handshake_schema_version = EXCLUDED.handshake_schema_version,
                last_handshake_at = EXCLUDED.last_handshake_at,
                status = EXCLUDED.status",
        )
        .bind(record.sha256.as_slice())
        .bind(&record.plugin_name)
        .bind(&record.plugin_version)
        .bind(i64::from(record.abi_envelope))
        .bind(metadata)
        .bind(record.host_offer_hash.as_slice())
        .bind(i64::from(record.handshake_schema_version))
        .bind(record.last_handshake_at)
        .bind(status_as_str(record.status))
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        Ok(())
    }

    async fn get_by_sha256(
        &self,
        sha256: &[u8; 32],
    ) -> Result<Option<PluginRegistryRecord>, RepoError> {
        let row = sqlx::query("SELECT * FROM plugin_registry WHERE sha256 = $1")
            .bind(sha256.as_slice())
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_error)?;

        row.map(record_from_row).transpose()
    }

    async fn list_active(&self) -> Result<Vec<PluginRegistryRecord>, RepoError> {
        let rows = sqlx::query(
            "SELECT * FROM plugin_registry WHERE status = 'active' ORDER BY plugin_name ASC, sha256 ASC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        rows.into_iter().map(record_from_row).collect()
    }

    async fn set_status(
        &self,
        sha256: &[u8; 32],
        status: PluginRegistryStatus,
    ) -> Result<(), RepoError> {
        sqlx::query("UPDATE plugin_registry SET status = $2 WHERE sha256 = $1")
            .bind(sha256.as_slice())
            .bind(status_as_str(status))
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_error)?;

        Ok(())
    }

    async fn delete_by_sha256(&self, sha256: &[u8; 32]) -> Result<(), RepoError> {
        sqlx::query("DELETE FROM plugin_registry WHERE sha256 = $1")
            .bind(sha256.as_slice())
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_error)?;

        Ok(())
    }

    async fn count(&self) -> Result<usize, RepoError> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM plugin_registry")
            .fetch_one(&self.pool)
            .await
            .map_err(map_sqlx_error)?;

        usize::try_from(count).map_err(|_| StorageError::Corrupted {
            message: "plugin_registry count cannot fit usize".to_owned(),
        })
    }

    async fn get_shutdown_marker(&self) -> Result<Option<i64>, RepoError> {
        sqlx::query_scalar("SELECT unix_secs FROM plugin_registry_marker WHERE key = $1")
            .bind(SHUTDOWN_MARKER_KEY)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_error)
    }

    async fn set_shutdown_marker(&self, unix_secs: i64) -> Result<(), RepoError> {
        sqlx::query(
            "INSERT INTO plugin_registry_marker (key, unix_secs) VALUES ($1, $2)
             ON CONFLICT (key) DO UPDATE SET unix_secs = EXCLUDED.unix_secs",
        )
        .bind(SHUTDOWN_MARKER_KEY)
        .bind(unix_secs)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        Ok(())
    }

    async fn clear_shutdown_marker(&self) -> Result<(), RepoError> {
        sqlx::query("DELETE FROM plugin_registry_marker WHERE key = $1")
            .bind(SHUTDOWN_MARKER_KEY)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_error)?;

        Ok(())
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

fn record_from_row(row: PgRow) -> Result<PluginRegistryRecord, RepoError> {
    let metadata_bytes: Vec<u8> = row.try_get("augmented_metadata").map_err(map_sqlx_error)?;
    let augmented_metadata = serde_json::from_slice::<AugmentedMetadata>(&metadata_bytes)?;
    let status = parse_status(&row.try_get::<String, _>("status").map_err(map_sqlx_error)?)?;

    Ok(PluginRegistryRecord {
        sha256: sha_to_array(
            &row.try_get::<Vec<u8>, _>("sha256")
                .map_err(map_sqlx_error)?,
        )?,
        plugin_name: row.try_get("plugin_name").map_err(map_sqlx_error)?,
        plugin_version: row.try_get("plugin_version").map_err(map_sqlx_error)?,
        abi_envelope: u32_from_i64(
            row.try_get("abi_envelope").map_err(map_sqlx_error)?,
            "abi_envelope",
        )?,
        augmented_metadata,
        host_offer_hash: sha_to_array(
            &row.try_get::<Vec<u8>, _>("host_offer_hash")
                .map_err(map_sqlx_error)?,
        )?,
        handshake_schema_version: u32_from_i64(
            row.try_get("handshake_schema_version")
                .map_err(map_sqlx_error)?,
            "handshake_schema_version",
        )?,
        last_handshake_at: row.try_get("last_handshake_at").map_err(map_sqlx_error)?,
        status,
    })
}

fn sha_to_array(bytes: &[u8]) -> Result<[u8; 32], RepoError> {
    <[u8; 32]>::try_from(bytes).map_err(|_| StorageError::Corrupted {
        message: "sha256 must be 32 bytes".to_owned(),
    })
}

fn u32_from_i64(value: i64, field: &str) -> Result<u32, RepoError> {
    u32::try_from(value).map_err(|_| StorageError::Corrupted {
        message: format!("{field} is outside u32 range"),
    })
}

fn status_as_str(status: PluginRegistryStatus) -> &'static str {
    match status {
        PluginRegistryStatus::Active => "active",
        PluginRegistryStatus::Disabled => "disabled",
    }
}

fn parse_status(value: &str) -> Result<PluginRegistryStatus, RepoError> {
    match value {
        "active" => Ok(PluginRegistryStatus::Active),
        "disabled" => Ok(PluginRegistryStatus::Disabled),
        value => Err(StorageError::Corrupted {
            message: format!("invalid plugin registry status {value}"),
        }),
    }
}

#[async_trait]
impl PluginRegistryRepo for PostgresStorage {
    async fn upsert_record(&self, record: &PluginRegistryRecord) -> Result<(), RepoError> {
        PostgresPluginRegistryRepo::from_storage(self)
            .upsert_record(record)
            .await
    }

    async fn get_by_sha256(
        &self,
        sha256: &[u8; 32],
    ) -> Result<Option<PluginRegistryRecord>, RepoError> {
        PostgresPluginRegistryRepo::from_storage(self)
            .get_by_sha256(sha256)
            .await
    }

    async fn list_active(&self) -> Result<Vec<PluginRegistryRecord>, RepoError> {
        PostgresPluginRegistryRepo::from_storage(self)
            .list_active()
            .await
    }

    async fn set_status(
        &self,
        sha256: &[u8; 32],
        status: PluginRegistryStatus,
    ) -> Result<(), RepoError> {
        PostgresPluginRegistryRepo::from_storage(self)
            .set_status(sha256, status)
            .await
    }

    async fn delete_by_sha256(&self, sha256: &[u8; 32]) -> Result<(), RepoError> {
        PostgresPluginRegistryRepo::from_storage(self)
            .delete_by_sha256(sha256)
            .await
    }

    async fn count(&self) -> Result<usize, RepoError> {
        PostgresPluginRegistryRepo::from_storage(self).count().await
    }

    async fn get_shutdown_marker(&self) -> Result<Option<i64>, RepoError> {
        PostgresPluginRegistryRepo::from_storage(self)
            .get_shutdown_marker()
            .await
    }

    async fn set_shutdown_marker(&self, unix_secs: i64) -> Result<(), RepoError> {
        PostgresPluginRegistryRepo::from_storage(self)
            .set_shutdown_marker(unix_secs)
            .await
    }

    async fn clear_shutdown_marker(&self) -> Result<(), RepoError> {
        PostgresPluginRegistryRepo::from_storage(self)
            .clear_shutdown_marker()
            .await
    }
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
