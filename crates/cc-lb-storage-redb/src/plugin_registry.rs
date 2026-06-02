use async_trait::async_trait;
use cc_lb_storage_api::{
    PluginBlobRepo, PluginRegistryRecord, PluginRegistryRepo, PluginRegistryStatus, RepoError,
};
use redb::ReadableTable;

use crate::adapter::error_map::{map_join_err, map_redb_err};
use crate::{PLUGIN_BLOBS, PLUGIN_REGISTRY, PLUGIN_REGISTRY_MARKER, RedbStorage, StorageError};

const SHUTDOWN_MARKER_KEY: &str = "shutdown_marker";

#[derive(Clone)]
pub struct RedbPluginRegistryRepo {
    storage: RedbStorage,
}

impl RedbPluginRegistryRepo {
    pub fn new(storage: RedbStorage) -> Result<Self, StorageError> {
        ensure_tables(&storage)?;
        Ok(Self { storage })
    }

    fn upsert_record_sync(&self, record: &PluginRegistryRecord) -> Result<(), StorageError> {
        let write_txn = self.storage.begin_write()?;
        {
            let mut registry = write_txn.open_table(PLUGIN_REGISTRY)?;
            let payload = serde_json::to_vec(record)?;
            registry.insert(record.sha256.as_slice(), payload.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    fn get_by_sha256_sync(
        &self,
        sha256: [u8; 32],
    ) -> Result<Option<PluginRegistryRecord>, StorageError> {
        let read_txn = self.storage.begin_read()?;
        let registry = read_txn.open_table(PLUGIN_REGISTRY)?;
        registry
            .get(sha256.as_slice())?
            .map(|stored| serde_json::from_slice(stored.value()).map_err(StorageError::from))
            .transpose()
    }

    fn list_active_sync(&self) -> Result<Vec<PluginRegistryRecord>, StorageError> {
        let read_txn = self.storage.begin_read()?;
        let registry = read_txn.open_table(PLUGIN_REGISTRY)?;
        let mut records = Vec::new();

        for row in registry.iter()? {
            let (_, value) = row?;
            let record: PluginRegistryRecord = serde_json::from_slice(value.value())?;
            if record.status == PluginRegistryStatus::Active {
                records.push(record);
            }
        }

        records.sort_by_key(|record| record.sha256);
        Ok(records)
    }

    fn set_status_sync(
        &self,
        sha256: [u8; 32],
        status: PluginRegistryStatus,
    ) -> Result<(), StorageError> {
        let write_txn = self.storage.begin_write()?;
        {
            let mut registry = write_txn.open_table(PLUGIN_REGISTRY)?;
            let existing: Option<PluginRegistryRecord> = registry
                .get(sha256.as_slice())?
                .map(|stored| serde_json::from_slice(stored.value()).map_err(StorageError::from))
                .transpose()?;
            if let Some(mut record) = existing {
                record.status = status;
                let payload = serde_json::to_vec(&record)?;
                registry.insert(sha256.as_slice(), payload.as_slice())?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    fn delete_by_sha256_sync(&self, sha256: [u8; 32]) -> Result<(), StorageError> {
        let write_txn = self.storage.begin_write()?;
        {
            let mut registry = write_txn.open_table(PLUGIN_REGISTRY)?;
            registry.remove(sha256.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    fn count_sync(&self) -> Result<usize, StorageError> {
        let read_txn = self.storage.begin_read()?;
        let registry = read_txn.open_table(PLUGIN_REGISTRY)?;
        let mut count = 0;
        for row in registry.iter()? {
            row?;
            count += 1;
        }
        Ok(count)
    }

    fn get_shutdown_marker_sync(&self) -> Result<Option<i64>, StorageError> {
        let read_txn = self.storage.begin_read()?;
        let marker = read_txn.open_table(PLUGIN_REGISTRY_MARKER)?;
        Ok(marker
            .get(SHUTDOWN_MARKER_KEY)?
            .map(|stored| stored.value()))
    }

    fn set_shutdown_marker_sync(&self, unix_secs: i64) -> Result<(), StorageError> {
        let write_txn = self.storage.begin_write()?;
        {
            let mut marker = write_txn.open_table(PLUGIN_REGISTRY_MARKER)?;
            marker.insert(SHUTDOWN_MARKER_KEY, &unix_secs)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    fn clear_shutdown_marker_sync(&self) -> Result<(), StorageError> {
        let write_txn = self.storage.begin_write()?;
        {
            let mut marker = write_txn.open_table(PLUGIN_REGISTRY_MARKER)?;
            marker.remove(SHUTDOWN_MARKER_KEY)?;
        }
        write_txn.commit()?;
        Ok(())
    }
}

#[async_trait]
impl PluginRegistryRepo for RedbPluginRegistryRepo {
    async fn upsert_record(&self, record: &PluginRegistryRecord) -> Result<(), RepoError> {
        let repo = self.clone();
        let record = record.clone();
        tokio::task::spawn_blocking(move || repo.upsert_record_sync(&record))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn get_by_sha256(
        &self,
        sha256: &[u8; 32],
    ) -> Result<Option<PluginRegistryRecord>, RepoError> {
        let repo = self.clone();
        let sha256 = *sha256;
        tokio::task::spawn_blocking(move || repo.get_by_sha256_sync(sha256))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn list_active(&self) -> Result<Vec<PluginRegistryRecord>, RepoError> {
        let repo = self.clone();
        tokio::task::spawn_blocking(move || repo.list_active_sync())
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn set_status(
        &self,
        sha256: &[u8; 32],
        status: PluginRegistryStatus,
    ) -> Result<(), RepoError> {
        let repo = self.clone();
        let sha256 = *sha256;
        tokio::task::spawn_blocking(move || repo.set_status_sync(sha256, status))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn delete_by_sha256(&self, sha256: &[u8; 32]) -> Result<(), RepoError> {
        let repo = self.clone();
        let sha256 = *sha256;
        tokio::task::spawn_blocking(move || repo.delete_by_sha256_sync(sha256))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn count(&self) -> Result<usize, RepoError> {
        let repo = self.clone();
        tokio::task::spawn_blocking(move || repo.count_sync())
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn get_shutdown_marker(&self) -> Result<Option<i64>, RepoError> {
        let repo = self.clone();
        tokio::task::spawn_blocking(move || repo.get_shutdown_marker_sync())
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn set_shutdown_marker(&self, unix_secs: i64) -> Result<(), RepoError> {
        let repo = self.clone();
        tokio::task::spawn_blocking(move || repo.set_shutdown_marker_sync(unix_secs))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn clear_shutdown_marker(&self) -> Result<(), RepoError> {
        let repo = self.clone();
        tokio::task::spawn_blocking(move || repo.clear_shutdown_marker_sync())
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }
}

#[derive(Clone)]
pub struct RedbPluginBlobRepo {
    storage: RedbStorage,
}

impl RedbPluginBlobRepo {
    pub fn new(storage: RedbStorage) -> Result<Self, StorageError> {
        ensure_tables(&storage)?;
        Ok(Self { storage })
    }

    fn put_blob_sync(&self, sha256: [u8; 32], bytes: &[u8]) -> Result<(), StorageError> {
        let write_txn = self.storage.begin_write()?;
        {
            let mut blobs = write_txn.open_table(PLUGIN_BLOBS)?;
            blobs.insert(sha256.as_slice(), bytes)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    fn get_blob_sync(&self, sha256: [u8; 32]) -> Result<Option<Vec<u8>>, StorageError> {
        let read_txn = self.storage.begin_read()?;
        let blobs = read_txn.open_table(PLUGIN_BLOBS)?;
        Ok(blobs
            .get(sha256.as_slice())?
            .map(|stored| stored.value().to_vec()))
    }

    fn delete_blob_sync(&self, sha256: [u8; 32]) -> Result<(), StorageError> {
        let write_txn = self.storage.begin_write()?;
        {
            let mut blobs = write_txn.open_table(PLUGIN_BLOBS)?;
            blobs.remove(sha256.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    fn list_blob_keys_sync(&self) -> Result<Vec<[u8; 32]>, StorageError> {
        let read_txn = self.storage.begin_read()?;
        let blobs = read_txn.open_table(PLUGIN_BLOBS)?;
        let mut keys = Vec::new();

        for row in blobs.iter()? {
            let (key, _) = row?;
            let sha256 = <[u8; 32]>::try_from(key.value()).map_err(|_| {
                StorageError::PluginRegistryConflict {
                    message: "plugin blob sha256 key must be 32 bytes".to_owned(),
                }
            })?;
            keys.push(sha256);
        }

        keys.sort();
        Ok(keys)
    }
}

#[async_trait]
impl PluginBlobRepo for RedbPluginBlobRepo {
    async fn put_blob(&self, sha256: &[u8; 32], bytes: &[u8]) -> Result<(), RepoError> {
        let repo = self.clone();
        let sha256 = *sha256;
        let bytes = bytes.to_vec();
        tokio::task::spawn_blocking(move || repo.put_blob_sync(sha256, &bytes))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn get_blob(&self, sha256: &[u8; 32]) -> Result<Option<Vec<u8>>, RepoError> {
        let repo = self.clone();
        let sha256 = *sha256;
        tokio::task::spawn_blocking(move || repo.get_blob_sync(sha256))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn delete_blob(&self, sha256: &[u8; 32]) -> Result<(), RepoError> {
        let repo = self.clone();
        let sha256 = *sha256;
        tokio::task::spawn_blocking(move || repo.delete_blob_sync(sha256))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn list_blob_keys(&self) -> Result<Vec<[u8; 32]>, RepoError> {
        let repo = self.clone();
        tokio::task::spawn_blocking(move || repo.list_blob_keys_sync())
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }
}

fn ensure_tables(storage: &RedbStorage) -> Result<(), StorageError> {
    let write_txn = storage.begin_write()?;
    {
        write_txn.open_table(PLUGIN_REGISTRY)?;
    }
    {
        write_txn.open_table(PLUGIN_REGISTRY_MARKER)?;
    }
    {
        write_txn.open_table(PLUGIN_BLOBS)?;
    }
    write_txn.commit()?;
    Ok(())
}

#[async_trait]
impl PluginRegistryRepo for RedbStorage {
    async fn upsert_record(&self, record: &PluginRegistryRecord) -> Result<(), RepoError> {
        RedbPluginRegistryRepo::new(self.clone())
            .map_err(map_redb_err)?
            .upsert_record(record)
            .await
    }

    async fn get_by_sha256(
        &self,
        sha256: &[u8; 32],
    ) -> Result<Option<PluginRegistryRecord>, RepoError> {
        RedbPluginRegistryRepo::new(self.clone())
            .map_err(map_redb_err)?
            .get_by_sha256(sha256)
            .await
    }

    async fn list_active(&self) -> Result<Vec<PluginRegistryRecord>, RepoError> {
        RedbPluginRegistryRepo::new(self.clone())
            .map_err(map_redb_err)?
            .list_active()
            .await
    }

    async fn set_status(
        &self,
        sha256: &[u8; 32],
        status: PluginRegistryStatus,
    ) -> Result<(), RepoError> {
        RedbPluginRegistryRepo::new(self.clone())
            .map_err(map_redb_err)?
            .set_status(sha256, status)
            .await
    }

    async fn delete_by_sha256(&self, sha256: &[u8; 32]) -> Result<(), RepoError> {
        RedbPluginRegistryRepo::new(self.clone())
            .map_err(map_redb_err)?
            .delete_by_sha256(sha256)
            .await
    }

    async fn count(&self) -> Result<usize, RepoError> {
        RedbPluginRegistryRepo::new(self.clone())
            .map_err(map_redb_err)?
            .count()
            .await
    }

    async fn get_shutdown_marker(&self) -> Result<Option<i64>, RepoError> {
        RedbPluginRegistryRepo::new(self.clone())
            .map_err(map_redb_err)?
            .get_shutdown_marker()
            .await
    }

    async fn set_shutdown_marker(&self, unix_secs: i64) -> Result<(), RepoError> {
        RedbPluginRegistryRepo::new(self.clone())
            .map_err(map_redb_err)?
            .set_shutdown_marker(unix_secs)
            .await
    }

    async fn clear_shutdown_marker(&self) -> Result<(), RepoError> {
        RedbPluginRegistryRepo::new(self.clone())
            .map_err(map_redb_err)?
            .clear_shutdown_marker()
            .await
    }
}

#[async_trait]
impl PluginBlobRepo for RedbStorage {
    async fn put_blob(&self, sha256: &[u8; 32], bytes: &[u8]) -> Result<(), RepoError> {
        RedbPluginBlobRepo::new(self.clone())
            .map_err(map_redb_err)?
            .put_blob(sha256, bytes)
            .await
    }

    async fn get_blob(&self, sha256: &[u8; 32]) -> Result<Option<Vec<u8>>, RepoError> {
        RedbPluginBlobRepo::new(self.clone())
            .map_err(map_redb_err)?
            .get_blob(sha256)
            .await
    }

    async fn delete_blob(&self, sha256: &[u8; 32]) -> Result<(), RepoError> {
        RedbPluginBlobRepo::new(self.clone())
            .map_err(map_redb_err)?
            .delete_blob(sha256)
            .await
    }

    async fn list_blob_keys(&self) -> Result<Vec<[u8; 32]>, RepoError> {
        RedbPluginBlobRepo::new(self.clone())
            .map_err(map_redb_err)?
            .list_blob_keys()
            .await
    }
}
