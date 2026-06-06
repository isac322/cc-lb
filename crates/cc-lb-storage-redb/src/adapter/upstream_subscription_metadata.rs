use async_trait::async_trait;
use cc_lb_storage_api::{
    StorageResult, UpstreamSubscriptionMetadataRecord, UpstreamSubscriptionMetadataStore,
};
use redb::{ReadableDatabase, ReadableTable};
use uuid::Uuid;

use crate::adapter::error_map::{map_join_err, map_redb_err};
use crate::{RedbStorage, StorageError, UPSTREAM_SUBSCRIPTION_METADATA_V1};

#[async_trait]
impl UpstreamSubscriptionMetadataStore for RedbStorage {
    async fn put_upstream_subscription_metadata(
        &self,
        record: &UpstreamSubscriptionMetadataRecord,
    ) -> StorageResult<()> {
        let storage = self.clone();
        let record = record.clone();
        tokio::task::spawn_blocking(move || put_sync(&storage, &record))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn get_upstream_subscription_metadata(
        &self,
        upstream_id: Uuid,
    ) -> StorageResult<Option<UpstreamSubscriptionMetadataRecord>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || get_sync(&storage, upstream_id))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn list_upstream_subscription_metadata(
        &self,
    ) -> StorageResult<Vec<UpstreamSubscriptionMetadataRecord>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || list_sync(&storage))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }
}

fn put_sync(
    storage: &RedbStorage,
    record: &UpstreamSubscriptionMetadataRecord,
) -> Result<(), StorageError> {
    let write_txn = storage.db.begin_write()?;
    {
        let mut table = write_txn.open_table(UPSTREAM_SUBSCRIPTION_METADATA_V1)?;
        let bytes = serde_json::to_vec(record)?;
        let key = record.upstream_id.to_string();
        table.insert(key.as_str(), bytes.as_slice())?;
    }
    write_txn.commit()?;
    Ok(())
}

fn get_sync(
    storage: &RedbStorage,
    upstream_id: Uuid,
) -> Result<Option<UpstreamSubscriptionMetadataRecord>, StorageError> {
    let read_txn = storage.db.begin_read()?;
    let table = read_txn.open_table(UPSTREAM_SUBSCRIPTION_METADATA_V1)?;
    let key = upstream_id.to_string();
    table
        .get(key.as_str())?
        .map(|value| serde_json::from_slice(value.value()).map_err(StorageError::from))
        .transpose()
}

fn list_sync(
    storage: &RedbStorage,
) -> Result<Vec<UpstreamSubscriptionMetadataRecord>, StorageError> {
    let read_txn = storage.db.begin_read()?;
    let table = read_txn.open_table(UPSTREAM_SUBSCRIPTION_METADATA_V1)?;
    let mut records = Vec::new();
    for row in table.iter()? {
        let (_, value) = row?;
        records.push(serde_json::from_slice(value.value())?);
    }
    records.sort_by_key(|record: &UpstreamSubscriptionMetadataRecord| record.upstream_id);
    Ok(records)
}
