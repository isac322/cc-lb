use async_trait::async_trait;
use cc_lb_storage_api::{OrganizationMetadataRecord, OrganizationMetadataStore, StorageResult};
use redb::{ReadableDatabase, ReadableTable};

use crate::adapter::error_map::{map_join_err, map_redb_err};
use crate::{ORGANIZATION_METADATA_V1, RedbStorage, StorageError};

#[async_trait]
impl OrganizationMetadataStore for RedbStorage {
    async fn put_organization_metadata(
        &self,
        record: &OrganizationMetadataRecord,
    ) -> StorageResult<()> {
        let storage = self.clone();
        let record = record.clone();
        tokio::task::spawn_blocking(move || put_sync(&storage, &record))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn get_organization_metadata(
        &self,
        organization_uuid: &str,
    ) -> StorageResult<Option<OrganizationMetadataRecord>> {
        let storage = self.clone();
        let organization_uuid = organization_uuid.to_owned();
        tokio::task::spawn_blocking(move || get_sync(&storage, &organization_uuid))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn list_organization_metadata(&self) -> StorageResult<Vec<OrganizationMetadataRecord>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || list_sync(&storage))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }
}

fn put_sync(
    storage: &RedbStorage,
    record: &OrganizationMetadataRecord,
) -> Result<(), StorageError> {
    let write_txn = storage.db.begin_write()?;
    {
        let mut table = write_txn.open_table(ORGANIZATION_METADATA_V1)?;
        let bytes = serde_json::to_vec(record)?;
        table.insert(record.organization_uuid.as_str(), bytes.as_slice())?;
    }
    write_txn.commit()?;
    Ok(())
}

fn get_sync(
    storage: &RedbStorage,
    organization_uuid: &str,
) -> Result<Option<OrganizationMetadataRecord>, StorageError> {
    let read_txn = storage.db.begin_read()?;
    let table = read_txn.open_table(ORGANIZATION_METADATA_V1)?;
    table
        .get(organization_uuid)?
        .map(|value| serde_json::from_slice(value.value()).map_err(StorageError::from))
        .transpose()
}

fn list_sync(storage: &RedbStorage) -> Result<Vec<OrganizationMetadataRecord>, StorageError> {
    let read_txn = storage.db.begin_read()?;
    let table = read_txn.open_table(ORGANIZATION_METADATA_V1)?;
    let mut records = Vec::new();
    for row in table.iter()? {
        let (_, value) = row?;
        records.push(serde_json::from_slice(value.value())?);
    }
    records.sort_by(|left: &OrganizationMetadataRecord, right| {
        left.organization_uuid.cmp(&right.organization_uuid)
    });
    Ok(records)
}
