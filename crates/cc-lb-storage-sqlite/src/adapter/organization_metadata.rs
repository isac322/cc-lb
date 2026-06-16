use async_trait::async_trait;
use cc_lb_storage_api::{OrganizationMetadataRecord, OrganizationMetadataStore, StorageResult};

use crate::SqliteStorage;

#[async_trait]
impl OrganizationMetadataStore for SqliteStorage {
    async fn put_organization_metadata(
        &self,
        _record: &OrganizationMetadataRecord,
    ) -> StorageResult<()> {
        unimplemented!()
    }

    async fn get_organization_metadata(
        &self,
        _organization_uuid: &str,
    ) -> StorageResult<Option<OrganizationMetadataRecord>> {
        unimplemented!()
    }

    async fn list_organization_metadata(&self) -> StorageResult<Vec<OrganizationMetadataRecord>> {
        unimplemented!()
    }
}
