use async_trait::async_trait;
use cc_lb_storage_api::{OrganizationMetadataRecord, OrganizationMetadataStore, StorageResult};
use sqlx::Row;

use crate::{SqliteStorage, map_sqlx_error};

#[async_trait]
impl OrganizationMetadataStore for SqliteStorage {
    async fn put_organization_metadata(
        &self,
        record: &OrganizationMetadataRecord,
    ) -> StorageResult<()> {
        let payload = serde_json::to_string(record)?;
        sqlx::query(
            "INSERT INTO organization_metadata_v1 (key, value) VALUES (?, ?) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .bind(&record.organization_uuid)
        .bind(payload)
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn get_organization_metadata(
        &self,
        organization_uuid: &str,
    ) -> StorageResult<Option<OrganizationMetadataRecord>> {
        let row = sqlx::query("SELECT value FROM organization_metadata_v1 WHERE key = ?")
            .bind(organization_uuid)
            .fetch_optional(self.pool())
            .await
            .map_err(map_sqlx_error)?;

        row.map(row_to_record).transpose()
    }

    async fn list_organization_metadata(&self) -> StorageResult<Vec<OrganizationMetadataRecord>> {
        let rows = sqlx::query("SELECT value FROM organization_metadata_v1 ORDER BY key ASC")
            .fetch_all(self.pool())
            .await
            .map_err(map_sqlx_error)?;

        rows.into_iter().map(row_to_record).collect()
    }
}

fn row_to_record(row: sqlx::sqlite::SqliteRow) -> StorageResult<OrganizationMetadataRecord> {
    let payload: String = row.try_get("value").map_err(map_sqlx_error)?;
    serde_json::from_str(&payload).map_err(Into::into)
}
