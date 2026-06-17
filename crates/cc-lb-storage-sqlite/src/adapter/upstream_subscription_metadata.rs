use async_trait::async_trait;
use cc_lb_storage_api::{
    StorageResult, UpstreamSubscriptionMetadataRecord, UpstreamSubscriptionMetadataStore,
};
use sqlx::Row;
use uuid::Uuid;

use crate::{SqliteStorage, map_sqlx_error};

#[async_trait]
impl UpstreamSubscriptionMetadataStore for SqliteStorage {
    async fn put_upstream_subscription_metadata(
        &self,
        record: &UpstreamSubscriptionMetadataRecord,
    ) -> StorageResult<()> {
        let payload = serde_json::to_string(record)?;
        sqlx::query(
            "INSERT INTO upstream_subscription_metadata_v1 (upstream_id, payload, updated_at) \
             VALUES (?, ?, ?) \
             ON CONFLICT(upstream_id) DO UPDATE SET \
             payload = excluded.payload, updated_at = excluded.updated_at",
        )
        .bind(record.upstream_id.to_string())
        .bind(payload)
        .bind(record.observed_at_unix_millis)
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn get_upstream_subscription_metadata(
        &self,
        upstream_id: Uuid,
    ) -> StorageResult<Option<UpstreamSubscriptionMetadataRecord>> {
        let row = sqlx::query(
            "SELECT payload FROM upstream_subscription_metadata_v1 WHERE upstream_id = ?",
        )
        .bind(upstream_id.to_string())
        .fetch_optional(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        row.map(row_to_record).transpose()
    }

    async fn list_upstream_subscription_metadata(
        &self,
    ) -> StorageResult<Vec<UpstreamSubscriptionMetadataRecord>> {
        let rows = sqlx::query(
            "SELECT payload FROM upstream_subscription_metadata_v1 ORDER BY upstream_id ASC",
        )
        .fetch_all(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        rows.into_iter().map(row_to_record).collect()
    }
}

fn row_to_record(
    row: sqlx::sqlite::SqliteRow,
) -> StorageResult<UpstreamSubscriptionMetadataRecord> {
    let payload: String = row.try_get("payload").map_err(map_sqlx_error)?;
    serde_json::from_str(&payload).map_err(Into::into)
}
