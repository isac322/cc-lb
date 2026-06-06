use async_trait::async_trait;
use cc_lb_storage_api::{
    StorageResult, UpstreamSubscriptionMetadataRecord, UpstreamSubscriptionMetadataStore,
};
use sqlx::{Row, postgres::PgRow};
use uuid::Uuid;

use crate::{adapter::PostgresStorage, error_map::map_sqlx_error};

#[async_trait]
impl UpstreamSubscriptionMetadataStore for PostgresStorage {
    async fn put_upstream_subscription_metadata(
        &self,
        record: &UpstreamSubscriptionMetadataRecord,
    ) -> StorageResult<()> {
        sqlx::query(
            "INSERT INTO upstream_subscription_metadata_v1 \
             (upstream_id, organization_uuid, organization_role, workspace_role, observed_at_unix_millis, \
              last_error, raw_roles, raw_bootstrap) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8) \
             ON CONFLICT (upstream_id) DO UPDATE SET \
              organization_uuid = EXCLUDED.organization_uuid, \
              organization_role = EXCLUDED.organization_role, \
              workspace_role = EXCLUDED.workspace_role, \
              observed_at_unix_millis = EXCLUDED.observed_at_unix_millis, \
              last_error = EXCLUDED.last_error, \
              raw_roles = EXCLUDED.raw_roles, \
              raw_bootstrap = EXCLUDED.raw_bootstrap",
        )
        .bind(record.upstream_id)
        .bind(&record.organization_uuid)
        .bind(&record.organization_role)
        .bind(&record.workspace_role)
        .bind(record.observed_at_unix_millis)
        .bind(&record.last_error)
        .bind(&record.raw_roles)
        .bind(&record.raw_bootstrap)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn get_upstream_subscription_metadata(
        &self,
        upstream_id: Uuid,
    ) -> StorageResult<Option<UpstreamSubscriptionMetadataRecord>> {
        let row = sqlx::query(
            "SELECT upstream_id, organization_uuid, organization_role, workspace_role, observed_at_unix_millis, \
             last_error, raw_roles, raw_bootstrap \
             FROM upstream_subscription_metadata_v1 WHERE upstream_id = $1",
        )
        .bind(upstream_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        row.map(row_to_record).transpose()
    }

    async fn list_upstream_subscription_metadata(
        &self,
    ) -> StorageResult<Vec<UpstreamSubscriptionMetadataRecord>> {
        let rows = sqlx::query(
            "SELECT upstream_id, organization_uuid, organization_role, workspace_role, observed_at_unix_millis, \
             last_error, raw_roles, raw_bootstrap \
             FROM upstream_subscription_metadata_v1 ORDER BY upstream_id ASC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        rows.into_iter().map(row_to_record).collect()
    }
}

fn row_to_record(row: PgRow) -> StorageResult<UpstreamSubscriptionMetadataRecord> {
    Ok(UpstreamSubscriptionMetadataRecord {
        upstream_id: row.try_get("upstream_id").map_err(map_sqlx_error)?,
        organization_uuid: row.try_get("organization_uuid").map_err(map_sqlx_error)?,
        organization_role: row.try_get("organization_role").map_err(map_sqlx_error)?,
        workspace_role: row.try_get("workspace_role").map_err(map_sqlx_error)?,
        observed_at_unix_millis: row
            .try_get("observed_at_unix_millis")
            .map_err(map_sqlx_error)?,
        last_error: row.try_get("last_error").map_err(map_sqlx_error)?,
        raw_roles: row.try_get("raw_roles").map_err(map_sqlx_error)?,
        raw_bootstrap: row.try_get("raw_bootstrap").map_err(map_sqlx_error)?,
    })
}
