mod metadata_overrides;
mod ratios;
mod upstreams;

use async_trait::async_trait;
use cc_lb_storage_api::{
    MetadataTierMappingOverrideRecord, PlanTierRatioRecord, PlanTierStore, StorageResult,
    UpstreamPlanTierRecord,
};

use crate::adapter::PostgresStorage;

pub(super) async fn lock_logical_key(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    classid: i32,
    key_text: &str,
) -> StorageResult<()> {
    sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext($2))")
        .bind(classid)
        .bind(key_text)
        .execute(&mut **tx)
        .await
        .map_err(crate::error_map::map_sqlx_error)?;
    Ok(())
}

#[async_trait]
impl PlanTierStore for PostgresStorage {
    async fn list_current_plan_tier_ratios(&self) -> StorageResult<Vec<PlanTierRatioRecord>> {
        ratios::list_current(self).await
    }

    async fn list_current_metadata_tier_overrides(
        &self,
    ) -> StorageResult<Vec<MetadataTierMappingOverrideRecord>> {
        metadata_overrides::list_current(self).await
    }

    async fn append_upstream_plan_tier(
        &self,
        record: &UpstreamPlanTierRecord,
    ) -> StorageResult<()> {
        upstreams::append(self, record).await
    }

    async fn list_current_upstream_plan_tiers(&self) -> StorageResult<Vec<UpstreamPlanTierRecord>> {
        upstreams::list_current(self).await
    }
}
