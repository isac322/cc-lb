mod backfill;
mod metadata_overrides;
mod ratios;
mod upstreams;

use async_trait::async_trait;
use cc_lb_storage_api::{
    BackfillApplyOutcome, MetadataTierMappingOverrideRecord, PlanTierRatioRecord, PlanTierStore,
    StorageResult, UpstreamPlanTierRecord,
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
    async fn upsert_plan_tier_ratio(&self, record: &PlanTierRatioRecord) -> StorageResult<()> {
        ratios::upsert(self, record).await
    }

    async fn list_current_plan_tier_ratios(&self) -> StorageResult<Vec<PlanTierRatioRecord>> {
        ratios::list_current(self).await
    }

    async fn list_plan_tier_ratios_as_of(
        &self,
        as_of_unix_millis: i64,
    ) -> StorageResult<Vec<PlanTierRatioRecord>> {
        ratios::list_as_of(self, as_of_unix_millis).await
    }

    async fn upsert_metadata_tier_override(
        &self,
        record: &MetadataTierMappingOverrideRecord,
    ) -> StorageResult<()> {
        metadata_overrides::upsert(self, record).await
    }

    async fn list_current_metadata_tier_overrides(
        &self,
    ) -> StorageResult<Vec<MetadataTierMappingOverrideRecord>> {
        metadata_overrides::list_current(self).await
    }

    async fn list_metadata_tier_overrides_as_of(
        &self,
        as_of_unix_millis: i64,
    ) -> StorageResult<Vec<MetadataTierMappingOverrideRecord>> {
        metadata_overrides::list_as_of(self, as_of_unix_millis).await
    }

    async fn append_upstream_plan_tier(
        &self,
        record: &UpstreamPlanTierRecord,
    ) -> StorageResult<()> {
        upstreams::append(self, record).await
    }

    async fn backfill_upstream_plan_tier_intervals(
        &self,
        upstream_id: uuid::Uuid,
        intervals: &[UpstreamPlanTierRecord],
        terminal_cap_unix_millis: i64,
        provenance: &str,
    ) -> StorageResult<BackfillApplyOutcome> {
        backfill::apply(
            self,
            upstream_id,
            intervals,
            terminal_cap_unix_millis,
            provenance,
        )
        .await
    }

    async fn list_current_upstream_plan_tiers(&self) -> StorageResult<Vec<UpstreamPlanTierRecord>> {
        upstreams::list_current(self).await
    }

    async fn list_upstream_plan_tiers_as_of(
        &self,
        as_of_unix_millis: i64,
    ) -> StorageResult<Vec<UpstreamPlanTierRecord>> {
        upstreams::list_as_of(self, as_of_unix_millis).await
    }
}
