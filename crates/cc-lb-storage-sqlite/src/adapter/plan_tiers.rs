mod codec;
mod reads;
mod writes;

#[cfg(test)]
mod test_support;

use async_trait::async_trait;
use cc_lb_storage_api::{
    MetadataTierMappingOverrideRecord, PlanTierRatioRecord, PlanTierStore, StorageResult,
    UpstreamPlanTierRecord,
};

use crate::{SqliteStorage, map_sqlx_error};

use self::writes::{
    close_requires_later_effective_from, insert_upstream, upstream_open_matches,
    validate_upstream_tier_key,
};

#[async_trait]
impl PlanTierStore for SqliteStorage {
    async fn list_current_plan_tier_ratios(&self) -> StorageResult<Vec<PlanTierRatioRecord>> {
        reads::list_current_ratios(self).await
    }

    async fn list_current_metadata_tier_overrides(
        &self,
    ) -> StorageResult<Vec<MetadataTierMappingOverrideRecord>> {
        reads::list_current_overrides(self).await
    }

    async fn append_upstream_plan_tier(
        &self,
        record: &UpstreamPlanTierRecord,
    ) -> StorageResult<()> {
        validate_upstream_tier_key(record)?;
        let upstream_id = record.upstream_id.to_string();
        let mut tx = self.begin_immediate().await?;
        let open = sqlx::query(
            "SELECT tier_key, resolution_source, organization_type, rate_limit_tier, seat_tier, effective_from_unix_millis FROM upstream_plan_tier_history_v1 WHERE upstream_id = ? AND effective_to_unix_millis IS NULL",
        )
        .bind(&upstream_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;

        if let Some(open) = open {
            if upstream_open_matches(&open, record)? {
                tx.commit().await.map_err(map_sqlx_error)?;
                return Ok(());
            }
            close_requires_later_effective_from(&open, record.effective_from_unix_millis)?;
            sqlx::query("UPDATE upstream_plan_tier_history_v1 SET effective_to_unix_millis = ? WHERE upstream_id = ? AND effective_to_unix_millis IS NULL")
                .bind(record.effective_from_unix_millis)
                .bind(&upstream_id)
                .execute(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
        }

        insert_upstream(&mut tx, record).await?;
        tx.commit().await.map_err(map_sqlx_error)
    }

    async fn list_current_upstream_plan_tiers(&self) -> StorageResult<Vec<UpstreamPlanTierRecord>> {
        reads::list_current_upstreams(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{exercise_plan_tier_store, migrated_storage};

    #[tokio::test]
    async fn plan_tier_store_preserves_scd2_history_and_idempotency() {
        let (_temp_dir, storage) = migrated_storage().await;
        exercise_plan_tier_store(&storage).await;
    }
}
