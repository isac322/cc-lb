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
use sqlx::Row;

use crate::{SqliteStorage, map_sqlx_error};

use self::{
    codec::{encode_absent, invalid_input},
    writes::{
        close_requires_later_effective_from, insert_override, insert_ratio, insert_upstream,
        upstream_open_matches, validate_upstream_tier_key,
    },
};

#[async_trait]
impl PlanTierStore for SqliteStorage {
    async fn upsert_plan_tier_ratio(&self, record: &PlanTierRatioRecord) -> StorageResult<()> {
        if !record.pro_relative_ratio.is_finite() || record.pro_relative_ratio <= 0.0 {
            return Err(invalid_input(
                "pro_relative_ratio",
                "must be finite and > 0",
            ));
        }

        let mut tx = self.begin_immediate().await?;
        let open = sqlx::query(
            "SELECT pro_relative_ratio, effective_from_unix_millis FROM plan_tier_ratio_history_v1 WHERE tier_key = ? AND effective_to_unix_millis IS NULL",
        )
        .bind(&record.tier_key)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;

        if let Some(open) = open {
            let current_ratio = open
                .try_get::<f64, _>("pro_relative_ratio")
                .map_err(map_sqlx_error)?;
            if current_ratio == record.pro_relative_ratio {
                tx.commit().await.map_err(map_sqlx_error)?;
                return Ok(());
            }
            close_requires_later_effective_from(&open, record.effective_from_unix_millis)?;
            sqlx::query("UPDATE plan_tier_ratio_history_v1 SET effective_to_unix_millis = ? WHERE tier_key = ? AND effective_to_unix_millis IS NULL")
                .bind(record.effective_from_unix_millis)
                .bind(&record.tier_key)
                .execute(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
        }

        insert_ratio(&mut tx, record).await?;
        tx.commit().await.map_err(map_sqlx_error)
    }

    async fn list_current_plan_tier_ratios(&self) -> StorageResult<Vec<PlanTierRatioRecord>> {
        reads::list_current_ratios(self).await
    }

    async fn list_plan_tier_ratios_as_of(
        &self,
        as_of_unix_millis: i64,
    ) -> StorageResult<Vec<PlanTierRatioRecord>> {
        reads::list_ratios_as_of(self, as_of_unix_millis).await
    }

    async fn upsert_metadata_tier_override(
        &self,
        record: &MetadataTierMappingOverrideRecord,
    ) -> StorageResult<()> {
        let organization_type = encode_absent(&record.organization_type);
        let rate_limit_tier = encode_absent(&record.rate_limit_tier);
        let seat_tier = encode_absent(&record.seat_tier);
        let mut tx = self.begin_immediate().await?;
        let open = sqlx::query(
            "SELECT tier_key, effective_from_unix_millis FROM metadata_tier_mapping_override_v1 WHERE organization_type = ? AND rate_limit_tier = ? AND seat_tier = ? AND effective_to_unix_millis IS NULL",
        )
        .bind(organization_type)
        .bind(rate_limit_tier)
        .bind(seat_tier)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;

        if let Some(open) = open {
            let tier_key = open
                .try_get::<String, _>("tier_key")
                .map_err(map_sqlx_error)?;
            if tier_key == record.tier_key {
                tx.commit().await.map_err(map_sqlx_error)?;
                return Ok(());
            }
            close_requires_later_effective_from(&open, record.effective_from_unix_millis)?;
            sqlx::query("UPDATE metadata_tier_mapping_override_v1 SET effective_to_unix_millis = ? WHERE organization_type = ? AND rate_limit_tier = ? AND seat_tier = ? AND effective_to_unix_millis IS NULL")
                .bind(record.effective_from_unix_millis)
                .bind(organization_type)
                .bind(rate_limit_tier)
                .bind(seat_tier)
                .execute(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
        }

        insert_override(&mut tx, record).await?;
        tx.commit().await.map_err(map_sqlx_error)
    }

    async fn list_current_metadata_tier_overrides(
        &self,
    ) -> StorageResult<Vec<MetadataTierMappingOverrideRecord>> {
        reads::list_current_overrides(self).await
    }

    async fn list_metadata_tier_overrides_as_of(
        &self,
        as_of_unix_millis: i64,
    ) -> StorageResult<Vec<MetadataTierMappingOverrideRecord>> {
        reads::list_overrides_as_of(self, as_of_unix_millis).await
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

    async fn list_upstream_plan_tiers_as_of(
        &self,
        as_of_unix_millis: i64,
    ) -> StorageResult<Vec<UpstreamPlanTierRecord>> {
        reads::list_upstreams_as_of(self, as_of_unix_millis).await
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
