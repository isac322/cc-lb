use cc_lb_storage_api::{
    MetadataTierMappingOverrideRecord, PlanTierRatioRecord, StorageResult, UpstreamPlanTierRecord,
};

use crate::{SqliteStorage, map_sqlx_error};

use super::codec::{row_to_override, row_to_ratio, row_to_upstream};

pub(super) async fn list_current_ratios(
    storage: &SqliteStorage,
) -> StorageResult<Vec<PlanTierRatioRecord>> {
    sqlx::query(
        "SELECT tier_key, pro_relative_ratio, effective_from_unix_millis, effective_to_unix_millis, provenance, created_at_unix_millis FROM plan_tier_ratio_history_v1 WHERE effective_to_unix_millis IS NULL ORDER BY tier_key ASC",
    )
    .fetch_all(storage.pool())
    .await
    .map_err(map_sqlx_error)?
    .into_iter()
    .map(row_to_ratio)
    .collect()
}

pub(super) async fn list_current_overrides(
    storage: &SqliteStorage,
) -> StorageResult<Vec<MetadataTierMappingOverrideRecord>> {
    sqlx::query(
        "SELECT organization_type, rate_limit_tier, seat_tier, tier_key, effective_from_unix_millis, effective_to_unix_millis, provenance, created_at_unix_millis FROM metadata_tier_mapping_override_v1 WHERE effective_to_unix_millis IS NULL ORDER BY organization_type ASC, rate_limit_tier ASC, seat_tier ASC",
    )
    .fetch_all(storage.pool())
    .await
    .map_err(map_sqlx_error)?
    .into_iter()
    .map(row_to_override)
    .collect()
}

pub(super) async fn list_current_upstreams(
    storage: &SqliteStorage,
) -> StorageResult<Vec<UpstreamPlanTierRecord>> {
    sqlx::query(
        "SELECT upstream_id, organization_uuid, organization_type, rate_limit_tier, seat_tier, tier_key, resolution_source, resolved_ratio_snapshot, observed_at_unix_millis, effective_from_unix_millis, effective_to_unix_millis, provenance, created_at_unix_millis FROM upstream_plan_tier_history_v1 WHERE effective_to_unix_millis IS NULL ORDER BY upstream_id ASC",
    )
    .fetch_all(storage.pool())
    .await
    .map_err(map_sqlx_error)?
    .into_iter()
    .map(row_to_upstream)
    .collect()
}
