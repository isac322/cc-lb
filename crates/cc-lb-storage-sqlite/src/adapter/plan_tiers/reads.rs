use cc_lb_storage_api::{
    MetadataTierMappingOverrideRecord, PlanTierRatioRecord, StorageResult, UpstreamPlanTierRecord,
};
use sqlx::AssertSqlSafe;

use crate::{SqliteStorage, map_sqlx_error};

use super::codec::{row_to_override, row_to_ratio, row_to_upstream};

pub(super) async fn list_current_ratios(
    storage: &SqliteStorage,
) -> StorageResult<Vec<PlanTierRatioRecord>> {
    list_ratios(storage, "effective_to_unix_millis IS NULL", None).await
}

pub(super) async fn list_ratios_as_of(
    storage: &SqliteStorage,
    as_of_unix_millis: i64,
) -> StorageResult<Vec<PlanTierRatioRecord>> {
    list_ratios(storage, AS_OF_PREDICATE, Some(as_of_unix_millis)).await
}

pub(super) async fn list_current_overrides(
    storage: &SqliteStorage,
) -> StorageResult<Vec<MetadataTierMappingOverrideRecord>> {
    list_overrides(storage, "effective_to_unix_millis IS NULL", None).await
}

pub(super) async fn list_overrides_as_of(
    storage: &SqliteStorage,
    as_of_unix_millis: i64,
) -> StorageResult<Vec<MetadataTierMappingOverrideRecord>> {
    list_overrides(storage, AS_OF_PREDICATE, Some(as_of_unix_millis)).await
}

pub(super) async fn list_current_upstreams(
    storage: &SqliteStorage,
) -> StorageResult<Vec<UpstreamPlanTierRecord>> {
    list_upstreams(storage, "effective_to_unix_millis IS NULL", None).await
}

pub(super) async fn list_upstreams_as_of(
    storage: &SqliteStorage,
    as_of_unix_millis: i64,
) -> StorageResult<Vec<UpstreamPlanTierRecord>> {
    list_upstreams(storage, AS_OF_PREDICATE, Some(as_of_unix_millis)).await
}

const AS_OF_PREDICATE: &str = "effective_from_unix_millis <= ? AND (effective_to_unix_millis IS NULL OR effective_to_unix_millis > ?)";

async fn list_ratios(
    storage: &SqliteStorage,
    predicate: &str,
    as_of_unix_millis: Option<i64>,
) -> StorageResult<Vec<PlanTierRatioRecord>> {
    let sql = format!(
        "SELECT tier_key, pro_relative_ratio, effective_from_unix_millis, effective_to_unix_millis, provenance, created_at_unix_millis FROM plan_tier_ratio_history_v1 WHERE {predicate} ORDER BY tier_key ASC"
    );
    let mut query = sqlx::query(AssertSqlSafe(sql));
    if let Some(as_of_unix_millis) = as_of_unix_millis {
        query = query.bind(as_of_unix_millis).bind(as_of_unix_millis);
    }
    query
        .fetch_all(storage.pool())
        .await
        .map_err(map_sqlx_error)?
        .into_iter()
        .map(row_to_ratio)
        .collect()
}

async fn list_overrides(
    storage: &SqliteStorage,
    predicate: &str,
    as_of_unix_millis: Option<i64>,
) -> StorageResult<Vec<MetadataTierMappingOverrideRecord>> {
    let sql = format!(
        "SELECT organization_type, rate_limit_tier, seat_tier, tier_key, effective_from_unix_millis, effective_to_unix_millis, provenance, created_at_unix_millis FROM metadata_tier_mapping_override_v1 WHERE {predicate} ORDER BY organization_type ASC, rate_limit_tier ASC, seat_tier ASC"
    );
    let mut query = sqlx::query(AssertSqlSafe(sql));
    if let Some(as_of_unix_millis) = as_of_unix_millis {
        query = query.bind(as_of_unix_millis).bind(as_of_unix_millis);
    }
    query
        .fetch_all(storage.pool())
        .await
        .map_err(map_sqlx_error)?
        .into_iter()
        .map(row_to_override)
        .collect()
}

async fn list_upstreams(
    storage: &SqliteStorage,
    predicate: &str,
    as_of_unix_millis: Option<i64>,
) -> StorageResult<Vec<UpstreamPlanTierRecord>> {
    let sql = format!(
        "SELECT upstream_id, organization_uuid, organization_type, rate_limit_tier, seat_tier, tier_key, resolution_source, resolved_ratio_snapshot, observed_at_unix_millis, effective_from_unix_millis, effective_to_unix_millis, provenance, created_at_unix_millis FROM upstream_plan_tier_history_v1 WHERE {predicate} ORDER BY upstream_id ASC"
    );
    let mut query = sqlx::query(AssertSqlSafe(sql));
    if let Some(as_of_unix_millis) = as_of_unix_millis {
        query = query.bind(as_of_unix_millis).bind(as_of_unix_millis);
    }
    query
        .fetch_all(storage.pool())
        .await
        .map_err(map_sqlx_error)?
        .into_iter()
        .map(row_to_upstream)
        .collect()
}
