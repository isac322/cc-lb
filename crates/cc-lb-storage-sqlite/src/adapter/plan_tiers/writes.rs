use cc_lb_storage_api::{
    MetadataTierMappingOverrideRecord, PlanTierRatioRecord, StorageResult, TierResolutionSource,
    UpstreamPlanTierRecord,
};
use sqlx::Row;

use crate::map_sqlx_error;

use super::codec::{conflict, encode_absent, invalid_input, parse_resolution_source};

pub(super) async fn insert_ratio(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    record: &PlanTierRatioRecord,
) -> StorageResult<()> {
    sqlx::query("INSERT INTO plan_tier_ratio_history_v1 (tier_key, pro_relative_ratio, effective_from_unix_millis, effective_to_unix_millis, provenance, created_at_unix_millis) VALUES (?, ?, ?, NULL, ?, ?)")
        .bind(&record.tier_key)
        .bind(record.pro_relative_ratio)
        .bind(record.effective_from_unix_millis)
        .bind(&record.provenance)
        .bind(record.created_at_unix_millis)
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
    Ok(())
}

pub(super) async fn insert_override(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    record: &MetadataTierMappingOverrideRecord,
) -> StorageResult<()> {
    sqlx::query("INSERT INTO metadata_tier_mapping_override_v1 (organization_type, rate_limit_tier, seat_tier, tier_key, effective_from_unix_millis, effective_to_unix_millis, provenance, created_at_unix_millis) VALUES (?, ?, ?, ?, ?, NULL, ?, ?)")
        .bind(encode_absent(&record.organization_type))
        .bind(encode_absent(&record.rate_limit_tier))
        .bind(encode_absent(&record.seat_tier))
        .bind(&record.tier_key)
        .bind(record.effective_from_unix_millis)
        .bind(&record.provenance)
        .bind(record.created_at_unix_millis)
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
    Ok(())
}

pub(super) async fn insert_upstream(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    record: &UpstreamPlanTierRecord,
) -> StorageResult<()> {
    sqlx::query("INSERT INTO upstream_plan_tier_history_v1 (upstream_id, organization_uuid, organization_type, rate_limit_tier, seat_tier, tier_key, resolution_source, resolved_ratio_snapshot, observed_at_unix_millis, effective_from_unix_millis, effective_to_unix_millis, provenance, created_at_unix_millis) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)")
        .bind(record.upstream_id.to_string())
        .bind(record.organization_uuid.as_deref())
        .bind(record.organization_type.as_deref())
        .bind(record.rate_limit_tier.as_deref())
        .bind(record.seat_tier.as_deref())
        .bind(record.tier_key.as_deref())
        .bind(record.resolution_source.as_str())
        .bind(record.resolved_ratio_snapshot)
        .bind(record.observed_at_unix_millis)
        .bind(record.effective_from_unix_millis)
        .bind(&record.provenance)
        .bind(record.created_at_unix_millis)
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
    Ok(())
}

pub(super) async fn insert_closed_upstream(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    record: &UpstreamPlanTierRecord,
    effective_to_unix_millis: i64,
) -> StorageResult<()> {
    sqlx::query("INSERT INTO upstream_plan_tier_history_v1 (upstream_id, organization_uuid, organization_type, rate_limit_tier, seat_tier, tier_key, resolution_source, resolved_ratio_snapshot, observed_at_unix_millis, effective_from_unix_millis, effective_to_unix_millis, provenance, created_at_unix_millis) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(record.upstream_id.to_string())
        .bind(record.organization_uuid.as_deref())
        .bind(record.organization_type.as_deref())
        .bind(record.rate_limit_tier.as_deref())
        .bind(record.seat_tier.as_deref())
        .bind(record.tier_key.as_deref())
        .bind(record.resolution_source.as_str())
        .bind(record.resolved_ratio_snapshot)
        .bind(record.observed_at_unix_millis)
        .bind(record.effective_from_unix_millis)
        .bind(effective_to_unix_millis)
        .bind(&record.provenance)
        .bind(record.created_at_unix_millis)
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
    Ok(())
}

pub(super) fn upstream_open_matches(
    row: &sqlx::sqlite::SqliteRow,
    record: &UpstreamPlanTierRecord,
) -> StorageResult<bool> {
    Ok(row
        .try_get::<Option<String>, _>("tier_key")
        .map_err(map_sqlx_error)?
        == record.tier_key
        && parse_resolution_source(
            &row.try_get::<String, _>("resolution_source")
                .map_err(map_sqlx_error)?,
        )? == record.resolution_source
        && row
            .try_get::<Option<String>, _>("organization_type")
            .map_err(map_sqlx_error)?
            == record.organization_type
        && row
            .try_get::<Option<String>, _>("rate_limit_tier")
            .map_err(map_sqlx_error)?
            == record.rate_limit_tier
        && row
            .try_get::<Option<String>, _>("seat_tier")
            .map_err(map_sqlx_error)?
            == record.seat_tier)
}

pub(super) fn close_requires_later_effective_from(
    row: &sqlx::sqlite::SqliteRow,
    effective_from_unix_millis: i64,
) -> StorageResult<()> {
    let current = row
        .try_get::<i64, _>("effective_from_unix_millis")
        .map_err(map_sqlx_error)?;
    if effective_from_unix_millis <= current {
        return Err(conflict(
            "effective_from_unix_millis must increase when closing an open plan-tier row",
        ));
    }
    Ok(())
}

pub(super) fn validate_upstream_tier_key(record: &UpstreamPlanTierRecord) -> StorageResult<()> {
    match record.resolution_source {
        TierResolutionSource::Unknown => {
            if record.tier_key.is_some() {
                return Err(invalid_input(
                    "tier_key",
                    "must be NULL when resolution_source is unknown",
                ));
            }
            if record.resolved_ratio_snapshot.is_some() {
                return Err(invalid_input(
                    "resolved_ratio_snapshot",
                    "must be NULL when resolution_source is unknown",
                ));
            }
            Ok(())
        }
        TierResolutionSource::Override
        | TierResolutionSource::Builtin
        | TierResolutionSource::Backfill => {
            if record.tier_key.is_none() {
                return Err(invalid_input(
                    "tier_key",
                    "must be set when resolution_source is override, builtin, or backfill",
                ));
            }
            if record.resolved_ratio_snapshot.is_none() {
                return Err(invalid_input(
                    "resolved_ratio_snapshot",
                    "must be set when resolution_source is override, builtin, or backfill",
                ));
            }
            Ok(())
        }
    }
}
