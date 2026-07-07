use cc_lb_storage_api::{
    MetadataTierMappingOverrideRecord, PlanTierRatioRecord, StorageError, StorageResult,
    TierResolutionSource, UpstreamPlanTierRecord,
};
use sqlx::Row;
use uuid::Uuid;

use crate::map_sqlx_error;

pub(super) fn row_to_ratio(row: sqlx::sqlite::SqliteRow) -> StorageResult<PlanTierRatioRecord> {
    Ok(PlanTierRatioRecord {
        tier_key: row.try_get("tier_key").map_err(map_sqlx_error)?,
        pro_relative_ratio: row.try_get("pro_relative_ratio").map_err(map_sqlx_error)?,
        effective_from_unix_millis: row
            .try_get("effective_from_unix_millis")
            .map_err(map_sqlx_error)?,
        effective_to_unix_millis: row
            .try_get("effective_to_unix_millis")
            .map_err(map_sqlx_error)?,
        provenance: row.try_get("provenance").map_err(map_sqlx_error)?,
        created_at_unix_millis: row
            .try_get("created_at_unix_millis")
            .map_err(map_sqlx_error)?,
    })
}

pub(super) fn row_to_override(
    row: sqlx::sqlite::SqliteRow,
) -> StorageResult<MetadataTierMappingOverrideRecord> {
    Ok(MetadataTierMappingOverrideRecord {
        organization_type: decode_absent(
            row.try_get::<String, _>("organization_type")
                .map_err(map_sqlx_error)?,
        ),
        rate_limit_tier: decode_absent(
            row.try_get::<String, _>("rate_limit_tier")
                .map_err(map_sqlx_error)?,
        ),
        seat_tier: decode_absent(
            row.try_get::<String, _>("seat_tier")
                .map_err(map_sqlx_error)?,
        ),
        tier_key: row.try_get("tier_key").map_err(map_sqlx_error)?,
        effective_from_unix_millis: row
            .try_get("effective_from_unix_millis")
            .map_err(map_sqlx_error)?,
        effective_to_unix_millis: row
            .try_get("effective_to_unix_millis")
            .map_err(map_sqlx_error)?,
        provenance: row.try_get("provenance").map_err(map_sqlx_error)?,
        created_at_unix_millis: row
            .try_get("created_at_unix_millis")
            .map_err(map_sqlx_error)?,
    })
}

pub(super) fn row_to_upstream(
    row: sqlx::sqlite::SqliteRow,
) -> StorageResult<UpstreamPlanTierRecord> {
    let upstream_id = row
        .try_get::<String, _>("upstream_id")
        .map_err(map_sqlx_error)?;
    Ok(UpstreamPlanTierRecord {
        upstream_id: parse_uuid(&upstream_id, "upstream_id")?,
        organization_uuid: row.try_get("organization_uuid").map_err(map_sqlx_error)?,
        organization_type: row.try_get("organization_type").map_err(map_sqlx_error)?,
        rate_limit_tier: row.try_get("rate_limit_tier").map_err(map_sqlx_error)?,
        seat_tier: row.try_get("seat_tier").map_err(map_sqlx_error)?,
        tier_key: row.try_get("tier_key").map_err(map_sqlx_error)?,
        resolution_source: parse_resolution_source(
            &row.try_get::<String, _>("resolution_source")
                .map_err(map_sqlx_error)?,
        )?,
        resolved_ratio_snapshot: row
            .try_get("resolved_ratio_snapshot")
            .map_err(map_sqlx_error)?,
        observed_at_unix_millis: row
            .try_get("observed_at_unix_millis")
            .map_err(map_sqlx_error)?,
        effective_from_unix_millis: row
            .try_get("effective_from_unix_millis")
            .map_err(map_sqlx_error)?,
        effective_to_unix_millis: row
            .try_get("effective_to_unix_millis")
            .map_err(map_sqlx_error)?,
        provenance: row.try_get("provenance").map_err(map_sqlx_error)?,
        created_at_unix_millis: row
            .try_get("created_at_unix_millis")
            .map_err(map_sqlx_error)?,
    })
}

pub(super) fn parse_resolution_source(value: &str) -> StorageResult<TierResolutionSource> {
    match value {
        "override" => Ok(TierResolutionSource::Override),
        "builtin" => Ok(TierResolutionSource::Builtin),
        "unknown" => Ok(TierResolutionSource::Unknown),
        value => Err(StorageError::Corrupted {
            message: format!("invalid tier resolution source {value}"),
        }),
    }
}

pub(super) fn encode_absent(value: &Option<String>) -> &str {
    value.as_deref().unwrap_or("")
}

pub(super) fn invalid_input(field: &str, reason: &str) -> StorageError {
    StorageError::InvalidInput {
        field: field.to_owned(),
        reason: reason.to_owned(),
    }
}

pub(super) fn conflict(message: &str) -> StorageError {
    StorageError::Conflict {
        message: message.to_owned(),
    }
}

fn parse_uuid(value: &str, field: &str) -> StorageResult<Uuid> {
    Uuid::parse_str(value).map_err(|error| StorageError::Corrupted {
        message: format!("invalid {field} {value}: {error}"),
    })
}

fn decode_absent(value: String) -> Option<String> {
    if value.is_empty() { None } else { Some(value) }
}
