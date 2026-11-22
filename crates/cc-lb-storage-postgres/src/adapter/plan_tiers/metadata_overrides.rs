use cc_lb_storage_api::{MetadataTierMappingOverrideRecord, StorageResult};
use sqlx::Row;

use crate::{adapter::PostgresStorage, error_map::map_sqlx_error};

pub(super) async fn list_current(
    storage: &PostgresStorage,
) -> StorageResult<Vec<MetadataTierMappingOverrideRecord>> {
    let rows = sqlx::query(
        "SELECT organization_type, rate_limit_tier, seat_tier, tier_key, \
                effective_from_unix_millis, effective_to_unix_millis, provenance, \
                created_at_unix_millis \
         FROM metadata_tier_mapping_override_v1 \
         WHERE effective_to_unix_millis IS NULL \
         ORDER BY organization_type ASC, rate_limit_tier ASC, seat_tier ASC",
    )
    .fetch_all(&storage.pool)
    .await
    .map_err(map_sqlx_error)?;
    rows.into_iter().map(row_to_record).collect()
}

fn row_to_record(row: sqlx::postgres::PgRow) -> StorageResult<MetadataTierMappingOverrideRecord> {
    Ok(MetadataTierMappingOverrideRecord {
        organization_type: empty_to_absent(
            row.try_get("organization_type").map_err(map_sqlx_error)?,
        ),
        rate_limit_tier: empty_to_absent(row.try_get("rate_limit_tier").map_err(map_sqlx_error)?),
        seat_tier: empty_to_absent(row.try_get("seat_tier").map_err(map_sqlx_error)?),
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

fn empty_to_absent(value: String) -> Option<String> {
    if value.is_empty() { None } else { Some(value) }
}
