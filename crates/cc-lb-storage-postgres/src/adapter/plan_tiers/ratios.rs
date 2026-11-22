use cc_lb_storage_api::{PlanTierRatioRecord, StorageResult};
use sqlx::Row;

use crate::{adapter::PostgresStorage, error_map::map_sqlx_error};

pub(super) async fn list_current(
    storage: &PostgresStorage,
) -> StorageResult<Vec<PlanTierRatioRecord>> {
    let rows = sqlx::query(
        "SELECT tier_key, pro_relative_ratio, effective_from_unix_millis, \
                effective_to_unix_millis, provenance, created_at_unix_millis \
         FROM plan_tier_ratio_history_v1 \
         WHERE effective_to_unix_millis IS NULL \
         ORDER BY tier_key ASC",
    )
    .fetch_all(&storage.pool)
    .await
    .map_err(map_sqlx_error)?;
    rows.into_iter().map(row_to_record).collect()
}

fn row_to_record(row: sqlx::postgres::PgRow) -> StorageResult<PlanTierRatioRecord> {
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
