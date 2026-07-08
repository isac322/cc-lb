use cc_lb_storage_api::{PlanTierRatioRecord, StorageError, StorageResult};
use sqlx::Row;

use crate::{adapter::PostgresStorage, error_map::map_sqlx_error};

const PLAN_TIER_RATIO_LOCK_CLASSID: i32 = 69_001;

pub(super) async fn upsert(
    storage: &PostgresStorage,
    record: &PlanTierRatioRecord,
) -> StorageResult<()> {
    if !record.pro_relative_ratio.is_finite() || record.pro_relative_ratio <= 0.0 {
        return Err(StorageError::InvalidInput {
            field: "plan_tier_ratio.pro_relative_ratio".to_owned(),
            reason: "must be finite and greater than zero".to_owned(),
        });
    }

    let mut tx = storage.pool.begin().await.map_err(map_sqlx_error)?;
    super::lock_logical_key(
        &mut tx,
        PLAN_TIER_RATIO_LOCK_CLASSID,
        record.tier_key.as_str(),
    )
    .await?;
    let open_row = sqlx::query(
        "SELECT pro_relative_ratio, effective_from_unix_millis \
         FROM plan_tier_ratio_history_v1 \
         WHERE tier_key = $1 AND effective_to_unix_millis IS NULL \
         FOR UPDATE",
    )
    .bind(record.tier_key.as_str())
    .fetch_optional(&mut *tx)
    .await
    .map_err(map_sqlx_error)?;

    if let Some(row) = open_row {
        let open_ratio = row
            .try_get::<f64, _>("pro_relative_ratio")
            .map_err(map_sqlx_error)?;
        let open_effective_from = row
            .try_get::<i64, _>("effective_from_unix_millis")
            .map_err(map_sqlx_error)?;
        if open_ratio == record.pro_relative_ratio {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(());
        }
        require_effective_from_advance(record.effective_from_unix_millis, open_effective_from)?;
        sqlx::query(
            "UPDATE plan_tier_ratio_history_v1 \
             SET effective_to_unix_millis = $2 \
             WHERE tier_key = $1 AND effective_to_unix_millis IS NULL",
        )
        .bind(record.tier_key.as_str())
        .bind(record.effective_from_unix_millis)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
    }

    sqlx::query(
        "INSERT INTO plan_tier_ratio_history_v1 \
         (tier_key, pro_relative_ratio, effective_from_unix_millis, effective_to_unix_millis, \
          provenance, created_at_unix_millis) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(record.tier_key.as_str())
    .bind(record.pro_relative_ratio)
    .bind(record.effective_from_unix_millis)
    .bind(Option::<i64>::None)
    .bind(record.provenance.as_str())
    .bind(record.created_at_unix_millis)
    .execute(&mut *tx)
    .await
    .map_err(map_sqlx_error)?;

    tx.commit().await.map_err(map_sqlx_error)
}

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

pub(super) async fn list_as_of(
    storage: &PostgresStorage,
    as_of_unix_millis: i64,
) -> StorageResult<Vec<PlanTierRatioRecord>> {
    let rows = sqlx::query(
        "SELECT tier_key, pro_relative_ratio, effective_from_unix_millis, \
                effective_to_unix_millis, provenance, created_at_unix_millis \
         FROM plan_tier_ratio_history_v1 \
         WHERE effective_from_unix_millis <= $1 \
           AND (effective_to_unix_millis IS NULL OR effective_to_unix_millis > $1) \
         ORDER BY tier_key ASC",
    )
    .bind(as_of_unix_millis)
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

fn require_effective_from_advance(new_value: i64, open_value: i64) -> StorageResult<()> {
    if new_value <= open_value {
        return Err(StorageError::Conflict {
            message: "plan tier ratio effective_from_unix_millis must advance".to_owned(),
        });
    }
    Ok(())
}
