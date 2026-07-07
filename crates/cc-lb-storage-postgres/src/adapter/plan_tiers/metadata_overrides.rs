use cc_lb_storage_api::{MetadataTierMappingOverrideRecord, StorageError, StorageResult};
use sqlx::Row;

use crate::{adapter::PostgresStorage, error_map::map_sqlx_error};

const METADATA_TIER_OVERRIDE_LOCK_CLASSID: i32 = 69_002;

pub(super) async fn upsert(
    storage: &PostgresStorage,
    record: &MetadataTierMappingOverrideRecord,
) -> StorageResult<()> {
    let organization_type = absent_to_empty(&record.organization_type);
    let rate_limit_tier = absent_to_empty(&record.rate_limit_tier);
    let seat_tier = absent_to_empty(&record.seat_tier);
    let lock_key = format!("{organization_type}\x1f{rate_limit_tier}\x1f{seat_tier}");
    let mut tx = storage.pool.begin().await.map_err(map_sqlx_error)?;
    super::lock_logical_key(&mut tx, METADATA_TIER_OVERRIDE_LOCK_CLASSID, &lock_key).await?;
    let open_row = sqlx::query(
        "SELECT tier_key, effective_from_unix_millis \
         FROM metadata_tier_mapping_override_v1 \
         WHERE organization_type = $1 AND rate_limit_tier = $2 AND seat_tier = $3 \
           AND effective_to_unix_millis IS NULL \
         FOR UPDATE",
    )
    .bind(organization_type)
    .bind(rate_limit_tier)
    .bind(seat_tier)
    .fetch_optional(&mut *tx)
    .await
    .map_err(map_sqlx_error)?;

    if let Some(row) = open_row {
        let open_tier_key = row
            .try_get::<String, _>("tier_key")
            .map_err(map_sqlx_error)?;
        let open_effective_from = row
            .try_get::<i64, _>("effective_from_unix_millis")
            .map_err(map_sqlx_error)?;
        if open_tier_key == record.tier_key {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(());
        }
        require_effective_from_advance(record.effective_from_unix_millis, open_effective_from)?;
        sqlx::query(
            "UPDATE metadata_tier_mapping_override_v1 \
             SET effective_to_unix_millis = $4 \
             WHERE organization_type = $1 AND rate_limit_tier = $2 AND seat_tier = $3 \
               AND effective_to_unix_millis IS NULL",
        )
        .bind(organization_type)
        .bind(rate_limit_tier)
        .bind(seat_tier)
        .bind(record.effective_from_unix_millis)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
    }

    sqlx::query(
        "INSERT INTO metadata_tier_mapping_override_v1 \
         (organization_type, rate_limit_tier, seat_tier, tier_key, effective_from_unix_millis, \
          effective_to_unix_millis, provenance, created_at_unix_millis) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(organization_type)
    .bind(rate_limit_tier)
    .bind(seat_tier)
    .bind(record.tier_key.as_str())
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

pub(super) async fn list_as_of(
    storage: &PostgresStorage,
    as_of_unix_millis: i64,
) -> StorageResult<Vec<MetadataTierMappingOverrideRecord>> {
    let rows = sqlx::query(
        "SELECT organization_type, rate_limit_tier, seat_tier, tier_key, \
                effective_from_unix_millis, effective_to_unix_millis, provenance, \
                created_at_unix_millis \
         FROM metadata_tier_mapping_override_v1 \
         WHERE effective_from_unix_millis <= $1 \
           AND (effective_to_unix_millis IS NULL OR effective_to_unix_millis > $1) \
         ORDER BY organization_type ASC, rate_limit_tier ASC, seat_tier ASC",
    )
    .bind(as_of_unix_millis)
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

fn require_effective_from_advance(new_value: i64, open_value: i64) -> StorageResult<()> {
    if new_value <= open_value {
        return Err(StorageError::Conflict {
            message: "metadata tier override effective_from_unix_millis must advance".to_owned(),
        });
    }
    Ok(())
}

fn absent_to_empty(value: &Option<String>) -> &str {
    value.as_deref().unwrap_or("")
}

fn empty_to_absent(value: String) -> Option<String> {
    if value.is_empty() { None } else { Some(value) }
}
