use cc_lb_storage_api::{
    StorageError, StorageResult, TierResolutionSource, UpstreamPlanTierRecord,
};
use sqlx::Row;

use crate::{adapter::PostgresStorage, error_map::map_sqlx_error};

const UPSTREAM_PLAN_TIER_LOCK_CLASSID: i32 = 69_003;

pub(super) async fn append(
    storage: &PostgresStorage,
    record: &UpstreamPlanTierRecord,
) -> StorageResult<()> {
    validate_tier_key(record)?;
    let mut tx = storage.pool.begin().await.map_err(map_sqlx_error)?;
    let lock_key = record.upstream_id.to_string();
    super::lock_logical_key(&mut tx, UPSTREAM_PLAN_TIER_LOCK_CLASSID, &lock_key).await?;
    let open_row = sqlx::query(
        "SELECT tier_key, resolution_source, organization_type, rate_limit_tier, \
                seat_tier, effective_from_unix_millis \
         FROM upstream_plan_tier_history_v1 \
         WHERE upstream_id = $1 AND effective_to_unix_millis IS NULL \
         FOR UPDATE",
    )
    .bind(record.upstream_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(map_sqlx_error)?;

    if let Some(row) = open_row {
        let open_effective_from = row
            .try_get::<i64, _>("effective_from_unix_millis")
            .map_err(map_sqlx_error)?;
        if open_row_matches(&row, record)? {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(());
        }
        require_effective_from_advance(record.effective_from_unix_millis, open_effective_from)?;
        sqlx::query(
            "UPDATE upstream_plan_tier_history_v1 \
             SET effective_to_unix_millis = $2 \
             WHERE upstream_id = $1 AND effective_to_unix_millis IS NULL",
        )
        .bind(record.upstream_id)
        .bind(record.effective_from_unix_millis)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
    }

    sqlx::query(
        "INSERT INTO upstream_plan_tier_history_v1 \
         (upstream_id, organization_uuid, organization_type, rate_limit_tier, seat_tier, tier_key, \
          resolution_source, resolved_ratio_snapshot, observed_at_unix_millis, \
          effective_from_unix_millis, effective_to_unix_millis, provenance, created_at_unix_millis) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
    )
    .bind(record.upstream_id)
    .bind(record.organization_uuid.as_deref())
    .bind(record.organization_type.as_deref())
    .bind(record.rate_limit_tier.as_deref())
    .bind(record.seat_tier.as_deref())
    .bind(record.tier_key.as_deref())
    .bind(record.resolution_source.as_str())
    .bind(record.resolved_ratio_snapshot)
    .bind(record.observed_at_unix_millis)
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
) -> StorageResult<Vec<UpstreamPlanTierRecord>> {
    let rows = sqlx::query(
        "SELECT upstream_id, organization_uuid, organization_type, rate_limit_tier, seat_tier, \
                tier_key, resolution_source, resolved_ratio_snapshot, observed_at_unix_millis, \
                effective_from_unix_millis, effective_to_unix_millis, provenance, \
                created_at_unix_millis \
         FROM upstream_plan_tier_history_v1 \
         WHERE effective_to_unix_millis IS NULL \
         ORDER BY upstream_id ASC",
    )
    .fetch_all(&storage.pool)
    .await
    .map_err(map_sqlx_error)?;
    rows.into_iter().map(row_to_record).collect()
}

pub(super) async fn list_as_of(
    storage: &PostgresStorage,
    as_of_unix_millis: i64,
) -> StorageResult<Vec<UpstreamPlanTierRecord>> {
    let rows = sqlx::query(
        "SELECT upstream_id, organization_uuid, organization_type, rate_limit_tier, seat_tier, \
                tier_key, resolution_source, resolved_ratio_snapshot, observed_at_unix_millis, \
                effective_from_unix_millis, effective_to_unix_millis, provenance, \
                created_at_unix_millis \
         FROM upstream_plan_tier_history_v1 \
         WHERE effective_from_unix_millis <= $1 \
           AND (effective_to_unix_millis IS NULL OR effective_to_unix_millis > $1) \
         ORDER BY upstream_id ASC",
    )
    .bind(as_of_unix_millis)
    .fetch_all(&storage.pool)
    .await
    .map_err(map_sqlx_error)?;
    rows.into_iter().map(row_to_record).collect()
}

fn row_to_record(row: sqlx::postgres::PgRow) -> StorageResult<UpstreamPlanTierRecord> {
    let source_text = row
        .try_get::<String, _>("resolution_source")
        .map_err(map_sqlx_error)?;
    Ok(UpstreamPlanTierRecord {
        upstream_id: row.try_get("upstream_id").map_err(map_sqlx_error)?,
        organization_uuid: row.try_get("organization_uuid").map_err(map_sqlx_error)?,
        organization_type: row.try_get("organization_type").map_err(map_sqlx_error)?,
        rate_limit_tier: row.try_get("rate_limit_tier").map_err(map_sqlx_error)?,
        seat_tier: row.try_get("seat_tier").map_err(map_sqlx_error)?,
        tier_key: row.try_get("tier_key").map_err(map_sqlx_error)?,
        resolution_source: source_from_str(&source_text)?,
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

fn open_row_matches(
    row: &sqlx::postgres::PgRow,
    record: &UpstreamPlanTierRecord,
) -> StorageResult<bool> {
    let source_text = row
        .try_get::<String, _>("resolution_source")
        .map_err(map_sqlx_error)?;
    Ok(row
        .try_get::<Option<String>, _>("tier_key")
        .map_err(map_sqlx_error)?
        == record.tier_key
        && source_from_str(&source_text)? == record.resolution_source
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

fn validate_tier_key(record: &UpstreamPlanTierRecord) -> StorageResult<()> {
    match record.resolution_source {
        TierResolutionSource::Unknown => {
            if record.tier_key.is_some() {
                return Err(invalid_tier_key(
                    "must be absent when resolution_source is unknown",
                ));
            }
            if record.resolved_ratio_snapshot.is_some() {
                return Err(StorageError::InvalidInput {
                    field: "upstream_plan_tier.resolved_ratio_snapshot".to_owned(),
                    reason: "must be absent when resolution_source is unknown".to_owned(),
                });
            }
            Ok(())
        }
        TierResolutionSource::Override
        | TierResolutionSource::Builtin
        | TierResolutionSource::Backfill => {
            if record.tier_key.is_none() {
                return Err(invalid_tier_key(
                    "must be present when resolution_source is override or builtin",
                ));
            }
            if record.resolved_ratio_snapshot.is_none() {
                return Err(StorageError::InvalidInput {
                    field: "upstream_plan_tier.resolved_ratio_snapshot".to_owned(),
                    reason: "must be present when resolution_source is override or builtin"
                        .to_owned(),
                });
            }
            Ok(())
        }
    }
}

fn source_from_str(value: &str) -> StorageResult<TierResolutionSource> {
    match value {
        "override" => Ok(TierResolutionSource::Override),
        "builtin" => Ok(TierResolutionSource::Builtin),
        "unknown" => Ok(TierResolutionSource::Unknown),
        other => Err(StorageError::Corrupted {
            message: format!("unknown tier resolution source: {other}"),
        }),
    }
}

fn require_effective_from_advance(new_value: i64, open_value: i64) -> StorageResult<()> {
    if new_value <= open_value {
        return Err(StorageError::Conflict {
            message: "upstream plan tier effective_from_unix_millis must advance".to_owned(),
        });
    }
    Ok(())
}

fn invalid_tier_key(reason: &str) -> StorageError {
    StorageError::InvalidInput {
        field: "upstream_plan_tier.tier_key".to_owned(),
        reason: reason.to_owned(),
    }
}
