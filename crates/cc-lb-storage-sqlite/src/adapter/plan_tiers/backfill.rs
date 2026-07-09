use cc_lb_storage_api::{
    BackfillApplyCounts, BackfillApplyOutcome, StorageError, StorageResult, UpstreamPlanTierRecord,
};
use uuid::Uuid;

use crate::{SqliteStorage, map_sqlx_error};

use super::{codec, writes};

pub(super) async fn apply(
    storage: &SqliteStorage,
    upstream_id: Uuid,
    intervals: &[UpstreamPlanTierRecord],
    terminal_cap_unix_millis: i64,
    provenance: &str,
) -> StorageResult<BackfillApplyOutcome> {
    let upstream_id_text = upstream_id.to_string();
    let mut tx = storage.begin_immediate().await?;
    let has_provenance = sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM upstream_plan_tier_history_v1 WHERE upstream_id = ? AND provenance = ? LIMIT 1",
    )
    .bind(&upstream_id_text)
    .bind(provenance)
    .fetch_optional(&mut *tx)
    .await
    .map_err(map_sqlx_error)?;
    if has_provenance.is_some() {
        tx.commit().await.map_err(map_sqlx_error)?;
        return Ok(BackfillApplyOutcome::Skipped);
    }

    let cutoff = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT MIN(effective_from_unix_millis) FROM upstream_plan_tier_history_v1 WHERE upstream_id = ?",
    )
    .bind(&upstream_id_text)
    .fetch_one(&mut *tx)
    .await
    .map_err(map_sqlx_error)?;
    let cap = cutoff
        .map(|cutoff| cutoff.min(terminal_cap_unix_millis))
        .unwrap_or(terminal_cap_unix_millis);
    let mut counts = BackfillApplyCounts {
        inserted: 0,
        skipped_zero_dur: 0,
        capped: 0,
    };
    let mut prepared = Vec::with_capacity(intervals.len());

    for interval in intervals {
        validate_interval(upstream_id, interval, provenance)?;
        let Some(clamped_to) = clamped_effective_to(interval, cap, &mut counts)? else {
            continue;
        };
        prepared.push(PreparedBackfillInterval {
            record: interval,
            clamped_to,
        });
    }

    validate_batch_intervals_do_not_overlap(&prepared)?;
    for interval in &prepared {
        if existing_row_conflicts(&mut tx, interval.record, interval.clamped_to).await? {
            return Err(StorageError::Conflict {
                message: "upstream plan tier backfill conflicts with existing row".to_owned(),
            });
        }
    }
    for interval in prepared {
        writes::insert_closed_upstream(&mut tx, interval.record, interval.clamped_to).await?;
        counts.inserted += 1;
    }

    tx.commit().await.map_err(map_sqlx_error)?;
    Ok(BackfillApplyOutcome::Applied(counts))
}

struct PreparedBackfillInterval<'a> {
    record: &'a UpstreamPlanTierRecord,
    clamped_to: i64,
}

fn validate_batch_intervals_do_not_overlap(
    intervals: &[PreparedBackfillInterval<'_>],
) -> StorageResult<()> {
    let mut ordered = intervals.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|interval| {
        (
            interval.record.effective_from_unix_millis,
            interval.clamped_to,
        )
    });
    let mut previous_to = None;
    for interval in ordered {
        if previous_to.is_some_and(|to| interval.record.effective_from_unix_millis < to) {
            return Err(StorageError::Conflict {
                message: "upstream plan tier backfill batch contains overlapping intervals"
                    .to_owned(),
            });
        }
        previous_to = Some(interval.clamped_to);
    }
    Ok(())
}

fn validate_interval(
    upstream_id: Uuid,
    record: &UpstreamPlanTierRecord,
    provenance: &str,
) -> StorageResult<()> {
    if record.upstream_id != upstream_id {
        return Err(StorageError::InvalidInput {
            field: "upstream_plan_tier.upstream_id".to_owned(),
            reason: "must match the backfilled upstream_id".to_owned(),
        });
    }
    if record.provenance != provenance {
        return Err(StorageError::InvalidInput {
            field: "upstream_plan_tier.provenance".to_owned(),
            reason: "must match the backfill provenance".to_owned(),
        });
    }
    writes::validate_upstream_tier_key(record)
}

fn clamped_effective_to(
    interval: &UpstreamPlanTierRecord,
    cap: i64,
    counts: &mut BackfillApplyCounts,
) -> StorageResult<Option<i64>> {
    if interval.effective_from_unix_millis >= cap {
        counts.skipped_zero_dur += 1;
        return Ok(None);
    }
    let effective_to =
        interval
            .effective_to_unix_millis
            .ok_or_else(|| StorageError::InvalidInput {
                field: "upstream_plan_tier.effective_to_unix_millis".to_owned(),
                reason: "backfill intervals must be closed".to_owned(),
            })?;
    let clamped_to = effective_to.min(cap);
    if clamped_to < effective_to {
        counts.capped += 1;
    }
    if clamped_to <= interval.effective_from_unix_millis {
        counts.skipped_zero_dur += 1;
        return Ok(None);
    }
    Ok(Some(clamped_to))
}

async fn existing_row_conflicts(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    record: &UpstreamPlanTierRecord,
    effective_to_unix_millis: i64,
) -> StorageResult<bool> {
    let row = sqlx::query(
        "SELECT upstream_id, organization_uuid, organization_type, rate_limit_tier, seat_tier, tier_key, resolution_source, resolved_ratio_snapshot, observed_at_unix_millis, effective_from_unix_millis, effective_to_unix_millis, provenance, created_at_unix_millis FROM upstream_plan_tier_history_v1 WHERE upstream_id = ? AND effective_from_unix_millis = ?",
    )
    .bind(record.upstream_id.to_string())
    .bind(record.effective_from_unix_millis)
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    let Some(row) = row else {
        return Ok(false);
    };
    Ok(!rows_match(
        &codec::row_to_upstream(row)?,
        record,
        effective_to_unix_millis,
    ))
}

fn rows_match(
    existing: &UpstreamPlanTierRecord,
    record: &UpstreamPlanTierRecord,
    effective_to_unix_millis: i64,
) -> bool {
    existing.upstream_id == record.upstream_id
        && existing.organization_uuid == record.organization_uuid
        && existing.organization_type == record.organization_type
        && existing.rate_limit_tier == record.rate_limit_tier
        && existing.seat_tier == record.seat_tier
        && existing.tier_key == record.tier_key
        && existing.resolution_source == record.resolution_source
        && existing.resolved_ratio_snapshot == record.resolved_ratio_snapshot
        && existing.observed_at_unix_millis == record.observed_at_unix_millis
        && existing.effective_from_unix_millis == record.effective_from_unix_millis
        && existing.effective_to_unix_millis == Some(effective_to_unix_millis)
        && existing.provenance == record.provenance
        && existing.created_at_unix_millis == record.created_at_unix_millis
}
