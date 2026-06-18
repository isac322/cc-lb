use std::collections::HashSet;

use sqlx::{Pool, Row, Sqlite};

use super::specs::{ENTITY_MAX_ATTEMPTS, ReconcileJobSpec};
use super::{i64_to_u32, i64_to_u64, row_id, safe_summary, u64_to_i64};
use crate::error::Result;
use crate::idempotency::SchedulerFailuresStore;

pub(super) async fn ensure_job(
    pool: &Pool<Sqlite>,
    job: &ReconcileJobSpec,
    now_unix_secs: u64,
) -> Result<u64> {
    let id = row_id(&job.idempotency_key, now_unix_secs);
    let result = sqlx::query(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, last_result, lock_at, lock_by, done_at, priority, metadata, idempotency_key) \
         SELECT ?1, ?2, ?3, 'Pending', 0, ?4, ?5, NULL, NULL, NULL, NULL, 0, ?6, ?7 \
         WHERE NOT EXISTS (SELECT 1 FROM Jobs WHERE job_type = ?3 AND idempotency_key = ?7 AND status IN ('Pending','Running','Queued'))",
    )
    .bind(&job.payload)
    .bind(id)
    .bind(job.job_type)
    .bind(ENTITY_MAX_ATTEMPTS)
    .bind(u64_to_i64(now_unix_secs, "now_unix_secs")?)
    .bind("{}")
    .bind(&job.idempotency_key)
    .execute(pool)
    .await;
    rows_or_ignore_unique(result)
}

pub(super) async fn prune_orphans(pool: &Pool<Sqlite>, live_keys: &HashSet<String>) -> Result<u64> {
    let rows = sqlx::query(
        "SELECT job_type, idempotency_key FROM Jobs WHERE job_type IN ('entity:warmup','entity:oauth_refresh','entity:oauth_usage_poll') AND status IN ('Pending','Running','Queued')",
    )
    .fetch_all(pool)
    .await?;
    let mut pruned = 0;
    for row in rows {
        let key: String = row.try_get("idempotency_key")?;
        if !live_keys.contains(&key) {
            let job_type: String = row.try_get("job_type")?;
            let rows = sqlx::query(
                "DELETE FROM Jobs WHERE idempotency_key = ?1 AND status IN ('Pending','Running','Queued')",
            )
            .bind(key)
            .execute(pool)
            .await?
            .rows_affected();
            crate::scheduler_metrics::record_reconcile_orphan_pruned(&job_type, rows);
            pruned += rows;
        }
    }
    Ok(pruned)
}

pub(super) async fn surface_failures(pool: &Pool<Sqlite>, now_unix_secs: u64) -> Result<u64> {
    let rows = sqlx::query(
        "SELECT job_type, idempotency_key, COALESCE(last_result, 'unknown') AS last_error, attempts, COALESCE(done_at, run_at) AS failed_at \
         FROM Jobs WHERE status = 'Failed' AND attempts >= max_attempts",
    )
    .fetch_all(pool)
    .await?;
    let store = SchedulerFailuresStore::new(pool.clone());
    let mut recorded = 0;
    for row in rows {
        let job_type: String = row.try_get("job_type")?;
        let summary = safe_summary(&job_type, row.try_get("idempotency_key")?);
        let failed_at = i64_to_u64(row.try_get("failed_at")?, "failed_at")?;
        if failure_exists(pool, &job_type, &summary, failed_at).await? {
            continue;
        }
        let attempts = i64_to_u32(row.try_get("attempts")?, "attempts")?;
        let last_error: String = row.try_get("last_error")?;
        store
            .record(&job_type, &summary, &last_error, attempts, now_unix_secs)
            .await?;
        crate::scheduler_metrics::record_scheduler_failure(&job_type, 1);
        recorded += 1;
    }
    Ok(recorded)
}

async fn failure_exists(
    pool: &Pool<Sqlite>,
    job_type: &str,
    payload_summary: &str,
    failed_at: u64,
) -> Result<bool> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM scheduler_failures WHERE job_type = ?1 AND payload_summary = ?2 AND last_failed_at_unix_secs = ?3",
    )
    .bind(job_type)
    .bind(payload_summary)
    .bind(u64_to_i64(failed_at, "failed_at")?)
    .fetch_one(pool)
    .await?;
    Ok(count > 0)
}

fn rows_or_ignore_unique(
    result: std::result::Result<sqlx::sqlite::SqliteQueryResult, sqlx::Error>,
) -> Result<u64> {
    match result {
        Ok(done) => Ok(done.rows_affected()),
        Err(sqlx::Error::Database(error)) if error.is_unique_violation() => Ok(0),
        Err(error) => Err(error.into()),
    }
}
