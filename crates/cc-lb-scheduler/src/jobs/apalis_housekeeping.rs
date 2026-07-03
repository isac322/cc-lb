use std::time::Duration;

#[cfg(feature = "postgres")]
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
#[cfg(feature = "postgres")]
use sqlx::Postgres;
#[cfg(feature = "sqlite")]
use sqlx::Sqlite;
use sqlx::{Database, Pool};

use crate::{
    error::{Result, SchedulerError},
    middleware::TraceparentCarrier,
};

pub const APALIS_WORKER_RETENTION_SECS: u64 = 3_600;
pub const APALIS_HOUSEKEEPING_RETRY_DELAY: Duration = Duration::from_secs(60);
pub const APALIS_STALE_LOCK_THRESHOLD_SECS: u64 = 120;
const SECONDS_PER_DAY: u64 = 86_400;
const STALE_LOCK_LAST_RESULT: &str =
    "{\"Err\":\"Re-enqueued by cc-lb housekeeping: stale Running lock exceeded threshold.\"}";

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ApalisHousekeepingJob {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
}

impl TraceparentCarrier for ApalisHousekeepingJob {
    fn traceparent(&self) -> Option<&str> {
        self.traceparent.as_deref()
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        self.traceparent = traceparent;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApalisHousekeepingConfig {
    pub dlq_retention_days: u32,
    pub stale_lock_threshold_secs: u64,
}

impl ApalisHousekeepingConfig {
    pub const fn new(dlq_retention_days: u32) -> Self {
        Self {
            dlq_retention_days,
            stale_lock_threshold_secs: APALIS_STALE_LOCK_THRESHOLD_SECS,
        }
    }

    pub const fn with_stale_lock_threshold_secs(mut self, secs: u64) -> Self {
        self.stale_lock_threshold_secs = secs;
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApalisHousekeepingJobStats {
    pub workers_removed: u64,
    pub jobs_removed: u64,
    pub stale_locks_reaped: u64,
    pub cutoff_unix_secs: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApalisHousekeepingJobResult {
    Done {
        workers_removed: u64,
        jobs_removed: u64,
        stale_locks_reaped: u64,
        cutoff_unix_secs: u64,
    },
    Retry {
        delay: Duration,
        error: String,
    },
}

#[derive(Clone, Debug)]
pub struct ApalisHousekeepingJobHandler<Db: Database> {
    pool: Pool<Db>,
    config: ApalisHousekeepingConfig,
}

impl<Db: Database> ApalisHousekeepingJobHandler<Db> {
    pub const fn new(pool: Pool<Db>, config: ApalisHousekeepingConfig) -> Self {
        Self { pool, config }
    }
}

#[derive(Clone, Debug)]
struct StaleLockRow {
    id: String,
    job_type: String,
    lock_by: Option<String>,
    lock_at_unix_secs: Option<i64>,
    attempts: i64,
}

#[cfg(feature = "sqlite")]
impl ApalisHousekeepingJobHandler<Sqlite> {
    pub async fn handle(
        &self,
        job: ApalisHousekeepingJob,
        now_unix_secs: u64,
    ) -> ApalisHousekeepingJobResult {
        log_traceparent(job.traceparent.as_deref());
        finish(self.prune(now_unix_secs).await)
    }

    async fn prune(&self, now_unix_secs: u64) -> Result<ApalisHousekeepingJobStats> {
        let worker_cutoff = now_unix_secs.saturating_sub(APALIS_WORKER_RETENTION_SECS);
        let job_cutoff = retention_cutoff(now_unix_secs, self.config.dlq_retention_days);
        let workers_removed = sqlx::query("DELETE FROM Workers WHERE last_seen < ?1")
            .bind(unix_i64(worker_cutoff, "worker_cutoff")?)
            .execute(&self.pool)
            .await?
            .rows_affected();
        let jobs_removed = sqlx::query(
            "DELETE FROM Jobs WHERE status IN ('Done','Failed') AND done_at IS NOT NULL AND done_at < ?1",
        )
        .bind(unix_i64(job_cutoff, "job_cutoff")?)
        .execute(&self.pool)
        .await?
        .rows_affected();
        let reaped = self.reap_stale_locks(now_unix_secs).await?;
        log_reaped_rows(
            &reaped,
            self.config.stale_lock_threshold_secs,
            now_unix_secs,
        );
        let stale_locks_reaped = reaped.len() as u64;
        Ok(ApalisHousekeepingJobStats {
            workers_removed,
            jobs_removed,
            stale_locks_reaped,
            cutoff_unix_secs: job_cutoff,
        })
    }

    async fn reap_stale_locks(&self, now_unix_secs: u64) -> Result<Vec<StaleLockRow>> {
        let threshold_i64 = unix_i64(
            self.config.stale_lock_threshold_secs,
            "stale_lock_threshold_secs",
        )?;
        let now_i64 = unix_i64(now_unix_secs, "now_unix_secs")?;
        let mut tx = self.pool.begin().await?;
        let rows = sqlx::query_as::<_, (String, String, Option<String>, Option<i64>, i64)>(
            "SELECT id, job_type, lock_by, lock_at, attempts
             FROM Jobs
             WHERE status = 'Running'
               AND lock_at IS NOT NULL
               AND (?1 - lock_at) > ?2",
        )
        .bind(now_i64)
        .bind(threshold_i64)
        .fetch_all(&mut *tx)
        .await?;
        if rows.is_empty() {
            tx.commit().await?;
            return Ok(Vec::new());
        }
        sqlx::query(
            "UPDATE Jobs
             SET status = 'Pending',
                 done_at = NULL,
                 lock_by = NULL,
                 lock_at = NULL,
                 attempts = attempts + 1,
                 last_result = ?3
             WHERE status = 'Running'
               AND lock_at IS NOT NULL
               AND (?1 - lock_at) > ?2",
        )
        .bind(now_i64)
        .bind(threshold_i64)
        .bind(STALE_LOCK_LAST_RESULT)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(rows
            .into_iter()
            .map(|(id, job_type, lock_by, lock_at, attempts)| StaleLockRow {
                id,
                job_type,
                lock_by,
                lock_at_unix_secs: lock_at,
                attempts,
            })
            .collect())
    }
}

#[cfg(feature = "postgres")]
impl ApalisHousekeepingJobHandler<Postgres> {
    pub async fn handle(
        &self,
        job: ApalisHousekeepingJob,
        now_unix_secs: u64,
    ) -> ApalisHousekeepingJobResult {
        log_traceparent(job.traceparent.as_deref());
        finish(self.prune(now_unix_secs).await)
    }

    async fn prune(&self, now_unix_secs: u64) -> Result<ApalisHousekeepingJobStats> {
        let worker_cutoff = utc_timestamp(
            now_unix_secs.saturating_sub(APALIS_WORKER_RETENTION_SECS),
            "worker_cutoff",
        )?;
        let job_cutoff = retention_cutoff(now_unix_secs, self.config.dlq_retention_days);
        let job_cutoff_timestamp = utc_timestamp(job_cutoff, "job_cutoff")?;
        let workers_removed = sqlx::query("DELETE FROM apalis.workers WHERE last_seen < $1")
            .bind(worker_cutoff)
            .execute(&self.pool)
            .await?
            .rows_affected();
        let jobs_removed = sqlx::query(
            "DELETE FROM apalis.jobs WHERE status IN ('Done','Failed') AND done_at IS NOT NULL AND done_at < $1",
        )
        .bind(job_cutoff_timestamp)
        .execute(&self.pool)
        .await?
        .rows_affected();
        let reaped = self.reap_stale_locks(now_unix_secs).await?;
        log_reaped_rows(
            &reaped,
            self.config.stale_lock_threshold_secs,
            now_unix_secs,
        );
        let stale_locks_reaped = reaped.len() as u64;
        Ok(ApalisHousekeepingJobStats {
            workers_removed,
            jobs_removed,
            stale_locks_reaped,
            cutoff_unix_secs: job_cutoff,
        })
    }

    async fn reap_stale_locks(&self, now_unix_secs: u64) -> Result<Vec<StaleLockRow>> {
        let threshold_i64 = unix_i64(
            self.config.stale_lock_threshold_secs,
            "stale_lock_threshold_secs",
        )?;
        let now_ts = utc_timestamp(now_unix_secs, "now_unix_secs")?;
        let mut tx = self.pool.begin().await?;
        let rows =
            sqlx::query_as::<_, (String, String, Option<String>, Option<DateTime<Utc>>, i64)>(
                "SELECT id, job_type, lock_by, lock_at, attempts
             FROM apalis.jobs
             WHERE status = 'Running'
               AND lock_at IS NOT NULL
               AND EXTRACT(EPOCH FROM ($1::timestamptz - lock_at))::BIGINT > $2",
            )
            .bind(now_ts)
            .bind(threshold_i64)
            .fetch_all(&mut *tx)
            .await?;
        if rows.is_empty() {
            tx.commit().await?;
            return Ok(Vec::new());
        }
        sqlx::query(
            "UPDATE apalis.jobs
             SET status = 'Pending',
                 done_at = NULL,
                 lock_by = NULL,
                 lock_at = NULL,
                 attempts = attempts + 1,
                 last_result = $3
             WHERE status = 'Running'
               AND lock_at IS NOT NULL
               AND EXTRACT(EPOCH FROM ($1::timestamptz - lock_at))::BIGINT > $2",
        )
        .bind(now_ts)
        .bind(threshold_i64)
        .bind(STALE_LOCK_LAST_RESULT)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(rows
            .into_iter()
            .map(|(id, job_type, lock_by, lock_at, attempts)| StaleLockRow {
                id,
                job_type,
                lock_by,
                lock_at_unix_secs: lock_at.map(|ts| ts.timestamp()),
                attempts,
            })
            .collect())
    }
}

fn retention_cutoff(now_unix_secs: u64, retention_days: u32) -> u64 {
    now_unix_secs.saturating_sub(u64::from(retention_days).saturating_mul(SECONDS_PER_DAY))
}

fn finish(result: Result<ApalisHousekeepingJobStats>) -> ApalisHousekeepingJobResult {
    match result {
        Ok(stats) => ApalisHousekeepingJobResult::Done {
            workers_removed: stats.workers_removed,
            jobs_removed: stats.jobs_removed,
            stale_locks_reaped: stats.stale_locks_reaped,
            cutoff_unix_secs: stats.cutoff_unix_secs,
        },
        Err(error) => ApalisHousekeepingJobResult::Retry {
            delay: APALIS_HOUSEKEEPING_RETRY_DELAY,
            error: error.to_string(),
        },
    }
}

fn log_traceparent(traceparent: Option<&str>) {
    if let Some(traceparent) = traceparent {
        tracing::debug!(traceparent, "handling apalis housekeeping job");
    }
}

fn log_reaped_rows(rows: &[StaleLockRow], threshold_secs: u64, now_unix_secs: u64) {
    if rows.is_empty() {
        return;
    }
    tracing::error!(
        count = rows.len(),
        threshold_secs = threshold_secs,
        "apalis housekeeping reaped stale Running locks; a scheduler worker acquired jobs and never released them. Investigate immediately",
    );
    for row in rows {
        let lock_age_secs = row.lock_at_unix_secs.map(|lock_at| {
            let lock_at_u = u64::try_from(lock_at).unwrap_or(0);
            now_unix_secs.saturating_sub(lock_at_u)
        });
        tracing::warn!(
            job_id = %row.id,
            job_type = %row.job_type,
            lock_by = row.lock_by.as_deref().unwrap_or("<none>"),
            lock_at_unix_secs = row.lock_at_unix_secs.unwrap_or(0),
            lock_age_secs = lock_age_secs.unwrap_or(0),
            attempts = row.attempts,
            "reaped stale apalis job lock and re-enqueued as Pending",
        );
    }
}

fn unix_i64(value: u64, field: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| SchedulerError::Job(format!("{field} exceeds i64::MAX")))
}

#[cfg(feature = "postgres")]
fn utc_timestamp(value: u64, field: &str) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp(unix_i64(value, field)?, 0)
        .ok_or_else(|| SchedulerError::Job(format!("{field} is outside timestamp range")))
}

#[cfg(test)]
mod tests;
