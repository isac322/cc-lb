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
    worker::CACHE_KEEPALIVE_QUEUE,
};

pub const APALIS_WORKER_RETENTION_SECS: u64 = 3_600;
pub const APALIS_HOUSEKEEPING_RETRY_DELAY: Duration = Duration::from_secs(60);
pub const APALIS_STALE_LOCK_THRESHOLD_SECS: u64 = 120;
const CACHE_KEEPALIVE_IDEMPOTENCY_PREFIX: &str = "cache_keepalive:%";
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
    pub cache_keepalive_sessions_removed: u64,
    pub cache_keepalive_jobs_removed: u64,
    pub cutoff_unix_secs: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApalisHousekeepingJobResult {
    Done {
        workers_removed: u64,
        jobs_removed: u64,
        stale_locks_reaped: u64,
        cache_keepalive_sessions_removed: u64,
        cache_keepalive_jobs_removed: u64,
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
        let reaped = self.reap_stale_locks(now_unix_secs).await?;
        let cache_keepalive_sessions_removed = self
            .prune_cache_keepalive_sessions(now_unix_secs, worker_cutoff)
            .await?;
        let workers_removed = self.prune_stale_workers(worker_cutoff).await?;
        let jobs_removed = sqlx::query(
            "DELETE FROM Jobs WHERE status IN ('Done','Failed','Killed') AND done_at IS NOT NULL AND done_at < ?1",
        )
        .bind(unix_i64(job_cutoff, "job_cutoff")?)
        .execute(&self.pool)
        .await?
        .rows_affected();
        let cache_keepalive_jobs_removed = self.prune_cache_keepalive_jobs(worker_cutoff).await?;
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
            cache_keepalive_sessions_removed,
            cache_keepalive_jobs_removed,
            cutoff_unix_secs: job_cutoff,
        })
    }

    async fn prune_stale_workers(&self, worker_cutoff: u64) -> Result<u64> {
        let worker_cutoff = unix_i64(worker_cutoff, "worker_cutoff")?;
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "UPDATE Jobs
             SET lock_by = NULL
             WHERE lock_by IN (SELECT id FROM Workers WHERE last_seen < ?1)",
        )
        .bind(worker_cutoff)
        .execute(&mut *tx)
        .await?;
        let removed = sqlx::query("DELETE FROM Workers WHERE last_seen < ?1")
            .bind(worker_cutoff)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        tx.commit().await?;
        Ok(removed)
    }

    async fn prune_cache_keepalive_sessions(
        &self,
        now_unix_secs: u64,
        stale_pending_cutoff: u64,
    ) -> Result<u64> {
        let stale_running_cutoff =
            now_unix_secs.saturating_sub(self.config.stale_lock_threshold_secs);
        let recovered = sqlx::query(
            "UPDATE cache_keepalive_sessions
             SET status = 'terminal', terminal_reason = 'stale', running_since_unix_secs = NULL, updated_at = ?1
             WHERE status = 'active'
               AND enqueue_state = 'running'
               AND running_since_unix_secs IS NOT NULL
               AND running_since_unix_secs < ?2",
        )
        .bind(unix_i64(now_unix_secs, "now_unix_secs")?)
        .bind(unix_i64(stale_running_cutoff, "stale_running_cutoff")?)
        .execute(&self.pool)
        .await;
        let recovered = match recovered {
            Ok(result) => result.rows_affected(),
            Err(error) if is_missing_cache_keepalive_table(&error) => return Ok(0),
            Err(error) => return Err(error.into()),
        };
        sqlx::query(
            "UPDATE Jobs
             SET status = 'Killed', done_at = ?1, lock_by = NULL, lock_at = NULL, last_result = ?3
             WHERE status = 'Running'
               AND lock_at IS NOT NULL
               AND idempotency_key LIKE ?4
               AND (?1 - lock_at) > ?2",
        )
        .bind(unix_i64(now_unix_secs, "now_unix_secs")?)
        .bind(unix_i64(
            self.config.stale_lock_threshold_secs,
            "stale_lock_threshold_secs",
        )?)
        .bind(STALE_LOCK_LAST_RESULT)
        .bind(CACHE_KEEPALIVE_IDEMPOTENCY_PREFIX)
        .execute(&self.pool)
        .await?;
        let removed = sqlx::query(
            "DELETE FROM cache_keepalive_sessions
             WHERE expires_at < ?1
                OR (status = 'active' AND enqueue_state = 'pending' AND updated_at < ?2)",
        )
        .bind(unix_i64(now_unix_secs, "now_unix_secs")?)
        .bind(unix_i64(stale_pending_cutoff, "stale_pending_cutoff")?)
        .execute(&self.pool)
        .await;
        match removed {
            Ok(result) => Ok(recovered.saturating_add(result.rows_affected())),
            Err(error) if is_missing_cache_keepalive_table(&error) => Ok(0),
            Err(error) => Err(error.into()),
        }
    }

    async fn prune_cache_keepalive_jobs(&self, cutoff_unix_secs: u64) -> Result<u64> {
        let result = sqlx::query(
            "DELETE FROM Jobs
             WHERE job_type = ?1
               AND idempotency_key LIKE ?2
               AND (
                   (status IN ('Done','Failed','Killed') AND COALESCE(done_at, run_at, 0) < ?3)
                   OR (status = 'Pending' AND run_at < ?3)
               )",
        )
        .bind(CACHE_KEEPALIVE_QUEUE)
        .bind(CACHE_KEEPALIVE_IDEMPOTENCY_PREFIX)
        .bind(unix_i64(cutoff_unix_secs, "cache_keepalive_job_cutoff")?)
        .execute(&self.pool)
        .await;
        match result {
            Ok(result) => Ok(result.rows_affected()),
            Err(error) => Err(error.into()),
        }
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
               AND COALESCE(idempotency_key, '') NOT LIKE ?3
               AND (?1 - lock_at) > ?2",
        )
        .bind(now_i64)
        .bind(threshold_i64)
        .bind(CACHE_KEEPALIVE_IDEMPOTENCY_PREFIX)
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
                 last_result = ?4
             WHERE status = 'Running'
               AND lock_at IS NOT NULL
               AND COALESCE(idempotency_key, '') NOT LIKE ?3
               AND (?1 - lock_at) > ?2",
        )
        .bind(now_i64)
        .bind(threshold_i64)
        .bind(CACHE_KEEPALIVE_IDEMPOTENCY_PREFIX)
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
        let worker_cutoff = now_unix_secs.saturating_sub(APALIS_WORKER_RETENTION_SECS);
        let worker_cutoff_timestamp = utc_timestamp(worker_cutoff, "worker_cutoff")?;
        let job_cutoff = retention_cutoff(now_unix_secs, self.config.dlq_retention_days);
        let job_cutoff_timestamp = utc_timestamp(job_cutoff, "job_cutoff")?;
        let reaped = self.reap_stale_locks(now_unix_secs).await?;
        let cache_keepalive_sessions_removed = self
            .prune_cache_keepalive_sessions(now_unix_secs, worker_cutoff)
            .await?;
        let workers_removed = self.prune_stale_workers(worker_cutoff_timestamp).await?;
        let jobs_removed = sqlx::query(
            "DELETE FROM apalis.jobs WHERE status IN ('Done','Failed','Killed') AND done_at IS NOT NULL AND done_at < $1",
        )
        .bind(job_cutoff_timestamp)
        .execute(&self.pool)
        .await?
        .rows_affected();
        let cache_keepalive_jobs_removed = self.prune_cache_keepalive_jobs(worker_cutoff).await?;
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
            cache_keepalive_sessions_removed,
            cache_keepalive_jobs_removed,
            cutoff_unix_secs: job_cutoff,
        })
    }

    async fn prune_stale_workers(&self, worker_cutoff: DateTime<Utc>) -> Result<u64> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "UPDATE apalis.jobs
             SET lock_by = NULL
             WHERE lock_by IN (SELECT id FROM apalis.workers WHERE last_seen < $1)",
        )
        .bind(worker_cutoff)
        .execute(&mut *tx)
        .await?;
        let removed = sqlx::query("DELETE FROM apalis.workers WHERE last_seen < $1")
            .bind(worker_cutoff)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        tx.commit().await?;
        Ok(removed)
    }

    async fn prune_cache_keepalive_sessions(
        &self,
        now_unix_secs: u64,
        stale_pending_cutoff: u64,
    ) -> Result<u64> {
        let stale_running_cutoff =
            now_unix_secs.saturating_sub(self.config.stale_lock_threshold_secs);
        let recovered = sqlx::query(
            "UPDATE cache_keepalive_sessions
             SET status = 'terminal', terminal_reason = 'stale', running_since_unix_secs = NULL, updated_at = $1
             WHERE status = 'active'
               AND enqueue_state = 'running'
               AND running_since_unix_secs IS NOT NULL
               AND running_since_unix_secs < $2",
        )
        .bind(unix_i64(now_unix_secs, "now_unix_secs")?)
        .bind(unix_i64(stale_running_cutoff, "stale_running_cutoff")?)
        .execute(&self.pool)
        .await;
        let recovered = match recovered {
            Ok(result) => result.rows_affected(),
            Err(error) if is_missing_cache_keepalive_table(&error) => return Ok(0),
            Err(error) => return Err(error.into()),
        };
        sqlx::query(
            "UPDATE apalis.jobs
             SET status = 'Killed', done_at = $1, lock_by = NULL, lock_at = NULL, last_result = $3::jsonb
             WHERE status = 'Running'
               AND lock_at IS NOT NULL
               AND idempotency_key LIKE $4
               AND EXTRACT(EPOCH FROM ($1::timestamptz - lock_at))::BIGINT > $2",
        )
        .bind(utc_timestamp(now_unix_secs, "now_unix_secs")?)
        .bind(unix_i64(
            self.config.stale_lock_threshold_secs,
            "stale_lock_threshold_secs",
        )?)
        .bind(STALE_LOCK_LAST_RESULT)
        .bind(CACHE_KEEPALIVE_IDEMPOTENCY_PREFIX)
        .execute(&self.pool)
        .await?;
        let removed = sqlx::query(
            "DELETE FROM cache_keepalive_sessions
             WHERE expires_at < $1
                OR (status = 'active' AND enqueue_state = 'pending' AND updated_at < $2)",
        )
        .bind(unix_i64(now_unix_secs, "now_unix_secs")?)
        .bind(unix_i64(stale_pending_cutoff, "stale_pending_cutoff")?)
        .execute(&self.pool)
        .await;
        match removed {
            Ok(result) => Ok(recovered.saturating_add(result.rows_affected())),
            Err(error) if is_missing_cache_keepalive_table(&error) => Ok(0),
            Err(error) => Err(error.into()),
        }
    }

    async fn prune_cache_keepalive_jobs(&self, cutoff_unix_secs: u64) -> Result<u64> {
        let cutoff = utc_timestamp(cutoff_unix_secs, "cache_keepalive_job_cutoff")?;
        let result = sqlx::query(
            "DELETE FROM apalis.jobs
             WHERE job_type = $1
               AND idempotency_key LIKE $2
               AND (
                   (status IN ('Done','Failed','Killed') AND COALESCE(done_at, run_at) < $3)
                   OR (status = 'Pending' AND run_at < $3)
               )",
        )
        .bind(CACHE_KEEPALIVE_QUEUE)
        .bind(CACHE_KEEPALIVE_IDEMPOTENCY_PREFIX)
        .bind(cutoff)
        .execute(&self.pool)
        .await;
        match result {
            Ok(result) => Ok(result.rows_affected()),
            Err(error) => Err(error.into()),
        }
    }

    async fn reap_stale_locks(&self, now_unix_secs: u64) -> Result<Vec<StaleLockRow>> {
        let threshold_i64 = unix_i64(
            self.config.stale_lock_threshold_secs,
            "stale_lock_threshold_secs",
        )?;
        let now_ts = utc_timestamp(now_unix_secs, "now_unix_secs")?;
        let mut tx = self.pool.begin().await?;
        let rows =
            sqlx::query_as::<_, (String, String, Option<String>, Option<DateTime<Utc>>, i32)>(
                "SELECT id, job_type, lock_by, lock_at, attempts
             FROM apalis.jobs
             WHERE status = 'Running'
               AND lock_at IS NOT NULL
               AND COALESCE(idempotency_key, '') NOT LIKE $3
               AND EXTRACT(EPOCH FROM ($1::timestamptz - lock_at))::BIGINT > $2",
            )
            .bind(now_ts)
            .bind(threshold_i64)
            .bind(CACHE_KEEPALIVE_IDEMPOTENCY_PREFIX)
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
                 last_result = $4::jsonb
             WHERE status = 'Running'
               AND lock_at IS NOT NULL
               AND COALESCE(idempotency_key, '') NOT LIKE $3
               AND EXTRACT(EPOCH FROM ($1::timestamptz - lock_at))::BIGINT > $2",
        )
        .bind(now_ts)
        .bind(threshold_i64)
        .bind(CACHE_KEEPALIVE_IDEMPOTENCY_PREFIX)
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
                attempts: i64::from(attempts),
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
            cache_keepalive_sessions_removed: stats.cache_keepalive_sessions_removed,
            cache_keepalive_jobs_removed: stats.cache_keepalive_jobs_removed,
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

fn is_missing_cache_keepalive_table(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::Database(database) => database
            .code()
            .as_deref()
            .is_some_and(|code| code == "42P01" || code == "1" || code == "101"),
        _ => error.to_string().contains("cache_keepalive_sessions"),
    }
}

#[cfg(feature = "postgres")]
fn utc_timestamp(value: u64, field: &str) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp(unix_i64(value, field)?, 0)
        .ok_or_else(|| SchedulerError::Job(format!("{field} is outside timestamp range")))
}

#[cfg(test)]
mod tests;
