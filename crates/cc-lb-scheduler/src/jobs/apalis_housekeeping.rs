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
    idempotency::SchedulerFailuresStore,
    middleware::TraceparentCarrier,
};

pub const APALIS_WORKER_RETENTION_SECS: u64 = 3_600;
pub const APALIS_HOUSEKEEPING_RETRY_DELAY: Duration = Duration::from_secs(60);
const SECONDS_PER_DAY: u64 = 86_400;

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
}

impl ApalisHousekeepingConfig {
    pub const fn new(dlq_retention_days: u32) -> Self {
        Self { dlq_retention_days }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApalisHousekeepingJobStats {
    pub workers_removed: u64,
    pub jobs_removed: u64,
    pub scheduler_failures_removed: u64,
    pub cutoff_unix_secs: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApalisHousekeepingJobResult {
    Done {
        workers_removed: u64,
        jobs_removed: u64,
        scheduler_failures_removed: u64,
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
        let scheduler_failures_removed = SchedulerFailuresStore::new(self.pool.clone())
            .prune_older_than(job_cutoff)
            .await?;
        Ok(ApalisHousekeepingJobStats {
            workers_removed,
            jobs_removed,
            scheduler_failures_removed,
            cutoff_unix_secs: job_cutoff,
        })
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
        let scheduler_failures_removed = SchedulerFailuresStore::new(self.pool.clone())
            .prune_older_than(job_cutoff)
            .await?;
        Ok(ApalisHousekeepingJobStats {
            workers_removed,
            jobs_removed,
            scheduler_failures_removed,
            cutoff_unix_secs: job_cutoff,
        })
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
            scheduler_failures_removed: stats.scheduler_failures_removed,
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
