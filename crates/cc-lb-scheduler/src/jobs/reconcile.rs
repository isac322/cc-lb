use std::time::Duration;

#[cfg(feature = "postgres")]
use sqlx::Postgres;
#[cfg(feature = "sqlite")]
use sqlx::Sqlite;
use sqlx::{Database, Pool};

#[cfg(feature = "postgres")]
mod postgres;
mod specs;
#[cfg(feature = "sqlite")]
mod sqlite;

pub use specs::ReconcileUpstreams;

use serde::{Deserialize, Serialize};

use crate::error::{Result, SchedulerError};
use crate::middleware::TraceparentCarrier;

const RETRY_DELAY_SECS: u64 = 60;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SchedulerReconcileJob {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
}

impl TraceparentCarrier for SchedulerReconcileJob {
    fn traceparent(&self) -> Option<&str> {
        self.traceparent.as_deref()
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        self.traceparent = traceparent;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchedulerReconcileConfig {
    pub prune_orphans: bool,
    pub reconcile_interval_secs: u64,
}

impl SchedulerReconcileConfig {
    pub const fn new(prune_orphans: bool, reconcile_interval_secs: u64) -> Self {
        Self {
            prune_orphans,
            reconcile_interval_secs,
        }
    }
}

impl Default for SchedulerReconcileConfig {
    fn default() -> Self {
        Self::new(true, 300)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchedulerReconcileStats {
    pub jobs_ensured: u64,
    pub jobs_pruned: u64,
    pub failures_recorded: u64,
    pub reconcile_interval_secs: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SchedulerReconcileJobResult {
    Done(SchedulerReconcileStats),
    Retry { delay: Duration, error: String },
}

#[derive(Clone, Debug)]
pub struct SchedulerReconcileJobHandler<Db: Database, Upstreams> {
    pool: Pool<Db>,
    upstreams: Upstreams,
    config: SchedulerReconcileConfig,
}

impl<Db: Database, Upstreams> SchedulerReconcileJobHandler<Db, Upstreams> {
    pub const fn new(pool: Pool<Db>, upstreams: Upstreams) -> Self {
        Self {
            pool,
            upstreams,
            config: SchedulerReconcileConfig::new(true, 300),
        }
    }

    pub const fn with_config(
        pool: Pool<Db>,
        upstreams: Upstreams,
        config: SchedulerReconcileConfig,
    ) -> Self {
        Self {
            pool,
            upstreams,
            config,
        }
    }

    const fn stats(
        &self,
        jobs_ensured: u64,
        jobs_pruned: u64,
        failures_recorded: u64,
    ) -> SchedulerReconcileStats {
        SchedulerReconcileStats {
            jobs_ensured,
            jobs_pruned,
            failures_recorded,
            reconcile_interval_secs: self.config.reconcile_interval_secs,
        }
    }
}

#[cfg(feature = "sqlite")]
impl<Upstreams> SchedulerReconcileJobHandler<Sqlite, Upstreams>
where
    Upstreams: ReconcileUpstreams,
{
    pub async fn handle(
        &self,
        job: SchedulerReconcileJob,
        now_unix_secs: u64,
    ) -> SchedulerReconcileJobResult {
        finish(self.reconcile(job, now_unix_secs).await)
    }

    async fn reconcile(
        &self,
        job: SchedulerReconcileJob,
        now_unix_secs: u64,
    ) -> Result<SchedulerReconcileStats> {
        let (jobs, live_upstream_keys) =
            specs::collect_specs(&self.upstreams, &job, now_unix_secs).await?;
        let mut jobs_ensured = 0;
        for job in &jobs {
            jobs_ensured += sqlite::ensure_job(&self.pool, job, now_unix_secs).await?;
        }
        let jobs_pruned = if self.config.prune_orphans {
            sqlite::prune_orphans(&self.pool, &live_upstream_keys).await?
        } else {
            0
        };
        let failures_recorded = sqlite::surface_failures(&self.pool, now_unix_secs).await?;
        Ok(self.stats(jobs_ensured, jobs_pruned, failures_recorded))
    }
}

#[cfg(feature = "postgres")]
impl<Upstreams> SchedulerReconcileJobHandler<Postgres, Upstreams>
where
    Upstreams: ReconcileUpstreams,
{
    pub async fn handle(
        &self,
        job: SchedulerReconcileJob,
        now_unix_secs: u64,
    ) -> SchedulerReconcileJobResult {
        finish(self.reconcile(job, now_unix_secs).await)
    }

    async fn reconcile(
        &self,
        job: SchedulerReconcileJob,
        now_unix_secs: u64,
    ) -> Result<SchedulerReconcileStats> {
        let (jobs, live_upstream_keys) =
            specs::collect_specs(&self.upstreams, &job, now_unix_secs).await?;
        let mut jobs_ensured = 0;
        for job in &jobs {
            jobs_ensured += postgres::ensure_job(&self.pool, job, now_unix_secs).await?;
        }
        let jobs_pruned = if self.config.prune_orphans {
            postgres::prune_orphans(&self.pool, &live_upstream_keys).await?
        } else {
            0
        };
        let failures_recorded = postgres::surface_failures(&self.pool, now_unix_secs).await?;
        Ok(self.stats(jobs_ensured, jobs_pruned, failures_recorded))
    }
}

fn finish(result: Result<SchedulerReconcileStats>) -> SchedulerReconcileJobResult {
    match result {
        Ok(stats) => SchedulerReconcileJobResult::Done(stats),
        Err(error) => SchedulerReconcileJobResult::Retry {
            delay: Duration::from_secs(RETRY_DELAY_SECS),
            error: error.to_string(),
        },
    }
}

fn row_id(idempotency_key: &str, now_unix_secs: u64) -> String {
    format!("{idempotency_key}:{now_unix_secs}")
}

fn safe_summary(job_type: &str, idempotency_key: Option<String>) -> String {
    idempotency_key
        .filter(|value| value.starts_with("entity:"))
        .unwrap_or_else(|| job_type.to_owned())
}

fn u64_to_i64(value: u64, field: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| SchedulerError::Job(format!("{field} exceeds i64::MAX")))
}

fn i64_to_u64(value: i64, field: &str) -> Result<u64> {
    u64::try_from(value).map_err(|_| SchedulerError::Job(format!("{field} is negative")))
}

fn i64_to_u32(value: i64, field: &str) -> Result<u32> {
    u32::try_from(value).map_err(|_| SchedulerError::Job(format!("{field} is outside u32")))
}

#[cfg(feature = "postgres")]
fn utc_timestamp(value: u64, field: &str) -> Result<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::from_timestamp(u64_to_i64(value, field)?, 0)
        .ok_or_else(|| SchedulerError::Job(format!("{field} is outside timestamp range")))
}

#[cfg(test)]
mod tests;
