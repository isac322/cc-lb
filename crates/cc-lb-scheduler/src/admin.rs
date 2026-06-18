use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use apalis_core::backend::codec::Codec as _;
use cc_lb_config::SchedulerConfig;
use serde::Serialize;

use crate::error::{Result, SchedulerError};
use crate::idempotency::{SchedulerFailure, SchedulerFailuresStore};
use crate::jobs::reconcile::SchedulerReconcileJob;
use crate::leader_election::{LeaderElection, LeaderState};
use crate::worker::{SINGLETON_QUEUE, SchedulerBackend, SingletonJob};

pub const SCHEDULER_RECONCILE_QUEUE: &str = "scheduler_reconcile";
const RECONCILE_MAX_ATTEMPTS: i32 = 1;

#[derive(Clone, Debug)]
pub struct SchedulerAdminHandle {
    backend: SchedulerBackend,
    leader: Arc<LeaderElection>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SchedulerStatusSnapshot {
    pub leader_status: &'static str,
    pub recurring_jobs: Vec<SchedulerRecurringJobStatus>,
    pub pool_in_use: u32,
    pub pool_idle: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SchedulerRecurringJobStatus {
    pub name: String,
    pub next_run_at: Option<u64>,
    pub last_run_status: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct RecurringRuntime {
    next_run_at: Option<u64>,
    last_run_status: Option<String>,
}

impl SchedulerAdminHandle {
    pub const fn new(backend: SchedulerBackend, leader: Arc<LeaderElection>) -> Self {
        Self { backend, leader }
    }

    pub async fn status(&self, config: &SchedulerConfig) -> Result<SchedulerStatusSnapshot> {
        let mut runtime = self.recurring_runtime().await?;
        let (pool_in_use, pool_idle) = self.pool_stats();
        let mut names = config
            .recurring_jobs
            .iter()
            .filter_map(|(name, job)| job.enabled.then_some(name.clone()))
            .collect::<Vec<_>>();
        names.sort();
        let recurring_jobs = names
            .into_iter()
            .map(|name| {
                let runtime = runtime.remove(&name).unwrap_or_default();
                SchedulerRecurringJobStatus {
                    name,
                    next_run_at: runtime.next_run_at,
                    last_run_status: runtime.last_run_status,
                }
            })
            .collect();

        Ok(SchedulerStatusSnapshot {
            leader_status: self.leader.current_state().as_str(),
            recurring_jobs,
            pool_in_use,
            pool_idle,
        })
    }

    pub async fn list_failures(
        &self,
        job_type_filter: Option<&str>,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<SchedulerFailure>> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            SchedulerBackend::Sqlite(sqlite) => {
                SchedulerFailuresStore::new(sqlite.pool.clone())
                    .list_paged(job_type_filter, limit, offset)
                    .await
            }
            #[cfg(feature = "postgres")]
            SchedulerBackend::Postgres(postgres) => {
                SchedulerFailuresStore::new(postgres.pool.clone())
                    .list_paged(job_type_filter, limit, offset)
                    .await
            }
        }
    }

    pub async fn enqueue_reconcile(&self, traceparent: Option<String>) -> Result<String> {
        let job = SchedulerReconcileJob { traceparent };
        let payload = apalis_codec::json::JsonCodec::<Vec<u8>>::encode(&job)
            .map_err(|error| SchedulerError::Job(format!("encode reconcile job: {error}")))?;
        let id = ulid::Ulid::new().to_string();
        let now = now_unix_secs()?;
        match &self.backend {
            #[cfg(feature = "sqlite")]
            SchedulerBackend::Sqlite(sqlite) => {
                sqlx::query(
                    "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, last_result, lock_at, lock_by, done_at, priority, metadata, idempotency_key) \
                     VALUES (?1, ?2, ?3, 'Pending', 0, ?4, ?5, NULL, NULL, NULL, NULL, 0, ?6, ?7)",
                )
                .bind(payload)
                .bind(&id)
                .bind(SCHEDULER_RECONCILE_QUEUE)
                .bind(RECONCILE_MAX_ATTEMPTS)
                .bind(u64_to_i64(now, "now_unix_secs")?)
                .bind("{}")
                .bind(format!("{SCHEDULER_RECONCILE_QUEUE}:{id}"))
                .execute(&sqlite.pool)
                .await?;
            }
            #[cfg(feature = "postgres")]
            SchedulerBackend::Postgres(postgres) => {
                let run_at = unix_timestamp(now, "now_unix_secs")?;
                sqlx::query(
                    "INSERT INTO apalis.jobs (job, id, job_type, status, attempts, max_attempts, run_at, priority, metadata, idempotency_key) \
                     VALUES ($1, $2, $3, 'Pending', 0, $4, $5, 0, $6, $7)",
                )
                .bind(payload)
                .bind(&id)
                .bind(SCHEDULER_RECONCILE_QUEUE)
                .bind(RECONCILE_MAX_ATTEMPTS)
                .bind(run_at)
                .bind(serde_json::json!({}))
                .bind(format!("{SCHEDULER_RECONCILE_QUEUE}:{id}"))
                .execute(&postgres.pool)
                .await?;
            }
        }
        Ok(id)
    }

    async fn recurring_runtime(&self) -> Result<BTreeMap<String, RecurringRuntime>> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            SchedulerBackend::Sqlite(sqlite) => sqlite_recurring_runtime(&sqlite.pool).await,
            #[cfg(feature = "postgres")]
            SchedulerBackend::Postgres(postgres) => {
                postgres_recurring_runtime(&postgres.pool).await
            }
        }
    }

    fn pool_stats(&self) -> (u32, u32) {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            SchedulerBackend::Sqlite(sqlite) => pool_stats(&sqlite.pool),
            #[cfg(feature = "postgres")]
            SchedulerBackend::Postgres(postgres) => pool_stats(&postgres.pool),
        }
    }
}

#[cfg(feature = "sqlite")]
async fn sqlite_recurring_runtime(
    pool: &sqlx::Pool<sqlx::Sqlite>,
) -> Result<BTreeMap<String, RecurringRuntime>> {
    use sqlx::Row as _;

    let rows = sqlx::query(
        "SELECT job, status, run_at FROM Jobs WHERE job_type = ?1 ORDER BY run_at ASC, id ASC",
    )
    .bind(SINGLETON_QUEUE)
    .fetch_all(pool)
    .await?;
    let mut runtime = BTreeMap::new();
    for row in rows {
        let payload: Vec<u8> = row.try_get("job")?;
        let status: String = row.try_get("status")?;
        let run_at = i64_to_u64(row.try_get("run_at")?, "run_at")?;
        record_recurring_row(&mut runtime, &payload, status, run_at)?;
    }
    Ok(runtime)
}

#[cfg(feature = "postgres")]
async fn postgres_recurring_runtime(
    pool: &sqlx::Pool<sqlx::Postgres>,
) -> Result<BTreeMap<String, RecurringRuntime>> {
    use sqlx::Row as _;

    let rows = sqlx::query(
        "SELECT job, status, EXTRACT(EPOCH FROM run_at)::BIGINT AS run_at \
         FROM apalis.jobs WHERE job_type = $1 ORDER BY run_at ASC, id ASC",
    )
    .bind(SINGLETON_QUEUE)
    .fetch_all(pool)
    .await?;
    let mut runtime = BTreeMap::new();
    for row in rows {
        let payload: Vec<u8> = row.try_get("job")?;
        let status: String = row.try_get("status")?;
        let run_at = i64_to_u64(row.try_get("run_at")?, "run_at")?;
        record_recurring_row(&mut runtime, &payload, status, run_at)?;
    }
    Ok(runtime)
}

fn record_recurring_row(
    runtime: &mut BTreeMap<String, RecurringRuntime>,
    payload: &[u8],
    status: String,
    run_at: u64,
) -> Result<()> {
    let job: SingletonJob = serde_json::from_slice(payload)
        .map_err(|error| SchedulerError::Job(format!("decode singleton job: {error}")))?;
    let entry = runtime.entry(job.kind().to_owned()).or_default();
    if is_active_status(&status) {
        entry.next_run_at = entry
            .next_run_at
            .map_or(Some(run_at), |current| Some(current.min(run_at)));
    }
    entry.last_run_status = Some(status);
    Ok(())
}

fn is_active_status(status: &str) -> bool {
    matches!(status, "Pending" | "Queued" | "Running")
}

fn pool_stats<Db: sqlx::Database>(pool: &sqlx::Pool<Db>) -> (u32, u32) {
    let size = pool.size();
    let idle = usize_to_u32(pool.num_idle());
    (size.saturating_sub(idle), idle)
}

fn usize_to_u32(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn now_unix_secs() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| SchedulerError::Job(format!("system clock before unix epoch: {error}")))
        .map(|duration| duration.as_secs())
}

fn u64_to_i64(value: u64, field: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| SchedulerError::Job(format!("{field} exceeds i64::MAX")))
}

fn i64_to_u64(value: i64, field: &str) -> Result<u64> {
    u64::try_from(value).map_err(|_| SchedulerError::Job(format!("{field} is negative")))
}

#[cfg(feature = "postgres")]
fn unix_timestamp(value: u64, field: &str) -> Result<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::from_timestamp(u64_to_i64(value, field)?, 0)
        .ok_or_else(|| SchedulerError::Job(format!("{field} is outside timestamp range")))
}

impl From<LeaderState> for &'static str {
    fn from(value: LeaderState) -> Self {
        value.as_str()
    }
}
