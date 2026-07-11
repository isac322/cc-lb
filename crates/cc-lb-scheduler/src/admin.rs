use std::collections::BTreeMap;

use cc_lb_config::SchedulerConfig;
use serde::Serialize;
use sqlx::Row as _;
use uuid::Uuid;

use crate::error::{Result, SchedulerError};
use crate::worker::{
    ADAPTIVE_QUEUE, AdaptiveJob, CRON_QUEUE, CronJob, SchedulerBackend, SchedulerPushTask,
};

const FAILURE_SUMMARY_MAX_CHARS: usize = 256;
const REDACTED_FAILURE_SUMMARY: &str = "<redacted>";

#[derive(Clone, Debug)]
pub struct SchedulerAdminHandle {
    backend: SchedulerBackend,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SchedulerStatusSnapshot {
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SchedulerFailure {
    pub id: String,
    pub job_type: String,
    pub payload_summary: String,
    pub last_error: String,
    pub attempts: u32,
    pub first_failed_at_unix_secs: u64,
    pub last_failed_at_unix_secs: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct RecurringRuntime {
    next_run_at: Option<u64>,
    last_run_status: Option<String>,
}

impl SchedulerAdminHandle {
    pub const fn new(backend: SchedulerBackend) -> Self {
        Self { backend }
    }

    pub async fn push_adaptive_task(&self, task: SchedulerPushTask<AdaptiveJob>) -> Result<()> {
        self.backend.push_adaptive_task(task).await
    }

    pub async fn push_cron_task(&self, task: SchedulerPushTask<CronJob>) -> Result<()> {
        self.backend.push_cron_task(task).await
    }

    pub async fn next_run_for_upstream(
        &self,
        upstream_id: Uuid,
        job_kind: &str,
    ) -> Result<Option<i64>> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            SchedulerBackend::Sqlite(sqlite) => {
                sqlite_next_run_for_upstream(sqlite.pool(), upstream_id, job_kind).await
            }
            #[cfg(feature = "postgres")]
            SchedulerBackend::Postgres(postgres) => {
                postgres_next_run_for_upstream(postgres.pool(), upstream_id, job_kind).await
            }
        }
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
                sqlite_failures(sqlite.pool(), job_type_filter, limit, offset).await
            }
            #[cfg(feature = "postgres")]
            SchedulerBackend::Postgres(postgres) => {
                postgres_failures(postgres.pool(), job_type_filter, limit, offset).await
            }
        }
    }

    async fn recurring_runtime(&self) -> Result<BTreeMap<String, RecurringRuntime>> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            SchedulerBackend::Sqlite(sqlite) => sqlite_recurring_runtime(sqlite.pool()).await,
            #[cfg(feature = "postgres")]
            SchedulerBackend::Postgres(postgres) => {
                postgres_recurring_runtime(postgres.pool()).await
            }
        }
    }

    fn pool_stats(&self) -> (u32, u32) {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            SchedulerBackend::Sqlite(sqlite) => pool_stats(sqlite.pool()),
            #[cfg(feature = "postgres")]
            SchedulerBackend::Postgres(postgres) => pool_stats(postgres.pool()),
        }
    }
}

#[cfg(feature = "sqlite")]
async fn sqlite_next_run_for_upstream(
    pool: &sqlx::Pool<sqlx::Sqlite>,
    upstream_id: Uuid,
    job_kind: &str,
) -> Result<Option<i64>> {
    let prefix = format!("{ADAPTIVE_QUEUE}:{job_kind}:{upstream_id}:%");
    let row = sqlx::query(
        "SELECT MIN(run_at) AS next_run_at \
         FROM Jobs \
         WHERE job_type = ?1 \
           AND idempotency_key LIKE ?2 \
           AND (status IN ('Pending', 'Queued', 'Running') \
                OR (status = 'Failed' AND attempts < max_attempts))",
    )
    .bind(ADAPTIVE_QUEUE)
    .bind(prefix)
    .fetch_one(pool)
    .await?;
    Ok(row.try_get("next_run_at")?)
}

#[cfg(feature = "postgres")]
async fn postgres_next_run_for_upstream(
    pool: &sqlx::Pool<sqlx::Postgres>,
    upstream_id: Uuid,
    job_kind: &str,
) -> Result<Option<i64>> {
    let prefix = format!("{ADAPTIVE_QUEUE}:{job_kind}:{upstream_id}:%");
    let row = sqlx::query(
        "SELECT EXTRACT(EPOCH FROM MIN(run_at))::BIGINT AS next_run_at \
         FROM apalis.jobs \
         WHERE job_type = $1 \
           AND idempotency_key LIKE $2 \
           AND (status IN ('Pending', 'Queued', 'Running') \
                OR (status = 'Failed' AND attempts < max_attempts))",
    )
    .bind(ADAPTIVE_QUEUE)
    .bind(prefix)
    .fetch_one(pool)
    .await?;
    Ok(row.try_get("next_run_at")?)
}

#[cfg(feature = "sqlite")]
async fn sqlite_failures(
    pool: &sqlx::Pool<sqlx::Sqlite>,
    job_type_filter: Option<&str>,
    limit: u32,
    offset: u32,
) -> Result<Vec<SchedulerFailure>> {
    let limit = i64::from(limit);
    let offset = i64::from(offset);
    let rows = if let Some(job_type) = job_type_filter {
        sqlx::query(
            "SELECT id, job_type, idempotency_key, last_result, attempts, COALESCE(done_at, run_at, 0) AS failed_at \
             FROM Jobs WHERE status IN ('Failed','Killed') AND job_type = ?1 \
             ORDER BY failed_at DESC, id DESC LIMIT ?2 OFFSET ?3",
        )
        .bind(job_type)
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query(
            "SELECT id, job_type, idempotency_key, last_result, attempts, COALESCE(done_at, run_at, 0) AS failed_at \
             FROM Jobs WHERE status IN ('Failed','Killed') \
             ORDER BY failed_at DESC, id DESC LIMIT ?1 OFFSET ?2",
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await?
    };
    rows.into_iter().map(sqlite_failure_from_row).collect()
}

#[cfg(feature = "postgres")]
async fn postgres_failures(
    pool: &sqlx::Pool<sqlx::Postgres>,
    job_type_filter: Option<&str>,
    limit: u32,
    offset: u32,
) -> Result<Vec<SchedulerFailure>> {
    let limit = i64::from(limit);
    let offset = i64::from(offset);
    let rows = if let Some(job_type) = job_type_filter {
        sqlx::query(
            "SELECT id, job_type, idempotency_key, last_result::TEXT AS last_result, attempts, \
                    EXTRACT(EPOCH FROM COALESCE(done_at, run_at))::BIGINT AS failed_at \
             FROM apalis.jobs WHERE status IN ('Failed','Killed') AND job_type = $1 \
             ORDER BY failed_at DESC, id DESC LIMIT $2 OFFSET $3",
        )
        .bind(job_type)
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query(
            "SELECT id, job_type, idempotency_key, last_result::TEXT AS last_result, attempts, \
                    EXTRACT(EPOCH FROM COALESCE(done_at, run_at))::BIGINT AS failed_at \
             FROM apalis.jobs WHERE status IN ('Failed','Killed') \
             ORDER BY failed_at DESC, id DESC LIMIT $1 OFFSET $2",
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await?
    };
    rows.into_iter().map(postgres_failure_from_row).collect()
}

#[cfg(feature = "sqlite")]
fn sqlite_failure_from_row(row: sqlx::sqlite::SqliteRow) -> Result<SchedulerFailure> {
    failure_from_parts(
        row.try_get("id")?,
        row.try_get("job_type")?,
        row.try_get("idempotency_key")?,
        row.try_get("last_result")?,
        row.try_get::<i64, _>("attempts")?,
        row.try_get("failed_at")?,
    )
}

#[cfg(feature = "postgres")]
fn postgres_failure_from_row(row: sqlx::postgres::PgRow) -> Result<SchedulerFailure> {
    failure_from_parts(
        row.try_get("id")?,
        row.try_get("job_type")?,
        row.try_get("idempotency_key")?,
        row.try_get("last_result")?,
        row.try_get::<i32, _>("attempts")?.into(),
        row.try_get("failed_at")?,
    )
}

fn failure_from_parts(
    id: String,
    job_type: String,
    idempotency_key: Option<String>,
    last_result: Option<String>,
    attempts: i64,
    failed_at: i64,
) -> Result<SchedulerFailure> {
    let failed_at = i64_to_u64(failed_at, "failed_at")?;
    Ok(SchedulerFailure {
        id,
        job_type,
        payload_summary: safe_failure_summary(idempotency_key.as_deref()),
        last_error: safe_failure_summary(last_result.as_deref()),
        attempts: u32::try_from(attempts)
            .map_err(|_| SchedulerError::Job("attempts is outside u32".to_owned()))?,
        first_failed_at_unix_secs: failed_at,
        last_failed_at_unix_secs: failed_at,
    })
}

fn safe_failure_summary(value: Option<&str>) -> String {
    let Some(value) = value else {
        return String::new();
    };
    if contains_sensitive_failure_material(value) {
        return REDACTED_FAILURE_SUMMARY.to_owned();
    }
    truncate_chars(value, FAILURE_SUMMARY_MAX_CHARS)
}

fn contains_sensitive_failure_material(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    [
        "authorization",
        "x-api-key",
        "api_key",
        "bearer ",
        "sk-ant-",
        "prompt",
        "messages",
        "encrypted_payload",
        "ciphertext",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    let mut output = String::new();
    for character in value.chars().take(max_chars) {
        output.push(character);
    }
    output
}

#[cfg(feature = "sqlite")]
async fn sqlite_recurring_runtime(
    pool: &sqlx::Pool<sqlx::Sqlite>,
) -> Result<BTreeMap<String, RecurringRuntime>> {
    use sqlx::Row as _;

    let rows = sqlx::query(
        "SELECT job, status, run_at FROM Jobs WHERE job_type = ?1 ORDER BY run_at ASC, id ASC",
    )
    .bind(CRON_QUEUE)
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
    .bind(CRON_QUEUE)
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
    let job: CronJob = serde_json::from_slice(payload)
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

fn i64_to_u64(value: i64, field: &str) -> Result<u64> {
    u64::try_from(value).map_err(|_| SchedulerError::Job(format!("{field} is negative")))
}

#[cfg(test)]
mod tests {
    use super::{FAILURE_SUMMARY_MAX_CHARS, REDACTED_FAILURE_SUMMARY, failure_from_parts};

    #[test]
    fn cache_keepalive_failure_summary_redacts_prompt_and_auth_material() {
        let failure = failure_from_parts(
            "failed-keepalive".to_owned(),
            "adaptive".to_owned(),
            Some("cache_keepalive:session-hash:7".to_owned()),
            Some(
                r#"{"Err":"prompt contains secret; x-api-key=sk-ant-downstream; Authorization: Bearer token"}"#
                    .to_owned(),
            ),
            1,
            1_800_000_000,
        )
        .expect("failure row converts");

        assert_eq!(failure.payload_summary, "cache_keepalive:session-hash:7");
        assert_eq!(failure.last_error, REDACTED_FAILURE_SUMMARY);
    }

    #[test]
    fn scheduler_failure_summaries_are_bounded() {
        let long = "x".repeat(FAILURE_SUMMARY_MAX_CHARS + 64);
        let failure = failure_from_parts(
            "failed-job".to_owned(),
            "adaptive".to_owned(),
            Some(long.clone()),
            Some(long),
            1,
            1_800_000_000,
        )
        .expect("failure row converts");

        assert_eq!(failure.payload_summary.len(), FAILURE_SUMMARY_MAX_CHARS);
        assert_eq!(failure.last_error.len(), FAILURE_SUMMARY_MAX_CHARS);
    }
}
