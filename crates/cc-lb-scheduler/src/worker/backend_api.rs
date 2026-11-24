pub use apalis_core::backend::Filter;
use apalis_core::backend::ListTasks;
use apalis_core::backend::codec::Codec as _;
use apalis_core::task::builder::TaskBuilder;
pub use apalis_core::task::status::Status as TaskStatus;

use serde::de::DeserializeOwned;

use crate::error::SchedulerError;

use super::{
    ADAPTIVE_QUEUE, AdaptiveJob, CACHE_KEEPALIVE_QUEUE, CRON_QUEUE, CronJob, SchedulerBackend,
};

const DEFAULT_MAX_ATTEMPTS: i32 = 5;

#[cfg(feature = "sqlite")]
fn from_sqlite_apalis_error(error: apalis_sqlite::Error) -> SchedulerError {
    match error {
        apalis_sqlite::Error::Database(db_err) => SchedulerError::Database(db_err),
        other => SchedulerError::Job(other.to_string()),
    }
}

#[cfg(feature = "postgres")]
fn from_postgres_apalis_error(error: apalis_postgres::Error) -> SchedulerError {
    match error {
        apalis_postgres::Error::Database(db_err) => SchedulerError::Database(db_err),
        other => SchedulerError::Job(other.to_string()),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchedulerTaskRow<Args> {
    pub args: Args,
    pub idempotency_key: Option<String>,
    pub run_at_unix_secs: u64,
    pub status: TaskStatus,
    pub attempts: i32,
    pub max_attempts: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchedulerPushTask<Args> {
    pub args: Args,
    pub idempotency_key: Option<String>,
    pub run_at_unix_secs: Option<u64>,
    pub max_attempts: Option<i32>,
}

impl SchedulerBackend {
    pub async fn push_adaptive_task(
        &self,
        task: SchedulerPushTask<AdaptiveJob>,
    ) -> Result<(), SchedulerError> {
        self.push_task_with_mode(ADAPTIVE_QUEUE, task, false).await
    }

    /// Pushes an adaptive task or expedites a matching unstarted `Pending`
    /// task (`attempts = 0`) to run at `min(existing.run_at, new.run_at)`.
    pub async fn push_or_expedite_adaptive_task(
        &self,
        task: SchedulerPushTask<AdaptiveJob>,
    ) -> Result<(), SchedulerError> {
        self.push_task_with_mode(ADAPTIVE_QUEUE, task, true).await
    }

    async fn push_task_with_mode<Args>(
        &self,
        queue: &str,
        task: SchedulerPushTask<Args>,
        expedite_pending: bool,
    ) -> Result<(), SchedulerError>
    where
        Args: DeserializeOwned + serde::Serialize,
    {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(sqlite) => {
                insert_sqlite_task_with_mode(sqlite.pool(), queue, task, expedite_pending).await
            }
            #[cfg(feature = "postgres")]
            Self::Postgres(postgres) => {
                insert_postgres_task_with_mode(postgres.pool(), queue, task, expedite_pending).await
            }
        }
    }

    pub async fn push_keepalive_task(
        &self,
        task: SchedulerPushTask<AdaptiveJob>,
    ) -> Result<(), SchedulerError> {
        self.push_task_with_mode(CACHE_KEEPALIVE_QUEUE, task, false)
            .await
    }

    pub async fn push_cron_task(
        &self,
        task: SchedulerPushTask<CronJob>,
    ) -> Result<(), SchedulerError> {
        self.push_task_with_mode(CRON_QUEUE, task, false).await
    }

    pub async fn list_adaptive_tasks(
        &self,
        filter: &Filter,
    ) -> Result<Vec<SchedulerTaskRow<AdaptiveJob>>, SchedulerError> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(sqlite) => {
                let storage = sqlite.adaptive_storage();
                storage
                    .list_tasks(filter)
                    .await
                    .map_err(from_sqlite_apalis_error)?
                    .into_iter()
                    .map(sqlite_task_to_row)
                    .collect()
            }
            #[cfg(feature = "postgres")]
            Self::Postgres(postgres) => {
                let storage = postgres.adaptive_operation_storage();
                storage
                    .list_tasks(filter)
                    .await
                    .map_err(from_postgres_apalis_error)?
                    .into_iter()
                    .map(postgres_task_to_row)
                    .collect()
            }
        }
    }

    pub async fn list_keepalive_tasks(
        &self,
        filter: &Filter,
    ) -> Result<Vec<SchedulerTaskRow<AdaptiveJob>>, SchedulerError> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(sqlite) => {
                let storage = sqlite.keepalive_storage();
                storage
                    .list_tasks(filter)
                    .await
                    .map_err(from_sqlite_apalis_error)?
                    .into_iter()
                    .map(sqlite_task_to_row)
                    .collect()
            }
            #[cfg(feature = "postgres")]
            Self::Postgres(postgres) => {
                let storage = postgres.keepalive_operation_storage();
                storage
                    .list_tasks(filter)
                    .await
                    .map_err(from_postgres_apalis_error)?
                    .into_iter()
                    .map(postgres_task_to_row)
                    .collect()
            }
        }
    }

    /// Atomically claim an adaptive task for exclusive in-process fallback execution.
    /// Refuses `Queued` and `Running` rows (owned by a worker) and, when `allow_done`
    /// is false, refuses `Done` rows.
    pub async fn claim_adaptive_task_for_fallback(
        &self,
        idempotency_key: &str,
        holder: &str,
        now_unix_secs: u64,
        allow_done: bool,
    ) -> Result<bool, SchedulerError> {
        let now = u64_to_i64(now_unix_secs, "now_unix_secs")?;
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(sqlite) => {
                let mut tx = sqlite.pool().begin().await.map_err(|error| {
                    SchedulerError::Job(format!("begin claim fallback tx failed: {error}"))
                })?;
                sqlx::query(
                    "INSERT INTO Workers (id, worker_type, storage_name, layers, last_seen, started_at) \
                     VALUES (?1, 'lazy-fallback', 'adaptive', '', ?2, ?2) \
                     ON CONFLICT(id) DO UPDATE SET last_seen = excluded.last_seen",
                )
                .bind(holder)
                .bind(now)
                .execute(&mut *tx)
                .await
                .map_err(|error| {
                    SchedulerError::Job(format!("register fallback worker failed: {error}"))
                })?;
                let result = sqlx::query(
                    "UPDATE Jobs \
                     SET status = 'Running', lock_by = ?1, lock_at = ?2, done_at = NULL \
                     WHERE job_type = ?3 AND idempotency_key = ?4 \
                       AND ((status = 'Pending' AND lock_by IS NULL) OR (?5 AND status = 'Done'))",
                )
                .bind(holder)
                .bind(now)
                .bind(ADAPTIVE_QUEUE)
                .bind(idempotency_key)
                .bind(allow_done)
                .execute(&mut *tx)
                .await
                .map_err(|error| {
                    SchedulerError::Job(format!("claim fallback task failed: {error}"))
                })?;
                if result.rows_affected() > 0 {
                    tx.commit().await.map_err(|error| {
                        SchedulerError::Job(format!("commit claim fallback tx failed: {error}"))
                    })?;
                    Ok(true)
                } else {
                    tx.rollback().await.map_err(|error| {
                        SchedulerError::Job(format!("rollback claim fallback tx failed: {error}"))
                    })?;
                    Ok(false)
                }
            }
            #[cfg(feature = "postgres")]
            Self::Postgres(postgres) => {
                let lock_at =
                    chrono::DateTime::<chrono::Utc>::from_timestamp(now, 0).ok_or_else(|| {
                        SchedulerError::Job("now_unix_secs outside chrono range".to_owned())
                    })?;
                let mut tx = postgres.pool().begin().await.map_err(|error| {
                    SchedulerError::Job(format!("begin claim fallback tx failed: {error}"))
                })?;
                sqlx::query(
                    "INSERT INTO apalis.workers (id, worker_type, storage_name, layers, last_seen, started_at) \
                     VALUES ($1, 'lazy-fallback', 'adaptive', '', $2, $2) \
                     ON CONFLICT (id) DO UPDATE SET last_seen = EXCLUDED.last_seen",
                )
                .bind(holder)
                .bind(lock_at)
                .execute(&mut *tx)
                .await
                .map_err(|error| {
                    SchedulerError::Job(format!("register fallback worker failed: {error}"))
                })?;
                let result = sqlx::query(
                    "UPDATE apalis.jobs \
                     SET status = 'Running', lock_by = $1, lock_at = $2, done_at = NULL \
                     WHERE job_type = $3 AND idempotency_key = $4 \
                       AND ((status = 'Pending' AND lock_by IS NULL) OR ($5 AND status = 'Done'))",
                )
                .bind(holder)
                .bind(lock_at)
                .bind(ADAPTIVE_QUEUE)
                .bind(idempotency_key)
                .bind(allow_done)
                .execute(&mut *tx)
                .await
                .map_err(|error| {
                    SchedulerError::Job(format!("claim fallback task failed: {error}"))
                })?;
                if result.rows_affected() > 0 {
                    tx.commit().await.map_err(|error| {
                        SchedulerError::Job(format!("commit claim fallback tx failed: {error}"))
                    })?;
                    Ok(true)
                } else {
                    tx.rollback().await.map_err(|error| {
                        SchedulerError::Job(format!("rollback claim fallback tx failed: {error}"))
                    })?;
                    Ok(false)
                }
            }
        }
    }

    /// Release or complete an adaptive task previously claimed by
    /// `claim_adaptive_task_for_fallback`.
    pub async fn finish_adaptive_task_fallback(
        &self,
        idempotency_key: &str,
        holder: &str,
        succeeded: bool,
        restore_done: bool,
        now_unix_secs: u64,
    ) -> Result<(), SchedulerError> {
        let now = u64_to_i64(now_unix_secs, "now_unix_secs")?;
        let mark_done = succeeded || restore_done;
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(sqlite) => {
                let mut tx = sqlite.pool().begin().await.map_err(|error| {
                    SchedulerError::Job(format!("begin finish fallback tx failed: {error}"))
                })?;
                if mark_done {
                    sqlx::query(
                        "UPDATE Jobs \
                         SET status = 'Done', done_at = ?1, lock_by = NULL, lock_at = NULL \
                         WHERE job_type = ?2 AND idempotency_key = ?3 \
                           AND status = 'Running' AND lock_by = ?4",
                    )
                    .bind(now)
                    .bind(ADAPTIVE_QUEUE)
                    .bind(idempotency_key)
                    .bind(holder)
                    .execute(&mut *tx)
                    .await
                } else {
                    sqlx::query(
                        "UPDATE Jobs \
                         SET status = 'Pending', lock_by = NULL, lock_at = NULL \
                         WHERE job_type = ?1 AND idempotency_key = ?2 \
                           AND status = 'Running' AND lock_by = ?3",
                    )
                    .bind(ADAPTIVE_QUEUE)
                    .bind(idempotency_key)
                    .bind(holder)
                    .execute(&mut *tx)
                    .await
                }
                .map_err(|error| {
                    SchedulerError::Job(format!("finish fallback task failed: {error}"))
                })?;
                sqlx::query(
                    "DELETE FROM Workers \
                     WHERE id = ?1 \
                       AND worker_type = 'lazy-fallback' \
                       AND NOT EXISTS (SELECT 1 FROM Jobs WHERE lock_by = ?1)",
                )
                .bind(holder)
                .execute(&mut *tx)
                .await
                .map_err(|error| {
                    SchedulerError::Job(format!("cleanup fallback worker failed: {error}"))
                })?;
                tx.commit().await.map_err(|error| {
                    SchedulerError::Job(format!("commit finish fallback tx failed: {error}"))
                })?;
                Ok(())
            }
            #[cfg(feature = "postgres")]
            Self::Postgres(postgres) => {
                let mut tx = postgres.pool().begin().await.map_err(|error| {
                    SchedulerError::Job(format!("begin finish fallback tx failed: {error}"))
                })?;
                if mark_done {
                    let done_at = chrono::DateTime::<chrono::Utc>::from_timestamp(now, 0)
                        .ok_or_else(|| {
                            SchedulerError::Job("now_unix_secs outside chrono range".to_owned())
                        })?;
                    sqlx::query(
                        "UPDATE apalis.jobs \
                         SET status = 'Done', done_at = $1, lock_by = NULL, lock_at = NULL \
                         WHERE job_type = $2 AND idempotency_key = $3 \
                           AND status = 'Running' AND lock_by = $4",
                    )
                    .bind(done_at)
                    .bind(ADAPTIVE_QUEUE)
                    .bind(idempotency_key)
                    .bind(holder)
                    .execute(&mut *tx)
                    .await
                } else {
                    sqlx::query(
                        "UPDATE apalis.jobs \
                         SET status = 'Pending', lock_by = NULL, lock_at = NULL \
                         WHERE job_type = $1 AND idempotency_key = $2 \
                           AND status = 'Running' AND lock_by = $3",
                    )
                    .bind(ADAPTIVE_QUEUE)
                    .bind(idempotency_key)
                    .bind(holder)
                    .execute(&mut *tx)
                    .await
                }
                .map_err(|error| {
                    SchedulerError::Job(format!("finish fallback task failed: {error}"))
                })?;
                sqlx::query(
                    "DELETE FROM apalis.workers \
                     WHERE id = $1 \
                       AND worker_type = 'lazy-fallback' \
                       AND NOT EXISTS (SELECT 1 FROM apalis.jobs WHERE lock_by = $1)",
                )
                .bind(holder)
                .execute(&mut *tx)
                .await
                .map_err(|error| {
                    SchedulerError::Job(format!("cleanup fallback worker failed: {error}"))
                })?;
                tx.commit().await.map_err(|error| {
                    SchedulerError::Job(format!("commit finish fallback tx failed: {error}"))
                })?;
                Ok(())
            }
        }
    }
}

#[cfg(feature = "sqlite")]
fn build_sqlite_task<Args>(task: SchedulerPushTask<Args>) -> apalis_sqlite::SqliteTask<Args> {
    build_apalis_task(task)
}

#[cfg(feature = "postgres")]
fn build_postgres_task<Args>(task: SchedulerPushTask<Args>) -> apalis_postgres::PgTask<Args> {
    build_apalis_task(task)
}

fn build_apalis_task<Args>(task: SchedulerPushTask<Args>) -> apalis_core::task::Task<Args> {
    let builder = TaskBuilder::new(task.args);
    let builder = match task.idempotency_key {
        Some(idempotency_key) => builder.idempotency_key(idempotency_key),
        None => builder,
    };
    let builder = match task.run_at_unix_secs {
        Some(run_at_unix_secs) => builder.run_at_timestamp(run_at_unix_secs),
        None => builder,
    };
    builder.build()
}

#[cfg(all(test, feature = "sqlite"))]
async fn insert_sqlite_task<Args>(
    pool: &sqlx::SqlitePool,
    queue: &str,
    task: SchedulerPushTask<Args>,
) -> Result<(), SchedulerError>
where
    Args: DeserializeOwned + serde::Serialize,
{
    insert_sqlite_task_with_mode(pool, queue, task, false).await
}

#[cfg(feature = "sqlite")]
async fn insert_sqlite_task_with_mode<Args>(
    pool: &sqlx::SqlitePool,
    queue: &str,
    task: SchedulerPushTask<Args>,
    expedite_pending: bool,
) -> Result<(), SchedulerError>
where
    Args: DeserializeOwned + serde::Serialize,
{
    let max_attempts = task.max_attempts;
    let task = build_sqlite_task(task);
    let payload = encode_task_args(&task.args)?;
    let default_run_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let run_at = u64_to_i64(task.run_at().unwrap_or(default_run_at), "run_at_unix_secs")?;
    let default_max_attempts = task
        .max_attempts()
        .and_then(|v| i32::try_from(v).ok())
        .unwrap_or(DEFAULT_MAX_ATTEMPTS);
    let max_attempts = max_attempts.unwrap_or(default_max_attempts);
    let priority = task
        .priority()
        .and_then(|v| i32::try_from(v).ok())
        .unwrap_or(0);
    let metadata = serde_json::to_string(task.metadata())
        .map_err(|error| SchedulerError::Job(format!("encode task metadata: {error}")))?;
    let query = if expedite_pending {
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, last_result, lock_at, lock_by, done_at, priority, metadata, idempotency_key) \
         VALUES (?1, ?2, ?3, 'Pending', 0, ?4, ?5, NULL, NULL, NULL, NULL, ?6, ?7, ?8) \
         ON CONFLICT(job_type, idempotency_key) DO UPDATE \
         SET run_at = MIN(Jobs.run_at, excluded.run_at) \
         WHERE Jobs.status = 'Pending' AND Jobs.attempts = 0"
    } else {
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, last_result, lock_at, lock_by, done_at, priority, metadata, idempotency_key) \
         VALUES (?1, ?2, ?3, 'Pending', 0, ?4, ?5, NULL, NULL, NULL, NULL, ?6, ?7, ?8)"
    };
    sqlx::query(query)
        .bind(payload)
        .bind(ulid::Ulid::generate().to_string())
        .bind(queue)
        .bind(max_attempts)
        .bind(run_at)
        .bind(priority)
        .bind(metadata)
        .bind(task.idempotency_key().map(ToOwned::to_owned))
        .execute(pool)
        .await
        .map_err(SchedulerError::from_task_push_sqlx_error)?;
    Ok(())
}

#[cfg(feature = "postgres")]
async fn insert_postgres_task_with_mode<Args>(
    pool: &sqlx::PgPool,
    queue: &str,
    task: SchedulerPushTask<Args>,
    expedite_pending: bool,
) -> Result<(), SchedulerError>
where
    Args: DeserializeOwned + serde::Serialize,
{
    let max_attempts = task.max_attempts;
    let task = build_postgres_task(task);
    let payload = encode_task_args(&task.args)?;
    let default_run_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let run_at = u64_to_i64(task.run_at().unwrap_or(default_run_at), "run_at_unix_secs")?;
    let run_at = chrono::DateTime::<chrono::Utc>::from_timestamp(run_at, 0)
        .ok_or_else(|| SchedulerError::Job("run_at outside chrono timestamp range".to_owned()))?;
    let default_max_attempts = task
        .max_attempts()
        .and_then(|v| i32::try_from(v).ok())
        .unwrap_or(DEFAULT_MAX_ATTEMPTS);
    let max_attempts = max_attempts.unwrap_or(default_max_attempts);
    let priority = task
        .priority()
        .and_then(|v| i32::try_from(v).ok())
        .unwrap_or(0);
    let query = if expedite_pending {
        "INSERT INTO apalis.jobs (id, job_type, job, status, attempts, max_attempts, run_at, priority, metadata, idempotency_key) \
         VALUES ($1, $2, $3, 'Pending', 0, $4, $5, $6, ''::hstore, $7) \
         ON CONFLICT (job_type, idempotency_key) DO UPDATE \
         SET run_at = LEAST(apalis.jobs.run_at, EXCLUDED.run_at) \
         WHERE apalis.jobs.status = 'Pending' AND apalis.jobs.attempts = 0"
    } else {
        "INSERT INTO apalis.jobs (id, job_type, job, status, attempts, max_attempts, run_at, priority, metadata, idempotency_key) \
         VALUES ($1, $2, $3, 'Pending', 0, $4, $5, $6, ''::hstore, $7)"
    };
    sqlx::query(query)
        .bind(ulid::Ulid::generate().to_string())
        .bind(queue)
        .bind(payload)
        .bind(max_attempts)
        .bind(run_at)
        .bind(priority)
        .bind(task.idempotency_key().map(ToOwned::to_owned))
        .execute(pool)
        .await
        .map_err(SchedulerError::from_task_push_sqlx_error)?;
    Ok(())
}

fn encode_task_args<Args>(args: &Args) -> Result<Vec<u8>, SchedulerError>
where
    Args: DeserializeOwned + serde::Serialize,
{
    apalis_codec::json::JsonCodec::<Vec<u8>>::default()
        .encode(args)
        .map_err(|error| SchedulerError::Job(format!("encode scheduler task: {error}")))
}

fn decode_task_args<Args>(bytes: &[u8]) -> Result<Args, SchedulerError>
where
    Args: DeserializeOwned + serde::Serialize,
{
    apalis_codec::json::JsonCodec::<Vec<u8>>::default()
        .decode(&bytes.to_vec())
        .map_err(|error| SchedulerError::Job(format!("decode scheduler task: {error}")))
}

#[cfg(feature = "sqlite")]
fn sqlite_task_to_row<Args>(
    task: apalis_sqlite::SqliteTask,
) -> Result<SchedulerTaskRow<Args>, SchedulerError>
where
    Args: DeserializeOwned + serde::Serialize,
{
    let args = decode_task_args(&task.args)?;
    let idempotency_key = task.idempotency_key().map(ToOwned::to_owned);
    let run_at = task.run_at().unwrap_or(0);
    let status = task.status();
    let attempts = task.attempt();
    let max_attempts = task
        .max_attempts()
        .and_then(|v| i32::try_from(v).ok())
        .unwrap_or(DEFAULT_MAX_ATTEMPTS);
    scheduler_task_row(
        args,
        idempotency_key,
        run_at,
        status,
        attempts,
        max_attempts,
    )
}

#[cfg(feature = "postgres")]
fn postgres_task_to_row<Args>(
    task: apalis_postgres::PgTask,
) -> Result<SchedulerTaskRow<Args>, SchedulerError>
where
    Args: DeserializeOwned + serde::Serialize,
{
    let args = decode_task_args(&task.args)?;
    let idempotency_key = task.idempotency_key().map(ToOwned::to_owned);
    let run_at = task.run_at().unwrap_or(0);
    let status = task.status();
    let attempts = task.attempt();
    let max_attempts = task
        .max_attempts()
        .and_then(|v| i32::try_from(v).ok())
        .unwrap_or(DEFAULT_MAX_ATTEMPTS);
    scheduler_task_row(
        args,
        idempotency_key,
        run_at,
        status,
        attempts,
        max_attempts,
    )
}

fn scheduler_task_row<Args>(
    args: Args,
    idempotency_key: Option<String>,
    run_at_unix_secs: u64,
    status: TaskStatus,
    attempts: usize,
    max_attempts: i32,
) -> Result<SchedulerTaskRow<Args>, SchedulerError> {
    let attempts = i32::try_from(attempts)
        .map_err(|_| SchedulerError::Job("task attempts exceeds i32::MAX".to_owned()))?;
    Ok(SchedulerTaskRow {
        args,
        idempotency_key,
        run_at_unix_secs,
        status,
        attempts,
        max_attempts,
    })
}

fn u64_to_i64(value: u64, field: &str) -> Result<i64, SchedulerError> {
    i64::try_from(value).map_err(|_| SchedulerError::Job(format!("{field} exceeds i64::MAX")))
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use cc_lb_storage_api::CacheTtl;
    use sqlx::SqlitePool;
    use uuid::Uuid;

    use crate::jobs::cache_keepalive::CacheKeepaliveJob;
    use crate::worker::{AdaptiveJob, SchedulerPushTask};

    use super::{ADAPTIVE_QUEUE, insert_sqlite_task};

    const CREATE_JOBS: &str = "
        CREATE TABLE Jobs (
            job BLOB NOT NULL,
            id TEXT NOT NULL UNIQUE,
            job_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'Pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT 25,
            run_at INTEGER NOT NULL,
            last_result TEXT,
            lock_at INTEGER,
            lock_by TEXT,
            done_at INTEGER,
            priority INTEGER NOT NULL DEFAULT 0,
            metadata TEXT,
            idempotency_key TEXT
        )
    ";

    #[tokio::test]
    async fn sqlite_insert_uses_task_max_attempts_override() {
        let pool = SqlitePool::connect(":memory:").await.expect("sqlite pool");
        sqlx::query(CREATE_JOBS)
            .execute(&pool)
            .await
            .expect("create Jobs table");
        let job = CacheKeepaliveJob {
            session_key_hash: "session-hash".to_owned(),
            generation: 1,
            principal_id: "principal".to_owned(),
            upstream_id: Uuid::from_u128(1),
            ttl: CacheTtl::Ttl5m,
            cache_anchor_at_unix_secs: 1_800_000_000,
            expires_at_unix_secs: 1_800_000_300,
            refresh_delay_secs: 270,
            max_refreshes: 12,
            max_total_duration_secs: 14_400,
            traceparent: None,
        };

        insert_sqlite_task(
            &pool,
            ADAPTIVE_QUEUE,
            SchedulerPushTask {
                args: AdaptiveJob::CacheKeepalive(job),
                idempotency_key: Some("cache_keepalive:session-hash:1".to_owned()),
                run_at_unix_secs: Some(1_800_000_270),
                max_attempts: Some(1),
            },
        )
        .await
        .expect("insert scheduler task");

        let max_attempts: i64 = sqlx::query_scalar("SELECT max_attempts FROM Jobs")
            .fetch_one(&pool)
            .await
            .expect("read max_attempts");
        assert_eq!(max_attempts, 1);
    }

    #[tokio::test]
    async fn sqlite_insert_defaults_max_attempts_to_five_when_none() {
        let pool = SqlitePool::connect(":memory:").await.expect("sqlite pool");
        sqlx::query(CREATE_JOBS)
            .execute(&pool)
            .await
            .expect("create Jobs table");
        let job = CacheKeepaliveJob {
            session_key_hash: "session-hash".to_owned(),
            generation: 1,
            principal_id: "principal".to_owned(),
            upstream_id: Uuid::from_u128(1),
            ttl: CacheTtl::Ttl5m,
            cache_anchor_at_unix_secs: 1_800_000_000,
            expires_at_unix_secs: 1_800_000_300,
            refresh_delay_secs: 270,
            max_refreshes: 12,
            max_total_duration_secs: 14_400,
            traceparent: None,
        };

        insert_sqlite_task(
            &pool,
            ADAPTIVE_QUEUE,
            SchedulerPushTask {
                args: AdaptiveJob::CacheKeepalive(job),
                idempotency_key: Some("cache_keepalive:session-hash:default".to_owned()),
                run_at_unix_secs: Some(1_800_000_270),
                max_attempts: None,
            },
        )
        .await
        .expect("insert scheduler task");

        let max_attempts: i64 = sqlx::query_scalar("SELECT max_attempts FROM Jobs")
            .fetch_one(&pool)
            .await
            .expect("read max_attempts");
        assert_eq!(max_attempts, 5);
    }

    #[tokio::test]
    async fn sqlite_claim_adaptive_task_for_fallback_enforces_exclusive_ownership() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                sqlx::sqlite::SqliteConnectOptions::new()
                    .in_memory(true)
                    .foreign_keys(true),
            )
            .await
            .expect("sqlite pool");
        sqlx::query(
            "CREATE TABLE Workers (
                id TEXT NOT NULL UNIQUE,
                worker_type TEXT NOT NULL,
                storage_name TEXT NOT NULL,
                layers TEXT,
                last_seen INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                started_at INTEGER
            )",
        )
        .execute(&pool)
        .await
        .expect("create Workers table");
        sqlx::query(
            "CREATE TABLE Jobs (
                job BLOB NOT NULL,
                id TEXT NOT NULL UNIQUE,
                job_type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'Pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 25,
                run_at INTEGER NOT NULL,
                last_result TEXT,
                lock_at INTEGER,
                lock_by TEXT,
                done_at INTEGER,
                priority INTEGER NOT NULL DEFAULT 0,
                metadata TEXT,
                idempotency_key TEXT,
                PRIMARY KEY(id),
                FOREIGN KEY(lock_by) REFERENCES Workers(id)
            )",
        )
        .execute(&pool)
        .await
        .expect("create Jobs table");
        let backend =
            crate::worker::SchedulerBackend::Sqlite(crate::worker::SqliteSchedulerBackend::new(
                pool.clone(),
                std::sync::Arc::new(cc_lb_clock::SystemClock),
            ));
        let upstream_id = Uuid::from_u128(99);
        let job = crate::jobs::oauth_refresh::OAuthRefreshJob::new(upstream_id);
        let key_pending = job.idempotency_key(1_800_000_000);
        let key_running = job.idempotency_key(1_800_000_100);
        let key_done = job.idempotency_key(1_800_000_200);
        for key in [&key_pending, &key_running, &key_done] {
            insert_sqlite_task(
                &pool,
                ADAPTIVE_QUEUE,
                SchedulerPushTask {
                    args: AdaptiveJob::OAuthRefresh(job.clone()),
                    idempotency_key: Some(key.clone()),
                    run_at_unix_secs: Some(1_800_000_000),
                    max_attempts: None,
                },
            )
            .await
            .expect("insert oauth refresh task");
        }
        sqlx::query(
            "INSERT INTO Workers (id, worker_type, storage_name, layers, last_seen, started_at) \
             VALUES ('worker-1', 'adaptive', 'default', '', 1800000000, 1800000000)",
        )
        .execute(&pool)
        .await
        .expect("insert active worker");
        sqlx::query(
            "UPDATE Jobs SET status = 'Running', lock_by = 'worker-1' WHERE idempotency_key = ?1",
        )
        .bind(&key_running)
        .execute(&pool)
        .await
        .expect("mark running");
        sqlx::query(
            "UPDATE Jobs SET status = 'Done', done_at = 1800000010 WHERE idempotency_key = ?1",
        )
        .bind(&key_done)
        .execute(&pool)
        .await
        .expect("mark done");

        assert!(
            !backend
                .claim_adaptive_task_for_fallback(&key_running, "fallback-a", 1_800_000_050, false)
                .await
                .expect("running claim check"),
            "active scheduled job must refuse fallback ownership"
        );
        assert!(
            !backend
                .claim_adaptive_task_for_fallback(&key_done, "fallback-a", 1_800_000_050, false)
                .await
                .expect("done claim check when allow_done=false"),
            "allow_done=false must refuse completed job"
        );
        let fallback_workers_after_rejects: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM Workers WHERE worker_type = 'lazy-fallback'")
                .fetch_one(&pool)
                .await
                .expect("count fallback workers after rejects");
        assert_eq!(
            fallback_workers_after_rejects, 0,
            "rejected fallback claims must roll back synthetic worker row"
        );

        assert!(
            backend
                .claim_adaptive_task_for_fallback(&key_done, "fallback-a", 1_800_000_050, true)
                .await
                .expect("done claim check when allow_done=true")
        );
        assert!(
            !backend
                .claim_adaptive_task_for_fallback(&key_done, "fallback-b", 1_800_000_051, true)
                .await
                .expect("second claimer on done job must fail while running")
        );
        let rejected_second_worker: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM Workers WHERE id = 'fallback-b'")
                .fetch_one(&pool)
                .await
                .expect("check rejected fallback-b worker");
        assert_eq!(
            rejected_second_worker, 0,
            "rejected concurrent claim must not leak fallback-b into Workers"
        );
        backend
            .finish_adaptive_task_fallback(&key_done, "fallback-a", false, true, 1_800_000_052)
            .await
            .expect("restore done status");
        let restored_status: String =
            sqlx::query_scalar("SELECT status FROM Jobs WHERE idempotency_key = ?1")
                .bind(&key_done)
                .fetch_one(&pool)
                .await
                .expect("read done status");
        assert_eq!(restored_status, "Done");

        assert!(
            backend
                .claim_adaptive_task_for_fallback(&key_pending, "fallback-a", 1_800_000_060, false)
                .await
                .expect("first claim on pending succeeds")
        );
        assert!(
            !backend
                .claim_adaptive_task_for_fallback(&key_pending, "fallback-b", 1_800_000_061, false)
                .await
                .expect("concurrent second claim on pending fails")
        );
        let rejected_pending_second: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM Workers WHERE id = 'fallback-b'")
                .fetch_one(&pool)
                .await
                .expect("check rejected fallback-b worker on pending");
        assert_eq!(rejected_pending_second, 0);
        backend
            .finish_adaptive_task_fallback(&key_pending, "fallback-a", true, false, 1_800_000_062)
            .await
            .expect("complete pending fallback");
        let final_status: String =
            sqlx::query_scalar("SELECT status FROM Jobs WHERE idempotency_key = ?1")
                .bind(&key_pending)
                .fetch_one(&pool)
                .await
                .expect("read completed status");
        assert_eq!(final_status, "Done");
        let remaining_fallback_workers: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM Workers WHERE worker_type = 'lazy-fallback'")
                .fetch_one(&pool)
                .await
                .expect("count remaining fallback workers");
        assert_eq!(
            remaining_fallback_workers, 0,
            "completed fallback claims must clean up lazy-fallback worker rows"
        );
        let total_workers: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM Workers")
            .fetch_one(&pool)
            .await
            .expect("count total workers");
        assert_eq!(total_workers, 1, "only worker-1 should remain in Workers");
    }
}
