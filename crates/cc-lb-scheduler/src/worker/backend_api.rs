pub use apalis_core::backend::Filter;
use apalis_core::backend::ListTasks;
use apalis_core::backend::codec::Codec as _;
use apalis_core::task::builder::TaskBuilder;
pub use apalis_core::task::status::Status as TaskStatus;

use serde::de::DeserializeOwned;

use crate::error::SchedulerError;

use super::{ADAPTIVE_QUEUE, AdaptiveJob, CRON_QUEUE, CronJob, SchedulerBackend};

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
}

impl SchedulerBackend {
    pub async fn push_adaptive_task(
        &self,
        task: SchedulerPushTask<AdaptiveJob>,
    ) -> Result<(), SchedulerError> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(sqlite) => {
                insert_sqlite_task(&sqlite.pool, ADAPTIVE_QUEUE, build_sqlite_task(task)).await
            }
            #[cfg(feature = "postgres")]
            Self::Postgres(postgres) => {
                insert_postgres_task(&postgres.pool, ADAPTIVE_QUEUE, build_postgres_task(task))
                    .await
            }
        }
    }

    pub async fn push_cron_task(
        &self,
        task: SchedulerPushTask<CronJob>,
    ) -> Result<(), SchedulerError> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(sqlite) => {
                insert_sqlite_task(&sqlite.pool, CRON_QUEUE, build_sqlite_task(task)).await
            }
            #[cfg(feature = "postgres")]
            Self::Postgres(postgres) => {
                insert_postgres_task(&postgres.pool, CRON_QUEUE, build_postgres_task(task)).await
            }
        }
    }

    pub async fn list_adaptive_tasks(
        &self,
        filter: &Filter,
    ) -> Result<Vec<SchedulerTaskRow<AdaptiveJob>>, SchedulerError> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(sqlite) => sqlite
                .storage
                .list_tasks(filter)
                .await
                .map_err(SchedulerError::Database)?
                .into_iter()
                .map(sqlite_task_to_row)
                .collect(),
            #[cfg(feature = "postgres")]
            Self::Postgres(postgres) => postgres
                .storage
                .list_tasks(filter)
                .await
                .map_err(SchedulerError::Database)?
                .into_iter()
                .map(postgres_task_to_row)
                .collect(),
        }
    }

    pub async fn list_cron_tasks(
        &self,
        filter: &Filter,
    ) -> Result<Vec<SchedulerTaskRow<CronJob>>, SchedulerError> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(sqlite) => {
                let storage = apalis_sqlite::SqliteStorage::<CronJob, (), ()>::new_in_queue(
                    &sqlite.pool,
                    CRON_QUEUE,
                );
                storage
                    .list_tasks(filter)
                    .await
                    .map_err(SchedulerError::Database)?
                    .into_iter()
                    .map(sqlite_task_to_row)
                    .collect()
            }
            #[cfg(feature = "postgres")]
            Self::Postgres(postgres) => {
                let storage = apalis_postgres::PostgresStorage::<CronJob>::new_with_config(
                    &postgres.pool,
                    &apalis_postgres::Config::new(CRON_QUEUE),
                );
                storage
                    .list_tasks(filter)
                    .await
                    .map_err(SchedulerError::Database)?
                    .into_iter()
                    .map(postgres_task_to_row)
                    .collect()
            }
        }
    }
}

#[cfg(feature = "sqlite")]
fn build_sqlite_task<Args>(task: SchedulerPushTask<Args>) -> apalis_sqlite::SqliteTask<Args> {
    build_apalis_task::<Args, apalis_sqlite::SqliteContext, ulid::Ulid>(task)
}

#[cfg(feature = "postgres")]
fn build_postgres_task<Args>(task: SchedulerPushTask<Args>) -> apalis_postgres::PgTask<Args> {
    build_apalis_task::<Args, apalis_postgres::PgContext, ulid::Ulid>(task)
}

fn build_apalis_task<Args, Ctx, IdType>(
    task: SchedulerPushTask<Args>,
) -> apalis_core::task::Task<Args, Ctx, IdType>
where
    Ctx: Default,
{
    let builder = TaskBuilder::new(task.args);
    let builder = match task.idempotency_key {
        Some(idempotency_key) => builder.with_idempotency_key(idempotency_key),
        None => builder,
    };
    let builder = match task.run_at_unix_secs {
        Some(run_at_unix_secs) => builder.run_at_timestamp(run_at_unix_secs),
        None => builder,
    };
    builder.build()
}

#[cfg(feature = "sqlite")]
async fn insert_sqlite_task<Args>(
    pool: &sqlx::SqlitePool,
    queue: &str,
    task: apalis_sqlite::SqliteTask<Args>,
) -> Result<(), SchedulerError>
where
    Args: DeserializeOwned + serde::Serialize,
{
    let payload = encode_task_args(&task.args)?;
    let run_at = u64_to_i64(task.parts.run_at, "run_at_unix_secs")?;
    let metadata = serde_json::to_string(task.parts.ctx.meta())
        .map_err(|error| SchedulerError::Job(format!("encode task metadata: {error}")))?;
    sqlx::query(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, last_result, lock_at, lock_by, done_at, priority, metadata, idempotency_key) \
         VALUES (?1, ?2, ?3, 'Pending', 0, ?4, ?5, NULL, NULL, NULL, NULL, ?6, ?7, ?8)",
    )
    .bind(payload)
    .bind(ulid::Ulid::new().to_string())
    .bind(queue)
    .bind(task.parts.ctx.max_attempts())
    .bind(run_at)
    .bind(task.parts.ctx.priority())
    .bind(metadata)
    .bind(task.parts.idempotency_key)
    .execute(pool)
    .await
    .map_err(SchedulerError::from_task_push_sqlx_error)?;
    Ok(())
}

#[cfg(feature = "postgres")]
async fn insert_postgres_task<Args>(
    pool: &sqlx::PgPool,
    queue: &str,
    task: apalis_postgres::PgTask<Args>,
) -> Result<(), SchedulerError>
where
    Args: DeserializeOwned + serde::Serialize,
{
    let payload = encode_task_args(&task.args)?;
    let run_at = u64_to_i64(task.parts.run_at, "run_at_unix_secs")?;
    let run_at = chrono::DateTime::<chrono::Utc>::from_timestamp(run_at, 0)
        .ok_or_else(|| SchedulerError::Job("run_at outside chrono timestamp range".to_owned()))?;
    let metadata = serde_json::Value::Object(task.parts.ctx.meta().clone());
    sqlx::query(
        "INSERT INTO apalis.jobs (id, job_type, job, status, attempts, max_attempts, run_at, priority, metadata, idempotency_key) \
         VALUES ($1, $2, $3, 'Pending', 0, $4, $5, $6, $7, $8)",
    )
    .bind(ulid::Ulid::new().to_string())
    .bind(queue)
    .bind(payload)
    .bind(task.parts.ctx.max_attempts())
    .bind(run_at)
    .bind(task.parts.ctx.priority())
    .bind(metadata)
    .bind(task.parts.idempotency_key)
    .execute(pool)
    .await
    .map_err(SchedulerError::from_task_push_sqlx_error)?;
    Ok(())
}

fn encode_task_args<Args>(args: &Args) -> Result<Vec<u8>, SchedulerError>
where
    Args: DeserializeOwned + serde::Serialize,
{
    apalis_codec::json::JsonCodec::<Vec<u8>>::encode(args)
        .map_err(|error| SchedulerError::Job(format!("encode scheduler task: {error}")))
}

#[cfg(feature = "sqlite")]
fn sqlite_task_to_row<Args>(
    task: apalis_sqlite::SqliteTask<Args>,
) -> Result<SchedulerTaskRow<Args>, SchedulerError> {
    scheduler_task_row(
        task.args,
        task.parts.idempotency_key,
        task.parts.run_at,
        task.parts.status.load(),
        task.parts.attempt.current(),
        task.parts.ctx.max_attempts(),
    )
}

#[cfg(feature = "postgres")]
fn postgres_task_to_row<Args>(
    task: apalis_postgres::PgTask<Args>,
) -> Result<SchedulerTaskRow<Args>, SchedulerError> {
    scheduler_task_row(
        task.args,
        task.parts.idempotency_key,
        task.parts.run_at,
        task.parts.status.load(),
        task.parts.attempt.current(),
        task.parts.ctx.max_attempts(),
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
