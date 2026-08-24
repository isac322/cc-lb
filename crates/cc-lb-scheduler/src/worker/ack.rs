use std::time::Duration;

use apalis_core::backend::Backend;
use apalis_core::error::BoxDynError;
use apalis_core::layers::Stack;
#[cfg(feature = "sqlite")]
use apalis_core::layers::{Layer, Service};
use apalis_core::task::Parts;
use apalis_core::task::status::Status;
use apalis_core::worker::context::WorkerContext;
use apalis_core::worker::ext::ack::{Acknowledge, AcknowledgeLayer};
use futures_util::FutureExt as _;
use futures_util::future::BoxFuture;
use ulid::Ulid;

use crate::retry::JobOutcome;

fn retry_delay_secs(delay: Duration) -> Result<i64, sqlx::Error> {
    let seconds = delay
        .as_secs()
        .saturating_add(u64::from(delay.subsec_nanos() != 0));
    i64::try_from(seconds)
        .map_err(|_| sqlx::Error::Protocol("scheduler retry delay exceeds i64::MAX".to_owned()))
}

fn terminal_status<Ctx>(
    result: &Result<JobOutcome, BoxDynError>,
    parts: &Parts<Ctx, Ulid>,
    max_attempts: i32,
) -> Status {
    match result {
        Ok(JobOutcome::DeadLetter) => Status::Killed,
        Ok(
            JobOutcome::Done | JobOutcome::Skip | JobOutcome::Noop | JobOutcome::DuplicateEffect,
        ) => Status::Done,
        Ok(JobOutcome::Retry { delay: _ }) => Status::Pending,
        Err(_) if usize::try_from(max_attempts).unwrap_or(0) <= parts.attempt.current() => {
            Status::Killed
        }
        Err(_) => Status::Failed,
    }
}
#[cfg(feature = "sqlite")]
#[derive(Clone, Debug)]
pub(crate) struct SqliteLockTaskLayer {
    pool: sqlx::SqlitePool,
}

#[cfg(feature = "sqlite")]
impl SqliteLockTaskLayer {
    fn new(pool: sqlx::SqlitePool) -> Self {
        Self { pool }
    }
}

#[cfg(feature = "sqlite")]
impl<S> Layer<S> for SqliteLockTaskLayer {
    type Service = SqliteLockTaskService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        SqliteLockTaskService {
            inner,
            pool: self.pool.clone(),
        }
    }
}

#[cfg(feature = "sqlite")]
#[derive(Clone, Debug)]
pub(crate) struct SqliteLockTaskService<S> {
    inner: S,
    pool: sqlx::SqlitePool,
}

#[cfg(feature = "sqlite")]
impl<S, Args> Service<apalis_sqlite::SqliteTask<Args>> for SqliteLockTaskService<S>
where
    S: Service<apalis_sqlite::SqliteTask<Args>> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxDynError>,
    Args: Send + 'static,
{
    type Response = S::Response;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context).map_err(Into::into)
    }

    fn call(&mut self, mut task: apalis_sqlite::SqliteTask<Args>) -> Self::Future {
        let pool = self.pool.clone();
        let worker_id = task
            .parts
            .data
            .get::<WorkerContext>()
            .map(|worker| worker.name().to_owned());
        let task_id = task.parts.task_id.map(|id| id.to_string());
        if let Some(worker_id) = worker_id.as_ref() {
            task.parts.ctx = task.parts.ctx.clone().with_lock_by(Some(worker_id.clone()));
        }
        let future = self.inner.call(task);

        async move {
            let task_id =
                task_id.ok_or_else(|| sqlx::Error::ColumnNotFound("TASK_ID_FOR_LOCK".to_owned()))?;
            let worker_id = worker_id
                .ok_or_else(|| sqlx::Error::ColumnNotFound("WORKER_ID_FOR_LOCK".to_owned()))?;
            let locked = sqlx::query(
                "UPDATE Jobs \
                 SET status = 'Running', lock_at = CAST(strftime('%s', 'now') AS INTEGER), lock_by = ?1 \
                 WHERE id = ?2 \
                   AND (status = 'Queued' OR status = 'Pending' \
                        OR (status = 'Failed' AND attempts < max_attempts))",
            )
            .bind(worker_id)
            .bind(task_id)
            .execute(&pool)
            .await?;
            if locked.rows_affected() != 1 {
                return Err(sqlx::Error::RowNotFound.into());
            }
            future.await.map_err(Into::into)
        }
        .boxed()
    }
}

#[cfg(feature = "sqlite")]
#[derive(Clone, Debug)]
pub(crate) struct RetryAwareSqliteBackend<B> {
    inner: B,
    pool: sqlx::SqlitePool,
}

#[cfg(feature = "sqlite")]
impl<B> RetryAwareSqliteBackend<B> {
    pub(crate) fn new(inner: B, pool: sqlx::SqlitePool) -> Self {
        Self { inner, pool }
    }
}

#[cfg(feature = "sqlite")]
impl<Args, B> Backend for RetryAwareSqliteBackend<B>
where
    B: Backend<
            Args = Args,
            IdType = Ulid,
            Context = apalis_sqlite::SqliteContext,
            Error = sqlx::Error,
        >,
{
    type Args = Args;
    type IdType = Ulid;
    type Context = apalis_sqlite::SqliteContext;
    type Error = sqlx::Error;
    type Stream = B::Stream;
    type Beat = B::Beat;
    type Layer = Stack<SqliteLockTaskLayer, AcknowledgeLayer<SqliteSchedulerAck>>;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        self.inner.heartbeat(worker)
    }

    fn middleware(&self) -> Self::Layer {
        Stack::new(
            SqliteLockTaskLayer::new(self.pool.clone()),
            AcknowledgeLayer::new(SqliteSchedulerAck::new(self.pool.clone())),
        )
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        self.inner.poll(worker)
    }
}

#[cfg(feature = "sqlite")]
#[derive(Clone, Debug)]
pub(crate) struct SqliteSchedulerAck {
    pool: sqlx::SqlitePool,
}

#[cfg(feature = "sqlite")]
impl SqliteSchedulerAck {
    fn new(pool: sqlx::SqlitePool) -> Self {
        Self { pool }
    }
}

#[cfg(feature = "sqlite")]
impl Acknowledge<JobOutcome, apalis_sqlite::SqliteContext, Ulid> for SqliteSchedulerAck {
    type Error = sqlx::Error;
    type Future = BoxFuture<'static, Result<(), Self::Error>>;

    fn ack(
        &mut self,
        result: &Result<JobOutcome, BoxDynError>,
        parts: &Parts<apalis_sqlite::SqliteContext, Ulid>,
    ) -> Self::Future {
        let pool = self.pool.clone();
        let task_id = parts.task_id;
        let worker_id = parts.ctx.lock_by().clone();
        let attempt = i32::try_from(parts.attempt.current())
            .map_err(|_| sqlx::Error::Protocol("scheduler attempt exceeds i32::MAX".to_owned()));
        let response = serde_json::to_string(&result.as_ref().map_err(|error| error.to_string()))
            .map_err(|error| sqlx::Error::Decode(error.into()));
        let status = terminal_status(result, parts, parts.ctx.max_attempts());
        let retry_delay = match result {
            Ok(JobOutcome::Retry { delay }) => Some(retry_delay_secs(*delay)),
            _ => None,
        };
        parts.status.store(status.clone());

        async move {
            let task_id = task_id
                .ok_or_else(|| sqlx::Error::ColumnNotFound("TASK_ID_FOR_ACK".to_owned()))?
                .to_string();
            let worker_id = worker_id
                .ok_or_else(|| sqlx::Error::ColumnNotFound("WORKER_ID_LOCK_BY".to_owned()))?;
            let attempt = attempt?;
            let response = response?;

            let updated = if let Some(delay) = retry_delay {
                sqlx::query(
                    "UPDATE Jobs \
                     SET status = 'Pending', attempts = ?1, last_result = ?2, \
                         run_at = CAST(strftime('%s', 'now') AS INTEGER) + ?3, \
                         done_at = NULL, lock_at = NULL, lock_by = NULL \
                     WHERE id = ?4 AND status = 'Running' AND lock_by = ?5",
                )
                .bind(attempt)
                .bind(response)
                .bind(delay?)
                .bind(task_id)
                .bind(worker_id)
                .execute(&pool)
                .await?
            } else {
                sqlx::query(
                    "UPDATE Jobs \
                     SET status = ?1, attempts = ?2, last_result = ?3, \
                         done_at = CAST(strftime('%s', 'now') AS INTEGER), \
                         lock_at = NULL, lock_by = NULL \
                     WHERE id = ?4 AND status = 'Running' AND lock_by = ?5",
                )
                .bind(status.to_string())
                .bind(attempt)
                .bind(response)
                .bind(task_id)
                .bind(worker_id)
                .execute(&pool)
                .await?
            };

            if updated.rows_affected() != 1 {
                return Err(sqlx::Error::RowNotFound);
            }
            Ok(())
        }
        .boxed()
    }
}

#[cfg(feature = "postgres")]
#[derive(Clone, Debug)]
pub(crate) struct RetryAwarePostgresBackend<B> {
    inner: B,
    pool: sqlx::PgPool,
}

#[cfg(feature = "postgres")]
impl<B> RetryAwarePostgresBackend<B> {
    pub(crate) fn new(inner: B, pool: sqlx::PgPool) -> Self {
        Self { inner, pool }
    }
}

#[cfg(feature = "postgres")]
impl<Args, B> Backend for RetryAwarePostgresBackend<B>
where
    B: Backend<
            Args = Args,
            IdType = Ulid,
            Context = apalis_postgres::PgContext,
            Error = sqlx::Error,
        >,
{
    type Args = Args;
    type IdType = Ulid;
    type Context = apalis_postgres::PgContext;
    type Error = sqlx::Error;
    type Stream = B::Stream;
    type Beat = B::Beat;
    type Layer = Stack<apalis_postgres::LockTaskLayer, AcknowledgeLayer<PostgresSchedulerAck>>;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        self.inner.heartbeat(worker)
    }

    fn middleware(&self) -> Self::Layer {
        Stack::new(
            apalis_postgres::LockTaskLayer::new(self.pool.clone()),
            AcknowledgeLayer::new(PostgresSchedulerAck::new(self.pool.clone())),
        )
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        self.inner.poll(worker)
    }
}

#[cfg(feature = "postgres")]
#[derive(Clone, Debug)]
pub(crate) struct PostgresSchedulerAck {
    pool: sqlx::PgPool,
}

#[cfg(feature = "postgres")]
impl PostgresSchedulerAck {
    fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[cfg(feature = "postgres")]
impl Acknowledge<JobOutcome, apalis_postgres::PgContext, Ulid> for PostgresSchedulerAck {
    type Error = sqlx::Error;
    type Future = BoxFuture<'static, Result<(), Self::Error>>;

    fn ack(
        &mut self,
        result: &Result<JobOutcome, BoxDynError>,
        parts: &Parts<apalis_postgres::PgContext, Ulid>,
    ) -> Self::Future {
        let pool = self.pool.clone();
        let task_id = parts.task_id;
        let worker_id = parts.ctx.lock_by().clone();
        let attempt = i32::try_from(parts.attempt.current())
            .map_err(|_| sqlx::Error::Protocol("scheduler attempt exceeds i32::MAX".to_owned()));
        let response = serde_json::to_value(result.as_ref().map_err(|error| error.to_string()))
            .map_err(|error| sqlx::Error::Decode(error.into()));
        let status = terminal_status(result, parts, parts.ctx.max_attempts());
        let retry_delay = match result {
            Ok(JobOutcome::Retry { delay }) => Some(retry_delay_secs(*delay)),
            _ => None,
        };
        parts.status.store(status.clone());

        async move {
            let task_id = task_id
                .ok_or_else(|| sqlx::Error::ColumnNotFound("TASK_ID_FOR_ACK".to_owned()))?
                .to_string();
            let worker_id = worker_id
                .ok_or_else(|| sqlx::Error::ColumnNotFound("WORKER_ID_LOCK_BY".to_owned()))?;
            let attempt = attempt?;
            let response = response?;

            let updated = if let Some(delay) = retry_delay {
                sqlx::query(
                    "UPDATE apalis.jobs \
                     SET status = 'Pending', attempts = $1, last_result = $2, \
                         run_at = NOW() + ($3 * INTERVAL '1 second'), \
                         done_at = NULL, lock_at = NULL, lock_by = NULL \
                     WHERE id = $4 AND status = 'Running' AND lock_by = $5",
                )
                .bind(attempt)
                .bind(response)
                .bind(delay?)
                .bind(task_id)
                .bind(worker_id)
                .execute(&pool)
                .await?
            } else {
                sqlx::query(
                    "UPDATE apalis.jobs \
                     SET status = $1, attempts = $2, last_result = $3, \
                         done_at = NOW(), lock_at = NULL, lock_by = NULL \
                     WHERE id = $4 AND status = 'Running' AND lock_by = $5",
                )
                .bind(status.to_string())
                .bind(attempt)
                .bind(response)
                .bind(task_id)
                .bind(worker_id)
                .execute(&pool)
                .await?
            };

            if updated.rows_affected() != 1 {
                return Err(sqlx::Error::RowNotFound);
            }
            Ok(())
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use apalis_core::task::Task;
    use apalis_core::task::attempt::Attempt;
    use apalis_core::task::builder::TaskBuilder;
    use apalis_core::task::status::Status;
    use apalis_core::task::task_id::TaskId;

    use super::*;

    const WORKER_ID: &str = "retry-worker";
    const IDEMPOTENCY_KEY: &str = "adaptive:oauth_refresh:test-upstream:1700000000";
    const PAYLOAD: &[u8] = b"{\"type\":\"oauth_refresh\"}";
    #[cfg(feature = "sqlite")]
    type SqliteRetryRow = (
        Vec<u8>,
        String,
        String,
        i64,
        i64,
        Option<String>,
        Option<i64>,
        Option<i64>,
        String,
    );

    #[cfg(feature = "postgres")]
    type PostgresRetryRow = (
        Vec<u8>,
        String,
        String,
        i32,
        chrono::DateTime<chrono::Utc>,
        Option<String>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
        String,
    );
    #[cfg(feature = "postgres")]
    type PostgresTerminalRow = (
        String,
        i32,
        Option<String>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
    );

    #[test]
    fn terminal_status_preserves_error_retry_limits() {
        let failed_task: Task<(), (), Ulid> = TaskBuilder::new(())
            .with_attempt(Attempt::new_with_value(4))
            .build();
        let failed_result: Result<JobOutcome, BoxDynError> =
            Err(std::io::Error::other("retryable handler failure").into());
        assert_eq!(
            terminal_status(&failed_result, &failed_task.parts, 5),
            Status::Failed
        );

        let killed_task: Task<(), (), Ulid> = TaskBuilder::new(())
            .with_attempt(Attempt::new_with_value(5))
            .build();
        let killed_result: Result<JobOutcome, BoxDynError> =
            Err(std::io::Error::other("exhausted handler failure").into());
        assert_eq!(
            terminal_status(&killed_result, &killed_task.parts, 5),
            Status::Killed
        );
        let dead_letter_result: Result<JobOutcome, BoxDynError> = Ok(JobOutcome::DeadLetter);
        assert_eq!(
            terminal_status(&dead_letter_result, &failed_task.parts, 5),
            Status::Killed
        );
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn sqlite_retry_rearms_same_row_and_dead_letter_terminalizes_it()
    -> Result<(), Box<dyn std::error::Error>> {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await?;
        apalis_sqlite::SqliteStorage::setup(&pool).await?;
        crate::migrations::apply_post_setup_migrations(&pool).await?;
        sqlx::query(
            "INSERT INTO Workers (id, worker_type, storage_name, layers) \
             VALUES (?1, 'adaptive', 'sqlite', '')",
        )
        .bind(WORKER_ID)
        .execute(&pool)
        .await?;

        let task_id = Ulid::new();
        insert_sqlite_running_job(&pool, task_id).await?;
        let before_retry: i64 = sqlx::query_scalar("SELECT CAST(strftime('%s', 'now') AS INTEGER)")
            .fetch_one(&pool)
            .await?;
        let retry_parts = sqlite_parts(task_id, 1, 5);
        let retry_result: Result<JobOutcome, BoxDynError> = Ok(JobOutcome::Retry {
            delay: Duration::from_millis(30_001),
        });

        SqliteSchedulerAck::new(pool.clone())
            .ack(&retry_result, &retry_parts)
            .await?;

        let after_retry: i64 = sqlx::query_scalar("SELECT CAST(strftime('%s', 'now') AS INTEGER)")
            .fetch_one(&pool)
            .await?;
        let row: SqliteRetryRow = sqlx::query_as(
            "SELECT job, job_type, status, attempts, run_at, lock_by, lock_at, done_at, idempotency_key \
             FROM Jobs WHERE id = ?1",
        )
        .bind(task_id.to_string())
        .fetch_one(&pool)
        .await?;
        assert_eq!(row.0, PAYLOAD);
        assert_eq!(row.1, "adaptive");
        assert_eq!(row.2, "Pending");
        assert_eq!(row.3, 1);
        assert!(row.4 >= before_retry + 31);
        assert!(row.4 <= after_retry + 31);
        assert_eq!(row.5, None);
        assert_eq!(row.6, None);
        assert_eq!(row.7, None);
        assert_eq!(row.8, IDEMPOTENCY_KEY);
        assert_eq!(retry_parts.status.load(), Status::Pending);

        let duplicate = sqlx::query(
            "INSERT INTO Jobs \
             (job, id, job_type, status, attempts, max_attempts, run_at, priority, metadata, idempotency_key) \
             VALUES (?1, ?2, 'adaptive', 'Pending', 0, 5, 0, 0, '{}', ?3) \
             ON CONFLICT(job_type, idempotency_key) DO NOTHING",
        )
        .bind(b"duplicate".as_slice())
        .bind(Ulid::new().to_string())
        .bind(IDEMPOTENCY_KEY)
        .execute(&pool)
        .await?;
        assert_eq!(duplicate.rows_affected(), 0);
        let row_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE job_type = 'adaptive' AND idempotency_key = ?1",
        )
        .bind(IDEMPOTENCY_KEY)
        .fetch_one(&pool)
        .await?;
        assert_eq!(row_count, 1);

        sqlx::query(
            "UPDATE Jobs \
             SET status = 'Running', lock_by = ?1, lock_at = CAST(strftime('%s', 'now') AS INTEGER) \
             WHERE id = ?2",
        )
        .bind(WORKER_ID)
        .bind(task_id.to_string())
        .execute(&pool)
        .await?;
        let dead_letter_parts = sqlite_parts(task_id, 6, 5);
        let dead_letter_result: Result<JobOutcome, BoxDynError> = Ok(JobOutcome::DeadLetter);
        SqliteSchedulerAck::new(pool.clone())
            .ack(&dead_letter_result, &dead_letter_parts)
            .await?;

        let terminal: (String, i64, Option<String>, Option<i64>, Option<i64>) = sqlx::query_as(
            "SELECT status, attempts, lock_by, lock_at, done_at FROM Jobs WHERE id = ?1",
        )
        .bind(task_id.to_string())
        .fetch_one(&pool)
        .await?;
        assert_eq!(terminal.0, "Killed");
        assert_eq!(terminal.1, 6);
        assert_eq!(terminal.2, None);
        assert_eq!(terminal.3, None);
        assert!(terminal.4.is_some());
        assert_eq!(dead_letter_parts.status.load(), Status::Killed);
        Ok(())
    }

    #[cfg(feature = "sqlite")]
    async fn insert_sqlite_running_job(
        pool: &sqlx::SqlitePool,
        task_id: Ulid,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO Jobs \
             (job, id, job_type, status, attempts, max_attempts, run_at, last_result, \
              lock_at, lock_by, done_at, priority, metadata, idempotency_key) \
             VALUES (?1, ?2, 'adaptive', 'Running', 0, 5, 0, NULL, 0, ?3, NULL, 0, '{}', ?4)",
        )
        .bind(PAYLOAD)
        .bind(task_id.to_string())
        .bind(WORKER_ID)
        .bind(IDEMPOTENCY_KEY)
        .execute(pool)
        .await?;
        Ok(())
    }

    #[cfg(feature = "sqlite")]
    fn sqlite_parts(
        task_id: Ulid,
        attempt: usize,
        max_attempts: i32,
    ) -> Parts<apalis_sqlite::SqliteContext, Ulid> {
        let task: Task<(), apalis_sqlite::SqliteContext, Ulid> = TaskBuilder::new(())
            .with_task_id(TaskId::new(task_id))
            .with_ctx(
                apalis_sqlite::SqliteContext::new()
                    .with_max_attempts(max_attempts)
                    .with_lock_by(Some(WORKER_ID.to_owned())),
            )
            .with_attempt(Attempt::new_with_value(attempt))
            .with_status(Status::Running)
            .build();
        task.parts
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn postgres_retry_rearms_same_row_and_dead_letter_terminalizes_it()
    -> Result<(), Box<dyn std::error::Error>> {
        use testcontainers_modules::postgres::Postgres;
        use testcontainers_modules::testcontainers::ImageExt as _;
        use testcontainers_modules::testcontainers::runners::AsyncRunner as _;

        let docker_host = match std::env::var("DOCKER_HOST") {
            Ok(value) => value,
            Err(error) => {
                eprintln!(
                    "SKIP: DOCKER_HOST not set for PostgreSQL retry acknowledgement test: {error}"
                );
                return Ok(());
            }
        };
        let container = match Postgres::default().with_tag("18-alpine").start().await {
            Ok(container) => container,
            Err(error) => {
                eprintln!(
                    "SKIP: could not start PostgreSQL testcontainer using DOCKER_HOST={docker_host}: {error}"
                );
                return Ok(());
            }
        };
        let port = match container.get_host_port_ipv4(5432).await {
            Ok(port) => port,
            Err(error) => {
                eprintln!("SKIP: could not read PostgreSQL testcontainer port: {error}");
                return Ok(());
            }
        };
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect(&format!(
                "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
            ))
            .await?;
        apalis_postgres::PostgresStorage::setup(&pool).await?;
        crate::migrations::apply_post_setup_migrations(&pool).await?;
        sqlx::query(
            "INSERT INTO apalis.workers (id, worker_type, storage_name, layers) \
             VALUES ($1, 'adaptive', 'postgres', '')",
        )
        .bind(WORKER_ID)
        .execute(&pool)
        .await?;

        let task_id = Ulid::new();
        insert_postgres_running_job(&pool, task_id).await?;
        let before_retry: chrono::DateTime<chrono::Utc> =
            sqlx::query_scalar("SELECT NOW()").fetch_one(&pool).await?;
        let retry_parts = postgres_parts(task_id, 1, 5);
        let retry_result: Result<JobOutcome, BoxDynError> = Ok(JobOutcome::Retry {
            delay: Duration::from_millis(30_001),
        });

        PostgresSchedulerAck::new(pool.clone())
            .ack(&retry_result, &retry_parts)
            .await?;

        let after_retry: chrono::DateTime<chrono::Utc> =
            sqlx::query_scalar("SELECT NOW()").fetch_one(&pool).await?;
        let row: PostgresRetryRow = sqlx::query_as(
            "SELECT job, job_type, status, attempts, run_at, lock_by, lock_at, done_at, idempotency_key \
             FROM apalis.jobs WHERE id = $1",
        )
        .bind(task_id.to_string())
        .fetch_one(&pool)
        .await?;
        assert_eq!(row.0, PAYLOAD);
        assert_eq!(row.1, "adaptive");
        assert_eq!(row.2, "Pending");
        assert_eq!(row.3, 1);
        assert!(row.4 >= before_retry + chrono::Duration::seconds(31));
        assert!(row.4 <= after_retry + chrono::Duration::seconds(31));
        assert_eq!(row.5, None);
        assert_eq!(row.6, None);
        assert_eq!(row.7, None);
        assert_eq!(row.8, IDEMPOTENCY_KEY);
        assert_eq!(retry_parts.status.load(), Status::Pending);

        let duplicate = sqlx::query(
            "INSERT INTO apalis.jobs \
             (job, id, job_type, status, attempts, max_attempts, run_at, priority, metadata, idempotency_key) \
             VALUES ($1, $2, 'adaptive', 'Pending', 0, 5, NOW(), 0, '{}'::jsonb, $3) \
             ON CONFLICT(job_type, idempotency_key) DO NOTHING",
        )
        .bind(b"duplicate".as_slice())
        .bind(Ulid::new().to_string())
        .bind(IDEMPOTENCY_KEY)
        .execute(&pool)
        .await?;
        assert_eq!(duplicate.rows_affected(), 0);
        let row_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM apalis.jobs WHERE job_type = 'adaptive' AND idempotency_key = $1",
        )
        .bind(IDEMPOTENCY_KEY)
        .fetch_one(&pool)
        .await?;
        assert_eq!(row_count, 1);

        sqlx::query(
            "UPDATE apalis.jobs SET status = 'Running', lock_by = $1, lock_at = NOW() WHERE id = $2",
        )
        .bind(WORKER_ID)
        .bind(task_id.to_string())
        .execute(&pool)
        .await?;
        let dead_letter_parts = postgres_parts(task_id, 6, 5);
        let dead_letter_result: Result<JobOutcome, BoxDynError> = Ok(JobOutcome::DeadLetter);
        PostgresSchedulerAck::new(pool.clone())
            .ack(&dead_letter_result, &dead_letter_parts)
            .await?;

        let terminal: PostgresTerminalRow = sqlx::query_as(
            "SELECT status, attempts, lock_by, lock_at, done_at FROM apalis.jobs WHERE id = $1",
        )
        .bind(task_id.to_string())
        .fetch_one(&pool)
        .await?;
        assert_eq!(terminal.0, "Killed");
        assert_eq!(terminal.1, 6);
        assert_eq!(terminal.2, None);
        assert_eq!(terminal.3, None);
        assert!(terminal.4.is_some());
        assert_eq!(dead_letter_parts.status.load(), Status::Killed);

        pool.close().await;
        drop(container);
        Ok(())
    }

    #[cfg(feature = "postgres")]
    async fn insert_postgres_running_job(
        pool: &sqlx::PgPool,
        task_id: Ulid,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO apalis.jobs \
             (job, id, job_type, status, attempts, max_attempts, run_at, last_result, \
              lock_at, lock_by, done_at, priority, metadata, idempotency_key) \
             VALUES ($1, $2, 'adaptive', 'Running', 0, 5, NOW(), NULL, NOW(), $3, NULL, 0, '{}'::jsonb, $4)",
        )
        .bind(PAYLOAD)
        .bind(task_id.to_string())
        .bind(WORKER_ID)
        .bind(IDEMPOTENCY_KEY)
        .execute(pool)
        .await?;
        Ok(())
    }

    #[cfg(feature = "postgres")]
    fn postgres_parts(
        task_id: Ulid,
        attempt: usize,
        max_attempts: i32,
    ) -> Parts<apalis_postgres::PgContext, Ulid> {
        let task: Task<(), apalis_postgres::PgContext, Ulid> = TaskBuilder::new(())
            .with_task_id(TaskId::new(task_id))
            .with_ctx(
                apalis_postgres::PgContext::new()
                    .with_max_attempts(max_attempts)
                    .with_lock_by(Some(WORKER_ID.to_owned())),
            )
            .with_attempt(Attempt::new_with_value(attempt))
            .with_status(Status::Running)
            .build();
        task.parts
    }
}
