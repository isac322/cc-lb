use std::pin::Pin;
use std::task::{Context, Poll};

use apalis_core::backend::codec::Codec as _;
use apalis_core::backend::{Backend, TaskSink, TaskSinkError};
use apalis_core::task::Task;
use cc_lb_clock::{Clock, ClockHandle, unix_secs};
use futures_util::{Stream, StreamExt as _};
use sqlx::SqlitePool;

use crate::error::{Result, SchedulerError};
use crate::worker::{AdaptiveJob, CronJob};

const DEFAULT_MAX_ATTEMPTS: i32 = 5;
const DEFAULT_PRIORITY: i32 = 0;

#[derive(Clone)]
pub(crate) struct SqliteSingletonCronStorage {
    pool: SqlitePool,
    queue: &'static str,
    clock: ClockHandle,
}

impl SqliteSingletonCronStorage {
    pub(crate) fn new(pool: SqlitePool, queue: &'static str, clock: ClockHandle) -> Self {
        Self { pool, queue, clock }
    }
}

impl Backend for SqliteSingletonCronStorage {
    type Task = Task<CronJob>;
    type Error = sqlx::Error;

    fn poll_ready(
        &mut self,
        _cx: &mut Context<'_>,
        _worker: &apalis_core::worker::context::WorkerContext,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_next(
        &mut self,
        _cx: &mut Context<'_>,
        _worker: &apalis_core::worker::context::WorkerContext,
    ) -> Poll<Option<std::result::Result<Self::Task, Self::Error>>> {
        Poll::Ready(None)
    }

    fn poll_close(
        &mut self,
        _cx: &mut Context<'_>,
        _worker: &apalis_core::worker::context::WorkerContext,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

impl TaskSink<CronJob, ()> for SqliteSingletonCronStorage {
    fn start_send(
        self: Pin<&mut Self>,
        _item: Task<CronJob>,
    ) -> std::result::Result<(), TaskSinkError<Self::Error>> {
        Ok(())
    }

    fn poll_ready(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), TaskSinkError<Self::Error>>> {
        Poll::Ready(Ok(()))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), TaskSinkError<Self::Error>>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), TaskSinkError<Self::Error>>> {
        Poll::Ready(Ok(()))
    }

    async fn push(&mut self, job: CronJob) -> std::result::Result<(), TaskSinkError<Self::Error>> {
        let payload = encode_singleton_job(&job)?;
        insert_singleton_job(
            &self.pool,
            self.queue,
            payload,
            ulid::Ulid::generate().to_string(),
            DEFAULT_MAX_ATTEMPTS,
            now_unix_secs_sqlx(&*self.clock)?,
            DEFAULT_PRIORITY,
            "{}".to_owned(),
            singleton_idempotency_key(&job),
        )
        .await
        .map_err(TaskSinkError::PushError)
    }

    async fn push_bulk(
        &mut self,
        jobs: Vec<CronJob>,
    ) -> std::result::Result<(), TaskSinkError<Self::Error>> {
        for job in jobs {
            self.push(job).await?;
        }
        Ok(())
    }

    async fn push_stream(
        &mut self,
        mut jobs: impl Stream<Item = CronJob> + Unpin + Send,
    ) -> std::result::Result<(), TaskSinkError<Self::Error>> {
        while let Some(job) = jobs.next().await {
            self.push(job).await?;
        }
        Ok(())
    }

    async fn push_task(
        &mut self,
        task: Task<CronJob>,
    ) -> std::result::Result<(), TaskSinkError<Self::Error>> {
        let payload = encode_singleton_job(&task.args)?;
        let run_at_u64 = task.run_at().unwrap_or_else(|| unix_secs(self.clock.now()));
        let idempotency_key = task
            .idempotency_key()
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| singleton_tick_idempotency_key(&task.args, run_at_u64));
        let metadata = serde_json::to_string(task.metadata())
            .map_err(|error| TaskSinkError::CodecError(error.into()))?;
        let id = task
            .task_id()
            .map(ToString::to_string)
            .unwrap_or_else(|| ulid::Ulid::generate().to_string());
        let run_at = i64::try_from(run_at_u64).map_err(|_| {
            TaskSinkError::PushError(sqlx::Error::Protocol("run_at exceeds i64::MAX".to_owned()))
        })?;
        let max_attempts = task
            .max_attempts()
            .and_then(|v| i32::try_from(v).ok())
            .unwrap_or(DEFAULT_MAX_ATTEMPTS);
        let priority = task
            .priority()
            .and_then(|v| i32::try_from(v).ok())
            .unwrap_or(DEFAULT_PRIORITY);
        insert_singleton_job(
            &self.pool,
            self.queue,
            payload,
            id,
            max_attempts,
            run_at,
            priority,
            metadata,
            idempotency_key,
        )
        .await
        .map_err(TaskSinkError::PushError)
    }

    async fn push_all(
        &mut self,
        mut tasks: impl Stream<Item = Task<CronJob>> + Unpin + Send,
    ) -> std::result::Result<(), TaskSinkError<Self::Error>> {
        while let Some(task) = tasks.next().await {
            self.push_task(task).await?;
        }
        Ok(())
    }
}

pub(crate) async fn push_entity_job(
    pool: &SqlitePool,
    queue: &str,
    job: AdaptiveJob,
    clock: &dyn Clock,
) -> Result<()> {
    let payload = apalis_codec::json::JsonCodec::<Vec<u8>>::default()
        .encode(&job)
        .map_err(|error| SchedulerError::Job(format!("encode entity job: {error}")))?;
    let run_at = now_unix_secs(clock)?;
    let idempotency_key = entity_idempotency_key(&job, run_at)?;
    sqlx::query(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, last_result, lock_at, lock_by, done_at, priority, metadata, idempotency_key) \
         VALUES (?1, ?2, ?3, 'Pending', 0, ?4, ?5, NULL, NULL, NULL, NULL, 0, ?6, ?7) \
         ON CONFLICT(job_type, idempotency_key) DO NOTHING",
    )
    .bind(payload)
    .bind(ulid::Ulid::generate().to_string())
    .bind(queue)
    .bind(DEFAULT_MAX_ATTEMPTS)
    .bind(run_at)
    .bind("{}")
    .bind(idempotency_key)
    .execute(pool)
    .await?;
    Ok(())
}

fn encode_singleton_job(job: &CronJob) -> std::result::Result<Vec<u8>, TaskSinkError<sqlx::Error>> {
    apalis_codec::json::JsonCodec::<Vec<u8>>::default()
        .encode(job)
        .map_err(|error| TaskSinkError::CodecError(error.into()))
}

#[allow(clippy::too_many_arguments)]
async fn insert_singleton_job(
    pool: &SqlitePool,
    queue: &str,
    payload: Vec<u8>,
    id: String,
    max_attempts: i32,
    run_at: i64,
    priority: i32,
    metadata: String,
    idempotency_key: String,
) -> std::result::Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, last_result, lock_at, lock_by, done_at, priority, metadata, idempotency_key) \
         VALUES (?1, ?2, ?3, 'Pending', 0, ?4, ?5, NULL, NULL, NULL, NULL, ?6, ?7, ?8)",
    )
    .bind(payload)
    .bind(id)
    .bind(queue)
    .bind(max_attempts)
    .bind(run_at)
    .bind(priority)
    .bind(metadata)
    .bind(idempotency_key)
    .execute(pool)
    .await?;
    Ok(())
}

fn singleton_idempotency_key(job: &CronJob) -> String {
    format!("cron:{}", job.kind())
}

fn singleton_tick_idempotency_key(job: &CronJob, tick_unix_secs: u64) -> String {
    format!("cron:{}:{tick_unix_secs}", job.kind())
}

fn entity_idempotency_key(job: &AdaptiveJob, run_at_unix_secs: i64) -> Result<String> {
    let run_at_unix_secs = u64::try_from(run_at_unix_secs)
        .map_err(|_| SchedulerError::Job("run_at_unix_secs is negative".to_owned()))?;
    match job {
        AdaptiveJob::Warmup(job) => Ok(job.idempotency_key(job.cycle_key)),
        AdaptiveJob::OAuthRefresh(job) => Ok(job.idempotency_key(run_at_unix_secs)),
        AdaptiveJob::MetadataRefresh(job) => Ok(job.idempotency_key()),
        AdaptiveJob::CacheKeepalive(job) => Ok(job.idempotency_key()),
    }
}

fn now_unix_secs(clock: &dyn Clock) -> Result<i64> {
    let seconds = unix_secs(clock.now());
    i64::try_from(seconds)
        .map_err(|_| SchedulerError::Job("current time exceeds i64::MAX".to_owned()))
}

fn now_unix_secs_sqlx(clock: &dyn Clock) -> std::result::Result<i64, sqlx::Error> {
    now_unix_secs(clock).map_err(|error| sqlx::Error::Protocol(error.to_string()))
}
