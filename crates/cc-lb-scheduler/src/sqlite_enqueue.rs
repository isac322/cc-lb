use std::time::{SystemTime, UNIX_EPOCH};

use apalis_core::backend::codec::Codec as _;
use apalis_core::backend::{Backend, TaskSink, TaskSinkError};
use apalis_core::task::Task;
use futures_util::stream::{self, BoxStream};
use futures_util::{Stream, StreamExt as _};
use sqlx::SqlitePool;

use crate::error::{Result, SchedulerError};
use crate::worker::{EntityJob, SingletonJob};

const DEFAULT_MAX_ATTEMPTS: i32 = 5;
const DEFAULT_PRIORITY: i32 = 0;

#[derive(Clone, Debug)]
pub(crate) struct SqliteSingletonCronStorage {
    pool: SqlitePool,
    queue: &'static str,
}

impl SqliteSingletonCronStorage {
    pub(crate) const fn new(pool: SqlitePool, queue: &'static str) -> Self {
        Self { pool, queue }
    }
}

impl Backend for SqliteSingletonCronStorage {
    type Args = SingletonJob;
    type IdType = ulid::Ulid;
    type Context = apalis_sqlite::SqliteContext;
    type Error = sqlx::Error;
    type Stream = BoxStream<
        'static,
        std::result::Result<Option<Task<Self::Args, Self::Context, Self::IdType>>, Self::Error>,
    >;
    type Beat = BoxStream<'static, std::result::Result<(), Self::Error>>;
    type Layer = ();

    fn heartbeat(&self, _worker: &apalis_core::worker::context::WorkerContext) -> Self::Beat {
        stream::empty().boxed()
    }

    fn middleware(&self) -> Self::Layer {}

    fn poll(self, _worker: &apalis_core::worker::context::WorkerContext) -> Self::Stream {
        stream::empty().boxed()
    }
}

impl TaskSink<SingletonJob> for SqliteSingletonCronStorage {
    async fn push(
        &mut self,
        job: SingletonJob,
    ) -> std::result::Result<(), TaskSinkError<Self::Error>> {
        let payload = encode_singleton_job(&job)?;
        insert_singleton_job(
            &self.pool,
            self.queue,
            payload,
            ulid::Ulid::new().to_string(),
            DEFAULT_MAX_ATTEMPTS,
            now_unix_secs_sqlx()?,
            DEFAULT_PRIORITY,
            "{}".to_owned(),
            singleton_idempotency_key(&job),
        )
        .await
        .map_err(TaskSinkError::PushError)
    }

    async fn push_bulk(
        &mut self,
        jobs: Vec<SingletonJob>,
    ) -> std::result::Result<(), TaskSinkError<Self::Error>> {
        for job in jobs {
            self.push(job).await?;
        }
        Ok(())
    }

    async fn push_stream(
        &mut self,
        mut jobs: impl Stream<Item = SingletonJob> + Unpin + Send,
    ) -> std::result::Result<(), TaskSinkError<Self::Error>> {
        while let Some(job) = jobs.next().await {
            self.push(job).await?;
        }
        Ok(())
    }

    async fn push_task(
        &mut self,
        task: Task<SingletonJob, Self::Context, Self::IdType>,
    ) -> std::result::Result<(), TaskSinkError<Self::Error>> {
        let payload = encode_singleton_job(&task.args)?;
        let idempotency_key = task
            .parts
            .idempotency_key
            .clone()
            .unwrap_or_else(|| singleton_idempotency_key(&task.args));
        let metadata = serde_json::to_string(task.parts.ctx.meta())
            .map_err(|error| TaskSinkError::CodecError(error.into()))?;
        let id = task
            .parts
            .task_id
            .map(|task_id| task_id.to_string())
            .unwrap_or_else(|| ulid::Ulid::new().to_string());
        let run_at = i64::try_from(task.parts.run_at).map_err(|_| {
            TaskSinkError::PushError(sqlx::Error::Protocol("run_at exceeds i64::MAX".to_owned()))
        })?;
        insert_singleton_job(
            &self.pool,
            self.queue,
            payload,
            id,
            task.parts.ctx.max_attempts(),
            run_at,
            task.parts.ctx.priority(),
            metadata,
            idempotency_key,
        )
        .await
        .map_err(TaskSinkError::PushError)
    }

    async fn push_all(
        &mut self,
        mut tasks: impl Stream<Item = Task<SingletonJob, Self::Context, Self::IdType>> + Unpin + Send,
    ) -> std::result::Result<(), TaskSinkError<Self::Error>> {
        while let Some(task) = tasks.next().await {
            self.push_task(task).await?;
        }
        Ok(())
    }
}

pub(crate) async fn push_entity_job(pool: &SqlitePool, queue: &str, job: EntityJob) -> Result<()> {
    let payload = apalis_codec::json::JsonCodec::<Vec<u8>>::encode(&job)
        .map_err(|error| SchedulerError::Job(format!("encode entity job: {error}")))?;
    let idempotency_key = entity_idempotency_key(&job);
    sqlx::query(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, last_result, lock_at, lock_by, done_at, priority, metadata, idempotency_key) \
         VALUES (?1, ?2, ?3, 'Pending', 0, ?4, ?5, NULL, NULL, NULL, NULL, 0, ?6, ?7) \
         ON CONFLICT(job_type, idempotency_key) WHERE status IN ('Pending','Running','Queued') DO NOTHING",
    )
    .bind(payload)
    .bind(ulid::Ulid::new().to_string())
    .bind(queue)
    .bind(DEFAULT_MAX_ATTEMPTS)
    .bind(now_unix_secs()?)
    .bind("{}")
    .bind(idempotency_key)
    .execute(pool)
    .await?;
    Ok(())
}

fn encode_singleton_job(
    job: &SingletonJob,
) -> std::result::Result<Vec<u8>, TaskSinkError<sqlx::Error>> {
    apalis_codec::json::JsonCodec::<Vec<u8>>::encode(job)
        .map_err(|error| TaskSinkError::CodecError(error.into()))
}

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
         VALUES (?1, ?2, ?3, 'Pending', 0, ?4, ?5, NULL, NULL, NULL, NULL, ?6, ?7, ?8) \
         ON CONFLICT(job_type, idempotency_key) WHERE status IN ('Pending','Running','Queued') DO NOTHING",
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

fn singleton_idempotency_key(job: &SingletonJob) -> String {
    format!("singleton:{}", job.kind())
}

fn entity_idempotency_key(job: &EntityJob) -> String {
    match job {
        EntityJob::Warmup(job) => format!("entity:warmup:{}", job.upstream_id),
        EntityJob::OAuthRefresh(job) => job.idempotency_key(),
        EntityJob::OAuthUsagePoll(job) => job.idempotency_key(),
        EntityJob::AnthropicCompatRefresh(job) => {
            format!("entity:anthropic_compat_refresh:{}", job.key)
        }
        EntityJob::MetadataRefresh(job) => job.idempotency_key(),
    }
}

fn now_unix_secs() -> Result<i64> {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| SchedulerError::Job(format!("system clock before unix epoch: {error}")))?
        .as_secs();
    i64::try_from(seconds)
        .map_err(|_| SchedulerError::Job("current time exceeds i64::MAX".to_owned()))
}

fn now_unix_secs_sqlx() -> std::result::Result<i64, sqlx::Error> {
    now_unix_secs().map_err(|error| sqlx::Error::Protocol(error.to_string()))
}
