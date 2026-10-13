//! Cron job scheduling and tick generation.
//!
//! Each replica polls its own [`Schedule`] and pushes ticks to the shared
//! Apalis storage keyed by `cron:{kind}:{tick_secs}`. The storage's unique
//! index on `(job_type, idempotency_key)` collapses simultaneous pushes from
//! every replica to exactly one queued job per tick, so no external leader
//! election is required. See `docs/scheduler.md` section 5 for details.

use apalis_core::backend::pipe::{Pipe, PipeExt};
use apalis_core::backend::{TaskSink, TaskSinkError};
use apalis_core::task::builder::TaskBuilder;
use apalis_cron::Schedule;
use chrono::Utc;
use thiserror::Error;

pub trait CronStreamPipeToStorageExt<Storage, Args, Ctx>: Sized {
    fn pipe_to_storage(self, storage: Storage) -> Pipe<Self, Storage, Args, Ctx>;
}

impl<Stream, Storage, Args, Ctx> CronStreamPipeToStorageExt<Storage, Args, Ctx> for Stream
where
    Stream: PipeExt<Storage, Args, Ctx>,
{
    fn pipe_to_storage(self, storage: Storage) -> Pipe<Self, Storage, Args, Ctx> {
        self.pipe_to(storage)
    }
}

pub trait SingletonCronJob {
    fn singleton_kind(&self) -> &'static str;
}

pub trait IntoJobFactory<Job>: Clone + Send + 'static {
    fn call(&self, tick_unix_secs: u64) -> Job;
}

impl<Job: 'static> IntoJobFactory<Job> for fn(u64) -> Job {
    fn call(&self, tick_unix_secs: u64) -> Job {
        (self)(tick_unix_secs)
    }
}

#[derive(Debug, Error)]
pub enum CronError {
    #[error("cron stream failed: {message}")]
    Stream { message: String },
    #[error("cron enqueue failed: {message}")]
    Enqueue { message: String },
}

pub struct WorkerBuilder<Job, S, Storage, JobFactory> {
    schedule: S,
    storage: Storage,
    job_factory: JobFactory,
    max_ticks: Option<usize>,
    _job: std::marker::PhantomData<fn() -> Job>,
}

impl<Job, S, Storage, JobFactory> WorkerBuilder<Job, S, Storage, JobFactory> {
    pub fn singleton_queue_factory(schedule: S, storage: Storage, job_factory: JobFactory) -> Self {
        Self {
            schedule,
            storage,
            job_factory,
            max_ticks: None,
            _job: std::marker::PhantomData,
        }
    }

    #[must_use]
    pub const fn max_ticks(mut self, max_ticks: usize) -> Self {
        self.max_ticks = Some(max_ticks);
        self
    }
}

impl<Job, S, Storage, JobFactory> WorkerBuilder<Job, S, Storage, JobFactory>
where
    Job: SingletonCronJob + Send + 'static,
    JobFactory: IntoJobFactory<Job>,
    S: Schedule<Utc> + Unpin + Send + 'static,
    Storage: TaskSink<Job, Error = sqlx::Error> + Send + 'static,
{
    /// Runs the cron producer loop on every replica. Duplicate pushes from
    /// sibling replicas are absorbed by the storage-level unique index on
    /// `(job_type, idempotency_key)`, so exactly one job per tick makes it
    /// into the queue.
    ///
    /// The schedule is driven directly rather than through
    /// `apalis_cron::CronStream`: `CronStream` samples `Utc::now()` after
    /// `next_tick()` returns and converts `tick - now` with
    /// `chrono::Duration::to_std`, which fails whenever the tick has already
    /// elapsed (a stalled poll, clock drift, or a schedule that lags the wall
    /// clock) — and the error repeats forever because the stream never
    /// advances. Here a negative wait saturates to zero so an elapsed tick
    /// fires immediately instead of killing the producer.
    pub async fn run(mut self) -> Result<(), CronError> {
        let mut tick_count = 0_usize;
        while self
            .max_ticks
            .is_none_or(|max_ticks| tick_count < max_ticks)
        {
            let Some(tick) = self.schedule.next_tick(&Utc) else {
                break;
            };
            let wait = tick
                .signed_duration_since(Utc::now())
                .to_std()
                .unwrap_or_default();
            if !wait.is_zero() {
                tokio::time::sleep(wait).await;
            }
            let tick_secs = u64::try_from(tick.timestamp()).map_err(|_| CronError::Stream {
                message: "cron tick timestamp is before the unix epoch".to_owned(),
            })?;
            let job = self.job_factory.call(tick_secs);
            let kind = job.singleton_kind();
            let task = TaskBuilder::<Job, Storage::Context, Storage::IdType>::new(job)
                .run_at_timestamp(tick_secs)
                .with_idempotency_key(format!("cron:{kind}:{tick_secs}"))
                .build();
            match self.storage.push_task(task).await {
                Ok(()) => {}
                Err(TaskSinkError::PushError(sqlx::Error::Database(db_err)))
                    if db_err.is_unique_violation() => {}
                Err(error) => {
                    return Err(CronError::Enqueue {
                        message: error.to_string(),
                    });
                }
            }
            tick_count += 1;
        }
        Ok(())
    }
}
