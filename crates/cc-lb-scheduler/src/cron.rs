//! Cron job scheduling and tick generation.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use apalis_core::backend::pipe::{Pipe, PipeExt};
use apalis_core::backend::{TaskSink, TaskSinkError};
use apalis_core::task::builder::TaskBuilder;
use apalis_cron::{CronStream, Schedule};
use chrono::Utc;
use futures_util::StreamExt;
use thiserror::Error;
use tokio::sync::Mutex;

use crate::leader_election::{LeaderElection, LeaderError};

pub type LeaderRunFuture<'a> = Pin<Box<dyn Future<Output = Result<(), LeaderError>> + Send + 'a>>;

pub trait LeaderRunner {
    fn run<'a, F, Fut>(&'a self, work: F) -> LeaderRunFuture<'a>
    where
        F: FnOnce() -> Fut + Send + 'a,
        Fut: Future<Output = ()> + Send + 'a;
}

impl LeaderRunner for LeaderElection {
    fn run<'a, F, Fut>(&'a self, work: F) -> LeaderRunFuture<'a>
    where
        F: FnOnce() -> Fut + Send + 'a,
        Fut: Future<Output = ()> + Send + 'a,
    {
        Box::pin(async move { LeaderElection::run(self, work).await })
    }
}

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

impl<Job: 'static> IntoJobFactory<Job> for Arc<dyn Fn(u64) -> Job + Send + Sync + 'static> {
    fn call(&self, tick_unix_secs: u64) -> Job {
        (self)(tick_unix_secs)
    }
}

impl<Job: 'static> IntoJobFactory<Job> for fn(u64) -> Job {
    fn call(&self, tick_unix_secs: u64) -> Job {
        (self)(tick_unix_secs)
    }
}

#[derive(Debug, Error)]
pub enum CronError {
    #[error(transparent)]
    Leader(#[from] LeaderError),
    #[error("cron stream failed: {message}")]
    Stream { message: String },
    #[error("cron enqueue failed: {message}")]
    Enqueue { message: String },
}

pub struct WorkerBuilder<
    Job,
    S,
    Storage,
    JobFactory = Arc<dyn Fn(u64) -> Job + Send + Sync + 'static>,
> {
    schedule: S,
    storage: Storage,
    job_factory: JobFactory,
    max_ticks: Option<usize>,
    _job: std::marker::PhantomData<fn() -> Job>,
}

impl<Job, S, Storage>
    WorkerBuilder<Job, S, Storage, Arc<dyn Fn(u64) -> Job + Send + Sync + 'static>>
{
    pub fn singleton_queue(
        _queue: impl Into<String>,
        schedule: S,
        storage: Storage,
        job: Job,
    ) -> Self
    where
        Job: Clone + Send + Sync + 'static,
    {
        Self {
            schedule,
            storage,
            job_factory: Arc::new(move |_| job.clone()),
            max_ticks: None,
            _job: std::marker::PhantomData,
        }
    }
}

impl<Job, S, Storage, JobFactory> WorkerBuilder<Job, S, Storage, JobFactory> {
    pub fn singleton_queue_factory(
        _queue: impl Into<String>,
        schedule: S,
        storage: Storage,
        job_factory: JobFactory,
    ) -> Self {
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
    pub async fn run(self, leader: &LeaderElection) -> Result<(), CronError> {
        self.run_with(leader).await
    }

    pub async fn run_with<Leader>(self, leader: &Leader) -> Result<(), CronError>
    where
        Leader: LeaderRunner + Sync,
    {
        let outcome = Arc::new(Mutex::new(None));
        let outcome_writer = Arc::clone(&outcome);
        leader
            .run(move || async move {
                let result = self.run_worker().await;
                *outcome_writer.lock().await = Some(result);
            })
            .await?;

        let mut outcome = outcome.lock().await;
        outcome.take().unwrap_or(Ok(()))
    }

    async fn run_worker(mut self) -> Result<(), CronError> {
        let mut stream = CronStream::new(self.schedule);
        let mut tick_count = 0_usize;
        while self
            .max_ticks
            .is_none_or(|max_ticks| tick_count < max_ticks)
        {
            let Some(tick) = stream.next().await else {
                break;
            };
            let tick = tick.map_err(|error| CronError::Stream {
                message: error.to_string(),
            })?;
            let tick_secs =
                u64::try_from(tick.get_timestamp().timestamp()).map_err(|_| CronError::Stream {
                    message: "cron tick timestamp is before the unix epoch".to_owned(),
                })?;
            let job = self.job_factory.call(tick_secs);
            let kind = job.singleton_kind();
            let task = TaskBuilder::<Job, Storage::Context, Storage::IdType>::new(job)
                .run_at_timestamp(tick_secs)
                .with_idempotency_key(format!("singleton:{kind}:{tick_secs}"))
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
