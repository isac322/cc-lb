//! Cron job scheduling and tick generation.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use apalis_core::backend::TaskSink;
use apalis_core::backend::pipe::{Pipe, PipeExt};
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

#[derive(Debug, Error)]
pub enum CronError {
    #[error(transparent)]
    Leader(#[from] LeaderError),
    #[error("cron stream failed: {message}")]
    Stream { message: String },
    #[error("cron enqueue failed: {message}")]
    Enqueue { message: String },
}

#[derive(Debug)]
pub struct WorkerBuilder<Job, S, Storage> {
    schedule: S,
    storage: Storage,
    job: Job,
    max_ticks: Option<usize>,
}

impl<Job, S, Storage> WorkerBuilder<Job, S, Storage> {
    pub fn singleton_queue(
        _queue: impl Into<String>,
        schedule: S,
        storage: Storage,
        job: Job,
    ) -> Self {
        Self {
            schedule,
            storage,
            job,
            max_ticks: None,
        }
    }

    #[must_use]
    pub const fn max_ticks(mut self, max_ticks: usize) -> Self {
        self.max_ticks = Some(max_ticks);
        self
    }
}

impl<Job, S, Storage> WorkerBuilder<Job, S, Storage>
where
    Job: Clone + Send + 'static,
    S: Schedule<Utc> + Unpin + Send + 'static,
    Storage: TaskSink<Job> + Send + 'static,
    Storage::Error: std::error::Error + Send + Sync + 'static,
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
            tick.map_err(|error| CronError::Stream {
                message: error.to_string(),
            })?;
            self.storage
                .push(self.job.clone())
                .await
                .map_err(|error| CronError::Enqueue {
                    message: error.to_string(),
                })?;
            tick_count += 1;
        }
        Ok(())
    }
}
