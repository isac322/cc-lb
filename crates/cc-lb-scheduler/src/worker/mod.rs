//! Worker pool for executing entity jobs.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

#[cfg(feature = "postgres")]
use apalis::prelude::TaskSink;
use cc_lb_config::{Config, SchedulerConfig};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::SchedulerError;
use crate::leader_election::LeaderElection;
use crate::retry::JobOutcome;

mod backend_api;
mod cron;
mod dispatch;
mod jobs;
mod layers;

pub use backend_api::{Filter, SchedulerPushTask, SchedulerTaskRow, TaskStatus};
pub use jobs::{AdaptiveJob, CronJob};
pub use layers::{AdaptiveWorker, CronWorker, build_cron_worker};

pub const ADAPTIVE_QUEUE: &str = "adaptive";
pub const CRON_QUEUE: &str = "cron";

pub type AdaptiveDispatchFuture =
    Pin<Box<dyn Future<Output = Result<JobOutcome, SchedulerError>> + Send>>;
pub type CronDispatchFuture =
    Pin<Box<dyn Future<Output = Result<JobOutcome, SchedulerError>> + Send>>;

pub type AdaptiveDispatchFn = Arc<dyn Fn(AdaptiveJob) -> AdaptiveDispatchFuture + Send + Sync>;
pub type CronDispatchFn = Arc<dyn Fn(CronJob) -> CronDispatchFuture + Send + Sync>;

#[derive(Clone)]
pub struct SchedulerCtx {
    pub config: SchedulerConfig,
    pub adaptive_dispatch: AdaptiveDispatchFn,
    pub cron_dispatch: CronDispatchFn,
}

impl SchedulerCtx {
    pub fn new(
        config: SchedulerConfig,
        adaptive_dispatch: AdaptiveDispatchFn,
        cron_dispatch: CronDispatchFn,
    ) -> Self {
        Self {
            config,
            adaptive_dispatch,
            cron_dispatch,
        }
    }
}

impl Default for SchedulerCtx {
    fn default() -> Self {
        Self {
            config: SchedulerConfig::default(),
            adaptive_dispatch: Arc::new(|_job| Box::pin(async { Ok(JobOutcome::Done) })),
            cron_dispatch: Arc::new(|_job| Box::pin(async { Ok(JobOutcome::Done) })),
        }
    }
}

#[derive(Clone, Debug)]
pub enum SchedulerBackend {
    #[cfg(feature = "sqlite")]
    Sqlite(SqliteSchedulerStorage),
    #[cfg(feature = "postgres")]
    Postgres(PostgresSchedulerStorage),
}

#[cfg(feature = "sqlite")]
pub type SqliteApalisStorage = apalis_sqlite::SqliteStorage<
    AdaptiveJob,
    apalis_codec::json::JsonCodec<apalis_sqlite::CompactType>,
    apalis_sqlite::fetcher::SqliteFetcher,
>;

#[cfg(feature = "sqlite")]
#[derive(Clone, Debug)]
pub struct SqliteSchedulerStorage {
    pub pool: sqlx::SqlitePool,
    pub storage: SqliteApalisStorage,
}

#[cfg(feature = "postgres")]
pub type PostgresApalisStorage = apalis_postgres::PostgresStorage<AdaptiveJob>;

#[cfg(feature = "postgres")]
#[derive(Clone)]
pub struct PostgresSchedulerStorage {
    pub pool: sqlx::PgPool,
    pub storage: PostgresApalisStorage,
}

impl SchedulerBackend {
    pub async fn spawn(
        &self,
        config: Config,
        ctx: SchedulerCtx,
        leader: Arc<LeaderElection>,
        cancel: CancellationToken,
    ) -> Result<Vec<JoinHandle<()>>, SchedulerError> {
        let mut handles = self.spawn_consumers(ctx, cancel.clone())?;
        handles.push(cron::spawn_cron_producer(
            self.clone(),
            config,
            leader,
            cancel,
        ));
        Ok(handles)
    }

    pub async fn push_job(&self, job: AdaptiveJob) -> Result<(), SchedulerError> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(sqlite) => {
                let queue = sqlite.storage.config().queue().as_ref().to_owned();
                crate::sqlite_enqueue::push_entity_job(&sqlite.pool, &queue, job).await?;
            }
            #[cfg(feature = "postgres")]
            Self::Postgres(postgres) => postgres
                .storage
                .clone()
                .push(job)
                .await
                .map_err(|error| SchedulerError::Job(error.to_string()))?,
        }
        Ok(())
    }

    fn spawn_consumers(
        &self,
        ctx: SchedulerCtx,
        cancel: CancellationToken,
    ) -> Result<Vec<JoinHandle<()>>, SchedulerError> {
        let entity_worker = build_adaptive_worker(self, ctx.clone())?;
        let singleton_worker = build_cron_worker(self, ctx)?;
        let entity_cancel = cancel.clone();
        let singleton_cancel = cancel;
        Ok(vec![
            tokio::spawn(async move {
                if let Err(error) = entity_worker.run_until_cancelled(entity_cancel).await {
                    tracing::error!(error = %error, "scheduler entity worker exited with error");
                }
            }),
            tokio::spawn(async move {
                if let Err(error) = singleton_worker.run_until_cancelled(singleton_cancel).await {
                    tracing::error!(error = %error, "scheduler singleton worker exited with error");
                }
            }),
        ])
    }
}

#[cfg(feature = "postgres")]
impl std::fmt::Debug for PostgresSchedulerStorage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PostgresSchedulerStorage")
            .field("pool", &self.pool)
            .finish_non_exhaustive()
    }
}

pub fn build_adaptive_worker(
    backend: &SchedulerBackend,
    ctx: SchedulerCtx,
) -> Result<AdaptiveWorker, SchedulerError> {
    layers::build_backend_adaptive_worker(backend, ctx)
}
