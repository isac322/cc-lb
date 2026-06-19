//! Worker pool for executing entity jobs.

use std::sync::Arc;

#[cfg(feature = "postgres")]
use apalis::prelude::TaskSink;
use cc_lb_config::{Config, SchedulerConfig};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::SchedulerError;
use crate::leader_election::LeaderElection;

mod cron;
mod dispatch;
mod jobs;
mod layers;
mod registry;

pub use jobs::{EntityJob, SingletonJob};
pub use layers::{EntityWorker, SingletonWorker, build_singleton_worker};
pub use registry::{EntityHandlerRegistry, EntityJobKind};

pub const ENTITY_QUEUE: &str = "entity";
pub const SINGLETON_QUEUE: &str = "singleton";

#[derive(Clone, Debug, Default)]
pub struct SchedulerCtx {
    pub config: SchedulerConfig,
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
    EntityJob,
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
pub type PostgresApalisStorage = apalis_postgres::PostgresStorage<EntityJob>;

#[cfg(feature = "postgres")]
#[derive(Clone)]
pub struct PostgresSchedulerStorage {
    pub pool: sqlx::PgPool,
    pub storage: PostgresApalisStorage,
}

impl SchedulerBackend {
    pub fn spawn(
        &self,
        config: Config,
        leader: Arc<LeaderElection>,
        cancel: CancellationToken,
    ) -> Result<Vec<JoinHandle<()>>, SchedulerError> {
        let mut handles = self.spawn_consumers(config.scheduler.clone(), cancel.clone())?;
        handles.push(cron::spawn_cron_producer(
            self.clone(),
            config,
            leader,
            cancel,
        ));
        Ok(handles)
    }

    pub async fn push_job(&self, job: EntityJob) -> Result<(), SchedulerError> {
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
        config: SchedulerConfig,
        cancel: CancellationToken,
    ) -> Result<Vec<JoinHandle<()>>, SchedulerError> {
        let entity_worker = build_entity_worker(
            self,
            SchedulerCtx {
                config: config.clone(),
            },
        )?;
        let singleton_worker = build_singleton_worker(self, SchedulerCtx { config })?;
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

pub fn build_entity_worker(
    backend: &SchedulerBackend,
    ctx: SchedulerCtx,
) -> Result<EntityWorker, SchedulerError> {
    let _registry = EntityHandlerRegistry::register_all_entity_handlers()?;
    layers::build_backend_entity_worker(backend, ctx)
}
