use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use apalis::layers::TimeoutLayer;
use apalis::layers::catch_panic::CatchPanicLayer;
use apalis::layers::limit::ConcurrencyLimitLayer;
use apalis::layers::prometheus::PrometheusLayer;
use apalis::prelude::{WorkerBuilder as ApalisWorkerBuilder, WorkerError};
use tokio_util::sync::CancellationToken;

use crate::error::SchedulerError;
use crate::middleware::TraceparentLayer;
use crate::retry::RetryClass;

use super::super::dispatch::{SingletonHandlerFn, singleton_job_handler};
use super::super::{CRON_QUEUE, CronJob, SchedulerBackend, SchedulerCtx};

const SINGLETON_TIMEOUT: Duration = Duration::from_secs(60);

#[cfg(feature = "sqlite")]
type SqliteSingletonStorage = apalis_sqlite::SqliteStorage<
    CronJob,
    apalis_codec::json::JsonCodec<apalis_sqlite::CompactType>,
    apalis_sqlite::fetcher::SqliteFetcher,
>;

type CronWorkerFuture = Pin<Box<dyn Future<Output = Result<(), WorkerError>> + Send>>;

pub struct CronWorker {
    run: Box<dyn FnOnce(CancellationToken) -> CronWorkerFuture + Send>,
}

impl CronWorker {
    pub async fn run_until_cancelled(self, cancel: CancellationToken) -> Result<(), WorkerError> {
        (self.run)(cancel).await
    }
}

pub fn build_cron_worker(
    backend: &SchedulerBackend,
    ctx: SchedulerCtx,
) -> Result<CronWorker, SchedulerError> {
    build_cron_worker_named(backend, ctx, CRON_QUEUE.to_owned())
}

pub fn build_cron_worker_named(
    backend: &SchedulerBackend,
    ctx: SchedulerCtx,
    worker_name: String,
) -> Result<CronWorker, SchedulerError> {
    match backend {
        #[cfg(feature = "sqlite")]
        SchedulerBackend::Sqlite(sqlite) => Ok(build_sqlite_singleton_worker(
            apalis_sqlite::SqliteStorage::<CronJob, (), ()>::new_in_queue(&sqlite.pool, CRON_QUEUE),
            ctx,
            worker_name,
        )),
        #[cfg(feature = "postgres")]
        SchedulerBackend::Postgres(postgres) => Ok(build_postgres_singleton_worker(
            apalis_postgres::PostgresStorage::<CronJob>::new_with_config(
                &postgres.pool,
                &apalis_postgres::Config::new(CRON_QUEUE),
            ),
            ctx,
            worker_name,
        )),
    }
}

#[cfg(feature = "sqlite")]
fn build_sqlite_singleton_worker(
    storage: SqliteSingletonStorage,
    ctx: SchedulerCtx,
    worker_name: String,
) -> CronWorker {
    let concurrency = ctx.config.singleton_concurrency;
    CronWorker {
        run: Box::new(move |cancel| {
            Box::pin(async move {
                let worker = ApalisWorkerBuilder::new(worker_name)
                    .backend(storage)
                    .data(ctx)
                    .layer(TraceparentLayer::new().with_scheduler_metrics())
                    .layer(RetryClass::Maintenance.layer())
                    .layer(TimeoutLayer::new(SINGLETON_TIMEOUT))
                    .layer(CatchPanicLayer::new())
                    .layer(PrometheusLayer::default())
                    .layer(ConcurrencyLimitLayer::new(concurrency))
                    .build(singleton_job_handler as SingletonHandlerFn);
                worker
                    .run_until(async move {
                        cancel.cancelled().await;
                        Ok::<(), WorkerError>(())
                    })
                    .await
            })
        }),
    }
}

#[cfg(feature = "postgres")]
fn build_postgres_singleton_worker(
    storage: apalis_postgres::PostgresStorage<CronJob>,
    ctx: SchedulerCtx,
    worker_name: String,
) -> CronWorker {
    let concurrency = ctx.config.singleton_concurrency;
    CronWorker {
        run: Box::new(move |cancel| {
            Box::pin(async move {
                let worker = ApalisWorkerBuilder::new(worker_name)
                    .backend(storage)
                    .data(ctx)
                    .layer(TraceparentLayer::new().with_scheduler_metrics())
                    .layer(RetryClass::Maintenance.layer())
                    .layer(TimeoutLayer::new(SINGLETON_TIMEOUT))
                    .layer(CatchPanicLayer::new())
                    .layer(PrometheusLayer::default())
                    .layer(ConcurrencyLimitLayer::new(concurrency))
                    .build(singleton_job_handler as SingletonHandlerFn);
                worker
                    .run_until(async move {
                        cancel.cancelled().await;
                        Ok::<(), WorkerError>(())
                    })
                    .await
            })
        }),
    }
}
