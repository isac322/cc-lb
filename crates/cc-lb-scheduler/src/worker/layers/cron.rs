use std::future::Future;
use std::pin::Pin;

use apalis::layers::catch_panic::CatchPanicLayer;
use apalis::layers::limit::ConcurrencyLimitLayer;
use apalis::layers::prometheus::PrometheusLayer;
use apalis::prelude::{WorkerBuilder as ApalisWorkerBuilder, WorkerError};
use tokio_util::sync::CancellationToken;

use crate::error::SchedulerError;
use crate::middleware::TraceparentLayer;
use crate::retry::RetryClass;

#[cfg(feature = "postgres")]
use super::super::backend::PostgresCronApalisStorage;
#[cfg(feature = "sqlite")]
use super::super::backend::SqliteCronApalisStorage;
use super::super::dispatch::{SingletonHandlerFn, singleton_job_handler};
use super::super::{CRON_QUEUE, SchedulerBackend, SchedulerCtx};

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
            sqlite.cron_storage(),
            ctx,
            worker_name,
        )),
        #[cfg(feature = "postgres")]
        SchedulerBackend::Postgres(postgres) => Ok(build_postgres_singleton_worker(
            postgres.cron_worker_storage(),
            ctx,
            worker_name,
        )),
    }
}

#[cfg(feature = "sqlite")]
fn build_sqlite_singleton_worker(
    storage: SqliteCronApalisStorage,
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
    storage: PostgresCronApalisStorage,
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
