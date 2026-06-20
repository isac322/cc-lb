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
use super::super::{SINGLETON_QUEUE, SchedulerBackend, SchedulerCtx, SingletonJob};

const SINGLETON_TIMEOUT: Duration = Duration::from_secs(60);

#[cfg(feature = "sqlite")]
type SqliteSingletonStorage = apalis_sqlite::SqliteStorage<
    SingletonJob,
    apalis_codec::json::JsonCodec<apalis_sqlite::CompactType>,
    apalis_sqlite::fetcher::SqliteFetcher,
>;

type SingletonWorkerFuture = Pin<Box<dyn Future<Output = Result<(), WorkerError>> + Send>>;

pub struct SingletonWorker {
    run: Box<dyn FnOnce(CancellationToken) -> SingletonWorkerFuture + Send>,
}

impl SingletonWorker {
    pub async fn run_until_cancelled(self, cancel: CancellationToken) -> Result<(), WorkerError> {
        (self.run)(cancel).await
    }
}

pub fn build_singleton_worker(
    backend: &SchedulerBackend,
    ctx: SchedulerCtx,
) -> Result<SingletonWorker, SchedulerError> {
    match backend {
        #[cfg(feature = "sqlite")]
        SchedulerBackend::Sqlite(sqlite) => Ok(build_sqlite_singleton_worker(
            apalis_sqlite::SqliteStorage::<SingletonJob, (), ()>::new_in_queue(
                &sqlite.pool,
                SINGLETON_QUEUE,
            ),
            ctx,
        )),
        #[cfg(feature = "postgres")]
        SchedulerBackend::Postgres(postgres) => Ok(build_postgres_singleton_worker(
            apalis_postgres::PostgresStorage::<SingletonJob>::new_with_config(
                &postgres.pool,
                &apalis_postgres::Config::new(SINGLETON_QUEUE),
            ),
            ctx,
        )),
    }
}

#[cfg(feature = "sqlite")]
fn build_sqlite_singleton_worker(
    storage: SqliteSingletonStorage,
    ctx: SchedulerCtx,
) -> SingletonWorker {
    let concurrency = ctx.config.singleton_concurrency;
    SingletonWorker {
        run: Box::new(move |cancel| {
            Box::pin(async move {
                let worker = ApalisWorkerBuilder::new(SINGLETON_QUEUE)
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
    storage: apalis_postgres::PostgresStorage<SingletonJob>,
    ctx: SchedulerCtx,
) -> SingletonWorker {
    let concurrency = ctx.config.singleton_concurrency;
    SingletonWorker {
        run: Box::new(move |cancel| {
            Box::pin(async move {
                let worker = ApalisWorkerBuilder::new(SINGLETON_QUEUE)
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
