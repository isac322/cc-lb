use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use apalis::layers::catch_panic::CatchPanicLayer;
use apalis::layers::limit::ConcurrencyLimitLayer;
use apalis::layers::prometheus::PrometheusLayer;
use apalis::prelude::{WorkerBuilder as ApalisWorkerBuilder, WorkerError};
use tokio_util::sync::CancellationToken;

use crate::error::SchedulerError;
use crate::middleware::TraceparentLayer;
use crate::retry::RetryClass;

#[cfg(feature = "postgres")]
use super::super::backend::PostgresAdaptiveWorkerStorage;
#[cfg(feature = "sqlite")]
use super::super::backend::SqliteAdaptiveWorkerStorage;
use super::super::dispatch::{EntityHandlerFn, entity_job_handler};
use super::super::{ADAPTIVE_QUEUE, SchedulerBackend, SchedulerCtx};

type AdaptiveWorkerFuture = Pin<Box<dyn Future<Output = Result<(), WorkerError>> + Send>>;

pub struct AdaptiveWorker {
    run: Box<dyn FnOnce(CancellationToken) -> AdaptiveWorkerFuture + Send>,
}

impl AdaptiveWorker {
    pub async fn run_for(self, duration: Duration) -> Result<(), WorkerError> {
        let cancel = CancellationToken::new();
        let stop = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            stop.cancel();
        });
        self.run_until_cancelled(cancel).await
    }

    pub async fn run_until_cancelled(self, cancel: CancellationToken) -> Result<(), WorkerError> {
        (self.run)(cancel).await
    }
}

pub(in crate::worker) fn build_backend_adaptive_worker(
    backend: &SchedulerBackend,
    ctx: SchedulerCtx,
) -> Result<AdaptiveWorker, SchedulerError> {
    build_backend_adaptive_worker_named(backend, ctx, ADAPTIVE_QUEUE.to_owned())
}

pub(in crate::worker) fn build_backend_adaptive_worker_named(
    backend: &SchedulerBackend,
    ctx: SchedulerCtx,
    worker_name: String,
) -> Result<AdaptiveWorker, SchedulerError> {
    match backend {
        #[cfg(feature = "sqlite")]
        SchedulerBackend::Sqlite(sqlite) => {
            let concurrency = ctx.config.entity_concurrency;
            Ok(build_sqlite_worker(
                sqlite.adaptive_worker_storage(),
                ctx,
                worker_name,
                concurrency,
            ))
        }
        #[cfg(feature = "postgres")]
        SchedulerBackend::Postgres(postgres) => {
            let concurrency = ctx.config.entity_concurrency;
            Ok(build_postgres_worker(
                postgres.adaptive_worker_storage(),
                ctx,
                worker_name,
                concurrency,
            ))
        }
    }
}

/// Builds a worker bound to the dedicated `cache_keepalive` queue so keepalive
/// refreshes get an independent concurrency budget and cannot starve the
/// entity-job pool. The queue is a `job_type` namespace on the shared table
/// (no extra table/migration); only the storage handle differs from adaptive.
pub(in crate::worker) fn build_backend_keepalive_worker_named(
    backend: &SchedulerBackend,
    ctx: SchedulerCtx,
    worker_name: String,
) -> Result<AdaptiveWorker, SchedulerError> {
    match backend {
        #[cfg(feature = "sqlite")]
        SchedulerBackend::Sqlite(sqlite) => {
            let concurrency = ctx.config.keepalive_concurrency;
            let storage = sqlite.keepalive_worker_storage();
            Ok(build_sqlite_worker(storage, ctx, worker_name, concurrency))
        }
        #[cfg(feature = "postgres")]
        SchedulerBackend::Postgres(postgres) => {
            let concurrency = ctx.config.keepalive_concurrency;
            let storage = postgres.keepalive_worker_storage();
            Ok(build_postgres_worker(
                storage,
                ctx,
                worker_name,
                concurrency,
            ))
        }
    }
}

#[cfg(feature = "sqlite")]
fn build_sqlite_worker(
    storage: SqliteAdaptiveWorkerStorage,
    ctx: SchedulerCtx,
    worker_name: String,
    concurrency: usize,
) -> AdaptiveWorker {
    AdaptiveWorker {
        run: Box::new(move |cancel| {
            Box::pin(async move {
                let worker = ApalisWorkerBuilder::new(worker_name)
                    .backend(storage)
                    .data(ctx)
                    .layer(TraceparentLayer::new().with_scheduler_metrics())
                    .layer(RetryClass::Adaptive.layer())
                    .layer(CatchPanicLayer::new())
                    .layer(PrometheusLayer::default())
                    .layer(ConcurrencyLimitLayer::new(concurrency))
                    .build(entity_job_handler as EntityHandlerFn);
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
fn build_postgres_worker(
    storage: PostgresAdaptiveWorkerStorage,
    ctx: SchedulerCtx,
    worker_name: String,
    concurrency: usize,
) -> AdaptiveWorker {
    AdaptiveWorker {
        run: Box::new(move |cancel| {
            Box::pin(async move {
                let worker = ApalisWorkerBuilder::new(worker_name)
                    .backend(storage)
                    .data(ctx)
                    .layer(TraceparentLayer::new().with_scheduler_metrics())
                    .layer(RetryClass::Adaptive.layer())
                    .layer(CatchPanicLayer::new())
                    .layer(PrometheusLayer::default())
                    .layer(ConcurrencyLimitLayer::new(concurrency))
                    .build(entity_job_handler as EntityHandlerFn);
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
