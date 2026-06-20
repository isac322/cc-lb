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

#[cfg(feature = "postgres")]
use super::super::PostgresApalisStorage;
#[cfg(feature = "sqlite")]
use super::super::SqliteApalisStorage;
use super::super::dispatch::{EntityHandlerFn, entity_job_handler};
use super::super::{ENTITY_QUEUE, SchedulerBackend, SchedulerCtx};

const ENTITY_TIMEOUT: Duration = Duration::from_secs(60);

type EntityWorkerFuture = Pin<Box<dyn Future<Output = Result<(), WorkerError>> + Send>>;

pub struct EntityWorker {
    run: Box<dyn FnOnce(CancellationToken) -> EntityWorkerFuture + Send>,
}

impl EntityWorker {
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

pub(in crate::worker) fn build_backend_entity_worker(
    backend: &SchedulerBackend,
    ctx: SchedulerCtx,
) -> Result<EntityWorker, SchedulerError> {
    match backend {
        #[cfg(feature = "sqlite")]
        SchedulerBackend::Sqlite(sqlite) => Ok(build_sqlite_worker(sqlite.storage.clone(), ctx)),
        #[cfg(feature = "postgres")]
        SchedulerBackend::Postgres(postgres) => {
            Ok(build_postgres_worker(postgres.storage.clone(), ctx))
        }
    }
}

#[cfg(feature = "sqlite")]
fn build_sqlite_worker(storage: SqliteApalisStorage, ctx: SchedulerCtx) -> EntityWorker {
    let concurrency = ctx.config.entity_concurrency;
    EntityWorker {
        run: Box::new(move |cancel| {
            Box::pin(async move {
                let worker = ApalisWorkerBuilder::new(ENTITY_QUEUE)
                    .backend(storage)
                    .data(ctx)
                    .layer(TraceparentLayer::new().with_scheduler_metrics())
                    .layer(RetryClass::Entity.layer())
                    .layer(TimeoutLayer::new(ENTITY_TIMEOUT))
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
fn build_postgres_worker(storage: PostgresApalisStorage, ctx: SchedulerCtx) -> EntityWorker {
    let concurrency = ctx.config.entity_concurrency;
    EntityWorker {
        run: Box::new(move |cancel| {
            Box::pin(async move {
                let worker = ApalisWorkerBuilder::new(ENTITY_QUEUE)
                    .backend(storage)
                    .data(ctx)
                    .layer(TraceparentLayer::new().with_scheduler_metrics())
                    .layer(RetryClass::Entity.layer())
                    .layer(TimeoutLayer::new(ENTITY_TIMEOUT))
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
