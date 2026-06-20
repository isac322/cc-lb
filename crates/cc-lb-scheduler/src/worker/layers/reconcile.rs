use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use apalis::layers::TimeoutLayer;
use apalis::layers::catch_panic::CatchPanicLayer;
use apalis::layers::limit::ConcurrencyLimitLayer;
use apalis::layers::prometheus::PrometheusLayer;
use apalis::prelude::{WorkerBuilder as ApalisWorkerBuilder, WorkerError};
use tokio_util::sync::CancellationToken;

use crate::admin::SCHEDULER_RECONCILE_QUEUE;
use crate::error::SchedulerError;
use crate::jobs::reconcile::SchedulerReconcileJob;
use crate::middleware::TraceparentLayer;
use crate::retry::RetryClass;

use super::super::dispatch::{ReconcileHandlerFn, reconcile_job_handler};
use super::super::{SchedulerBackend, SchedulerCtx};

const RECONCILE_TIMEOUT: Duration = Duration::from_secs(60);

#[cfg(feature = "sqlite")]
type SqliteReconcileStorage = apalis_sqlite::SqliteStorage<
    SchedulerReconcileJob,
    apalis_codec::json::JsonCodec<apalis_sqlite::CompactType>,
    apalis_sqlite::fetcher::SqliteFetcher,
>;

type ReconcileWorkerFuture = Pin<Box<dyn Future<Output = Result<(), WorkerError>> + Send>>;

pub struct ReconcileWorker {
    run: Box<dyn FnOnce(CancellationToken) -> ReconcileWorkerFuture + Send>,
}

impl ReconcileWorker {
    pub async fn run_until_cancelled(self, cancel: CancellationToken) -> Result<(), WorkerError> {
        (self.run)(cancel).await
    }
}

pub(in crate::worker) fn build_reconcile_worker(
    backend: &SchedulerBackend,
    ctx: SchedulerCtx,
) -> Result<ReconcileWorker, SchedulerError> {
    match backend {
        #[cfg(feature = "sqlite")]
        SchedulerBackend::Sqlite(sqlite) => Ok(build_sqlite_reconcile_worker(
            apalis_sqlite::SqliteStorage::<SchedulerReconcileJob, (), ()>::new_in_queue(
                &sqlite.pool,
                SCHEDULER_RECONCILE_QUEUE,
            ),
            ctx,
        )),
        #[cfg(feature = "postgres")]
        SchedulerBackend::Postgres(postgres) => Ok(build_postgres_reconcile_worker(
            apalis_postgres::PostgresStorage::<SchedulerReconcileJob>::new_with_config(
                &postgres.pool,
                &apalis_postgres::Config::new(SCHEDULER_RECONCILE_QUEUE),
            ),
            ctx,
        )),
    }
}

#[cfg(feature = "sqlite")]
fn build_sqlite_reconcile_worker(
    storage: SqliteReconcileStorage,
    ctx: SchedulerCtx,
) -> ReconcileWorker {
    ReconcileWorker {
        run: Box::new(move |cancel| {
            Box::pin(async move {
                let worker = ApalisWorkerBuilder::new(SCHEDULER_RECONCILE_QUEUE)
                    .backend(storage)
                    .data(ctx)
                    .layer(TraceparentLayer::new().with_scheduler_metrics())
                    .layer(RetryClass::Maintenance.layer())
                    .layer(TimeoutLayer::new(RECONCILE_TIMEOUT))
                    .layer(CatchPanicLayer::new())
                    .layer(PrometheusLayer::default())
                    .layer(ConcurrencyLimitLayer::new(1))
                    .build(reconcile_job_handler as ReconcileHandlerFn);
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
fn build_postgres_reconcile_worker(
    storage: apalis_postgres::PostgresStorage<SchedulerReconcileJob>,
    ctx: SchedulerCtx,
) -> ReconcileWorker {
    ReconcileWorker {
        run: Box::new(move |cancel| {
            Box::pin(async move {
                let worker = ApalisWorkerBuilder::new(SCHEDULER_RECONCILE_QUEUE)
                    .backend(storage)
                    .data(ctx)
                    .layer(TraceparentLayer::new().with_scheduler_metrics())
                    .layer(RetryClass::Maintenance.layer())
                    .layer(TimeoutLayer::new(RECONCILE_TIMEOUT))
                    .layer(CatchPanicLayer::new())
                    .layer(PrometheusLayer::default())
                    .layer(ConcurrencyLimitLayer::new(1))
                    .build(reconcile_job_handler as ReconcileHandlerFn);
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
