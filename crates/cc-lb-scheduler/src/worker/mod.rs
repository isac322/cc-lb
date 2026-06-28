//! Worker pool for executing entity jobs.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[cfg(feature = "postgres")]
use apalis::prelude::TaskSink;
use cc_lb_config::{Config, SchedulerConfig};
use cc_lb_core::clock::{Clock, ClockHandle, unix_millis};
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
    pub clock: ClockHandle,
}

impl SchedulerCtx {
    pub fn new(
        config: SchedulerConfig,
        adaptive_dispatch: AdaptiveDispatchFn,
        cron_dispatch: CronDispatchFn,
        clock: ClockHandle,
    ) -> Self {
        Self {
            config,
            adaptive_dispatch,
            cron_dispatch,
            clock,
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
#[derive(Clone)]
pub struct SqliteSchedulerStorage {
    pub pool: sqlx::SqlitePool,
    pub storage: SqliteApalisStorage,
    pub clock: ClockHandle,
}

#[cfg(feature = "postgres")]
pub type PostgresApalisStorage = apalis_postgres::PostgresStorage<
    AdaptiveJob,
    apalis_postgres::CompactType,
    apalis_postgres::JsonCodec<apalis_postgres::CompactType>,
    apalis_postgres::PgNotify,
>;

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
        let mut handles = self.spawn_consumers(ctx.clone(), cancel.clone())?;
        handles.push(cron::spawn_cron_producer(
            self.clone(),
            config,
            leader,
            cancel,
            ctx.clock,
        ));
        Ok(handles)
    }

    pub async fn push_job(&self, job: AdaptiveJob) -> Result<(), SchedulerError> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(sqlite) => {
                let queue = sqlite.storage.config().queue().as_ref().to_owned();
                crate::sqlite_enqueue::push_entity_job(&sqlite.pool, &queue, job, &*sqlite.clock)
                    .await?;
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
        let backend = self.clone();
        let worker_instance_id = make_worker_instance_id(&*ctx.clock);
        Ok(vec![
            tokio::spawn(run_adaptive_consumer_loop(
                backend.clone(),
                ctx.clone(),
                cancel.clone(),
                worker_instance_id.clone(),
            )),
            tokio::spawn(run_singleton_consumer_loop(
                backend,
                ctx,
                cancel,
                worker_instance_id,
            )),
        ])
    }
}

const CONSUMER_BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const CONSUMER_BACKOFF_CAP: Duration = Duration::from_secs(60);
const CONSUMER_LONG_RUN_THRESHOLD: Duration = Duration::from_secs(30);

pub fn make_worker_instance_id(clock: &dyn Clock) -> String {
    let pid = std::process::id();
    let started_ms = unix_millis(clock.now());
    format!("{pid}-{started_ms}")
}

static WORKER_RUN_SEQ: AtomicU64 = AtomicU64::new(0);

fn next_worker_name(queue: &str, worker_instance_id: &str) -> String {
    let seq = WORKER_RUN_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{queue}-{worker_instance_id}-{seq}")
}

fn jitter(d: Duration, clock: &dyn Clock) -> Duration {
    let millis = unix_millis(clock.now());
    let frac = (millis % 400) as f64 / 1000.0;
    let factor = 0.8_f64 + frac;
    Duration::from_secs_f64(d.as_secs_f64() * factor)
}

async fn sleep_or_cancel(d: Duration, cancel: &CancellationToken) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(d) => true,
        _ = cancel.cancelled() => false,
    }
}

async fn run_adaptive_consumer_loop(
    backend: SchedulerBackend,
    ctx: SchedulerCtx,
    cancel: CancellationToken,
    worker_instance_id: String,
) {
    let mut backoff = CONSUMER_BACKOFF_INITIAL;
    loop {
        if cancel.is_cancelled() {
            return;
        }
        let worker_name = next_worker_name(ADAPTIVE_QUEUE, &worker_instance_id);
        let worker = match layers::build_backend_adaptive_worker_named(
            &backend,
            ctx.clone(),
            worker_name.clone(),
        ) {
            Ok(worker) => worker,
            Err(error) => {
                tracing::error!(
                    error = %error,
                    worker = %worker_name,
                    "scheduler adaptive worker build failed; supervising restart",
                );
                ::metrics::counter!(
                    "cclb_scheduler_consumer_restarts_total",
                    "consumer" => "adaptive",
                    "reason" => "build_failed",
                )
                .increment(1);
                if !sleep_or_cancel(jitter(backoff, &*ctx.clock), &cancel).await {
                    return;
                }
                backoff = (backoff * 2).min(CONSUMER_BACKOFF_CAP);
                continue;
            }
        };
        let started_at = Instant::now();
        let result = worker.run_until_cancelled(cancel.clone()).await;
        if cancel.is_cancelled() {
            return;
        }
        let elapsed = started_at.elapsed();
        match result {
            Ok(()) => {
                tracing::warn!(
                    worker = %worker_name,
                    elapsed_ms = elapsed.as_millis() as u64,
                    "scheduler adaptive worker exited without cancel; supervising restart",
                );
                ::metrics::counter!(
                    "cclb_scheduler_consumer_restarts_total",
                    "consumer" => "adaptive",
                    "reason" => "exited_ok",
                )
                .increment(1);
            }
            Err(error) => {
                tracing::warn!(
                    worker = %worker_name,
                    elapsed_ms = elapsed.as_millis() as u64,
                    error = %error,
                    "scheduler adaptive worker exited with error; supervising restart",
                );
                ::metrics::counter!(
                    "cclb_scheduler_consumer_restarts_total",
                    "consumer" => "adaptive",
                    "reason" => "exited_error",
                )
                .increment(1);
            }
        }
        backoff = if elapsed >= CONSUMER_LONG_RUN_THRESHOLD {
            CONSUMER_BACKOFF_INITIAL
        } else {
            (backoff * 2).min(CONSUMER_BACKOFF_CAP)
        };
        if !sleep_or_cancel(jitter(backoff, &*ctx.clock), &cancel).await {
            return;
        }
    }
}

async fn run_singleton_consumer_loop(
    backend: SchedulerBackend,
    ctx: SchedulerCtx,
    cancel: CancellationToken,
    worker_instance_id: String,
) {
    let mut backoff = CONSUMER_BACKOFF_INITIAL;
    loop {
        if cancel.is_cancelled() {
            return;
        }
        let worker_name = next_worker_name(CRON_QUEUE, &worker_instance_id);
        let worker =
            match layers::build_cron_worker_named(&backend, ctx.clone(), worker_name.clone()) {
                Ok(worker) => worker,
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        worker = %worker_name,
                        "scheduler singleton worker build failed; supervising restart",
                    );
                    ::metrics::counter!(
                        "cclb_scheduler_consumer_restarts_total",
                        "consumer" => "singleton",
                        "reason" => "build_failed",
                    )
                    .increment(1);
                    if !sleep_or_cancel(jitter(backoff, &*ctx.clock), &cancel).await {
                        return;
                    }
                    backoff = (backoff * 2).min(CONSUMER_BACKOFF_CAP);
                    continue;
                }
            };
        let started_at = Instant::now();
        let result = worker.run_until_cancelled(cancel.clone()).await;
        if cancel.is_cancelled() {
            return;
        }
        let elapsed = started_at.elapsed();
        match result {
            Ok(()) => {
                tracing::warn!(
                    worker = %worker_name,
                    elapsed_ms = elapsed.as_millis() as u64,
                    "scheduler singleton worker exited without cancel; supervising restart",
                );
                ::metrics::counter!(
                    "cclb_scheduler_consumer_restarts_total",
                    "consumer" => "singleton",
                    "reason" => "exited_ok",
                )
                .increment(1);
            }
            Err(error) => {
                tracing::warn!(
                    worker = %worker_name,
                    elapsed_ms = elapsed.as_millis() as u64,
                    error = %error,
                    "scheduler singleton worker exited with error; supervising restart",
                );
                ::metrics::counter!(
                    "cclb_scheduler_consumer_restarts_total",
                    "consumer" => "singleton",
                    "reason" => "exited_error",
                )
                .increment(1);
            }
        }
        backoff = if elapsed >= CONSUMER_LONG_RUN_THRESHOLD {
            CONSUMER_BACKOFF_INITIAL
        } else {
            (backoff * 2).min(CONSUMER_BACKOFF_CAP)
        };
        if !sleep_or_cancel(jitter(backoff, &*ctx.clock), &cancel).await {
            return;
        }
    }
}

#[cfg(feature = "sqlite")]
impl std::fmt::Debug for SqliteSchedulerStorage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqliteSchedulerStorage")
            .field("pool", &self.pool)
            .finish_non_exhaustive()
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
