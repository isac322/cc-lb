//! Worker pool for executing entity jobs.

use std::collections::HashSet;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use apalis::layers::TimeoutLayer;
use apalis::layers::catch_panic::CatchPanicLayer;
use apalis::layers::limit::ConcurrencyLimitLayer;
use apalis::layers::prometheus::PrometheusLayer;
use apalis::prelude::{Data, TaskSink, WorkerBuilder as ApalisWorkerBuilder, WorkerError};
use apalis_core::task::Task;
use cc_lb_config::{Config, SchedulerConfig};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::cron::WorkerBuilder as CronWorkerBuilder;
use crate::error::SchedulerError;
use crate::jobs::apalis_housekeeping::ApalisHousekeepingJob;
use crate::jobs::compat::AnthropicCompatRefreshJob;
use crate::jobs::metadata_refresh::MetadataRefreshJob;
use crate::jobs::oauth_refresh::OAuthRefreshJob;
use crate::jobs::oauth_usage_poll::OAuthUsagePollJob;
use crate::jobs::price_catalog::PriceCatalogRefreshJob;
use crate::jobs::prompt_cache_purge::PromptCacheObservationPurgeJob;
use crate::jobs::quota_gc::SubscriptionQuotaGcJob;
use crate::jobs::usage_prune::UsagePruneJob;
use crate::jobs::usage_rollup::UsageRollupTask;
use crate::jobs::warmup::UpstreamWarmupJob;
use crate::leader_election::LeaderElection;
use crate::middleware::{TraceparentCarrier, TraceparentLayer};
use crate::retry::{JobOutcome, RetryClass, RetryPayload};

pub const ENTITY_QUEUE: &str = "entity";
pub const SINGLETON_QUEUE: &str = "singleton";
const ENTITY_TIMEOUT: Duration = Duration::from_secs(60);
const SINGLETON_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum EntityJob {
    Warmup(UpstreamWarmupJob),
    OAuthRefresh(OAuthRefreshJob),
    OAuthUsagePoll(OAuthUsagePollJob),
    AnthropicCompatRefresh(AnthropicCompatRefreshJob),
    MetadataRefresh(MetadataRefreshJob),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum SingletonJob {
    UsageRollup(UsageRollupTask),
    UsagePrune(UsagePruneJob),
    QuotaGc(SubscriptionQuotaGcJob),
    PromptCachePurge(PromptCacheObservationPurgeJob),
    PriceCatalogRefresh(PriceCatalogRefreshJob),
    ApalisHousekeeping(ApalisHousekeepingJob),
}

impl SingletonJob {
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::UsageRollup(_) => "usage_rollup",
            Self::UsagePrune(_) => "usage_prune",
            Self::QuotaGc(_) => "quota_gc",
            Self::PromptCachePurge(_) => "prompt_cache_purge",
            Self::PriceCatalogRefresh(_) => "price_catalog_refresh",
            Self::ApalisHousekeeping(_) => "apalis_housekeeping",
        }
    }
}

impl EntityJob {
    pub const fn kind(&self) -> EntityJobKind {
        match self {
            Self::Warmup(_) => EntityJobKind::Warmup,
            Self::OAuthRefresh(_) => EntityJobKind::OAuthRefresh,
            Self::OAuthUsagePoll(_) => EntityJobKind::OAuthUsagePoll,
            Self::AnthropicCompatRefresh(_) => EntityJobKind::AnthropicCompatRefresh,
            Self::MetadataRefresh(_) => EntityJobKind::MetadataRefresh,
        }
    }
}

impl TraceparentCarrier for EntityJob {
    fn traceparent(&self) -> Option<&str> {
        match self {
            Self::Warmup(_) => None,
            Self::OAuthRefresh(job) => job.traceparent(),
            Self::OAuthUsagePoll(job) => job.traceparent(),
            Self::AnthropicCompatRefresh(job) => job.traceparent(),
            Self::MetadataRefresh(job) => job.traceparent(),
        }
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        match self {
            Self::Warmup(_) => {}
            Self::OAuthRefresh(job) => job.set_traceparent(traceparent),
            Self::OAuthUsagePoll(job) => job.set_traceparent(traceparent),
            Self::AnthropicCompatRefresh(job) => job.set_traceparent(traceparent),
            Self::MetadataRefresh(job) => job.set_traceparent(traceparent),
        }
    }
}

impl<Ctx, IdType> TraceparentCarrier for Task<EntityJob, Ctx, IdType> {
    fn traceparent(&self) -> Option<&str> {
        self.args.traceparent()
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        self.args.set_traceparent(traceparent);
    }
}

impl<Ctx, IdType> RetryPayload for Task<EntityJob, Ctx, IdType> {
    fn attempt_count(&self) -> u32 {
        u32::try_from(self.parts.attempt.current()).unwrap_or(u32::MAX)
    }
}

impl TraceparentCarrier for SingletonJob {
    fn traceparent(&self) -> Option<&str> {
        match self {
            Self::UsageRollup(job) => job.traceparent.as_deref(),
            Self::UsagePrune(job) => job.traceparent.as_deref(),
            Self::QuotaGc(job) => job.traceparent(),
            Self::PromptCachePurge(job) => job.traceparent(),
            Self::PriceCatalogRefresh(job) => job.traceparent(),
            Self::ApalisHousekeeping(job) => job.traceparent(),
        }
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        match self {
            Self::UsageRollup(job) => job.traceparent = traceparent,
            Self::UsagePrune(job) => job.traceparent = traceparent,
            Self::QuotaGc(job) => job.set_traceparent(traceparent),
            Self::PromptCachePurge(job) => job.set_traceparent(traceparent),
            Self::PriceCatalogRefresh(job) => job.set_traceparent(traceparent),
            Self::ApalisHousekeeping(job) => job.set_traceparent(traceparent),
        }
    }
}

impl<Ctx, IdType> TraceparentCarrier for Task<SingletonJob, Ctx, IdType> {
    fn traceparent(&self) -> Option<&str> {
        self.args.traceparent()
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        self.args.set_traceparent(traceparent);
    }
}

impl<Ctx, IdType> RetryPayload for Task<SingletonJob, Ctx, IdType> {
    fn attempt_count(&self) -> u32 {
        u32::try_from(self.parts.attempt.current()).unwrap_or(u32::MAX)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum EntityJobKind {
    Warmup,
    OAuthRefresh,
    OAuthUsagePoll,
    AnthropicCompatRefresh,
    MetadataRefresh,
}

#[derive(Debug, Default)]
pub struct EntityHandlerRegistry {
    registered: HashSet<EntityJobKind>,
}

impl EntityHandlerRegistry {
    pub fn register(&mut self, kind: EntityJobKind) -> Result<(), SchedulerError> {
        if !self.registered.insert(kind) {
            return Err(SchedulerError::Job(format!(
                "duplicate entity handler registration for {kind:?}"
            )));
        }
        Ok(())
    }

    pub fn register_all_entity_handlers() -> Result<Self, SchedulerError> {
        let mut registry = Self::default();
        registry.register(EntityJobKind::Warmup)?;
        registry.register(EntityJobKind::OAuthRefresh)?;
        registry.register(EntityJobKind::OAuthUsagePoll)?;
        registry.register(EntityJobKind::AnthropicCompatRefresh)?;
        registry.register(EntityJobKind::MetadataRefresh)?;
        Ok(registry)
    }
}

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
type SqliteSingletonStorage = apalis_sqlite::SqliteStorage<
    SingletonJob,
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
        handles.push(spawn_cron_producer(self.clone(), config, leader, cancel));
        Ok(handles)
    }

    pub async fn push_job(&self, job: EntityJob) -> Result<(), SchedulerError> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(sqlite) => sqlite
                .storage
                .clone()
                .push(job)
                .await
                .map_err(|error| SchedulerError::Job(error.to_string()))?,
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

type SingletonWorkerFuture = Pin<Box<dyn Future<Output = Result<(), WorkerError>> + Send>>;

pub struct SingletonWorker {
    run: Box<dyn FnOnce(CancellationToken) -> SingletonWorkerFuture + Send>,
}

impl SingletonWorker {
    pub async fn run_until_cancelled(self, cancel: CancellationToken) -> Result<(), WorkerError> {
        (self.run)(cancel).await
    }
}

pub fn build_entity_worker(
    backend: &SchedulerBackend,
    ctx: SchedulerCtx,
) -> Result<EntityWorker, SchedulerError> {
    let _registry = EntityHandlerRegistry::register_all_entity_handlers()?;
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
        run: Box::new(move |duration| {
            Box::pin(async move {
                let worker = ApalisWorkerBuilder::new(ENTITY_QUEUE)
                    .backend(storage)
                    .data(ctx)
                    .layer(TraceparentLayer::new())
                    .layer(RetryClass::Entity.layer())
                    .layer(TimeoutLayer::new(ENTITY_TIMEOUT))
                    .layer(CatchPanicLayer::new())
                    .layer(PrometheusLayer::default())
                    .layer(ConcurrencyLimitLayer::new(concurrency))
                    .build(entity_job_handler as EntityHandlerFn);
                worker
                    .run_until(async move {
                        duration.cancelled().await;
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
        run: Box::new(move |duration| {
            Box::pin(async move {
                let worker = ApalisWorkerBuilder::new(ENTITY_QUEUE)
                    .backend(storage)
                    .data(ctx)
                    .layer(TraceparentLayer::new())
                    .layer(RetryClass::Entity.layer())
                    .layer(TimeoutLayer::new(ENTITY_TIMEOUT))
                    .layer(CatchPanicLayer::new())
                    .layer(PrometheusLayer::default())
                    .layer(ConcurrencyLimitLayer::new(concurrency))
                    .build(entity_job_handler as EntityHandlerFn);
                worker
                    .run_until(async move {
                        duration.cancelled().await;
                        Ok::<(), WorkerError>(())
                    })
                    .await
            })
        }),
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
                    .layer(TraceparentLayer::new())
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
                    .layer(TraceparentLayer::new())
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

type EntityHandlerFn =
    fn(
        EntityJob,
        Data<SchedulerCtx>,
    ) -> Pin<Box<dyn Future<Output = Result<JobOutcome, SchedulerError>> + Send>>;

fn entity_job_handler(
    job: EntityJob,
    _ctx: Data<SchedulerCtx>,
) -> Pin<Box<dyn Future<Output = Result<JobOutcome, SchedulerError>> + Send>> {
    Box::pin(async move {
        match job {
            EntityJob::Warmup(_) => record_entity_status("warmup"),
            EntityJob::OAuthRefresh(_) => record_entity_status("oauth_refresh"),
            EntityJob::OAuthUsagePoll(_) => record_entity_status("oauth_usage_poll"),
            EntityJob::AnthropicCompatRefresh(_) => {
                record_entity_status("anthropic_compat_refresh")
            }
            EntityJob::MetadataRefresh(_) => record_entity_status("metadata_refresh"),
        }
        Ok(JobOutcome::Done)
    })
}

type SingletonHandlerFn =
    fn(
        SingletonJob,
        Data<SchedulerCtx>,
    ) -> Pin<Box<dyn Future<Output = Result<JobOutcome, SchedulerError>> + Send>>;

fn singleton_job_handler(
    job: SingletonJob,
    _ctx: Data<SchedulerCtx>,
) -> Pin<Box<dyn Future<Output = Result<JobOutcome, SchedulerError>> + Send>> {
    Box::pin(async move {
        record_singleton_status(job.kind());
        Ok(JobOutcome::Done)
    })
}

fn record_entity_status(job_type: &'static str) {
    metrics::counter!("cclb_scheduler_entity_jobs_total", "job_type" => job_type, "status" => "done")
        .increment(1);
}

fn record_singleton_status(job_type: &'static str) {
    metrics::counter!("cclb_scheduler_singleton_jobs_total", "job_type" => job_type, "status" => "done")
        .increment(1);
}

fn spawn_cron_producer(
    backend: SchedulerBackend,
    config: Config,
    leader: Arc<LeaderElection>,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        match backend {
            #[cfg(feature = "sqlite")]
            SchedulerBackend::Sqlite(sqlite) => {
                run_sqlite_cron_producer(sqlite, config, leader, cancel).await;
            }
            #[cfg(feature = "postgres")]
            SchedulerBackend::Postgres(postgres) => {
                run_postgres_cron_producer(postgres, config, leader, cancel).await;
            }
        }
    })
}

#[cfg(feature = "sqlite")]
async fn run_sqlite_cron_producer(
    sqlite: SqliteSchedulerStorage,
    config: Config,
    leader: Arc<LeaderElection>,
    cancel: CancellationToken,
) {
    let mut handles = Vec::new();
    for spec in singleton_cron_specs(&config) {
        let storage = apalis_sqlite::SqliteStorage::<SingletonJob, (), ()>::new_in_queue(
            &sqlite.pool,
            SINGLETON_QUEUE,
        );
        let worker =
            CronWorkerBuilder::singleton_queue(SINGLETON_QUEUE, spec.schedule, storage, spec.job);
        handles.push(spawn_cron_worker(
            spec.name,
            worker,
            leader.clone(),
            cancel.clone(),
        ));
    }
    cancel.cancelled().await;
    abort_join_handles(handles).await;
}

#[cfg(feature = "postgres")]
async fn run_postgres_cron_producer(
    postgres: PostgresSchedulerStorage,
    config: Config,
    leader: Arc<LeaderElection>,
    cancel: CancellationToken,
) {
    let mut handles = Vec::new();
    for spec in singleton_cron_specs(&config) {
        let storage = apalis_postgres::PostgresStorage::<SingletonJob>::new_with_config(
            &postgres.pool,
            &apalis_postgres::Config::new(SINGLETON_QUEUE),
        );
        let worker =
            CronWorkerBuilder::singleton_queue(SINGLETON_QUEUE, spec.schedule, storage, spec.job);
        handles.push(spawn_cron_worker(
            spec.name,
            worker,
            leader.clone(),
            cancel.clone(),
        ));
    }
    cancel.cancelled().await;
    abort_join_handles(handles).await;
}

fn spawn_cron_worker<Storage>(
    name: &'static str,
    worker: CronWorkerBuilder<SingletonJob, IntervalSchedule, Storage>,
    leader: Arc<LeaderElection>,
    cancel: CancellationToken,
) -> JoinHandle<()>
where
    Storage: TaskSink<SingletonJob> + Send + 'static,
    Storage::Error: Error + Send + Sync + 'static,
{
    tokio::spawn(async move {
        tokio::select! {
            result = worker.run(leader.as_ref()) => {
                if let Err(error) = result {
                    tracing::error!(job = name, error = %error, "scheduler cron producer exited with error");
                }
            }
            _ = cancel.cancelled() => {}
        }
    })
}

async fn abort_join_handles(handles: Vec<JoinHandle<()>>) {
    for handle in handles {
        handle.abort();
        let _ = handle.await;
    }
}

struct SingletonCronSpec {
    name: &'static str,
    schedule: IntervalSchedule,
    job: SingletonJob,
}

fn singleton_cron_specs(config: &Config) -> Vec<SingletonCronSpec> {
    let mut specs = Vec::new();
    push_singleton_spec(
        &mut specs,
        config,
        "usage_rollup",
        SingletonJob::UsageRollup(UsageRollupTask::default()),
    );
    push_singleton_spec(
        &mut specs,
        config,
        "usage_prune",
        SingletonJob::UsagePrune(UsagePruneJob::default()),
    );
    push_singleton_spec(
        &mut specs,
        config,
        "quota_gc",
        SingletonJob::QuotaGc(SubscriptionQuotaGcJob::default()),
    );
    push_singleton_spec(
        &mut specs,
        config,
        "prompt_cache_purge",
        SingletonJob::PromptCachePurge(PromptCacheObservationPurgeJob::default()),
    );
    push_singleton_spec(
        &mut specs,
        config,
        "price_catalog_refresh",
        SingletonJob::PriceCatalogRefresh(PriceCatalogRefreshJob::default()),
    );
    push_singleton_spec(
        &mut specs,
        config,
        "apalis_housekeeping",
        SingletonJob::ApalisHousekeeping(ApalisHousekeepingJob::default()),
    );
    specs
}

fn push_singleton_spec(
    specs: &mut Vec<SingletonCronSpec>,
    config: &Config,
    name: &'static str,
    job: SingletonJob,
) {
    let Some(job_config) = config.scheduler.recurring_jobs.get(name) else {
        return;
    };
    if !job_config.enabled {
        return;
    }
    specs.push(SingletonCronSpec {
        name,
        schedule: IntervalSchedule::new(name, job_config.interval_secs, job_config.jitter_secs),
        job,
    });
}

#[derive(Clone, Debug)]
struct IntervalSchedule {
    interval: chrono::Duration,
    first_delay: chrono::Duration,
    next: Option<DateTime<Utc>>,
}

impl IntervalSchedule {
    fn new(name: &str, interval_secs: u64, jitter_secs: u64) -> Self {
        let bounded_jitter = if jitter_secs == 0 {
            0
        } else {
            stable_hash(name) % jitter_secs.saturating_add(1)
        };
        Self {
            interval: chrono_seconds(interval_secs.max(1)),
            first_delay: chrono_seconds(interval_secs.max(1).saturating_add(bounded_jitter)),
            next: None,
        }
    }
}

impl apalis_cron::Schedule<Utc> for IntervalSchedule {
    fn next_tick(&mut self, _: &Utc) -> Option<DateTime<Utc>> {
        let next = self.next.unwrap_or_else(|| Utc::now() + self.first_delay);
        self.next = Some(next + self.interval);
        Some(next)
    }
}

fn chrono_seconds(seconds: u64) -> chrono::Duration {
    let seconds = i64::try_from(seconds).unwrap_or(i64::MAX);
    chrono::Duration::try_seconds(seconds).unwrap_or(chrono::Duration::MAX)
}

fn stable_hash(value: &str) -> u64 {
    value.as_bytes().iter().fold(0_u64, |hash, byte| {
        hash.wrapping_mul(31).wrapping_add(u64::from(*byte))
    })
}
