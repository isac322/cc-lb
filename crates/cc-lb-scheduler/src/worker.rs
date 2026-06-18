//! Worker pool for executing entity jobs.

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use apalis::layers::TimeoutLayer;
use apalis::layers::catch_panic::CatchPanicLayer;
use apalis::layers::limit::ConcurrencyLimitLayer;
use apalis::layers::prometheus::PrometheusLayer;
use apalis::prelude::{Data, WorkerBuilder, WorkerError};
use apalis_core::task::Task;
use cc_lb_config::SchedulerConfig;
use serde::{Deserialize, Serialize};

use crate::error::SchedulerError;
use crate::jobs::compat::AnthropicCompatRefreshJob;
use crate::jobs::metadata_refresh::MetadataRefreshJob;
use crate::jobs::oauth_refresh::OAuthRefreshJob;
use crate::jobs::oauth_usage_poll::OAuthUsagePollJob;
use crate::jobs::warmup::UpstreamWarmupJob;
use crate::middleware::{TraceparentCarrier, TraceparentLayer};
use crate::retry::{JobOutcome, RetryClass, RetryPayload};

pub const ENTITY_QUEUE: &str = "entity";
const ENTITY_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum EntityJob {
    Warmup(UpstreamWarmupJob),
    OAuthRefresh(OAuthRefreshJob),
    OAuthUsagePoll(OAuthUsagePollJob),
    AnthropicCompatRefresh(AnthropicCompatRefreshJob),
    MetadataRefresh(MetadataRefreshJob),
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

#[derive(Debug)]
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
#[derive(Debug)]
pub struct SqliteSchedulerStorage {
    pub pool: sqlx::SqlitePool,
    pub storage: SqliteApalisStorage,
}

#[cfg(feature = "postgres")]
pub type PostgresApalisStorage = apalis_postgres::PostgresStorage<EntityJob>;

#[cfg(feature = "postgres")]
pub struct PostgresSchedulerStorage {
    pub pool: sqlx::PgPool,
    pub storage: PostgresApalisStorage,
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
    run: Box<dyn FnOnce(Duration) -> EntityWorkerFuture + Send>,
}

impl EntityWorker {
    pub async fn run_for(self, duration: Duration) -> Result<(), WorkerError> {
        (self.run)(duration).await
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
                let worker = WorkerBuilder::new(ENTITY_QUEUE)
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
                        tokio::time::sleep(duration).await;
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
                let worker = WorkerBuilder::new(ENTITY_QUEUE)
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
                        tokio::time::sleep(duration).await;
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

fn record_entity_status(job_type: &'static str) {
    metrics::counter!("cclb_scheduler_entity_jobs_total", "job_type" => job_type, "status" => "done")
        .increment(1);
}
