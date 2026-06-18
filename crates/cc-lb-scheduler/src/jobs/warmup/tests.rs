#[cfg(feature = "postgres")]
use std::str::FromStr;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

#[cfg(feature = "postgres")]
use sqlx::Executor;
use sqlx::{Database, Pool};
use uuid::Uuid;

use super::{
    UpstreamWarmupJob, UpstreamWarmupJobHandler, UpstreamWarmupOutcome, WarmupCycleEffects,
};
use crate::{
    error::{Result, SchedulerError},
    idempotency::WarmupEffectsStore,
};

const CYCLE_KEY: u64 = 1_800_000_000;
const COMPLETED_AT_UNIX_SECS: u64 = 1_800_000_001;
type FireFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

async fn fires_at_most_once_per_cycle<Db>(pool: Pool<Db>) -> Result<()>
where
    Db: Database,
    WarmupEffectsStore<Db>: Clone + Send + Sync + WarmupCycleEffects,
{
    let store = WarmupEffectsStore::new(pool);
    let handler = UpstreamWarmupJobHandler::new(store);
    let job = UpstreamWarmupJob::new(Uuid::new_v4(), CYCLE_KEY);
    let fired = Arc::new(AtomicUsize::new(0));

    let first = handler
        .handle(
            job,
            COMPLETED_AT_UNIX_SECS,
            |_| async { Ok(true) },
            incrementing_fire(Arc::clone(&fired)),
        )
        .await?;
    let second = handler
        .handle(
            job,
            COMPLETED_AT_UNIX_SECS + 1,
            |_| async { Ok(true) },
            incrementing_fire(Arc::clone(&fired)),
        )
        .await?;

    assert_eq!(first, UpstreamWarmupOutcome::Fired);
    assert_eq!(second, UpstreamWarmupOutcome::AlreadyCompleted);
    assert_eq!(fired.load(Ordering::SeqCst), 1);
    Ok(())
}

async fn retries_after_crash_mid_fire<Db>(pool: Pool<Db>) -> Result<()>
where
    Db: Database,
    WarmupEffectsStore<Db>: Clone + Send + Sync + WarmupCycleEffects,
{
    let store = WarmupEffectsStore::new(pool);
    let handler = UpstreamWarmupJobHandler::new(store.clone());
    let job = UpstreamWarmupJob::new(Uuid::new_v4(), CYCLE_KEY);
    let fired = Arc::new(AtomicUsize::new(0));

    let failed = handler
        .handle(
            job,
            COMPLETED_AT_UNIX_SECS,
            |_| async { Ok(true) },
            failing_fire(Arc::clone(&fired)),
        )
        .await;
    let retried = handler
        .handle(
            job,
            COMPLETED_AT_UNIX_SECS + 1,
            |_| async { Ok(true) },
            incrementing_fire(Arc::clone(&fired)),
        )
        .await?;

    assert!(failed.is_err());
    assert_eq!(retried, UpstreamWarmupOutcome::Fired);
    assert_eq!(fired.load(Ordering::SeqCst), 2);
    assert!(WarmupCycleEffects::is_already_done(&store, job.upstream_id, job.cycle_key).await?);
    Ok(())
}

async fn skips_deleted_upstream<Db>(pool: Pool<Db>) -> Result<()>
where
    Db: Database,
    WarmupEffectsStore<Db>: Clone + Send + Sync + WarmupCycleEffects,
{
    let store = WarmupEffectsStore::new(pool);
    let handler = UpstreamWarmupJobHandler::new(store.clone());
    let job = UpstreamWarmupJob::new(Uuid::new_v4(), CYCLE_KEY);
    let fired = Arc::new(AtomicUsize::new(0));

    let outcome = handler
        .handle(
            job,
            COMPLETED_AT_UNIX_SECS,
            |_| async { Ok(false) },
            incrementing_fire(Arc::clone(&fired)),
        )
        .await?;

    assert_eq!(outcome, UpstreamWarmupOutcome::UpstreamDeleted);
    assert_eq!(fired.load(Ordering::SeqCst), 0);
    assert!(!WarmupCycleEffects::is_already_done(&store, job.upstream_id, job.cycle_key).await?);
    Ok(())
}

fn incrementing_fire(
    fired: Arc<AtomicUsize>,
) -> impl FnOnce(UpstreamWarmupJob) -> FireFuture + Send {
    move |_| {
        Box::pin(async move {
            fired.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}

fn failing_fire(fired: Arc<AtomicUsize>) -> impl FnOnce(UpstreamWarmupJob) -> FireFuture + Send {
    move |_| {
        Box::pin(async move {
            fired.fetch_add(1, Ordering::SeqCst);
            Err(SchedulerError::Job("crash mid-fire".to_owned()))
        })
    }
}

#[cfg(feature = "sqlite")]
async fn sqlite_pool() -> Result<Pool<sqlx::Sqlite>> {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    sqlx::raw_sql(include_str!(
        "../../../migrations/sqlite/0002_idempotency_tables.sql"
    ))
    .execute(&pool)
    .await?;
    Ok(pool)
}

#[cfg(feature = "postgres")]
async fn with_postgres_pool<F, Fut>(run: F) -> Result<()>
where
    F: FnOnce(Pool<sqlx::Postgres>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("SKIP: DATABASE_URL not set - skipping postgres warmup job test");
        return Ok(());
    };
    let admin = sqlx::PgPool::connect(&url).await?;
    let schema = format!("warmup_job_{}", Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await?;
    let options = sqlx::postgres::PgConnectOptions::from_str(&url)?
        .options([("search_path", schema.as_str())]);
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await?;
    pool.execute(include_str!(
        "../../../migrations/postgres/0002_idempotency_tables.sql"
    ))
    .await?;
    let outcome = run(pool).await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await?;
    outcome
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_fires_at_most_once_per_cycle() -> Result<()> {
    fires_at_most_once_per_cycle(sqlite_pool().await?).await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_retries_after_crash_mid_fire() -> Result<()> {
    retries_after_crash_mid_fire(sqlite_pool().await?).await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_skips_deleted_upstream() -> Result<()> {
    skips_deleted_upstream(sqlite_pool().await?).await
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_fires_at_most_once_per_cycle() -> Result<()> {
    with_postgres_pool(fires_at_most_once_per_cycle).await
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_retries_after_crash_mid_fire() -> Result<()> {
    with_postgres_pool(retries_after_crash_mid_fire).await
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_skips_deleted_upstream() -> Result<()> {
    with_postgres_pool(skips_deleted_upstream).await
}
