use std::str::FromStr as _;
use std::time::Duration;

use apalis::layers::catch_panic::CatchPanicLayer;
use apalis::prelude::{IntervalStrategy, StrategyBuilder, WorkerBuilder, WorkerError};
use apalis_core::backend::codec::Codec as _;
use cc_lb_scheduler::idempotency::OAuthUsagePollCursorsStore;
use cc_lb_scheduler::jobs::oauth_usage_poll::OAuthUsagePollHandler;
use cc_lb_scheduler::middleware::TraceparentLayer;
use cc_lb_scheduler::worker::{ENTITY_QUEUE, EntityJob, SqliteApalisStorage};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::common::{RunningWorker, TestResult, schedule_config, stop_worker, usage_url};
use crate::fake::FakeAnthropic;
use crate::scenario::{assert_restart_uses_persisted_cursor, drive_complete_cycle};
use crate::state::{UsagePollWorkerState, entity_job_handler};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_usage_poll_cursor_survives_scheduler_backend_restart() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let scheduler_url = format!("sqlite://{}", dir.path().join("scheduler.sqlite").display());
    let fake = FakeAnthropic::spawn().await?;
    let usage_url = usage_url(&fake);
    let upstream_id = Uuid::new_v4();

    let snapshot = {
        let pool = open_sqlite_pool(&scheduler_url).await?;
        setup_sqlite_schema(&pool).await?;
        let storage = sqlite_storage(&pool);
        let state = UsagePollWorkerState::new(sqlite_handler(pool.clone()), usage_url.clone());
        let worker = spawn_sqlite_worker(storage.clone(), state.clone());
        let snapshot = drive_complete_cycle(
            &state,
            upstream_id,
            sqlite_push(pool.clone()),
            sqlite_read_cursor(pool.clone(), upstream_id),
        )
        .await?;
        stop_worker(worker).await?;
        pool.close().await;
        snapshot
    };

    let pool = open_sqlite_pool(&scheduler_url).await?;
    let storage = sqlite_storage(&pool);
    let state = UsagePollWorkerState::new(sqlite_handler(pool.clone()), usage_url);
    let worker = spawn_sqlite_worker(storage.clone(), state.clone());
    assert_restart_uses_persisted_cursor(
        &state,
        upstream_id,
        &snapshot,
        sqlite_push(pool.clone()),
        sqlite_read_cursor(pool.clone(), upstream_id),
    )
    .await?;
    stop_worker(worker).await?;
    pool.close().await;
    Ok(())
}

async fn open_sqlite_pool(url: &str) -> TestResult<sqlx::SqlitePool> {
    let options = SqliteConnectOptions::from_str(url)?.create_if_missing(true);
    Ok(SqlitePoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await?)
}

async fn setup_sqlite_schema(pool: &sqlx::SqlitePool) -> TestResult<()> {
    apalis_sqlite::SqliteStorage::setup(pool).await?;
    cc_lb_scheduler::migrations::apply_post_setup_migrations(pool).await?;
    Ok(())
}

fn sqlite_handler(pool: sqlx::SqlitePool) -> OAuthUsagePollHandler<sqlx::Sqlite> {
    OAuthUsagePollHandler::new(OAuthUsagePollCursorsStore::new(pool), schedule_config())
}

fn sqlite_storage(pool: &sqlx::SqlitePool) -> SqliteApalisStorage {
    apalis_sqlite::SqliteStorage::<EntityJob, (), ()>::new_with_config(pool, &sqlite_queue_config())
}

fn sqlite_queue_config() -> apalis_sqlite::Config {
    let poll_strategy = StrategyBuilder::new()
        .apply(IntervalStrategy::new(Duration::from_millis(1)))
        .build();
    apalis_sqlite::Config::new(ENTITY_QUEUE)
        .with_poll_interval(poll_strategy)
        .set_buffer_size(8)
}

fn sqlite_push(
    pool: sqlx::SqlitePool,
) -> impl FnMut(EntityJob) -> std::pin::Pin<Box<dyn std::future::Future<Output = TestResult<()>> + Send>>
{
    move |job| {
        let pool = pool.clone();
        Box::pin(async move {
            let payload = apalis_codec::json::JsonCodec::<Vec<u8>>::encode(&job)?;
            sqlx::query(
                "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, last_result, lock_at, lock_by, done_at, priority, metadata, idempotency_key) \
                 VALUES (?1, ?2, ?3, 'Pending', 0, 5, 0, NULL, NULL, NULL, NULL, 0, ?4, NULL)",
            )
            .bind(payload)
            .bind(ulid::Ulid::new().to_string())
            .bind(ENTITY_QUEUE)
            .bind("{}")
            .execute(&pool)
            .await?;
            Ok(())
        })
    }
}

fn sqlite_read_cursor(
    pool: sqlx::SqlitePool,
    upstream_id: Uuid,
) -> impl FnMut() -> std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = TestResult<Option<cc_lb_scheduler::idempotency::OAuthUsagePollCursor>>,
            > + Send,
    >,
> {
    move || {
        let store = OAuthUsagePollCursorsStore::new(pool.clone());
        Box::pin(async move { Ok(store.read(upstream_id).await?) })
    }
}

fn spawn_sqlite_worker(
    storage: SqliteApalisStorage,
    state: UsagePollWorkerState<sqlx::Sqlite>,
) -> RunningWorker {
    let cancel = CancellationToken::new();
    let worker_cancel = cancel.clone();
    let join: JoinHandle<Result<(), WorkerError>> = tokio::spawn(async move {
        WorkerBuilder::new(ENTITY_QUEUE)
            .backend(storage)
            .data(state)
            .layer(TraceparentLayer::new())
            .layer(CatchPanicLayer::new())
            .build(entity_job_handler::<sqlx::Sqlite>)
            .run_until(async move {
                worker_cancel.cancelled().await;
                Ok::<(), WorkerError>(())
            })
            .await
    });
    RunningWorker::new(cancel, join)
}
