use std::future::Future;
use std::time::Duration;

use apalis::layers::catch_panic::CatchPanicLayer;
use apalis::prelude::{IntervalStrategy, StrategyBuilder, TaskSink, WorkerBuilder, WorkerError};
use cc_lb_scheduler::jobs::oauth_usage_poll::OAuthUsagePollHandler;
use cc_lb_scheduler::middleware::TraceparentLayer;
use cc_lb_scheduler::state_stores::OAuthUsagePollCursorsStore;
use cc_lb_scheduler::worker::{ADAPTIVE_QUEUE, AdaptiveJob, PostgresApalisStorage};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::common::{RunningWorker, TestResult, schedule_config, stop_worker, usage_url};
use crate::fake::FakeAnthropic;
use crate::scenario::{assert_restart_uses_persisted_cursor, drive_complete_cycle};
use crate::state::{UsagePollWorkerState, entity_job_handler};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_usage_poll_cursor_survives_scheduler_backend_restart() -> TestResult<()> {
    with_postgres_container(run_postgres_durability).await
}

async fn run_postgres_durability(url: String) -> TestResult<()> {
    let fake = FakeAnthropic::spawn().await?;
    let usage_url = usage_url(&fake);
    let upstream_id = Uuid::new_v4();

    let snapshot = {
        let pool = open_postgres_pool(&url).await?;
        setup_postgres_schema(&pool).await?;
        let storage = postgres_storage(&pool);
        let state = UsagePollWorkerState::new(postgres_handler(pool.clone()), usage_url.clone());
        let worker = spawn_postgres_worker(storage.clone(), state.clone());
        let snapshot = drive_complete_cycle(
            &state,
            upstream_id,
            postgres_push(storage),
            postgres_read_cursor(pool.clone(), upstream_id),
        )
        .await?;
        stop_worker(worker).await?;
        pool.close().await;
        snapshot
    };

    let pool = open_postgres_pool(&url).await?;
    let storage = postgres_storage(&pool);
    let state = UsagePollWorkerState::new(postgres_handler(pool.clone()), usage_url);
    let worker = spawn_postgres_worker(storage.clone(), state.clone());
    assert_restart_uses_persisted_cursor(
        &state,
        upstream_id,
        &snapshot,
        postgres_push(storage),
        postgres_read_cursor(pool.clone(), upstream_id),
    )
    .await?;
    stop_worker(worker).await?;
    pool.close().await;
    Ok(())
}

async fn open_postgres_pool(url: &str) -> TestResult<PgPool> {
    Ok(PgPoolOptions::new().max_connections(4).connect(url).await?)
}

async fn setup_postgres_schema(pool: &PgPool) -> TestResult<()> {
    apalis_postgres::PostgresStorage::setup(pool).await?;
    cc_lb_scheduler::migrations::apply_post_setup_migrations(pool).await?;
    Ok(())
}

fn postgres_handler(pool: PgPool) -> OAuthUsagePollHandler<sqlx::Postgres> {
    OAuthUsagePollHandler::new(OAuthUsagePollCursorsStore::new(pool), schedule_config())
}

fn postgres_storage(pool: &PgPool) -> PostgresApalisStorage {
    PostgresApalisStorage::new_with_config(pool, &postgres_queue_config())
}

fn postgres_queue_config() -> apalis_postgres::Config {
    let poll_strategy = StrategyBuilder::new()
        .apply(IntervalStrategy::new(Duration::from_millis(1)))
        .build();
    apalis_postgres::Config::new(ADAPTIVE_QUEUE)
        .with_poll_interval(poll_strategy)
        .set_buffer_size(8)
}

fn postgres_push(
    storage: PostgresApalisStorage,
) -> impl FnMut(AdaptiveJob) -> std::pin::Pin<Box<dyn Future<Output = TestResult<()>> + Send>> {
    move |job| {
        let mut storage = storage.clone();
        Box::pin(async move {
            storage.push(job).await?;
            Ok(())
        })
    }
}

fn postgres_read_cursor(
    pool: PgPool,
    upstream_id: Uuid,
) -> impl FnMut() -> std::pin::Pin<
    Box<
        dyn Future<Output = TestResult<Option<cc_lb_scheduler::state_stores::OAuthUsagePollCursor>>>
            + Send,
    >,
> {
    move || {
        let store = OAuthUsagePollCursorsStore::new(pool.clone());
        Box::pin(async move { Ok(store.read(upstream_id).await?) })
    }
}

fn spawn_postgres_worker(
    storage: PostgresApalisStorage,
    state: UsagePollWorkerState<sqlx::Postgres>,
) -> RunningWorker {
    let cancel = CancellationToken::new();
    let worker_cancel = cancel.clone();
    let join: JoinHandle<Result<(), WorkerError>> = tokio::spawn(async move {
        WorkerBuilder::new(ADAPTIVE_QUEUE)
            .backend(storage)
            .data(state)
            .layer(TraceparentLayer::new())
            .layer(CatchPanicLayer::new())
            .build(entity_job_handler::<sqlx::Postgres>)
            .run_until(async move {
                worker_cancel.cancelled().await;
                Ok::<(), WorkerError>(())
            })
            .await
    });
    RunningWorker::new(cancel, join)
}

async fn with_postgres_container<F, Fut>(run: F) -> TestResult<()>
where
    F: FnOnce(String) -> Fut,
    Fut: Future<Output = TestResult<()>>,
{
    let docker_host = match std::env::var("DOCKER_HOST") {
        Ok(value) => value,
        Err(error) => {
            eprintln!(
                "SKIP: DOCKER_HOST not set for usage-poll durability test; expected DOCKER_HOST=tcp://localhost:2375 ({error})"
            );
            return Ok(());
        }
    };
    let container = match Postgres::default().start().await {
        Ok(container) => container,
        Err(error) => {
            eprintln!(
                "SKIP: could not start postgres testcontainer using DOCKER_HOST={docker_host}: {error}"
            );
            return Ok(());
        }
    };
    let port = match container.get_host_port_ipv4(5432).await {
        Ok(port) => port,
        Err(error) => {
            eprintln!("SKIP: could not read postgres testcontainer port: {error}");
            return Ok(());
        }
    };
    run(format!(
        "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
    ))
    .await
}
