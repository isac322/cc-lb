use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use apalis::layers::catch_panic::CatchPanicLayer;
use apalis::prelude::{IntervalStrategy, StrategyBuilder, WorkerBuilder, WorkerError};
use cc_lb_scheduler::idempotency::WarmupEffectsStore;
use cc_lb_scheduler::jobs::warmup::UpstreamWarmupJob;
use cc_lb_scheduler::middleware::TraceparentLayer;
use cc_lb_scheduler::retry::RetryClass;
use cc_lb_scheduler::worker::{
    ENTITY_QUEUE, EntityJob, PostgresApalisStorage, PostgresSchedulerStorage, SchedulerBackend,
};
use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::common::{
    CYCLE_KEY, FakeWarmupHttp, HandlerState, TestResult, entity_job_handler, run_with_recorder,
    stop_workers,
};
use crate::scenario::run_duplicate_scenario;

#[test]
fn postgres_warmup_duplicate_with_crash() -> TestResult<()> {
    run_with_recorder(run_postgres_duplicate)
}

async fn run_postgres_duplicate(
    handle: metrics_exporter_prometheus::PrometheusHandle,
) -> TestResult<()> {
    with_postgres_container(|url| async move {
        let pool_a = PgPoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await?;
        let pool_b = PgPoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await?;
        apalis_postgres::PostgresStorage::setup(&pool_a).await?;
        cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool_a).await?;

        let config = postgres_queue_config();
        let storage_a = PostgresApalisStorage::new_with_config(&pool_a, &config);
        let storage_b = PostgresApalisStorage::new_with_config(&pool_b, &config);
        let backend = SchedulerBackend::Postgres(PostgresSchedulerStorage {
            pool: pool_a.clone(),
            storage: storage_a.clone(),
        });
        let http = FakeWarmupHttp::new();
        let outcomes = Arc::new(Mutex::new(Vec::new()));
        let state_a = HandlerState::new(
            WarmupEffectsStore::new(pool_a.clone()),
            http.clone(),
            outcomes.clone(),
        );
        let state_b = HandlerState::new(
            WarmupEffectsStore::new(pool_b.clone()),
            http.clone(),
            outcomes.clone(),
        );
        let cancel = CancellationToken::new();
        let workers = [
            spawn_postgres_worker(storage_a, state_a, cancel.clone()),
            spawn_postgres_worker(storage_b, state_b, cancel.clone()),
        ];

        let upstream_id = Uuid::new_v4();
        let result = run_duplicate_scenario(
            &handle,
            EntityJob::Warmup(UpstreamWarmupJob::new(upstream_id, CYCLE_KEY)),
            &http,
            &outcomes,
            move |job| {
                let backend = backend.clone();
                async move { Ok(backend.push_job(job).await?) }
            },
            move || {
                let pool = pool_a.clone();
                async move { postgres_effect_count(&pool, upstream_id).await }
            },
        )
        .await;
        stop_workers(cancel, workers).await?;
        result
    })
    .await
}

fn spawn_postgres_worker(
    storage: PostgresApalisStorage,
    state: HandlerState<WarmupEffectsStore<sqlx::Postgres>>,
    cancel: CancellationToken,
) -> JoinHandle<Result<(), WorkerError>> {
    tokio::spawn(async move {
        WorkerBuilder::new(ENTITY_QUEUE)
            .backend(storage)
            .data(state)
            .layer(TraceparentLayer::new().with_scheduler_metrics())
            .layer(RetryClass::Probe.layer())
            .layer(CatchPanicLayer::new())
            .build(entity_job_handler::<WarmupEffectsStore<sqlx::Postgres>>)
            .run_until(async move {
                cancel.cancelled().await;
                Ok::<(), WorkerError>(())
            })
            .await
    })
}

fn postgres_queue_config() -> apalis_postgres::Config {
    let poll_strategy = StrategyBuilder::new()
        .apply(IntervalStrategy::new(Duration::from_millis(10)))
        .build();
    apalis_postgres::Config::new(ENTITY_QUEUE)
        .with_poll_interval(poll_strategy)
        .set_buffer_size(8)
}

async fn postgres_effect_count(pool: &sqlx::PgPool, upstream_id: Uuid) -> TestResult<i64> {
    Ok(sqlx::query_scalar(
        "SELECT COUNT(*) FROM warmup_effects WHERE upstream_id = $1 AND cycle_key = $2",
    )
    .bind(upstream_id)
    .bind(i64::try_from(CYCLE_KEY)?)
    .fetch_one(pool)
    .await?)
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
                "SKIP: DOCKER_HOST not set for postgres warmup duplicate test; expected DOCKER_HOST=tcp://localhost:2375 ({error})"
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
