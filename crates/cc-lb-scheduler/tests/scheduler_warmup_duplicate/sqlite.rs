use std::sync::{Arc, Mutex};
use std::time::Duration;

use apalis::layers::catch_panic::CatchPanicLayer;
use apalis::prelude::{IntervalStrategy, StrategyBuilder, WorkerBuilder, WorkerError};
use apalis_core::backend::codec::Codec as _;
use cc_lb_scheduler::idempotency::WarmupEffectsStore;
use cc_lb_scheduler::jobs::warmup::UpstreamWarmupJob;
use cc_lb_scheduler::middleware::TraceparentLayer;
use cc_lb_scheduler::retry::RetryClass;
use cc_lb_scheduler::worker::{ENTITY_QUEUE, EntityJob, SqliteApalisStorage};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::common::{
    CYCLE_KEY, FakeWarmupHttp, HandlerState, TestResult, entity_job_handler, run_with_recorder,
    stop_workers,
};
use crate::scenario::run_duplicate_scenario;

#[test]
fn sqlite_warmup_duplicate_with_crash() -> TestResult<()> {
    run_with_recorder(run_sqlite_duplicate)
}

async fn run_sqlite_duplicate(
    handle: metrics_exporter_prometheus::PrometheusHandle,
) -> TestResult<()> {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(2)
        .connect("sqlite::memory:")
        .await?;
    apalis_sqlite::SqliteStorage::setup(&pool).await?;
    cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool).await?;

    let config = sqlite_queue_config();
    let storage_a: SqliteApalisStorage =
        apalis_sqlite::SqliteStorage::<EntityJob, (), ()>::new_with_config(&pool, &config);
    let storage_b: SqliteApalisStorage =
        apalis_sqlite::SqliteStorage::<EntityJob, (), ()>::new_with_config(&pool, &config);
    let http = FakeWarmupHttp::new();
    let outcomes = Arc::new(Mutex::new(Vec::new()));
    let state = HandlerState::new(
        WarmupEffectsStore::new(pool.clone()),
        http.clone(),
        outcomes.clone(),
    );
    let cancel = CancellationToken::new();
    let workers = [
        spawn_sqlite_worker(storage_a, state.clone(), cancel.clone()),
        spawn_sqlite_worker(storage_b, state, cancel.clone()),
    ];

    let upstream_id = Uuid::new_v4();
    let push_pool = pool.clone();
    let count_pool = pool.clone();
    let result = run_duplicate_scenario(
        &handle,
        EntityJob::Warmup(UpstreamWarmupJob::new(upstream_id, CYCLE_KEY)),
        &http,
        &outcomes,
        move |job| {
            let pool = push_pool.clone();
            async move { sqlite_push_duplicate_job(&pool, job).await }
        },
        move || {
            let pool = count_pool.clone();
            async move { sqlite_effect_count(&pool, upstream_id).await }
        },
    )
    .await;
    stop_workers(cancel, workers).await?;
    result
}

fn spawn_sqlite_worker(
    storage: SqliteApalisStorage,
    state: HandlerState<WarmupEffectsStore<sqlx::Sqlite>>,
    cancel: CancellationToken,
) -> JoinHandle<Result<(), WorkerError>> {
    tokio::spawn(async move {
        WorkerBuilder::new(ENTITY_QUEUE)
            .backend(storage)
            .data(state)
            .layer(TraceparentLayer::new().with_scheduler_metrics())
            .layer(RetryClass::Probe.layer())
            .layer(CatchPanicLayer::new())
            .build(entity_job_handler::<WarmupEffectsStore<sqlx::Sqlite>>)
            .run_until(async move {
                cancel.cancelled().await;
                Ok::<(), WorkerError>(())
            })
            .await
    })
}

fn sqlite_queue_config() -> apalis_sqlite::Config {
    let poll_strategy = StrategyBuilder::new()
        .apply(IntervalStrategy::new(Duration::from_millis(10)))
        .build();
    apalis_sqlite::Config::new(ENTITY_QUEUE)
        .with_poll_interval(poll_strategy)
        .set_buffer_size(8)
}

async fn sqlite_effect_count(pool: &sqlx::SqlitePool, upstream_id: Uuid) -> TestResult<i64> {
    Ok(sqlx::query_scalar(
        "SELECT COUNT(*) FROM warmup_effects WHERE upstream_id = ?1 AND cycle_key = ?2",
    )
    .bind(upstream_id.as_bytes().to_vec())
    .bind(i64::try_from(CYCLE_KEY)?)
    .fetch_one(pool)
    .await?)
}

async fn sqlite_push_duplicate_job(pool: &sqlx::SqlitePool, job: EntityJob) -> TestResult<()> {
    let payload = apalis_codec::json::JsonCodec::<Vec<u8>>::encode(&job)?;
    sqlx::query(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, last_result, lock_at, lock_by, done_at, priority, metadata, idempotency_key) \
         VALUES (?1, ?2, ?3, 'Pending', 0, 5, 0, NULL, NULL, NULL, NULL, 0, ?4, NULL)",
    )
    .bind(payload)
    .bind(ulid::Ulid::new().to_string())
    .bind(ENTITY_QUEUE)
    .bind("{}")
    .execute(pool)
    .await?;
    Ok(())
}
