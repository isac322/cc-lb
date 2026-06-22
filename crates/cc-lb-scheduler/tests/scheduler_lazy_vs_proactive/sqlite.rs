use std::str::FromStr as _;
use std::sync::Arc;
use std::time::Duration;

use apalis::layers::catch_panic::CatchPanicLayer;
use apalis::prelude::{IntervalStrategy, StrategyBuilder, WorkerBuilder, WorkerError};
use cc_lb_aead::AeadService;
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_scheduler::middleware::TraceparentLayer;
use cc_lb_scheduler::retry::RetryClass;
use cc_lb_scheduler::worker::{ADAPTIVE_QUEUE, SchedulerBackend, SqliteSchedulerStorage};
use cc_lb_server::refresh::LazyRefresher;
use cc_lb_storage_api::{BackendKind, MetaStore};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::common::{
    TestResult, create_oauth_upstream, metadata_key_prefix, read_upstream_generation,
    stores_from_storage,
};
use crate::fake::FakeAnthropic;
use crate::scenario::run_race_scenario;
use crate::worker::{OAuthWorkerProbe, OAuthWorkerState, entity_job_handler};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_lazy_refresher_vs_proactive_apalis_oauth_refresh_race() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let fake = FakeAnthropic::spawn().await?;
    let storage_url = format!("sqlite://{}", dir.path().join("runtime.sqlite").display());
    let storage = Arc::new(cc_lb_storage_sqlite::open_sqlite(&storage_url).await?);
    storage.initialize(BackendKind::Sqlite).await?;
    let scheduler_url = format!("sqlite://{}", dir.path().join("scheduler.sqlite").display());
    let scheduler_options = SqliteConnectOptions::from_str(&scheduler_url)?.create_if_missing(true);
    let scheduler_pool = SqlitePoolOptions::new()
        .max_connections(4)
        .connect_with(scheduler_options)
        .await?;
    apalis_sqlite::SqliteStorage::setup(&scheduler_pool).await?;
    cc_lb_scheduler::migrations::apply_post_setup_migrations(&scheduler_pool).await?;

    let config = sqlite_queue_config();
    let backend = SchedulerBackend::Sqlite(SqliteSchedulerStorage {
        pool: scheduler_pool.clone(),
        storage: apalis_sqlite::SqliteStorage::new_with_config(&scheduler_pool, &config),
    });
    let aead = Arc::new(AeadService::from_master_key([38; 32]));
    let oauth_cfg = Arc::new(AnthropicOAuthConfig {
        client_id: "test-client".to_owned(),
        auth_url: fake.auth_url(),
        token_url: fake.token_url(),
        redirect_uri: Url::parse("http://localhost/callback")?,
        scopes: vec!["messages".to_owned()],
    });
    let upstream_id = create_oauth_upstream(
        storage.as_ref(),
        aead.as_ref(),
        fake.initial_tokens().await?,
    )
    .await?;
    let probe = OAuthWorkerProbe::new();
    let state = OAuthWorkerState::new(
        storage.as_ref().clone(),
        Uuid::new_v4(),
        aead.clone(),
        oauth_cfg.clone(),
        backend.clone(),
        probe.clone(),
    );
    let cancel = CancellationToken::new();
    let worker = spawn_sqlite_worker(
        apalis_sqlite::SqliteStorage::new_with_config(&scheduler_pool, &config),
        state,
        cancel.clone(),
    );
    let lazy = LazyRefresher::new(
        stores_from_storage(storage.clone()),
        aead,
        oauth_cfg,
        Uuid::new_v4(),
        None,
        CancellationToken::new(),
        backend.clone(),
    );

    let result = run_race_scenario(
        &fake,
        backend,
        lazy,
        upstream_id,
        || {
            let storage = storage.clone();
            async move { read_upstream_generation(storage.as_ref(), upstream_id).await }
        },
        || {
            let pool = scheduler_pool.clone();
            async move { sqlite_metadata_count(&pool, upstream_id).await }
        },
        || {
            let probe = probe.clone();
            async move { Ok(probe.has_started()) }
        },
    )
    .await;
    cancel.cancel();
    worker.await??;
    result
}

fn spawn_sqlite_worker(
    storage: cc_lb_scheduler::worker::SqliteApalisStorage,
    state: OAuthWorkerState<cc_lb_storage_sqlite::SqliteStorage>,
    cancel: CancellationToken,
) -> JoinHandle<Result<(), WorkerError>> {
    tokio::spawn(async move {
        WorkerBuilder::new(ADAPTIVE_QUEUE)
            .backend(storage)
            .data(state)
            .layer(TraceparentLayer::new().with_scheduler_metrics())
            .layer(RetryClass::Probe.layer())
            .layer(CatchPanicLayer::new())
            .build(entity_job_handler::<cc_lb_storage_sqlite::SqliteStorage>)
            .run_until(async move {
                cancel.cancelled().await;
                Ok::<(), WorkerError>(())
            })
            .await
    })
}

fn sqlite_queue_config() -> apalis_sqlite::Config {
    let poll_strategy = StrategyBuilder::new()
        .apply(IntervalStrategy::new(Duration::from_millis(1)))
        .build();
    apalis_sqlite::Config::new(ADAPTIVE_QUEUE)
        .with_poll_interval(poll_strategy)
        .set_buffer_size(8)
}

async fn sqlite_metadata_count(pool: &sqlx::SqlitePool, upstream_id: Uuid) -> TestResult<i64> {
    Ok(
        sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE idempotency_key LIKE ?1")
            .bind(metadata_key_prefix(upstream_id))
            .fetch_one(pool)
            .await?,
    )
}
