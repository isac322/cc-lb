use std::str::FromStr as _;
use std::sync::Arc;
use std::time::Duration;

use apalis::layers::catch_panic::CatchPanicLayer;
use apalis::prelude::{WorkerBuilder, WorkerError};
use apalis_core::backend::ext::BackendExt as _;
use cc_lb_aead::AeadService;
use cc_lb_clock::SystemClock;
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_scheduler::middleware::TraceparentLayer;
use cc_lb_scheduler::retry::RetryClass;
use cc_lb_scheduler::worker::{ADAPTIVE_QUEUE, SchedulerBackend, SqliteSchedulerBackend};
use cc_lb_server::refresh::{LazyRefresher, LazyRefresherDeps, LazyRefresherParams};
use cc_lb_storage_api::MetaStore;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use super::common::{
    TestResult, create_oauth_upstream, metadata_key_prefix, read_upstream_generation,
    stores_from_storage,
};
use super::fake::FakeAnthropic;
use super::scenario::run_race_scenario;
use super::worker::{OAuthWorkerProbe, OAuthWorkerState, entity_job_handler};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_lazy_refresher_vs_proactive_apalis_oauth_refresh_race() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let fake = FakeAnthropic::spawn().await?;
    let storage_url = format!("sqlite://{}", dir.path().join("runtime.sqlite").display());
    let storage = Arc::new(
        cc_lb_storage_sqlite::open_sqlite(&storage_url, Arc::new(cc_lb_clock::SystemClock)).await?,
    );
    storage.initialize().await?;
    let scheduler_url = format!("sqlite://{}", dir.path().join("scheduler.sqlite").display());
    let scheduler_options = SqliteConnectOptions::from_str(&scheduler_url)?.create_if_missing(true);
    let scheduler_pool = SqlitePoolOptions::new()
        .max_connections(4)
        .connect_with(scheduler_options)
        .await?;
    apalis_sqlite::SqliteStorage::setup(&scheduler_pool).await?;
    cc_lb_scheduler::migrations::apply_post_setup_migrations(&scheduler_pool).await?;

    let config = sqlite_queue_config();
    let backend = SchedulerBackend::Sqlite(SqliteSchedulerBackend::new(
        scheduler_pool.clone(),
        Arc::new(SystemClock),
    ));
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
        apalis_sqlite::SqliteStorage::new(&scheduler_pool)
            .with_config(config)
            .poll_with_interval(Duration::from_millis(1)),
        state,
        cancel.clone(),
    );
    let lazy = LazyRefresher::new(LazyRefresherParams {
        deps: LazyRefresherDeps {
            stores: stores_from_storage(storage.clone()),
            aead,
            oauth_cfg,
            clock: Arc::new(cc_lb_clock::SystemClock),
        },
        replica_id: Uuid::new_v4(),
        metadata_hook: None,
        cancel: CancellationToken::new(),
        apalis_handle: backend.clone(),
    });

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
            .layer(test_catch_panic_layer())
            .build(entity_job_handler::<cc_lb_storage_sqlite::SqliteStorage>)
            .run_until(async move {
                cancel.cancelled_owned().await;
                Ok::<(), WorkerError>(())
            })
            .await
    })
}

fn test_catch_panic_layer() -> CatchPanicLayer<
    impl FnMut(&Box<dyn std::any::Any + Send>) -> apalis_core::error::AbortError
    + Clone
    + Send
    + Sync
    + 'static,
    apalis_core::error::AbortError,
> {
    CatchPanicLayer::with_panic_handler(|_: &Box<dyn std::any::Any + Send>| {
        apalis_core::error::AbortError::new(apalis::layers::catch_panic::PanicError(
            "worker panicked".to_owned(),
        ))
    })
}

fn sqlite_queue_config() -> apalis_sqlite::Config {
    apalis_sqlite::Config::default()
        .queue(ADAPTIVE_QUEUE)
        .batch_size(8)
}

async fn sqlite_metadata_count(pool: &sqlx::SqlitePool, upstream_id: Uuid) -> TestResult<i64> {
    Ok(
        sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE idempotency_key LIKE ?1")
            .bind(metadata_key_prefix(upstream_id))
            .fetch_one(pool)
            .await?,
    )
}
