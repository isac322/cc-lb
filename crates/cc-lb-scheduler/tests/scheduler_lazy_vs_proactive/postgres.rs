use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use apalis::layers::catch_panic::CatchPanicLayer;
use apalis::prelude::{IntervalStrategy, StrategyBuilder, WorkerBuilder, WorkerError};
use cc_lb_aead::AeadService;
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_scheduler::middleware::TraceparentLayer;
use cc_lb_scheduler::retry::RetryClass;
use cc_lb_scheduler::worker::{
    ADAPTIVE_QUEUE, AdaptiveJob, PostgresApalisStorage, PostgresSchedulerBackend, SchedulerBackend,
};
use cc_lb_server::refresh::{ApalisLazyRefreshClaimGuard, LazyRefresher, LazyRefresherDeps};
use cc_lb_storage_api::{BackendKind, MetaStore};
use sqlx::postgres::PgPoolOptions;
use storage_sqlx::postgres::PgPoolOptions as StoragePgPoolOptions;
use testcontainers_modules::{
    postgres::Postgres,
    testcontainers::{ImageExt, runners::AsyncRunner},
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use super::common::{
    TestResult, create_oauth_upstream, read_upstream_generation, stores_from_storage,
};
use super::fake::FakeAnthropic;
use super::scenario::{ClaimContentionObserver, run_race_scenario};
use super::worker::{OAuthWorkerProbe, OAuthWorkerState, entity_job_handler};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_lazy_refresher_vs_proactive_apalis_oauth_refresh_race() -> TestResult<()> {
    with_postgres_container(run_postgres_race).await
}

async fn run_postgres_race(url: String) -> TestResult<()> {
    let fake = FakeAnthropic::spawn().await?;
    let (scheduler_url, runtime_url) = create_test_databases(&url).await?;
    let pool = PgPoolOptions::new()
        .max_connections(6)
        .connect(&scheduler_url)
        .await?;
    let storage_pool = StoragePgPoolOptions::new()
        .max_connections(4)
        .connect(&runtime_url)
        .await?;
    apalis_postgres::PostgresStorage::setup(&pool).await?;
    cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool).await?;
    let storage = Arc::new(cc_lb_storage_postgres::PostgresStorage::new(
        storage_pool.clone(),
        Arc::new(cc_lb_clock::SystemClock),
    ));
    storage.initialize(BackendKind::Postgres).await?;

    let config = postgres_queue_config();
    let backend = SchedulerBackend::Postgres(PostgresSchedulerBackend::new(pool.clone()));
    let aead = Arc::new(AeadService::from_master_key([83; 32]));
    let oauth_cfg = Arc::new(AnthropicOAuthConfig {
        client_id: "test-client".to_owned(),
        auth_url: fake.auth_url(),
        token_url: fake.token_url(),
        redirect_uri: Url::parse("http://localhost/callback")?,
        scopes: vec!["messages".to_owned()],
    });
    let (upstream_id, expires_at_unix_secs) = create_oauth_upstream(
        storage.as_ref(),
        aead.as_ref(),
        fake.initial_tokens().await?,
    )
    .await?;
    let probe = OAuthWorkerProbe::new();
    let state = OAuthWorkerState::new(
        storage.as_ref().clone(),
        aead.clone(),
        oauth_cfg.clone(),
        backend.clone(),
        probe.clone(),
    );
    let cancel = CancellationToken::new();
    let worker = spawn_postgres_worker(
        apalis_postgres::PostgresStorage::<AdaptiveJob>::new_with_notify(&pool, &config),
        state,
        cancel.clone(),
    );
    let stores = stores_from_storage(storage.clone());
    let claim_guard =
        ClaimContentionObserver::new(Arc::new(ApalisLazyRefreshClaimGuard::new(backend.clone())));
    let lazy = LazyRefresher::new_with_claim_guard(
        LazyRefresherDeps {
            stores,
            aead,
            clock: Arc::new(cc_lb_clock::SystemClock),
        },
        CancellationToken::new(),
        claim_guard.clone(),
        backend.clone(),
    );

    let result = run_race_scenario(
        &fake,
        backend,
        lazy,
        claim_guard,
        upstream_id,
        expires_at_unix_secs,
        || {
            let storage = storage.clone();
            async move { read_upstream_generation(storage.as_ref(), upstream_id).await }
        },
        || {
            let pool = pool.clone();
            async move { postgres_metadata_count(&pool, upstream_id).await }
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

async fn create_test_databases(base_url: &str) -> TestResult<(String, String)> {
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(base_url)
        .await?;
    let suffix = Uuid::new_v4().simple().to_string();
    let scheduler_db = format!("scheduler_{suffix}");
    let runtime_db = format!("runtime_{suffix}");
    sqlx::query(&format!(r#"CREATE DATABASE "{scheduler_db}""#))
        .execute(&admin)
        .await?;
    sqlx::query(&format!(r#"CREATE DATABASE "{runtime_db}""#))
        .execute(&admin)
        .await?;
    admin.close().await;
    Ok((
        database_url(base_url, &scheduler_db)?,
        database_url(base_url, &runtime_db)?,
    ))
}

fn database_url(base_url: &str, database: &str) -> TestResult<String> {
    let mut url = Url::parse(base_url)?;
    url.set_path(database);
    Ok(url.to_string())
}

fn spawn_postgres_worker(
    storage: PostgresApalisStorage,
    state: OAuthWorkerState<cc_lb_storage_postgres::PostgresStorage>,
    cancel: CancellationToken,
) -> JoinHandle<Result<(), WorkerError>> {
    tokio::spawn(async move {
        WorkerBuilder::new(ADAPTIVE_QUEUE)
            .backend(storage)
            .data(state)
            .layer(TraceparentLayer::new().with_scheduler_metrics())
            .layer(RetryClass::Probe.layer())
            .layer(CatchPanicLayer::new())
            .build(entity_job_handler::<cc_lb_storage_postgres::PostgresStorage>)
            .run_until(async move {
                cancel.cancelled().await;
                Ok::<(), WorkerError>(())
            })
            .await
    })
}

fn postgres_queue_config() -> apalis_postgres::Config {
    let poll_strategy = StrategyBuilder::new()
        .apply(IntervalStrategy::new(Duration::from_millis(1)))
        .build();
    apalis_postgres::Config::new(ADAPTIVE_QUEUE)
        .with_poll_interval(poll_strategy)
        .set_buffer_size(8)
}

async fn postgres_metadata_count(pool: &sqlx::PgPool, upstream_id: Uuid) -> TestResult<i64> {
    Ok(sqlx::query_scalar(
        "SELECT COUNT(*) FROM (
            SELECT convert_from(job, 'UTF8')::jsonb AS payload
            FROM apalis.jobs
            WHERE job_type = $1
         ) jobs
         WHERE payload ->> 'type' = 'metadata_refresh'
           AND payload #>> '{payload,upstream_id}' = $2",
    )
    .bind(ADAPTIVE_QUEUE)
    .bind(upstream_id.to_string())
    .fetch_one(pool)
    .await?)
}

async fn with_postgres_container<F, Fut>(run: F) -> TestResult<()>
where
    F: FnOnce(String) -> Fut,
    Fut: Future<Output = TestResult<()>>,
{
    if let Ok(url) = std::env::var("CI_POSTGRES_URL") {
        return run(url).await;
    }
    let docker_host = match std::env::var("DOCKER_HOST") {
        Ok(value) => value,
        Err(error) => {
            eprintln!(
                "SKIP: DOCKER_HOST not set for lazy/proactive race test; expected DOCKER_HOST=tcp://localhost:2375 ({error})"
            );
            return Ok(());
        }
    };
    let container = match Postgres::default().with_tag("18-alpine").start().await {
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
