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
use cc_lb_server::refresh::{LazyRefresher, LazyRefresherDeps, LazyRefresherParams};
use cc_lb_storage_api::{BackendKind, MetaStore};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use storage_sqlx::postgres::PgPoolOptions as StoragePgPoolOptions;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use super::common::{
    TestResult, create_oauth_upstream, read_upstream_generation, stores_from_storage,
};
use super::fake::FakeAnthropic;
use super::scenario::run_race_scenario;
use super::worker::{OAuthWorkerProbe, OAuthWorkerState, entity_job_handler};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t3_postgres__lazy_refresher_vs_proactive_apalis_oauth_refresh_race() -> TestResult<()> {
    let fixture = crate::postgres_fixture().await?;
    let admin_pool = crate::scheduler_postgres_pool(&fixture).await?;
    let result =
        run_postgres_race(&admin_pool, fixture.database_url(), fixture.schema_name()).await;
    admin_pool.close().await;
    let teardown = fixture.teardown().await;
    result?;
    teardown?;
    Ok(())
}

async fn run_postgres_race(
    admin: &PgPool,
    database_url: &str,
    isolation_name: &str,
) -> TestResult<()> {
    let databases = create_test_databases(admin, database_url, isolation_name).await?;
    let result = run_postgres_race_inner(&databases.scheduler_url, &databases.runtime_url).await;
    let cleanup = databases.drop_databases().await;
    result?;
    cleanup
}

async fn run_postgres_race_inner(scheduler_url: &str, runtime_url: &str) -> TestResult<()> {
    let fake = FakeAnthropic::spawn().await?;
    let pool = PgPoolOptions::new()
        .max_connections(6)
        .connect(scheduler_url)
        .await?;
    let storage_pool = StoragePgPoolOptions::new()
        .max_connections(4)
        .connect(runtime_url)
        .await?;
    apalis_postgres::PostgresStorage::setup(&pool).await?;
    cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool).await?;
    let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(1_800_000_000));
    let storage = Arc::new(cc_lb_storage_postgres::PostgresStorage::new(
        storage_pool.clone(),
        clock.clone(),
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
    let upstream_id = create_oauth_upstream(
        storage.as_ref(),
        aead.as_ref(),
        fake.initial_tokens().await?,
    )
    .await?;
    let probe = OAuthWorkerProbe::new();
    let state = OAuthWorkerState::new(
        storage.as_ref().clone(),
        Uuid::from_u128(1),
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
    let lazy = LazyRefresher::new(LazyRefresherParams {
        deps: LazyRefresherDeps {
            stores: stores_from_storage(storage.clone()),
            aead,
            oauth_cfg,
            clock,
        },
        replica_id: Uuid::from_u128(2),
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
            let pool = pool.clone();
            async move { postgres_metadata_count(&pool, upstream_id).await }
        },
        &probe,
    )
    .await;
    cancel.cancel();
    worker.await??;
    drop(storage);
    storage_pool.close().await;
    pool.close().await;
    result
}

struct TestDatabases {
    admin: PgPool,
    scheduler_name: String,
    runtime_name: String,
    scheduler_url: String,
    runtime_url: String,
}

async fn create_test_databases(
    admin: &PgPool,
    source_database_url: &str,
    isolation_name: &str,
) -> TestResult<TestDatabases> {
    let base_url = Url::parse(source_database_url)?;
    let suffix = isolation_name.trim_start_matches("cc_lb_test_");
    let scheduler_name = format!("scheduler_{suffix}");
    let runtime_name = format!("runtime_{suffix}");
    sqlx::query(&format!(r#"CREATE DATABASE "{scheduler_name}""#))
        .execute(admin)
        .await?;
    sqlx::query(&format!(r#"CREATE DATABASE "{runtime_name}""#))
        .execute(admin)
        .await?;
    Ok(TestDatabases {
        scheduler_url: database_url(&base_url, &scheduler_name),
        runtime_url: database_url(&base_url, &runtime_name),
        admin: admin.clone(),
        scheduler_name,
        runtime_name,
    })
}

impl TestDatabases {
    async fn drop_databases(self) -> TestResult<()> {
        for database in [&self.scheduler_name, &self.runtime_name] {
            sqlx::query(&format!(
                r#"DROP DATABASE IF EXISTS "{database}" WITH (FORCE)"#
            ))
            .execute(&self.admin)
            .await?;
        }
        Ok(())
    }
}

fn database_url(base_url: &Url, database: &str) -> String {
    let mut url = base_url.clone();
    url.set_path(database);
    url.to_string()
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
