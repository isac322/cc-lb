use std::future::Future;
use std::sync::Arc;

use cc_lb_aead::AeadService;
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_scheduler::worker::{ADAPTIVE_QUEUE, PostgresSchedulerBackend, SchedulerBackend};
use cc_lb_server::refresh::{LazyRefresher, LazyRefresherDeps, LazyRefresherParams};
use cc_lb_storage_api::MetaStore;
use sqlx::postgres::PgPoolOptions;
use sqlx::postgres::PgPoolOptions as StoragePgPoolOptions;
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use super::common::{
    TestResult, create_oauth_upstream, read_upstream_generation, stores_from_storage,
};
use super::fake::FakeAnthropic;
use super::scenario::run_race_scenario;
use super::worker::{OAuthWorkerProbe, OAuthWorkerState, spawn_oauth_worker};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_lazy_refresher_vs_proactive_apalis_oauth_refresh_race() -> TestResult<()> {
    with_postgres_database(run_postgres_race).await
}

async fn run_postgres_race(url: String) -> TestResult<()> {
    if !is_safe_database_url(&url) {
        eprintln!("SKIP: PostgreSQL URL is not a recognized local test database");
        return Ok(());
    }

    let databases = create_test_databases(&url).await?;
    let result = run_postgres_race_inner(&databases).await;
    let cleanup_result = drop_test_databases(&url, &databases).await;

    match (result, cleanup_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(test_error), Ok(())) => Err(test_error),
        (Ok(()), Err(cleanup_error)) => Err(cleanup_error),
        (Err(test_error), Err(cleanup_error)) => Err(format!(
            "{test_error}; failed to clean up temporary PostgreSQL databases: {cleanup_error}"
        )
        .into()),
    }
}

async fn run_postgres_race_inner(databases: &TestDatabases) -> TestResult<()> {
    let fake = FakeAnthropic::spawn().await?;
    let pool = PgPoolOptions::new()
        .max_connections(6)
        .connect(&databases.scheduler_url)
        .await?;
    let storage_pool = StoragePgPoolOptions::new()
        .max_connections(4)
        .connect(&databases.runtime_url)
        .await?;
    apalis_postgres::PostgresStorage::setup(&pool).await?;
    cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool).await?;
    let storage = Arc::new(cc_lb_storage_postgres::PostgresStorage::new(
        storage_pool.clone(),
        Arc::new(cc_lb_clock::SystemClock),
    ));
    storage.initialize().await?;

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
        Uuid::new_v4(),
        aead.clone(),
        oauth_cfg.clone(),
        backend.clone(),
        probe.clone(),
    );
    let cancel = CancellationToken::new();
    let worker = spawn_oauth_worker(&backend, state, cancel.clone())?;
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
    pool.close().await;
    storage_pool.close().await;
    result
}

struct TestDatabases {
    scheduler_url: String,
    runtime_url: String,
    scheduler_name: String,
    runtime_name: String,
}

async fn create_test_databases(base_url: &str) -> TestResult<TestDatabases> {
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(base_url)
        .await?;
    let suffix = Uuid::new_v4().simple().to_string();
    let scheduler_name = format!("scheduler_{suffix}");
    let runtime_name = format!("runtime_{suffix}");
    let scheduler_url = database_url(base_url, &scheduler_name)?;
    let runtime_url = database_url(base_url, &runtime_name)?;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"CREATE DATABASE "{scheduler_name}""#
    )))
    .execute(&admin)
    .await?;
    if let Err(error) = sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"CREATE DATABASE "{runtime_name}""#
    )))
    .execute(&admin)
    .await
    {
        let _ = drop_database(&admin, &scheduler_name).await;
        admin.close().await;
        return Err(error.into());
    }
    admin.close().await;

    Ok(TestDatabases {
        scheduler_url,
        runtime_url,
        scheduler_name,
        runtime_name,
    })
}

async fn drop_test_databases(base_url: &str, databases: &TestDatabases) -> TestResult<()> {
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(base_url)
        .await?;
    let scheduler_result = drop_database(&admin, &databases.scheduler_name).await;
    let runtime_result = drop_database(&admin, &databases.runtime_name).await;
    admin.close().await;
    scheduler_result?;
    runtime_result?;
    Ok(())
}

async fn drop_database(pool: &sqlx::PgPool, database: &str) -> Result<(), sqlx::Error> {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"DROP DATABASE IF EXISTS "{database}" WITH (FORCE)"#
    )))
    .execute(pool)
    .await?;
    Ok(())
}

fn is_safe_database_url(url: &str) -> bool {
    url.contains("localhost") || url.contains("127.0.0.1") || url.contains("cc_lb_test")
}

fn database_url(base_url: &str, database: &str) -> TestResult<String> {
    let mut url = Url::parse(base_url)?;
    url.set_path(database);
    Ok(url.to_string())
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

async fn with_postgres_database<F, Fut>(run: F) -> TestResult<()>
where
    F: FnOnce(String) -> Fut,
    Fut: Future<Output = TestResult<()>>,
{
    let database_url = std::env::var("CI_POSTGRES_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            std::env::var("DATABASE_URL")
                .ok()
                .filter(|value| !value.trim().is_empty())
        });
    let Some(database_url) = database_url else {
        eprintln!("SKIP: CI_POSTGRES_URL or DATABASE_URL not set for lazy/proactive race test");
        return Ok(());
    };
    run(database_url).await
}
