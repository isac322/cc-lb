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
    let url = std::env::var("CI_POSTGRES_URL").map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "postgres lazy/proactive race test requires CI_POSTGRES_URL for the prepared PostgreSQL service",
        )
    })?;
    if url.trim().is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "postgres lazy/proactive race test requires a non-empty CI_POSTGRES_URL for the prepared PostgreSQL service",
        )
        .into());
    }
    run_postgres_race(url).await
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
    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"CREATE DATABASE "{scheduler_db}""#
    )))
    .execute(&admin)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"CREATE DATABASE "{runtime_db}""#
    )))
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
