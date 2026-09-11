use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use axum::response::Response;
use cc_lb_config::Config;
use cc_lb_scheduler::admin::SchedulerAdminHandle;
use cc_lb_scheduler::worker::SchedulerBackend;
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

pub const NOW_SECS: u64 = 1_800_000_000;

pub struct RouteFixture<Pool> {
    pub pool: Pool,
    pub handle: SchedulerAdminHandle,
}

#[cfg(feature = "postgres")]
pub struct PostgresRouteFixture {
    pub pool: scheduler_sqlx::PgPool,
    pub handle: SchedulerAdminHandle,
    database_name: String,
    base: Option<cc_lb_storage_conformance::PostgresFixture>,
}

#[cfg(feature = "postgres")]
impl PostgresRouteFixture {
    pub async fn teardown(mut self) -> anyhow::Result<()> {
        self.pool.close().await;
        let base = self
            .base
            .take()
            .expect("PostgresRouteFixture teardown called once");
        cleanup_database_and_base(base, &self.database_name).await
    }
}

pub fn app_with_scheduler(scheduler: SchedulerAdminHandle) -> axum::Router {
    let config = Config::default();
    let state = cc_lb_admin::AdminState {
        storage: None,
        key_store: None,
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: crate::admin_test_common::limit_engine(),
        lifecycle: None,
        subscription_metadata_hook: None,
        lazy_refresher: None,
        runtime: None,
        data_dir: None,
        warmup_dialect_dispatcher: None,
        dynamic_view: crate::admin_test_common::dynamic_view_holder(&config),
        config: Arc::new(config),
        scheduler: Some(scheduler),
        admin_auth: crate::admin_test_common::static_token_auth("test-token"),
        start_time: std::time::Instant::now(),
        event_bus: None,
        storage_tail: cc_lb_admin::events::storage_tail_channel(),
        clock: Arc::new(cc_lb_clock::SystemClock),
    };
    cc_lb_admin::router(state)
}

pub async fn authed_json(
    app: axum::Router,
    method: &str,
    uri: &str,
    headers: &[(&str, &str)],
) -> Result<(StatusCode, Value), Box<dyn std::error::Error>> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, "Bearer test-token");
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    let response = app.oneshot(builder.body(Body::empty())?).await?;
    let status = response.status();
    let body = response.into_body().collect().await?.to_bytes();
    let value = serde_json::from_slice(&body)?;
    Ok((status, value))
}

pub async fn request(
    app: axum::Router,
    method: &str,
    uri: &str,
    headers: &[(&str, &str)],
) -> Result<Response, Box<dyn std::error::Error>> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, "Bearer test-token");
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    Ok(app.oneshot(builder.body(Body::empty())?).await?)
}

#[cfg(feature = "sqlite")]
pub async fn sqlite_fixture()
-> Result<RouteFixture<scheduler_sqlx::SqlitePool>, Box<dyn std::error::Error>> {
    let pool = scheduler_sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    apalis_sqlite::SqliteStorage::setup(&pool).await?;
    cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool).await?;
    let backend = SchedulerBackend::Sqlite(cc_lb_scheduler::worker::SqliteSchedulerBackend::new(
        pool.clone(),
        std::sync::Arc::new(cc_lb_clock::SystemClock),
    ));
    Ok(RouteFixture {
        pool,
        handle: SchedulerAdminHandle::new(backend),
    })
}

#[cfg(feature = "sqlite")]
pub async fn seed_sqlite_failed_warmup(
    pool: &scheduler_sqlx::SqlitePool,
) -> scheduler_sqlx::Result<()> {
    scheduler_sqlx::query(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, last_result, done_at, idempotency_key) \
         VALUES (?1, 'failed-warmup', 'adaptive', 'Failed', 5, 5, ?2, 'dial timeout', ?2, ?3)",
    )
    .bind(Vec::<u8>::new())
    .bind(i64::try_from(NOW_SECS).expect("test timestamp fits"))
    .bind("adaptive:warmup:test")
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(feature = "postgres")]
pub async fn seed_postgres_failed_warmup(
    pool: &scheduler_sqlx::PgPool,
) -> scheduler_sqlx::Result<()> {
    scheduler_sqlx::query(
        "INSERT INTO apalis.jobs (job, id, job_type, status, attempts, max_attempts, run_at, last_result, done_at, idempotency_key) \
         VALUES ($1, 'failed-warmup', 'adaptive', 'Failed', 5, 5, to_timestamp($2), '{\"Err\":\"dial timeout\"}'::jsonb, to_timestamp($2), $3)",
    )
    .bind(Vec::<u8>::new())
    .bind(i64::try_from(NOW_SECS).expect("test timestamp fits"))
    .bind("adaptive:warmup:pg")
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(feature = "sqlite")]
pub async fn seed_sqlite_usage_rollup(
    pool: &scheduler_sqlx::SqlitePool,
) -> scheduler_sqlx::Result<()> {
    let payload = serde_json::to_vec(&cc_lb_scheduler::worker::CronJob::UsageRollup(
        cc_lb_scheduler::jobs::usage_rollup::UsageRollupTask::default(),
    ))
    .map_err(|error| scheduler_sqlx::Error::Protocol(error.to_string()))?;
    scheduler_sqlx::query(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, idempotency_key) \
         VALUES (?1, 'usage-rollup', ?2, 'Pending', 0, 1, ?3, 'cron:usage_rollup')",
    )
    .bind(payload)
    .bind(cc_lb_scheduler::worker::CRON_QUEUE)
    .bind(i64::try_from(NOW_SECS).expect("test timestamp fits"))
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(feature = "postgres")]
pub async fn postgres_fixture() -> anyhow::Result<PostgresRouteFixture> {
    use std::str::FromStr as _;

    use anyhow::Context as _;
    use scheduler_sqlx::postgres::PgConnectOptions;

    let base = cc_lb_storage_conformance::postgres_fixture().await?;
    let connect_options = match PgConnectOptions::from_str(base.database_url())
        .context("parse CI_POSTGRES_URL for scheduler admin fixture")
    {
        Ok(options) => options,
        Err(error) => {
            let cleanup = base.teardown().await;
            return Err(with_cleanup_error(error, cleanup, "teardown base fixture"));
        }
    };
    let endpoint = format!(
        "{} {}",
        connect_options.get_host(),
        connect_options.get_database().unwrap_or_default()
    );
    if !is_safe_database_url(&endpoint) {
        let error =
            anyhow::anyhow!("CI_POSTGRES_URL must identify localhost, 127.0.0.1, or cc_lb_test");
        let cleanup = base.teardown().await;
        return Err(with_cleanup_error(error, cleanup, "teardown base fixture"));
    }

    let suffix = base.schema_name().trim_start_matches("cc_lb_test_");
    let database_name = format!("cc_lb_scheduler_admin_{suffix}");
    if let Err(error) = create_database(base.pool(), &database_name).await {
        let cleanup = base.teardown().await;
        return Err(with_cleanup_error(error, cleanup, "teardown base fixture"));
    }

    match setup_postgres_database(base.database_url(), &connect_options, &database_name).await {
        Ok((pool, handle)) => Ok(PostgresRouteFixture {
            pool,
            handle,
            database_name,
            base: Some(base),
        }),
        Err(error) => {
            let cleanup = cleanup_database_and_base(base, &database_name).await;
            Err(with_cleanup_error(
                error,
                cleanup,
                "drop scheduler database and teardown base fixture",
            ))
        }
    }
}

#[cfg(feature = "postgres")]
async fn setup_postgres_database(
    database_url: &str,
    connect_options: &scheduler_sqlx::postgres::PgConnectOptions,
    database_name: &str,
) -> anyhow::Result<(scheduler_sqlx::PgPool, SchedulerAdminHandle)> {
    use std::str::FromStr as _;

    use anyhow::Context as _;
    use scheduler_sqlx::postgres::PgPoolOptions;

    let storage_options = sqlx::postgres::PgConnectOptions::from_str(database_url)
        .context("parse CI_POSTGRES_URL for cc-lb storage migration pool")?
        .database(database_name)
        .options([("search_path", "public")]);
    let storage_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(storage_options)
        .await
        .context("connect cc-lb storage migration pool")?;
    let storage_setup = async {
        sqlx::migrate!("../cc-lb-storage-postgres/migrations")
            .run(&storage_pool)
            .await
            .context("apply cc-lb storage migrations in public schema")?;
        sqlx::query("CREATE SCHEMA cc_lb_scheduler")
            .execute(&storage_pool)
            .await
            .context("create scheduler migration schema")?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    storage_pool.close().await;
    storage_setup?;

    let scheduler_options = connect_options
        .clone()
        .database(database_name)
        .options([("search_path", "cc_lb_scheduler,apalis,public")]);
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(scheduler_options)
        .await
        .context("connect scheduler migration pool")?;
    let setup = async {
        apalis_postgres::PostgresStorage::setup(&pool)
            .await
            .context("apply Apalis migrations")?;
        cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool)
            .await
            .context("apply scheduler post-setup migrations")?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    if let Err(error) = setup {
        pool.close().await;
        return Err(error);
    }

    let backend = SchedulerBackend::Postgres(
        cc_lb_scheduler::worker::PostgresSchedulerBackend::new(pool.clone()),
    );
    Ok((pool, SchedulerAdminHandle::new(backend)))
}

#[cfg(feature = "postgres")]
async fn create_database(pool: &sqlx::PgPool, database_name: &str) -> anyhow::Result<()> {
    use anyhow::Context as _;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE DATABASE {}",
        quote_identifier(database_name)
    )))
    .execute(pool)
    .await
    .with_context(|| format!("create scheduler test database {database_name}"))?;
    Ok(())
}

#[cfg(feature = "postgres")]
async fn cleanup_database_and_base(
    base: cc_lb_storage_conformance::PostgresFixture,
    database_name: &str,
) -> anyhow::Result<()> {
    use anyhow::Context as _;

    let drop_database = sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP DATABASE IF EXISTS {}",
        quote_identifier(database_name)
    )))
    .execute(base.pool())
    .await
    .map(|_| ())
    .with_context(|| format!("drop scheduler test database {database_name}"));
    let teardown_base = base.teardown().await;
    match (drop_database, teardown_base) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(teardown_error)) => Err(error.context(format!(
            "also failed to teardown base PostgreSQL fixture: {teardown_error:#}"
        ))),
    }
}

#[cfg(feature = "postgres")]
fn with_cleanup_error(
    error: anyhow::Error,
    cleanup: anyhow::Result<()>,
    cleanup_action: &str,
) -> anyhow::Error {
    match cleanup {
        Ok(()) => error,
        Err(cleanup_error) => error.context(format!(
            "also failed to {cleanup_action}: {cleanup_error:#}"
        )),
    }
}

#[cfg(feature = "postgres")]
fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

#[cfg(feature = "postgres")]
fn is_safe_database_url(endpoint: &str) -> bool {
    endpoint.contains("localhost")
        || endpoint.contains("127.0.0.1")
        || endpoint.contains("cc_lb_test")
}
