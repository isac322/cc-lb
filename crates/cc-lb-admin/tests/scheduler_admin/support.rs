use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use cc_lb_config::Config;
use cc_lb_scheduler::admin::SchedulerAdminHandle;
use cc_lb_scheduler::worker::{ENTITY_QUEUE, EntityJob, SchedulerBackend};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

pub const NOW_SECS: u64 = 1_800_000_000;

pub struct RouteFixture<Pool> {
    pub pool: Pool,
    pub handle: SchedulerAdminHandle,
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
        audit_sink: None,
        dynamic_view: crate::admin_test_common::dynamic_view_holder(&config),
        config: Arc::new(config),
        scheduler: Some(scheduler),
        admin_token: Some("test-token".to_owned()),
        start_time: std::time::Instant::now(),
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

#[cfg(feature = "sqlite")]
pub async fn sqlite_fixture()
-> Result<RouteFixture<scheduler_sqlx::SqlitePool>, Box<dyn std::error::Error>> {
    let pool = scheduler_sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    apalis_sqlite::SqliteStorage::setup(&pool).await?;
    cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool).await?;
    let storage =
        apalis_sqlite::SqliteStorage::<EntityJob, (), ()>::new_in_queue(&pool, ENTITY_QUEUE);
    let backend = SchedulerBackend::Sqlite(cc_lb_scheduler::worker::SqliteSchedulerStorage {
        pool: pool.clone(),
        storage,
    });
    Ok(RouteFixture {
        pool,
        handle: SchedulerAdminHandle::new(
            backend,
            Arc::new(cc_lb_scheduler::leader_election::LeaderElection::sqlite()),
        ),
    })
}

#[cfg(feature = "sqlite")]
pub async fn seed_sqlite_failed_warmup(
    pool: &scheduler_sqlx::SqlitePool,
) -> scheduler_sqlx::Result<()> {
    scheduler_sqlx::query(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, last_result, done_at, idempotency_key) \
         VALUES (?1, 'failed-warmup', 'entity:warmup', 'Failed', 5, 5, ?2, 'dial timeout', ?2, ?3)",
    )
    .bind(Vec::<u8>::new())
    .bind(i64::try_from(NOW_SECS).expect("test timestamp fits"))
    .bind("entity:warmup:test")
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(feature = "sqlite")]
pub async fn seed_sqlite_usage_rollup(
    pool: &scheduler_sqlx::SqlitePool,
) -> scheduler_sqlx::Result<()> {
    let payload = serde_json::to_vec(&cc_lb_scheduler::worker::SingletonJob::UsageRollup(
        cc_lb_scheduler::jobs::usage_rollup::UsageRollupTask::default(),
    ))
    .map_err(|error| scheduler_sqlx::Error::Protocol(error.to_string()))?;
    scheduler_sqlx::query(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, idempotency_key) \
         VALUES (?1, 'usage-rollup', ?2, 'Pending', 0, 1, ?3, 'singleton:usage_rollup')",
    )
    .bind(payload)
    .bind(cc_lb_scheduler::worker::SINGLETON_QUEUE)
    .bind(i64::try_from(NOW_SECS).expect("test timestamp fits"))
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(feature = "postgres")]
pub async fn postgres_fixture() -> Result<
    Option<(
        scheduler_sqlx::PgPool,
        String,
        RouteFixture<scheduler_sqlx::PgPool>,
    )>,
    Box<dyn std::error::Error>,
> {
    use std::str::FromStr as _;

    let Ok(url) = std::env::var("DATABASE_URL") else {
        return Ok(None);
    };
    if !(url.contains("localhost") || url.contains("127.0.0.1") || url.contains("cc_lb_test")) {
        return Ok(None);
    }
    let options = scheduler_sqlx::postgres::PgConnectOptions::from_str(&url)?;
    let admin = scheduler_sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options.clone())
        .await?;
    let db = format!("cc_lb_admin_scheduler_{}", uuid::Uuid::new_v4().simple());
    if scheduler_sqlx::query(&format!(r#"CREATE DATABASE "{db}""#))
        .execute(&admin)
        .await
        .is_err()
    {
        admin.close().await;
        return Ok(None);
    }
    let pool = scheduler_sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options.database(&db))
        .await?;
    apalis_postgres::PostgresStorage::setup(&pool).await?;
    cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool).await?;
    let storage = apalis_postgres::PostgresStorage::<EntityJob>::new_with_config(
        &pool,
        &apalis_postgres::Config::new(ENTITY_QUEUE),
    );
    let backend = SchedulerBackend::Postgres(cc_lb_scheduler::worker::PostgresSchedulerStorage {
        pool: pool.clone(),
        storage,
    });
    Ok(Some((
        admin,
        db,
        RouteFixture {
            pool,
            handle: SchedulerAdminHandle::new(
                backend,
                Arc::new(cc_lb_scheduler::leader_election::LeaderElection::sqlite()),
            ),
        },
    )))
}

#[cfg(feature = "sqlite")]
pub fn counter_value(rendered: &str, job_type: &str) -> f64 {
    rendered
        .lines()
        .find(|line| {
            line.starts_with("cclb_scheduler_failures_total{")
                && line.contains(&format!(r#"job_type="{job_type}""#))
        })
        .and_then(|line| line.split_whitespace().last())
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(0.0)
}
