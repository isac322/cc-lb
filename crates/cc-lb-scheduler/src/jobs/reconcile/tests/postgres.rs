use std::str::FromStr as _;

use apalis_postgres::PostgresStorage;
use chrono::{DateTime, Utc};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

use super::{
    FakeReconcileUpstreams, NOW_SECS, assert_done, compat_keys_only, expected_keys, oauth_record,
    warmup_key,
};
use crate::jobs::reconcile::{
    SchedulerReconcileConfig, SchedulerReconcileJob, SchedulerReconcileJobHandler,
};
use crate::{error::Result, migrations::apply_post_setup_migrations};

#[tokio::test]
async fn adds_and_prunes_upstream_entity_jobs()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    with_database(|pool| {
        Box::pin(async move {
            let upstream_id = Uuid::new_v4();
            let upstreams = FakeReconcileUpstreams::new(vec![oauth_record(upstream_id, true)]);
            let handler = SchedulerReconcileJobHandler::new(pool.clone(), upstreams.clone());
            let first = handler
                .handle(SchedulerReconcileJob::default(), NOW_SECS)
                .await;
            let second = handler
                .handle(SchedulerReconcileJob::default(), NOW_SECS)
                .await;
            assert_done(&first, 4, 0, 0);
            assert_done(&second, 0, 0, 0);
            assert_eq!(active_keys(&pool).await?, expected_keys(upstream_id));
            upstreams.replace(Vec::new());
            let pruned = handler
                .handle(SchedulerReconcileJob::default(), NOW_SECS + 5)
                .await;
            assert_done(&pruned, 0, 3, 0);
            assert_eq!(active_keys(&pool).await?, compat_keys_only());
            Ok(())
        })
    })
    .await
}

#[tokio::test]
async fn admin_sla_uses_five_second_reconcile_interval()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    with_database(|pool| {
        Box::pin(async move {
            let upstream_id = Uuid::new_v4();
            let upstreams = FakeReconcileUpstreams::new(vec![oauth_record(upstream_id, false)]);
            let config = SchedulerReconcileConfig::new(true, 5);
            let handler =
                SchedulerReconcileJobHandler::with_config(pool.clone(), upstreams.clone(), config);
            let before = handler
                .handle(SchedulerReconcileJob::default(), NOW_SECS)
                .await;
            assert_done(&before, 3, 0, 0);
            assert!(!has_key(&pool, warmup_key(upstream_id)).await?);
            upstreams.replace(vec![oauth_record(upstream_id, true)]);
            let after = handler
                .handle(
                    SchedulerReconcileJob::default(),
                    NOW_SECS + config.reconcile_interval_secs,
                )
                .await;
            assert_done(&after, 1, 0, 0);
            assert!(has_key(&pool, warmup_key(upstream_id)).await?);
            Ok(())
        })
    })
    .await
}

#[tokio::test]
async fn surfaces_exhausted_apalis_jobs_once() -> std::result::Result<(), Box<dyn std::error::Error>>
{
    with_database(|pool| {
        Box::pin(async move {
            seed_failed_job(
                &pool,
                "entity:oauth_refresh",
                "entity:oauth_refresh:missing",
            )
            .await?;
            let handler = SchedulerReconcileJobHandler::new(
                pool.clone(),
                FakeReconcileUpstreams::new(Vec::new()),
            );
            let first = handler
                .handle(SchedulerReconcileJob::default(), NOW_SECS)
                .await;
            let second = handler
                .handle(SchedulerReconcileJob::default(), NOW_SECS)
                .await;
            assert_done(&first, 1, 0, 1);
            assert_done(&second, 0, 0, 0);
            assert_eq!(failure_count(&pool).await?, 1);
            Ok(())
        })
    })
    .await
}

async fn with_database<F>(test: F) -> std::result::Result<(), Box<dyn std::error::Error>>
where
    F: FnOnce(PgPool) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>,
{
    let Ok(url) = std::env::var("DATABASE_URL") else {
        return Ok(());
    };
    if !(url.contains("localhost") || url.contains("127.0.0.1") || url.contains("cc_lb_test")) {
        return Ok(());
    }
    let db = format!("cc_lb_scheduler_reconcile_{}", Uuid::new_v4().simple());
    let options = PgConnectOptions::from_str(&url)?;
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options.clone())
        .await?;
    if sqlx::query(&format!(r#"CREATE DATABASE "{db}""#))
        .execute(&admin)
        .await
        .is_err()
    {
        admin.close().await;
        return Ok(());
    }
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options.database(&db))
        .await?;
    PostgresStorage::setup(&pool).await?;
    apply_post_setup_migrations(&pool).await?;
    sqlx::raw_sql(include_str!(
        "../../../../migrations/postgres/0002_idempotency_tables.sql"
    ))
    .execute(&pool)
    .await?;
    let result = test(pool.clone()).await;
    pool.close().await;
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{db}""#))
        .execute(&admin)
        .await?;
    admin.close().await;
    result.map_err(Into::into)
}

async fn active_keys(pool: &PgPool) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar("SELECT idempotency_key FROM apalis.jobs WHERE status IN ('Pending','Running','Queued') ORDER BY idempotency_key").fetch_all(pool).await?)
}

async fn has_key(pool: &PgPool, key: String) -> Result<bool> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM apalis.jobs WHERE idempotency_key = $1 AND status IN ('Pending','Running','Queued')").bind(key).fetch_one(pool).await?;
    Ok(count > 0)
}

async fn seed_failed_job(pool: &PgPool, job_type: &str, key: &str) -> Result<()> {
    sqlx::query("INSERT INTO apalis.jobs (job, id, job_type, status, attempts, max_attempts, run_at, last_result, done_at, idempotency_key) VALUES ($1, 'failed-job', $2, 'Failed', 5, 5, $3, $4, $3, $5)")
        .bind(Vec::<u8>::new())
        .bind(job_type)
        .bind(DateTime::<Utc>::from_timestamp(i64::try_from(NOW_SECS).expect("test timestamp fits i64"), 0).expect("test timestamp"))
        .bind(serde_json::json!("token endpoint 401"))
        .bind(key)
        .execute(pool)
        .await?;
    Ok(())
}

async fn failure_count(pool: &PgPool) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT COUNT(*) FROM scheduler_failures")
            .fetch_one(pool)
            .await?,
    )
}
