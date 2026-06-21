use apalis_sqlite::SqliteStorage;
use sqlx::SqlitePool;
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
async fn adds_and_prunes_upstream_entity_jobs() -> Result<()> {
    let pool = setup().await?;
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
    assert_eq!(active_job_types(&pool).await?, vec!["entity"; 4]);

    upstreams.replace(Vec::new());
    let pruned = handler
        .handle(SchedulerReconcileJob::default(), NOW_SECS + 5)
        .await;

    assert_done(&pruned, 0, 3, 0);
    assert_eq!(active_keys(&pool).await?, compat_keys_only());
    assert_eq!(active_job_types(&pool).await?, vec!["entity"]);
    Ok(())
}

#[tokio::test]
async fn admin_sla_uses_five_second_reconcile_interval() -> Result<()> {
    let pool = setup().await?;
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
}

#[tokio::test]
async fn surfaces_exhausted_apalis_jobs_once() -> Result<()> {
    let pool = setup().await?;
    seed_failed_job(&pool, "entity", "entity:oauth_refresh:missing").await?;
    let handler =
        SchedulerReconcileJobHandler::new(pool.clone(), FakeReconcileUpstreams::new(Vec::new()));

    let first = handler
        .handle(SchedulerReconcileJob::default(), NOW_SECS)
        .await;
    let second = handler
        .handle(SchedulerReconcileJob::default(), NOW_SECS)
        .await;

    assert_done(&first, 1, 0, 1);
    assert_done(&second, 0, 0, 0);
    assert_eq!(failure_count(&pool).await?, 1);
    assert_eq!(
        failure_job_types(&pool).await?,
        vec!["entity:oauth_refresh"]
    );
    Ok(())
}

async fn setup() -> Result<SqlitePool> {
    let pool = SqlitePool::connect(":memory:").await?;
    SqliteStorage::setup(&pool).await?;
    apply_post_setup_migrations(&pool).await?;
    sqlx::raw_sql(include_str!(
        "../../../../migrations/sqlite/0002_idempotency_tables.sql"
    ))
    .execute(&pool)
    .await?;
    Ok(pool)
}

async fn active_keys(pool: &SqlitePool) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar("SELECT idempotency_key FROM Jobs WHERE status IN ('Pending','Running','Queued') ORDER BY idempotency_key").fetch_all(pool).await?)
}

async fn active_job_types(pool: &SqlitePool) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar("SELECT job_type FROM Jobs WHERE status IN ('Pending','Running','Queued') ORDER BY idempotency_key").fetch_all(pool).await?)
}

async fn has_key(pool: &SqlitePool, key: String) -> Result<bool> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE idempotency_key = ?1 AND status IN ('Pending','Running','Queued')").bind(key).fetch_one(pool).await?;
    Ok(count > 0)
}

async fn seed_failed_job(pool: &SqlitePool, job_type: &str, key: &str) -> Result<()> {
    sqlx::query("INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, last_result, done_at, idempotency_key) VALUES (?1, 'failed-job', ?2, 'Failed', 5, 5, ?3, ?4, ?3, ?5)")
        .bind(Vec::<u8>::new())
        .bind(job_type)
        .bind(i64::try_from(NOW_SECS).expect("test timestamp fits i64"))
        .bind("token endpoint 401")
        .bind(key)
        .execute(pool)
        .await?;
    Ok(())
}

async fn failure_count(pool: &SqlitePool) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT COUNT(*) FROM scheduler_failures")
            .fetch_one(pool)
            .await?,
    )
}

async fn failure_job_types(pool: &SqlitePool) -> Result<Vec<String>> {
    Ok(
        sqlx::query_scalar("SELECT job_type FROM scheduler_failures ORDER BY job_type")
            .fetch_all(pool)
            .await?,
    )
}
