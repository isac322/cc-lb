#![cfg(feature = "sqlite")]

use std::sync::Arc;

use cc_lb_core::clock::SystemClock;
use cc_lb_scheduler::admin::SchedulerAdminHandle;
use cc_lb_scheduler::worker::{ADAPTIVE_QUEUE, AdaptiveJob, SchedulerBackend};
use uuid::Uuid;

#[tokio::test]
async fn next_run_for_upstream_returns_earliest_active_warmup() {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .expect("sqlite opens");
    apalis_sqlite::SqliteStorage::setup(&pool)
        .await
        .expect("apalis tables setup");
    cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool)
        .await
        .expect("scheduler migrations apply");
    let storage =
        apalis_sqlite::SqliteStorage::<AdaptiveJob, (), ()>::new_in_queue(&pool, ADAPTIVE_QUEUE);
    let handle = SchedulerAdminHandle::new(
        SchedulerBackend::Sqlite(cc_lb_scheduler::worker::SqliteSchedulerStorage {
            pool: pool.clone(),
            storage,
            clock: Arc::new(SystemClock),
        }),
        Arc::new(cc_lb_scheduler::leader_election::LeaderElection::sqlite()),
    );
    let upstream_id = Uuid::from_u128(0x1234_5678_90ab_cdef_1234_5678_90ab_cdef);

    seed_job(
        &pool,
        "pending-later",
        upstream_id,
        "Pending",
        0,
        5,
        1_800_000_100,
    )
    .await;
    seed_job(
        &pool,
        "queued-earlier",
        upstream_id,
        "Queued",
        0,
        5,
        1_800_000_050,
    )
    .await;
    seed_job(
        &pool,
        "failed-retry",
        upstream_id,
        "Failed",
        1,
        5,
        1_800_000_025,
    )
    .await;
    seed_job(
        &pool,
        "failed-exhausted",
        upstream_id,
        "Failed",
        5,
        5,
        1_800_000_001,
    )
    .await;
    seed_job(
        &pool,
        "done-ignored",
        upstream_id,
        "Done",
        0,
        5,
        1_800_000_000,
    )
    .await;
    seed_other_upstream(&pool, upstream_id).await;

    let next_run = handle
        .next_run_for_upstream(upstream_id, "warmup")
        .await
        .expect("next run query succeeds");

    assert_eq!(next_run, Some(1_800_000_025));
}

#[tokio::test]
async fn next_run_for_upstream_returns_none_without_active_warmup() {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .expect("sqlite opens");
    apalis_sqlite::SqliteStorage::setup(&pool)
        .await
        .expect("apalis tables setup");
    cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool)
        .await
        .expect("scheduler migrations apply");
    let storage =
        apalis_sqlite::SqliteStorage::<AdaptiveJob, (), ()>::new_in_queue(&pool, ADAPTIVE_QUEUE);
    let handle = SchedulerAdminHandle::new(
        SchedulerBackend::Sqlite(cc_lb_scheduler::worker::SqliteSchedulerStorage {
            pool,
            storage,
            clock: Arc::new(SystemClock),
        }),
        Arc::new(cc_lb_scheduler::leader_election::LeaderElection::sqlite()),
    );

    let next_run = handle
        .next_run_for_upstream(Uuid::new_v4(), "warmup")
        .await
        .expect("next run query succeeds");

    assert_eq!(next_run, None);
}

async fn seed_job(
    pool: &sqlx::SqlitePool,
    id: &str,
    upstream_id: Uuid,
    status: &str,
    attempts: i64,
    max_attempts: i64,
    run_at: i64,
) {
    sqlx::query(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, idempotency_key) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
    )
    .bind(Vec::<u8>::new())
    .bind(id)
    .bind(ADAPTIVE_QUEUE)
    .bind(status)
    .bind(attempts)
    .bind(max_attempts)
    .bind(run_at)
    .bind(format!("adaptive:warmup:{upstream_id}:{run_at}"))
    .execute(pool)
    .await
    .expect("job inserts");
}

async fn seed_other_upstream(pool: &sqlx::SqlitePool, upstream_id: Uuid) {
    let other_upstream_id = Uuid::from_u128(upstream_id.as_u128() + 1);
    seed_job(
        pool,
        "other-upstream",
        other_upstream_id,
        "Pending",
        0,
        5,
        1_800_000_010,
    )
    .await;
}
