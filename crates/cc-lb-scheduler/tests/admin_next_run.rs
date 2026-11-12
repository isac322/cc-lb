#![cfg(feature = "sqlite")]

use std::sync::Arc;

use cc_lb_clock::SystemClock;
use cc_lb_scheduler::admin::SchedulerAdminHandle;
use cc_lb_scheduler::jobs::warmup::UpstreamWarmupJob;
use cc_lb_scheduler::worker::{ADAPTIVE_QUEUE, SchedulerBackend};
use cc_lb_storage_api::WarmupLane;
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
    let handle = SchedulerAdminHandle::new(SchedulerBackend::Sqlite(
        cc_lb_scheduler::worker::SqliteSchedulerBackend::new(pool.clone(), Arc::new(SystemClock)),
    ));
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
    let handle = SchedulerAdminHandle::new(SchedulerBackend::Sqlite(
        cc_lb_scheduler::worker::SqliteSchedulerBackend::new(pool, Arc::new(SystemClock)),
    ));

    let next_run = handle
        .next_run_for_upstream(Uuid::new_v4(), "warmup")
        .await
        .expect("next run query succeeds");

    assert_eq!(next_run, None);
}

#[tokio::test]
async fn next_run_for_upstream_isolates_warmup_lanes() {
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
    let handle = SchedulerAdminHandle::new(SchedulerBackend::Sqlite(
        cc_lb_scheduler::worker::SqliteSchedulerBackend::new(pool.clone(), Arc::new(SystemClock)),
    ));
    let upstream_id = Uuid::from_u128(0x1234_5678_90ab_cdef_1234_5678_90ab_cdef);

    // The third-party job runs earlier; the primary lane must not report it.
    seed_lane_job(
        &pool,
        "third-party-earlier",
        upstream_id,
        WarmupLane::ThirdParty,
        1_800_000_010,
    )
    .await;
    seed_lane_job(
        &pool,
        "primary-later",
        upstream_id,
        WarmupLane::Primary,
        1_800_000_500,
    )
    .await;

    let primary = handle
        .next_run_for_upstream(upstream_id, WarmupLane::Primary.job_kind())
        .await
        .expect("primary next run query succeeds");
    let third_party = handle
        .next_run_for_upstream(upstream_id, WarmupLane::ThirdParty.job_kind())
        .await
        .expect("third-party next run query succeeds");

    assert_eq!(primary, Some(1_800_000_500));
    assert_eq!(third_party, Some(1_800_000_010));
}

#[tokio::test]
async fn primary_next_run_ignores_third_party_only_upstream() {
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
    let handle = SchedulerAdminHandle::new(SchedulerBackend::Sqlite(
        cc_lb_scheduler::worker::SqliteSchedulerBackend::new(pool.clone(), Arc::new(SystemClock)),
    ));
    let upstream_id = Uuid::new_v4();
    seed_lane_job(
        &pool,
        "third-party-only",
        upstream_id,
        WarmupLane::ThirdParty,
        1_800_000_010,
    )
    .await;

    let primary = handle
        .next_run_for_upstream(upstream_id, "warmup")
        .await
        .expect("primary next run query succeeds");

    assert_eq!(primary, None);
}

async fn seed_lane_job(
    pool: &sqlx::SqlitePool,
    id: &str,
    upstream_id: Uuid,
    lane: WarmupLane,
    run_at: i64,
) {
    let key = UpstreamWarmupJob::new_for_lane(upstream_id, 0, lane)
        .idempotency_key(u64::try_from(run_at).expect("non-negative run_at"));
    sqlx::query(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, idempotency_key) \
         VALUES (?1, ?2, ?3, 'Pending', 0, 5, ?4, ?5)",
    )
    .bind(Vec::<u8>::new())
    .bind(id)
    .bind(ADAPTIVE_QUEUE)
    .bind(run_at)
    .bind(key)
    .execute(pool)
    .await
    .expect("job inserts");
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
