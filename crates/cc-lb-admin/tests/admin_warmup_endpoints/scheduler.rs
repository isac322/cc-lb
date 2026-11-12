use cc_lb_scheduler::admin::SchedulerAdminHandle;
use cc_lb_scheduler::worker::{ADAPTIVE_QUEUE, SchedulerBackend};
use uuid::Uuid;

pub const NEXT_SCHEDULED_AT: i64 = 1_900_000_000;
pub const NEXT_THIRD_PARTY_SCHEDULED_AT: i64 = 1_900_003_600;

pub async fn scheduler_with_next_warmup(
    upstream_id: Uuid,
    clock: cc_lb_clock::ClockHandle,
) -> SchedulerAdminHandle {
    scheduler_with_jobs(
        &[(
            format!("adaptive:warmup:{upstream_id}:seed"),
            NEXT_SCHEDULED_AT,
        )],
        clock,
    )
    .await
}

/// Seeds one pending primary-lane and one pending third-party-lane warm-up job.
pub async fn scheduler_with_next_lane_warmups(
    upstream_id: Uuid,
    clock: cc_lb_clock::ClockHandle,
) -> SchedulerAdminHandle {
    scheduler_with_jobs(
        &[
            (
                format!("adaptive:warmup:{upstream_id}:seed"),
                NEXT_SCHEDULED_AT,
            ),
            (
                format!("adaptive:warmup_third_party:{upstream_id}:seed"),
                NEXT_THIRD_PARTY_SCHEDULED_AT,
            ),
        ],
        clock,
    )
    .await
}

async fn scheduler_with_jobs(
    jobs: &[(String, i64)],
    clock: cc_lb_clock::ClockHandle,
) -> SchedulerAdminHandle {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .expect("scheduler sqlite opens");
    apalis_sqlite::SqliteStorage::setup(&pool)
        .await
        .expect("apalis sqlite schema initializes");
    cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool)
        .await
        .expect("scheduler migrations initialize");
    for (index, (idempotency_key, run_at)) in jobs.iter().enumerate() {
        sqlx::query(
            "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, idempotency_key) \
             VALUES (?1, ?2, ?3, 'Pending', 0, 3, ?4, ?5)",
        )
        .bind(Vec::<u8>::new())
        .bind(format!("next-warmup-{index}"))
        .bind(ADAPTIVE_QUEUE)
        .bind(*run_at)
        .bind(idempotency_key)
        .execute(&pool)
        .await
        .expect("scheduler warmup job seeds");
    }
    SchedulerAdminHandle::new(SchedulerBackend::Sqlite(
        cc_lb_scheduler::worker::SqliteSchedulerBackend::new(pool, clock),
    ))
}
