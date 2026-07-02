use cc_lb_scheduler::admin::SchedulerAdminHandle;
use cc_lb_scheduler::worker::{ADAPTIVE_QUEUE, AdaptiveJob, SchedulerBackend};
use uuid::Uuid;

pub const NEXT_SCHEDULED_AT: i64 = 1_900_000_000;

pub async fn scheduler_with_next_warmup(
    upstream_id: Uuid,
    clock: cc_lb_core::ClockHandle,
) -> SchedulerAdminHandle {
    let pool = scheduler_sqlx::sqlite::SqlitePoolOptions::new()
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
    scheduler_sqlx::query(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, idempotency_key) \
         VALUES (?1, 'next-warmup', ?2, 'Pending', 0, 3, ?3, ?4)",
    )
    .bind(Vec::<u8>::new())
    .bind(ADAPTIVE_QUEUE)
    .bind(NEXT_SCHEDULED_AT)
    .bind(format!("adaptive:warmup:{upstream_id}:seed"))
    .execute(&pool)
    .await
    .expect("scheduler warmup job seeds");
    let storage =
        apalis_sqlite::SqliteStorage::<AdaptiveJob, (), ()>::new_in_queue(&pool, ADAPTIVE_QUEUE);
    SchedulerAdminHandle::new(SchedulerBackend::Sqlite(
        cc_lb_scheduler::worker::SqliteSchedulerStorage {
            pool,
            storage,
            clock,
        },
    ))
}
