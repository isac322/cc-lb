#![cfg(feature = "postgres")]

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::time::Duration;

use cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob;
use cc_lb_scheduler::jobs::watchdog::{OAuthRefreshWatchdogTarget, run_oauth_refresh_watchdog};
use cc_lb_scheduler::worker::{
    ADAPTIVE_QUEUE, AdaptiveJob, PostgresSchedulerBackend, SchedulerBackend, SchedulerPushTask,
};
use futures_util::FutureExt as _;
use sqlx::postgres::PgPoolOptions;
use tokio::time::timeout;
use url::Url;
use uuid::Uuid;

const TICK_UNIX_SECS: u64 = 1_800_000_000;

type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn postgres_oauth_refresh_watchdog_recovers_exhausted_failed_and_killed_same_generation_rows()
-> TestResult<()> {
    with_isolated_postgres(|pool, backend| {
        Box::pin(async move {
            for terminal_status in ["Failed", "Killed"] {
                let upstream_id = Uuid::new_v4();
                let target_expires = TICK_UNIX_SECS + 3_600;
                let generation = 3;
                let canonical_job =
                    OAuthRefreshJob::for_credential(upstream_id, generation, target_expires);
                let canonical_key =
                    canonical_job.generation_idempotency_key(generation, target_expires);
                let run_at = OAuthRefreshJob::run_at_for_expires_at(target_expires);

                backend
                    .push_adaptive_task(SchedulerPushTask {
                        args: AdaptiveJob::OAuthRefresh(canonical_job.clone()),
                        idempotency_key: Some(canonical_key.clone()),
                        run_at_unix_secs: Some(run_at),
                        max_attempts: Some(5),
                    })
                    .await?;

                // Retryable Failed (attempts < max_attempts) must stay covered and not duplicate.
                sqlx::query(
                    "UPDATE apalis.jobs SET status = 'Failed', attempts = 2, max_attempts = 5 WHERE job_type = $1 AND idempotency_key = $2",
                )
                .bind(ADAPTIVE_QUEUE)
                .bind(&canonical_key)
                .execute(&pool)
                .await?;
                let target = OAuthRefreshWatchdogTarget {
                    upstream_id,
                    expected_generation: generation,
                    expected_expires_at_unix_secs: target_expires,
                    is_anthropic_oauth: false,
                };
                let retryable =
                    run_oauth_refresh_watchdog(&backend, &[target], TICK_UNIX_SECS).await?;
                assert_eq!(retryable.seeded, 0);

                // Exhausted Failed or Killed at canonical key triggers distinct recovery row.
                let attempts = if terminal_status == "Failed" { 5 } else { 1 };
                sqlx::query(
                    "UPDATE apalis.jobs SET status = $1, attempts = $2, max_attempts = 5 WHERE job_type = $3 AND idempotency_key = $4",
                )
                .bind(terminal_status)
                .bind(attempts)
                .bind(ADAPTIVE_QUEUE)
                .bind(&canonical_key)
                .execute(&pool)
                .await?;

                let first = run_oauth_refresh_watchdog(&backend, &[target], TICK_UNIX_SECS).await?;
                assert_eq!(first.seeded, 1, "{terminal_status}: recovery must seed");
                assert_eq!(first.seeded_targets, vec![target]);

                let recovery_key_1 = canonical_job.watchdog_recovery_idempotency_key(
                    generation,
                    target_expires,
                    TICK_UNIX_SECS,
                );
                let row: (Vec<u8>, i64) = sqlx::query_as(
                    "SELECT job, EXTRACT(EPOCH FROM run_at)::BIGINT FROM apalis.jobs WHERE job_type = $1 AND idempotency_key = $2",
                )
                .bind(ADAPTIVE_QUEUE)
                .bind(&recovery_key_1)
                .fetch_one(&pool)
                .await?;
                let payload: serde_json::Value = serde_json::from_slice(&row.0)?;
                assert_eq!(
                    payload,
                    serde_json::to_value(AdaptiveJob::OAuthRefresh(canonical_job.clone()))?,
                );
                assert_eq!(u64::try_from(row.1)?, run_at);

                // Single-snapshot family helpers observe the active recovery row even while
                // the canonical row is terminal, including Running -> retryable Pending transitions.
                assert_eq!(
                    backend.adaptive_task_key_family_state(&canonical_key).await?,
                    cc_lb_scheduler::worker::AdaptiveTaskKeyState::Active
                );
                assert_eq!(
                    backend
                        .find_active_adaptive_key_in_family(&canonical_key)
                        .await?
                        .as_deref(),
                    Some(recovery_key_1.as_str())
                );

                // Subsequent tick sees the active recovery row and seeds nothing.
                let second =
                    run_oauth_refresh_watchdog(&backend, &[target], TICK_UNIX_SECS + 60).await?;
                assert_eq!(second.seeded, 0);

                // Rescheduling recovery row from Running back to retryable Pending stays Active.
                sqlx::query(
                    "UPDATE apalis.jobs SET status = 'Pending', attempts = 1, max_attempts = 5 WHERE job_type = $1 AND idempotency_key = $2",
                )
                .bind(ADAPTIVE_QUEUE)
                .bind(&recovery_key_1)
                .execute(&pool)
                .await?;
                assert_eq!(
                    backend.adaptive_task_key_family_state(&recovery_key_1).await?,
                    cc_lb_scheduler::worker::AdaptiveTaskKeyState::Active
                );

                // If the recovery row also exhausts, family becomes TerminalFailure and a later
                // tick seeds a new recovery row.
                sqlx::query(
                    "UPDATE apalis.jobs SET status = 'Failed', attempts = 5, max_attempts = 5 WHERE job_type = $1 AND idempotency_key = $2",
                )
                .bind(ADAPTIVE_QUEUE)
                .bind(&recovery_key_1)
                .execute(&pool)
                .await?;
                assert_eq!(
                    backend.adaptive_task_key_family_state(&canonical_key).await?,
                    cc_lb_scheduler::worker::AdaptiveTaskKeyState::TerminalFailure
                );
                let third =
                    run_oauth_refresh_watchdog(&backend, &[target], TICK_UNIX_SECS + 120).await?;
                assert_eq!(third.seeded, 1);

                let recovery_key_2 = canonical_job.watchdog_recovery_idempotency_key(
                    generation,
                    target_expires,
                    TICK_UNIX_SECS + 120,
                );
                let count2: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM apalis.jobs WHERE job_type = $1 AND idempotency_key = $2",
                )
                .bind(ADAPTIVE_QUEUE)
                .bind(&recovery_key_2)
                .fetch_one(&pool)
                .await?;
                assert_eq!(count2, 1);

                // Done recovery row takes precedence over Killed canonical sibling.
                sqlx::query(
                    "UPDATE apalis.jobs SET status = 'Done' WHERE job_type = $1 AND idempotency_key = $2",
                )
                .bind(ADAPTIVE_QUEUE)
                .bind(&recovery_key_2)
                .execute(&pool)
                .await?;
                assert_eq!(
                    backend.adaptive_task_key_family_state(&canonical_key).await?,
                    cc_lb_scheduler::worker::AdaptiveTaskKeyState::Done
                );
            }
            Ok(())
        })
    })
    .await
}

async fn with_isolated_postgres<F>(test: F) -> TestResult<()>
where
    F: FnOnce(
        sqlx::PgPool,
        SchedulerBackend,
    ) -> Pin<Box<dyn Future<Output = TestResult<()>> + Send>>,
{
    let database_url = std::env::var("CI_POSTGRES_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            std::env::var("DATABASE_URL")
                .ok()
                .filter(|value| !value.trim().is_empty())
        });
    let Some(admin_url) = database_url else {
        eprintln!("SKIP: CI_POSTGRES_URL or DATABASE_URL not set for watchdog_tests_postgres");
        return Ok(());
    };

    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&admin_url)
        .await?;
    let database_name = format!("cclb_wdog_{}", Uuid::new_v4().simple());
    let mut target_url = Url::parse(&admin_url)?;
    target_url.set_path(&database_name);
    crate::common::create_postgres_database(&admin, &database_name, "CC_LB_PG_SCHED_TEMPLATE")
        .await?;

    let test_result = timeout(
        Duration::from_secs(30),
        AssertUnwindSafe(async {
            let pool = PgPoolOptions::new()
                .max_connections(4)
                .connect(target_url.as_str())
                .await?;
            apalis_postgres::PostgresStorage::setup(&pool).await?;
            cc_lb_scheduler::migrations::apply_post_setup_migrations(&pool).await?;
            let backend = SchedulerBackend::Postgres(PostgresSchedulerBackend::new(pool.clone()));
            let res = test(pool.clone(), backend).await;
            pool.close().await;
            res
        })
        .catch_unwind(),
    )
    .await;

    let drop_res = sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)"#
    )))
    .execute(&admin)
    .await;
    admin.close().await;

    let outcome = match test_result {
        Ok(Ok(inner)) => inner,
        Ok(Err(_panic)) => Err("watchdog_tests_postgres panicked".into()),
        Err(elapsed) => Err(Box::new(elapsed) as Box<dyn std::error::Error + Send + Sync>),
    };
    drop_res?;
    outcome
}
