use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use cc_lb_scheduler::error::{Result, SchedulerError};
use cc_lb_scheduler::jobs::oauth_usage_poll::{
    OAuthUsagePollHandler, OAuthUsagePollJob, OAuthUsagePollObservation, compute_next_run_at,
};
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::state_stores::{
    OAuthUsagePollCursor, OAuthUsagePollCursorsStore, OAuthUsagePollScheduleConfig,
};
use proptest::prelude::*;
use uuid::Uuid;

mod jobs {
    pub mod oauth_usage_poll {
        use super::super::*;

        proptest! {
            #[test]
            fn compute_next_run_at_pure_function(now in 1_000_u64..1_000_000_u64, throttle_count in 1_u32..20_u32) {
                let upstream_id = Uuid::nil();
                let config = schedule_config();
                let success_cursor = OAuthUsagePollCursor {
                    upstream_id,
                    last_status: Some(200),
                    last_observed_at_unix_secs: Some(now),
                    recent_successes_unix_secs: vec![now - 240, now - 180, now - 120, now - 60, now],
                    ..OAuthUsagePollCursor::new(upstream_id)
                };
                let throttle_cursor = OAuthUsagePollCursor {
                    upstream_id,
                    last_status: Some(429),
                    last_throttle_at_unix_secs: Some(now),
                    last_throttle_count: throttle_count,
                    last_observed_at_unix_secs: Some(now),
                    ..OAuthUsagePollCursor::new(upstream_id)
                };

                let first = compute_next_run_at(now, &config, Some(&success_cursor));
                prop_assert_eq!(first, compute_next_run_at(now, &config, Some(&success_cursor)));
                prop_assert_eq!(first, now + 65);

                let throttle_next = compute_next_run_at(now, &config, Some(&throttle_cursor));
                prop_assert!(throttle_next >= now + 5);
                prop_assert!(throttle_next <= now + config.max_interval_secs);
            }
        }

        #[cfg(feature = "sqlite")]
        #[tokio::test]
        async fn cursor_survives_restart() -> Result<()> {
            let path = std::env::temp_dir().join(format!("oauth-usage-{}.db", Uuid::new_v4()));
            let url = format!("sqlite://{}?mode=rwc", path.display());
            let upstream_id = Uuid::new_v4();
            let calls = Arc::new(AtomicUsize::new(0));

            {
                let pool = sqlite_pool(&url).await?;
                migrate_sqlite(&pool).await?;
                let handler = sqlite_handler(pool, schedule_config());
                handler
                    .handle(
                        job(upstream_id),
                        1_000,
                        success_poller(Arc::clone(&calls), 1_000),
                    )
                    .await?;
            }

            let pool = sqlite_pool(&url).await?;
            let handler = sqlite_handler(pool.clone(), schedule_config());
            let outcome = handler
                .handle(
                    job(upstream_id),
                    1_001,
                    success_poller(Arc::clone(&calls), 1_001),
                )
                .await?;

            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(outcome, JobOutcome::Done);
            let cursor = OAuthUsagePollCursorsStore::new(pool)
                .read(upstream_id)
                .await?
                .expect("cursor exists");
            assert_eq!(
                compute_next_run_at(1_001, &schedule_config(), Some(&cursor)),
                1_060
            );
            let _ = std::fs::remove_file(path);
            Ok(())
        }

        #[cfg(feature = "sqlite")]
        #[tokio::test]
        async fn throttle_observation_persists() -> Result<()> {
            let pool = sqlite_memory().await?;
            let upstream_id = Uuid::new_v4();
            let calls = Arc::new(AtomicUsize::new(0));
            let handler = sqlite_handler(pool.clone(), schedule_config());

            let first = handler
                .handle(
                    job(upstream_id),
                    2_000,
                    throttle_poller(Arc::clone(&calls), 2_000),
                )
                .await?;
            let second = handler
                .handle(
                    job(upstream_id),
                    2_001,
                    throttle_poller(Arc::clone(&calls), 2_001),
                )
                .await?;
            let cursor = OAuthUsagePollCursorsStore::new(pool)
                .read(upstream_id)
                .await?
                .expect("cursor exists");

            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(cursor.recent_throttles_unix_secs, vec![2_000]);
            assert_eq!(first, JobOutcome::Done);
            assert_eq!(second, JobOutcome::Done);
            assert_eq!(
                compute_next_run_at(2_001, &schedule_config(), Some(&cursor)),
                2_300
            );
            Ok(())
        }

        #[cfg(feature = "sqlite")]
        #[tokio::test]
        async fn apalis_key_single_flight() -> Result<()> {
            use apalis_core::backend::TaskSink;
            use apalis_sqlite::SqliteStorage;

            let pool = sqlite_memory().await?;
            SqliteStorage::setup(&pool).await?;
            let upstream_id = Uuid::new_v4();
            let mut storage = SqliteStorage::<OAuthUsagePollJob, (), ()>::new(&pool);

            storage
                .push_task(job(upstream_id).into_apalis_task(1_000))
                .await
                .map_err(|error| SchedulerError::Job(error.to_string()))?;
            storage
                .push_task(job(upstream_id).into_apalis_task(1_000))
                .await
                .map_err(|error| SchedulerError::Job(error.to_string()))?;
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE idempotency_key = ?")
                    .bind(job(upstream_id).idempotency_key(1_000))
                    .fetch_one(&pool)
                    .await?;

            assert_eq!(count, 1);
            Ok(())
        }
    }
}

#[cfg(feature = "sqlite")]
async fn sqlite_memory() -> Result<sqlx::SqlitePool> {
    let pool = sqlite_pool("sqlite::memory:").await?;
    migrate_sqlite(&pool).await?;
    Ok(pool)
}

#[cfg(feature = "sqlite")]
async fn sqlite_pool(url: &str) -> Result<sqlx::SqlitePool> {
    Ok(sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await?)
}

#[cfg(feature = "sqlite")]
async fn migrate_sqlite(pool: &sqlx::SqlitePool) -> Result<()> {
    sqlx::raw_sql(include_str!(
        "../migrations/sqlite/0002_idempotency_tables.sql"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(feature = "sqlite")]
fn sqlite_handler(
    pool: sqlx::SqlitePool,
    config: OAuthUsagePollScheduleConfig,
) -> OAuthUsagePollHandler<sqlx::Sqlite> {
    OAuthUsagePollHandler::new(OAuthUsagePollCursorsStore::new(pool), config)
}

fn schedule_config() -> OAuthUsagePollScheduleConfig {
    OAuthUsagePollScheduleConfig::default()
}

fn job(upstream_id: Uuid) -> OAuthUsagePollJob {
    OAuthUsagePollJob {
        upstream_id,
        traceparent: None,
    }
}

fn success_poller(
    calls: Arc<AtomicUsize>,
    observed_at: u64,
) -> impl FnOnce(OAuthUsagePollJob) -> std::future::Ready<Result<OAuthUsagePollObservation>> {
    move |_| {
        calls.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(OAuthUsagePollObservation::Success {
            observed_at_unix_secs: observed_at,
            window_start_unix_millis: observed_at.saturating_mul(1_000),
            window_end_unix_millis: observed_at.saturating_add(60).saturating_mul(1_000),
        }))
    }
}

fn throttle_poller(
    calls: Arc<AtomicUsize>,
    observed_at: u64,
) -> impl FnOnce(OAuthUsagePollJob) -> std::future::Ready<Result<OAuthUsagePollObservation>> {
    move |_| {
        calls.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(OAuthUsagePollObservation::Throttled {
            observed_at_unix_secs: observed_at,
        }))
    }
}
