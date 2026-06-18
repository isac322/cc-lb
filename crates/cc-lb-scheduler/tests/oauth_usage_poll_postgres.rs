#![cfg(feature = "postgres")]

use std::str::FromStr;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use cc_lb_scheduler::error::Result;
use cc_lb_scheduler::idempotency::{OAuthUsagePollCursorsStore, OAuthUsagePollScheduleConfig};
use cc_lb_scheduler::jobs::oauth_usage_poll::{
    OAuthUsagePollHandler, OAuthUsagePollJob, OAuthUsagePollObservation,
};
use cc_lb_scheduler::retry::JobOutcome;
use sqlx::{Executor, PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

mod jobs {
    pub mod oauth_usage_poll {
        use super::super::*;

        #[tokio::test]
        async fn postgres_cursor_survives_restart() -> Result<()> {
            let Some((admin, pool, schema)) = postgres_pool().await? else {
                eprintln!("SKIP: DATABASE_URL not set - skipping postgres oauth usage poll test");
                return Ok(());
            };
            let upstream_id = Uuid::new_v4();
            let calls = Arc::new(AtomicUsize::new(0));

            let first = postgres_handler(pool.clone());
            first
                .handle(
                    job(upstream_id),
                    1_000,
                    success_poller(Arc::clone(&calls), 1_000),
                )
                .await?;
            let second = postgres_handler(pool);
            let outcome = second
                .handle(
                    job(upstream_id),
                    1_001,
                    success_poller(Arc::clone(&calls), 1_001),
                )
                .await?;

            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                outcome,
                JobOutcome::Retry {
                    delay: Duration::from_secs(59)
                }
            );
            sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
                .execute(&admin)
                .await?;
            Ok(())
        }
    }
}

async fn postgres_pool() -> Result<Option<(PgPool, PgPool, String)>> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        return Ok(None);
    };
    let admin = PgPool::connect(&url).await?;
    let schema = format!("oauth_usage_poll_{}", Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await?;
    let options = sqlx::postgres::PgConnectOptions::from_str(&url)?
        .options([("search_path", schema.as_str())]);
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options)
        .await?;
    pool.execute(include_str!(
        "../migrations/postgres/0002_idempotency_tables.sql"
    ))
    .await?;
    Ok(Some((admin, pool, schema)))
}

fn postgres_handler(pool: PgPool) -> OAuthUsagePollHandler<sqlx::Postgres> {
    OAuthUsagePollHandler::new(
        OAuthUsagePollCursorsStore::new(pool),
        OAuthUsagePollScheduleConfig::default(),
    )
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
