#![cfg(feature = "postgres")]

use anyhow::Result;
use cc_lb_scheduler::state_stores::OAuthUsagePollCursorsStore;
use sqlx::Executor;
use uuid::Uuid;

#[tokio::test]
async fn t3_postgres__record_attempt_appends_successive_statuses_and_increments_count() -> Result<()>
{
    let (fixture, pool) = scheduler_postgres_fixture().await?;
    let upstream_id = Uuid::from_u128(1);
    let cursors = OAuthUsagePollCursorsStore::new(pool.clone());

    cursors.record_attempt(upstream_id, 1_000, 200).await?;
    cursors.record_attempt(upstream_id, 1_060, -1).await?;

    let cursor = cursors.read(upstream_id).await?.expect("cursor exists");
    assert_eq!(cursor.last_observed_at_unix_secs, Some(1_060));
    assert_eq!(cursor.last_status, Some(-1));
    assert_eq!(cursor.attempt_count, 2);
    pool.close().await;
    fixture.teardown().await
}

#[tokio::test]
async fn t3_postgres__record_attempt_does_not_let_older_tick_overwrite_newer_state() -> Result<()> {
    let (fixture, pool) = scheduler_postgres_fixture().await?;
    let upstream_id = Uuid::from_u128(2);
    let cursors = OAuthUsagePollCursorsStore::new(pool.clone());

    cursors.record_attempt(upstream_id, 1_060, 200).await?;
    cursors.record_attempt(upstream_id, 1_000, 429).await?;

    let cursor = cursors.read(upstream_id).await?.expect("cursor exists");
    assert_eq!(cursor.last_observed_at_unix_secs, Some(1_060));
    assert_eq!(cursor.last_status, Some(200));
    assert_eq!(cursor.attempt_count, 2);
    pool.close().await;
    fixture.teardown().await
}

#[tokio::test]
async fn t3_postgres__record_attempt_increments_count_under_concurrent_writers() -> Result<()> {
    let (fixture, pool) = scheduler_postgres_fixture().await?;
    let upstream_id = Uuid::from_u128(3);
    let cursors = OAuthUsagePollCursorsStore::new(pool.clone());

    let mut handles = Vec::new();
    for index in 0..8u64 {
        let cursors = cursors.clone();
        handles.push(tokio::spawn(async move {
            cursors
                .record_attempt(upstream_id, 1_000 + index, 200)
                .await
        }));
    }
    for handle in handles {
        handle.await.expect("join")?;
    }

    let cursor = cursors.read(upstream_id).await?.expect("cursor exists");
    assert_eq!(cursor.attempt_count, 8);
    assert_eq!(cursor.last_observed_at_unix_secs, Some(1_007));
    assert_eq!(cursor.last_status, Some(200));
    pool.close().await;
    fixture.teardown().await
}

async fn scheduler_postgres_fixture()
-> Result<(cc_lb_storage_conformance::PostgresFixture, sqlx::PgPool)> {
    let fixture = crate::postgres_fixture().await?;
    let pool = crate::scheduler_postgres_pool(&fixture).await?;
    pool.execute(include_str!(
        "../migrations/postgres/0002_idempotency_tables.sql"
    ))
    .await?;
    pool.execute(include_str!(
        "../migrations/postgres/0008_slim_oauth_usage_poll_cursors.sql"
    ))
    .await?;
    Ok((fixture, pool))
}
