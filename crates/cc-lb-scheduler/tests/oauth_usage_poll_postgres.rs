#![cfg(feature = "postgres")]

use std::str::FromStr;

use cc_lb_scheduler::error::Result;
use cc_lb_scheduler::state_stores::OAuthUsagePollCursorsStore;
use sqlx::{Executor, PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

#[tokio::test]
async fn postgres_record_attempt_appends_successive_statuses_and_increments_count() -> Result<()> {
    let Some((admin, pool, schema)) = postgres_pool(2).await? else {
        eprintln!("SKIP: DATABASE_URL not set - skipping postgres oauth usage poll test");
        return Ok(());
    };
    let upstream_id = Uuid::new_v4();
    let cursors = OAuthUsagePollCursorsStore::new(pool);

    cursors.record_attempt(upstream_id, 1_000, 200).await?;
    cursors.record_attempt(upstream_id, 1_060, -1).await?;

    let cursor = cursors.read(upstream_id).await?.expect("cursor exists");
    assert_eq!(cursor.last_observed_at_unix_secs, Some(1_060));
    assert_eq!(cursor.last_status, Some(-1));
    assert_eq!(cursor.attempt_count, 2);
    drop_schema(&admin, &schema).await
}

#[tokio::test]
async fn postgres_record_attempt_does_not_let_older_tick_overwrite_newer_state() -> Result<()> {
    let Some((admin, pool, schema)) = postgres_pool(2).await? else {
        eprintln!("SKIP: DATABASE_URL not set - skipping postgres oauth usage poll test");
        return Ok(());
    };
    let upstream_id = Uuid::new_v4();
    let cursors = OAuthUsagePollCursorsStore::new(pool);

    cursors.record_attempt(upstream_id, 1_060, 200).await?;
    cursors.record_attempt(upstream_id, 1_000, 429).await?;

    let cursor = cursors.read(upstream_id).await?.expect("cursor exists");
    assert_eq!(cursor.last_observed_at_unix_secs, Some(1_060));
    assert_eq!(cursor.last_status, Some(200));
    assert_eq!(cursor.attempt_count, 2);
    drop_schema(&admin, &schema).await
}

#[tokio::test]
async fn postgres_record_attempt_increments_count_under_concurrent_writers() -> Result<()> {
    let Some((admin, pool, schema)) = postgres_pool(8).await? else {
        eprintln!("SKIP: DATABASE_URL not set - skipping postgres oauth usage poll test");
        return Ok(());
    };
    let upstream_id = Uuid::new_v4();
    let cursors = OAuthUsagePollCursorsStore::new(pool);

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
    drop_schema(&admin, &schema).await
}

async fn postgres_pool(max_connections: u32) -> Result<Option<(PgPool, PgPool, String)>> {
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
        .max_connections(max_connections)
        .connect_with(options)
        .await?;
    pool.execute(include_str!(
        "../migrations/postgres/0002_idempotency_tables.sql"
    ))
    .await?;
    pool.execute(include_str!(
        "../migrations/postgres/0008_slim_oauth_usage_poll_cursors.sql"
    ))
    .await?;
    Ok(Some((admin, pool, schema)))
}

async fn drop_schema(admin: &PgPool, schema: &str) -> Result<()> {
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(admin)
        .await?;
    Ok(())
}
