#[cfg(feature = "sqlite")]
use cc_lb_scheduler::error::Result;
use cc_lb_scheduler::jobs::oauth_usage_poll::OAuthUsagePollCronJob;
#[cfg(feature = "sqlite")]
use cc_lb_scheduler::state_stores::OAuthUsagePollCursorsStore;
use cc_lb_scheduler::worker::CronJob;
#[cfg(feature = "sqlite")]
use uuid::Uuid;

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_record_attempt_appends_successive_statuses_and_increments_count() -> Result<()> {
    let pool = sqlite_memory().await?;
    let upstream_id = Uuid::new_v4();
    let cursors = OAuthUsagePollCursorsStore::new(pool);

    cursors.record_attempt(upstream_id, 1_000, 200).await?;
    cursors.record_attempt(upstream_id, 1_060, 429).await?;

    let cursor = cursors.read(upstream_id).await?.expect("cursor exists");
    assert_eq!(cursor.last_observed_at_unix_secs, Some(1_060));
    assert_eq!(cursor.last_status, Some(429));
    assert_eq!(cursor.attempt_count, 2);
    Ok(())
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_record_attempt_does_not_let_older_tick_overwrite_newer_state() -> Result<()> {
    let pool = sqlite_memory().await?;
    let upstream_id = Uuid::new_v4();
    let cursors = OAuthUsagePollCursorsStore::new(pool);

    cursors.record_attempt(upstream_id, 1_060, 200).await?;
    cursors.record_attempt(upstream_id, 1_000, 429).await?;

    let cursor = cursors.read(upstream_id).await?.expect("cursor exists");
    assert_eq!(cursor.last_observed_at_unix_secs, Some(1_060));
    assert_eq!(cursor.last_status, Some(200));
    assert_eq!(cursor.attempt_count, 2);
    Ok(())
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_record_attempt_increments_count_under_concurrent_writers() -> Result<()> {
    let pool = sqlite_file_with_pool(8).await?;
    let upstream_id = Uuid::new_v4();
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
    Ok(())
}

#[test]
fn oauth_usage_poll_cron_job_uses_tick_payload() {
    let job = OAuthUsagePollCronJob::new(1_800_000_060);
    let cron = CronJob::OAuthUsagePoll(job.clone());

    assert_eq!(job.tick_unix_secs, 1_800_000_060);
    assert_eq!(cron.kind(), "oauth_usage_poll");
}

#[cfg(feature = "sqlite")]
async fn sqlite_memory() -> Result<sqlx::SqlitePool> {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    apply_cursor_migrations(&pool).await?;
    Ok(pool)
}

#[cfg(feature = "sqlite")]
async fn sqlite_file_with_pool(max_connections: u32) -> Result<sqlx::SqlitePool> {
    let path = std::env::temp_dir().join(format!("oauth-usage-{}.db", Uuid::new_v4()));
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(max_connections)
        .connect(&url)
        .await?;
    sqlx::query("PRAGMA journal_mode=WAL")
        .execute(&pool)
        .await?;
    sqlx::query("PRAGMA busy_timeout=5000")
        .execute(&pool)
        .await?;
    apply_cursor_migrations(&pool).await?;
    Ok(pool)
}

#[cfg(feature = "sqlite")]
async fn apply_cursor_migrations(pool: &sqlx::SqlitePool) -> Result<()> {
    sqlx::raw_sql(include_str!(
        "../migrations/sqlite/0002_idempotency_tables.sql"
    ))
    .execute(pool)
    .await?;
    sqlx::raw_sql(include_str!(
        "../migrations/sqlite/0008_slim_oauth_usage_poll_cursors.sql"
    ))
    .execute(pool)
    .await?;
    Ok(())
}
