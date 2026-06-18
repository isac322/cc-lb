use super::*;
use crate::error::Result;
use uuid::Uuid;

macro_rules! exercise_stores {
    ($pool:expr) => {{
        let upstream_id = Uuid::new_v4();
        let warmups = WarmupEffectsStore::new($pool.clone());
        assert!(!warmups.is_already_done(upstream_id, 42).await?);
        assert!(warmups.try_acquire_cycle(upstream_id, 42, 100).await?);
        assert!(!warmups.try_acquire_cycle(upstream_id, 42, 101).await?);
        assert!(warmups.is_already_done(upstream_id, 42).await?);
        assert_eq!(warmups.prune_older_than(101).await?, 1);

        let claims = OAuthRefreshClaimsStore::new($pool.clone());
        assert!(
            claims
                .try_acquire(upstream_id, "holder-a", 60, 1_000)
                .await?
        );
        assert!(
            !claims
                .try_acquire(upstream_id, "holder-b", 60, 1_010)
                .await?
        );
        assert!(
            claims
                .try_acquire(upstream_id, "holder-b", 60, 1_061)
                .await?
        );
        assert!(
            claims
                .complete_and_bump_generation(upstream_id, "holder-b", 7)
                .await?
        );
        assert_eq!(claims.read_current_generation(upstream_id).await?, 7);
        assert!(!claims.release_if_holder(upstream_id, "holder-b").await?);

        let bump_id = Uuid::new_v4();
        let left = claims.clone();
        let right = claims.clone();
        let left_task = tokio::spawn(async move {
            for _ in 0..100 {
                left.bump_generation(bump_id).await?;
            }
            Result::<()>::Ok(())
        });
        let right_task = tokio::spawn(async move {
            for _ in 0..100 {
                right.bump_generation(bump_id).await?;
            }
            Result::<()>::Ok(())
        });
        left_task.await.unwrap()?;
        right_task.await.unwrap()?;
        assert_eq!(claims.read_current_generation(bump_id).await?, 200);

        let cursors = OAuthUsagePollCursorsStore::new($pool.clone());
        assert!(cursors.read(upstream_id).await?.is_none());
        cursors
            .record_success(upstream_id, 2_000, 10_000, 20_000, 2)
            .await?;
        cursors.record_throttle(upstream_id, 2_100, 3, 2).await?;
        cursors
            .record_success(upstream_id, 2_200, 20_000, 30_000, 2)
            .await?;
        let cursor = cursors.read(upstream_id).await?.unwrap();
        assert_eq!(cursor.recent_successes_unix_secs, vec![2_000, 2_200]);
        assert_eq!(cursor.recent_throttles_unix_secs, vec![2_100]);
        assert_eq!(cursor.last_throttle_count, 3);
        let config = OAuthUsagePollScheduleConfig {
            success_window_secs: 300,
            success_capacity: 2,
            success_safety_secs: 0,
            ..OAuthUsagePollScheduleConfig::default()
        };
        assert_eq!(
            cursors.compute_next_run_at(2_210, &config, Some(&cursor)),
            2_300
        );

        let compat = AnthropicCompatEtagsStore::new($pool.clone());
        compat
            .upsert_value("models", Some("etag-1"), "hash-1", 3_000)
            .await?;
        assert_eq!(
            compat.read("models").await?.unwrap().last_value_hash,
            "hash-1"
        );

        let prices = PriceCatalogVersionsStore::new($pool.clone());
        prices.upsert_fingerprint("litellm", "fp-1", 4_000).await?;
        assert_eq!(prices.read("litellm").await?.unwrap().fingerprint, "fp-1");

        let failures = SchedulerFailuresStore::new($pool.clone());
        let id = failures
            .record("warmup", "payload", "boom", 2, 5_000)
            .await?;
        let rows = failures.list_paged(Some("warmup"), 10, 0).await?;
        assert_eq!(rows[0].id, id);
        assert_eq!(failures.prune_older_than(5_001).await?, 1);
        Result::<()>::Ok(())
    }};
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn idempotency_sqlite_stores_cover_insert_read_ttl_and_atomic_bump() -> Result<()> {
    use sqlx::sqlite::SqlitePoolOptions;
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    sqlx::raw_sql(include_str!(
        "../../migrations/sqlite/0002_idempotency_tables.sql"
    ))
    .execute(&pool)
    .await?;
    exercise_stores!(pool)
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn idempotency_postgres_stores_cover_insert_read_ttl_and_atomic_bump() -> Result<()> {
    use sqlx::{Executor, PgPool, postgres::PgPoolOptions};
    use std::str::FromStr;

    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("SKIP: DATABASE_URL not set - skipping postgres idempotency test");
        return Ok(());
    };
    let admin = PgPool::connect(&url).await?;
    let schema = format!("idempotency_{}", Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await?;
    let options = sqlx::postgres::PgConnectOptions::from_str(&url)?
        .options([("search_path", schema.as_str())]);
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await?;
    pool.execute(include_str!(
        "../../migrations/postgres/0002_idempotency_tables.sql"
    ))
    .await?;
    let outcome = exercise_stores!(pool);
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await?;
    outcome
}
