use super::*;
use crate::error::Result;
use uuid::Uuid;

macro_rules! exercise_stores {
    ($pool:expr) => {{
        let upstream_id = Uuid::new_v4();
        let cursors = OAuthUsagePollCursorsStore::new($pool.clone());
        assert!(cursors.read(upstream_id).await?.is_none());
        let mut cursor = OAuthUsagePollCursor::new(upstream_id);
        cursor.last_observed_at_unix_secs = Some(2_000);
        cursor.last_status = Some(200);
        cursor.attempt_count = 1;
        cursors.upsert(&cursor).await?;

        cursor.last_observed_at_unix_secs = Some(2_100);
        cursor.last_status = Some(429);
        cursor.attempt_count = 2;
        cursors.upsert(&cursor).await?;

        let cursor = cursors.read(upstream_id).await?.unwrap();
        assert_eq!(cursor.upstream_id, upstream_id);
        assert_eq!(cursor.last_observed_at_unix_secs, Some(2_100));
        assert_eq!(cursor.last_status, Some(429));
        assert_eq!(cursor.attempt_count, 2);

        let compat = AnthropicCompatEtagsStore::new($pool.clone());
        compat.upsert_value("models", "hash-1", 3_000).await?;
        assert_eq!(
            compat.read("models").await?.unwrap().last_value_hash,
            "hash-1"
        );

        let prices = PriceCatalogVersionsStore::new($pool.clone());
        prices.upsert_fingerprint("litellm", "fp-1", 4_000).await?;
        assert_eq!(prices.read("litellm").await?.unwrap().fingerprint, "fp-1");
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
    sqlx::raw_sql(include_str!(
        "../../migrations/sqlite/0008_slim_oauth_usage_poll_cursors.sql"
    ))
    .execute(&pool)
    .await?;
    sqlx::raw_sql(include_str!(
        "../../migrations/sqlite/0010_drop_anthropic_compat_etag.sql"
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
    pool.execute(include_str!(
        "../../migrations/postgres/0008_slim_oauth_usage_poll_cursors.sql"
    ))
    .await?;
    pool.execute(include_str!(
        "../../migrations/postgres/0010_drop_anthropic_compat_etag.sql"
    ))
    .await?;
    let outcome = exercise_stores!(pool);
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await?;
    outcome
}
