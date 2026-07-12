use cc_lb_control::anthropic_compat::CLAUDE_CODE_STABLE_VERSION_KEY;

use crate::state_stores::AnthropicCompatEtagsStore;

use super::test_support::RecordingCompatibilityKv;
use super::*;

const INITIAL_TIME: u64 = 1_000;
const UPDATED_TIME: u64 = 2_000;

async fn exercise_etag_deduplicates_not_modified<E>(etags: E) -> Result<()>
where
    E: CompatEtagRepository + Sync,
{
    let stored_hash = compatibility_value_hash("2.1.150");
    etags
        .upsert_compat_value(
            CLAUDE_CODE_STABLE_VERSION_KEY,
            Some("etag-1"),
            &stored_hash,
            INITIAL_TIME,
        )
        .await?;
    let compatibility_kv = RecordingCompatibilityKv::default();

    let outcome = handle_anthropic_compat_refresh_job(
        AnthropicCompatRefreshJob::new(CLAUDE_CODE_STABLE_VERSION_KEY),
        &etags,
        &compatibility_kv,
        |compatibility_key, stored_etag| async move {
            assert_eq!(compatibility_key.name, CLAUDE_CODE_STABLE_VERSION_KEY);
            assert_eq!(stored_etag.as_deref(), Some("etag-1"));
            Ok(CompatFetch::NotModified {
                etag: Some("etag-1".to_owned()),
            })
        },
        UPDATED_TIME,
    )
    .await?;

    let row = etags
        .read_compat_etag(CLAUDE_CODE_STABLE_VERSION_KEY)
        .await?
        .expect("etag row");
    assert_eq!(outcome, JobOutcome::Noop);
    assert_eq!(row.last_applied_at_unix_secs, UPDATED_TIME);
    assert_eq!(row.last_value_hash, stored_hash);
    assert_eq!(compatibility_kv.value_write_count(), 0);
    Ok(())
}

async fn exercise_unchanged_hash_skips_value_write<E>(etags: E) -> Result<()>
where
    E: CompatEtagRepository + Sync,
{
    let stored_hash = compatibility_value_hash("2.1.150");
    etags
        .upsert_compat_value(
            CLAUDE_CODE_STABLE_VERSION_KEY,
            Some("etag-1"),
            &stored_hash,
            INITIAL_TIME,
        )
        .await?;
    let compatibility_kv = RecordingCompatibilityKv::default();

    let outcome = handle_anthropic_compat_refresh_job(
        AnthropicCompatRefreshJob::new(CLAUDE_CODE_STABLE_VERSION_KEY),
        &etags,
        &compatibility_kv,
        |_compatibility_key, _stored_etag| async move {
            Ok(CompatFetch::Modified {
                value: "2.1.150".to_owned(),
                etag: Some("etag-2".to_owned()),
                source_url: Some("https://example.test/stable".to_owned()),
            })
        },
        UPDATED_TIME,
    )
    .await?;

    let row = etags
        .read_compat_etag(CLAUDE_CODE_STABLE_VERSION_KEY)
        .await?
        .expect("etag row");
    assert_eq!(outcome, JobOutcome::Noop);
    assert_eq!(row.etag.as_deref(), Some("etag-2"));
    assert_eq!(row.last_applied_at_unix_secs, UPDATED_TIME);
    assert_eq!(row.last_value_hash, stored_hash);
    assert_eq!(compatibility_kv.value_write_count(), 0);
    Ok(())
}

#[cfg(feature = "sqlite")]
async fn sqlite_etags() -> Result<AnthropicCompatEtagsStore<sqlx::Sqlite>> {
    use sqlx::sqlite::SqlitePoolOptions;

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    sqlx::raw_sql(include_str!(
        "../../../migrations/sqlite/0002_idempotency_tables.sql"
    ))
    .execute(&pool)
    .await?;
    Ok(AnthropicCompatEtagsStore::new(pool))
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn jobs_compat_etag_deduplicates_not_modified_sqlite() -> Result<()> {
    exercise_etag_deduplicates_not_modified(sqlite_etags().await?).await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn jobs_compat_unchanged_hash_skips_value_write_sqlite() -> Result<()> {
    exercise_unchanged_hash_skips_value_write(sqlite_etags().await?).await
}

#[cfg(feature = "postgres")]
async fn postgres_etags() -> Result<
    Option<(
        sqlx::PgPool,
        String,
        AnthropicCompatEtagsStore<sqlx::Postgres>,
    )>,
> {
    use std::str::FromStr;

    use sqlx::{Executor as _, PgPool, postgres::PgPoolOptions};
    use uuid::Uuid;

    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("SKIP: DATABASE_URL not set - skipping postgres compat test");
        return Ok(None);
    };
    let admin = PgPool::connect(&url).await?;
    let schema = format!("jobs_compat_{}", Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await?;
    let options = sqlx::postgres::PgConnectOptions::from_str(&url)?
        .options([("search_path", schema.as_str())]);
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;
    pool.execute(include_str!(
        "../../../migrations/postgres/0002_idempotency_tables.sql"
    ))
    .await?;
    Ok(Some((admin, schema, AnthropicCompatEtagsStore::new(pool))))
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn jobs_compat_etag_deduplicates_not_modified_postgres() -> Result<()> {
    let Some((admin, schema, etags)) = postgres_etags().await? else {
        return Ok(());
    };
    let outcome = exercise_etag_deduplicates_not_modified(etags).await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await?;
    outcome
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn jobs_compat_unchanged_hash_skips_value_write_postgres() -> Result<()> {
    let Some((admin, schema, etags)) = postgres_etags().await? else {
        return Ok(());
    };
    let outcome = exercise_unchanged_hash_skips_value_write(etags).await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await?;
    outcome
}
