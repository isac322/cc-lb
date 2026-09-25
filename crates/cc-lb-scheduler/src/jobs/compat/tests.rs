use cc_lb_control::anthropic_compat::{
    CLAUDE_CODE_LATEST_VERSION_KEY, CLAUDE_CODE_STABLE_VERSION_KEY, COMPATIBILITY_KEYS,
};

use crate::state_stores::AnthropicCompatEtagsStore;

use super::test_support::{RecordingCompatEtags, RecordingCompatibilityKv};
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

#[tokio::test]
async fn refresh_all_keys_writes_each_key_independently() {
    let etags = RecordingCompatEtags::default();
    let compatibility_kv = RecordingCompatibilityKv::default();

    let outcome = handle_anthropic_compat_refresh_job(
        AnthropicCompatRefreshJob::all(),
        &etags,
        &compatibility_kv,
        |compatibility_key, _stored_etag| async move {
            Ok(CompatFetch::Modified {
                value: format!("value-for-{}", compatibility_key.name),
                etag: Some(format!("etag-for-{}", compatibility_key.name)),
                source_url: None,
            })
        },
        UPDATED_TIME,
    )
    .await
    .expect("all-keys refresh");

    assert_eq!(outcome, JobOutcome::Done);
    assert_eq!(
        compatibility_kv.value_write_count(),
        COMPATIBILITY_KEYS.len()
    );
    for key in COMPATIBILITY_KEYS {
        let record = compatibility_kv
            .record(key.name)
            .unwrap_or_else(|| panic!("{} record persisted", key.name));
        assert_eq!(record.value, format!("value-for-{}", key.name));
        let row = etags
            .read_compat_etag(key.name)
            .await
            .expect("etag read")
            .unwrap_or_else(|| panic!("{} etag row", key.name));
        assert_eq!(
            row.etag.as_deref(),
            Some(format!("etag-for-{}", key.name).as_str())
        );
        assert_eq!(
            row.last_value_hash,
            compatibility_value_hash(&format!("value-for-{}", key.name))
        );
    }
}

#[tokio::test]
async fn refresh_all_keys_failed_fetch_does_not_block_other_keys() {
    let etags = RecordingCompatEtags::default();
    let compatibility_kv = RecordingCompatibilityKv::default();

    let outcome = handle_anthropic_compat_refresh_job(
        AnthropicCompatRefreshJob::all(),
        &etags,
        &compatibility_kv,
        |compatibility_key, _stored_etag| async move {
            if compatibility_key.name == CLAUDE_CODE_STABLE_VERSION_KEY {
                Err(crate::error::SchedulerError::Job(
                    "fetch blew up".to_owned(),
                ))
            } else {
                Ok(CompatFetch::Modified {
                    value: "9.9.9".to_owned(),
                    etag: None,
                    source_url: None,
                })
            }
        },
        UPDATED_TIME,
    )
    .await
    .expect("all-keys refresh");

    // The failed key reschedules the job; the healthy key still persisted.
    assert_eq!(
        outcome,
        JobOutcome::Retry {
            delay: COMPAT_REFRESH_RETRY_DELAY
        }
    );
    assert_eq!(
        compatibility_kv
            .record(CLAUDE_CODE_LATEST_VERSION_KEY)
            .map(|record| record.value),
        Some("9.9.9".to_owned())
    );
    assert!(
        compatibility_kv
            .record(CLAUDE_CODE_STABLE_VERSION_KEY)
            .is_none()
    );
    assert_eq!(compatibility_kv.failure_writes().len(), 1);
}

#[tokio::test]
async fn refresh_all_keys_starts_every_fetch_before_any_completes() {
    let etags = RecordingCompatEtags::default();
    let compatibility_kv = RecordingCompatibilityKv::default();
    // Every fetch parks on the barrier until all keys' fetches have started, so
    // a sequential dispatch loop deadlocks on the first key and trips the guard.
    let barrier = tokio::sync::Barrier::new(COMPATIBILITY_KEYS.len());
    let barrier = &barrier;

    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        handle_anthropic_compat_refresh_job(
            AnthropicCompatRefreshJob::all(),
            &etags,
            &compatibility_kv,
            move |compatibility_key, _stored_etag| async move {
                barrier.wait().await;
                Ok(CompatFetch::Modified {
                    value: format!("value-for-{}", compatibility_key.name),
                    etag: None,
                    source_url: None,
                })
            },
            UPDATED_TIME,
        ),
    )
    .await
    .expect("key fetches must run concurrently")
    .expect("all-keys refresh");

    assert_eq!(outcome, JobOutcome::Done);
    for key in COMPATIBILITY_KEYS {
        assert_eq!(
            compatibility_kv.record(key.name).map(|record| record.value),
            Some(format!("value-for-{}", key.name))
        );
    }
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
