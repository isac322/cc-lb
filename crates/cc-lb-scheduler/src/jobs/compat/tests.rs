#![allow(non_snake_case)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use cc_lb_control::anthropic_compat::CLAUDE_CODE_STABLE_VERSION_KEY;

use super::test_support::RecordingCompatibilityKv;
use super::*;

const INITIAL_TIME: u64 = 1_000;
const UPDATED_TIME: u64 = 2_000;
type MaybeCompatEtag = Option<crate::state_stores::AnthropicCompatEtag>;

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

#[derive(Clone, Default)]
struct RecordingCompatEtags {
    state: Arc<Mutex<RecordingCompatEtagsState>>,
}

#[derive(Default)]
struct RecordingCompatEtagsState {
    etags: BTreeMap<String, crate::state_stores::AnthropicCompatEtag>,
    read_keys: Vec<String>,
    upserts: Vec<CompatUpsert>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CompatUpsert {
    key: String,
    etag: Option<String>,
    hash: String,
    now_unix_secs: u64,
}

impl RecordingCompatEtags {
    fn read_keys(&self) -> Vec<String> {
        self.state
            .lock()
            .expect("compat etags lock")
            .read_keys
            .clone()
    }

    fn upserts(&self) -> Vec<CompatUpsert> {
        self.state
            .lock()
            .expect("compat etags lock")
            .upserts
            .clone()
    }
}

impl CompatEtagRepository for RecordingCompatEtags {
    fn read_compat_etag<'a>(
        &'a self,
        key: &'a str,
    ) -> CompatJobFuture<'a, Result<MaybeCompatEtag>> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("compat etags lock");
            state.read_keys.push(key.to_owned());
            Ok(state.etags.get(key).cloned())
        })
    }

    fn upsert_compat_value<'a>(
        &'a self,
        key: &'a str,
        etag: Option<&'a str>,
        hash: &'a str,
        now_unix_secs: u64,
    ) -> CompatJobFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("compat etags lock");
            state.upserts.push(CompatUpsert {
                key: key.to_owned(),
                etag: etag.map(str::to_owned),
                hash: hash.to_owned(),
                now_unix_secs,
            });
            state.etags.insert(
                key.to_owned(),
                crate::state_stores::AnthropicCompatEtag {
                    key: key.to_owned(),
                    etag: etag.map(str::to_owned),
                    last_applied_at_unix_secs: now_unix_secs,
                    last_value_hash: hash.to_owned(),
                },
            );
            Ok(())
        })
    }
}

#[tokio::test]
async fn t2__etag_deduplicates_not_modified() -> Result<()> {
    let stored_hash = compatibility_value_hash("2.1.150");
    let etags = RecordingCompatEtags::default();

    exercise_etag_deduplicates_not_modified(etags.clone()).await?;

    assert_eq!(
        etags.read_keys(),
        vec![
            CLAUDE_CODE_STABLE_VERSION_KEY.to_owned(),
            CLAUDE_CODE_STABLE_VERSION_KEY.to_owned(),
        ]
    );
    assert_eq!(
        etags.upserts(),
        vec![
            CompatUpsert {
                key: CLAUDE_CODE_STABLE_VERSION_KEY.to_owned(),
                etag: Some("etag-1".to_owned()),
                hash: stored_hash.clone(),
                now_unix_secs: INITIAL_TIME,
            },
            CompatUpsert {
                key: CLAUDE_CODE_STABLE_VERSION_KEY.to_owned(),
                etag: Some("etag-1".to_owned()),
                hash: stored_hash,
                now_unix_secs: UPDATED_TIME,
            },
        ]
    );
    Ok(())
}

#[tokio::test]
async fn t2__unchanged_hash_skips_value_write() -> Result<()> {
    let stored_hash = compatibility_value_hash("2.1.150");
    let etags = RecordingCompatEtags::default();

    exercise_unchanged_hash_skips_value_write(etags.clone()).await?;

    assert_eq!(
        etags.read_keys(),
        vec![
            CLAUDE_CODE_STABLE_VERSION_KEY.to_owned(),
            CLAUDE_CODE_STABLE_VERSION_KEY.to_owned(),
        ]
    );
    assert_eq!(
        etags.upserts(),
        vec![
            CompatUpsert {
                key: CLAUDE_CODE_STABLE_VERSION_KEY.to_owned(),
                etag: Some("etag-1".to_owned()),
                hash: stored_hash.clone(),
                now_unix_secs: INITIAL_TIME,
            },
            CompatUpsert {
                key: CLAUDE_CODE_STABLE_VERSION_KEY.to_owned(),
                etag: Some("etag-2".to_owned()),
                hash: stored_hash,
                now_unix_secs: UPDATED_TIME,
            },
        ]
    );
    Ok(())
}

#[cfg(feature = "sqlite")]
mod t3__sqlite {
    use super::*;
    use crate::state_stores::AnthropicCompatEtagsStore;

    async fn etags() -> Result<AnthropicCompatEtagsStore<sqlx::Sqlite>> {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
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

    #[tokio::test]
    async fn etag_deduplicates_not_modified() -> Result<()> {
        exercise_etag_deduplicates_not_modified(etags().await?).await
    }

    #[tokio::test]
    async fn unchanged_hash_skips_value_write() -> Result<()> {
        exercise_unchanged_hash_skips_value_write(etags().await?).await
    }
}

#[cfg(feature = "postgres")]
mod t3_postgres__postgres {
    use std::str::FromStr as _;

    use sqlx::Executor as _;
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

    use super::*;
    use crate::state_stores::AnthropicCompatEtagsStore;

    async fn etags() -> anyhow::Result<(
        cc_lb_storage_conformance::PostgresFixture,
        sqlx::PgPool,
        AnthropicCompatEtagsStore<sqlx::Postgres>,
    )> {
        let fixture = cc_lb_storage_conformance::postgres_fixture().await?;
        let search_path = format!("{},public", fixture.schema_name());
        let options = PgConnectOptions::from_str(fixture.database_url())?
            .options([("search_path", search_path.as_str())]);
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect_with(options)
            .await?;
        pool.execute(include_str!(
            "../../../migrations/postgres/0002_idempotency_tables.sql"
        ))
        .await?;
        let etags = AnthropicCompatEtagsStore::new(pool.clone());
        Ok((fixture, pool, etags))
    }

    #[tokio::test]
    async fn etag_deduplicates_not_modified() -> anyhow::Result<()> {
        let (fixture, pool, etags) = etags().await?;
        let outcome = exercise_etag_deduplicates_not_modified(etags).await;
        pool.close().await;
        fixture.teardown().await?;
        outcome?;
        Ok(())
    }

    #[tokio::test]
    async fn unchanged_hash_skips_value_write() -> anyhow::Result<()> {
        let (fixture, pool, etags) = etags().await?;
        let outcome = exercise_unchanged_hash_skips_value_write(etags).await;
        pool.close().await;
        fixture.teardown().await?;
        outcome?;
        Ok(())
    }
}
