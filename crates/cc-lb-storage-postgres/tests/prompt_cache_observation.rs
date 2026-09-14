// Live-Postgres prompt cache observation adapter tests.
//
// The crate has no `integration` feature, so these are ignored by default to keep
// `cargo test -p cc-lb-storage-postgres` usable without a database.
// Run with:
// CI_POSTGRES_URL=postgres://... cargo test -p cc-lb-storage-postgres --test prompt_cache_observation -- --ignored --nocapture

use std::{error::Error, str::FromStr};

use cc_lb_domain::TtlClass;
use cc_lb_storage_api::{PromptCacheObservationRecord, PromptCacheObservationStore};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{
    AssertSqlSafe,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

use cc_lb_storage_api::{BackendKind, MetaStore};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn t3_postgres__postgres_prompt_cache_upsert_then_list_returns_active() -> TestResult<()> {
    let fixture = Fixture::create().await?;
    let store = fixture.store();
    let upstream_id = Uuid::from_u128(1);
    let mut record = observation(
        upstream_id,
        "sha256:active",
        TtlClass::Ephemeral5m,
        200,
        100,
    );

    PromptCacheObservationStore::upsert_observation(&store, &record).await?;
    record.expires_at_unix_secs = 300;
    record.last_observed_at_unix_secs = 150;
    PromptCacheObservationStore::upsert_observation(&store, &record).await?;

    let active =
        PromptCacheObservationStore::list_active_for_upstream(&store, upstream_id, 250).await?;
    assert_eq!(active, vec![record]);

    fixture.drop_schema().await?;
    Ok(())
}

#[tokio::test]
async fn t3_postgres__postgres_prompt_cache_purge_removes_expired() -> TestResult<()> {
    let fixture = Fixture::create().await?;
    let store = fixture.store();
    let upstream_id = Uuid::from_u128(2);
    let expired = observation(
        upstream_id,
        "sha256:expired",
        TtlClass::Ephemeral5m,
        100,
        90,
    );
    let active = observation(
        upstream_id,
        "sha256:active",
        TtlClass::Ephemeral1h,
        300,
        120,
    );

    PromptCacheObservationStore::upsert_observation(&store, &expired).await?;
    PromptCacheObservationStore::upsert_observation(&store, &active).await?;

    let purged = PromptCacheObservationStore::purge_expired_before(&store, 200).await?;
    assert_eq!(purged, 1);
    assert_eq!(
        PromptCacheObservationStore::list_active_for_upstream(&store, upstream_id, 200).await?,
        vec![active]
    );
    assert_eq!(PromptCacheObservationStore::count(&store).await?, 1);

    fixture.drop_schema().await?;
    Ok(())
}

#[tokio::test]
async fn t3_postgres__postgres_prompt_cache_count_after_inserts() -> TestResult<()> {
    let fixture = Fixture::create().await?;
    let store = fixture.store();
    let upstream_id = Uuid::from_u128(3);
    let records = [
        observation(upstream_id, "sha256:a", TtlClass::Ephemeral5m, 200, 100),
        observation(upstream_id, "sha256:b", TtlClass::Ephemeral1h, 300, 100),
        observation(upstream_id, "sha256:c", TtlClass::Ephemeral5m, 400, 100),
    ];

    for record in &records {
        PromptCacheObservationStore::upsert_observation(&store, record).await?;
    }

    assert_eq!(PromptCacheObservationStore::count(&store).await?, 3);

    fixture.drop_schema().await?;
    Ok(())
}

struct Fixture {
    url: String,
    schema: String,
    pool: sqlx::PgPool,
}

impl Fixture {
    async fn create() -> TestResult<Self> {
        let url = crate::postgres_fixture::required_postgres_url();
        let schema = format!("test_prompt_cache_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(PgConnectOptions::from_str(&url)?)
            .await?;
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin_pool)
            .await?;
        admin_pool.close().await;

        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(
                PgConnectOptions::from_str(&url)?.options([("search_path", schema.as_str())]),
            )
            .await?;
        // Run the real migrator rather than hand-picking migrations. A pinned subset is how this
        // fixture rotted: it applied only `0030` and never saw `0072` rename `prefix_hash` to
        // `v3_prefix_key`. `initialize` is the same entry point the sibling fixtures in this
        // directory use, so it cannot fall behind the migrations directory.
        PostgresStorage::new(pool.clone(), cc_lb_testkit::fixed_clock(1_700_000_000))
            .initialize(BackendKind::Postgres)
            .await?;

        Ok(Self { url, schema, pool })
    }

    fn store(&self) -> PostgresStorage {
        PostgresStorage::new(self.pool.clone(), cc_lb_testkit::fixed_clock(1_700_000_000))
    }

    async fn drop_schema(self) -> TestResult<()> {
        self.pool.close().await;
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(PgConnectOptions::from_str(&self.url)?)
            .await?;
        sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            self.schema
        )))
        .execute(&admin_pool)
        .await?;
        admin_pool.close().await;
        Ok(())
    }
}

/// Opaque to the storage layer: the column has no default or constraint and these tests only
/// round-trip it, so the value is arbitrary and deliberately not tied to the engine's
/// HASH_SCHEMA_VERSION (which would drag cc-lb-engine into this crate's dev graph).
const FIXTURE_SCHEMA_VERSION: u8 = 5;

fn observation(
    upstream_id: Uuid,
    prefix_hash: &str,
    ttl_class: TtlClass,
    expires_at_unix_secs: u64,
    last_observed_at_unix_secs: u64,
) -> PromptCacheObservationRecord {
    PromptCacheObservationRecord {
        upstream_id,
        canonical_model_id: "claude-sonnet-4-5-20250929".to_owned(),
        v3_prefix_key: prefix_hash.to_owned(),
        ttl_class,
        expires_at_unix_secs,
        last_observed_at_unix_secs,
        hash_schema_version: FIXTURE_SCHEMA_VERSION,
        prefix_content_block_index: 7,
        estimated_prefix_tokens: 12_345,
        token_estimate_source: "local_tiktoken_v1".to_owned(),
    }
}
