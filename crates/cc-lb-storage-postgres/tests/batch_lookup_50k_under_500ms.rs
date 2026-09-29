use std::{
    error::Error,
    str::FromStr,
    time::{Duration, Instant},
};

use cc_lb_domain::TtlClass;
use cc_lb_storage_api::{PromptCacheObservationRecord, PromptCacheObservationStore};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{
    AssertSqlSafe,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

const UPSTREAM_COUNT: usize = 5;
const RECORDS_PER_UPSTREAM: usize = 10_000;
const EXPIRED_PER_UPSTREAM: usize = 9_000;
const ACTIVE_PER_UPSTREAM: usize = RECORDS_PER_UPSTREAM - EXPIRED_PER_UPSTREAM;
const TOTAL_RECORDS: u64 = (UPSTREAM_COUNT * RECORDS_PER_UPSTREAM) as u64;
const NOW_UNIX_SECS: u64 = 1_700_000_000;
// Heuristic budget carried over from the hydration workload this test replaced.
// It guards gross regressions only: at 50k rows a sequential scan can still
// pass, so this does not prove index usage — measure the plan separately.
const MAX_LOOKUP_ELAPSED: Duration = Duration::from_millis(500);

use cc_lb_storage_api::MetaStore;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
#[ignore = "requires CI_POSTGRES_URL; run with --release --ignored"]
async fn batch_lookup_50k_rows_90percent_expired_under_500ms() -> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let store = fixture.store();
    let upstream_ids = upstream_ids();

    let mut all_keys = Vec::with_capacity(UPSTREAM_COUNT * RECORDS_PER_UPSTREAM);
    for (upstream_index, upstream_id) in upstream_ids.iter().copied().enumerate() {
        for record_index in 0..RECORDS_PER_UPSTREAM {
            let record = observation(upstream_index, upstream_id, record_index);
            all_keys.push(record.v3_prefix_key.clone());
            PromptCacheObservationStore::upsert_observation(&store, &record).await?;
        }
    }
    assert_eq!(
        PromptCacheObservationStore::count(&store).await?,
        TOTAL_RECORDS
    );

    let started_at = Instant::now();
    let records = PromptCacheObservationStore::list_active_for_candidates(
        &store,
        &upstream_ids,
        "claude-sonnet-4-5-20250929",
        &all_keys,
        NOW_UNIX_SECS,
    )
    .await?;
    let elapsed = started_at.elapsed();

    assert_eq!(records.len(), UPSTREAM_COUNT * ACTIVE_PER_UPSTREAM);
    eprintln!("batch_lookup_elapsed_ms={:.3}", duration_ms(elapsed));

    fixture.drop_schema().await?;

    assert!(
        elapsed < MAX_LOOKUP_ELAPSED,
        "batch lookup of active rows took {elapsed:?}, expected < {MAX_LOOKUP_ELAPSED:?}"
    );

    Ok(())
}

struct Fixture {
    url: String,
    schema: String,
    pool: sqlx::PgPool,
}

impl Fixture {
    async fn create() -> TestResult<Option<Self>> {
        let Some(url) = std::env::var("CI_POSTGRES_URL").ok() else {
            eprintln!("skipped: CI_POSTGRES_URL not set");
            return Ok(None);
        };
        let schema = format!("test_batch_lookup_50k_{}", Uuid::new_v4().simple());
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
        // Real migrator, not a hand-picked subset — see the note in `prompt_cache_observation.rs`.
        PostgresStorage::new(pool.clone(), std::sync::Arc::new(cc_lb_clock::SystemClock))
            .initialize()
            .await?;

        Ok(Some(Self { url, schema, pool }))
    }

    fn store(&self) -> PostgresStorage {
        PostgresStorage::new(
            self.pool.clone(),
            std::sync::Arc::new(cc_lb_clock::SystemClock),
        )
    }

    async fn drop_schema(self) -> TestResult {
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

fn upstream_ids() -> [Uuid; UPSTREAM_COUNT] {
    std::array::from_fn(|index| {
        Uuid::from_u128(0x4000_0000_0000_0000_0000_0000_0000_0000 + index as u128)
    })
}

/// Opaque to the storage layer; see the note in `prompt_cache_observation.rs`.
const FIXTURE_SCHEMA_VERSION: u8 = 5;

fn observation(
    upstream_index: usize,
    upstream_id: Uuid,
    record_index: usize,
) -> PromptCacheObservationRecord {
    let is_expired = record_index < EXPIRED_PER_UPSTREAM;
    PromptCacheObservationRecord {
        upstream_id,
        canonical_model_id: "claude-sonnet-4-5-20250929".to_owned(),
        v3_prefix_key: format!("v3:t30-postgres-{upstream_index:02}-{record_index:05}"),
        ttl_class: ttl_class(record_index),
        expires_at_unix_secs: if is_expired {
            NOW_UNIX_SECS - 1
        } else {
            NOW_UNIX_SECS + 300
        },
        last_observed_at_unix_secs: NOW_UNIX_SECS.saturating_sub(60),
        hash_schema_version: FIXTURE_SCHEMA_VERSION,
        prefix_content_block_index: u32::try_from(record_index).expect("record index fits u32"),
        estimated_prefix_tokens: 1_000 + record_index as u64,
        token_estimate_source: "local_tiktoken_v1".to_owned(),
    }
}

fn ttl_class(record_index: usize) -> TtlClass {
    if record_index.is_multiple_of(2) {
        TtlClass::Ephemeral5m
    } else {
        TtlClass::Ephemeral1h
    }
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}
