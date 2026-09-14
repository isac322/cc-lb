use std::{future::Future, str::FromStr, sync::Arc};

use anyhow::Result;
use cc_lb_storage_api::{
    BackendKind, CacheKeepaliveHitRefreshRequest, CacheKeepaliveReplaceRequest,
    CacheKeepaliveSessionStore, CacheTtl, MetaStore, cache_keepalive_job_key,
};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{AssertSqlSafe, PgPool, postgres::PgConnectOptions, postgres::PgPoolOptions};
use uuid::Uuid;

#[test]
fn t3_postgres__native_mvcc_concurrent_replace_serializes_generation_increments() {
    run_native_scenario(
        "native_mvcc_concurrent_replace_serializes_generation_increments",
        concurrent_replace_from_real_request_bumps_each_generation,
    );
}

#[test]
fn t3_postgres__native_mvcc_concurrent_reschedule_has_one_compare_and_swap_winner() {
    run_native_scenario(
        "native_mvcc_concurrent_reschedule_has_one_compare_and_swap_winner",
        concurrent_hit_reschedule_allows_only_one_generation_cas,
    );
}

fn run_native_scenario<F, Fut>(name: &str, scenario: F)
where
    F: FnOnce(PostgresStorage) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let url = crate::postgres_fixture::required_postgres_url();
    tokio::runtime::Runtime::new()
        .expect("tokio runtime")
        .block_on(async move {
            let fixture = Fixture::create(&url).await?;
            let storage = PostgresStorage::new(
                fixture.pool.clone(),
                cc_lb_testkit::fixed_clock(1_700_000_000),
            );
            storage.initialize(BackendKind::Postgres).await?;
            let result = scenario(storage).await;
            let teardown = fixture.drop_schema().await;
            result?;
            teardown
        })
        .unwrap_or_else(|error| panic!("{name}: {error}"));
}

async fn concurrent_replace_from_real_request_bumps_each_generation(
    storage: PostgresStorage,
) -> Result<()> {
    let session_key_hash = "concurrent-replace";
    let original = storage
        .replace_from_real_request(&replace_request(session_key_hash, b"init", 100))
        .await?;
    assert_eq!(original.generation, 1);
    let barrier = Arc::new(tokio::sync::Barrier::new(2));

    let first = tokio::spawn({
        let storage = storage.clone();
        let barrier = Arc::clone(&barrier);
        async move {
            barrier.wait().await;
            storage
                .replace_from_real_request(&replace_request(session_key_hash, b"first", 110))
                .await
        }
    });
    let second = tokio::spawn({
        let storage = storage.clone();
        let barrier = Arc::clone(&barrier);
        async move {
            barrier.wait().await;
            storage
                .replace_from_real_request(&replace_request(session_key_hash, b"second", 120))
                .await
        }
    });

    let (first, second) = tokio::try_join!(first, second)?;
    let mut generations = vec![first?.generation, second?.generation];
    generations.sort_unstable();
    assert_eq!(generations, [2, 3]);
    let final_record = storage
        .get_cache_keepalive_session(session_key_hash)
        .await?
        .expect("row exists");
    assert_eq!(final_record.generation, 3);
    assert_eq!(
        final_record.current_job_key,
        cache_keepalive_job_key(session_key_hash, 3)
    );
    Ok(())
}

async fn concurrent_hit_reschedule_allows_only_one_generation_cas(
    storage: PostgresStorage,
) -> Result<()> {
    let session_key_hash = "concurrent-hit-cas";
    let original = storage
        .replace_from_real_request(&replace_request(session_key_hash, b"init", 100))
        .await?;
    let barrier = Arc::new(tokio::sync::Barrier::new(2));

    let first = tokio::spawn({
        let storage = storage.clone();
        let barrier = Arc::clone(&barrier);
        async move {
            barrier.wait().await;
            storage
                .reschedule_after_cache_hit(&hit_request(session_key_hash, original.generation))
                .await
        }
    });
    let second = tokio::spawn({
        let storage = storage.clone();
        let barrier = Arc::clone(&barrier);
        async move {
            barrier.wait().await;
            storage
                .reschedule_after_cache_hit(&hit_request(session_key_hash, original.generation))
                .await
        }
    });

    let (first, second) = tokio::try_join!(first, second)?;
    let first = first?;
    let second = second?;
    assert_eq!(first.is_some() as u8 + second.is_some() as u8, 1);
    let winner = first.or(second).expect("one CAS succeeds");
    assert_eq!(winner.generation, 2);
    let final_record = storage
        .get_cache_keepalive_session(session_key_hash)
        .await?
        .expect("row exists");
    assert_eq!(final_record.generation, 2);
    assert_eq!(final_record.refresh_count, 1);
    Ok(())
}

fn replace_request(
    session_key_hash: &str,
    payload: &[u8],
    now: u64,
) -> CacheKeepaliveReplaceRequest {
    CacheKeepaliveReplaceRequest {
        session_key_hash: session_key_hash.to_owned(),
        principal_id: "principal".to_owned(),
        upstream_id: Uuid::from_u128(7),
        cache_anchor_at_unix_secs: now,
        ttl: CacheTtl::Ttl5m,
        run_at_unix_secs: now + 270,
        expires_at_unix_secs: now + 300,
        encrypted_payload: payload.to_vec(),
        accounting_key_id: None,
        display_reason: "agent-in-turn".to_owned(),
        config_snapshot: cc_lb_storage_api::CacheKeepaliveConfigSnapshot {
            refresh_lead_time_5m_secs: 30,
            refresh_lead_time_1h_secs: 300,
            max_refreshes_per_session: 12,
            max_total_duration_secs: 14_400,
            snapshot_max_bytes: 524_288,
        },
        now_unix_secs: now,
    }
}

fn hit_request(session_key_hash: &str, generation: u64) -> CacheKeepaliveHitRefreshRequest {
    CacheKeepaliveHitRefreshRequest {
        session_key_hash: session_key_hash.to_owned(),
        generation,
        cache_anchor_at_unix_secs: 150,
        run_at_unix_secs: 420,
        expires_at_unix_secs: 450,
        encrypted_payload: None,
        now_unix_secs: 151,
    }
}

struct Fixture {
    schema: String,
    admin_pool: PgPool,
    pool: PgPool,
}

impl Fixture {
    async fn create(url: &str) -> Result<Self> {
        let schema = format!("cache_keepalive_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
        sqlx::query(AssertSqlSafe(format!(
            "CREATE SCHEMA {}",
            quote_ident(&schema)
        )))
        .execute(&admin_pool)
        .await?;
        let pool = schema_pool(url, &schema).await?;
        Ok(Self {
            schema,
            admin_pool,
            pool,
        })
    }

    async fn drop_schema(self) -> Result<()> {
        self.pool.close().await;
        sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            quote_ident(&self.schema)
        )))
        .execute(&self.admin_pool)
        .await?;
        self.admin_pool.close().await;
        Ok(())
    }
}

async fn schema_pool(url: &str, schema: &str) -> Result<PgPool> {
    let options = PgConnectOptions::from_str(url)?.options([("search_path", schema)]);
    Ok(PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await?)
}

fn quote_ident(identifier: &str) -> String {
    assert!(
        identifier.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
        }),
        "unsafe postgres identifier: {identifier}"
    );
    format!("\"{identifier}\"")
}
