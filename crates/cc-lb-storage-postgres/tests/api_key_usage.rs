use std::{str::FromStr, sync::Arc};

use anyhow::{Context, Result};
use cc_lb_clock::TestClock;
use cc_lb_storage_api::{
    ApiKeyUsage, ApiKeyUsageBucketDelta, ApiKeyUsageBucketKey, ApiKeyUsageBucketQuery,
    ApiKeyUsageBucketStore, ApiKeyUsageFlush, ApiKeyUsageFlushResult, BackendKind, MetaStore,
};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{
    AssertSqlSafe, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

const NOW: u64 = 10_000;

#[test]
fn t3_postgres__api_key_usage_contracts() {
    let url = crate::postgres_fixture::required_postgres_url();
    tokio::runtime::Runtime::new()
        .expect("tokio runtime")
        .block_on(async move {
            let fixture = Fixture::create(&url).await?;
            let result = run_contracts(&fixture.pool).await;
            fixture
                .drop_schema()
                .await
                .context("drop API-key usage schema")?;
            result
        })
        .expect("PostgreSQL durable API-key usage contracts");
}

async fn run_contracts(pool: &PgPool) -> Result<()> {
    let clock = Arc::new(TestClock::new_at_secs(NOW));
    let storage = PostgresStorage::new(pool.clone(), clock.clone());
    storage.initialize(BackendKind::Postgres).await?;
    let first = Uuid::now_v7();
    let second = Uuid::now_v7();
    for (writer, requests) in [(first, 2), (second, 3)] {
        storage
            .register_api_key_usage_writer(writer, NOW + 60)
            .await?;
        let flush = usage_flush(writer, requests, NOW + 60);
        assert_eq!(
            storage.flush_api_key_usage(&flush).await?,
            ApiKeyUsageFlushResult::Applied
        );
        assert_eq!(
            storage.flush_api_key_usage(&flush).await?,
            ApiKeyUsageFlushResult::AlreadyApplied
        );
    }
    let remote = storage
        .query_api_key_usage_buckets(&ApiKeyUsageBucketQuery {
            key_ids: vec!["key-a".to_owned()],
            since_unix_secs: 0,
            until_unix_secs: NOW,
            exclude_writer_epoch: first,
        })
        .await?;
    assert_eq!(remote.len(), 1);
    assert_eq!(remote[0].usage.requests, 3);

    let expired = Uuid::now_v7();
    storage
        .register_api_key_usage_writer(expired, NOW - 1)
        .await?;
    assert_eq!(
        storage
            .flush_api_key_usage(&usage_flush(expired, 9, NOW + 60))
            .await?,
        ApiKeyUsageFlushResult::LeaseLost
    );

    clock.advance_secs(180);
    let first_run = storage
        .compact_api_key_usage_buckets(60, 7 * 86_400, 1)
        .await?;
    let second_run = storage
        .compact_api_key_usage_buckets(60, 7 * 86_400, 1)
        .await?;
    assert_eq!(first_run.folded_rows + second_run.folded_rows, 2);
    assert_eq!(
        first_run.pruned_rows + second_run.pruned_rows,
        0,
        "live retention buckets must survive"
    );
    assert_eq!(
        storage
            .flush_api_key_usage(&usage_flush(first, 7, NOW + 240))
            .await?,
        ApiKeyUsageFlushResult::LeaseLost
    );
    let buckets = storage
        .query_api_key_usage_buckets(&ApiKeyUsageBucketQuery {
            key_ids: vec!["key-a".to_owned()],
            since_unix_secs: 0,
            until_unix_secs: NOW + 180,
            exclude_writer_epoch: Uuid::now_v7(),
        })
        .await?;
    assert_eq!(buckets.len(), 1);
    assert_eq!(
        buckets[0].usage,
        ApiKeyUsage {
            requests: 5,
            input_tokens: 50,
            output_tokens: 100,
            cost_usd_micros: 150
        }
    );
    Ok(())
}

fn usage_flush(writer_epoch: Uuid, requests: i64, lease_until_unix_secs: u64) -> ApiKeyUsageFlush {
    ApiKeyUsageFlush {
        writer_epoch,
        flush_id: Uuid::now_v7(),
        lease_until_unix_secs,
        deltas: vec![ApiKeyUsageBucketDelta {
            key: ApiKeyUsageBucketKey {
                key_id: "key-a".to_owned(),
                bucket_width_secs: 60,
                bucket_start_unix_secs: NOW - 60,
            },
            usage: ApiKeyUsage {
                requests,
                input_tokens: requests * 10,
                output_tokens: requests * 20,
                cost_usd_micros: requests * 30,
            },
        }],
    }
}

struct Fixture {
    schema: String,
    admin_pool: PgPool,
    pool: PgPool,
}
impl Fixture {
    async fn create(url: &str) -> Result<Self> {
        let schema = format!("api_key_usage_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
        sqlx::query(AssertSqlSafe(format!(
            "CREATE SCHEMA {}",
            quote_ident(&schema)
        )))
        .execute(&admin_pool)
        .await?;
        let options = PgConnectOptions::from_str(url)?.options([("search_path", schema.as_str())]);
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;
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
fn quote_ident(identifier: &str) -> String {
    assert!(
        identifier
            .chars()
            .all(|character| character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || character == '_')
    );
    format!("\"{identifier}\"")
}
