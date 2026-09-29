#![cfg(feature = "postgres")]

use std::{str::FromStr, sync::Arc};

use anyhow::Result;
use cc_lb_clock::SystemClock;
use cc_lb_storage_api::{
    MetaStore, SubscriptionQuotaCheckpointRangeQuery, SubscriptionQuotaCheckpointRecord,
    SubscriptionQuotaProviderLotQuery, SubscriptionQuotaSample, SubscriptionQuotaSampleKind,
    SubscriptionQuotaSource, SubscriptionQuotaSourceMerge, SubscriptionQuotaStatus,
    SubscriptionQuotaWindow, UpstreamSubscriptionQuotaAggregateStore,
    UpstreamSubscriptionQuotaStore, UsageTokenInterval, UsageTokenIntervalStore,
};
use cc_lb_storage_postgres::PostgresStorage;
use cc_lb_storage_sqlite::open_sqlite;
use sqlx::{
    AssertSqlSafe, PgPool, SqlitePool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

#[tokio::test]
async fn quota_aggregate_parity_is_byte_equal() -> Result<()> {
    let Some(url) = std::env::var("CI_POSTGRES_URL").ok() else {
        eprintln!("skip: CI_POSTGRES_URL not set; requires isolated local/test postgres DSN");
        return Ok(());
    };
    let postgres = PostgresFixture::create(&url).await?;
    let postgres_storage = PostgresStorage::new(postgres.pool.clone(), Arc::new(SystemClock));
    postgres_storage.initialize().await?;
    let sqlite_dir = tempfile::tempdir()?;
    let sqlite_url = format!(
        "sqlite://{}",
        sqlite_dir.path().join("quota-parity.sqlite").display()
    );
    let sqlite_storage = open_sqlite(&sqlite_url, Arc::new(SystemClock)).await?;
    sqlite_storage.initialize().await?;
    let sqlite_pool = SqlitePool::connect(&sqlite_url).await?;
    let upstream_id = Uuid::from_u128(7);
    let checkpoints = vec![
        checkpoint(upstream_id, 30_000, 2, SubscriptionQuotaSource::Header, 0.2),
        checkpoint(upstream_id, 30_000, 1, SubscriptionQuotaSource::Header, 0.1),
        checkpoint(upstream_id, 75_000, 4, SubscriptionQuotaSource::Header, 0.4),
        checkpoint(upstream_id, 75_000, 3, SubscriptionQuotaSource::Header, 0.3),
        checkpoint(upstream_id, 75_000, 9, SubscriptionQuotaSource::Api, 0.35),
        checkpoint(
            upstream_id,
            135_000,
            5,
            SubscriptionQuotaSource::Header,
            0.1,
        ),
    ];
    postgres_storage
        .put_subscription_quota_checkpoints(&checkpoints)
        .await?;
    sqlite_storage
        .put_subscription_quota_checkpoints(&checkpoints)
        .await?;
    for (bucket, tokens) in [
        (60, [1, 2, 3, 4]),
        (120, [5, 6, 7, 8]),
        (180, [9, 10, 11, 12]),
    ] {
        insert_postgres_rollup(&postgres.pool, upstream_id, bucket, tokens).await?;
        insert_sqlite_rollup(&sqlite_pool, upstream_id, bucket, tokens).await?;
    }
    let range_query = SubscriptionQuotaCheckpointRangeQuery {
        upstream_ids: vec![upstream_id],
        windows: vec![SubscriptionQuotaWindow::FiveHour],
        sources: vec![
            SubscriptionQuotaSource::Header,
            SubscriptionQuotaSource::Api,
        ],
        since_unix_millis: 60_000,
        until_unix_millis: 180_000,
    };
    let lot_query = SubscriptionQuotaProviderLotQuery {
        upstream_ids: vec![upstream_id],
        windows: vec![SubscriptionQuotaWindow::FiveHour],
        sources: vec![
            SubscriptionQuotaSource::Header,
            SubscriptionQuotaSource::Api,
        ],
        since_unix_millis: 60_000,
        until_unix_millis: 180_000,
        source_merge: SubscriptionQuotaSourceMerge::Merged,
        evaluation_unix_secs: 180,
    };
    let intervals = [
        UsageTokenInterval {
            interval_id: 1,
            upstream_id,
            start_unix_secs: 60,
            end_unix_secs: 120,
        },
        UsageTokenInterval {
            interval_id: 2,
            upstream_id,
            start_unix_secs: 180,
            end_unix_secs: 180,
        },
    ];
    let postgres_bytes = serde_json::to_vec(&(
        postgres_storage
            .list_subscription_quota_slim_checkpoints(range_query.clone())
            .await?,
        postgres_storage
            .list_subscription_quota_provider_lots(lot_query.clone())
            .await?,
        postgres_storage
            .sum_usage_tokens_for_intervals(&intervals)
            .await?,
    ))?;
    let sqlite_bytes = serde_json::to_vec(&(
        sqlite_storage
            .list_subscription_quota_slim_checkpoints(range_query)
            .await?,
        sqlite_storage
            .list_subscription_quota_provider_lots(lot_query)
            .await?,
        sqlite_storage
            .sum_usage_tokens_for_intervals(&intervals)
            .await?,
    ))?;
    assert_eq!(postgres_bytes, sqlite_bytes);
    sqlite_pool.close().await;
    postgres.drop_schema().await
}

fn checkpoint(
    upstream_id: Uuid,
    observed_at_unix_millis: u64,
    sample_id: u128,
    source: SubscriptionQuotaSource,
    utilization: f64,
) -> SubscriptionQuotaCheckpointRecord {
    let resets_at_unix_secs = if observed_at_unix_millis < 120_000 {
        2_000_000
    } else {
        2_018_000
    };
    SubscriptionQuotaCheckpointRecord::from(&SubscriptionQuotaSample {
        upstream_id,
        window: SubscriptionQuotaWindow::FiveHour,
        source,
        sample_kind: SubscriptionQuotaSampleKind::Sample,
        observed_at_unix_millis,
        sample_id: Uuid::from_u128(sample_id),
        utilization: Some(utilization),
        status: Some(SubscriptionQuotaStatus::Allowed),
        resets_at_unix_secs: Some(resets_at_unix_secs),
        surpassed_threshold: None,
        representative_claim: None,
        fallback_percentage: None,
        fallback_available: None,
        overage_in_use: None,
        overage_period_monthly_utilization: None,
        upgrade_paths: None,
        disabled_reason: None,
        extra_usage_enabled: None,
        extra_usage_monthly_limit: None,
        extra_usage_used_credits: None,
        ingested_at_unix_millis: observed_at_unix_millis,
    })
}

async fn insert_postgres_rollup(
    pool: &PgPool,
    upstream_id: Uuid,
    bucket_start_unix_secs: i64,
    tokens: [i64; 4],
) -> Result<()> {
    sqlx::query(
        "INSERT INTO usage_rollups_v2 \
         (resolution, bucket_start_unix_secs, principal_id, upstream_id, upstream_name, model, \
          input_tokens, output_tokens, cache_creation_input_tokens, cache_read_input_tokens, updated_at) \
         VALUES ('minute', $1, 'principal', $2, 'upstream', $3, $4, $5, $6, $7, NOW())",
    )
    .bind(bucket_start_unix_secs)
    .bind(upstream_id)
    .bind(format!("model-{bucket_start_unix_secs}"))
    .bind(tokens[0])
    .bind(tokens[1])
    .bind(tokens[2])
    .bind(tokens[3])
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_sqlite_rollup(
    pool: &SqlitePool,
    upstream_id: Uuid,
    bucket_start_unix_secs: i64,
    tokens: [i64; 4],
) -> Result<()> {
    sqlx::query(
        "INSERT INTO usage_rollups_v2 \
         (resolution, bucket_start_unix_secs, principal_id, upstream_id, upstream_name, model, \
          input_tokens, output_tokens, cache_creation_input_tokens, cache_read_input_tokens, updated_at) \
         VALUES ('minute', ?, 'principal', ?, 'upstream', ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)",
    )
    .bind(bucket_start_unix_secs)
    .bind(upstream_id.to_string())
    .bind(format!("model-{bucket_start_unix_secs}"))
    .bind(tokens[0])
    .bind(tokens[1])
    .bind(tokens[2])
    .bind(tokens[3])
    .execute(pool)
    .await?;
    Ok(())
}

struct PostgresFixture {
    schema: String,
    admin_pool: PgPool,
    pool: PgPool,
}

impl PostgresFixture {
    async fn create(url: &str) -> Result<Self> {
        let schema = format!("quota_parity_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
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
            self.schema
        )))
        .execute(&self.admin_pool)
        .await?;
        self.admin_pool.close().await;
        Ok(())
    }
}
