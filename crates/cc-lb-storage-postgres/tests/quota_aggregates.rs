use std::str::FromStr;

use anyhow::Result;
use cc_lb_storage_api::{
    BackendKind, MetaStore, SubscriptionQuotaCheckpointRangeQuery,
    SubscriptionQuotaCheckpointRecord, SubscriptionQuotaProviderLotQuery, SubscriptionQuotaSample,
    SubscriptionQuotaSampleKind, SubscriptionQuotaSource, SubscriptionQuotaSourceMerge,
    SubscriptionQuotaStatus, SubscriptionQuotaWindow, UpstreamSubscriptionQuotaAggregateStore,
    UpstreamSubscriptionQuotaStore, UsageTokenInterval, UsageTokenIntervalStore,
};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{AssertSqlSafe, PgPool, postgres::PgConnectOptions, postgres::PgPoolOptions};
use uuid::Uuid;

#[tokio::test]
async fn t3_postgres__quota_aggregates_preserve_anchor_ties_and_inclusive_token_boundaries()
-> Result<()> {
    let url = crate::postgres_fixture::required_postgres_url();
    let fixture = Fixture::create(&url).await?;
    let storage = PostgresStorage::new(
        fixture.pool.clone(),
        cc_lb_testkit::fixed_clock(1_700_000_000),
    );
    storage.initialize(BackendKind::Postgres).await?;
    let upstream_id = Uuid::from_u128(1);
    let checkpoints = [
        checkpoint(upstream_id, 30_000, 2, 0.2, 2_000_000),
        checkpoint(upstream_id, 30_000, 1, 0.1, 2_000_000),
        checkpoint(upstream_id, 75_000, 4, 0.4, 2_000_000),
        checkpoint(upstream_id, 75_000, 3, 0.3, 2_000_000),
        checkpoint(upstream_id, 135_000, 5, 0.1, 2_018_000),
        checkpoint(upstream_id, 240_000, 6, 0.8, 2_018_000),
    ];
    storage
        .put_subscription_quota_checkpoints(&checkpoints)
        .await?;

    let slim = storage
        .list_subscription_quota_slim_checkpoints(SubscriptionQuotaCheckpointRangeQuery {
            upstream_ids: vec![upstream_id],
            windows: vec![SubscriptionQuotaWindow::FiveHour],
            sources: vec![SubscriptionQuotaSource::Header],
            since_unix_millis: 60_000,
            until_unix_millis: 180_000,
        })
        .await?;
    assert_eq!(
        slim.iter().map(|row| row.sample_id).collect::<Vec<_>>(),
        vec![
            Uuid::from_u128(2),
            Uuid::from_u128(3),
            Uuid::from_u128(4),
            Uuid::from_u128(5),
        ],
        "the left anchor must be the greatest same-millis sample id and range ties must sort ascending",
    );

    let lots = storage
        .list_subscription_quota_provider_lots(SubscriptionQuotaProviderLotQuery {
            upstream_ids: vec![upstream_id],
            windows: vec![SubscriptionQuotaWindow::FiveHour],
            sources: vec![SubscriptionQuotaSource::Header],
            since_unix_millis: 60_000,
            until_unix_millis: 180_000,
            source_merge: SubscriptionQuotaSourceMerge::Header,
            evaluation_unix_secs: 180,
        })
        .await?;
    assert_eq!(lots.len(), 2);
    assert_eq!(lots[0].observed_at_unix_millis, 75_000);
    assert_eq!(lots[0].utilization, 0.4);
    assert_eq!(lots[0].provider_reset_unix_secs, Some(2_000_000));
    assert_eq!(lots[0].provider_start_unix_secs, Some(1_982_000));
    assert_eq!(lots[0].evaluation_unix_secs, 60);
    assert_eq!(lots[1].observed_at_unix_millis, 135_000);
    assert_eq!(lots[1].utilization, 0.1);
    assert_eq!(lots[1].evaluation_unix_secs, 180);

    insert_rollup(&fixture.pool, upstream_id, 100, [1, 2, 3, 4]).await?;
    insert_rollup(&fixture.pool, upstream_id, 200, [5, 6, 7, 8]).await?;
    insert_rollup(&fixture.pool, upstream_id, 201, [25, 25, 25, 25]).await?;
    let interval_sums = storage
        .sum_usage_tokens_for_intervals(&[
            UsageTokenInterval {
                interval_id: 10,
                upstream_id,
                start_unix_secs: 100,
                end_unix_secs: 200,
            },
            UsageTokenInterval {
                interval_id: 11,
                upstream_id,
                start_unix_secs: 200,
                end_unix_secs: 200,
            },
            UsageTokenInterval {
                interval_id: 12,
                upstream_id: Uuid::from_u128(99),
                start_unix_secs: 0,
                end_unix_secs: 1_000,
            },
        ])
        .await?;
    assert_eq!(
        interval_sums
            .iter()
            .map(|sum| (sum.interval_id, sum.tokens))
            .collect::<Vec<_>>(),
        vec![(10, 36), (11, 26), (12, 0)],
        "both interval boundaries must be inclusive and empty intervals must return zero",
    );
    fixture.drop_schema().await
}

fn checkpoint(
    upstream_id: Uuid,
    observed_at_unix_millis: u64,
    sample_id: u128,
    utilization: f64,
    resets_at_unix_secs: u64,
) -> SubscriptionQuotaCheckpointRecord {
    SubscriptionQuotaCheckpointRecord::from(&SubscriptionQuotaSample {
        upstream_id,
        window: SubscriptionQuotaWindow::FiveHour,
        source: SubscriptionQuotaSource::Header,
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

async fn insert_rollup(
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

struct Fixture {
    schema: String,
    admin_pool: PgPool,
    pool: PgPool,
}

impl Fixture {
    async fn create(url: &str) -> Result<Self> {
        let schema = format!("quota_aggregates_{}", Uuid::new_v4().simple());
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
