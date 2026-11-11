use std::{str::FromStr, sync::Arc};

use anyhow::Result;
use cc_lb_clock::SystemClock;
use cc_lb_storage_api::{
    CacheKeepaliveSessionStore, MetaStore, PoolQuotaHistoryStore, PoolQuotaSnapshotRecord,
    PriceCatalogCache, Provider, SubscriptionQuotaCheckpointRangeQuery,
    SubscriptionQuotaCheckpointRecord, SubscriptionQuotaProviderLotQuery, SubscriptionQuotaSample,
    SubscriptionQuotaSampleKind, SubscriptionQuotaSource, SubscriptionQuotaSourceMerge,
    SubscriptionQuotaStatus, SubscriptionQuotaWindow, UpstreamSubscriptionQuotaAggregateStore,
    UpstreamSubscriptionQuotaStore, UsageRollupStore, UsageTokenInterval, UsageTokenIntervalStore,
};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{AssertSqlSafe, PgPool, postgres::PgConnectOptions, postgres::PgPoolOptions};
use uuid::Uuid;

#[tokio::test]
async fn quota_aggregates_preserve_anchor_ties_and_inclusive_token_boundaries() -> Result<()> {
    let Some(url) = std::env::var("CI_POSTGRES_URL").ok() else {
        eprintln!("skip: CI_POSTGRES_URL not set; requires isolated local/test postgres DSN");
        return Ok(());
    };
    let fixture = Fixture::create(&url).await?;
    let storage = PostgresStorage::new(fixture.pool.clone(), Arc::new(SystemClock));
    storage.initialize().await?;
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

#[tokio::test]
async fn quota_deadband_keeps_exact_latest_and_pruning_preserves_each_anchor() -> Result<()> {
    let Some(url) = std::env::var("CI_POSTGRES_URL").ok() else {
        return Ok(());
    };
    let fixture = Fixture::create(&url).await?;
    let storage = PostgresStorage::new(fixture.pool.clone(), Arc::new(SystemClock));
    storage.initialize().await?;
    let result: Result<()> = async {
        let upstream = Uuid::from_u128(700);
        storage
            .record_subscription_quota_samples(&[sample(upstream, 10, 1, 0.2, 1000)])
            .await?;
        storage
            .record_subscription_quota_samples(&[sample(upstream, 20, 2, 0.2005, 1000)])
            .await?;
        let latest = storage
            .list_latest_subscription_quota_for_upstreams(&[upstream])
            .await?;
        assert_eq!(latest[0].utilization, Some(0.2005));
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM upstream_subscription_quota_checkpoints_v1")
                .fetch_one(&fixture.pool)
                .await?;
        assert_eq!(
            count, 1,
            "deadband must skip only the checkpoint, never the exact latest sample"
        );
        storage
            .record_subscription_quota_samples(&[sample(upstream, 30, 3, 0.202, 1000)])
            .await?;
        let mut changed = sample(upstream, 40, 4, 0.2025, 1000);
        changed.status = Some(SubscriptionQuotaStatus::Rejected);
        storage
            .record_subscription_quota_samples(&[changed])
            .await?;
        let mut threshold_changed = sample(upstream, 45, 8, 0.2026, 1000);
        threshold_changed.status = Some(SubscriptionQuotaStatus::Rejected);
        threshold_changed.surpassed_threshold = Some(0.95);
        storage
            .record_subscription_quota_samples(&[threshold_changed])
            .await?;
        storage
            .put_subscription_quota_checkpoints(&[
                checkpoint(upstream, 50, 5, 0.4, 1000),
                checkpoint(upstream, 50, 6, 0.5, 1000),
                checkpoint(Uuid::from_u128(701), 5, 7, 0.4, 1000),
            ])
            .await?;
        assert_eq!(
            storage
                .prune_subscription_quota_checkpoints_before(60, 0)
                .await?,
            0
        );
        assert_eq!(
            storage
                .prune_subscription_quota_checkpoints_before(60, 2)
                .await?,
            2
        );
        assert_eq!(
            storage
                .prune_subscription_quota_checkpoints_before(60, 100)
                .await?,
            3
        );
        let ids: Vec<Uuid> = sqlx::query_scalar(
            "SELECT sample_id FROM upstream_subscription_quota_checkpoints_v1 ORDER BY sample_id",
        )
        .fetch_all(&fixture.pool)
        .await?;
        assert_eq!(ids, vec![Uuid::from_u128(6), Uuid::from_u128(7)]);
        assert_eq!(
            storage
                .prune_subscription_quota_checkpoints_before(60, 100)
                .await?,
            0
        );
        Ok(())
    }
    .await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn auxiliary_pruners_share_batch_budget_and_pool_ranges_keep_left_anchors() -> Result<()> {
    let Some(url) = std::env::var("CI_POSTGRES_URL").ok() else {
        return Ok(());
    };
    let fixture = Fixture::create(&url).await?;
    let storage = PostgresStorage::new(fixture.pool.clone(), Arc::new(SystemClock));
    storage.initialize().await?;
    let result: Result<()> = async {
        let upstream = Uuid::from_u128(800);
        insert_rollup(&fixture.pool, upstream, 10, [1;4]).await?;
        insert_rollup(&fixture.pool, upstream, 20, [1;4]).await?;
        sqlx::query("INSERT INTO overview_excluded_error_rollups_v1 (resolution, bucket_start_unix_secs, error_count, updated_at) VALUES ('minute', 10, 1, NOW()), ('hour', 10, 1, NOW())")
            .execute(&fixture.pool).await?;
        assert_eq!(storage.prune_usage_rollups_before(15, 5, 0).await?, 0);
        assert_eq!(storage.prune_usage_rollups_before(15, 5, 1).await?, 1);
        assert_eq!(storage.prune_usage_rollups_before(15, 5, 1).await?, 1);
        assert_eq!(storage.prune_usage_rollups_before(15, 5, 1).await?, 0);
        assert_eq!(storage.prune_usage_rollups_before(15, 15, 10).await?, 1);
        sqlx::query("INSERT INTO cache_keepalive_decisions (source_ref_id, decision, reason, generation, ts, last_message_at_ms) VALUES ('old-1', 'skip', 'test', 1, 10, 10000), ('old-2', 'skip', 'test', 1, 10, 10000), ('boundary', 'skip', 'test', 1, 20, 20000)")
            .execute(&fixture.pool).await?;
        sqlx::query("INSERT INTO cache_keepalive_turns (source_ref_id, session_key_hash, principal_id, upstream_id, model, input_tokens, output_tokens, cache_creation_input_tokens, cache_creation_input_tokens_5m, cache_creation_input_tokens_1h, cache_read_input_tokens, cost_micros, hit_miss, ts) VALUES ('old-turn', 'session', 'principal', $1, 'model', 0,0,0,0,0,0,0,'hit',10)")
            .bind(upstream).execute(&fixture.pool).await?;
        assert_eq!(storage.prune_cache_keepalive_history_before(20, 0).await?, 0);
        assert_eq!(storage.prune_cache_keepalive_history_before(20, 2).await?, 2);
        assert_eq!(storage.prune_cache_keepalive_history_before(20, 2).await?, 1);
        assert_eq!(storage.prune_cache_keepalive_history_before(20, 2).await?, 0);
        let decisions: Vec<String> = sqlx::query_scalar("SELECT source_ref_id FROM cache_keepalive_decisions")
            .fetch_all(&fixture.pool).await?;
        assert_eq!(decisions, vec!["boundary"]);
        sqlx::query("INSERT INTO pool_subscription_quota_history_v1 (snapshot_at_unix_secs, quota_window, utilization, weighted_utilization_sum, capacity_ratio_sum, eligible_upstreams, contributing_upstreams, stale_upstreams, missing_observation_upstreams, missing_metadata_upstreams, header_contributing_upstreams, api_contributing_upstreams, computed_at_unix_millis) SELECT t, w, t::double precision / 100, 0,0,0,0,0,0,0,0,0,0 FROM UNNEST(ARRAY[10::bigint,20,30]) t CROSS JOIN UNNEST(ARRAY['5h','7d']) w")
            .execute(&fixture.pool).await?;
        assert_eq!(storage.prune_pool_quota_history_before(25, 0).await?, 0);
        assert_eq!(storage.prune_pool_quota_history_before(25, 1).await?, 1);
        assert_eq!(storage.prune_pool_quota_history_before(25, 10).await?, 1);
        let windows = [SubscriptionQuotaWindow::FiveHour, SubscriptionQuotaWindow::SevenDay];
        let summaries = storage.list_pool_quota_snapshot_summaries_in_range(cc_lb_storage_api::Provider::Anthropic,&windows, 25, 35).await?;
        assert_eq!(summaries.len(), 4);
        let idle = storage.list_pool_quota_snapshot_summaries_in_range(cc_lb_storage_api::Provider::Anthropic,&windows, 25, 29).await?;
        assert_eq!(idle.len(), 2);
        assert!(idle.iter().all(|row| row.snapshot_at_unix_secs == 25 && row.utilization == Some(0.2)));
        let exact = storage.list_pool_quota_snapshot_summaries_in_range(cc_lb_storage_api::Provider::Anthropic,&windows, 30, 35).await?;
        assert_eq!(exact.len(), 2, "exact summary boundary suppresses synthetic anchors");
        assert!(exact.iter().all(|row| row.snapshot_at_unix_secs == 30 && row.utilization == Some(0.3)));
        for window in windows {
            let points: Vec<_> = summaries.iter().filter(|row| row.window == window).collect();
            assert_eq!(points[0].snapshot_at_unix_secs, 25);
            assert_eq!(points[0].utilization, Some(0.2));
            assert_eq!(points[1].snapshot_at_unix_secs, 30);
        }
        for (bucket, expected_start) in [(None, 25), (Some(10), 20)] {
            let points = storage.list_pool_quota_chart_points_in_range(cc_lb_storage_api::Provider::Anthropic,&windows, 25, 35, bucket).await?;
            assert_eq!(points.len(), 4);
            assert_eq!(points.iter().filter(|row| row.snapshot_at_unix_secs == expected_start).count(), 2);
            let idle = storage.list_pool_quota_chart_points_in_range(cc_lb_storage_api::Provider::Anthropic,&windows, 25, 29, bucket).await?;
            assert_eq!(idle.len(), 2);
            assert!(idle.iter().all(|row| row.snapshot_at_unix_secs == expected_start && row.utilization == Some(0.2)));
            let exact = storage.list_pool_quota_chart_points_in_range(cc_lb_storage_api::Provider::Anthropic,&windows, 30, 35, bucket).await?;
            assert_eq!(exact.len(), 2, "exact boundary suppresses synthetic anchors");
            assert!(exact.iter().all(|row| row.utilization == Some(0.3)));
        }
        Ok(())
    }.await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn price_snapshots_retain_three_newest_distinct_payloads() -> Result<()> {
    let Some(url) = std::env::var("CI_POSTGRES_URL").ok() else {
        return Ok(());
    };
    let fixture = Fixture::create(&url).await?;
    let storage = PostgresStorage::new(fixture.pool.clone(), Arc::new(SystemClock));
    storage.initialize().await?;
    let result: Result<()> = async {
        for (bytes, ts) in [(br#"{"a":1}"#.as_slice(),10), (br#"{"b":2}"#.as_slice(),20),
            (br#"{"c":3}"#.as_slice(),30), (br#"{"a":1}"#.as_slice(),40), (br#"{"d":4}"#.as_slice(),30)] {
            storage.put_price_snapshot(bytes, ts).await?;
        }
        let rows: Vec<(String,i64)> = sqlx::query_as("SELECT payload, fetched_at_ms FROM price_catalog_snapshots_v1 ORDER BY fetched_at_ms DESC, id DESC")
            .fetch_all(&fixture.pool).await?;
        assert_eq!(rows, vec![(r#"{"a":1}"#.to_owned(),40),(r#"{"d":4}"#.to_owned(),30),(r#"{"c":3}"#.to_owned(),30)]);
        Ok(())
    }.await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

fn sample(
    upstream_id: Uuid,
    observed_at_unix_millis: u64,
    sample_id: u128,
    utilization: f64,
    resets_at_unix_secs: u64,
) -> SubscriptionQuotaSample {
    SubscriptionQuotaSample {
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
        credits: None,
        ingested_at_unix_millis: observed_at_unix_millis,
    }
}

fn checkpoint(
    upstream_id: Uuid,
    observed_at_unix_millis: u64,
    sample_id: u128,
    utilization: f64,
    resets_at_unix_secs: u64,
) -> SubscriptionQuotaCheckpointRecord {
    SubscriptionQuotaCheckpointRecord::from(&sample(
        upstream_id,
        observed_at_unix_millis,
        sample_id,
        utilization,
        resets_at_unix_secs,
    ))
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

#[tokio::test]
async fn pool_history_provider_migration_preserves_legacy_anthropic() -> Result<()> {
    let Some(url) = std::env::var("CI_POSTGRES_URL").ok() else {
        eprintln!("skip: CI_POSTGRES_URL not set; requires isolated local/test postgres DSN");
        return Ok(());
    };
    let fixture = Fixture::create(&url).await?;
    let result: Result<()> = async {
        let mut connection = fixture.pool.acquire().await?;
        sqlx::migrate!("./migrations")
            .run_direct(Some(162), &mut *connection, false)
            .await?;
        sqlx::query(
            "INSERT INTO pool_subscription_quota_history_v1 (
                snapshot_at_unix_secs, quota_window, utilization, weighted_utilization_sum,
                capacity_ratio_sum, eligible_upstreams, contributing_upstreams,
                stale_upstreams, missing_observation_upstreams, missing_metadata_upstreams,
                header_contributing_upstreams, api_contributing_upstreams,
                max_observed_at_unix_millis, computed_at_unix_millis, policy_version
            ) VALUES (10, '5h', 0.3, 1.2, 4, 7, 3, 2, 1, 1, 1, 2, 9000, 11000, 7),
                     (20, '7d', NULL, 1.2, 4, 7, 3, 2, 1, 1, 1, 2, 9000, 11000, 7)",
        )
        .execute(&mut *connection)
        .await?;
        drop(connection);
        let storage = PostgresStorage::new(fixture.pool.clone(), Arc::new(SystemClock));
        storage.initialize().await?;
        storage.initialize().await?;
        let expected = PoolQuotaSnapshotRecord {
            provider: Provider::Anthropic,
            snapshot_at_unix_secs: 10,
            window: SubscriptionQuotaWindow::FiveHour,
            utilization: Some(0.3),
            weighted_utilization_sum: 1.2,
            capacity_ratio_sum: 4.0,
            eligible_upstreams: 7,
            contributing_upstreams: 3,
            stale_upstreams: 2,
            missing_observation_upstreams: 1,
            missing_metadata_upstreams: 1,
            header_contributing_upstreams: 1,
            api_contributing_upstreams: 2,
            max_observed_at_unix_millis: Some(9000),
            computed_at_unix_millis: 11000,
            policy_version: 7,
        };
        let windows = [
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaWindow::SevenDay,
        ];
        let mut legacy = storage
            .list_latest_pool_quota_snapshots(Provider::Anthropic, &windows)
            .await?;
        assert_eq!(
            legacy,
            vec![
                expected.clone(),
                PoolQuotaSnapshotRecord {
                    snapshot_at_unix_secs: 20,
                    window: SubscriptionQuotaWindow::SevenDay,
                    utilization: None,
                    ..expected.clone()
                },
            ]
        );
        for provider in [Provider::Openai, Provider::Gemini] {
            assert!(
                storage
                    .list_latest_pool_quota_snapshots(provider, &windows)
                    .await?
                    .is_empty()
            );
        }
        // A running older node must still be able to record Anthropic history.
        sqlx::query(
            "INSERT INTO pool_subscription_quota_history_v1 (
                snapshot_at_unix_secs, quota_window, utilization, weighted_utilization_sum,
                capacity_ratio_sum, eligible_upstreams, contributing_upstreams,
                stale_upstreams, missing_observation_upstreams, missing_metadata_upstreams,
                header_contributing_upstreams, api_contributing_upstreams,
                max_observed_at_unix_millis, computed_at_unix_millis, policy_version
            ) VALUES (30, '5h', 0.3, 1.2, 4, 7, 3, 2, 1, 1, 1, 2, 9000, 11000, 7)",
        )
        .execute(storage.pool())
        .await?;
        legacy[0].snapshot_at_unix_secs = 30;
        storage
            .record_pool_quota_snapshots(&[PoolQuotaSnapshotRecord {
                provider: Provider::Openai,
                snapshot_at_unix_secs: 40,
                utilization: Some(0.8),
                ..expected
            }])
            .await?;
        assert_eq!(
            storage
                .list_latest_pool_quota_snapshots(Provider::Anthropic, &windows)
                .await?,
            legacy
        );
        let old_reader: (i64, Option<f64>) = sqlx::query_as(
            "SELECT snapshot_at_unix_secs, utilization
             FROM pool_subscription_quota_history_v1
             WHERE quota_window = '5h'
             ORDER BY snapshot_at_unix_secs DESC LIMIT 1",
        )
        .fetch_one(storage.pool())
        .await?;
        assert_eq!(
            old_reader,
            (30, Some(0.3)),
            "old nodes must not read OpenAI as Anthropic"
        );
        Ok(())
    }
    .await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

fn provider_pool_record(
    provider: Provider,
    window: SubscriptionQuotaWindow,
    at: i64,
) -> PoolQuotaSnapshotRecord {
    PoolQuotaSnapshotRecord {
        provider,
        snapshot_at_unix_secs: at,
        window,
        utilization: Some(0.25),
        weighted_utilization_sum: 0.25,
        capacity_ratio_sum: 1.0,
        eligible_upstreams: 2,
        contributing_upstreams: 1,
        stale_upstreams: 1,
        missing_observation_upstreams: 0,
        missing_metadata_upstreams: 0,
        header_contributing_upstreams: 0,
        api_contributing_upstreams: 1,
        max_observed_at_unix_millis: Some(at * 1000),
        computed_at_unix_millis: at * 1000,
        policy_version: 1,
    }
}

#[tokio::test]
async fn pool_history_provider_split_mixed_writes_are_atomic() -> Result<()> {
    let Some(url) = std::env::var("CI_POSTGRES_URL").ok() else {
        return Ok(());
    };
    let fixture = Fixture::create(&url).await?;
    let storage = PostgresStorage::new(fixture.pool.clone(), Arc::new(SystemClock));
    storage.initialize().await?;
    let result: Result<()> = async {
        let original =
            provider_pool_record(Provider::Anthropic, SubscriptionQuotaWindow::FiveHour, 10);
        storage
            .record_pool_quota_snapshots(std::slice::from_ref(&original))
            .await?;
        let replacement = PoolQuotaSnapshotRecord {
            utilization: Some(0.75),
            weighted_utilization_sum: 0.75,
            ..original.clone()
        };
        let records = [
            replacement,
            provider_pool_record(Provider::Openai, SubscriptionQuotaWindow::FiveHour, 10),
            provider_pool_record(
                Provider::Gemini,
                SubscriptionQuotaWindow::GeminiFiveHour,
                10,
            ),
        ];
        sqlx::raw_sql(
            "CREATE FUNCTION reject_provider_pool_insert() RETURNS trigger
             LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture insert failure'; END $$;
             CREATE TRIGGER reject_provider_pool_insert BEFORE INSERT
             ON pool_subscription_quota_history_v2 FOR EACH ROW
             EXECUTE FUNCTION reject_provider_pool_insert();",
        )
        .execute(storage.pool())
        .await?;
        assert!(storage.record_pool_quota_snapshots(&records).await.is_err());
        assert_eq!(
            storage
                .list_latest_pool_quota_snapshots(Provider::Anthropic, &[original.window])
                .await?,
            vec![original],
        );
        for record in &records[1..] {
            assert!(
                storage
                    .list_latest_pool_quota_snapshots(record.provider, &[record.window])
                    .await?
                    .is_empty()
            );
        }
        sqlx::query(
            "DROP TRIGGER reject_provider_pool_insert ON pool_subscription_quota_history_v2",
        )
        .execute(storage.pool())
        .await?;
        storage.record_pool_quota_snapshots(&records).await?;
        storage.record_pool_quota_snapshots(&records).await?;
        for record in records {
            assert_eq!(
                storage
                    .list_pool_quota_snapshots_in_range(record.provider, &[record.window], 10, 10)
                    .await?,
                vec![record],
            );
        }
        Ok(())
    }
    .await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn pool_history_provider_split_prunes_oldest_with_one_budget() -> Result<()> {
    let Some(url) = std::env::var("CI_POSTGRES_URL").ok() else {
        return Ok(());
    };
    let fixture = Fixture::create(&url).await?;
    let storage = PostgresStorage::new(fixture.pool.clone(), Arc::new(SystemClock));
    storage.initialize().await?;
    let result: Result<()> = async {
        let series = [
            (
                Provider::Anthropic,
                SubscriptionQuotaWindow::FiveHour,
                10,
                100,
            ),
            (
                Provider::Anthropic,
                SubscriptionQuotaWindow::SevenDayFable,
                15,
                90,
            ),
            (Provider::Openai, SubscriptionQuotaWindow::FiveHour, 20, 100),
            (Provider::Openai, SubscriptionQuotaWindow::SevenDay, 25, 80),
            (
                Provider::Gemini,
                SubscriptionQuotaWindow::GeminiFiveHour,
                30,
                100,
            ),
        ];
        let mut expected = series
            .iter()
            .flat_map(|&(provider, window, old, anchor)| {
                [old, anchor, 110].map(|at| provider_pool_record(provider, window, at))
            })
            .collect::<Vec<_>>();
        storage.record_pool_quota_snapshots(&expected).await?;
        assert_eq!(storage.prune_pool_quota_history_before(100, 0).await?, 0);
        for (provider, window, old, _) in series {
            assert_eq!(storage.prune_pool_quota_history_before(100, 1).await?, 1);
            expected.retain(|record| {
                !(record.provider == provider
                    && record.window == window
                    && record.snapshot_at_unix_secs == old)
            });
            for (provider, window, _, _) in series {
                let rows = storage
                    .list_pool_quota_snapshots_in_range(provider, &[window], 0, 200)
                    .await?;
                let remaining = expected
                    .iter()
                    .filter(|record| record.provider == provider && record.window == window)
                    .cloned()
                    .collect::<Vec<_>>();
                assert_eq!(
                    rows, remaining,
                    "one oldest row across both tables, with independent anchors"
                );
            }
        }
        assert_eq!(storage.prune_pool_quota_history_before(100, 1).await?, 0);
        Ok(())
    }
    .await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}

#[tokio::test]
async fn pool_history_provider_split_prune_failure_rolls_back_both_tables() -> Result<()> {
    let Some(url) = std::env::var("CI_POSTGRES_URL").ok() else {
        return Ok(());
    };
    let fixture = Fixture::create(&url).await?;
    let storage = PostgresStorage::new(fixture.pool.clone(), Arc::new(SystemClock));
    storage.initialize().await?;
    let result: Result<()> = async {
        let series = [
            (Provider::Anthropic, SubscriptionQuotaWindow::FiveHour, 10),
            (Provider::Openai, SubscriptionQuotaWindow::FiveHour, 20),
        ];
        let records = series
            .iter()
            .flat_map(|&(provider, window, old)| {
                [old, 100].map(|at| provider_pool_record(provider, window, at))
            })
            .collect::<Vec<_>>();
        storage.record_pool_quota_snapshots(&records).await?;
        sqlx::raw_sql(
            "CREATE TABLE provider_pool_prune_probe (calls INTEGER NOT NULL);
             INSERT INTO provider_pool_prune_probe VALUES (0);
             CREATE FUNCTION reject_second_pool_delete() RETURNS trigger
             LANGUAGE plpgsql AS $$
             DECLARE calls_after INTEGER;
             BEGIN
                 UPDATE provider_pool_prune_probe SET calls = calls + 1
                 RETURNING calls INTO calls_after;
                 IF calls_after = 2 THEN RAISE EXCEPTION 'fixture second delete failure'; END IF;
                 RETURN NULL;
             END $$;
             CREATE TRIGGER reject_pool_delete AFTER DELETE
             ON pool_subscription_quota_history_v1 FOR EACH STATEMENT
             EXECUTE FUNCTION reject_second_pool_delete();
             CREATE TRIGGER reject_pool_delete AFTER DELETE
             ON pool_subscription_quota_history_v2 FOR EACH STATEMENT
             EXECUTE FUNCTION reject_second_pool_delete();",
        )
        .execute(storage.pool())
        .await?;
        assert!(
            storage
                .prune_pool_quota_history_before(100, 2)
                .await
                .is_err()
        );
        let calls: i32 = sqlx::query_scalar("SELECT calls FROM provider_pool_prune_probe")
            .fetch_one(storage.pool())
            .await?;
        assert_eq!(
            calls, 0,
            "the first table's trigger mutation must also roll back"
        );
        for (provider, window, _) in series {
            let actual = storage
                .list_pool_quota_snapshots_in_range(provider, &[window], 0, 200)
                .await?;
            let expected = records
                .iter()
                .filter(|record| record.provider == provider)
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
        }
        sqlx::raw_sql(
            "DROP TRIGGER reject_pool_delete ON pool_subscription_quota_history_v1;
             DROP TRIGGER reject_pool_delete ON pool_subscription_quota_history_v2;",
        )
        .execute(storage.pool())
        .await?;
        assert_eq!(storage.prune_pool_quota_history_before(100, 2).await?, 2);
        for (provider, window, _) in series {
            assert_eq!(
                storage
                    .list_pool_quota_snapshots_in_range(provider, &[window], 0, 200)
                    .await?,
                vec![provider_pool_record(provider, window, 100)],
            );
        }
        Ok(())
    }
    .await;
    let teardown = fixture.drop_schema().await;
    result?;
    teardown
}
