use cc_lb_storage_api::{
    BackendKind, MetaStore, SubscriptionQuotaCheckpointRangeQuery,
    SubscriptionQuotaCheckpointRecord, SubscriptionQuotaProviderLot,
    SubscriptionQuotaProviderLotQuery, SubscriptionQuotaSampleKind,
    SubscriptionQuotaSemanticFingerprint, SubscriptionQuotaSlimCheckpoint, SubscriptionQuotaSource,
    SubscriptionQuotaSourceMerge, SubscriptionQuotaStatus, SubscriptionQuotaWindow,
    UpstreamSubscriptionQuotaAggregateStore, UpstreamSubscriptionQuotaStore, UsageTokenInterval,
    UsageTokenIntervalStore, UsageTokenIntervalSum,
};
use cc_lb_storage_sqlite::SqliteStorage;
use sqlx::Row;
use tempfile::TempDir;
use uuid::Uuid;

async fn quota_storage(database_name: &str) -> (TempDir, SqliteStorage) {
    let temp_dir = tempfile::tempdir().expect("create quota tempdir");
    let database_url = format!("sqlite://{}", temp_dir.path().join(database_name).display());
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, cc_lb_testkit::fixed_clock(1_700_000_000))
            .await
            .expect("open quota sqlite");
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("initialize quota sqlite");
    (temp_dir, storage)
}

fn quota_checkpoint(
    upstream_id: Uuid,
    changed_at_unix_millis: u64,
    sample_id: u128,
    utilization: f64,
    resets_at_unix_secs: Option<u64>,
) -> SubscriptionQuotaCheckpointRecord {
    SubscriptionQuotaCheckpointRecord {
        upstream_id,
        window: SubscriptionQuotaWindow::FiveHour,
        source: SubscriptionQuotaSource::Header,
        changed_at_unix_millis,
        semantic_fingerprint: SubscriptionQuotaSemanticFingerprint::from_bytes(
            [u8::try_from(sample_id).expect("sample id fits fixture fingerprint"); 32],
        ),
        sample_kind: SubscriptionQuotaSampleKind::Sample,
        sample_id: Uuid::from_u128(sample_id),
        representative_claim: None,
        utilization: Some(utilization),
        status: Some(SubscriptionQuotaStatus::Allowed),
        resets_at_unix_secs,
        surpassed_threshold: None,
        fallback_percentage: None,
        fallback_available: None,
        overage_in_use: None,
        overage_period_monthly_utilization: None,
        upgrade_paths: None,
        disabled_reason: None,
        extra_usage_enabled: None,
        extra_usage_monthly_limit: None,
        extra_usage_used_credits: None,
        ingested_at_unix_millis: changed_at_unix_millis,
    }
}

fn slim(checkpoint: SubscriptionQuotaCheckpointRecord) -> SubscriptionQuotaSlimCheckpoint {
    SubscriptionQuotaSlimCheckpoint {
        upstream_id: checkpoint.upstream_id,
        window: checkpoint.window,
        source: checkpoint.source,
        changed_at_unix_millis: checkpoint.changed_at_unix_millis,
        sample_id: checkpoint.sample_id,
        utilization: checkpoint.utilization,
        status: checkpoint.status,
        resets_at_unix_secs: checkpoint.resets_at_unix_secs,
    }
}

#[tokio::test]
async fn t3__quota_slim_checkpoints_match_old_ranges_with_anchor_and_sample_ties() {
    let (_temp_dir, storage) = quota_storage("slim-parity.sqlite").await;
    let upstream_id = Uuid::from_u128(0x100);
    let records = vec![
        quota_checkpoint(upstream_id, 1_000, 1, 0.1, Some(18_000)),
        quota_checkpoint(upstream_id, 1_000, 2, 0.2, Some(18_000)),
        quota_checkpoint(upstream_id, 2_000, 3, 0.3, Some(18_000)),
        quota_checkpoint(upstream_id, 3_000, 4, 0.4, Some(18_000)),
        quota_checkpoint(upstream_id, 3_000, 5, 0.5, Some(18_000)),
    ];
    storage
        .put_subscription_quota_checkpoints(&records)
        .await
        .expect("seed quota checkpoints");
    let query = SubscriptionQuotaCheckpointRangeQuery {
        upstream_ids: vec![upstream_id],
        windows: vec![SubscriptionQuotaWindow::FiveHour],
        sources: vec![SubscriptionQuotaSource::Header],
        since_unix_millis: 2_000,
        until_unix_millis: 3_000,
    };

    let old_ranges = storage
        .list_subscription_quota_checkpoint_ranges(query.clone())
        .await
        .expect("read old quota ranges");
    let mut expected = Vec::new();
    for range in old_ranges {
        expected.extend(range.left_anchor.map(slim));
        expected.extend(range.checkpoints.into_iter().map(slim));
    }
    let actual = storage
        .list_subscription_quota_slim_checkpoints(query)
        .await
        .expect("read slim quota checkpoints");

    assert_eq!(actual, expected);
    assert_eq!(actual[0].sample_id, Uuid::from_u128(2));
    assert_eq!(actual[3].sample_id, Uuid::from_u128(5));
}

#[tokio::test]
async fn t3__quota_left_anchor_plan_avoids_correlated_anti_join() {
    let (_temp_dir, storage) = quota_storage("left-anchor-plan.sqlite").await;
    let upstream_id = Uuid::from_u128(0x101);

    let plan = sqlx::query(
        "EXPLAIN QUERY PLAN \
         WITH anchor_ts AS ( \
             SELECT upstream_id, window, source, MAX(changed_at_unix_millis) AS changed_at_unix_millis \
             FROM upstream_subscription_quota_checkpoints_v1 \
             WHERE upstream_id IN (?) \
               AND window IN ('5h') \
               AND source IN ('header') \
               AND changed_at_unix_millis < ? \
             GROUP BY upstream_id, window, source \
         ), \
         anchor_ids AS ( \
             SELECT checkpoint.upstream_id, checkpoint.window, checkpoint.source, \
                    checkpoint.changed_at_unix_millis, MAX(checkpoint.sample_id) AS sample_id \
             FROM upstream_subscription_quota_checkpoints_v1 checkpoint \
             INNER JOIN anchor_ts anchor \
               ON anchor.upstream_id = checkpoint.upstream_id \
              AND anchor.window = checkpoint.window \
              AND anchor.source = checkpoint.source \
              AND anchor.changed_at_unix_millis = checkpoint.changed_at_unix_millis \
             GROUP BY checkpoint.upstream_id, checkpoint.window, checkpoint.source, \
                      checkpoint.changed_at_unix_millis \
         ) \
         SELECT checkpoint.upstream_id, checkpoint.window, checkpoint.source, \
                checkpoint.changed_at_unix_millis, checkpoint.sample_id, \
                checkpoint.utilization, checkpoint.status, checkpoint.resets_at_unix_secs \
         FROM upstream_subscription_quota_checkpoints_v1 checkpoint \
         INNER JOIN anchor_ids anchor \
           ON anchor.upstream_id = checkpoint.upstream_id \
          AND anchor.window = checkpoint.window \
          AND anchor.source = checkpoint.source \
          AND anchor.changed_at_unix_millis = checkpoint.changed_at_unix_millis \
          AND anchor.sample_id = checkpoint.sample_id",
    )
    .bind(upstream_id.to_string())
    .bind(2_000_i64)
    .fetch_all(storage.pool())
    .await
    .expect("explain left-anchor query");
    let details = plan
        .into_iter()
        .map(|row| row.try_get::<String, _>("detail").expect("plan detail"))
        .collect::<Vec<_>>();

    assert!(
        details
            .iter()
            .all(|detail| !detail.contains("CORRELATED SCALAR SUBQUERY")),
        "left-anchor query must not use a correlated anti-join plan: {details:?}"
    );
}

#[tokio::test]
async fn t3__quota_provider_lots_preserve_reset_cycles_without_full_records() {
    let (_temp_dir, storage) = quota_storage("provider-lots.sqlite").await;
    let upstream_id = Uuid::from_u128(0x200);
    storage
        .put_subscription_quota_checkpoints(&[
            quota_checkpoint(upstream_id, 0, 1, 0.8, Some(18_000)),
            quota_checkpoint(upstream_id, 60_000, 2, 0.2, Some(36_000)),
        ])
        .await
        .expect("seed provider lot checkpoints");

    let lots = storage
        .list_subscription_quota_provider_lots(SubscriptionQuotaProviderLotQuery {
            upstream_ids: vec![upstream_id],
            windows: vec![SubscriptionQuotaWindow::FiveHour],
            sources: vec![
                SubscriptionQuotaSource::Header,
                SubscriptionQuotaSource::Api,
            ],
            since_unix_millis: 0,
            until_unix_millis: 120_000,
            source_merge: SubscriptionQuotaSourceMerge::Header,
            evaluation_unix_secs: 1_000,
        })
        .await
        .expect("read provider lots");

    assert_eq!(
        lots,
        vec![
            SubscriptionQuotaProviderLot {
                upstream_id,
                window: SubscriptionQuotaWindow::FiveHour,
                source: SubscriptionQuotaSourceMerge::Header,
                provider_start_unix_secs: Some(0),
                provider_reset_unix_secs: Some(18_000),
                observed_at_unix_millis: 0,
                evaluation_unix_secs: 0,
                utilization: 0.8,
            },
            SubscriptionQuotaProviderLot {
                upstream_id,
                window: SubscriptionQuotaWindow::FiveHour,
                source: SubscriptionQuotaSourceMerge::Header,
                provider_start_unix_secs: Some(18_000),
                provider_reset_unix_secs: Some(36_000),
                observed_at_unix_millis: 60_000,
                evaluation_unix_secs: 1_000,
                utilization: 0.2,
            },
        ]
    );
}

#[tokio::test]
async fn t3__quota_usage_token_intervals_include_both_boundaries_and_use_covering_indexes() {
    let (_temp_dir, storage) = quota_storage("token-intervals.sqlite").await;
    let upstream_id = Uuid::from_u128(0x300);
    for (resolution, bucket, tokens) in [
        ("minute", 99_i64, [100_i64, 0, 0, 0]),
        ("minute", 100, [1, 2, 3, 4]),
        ("minute", 200, [5, 6, 7, 8]),
        ("minute", 201, [100, 0, 0, 0]),
        ("hour", 100, [1_000, 0, 0, 0]),
    ] {
        sqlx::query(
            "INSERT INTO usage_rollups_v2 \
             (resolution, bucket_start_unix_secs, principal_id, upstream_id, upstream_name, model, \
              input_tokens, output_tokens, cache_creation_input_tokens, cache_read_input_tokens, updated_at) \
             VALUES (?, ?, 'principal', ?, 'upstream', 'model', ?, ?, ?, ?, 0)",
        )
        .bind(resolution)
        .bind(bucket)
        .bind(upstream_id.to_string())
        .bind(tokens[0])
        .bind(tokens[1])
        .bind(tokens[2])
        .bind(tokens[3])
        .execute(storage.pool())
        .await
        .expect("seed usage rollup");
    }

    let sums = storage
        .sum_usage_tokens_for_intervals(&[
            UsageTokenInterval {
                interval_id: 7,
                upstream_id,
                start_unix_secs: 100,
                end_unix_secs: 200,
            },
            UsageTokenInterval {
                interval_id: 8,
                upstream_id: Uuid::from_u128(0x301),
                start_unix_secs: 100,
                end_unix_secs: 200,
            },
        ])
        .await
        .expect("sum quota usage tokens");
    assert_eq!(
        sums,
        vec![
            UsageTokenIntervalSum {
                interval_id: 7,
                tokens: 36,
            },
            UsageTokenIntervalSum {
                interval_id: 8,
                tokens: 0,
            },
        ]
    );

    let checkpoint_plan = sqlx::query(
        "EXPLAIN QUERY PLAN \
         SELECT utilization, status, resets_at_unix_secs \
         FROM upstream_subscription_quota_checkpoints_v1 \
         WHERE upstream_id = ? AND window = '5h' AND source = 'header' \
           AND changed_at_unix_millis >= 0 AND changed_at_unix_millis <= 604800000 \
         ORDER BY changed_at_unix_millis ASC, sample_id ASC",
    )
    .bind(upstream_id.to_string())
    .fetch_all(storage.pool())
    .await
    .expect("explain slim checkpoint query");
    let checkpoint_details = checkpoint_plan
        .into_iter()
        .map(|row| {
            row.try_get::<String, _>("detail")
                .expect("checkpoint plan detail")
        })
        .collect::<Vec<_>>();
    assert!(checkpoint_details.iter().any(|detail| {
        detail.contains("USING COVERING INDEX upstream_subscription_quota_checkpoints_slim_idx")
    }));

    let token_plan = sqlx::query(
        "EXPLAIN QUERY PLAN \
         WITH intervals(ordinal, interval_id, upstream_id, start_unix_secs, end_unix_secs) \
         AS (VALUES (0, 7, ?, 100, 200)) \
         SELECT intervals.interval_id, \
                COALESCE(SUM(rollup.input_tokens + rollup.output_tokens + \
                             rollup.cache_creation_input_tokens + rollup.cache_read_input_tokens), 0) \
         FROM intervals \
         LEFT JOIN usage_rollups_v2 rollup \
           ON rollup.upstream_id = intervals.upstream_id \
          AND rollup.resolution = 'minute' \
          AND rollup.bucket_start_unix_secs >= intervals.start_unix_secs \
          AND rollup.bucket_start_unix_secs <= intervals.end_unix_secs \
         GROUP BY intervals.ordinal, intervals.interval_id",
    )
    .bind(upstream_id.to_string())
    .fetch_all(storage.pool())
    .await
    .expect("explain quota token interval query");
    let token_details = token_plan
        .into_iter()
        .map(|row| {
            row.try_get::<String, _>("detail")
                .expect("token plan detail")
        })
        .collect::<Vec<_>>();
    assert!(token_details.iter().any(|detail| {
        detail.contains("USING COVERING INDEX usage_rollups_v2_token_interval_idx")
    }));
}
