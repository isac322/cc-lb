use std::sync::Arc;

use cc_lb_aead::EncryptedOAuthTokens;
use cc_lb_storage_api::{
    MetaStore, OAuthTokensCreate, StorageError, SubscriptionQuotaCheckpointRangeQuery,
    SubscriptionQuotaCheckpointRecord, SubscriptionQuotaPollFence, SubscriptionQuotaProviderLot,
    SubscriptionQuotaProviderLotQuery, SubscriptionQuotaSample, SubscriptionQuotaSampleKind,
    SubscriptionQuotaSlimCheckpoint, SubscriptionQuotaSource, SubscriptionQuotaSourceMerge,
    SubscriptionQuotaStatus, SubscriptionQuotaWindow, UpstreamCreate, UpstreamStore,
    UpstreamSubscriptionQuotaAggregateStore, UpstreamSubscriptionQuotaStore, UsageTokenInterval,
    UsageTokenIntervalStore, UsageTokenIntervalSum, upstream::UpstreamKind,
};
use cc_lb_storage_sqlite::SqliteStorage;
use sqlx::Row;
use tempfile::TempDir;
use uuid::Uuid;

async fn quota_storage(database_name: &str) -> (TempDir, SqliteStorage) {
    let temp_dir = tempfile::tempdir().expect("create quota tempdir");
    let database_url = format!("sqlite://{}", temp_dir.path().join(database_name).display());
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .expect("open quota sqlite");
    storage.initialize().await.expect("initialize quota sqlite");
    (temp_dir, storage)
}

fn quota_checkpoint(
    upstream_id: Uuid,
    changed_at_unix_millis: u64,
    sample_id: u128,
    utilization: f64,
    resets_at_unix_secs: Option<u64>,
) -> SubscriptionQuotaCheckpointRecord {
    SubscriptionQuotaCheckpointRecord::from(&SubscriptionQuotaSample {
        upstream_id,
        window: SubscriptionQuotaWindow::FiveHour,
        source: SubscriptionQuotaSource::Header,
        sample_kind: SubscriptionQuotaSampleKind::Sample,
        observed_at_unix_millis: changed_at_unix_millis,
        sample_id: Uuid::from_u128(sample_id),
        utilization: Some(utilization),
        status: Some(SubscriptionQuotaStatus::Allowed),
        resets_at_unix_secs,
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
        ingested_at_unix_millis: changed_at_unix_millis,
    })
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

async fn quota_poll_fence(storage: &SqliteStorage) -> SubscriptionQuotaPollFence {
    let upstream_id = Uuid::new_v4();
    let upstream = storage
        .create(UpstreamCreate {
            id: upstream_id,
            name: format!("quota-poll-{upstream_id}"),
            kind: UpstreamKind::AnthropicOauth,
            base_url: None,
            api_key_ciphertext: None,
            oauth_tokens: Some(OAuthTokensCreate {
                tokens: EncryptedOAuthTokens::from_ciphertext(vec![1; 8]),
                never_refresh: false,
            }),
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        })
        .await
        .expect("create poll upstream");
    SubscriptionQuotaPollFence {
        upstream_id,
        oauth_token_generation: upstream.oauth_token_generation,
        epoch_key: format!("quota-poll-epoch:{upstream_id}"),
        epoch_value: None,
    }
}

fn quota_poll_sample(
    upstream_id: Uuid,
    at: u64,
    id: u128,
    utilization: f64,
) -> SubscriptionQuotaSample {
    SubscriptionQuotaSample {
        upstream_id,
        window: SubscriptionQuotaWindow::FiveHour,
        source: SubscriptionQuotaSource::Api,
        sample_kind: SubscriptionQuotaSampleKind::Sample,
        observed_at_unix_millis: at,
        sample_id: Uuid::from_u128(id),
        utilization: Some(utilization),
        status: Some(SubscriptionQuotaStatus::Allowed),
        resets_at_unix_secs: Some(18_000),
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
        ingested_at_unix_millis: at,
    }
}

async fn assert_quota_poll_records(
    storage: &SqliteStorage,
    upstream_id: Uuid,
    latest: &[SubscriptionQuotaSample],
    checkpoints: &[SubscriptionQuotaSample],
) {
    assert_eq!(
        storage
            .list_latest_subscription_quota_for_upstreams(&[upstream_id])
            .await
            .expect("read poll latest"),
        latest
    );
    assert_eq!(
        storage
            .list_subscription_quota_slim_checkpoints(SubscriptionQuotaCheckpointRangeQuery {
                upstream_ids: vec![upstream_id],
                windows: vec![SubscriptionQuotaWindow::FiveHour],
                sources: vec![
                    SubscriptionQuotaSource::Api,
                    SubscriptionQuotaSource::Header
                ],
                since_unix_millis: 0,
                until_unix_millis: 10_000,
            })
            .await
            .expect("read poll checkpoints"),
        checkpoints
            .iter()
            .map(|record| slim(SubscriptionQuotaCheckpointRecord::from(record)))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn quota_poll_rejects_stale_epoch_across_storage_handles_without_any_writes() {
    let (temp_dir, storage) = quota_storage("poll-epoch.sqlite").await;
    let database_url = format!(
        "sqlite://{}",
        temp_dir.path().join("poll-epoch.sqlite").display()
    );
    let replica =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .expect("open second poll storage handle");
    let initial_fence = quota_poll_fence(&storage).await;
    let initial = quota_poll_sample(initial_fence.upstream_id, 100, 1, 0.1);
    let unexpected_epoch = SubscriptionQuotaPollFence {
        epoch_value: Some("0".to_owned()),
        ..initial_fence.clone()
    };
    assert!(
        !storage
            .record_subscription_quota_poll(std::slice::from_ref(&initial), &unexpected_epoch)
            .await
            .expect("present epoch must not match absent epoch")
    );
    assert_quota_poll_records(&storage, initial_fence.upstream_id, &[], &[]).await;
    assert!(
        storage
            .record_subscription_quota_poll(std::slice::from_ref(&initial), &initial_fence)
            .await
            .expect("publish initial absent epoch")
    );
    assert!(
        replica
            .compare_and_put_meta_value(&initial_fence.epoch_key, None, "0")
            .await
            .expect("create epoch")
    );
    let old_fence = unexpected_epoch;
    assert!(
        replica
            .compare_and_put_meta_value(&old_fence.epoch_key, Some("0"), "1")
            .await
            .expect("advance epoch")
    );
    let current_fence = SubscriptionQuotaPollFence {
        epoch_value: Some("1".to_owned()),
        ..old_fence.clone()
    };
    let fresh = quota_poll_sample(initial_fence.upstream_id, 200, 2, 0.2);
    assert!(
        replica
            .record_subscription_quota_poll(std::slice::from_ref(&fresh), &current_fence)
            .await
            .expect("publish current epoch")
    );
    let stale = quota_poll_sample(initial_fence.upstream_id, 900, 3, 0.9);
    for fence in [&old_fence, &initial_fence] {
        assert!(
            !storage
                .record_subscription_quota_poll(std::slice::from_ref(&stale), fence)
                .await
                .expect("reject stale epoch despite its higher timestamp")
        );
    }
    assert_quota_poll_records(
        &replica,
        initial_fence.upstream_id,
        std::slice::from_ref(&fresh),
        &[initial, fresh.clone()],
    )
    .await;
}

#[tokio::test]
async fn quota_poll_rejects_changed_credentials_deleted_and_missing_upstreams() {
    let (_dir, storage) = quota_storage("poll-generation.sqlite").await;
    let old_fence = quota_poll_fence(&storage).await;
    let initial = quota_poll_sample(old_fence.upstream_id, 100, 1, 0.1);
    assert!(
        storage
            .record_subscription_quota_poll(std::slice::from_ref(&initial), &old_fence)
            .await
            .expect("publish old generation before rotation")
    );
    let rotated = storage
        .update_oauth_token(
            old_fence.upstream_id,
            EncryptedOAuthTokens::from_ciphertext(vec![2; 8]),
        )
        .await
        .expect("rotate poll credential");
    let stale = quota_poll_sample(old_fence.upstream_id, 900, 3, 0.9);
    assert!(
        !storage
            .record_subscription_quota_poll(std::slice::from_ref(&stale), &old_fence)
            .await
            .expect("reject old credential generation")
    );
    let current_fence = SubscriptionQuotaPollFence {
        oauth_token_generation: rotated.oauth_token_generation,
        ..old_fence.clone()
    };
    let fresh = quota_poll_sample(old_fence.upstream_id, 200, 2, 0.2);
    assert!(
        storage
            .record_subscription_quota_poll(std::slice::from_ref(&fresh), &current_fence)
            .await
            .expect("publish rotated credential generation")
    );
    sqlx::query("UPDATE upstream_spec_v1 SET deleted_at = unixepoch() WHERE id = ?")
        .bind(old_fence.upstream_id.to_string())
        .execute(storage.pool())
        .await
        .expect("soft-delete poll upstream");
    assert!(
        !storage
            .record_subscription_quota_poll(std::slice::from_ref(&stale), &current_fence)
            .await
            .expect("reject deleted upstream")
    );
    assert_quota_poll_records(
        &storage,
        old_fence.upstream_id,
        std::slice::from_ref(&fresh),
        &[initial, fresh.clone()],
    )
    .await;
    let missing_fence = SubscriptionQuotaPollFence {
        upstream_id: Uuid::new_v4(),
        oauth_token_generation: 0,
        ..old_fence
    };
    let missing = quota_poll_sample(missing_fence.upstream_id, 100, 4, 0.4);
    assert!(
        !storage
            .record_subscription_quota_poll(std::slice::from_ref(&missing), &missing_fence)
            .await
            .expect("missing upstream must not match generation zero")
    );
    assert_quota_poll_records(&storage, missing_fence.upstream_id, &[], &[]).await;
}

#[tokio::test]
async fn quota_poll_rejects_mixed_upstreams_and_non_api_sources_before_writing() {
    let (_dir, storage) = quota_storage("poll-invalid.sqlite").await;
    let fence = quota_poll_fence(&storage).await;
    let valid = quota_poll_sample(fence.upstream_id, 100, 1, 0.1);
    let other_upstream = quota_poll_sample(Uuid::new_v4(), 200, 2, 0.2);
    let mut header = quota_poll_sample(fence.upstream_id, 300, 3, 0.3);
    header.source = SubscriptionQuotaSource::Header;
    for invalid in [other_upstream.clone(), header] {
        assert!(matches!(
            storage
                .record_subscription_quota_poll(&[valid.clone(), invalid], &fence)
                .await,
            Err(StorageError::InvalidInput { .. })
        ));
    }
    assert_quota_poll_records(&storage, fence.upstream_id, &[], &[]).await;
    assert_quota_poll_records(&storage, other_upstream.upstream_id, &[], &[]).await;
}

#[tokio::test]
async fn quota_poll_rolls_back_latest_and_checkpoints_when_a_later_sample_is_invalid() {
    let (_dir, storage) = quota_storage("poll-rollback.sqlite").await;
    let fence = quota_poll_fence(&storage).await;
    let valid = quota_poll_sample(fence.upstream_id, 100, 1, 0.1);
    let invalid = quota_poll_sample(fence.upstream_id, u64::MAX, 2, 0.2);
    assert!(matches!(
        storage
            .record_subscription_quota_poll(&[valid.clone(), invalid], &fence)
            .await,
        Err(StorageError::InvalidInput { .. })
    ));
    assert_quota_poll_records(&storage, fence.upstream_id, &[], &[]).await;
    assert!(
        storage
            .record_subscription_quota_poll(std::slice::from_ref(&valid), &fence)
            .await
            .expect("publish after failed transaction")
    );
    assert_quota_poll_records(
        &storage,
        fence.upstream_id,
        std::slice::from_ref(&valid),
        std::slice::from_ref(&valid),
    )
    .await;
}

#[tokio::test]
async fn quota_slim_checkpoints_return_left_anchor_and_inclusive_range_with_sample_ties() {
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

    // Left anchor: the greatest-sample_id checkpoint before `since`; then every
    // checkpoint inside the inclusive `[since, until]` range in sample order.
    let expected = records[1..].iter().cloned().map(slim).collect::<Vec<_>>();
    let actual = storage
        .list_subscription_quota_slim_checkpoints(query)
        .await
        .expect("read slim quota checkpoints");

    assert_eq!(actual, expected);
    assert_eq!(actual[0].sample_id, Uuid::from_u128(2));
    assert_eq!(actual[3].sample_id, Uuid::from_u128(5));
}

#[tokio::test]
async fn quota_left_anchor_plan_avoids_correlated_anti_join() {
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
async fn quota_provider_lots_preserve_reset_cycles_without_full_records() {
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
async fn quota_usage_token_intervals_include_both_boundaries_and_use_range_indexes() {
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
    assert!(
        token_details.iter().any(|detail| {
            detail.contains("USING INDEX usage_rollups_v2_upstream_resolution_bucket_idx")
        }),
        "token intervals retain the upstream/resolution/bucket range index: {token_details:?}"
    );
}

/// 0098 drops `range_idx`/`anchor_idx`: the primary key
/// (upstream_id, window, source, changed_at_unix_millis, sample_id) must still
/// serve the DESC latest-fingerprint lookup without a scan or a sort.
#[tokio::test]
async fn quota_latest_checkpoint_lookup_uses_primary_key_after_duplicate_drop() {
    let (_temp_dir, storage) = quota_storage("latest-checkpoint-plan.sqlite").await;
    let indexes = sqlx::query_scalar::<_, String>(
        "SELECT name FROM sqlite_master \
         WHERE type = 'index' AND tbl_name = 'upstream_subscription_quota_checkpoints_v1'",
    )
    .fetch_all(storage.pool())
    .await
    .expect("list checkpoint indexes");
    for dropped in [
        "upstream_subscription_quota_checkpoints_range_idx",
        "upstream_subscription_quota_checkpoints_anchor_idx",
    ] {
        assert!(
            !indexes.iter().any(|name| name == dropped),
            "{dropped} must be dropped by 0098: {indexes:?}"
        );
    }

    let plan = sqlx::query(
        "EXPLAIN QUERY PLAN \
         SELECT semantic_fingerprint FROM upstream_subscription_quota_checkpoints_v1 \
         WHERE upstream_id = ? AND window = ? AND source = ? \
         ORDER BY changed_at_unix_millis DESC, sample_id DESC \
         LIMIT 1",
    )
    .bind(Uuid::from_u128(0x0097).to_string())
    .bind("5h")
    .bind("header")
    .fetch_all(storage.pool())
    .await
    .expect("explain latest checkpoint lookup")
    .into_iter()
    .map(|row| row.try_get::<String, _>("detail").expect("plan detail"))
    .collect::<Vec<_>>();
    assert!(
        plan.iter().any(|detail| detail.contains("USING")),
        "latest checkpoint lookup must use an index: {plan:?}"
    );
    assert!(
        !plan.iter().any(|detail| detail.contains("TEMP B-TREE")),
        "latest checkpoint lookup must not sort: {plan:?}"
    );
}

#[tokio::test]
async fn quota_checkpoint_deadband_preserves_exact_latest_and_semantic_changes() {
    let (_dir, storage) = quota_storage("quota-deadband.sqlite").await;
    let upstream_id = Uuid::from_u128(0x301);
    let sample = |at, id, utilization, status| SubscriptionQuotaSample {
        upstream_id,
        window: SubscriptionQuotaWindow::FiveHour,
        source: SubscriptionQuotaSource::Header,
        sample_kind: SubscriptionQuotaSampleKind::Sample,
        observed_at_unix_millis: at,
        sample_id: Uuid::from_u128(id),
        utilization,
        status,
        resets_at_unix_secs: Some(18_000),
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
        ingested_at_unix_millis: at,
    };
    let records = [
        sample(100, 1, Some(0.25), Some(SubscriptionQuotaStatus::Allowed)),
        sample(200, 2, Some(0.2505), Some(SubscriptionQuotaStatus::Allowed)),
        sample(300, 3, Some(0.2515), Some(SubscriptionQuotaStatus::Allowed)),
        sample(
            400,
            4,
            Some(0.2516),
            Some(SubscriptionQuotaStatus::Rejected),
        ),
        sample(500, 5, None, Some(SubscriptionQuotaStatus::Rejected)),
    ];
    for record in &records[..2] {
        storage
            .record_subscription_quota_samples(std::slice::from_ref(record))
            .await
            .expect("record sample");
    }
    let latest = storage
        .list_latest_subscription_quota_for_upstreams(&[upstream_id])
        .await
        .expect("exact latest");
    assert_eq!(latest, vec![records[1].clone()]);
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM upstream_subscription_quota_checkpoints_v1")
            .fetch_one(storage.pool())
            .await
            .expect("checkpoint count");
    assert_eq!(
        count, 1,
        "sub-deadband sample skips only historical checkpoint"
    );
    storage
        .record_subscription_quota_samples(&records[2..])
        .await
        .expect("record changes");
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM upstream_subscription_quota_checkpoints_v1")
            .fetch_one(storage.pool())
            .await
            .expect("checkpoint count");
    assert_eq!(
        count, 4,
        "threshold, status and Some-to-None changes survive"
    );
}

#[tokio::test]
async fn quota_checkpoint_pruning_is_bounded_and_retains_partition_anchors() {
    let (_dir, storage) = quota_storage("quota-prune.sqlite").await;
    let upstream_id = Uuid::from_u128(0x302);
    let mut records = vec![
        quota_checkpoint(upstream_id, 100, 1, 0.1, Some(18_000)),
        quota_checkpoint(upstream_id, 200, 2, 0.2, Some(18_000)),
        quota_checkpoint(upstream_id, 200, 3, 0.3, Some(18_000)),
        quota_checkpoint(upstream_id, 300, 4, 0.4, Some(18_000)),
    ];
    let mut other = quota_checkpoint(upstream_id, 50, 5, 0.5, Some(18_000));
    other.source = SubscriptionQuotaSource::Api;
    records.push(other);
    storage
        .put_subscription_quota_checkpoints(&records)
        .await
        .expect("seed checkpoints");
    assert_eq!(
        storage
            .prune_subscription_quota_checkpoints_before(250, 0)
            .await
            .expect("zero batch"),
        0
    );
    assert_eq!(
        storage
            .prune_subscription_quota_checkpoints_before(250, 1)
            .await
            .expect("first batch"),
        1
    );
    assert_eq!(
        storage
            .prune_subscription_quota_checkpoints_before(250, 10)
            .await
            .expect("second batch"),
        1
    );
    let ids = sqlx::query_scalar::<_, String>(
        "SELECT sample_id FROM upstream_subscription_quota_checkpoints_v1 ORDER BY sample_id",
    )
    .fetch_all(storage.pool())
    .await
    .expect("retained anchors");
    assert_eq!(
        ids,
        vec![
            Uuid::from_u128(3).to_string(),
            Uuid::from_u128(4).to_string(),
            Uuid::from_u128(5).to_string()
        ]
    );
    assert_eq!(
        storage
            .prune_subscription_quota_checkpoints_before(300, 10)
            .await
            .expect("inclusive anchor"),
        1
    );
    assert_eq!(
        storage
            .prune_subscription_quota_checkpoints_before(300, 10)
            .await
            .expect("stable anchors"),
        0
    );
}
