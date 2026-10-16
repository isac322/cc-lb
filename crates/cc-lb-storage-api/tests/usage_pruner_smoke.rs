use cc_lb_clock::{Clock, ClockHandle, SystemClock, TestClock, unix_millis, unix_secs};
use cc_lb_storage_api::upstream::{UpstreamCreate, UpstreamKind, UpstreamStore};
use cc_lb_storage_api::{
    ApiKeyUsagePollOutcome, ApiUsageAttribution, AuditEntry, AuditQueryScope, AuditStore,
    MetaStore, PoolQuotaHistoryStore, PoolQuotaSnapshotRecord, RequestEvent, RequestEventStore,
    RequestEventStreamFilters, SubscriptionQuotaCheckpointRangeQuery, SubscriptionQuotaSample,
    SubscriptionQuotaSampleKind, SubscriptionQuotaSource, SubscriptionQuotaWindow,
    UpstreamApiKeyStore, UpstreamApiUsageBucketQuery, UpstreamApiUsageBucketWrite,
    UpstreamSubscriptionQuotaAggregateStore, UpstreamSubscriptionQuotaStore, UsageRollupResolution,
    UsageRollupStore,
    usage_pruner::{PruneResult, UsagePruner, utc_month_start_unix_secs},
};
use cc_lb_storage_sqlite::SqliteStorage;
use std::sync::Arc;
use uuid::Uuid;

const DAY_MS: u64 = 86_400_000;
const DAY_SECS: u64 = 86_400;

#[tokio::test]
async fn prune_old_request_events() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, storage) = new_storage().await?;
    let clock = test_clock();
    let old_ts_ms = now_unix_ms(&*clock).saturating_sub(100 * DAY_MS);
    insert_request_events(storage.as_ref(), old_ts_ms).await?;

    let pruner = UsagePruner::new(Arc::clone(&storage), 90, Arc::clone(&clock));
    let result = pruner.prune_once().await;

    assert!(result.request_events_removed >= 5);
    let remaining = stored_request_events(storage.as_ref()).await?;
    assert_eq!(remaining.len(), 0);
    Ok(())
}

#[tokio::test]
async fn retention_zero_is_no_op() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, storage) = new_storage().await?;
    let clock = test_clock();
    let old_ts_ms = now_unix_ms(&*clock).saturating_sub(100 * DAY_MS);
    insert_request_events(storage.as_ref(), old_ts_ms).await?;

    let pruner = UsagePruner::new(Arc::clone(&storage), 0, Arc::clone(&clock));
    let result = pruner.prune_once().await;

    assert_eq!(result, PruneResult::default());
    let remaining = stored_request_events(storage.as_ref()).await?;
    assert_eq!(remaining.len(), 5);
    Ok(())
}

#[tokio::test]
async fn recent_rows_preserved() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, storage) = new_storage().await?;
    let clock = test_clock();
    insert_request_events(storage.as_ref(), now_unix_ms(&*clock)).await?;

    let pruner = UsagePruner::new(Arc::clone(&storage), 90, Arc::clone(&clock));
    let result = pruner.prune_once().await;

    assert_eq!(result.request_events_removed, 0);
    let remaining = stored_request_events(storage.as_ref()).await?;
    assert_eq!(remaining.len(), 5);
    Ok(())
}

#[tokio::test]
async fn prune_old_audit_log() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, storage) = new_storage().await?;
    let clock = test_clock();
    let old_ts = now_unix_secs(&*clock).saturating_sub(100 * DAY_SECS);
    insert_audit_entries(storage.as_ref(), old_ts).await?;

    let pruner = UsagePruner::new(Arc::clone(&storage), 90, Arc::clone(&clock));
    let result = pruner.prune_once().await;

    assert!(result.audit_log_removed >= 5);
    let remaining = storage
        .query_recent_audit(AuditQueryScope::All, 0, u64::MAX, 100, false)
        .await?;
    assert_eq!(remaining.len(), 0);
    Ok(())
}

#[tokio::test]
async fn auxiliary_rollups_follow_resolution_horizons() -> Result<(), Box<dyn std::error::Error>> {
    for retention_days in [90, 7, 0] {
        let (_dir, storage) = new_storage().await?;
        let clock = test_clock();
        let now = now_unix_secs(&*clock);
        for age_days in [1, 15, 91] {
            let ts = now.saturating_sub(age_days * DAY_SECS);
            storage
                .append_request_event(&RequestEvent {
                    event_id: Some(format!("rollup-retention-{age_days}")),
                    request_id: format!("rollup-retention-{age_days}"),
                    ts,
                    ts_ms: Some(ts * 1_000),
                    status: 401,
                    duration_ms: 25,
                    input_tokens: Some(10),
                    ..Default::default()
                })
                .await?;
        }
        assert_eq!(storage.rollup_usage_once().await?.processed_events, 3);
        UsagePruner::new(Arc::clone(&storage), retention_days, Arc::clone(&clock))
            .prune_once()
            .await;
        for resolution in [UsageRollupResolution::Minute, UsageRollupResolution::Hour] {
            let expected_count = match (retention_days, resolution) {
                (0, _) => 3,
                (90, UsageRollupResolution::Hour) => 2,
                _ => 1,
            };
            let rollups = storage
                .query_usage_rollups_in_range(resolution, 0, now + 1)
                .await?;
            assert_eq!(
                rollups.len(),
                expected_count,
                "retention={retention_days}, resolution={resolution:?}"
            );
            let excluded = storage
                .query_overview_excluded_error_buckets_in_range(resolution, 0, now + 1)
                .await?;
            assert_eq!(
                excluded.len(),
                expected_count,
                "excluded retention={retention_days}, resolution={resolution:?}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn auxiliary_rollups_prune_until_all_batches_complete()
-> Result<(), Box<dyn std::error::Error>> {
    let (_dir, storage) = new_storage().await?;
    let clock = test_clock();
    let now = now_unix_secs(&*clock);
    for index in 0..1_001 {
        let ts = now.saturating_sub(15 * DAY_SECS) + index * 60;
        storage
            .append_request_event(&RequestEvent {
                event_id: Some(format!("rollup-batch-{index}")),
                request_id: format!("rollup-batch-{index}"),
                principal_id: Some(format!("principal-{index}")),
                ts,
                ts_ms: Some(ts * 1_000),
                status: 401,
                duration_ms: 25,
                ..Default::default()
            })
            .await?;
    }
    assert_eq!(storage.rollup_usage_once().await?.processed_events, 1_001);
    UsagePruner::new(Arc::clone(&storage), 90, Arc::clone(&clock))
        .prune_once()
        .await;
    assert!(
        storage
            .query_usage_rollups_in_range(UsageRollupResolution::Minute, 0, now + 1)
            .await?
            .is_empty()
    );
    assert!(
        storage
            .query_overview_excluded_error_buckets_in_range(
                UsageRollupResolution::Minute,
                0,
                now + 1,
            )
            .await?
            .is_empty()
    );
    assert_eq!(
        storage
            .query_usage_rollups_in_range(UsageRollupResolution::Hour, 0, now + 1)
            .await?
            .len(),
        1_001
    );
    Ok(())
}

#[tokio::test]
async fn auxiliary_quota_history_keeps_cutoff_anchors() -> Result<(), Box<dyn std::error::Error>> {
    for retention_days in [90, 7, 0] {
        let (_dir, storage) = new_storage().await?;
        let clock = test_clock();
        let now = now_unix_secs(&*clock);
        let upstream_id = seed_usage_buckets(storage.as_ref(), &[]).await?;
        for (index, age_days) in [95, 50, 31, 29, 1].into_iter().enumerate() {
            let ts = now.saturating_sub(age_days * DAY_SECS);
            let utilization = Some((index as f64 + 1.0) / 10.0);
            storage
                .record_subscription_quota_samples(&[SubscriptionQuotaSample {
                    upstream_id,
                    window: SubscriptionQuotaWindow::FiveHour,
                    source: SubscriptionQuotaSource::Api,
                    sample_kind: SubscriptionQuotaSampleKind::Sample,
                    observed_at_unix_millis: ts * 1_000,
                    sample_id: Uuid::new_v4(),
                    utilization,
                    status: None,
                    resets_at_unix_secs: None,
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
                    ingested_at_unix_millis: ts * 1_000,
                }])
                .await?;
            storage
                .record_pool_quota_snapshots(&[PoolQuotaSnapshotRecord {
                    snapshot_at_unix_secs: i64::try_from(ts)?,
                    window: SubscriptionQuotaWindow::FiveHour,
                    utilization,
                    weighted_utilization_sum: index as f64 + 1.0,
                    capacity_ratio_sum: 10.0,
                    eligible_upstreams: 1,
                    contributing_upstreams: 1,
                    stale_upstreams: 0,
                    missing_observation_upstreams: 0,
                    missing_metadata_upstreams: 0,
                    header_contributing_upstreams: 0,
                    api_contributing_upstreams: 1,
                    max_observed_at_unix_millis: Some(i64::try_from(ts * 1_000)?),
                    computed_at_unix_millis: i64::try_from(ts * 1_000)?,
                    policy_version: 1,
                }])
                .await?;
        }
        UsagePruner::new(Arc::clone(&storage), retention_days, Arc::clone(&clock))
            .prune_once()
            .await;
        let expected_ages: &[u64] = match retention_days {
            0 => &[95, 50, 31, 29, 1],
            7 => &[29, 1],
            _ => &[31, 29, 1],
        };
        let expected_times: Vec<u64> = expected_ages
            .iter()
            .map(|age| now - age * DAY_SECS)
            .collect();
        let checkpoints = storage
            .list_subscription_quota_slim_checkpoints(SubscriptionQuotaCheckpointRangeQuery {
                upstream_ids: vec![upstream_id],
                windows: vec![SubscriptionQuotaWindow::FiveHour],
                sources: vec![SubscriptionQuotaSource::Api],
                since_unix_millis: 0,
                until_unix_millis: now * 1_000,
            })
            .await?;
        assert_eq!(
            checkpoints
                .iter()
                .map(|row| row.changed_at_unix_millis / 1_000)
                .collect::<Vec<_>>(),
            expected_times,
            "checkpoint retention={retention_days}"
        );
        let snapshots = storage
            .list_pool_quota_snapshot_summaries_in_range(
                &[SubscriptionQuotaWindow::FiveHour],
                0,
                i64::try_from(now)?,
            )
            .await?;
        assert_eq!(
            snapshots
                .iter()
                .map(|row| u64::try_from(row.snapshot_at_unix_secs))
                .collect::<Result<Vec<_>, _>>()?,
            expected_times,
            "pool retention={retention_days}"
        );
    }
    Ok(())
}

/// 2026-10-20T13:45:00Z, day 20 of the month.
const DAY_20: u64 = 1_792_503_900;
/// 2026-10-01T00:00:00Z.
const OCT_1: u64 = 1_790_812_800;

/// POL-21: with 7-day retention on day 20, reported daily buckets from the
/// 1st survive the prune, so the month summary still starts on the 1st.
#[tokio::test]
async fn pol21_prune_keeps_the_current_month() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, storage) = new_storage().await?;
    let clock: ClockHandle = Arc::new(TestClock::new_at_secs(DAY_20));
    assert_eq!(utc_month_start_unix_secs(DAY_20), OCT_1);
    let upstream_id = seed_usage_buckets(
        storage.as_ref(),
        &[
            OCT_1 - 40 * DAY_SECS,
            OCT_1 - 3_600,
            OCT_1,
            OCT_1 + DAY_SECS,
            OCT_1 + 19 * DAY_SECS,
        ],
    )
    .await?;

    let result = UsagePruner::new(Arc::clone(&storage), 7, clock)
        .prune_once()
        .await;

    assert_eq!(result.upstream_api_usage_removed, 2);
    assert_eq!(
        bucket_starts(storage.as_ref(), upstream_id).await?,
        vec![OCT_1, OCT_1 + DAY_SECS, OCT_1 + 19 * DAY_SECS]
    );
    Ok(())
}

/// POL-18: buckets older than the retention horizon are pruned, recent ones
/// and the current month are kept, and retention 0 never prunes them.
#[tokio::test]
async fn pol18_prune_follows_retention_horizon() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, storage) = new_storage().await?;
    let clock: ClockHandle = Arc::new(TestClock::new_at_secs(DAY_20));
    let horizon = DAY_20 - 30 * DAY_SECS;
    let upstream_id = seed_usage_buckets(
        storage.as_ref(),
        &[horizon - 2 * DAY_SECS, horizon + DAY_SECS, OCT_1 + DAY_SECS],
    )
    .await?;

    let disabled = UsagePruner::new(Arc::clone(&storage), 0, Arc::clone(&clock))
        .prune_once()
        .await;
    assert_eq!(disabled, PruneResult::default());
    assert_eq!(bucket_starts(storage.as_ref(), upstream_id).await?.len(), 3);

    let result = UsagePruner::new(Arc::clone(&storage), 30, clock)
        .prune_once()
        .await;

    assert_eq!(result.upstream_api_usage_removed, 1);
    assert_eq!(
        bucket_starts(storage.as_ref(), upstream_id).await?,
        vec![horizon + DAY_SECS, OCT_1 + DAY_SECS]
    );
    Ok(())
}

async fn seed_usage_buckets(
    storage: &SqliteStorage,
    bucket_starts: &[u64],
) -> Result<Uuid, Box<dyn std::error::Error>> {
    let upstream = UpstreamStore::create(
        storage,
        UpstreamCreate {
            id: Uuid::new_v4(),
            name: "api-key-prune".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            base_url: None,
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        },
    )
    .await?;
    let buckets = bucket_starts
        .iter()
        .map(|start| UpstreamApiUsageBucketWrite {
            attribution: ApiUsageAttribution::ApiKey,
            subject_id: "apikey_01".to_owned(),
            bucket_width_secs: 86_400,
            bucket_start_unix_secs: *start,
            model: "claude-sonnet-4-5".to_owned(),
            uncached_input_tokens: 10,
            cache_creation_5m_input_tokens: 0,
            cache_creation_1h_input_tokens: 0,
            cache_read_input_tokens: 0,
            output_tokens: 5,
            list_cost_micros: 0,
        })
        .collect();
    storage
        .write_api_key_usage_poll_outcome(
            upstream.id,
            upstream.revision,
            &ApiKeyUsagePollOutcome {
                attempted_at_unix_secs: DAY_20,
                usage_report_state: None,
                access_revision_seen: 0,
                consecutive_failures: 0,
                next_poll_after_unix_secs: None,
                last_success_at_unix_secs: Some(DAY_20),
                legacy_key_hint: None,
                claim_token: None,
                buckets,
            },
        )
        .await?;
    Ok(upstream.id)
}

async fn bucket_starts(
    storage: &SqliteStorage,
    upstream_id: Uuid,
) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
    Ok(storage
        .query_upstream_api_usage_buckets(&UpstreamApiUsageBucketQuery {
            upstream_id,
            attribution: ApiUsageAttribution::ApiKey,
            subject_id: "apikey_01".to_owned(),
            bucket_width_secs: 86_400,
            start_unix_secs: 0,
            end_unix_secs: i64::MAX as u64,
        })
        .await?
        .into_iter()
        .map(|bucket| bucket.bucket_start_unix_secs)
        .collect())
}

async fn new_storage() -> Result<(tempfile::TempDir, Arc<SqliteStorage>), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let database_url = format!(
        "sqlite://{}",
        dir.path().join("usage-pruner.sqlite").display()
    );
    let storage = cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(SystemClock)).await?;
    storage.initialize().await?;
    Ok((dir, Arc::new(storage)))
}

async fn insert_request_events(
    storage: &SqliteStorage,
    ts_ms: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    for index in 0..5 {
        storage
            .append_request_event(&RequestEvent {
                event_id: Some(format!("usage-pruner-{ts_ms}-{index}")),
                ts_ms: Some(ts_ms),
                principal_id: Some("principal-1".to_owned()),
                key_id: Some(format!("key-{index}")),
                model: Some("claude-sonnet-4-5".to_owned()),
                input_tokens: Some(10),
                output_tokens: Some(20),
                cache_creation_input_tokens: Some(0),
                cache_read_input_tokens: Some(0),
                cost_usd_micros: Some(100),
                duration_ms: 25,
                status: 200,
                ..Default::default()
            })
            .await?;
    }
    Ok(())
}

async fn insert_audit_entries(
    storage: &SqliteStorage,
    ts: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    for index in 0..5 {
        storage
            .append_audit(&AuditEntry {
                ts,
                request_id: format!("request-{index}"),
                principal_id: "principal-1".to_owned(),
                route: "messages".to_owned(),
                upstream: "anthropic_direct".to_owned(),
                model: Some("claude-sonnet-4-5".to_owned()),
                status: 200,
                input_tokens: Some(10),
                output_tokens: Some(20),
                duration_ms: 25,
                agent_label: None,
                ..Default::default()
            })
            .await?;
    }
    Ok(())
}

fn test_clock() -> ClockHandle {
    Arc::new(TestClock::new_at_secs(4_100_000_000))
}

fn now_unix_ms(clock: &dyn Clock) -> u64 {
    unix_millis(clock.now()).min(u128::from(u64::MAX)) as u64
}

fn now_unix_secs(clock: &dyn Clock) -> u64 {
    unix_secs(clock.now())
}

async fn stored_request_events(
    storage: &SqliteStorage,
) -> Result<Vec<RequestEvent>, Box<dyn std::error::Error>> {
    let cursor = storage.current_request_event_cursor().await?;
    Ok(storage
        .query_request_events_between_cursors(0, cursor, 500, &RequestEventStreamFilters::default())
        .await?
        .into_iter()
        .map(|(_, event)| event)
        .collect())
}
