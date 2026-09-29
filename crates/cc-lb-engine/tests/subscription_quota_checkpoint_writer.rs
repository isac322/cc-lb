use std::sync::Arc;

use cc_lb_clock::SystemClock;
use cc_lb_engine::{
    SubscriptionQuotaSink, SubscriptionQuotaWriterConfig, start_subscription_quota_writer,
};
use cc_lb_storage_api::{
    MetaStore, Storage, SubscriptionQuotaSample, SubscriptionQuotaSampleKind,
    SubscriptionQuotaSource, SubscriptionQuotaStatus, SubscriptionQuotaWindow,
    UpstreamSubscriptionQuotaStore,
};
use cc_lb_storage_sqlite::SqliteStorage;
use sqlx::Row;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[tokio::test]
async fn checkpoint_writer_latest_freshness() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, storage) = new_storage().await?;
    let upstream = upstream_id(1);
    let first = observation(upstream, 0, 1, 0.31);
    let after_heartbeat = observation(upstream, 31_000, 2, 0.31);
    let freshest = observation(upstream, 35_000, 3, 0.31);

    write_records(
        storage.clone(),
        [first.clone(), after_heartbeat, freshest.clone()],
    )
    .await?;

    let latest = storage
        .list_latest_subscription_quota_for_upstreams(&[upstream])
        .await?;
    assert_eq!(latest, [freshest]);

    let checkpoints = latest_checkpoints(&storage, upstream).await?;
    assert_eq!(checkpoints.len(), 1);
    assert_eq!(
        checkpoints[0].semantic_fingerprint,
        *first.semantic_checkpoint_fingerprint().as_bytes()
    );
    Ok(())
}

#[tokio::test]
async fn checkpoint_writer_representative_claim_only() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, storage) = new_storage().await?;
    let upstream = upstream_id(2);
    let first = observation(upstream, 0, 1, 0.31);
    let mut changed_claim = observation(upstream, 1_000, 2, 0.31);
    changed_claim.representative_claim = Some("different evidence".to_owned());

    write_records(storage.clone(), [first.clone(), changed_claim.clone()]).await?;

    let latest = storage
        .list_latest_subscription_quota_for_upstreams(&[upstream])
        .await?;
    assert_eq!(latest, [changed_claim]);

    let checkpoints = latest_checkpoints(&storage, upstream).await?;
    assert_eq!(checkpoints.len(), 1);
    assert_eq!(
        checkpoints[0].representative_claim,
        first.representative_claim
    );
    Ok(())
}

#[tokio::test]
async fn checkpoint_writer_semantic_payload_change() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, storage) = new_storage().await?;
    let upstream = upstream_id(3);
    let first = observation(upstream, 0, 1, 0.31);
    let changed_payload = observation(upstream, 1_000, 2, 0.32);

    write_records(storage.clone(), [first, changed_payload.clone()]).await?;

    let checkpoints = latest_checkpoints(&storage, upstream).await?;
    assert_eq!(checkpoints.len(), 1);
    assert_eq!(
        checkpoints[0].changed_at_unix_millis,
        changed_payload.observed_at_unix_millis
    );
    assert_eq!(checkpoints[0].utilization, changed_payload.utilization);
    Ok(())
}

#[tokio::test]
async fn checkpoint_writer_decrease() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, storage) = new_storage().await?;
    let upstream = upstream_id(4);
    let first = observation(upstream, 0, 1, 0.31);
    let decreased = observation(upstream, 1_000, 2, 0.30);

    write_records(storage.clone(), [first, decreased.clone()]).await?;

    let checkpoints = latest_checkpoints(&storage, upstream).await?;
    assert_eq!(checkpoints.len(), 1);
    assert_eq!(
        checkpoints[0].changed_at_unix_millis,
        decreased.observed_at_unix_millis
    );
    assert_eq!(checkpoints[0].utilization, decreased.utilization);
    Ok(())
}

async fn write_records<const N: usize>(
    storage: Arc<SqliteStorage>,
    records: [SubscriptionQuotaSample; N],
) -> Result<(), Box<dyn std::error::Error>> {
    let (sink, receiver) = SubscriptionQuotaSink::with_capacity(N);
    let storage_for_writer: Arc<dyn Storage> = storage;
    let handle = start_subscription_quota_writer(
        storage_for_writer,
        receiver,
        SubscriptionQuotaWriterConfig {
            batch_max_records: N,
            flush_max_ms: 1,
        },
        CancellationToken::new(),
    );
    for record in records {
        sink.enqueue(record)?;
    }
    drop(sink);
    handle.await?;
    Ok(())
}

/// Persisted fields of the latest checkpoint per `(window, source)` key.
struct LatestCheckpoint {
    changed_at_unix_millis: u64,
    semantic_fingerprint: [u8; 32],
    representative_claim: Option<String>,
    utilization: Option<f64>,
}

/// Test-only read of the checkpoint table: storage exposes only slim
/// checkpoints, which omit the semantic fingerprint and representative claim.
async fn latest_checkpoints(
    storage: &SqliteStorage,
    upstream_id: Uuid,
) -> Result<Vec<LatestCheckpoint>, Box<dyn std::error::Error>> {
    let rows = sqlx::query(
        "SELECT checkpoint.changed_at_unix_millis, checkpoint.semantic_fingerprint, \
                checkpoint.representative_claim, checkpoint.utilization \
         FROM upstream_subscription_quota_checkpoints_v1 checkpoint \
         WHERE checkpoint.upstream_id = ? \
           AND NOT EXISTS ( \
               SELECT 1 FROM upstream_subscription_quota_checkpoints_v1 newer \
               WHERE newer.upstream_id = checkpoint.upstream_id \
                 AND newer.window = checkpoint.window \
                 AND newer.source = checkpoint.source \
                 AND (newer.changed_at_unix_millis > checkpoint.changed_at_unix_millis \
                      OR (newer.changed_at_unix_millis = checkpoint.changed_at_unix_millis \
                          AND newer.sample_id > checkpoint.sample_id)) \
           ) \
         ORDER BY checkpoint.window ASC, checkpoint.source ASC",
    )
    .bind(upstream_id.to_string())
    .fetch_all(storage.pool())
    .await?;
    rows.into_iter()
        .map(
            |row| -> Result<LatestCheckpoint, Box<dyn std::error::Error>> {
                let fingerprint: Vec<u8> = row.try_get("semantic_fingerprint")?;
                let fingerprint = <[u8; 32]>::try_from(fingerprint)
                    .map_err(|bytes| format!("fingerprint has {} bytes", bytes.len()))?;
                Ok(LatestCheckpoint {
                    changed_at_unix_millis: u64::try_from(
                        row.try_get::<i64, _>("changed_at_unix_millis")?,
                    )?,
                    semantic_fingerprint: fingerprint,
                    representative_claim: row.try_get("representative_claim")?,
                    utilization: row.try_get("utilization")?,
                })
            },
        )
        .collect()
}

async fn new_storage() -> Result<(tempfile::TempDir, Arc<SqliteStorage>), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let database_url = format!(
        "sqlite://{}",
        dir.path()
            .join("subscription-quota-checkpoint-writer.sqlite")
            .display()
    );
    let storage = cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(SystemClock)).await?;
    storage.initialize().await?;
    Ok((dir, Arc::new(storage)))
}

fn observation(
    upstream_id: Uuid,
    observed_at_unix_millis: u64,
    sample_id: u128,
    utilization: f64,
) -> SubscriptionQuotaSample {
    SubscriptionQuotaSample {
        upstream_id,
        window: SubscriptionQuotaWindow::FiveHour,
        source: SubscriptionQuotaSource::Api,
        sample_kind: SubscriptionQuotaSampleKind::Sample,
        observed_at_unix_millis,
        sample_id: Uuid::from_u128(sample_id),
        utilization: Some(utilization),
        status: Some(SubscriptionQuotaStatus::Allowed),
        resets_at_unix_secs: Some(1_800_000_000),
        surpassed_threshold: Some(0.8),
        representative_claim: Some(format!("claim-{sample_id}")),
        fallback_percentage: Some(0.5),
        fallback_available: Some(true),
        overage_in_use: Some(false),
        overage_period_monthly_utilization: Some(0.2),
        upgrade_paths: Some(vec!["max_5x".to_owned()]),
        disabled_reason: None,
        extra_usage_enabled: Some(true),
        extra_usage_monthly_limit: Some(10.0),
        extra_usage_used_credits: Some(utilization),
        ingested_at_unix_millis: observed_at_unix_millis + 1,
    }
}

fn upstream_id(value: u128) -> Uuid {
    Uuid::from_u128(0x3000 + value)
}
