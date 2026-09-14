use std::sync::Arc;

use cc_lb_engine::{
    SubscriptionQuotaSink, SubscriptionQuotaWriterConfig, start_subscription_quota_writer,
};
use cc_lb_storage_api::{
    Storage, SubscriptionQuotaSample, SubscriptionQuotaSampleKind, SubscriptionQuotaSource,
    SubscriptionQuotaStatus, SubscriptionQuotaWindow, UpstreamSubscriptionQuotaStore,
};
use cc_lb_testkit::InMemoryStorage;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[tokio::test]
async fn t2__checkpoint_writer_latest_freshness() -> Result<(), Box<dyn std::error::Error>> {
    let storage = InMemoryStorage::new();
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

    let checkpoints = storage
        .list_latest_subscription_quota_checkpoints_for_upstreams(&[upstream])
        .await?;
    assert_eq!(checkpoints.len(), 1);
    assert_eq!(
        checkpoints[0].changed_at_unix_millis,
        first.observed_at_unix_millis
    );
    assert_eq!(
        checkpoints[0].semantic_fingerprint,
        first.semantic_checkpoint_fingerprint()
    );
    Ok(())
}

#[tokio::test]
async fn t2__checkpoint_writer_representative_claim_only() -> Result<(), Box<dyn std::error::Error>>
{
    let storage = InMemoryStorage::new();
    let upstream = upstream_id(2);
    let first = observation(upstream, 0, 1, 0.31);
    let mut changed_claim = observation(upstream, 1_000, 2, 0.31);
    changed_claim.representative_claim = Some("different evidence".to_owned());

    write_records(storage.clone(), [first.clone(), changed_claim.clone()]).await?;

    let latest = storage
        .list_latest_subscription_quota_for_upstreams(&[upstream])
        .await?;
    assert_eq!(latest, [changed_claim]);

    let checkpoints = storage
        .list_latest_subscription_quota_checkpoints_for_upstreams(&[upstream])
        .await?;
    assert_eq!(checkpoints.len(), 1);
    assert_eq!(
        checkpoints[0].representative_claim,
        first.representative_claim
    );
    Ok(())
}

#[tokio::test]
async fn t2__checkpoint_writer_semantic_payload_change() -> Result<(), Box<dyn std::error::Error>> {
    let storage = InMemoryStorage::new();
    let upstream = upstream_id(3);
    let first = observation(upstream, 0, 1, 0.31);
    let changed_payload = observation(upstream, 1_000, 2, 0.32);

    write_records(storage.clone(), [first, changed_payload.clone()]).await?;

    let checkpoints = storage
        .list_latest_subscription_quota_checkpoints_for_upstreams(&[upstream])
        .await?;
    assert_eq!(checkpoints.len(), 1);
    assert_eq!(
        checkpoints[0].changed_at_unix_millis,
        changed_payload.observed_at_unix_millis
    );
    assert_eq!(checkpoints[0].utilization, changed_payload.utilization);
    Ok(())
}

#[tokio::test]
async fn t2__checkpoint_writer_decrease() -> Result<(), Box<dyn std::error::Error>> {
    let storage = InMemoryStorage::new();
    let upstream = upstream_id(4);
    let first = observation(upstream, 0, 1, 0.31);
    let decreased = observation(upstream, 1_000, 2, 0.30);

    write_records(storage.clone(), [first, decreased.clone()]).await?;

    let checkpoints = storage
        .list_latest_subscription_quota_checkpoints_for_upstreams(&[upstream])
        .await?;
    assert_eq!(checkpoints.len(), 1);
    assert_eq!(
        checkpoints[0].changed_at_unix_millis,
        decreased.observed_at_unix_millis
    );
    assert_eq!(checkpoints[0].utilization, decreased.utilization);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn t2__subscription_quota_writer_batches_and_flushes_on_deadline()
-> Result<(), Box<dyn std::error::Error>> {
    let storage = InMemoryStorage::new();
    let upstreams = (1_u128..=10).map(upstream_id).collect::<Vec<_>>();
    let (sink, receiver) = SubscriptionQuotaSink::with_capacity(16);
    let cancel = CancellationToken::new();
    let storage_for_writer: Arc<dyn Storage> = storage.clone();
    let handle = start_subscription_quota_writer(
        storage_for_writer,
        receiver,
        SubscriptionQuotaWriterConfig {
            batch_max_records: 256,
            flush_max_ms: 100,
        },
        cancel.clone(),
    );
    for (index, upstream) in upstreams.iter().copied().enumerate() {
        sink.enqueue(observation(
            upstream,
            index as u64,
            100 + index as u128,
            0.1 + index as f64 / 100.0,
        ))?;
    }
    tokio::task::yield_now().await;

    tokio::time::advance(std::time::Duration::from_millis(99)).await;
    tokio::task::yield_now().await;
    assert!(
        storage
            .list_latest_subscription_quota_for_upstreams(&upstreams)
            .await?
            .is_empty(),
        "partial batch must wait for its configured deadline"
    );

    tokio::time::advance(std::time::Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    let persisted = storage
        .list_latest_subscription_quota_for_upstreams(&upstreams)
        .await?;
    assert_eq!(persisted.len(), 10);
    assert_eq!(
        persisted
            .iter()
            .map(|sample| sample.upstream_id)
            .collect::<std::collections::BTreeSet<_>>(),
        upstreams.iter().copied().collect()
    );
    let batches = storage.subscription_quota_sample_batches()?;
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].len(), 10);

    cancel.cancel();
    handle.await?;
    Ok(())
}

async fn write_records<const N: usize>(
    storage: Arc<InMemoryStorage>,
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
