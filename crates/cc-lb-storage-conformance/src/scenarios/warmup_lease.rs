use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::upstream::{UpstreamCreate, UpstreamKind, UpstreamStore};
use chrono::{TimeZone, Utc};

use crate::harness::{ConformanceBackend, with_conformance_fixture};

pub type ConformanceResult = Result<()>;

pub async fn run_all<B>(backend: Arc<B>) -> ConformanceResult
where
    B: ConformanceBackend,
{
    with_conformance_fixture(backend, |store| async move {
        warmup_lease_scenario(store.as_ref()).await
    })
    .await
}

pub async fn warmup_lease_scenario<S: UpstreamStore>(store: &S) -> ConformanceResult {
    let first_next_warmup_at = Utc
        .timestamp_opt(1_700_000_030, 0)
        .single()
        .expect("valid timestamp");
    let second_next_warmup_at = Utc
        .timestamp_opt(1_700_018_030, 0)
        .single()
        .expect("valid timestamp");

    let record = store
        .create(UpstreamCreate {
            name: "warmup-lease-conformance".to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: None,
            api_key_ciphertext: None,
            warmup_enabled: true,
            next_warmup_at: None,
            last_warmup_cycle_key: None,
            warmup_lease_holder: None,
            warmup_lease_until_unix_secs: None,
            warmup_dialect_plugin: None,
        })
        .await?;

    ensure!(
        record.warmup_enabled,
        "created upstream should opt into warmup"
    );
    ensure!(
        record.warmup_lease_holder.is_none(),
        "lease holder should start empty"
    );
    ensure!(
        record.warmup_lease_until_unix_secs.is_none(),
        "lease TTL should start empty"
    );
    ensure!(
        record.last_warmup_cycle_key.is_none(),
        "cycle key should start empty"
    );

    ensure!(
        store.claim_warmup_lease(record.id, "replica-a", 60).await?,
        "replica-a should claim a free warmup lease"
    );
    ensure!(
        !store.claim_warmup_lease(record.id, "replica-b", 60).await?,
        "replica-b should not steal a live warmup lease"
    );
    ensure!(
        store.claim_warmup_lease(record.id, "replica-a", 60).await?,
        "same warmup holder should refresh its lease"
    );

    ensure!(
        store
            .write_warmup_cycle_key(
                record.id,
                "replica-a",
                1_700_000_000,
                Some(first_next_warmup_at),
            )
            .await?,
        "lease holder should write the first warmup cycle key"
    );
    let stored = store.get_by_id(record.id).await?.expect("upstream remains");
    ensure!(
        stored.last_warmup_cycle_key == Some(1_700_000_000),
        "stored cycle key mismatch"
    );
    ensure!(
        stored.next_warmup_at == Some(first_next_warmup_at),
        "stored next_warmup_at mismatch"
    );
    ensure!(
        stored.warmup_lease_holder.is_none(),
        "successful cycle-key write should clear warmup lease holder"
    );
    ensure!(
        stored.warmup_lease_until_unix_secs.is_none(),
        "successful cycle-key write should clear warmup lease TTL"
    );

    ensure!(
        store.claim_warmup_lease(record.id, "replica-a", 60).await?,
        "replica-a should reclaim the cleared warmup lease"
    );

    ensure!(
        !store
            .write_warmup_cycle_key(
                record.id,
                "replica-a",
                1_700_000_000,
                Some(second_next_warmup_at),
            )
            .await?,
        "unchanged cycle key should not update next_warmup_at"
    );
    let stored = store.get_by_id(record.id).await?.expect("upstream remains");
    ensure!(
        stored.next_warmup_at == Some(first_next_warmup_at),
        "unchanged cycle key should preserve original next_warmup_at"
    );

    ensure!(
        !store
            .write_warmup_cycle_key(
                record.id,
                "replica-b",
                1_700_018_000,
                Some(second_next_warmup_at),
            )
            .await?,
        "wrong warmup lease holder should not write cycle key"
    );

    ensure!(
        !store.release_warmup_lease(record.id, "replica-b").await?,
        "wrong warmup lease holder should not release lease"
    );
    let stored = store.get_by_id(record.id).await?.expect("upstream remains");
    ensure!(
        stored.warmup_lease_holder.as_deref() == Some("replica-a"),
        "wrong holder release should leave lease holder intact"
    );
    ensure!(
        stored.warmup_lease_until_unix_secs.is_some(),
        "wrong holder release should leave lease TTL intact"
    );
    ensure!(
        store.release_warmup_lease(record.id, "replica-a").await?,
        "matching warmup lease holder should release lease"
    );
    let stored = store.get_by_id(record.id).await?.expect("upstream remains");
    ensure!(
        stored.warmup_lease_holder.is_none(),
        "matching holder release should clear lease holder"
    );
    ensure!(
        stored.warmup_lease_until_unix_secs.is_none(),
        "matching holder release should clear lease TTL"
    );

    let stored = store.get_by_id(record.id).await?.expect("upstream remains");
    store.soft_delete(record.id, stored.revision).await?;
    ensure!(
        !store
            .write_warmup_cycle_key(
                record.id,
                "replica-a",
                1_700_018_001,
                Some(second_next_warmup_at),
            )
            .await?,
        "soft-deleted upstream should reject warmup cycle-key writes"
    );

    Ok(())
}
