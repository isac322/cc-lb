use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_core::ClockHandle;
use cc_lb_storage_api::{PromptCacheObservationRecord, PromptCacheObservationStore, TtlClass};
use uuid::Uuid;

use crate::harness::{ConformanceBackend, with_conformance_fixture};

/// Insert one expired and one active prompt-cache observation for the same upstream.
/// Listing active observations at the injected clock timestamp returns exactly the
/// non-expired record and filters the expired row out of the store result.
pub async fn upsert_then_list_returns_active_only<B>(
    backend: Arc<B>,
    clock: ClockHandle,
) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: PromptCacheObservationStore,
{
    with_conformance_fixture(backend, |storage| async move {
        let now = clock.now_unix_secs();
        let upstream_id = upstream_id(1);
        let active = observation(
            upstream_id,
            "sha256:t15-active",
            TtlClass::Ephemeral5m,
            now + 300,
            now,
        );
        let expired = observation(
            upstream_id,
            "sha256:t15-expired",
            TtlClass::Ephemeral5m,
            now - 60,
            now - 60,
        );

        storage.upsert_observation(&expired).await?;
        storage.upsert_observation(&active).await?;

        let records = storage.list_active_for_upstream(upstream_id, now).await?;
        ensure!(records.len() == 1, "expected exactly one active record");
        ensure!(
            records[0].prefix_hash == active.prefix_hash,
            "active prefix hash should be returned"
        );
        Ok(())
    })
    .await
}

/// Assert the store-level visibility contract for asymmetric TTL inputs.
///
/// Layering note: The asymmetric promotion semantic (a 5m-marker request sees
/// both 5m and 1h entries, while a 1h-marker request sees only 1h entries) is
/// enforced at the `PromptCacheObservationCache` snapshot/cache layer, not at
/// the store contract. The store returns all active records for the upstream,
/// regardless of `ttl_class`; the cache filters the store snapshot per request.
pub async fn asymmetric_ttl_snapshot_visibility<B>(
    backend: Arc<B>,
    clock: ClockHandle,
) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: PromptCacheObservationStore,
{
    with_conformance_fixture(backend, |storage| async move {
        let now = clock.now_unix_secs();
        let upstream_id = upstream_id(2);
        let prefix_hash = "sha256:t15-asymmetric-shared";
        let record_5m = observation(
            upstream_id,
            prefix_hash,
            TtlClass::Ephemeral5m,
            now + 300,
            now,
        );
        let record_1h = observation(
            upstream_id,
            prefix_hash,
            TtlClass::Ephemeral1h,
            now + 3_600,
            now,
        );

        storage.upsert_observation(&record_5m).await?;
        storage.upsert_observation(&record_1h).await?;

        let records = storage.list_active_for_upstream(upstream_id, now).await?;
        ensure!(
            records.len() == 2,
            "store should return both active TTL-class rows"
        );
        ensure!(
            contains_ttl(&records, TtlClass::Ephemeral5m),
            "5m row should be visible at store layer"
        );
        ensure!(
            contains_ttl(&records, TtlClass::Ephemeral1h),
            "1h row should be visible at store layer"
        );
        Ok(())
    })
    .await
}

/// Purge observations that expired before the injected clock timestamp without
/// removing active observations. The returned purge count and remaining table
/// count both reflect only the expired rows that were removed.
pub async fn purge_expired_before_removes_only_expired<B>(
    backend: Arc<B>,
    clock: ClockHandle,
) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: PromptCacheObservationStore,
{
    with_conformance_fixture(backend, |storage| async move {
        let now = clock.now_unix_secs();
        let upstream_id = upstream_id(3);
        let active = observation(
            upstream_id,
            "sha256:t15-purge-active",
            TtlClass::Ephemeral5m,
            now + 600,
            now,
        );
        let expired_one = observation(
            upstream_id,
            "sha256:t15-purge-expired-1",
            TtlClass::Ephemeral5m,
            now - 100,
            now - 100,
        );
        let expired_two = observation(
            upstream_id,
            "sha256:t15-purge-expired-2",
            TtlClass::Ephemeral1h,
            now - 200,
            now - 200,
        );

        storage.upsert_observation(&active).await?;
        storage.upsert_observation(&expired_one).await?;
        storage.upsert_observation(&expired_two).await?;
        ensure!(storage.count().await? == 3, "initial count should be 3");

        let deleted = storage.purge_expired_before(now).await?;
        ensure!(deleted == 2, "purge should delete exactly two expired rows");
        ensure!(
            storage.count().await? == 1,
            "only the active row should remain"
        );

        let records = storage.list_active_for_upstream(upstream_id, now).await?;
        ensure!(records.len() == 1, "active list should contain one row");
        ensure!(
            records[0].prefix_hash == active.prefix_hash,
            "remaining active row should be R1"
        );
        Ok(())
    })
    .await
}

/// Populate a store, drop the first storage handle, reopen the same backing
/// storage, and verify hydration/listing at the injected clock timestamp returns
/// only non-expired observations. This covers persistence across restart,
/// request-time expiry filtering, and avoiding expired rows in hydrated state.
pub async fn hydrate_after_restart_filters_expired<B>(
    backend: Arc<B>,
    clock: ClockHandle,
) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: PromptCacheObservationStore,
{
    let fixture = backend.create_fixture().await?;
    let result = async {
        let now = clock.now_unix_secs();
        let upstream_id = upstream_id(4);
        let active = observation(
            upstream_id,
            "sha256:t15-restart-active",
            TtlClass::Ephemeral5m,
            now + 1_000,
            now,
        );
        let expired = observation(
            upstream_id,
            "sha256:t15-restart-expired",
            TtlClass::Ephemeral1h,
            now - 50,
            now - 50,
        );

        {
            let first_store = backend.open(&fixture).await?;
            first_store.upsert_observation(&active).await?;
            first_store.upsert_observation(&expired).await?;
        }

        let reopened_store = backend.open(&fixture).await?;
        let records = reopened_store
            .list_active_for_upstream(upstream_id, now)
            .await?;
        ensure!(
            records.len() == 1,
            "reopened store should list one active row"
        );
        ensure!(
            records[0].prefix_hash == active.prefix_hash,
            "reopened store should filter the expired row"
        );
        Ok(())
    }
    .await;
    let teardown = backend.teardown(fixture).await;
    result?;
    teardown
}

/// Verify that list_active_for_upstream returns results deterministically sorted
/// by prefix_hash, then ttl_class. This ensures consistent ordering across backends
/// for conformance and operational stability.
pub async fn observation_list_is_sorted<B>(
    backend: Arc<B>,
    clock: ClockHandle,
) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: PromptCacheObservationStore,
{
    with_conformance_fixture(backend, |storage| async move {
        let now = clock.now_unix_secs();
        let upstream_id = upstream_id(5);

        let records_to_insert = vec![
            observation(
                upstream_id,
                "sha256:prefix-c",
                TtlClass::Ephemeral1h,
                now + 3_600,
                now,
            ),
            observation(
                upstream_id,
                "sha256:prefix-a",
                TtlClass::Ephemeral5m,
                now + 300,
                now,
            ),
            observation(
                upstream_id,
                "sha256:prefix-a",
                TtlClass::Ephemeral1h,
                now + 3_600,
                now,
            ),
            observation(
                upstream_id,
                "sha256:prefix-b",
                TtlClass::Ephemeral5m,
                now + 300,
                now,
            ),
        ];

        for record in &records_to_insert {
            storage.upsert_observation(record).await?;
        }

        let records = storage.list_active_for_upstream(upstream_id, now).await?;
        ensure!(records.len() == 4, "should have 4 active records");

        let mut prev: Option<(String, i16)> = None;
        for record in &records {
            let ttl_value = match record.ttl_class {
                TtlClass::Ephemeral5m => 0i16,
                TtlClass::Ephemeral1h => 1i16,
            };
            let curr = (record.prefix_hash.clone(), ttl_value);
            if let Some(p) = &prev {
                ensure!(
                    &curr >= p,
                    "records should be sorted by (prefix_hash, ttl_class): expected {:?} >= {:?}",
                    curr,
                    p
                );
            }
            prev = Some(curr);
        }
        Ok(())
    })
    .await
}

fn observation(
    upstream_id: Uuid,
    prefix_hash: &str,
    ttl_class: TtlClass,
    expires_at_unix_secs: u64,
    last_observed_at_unix_secs: u64,
) -> PromptCacheObservationRecord {
    PromptCacheObservationRecord {
        upstream_id,
        canonical_model_id: "claude-sonnet-4-5-20250929".to_owned(),
        prefix_hash: prefix_hash.to_owned(),
        ttl_class,
        expires_at_unix_secs,
        last_observed_at_unix_secs,
        hash_schema_version: 1,
    }
}

fn contains_ttl(records: &[PromptCacheObservationRecord], ttl_class: TtlClass) -> bool {
    records.iter().any(|record| record.ttl_class == ttl_class)
}

fn upstream_id(value: u128) -> Uuid {
    Uuid::from_u128(0x1500 + value)
}
