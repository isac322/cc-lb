use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_clock::{Clock, TestClock};
use cc_lb_domain::TtlClass;
use cc_lb_engine::{clock::unix_secs, lifecycle::HASH_SCHEMA_VERSION};
use cc_lb_storage_api::{PromptCacheObservationRecord, PromptCacheObservationStore};
use uuid::Uuid;

use crate::harness::{ConformanceBackend, with_conformance_fixture};

/// Insert one expired and one active prompt-cache observation for the same upstream.
/// Listing active observations at the injected clock timestamp returns exactly the
/// non-expired record and filters the expired row out of the store result.
pub async fn upsert_then_list_returns_active_only<B>(
    backend: Arc<B>,
    clock: Arc<TestClock>,
) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: PromptCacheObservationStore,
{
    with_conformance_fixture(backend, |storage| async move {
        let now = unix_secs(clock.now());
        let upstream_id = upstream_id(1);
        let active = observation(
            upstream_id,
            "sha256:t15-active",
            TtlClass::Ephemeral5m,
            now + 300,
            now,
        );
        let expiring = observation(
            upstream_id,
            "sha256:t15-expiring",
            TtlClass::Ephemeral5m,
            now + 60,
            now,
        );

        storage.upsert_observation(&expiring).await?;
        storage.upsert_observation(&active).await?;
        ensure!(
            storage
                .list_active_for_upstream(upstream_id, now)
                .await?
                .len()
                == 2,
            "both records should be active before the expiry boundary"
        );

        clock.advance_secs(60);
        let expiry_boundary = unix_secs(clock.now());
        ensure!(
            expiry_boundary == now + 60,
            "test clock should reach the exact expiry boundary"
        );
        let records = storage
            .list_active_for_upstream(upstream_id, expiry_boundary)
            .await?;
        ensure!(records.len() == 1, "expected exactly one active record");
        ensure!(
            records[0].v3_prefix_key == active.v3_prefix_key,
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
    clock: Arc<TestClock>,
) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: PromptCacheObservationStore,
{
    with_conformance_fixture(backend, |storage| async move {
        let now = unix_secs(clock.now());
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

        clock.advance_secs(300);
        let five_minute_boundary = unix_secs(clock.now());
        ensure!(
            five_minute_boundary == now + 300,
            "test clock should reach the exact 5m boundary"
        );
        let records = storage
            .list_active_for_upstream(upstream_id, five_minute_boundary)
            .await?;
        ensure!(
            records.len() == 1 && contains_ttl(&records, TtlClass::Ephemeral1h),
            "the 5m row should expire exactly at its boundary while the 1h row remains"
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
    clock: Arc<TestClock>,
) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: PromptCacheObservationStore,
{
    with_conformance_fixture(backend, |storage| async move {
        let now = unix_secs(clock.now());
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
            now + 100,
            now,
        );
        let expired_two = observation(
            upstream_id,
            "sha256:t15-purge-expired-2",
            TtlClass::Ephemeral1h,
            now + 200,
            now,
        );

        storage.upsert_observation(&active).await?;
        storage.upsert_observation(&expired_one).await?;
        storage.upsert_observation(&expired_two).await?;
        ensure!(storage.count().await? == 3, "initial count should be 3");

        clock.advance_secs(200);
        let exact_cutoff = unix_secs(clock.now());
        ensure!(
            exact_cutoff == now + 200,
            "test clock should reach the exact purge cutoff"
        );
        let deleted_at_boundary = storage.purge_expired_before(exact_cutoff).await?;
        ensure!(
            deleted_at_boundary == 1,
            "purge must retain a row whose expiry equals the exclusive cutoff"
        );
        ensure!(
            storage.count().await? == 2,
            "the exact-boundary row and active row should remain"
        );

        clock.advance_secs(1);
        let after_cutoff = unix_secs(clock.now());
        let deleted_after_boundary = storage.purge_expired_before(after_cutoff).await?;
        ensure!(
            deleted_at_boundary + deleted_after_boundary == 2,
            "purge should delete exactly two expired rows"
        );
        ensure!(
            storage.count().await? == 1,
            "only the active row should remain"
        );

        let records = storage
            .list_active_for_upstream(upstream_id, after_cutoff)
            .await?;
        ensure!(records.len() == 1, "active list should contain one row");
        ensure!(
            records[0].v3_prefix_key == active.v3_prefix_key,
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
    clock: Arc<TestClock>,
) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: PromptCacheObservationStore,
{
    let fixture = backend.create_fixture().await?;
    let result = async {
        let now = unix_secs(clock.now());
        let upstream_id = upstream_id(4);
        let active = observation(
            upstream_id,
            "sha256:t15-restart-active",
            TtlClass::Ephemeral5m,
            now + 1_000,
            now,
        );
        let expiring = observation(
            upstream_id,
            "sha256:t15-restart-expiring",
            TtlClass::Ephemeral1h,
            now + 50,
            now,
        );

        {
            let first_store = backend.open(&fixture).await?;
            first_store.upsert_observation(&active).await?;
            first_store.upsert_observation(&expiring).await?;
        }

        clock.advance_secs(50);
        let expiry_boundary = unix_secs(clock.now());
        ensure!(
            expiry_boundary == now + 50,
            "test clock should reach the exact restart expiry boundary"
        );
        let reopened_store = backend.open(&fixture).await?;
        let records = reopened_store
            .list_active_for_upstream(upstream_id, expiry_boundary)
            .await?;
        ensure!(
            records.len() == 1,
            "reopened store should list one active row"
        );
        ensure!(
            records[0].v3_prefix_key == active.v3_prefix_key,
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
pub async fn observation_list_is_sorted<B>(backend: Arc<B>, clock: Arc<TestClock>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: PromptCacheObservationStore,
{
    with_conformance_fixture(backend, |storage| async move {
        let now = unix_secs(clock.now());
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
            let curr = (record.v3_prefix_key.clone(), ttl_value);
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

/// Two models that share the same (upstream, prefix_hash, ttl_class) must not
/// collide: the primary key includes canonical_model_id, so a second model's
/// observation keeps its own row instead of overwriting the first. Guards the
/// SQLite regression where the key omitted canonical_model_id.
pub async fn cross_model_same_prefix_keeps_both_rows<B>(
    backend: Arc<B>,
    clock: Arc<TestClock>,
) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: PromptCacheObservationStore,
{
    with_conformance_fixture(backend, |storage| async move {
        let now = unix_secs(clock.now());
        let upstream_id = upstream_id(6);
        let prefix_hash = "sha256:t15-cross-model-shared";
        let sonnet = observation_with_model(
            upstream_id,
            "claude-sonnet-4-5-20250929",
            prefix_hash,
            TtlClass::Ephemeral5m,
            now + 300,
            now,
        );
        let opus = observation_with_model(
            upstream_id,
            "claude-opus-4-1-20250805",
            prefix_hash,
            TtlClass::Ephemeral5m,
            now + 600,
            now,
        );

        storage.upsert_observation(&sonnet).await?;
        storage.upsert_observation(&opus).await?;

        let records = storage.list_active_for_upstream(upstream_id, now).await?;
        ensure!(
            records.len() == 2,
            "same prefix under two models must persist as two rows, got {}",
            records.len()
        );
        ensure!(
            records.iter().any(
                |record| record.canonical_model_id == sonnet.canonical_model_id
                    && record.expires_at_unix_secs == sonnet.expires_at_unix_secs
            ),
            "sonnet observation must survive with its own expiry"
        );
        ensure!(
            records.iter().any(
                |record| record.canonical_model_id == opus.canonical_model_id
                    && record.expires_at_unix_secs == opus.expires_at_unix_secs
            ),
            "opus observation must survive with its own expiry"
        );
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
    observation_with_model(
        upstream_id,
        "claude-sonnet-4-5-20250929",
        prefix_hash,
        ttl_class,
        expires_at_unix_secs,
        last_observed_at_unix_secs,
    )
}

fn observation_with_model(
    upstream_id: Uuid,
    canonical_model_id: &str,
    prefix_hash: &str,
    ttl_class: TtlClass,
    expires_at_unix_secs: u64,
    last_observed_at_unix_secs: u64,
) -> PromptCacheObservationRecord {
    PromptCacheObservationRecord {
        upstream_id,
        canonical_model_id: canonical_model_id.to_owned(),
        v3_prefix_key: prefix_hash.to_owned(),
        ttl_class,
        expires_at_unix_secs,
        last_observed_at_unix_secs,
        hash_schema_version: HASH_SCHEMA_VERSION,
        prefix_content_block_index: 7,
        estimated_prefix_tokens: 12_345,
        token_estimate_source: "local_tiktoken_v1".to_owned(),
    }
}

fn contains_ttl(records: &[PromptCacheObservationRecord], ttl_class: TtlClass) -> bool {
    records.iter().any(|record| record.ttl_class == ttl_class)
}

fn upstream_id(value: u128) -> Uuid {
    Uuid::from_u128(0x1500 + value)
}
