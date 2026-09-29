use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_clock::{ClockHandle, unix_secs};
use cc_lb_domain::TtlClass;
use cc_lb_engine::lifecycle::HASH_SCHEMA_VERSION;
use cc_lb_storage_api::{PromptCacheObservationRecord, PromptCacheObservationStore};
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
        let now = unix_secs(clock.now());
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

        let records = storage
            .list_active_for_candidates(
                &[upstream_id],
                &active.canonical_model_id,
                &[active.v3_prefix_key.clone(), expired.v3_prefix_key.clone()],
                now,
            )
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
    clock: ClockHandle,
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

        let records = storage
            .list_active_for_candidates(
                &[upstream_id],
                &record_5m.canonical_model_id,
                &[prefix_hash.to_owned()],
                now,
            )
            .await?;
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

        let records = storage
            .list_active_for_candidates(
                &[upstream_id],
                &active.canonical_model_id,
                &[
                    active.v3_prefix_key.clone(),
                    expired_one.v3_prefix_key.clone(),
                    expired_two.v3_prefix_key.clone(),
                ],
                now,
            )
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
    clock: ClockHandle,
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
            .list_active_for_candidates(
                &[upstream_id],
                &active.canonical_model_id,
                &[active.v3_prefix_key.clone(), expired.v3_prefix_key.clone()],
                now,
            )
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

/// Verify that list_active_for_candidates returns results deterministically
/// sorted by (upstream_id, v3_prefix_key, ttl_class) across upstreams. This
/// ensures consistent ordering across backends for conformance and operational
/// stability.
pub async fn observation_list_is_sorted<B>(backend: Arc<B>, clock: ClockHandle) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: PromptCacheObservationStore,
{
    with_conformance_fixture(backend, |storage| async move {
        let now = unix_secs(clock.now());
        // upstream_id(5) < upstream_id(6) under Uuid byte ordering, which both
        // backends use for this column (PG uuid ordering is byte-wise; SQLite
        // stores canonical lowercase text).
        let upstream_a = upstream_id(5);
        let upstream_b = upstream_id(6);

        let records_to_insert = vec![
            observation(
                upstream_b,
                "sha256:prefix-a",
                TtlClass::Ephemeral5m,
                now + 300,
                now,
            ),
            observation(
                upstream_a,
                "sha256:prefix-c",
                TtlClass::Ephemeral1h,
                now + 3_600,
                now,
            ),
            observation(
                upstream_a,
                "sha256:prefix-a",
                TtlClass::Ephemeral5m,
                now + 300,
                now,
            ),
            observation(
                upstream_a,
                "sha256:prefix-a",
                TtlClass::Ephemeral1h,
                now + 3_600,
                now,
            ),
            observation(
                upstream_a,
                "sha256:prefix-b",
                TtlClass::Ephemeral5m,
                now + 300,
                now,
            ),
        ];

        for record in &records_to_insert {
            storage.upsert_observation(record).await?;
        }

        let keys: Vec<String> = records_to_insert
            .iter()
            .map(|record| record.v3_prefix_key.clone())
            .collect();
        let records = storage
            .list_active_for_candidates(
                &[upstream_a, upstream_b],
                &records_to_insert[0].canonical_model_id,
                &keys,
                now,
            )
            .await?;
        ensure!(records.len() == 5, "should have 5 active records");

        let expected: Vec<(Uuid, &str, i16)> = vec![
            (upstream_a, "sha256:prefix-a", 0),
            (upstream_a, "sha256:prefix-a", 1),
            (upstream_a, "sha256:prefix-b", 0),
            (upstream_a, "sha256:prefix-c", 1),
            (upstream_b, "sha256:prefix-a", 0),
        ];
        let actual: Vec<(Uuid, &str, i16)> = records
            .iter()
            .map(|record| {
                (
                    record.upstream_id,
                    record.v3_prefix_key.as_str(),
                    match record.ttl_class {
                        TtlClass::Ephemeral5m => 0,
                        TtlClass::Ephemeral1h => 1,
                    },
                )
            })
            .collect();
        ensure!(
            actual == expected,
            "records should be sorted by (upstream_id, v3_prefix_key, ttl_class): {actual:?}"
        );
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
    clock: ClockHandle,
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

        let sonnet_records = storage
            .list_active_for_candidates(
                &[upstream_id],
                &sonnet.canonical_model_id,
                &[prefix_hash.to_owned()],
                now,
            )
            .await?;
        ensure!(
            sonnet_records == vec![sonnet.clone()],
            "querying the sonnet model must return only its row, got {sonnet_records:?}"
        );
        let opus_records = storage
            .list_active_for_candidates(
                &[upstream_id],
                &opus.canonical_model_id,
                &[prefix_hash.to_owned()],
                now,
            )
            .await?;
        ensure!(
            opus_records == vec![opus.clone()],
            "querying the opus model must return only its row, got {opus_records:?}"
        );
        ensure!(
            storage.count().await? == 2,
            "same prefix under two models must persist as two rows"
        );
        Ok(())
    })
    .await
}

/// A stale write must never regress a fresher row: upserting a record with an
/// older (expires_at, last_observed_at) pair leaves the stored row untouched,
/// including its metadata. This is the #825 regression guard at the store layer.
pub async fn upsert_stale_write_does_not_regress<B>(
    backend: Arc<B>,
    clock: ClockHandle,
) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: PromptCacheObservationStore,
{
    with_conformance_fixture(backend, |storage| async move {
        let now = unix_secs(clock.now());
        let upstream_id = upstream_id(7);
        let prefix_hash = "sha256:t15-stale-guard";
        let fresh = observation(
            upstream_id,
            prefix_hash,
            TtlClass::Ephemeral5m,
            now + 300,
            now,
        );
        let mut stale = observation(
            upstream_id,
            prefix_hash,
            TtlClass::Ephemeral5m,
            now + 60,
            now - 240,
        );
        // Distinct metadata proves the stale write did not splice fields into
        // the surviving row.
        stale.estimated_prefix_tokens = 1;
        stale.token_estimate_source = "stale_source".to_owned();
        stale.prefix_content_block_index = 0;

        storage.upsert_observation(&fresh).await?;
        storage.upsert_observation(&stale).await?;

        let records = storage
            .list_active_for_candidates(
                &[upstream_id],
                &fresh.canonical_model_id,
                &[prefix_hash.to_owned()],
                now,
            )
            .await?;
        ensure!(
            records == vec![fresh],
            "stale write must not regress the fresh row, got {records:?}"
        );
        Ok(())
    })
    .await
}

/// A newer write wins atomically: upserting a record with a newer (expires_at,
/// last_observed_at) pair replaces the stored row wholesale, metadata included.
pub async fn upsert_newer_write_overwrites<B>(backend: Arc<B>, clock: ClockHandle) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: PromptCacheObservationStore,
{
    with_conformance_fixture(backend, |storage| async move {
        let now = unix_secs(clock.now());
        let upstream_id = upstream_id(8);
        let prefix_hash = "sha256:t15-newer-wins";
        let older = observation(
            upstream_id,
            prefix_hash,
            TtlClass::Ephemeral5m,
            now + 60,
            now - 240,
        );
        let mut newer = observation(
            upstream_id,
            prefix_hash,
            TtlClass::Ephemeral5m,
            now + 300,
            now,
        );
        newer.estimated_prefix_tokens = 9_999;
        newer.token_estimate_source = "newer_source".to_owned();

        storage.upsert_observation(&older).await?;
        storage.upsert_observation(&newer).await?;

        let records = storage
            .list_active_for_candidates(
                &[upstream_id],
                &newer.canonical_model_id,
                &[prefix_hash.to_owned()],
                now,
            )
            .await?;
        ensure!(
            records == vec![newer],
            "newer write must replace the row wholesale, got {records:?}"
        );
        Ok(())
    })
    .await
}

/// Tie-break ordering: equal expires_at is decided by last_observed_at, and a
/// full tie keeps the already-stored row whole — the winner's metadata is never
/// spliced with the loser's fields.
pub async fn upsert_tiebreak_keeps_coherent_winner<B>(
    backend: Arc<B>,
    clock: ClockHandle,
) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: PromptCacheObservationStore,
{
    with_conformance_fixture(backend, |storage| async move {
        let now = unix_secs(clock.now());
        let upstream_id = upstream_id(9);
        let prefix_hash = "sha256:t15-tiebreak";

        let older_observed = observation(
            upstream_id,
            prefix_hash,
            TtlClass::Ephemeral5m,
            now + 300,
            now,
        );
        // Same expires_at, newer last_observed_at: wins the tie-break.
        let mut winner = observation(
            upstream_id,
            prefix_hash,
            TtlClass::Ephemeral5m,
            now + 300,
            now + 30,
        );
        winner.estimated_prefix_tokens = 2_000;
        winner.token_estimate_source = "winner_source".to_owned();
        // A full tie must retain the already-stored record, including metadata.
        let mut tied = winner.clone();
        tied.estimated_prefix_tokens = 3_000;
        tied.token_estimate_source = "tied_source".to_owned();

        storage.upsert_observation(&older_observed).await?;
        storage.upsert_observation(&winner).await?;
        storage.upsert_observation(&tied).await?;

        let records = storage
            .list_active_for_candidates(
                &[upstream_id],
                &winner.canonical_model_id,
                &[prefix_hash.to_owned()],
                now,
            )
            .await?;
        ensure!(records.len() == 1, "expected exactly one stored row");
        ensure!(
            records.as_slice() == std::slice::from_ref(&winner),
            "full-tie write must keep the already-stored record whole, got {records:?}"
        );
        // Check the tie before replaying: a last-writer-wins bug would otherwise
        // be hidden by restoring the winner with this final write.
        storage.upsert_observation(&winner).await?;
        let replayed = storage
            .list_active_for_candidates(
                &[upstream_id],
                &winner.canonical_model_id,
                &[prefix_hash.to_owned()],
                now,
            )
            .await?;
        ensure!(
            replayed == records,
            "identical replay must preserve the row"
        );
        Ok(())
    })
    .await
}

/// Batch lookup returns exactly the requested (upstream, key) candidates for the
/// requested model: other upstreams, unrequested keys, other models, and expired
/// rows are all excluded. Empty input sets return empty without error.
pub async fn list_active_for_candidates_isolates_candidates<B>(
    backend: Arc<B>,
    clock: ClockHandle,
) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: PromptCacheObservationStore,
{
    with_conformance_fixture(backend, |storage| async move {
        let now = unix_secs(clock.now());
        let upstream_a = upstream_id(10);
        let upstream_b = upstream_id(11);
        let model = "claude-sonnet-4-5-20250929";

        let wanted_a = observation_with_model(
            upstream_a,
            model,
            "sha256:iso-a1",
            TtlClass::Ephemeral5m,
            now + 300,
            now,
        );
        let wanted_b = observation_with_model(
            upstream_b,
            model,
            "sha256:iso-b1",
            TtlClass::Ephemeral1h,
            now + 3_600,
            now,
        );
        let unrequested_key = observation_with_model(
            upstream_a,
            model,
            "sha256:iso-other-key",
            TtlClass::Ephemeral5m,
            now + 300,
            now,
        );
        let other_model = observation_with_model(
            upstream_a,
            "claude-opus-4-1-20250805",
            "sha256:iso-a1",
            TtlClass::Ephemeral5m,
            now + 300,
            now,
        );
        let expired = observation_with_model(
            upstream_b,
            model,
            "sha256:iso-b1",
            TtlClass::Ephemeral5m,
            now - 10,
            now - 10,
        );

        for record in [
            &wanted_a,
            &wanted_b,
            &unrequested_key,
            &other_model,
            &expired,
        ] {
            storage.upsert_observation(record).await?;
        }

        let records = storage
            .list_active_for_candidates(
                &[upstream_a, upstream_b],
                model,
                &["sha256:iso-a1".to_owned(), "sha256:iso-b1".to_owned()],
                now,
            )
            .await?;
        ensure!(
            records == vec![wanted_a, wanted_b],
            "batch lookup must return exactly the requested active candidates, got {records:?}"
        );

        // Empty input sets short-circuit to empty without issuing a query.
        let empty = storage
            .list_active_for_candidates(&[], model, &["sha256:iso-a1".to_owned()], now)
            .await?;
        ensure!(empty.is_empty(), "empty upstream set must return empty");
        let empty = storage
            .list_active_for_candidates(&[upstream_a], model, &[], now)
            .await?;
        ensure!(empty.is_empty(), "empty key set must return empty");
        Ok(())
    })
    .await
}

/// Two independent storage handles racing writes to the same key must always
/// converge on the freshest record, regardless of commit order. Covers the
/// multi-replica interleave where a delayed writer would otherwise clobber a
/// newer observation.
pub async fn concurrent_upserts_keep_freshest<B>(backend: Arc<B>, clock: ClockHandle) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: PromptCacheObservationStore,
{
    let fixture = backend.create_fixture().await?;
    let result = async {
        let now = unix_secs(clock.now());
        let upstream_id = upstream_id(12);
        let writer_a = Arc::new(backend.open(&fixture).await?);
        let writer_b = Arc::new(backend.open(&fixture).await?);

        const ROUNDS: usize = 8;
        let mut keys = Vec::with_capacity(ROUNDS);
        for round in 0..ROUNDS {
            let key = format!("sha256:t15-race-{round}");
            keys.push(key.clone());
            let fresh = observation(upstream_id, &key, TtlClass::Ephemeral5m, now + 300, now);
            let stale = observation(
                upstream_id,
                &key,
                TtlClass::Ephemeral5m,
                now + 60,
                now - 240,
            );
            let (a, b) = (Arc::clone(&writer_a), Arc::clone(&writer_b));
            let (first, second) = if round % 2 == 0 {
                ((a, stale), (b, fresh))
            } else {
                ((a, fresh), (b, stale))
            };
            let (first_handle, first_record) = first;
            let (second_handle, second_record) = second;
            let (first_result, second_result) = futures::join!(
                async move { first_handle.upsert_observation(&first_record).await },
                async move { second_handle.upsert_observation(&second_record).await },
            );
            first_result?;
            second_result?;
        }

        let records = writer_a
            .list_active_for_candidates(&[upstream_id], "claude-sonnet-4-5-20250929", &keys, now)
            .await?;
        ensure!(
            records.len() == ROUNDS,
            "expected one row per raced key, got {}",
            records.len()
        );
        for record in &records {
            ensure!(
                record.expires_at_unix_secs == now + 300
                    && record.last_observed_at_unix_secs == now,
                "raced key {} must keep the freshest record, got expires_at={} last_observed_at={}",
                record.v3_prefix_key,
                record.expires_at_unix_secs,
                record.last_observed_at_unix_secs
            );
        }
        Ok(())
    }
    .await;
    let teardown = backend.teardown(fixture).await;
    result?;
    teardown
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
