use std::{
    error::Error,
    time::{Duration, Instant},
};

use cc_lb_storage_api::{PromptCacheObservationRecord, PromptCacheObservationStore, TtlClass};
use cc_lb_storage_redb::RedbStorage;
use uuid::Uuid;

const UPSTREAM_COUNT: usize = 5;
const RECORDS_PER_UPSTREAM: usize = 10_000;
const EXPIRED_PER_UPSTREAM: usize = 9_000;
const ACTIVE_PER_UPSTREAM: usize = RECORDS_PER_UPSTREAM - EXPIRED_PER_UPSTREAM;
const TOTAL_RECORDS: u64 = (UPSTREAM_COUNT * RECORDS_PER_UPSTREAM) as u64;
const NOW_UNIX_SECS: u64 = 1_700_000_000;
const MAX_HYDRATE_ELAPSED: Duration = Duration::from_millis(500);

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
#[ignore = "run with --release --ignored"]
async fn hydrate_50k_rows_90percent_expired_under_500ms() -> TestResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("hydrate_50k.redb");
    let store = RedbStorage::open(&path, [30; 32])?;
    let upstream_ids = upstream_ids();

    for (upstream_index, upstream_id) in upstream_ids.iter().copied().enumerate() {
        for record_index in 0..RECORDS_PER_UPSTREAM {
            let record = observation(upstream_index, upstream_id, record_index);
            PromptCacheObservationStore::upsert_observation(&store, &record).await?;
        }
    }
    assert_eq!(
        PromptCacheObservationStore::count(&store).await?,
        TOTAL_RECORDS
    );

    let mut per_upstream_timings = Vec::with_capacity(UPSTREAM_COUNT);
    for upstream_id in upstream_ids {
        let started_at = Instant::now();
        let records = PromptCacheObservationStore::list_active_for_upstream(
            &store,
            upstream_id,
            NOW_UNIX_SECS,
        )
        .await?;
        let elapsed = started_at.elapsed();

        assert_eq!(records.len(), ACTIVE_PER_UPSTREAM, "upstream {upstream_id}");
        per_upstream_timings.push((upstream_id, elapsed));
    }

    let total_elapsed = per_upstream_timings
        .iter()
        .fold(Duration::ZERO, |sum, (_, elapsed)| sum + *elapsed);

    eprintln!("total_elapsed_ms={:.3}", duration_ms(total_elapsed));
    for (upstream_id, elapsed) in &per_upstream_timings {
        eprintln!(
            "upstream_id={upstream_id} elapsed_ms={:.3}",
            duration_ms(*elapsed)
        );
    }

    assert!(
        total_elapsed < MAX_HYDRATE_ELAPSED,
        "hydrate active rows took {total_elapsed:?}, expected < {MAX_HYDRATE_ELAPSED:?}"
    );

    Ok(())
}

fn upstream_ids() -> [Uuid; UPSTREAM_COUNT] {
    std::array::from_fn(|index| {
        Uuid::from_u128(0x3000_0000_0000_0000_0000_0000_0000_0000 + index as u128)
    })
}

fn observation(
    upstream_index: usize,
    upstream_id: Uuid,
    record_index: usize,
) -> PromptCacheObservationRecord {
    let is_expired = record_index < EXPIRED_PER_UPSTREAM;
    PromptCacheObservationRecord {
        upstream_id,
        canonical_model_id: "claude-sonnet-4-5-20250929".to_owned(),
        prefix_hash: format!("sha256:t30-redb-{upstream_index:02}-{record_index:05}"),
        ttl_class: ttl_class(record_index),
        expires_at_unix_secs: if is_expired {
            NOW_UNIX_SECS - 1
        } else {
            NOW_UNIX_SECS + 300
        },
        last_observed_at_unix_secs: NOW_UNIX_SECS.saturating_sub(60),
        hash_schema_version: 2,
    }
}

fn ttl_class(record_index: usize) -> TtlClass {
    if record_index.is_multiple_of(2) {
        TtlClass::Ephemeral5m
    } else {
        TtlClass::Ephemeral1h
    }
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}
