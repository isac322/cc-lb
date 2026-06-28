use std::sync::Arc;

use cc_lb_core::clock::{ClockHandle, TestClock, unix_secs};
use cc_lb_plugin_api::types::TtlClass;
use cc_lb_server::prompt_cache_observation_cache::PromptCacheObservationCache;
use uuid::Uuid;

#[global_allocator]
static A: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const BASE_TS: u64 = 1_700_000_000;
const MODEL: &str = "claude-sonnet-4-5-20250929";
const UPSTREAM_COUNT: usize = 10;
const OBSERVATIONS_PER_UPSTREAM: usize = 10_000;
const OBSERVATION_COUNT: usize = UPSTREAM_COUNT * OBSERVATIONS_PER_UPSTREAM;
const DELTA_LIMIT_MIB: usize = 200;
const BYTES_PER_MIB: usize = 1024 * 1024;

#[tokio::test]
#[ignore]
async fn prompt_cache_100k_observations_under_200mib() {
    let clock: ClockHandle = Arc::new(TestClock::new_at_secs(BASE_TS));
    let now = unix_secs(clock.now());
    let cache = PromptCacheObservationCache::new_with_debounce(clock, 30, 32, 60);
    let upstreams: Vec<Uuid> = (0..UPSTREAM_COUNT)
        .map(|upstream_index| Uuid::from_u128((upstream_index + 1) as u128))
        .collect();

    let before_bytes = allocated_bytes();

    for (upstream_index, upstream_id) in upstreams.iter().copied().enumerate() {
        for per_upstream_index in 0..OBSERVATIONS_PER_UPSTREAM {
            let observation_index = upstream_index * OBSERVATIONS_PER_UPSTREAM + per_upstream_index;
            cache.upsert_observation(
                upstream_id,
                MODEL.to_owned(),
                format!("hash-{observation_index:09}"),
                TtlClass::Ephemeral5m,
                now + 300,
                now,
            );
        }
    }

    let after_bytes = allocated_bytes();
    let delta_bytes = after_bytes.saturating_sub(before_bytes);
    let delta_mib = delta_bytes / BYTES_PER_MIB;

    eprintln!(
        "cache_memory_bound: before_bytes={before_bytes} after_bytes={after_bytes} delta_mib={delta_mib} observation_count={OBSERVATION_COUNT}"
    );

    assert!(
        delta_mib < DELTA_LIMIT_MIB,
        "jemalloc allocated delta {delta_mib} MiB ({delta_bytes} bytes) exceeds {DELTA_LIMIT_MIB} MiB"
    );
}

fn allocated_bytes() -> usize {
    jemalloc_ctl::epoch::advance().expect("advance jemalloc stats epoch");
    jemalloc_ctl::stats::allocated::read().expect("read jemalloc allocated bytes")
}
