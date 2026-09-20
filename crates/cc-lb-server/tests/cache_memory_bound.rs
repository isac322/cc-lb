use std::sync::Arc;

use cc_lb_engine::lifecycle::PromptCacheThreadUsage;
use cc_lb_server::prompt_cache_thread_usage::PromptCacheThreadUsageTracker;
use uuid::Uuid;

#[global_allocator]
static A: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const BASE_TS: u64 = 1_700_000_000;
const MODEL: &str = "claude-sonnet-4-5-20250929";
const UPSTREAM_COUNT: usize = 10;
const THREADS_PER_UPSTREAM: usize = 10_000;
const DELTA_LIMIT_MIB: usize = 200;
const BYTES_PER_MIB: usize = 1024 * 1024;

/// The tracker caps thread-usage entries per upstream, so recording far more
/// threads than the cap must stay within a small memory bound.
#[tokio::test]
#[ignore]
async fn thread_usage_tracker_stays_bounded_under_cap() {
    let tracker = Arc::new(PromptCacheThreadUsageTracker::new(30));
    let upstreams: Vec<Uuid> = (0..UPSTREAM_COUNT)
        .map(|upstream_index| Uuid::from_u128((upstream_index + 1) as u128))
        .collect();

    let before_bytes = allocated_bytes();

    for (upstream_index, upstream_id) in upstreams.iter().copied().enumerate() {
        for per_upstream_index in 0..THREADS_PER_UPSTREAM {
            let thread_index = upstream_index * THREADS_PER_UPSTREAM + per_upstream_index;
            tracker.record_thread_usage(
                upstream_id,
                MODEL,
                &format!("thread-{thread_index:09}"),
                PromptCacheThreadUsage {
                    cache_read_input_tokens: 1_024,
                    cache_creation_input_tokens_5m: 0,
                    cache_creation_input_tokens_1h: 0,
                },
                BASE_TS + thread_index as u64,
            );
        }
    }

    let after_bytes = allocated_bytes();
    let delta_bytes = after_bytes.saturating_sub(before_bytes);
    let delta_mib = delta_bytes / BYTES_PER_MIB;

    eprintln!(
        "cache_memory_bound: before_bytes={before_bytes} after_bytes={after_bytes} delta_mib={delta_mib}"
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
