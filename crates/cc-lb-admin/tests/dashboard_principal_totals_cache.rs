#[allow(dead_code)]
mod response_cache {
    include!("../src/response_cache.rs");

    pub(super) fn expire_ready_entries<K, V, E, O: ?Sized>(
        cache: &ShortTtlSingleFlightCache<K, V, E, O>,
        ttl: Duration,
    ) -> usize {
        let mut state = lock_cache_state(&cache.inner.state);
        let mut expired = 0;
        for entry in state.entries.values_mut() {
            if let CacheEntry::Ready { expires_at, .. } = entry {
                *expires_at = expires_at
                    .checked_sub(ttl)
                    .expect("ready expiry was initialized from now plus the TTL");
                expired += 1;
            }
        }
        expired
    }
}

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use cc_lb_testkit::ManualGate;
use response_cache::{PRINCIPAL_TOTALS_CACHE_TTL, ShortTtlSingleFlightCache, expire_ready_entries};
use tokio::{sync::Barrier, task::JoinSet};

#[derive(Debug)]
struct StorageOwner;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct QueryKey {
    range: &'static str,
    step: &'static str,
    group_by: &'static str,
    upstream: Option<u128>,
    projection: &'static str,
    window_start: u64,
    window_end: u64,
}

impl QueryKey {
    fn principal_totals() -> Self {
        Self {
            range: "1h",
            step: "minute",
            group_by: "principal",
            upstream: None,
            projection: "totals",
            window_start: 3_600,
            window_end: 7_200,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct BuildFailure;

type TestCache = ShortTtlSingleFlightCache<QueryKey, u64, BuildFailure, StorageOwner>;

async fn gated_build(
    builds: Arc<AtomicUsize>,
    release: ManualGate,
    result: Result<u64, BuildFailure>,
) -> Result<u64, BuildFailure> {
    builds.fetch_add(1, Ordering::SeqCst);
    release.wait().await;
    result
}

async fn wait_for_count(counter: &AtomicUsize, expected: usize, state: &str) {
    const MAX_YIELDS: usize = 1_024;

    for _ in 0..MAX_YIELDS {
        let actual = counter.load(Ordering::SeqCst);
        assert!(
            actual <= expected,
            "{state}: expected {expected}, got {actual}"
        );
        if actual == expected {
            return;
        }
        tokio::task::yield_now().await;
    }

    assert_eq!(
        counter.load(Ordering::SeqCst),
        expected,
        "{state} did not settle after {MAX_YIELDS} scheduler yields"
    );
}

async fn panicking_build(builds: Arc<AtomicUsize>) -> Result<u64, BuildFailure> {
    builds.fetch_add(1, Ordering::SeqCst);
    panic!("simulated leader panic");
}

#[tokio::test(start_paused = true)]
async fn fifty_waiters_share_one_backend_build() {
    const WAITER_COUNT: usize = 50;
    const REQUEST_COUNT: usize = WAITER_COUNT + 1;

    let cache = Arc::new(TestCache::new(Duration::from_secs(1)));
    let owner = Arc::new(StorageOwner);
    let key = QueryKey::principal_totals();
    let entered = Arc::new(AtomicUsize::new(0));
    let builds = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(REQUEST_COUNT + 1));
    let release = ManualGate::new();
    let mut requests = JoinSet::new();

    for _ in 0..REQUEST_COUNT {
        let cache = Arc::clone(&cache);
        let owner = Arc::clone(&owner);
        let key = key.clone();
        let entered = Arc::clone(&entered);
        let builds = Arc::clone(&builds);
        let barrier = Arc::clone(&barrier);
        let release = release.clone();
        requests.spawn(async move {
            barrier.wait().await;
            entered.fetch_add(1, Ordering::SeqCst);
            cache
                .get_or_build(&owner, key, gated_build(builds, release, Ok(41)))
                .await
        });
    }

    barrier.wait().await;
    wait_for_count(&entered, REQUEST_COUNT, "requests entered cache").await;
    wait_for_count(&builds, 1, "single backend build started").await;
    assert_eq!(builds.load(Ordering::SeqCst), 1);
    release.open();

    let mut completed = 0;
    while let Some(request) = requests.join_next().await {
        assert_eq!(
            *request.expect("request joins").expect("request succeeds"),
            41
        );
        completed += 1;
    }
    assert_eq!(completed, REQUEST_COUNT);
    assert_eq!(builds.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn different_queries_and_storage_owners_never_share() {
    let cache = TestCache::new(Duration::from_secs(1));
    let first_owner = Arc::new(StorageOwner);
    let second_owner = Arc::new(StorageOwner);
    let first_key = QueryKey::principal_totals();
    let mut second_key = first_key.clone();
    second_key.upstream = Some(7);
    let builds = Arc::new(AtomicUsize::new(0));

    let first = cache.get_or_build(&first_owner, first_key.clone(), {
        let builds = Arc::clone(&builds);
        async move {
            builds.fetch_add(1, Ordering::SeqCst);
            Ok(1)
        }
    });
    let second = cache.get_or_build(&first_owner, second_key, {
        let builds = Arc::clone(&builds);
        async move {
            builds.fetch_add(1, Ordering::SeqCst);
            Ok(2)
        }
    });
    let (first, second) = tokio::join!(first, second);
    assert_eq!(*first.expect("first query succeeds"), 1);
    assert_eq!(*second.expect("second query succeeds"), 2);

    let other_storage = cache
        .get_or_build(&second_owner, first_key, {
            let builds = Arc::clone(&builds);
            async move {
                builds.fetch_add(1, Ordering::SeqCst);
                Ok(3)
            }
        })
        .await;
    assert_eq!(*other_storage.expect("other storage succeeds"), 3);
    assert_eq!(builds.load(Ordering::SeqCst), 3);
}

#[tokio::test(start_paused = true)]
async fn successful_values_expire_and_bound_live_tail_staleness() {
    assert!(PRINCIPAL_TOTALS_CACHE_TTL >= Duration::from_secs(1));
    assert!(PRINCIPAL_TOTALS_CACHE_TTL <= Duration::from_secs(3));

    let owner = Arc::new(StorageOwner);
    let key = QueryKey::principal_totals();
    let backend_value = Arc::new(AtomicU64::new(10));

    let builds = Arc::new(AtomicUsize::new(0));
    let ttl = Duration::from_millis(100);
    let cache = TestCache::new(ttl);
    let first = cache
        .get_or_build(&owner, key.clone(), {
            let backend_value = Arc::clone(&backend_value);
            let builds = Arc::clone(&builds);
            async move {
                builds.fetch_add(1, Ordering::SeqCst);
                Ok(backend_value.load(Ordering::SeqCst))
            }
        })
        .await;
    assert_eq!(*first.expect("initial build succeeds"), 10);

    backend_value.store(20, Ordering::SeqCst);
    let cached = cache
        .get_or_build(&owner, key.clone(), {
            let backend_value = Arc::clone(&backend_value);
            let builds = Arc::clone(&builds);
            async move {
                builds.fetch_add(1, Ordering::SeqCst);
                Ok(backend_value.load(Ordering::SeqCst))
            }
        })
        .await;
    assert_eq!(*cached.expect("unexpired value remains cached"), 10);
    assert_eq!(builds.load(Ordering::SeqCst), 1);

    assert_eq!(expire_ready_entries(&cache, ttl), 1);
    let refreshed = cache
        .get_or_build(&owner, key, {
            let backend_value = Arc::clone(&backend_value);
            let builds = Arc::clone(&builds);
            async move {
                builds.fetch_add(1, Ordering::SeqCst);
                Ok(backend_value.load(Ordering::SeqCst))
            }
        })
        .await;
    assert_eq!(*refreshed.expect("expired value refreshes"), 20);
    assert_eq!(builds.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn leader_errors_wake_waiters_and_retry_immediately() {
    const REQUEST_COUNT: usize = 2;

    let cache = Arc::new(TestCache::new(Duration::from_secs(1)));
    let owner = Arc::new(StorageOwner);
    let key = QueryKey::principal_totals();
    let entered = Arc::new(AtomicUsize::new(0));
    let builds = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(REQUEST_COUNT + 1));
    let release = ManualGate::new();
    let mut requests = JoinSet::new();

    for _ in 0..REQUEST_COUNT {
        let cache = Arc::clone(&cache);
        let owner = Arc::clone(&owner);
        let key = key.clone();
        let entered = Arc::clone(&entered);
        let builds = Arc::clone(&builds);
        let barrier = Arc::clone(&barrier);
        let release = release.clone();
        requests.spawn(async move {
            barrier.wait().await;
            entered.fetch_add(1, Ordering::SeqCst);
            cache
                .get_or_build(&owner, key, gated_build(builds, release, Err(BuildFailure)))
                .await
        });
    }

    barrier.wait().await;
    wait_for_count(&entered, REQUEST_COUNT, "error requests entered cache").await;
    wait_for_count(&builds, 1, "failing backend build started").await;
    assert_eq!(builds.load(Ordering::SeqCst), 1);
    release.open();

    let first_error = requests
        .join_next()
        .await
        .expect("first request exists")
        .expect("first request joins")
        .expect_err("first request receives leader error");
    let second_error = requests
        .join_next()
        .await
        .expect("second request exists")
        .expect("second request joins")
        .expect_err("second request receives leader error");
    assert!(requests.is_empty());
    assert!(Arc::ptr_eq(&first_error, &second_error));
    assert_eq!(builds.load(Ordering::SeqCst), 1);

    let retry = cache
        .get_or_build(&owner, QueryKey::principal_totals(), {
            let builds = Arc::clone(&builds);
            async move {
                builds.fetch_add(1, Ordering::SeqCst);
                Ok(55)
            }
        })
        .await;
    assert_eq!(*retry.expect("error is not cached"), 55);
    assert_eq!(builds.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn panicked_leader_does_not_poison_the_cache_key() {
    let cache = Arc::new(TestCache::new(Duration::from_secs(1)));
    let owner = Arc::new(StorageOwner);
    let key = QueryKey::principal_totals();
    let builds = Arc::new(AtomicUsize::new(0));

    let failed_request = tokio::spawn({
        let cache = Arc::clone(&cache);
        let owner = Arc::clone(&owner);
        let key = key.clone();
        let builds = Arc::clone(&builds);
        async move {
            cache
                .get_or_build(&owner, key, panicking_build(builds))
                .await
        }
    });
    assert!(
        failed_request
            .await
            .expect_err("panicked leader fails its request")
            .is_panic()
    );

    let recovered = cache
        .get_or_build(&owner, key, {
            let builds = Arc::clone(&builds);
            async move {
                builds.fetch_add(1, Ordering::SeqCst);
                Ok(88)
            }
        })
        .await;
    assert_eq!(*recovered.expect("next request rebuilds"), 88);
    assert_eq!(builds.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn cancelling_a_waiter_does_not_cancel_the_leader() {
    let cache = Arc::new(TestCache::new(Duration::from_secs(1)));
    let owner = Arc::new(StorageOwner);
    let key = QueryKey::principal_totals();
    let waiter_entered = Arc::new(AtomicUsize::new(0));
    let builds = Arc::new(AtomicUsize::new(0));
    let release = ManualGate::new();
    let mut requests = JoinSet::new();

    requests.spawn({
        let cache = Arc::clone(&cache);
        let owner = Arc::clone(&owner);
        let key = key.clone();
        let builds = Arc::clone(&builds);
        let release = release.clone();
        async move {
            cache
                .get_or_build(&owner, key, gated_build(builds, release, Ok(77)))
                .await
        }
    });
    wait_for_count(&builds, 1, "leader backend build started").await;

    let waiter_abort = requests.spawn({
        let cache = Arc::clone(&cache);
        let owner = Arc::clone(&owner);
        let key = key.clone();
        let waiter_entered = Arc::clone(&waiter_entered);
        let builds = Arc::clone(&builds);
        async move {
            waiter_entered.fetch_add(1, Ordering::SeqCst);
            cache
                .get_or_build(&owner, key, async move {
                    builds.fetch_add(1, Ordering::SeqCst);
                    Ok(99)
                })
                .await
        }
    });
    wait_for_count(&waiter_entered, 1, "waiter entered cache").await;
    assert_eq!(builds.load(Ordering::SeqCst), 1);

    waiter_abort.abort();
    assert!(
        requests
            .join_next()
            .await
            .expect("cancelled waiter exists")
            .expect_err("waiter is cancelled")
            .is_cancelled()
    );
    assert_eq!(builds.load(Ordering::SeqCst), 1);

    release.open();
    assert_eq!(
        *requests
            .join_next()
            .await
            .expect("leader exists")
            .expect("leader joins")
            .expect("leader succeeds"),
        77
    );
    assert!(requests.is_empty());
    assert_eq!(builds.load(Ordering::SeqCst), 1);

    let cached = cache
        .get_or_build(&owner, key, async {
            Err::<u64, BuildFailure>(BuildFailure)
        })
        .await;
    assert_eq!(*cached.expect("leader result remains cached"), 77);
}
