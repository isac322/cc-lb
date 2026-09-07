#[allow(dead_code)]
#[path = "../src/response_cache.rs"]
mod response_cache;

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use response_cache::{PRINCIPAL_TOTALS_CACHE_TTL, ShortTtlSingleFlightCache};
use tokio::sync::{Barrier, watch};

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
    started: watch::Sender<usize>,
    mut release: watch::Receiver<bool>,
    result: Result<u64, BuildFailure>,
) -> Result<u64, BuildFailure> {
    builds.fetch_add(1, Ordering::SeqCst);
    started.send_modify(|count| *count += 1);
    release
        .wait_for(|released| *released)
        .await
        .expect("release sender remains alive");
    result
}

async fn panicking_build(builds: Arc<AtomicUsize>) -> Result<u64, BuildFailure> {
    builds.fetch_add(1, Ordering::SeqCst);
    panic!("simulated leader panic");
}

#[tokio::test]
async fn fifty_waiters_share_one_backend_build() {
    const WAITER_COUNT: usize = 50;

    let cache = Arc::new(TestCache::new(Duration::from_secs(1)));
    let owner = Arc::new(StorageOwner);
    let key = QueryKey::principal_totals();
    let builds = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(WAITER_COUNT + 2));
    let (started_tx, mut started_rx) = watch::channel(0usize);
    let (release_tx, release_rx) = watch::channel(false);
    let mut requests = Vec::with_capacity(WAITER_COUNT + 1);

    for _ in 0..=WAITER_COUNT {
        let cache = Arc::clone(&cache);
        let owner = Arc::clone(&owner);
        let key = key.clone();
        let builds = Arc::clone(&builds);
        let barrier = Arc::clone(&barrier);
        let started = started_tx.clone();
        let release = release_rx.clone();
        requests.push(tokio::spawn(async move {
            barrier.wait().await;
            cache
                .get_or_build(&owner, key, gated_build(builds, started, release, Ok(41)))
                .await
        }));
    }

    barrier.wait().await;
    started_rx
        .wait_for(|count| *count >= 1)
        .await
        .expect("leader starts");
    tokio::time::sleep(Duration::from_millis(10)).await;
    release_tx
        .send(true)
        .expect("leader holds release receiver");

    for request in requests {
        assert_eq!(
            *request
                .await
                .expect("request joins")
                .expect("request succeeds"),
            41
        );
    }
    assert_eq!(builds.load(Ordering::SeqCst), 1);
}

#[tokio::test]
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

#[tokio::test]
async fn successful_values_expire_and_bound_live_tail_staleness() {
    assert!(PRINCIPAL_TOTALS_CACHE_TTL >= Duration::from_secs(1));
    assert!(PRINCIPAL_TOTALS_CACHE_TTL <= Duration::from_secs(3));

    let ttl = Duration::from_millis(100);
    let cache = TestCache::new(ttl);
    let owner = Arc::new(StorageOwner);
    let key = QueryKey::principal_totals();
    let backend_value = Arc::new(AtomicU64::new(10));
    let builds = Arc::new(AtomicUsize::new(0));

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
    assert_eq!(*cached.expect("cached request succeeds"), 10);
    assert_eq!(builds.load(Ordering::SeqCst), 1);

    tokio::time::sleep(ttl + Duration::from_millis(100)).await;
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

#[tokio::test]
async fn leader_errors_wake_waiters_and_retry_immediately() {
    let cache = Arc::new(TestCache::new(Duration::from_secs(1)));
    let owner = Arc::new(StorageOwner);
    let key = QueryKey::principal_totals();
    let builds = Arc::new(AtomicUsize::new(0));
    let (started_tx, mut started_rx) = watch::channel(0usize);
    let (release_tx, release_rx) = watch::channel(false);

    let requests = tokio::spawn({
        let cache = Arc::clone(&cache);
        let owner = Arc::clone(&owner);
        let key_for_second = key.clone();
        let builds_for_first = Arc::clone(&builds);
        let builds_for_second = Arc::clone(&builds);
        let started_for_first = started_tx.clone();
        async move {
            tokio::join!(
                cache.get_or_build(
                    &owner,
                    key,
                    gated_build(
                        builds_for_first,
                        started_for_first,
                        release_rx.clone(),
                        Err(BuildFailure),
                    ),
                ),
                cache.get_or_build(
                    &owner,
                    key_for_second,
                    gated_build(builds_for_second, started_tx, release_rx, Err(BuildFailure),),
                ),
            )
        }
    });

    started_rx
        .wait_for(|count| *count >= 1)
        .await
        .expect("leader starts");
    release_tx
        .send(true)
        .expect("leader holds release receiver");
    let (first, second) = requests.await.expect("requests join");
    let first_error = first.expect_err("leader error propagates");
    let second_error = second.expect_err("waiter receives leader error");
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

#[tokio::test]
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

#[tokio::test]
async fn cancelling_a_waiter_does_not_cancel_the_leader() {
    let cache = Arc::new(TestCache::new(Duration::from_secs(1)));
    let owner = Arc::new(StorageOwner);
    let key = QueryKey::principal_totals();
    let builds = Arc::new(AtomicUsize::new(0));
    let (started_tx, mut started_rx) = watch::channel(0usize);
    let (release_tx, release_rx) = watch::channel(false);

    let leader = tokio::spawn({
        let cache = Arc::clone(&cache);
        let owner = Arc::clone(&owner);
        let key = key.clone();
        let builds = Arc::clone(&builds);
        async move {
            cache
                .get_or_build(
                    &owner,
                    key,
                    gated_build(builds, started_tx, release_rx, Ok(77)),
                )
                .await
        }
    });
    started_rx
        .wait_for(|count| *count >= 1)
        .await
        .expect("leader starts");

    let waiter = tokio::spawn({
        let cache = Arc::clone(&cache);
        let owner = Arc::clone(&owner);
        let key = key.clone();
        let builds = Arc::clone(&builds);
        async move {
            cache
                .get_or_build(&owner, key, async move {
                    builds.fetch_add(1, Ordering::SeqCst);
                    Ok(99)
                })
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    waiter.abort();
    assert!(
        waiter
            .await
            .expect_err("waiter is cancelled")
            .is_cancelled()
    );

    release_tx
        .send(true)
        .expect("leader holds release receiver");
    assert_eq!(
        *leader
            .await
            .expect("leader joins")
            .expect("leader succeeds"),
        77
    );
    assert_eq!(builds.load(Ordering::SeqCst), 1);

    let cached = cache
        .get_or_build(&owner, key, async {
            Err::<u64, BuildFailure>(BuildFailure)
        })
        .await;
    assert_eq!(*cached.expect("leader result remains cached"), 77);
}
