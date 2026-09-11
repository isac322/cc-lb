#![allow(non_snake_case)]

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use cc_lb_storage_api::{StorageError, StorageResult};
use tokio_util::sync::CancellationToken;

use super::{
    UPSTREAM_AFFINITY_PURGE_BATCH_SIZE, UPSTREAM_AFFINITY_PURGE_MAX_BATCHES_PER_JOB,
    UPSTREAM_AFFINITY_PURGE_RETRY_DELAY, UpstreamAffinityPurgeJob, UpstreamAffinityPurgeJobHandler,
    UpstreamAffinityPurgeJobResult, UpstreamAffinityPurgeStore,
};

const NOW_UNIX_SECS: u64 = 1_800_000_000;
const TTL_SECS: u64 = 90 * 86_400;

#[tokio::test]
async fn t2__affinity_purge_job_in_memory() {
    let store = FakeStore::new([Ok(UPSTREAM_AFFINITY_PURGE_BATCH_SIZE as u64), Ok(7)]);
    let result = UpstreamAffinityPurgeJobHandler::new(&store)
        .handle(
            UpstreamAffinityPurgeJob::default(),
            NOW_UNIX_SECS,
            TTL_SECS,
            &CancellationToken::new(),
        )
        .await;

    assert_eq!(
        result,
        UpstreamAffinityPurgeJobResult::Done {
            rows_removed: UPSTREAM_AFFINITY_PURGE_BATCH_SIZE as u64 + 7,
            batches: 2,
            limit_reached: false,
        }
    );
    assert_eq!(
        store.calls(),
        vec![
            (NOW_UNIX_SECS, TTL_SECS, UPSTREAM_AFFINITY_PURGE_BATCH_SIZE),
            (NOW_UNIX_SECS, TTL_SECS, UPSTREAM_AFFINITY_PURGE_BATCH_SIZE),
        ]
    );
}

#[tokio::test]
async fn t2__storage_failure_retries_without_discarding_completed_batch_counts() {
    let store = FakeStore::new([
        Ok(UPSTREAM_AFFINITY_PURGE_BATCH_SIZE as u64),
        Err(StorageError::Unavailable {
            message: "database unavailable".to_owned(),
        }),
    ]);
    let result = UpstreamAffinityPurgeJobHandler::new(&store)
        .handle(
            UpstreamAffinityPurgeJob::default(),
            NOW_UNIX_SECS,
            TTL_SECS,
            &CancellationToken::new(),
        )
        .await;

    assert_eq!(
        result,
        UpstreamAffinityPurgeJobResult::Retry {
            delay: UPSTREAM_AFFINITY_PURGE_RETRY_DELAY,
            error: "unavailable: database unavailable".to_owned(),
            rows_removed: UPSTREAM_AFFINITY_PURGE_BATCH_SIZE as u64,
            batches: 1,
        }
    );
    assert_eq!(store.calls().len(), 2);
}

#[tokio::test]
async fn t2__job_stops_at_the_bounded_batch_limit() {
    let store = FakeStore::new(
        (0..UPSTREAM_AFFINITY_PURGE_MAX_BATCHES_PER_JOB)
            .map(|_| Ok(UPSTREAM_AFFINITY_PURGE_BATCH_SIZE as u64)),
    );
    let result = UpstreamAffinityPurgeJobHandler::new(&store)
        .handle(
            UpstreamAffinityPurgeJob::default(),
            NOW_UNIX_SECS,
            TTL_SECS,
            &CancellationToken::new(),
        )
        .await;

    assert_eq!(
        result,
        UpstreamAffinityPurgeJobResult::Done {
            rows_removed: (UPSTREAM_AFFINITY_PURGE_BATCH_SIZE
                * UPSTREAM_AFFINITY_PURGE_MAX_BATCHES_PER_JOB) as u64,
            batches: UPSTREAM_AFFINITY_PURGE_MAX_BATCHES_PER_JOB,
            limit_reached: true,
        }
    );
    assert_eq!(
        store.calls().len(),
        UPSTREAM_AFFINITY_PURGE_MAX_BATCHES_PER_JOB
    );
}

#[tokio::test]
async fn t2__cancelled_job_does_not_start_another_batch() {
    let store = FakeStore::new([Ok(1)]);
    let cancel = CancellationToken::new();
    cancel.cancel();

    let result = UpstreamAffinityPurgeJobHandler::new(&store)
        .handle(
            UpstreamAffinityPurgeJob::default(),
            NOW_UNIX_SECS,
            TTL_SECS,
            &cancel,
        )
        .await;

    assert_eq!(
        result,
        UpstreamAffinityPurgeJobResult::Cancelled {
            rows_removed: 0,
            batches: 0,
        }
    );
    assert!(store.calls().is_empty());
}

#[tokio::test]
async fn t2__cancellation_after_a_full_batch_prevents_the_next_batch() {
    let cancel = CancellationToken::new();
    let store = CancelAfterFirstStore {
        calls: AtomicUsize::new(0),
        cancel: cancel.clone(),
    };

    let result = UpstreamAffinityPurgeJobHandler::new(&store)
        .handle(
            UpstreamAffinityPurgeJob::default(),
            NOW_UNIX_SECS,
            TTL_SECS,
            &cancel,
        )
        .await;

    assert_eq!(
        result,
        UpstreamAffinityPurgeJobResult::Cancelled {
            rows_removed: UPSTREAM_AFFINITY_PURGE_BATCH_SIZE as u64,
            batches: 1,
        }
    );
    assert_eq!(store.calls.load(Ordering::SeqCst), 1);
}

struct CancelAfterFirstStore {
    calls: AtomicUsize,
    cancel: CancellationToken,
}

impl UpstreamAffinityPurgeStore for &CancelAfterFirstStore {
    async fn purge_expired_upstream_affinities(
        &self,
        _now_unix_secs: u64,
        _ttl_secs: u64,
        _batch_size: usize,
    ) -> StorageResult<u64> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.cancel.cancel();
        Ok(UPSTREAM_AFFINITY_PURGE_BATCH_SIZE as u64)
    }
}

struct FakeStore {
    results: Mutex<VecDeque<StorageResult<u64>>>,
    calls: Mutex<Vec<(u64, u64, usize)>>,
}

impl FakeStore {
    fn new(results: impl IntoIterator<Item = StorageResult<u64>>) -> Self {
        Self {
            results: Mutex::new(results.into_iter().collect()),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn calls(&self) -> Vec<(u64, u64, usize)> {
        self.calls.lock().expect("calls lock").clone()
    }
}

impl UpstreamAffinityPurgeStore for &FakeStore {
    async fn purge_expired_upstream_affinities(
        &self,
        now_unix_secs: u64,
        ttl_secs: u64,
        batch_size: usize,
    ) -> StorageResult<u64> {
        self.calls
            .lock()
            .expect("calls lock")
            .push((now_unix_secs, ttl_secs, batch_size));
        self.results
            .lock()
            .expect("results lock")
            .pop_front()
            .expect("fake result for every call")
    }
}
