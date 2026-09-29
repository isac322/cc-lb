use std::future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use cc_lb_clock::{Clock, ClockHandle, TestClock, unix_millis};
use cc_lb_storage_api::{ApiKeyUsageCompactionRun, StorageResult, usage_pruner::PruneResult};

use super::{UsagePruneJob, UsagePruneJobResult, UsagePruneRunner, handle_usage_prune_job};

const DAY_SECS: u64 = 86_400;
const DAY_MS: u64 = DAY_SECS * 1_000;
const RETENTION_DAYS: u64 = 90;
const NOW_UNIX_SECS: u64 = 1_800_000_000;
const WRITER_INACTIVE_AFTER_SECS: u64 = 60;
const API_KEY_USAGE_RETAIN_FOR_SECS: u64 = 7 * DAY_SECS + 3_600;
const COMPACTION_BATCH_SIZE: usize = 1_000;

#[tokio::test]
async fn prune_execution_calls_pruner_and_api_key_compactor_once() {
    let clock = test_clock();
    let runner = RecordingUsagePruneRunner::default();
    runner.seed_usage_rows(expired_ms(&*clock, 100), 5);
    runner.set_compaction_result(7, 3);

    let result = run_job(&runner, RETENTION_DAYS, clock).await;

    assert_eq!(runner.prune_calls.load(Ordering::SeqCst), 1);
    assert_eq!(runner.compaction_calls.load(Ordering::SeqCst), 1);
    assert_eq!(runner.compaction_args(), expected_compaction_args());
    assert_eq!(result, done_result(5, 5, 7, 3));
    assert_eq!(runner.request_event_count(), 0);
    assert_eq!(runner.audit_entry_count(), 0);
}

#[tokio::test]
async fn retention_cutoff_preserves_rows_inside_retention_window() {
    let clock = test_clock();
    let runner = RecordingUsagePruneRunner::default();
    runner.seed_usage_rows(expired_ms(&*clock, 100), 3);
    runner.seed_usage_rows(expired_ms(&*clock, 1), 2);

    let result = run_job(&runner, RETENTION_DAYS, clock).await;

    assert_eq!(runner.prune_calls.load(Ordering::SeqCst), 1);
    assert_eq!(runner.compaction_calls.load(Ordering::SeqCst), 1);
    assert_eq!(result, done_result(3, 3, 0, 0));
    assert_eq!(runner.request_event_count(), 2);
    assert_eq!(runner.audit_entry_count(), 2);
}

#[tokio::test]
async fn zero_general_retention_still_compacts_api_key_usage() {
    let clock = test_clock();
    let runner = RecordingUsagePruneRunner::default();
    runner.seed_usage_rows(expired_ms(&*clock, 100), 2);
    runner.set_compaction_result(4, 2);

    let result = run_job(&runner, 0, clock).await;

    assert_eq!(runner.prune_calls.load(Ordering::SeqCst), 0);
    assert_eq!(runner.compaction_calls.load(Ordering::SeqCst), 1);
    assert_eq!(runner.compaction_args(), expected_compaction_args());
    assert_eq!(result, done_result(0, 0, 4, 2));
    assert_eq!(runner.request_event_count(), 2);
    assert_eq!(runner.audit_entry_count(), 2);
}

struct RecordingUsagePruneRunner {
    prune_calls: AtomicUsize,
    compaction_calls: AtomicUsize,
    compaction_args: Mutex<Vec<(u64, u64, usize)>>,
    compaction_result: Mutex<ApiKeyUsageCompactionRun>,
    request_event_timestamps_ms: Mutex<Vec<u64>>,
    audit_log_timestamps_secs: Mutex<Vec<u64>>,
}

impl Default for RecordingUsagePruneRunner {
    fn default() -> Self {
        Self {
            prune_calls: AtomicUsize::new(0),
            compaction_calls: AtomicUsize::new(0),
            compaction_args: Mutex::new(Vec::new()),
            compaction_result: Mutex::new(ApiKeyUsageCompactionRun {
                folded_rows: 0,
                pruned_rows: 0,
            }),
            request_event_timestamps_ms: Mutex::new(Vec::new()),
            audit_log_timestamps_secs: Mutex::new(Vec::new()),
        }
    }
}

impl RecordingUsagePruneRunner {
    fn seed_usage_rows(&self, timestamp_ms: u64, count: usize) {
        let mut request_events = self
            .request_event_timestamps_ms
            .lock()
            .expect("request event test rows lock");
        let mut audit_log = self
            .audit_log_timestamps_secs
            .lock()
            .expect("audit log test rows lock");
        for _ in 0..count {
            request_events.push(timestamp_ms);
            audit_log.push(timestamp_ms / 1_000);
        }
    }

    fn set_compaction_result(&self, folded_rows: u64, pruned_rows: u64) {
        *self
            .compaction_result
            .lock()
            .expect("compaction result lock") = ApiKeyUsageCompactionRun {
            folded_rows,
            pruned_rows,
        };
    }

    fn compaction_args(&self) -> Vec<(u64, u64, usize)> {
        self.compaction_args
            .lock()
            .expect("compaction args lock")
            .clone()
    }

    fn request_event_count(&self) -> usize {
        self.request_event_timestamps_ms
            .lock()
            .expect("request event test rows lock")
            .len()
    }

    fn audit_entry_count(&self) -> usize {
        self.audit_log_timestamps_secs
            .lock()
            .expect("audit log test rows lock")
            .len()
    }

    fn prune_records(&self, retention_days: u64, clock: &dyn Clock) -> PruneResult {
        self.prune_calls.fetch_add(1, Ordering::SeqCst);
        let cutoff_ms = now_unix_ms(clock).saturating_sub(retention_days.saturating_mul(DAY_MS));
        let cutoff_secs = cutoff_ms / 1_000;
        let request_events_removed = prune_less_than(
            &self.request_event_timestamps_ms,
            cutoff_ms,
            "request event test rows lock",
        );
        let audit_log_removed = prune_less_than(
            &self.audit_log_timestamps_secs,
            cutoff_secs,
            "audit log test rows lock",
        );
        PruneResult {
            request_events_removed,
            audit_log_removed,
        }
    }
}

impl UsagePruneRunner for RecordingUsagePruneRunner {
    fn prune_once_for_retention(
        &self,
        retention_days: u64,
        clock: ClockHandle,
    ) -> impl future::Future<Output = PruneResult> + Send + '_ {
        future::ready(self.prune_records(retention_days, &*clock))
    }

    fn compact_api_key_usage_buckets(
        &self,
        writer_inactive_after_secs: u64,
        retain_for_secs: u64,
        batch_size: usize,
    ) -> impl future::Future<Output = StorageResult<ApiKeyUsageCompactionRun>> + Send + '_ {
        self.compaction_calls.fetch_add(1, Ordering::SeqCst);
        self.compaction_args
            .lock()
            .expect("compaction args lock")
            .push((writer_inactive_after_secs, retain_for_secs, batch_size));
        let result = *self
            .compaction_result
            .lock()
            .expect("compaction result lock");
        future::ready(Ok(result))
    }
}

async fn run_job(
    runner: &RecordingUsagePruneRunner,
    retention_days: u64,
    clock: ClockHandle,
) -> UsagePruneJobResult {
    handle_usage_prune_job(
        UsagePruneJob::default(),
        runner,
        retention_days,
        clock,
        WRITER_INACTIVE_AFTER_SECS,
        API_KEY_USAGE_RETAIN_FOR_SECS,
        COMPACTION_BATCH_SIZE,
    )
    .await
    .expect("usage prune job")
}

fn prune_less_than(rows: &Mutex<Vec<u64>>, cutoff: u64, lock_name: &str) -> u64 {
    let mut rows = rows.lock().expect(lock_name);
    let before = rows.len();
    rows.retain(|timestamp| *timestamp >= cutoff);
    u64::try_from(before - rows.len()).expect("test row count fits in u64")
}

fn done_result(
    request_events_removed: u64,
    audit_log_removed: u64,
    folded_rows: u64,
    pruned_rows: u64,
) -> UsagePruneJobResult {
    UsagePruneJobResult::Done {
        result: PruneResult {
            request_events_removed,
            audit_log_removed,
        },
        api_key_usage_compaction: ApiKeyUsageCompactionRun {
            folded_rows,
            pruned_rows,
        },
    }
}

fn expected_compaction_args() -> Vec<(u64, u64, usize)> {
    vec![(
        WRITER_INACTIVE_AFTER_SECS,
        API_KEY_USAGE_RETAIN_FOR_SECS,
        COMPACTION_BATCH_SIZE,
    )]
}

fn test_clock() -> ClockHandle {
    Arc::new(TestClock::new_at_secs(NOW_UNIX_SECS))
}

fn expired_ms(clock: &dyn Clock, days_ago: u64) -> u64 {
    now_unix_ms(clock).saturating_sub(days_ago.saturating_mul(DAY_MS))
}

fn now_unix_ms(clock: &dyn Clock) -> u64 {
    u64::try_from(unix_millis(clock.now())).expect("test clock millis fit in u64")
}
