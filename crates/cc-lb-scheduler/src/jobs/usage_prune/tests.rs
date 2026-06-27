use std::future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use cc_lb_core::clock::{Clock, ClockHandle, TestClock, unix_millis};
use cc_lb_core::usage_pruner::PruneResult;

use super::{UsagePruneJob, UsagePruneJobResult, UsagePruneRunner, handle_usage_prune_job};

const DAY_SECS: u64 = 86_400;
const DAY_MS: u64 = DAY_SECS * 1_000;
const RETENTION_DAYS: u64 = 90;
const NOW_UNIX_SECS: u64 = 1_800_000_000;

#[tokio::test]
async fn prune_execution_calls_pruner_once_and_returns_removed_counts() {
    let clock = test_clock();
    let runner = RecordingUsagePruneRunner::default();
    runner.seed_usage_rows(expired_ms(&*clock, 100), 5);

    let result =
        handle_usage_prune_job(UsagePruneJob::default(), &runner, RETENTION_DAYS, clock).await;

    assert_eq!(runner.calls.load(Ordering::SeqCst), 1);
    assert_eq!(result, done_result(5, 5));
    assert_eq!(runner.request_event_count(), 0);
    assert_eq!(runner.audit_entry_count(), 0);
}

#[tokio::test]
async fn retention_cutoff_preserves_rows_inside_retention_window() {
    let clock = test_clock();
    let runner = RecordingUsagePruneRunner::default();
    runner.seed_usage_rows(expired_ms(&*clock, 100), 3);
    runner.seed_usage_rows(expired_ms(&*clock, 1), 2);

    let result =
        handle_usage_prune_job(UsagePruneJob::default(), &runner, RETENTION_DAYS, clock).await;

    assert_eq!(runner.calls.load(Ordering::SeqCst), 1);
    assert_eq!(result, done_result(3, 3));
    assert_eq!(runner.request_event_count(), 2);
    assert_eq!(runner.audit_entry_count(), 2);
}

#[tokio::test]
async fn zero_retention_days_skips_pruner() {
    let clock = test_clock();
    let runner = RecordingUsagePruneRunner::default();
    runner.seed_usage_rows(expired_ms(&*clock, 100), 2);

    let result = handle_usage_prune_job(UsagePruneJob::default(), &runner, 0, clock).await;

    assert_eq!(runner.calls.load(Ordering::SeqCst), 0);
    assert_eq!(result, UsagePruneJobResult::Skip);
    assert_eq!(runner.request_event_count(), 2);
    assert_eq!(runner.audit_entry_count(), 2);
}

#[derive(Default)]
struct RecordingUsagePruneRunner {
    calls: AtomicUsize,
    request_event_timestamps_ms: Mutex<Vec<u64>>,
    audit_log_timestamps_secs: Mutex<Vec<u64>>,
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
        self.calls.fetch_add(1, Ordering::SeqCst);
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
            usage_rollups_removed: 0,
            principal_limit_states_removed: 0,
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
}

fn prune_less_than(rows: &Mutex<Vec<u64>>, cutoff: u64, lock_name: &str) -> u64 {
    let mut rows = rows.lock().expect(lock_name);
    let before = rows.len();
    rows.retain(|timestamp| *timestamp >= cutoff);
    u64::try_from(before - rows.len()).expect("test row count fits in u64")
}

fn done_result(request_events_removed: u64, audit_log_removed: u64) -> UsagePruneJobResult {
    UsagePruneJobResult::Done {
        result: PruneResult {
            request_events_removed,
            usage_rollups_removed: 0,
            principal_limit_states_removed: 0,
            audit_log_removed,
        },
    }
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
