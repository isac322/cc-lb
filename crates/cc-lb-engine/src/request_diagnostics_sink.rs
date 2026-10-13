//! Per-request diagnostics delivery (`cc_lb.request_diagnostics.v1`).
//!
//! The request event assembler hands one [`RequestDiagnosticsRecord`] per persisted row to a
//! [`RequestDiagnosticsSink`]. The stdout sink renders and writes each record on a dedicated OS
//! thread, so neither serialization nor a stalled stdout pipe runs on the proxy request path.

use std::io::Write as _;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use cc_lb_request_log::{RequestDiagnosticsRecord, render_request_diagnostics_line};
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio::sync::watch;

/// How the assembler wants a record delivered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestDiagnosticsDelivery {
    /// strip OFF: the DB still holds the fields, so drop on a full queue (try_send).
    BestEffort,
    /// strip ON: the line is the only copy, so wait up to the timeout for queue space.
    BoundedWait(Duration),
}

#[async_trait]
pub trait RequestDiagnosticsSink: Send + Sync {
    /// Called only from the assembler task (off the proxy path). Never blocks a thread;
    /// awaits at most the `BoundedWait` timeout. Drops are counted internally.
    async fn offer(&self, record: RequestDiagnosticsRecord, delivery: RequestDiagnosticsDelivery);
    /// Drain queued records: called once, after the assembler task has stopped. Returns within
    /// two seconds even when stdout is stalled.
    async fn shutdown(&self);
}

pub const REQUEST_DIAGNOSTICS_QUEUE_CAPACITY: usize = 1024;
pub const REQUEST_DIAGNOSTICS_STRIP_SEND_TIMEOUT: Duration = Duration::from_millis(500);
/// Budget for draining queued lines at shutdown. Matches the assembler's own overflow drain
/// grace (`OVERFLOW_DRAIN_GRACE`), so the assembler shutdown hook stays bounded end to end and
/// a stalled stdout pipe cannot hold the server's sequential shutdown hooks until SIGKILL.
const REQUEST_DIAGNOSTICS_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

/// Assembler-side diagnostics configuration. The default (`sink: None`) does zero work.
#[derive(Clone, Default)]
pub struct RequestDiagnosticsOptions {
    /// `None` keeps the feature off: no clone, no render, no thread.
    pub sink: Option<Arc<dyn RequestDiagnosticsSink>>,
    /// Remove the five diagnostic fields from the row before it is persisted.
    pub strip_payload: bool,
}

impl std::fmt::Debug for RequestDiagnosticsOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestDiagnosticsOptions")
            .field(
                "sink",
                &self.sink.as_ref().map(|_| "RequestDiagnosticsSink"),
            )
            .field("strip_payload", &self.strip_payload)
            .finish()
    }
}

const QUEUE_FULL_REASON: &str = "request_diagnostics_queue_full";
const STRIP_QUEUE_FULL_REASON: &str = "request_diagnostics_strip_queue_full";
const STRIP_SEND_TIMEOUT_REASON: &str = "request_diagnostics_strip_send_timeout";
const QUEUE_CLOSED_REASON: &str = "request_diagnostics_queue_closed";
const JOB_PANIC_REASON: &str = "request_diagnostics_job_panic";
const WORKER_PANIC_REASON: &str = "request_diagnostics_worker_panic";
const SHUTDOWN_TIMEOUT_REASON: &str = "request_diagnostics_shutdown_timeout";

const OUTCOME_WRITTEN: &str = "written";
const OUTCOME_ENCODE_ERROR: &str = "encode_error";
const OUTCOME_WRITE_ERROR: &str = "write_error";

type QueueMessage = Option<RequestDiagnosticsRecord>;
type LineOutput = Box<dyn FnMut(&[u8]) -> std::io::Result<()> + Send>;

/// Metric sinks, injectable so unit tests can observe them without a global recorder.
#[derive(Clone, Copy)]
struct Recorders {
    drop: fn(&'static str, u64),
    line: fn(&'static str),
}

const METRICS_RECORDERS: Recorders = Recorders {
    drop: record_dropped,
    line: record_line_outcome,
};

fn record_dropped(reason: &'static str, amount: u64) {
    cc_lb_observability::increment_dropped_events_by(reason, amount);
}

fn record_line_outcome(outcome: &'static str) {
    metrics::counter!("cc_lb_request_diagnostics_lines_total", "outcome" => outcome).increment(1);
}

/// Accounting shared between the sink handle and its worker thread.
///
/// Every accepted record is claimed exactly once: either by the worker — which marks it
/// in-flight for its blocking write and records its line outcome only if it still owns the
/// record when the write returns — or by a timed-out shutdown, which counts every queued record
/// plus the record the worker is still writing as `request_diagnostics_shutdown_timeout`. A
/// record whose claim fails is skipped, so no record is counted twice and
/// `rows = lines + drops` holds.
#[derive(Default)]
struct WorkerState {
    /// Records accepted into the queue that the worker has not started writing.
    queued: AtomicU64,
    /// Ownership of the record the worker has started but not finished. A timed-out shutdown
    /// steals it (`IN_FLIGHT_SHUTDOWN`) so the record is counted even while its write stalls;
    /// the worker then suppresses the outcome when its write returns. The bytes may already be
    /// on the wire, so a timed-out in-flight line can appear once in the stream while counting
    /// as dropped: accounting is conservative.
    in_flight: AtomicU8,
}

const IN_FLIGHT_NONE: u8 = 0;
const IN_FLIGHT_WORKER: u8 = 1;
const IN_FLIGHT_SHUTDOWN: u8 = 2;

impl WorkerState {
    /// Takes one unit from `queued`; `false` once shutdown has claimed everything.
    fn claim_one(&self) -> bool {
        self.queued
            .try_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
            .is_ok()
    }

    /// Marks the just-claimed record in-flight. `false` when shutdown has already timed out:
    /// the record was claimed between the queue sweep and the state swap, so the caller counts
    /// it as `request_diagnostics_shutdown_timeout` and does not write it.
    fn begin_in_flight(&self) -> bool {
        self.in_flight
            .compare_exchange(
                IN_FLIGHT_NONE,
                IN_FLIGHT_WORKER,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Releases the in-flight record. `false` when a timed-out shutdown stole it and already
    /// counted it as dropped, in which case the caller suppresses the record's own outcome.
    fn finish_in_flight(&self) -> bool {
        self.in_flight
            .compare_exchange(
                IN_FLIGHT_WORKER,
                IN_FLIGHT_NONE,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Claims the in-flight record for a timed-out shutdown; `1` when the worker was writing,
    /// `0` when it was idle or shutdown already claimed it.
    fn steal_in_flight(&self) -> u64 {
        u64::from(self.in_flight.swap(IN_FLIGHT_SHUTDOWN, Ordering::AcqRel) == IN_FLIGHT_WORKER)
    }

    /// Returns the unit reserved by an offer whose enqueue failed.
    fn unreserve(&self) {
        let _ = self.claim_one();
    }
}

/// Writes one compact JSON line per record to process stdout from a dedicated OS thread.
///
/// Lines share the stdout pipe and its `std::io::Stdout` mutex with tracing, so a stalled log
/// collector also delays request-path tracing writes while this thread holds the lock.
pub struct StdoutRequestDiagnosticsSink {
    sender: ArcSwapOption<mpsc::Sender<QueueMessage>>,
    completion: watch::Receiver<bool>,
    degraded: AtomicBool,
    worker: Arc<WorkerState>,
    recorders: Recorders,
}

impl StdoutRequestDiagnosticsSink {
    pub fn new() -> std::io::Result<Self> {
        Self::spawn(
            REQUEST_DIAGNOSTICS_QUEUE_CAPACITY,
            Box::new(write_stdout_line),
            METRICS_RECORDERS,
        )
    }

    fn spawn(capacity: usize, output: LineOutput, recorders: Recorders) -> std::io::Result<Self> {
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        let (completion_tx, completion) = watch::channel(false);
        let worker = Arc::new(WorkerState::default());
        let worker_state = Arc::clone(&worker);
        std::thread::Builder::new()
            .name("cc-lb-request-diagnostics".to_owned())
            .spawn(move || {
                diagnostics_loop(receiver, completion_tx, output, recorders, &worker_state)
            })?;
        Ok(Self {
            sender: ArcSwapOption::from(Some(Arc::new(sender))),
            completion,
            degraded: AtomicBool::new(false),
            worker,
            recorders,
        })
    }

    fn try_offer(
        &self,
        sender: &mpsc::Sender<QueueMessage>,
        record: RequestDiagnosticsRecord,
        full_reason: &'static str,
    ) {
        // Reserve before enqueueing so the worker's claim can never run first.
        self.worker.queued.fetch_add(1, Ordering::AcqRel);
        match sender.try_send(Some(record)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.worker.unreserve();
                (self.recorders.drop)(full_reason, 1);
            }
            Err(TrySendError::Closed(_)) => {
                self.worker.unreserve();
                (self.recorders.drop)(QUEUE_CLOSED_REASON, 1);
            }
        }
    }
}

#[async_trait]
impl RequestDiagnosticsSink for StdoutRequestDiagnosticsSink {
    async fn offer(&self, record: RequestDiagnosticsRecord, delivery: RequestDiagnosticsDelivery) {
        let Some(sender) = self.sender.load_full() else {
            (self.recorders.drop)(QUEUE_CLOSED_REASON, 1);
            return;
        };
        let timeout = match delivery {
            RequestDiagnosticsDelivery::BestEffort => {
                self.try_offer(&sender, record, QUEUE_FULL_REASON);
                return;
            }
            RequestDiagnosticsDelivery::BoundedWait(timeout) => timeout,
        };
        if self.degraded.load(Ordering::Relaxed) {
            // One timeout per stdout-stall episode: stay non-blocking until the worker has
            // drained the queue to at least half empty.
            if sender.capacity() >= sender.max_capacity().div_ceil(2) {
                self.degraded.store(false, Ordering::Relaxed);
            } else {
                self.try_offer(&sender, record, STRIP_QUEUE_FULL_REASON);
                return;
            }
        }
        self.worker.queued.fetch_add(1, Ordering::AcqRel);
        match tokio::time::timeout(timeout, sender.send(Some(record))).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                self.worker.unreserve();
                (self.recorders.drop)(QUEUE_CLOSED_REASON, 1);
            }
            Err(_) => {
                self.worker.unreserve();
                (self.recorders.drop)(STRIP_SEND_TIMEOUT_REASON, 1);
                self.degraded.store(true, Ordering::Relaxed);
            }
        }
    }

    async fn shutdown(&self) {
        let mut completion = self.completion.clone();
        if *completion.borrow() {
            return;
        }
        let drained = tokio::time::timeout(REQUEST_DIAGNOSTICS_SHUTDOWN_TIMEOUT, async {
            if let Some(sender) = self.sender.swap(None) {
                let _ = sender.send(None).await;
            }
            while !*completion.borrow_and_update() {
                if completion.changed().await.is_err() {
                    break;
                }
            }
        })
        .await;
        if drained.is_err() {
            // The worker is stuck in a stdout write. Leave the thread detached instead of
            // joining it, and count every record it has not started plus the record it is
            // still writing, suppressing that record's own outcome if the write ever returns.
            // Deliberately no log here: tracing writes to the same stalled stdout and would
            // block shutdown.
            let dropped =
                self.worker.queued.swap(0, Ordering::AcqRel) + self.worker.steal_in_flight();
            if dropped > 0 {
                (self.recorders.drop)(SHUTDOWN_TIMEOUT_REASON, dropped);
            }
        }
    }
}

impl Drop for StdoutRequestDiagnosticsSink {
    fn drop(&mut self) {
        self.sender.store(None);
    }
}

/// One `write_all` on the locked process stdout, so a line never interleaves with tracing
/// output in-process. Deliberately bypasses the redacting tracing writer: producers already
/// redacted these values, and text redaction over compact JSON could corrupt the line.
fn write_stdout_line(line: &[u8]) -> std::io::Result<()> {
    std::io::stdout().lock().write_all(line)
}

// These catches protect unwind builds. They cannot intercept the workspace release profile's
// panic=abort behavior.
fn diagnostics_loop(
    mut receiver: mpsc::Receiver<QueueMessage>,
    completion: watch::Sender<bool>,
    mut output: LineOutput,
    recorders: Recorders,
    worker: &WorkerState,
) {
    let mut handle = |record: RequestDiagnosticsRecord| {
        if !worker.claim_one() {
            // Shutdown timed out and already counted this record as dropped.
            return;
        }
        if !worker.begin_in_flight() {
            // The record was claimed while shutdown timed out between the queue sweep and the
            // in-flight swap, so timeout accounting never saw it. Count it here instead.
            (recorders.drop)(SHUTDOWN_TIMEOUT_REASON, 1);
            return;
        }
        let outcome = process_record(&record, &mut output);
        if worker.finish_in_flight() {
            match outcome {
                RecordOutcome::Line(outcome) => (recorders.line)(outcome),
                RecordOutcome::JobPanic => (recorders.drop)(JOB_PANIC_REASON, 1),
            }
        }
        // Shutdown stole the record while its write was in flight: the line may appear once,
        // but its outcome is already counted as dropped and stays suppressed.
    };
    if catch_unwind(AssertUnwindSafe(|| {
        while let Some(message) = receiver.blocking_recv() {
            match message {
                Some(record) => handle(record),
                None => {
                    receiver.close();
                    while let Some(message) = receiver.blocking_recv() {
                        if let Some(record) = message {
                            handle(record);
                        }
                    }
                    break;
                }
            }
        }
    }))
    .is_err()
    {
        // A panic that escaped the per-record catch may have left an in-flight marker behind;
        // reset it so a later shutdown timeout cannot double-count that record, then count the
        // worker death as one conservative drop.
        let _ = worker.in_flight.compare_exchange(
            IN_FLIGHT_WORKER,
            IN_FLIGHT_NONE,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        (recorders.drop)(WORKER_PANIC_REASON, 1);
    }
    let _ = completion.send(true);
}

/// How one processed record should be accounted once it finishes.
enum RecordOutcome {
    /// A `cc_lb_request_diagnostics_lines_total` outcome label.
    Line(&'static str),
    /// Rendering or writing panicked; counted as a drop, not a line outcome.
    JobPanic,
}

fn process_record(record: &RequestDiagnosticsRecord, output: &mut LineOutput) -> RecordOutcome {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        match render_request_diagnostics_line(record) {
            Err(_) => OUTCOME_ENCODE_ERROR,
            Ok(line) => match (**output)(&line) {
                Ok(()) => OUTCOME_WRITTEN,
                Err(_) => OUTCOME_WRITE_ERROR,
            },
        }
    }));
    match outcome {
        Ok(outcome) => RecordOutcome::Line(outcome),
        Err(_) => RecordOutcome::JobPanic,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Condvar, Mutex as StdMutex, mpsc as std_mpsc};

    use cc_lb_domain::{InternalError, InternalErrorKind, InternalErrorStage};
    use cc_lb_request_log::{RequestDiagnostics, RequestDiagnosticsSource, RequestEvent};

    use super::*;

    static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    static TEST_METRICS: StdMutex<Vec<&'static str>> = StdMutex::new(Vec::new());
    /// Signalled on every recorded outcome so tests can wait for the worker deterministically.
    static TEST_METRICS_CHANGED: Condvar = Condvar::new();

    fn test_record_drop(reason: &'static str, amount: u64) {
        let mut metrics = TEST_METRICS.lock().expect("test metric lock");
        for _ in 0..amount {
            metrics.push(reason);
        }
        TEST_METRICS_CHANGED.notify_all();
    }

    fn test_record_line(outcome: &'static str) {
        TEST_METRICS.lock().expect("test metric lock").push(outcome);
        TEST_METRICS_CHANGED.notify_all();
    }

    const TEST_RECORDERS: Recorders = Recorders {
        drop: test_record_drop,
        line: test_record_line,
    };

    fn recorded() -> Vec<&'static str> {
        TEST_METRICS.lock().expect("test metric lock").clone()
    }

    fn reset_recorded() {
        TEST_METRICS.lock().expect("test metric lock").clear();
    }

    /// Blocks until `count` outcomes are recorded, i.e. the worker has finished every record the
    /// test expects written. Call before `shutdown()` so the shutdown deadline (which a paused
    /// clock auto-advances while the runtime idles) never races the worker. The real-time bound
    /// only turns a lost record into a failure instead of a hang.
    fn wait_recorded(count: usize) {
        let metrics = TEST_METRICS.lock().expect("test metric lock");
        let (metrics, timeout) = TEST_METRICS_CHANGED
            .wait_timeout_while(metrics, Duration::from_secs(10), |metrics| {
                metrics.len() < count
            })
            .expect("test metric wait");
        assert!(
            !timeout.timed_out(),
            "worker recorded {metrics:?}, expected {count} outcomes"
        );
    }

    type Release = Arc<(StdMutex<bool>, Condvar)>;

    /// Captures every line the worker writes; can block inside the write, fail, or panic.
    #[derive(Default)]
    struct OutputProbe {
        gate: Option<(std_mpsc::SyncSender<()>, Release)>,
        panic_first: AtomicBool,
        fail_first: AtomicBool,
        lines: StdMutex<Vec<Vec<u8>>>,
    }

    impl OutputProbe {
        fn blocking(entered: std_mpsc::SyncSender<()>, released: Release) -> Arc<Self> {
            Arc::new(Self {
                gate: Some((entered, released)),
                ..Self::default()
            })
        }

        fn lines(&self) -> Vec<Vec<u8>> {
            self.lines.lock().expect("line lock").clone()
        }

        fn event_ids(&self) -> Vec<String> {
            self.lines()
                .iter()
                .map(|line| {
                    let value: serde_json::Value =
                        serde_json::from_slice(line).expect("line is JSON");
                    value["event_id"]
                        .as_str()
                        .expect("event_id is a string")
                        .to_owned()
                })
                .collect()
        }

        fn output(self: &Arc<Self>) -> LineOutput {
            let probe = Arc::clone(self);
            Box::new(move |line: &[u8]| {
                if probe.panic_first.swap(false, Ordering::SeqCst) {
                    panic!("intentional diagnostics write panic");
                }
                if probe.fail_first.swap(false, Ordering::SeqCst) {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "injected stdout failure",
                    ));
                }
                probe.lines.lock().expect("line lock").push(line.to_vec());
                if let Some((entered, released)) = &probe.gate {
                    let _ = entered.send(());
                    let (released, wake) = &**released;
                    let mut released = released.lock().expect("release lock");
                    while !*released {
                        released = wake.wait(released).expect("release wait");
                    }
                }
                Ok(())
            })
        }
    }

    fn release(released: &Release) {
        let (release_lock, wake) = &**released;
        *release_lock.lock().expect("release lock") = true;
        wake.notify_all();
    }

    fn sink(capacity: usize, probe: &Arc<OutputProbe>) -> StdoutRequestDiagnosticsSink {
        StdoutRequestDiagnosticsSink::spawn(capacity, probe.output(), TEST_RECORDERS)
            .expect("spawn diagnostics worker")
    }

    fn record(event_id: &str) -> RequestDiagnosticsRecord {
        record_with_message(event_id, None)
    }

    fn record_with_message(event_id: &str, message: Option<&str>) -> RequestDiagnosticsRecord {
        let event = RequestEvent {
            ts_ms: Some(1_760_332_740_123),
            request_id: format!("req-{event_id}"),
            status: 200,
            event_id: Some(event_id.to_owned()),
            ..RequestEvent::default()
        };
        let diagnostics = RequestDiagnostics {
            cache_control_message_indices: vec![0, 3],
            internal_errors: message
                .map(|message| {
                    vec![InternalError {
                        stage: InternalErrorStage::Router,
                        kind: InternalErrorKind::PluginError,
                        message: Some(message.to_owned()),
                    }]
                })
                .unwrap_or_default(),
            ..RequestDiagnostics::default()
        };
        RequestDiagnosticsRecord::new(
            RequestDiagnosticsSource::Live,
            7,
            event_id.to_owned(),
            &event,
            diagnostics,
        )
    }

    fn entered(entered_rx: &std_mpsc::Receiver<()>, what: &str) {
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap_or_else(|_| panic!("worker never entered {what}"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn best_effort_counts_full_and_closed_without_waiting() {
        let _serial = SERIAL.lock().await;
        reset_recorded();
        let (entered_tx, entered_rx) = std_mpsc::sync_channel(4);
        let released: Release = Arc::new((StdMutex::new(false), Condvar::new()));
        let probe = OutputProbe::blocking(entered_tx, Arc::clone(&released));
        let sink = sink(1, &probe);

        sink.offer(record("evt-1"), RequestDiagnosticsDelivery::BestEffort)
            .await;
        entered(&entered_rx, "first line");
        sink.offer(record("evt-2"), RequestDiagnosticsDelivery::BestEffort)
            .await;
        // The queue is full while stdout is stalled; BestEffort must return immediately.
        tokio::time::timeout(
            Duration::from_secs(1),
            sink.offer(record("evt-3"), RequestDiagnosticsDelivery::BestEffort),
        )
        .await
        .expect("best-effort offer never waits for queue space");

        release(&released);
        // QUEUE_FULL plus evt-1 and evt-2 written.
        wait_recorded(3);
        sink.shutdown().await;
        sink.offer(record("evt-4"), RequestDiagnosticsDelivery::BestEffort)
            .await;

        assert_eq!(probe.event_ids(), ["evt-1", "evt-2"]);
        assert_eq!(
            recorded(),
            [
                QUEUE_FULL_REASON,
                OUTCOME_WRITTEN,
                OUTCOME_WRITTEN,
                QUEUE_CLOSED_REASON
            ]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bounded_wait_delivers_once_space_frees_within_timeout() {
        let _serial = SERIAL.lock().await;
        reset_recorded();
        let (entered_tx, entered_rx) = std_mpsc::sync_channel(4);
        let released: Release = Arc::new((StdMutex::new(false), Condvar::new()));
        let probe = OutputProbe::blocking(entered_tx, Arc::clone(&released));
        let sink = sink(1, &probe);

        sink.offer(record("evt-1"), RequestDiagnosticsDelivery::BestEffort)
            .await;
        entered(&entered_rx, "first line");
        sink.offer(record("evt-2"), RequestDiagnosticsDelivery::BestEffort)
            .await;

        let waiting = sink.offer(
            record("evt-3"),
            RequestDiagnosticsDelivery::BoundedWait(Duration::from_secs(30)),
        );
        tokio::pin!(waiting);
        tokio::select! {
            biased;
            () = &mut waiting => panic!("bounded-wait offer returned while the queue was full"),
            () = tokio::task::yield_now() => {}
        }
        release(&released);
        waiting.await;
        wait_recorded(3);
        sink.shutdown().await;

        assert_eq!(probe.event_ids(), ["evt-1", "evt-2", "evt-3"]);
        assert_eq!(recorded(), [OUTCOME_WRITTEN; 3]);
        assert!(!sink.degraded.load(Ordering::Relaxed));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn bounded_wait_times_out_degrades_and_recovers_when_half_empty() {
        let _serial = SERIAL.lock().await;
        reset_recorded();
        let (entered_tx, entered_rx) = std_mpsc::sync_channel(8);
        let released: Release = Arc::new((StdMutex::new(false), Condvar::new()));
        let probe = OutputProbe::blocking(entered_tx, Arc::clone(&released));
        let sink = sink(2, &probe);
        let strip = RequestDiagnosticsDelivery::BoundedWait(REQUEST_DIAGNOSTICS_STRIP_SEND_TIMEOUT);

        sink.offer(record("evt-1"), strip).await;
        entered(&entered_rx, "first line");
        sink.offer(record("evt-2"), strip).await;
        sink.offer(record("evt-3"), strip).await;

        // Queue full and stdout stalled: the offer waits exactly the strip timeout, then drops.
        let started = tokio::time::Instant::now();
        sink.offer(record("evt-timeout"), strip).await;
        let waited = started.elapsed();
        assert!(
            waited >= REQUEST_DIAGNOSTICS_STRIP_SEND_TIMEOUT
                && waited < REQUEST_DIAGNOSTICS_STRIP_SEND_TIMEOUT + Duration::from_millis(5),
            "bounded wait lasts the strip timeout, waited {waited:?}"
        );
        assert!(sink.degraded.load(Ordering::Relaxed));

        // Degraded: later rows do not pay the timeout again during the same stall.
        let started = tokio::time::Instant::now();
        sink.offer(record("evt-degraded"), strip).await;
        assert_eq!(started.elapsed(), Duration::ZERO);

        release(&released);
        // Timeout and degraded drops, then evt-1..evt-3 written: the worker has received both
        // queued records, so the queue is empty and at least half empty again.
        wait_recorded(5);

        sink.offer(record("evt-recovered"), strip).await;
        assert!(!sink.degraded.load(Ordering::Relaxed));
        wait_recorded(6);
        sink.shutdown().await;

        assert_eq!(
            probe.event_ids(),
            ["evt-1", "evt-2", "evt-3", "evt-recovered"]
        );
        let metrics = recorded();
        let drops: Vec<_> = metrics
            .iter()
            .filter(|metric| metric.starts_with("request_diagnostics_"))
            .copied()
            .collect();
        assert_eq!(drops, [STRIP_SEND_TIMEOUT_REASON, STRIP_QUEUE_FULL_REASON]);
        assert_eq!(
            metrics
                .iter()
                .filter(|metric| **metric == OUTCOME_WRITTEN)
                .count(),
            4
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn shutdown_with_stalled_stdout_returns_at_deadline_and_counts_every_accepted_line() {
        let _serial = SERIAL.lock().await;
        reset_recorded();
        let (entered_tx, entered_rx) = std_mpsc::sync_channel(4);
        let released: Release = Arc::new((StdMutex::new(false), Condvar::new()));
        // The writer blocks until released: a stdout pipe nobody reads.
        let probe = OutputProbe::blocking(entered_tx, Arc::clone(&released));
        let sink = sink(4, &probe);

        sink.offer(record("evt-1"), RequestDiagnosticsDelivery::BestEffort)
            .await;
        entered(&entered_rx, "first line");
        sink.offer(record("evt-2"), RequestDiagnosticsDelivery::BestEffort)
            .await;
        sink.offer(record("evt-3"), RequestDiagnosticsDelivery::BestEffort)
            .await;

        let started = tokio::time::Instant::now();
        tokio::time::timeout(
            REQUEST_DIAGNOSTICS_SHUTDOWN_TIMEOUT + Duration::from_secs(1),
            sink.shutdown(),
        )
        .await
        .expect("shutdown must not wait for a stalled stdout write");
        let waited = started.elapsed();
        assert!(
            waited >= REQUEST_DIAGNOSTICS_SHUTDOWN_TIMEOUT
                && waited < REQUEST_DIAGNOSTICS_SHUTDOWN_TIMEOUT + Duration::from_millis(5),
            "shutdown returns at its deadline, waited {waited:?}"
        );
        // All three accepted lines are dropped at the deadline: the two still queued plus the
        // in-flight line, whose write outcome shutdown claimed by stealing it.
        assert_eq!(recorded(), [SHUTDOWN_TIMEOUT_REASON; 3]);

        // Once stdout unblocks, the detached worker finishes writing the stolen line — its bytes
        // land, but the outcome stays suppressed — and skips the swept queued lines: each record
        // is counted exactly once, conservatively as dropped.
        release(&released);
        let mut completion = sink.completion.clone();
        while !*completion.borrow_and_update() {
            completion
                .changed()
                .await
                .expect("worker signals completion");
        }
        assert_eq!(probe.event_ids(), ["evt-1"]);
        assert_eq!(recorded(), [SHUTDOWN_TIMEOUT_REASON; 3]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_drains_every_accepted_record_in_order() {
        let _serial = SERIAL.lock().await;
        reset_recorded();
        let probe = Arc::new(OutputProbe::default());
        let sink = sink(4, &probe);

        for event_id in ["evt-1", "evt-2", "evt-3"] {
            sink.offer(record(event_id), RequestDiagnosticsDelivery::BestEffort)
                .await;
        }
        sink.shutdown().await;
        // A second shutdown is a no-op once the worker has completed.
        sink.shutdown().await;

        assert_eq!(probe.event_ids(), ["evt-1", "evt-2", "evt-3"]);
        assert_eq!(recorded(), [OUTCOME_WRITTEN; 3]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn write_error_is_counted_and_worker_keeps_writing() {
        let _serial = SERIAL.lock().await;
        reset_recorded();
        let probe = Arc::new(OutputProbe {
            fail_first: AtomicBool::new(true),
            ..OutputProbe::default()
        });
        let sink = sink(4, &probe);

        sink.offer(record("evt-1"), RequestDiagnosticsDelivery::BestEffort)
            .await;
        sink.offer(record("evt-2"), RequestDiagnosticsDelivery::BestEffort)
            .await;
        wait_recorded(2);
        sink.shutdown().await;

        assert_eq!(probe.event_ids(), ["evt-2"]);
        assert_eq!(recorded(), [OUTCOME_WRITE_ERROR, OUTCOME_WRITTEN]);
    }

    #[cfg(panic = "unwind")]
    #[tokio::test(flavor = "current_thread")]
    async fn write_panic_is_counted_and_worker_processes_following_record() {
        let _serial = SERIAL.lock().await;
        reset_recorded();
        let probe = Arc::new(OutputProbe {
            panic_first: AtomicBool::new(true),
            ..OutputProbe::default()
        });
        let sink = sink(4, &probe);

        sink.offer(record("evt-1"), RequestDiagnosticsDelivery::BestEffort)
            .await;
        sink.offer(record("evt-2"), RequestDiagnosticsDelivery::BestEffort)
            .await;
        wait_recorded(2);
        sink.shutdown().await;

        assert_eq!(probe.event_ids(), ["evt-2"]);
        assert_eq!(recorded(), [JOB_PANIC_REASON, OUTCOME_WRITTEN]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn producer_redacted_message_is_written_verbatim_as_valid_json() {
        let _serial = SERIAL.lock().await;
        reset_recorded();
        let probe = Arc::new(OutputProbe::default());
        let sink = sink(4, &probe);
        let record = record_with_message("evt-redacted", Some("token=[REDACTED] tail"));
        let expected = render_request_diagnostics_line(&record).expect("record renders");

        sink.offer(record, RequestDiagnosticsDelivery::BestEffort)
            .await;
        wait_recorded(1);
        sink.shutdown().await;

        let lines = probe.lines();
        assert_eq!(lines.len(), 1);
        let Some(line) = lines.first() else {
            panic!("one line written");
        };
        // The sink writes the shared renderer's bytes untouched: no second redaction pass.
        assert_eq!(line, &expected);
        assert!(line.ends_with(b"\n"));
        assert!(line.starts_with(br#"{"schema":"cc_lb.request_diagnostics.v1","#));
        let value: serde_json::Value = serde_json::from_slice(line).expect("line is valid JSON");
        let object = value.as_object().expect("line is a JSON object");
        assert_eq!(object.len(), 21, "every key is present: {object:?}");
        assert_eq!(
            value["internal_errors"][0]["message"],
            "token=[REDACTED] tail"
        );
        assert_eq!(
            value["cache_control_message_indices"],
            serde_json::json!([0, 3])
        );
        assert_eq!(value["source"], "live");
        assert_eq!(value["seq"], 7);
    }
}
