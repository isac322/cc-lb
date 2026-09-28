use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use opentelemetry::trace::{SpanId, TraceContextExt as _, TraceId};
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio::sync::watch;
use tracing::{Dispatch, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

const QUEUE_FULL_REASON: &str = "stream_completion_observer_full";
const QUEUE_CLOSED_REASON: &str = "stream_completion_observer_closed";
const JOB_PANIC_REASON: &str = "stream_completion_observer_job_panic";
const WORKER_PANIC_REASON: &str = "stream_completion_observer_worker_panic";
/// Bounded so a stalled log sink sheds latency lines instead of growing memory.
const COMPLETION_QUEUE_CAPACITY: usize = 4096;

pub(crate) struct CompletionObserver {
    sender: ArcSwapOption<mpsc::Sender<Option<StreamCompletionLog>>>,
    completion: watch::Receiver<bool>,
    record_drop: DropRecorder,
}

type DropRecorder = fn(&'static str, u64);

impl CompletionObserver {
    pub(crate) fn new() -> Self {
        Self::spawn(COMPLETION_QUEUE_CAPACITY, record_observation_drop)
    }

    fn spawn(capacity: usize, record_drop: DropRecorder) -> Self {
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        let (completion_tx, completion) = watch::channel(false);
        std::thread::Builder::new()
            .name("cc-lb-completion-observer".to_owned())
            .spawn(move || completion_observer_loop(receiver, completion_tx, record_drop))
            .expect("spawn completion observation worker");
        Self {
            sender: ArcSwapOption::from(Some(Arc::new(sender))),
            completion,
            record_drop,
        }
    }

    pub(crate) fn enqueue(
        &self,
        log: StreamCompletionLog,
    ) -> Result<(), CompletionObservationDrop> {
        let sender = self.sender.load();
        let Some(sender) = sender.as_ref() else {
            (self.record_drop)(QUEUE_CLOSED_REASON, 1);
            return Err(CompletionObservationDrop::Closed);
        };
        match sender.try_send(Some(log)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                (self.record_drop)(QUEUE_FULL_REASON, 1);
                Err(CompletionObservationDrop::Full)
            }
            Err(TrySendError::Closed(_)) => {
                (self.record_drop)(QUEUE_CLOSED_REASON, 1);
                Err(CompletionObservationDrop::Closed)
            }
        }
    }

    pub(crate) async fn shutdown(&self) {
        let mut completion = self.completion.clone();
        if *completion.borrow() {
            return;
        }
        if let Some(sender) = self.sender.swap(None) {
            let _ = sender.send(None).await;
        }
        while !*completion.borrow_and_update() {
            if completion.changed().await.is_err() {
                break;
            }
        }
    }

    #[cfg(test)]
    fn with_capacity_and_recorder(capacity: usize, record_drop: DropRecorder) -> Self {
        Self::spawn(capacity, record_drop)
    }
}

impl Drop for CompletionObserver {
    fn drop(&mut self) {
        self.sender.store(None);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompletionObservationDrop {
    Full,
    Closed,
}

pub(crate) struct StreamCompletionLog {
    request_id: String,
    trace_id: Option<TraceId>,
    span_id: Option<SpanId>,
    dispatch: Dispatch,
    latency: StreamLatency,
}

impl StreamCompletionLog {
    pub(crate) fn new(
        request_id: String,
        span: &Span,
        latency: StreamLatency,
        dispatch: Dispatch,
    ) -> Self {
        let context = span.context();
        let span = context.span();
        let span_context = span.span_context();
        let (trace_id, span_id) = if span_context.is_valid() {
            (Some(span_context.trace_id()), Some(span_context.span_id()))
        } else {
            (None, None)
        };
        Self {
            request_id,
            trace_id,
            span_id,
            latency,
            dispatch,
        }
    }

    fn emit(&self) {
        tracing::dispatcher::with_default(&self.dispatch, || {
            if !tracing::enabled!(
                target: "cc_lb_engine::lifecycle",
                tracing::Level::INFO
            ) {
                return;
            }
            let trace_id = self.trace_id.map(|id| id.to_string());
            let span_id = self.span_id.map(|id| id.to_string());
            let latency = &self.latency;
            tracing::info!(
                target: "cc_lb_engine::lifecycle",
                request_id = %self.request_id,
                trace_id = trace_id.as_deref().unwrap_or(""),
                span_id = span_id.as_deref().unwrap_or(""),
                status = latency.status,
                stream_first_chunk_ms = ?latency.stream_first_chunk_ms,
                stream_message_start_ms = ?latency.stream_message_start_ms,
                stream_content_block_start_ms = ?latency.stream_content_block_start_ms,
                stream_first_content_delta_ms = ?latency.stream_first_content_delta_ms,
                stream_last_content_delta_ms = ?latency.stream_last_content_delta_ms,
                stream_message_stop_ms = ?latency.stream_message_stop_ms,
                stream_last_chunk_ms = ?latency.stream_last_chunk_ms,
                stream_total_ms = latency.stream_total_ms,
                sse_event_count = latency.sse_event_count,
                content_delta_count = latency.content_delta_count,
                ping_count = latency.ping_count,
                inter_token_avg_ms = ?latency.inter_token_avg_ms,
                total_bytes = latency.total_bytes,
                "stream latency breakdown"
            );
        });
    }
}

pub(crate) struct StreamLatency {
    pub(crate) status: u16,
    pub(crate) stream_first_chunk_ms: Option<u64>,
    pub(crate) stream_message_start_ms: Option<u64>,
    pub(crate) stream_content_block_start_ms: Option<u64>,
    pub(crate) stream_first_content_delta_ms: Option<u64>,
    pub(crate) stream_last_content_delta_ms: Option<u64>,
    pub(crate) stream_message_stop_ms: Option<u64>,
    pub(crate) stream_last_chunk_ms: Option<u64>,
    pub(crate) stream_total_ms: u64,
    pub(crate) sse_event_count: u64,
    pub(crate) content_delta_count: u64,
    pub(crate) ping_count: u64,
    pub(crate) inter_token_avg_ms: Option<u64>,
    pub(crate) total_bytes: u64,
}

impl StreamLatency {
    pub(crate) fn record_on_span(&self, span: &Span) {
        record_optional(span, "stream_first_chunk_ms", self.stream_first_chunk_ms);
        record_optional(
            span,
            "stream_message_start_ms",
            self.stream_message_start_ms,
        );
        record_optional(
            span,
            "stream_content_block_start_ms",
            self.stream_content_block_start_ms,
        );
        record_optional(
            span,
            "stream_first_content_delta_ms",
            self.stream_first_content_delta_ms,
        );
        record_optional(
            span,
            "stream_last_content_delta_ms",
            self.stream_last_content_delta_ms,
        );
        record_optional(span, "stream_message_stop_ms", self.stream_message_stop_ms);
        record_optional(span, "stream_last_chunk_ms", self.stream_last_chunk_ms);
        span.record("stream_total_ms", self.stream_total_ms);
        span.record("sse_event_count", self.sse_event_count);
        span.record("content_delta_count", self.content_delta_count);
        span.record("ping_count", self.ping_count);
        record_optional(span, "inter_token_avg_ms", self.inter_token_avg_ms);
        span.record("total_bytes", self.total_bytes);
    }
}

fn record_optional(span: &Span, field: &'static str, value: Option<u64>) {
    if let Some(value) = value {
        span.record(field, value);
    }
}

fn record_observation_drop(reason: &'static str, amount: u64) {
    cc_lb_observability::increment_dropped_events_by(reason, amount);
}

// These catches protect unwind builds. They cannot intercept the workspace release profile's
// panic=abort behavior.
fn completion_observer_loop(
    mut receiver: mpsc::Receiver<Option<StreamCompletionLog>>,
    completion: watch::Sender<bool>,
    record_drop: DropRecorder,
) {
    if catch_unwind(AssertUnwindSafe(|| {
        while let Some(message) = receiver.blocking_recv() {
            match message {
                Some(log) => process_log(&log, record_drop),
                None => {
                    receiver.close();
                    while let Some(message) = receiver.blocking_recv() {
                        if let Some(log) = message {
                            process_log(&log, record_drop);
                        }
                    }
                    break;
                }
            }
        }
    }))
    .is_err()
    {
        record_drop(WORKER_PANIC_REASON, 1);
    }
    let _ = completion.send(true);
}

fn process_log(log: &StreamCompletionLog, record_drop: DropRecorder) {
    if catch_unwind(AssertUnwindSafe(|| log.emit())).is_err() {
        record_drop(JOB_PANIC_REASON, 1);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Condvar, Mutex as StdMutex, mpsc as std_mpsc};
    use std::time::Duration;

    use http::StatusCode;

    use super::*;

    static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    static TEST_DROPS: StdMutex<Vec<&'static str>> = StdMutex::new(Vec::new());

    fn test_record_drop(reason: &'static str, amount: u64) {
        let mut drops = TEST_DROPS.lock().expect("test drop lock");
        for _ in 0..amount {
            drops.push(reason);
        }
    }

    type Release = Arc<(StdMutex<bool>, Condvar)>;

    /// Records the `request_id` of every emitted latency line and can block or panic inside
    /// the worker's log emission.
    #[derive(Default)]
    struct LogProbe {
        gate: Option<(std_mpsc::SyncSender<()>, Release)>,
        panic_first: AtomicBool,
        request_ids: StdMutex<Vec<String>>,
    }

    impl LogProbe {
        fn blocking(entered: std_mpsc::SyncSender<()>, released: Release) -> Arc<Self> {
            Arc::new(Self {
                gate: Some((entered, released)),
                ..Self::default()
            })
        }

        fn request_ids(&self) -> Vec<String> {
            self.request_ids.lock().expect("request id lock").clone()
        }
    }

    #[derive(Default)]
    struct RequestIdVisitor(Option<String>);

    impl tracing::field::Visit for RequestIdVisitor {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "request_id" {
                self.0 = Some(format!("{value:?}"));
            }
        }
    }

    struct ProbeSubscriber(Arc<LogProbe>);

    impl tracing::Subscriber for ProbeSubscriber {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

        fn event(&self, event: &tracing::Event<'_>) {
            let probe = &self.0;
            if probe.panic_first.swap(false, Ordering::SeqCst) {
                panic!("intentional completion log panic");
            }
            let mut visitor = RequestIdVisitor::default();
            event.record(&mut visitor);
            if let Some(request_id) = visitor.0 {
                probe
                    .request_ids
                    .lock()
                    .expect("request id lock")
                    .push(request_id);
            }
            if let Some((entered, released)) = &probe.gate {
                let _ = entered.send(());
                let (released, wake) = &**released;
                let mut released = released.lock().expect("release lock");
                while !*released {
                    released = wake.wait(released).expect("release wait");
                }
            }
        }

        fn enter(&self, _: &tracing::span::Id) {}

        fn exit(&self, _: &tracing::span::Id) {}
    }

    fn log(request_id: &str, probe: &Arc<LogProbe>) -> StreamCompletionLog {
        StreamCompletionLog::new(
            request_id.to_owned(),
            &Span::none(),
            StreamLatency {
                status: StatusCode::OK.as_u16(),
                stream_first_chunk_ms: Some(1),
                stream_message_start_ms: Some(2),
                stream_content_block_start_ms: Some(3),
                stream_first_content_delta_ms: Some(4),
                stream_last_content_delta_ms: Some(5),
                stream_message_stop_ms: Some(6),
                stream_last_chunk_ms: Some(7),
                stream_total_ms: 8,
                sse_event_count: 9,
                content_delta_count: 10,
                ping_count: 11,
                inter_token_avg_ms: Some(12),
                total_bytes: 13,
            },
            Dispatch::new(ProbeSubscriber(Arc::clone(probe))),
        )
    }

    fn release(released: &Arc<(StdMutex<bool>, Condvar)>) {
        let (release_lock, wake) = &**released;
        *release_lock.lock().expect("release lock") = true;
        wake.notify_all();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn queue_full_and_closed_are_counted_and_never_emit_dropped_logs() {
        let _serial = SERIAL.lock().await;
        TEST_DROPS.lock().expect("test drop lock").clear();
        let (entered_tx, entered_rx) = std_mpsc::sync_channel(2);
        let released = Arc::new((StdMutex::new(false), Condvar::new()));
        let probe = LogProbe::blocking(entered_tx, Arc::clone(&released));
        let observer = CompletionObserver::with_capacity_and_recorder(1, test_record_drop);

        assert_eq!(observer.enqueue(log("req-1", &probe)), Ok(()));
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("worker entered first log");
        assert_eq!(observer.enqueue(log("req-2", &probe)), Ok(()));
        assert_eq!(
            observer.enqueue(log("req-3", &probe)),
            Err(CompletionObservationDrop::Full)
        );

        release(&released);
        observer.shutdown().await;
        assert_eq!(
            observer.enqueue(log("req-4", &probe)),
            Err(CompletionObservationDrop::Closed)
        );

        assert_eq!(probe.request_ids(), ["req-1", "req-2"]);
        assert_eq!(
            TEST_DROPS.lock().expect("test drop lock").as_slice(),
            &[QUEUE_FULL_REASON, QUEUE_CLOSED_REASON]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_drains_accepted_payloads_exactly_once() {
        let _serial = SERIAL.lock().await;
        TEST_DROPS.lock().expect("test drop lock").clear();
        let probe = Arc::new(LogProbe::default());
        let observer = CompletionObserver::with_capacity_and_recorder(4, test_record_drop);

        for request_id in ["req-1", "req-2", "req-3"] {
            assert_eq!(observer.enqueue(log(request_id, &probe)), Ok(()));
        }
        observer.shutdown().await;

        assert_eq!(probe.request_ids(), ["req-1", "req-2", "req-3"]);
        assert!(TEST_DROPS.lock().expect("test drop lock").is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_drains_observation_from_sender_loaded_before_close() {
        let _serial = SERIAL.lock().await;
        TEST_DROPS.lock().expect("test drop lock").clear();
        let (entered_tx, entered_rx) = std_mpsc::sync_channel(2);
        let released = Arc::new((StdMutex::new(false), Condvar::new()));
        let probe = LogProbe::blocking(entered_tx, Arc::clone(&released));
        let observer = CompletionObserver::with_capacity_and_recorder(2, test_record_drop);

        assert_eq!(observer.enqueue(log("req-1", &probe)), Ok(()));
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("worker entered first log");
        let preloaded_sender = observer.sender.load_full().expect("sender is open");
        let shutdown = observer.shutdown();
        tokio::pin!(shutdown);
        tokio::select! {
            biased;
            _ = &mut shutdown => panic!("shutdown completed while log emission remained blocked"),
            _ = tokio::task::yield_now() => {}
        }
        assert!(
            preloaded_sender
                .try_send(Some(log("req-2", &probe)))
                .is_ok(),
            "preloaded sender queues the racing observation behind shutdown"
        );

        release(&released);
        shutdown.await;

        assert_eq!(probe.request_ids(), ["req-1", "req-2"]);
        assert!(TEST_DROPS.lock().expect("test drop lock").is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn concurrent_shutdown_callers_wait_for_shared_completion() {
        let _serial = SERIAL.lock().await;
        TEST_DROPS.lock().expect("test drop lock").clear();
        let (entered_tx, entered_rx) = std_mpsc::sync_channel(1);
        let released = Arc::new((StdMutex::new(false), Condvar::new()));
        let probe = LogProbe::blocking(entered_tx, Arc::clone(&released));
        let observer = CompletionObserver::with_capacity_and_recorder(1, test_record_drop);

        assert_eq!(observer.enqueue(log("req-1", &probe)), Ok(()));
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("worker entered log");

        let first_shutdown = observer.shutdown();
        tokio::pin!(first_shutdown);
        tokio::select! {
            biased;
            _ = &mut first_shutdown => panic!("first shutdown completed while log emission remained blocked"),
            _ = tokio::task::yield_now() => {}
        }
        let second_shutdown = observer.shutdown();
        tokio::pin!(second_shutdown);
        tokio::select! {
            biased;
            _ = &mut second_shutdown => panic!("second shutdown returned before shared completion"),
            _ = tokio::task::yield_now() => {}
        }

        release(&released);
        first_shutdown.await;
        second_shutdown.await;

        assert_eq!(probe.request_ids(), ["req-1"]);
        assert!(TEST_DROPS.lock().expect("test drop lock").is_empty());
    }

    #[cfg(panic = "unwind")]
    #[tokio::test(flavor = "current_thread")]
    async fn log_panic_is_counted_and_worker_processes_following_job() {
        let _serial = SERIAL.lock().await;
        TEST_DROPS.lock().expect("test drop lock").clear();
        let probe = Arc::new(LogProbe {
            panic_first: AtomicBool::new(true),
            ..LogProbe::default()
        });
        let observer = CompletionObserver::with_capacity_and_recorder(2, test_record_drop);

        assert_eq!(observer.enqueue(log("req-1", &probe)), Ok(()));
        assert_eq!(observer.enqueue(log("req-2", &probe)), Ok(()));
        observer.shutdown().await;

        assert_eq!(probe.request_ids(), ["req-2"]);
        assert_eq!(
            TEST_DROPS.lock().expect("test drop lock").as_slice(),
            &[JOB_PANIC_REASON]
        );
    }
}
