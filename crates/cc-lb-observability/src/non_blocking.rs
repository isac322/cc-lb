use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use tokio::sync::mpsc;
use tracing::Metadata;
use tracing_subscriber::fmt::MakeWriter;

use crate::redaction::{RedactingWriter, RedactionPolicy};

const LOG_CHANNEL_CAPACITY: usize = 4096;
const LOG_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const INITIAL_EVENT_BUFFER_BYTES: usize = 256;
const LOG_DROPPED_EVENT_REASON: &str = "log_writer_queue_full";

#[derive(Debug)]
enum LogMessage {
    Bytes(Vec<u8>),
    Shutdown,
}

/// Non-blocking tracing `MakeWriter` that buffers one formatted log line on the
/// caller thread and hands it off over a lock-free bounded channel to a dedicated
/// background OS thread (which optionally redacts before writing atomically).
#[derive(Clone, Debug)]
pub(crate) struct NonBlockingMakeWriter {
    sender: mpsc::Sender<LogMessage>,
    dropped: Arc<AtomicU64>,
    dropped_counter: metrics::Counter,
}

impl NonBlockingMakeWriter {
    #[cfg(test)]
    pub(crate) fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

pub(crate) struct NonBlockingWriter<'writer> {
    sender: &'writer mpsc::Sender<LogMessage>,
    dropped: &'writer AtomicU64,
    dropped_counter: &'writer metrics::Counter,
    buffer: Vec<u8>,
}

impl<'writer> MakeWriter<'writer> for NonBlockingMakeWriter {
    type Writer = NonBlockingWriter<'writer>;

    fn make_writer(&'writer self) -> Self::Writer {
        NonBlockingWriter {
            sender: &self.sender,
            dropped: &self.dropped,
            dropped_counter: &self.dropped_counter,
            buffer: Vec::with_capacity(INITIAL_EVENT_BUFFER_BYTES),
        }
    }

    fn make_writer_for(&'writer self, _meta: &Metadata<'_>) -> Self::Writer {
        self.make_writer()
    }
}

impl NonBlockingWriter<'_> {
    fn flush_line_buffer(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        let bytes = std::mem::take(&mut self.buffer);
        match self.sender.try_send(LogMessage::Bytes(bytes)) {
            Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                // Bump the atomic and increment the pre-registered Counter
                // handle without locking any metrics registry or blocking on stdout.
                self.dropped.fetch_add(1, Ordering::Relaxed);
                self.dropped_counter.increment(1);
            }
        }
    }
}

impl Write for NonBlockingWriter<'_> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_line_buffer();
        Ok(())
    }
}

impl Drop for NonBlockingWriter<'_> {
    fn drop(&mut self) {
        self.flush_line_buffer();
    }
}

/// Writes each complete log line buffer in one `stdout().lock().write_all(...)`
/// syscall batch so lines cannot interleave mid-line with stdout diagnostics.
struct LineAtomicStdout;

impl Write for LineAtomicStdout {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut out = io::stdout().lock();
        out.write_all(buf)?;
        Ok(buf.len())
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        let mut out = io::stdout().lock();
        out.write_all(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        io::stdout().lock().flush()
    }
}

#[derive(Debug)]
pub(crate) struct LogWorkerGuard {
    sender: mpsc::Sender<LogMessage>,
    shutdown_requested: Arc<AtomicBool>,
    done: Arc<(Mutex<bool>, Condvar)>,
    join_handle: Option<JoinHandle<()>>,
    shutdown_timeout: Duration,
}

impl LogWorkerGuard {
    pub(crate) fn shutdown(&mut self) {
        let Some(handle) = self.join_handle.take() else {
            return;
        };
        // Signal via atomic + non-blocking try_send so dropping from inside a
        // Tokio runtime never calls blocking_send and a full queue still observes
        // `shutdown_requested` on the very next dequeued message.
        self.shutdown_requested.store(true, Ordering::Release);
        let _ = self.sender.try_send(LogMessage::Shutdown);

        let (lock, cvar) = &*self.done;
        let guard = lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (guard, _) = cvar
            .wait_timeout_while(guard, self.shutdown_timeout, |finished| !*finished)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *guard {
            let _ = handle.join();
        }
    }
}

impl Drop for LogWorkerGuard {
    fn drop(&mut self) {
        self.shutdown();
    }
}

pub(crate) fn spawn_stdout_log_worker(
    redaction_policy: Option<RedactionPolicy>,
) -> (NonBlockingMakeWriter, LogWorkerGuard) {
    match redaction_policy {
        Some(policy) => spawn_log_worker(
            RedactingWriter::new(LineAtomicStdout, policy),
            LOG_CHANNEL_CAPACITY,
            LOG_SHUTDOWN_TIMEOUT,
        ),
        None => spawn_log_worker(LineAtomicStdout, LOG_CHANNEL_CAPACITY, LOG_SHUTDOWN_TIMEOUT),
    }
}

pub(crate) fn spawn_log_worker<W>(
    mut writer: W,
    capacity: usize,
    shutdown_timeout: Duration,
) -> (NonBlockingMakeWriter, LogWorkerGuard)
where
    W: Write + Send + 'static,
{
    let (tx, mut rx) = mpsc::channel(capacity.max(1));
    let dropped = Arc::new(AtomicU64::new(0));
    let dropped_counter =
        metrics::counter!("cc_lb_dropped_events_total", "reason" => LOG_DROPPED_EVENT_REASON);
    let shutdown_requested = Arc::new(AtomicBool::new(false));
    let done = Arc::new((Mutex::new(false), Condvar::new()));

    let worker_shutdown = Arc::clone(&shutdown_requested);
    let worker_done = Arc::clone(&done);
    let join_handle = std::thread::Builder::new()
        .name("cc-lb-log-writer".to_owned())
        .spawn(move || {
            while let Some(msg) = rx.blocking_recv() {
                match msg {
                    LogMessage::Bytes(bytes) => {
                        let _ = writer.write_all(&bytes);
                        let _ = writer.flush();
                        if worker_shutdown.load(Ordering::Acquire) {
                            drain_remaining(&mut rx, &mut writer);
                            break;
                        }
                    }
                    LogMessage::Shutdown => {
                        drain_remaining(&mut rx, &mut writer);
                        break;
                    }
                }
            }
            let (lock, cvar) = &*worker_done;
            *lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
            cvar.notify_all();
        })
        .expect("spawn non-blocking log writer thread");

    let make_writer = NonBlockingMakeWriter {
        sender: tx.clone(),
        dropped,
        dropped_counter,
    };
    let guard = LogWorkerGuard {
        sender: tx,
        shutdown_requested,
        done,
        join_handle: Some(join_handle),
        shutdown_timeout,
    };
    (make_writer, guard)
}

fn drain_remaining<W: Write>(rx: &mut mpsc::Receiver<LogMessage>, writer: &mut W) {
    rx.close();
    while let Ok(rem) = rx.try_recv() {
        if let LogMessage::Bytes(bytes) = rem {
            let _ = writer.write_all(&bytes);
            let _ = writer.flush();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::{Duration, Instant};

    use tracing_subscriber::fmt::MakeWriter;

    use super::*;
    use crate::TracingGuard;
    use crate::redaction::{REDACTED, RedactingWriter, RedactionPolicy};

    #[derive(Clone, Default)]
    struct SharedBuf(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedBuf {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct BlockedWriter {
        started: Arc<(Mutex<bool>, Condvar)>,
        release: Arc<(Mutex<bool>, Condvar)>,
        written: Arc<Mutex<Vec<u8>>>,
    }

    impl Write for BlockedWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            {
                let (lock, cvar) = &*self.started;
                *lock
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
                cvar.notify_all();
            }
            {
                let (lock, cvar) = &*self.release;
                let guard = lock
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let _guard = cvar
                    .wait_while(guard, |released| !*released)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
            self.written
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn async_redaction_flushes_and_redacts_in_order_on_shutdown() {
        let captured = SharedBuf::default();
        let writer = RedactingWriter::new(captured.clone(), RedactionPolicy::new(false));
        let (make_writer, mut guard) = spawn_log_worker(writer, 64, Duration::from_secs(2));

        {
            let mut w = make_writer.make_writer();
            writeln!(w, "first authorization=Bearer secret-token-123").unwrap();
        }
        {
            let mut w = make_writer.make_writer();
            writeln!(w, "second key=sk-ant-api03-abcdefghijklmnop").unwrap();
        }

        guard.shutdown();

        let output = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
        assert!(!output.contains("secret-token-123"), "{output}");
        assert!(
            !output.contains("sk-ant-api03-abcdefghijklmnop"),
            "{output}"
        );
        assert!(output.contains(REDACTED), "{output}");
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), 2, "{output}");
        assert!(lines[0].starts_with("first "), "{output}");
        assert!(lines[1].starts_with("second "), "{output}");
    }

    #[test]
    fn queue_full_drops_non_blockingly_without_stalling_caller() {
        let started = Arc::new((Mutex::new(false), Condvar::new()));
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let written = Arc::new(Mutex::new(Vec::new()));
        let writer = BlockedWriter {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
            written: Arc::clone(&written),
        };
        let (make_writer, mut guard) = spawn_log_worker(writer, 4, Duration::from_secs(2));

        // First message blocks the worker thread on `release`.
        {
            let mut w = make_writer.make_writer();
            writeln!(w, "first").unwrap();
        }
        {
            let (lock, cvar) = &*started;
            let guard = lock.lock().unwrap();
            let _guard = cvar.wait_while(guard, |s| !*s).unwrap();
        }

        // Fill capacity (4) + 5 overflow items; caller must never block.
        let send_started = Instant::now();
        for idx in 0..9 {
            let mut w = make_writer.make_writer();
            writeln!(w, "msg-{idx}").unwrap();
        }
        assert!(
            send_started.elapsed() < Duration::from_millis(200),
            "try_send stalled on full log channel"
        );
        assert!(
            make_writer.dropped_count() >= 5,
            "expected >= 5 dropped events, got {}",
            make_writer.dropped_count()
        );

        {
            let (lock, cvar) = &*release;
            *lock.lock().unwrap() = true;
            cvar.notify_all();
        }
        guard.shutdown();
    }

    #[test]
    fn non_blocking_writer_flush_exports_dropped_deltas_on_worker_not_caller() {
        let started = Arc::new((Mutex::new(false), Condvar::new()));
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let written = Arc::new(Mutex::new(Vec::new()));
        let writer = BlockedWriter {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
            written,
        };
        let (make_writer, mut guard) = spawn_log_worker(writer, 2, Duration::from_secs(2));

        {
            let mut w = make_writer.make_writer();
            writeln!(w, "first").unwrap();
        }
        {
            let (lock, cvar) = &*started;
            let guard = lock.lock().unwrap();
            let _guard = cvar.wait_while(guard, |s| !*s).unwrap();
        }
        for idx in 0..5 {
            let mut w = make_writer.make_writer();
            writeln!(w, "overflow-{idx}").unwrap();
        }
        assert!(make_writer.dropped_count() >= 3);

        {
            let (lock, cvar) = &*release;
            *lock.lock().unwrap() = true;
            cvar.notify_all();
        }
        guard.shutdown();
    }

    #[test]
    fn tracing_guard_drop_with_log_worker_inside_tokio_runtime_does_not_panic() {
        let captured = SharedBuf::default();
        let (make_writer, log_worker) =
            spawn_log_worker(captured.clone(), 16, Duration::from_secs(2));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current_thread runtime");

        runtime.block_on(async {
            {
                let mut w = make_writer.make_writer();
                writeln!(w, "inside tokio runtime").unwrap();
            }
            drop(TracingGuard::for_log_worker_test(log_worker));
        });

        let output = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
        assert_eq!(output, "inside tokio runtime\n");
    }

    #[test]
    fn stalled_writer_respects_bounded_shutdown_timeout() {
        let started = Arc::new((Mutex::new(false), Condvar::new()));
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let written = Arc::new(Mutex::new(Vec::new()));
        let writer = BlockedWriter {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
            written,
        };
        let (make_writer, mut guard) = spawn_log_worker(writer, 4, Duration::from_millis(50));

        {
            let mut w = make_writer.make_writer();
            writeln!(w, "stall").unwrap();
        }
        {
            let (lock, cvar) = &*started;
            let guard = lock.lock().unwrap();
            let _guard = cvar.wait_while(guard, |s| !*s).unwrap();
        }

        let stop_started = Instant::now();
        guard.shutdown();
        let elapsed = stop_started.elapsed();
        assert!(
            elapsed < Duration::from_secs(1),
            "shutdown should detach stalled writer near 50ms, took {elapsed:?}"
        );

        // Unblock thread cleanly so test process exits promptly.
        let (lock, cvar) = &*release;
        *lock.lock().unwrap() = true;
        cvar.notify_all();
    }

    #[test]
    fn dropped_events_metric_increments_without_waiting_on_stalled_writer() {
        use metrics::{
            Counter, CounterFn, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString,
            Unit,
        };

        #[derive(Default)]
        struct AtomicCounterHandle(AtomicU64);
        impl CounterFn for AtomicCounterHandle {
            fn increment(&self, value: u64) {
                self.0.fetch_add(value, Ordering::Relaxed);
            }

            fn absolute(&self, value: u64) {
                self.0.store(value, Ordering::Relaxed);
            }
        }

        #[derive(Clone, Default)]
        struct DroppedRecorder {
            registered_counters: Arc<AtomicU64>,
            log_dropped_handle: Arc<AtomicCounterHandle>,
        }

        impl Recorder for DroppedRecorder {
            fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
            fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
            fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

            fn register_counter(&self, key: &Key, _meta: &Metadata<'_>) -> Counter {
                self.registered_counters.fetch_add(1, Ordering::Relaxed);
                if key.name() == "cc_lb_dropped_events_total"
                    && key.labels().any(|label| {
                        label.key() == "reason" && label.value() == LOG_DROPPED_EVENT_REASON
                    })
                {
                    Counter::from_arc(Arc::clone(&self.log_dropped_handle))
                } else {
                    Counter::noop()
                }
            }

            fn register_gauge(&self, _key: &Key, _meta: &Metadata<'_>) -> Gauge {
                Gauge::noop()
            }

            fn register_histogram(&self, _key: &Key, _meta: &Metadata<'_>) -> Histogram {
                Histogram::noop()
            }
        }

        let started = Arc::new((Mutex::new(false), Condvar::new()));
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let written = Arc::new(Mutex::new(Vec::new()));
        let writer = BlockedWriter {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
            written,
        };
        let recorder = DroppedRecorder::default();

        let (make_writer, mut guard) = metrics::with_local_recorder(&recorder, || {
            spawn_log_worker(writer, 1, Duration::from_millis(50))
        });
        assert_eq!(
            recorder.registered_counters.load(Ordering::Relaxed),
            1,
            "counter must be resolved once at worker initialization"
        );

        {
            let mut w = make_writer.make_writer();
            writeln!(w, "blocks-stdout").unwrap();
        }
        {
            let (lock, cvar) = &*started;
            let lock_guard = lock.lock().unwrap();
            let _lock_guard = cvar.wait_while(lock_guard, |s| !*s).unwrap();
        }

        // Fill capacity (1) + 3 overflow messages while stdout remains blocked.
        for idx in 0..4 {
            let mut w = make_writer.make_writer();
            writeln!(w, "overflow-{idx}").unwrap();
        }
        // Assert metric exported immediately BEFORE stdout unblocks and without
        // waiting on shutdown or worker drain.
        let drops_while_stalled = recorder.log_dropped_handle.0.load(Ordering::Relaxed);
        assert!(
            drops_while_stalled >= 3,
            "expected >= 3 exported dropped events while stdout is blocked, got {drops_while_stalled}"
        );
        assert_eq!(
            drops_while_stalled,
            make_writer.dropped_count(),
            "exported counter must match atomic drop count without waiting on worker"
        );
        assert_eq!(
            recorder.registered_counters.load(Ordering::Relaxed),
            1,
            "full-queue drops must reuse pre-registered handle without re-registering"
        );

        // Detach stalled worker via bounded shutdown; exported drops remain exact.
        guard.shutdown();
        assert_eq!(
            recorder.log_dropped_handle.0.load(Ordering::Relaxed),
            drops_while_stalled
        );

        let (lock, cvar) = &*release;
        *lock.lock().unwrap() = true;
        cvar.notify_all();
    }
}
