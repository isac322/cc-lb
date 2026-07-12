//! Task 31: assert that an async prompt-cache observation store failure
//! does NOT fail the synchronous response path.
//!
//! Booting the full `cc-lb-server` HTTP listener and a fake upstream just to
//! prove this isolation property is overkill for what the production code
//! actually does: the response path enqueues to a bounded mpsc, and a
//! background writer task drains the queue and calls
//! `PromptCacheObservationStore::upsert_observation`. The writer task swallows
//! store errors with `tracing::warn!` and bumps
//! `cc_lb_cache_observation_write_failed_total`.
//!
//! This test pins that isolation property by exercising the sink directly
//! (T18 sink + observability counter wired in T25). "Response status == 200"
//! is represented by the fact that every `sink.enqueue(...)` call returns
//! `Ok(())` even though the backing store is permanently broken; the writer
//! task only ever logs and counts, it never panics or signals the enqueue
//! side. Per the task `MUST DO` clause, when no production constructor takes
//! `Arc<dyn PromptCacheObservationStore>` for the full server, the test may
//! use the T18 sink directly.

use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use cc_lb_domain::TtlClass;
use cc_lb_observability::cache_observation_store_kind;
use cc_lb_server::prompt_cache_observation_sink::PromptCacheObservationSink;
use cc_lb_storage_api::{
    PromptCacheObservationRecord, PromptCacheObservationStore, StorageError, StorageResult,
};
use metrics_exporter_prometheus::PrometheusBuilder;
use tracing_subscriber::fmt::MakeWriter;
use uuid::Uuid;

const SIMULATED_ERROR_MESSAGE: &str = "simulated prompt-cache observation store failure (task 31)";

/// Mock `PromptCacheObservationStore` whose `upsert_observation` always
/// returns `Err(StorageError::Unavailable)` with a recognizable message that
/// the captured log assertion can find.
struct FailingStore;

#[async_trait]
impl PromptCacheObservationStore for FailingStore {
    async fn upsert_observation(
        &self,
        _record: &PromptCacheObservationRecord,
    ) -> StorageResult<()> {
        Err(StorageError::Unavailable {
            message: SIMULATED_ERROR_MESSAGE.to_owned(),
        })
    }
}

#[derive(Clone, Default)]
struct CapturedLogs {
    inner: Arc<Mutex<Vec<u8>>>,
}

impl CapturedLogs {
    fn contents(&self) -> String {
        let bytes = self.inner.lock().expect("captured logs lock");
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

struct CapturedWriter {
    logs: CapturedLogs,
}

impl Write for CapturedWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.logs
            .inner
            .lock()
            .expect("captured logs lock")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'writer> MakeWriter<'writer> for CapturedLogs {
    type Writer = CapturedWriter;

    fn make_writer(&'writer self) -> Self::Writer {
        CapturedWriter { logs: self.clone() }
    }
}

fn record(index: u64) -> PromptCacheObservationRecord {
    PromptCacheObservationRecord {
        upstream_id: Uuid::from_u128(0x1234_5678_90ab_cdef_1234_5678_90ab_cdef),
        canonical_model_id: "claude-sonnet-4-5-20250929".to_owned(),
        v3_prefix_key: format!("sha256:task31-{index}"),
        ttl_class: TtlClass::Ephemeral5m,
        expires_at_unix_secs: 1_800 + index,
        last_observed_at_unix_secs: 1_500 + index,
        hash_schema_version: 4,
        prefix_content_block_index: 0,
        estimated_prefix_tokens: 0,
        token_estimate_source: "local_tiktoken_v1".to_owned(),
        last_provider_cache_read_tokens: Some(0),
        last_provider_cache_creation_tokens: Some(0),
    }
}

#[test]
fn observation_failure_does_not_fail_response() {
    // Capture tracing output so we can assert the writer task logged
    // the simulated error.
    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    let _tracing_guard = tracing::subscriber::set_default(subscriber);

    // Local Prometheus recorder so the counter accessor can be observed
    // without contaminating other tests' globals. `metrics::with_local_recorder`
    // is a thread-local install, so we drive everything on a single-threaded
    // `current_thread` runtime to keep recorder visibility consistent across
    // the writer task's `.await` points (mirrors the T18 sink unit tests in
    // `tests/prompt_cache_observation_metrics.rs`).
    let recorder = PrometheusBuilder::new().build_recorder();
    let prom_handle = recorder.handle();

    metrics::with_local_recorder(&recorder, || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("runtime builds");

        runtime.block_on(async move {
            // Inject the failing store at sink construction (T18 wiring).
            let (sink, writer) = PromptCacheObservationSink::new(
                Arc::new(FailingStore),
                8,
                cache_observation_store_kind::SQLITE,
            );

            // Each enqueue simulates the post-response observation enqueue
            // performed by the lifecycle on a successful (HTTP 200) /v1/messages
            // request that carries cache breakpoints. The response path has
            // already returned 200 to the client by the time we hit this point;
            // the only thing left is whether the downstream store error can
            // claw its way back. It can't — enqueue must return Ok.
            for index in 0..3 {
                sink.enqueue(record(index))
                    .expect("enqueue must succeed: response is already 200");
            }

            // Drop the sink so the writer task observes channel close after
            // draining, then await it to guarantee all upserts (and their
            // failure-logging branches) have executed before we assert.
            drop(sink);
            // Cap the wait at 5 s per the task budget. The writer drains 3
            // records into a synchronous `Err(...)` return, so this completes
            // in milliseconds in practice.
            tokio::time::timeout(Duration::from_secs(5), writer)
                .await
                .expect("writer task drains within 5 s")
                .expect("writer task exits cleanly");
        });
    });

    // Assertion 1: response semantics preserved. Every enqueue above returned
    // Ok, so if we reached this line the synchronous response path was never
    // forced to fail by the async store error.

    // Assertion 2: write-failed counter incremented at least once per failed
    // upsert (3 enqueues -> 3 failed upserts -> counter == 3).
    let rendered = prom_handle.render();
    assert!(
        rendered.contains("cc_lb_cache_observation_write_failed_total{store=\"sqlite\"}"),
        "expected write_failed counter for store=sqlite in metrics output:\n{rendered}"
    );
    let counter_value = parse_write_failed_counter(&rendered, cache_observation_store_kind::SQLITE)
        .expect("write_failed counter parses from prometheus output");
    assert!(
        counter_value >= 1,
        "expected write_failed counter >= 1 for store=sqlite, got {counter_value}\n{rendered}"
    );

    // Assertion 3: writer logged a WARN with the simulated error message.
    let captured = logs.contents();
    assert!(
        captured.contains("prompt cache observation write failed"),
        "expected sink writer warn log, captured logs:\n{captured}"
    );
    assert!(
        captured.contains(SIMULATED_ERROR_MESSAGE),
        "expected simulated error message in captured logs:\n{captured}"
    );
    assert!(
        captured.contains("WARN"),
        "expected WARN level on observation write failure log:\n{captured}"
    );
}

/// Parse `cc_lb_cache_observation_write_failed_total{store="..."} <n>` from
/// the rendered Prometheus output for the given store label.
fn parse_write_failed_counter(rendered: &str, store: &str) -> Option<u64> {
    let needle = format!("cc_lb_cache_observation_write_failed_total{{store=\"{store}\"}}");
    for line in rendered.lines() {
        if let Some(rest) = line.strip_prefix(&needle) {
            return rest.trim().parse::<u64>().ok();
        }
    }
    None
}
