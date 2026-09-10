use std::future;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use async_trait::async_trait;
use cc_lb_domain::TtlClass;
use cc_lb_engine::lifecycle::HASH_SCHEMA_VERSION;
use cc_lb_observability::cache_observation_store_kind;
use cc_lb_server::prompt_cache_observation_sink::{EnqueueError, PromptCacheObservationSink};
use cc_lb_storage_api::{
    PromptCacheObservationRecord, PromptCacheObservationStore, StorageError, StorageResult,
};
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::runtime::Builder;
use uuid::Uuid;

struct BlockingStore {
    started: Arc<AtomicU64>,
}

#[async_trait]
impl PromptCacheObservationStore for BlockingStore {
    async fn upsert_observation(
        &self,
        _record: &PromptCacheObservationRecord,
    ) -> StorageResult<()> {
        self.started.fetch_add(1, Ordering::SeqCst);
        future::pending::<()>().await;
        Ok(())
    }
}

struct FailingStore;

#[async_trait]
impl PromptCacheObservationStore for FailingStore {
    async fn upsert_observation(
        &self,
        _record: &PromptCacheObservationRecord,
    ) -> StorageResult<()> {
        Err(StorageError::Unavailable {
            message: "synthetic prompt-cache observation write failure".to_owned(),
        })
    }
}

fn record(index: u64) -> PromptCacheObservationRecord {
    PromptCacheObservationRecord {
        upstream_id: Uuid::from_u128(0x1234_5678_90ab_cdef_1234_5678_90ab_cdef),
        canonical_model_id: "claude-sonnet-4-5-20250929".to_owned(),
        v3_prefix_key: format!("sha256:{index}"),
        ttl_class: TtlClass::Ephemeral5m,
        expires_at_unix_secs: 1_800 + index,
        last_observed_at_unix_secs: 1_500 + index,
        hash_schema_version: HASH_SCHEMA_VERSION,
        prefix_content_block_index: 0,
        estimated_prefix_tokens: 0,
        token_estimate_source: "local_tiktoken_v1".to_owned(),
    }
}

#[test]
fn queue_overflow_increments_prometheus_drop_counter() {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let runtime = Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("runtime builds");
    let started = Arc::new(AtomicU64::new(0));

    metrics::with_local_recorder(&recorder, || {
        runtime.block_on(async {
            let store = BlockingStore {
                started: Arc::clone(&started),
            };
            let (sink, writer) = PromptCacheObservationSink::new(
                Arc::new(store),
                1,
                cache_observation_store_kind::SQLITE,
            );
            sink.enqueue(record(0)).expect("first record enqueued");
            while started.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
            sink.enqueue(record(1)).expect("second record fills queue");
            assert_eq!(sink.enqueue(record(2)), Err(EnqueueError::ChannelFull));
            assert_eq!(sink.dropped_total(), 1);
            writer.abort();
            let _ = writer.await;
        });
    });

    let rendered = handle.render();
    assert!(
        rendered.contains("cc_lb_cache_observation_dropped_total{reason=\"queue_full\"} 1"),
        "rendered metrics:\n{rendered}"
    );
}

#[test]
fn store_error_increments_prometheus_write_failed_counter() {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let runtime = Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("runtime builds");

    metrics::with_local_recorder(&recorder, || {
        runtime.block_on(async {
            let (sink, writer) = PromptCacheObservationSink::new(
                Arc::new(FailingStore),
                2,
                cache_observation_store_kind::SQLITE,
            );
            sink.enqueue(record(0)).expect("record enqueued");
            drop(sink);
            writer.await.expect("writer exits");
        });
    });

    let rendered = handle.render();
    assert!(
        rendered.contains("cc_lb_cache_observation_write_failed_total{store=\"sqlite\"} 1"),
        "rendered metrics:\n{rendered}"
    );
}
