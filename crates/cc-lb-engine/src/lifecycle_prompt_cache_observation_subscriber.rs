//! Prompt-cache observation subscriber.
//!
//! Uses the single-event design: producers emit one
//! `PromptCacheObservationsProduced` event at non-stream finalization,
//! stream message_stop, or stream abort. This intentionally moves cache
//! upserts, sink enqueue, and drop metrics out of the request path while
//! accepting the small delay from stream message_start to message_stop.

use std::sync::Arc;

use cc_lb_config::LifecyclePromptCacheObservationSubscriberConfig;
use cc_lb_contract::{
    EngineMetricsHook, LifecycleEvent, PromptCacheObservationKindWire, PromptCacheObservationWire,
};
use cc_lb_observability::cache_observation_dropped_reason;
use cc_lb_storage_api::PromptCacheObservationRecord;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::lifecycle::{
    HASH_SCHEMA_VERSION, PromptCacheObservationCacheLike, PromptCacheObservationEnqueueError,
    PromptCacheObservationInput, PromptCacheObservationSinkLike,
};

pub struct PromptCacheObservationSubscriberHandle {
    shutdown_tx: oneshot::Sender<()>,
    join: JoinHandle<()>,
}

impl PromptCacheObservationSubscriberHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        if let Err(error) = self.join.await {
            tracing::warn!(%error, "lifecycle prompt cache observation subscriber task panicked");
        }
    }
}

pub fn spawn_lifecycle_prompt_cache_observation_subscriber(
    rx: mpsc::Receiver<LifecycleEvent>,
    config: LifecyclePromptCacheObservationSubscriberConfig,
    cache: Arc<dyn PromptCacheObservationCacheLike>,
    sink: Option<Arc<dyn PromptCacheObservationSinkLike>>,
    metrics: Arc<dyn EngineMetricsHook>,
) -> PromptCacheObservationSubscriberHandle {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let join = tokio::spawn(subscriber_loop(
        rx,
        config,
        cache,
        sink,
        metrics,
        shutdown_rx,
    ));
    PromptCacheObservationSubscriberHandle { shutdown_tx, join }
}

async fn subscriber_loop(
    mut rx: mpsc::Receiver<LifecycleEvent>,
    config: LifecyclePromptCacheObservationSubscriberConfig,
    cache: Arc<dyn PromptCacheObservationCacheLike>,
    sink: Option<Arc<dyn PromptCacheObservationSinkLike>>,
    metrics: Arc<dyn EngineMetricsHook>,
    mut shutdown: oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            biased;
                event = rx.recv() => {
                    match event {
                        Some(event) => handle_event(&config, cache.as_ref(), sink.as_deref(), metrics.as_ref(), event),
                        None => break,
                    }
                }
            _ = &mut shutdown => break,
        }
    }

    while let Ok(event) = rx.try_recv() {
        handle_event(
            &config,
            cache.as_ref(),
            sink.as_deref(),
            metrics.as_ref(),
            event,
        );
    }
}

fn handle_event(
    config: &LifecyclePromptCacheObservationSubscriberConfig,
    cache: &dyn PromptCacheObservationCacheLike,
    sink: Option<&dyn PromptCacheObservationSinkLike>,
    metrics: &dyn EngineMetricsHook,
    event: LifecycleEvent,
) {
    let LifecycleEvent::PromptCacheObservationsProduced {
        upstream_id,
        canonical_model_id,
        observations,
        dropped_below_threshold,
        dropped_aborted,
        ..
    } = event
    else {
        return;
    };
    if !config.enabled {
        return;
    }
    increment_drop_metric(
        metrics,
        cache_observation_dropped_reason::BELOW_THRESHOLD,
        dropped_below_threshold,
    );
    increment_drop_metric(
        metrics,
        cache_observation_dropped_reason::ABORT,
        dropped_aborted,
    );
    if observations.is_empty() {
        return;
    }
    let now_unix_secs = cache.clock_now_unix_secs();
    for observation in observations {
        cache.upsert_observation(PromptCacheObservationInput {
            upstream_id,
            canonical_model: canonical_model_id.clone(),
            prefix_hash: observation.prefix_hash.clone(),
            ttl_class: observation.ttl_class,
            expires_at_unix_secs: observation.expires_at_unix_secs,
            observed_at_unix_secs: now_unix_secs,
            prefix_content_block_index: observation.prefix_content_block_index,
            estimated_prefix_tokens: observation.estimated_prefix_tokens,
            token_estimate_source: observation
                .token_estimate_source
                .clone()
                .unwrap_or_else(|| "unknown".to_owned()),
        });
        enqueue_observation(
            sink,
            cache,
            upstream_id,
            &canonical_model_id,
            &observation,
            now_unix_secs,
        );
    }
}

fn enqueue_observation(
    sink: Option<&dyn PromptCacheObservationSinkLike>,
    cache: &dyn PromptCacheObservationCacheLike,
    upstream_id: Uuid,
    canonical_model_id: &str,
    observation: &PromptCacheObservationWire,
    now_unix_secs: u64,
) {
    let Some(sink) = sink else {
        return;
    };
    let refresh_should_persist = cache.refresh_on_hit(
        upstream_id,
        canonical_model_id,
        &observation.prefix_hash,
        observation.ttl_class,
        now_unix_secs,
    );
    let should_persist = match observation.kind {
        PromptCacheObservationKindWire::Hit => refresh_should_persist,
        PromptCacheObservationKindWire::Write => true,
    };
    if !should_persist {
        return;
    }
    let record = PromptCacheObservationRecord {
        upstream_id,
        canonical_model_id: canonical_model_id.to_owned(),
        v3_prefix_key: observation.prefix_hash.clone(),
        ttl_class: observation.ttl_class,
        expires_at_unix_secs: observation.expires_at_unix_secs,
        last_observed_at_unix_secs: now_unix_secs,
        hash_schema_version: HASH_SCHEMA_VERSION,
        prefix_content_block_index: observation.prefix_content_block_index,
        estimated_prefix_tokens: observation.estimated_prefix_tokens,
        token_estimate_source: observation
            .token_estimate_source
            .clone()
            .unwrap_or_else(|| "unknown".to_owned()),
        last_provider_cache_read_tokens: None,
        last_provider_cache_creation_tokens: None,
    };
    if let Err(error) = sink.enqueue(record) {
        match error {
            PromptCacheObservationEnqueueError::ChannelFull => {}
            PromptCacheObservationEnqueueError::ChannelClosed => {
                tracing::warn!("prompt cache observation sink is closed");
            }
        }
    }
}

fn increment_drop_metric(metrics: &dyn EngineMetricsHook, reason: &'static str, count: u32) {
    for _ in 0..count {
        metrics.record_cache_observation_dropped(reason);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use cc_lb_contract::{EngineMetricsHook, EventId, NoopMetricsHook};
    use cc_lb_domain::{TtlClass, WarmCacheEntry};

    use super::*;

    const TEST_MODEL: &str = "claude-sonnet-4-5-20250929";

    fn noop_metrics() -> Arc<dyn EngineMetricsHook> {
        Arc::new(NoopMetricsHook)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn success_event_upserts_and_enqueues_observations() {
        let cache = Arc::new(RecordingPromptCacheObservationCache::default());
        let sink = Arc::new(RecordingPromptCacheObservationSink::default());
        let (tx, rx) = mpsc::channel(16);
        let handle = spawn_lifecycle_prompt_cache_observation_subscriber(
            rx,
            LifecyclePromptCacheObservationSubscriberConfig::default(),
            cache.clone(),
            Some(sink.clone()),
            noop_metrics(),
        );

        tx.send(success_event(1))
            .await
            .expect("subscriber channel accepts success event");
        drop(tx);
        handle.shutdown().await;

        let upserts = cache.upserts();
        assert_eq!(upserts.len(), 1);
        assert_eq!(upserts[0].prefix_hash, "write");
        assert_eq!(upserts[0].canonical_model, TEST_MODEL);
        assert_eq!(upserts[0].prefix_content_block_index, 1);
        assert_eq!(upserts[0].estimated_prefix_tokens, 2_400);
        assert_eq!(upserts[0].token_estimate_source, "test");
        let records = sink.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].v3_prefix_key, "write");
        assert_eq!(records[0].canonical_model_id, TEST_MODEL);
        assert_eq!(records[0].hash_schema_version, HASH_SCHEMA_VERSION);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn aborted_event_produces_no_cache_or_sink_writes() {
        let cache = Arc::new(RecordingPromptCacheObservationCache::default());
        let sink = Arc::new(RecordingPromptCacheObservationSink::default());
        let (tx, rx) = mpsc::channel(16);
        let handle = spawn_lifecycle_prompt_cache_observation_subscriber(
            rx,
            LifecyclePromptCacheObservationSubscriberConfig::default(),
            cache.clone(),
            Some(sink.clone()),
            noop_metrics(),
        );

        tx.send(LifecycleEvent::PromptCacheObservationsProduced {
            event_id: eid("abort"),
            upstream_id: upstream_id(),
            canonical_model_id: TEST_MODEL.to_owned(),
            observations: Vec::new(),
            dropped_below_threshold: 0,
            dropped_aborted: 2,
        })
        .await
        .expect("subscriber channel accepts abort event");
        drop(tx);
        handle.shutdown().await;

        assert!(cache.upserts().is_empty());
        assert!(sink.records().is_empty());
    }

    fn success_event(dropped_below_threshold: u32) -> LifecycleEvent {
        LifecycleEvent::PromptCacheObservationsProduced {
            event_id: eid("success"),
            upstream_id: upstream_id(),
            canonical_model_id: TEST_MODEL.to_owned(),
            observations: vec![PromptCacheObservationWire {
                prefix_hash: "write".to_owned(),
                ttl_class: TtlClass::Ephemeral5m,
                expires_at_unix_secs: 1_800_000_300,
                kind: PromptCacheObservationKindWire::Write,
                prefix_content_block_index: 1,
                estimated_prefix_tokens: 2_400,
                token_estimate_source: Some("test".to_owned()),
            }],
            dropped_below_threshold,
            dropped_aborted: 0,
        }
    }

    fn eid(s: &str) -> EventId {
        s.to_owned()
    }

    fn upstream_id() -> Uuid {
        Uuid::from_u128(0x231)
    }

    #[derive(Default)]
    struct RecordingPromptCacheObservationCache {
        upserts: Mutex<Vec<PromptCacheObservationInput>>,
        refreshes: Mutex<HashMap<String, u64>>,
    }

    impl RecordingPromptCacheObservationCache {
        fn upserts(&self) -> Vec<PromptCacheObservationInput> {
            self.upserts.lock().expect("upserts lock").clone()
        }
    }

    impl PromptCacheObservationCacheLike for RecordingPromptCacheObservationCache {
        fn snapshot_for_upstream(
            &self,
            _upstream_id: Uuid,
            _canonical_model: &str,
            _request_breakpoint_hashes: &[(String, TtlClass)],
            _now_unix_secs: u64,
        ) -> Vec<WarmCacheEntry> {
            Vec::new()
        }

        fn upsert_observation(&self, observation: PromptCacheObservationInput) {
            self.upserts.lock().expect("upserts lock").push(observation);
        }

        fn refresh_on_hit(
            &self,
            _upstream_id: Uuid,
            _canonical_model: &str,
            prefix_hash: &str,
            _ttl_class: TtlClass,
            now_unix_secs: u64,
        ) -> bool {
            self.refreshes
                .lock()
                .expect("refreshes lock")
                .insert(prefix_hash.to_owned(), now_unix_secs);
            true
        }

        fn grace_margin_secs(&self) -> u64 {
            30
        }

        fn clock_now_unix_secs(&self) -> u64 {
            1_800_000_000
        }
    }

    #[derive(Default)]
    struct RecordingPromptCacheObservationSink {
        records: Mutex<Vec<PromptCacheObservationRecord>>,
    }

    impl RecordingPromptCacheObservationSink {
        fn records(&self) -> Vec<PromptCacheObservationRecord> {
            self.records.lock().expect("records lock").clone()
        }
    }

    impl PromptCacheObservationSinkLike for RecordingPromptCacheObservationSink {
        fn enqueue(
            &self,
            record: PromptCacheObservationRecord,
        ) -> Result<(), PromptCacheObservationEnqueueError> {
            self.records.lock().expect("records lock").push(record);
            Ok(())
        }
    }
}
