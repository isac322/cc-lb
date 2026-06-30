//! Durable subscriber that persists `Final` request events to storage.
//!
//! ## Role in the event-driven sink architecture
//!
//! The proxy publishes [`RequestEventUpdate`] envelopes through an
//! [`InMemoryBus`](crate::event_bus::InMemoryBus) which fans out to two
//! consumer roles:
//!
//! - **Ephemeral consumers** (admin SSE) — `broadcast` channel, drop-oldest
//!   on lag.
//! - **Durable consumer** — this writer task. Subscribes via
//!   [`InMemoryBus::attach_writer`](crate::event_bus::InMemoryBus::attach_writer)
//!   over a bounded mpsc and persists `Final` envelopes to
//!   `request_events_v1`.
//!
//! ## Shutdown protocol
//!
//! [`RequestEventWriterHandle::shutdown`] signals the loop to exit, drains
//! any remaining buffered envelopes via `try_recv`, then awaits the spawned
//! task. The shutdown hook in `app.rs` MUST call this **after** the listener
//! is closed and in-flight requests have completed, otherwise late `Final`
//! envelopes are observed but not persisted.
//!
//! ## Failure semantics
//!
//! - `Partial` envelopes are silently ignored — they exist only for the
//!   live UI subscriber.
//! - Storage append errors are `tracing::warn!`-logged and counted via
//!   `request_event_writer_storage_error` metric. The loop never panics.
//! - mpsc closure (all senders dropped) is treated as a normal exit
//!   condition; the task drains any buffered remainder and returns.

use std::sync::Arc;

use cc_lb_storage_api::RequestEventStore;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::event_bus::RequestEventUpdate;

/// Handle to a spawned request-event writer task.
pub struct RequestEventWriterHandle {
    shutdown_tx: oneshot::Sender<()>,
    join: JoinHandle<()>,
}

impl RequestEventWriterHandle {
    /// Signal the writer to drain its mpsc buffer and exit.
    ///
    /// Caller MUST ensure no new `Final` envelopes are produced after this
    /// returns (typically by closing the listener and waiting for in-flight
    /// requests first).
    pub async fn shutdown(self) {
        // Ignore error: writer task already exited (channel closed).
        let _ = self.shutdown_tx.send(());
        if let Err(error) = self.join.await {
            tracing::warn!(%error, "request event writer task panicked");
        }
    }
}

/// Spawn the durable writer task.
///
/// `rx` is obtained from
/// [`InMemoryBus::attach_writer`](crate::event_bus::InMemoryBus::attach_writer).
pub fn spawn_request_event_writer(
    storage: Arc<dyn RequestEventStore>,
    rx: mpsc::Receiver<RequestEventUpdate>,
) -> RequestEventWriterHandle {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let join = tokio::spawn(writer_loop(storage, rx, shutdown_rx));
    RequestEventWriterHandle {
        shutdown_tx,
        join,
    }
}

async fn writer_loop(
    storage: Arc<dyn RequestEventStore>,
    mut rx: mpsc::Receiver<RequestEventUpdate>,
    mut shutdown: oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            biased;
            update = rx.recv() => {
                match update {
                    Some(update) => persist_if_final(&*storage, update).await,
                    // All senders dropped — clean exit, nothing else to drain.
                    None => return,
                }
            }
            _ = &mut shutdown => break,
        }
    }
    // Shutdown signaled. Drain remaining buffered events without awaiting new
    // arrivals (caller guarantees no further produces).
    while let Ok(update) = rx.try_recv() {
        persist_if_final(&*storage, update).await;
    }
}

async fn persist_if_final(storage: &dyn RequestEventStore, update: RequestEventUpdate) {
    if !update.is_final() {
        return;
    }
    if let Err(error) = storage.append_request_event(&update.event).await {
        tracing::warn!(
            %error,
            request_id = %update.event.request_id,
            "request event writer: failed to persist event",
        );
        cc_lb_observability::increment_dropped_events_by(
            "request_event_writer_storage_error",
            1,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use cc_lb_storage_api::{RequestEvent, StorageResult};
    use std::sync::Mutex as StdMutex;
    use tokio::sync::mpsc;

    use crate::event_bus::{InMemoryBus, RequestEventBus, RequestEventUpdate};

    #[derive(Default)]
    struct FakeStorage {
        appended: StdMutex<Vec<RequestEvent>>,
        fail_count: StdMutex<u32>,
    }

    impl FakeStorage {
        fn appended_snapshot(&self) -> Vec<RequestEvent> {
            self.appended.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl RequestEventStore for FakeStorage {
        async fn append_request_event(&self, event: &RequestEvent) -> StorageResult<()> {
            {
                let mut fail = self.fail_count.lock().unwrap();
                if *fail > 0 {
                    *fail -= 1;
                    return Err(cc_lb_storage_api::StorageError::Unavailable {
                        message: "fake backend failure".to_owned(),
                    });
                }
            }
            self.appended.lock().unwrap().push(event.clone());
            Ok(())
        }

        async fn query_request_events(
            &self,
            _since: u64,
            _until: u64,
            _limit: usize,
        ) -> StorageResult<Vec<RequestEvent>> {
            Ok(Vec::new())
        }

        async fn query_recent_request_events(
            &self,
            _since: u64,
            _until: u64,
            _limit: usize,
        ) -> StorageResult<Vec<RequestEvent>> {
            Ok(Vec::new())
        }

        async fn prune_request_events_before(
            &self,
            _cutoff_ms_x_1m: u64,
            _batch_size: usize,
        ) -> StorageResult<u64> {
            Ok(0)
        }
    }

    fn sample_event(request_id: &str) -> RequestEvent {
        RequestEvent {
            ts: 1_700_000_000,
            request_id: request_id.to_string(),
            status: 200,
            duration_ms: 42,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn writer_persists_final_envelopes() {
        let storage = Arc::new(FakeStorage::default());
        let (tx, rx) = mpsc::channel(8);

        let writer = spawn_request_event_writer(storage.clone(), rx);
        tx.send(RequestEventUpdate::final_(sample_event("req-a")))
            .await
            .unwrap();
        tx.send(RequestEventUpdate::final_(sample_event("req-b")))
            .await
            .unwrap();
        drop(tx);
        writer.shutdown().await;

        let appended = storage.appended_snapshot();
        assert_eq!(appended.len(), 2);
        assert_eq!(appended[0].request_id, "req-a");
        assert_eq!(appended[1].request_id, "req-b");
    }

    #[tokio::test]
    async fn writer_ignores_partial_envelopes() {
        let storage = Arc::new(FakeStorage::default());
        let (tx, rx) = mpsc::channel(8);

        let writer = spawn_request_event_writer(storage.clone(), rx);
        tx.send(RequestEventUpdate::partial(sample_event("req-p1")))
            .await
            .unwrap();
        tx.send(RequestEventUpdate::partial(sample_event("req-p2")))
            .await
            .unwrap();
        tx.send(RequestEventUpdate::final_(sample_event("req-f")))
            .await
            .unwrap();
        drop(tx);
        writer.shutdown().await;

        let appended = storage.appended_snapshot();
        assert_eq!(appended.len(), 1);
        assert_eq!(appended[0].request_id, "req-f");
    }

    #[tokio::test]
    async fn writer_exits_when_all_senders_drop() {
        let storage = Arc::new(FakeStorage::default());
        let (tx, rx) = mpsc::channel(8);
        let writer = spawn_request_event_writer(storage.clone(), rx);
        drop(tx);
        // Without explicit shutdown, the task should still terminate.
        writer.shutdown().await;
    }

    #[tokio::test]
    async fn writer_drains_buffered_events_on_shutdown() {
        let storage = Arc::new(FakeStorage::default());
        let (tx, rx) = mpsc::channel(8);
        let writer = spawn_request_event_writer(storage.clone(), rx);

        // Send several events; rely on shutdown drain to persist them all.
        for i in 0..5 {
            tx.send(RequestEventUpdate::final_(sample_event(&format!(
                "req-{i}"
            ))))
            .await
            .unwrap();
        }
        writer.shutdown().await;
        // Drop tx after shutdown to keep mpsc alive until shutdown signal.
        drop(tx);

        assert_eq!(storage.appended_snapshot().len(), 5);
    }

    #[tokio::test]
    async fn storage_error_does_not_panic_writer() {
        let storage = Arc::new(FakeStorage::default());
        *storage.fail_count.lock().unwrap() = 2;
        let (tx, rx) = mpsc::channel(8);
        let writer = spawn_request_event_writer(storage.clone(), rx);

        tx.send(RequestEventUpdate::final_(sample_event("err-1")))
            .await
            .unwrap();
        tx.send(RequestEventUpdate::final_(sample_event("err-2")))
            .await
            .unwrap();
        tx.send(RequestEventUpdate::final_(sample_event("ok")))
            .await
            .unwrap();
        drop(tx);
        writer.shutdown().await;

        let appended = storage.appended_snapshot();
        assert_eq!(appended.len(), 1);
        assert_eq!(appended[0].request_id, "ok");
    }

    #[tokio::test]
    async fn end_to_end_via_in_memory_bus() {
        let storage = Arc::new(FakeStorage::default());
        let bus = InMemoryBus::new();
        let rx = bus.attach_writer(8);
        let writer = spawn_request_event_writer(storage.clone(), rx);

        bus.publish(RequestEventUpdate::partial(sample_event("p")));
        bus.publish(RequestEventUpdate::final_(sample_event("f1")));
        bus.publish(RequestEventUpdate::final_(sample_event("f2")));

        // Give writer a moment to drain.
        tokio::task::yield_now().await;
        writer.shutdown().await;

        let appended = storage.appended_snapshot();
        assert_eq!(appended.len(), 2);
        assert_eq!(appended[0].request_id, "f1");
        assert_eq!(appended[1].request_id, "f2");
    }
}
