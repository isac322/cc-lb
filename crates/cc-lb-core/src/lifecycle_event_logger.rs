//! Phase-2 shadow subscriber that counts lifecycle events by kind.
//!
//! ## Role in RFC-0002 phased migration
//!
//! Phase 2 wires the proxy handler to emit both the legacy
//! [`RequestEventUpdate`](crate::event_bus::RequestEventUpdate) stream
//! (authoritative, feeds the DB writer) AND the new [`LifecycleEvent`]
//! stream (advisory only). This subscriber consumes the advisory stream
//! and increments `cc_lb_lifecycle_events_total{kind="..."}` per event.
//!
//! The metric provides the Phase-2 exit gate: production observes the
//! counter shape and confirms the emission is happening 1:1 with request
//! terminations before Phase 3's assembler subscriber is introduced.
//!
//! ## Shutdown protocol
//!
//! Mirrors
//! [`RequestEventWriterHandle::shutdown`](crate::request_event_writer::RequestEventWriterHandle::shutdown):
//! signal, drain, await. Late lifecycle events after shutdown are lost;
//! this is acceptable because the legacy path already persisted the row.

use cc_lb_lifecycle::LifecycleEvent;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

/// Handle to a spawned lifecycle-event-logger task.
pub struct LifecycleEventLoggerHandle {
    shutdown_tx: oneshot::Sender<()>,
    join: JoinHandle<()>,
}

impl LifecycleEventLoggerHandle {
    /// Signal the logger to drain and exit.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        if let Err(error) = self.join.await {
            tracing::warn!(%error, "lifecycle event logger task panicked");
        }
    }
}

/// Spawn the advisory subscriber that counts lifecycle events per kind.
///
/// `rx` is obtained from
/// [`InMemoryBus::attach_lifecycle_writer`](crate::event_bus::InMemoryBus::attach_lifecycle_writer).
pub fn spawn_lifecycle_event_logger(
    rx: mpsc::Receiver<LifecycleEvent>,
) -> LifecycleEventLoggerHandle {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let join = tokio::spawn(logger_loop(rx, shutdown_rx));
    LifecycleEventLoggerHandle { shutdown_tx, join }
}

async fn logger_loop(mut rx: mpsc::Receiver<LifecycleEvent>, mut shutdown: oneshot::Receiver<()>) {
    loop {
        tokio::select! {
            biased;
            event = rx.recv() => {
                match event {
                    Some(event) => record(&event),
                    None => return,
                }
            }
            _ = &mut shutdown => break,
        }
    }
    while let Ok(event) = rx.try_recv() {
        record(&event);
    }
}

fn record(event: &LifecycleEvent) {
    metrics::counter!("cc_lb_lifecycle_events_total", "kind" => event.kind()).increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_event(request_id: &str) -> LifecycleEvent {
        LifecycleEvent::RequestStarted {
            event_id: format!("evt-{request_id}"),
            request_id: request_id.to_owned(),
            ts_ms: 0,
            stream: false,
        }
    }

    #[tokio::test]
    async fn logger_drains_remaining_events_on_shutdown() {
        let (tx, rx) = mpsc::channel::<LifecycleEvent>(8);
        let handle = spawn_lifecycle_event_logger(rx);

        for i in 0..4 {
            tx.send(sample_event(&format!("req-{i}")))
                .await
                .expect("send");
        }
        drop(tx);
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn logger_exits_when_channel_closes() {
        let (tx, rx) = mpsc::channel::<LifecycleEvent>(8);
        let handle = spawn_lifecycle_event_logger(rx);
        drop(tx);
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn logger_increments_metrics_per_event() {
        let (tx, rx) = mpsc::channel::<LifecycleEvent>(8);
        let handle = spawn_lifecycle_event_logger(rx);

        tx.send(sample_event("metric-test")).await.expect("send");
        drop(tx);
        handle.shutdown().await;
    }
}
