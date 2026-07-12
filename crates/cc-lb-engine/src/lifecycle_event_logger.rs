//! Lifecycle-event logger subscriber.
//!
//! Consumes the `LifecycleEvent` stream and increments
//! `cc_lb_lifecycle_events_total{kind="..."}` per event, providing a
//! coarse volume-and-shape signal for the bus independent of any
//! semantic subscriber.
//!
//! ## Shutdown protocol
//!
//! Signal, drain, await. Late lifecycle events after shutdown are lost;
//! this is acceptable because the assembler already persisted the row.

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

/// Spawn the subscriber that counts lifecycle events per kind.
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
