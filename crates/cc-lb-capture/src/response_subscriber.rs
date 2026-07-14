//! Capture response lifecycle subscriber.

use std::time::Duration;

use cc_lb_lifecycle::LifecycleEvent;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

use self::joiner::ResponseJoiner;
use crate::sink::CaptureSink;

pub const DEFAULT_CAPTURE_RESPONSE_MAP_CAP: usize = 4_096;
pub const DEFAULT_CAPTURE_RESPONSE_TTL: Duration = Duration::from_secs(300);
const SWEEP_INTERVAL: Duration = Duration::from_secs(30);

mod joiner;

pub struct CaptureResponseSubscriberHandle {
    shutdown_tx: oneshot::Sender<()>,
    join: JoinHandle<()>,
}

impl CaptureResponseSubscriberHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        if let Err(error) = self.join.await {
            tracing::warn!(%error, "lifecycle capture response subscriber task panicked");
        }
    }
}

pub fn spawn_lifecycle_capture_response_subscriber(
    rx: mpsc::Receiver<LifecycleEvent>,
    sink: CaptureSink,
) -> CaptureResponseSubscriberHandle {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let subscriber = SubscriberLoop {
        rx,
        joiner: ResponseJoiner::new(sink, DEFAULT_CAPTURE_RESPONSE_MAP_CAP),
        ttl: DEFAULT_CAPTURE_RESPONSE_TTL,
        shutdown_rx,
    };
    let join = tokio::spawn(subscriber.run());
    CaptureResponseSubscriberHandle { shutdown_tx, join }
}

struct SubscriberLoop {
    rx: mpsc::Receiver<LifecycleEvent>,
    joiner: ResponseJoiner,
    ttl: Duration,
    shutdown_rx: oneshot::Receiver<()>,
}

impl SubscriberLoop {
    async fn run(mut self) {
        let mut sweeper = tokio::time::interval(SWEEP_INTERVAL);
        sweeper.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        sweeper.tick().await;

        loop {
            tokio::select! {
                biased;
                event = self.rx.recv() => {
                    match event {
                        Some(event) => self.joiner.handle_event(event),
                        None => break,
                    }
                }
                _ = sweeper.tick() => self.joiner.sweep_orphans(self.ttl),
                _ = &mut self.shutdown_rx => break,
            }
        }

        while let Ok(event) = self.rx.try_recv() {
            self.joiner.handle_event(event);
        }
    }
}

#[cfg(test)]
mod tests;
