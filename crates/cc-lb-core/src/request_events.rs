use std::sync::Arc;

use cc_lb_storage_redb::{RedbStorage, RequestEvent};
use thiserror::Error;
use tokio::sync::mpsc::{self, Receiver, Sender, error::TrySendError};
use tokio::task::JoinHandle;

pub const DEFAULT_REQUEST_EVENT_CHANNEL_CAPACITY: usize = 4096;

#[derive(Clone, Debug)]
pub struct RequestEventSink {
    tx: Sender<RequestEvent>,
}

#[derive(Debug, Error)]
pub enum RequestEventEnqueueError {
    #[error("request event queue is full")]
    Full,
    #[error("request event queue is closed")]
    Closed,
}

impl RequestEventSink {
    pub fn new() -> (Self, Receiver<RequestEvent>) {
        Self::with_capacity(DEFAULT_REQUEST_EVENT_CHANNEL_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> (Self, Receiver<RequestEvent>) {
        let bounded_capacity = capacity.max(1);
        let (tx, rx) = mpsc::channel(bounded_capacity);
        (Self { tx }, rx)
    }

    pub fn enqueue(&self, event: RequestEvent) -> Result<(), RequestEventEnqueueError> {
        match self.tx.try_send(event) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                cc_lb_observability::increment_dropped_events("full");
                Err(RequestEventEnqueueError::Full)
            }
            Err(TrySendError::Closed(_)) => {
                cc_lb_observability::increment_dropped_events("closed");
                Err(RequestEventEnqueueError::Closed)
            }
        }
    }
}

pub fn start_request_event_writer(
    storage: Arc<RedbStorage>,
    mut receiver: Receiver<RequestEvent>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(event) = receiver.recv().await {
            let storage = storage.clone();
            match tokio::task::spawn_blocking(move || storage.append_request_event(&event)).await {
                Ok(Ok(())) => {}
                Ok(Err(source)) => {
                    cc_lb_observability::increment_dropped_events("worker_drop");
                    tracing::warn!(error = %source, "request event persistence failed");
                }
                Err(source) => {
                    cc_lb_observability::increment_dropped_events("worker_drop");
                    tracing::warn!(error = %source, "request event writer task failed");
                }
            }
        }
    })
}
