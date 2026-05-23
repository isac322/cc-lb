use std::sync::Arc;

use cc_lb_storage_redb::{PrincipalLimitState, RedbStorage};
use thiserror::Error;
use tokio::sync::mpsc::{self, Receiver, Sender, error::TrySendError};
use tokio::task::JoinHandle;

pub const DEFAULT_PRINCIPAL_LIMIT_STATE_CHANNEL_CAPACITY: usize = 1024;

#[derive(Clone, Debug)]
pub struct PrincipalLimitStateSink {
    tx: Sender<PrincipalLimitState>,
}

#[derive(Debug, Error)]
pub enum PrincipalLimitStateEnqueueError {
    #[error("principal limit state queue is full")]
    Full,
    #[error("principal limit state queue is closed")]
    Closed,
}

impl PrincipalLimitStateSink {
    pub fn new() -> (Self, Receiver<PrincipalLimitState>) {
        Self::with_capacity(DEFAULT_PRINCIPAL_LIMIT_STATE_CHANNEL_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> (Self, Receiver<PrincipalLimitState>) {
        let bounded_capacity = capacity.max(1);
        let (tx, rx) = mpsc::channel(bounded_capacity);
        (Self { tx }, rx)
    }

    pub fn enqueue(
        &self,
        state: PrincipalLimitState,
    ) -> Result<(), PrincipalLimitStateEnqueueError> {
        match self.tx.try_send(state) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                cc_lb_observability::increment_dropped_events("limit_state_full");
                Err(PrincipalLimitStateEnqueueError::Full)
            }
            Err(TrySendError::Closed(_)) => {
                cc_lb_observability::increment_dropped_events("limit_state_closed");
                Err(PrincipalLimitStateEnqueueError::Closed)
            }
        }
    }
}

pub fn start_principal_limit_state_writer(
    storage: Arc<RedbStorage>,
    mut receiver: Receiver<PrincipalLimitState>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(state) = receiver.recv().await {
            let storage = storage.clone();
            match tokio::task::spawn_blocking(move || storage.put_principal_limit_state(&state))
                .await
            {
                Ok(Ok(())) => {}
                Ok(Err(source)) => {
                    cc_lb_observability::increment_dropped_events("limit_state_worker_drop");
                    tracing::warn!(error = %source, "principal limit state persistence failed");
                }
                Err(source) => {
                    cc_lb_observability::increment_dropped_events("limit_state_worker_drop");
                    tracing::warn!(error = %source, "principal limit state writer task failed");
                }
            }
        }
    })
}
