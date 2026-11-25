use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use backon::{BackoffBuilder as _, ExponentialBuilder};
use cc_lb_storage_api::{
    ChangeChannel, ChangeEvent, RuntimeChangeNotifier, StorageError, StorageResult,
};
use sqlx::postgres::PgListener;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::adapter::PostgresStorage;

const INITIAL_RECONNECT_BACKOFF: Duration = Duration::from_millis(50);
const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(5);
const BROADCAST_CAPACITY: usize = 1024;

pub(crate) fn change_sender() -> tokio::sync::broadcast::Sender<ChangeEvent> {
    let (change_tx, _) = tokio::sync::broadcast::channel(BROADCAST_CAPACITY);
    change_tx
}

fn reconnect_backoff() -> backon::ExponentialBackoff {
    ExponentialBuilder::default()
        .with_min_delay(INITIAL_RECONNECT_BACKOFF)
        .with_max_delay(MAX_RECONNECT_BACKOFF)
        .with_jitter()
        .without_max_times()
        .build()
}

#[async_trait]
impl RuntimeChangeNotifier for PostgresStorage {
    async fn subscribe(&self) -> StorageResult<tokio::sync::broadcast::Receiver<ChangeEvent>> {
        Ok(self.change_tx.subscribe())
    }

    async fn run(&self, cancel: CancellationToken) -> StorageResult<()> {
        let _guard = RunGuard::try_acquire(&self.notifier_running)?;
        let mut backoff = reconnect_backoff();

        loop {
            let mut established = false;
            let outcome = self
                .listen_until_lost(cancel.clone(), &mut established)
                .await;
            if established {
                backoff = reconnect_backoff();
            }
            let delay = backoff.next().unwrap_or(MAX_RECONNECT_BACKOFF);
            match outcome {
                Ok(()) => return Ok(()),
                Err(ListenerLoopError::Lost) => {
                    tracing::warn!(
                        backoff_ms = delay.as_millis(),
                        "postgres runtime change listener lost; reconnecting"
                    );
                }
                Err(ListenerLoopError::Sqlx(error)) => {
                    tracing::warn!(
                        backoff_ms = delay.as_millis(),
                        error = %error,
                        "postgres runtime change listener failed; reconnecting"
                    );
                }
            }

            if cancel.is_cancelled() {
                return Ok(());
            }

            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                _ = sleep(delay) => {}
            }
        }
    }
}

impl PostgresStorage {
    async fn listen_until_lost(
        &self,
        cancel: CancellationToken,
        established: &mut bool,
    ) -> Result<(), ListenerLoopError> {
        let mut listener = PgListener::connect_with(&self.listener_pool).await?;
        listener.eager_reconnect(false);
        listener
            .listen_all(
                ChangeChannel::ALL
                    .iter()
                    .map(|channel| channel.postgres_channel()),
            )
            .await?;
        *established = true;
        // NOTIFY is not durable. Rebuild from storage after every LISTEN
        // establishment so changes committed during startup or reconnect are
        // observed even when their notification arrived before this session.
        let _ = self.change_tx.send(ChangeEvent::new(
            ChangeChannel::Principal,
            "postgres-listener-established",
            self.clock.now(),
        ));

        loop {
            let notification = tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                result = listener.try_recv() => result?,
            };

            let Some(notification) = notification else {
                return Err(ListenerLoopError::Lost);
            };

            let Some(channel) = ChangeChannel::from_postgres_channel(notification.channel()) else {
                continue;
            };
            let event = ChangeEvent::new(channel, notification.payload(), self.clock.now());
            let _ = self.change_tx.send(event);
        }
    }
}

#[derive(Debug)]
enum ListenerLoopError {
    Lost,
    Sqlx(sqlx::Error),
}

impl From<sqlx::Error> for ListenerLoopError {
    fn from(error: sqlx::Error) -> Self {
        Self::Sqlx(error)
    }
}

struct RunGuard<'a> {
    running: &'a AtomicBool,
}

impl<'a> RunGuard<'a> {
    fn try_acquire(running: &'a AtomicBool) -> StorageResult<Self> {
        if running.swap(true, Ordering::AcqRel) {
            return Err(StorageError::Conflict {
                message: "postgres runtime change notifier is already running".to_owned(),
            });
        }

        Ok(Self { running })
    }
}

impl Drop for RunGuard<'_> {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Release);
    }
}
