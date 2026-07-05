use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cc_lb_storage_api::{RequestEvent, RequestEventStore, RequestEventStreamFilters};
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;

const STORAGE_TAIL_PAGE_LIMIT: usize = 500;

#[derive(Debug, Clone)]
pub struct StorageTailUpdate {
    pub cursor: u64,
    pub event: RequestEvent,
}

pub struct StorageTailPoller<S: RequestEventStore + ?Sized> {
    storage: Arc<S>,
    tx: broadcast::Sender<StorageTailUpdate>,
    poll_interval: Duration,
    last_seen: AtomicU64,
}

impl<S> StorageTailPoller<S>
where
    S: RequestEventStore + ?Sized + 'static,
{
    pub fn spawn(
        storage: Arc<S>,
        tx: broadcast::Sender<StorageTailUpdate>,
        poll_interval: Duration,
        shutdown_rx: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        let poller = Self {
            storage,
            tx,
            poll_interval,
            last_seen: AtomicU64::new(0),
        };
        tokio::spawn(poller.run(shutdown_rx))
    }

    async fn run(self, mut shutdown_rx: watch::Receiver<bool>) {
        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        return;
                    }
                }
                _ = tokio::time::sleep(self.poll_interval) => self.poll_once().await,
            }
        }
    }

    async fn poll_once(&self) {
        metrics::counter!("sse_storage_tail_polls_total").increment(1);
        let last_seen = self.last_seen.load(Ordering::SeqCst);
        let current = match self.storage.current_request_event_cursor().await {
            Ok(cursor) => cursor,
            Err(error) => {
                tracing::warn!(%error, "storage tail cursor poll failed");
                return;
            }
        };
        if current <= last_seen {
            metrics::gauge!("sse_storage_tail_backlog_rows").set(0.0);
            return;
        }

        let filters = RequestEventStreamFilters::default();
        let rows = match self
            .storage
            .query_request_events_between_cursors(
                last_seen,
                current,
                STORAGE_TAIL_PAGE_LIMIT,
                &filters,
            )
            .await
        {
            Ok(rows) => rows,
            Err(error) => {
                tracing::warn!(%error, last_seen, current, "storage tail row poll failed");
                return;
            }
        };

        metrics::gauge!("sse_storage_tail_backlog_rows").set(rows.len() as f64);
        for (cursor, event) in rows {
            record_lag(&event);
            let _ = self.tx.send(StorageTailUpdate { cursor, event });
        }
        self.last_seen.store(current, Ordering::SeqCst);
    }
}

fn record_lag(event: &RequestEvent) {
    let event_ms = event.ts_ms.unwrap_or_else(|| event.ts.saturating_mul(1000));
    let lag = now_ms().saturating_sub(event_ms) as f64;
    metrics::histogram!("sse_storage_tail_lag_ms").record(lag);
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
