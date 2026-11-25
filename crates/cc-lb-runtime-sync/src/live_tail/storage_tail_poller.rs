use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cc_lb_request_log::StorageTailUpdate;
use cc_lb_storage_api::{RequestEvent, RequestEventStore, RequestEventStreamFilters};
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;

const STORAGE_TAIL_PAGE_LIMIT: usize = 500;
const MAX_EMPTY_RANGE_BACKOFF: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PollOutcome {
    Idle,
    Delivered,
    EmptyRange,
    Error,
}

fn next_poll_delay(base: Duration, consecutive_empty_ranges: u32) -> Duration {
    if consecutive_empty_ranges == 0 {
        return base;
    }
    let multiplier = 1u32
        .checked_shl(consecutive_empty_ranges.min(10))
        .unwrap_or(u32::MAX);
    base.saturating_mul(multiplier)
        .min(MAX_EMPTY_RANGE_BACKOFF.max(base))
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
        initial_cursor: u64,
        shutdown_rx: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        let poller = Self {
            storage,
            tx,
            poll_interval,
            last_seen: AtomicU64::new(initial_cursor),
        };
        tokio::spawn(poller.run(shutdown_rx))
    }

    async fn run(self, mut shutdown_rx: watch::Receiver<bool>) {
        let mut consecutive_empty_ranges = 0u32;
        loop {
            let delay = next_poll_delay(self.poll_interval, consecutive_empty_ranges);
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        return;
                    }
                }
                _ = tokio::time::sleep(delay) => {
                    match self.poll_once().await {
                        PollOutcome::Idle | PollOutcome::Delivered => {
                            consecutive_empty_ranges = 0;
                        }
                        PollOutcome::EmptyRange | PollOutcome::Error => {
                            consecutive_empty_ranges = consecutive_empty_ranges.saturating_add(1);
                        }
                    }
                }
            }
        }
    }

    async fn poll_once(&self) -> PollOutcome {
        metrics::counter!("sse_storage_tail_polls_total").increment(1);
        let last_seen = self.last_seen.load(Ordering::SeqCst);
        let current = match self.storage.current_request_event_cursor().await {
            Ok(cursor) => cursor,
            Err(error) => {
                tracing::warn!(%error, "storage tail cursor poll failed");
                return PollOutcome::Error;
            }
        };
        if current <= last_seen {
            metrics::gauge!("sse_storage_tail_backlog_rows").set(0.0);
            return PollOutcome::Idle;
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
                return PollOutcome::Error;
            }
        };

        metrics::gauge!("sse_storage_tail_backlog_rows").set(rows.len() as f64);
        if rows.is_empty() {
            return PollOutcome::EmptyRange;
        }
        let last_delivered = rows.last().map(|(cursor, _)| *cursor);
        for (cursor, event) in rows {
            record_lag(&event);
            let _ = self.tx.send(StorageTailUpdate { cursor, event });
        }
        if let Some(cursor) = last_delivered {
            self.last_seen.store(cursor, Ordering::SeqCst);
        }
        PollOutcome::Delivered
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

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicU64;

    use cc_lb_storage_api::StorageResult;

    use super::*;

    struct MockTailStore {
        current_cursor: AtomicU64,
        rows: Mutex<Vec<(u64, RequestEvent)>>,
        queries: Mutex<Vec<(u64, u64, usize)>>,
    }

    impl MockTailStore {
        fn new(current_cursor: u64, rows: Vec<(u64, RequestEvent)>) -> Self {
            Self {
                current_cursor: AtomicU64::new(current_cursor),
                rows: Mutex::new(rows),
                queries: Mutex::new(Vec::new()),
            }
        }
    }

    impl RequestEventStore for MockTailStore {
        fn append_request_event<'life0, 'life1, 'async_trait>(
            &'life0 self,
            _event: &'life1 RequestEvent,
        ) -> Pin<Box<dyn Future<Output = StorageResult<u64>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async { Ok(0) })
        }

        fn current_request_event_cursor<'life0, 'async_trait>(
            &'life0 self,
        ) -> Pin<Box<dyn Future<Output = StorageResult<u64>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            let cursor = self.current_cursor.load(Ordering::SeqCst);
            Box::pin(async move { Ok(cursor) })
        }

        fn query_request_events_between_cursors<'life0, 'life1, 'async_trait>(
            &'life0 self,
            after: u64,
            until: u64,
            limit: usize,
            _filters: &'life1 RequestEventStreamFilters,
        ) -> Pin<
            Box<dyn Future<Output = StorageResult<Vec<(u64, RequestEvent)>>> + Send + 'async_trait>,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.queries
                .lock()
                .expect("queries lock")
                .push((after, until, limit));
            let matched: Vec<_> = self
                .rows
                .lock()
                .expect("rows lock")
                .iter()
                .filter(|(c, _)| *c > after && *c <= until)
                .take(limit)
                .cloned()
                .collect();
            Box::pin(async move { Ok(matched) })
        }
    }

    #[tokio::test]
    async fn empty_range_preserves_last_seen_and_backs_off_poll_delay() {
        let store = Arc::new(MockTailStore::new(50, Vec::new()));
        let (tx, _rx) = broadcast::channel(8);
        let poller = StorageTailPoller {
            storage: Arc::clone(&store),
            tx,
            poll_interval: Duration::from_millis(100),
            last_seen: AtomicU64::new(10),
        };

        let outcome = poller.poll_once().await;
        assert_eq!(outcome, PollOutcome::EmptyRange);
        assert_eq!(poller.last_seen.load(Ordering::SeqCst), 10);

        let base = Duration::from_millis(100);
        assert_eq!(next_poll_delay(base, 0), base);
        assert_eq!(next_poll_delay(base, 1), Duration::from_millis(200));
        assert_eq!(next_poll_delay(base, 2), Duration::from_millis(400));
        assert_eq!(next_poll_delay(base, 10), MAX_EMPTY_RANGE_BACKOFF);
    }

    #[tokio::test]
    async fn sparse_page_advances_to_last_delivered_and_returns_delivered() {
        let store = Arc::new(MockTailStore::new(
            50,
            vec![(
                40,
                RequestEvent {
                    ts: 1,
                    request_id: "r40".into(),
                    ..Default::default()
                },
            )],
        ));
        let (tx, mut rx) = broadcast::channel(8);
        let poller = StorageTailPoller {
            storage: Arc::clone(&store),
            tx,
            poll_interval: Duration::from_millis(50),
            last_seen: AtomicU64::new(10),
        };

        let outcome = poller.poll_once().await;
        assert_eq!(outcome, PollOutcome::Delivered);
        let delivered = rx.try_recv().expect("row delivered");
        assert_eq!(delivered.cursor, 40);
        assert_eq!(poller.last_seen.load(Ordering::SeqCst), 40);
    }

    #[tokio::test]
    async fn idle_cursor_returns_idle_without_querying_rows() {
        let store = Arc::new(MockTailStore::new(10, Vec::new()));
        let (tx, _rx) = broadcast::channel(8);
        let poller = StorageTailPoller {
            storage: Arc::clone(&store),
            tx,
            poll_interval: Duration::from_millis(50),
            last_seen: AtomicU64::new(10),
        };

        let outcome = poller.poll_once().await;
        assert_eq!(outcome, PollOutcome::Idle);
        assert!(store.queries.lock().unwrap().is_empty());
    }
}
