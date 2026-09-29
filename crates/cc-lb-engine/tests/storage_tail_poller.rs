mod storage_support;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use cc_lb_engine::StorageTailPoller;
use cc_lb_storage_api::{
    RequestEvent, RequestEventStore, RequestEventStreamFilters, StorageResult,
};
use storage_support::TestStorage;

#[tokio::test]
async fn storage_tail_poller_broadcasts_rows_appended_after_spawn() {
    let storage = TestStorage::new();
    let (tx, mut rx) = tokio::sync::broadcast::channel(16);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let handle = StorageTailPoller::spawn(
        storage.as_request_event_store(),
        tx,
        Duration::from_millis(10),
        0,
        shutdown_rx,
    );

    let cursor = storage
        .append_request_event(&RequestEvent {
            request_id: "req-tail".to_owned(),
            event_id: Some("event-tail".to_owned()),
            ts: 1_700_000_000,
            ts_ms: Some(1_700_000_000_000),
            status: 200,
            duration_ms: 7,
            ..RequestEvent::default()
        })
        .await
        .expect("append request event");

    let update = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("tail poller should broadcast within timeout")
        .expect("tail broadcast should be open");
    assert_eq!(update.cursor, cursor);
    assert_eq!(update.event.event_id.as_deref(), Some("event-tail"));

    shutdown_tx.send(true).expect("shutdown signal sends");
    handle.await.expect("poller task joins");
}

#[tokio::test]
async fn storage_tail_poller_heals_final_missed_by_local_publish_path() {
    let storage = TestStorage::new();
    let (tx, mut rx) = tokio::sync::broadcast::channel(16);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let handle = StorageTailPoller::spawn(
        storage.as_request_event_store(),
        tx,
        Duration::from_millis(10),
        0,
        shutdown_rx,
    );

    storage
        .append_request_event(&RequestEvent {
            request_id: "req-crash-gap".to_owned(),
            event_id: Some("event-crash-gap".to_owned()),
            ts: 1_700_000_001,
            ts_ms: Some(1_700_000_001_000),
            status: 200,
            duration_ms: 11,
            ..RequestEvent::default()
        })
        .await
        .expect("append final without local bus publish");

    let update = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("storage tail should heal missed local publish")
        .expect("tail broadcast should be open");
    assert_eq!(update.event.event_id.as_deref(), Some("event-crash-gap"));

    shutdown_tx.send(true).expect("shutdown signal sends");
    handle.await.expect("poller task joins");
}

#[tokio::test]
async fn storage_tail_poller_starts_after_existing_rows() {
    let storage = HorizonBlockedStorage::new();
    storage.make_cursor_visible(
        1,
        RequestEvent {
            request_id: "req-existing".to_owned(),
            event_id: Some("event-existing".to_owned()),
            ts: 1_700_000_002,
            ts_ms: Some(1_700_000_002_000),
            status: 200,
            duration_ms: 13,
            ..RequestEvent::default()
        },
    );
    let initial_cursor = storage
        .current_request_event_cursor()
        .await
        .expect("startup cursor loads");
    let (tx, mut rx) = tokio::sync::broadcast::channel(16);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let handle = StorageTailPoller::spawn(
        storage.clone(),
        tx,
        Duration::from_millis(10),
        initial_cursor,
        shutdown_rx,
    );

    storage.make_cursor_visible(
        2,
        RequestEvent {
            request_id: "req-new".to_owned(),
            event_id: Some("event-new".to_owned()),
            ts: 1_700_000_003,
            ts_ms: Some(1_700_000_003_000),
            status: 200,
            duration_ms: 17,
            ..RequestEvent::default()
        },
    );

    let update = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("tail poller should broadcast the new row")
        .expect("tail broadcast should be open");
    assert_eq!(update.cursor, 2);
    assert_eq!(update.event.event_id.as_deref(), Some("event-new"));

    shutdown_tx.send(true).expect("shutdown signal sends");
    handle.await.expect("poller task joins");
}

#[tokio::test]
async fn storage_tail_poller_does_not_advance_past_ineligible_cursor() {
    let storage = HorizonBlockedStorage::new();
    let (tx, mut rx) = tokio::sync::broadcast::channel(16);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let handle = StorageTailPoller::spawn(
        storage.clone(),
        tx,
        Duration::from_millis(10),
        0,
        shutdown_rx,
    );

    storage.first_empty_query.notified().await;
    storage.make_cursor_visible(
        1,
        RequestEvent {
            request_id: "req-horizon-blocked".to_owned(),
            event_id: Some("event-horizon-blocked".to_owned()),
            ts: 1_700_000_002,
            ts_ms: Some(1_700_000_002_000),
            status: 200,
            duration_ms: 13,
            ..RequestEvent::default()
        },
    );

    let update = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("tail poller should retry a cursor whose row was not yet eligible")
        .expect("tail broadcast should be open");
    assert_eq!(update.cursor, 1);
    assert_eq!(
        update.event.event_id.as_deref(),
        Some("event-horizon-blocked")
    );

    shutdown_tx.send(true).expect("shutdown signal sends");
    handle.await.expect("poller task joins");
}

struct HorizonBlockedStorage {
    current: AtomicU64,
    rows: Mutex<VecDeque<(u64, RequestEvent)>>,
    query_count: AtomicU64,
    first_empty_query: tokio::sync::Notify,
}

impl HorizonBlockedStorage {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            current: AtomicU64::new(1),
            rows: Mutex::new(VecDeque::new()),
            query_count: AtomicU64::new(0),
            first_empty_query: tokio::sync::Notify::new(),
        })
    }

    fn make_cursor_visible(&self, cursor: u64, event: RequestEvent) {
        self.current.fetch_max(cursor, Ordering::SeqCst);
        self.rows
            .lock()
            .expect("horizon-blocked rows lock")
            .push_back((cursor, event));
    }
}

#[async_trait]
impl RequestEventStore for HorizonBlockedStorage {
    async fn append_request_event(&self, _event: &RequestEvent) -> StorageResult<u64> {
        unreachable!("horizon-blocked storage is read-only for this test")
    }

    async fn current_request_event_cursor(&self) -> StorageResult<u64> {
        Ok(self.current.load(Ordering::SeqCst))
    }

    async fn query_request_events_between_cursors(
        &self,
        after: u64,
        until: u64,
        limit: usize,
        _filters: &RequestEventStreamFilters,
    ) -> StorageResult<Vec<(u64, RequestEvent)>> {
        let count = self.query_count.fetch_add(1, Ordering::SeqCst);
        if count == 0 {
            self.first_empty_query.notify_one();
            return Ok(Vec::new());
        }

        let rows = self
            .rows
            .lock()
            .expect("horizon-blocked rows lock")
            .iter()
            .filter(|(cursor, _)| *cursor > after && *cursor <= until)
            .take(limit)
            .cloned()
            .collect();
        Ok(rows)
    }
}
