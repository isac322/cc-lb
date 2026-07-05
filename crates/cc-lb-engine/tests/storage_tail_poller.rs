mod storage_support;

use std::time::Duration;

use cc_lb_engine::StorageTailPoller;
use cc_lb_storage_api::{RequestEvent, RequestEventStore};
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
