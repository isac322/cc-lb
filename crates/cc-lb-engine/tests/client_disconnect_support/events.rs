use std::sync::Arc;
use std::time::Duration;

use cc_lb_control::{LifecycleBusReceiver, RequestEventBus};
use cc_lb_lifecycle::{LifecycleEvent, TerminationReason};
use cc_lb_request_log::RequestEventUpdate;
use cc_lb_storage_api::{BackendKind, MetaStore};
use cc_lb_storage_sqlite::SqliteStorage;
use tokio::sync::broadcast;

use crate::common::TestLifecycleBus;

pub fn lifecycle_receiver(test_bus: &TestLifecycleBus) -> broadcast::Receiver<LifecycleEvent> {
    let LifecycleBusReceiver::InMemory(rx) = test_bus.bus.subscribe_lifecycle() else {
        panic!("expected in-memory lifecycle receiver");
    };
    rx
}

pub async fn assert_error_terminal(
    rx: &mut broadcast::Receiver<LifecycleEvent>,
    expected_status: u16,
    expected_code: &str,
) {
    let terminal = one_terminal(rx).await;
    assert_eq!(terminal.0, expected_status);
    assert!(
        matches!(terminal.1, TerminationReason::ErrorCode(ref code) if code == expected_code),
        "unexpected termination reason: {:?}",
        terminal.1,
    );
}

pub async fn assert_success_terminal(
    rx: &mut broadcast::Receiver<LifecycleEvent>,
    expected_status: u16,
) {
    let terminal = one_terminal(rx).await;
    assert_eq!(terminal.0, expected_status);
    assert!(matches!(terminal.1, TerminationReason::Success));
}

pub async fn assert_dropped_terminal(rx: &mut broadcast::Receiver<LifecycleEvent>) {
    let terminal = one_terminal(rx).await;
    assert_eq!(terminal.0, 0);
    assert!(matches!(terminal.1, TerminationReason::Dropped));
}

async fn one_terminal(rx: &mut broadcast::Receiver<LifecycleEvent>) -> (u16, TerminationReason) {
    let terminal = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let LifecycleEvent::RequestTerminated {
                client_status,
                reason,
                ..
            } = rx.recv().await.expect("lifecycle event delivered")
            {
                break (client_status, reason);
            }
        }
    })
    .await
    .expect("request terminates");
    let duplicate_count = std::iter::from_fn(|| rx.try_recv().ok())
        .filter(|event| matches!(event, LifecycleEvent::RequestTerminated { .. }))
        .count();
    assert_eq!(duplicate_count, 0, "request terminated more than once");
    terminal
}

pub async fn assert_one_final(rx: &mut broadcast::Receiver<RequestEventUpdate>) {
    let final_count = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if matches!(
                rx.recv().await.expect("request update delivered"),
                RequestEventUpdate::Final(_)
            ) {
                break 1;
            }
        }
    })
    .await
    .expect("assembler publishes final")
        + std::iter::from_fn(|| rx.try_recv().ok())
            .filter(|update| matches!(update, RequestEventUpdate::Final(_)))
            .count();
    assert_eq!(final_count, 1, "assembler published duplicate finals");
}

pub async fn sqlite_storage(
    dir: &tempfile::TempDir,
) -> Result<SqliteStorage, Box<dyn std::error::Error>> {
    let database_url = format!(
        "sqlite://{}",
        dir.path().join("client-disconnect.sqlite").display()
    );
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_engine::SystemClock))
            .await?;
    storage.initialize(BackendKind::Sqlite).await?;
    Ok(storage)
}
