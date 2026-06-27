use cc_lb_core::usage_pruner::{PruneResult, UsagePruner};
use cc_lb_core::{Clock, ClockHandle, TestClock, unix_millis, unix_secs};
use cc_lb_storage_api::{
    AuditEntry, AuditStore, BackendKind, MetaStore, RequestEvent, RequestEventStore,
};
use cc_lb_storage_sqlite::SqliteStorage;
use std::sync::Arc;

const DAY_MS: u64 = 86_400_000;
const DAY_SECS: u64 = 86_400;

#[tokio::test]
async fn prune_old_request_events() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, storage) = new_storage().await?;
    let clock = test_clock();
    let old_ts_ms = now_unix_ms(&*clock).saturating_sub(100 * DAY_MS);
    insert_request_events(storage.as_ref(), old_ts_ms).await?;

    let pruner = UsagePruner::new(Arc::clone(&storage), 90, Arc::clone(&clock));
    let result = pruner.prune_once().await;

    assert!(result.request_events_removed >= 5);
    let remaining = storage.query_request_events(0, u64::MAX, 100).await?;
    assert_eq!(remaining.len(), 0);
    Ok(())
}

#[tokio::test]
async fn retention_zero_is_no_op() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, storage) = new_storage().await?;
    let clock = test_clock();
    let old_ts_ms = now_unix_ms(&*clock).saturating_sub(100 * DAY_MS);
    insert_request_events(storage.as_ref(), old_ts_ms).await?;

    let pruner = UsagePruner::new(Arc::clone(&storage), 0, Arc::clone(&clock));
    let result = pruner.prune_once().await;

    assert_eq!(result, PruneResult::default());
    let remaining = storage.query_request_events(0, u64::MAX, 100).await?;
    assert_eq!(remaining.len(), 5);
    Ok(())
}

#[tokio::test]
async fn recent_rows_preserved() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, storage) = new_storage().await?;
    let clock = test_clock();
    insert_request_events(storage.as_ref(), now_unix_ms(&*clock)).await?;

    let pruner = UsagePruner::new(Arc::clone(&storage), 90, Arc::clone(&clock));
    let result = pruner.prune_once().await;

    assert_eq!(result.request_events_removed, 0);
    let remaining = storage.query_request_events(0, u64::MAX, 100).await?;
    assert_eq!(remaining.len(), 5);
    Ok(())
}

#[tokio::test]
async fn prune_old_audit_log() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, storage) = new_storage().await?;
    let clock = test_clock();
    let old_ts = now_unix_secs(&*clock).saturating_sub(100 * DAY_SECS);
    insert_audit_entries(storage.as_ref(), old_ts).await?;

    let pruner = UsagePruner::new(Arc::clone(&storage), 90, Arc::clone(&clock));
    let result = pruner.prune_once().await;

    assert!(result.audit_log_removed >= 5);
    let remaining = storage.query_audit(None, 0, u64::MAX, 100).await?;
    assert_eq!(remaining.len(), 0);
    Ok(())
}

async fn new_storage() -> Result<(tempfile::TempDir, Arc<SqliteStorage>), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let database_url = format!(
        "sqlite://{}",
        dir.path().join("usage-pruner.sqlite").display()
    );
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_core::SystemClock))
            .await?;
    storage.initialize(BackendKind::Sqlite).await?;
    Ok((dir, Arc::new(storage)))
}

async fn insert_request_events(
    storage: &SqliteStorage,
    ts_ms: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    for index in 0..5 {
        storage
            .append_request_event(&RequestEvent {
                ts_ms: Some(ts_ms),
                principal_id: Some("principal-1".to_owned()),
                key_id: Some(format!("key-{index}")),
                model: Some("claude-sonnet-4-5".to_owned()),
                input_tokens: Some(10),
                output_tokens: Some(20),
                cache_creation_input_tokens: Some(0),
                cache_read_input_tokens: Some(0),
                cost_usd_micros: Some(100),
                duration_ms: 25,
                status: 200,
                ..Default::default()
            })
            .await?;
    }
    Ok(())
}

async fn insert_audit_entries(
    storage: &SqliteStorage,
    ts: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    for index in 0..5 {
        storage
            .append_audit(&AuditEntry {
                ts,
                request_id: format!("request-{index}"),
                principal_id: "principal-1".to_owned(),
                route: "messages".to_owned(),
                upstream: "anthropic_direct".to_owned(),
                model: Some("claude-sonnet-4-5".to_owned()),
                status: 200,
                input_tokens: Some(10),
                output_tokens: Some(20),
                duration_ms: 25,
                agent_label: None,
                ..Default::default()
            })
            .await?;
    }
    Ok(())
}

fn test_clock() -> ClockHandle {
    Arc::new(TestClock::new_at_secs(4_100_000_000))
}

fn now_unix_ms(clock: &dyn Clock) -> u64 {
    unix_millis(clock.now()).min(u128::from(u64::MAX)) as u64
}

fn now_unix_secs(clock: &dyn Clock) -> u64 {
    unix_secs(clock.now())
}
