use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cc_lb_core::usage_pruner::{PruneResult, UsagePruner};
use cc_lb_storage_redb::{AuditEntry, RequestEvent, Storage};

const DAY_MS: u64 = 86_400_000;
const DAY_SECS: u64 = 86_400;

#[tokio::test]
async fn prune_old_request_events() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, storage) = new_storage()?;
    let old_ts_ms = now_unix_ms().saturating_sub(100 * DAY_MS);
    insert_request_events(&storage, old_ts_ms)?;

    let pruner = UsagePruner::new(Arc::clone(&storage), 90);
    let result = pruner.prune_once().await;

    assert!(result.request_events_removed >= 5);
    let remaining =
        storage.query_request_events(0, u64::MAX, 100)?;
    assert_eq!(remaining.len(), 0);
    Ok(())
}

#[tokio::test]
async fn retention_zero_is_no_op() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, storage) = new_storage()?;
    let old_ts_ms = now_unix_ms().saturating_sub(100 * DAY_MS);
    insert_request_events(&storage, old_ts_ms)?;

    let pruner = UsagePruner::new(Arc::clone(&storage), 0);
    let result = pruner.prune_once().await;

    assert_eq!(result, PruneResult::default());
    let remaining =
        storage.query_request_events(0, u64::MAX, 100)?;
    assert_eq!(remaining.len(), 5);
    Ok(())
}

#[tokio::test]
async fn recent_rows_preserved() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, storage) = new_storage()?;
    insert_request_events(&storage, now_unix_ms())?;

    let pruner = UsagePruner::new(Arc::clone(&storage), 90);
    let result = pruner.prune_once().await;

    assert_eq!(result.request_events_removed, 0);
    let remaining =
        storage.query_request_events(0, u64::MAX, 100)?;
    assert_eq!(remaining.len(), 5);
    Ok(())
}

#[tokio::test]
async fn prune_old_audit_log() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, storage) = new_storage()?;
    let old_ts = now_unix_secs().saturating_sub(100 * DAY_SECS);
    insert_audit_entries(&storage, old_ts)?;

    let pruner = UsagePruner::new(Arc::clone(&storage), 90);
    let result = pruner.prune_once().await;

    assert!(result.audit_log_removed >= 5);
    let remaining = storage.query_audit(None, 0, u64::MAX, 100)?;
    assert_eq!(remaining.len(), 0);
    Ok(())
}

fn new_storage() -> Result<(tempfile::TempDir, Arc<Storage>), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let storage = Storage::open(&dir.path().join("usage-pruner.redb"), [32; 32])?;
    Ok((dir, Arc::new(storage)))
}

fn insert_request_events(storage: &Storage, ts_ms: u64) -> Result<(), Box<dyn std::error::Error>> {
    for index in 0..5 {
        storage.append_request_event(&RequestEvent {
            ts_ms,
            principal_id: "principal-1".to_owned(),
            key_id: format!("key-{index}"),
            model: "claude-sonnet-4-5".to_owned(),
            input_tokens: 10,
            output_tokens: 20,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            cost_usd_micros: 100,
            duration_ms: 25,
            status: 200,
        })?;
    }
    Ok(())
}

fn insert_audit_entries(storage: &Storage, ts: u64) -> Result<(), Box<dyn std::error::Error>> {
    for index in 0..5 {
        storage.append_audit(&AuditEntry {
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
        })?;
    }
    Ok(())
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
}
