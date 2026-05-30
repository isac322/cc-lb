use std::sync::Arc;
use std::time::Duration;

use cc_lb_core::{AuditEntry, spawn_audit_writer};
use cc_lb_storage_api::AuditStore;
use cc_lb_storage_redb::Storage;

#[tokio::test(flavor = "current_thread")]
async fn flush_100() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, storage) = new_storage()?;
    let audit_storage: Arc<dyn AuditStore> = storage.clone();
    let (sink, join) = spawn_audit_writer(audit_storage, 1024);

    for index in 0..100 {
        sink.try_enqueue(audit_entry(index))
            .expect("enqueue succeeds");
    }

    // NOTE [Priority-3 footgun]: spawn_audit_writer is a bounded-channel background
    // task. The original flat 2s sleep was enough for plain debug builds but raced
    // on CI under cargo-llvm-cov instrumentation (slower redb writes) - only ~31 of
    // 100 entries had flushed when the assertion fired. Poll up to 10s for the full
    // count before asserting; the drop(sink) below still proves drain-on-shutdown.
    const BUDGET: Duration = Duration::from_secs(10);
    const INTERVAL: Duration = Duration::from_millis(25);
    let deadline = std::time::Instant::now() + BUDGET;
    let final_count = loop {
        let count = storage.count_audit_entries()?;
        if count == 100 || std::time::Instant::now() >= deadline {
            break count;
        }
        tokio::time::sleep(INTERVAL).await;
    };
    assert_eq!(final_count, 100);

    drop(sink);
    join.await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn full_drops() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, storage) = new_storage()?;
    let audit_storage: Arc<dyn AuditStore> = storage.clone();
    let (sink, join) = spawn_audit_writer(audit_storage, 4);

    let dropped = (0..100)
        .filter(|index| sink.try_enqueue(audit_entry(*index)).is_err())
        .count();

    assert!(dropped > 0, "bounded channel should drop when full");

    drop(sink);
    join.await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_drain() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, storage) = new_storage()?;
    let audit_storage: Arc<dyn AuditStore> = storage.clone();
    let (sink, join) = spawn_audit_writer(audit_storage, 1024);

    for index in 0..20 {
        sink.try_enqueue(audit_entry(index))
            .expect("enqueue succeeds");
    }

    drop(sink);
    join.await?;
    assert_eq!(storage.count_audit_entries()?, 20);
    Ok(())
}

fn new_storage() -> Result<(tempfile::TempDir, Arc<Storage>), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let storage = Storage::open(&dir.path().join("audit-writer.redb"), [27; 32])?;
    Ok((dir, Arc::new(storage)))
}

fn audit_entry(index: usize) -> AuditEntry {
    AuditEntry {
        ts: 1_800_000_000 + index as u64,
        request_id: format!("req-{index}"),
        principal_id: "principal-1".to_owned(),
        route: "/v1/messages".to_owned(),
        upstream: "anthropic_direct".to_owned(),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        input_tokens: Some(index as u64),
        output_tokens: Some((index * 2) as u64),
        duration_ms: 25,
        agent_label: Some("audit-writer-smoke".to_owned()),
        api_key_id: Some("key-1".to_owned()),
        cost_usd_micros: Some(100 + index as u64),
        limit_violation: None,
        admin_action: None,
        actor: Some("system".to_owned()),
    }
}
