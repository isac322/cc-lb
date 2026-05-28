use std::sync::Arc;
use std::time::Duration;

use cc_lb_core::{AuditEntry, spawn_audit_writer};
use cc_lb_storage_redb::Storage;

#[tokio::test(flavor = "current_thread")]
async fn flush_100() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, storage) = new_storage()?;
    let (sink, join) = spawn_audit_writer(Arc::clone(&storage), 1024);

    for index in 0..100 {
        sink.try_enqueue(audit_entry(index))
            .expect("enqueue succeeds");
    }

    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(storage.count_audit_entries()?, 100);

    drop(sink);
    join.await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn full_drops() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, storage) = new_storage()?;
    let (sink, join) = spawn_audit_writer(storage, 4);

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
    let (sink, join) = spawn_audit_writer(Arc::clone(&storage), 1024);

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
