use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

use cc_lb_storage_redb::{AuditEntry, BucketKind, Storage};

#[test]
fn killed_writer_leaves_database_reopenable() -> Result<(), Box<dyn std::error::Error>> {
    if let Ok(path) = std::env::var("CC_LB_CRASH_CHILD_PATH") {
        child_writer(Path::new(&path))?;
        return Ok(());
    }

    let dir = tempfile::tempdir()?;
    let path = dir.path().join("crash.redb");
    let mut child = Command::new(std::env::current_exe()?)
        .env("CC_LB_CRASH_CHILD_PATH", &path)
        .arg("--exact")
        .arg("killed_writer_leaves_database_reopenable")
        .arg("--nocapture")
        .spawn()?;

    wait_for_database_bytes(&path)?;
    thread::sleep(Duration::from_millis(50));
    let _ = child.kill();
    let status = child.wait()?;
    println!("child status after SIGKILL attempt: {status}");

    let storage = Storage::open(&path, [23; 32])?;
    let parent_count = storage.incr_quota("parent", 1, BucketKind::Requests, 1)?;
    let audit_rows = storage.query_audit(None, 0, u64::MAX, usize::MAX)?;
    println!(
        "reopened after killed writer; parent counter={parent_count}; readable audit rows={}",
        audit_rows.len()
    );

    assert_eq!(parent_count, 1);
    Ok(())
}

fn child_writer(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let storage = Storage::open(path, [23; 32])?;
    for index in 0..10_000 {
        storage.incr_quota("child", index % 8, BucketKind::Requests, 1)?;
        storage.append_audit(&AuditEntry {
            ts: 1_700_000_000 + index,
            request_id: format!("crash-{index}"),
            principal_id: "child".to_owned(),
            route: "messages".to_owned(),
            upstream: "anthropic_direct".to_owned(),
            model: Some("claude-sonnet-4-5".to_owned()),
            status: 200,
            input_tokens: 1,
            output_tokens: 2,
            duration_ms: 3,
            agent_label: Some("crash-child".to_owned()),
            kind: None,
            payload: None,
        })?;
        if index % 64 == 0 {
            thread::sleep(Duration::from_millis(1));
        }
    }
    Ok(())
}

fn wait_for_database_bytes(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if std::fs::metadata(path)
            .map(|metadata| metadata.len() > 0)
            .unwrap_or(false)
        {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(10));
    }
    Err("child did not create redb file before timeout".into())
}
