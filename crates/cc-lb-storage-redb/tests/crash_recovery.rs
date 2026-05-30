use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

use cc_lb_storage_redb::{AuditEntry, Storage};

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
    storage.append_audit(&AuditEntry {
        ts: 1_800_000_000,
        request_id: "crash-parent-probe".to_owned(),
        principal_id: "parent".to_owned(),
        route: "messages".to_owned(),
        upstream: "anthropic_direct".to_owned(),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        input_tokens: Some(1),
        output_tokens: Some(1),
        duration_ms: 1,
        agent_label: Some("crash-parent".to_owned()),
        ..Default::default()
    })?;
    let audit_rows = storage.query_audit(None, 0, u64::MAX, usize::MAX)?;
    println!(
        "reopened after killed writer; readable audit rows={}",
        audit_rows.len()
    );

    assert!(!audit_rows.is_empty());
    Ok(())
}

fn child_writer(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let ready_path = ready_marker_path(path);
    let storage = Storage::open(path, [23; 32])?;
    for index in 0..10_000 {
        storage.append_audit(&AuditEntry {
            ts: 1_700_000_000 + index,
            request_id: format!("crash-{index}"),
            principal_id: "child".to_owned(),
            route: "messages".to_owned(),
            upstream: "anthropic_direct".to_owned(),
            model: Some("claude-sonnet-4-5".to_owned()),
            status: 200,
            input_tokens: Some(1),
            output_tokens: Some(2),
            duration_ms: 3,
            agent_label: Some("crash-child".to_owned()),
            ..Default::default()
        })?;
        // NOTE [Priority-3 footgun]: signal the parent ONLY after a handful of
        // commits have actually fsync'd. File size alone (the previous gate)
        // races under cargo-llvm-cov instrumentation - redb allocates the file
        // before the first commit is durable, so the parent SIGKILL caught the
        // child mid-write and reopen failed with Io(Kind(InvalidData)). Touching
        // the sentinel only after >=100 appends guarantees the on-disk b-tree
        // has at least one fully-committed revision the parent can recover.
        if index == 100 {
            std::fs::File::create(&ready_path)?;
        }
        if index % 64 == 0 {
            thread::sleep(Duration::from_millis(1));
        }
    }
    Ok(())
}

fn ready_marker_path(path: &Path) -> std::path::PathBuf {
    let mut marker = path.as_os_str().to_owned();
    marker.push(".ready");
    marker.into()
}

fn wait_for_database_bytes(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let marker = ready_marker_path(path);
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if marker.exists() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(10));
    }
    Err("child did not publish the ready sentinel before timeout".into())
}
