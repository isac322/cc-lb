use std::process::Command;

#[test]
fn multi_replica_notify_and_failover() {
    if std::env::var("CC_LB_MULTI_REPLICA_E2E").is_err() {
        eprintln!("skipped: CC_LB_MULTI_REPLICA_E2E not set");
        return;
    }

    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let root = manifest_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root");
    let status = Command::new("bash")
        .arg(root.join("tests/real-client/multi-replica-postgres.sh"))
        .status()
        .expect("run multi-replica postgres script");
    assert!(status.success(), "multi-replica postgres script failed");
}
