use std::process::Command;

#[test]
fn multi_replica_postgres_script() {
    if std::env::var("RUN_MULTI_REPLICA").ok().as_deref() != Some("1") {
        eprintln!("skipped: RUN_MULTI_REPLICA not set");
        return;
    }

    let status = Command::new("bash")
        .arg("tests/real-client/multi-replica-postgres.sh")
        .status()
        .expect("run multi-replica postgres script");
    assert!(status.success(), "multi-replica postgres script failed");
}
