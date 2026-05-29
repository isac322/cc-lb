use std::process::Command;

#[test]
fn multi_replica_postgres_script() {
    if std::env::var("CC_LB_MULTI_REPLICA_E2E").is_err() {
        eprintln!("skipped: CC_LB_MULTI_REPLICA_E2E not set");
        return;
    }

    let status = Command::new("bash")
        .arg("tests/real-client/multi-replica-postgres.sh")
        .status()
        .expect("run multi-replica postgres script");
    assert!(status.success(), "multi-replica postgres script failed");
}
