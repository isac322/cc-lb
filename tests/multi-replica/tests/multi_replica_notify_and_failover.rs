//! Multi-process E2E for Postgres LISTEN/NOTIFY failover.
//!
//! Requires prebuilt `CC_LB_MULTI_REPLICA_SERVER_BIN` and
//! `CC_LB_MULTI_REPLICA_FAKE_ANTHROPIC_BIN` executables, fixed localhost ports
//! 8888, 8889, 8001, 8002, 8003, 8004, and 18888, plus
//! `CC_LB_MULTI_REPLICA_POSTGRES_URL=postgres://...`.
//!
//! `CC_LB_MULTI_REPLICA_POSTGRES_MODE=external` uses an already-running local
//! Postgres service and host `psql`. The default `compose` mode owns a local
//! Docker Compose Postgres service. Missing prerequisites fail loudly.
use std::process::Command;

#[test]
fn t5__multi_replica_notify_and_failover() {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let output = Command::new("bash")
        .arg(manifest_dir.join("multi-replica-postgres.sh"))
        .output()
        .expect("run multi-replica postgres script");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    print!("{stdout}");
    eprint!("{stderr}");
    assert!(
        output.status.success(),
        "multi-replica postgres script failed with {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        stdout,
        stderr
    );
}
