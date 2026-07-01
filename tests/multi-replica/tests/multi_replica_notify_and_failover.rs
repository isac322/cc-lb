//! Local-only multi-process E2E for Postgres LISTEN/NOTIFY failover.
//!
//! Requires Docker (via `DOCKER_HOST`), the following fixed localhost ports —
//! 8888, 8889, 8001, 8002, 8003, 8004, 18888 — plus these env vars:
//!   - `CC_LB_MULTI_REPLICA_E2E=1`
//!   - `CC_LB_MULTI_REPLICA_POSTGRES_URL=postgres://...`
//!
//! Intentionally not enabled in PR CI: spinning up two `cc-lb-server`
//! replicas, a fake Anthropic upstream, and a shared Postgres container in
//! Docker on shared self-hosted runners is flaky by construction. The
//! `postgres-scheduled.yml` cron workflow covers Postgres-live coverage on a
//! schedule instead.
use std::process::Command;

#[test]
fn multi_replica_notify_and_failover() {
    if std::env::var("CC_LB_MULTI_REPLICA_E2E").is_err() {
        eprintln!("skipped: CC_LB_MULTI_REPLICA_E2E not set");
        return;
    }

    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let status = Command::new("bash")
        .arg(manifest_dir.join("multi-replica-postgres.sh"))
        .status()
        .expect("run multi-replica postgres script");
    assert!(status.success(), "multi-replica postgres script failed");
}
