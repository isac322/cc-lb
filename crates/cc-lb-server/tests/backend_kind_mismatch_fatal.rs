use std::process::Command;
use std::sync::Arc;

use cc_lb_storage_api::{BackendKind, MetaStore};
use cc_lb_storage_sqlite::open_sqlite;
use tokio::runtime::Runtime;

#[test]
fn backend_kind_mismatch_sqlite_stamped_as_postgres() {
    let dir = tempfile::tempdir().unwrap();
    let storage_path = dir.path().join("backend-kind-mismatch.sqlite");
    stamp_sqlite_as_postgres(&storage_path).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_cc-lb"))
        .arg("serve")
        .env("CC_LB_LISTENER__PROXY_ADDR", "127.0.0.1:0")
        .env("CC_LB_LISTENER__ADMIN_ADDR", "127.0.0.1:0")
        .env("CC_LB_LISTENER__METRICS_ADDR", "127.0.0.1:0")
        .env("CC_LB_STORAGE__KIND", "sqlite")
        .env("CC_LB_STORAGE__PATH", storage_path.display().to_string())
        .env("CC_LB_DATA_DIR", dir.path().display().to_string())
        .env("CC_LB_AEAD__KEY_ENV", "CC_LB_AEAD_KEY")
        .env(
            "CC_LB_AEAD_KEY",
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        )
        .env_remove("CC_LB_MASTER_KEY")
        .output()
        .unwrap();

    assert_eq!(
        output.status.code(),
        Some(2),
        "status={:?} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("backend"), "stderr={stderr}");
    assert!(stderr.contains("mismatch"), "stderr={stderr}");
}

fn stamp_sqlite_as_postgres(path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let database_url = format!("sqlite://{}", path.display());
    Runtime::new()?.block_on(async {
        let storage = open_sqlite(&database_url, Arc::new(cc_lb_engine::SystemClock)).await?;
        storage.initialize(BackendKind::Postgres).await
    })?;
    Ok(())
}
