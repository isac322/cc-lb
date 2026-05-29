use std::path::Path;
use std::process::Command;

use cc_lb_storage_redb::{CURRENT_SCHEMA_VERSION, META_BACKEND_KIND_V1, SCHEMA_VERSION_V1};

#[test]
fn backend_kind_mismatch_redb_stamped_as_postgres() {
    let dir = tempfile::tempdir().unwrap();
    let storage_path = dir.path().join("backend-kind-mismatch.redb");
    stamp_redb_as_postgres(&storage_path).unwrap();

    let config_path = dir.path().join("cc-lb.toml");
    std::fs::write(
        &config_path,
        format!(
            r#"
[listener]
proxy_addr = "127.0.0.1:0"
admin_addr = "127.0.0.1:0"
metrics_addr = "127.0.0.1:0"

[storage]
kind = "redb"
path = "{}"

[aead]
key_env = "CC_LB_AEAD_KEY"
"#,
            storage_path.display()
        ),
    )
    .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_cc-lb"))
        .args(["serve", "--config"])
        .arg(&config_path)
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

fn stamp_redb_as_postgres(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let db = redb::Database::create(path)?;
    let write_txn = db.begin_write()?;
    {
        let mut schema = write_txn.open_table(SCHEMA_VERSION_V1)?;
        schema.insert("version", &CURRENT_SCHEMA_VERSION)?;
    }
    {
        let mut backend = write_txn.open_table(META_BACKEND_KIND_V1)?;
        backend.insert("backend_kind", "postgres")?;
    }
    write_txn.commit()?;
    Ok(())
}
