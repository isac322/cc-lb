use std::process::Command;

#[test]
fn missing_aead_key_fatal() {
    let dir = tempfile::tempdir().unwrap();
    let storage_path = dir.path().join("missing-aead-key.sqlite");

    let output = Command::new(env!("CARGO_BIN_EXE_cc-lb"))
        .arg("serve")
        .env("CC_LB_LISTENER__PROXY_ADDR", "127.0.0.1:0")
        .env("CC_LB_LISTENER__ADMIN_ADDR", "127.0.0.1:0")
        .env("CC_LB_LISTENER__METRICS_ADDR", "127.0.0.1:0")
        .env("CC_LB_STORAGE__KIND", "sqlite")
        .env("CC_LB_STORAGE__PATH", storage_path.display().to_string())
        .env("CC_LB_DATA_DIR", dir.path().display().to_string())
        .env("CC_LB_AEAD__KEY_ENV", "CC_LB_AEAD_KEY")
        .env_remove("CC_LB_MASTER_KEY")
        .env_remove("CC_LB_AEAD_KEY")
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
    assert!(
        stderr.contains("AEAD") || stderr.contains("master key"),
        "stderr={stderr}"
    );
}
