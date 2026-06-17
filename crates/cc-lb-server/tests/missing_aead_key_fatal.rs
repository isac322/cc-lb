use std::process::Command;

#[test]
fn missing_aead_key_fatal() {
    let dir = tempfile::tempdir().unwrap();
    let storage_path = dir.path().join("missing-aead-key.sqlite");
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
kind = "sqlite"
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
