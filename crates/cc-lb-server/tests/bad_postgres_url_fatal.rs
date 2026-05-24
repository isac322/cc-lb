use std::process::Command;

#[cfg(feature = "postgres")]
#[test]
fn bad_postgres_url_fatal() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("cc-lb.toml");
    std::fs::write(
        &config_path,
        r#"
[storage]
kind = "postgres"
url = "postgres://user:secret@127.0.0.2:65499/testdb"

[storage.pool]
acquire_timeout_secs = 3

[aead]
key_env = "CC_LB_AEAD_KEY"
"#,
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

    assert!(!output.status.success(), "status={:?}", output.status);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains("secret"), "stderr={stderr}");
    assert!(stderr.contains("127.0.0.2"), "stderr={stderr}");
}
