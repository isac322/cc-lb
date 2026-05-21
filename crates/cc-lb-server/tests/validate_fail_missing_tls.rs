use std::process::Command;

#[test]
fn validate_fail_missing_tls() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("cc-lb.toml");
    std::fs::write(
        &config_path,
        r#"
[listener]

[listener.tls]
cert_path = "/definitely/missing/cert.pem"
key_path = "/definitely/missing/key.pem"
"#,
    )
    .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_cc-lb"))
        .args(["config", "validate", "--config"])
        .arg(&config_path)
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(2), "status={:?}", output.status);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("validation: failed"), "stderr={stderr}");
    assert!(stderr.contains("listener.tls.cert_path"), "stderr={stderr}");
}
