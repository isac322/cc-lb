use std::process::Command;

#[test]
fn validate_ok_config() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("cc-lb.toml");
    std::fs::write(
        &config_path,
        r#"
[listener]
"#,
    )
    .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_cc-lb"))
        .args(["config", "validate", "--config"])
        .arg(&config_path)
        .output()
        .unwrap();

    assert!(output.status.success(), "status={:?}", output.status);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("validation: ok"), "stdout={stdout}");
    assert!(stdout.contains("preflight: ok"), "stdout={stdout}");
}
