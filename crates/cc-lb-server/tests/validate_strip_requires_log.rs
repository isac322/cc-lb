use std::path::Path;
use std::process::{Command, Output};

fn validate(config_toml: &str, envs: &[(&str, &str)]) -> Output {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("cc-lb.toml");
    std::fs::write(&config_path, config_toml).unwrap();
    run_validate(&config_path, envs)
}

fn run_validate(config_path: &Path, envs: &[(&str, &str)]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_cc-lb"))
        .args(["config", "validate", "--config"])
        .arg(config_path)
        .env_remove("CC_LB_OBSERVABILITY__REQUEST_DIAGNOSTICS_LOG")
        .env_remove("CC_LB_REQUEST_EVENT_STRIP_DIAGNOSTICS")
        .envs(envs.iter().copied())
        .output()
        .unwrap()
}

#[test]
fn validate_rejects_strip_without_diagnostics_log() {
    let output = validate(
        r#"
request_event_strip_diagnostics = true

[listener]
"#,
        &[],
    );

    assert_eq!(output.status.code(), Some(2), "status={:?}", output.status);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("validation: failed"), "stderr={stderr}");
    assert!(
        stderr.contains("request_event_strip_diagnostics"),
        "stderr={stderr}"
    );
}

#[test]
fn validate_rejects_strip_enabled_only_through_env() {
    let output = validate(
        r#"
[listener]
"#,
        &[("CC_LB_REQUEST_EVENT_STRIP_DIAGNOSTICS", "true")],
    );

    assert_eq!(output.status.code(), Some(2), "status={:?}", output.status);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("request_event_strip_diagnostics"),
        "stderr={stderr}"
    );
}

#[test]
fn validate_accepts_strip_with_diagnostics_log() {
    let output = validate(
        r#"
request_event_strip_diagnostics = true

[listener]

[observability]
request_diagnostics_log = true
"#,
        &[],
    );

    assert!(
        output.status.success(),
        "status={:?} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("validation: ok"), "stdout={stdout}");
}

#[test]
fn validate_accepts_strip_with_diagnostics_log_from_env() {
    let output = validate(
        r#"
request_event_strip_diagnostics = true

[listener]
"#,
        &[("CC_LB_OBSERVABILITY__REQUEST_DIAGNOSTICS_LOG", "true")],
    );

    assert!(
        output.status.success(),
        "status={:?} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
}
