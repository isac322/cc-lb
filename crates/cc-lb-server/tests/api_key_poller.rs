//! `cc-lb config validate` for the API-key usage poll block (QA POL-09).
//! The poller itself is exercised through the dispatch arm in
//! `src/scheduler_dispatch/api_key_usage/tests.rs`.

use std::process::{Command, Output};

fn validate(toml: &str) -> Output {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = dir.path().join("cc-lb.toml");
    std::fs::write(&config_path, toml).expect("write config");
    Command::new(env!("CARGO_BIN_EXE_cc-lb"))
        .args(["config", "validate", "--config"])
        .arg(&config_path)
        .output()
        .expect("run cc-lb config validate")
}

/// POL-09: 60 s is accepted.
#[test]
fn pol09_validate_accepts_sixty_second_poll() {
    let output = validate(
        "[listener]\n\n[scheduler.recurring_jobs.api_key_usage_poll]\ninterval_secs = 60\njitter_secs = 15\n",
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "status={:?} stdout={stdout}",
        output.status
    );
    assert!(stdout.contains("validation: ok"), "stdout={stdout}");
}

/// POL-09: an interval below 60 s, a jitter not below the interval, and an
/// unknown field each fail and name the offending field.
#[test]
fn pol09_validate_rejects_out_of_bounds_poll() {
    for (block, expected) in [
        (
            "interval_secs = 59\njitter_secs = 0\n",
            "scheduler.recurring_jobs.api_key_usage_poll.interval_secs",
        ),
        (
            "interval_secs = 300\njitter_secs = 300\n",
            "scheduler.recurring_jobs.api_key_usage_poll.jitter_secs",
        ),
        ("api_key_env = \"ANTHROPIC_API_KEY\"\n", "api_key_env"),
    ] {
        let output = validate(&format!(
            "[listener]\n\n[scheduler.recurring_jobs.api_key_usage_poll]\n{block}"
        ));

        let combined = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!output.status.success(), "{block}: {combined}");
        assert!(combined.contains(expected), "{block}: {combined}");
    }
}
