use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::supervisor::{ProcessStage, Supervisor, SupervisorCommand};
use crate::supervisor_process::SupervisedChild;
use crate::topology_decision::BlockedStage;
use crate::verdict::Verdict;

#[test]
#[cfg(unix)]
fn supervisor_classifies_panic_fail() {
    // Given: a child that emits Rust panic text and exits unsuccessfully.
    let command = shell_command("printf \"thread 'main' panicked\\n\" >&2; exit 101");

    // When: the preflight supervisor records the completed child.
    let evidence = Supervisor::run(SupervisorCommand::new(
        "panic",
        ProcessStage::Preflight,
        command,
    ))
    .expect("supervisor should capture the child");

    // Then: panic evidence is a failure, never a capability block.
    assert_eq!(evidence.verdict, Verdict::Fail);
    assert!(evidence.processes[0].panic_observed);
    assert_eq!(evidence.processes[0].exit_status, Some(101));
    assert!(evidence.blocked_stage.is_none());
}

#[test]
fn supervisor_preflight_capability_blocked() {
    // Given: the required child-program capability is unavailable before traffic.
    let evidence = Supervisor::preflight_capability_blocked("child program is unavailable");

    // When: its evidence is classified.
    let verdict = evidence.verdict;

    // Then: only this preflight capability failure is blocked.
    assert_eq!(verdict, Verdict::Blocked);
    assert_eq!(evidence.blocked_stage, Some(BlockedStage::Preflight));
}

#[test]
#[cfg(unix)]
fn supervisor_posttraffic_child_death_fail() {
    // Given: a traffic child that exits unsuccessfully without panic text.
    let command = shell_command("exit 17");

    // When: the supervisor observes its post-preflight death.
    let evidence = Supervisor::run(SupervisorCommand::new(
        "traffic",
        ProcessStage::Traffic,
        command,
    ))
    .expect("supervisor should capture the child");

    // Then: a post-traffic death is a failure rather than blocked.
    assert_eq!(evidence.verdict, Verdict::Fail);
    assert!(evidence.blocked_stage.is_none());
    assert_eq!(evidence.processes[0].exit_status, Some(17));
}

#[test]
#[cfg(unix)]
fn supervisor_cleanup_kills_children() {
    // Given: a child that ignores TERM and does not exit on its own.
    let ready_path = unique_ready_path();
    let command = shell_command_with_arg(
        "trap '' TERM; : > \"$1\"; while :; do :; done",
        ready_path.as_path(),
    );
    let mut child = Supervisor::spawn(command).expect("child should spawn");
    wait_for_ready(ready_path.as_path());

    // When: cleanup is requested.
    let cleanup = child.cleanup();
    let _ = std::fs::remove_file(&ready_path);

    // Then: the child is forcibly reaped with a recorded cleanup receipt.
    assert!(cleanup.attempted);
    assert!(cleanup.graceful_signal_sent);
    assert!(cleanup.force_killed);
    assert!(cleanup.child_reaped);
}

#[test]
#[cfg(unix)]
fn readiness_diagnostic_includes_child_launch_and_stderr() {
    let mut child = SupervisedChild::spawn(shell_command("printf readiness-failure >&2; exit 17"))
        .expect("child should spawn");
    let _ = child.wait().expect("child should exit");

    let diagnostic = child.readiness_diagnostic();

    assert!(diagnostic.contains("pid="));
    assert!(diagnostic.contains("program="));
    assert!(diagnostic.contains("cwd="));
    assert!(diagnostic.contains("stderr=readiness-failure"));
}

#[cfg(unix)]
fn shell_command(script: &str) -> Command {
    let mut command = Command::new("sh");
    command.args(["-c", script]);
    command
}

#[cfg(unix)]
fn shell_command_with_arg(script: &str, argument: &Path) -> Command {
    let mut command = Command::new("sh");
    command.args(["-c", script, "sh"]);
    command.arg(argument);
    command
}

#[cfg(unix)]
fn unique_ready_path() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "cc-lb-supervisor-ready-{}-{nanos}",
        std::process::id()
    ))
}

#[cfg(unix)]
fn wait_for_ready(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "child did not install TERM trap before deadline"
        );
        std::thread::yield_now();
    }
}
