use std::path::Path;
use std::process::{Command, ExitCode};

use crate::cli::DummyMode;
use crate::supervisor::{
    OutputArtifact, ProcessRecord, ProcessSpecification, ProcessStatus, Supervisor,
    SupervisorCommand, SupervisorError, SupervisorEvidence, classify,
};
use crate::supervisor_process::{CompletedChild, SupervisorCleanupReceipt, exit_details};
use crate::verdict::Verdict;

const PANIC_MARKER: &str = "panicked";
const SNIPPET_LIMIT: usize = 2_048;

pub fn run_self_check(dummy: DummyMode, output: Option<&Path>) -> ExitCode {
    let Some(output) = output else {
        eprintln!("preflight --self-check=supervisor requires --output");
        return ExitCode::from(2);
    };
    let evidence = run_dummy(dummy, output);
    let evidence = match evidence {
        Ok(evidence) => evidence,
        Err(SupervisorError::Spawn(reason)) => Supervisor::preflight_capability_blocked(&reason),
        Err(error) => {
            eprintln!("supervisor self-check failed: {error}");
            return ExitCode::FAILURE;
        }
    };
    if let Err(error) = evidence
        .validate()
        .and_then(|_| write_evidence(output, &evidence))
    {
        eprintln!(
            "failed to write supervisor output {}: {error}",
            output.display()
        );
        return ExitCode::FAILURE;
    }
    match evidence.verdict {
        Verdict::Pass => ExitCode::SUCCESS,
        Verdict::Fail | Verdict::Blocked | Verdict::NotComparable => ExitCode::FAILURE,
    }
}

pub(crate) fn process_record(
    specification: &ProcessSpecification,
    completed: CompletedChild,
    cleanup: SupervisorCleanupReceipt,
) -> Result<ProcessRecord, SupervisorError> {
    let (exit_status, signal) = exit_details(completed.status);
    let status = match (exit_status, signal) {
        (Some(0), None) => ProcessStatus::ExitedOk,
        (_, Some(_)) => ProcessStatus::Signaled,
        _ => ProcessStatus::ExitedFailed,
    };
    let stderr = String::from_utf8_lossy(&completed.stderr).into_owned();
    let panic_observed = stderr.contains(PANIC_MARKER);
    let verdict = classify(specification.stage, &status, panic_observed);
    Ok(ProcessRecord {
        name: specification.name.clone(),
        stage: specification.stage,
        status,
        panic_observed,
        exit_status,
        signal,
        stdout: output_artifact(
            specification.artifact_prefix.as_deref(),
            "stdout",
            &completed.stdout,
        )?,
        stderr: output_artifact(
            specification.artifact_prefix.as_deref(),
            "stderr",
            &completed.stderr,
        )?,
        cleanup,
        verdict,
    })
}

fn run_dummy(dummy: DummyMode, output: &Path) -> Result<SupervisorEvidence, SupervisorError> {
    let argument = match dummy {
        DummyMode::Ok => "ok",
        DummyMode::Panic => "panic",
    };
    let mut command = Command::new(env!("CARGO"));
    command.args([
        "run",
        "--quiet",
        "--package",
        "cc-lb-stress-suite",
        "--example",
        "stress_dummy",
        "--",
        argument,
    ]);
    Supervisor::run(
        SupervisorCommand::new(
            "stress_dummy",
            crate::supervisor::ProcessStage::Preflight,
            command,
        )
        .with_artifact_prefix(output),
    )
}

fn output_artifact(
    prefix: Option<&Path>,
    stream: &str,
    bytes: &[u8],
) -> Result<OutputArtifact, SupervisorError> {
    let path = match prefix {
        Some(prefix) => Some(write_artifact(prefix, stream, bytes)?),
        None => None,
    };
    Ok(OutputArtifact {
        path,
        snippet: snippet(bytes),
    })
}

fn write_artifact(prefix: &Path, stream: &str, bytes: &[u8]) -> Result<String, SupervisorError> {
    let parent = prefix.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)
        .map_err(|error| SupervisorError::Artifact(error.to_string()))?;
    let name = prefix
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("supervisor");
    let path = parent.join(format!("{name}.{stream}.log"));
    std::fs::write(&path, bytes).map_err(|error| SupervisorError::Artifact(error.to_string()))?;
    Ok(path.display().to_string())
}

fn snippet(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    let truncated: String = text.chars().take(SNIPPET_LIMIT).collect();
    if truncated.len() < text.len() {
        format!("{truncated}...[truncated]")
    } else {
        truncated
    }
}

fn write_evidence(path: &Path, evidence: &SupervisorEvidence) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let bytes = serde_json::to_vec_pretty(evidence).map_err(|error| error.to_string())?;
    std::fs::write(path, bytes).map_err(|error| error.to_string())
}
