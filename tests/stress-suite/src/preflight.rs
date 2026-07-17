use std::path::Path;
use std::process::ExitCode;

use crate::docker::{Docker, DockerError};
use crate::fabric;
use crate::fabric_state::{FabricCleanup, ResourceNames};
use crate::preflight_evidence::{CapabilityFailure, PreflightEvidence};
use crate::topology_decision::CleanupReceipt;
use crate::verdict::Verdict;

pub fn run_docker_netem(run_id: &str, output: &Path) -> ExitCode {
    let evidence = docker_netem_evidence(run_id);
    if let Err(error) = evidence
        .validate()
        .and_then(|_| write_evidence(output, &evidence))
    {
        eprintln!(
            "failed to write preflight output {}: {error}",
            output.display()
        );
        return ExitCode::FAILURE;
    }
    match evidence.verdict {
        Verdict::Pass => ExitCode::SUCCESS,
        Verdict::Blocked | Verdict::Fail | Verdict::NotComparable => ExitCode::FAILURE,
    }
}

fn docker_netem_evidence(run_id: &str) -> PreflightEvidence {
    let names = match ResourceNames::new(run_id) {
        Ok(names) => names,
        Err(reason) => {
            return PreflightEvidence::capability_blocked(
                run_id,
                CapabilityFailure::docker(reason),
                CleanupReceipt::empty(),
            );
        }
    };
    let docker = Docker;
    if let Err(error) = docker.run(&[
        "version".to_owned(),
        "--format".to_owned(),
        "{{.Server.Version}}".to_owned(),
    ]) {
        let mut cleanup = FabricCleanup::new(&docker, &names, run_id);
        let receipt = cleanup.cleanup();
        return PreflightEvidence::capability_blocked(
            run_id,
            CapabilityFailure::docker(docker_error_reason(error)),
            receipt,
        );
    }
    PreflightEvidence::from_decision(fabric::prove_with_names(&docker, &names, run_id))
}

fn docker_error_reason(error: DockerError) -> String {
    format!("Docker daemon is unreachable: {error}")
}

fn write_evidence(path: &Path, evidence: &PreflightEvidence) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("create output directory: {error}"))?;
    }
    let bytes = serde_json::to_vec_pretty(evidence)
        .map_err(|error| format!("serialize preflight evidence: {error}"))?;
    std::fs::write(path, bytes).map_err(|error| format!("write output file: {error}"))
}
