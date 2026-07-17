use std::path::{Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::supervisor_process::{SupervisedChild, SupervisorCleanupReceipt};
use crate::topology_decision::BlockedStage;
use crate::verdict::Verdict;

pub const SUPERVISOR_SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessStage {
    Preflight,
    Traffic,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessStatus {
    ExitedOk,
    ExitedFailed,
    Signaled,
    CapabilityUnavailable,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OutputArtifact {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    pub snippet: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessRecord {
    pub name: String,
    pub stage: ProcessStage,
    pub status: ProcessStatus,
    pub panic_observed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_status: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    pub stdout: OutputArtifact,
    pub stderr: OutputArtifact,
    pub cleanup: SupervisorCleanupReceipt,
    pub verdict: Verdict,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SupervisorEvidence {
    pub schema_version: u16,
    pub verdict: Verdict,
    pub processes: Vec<ProcessRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_stage: Option<BlockedStage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<String>,
}

pub struct SupervisorCommand {
    process: ProcessSpecification,
    command: Command,
}

pub(crate) struct ProcessSpecification {
    pub(crate) name: String,
    pub(crate) stage: ProcessStage,
    pub(crate) artifact_prefix: Option<PathBuf>,
}

pub struct Supervisor;

#[derive(Debug, Error)]
pub enum SupervisorError {
    #[error("spawn child: {0}")]
    Spawn(String),
    #[error("observe child: {0}")]
    Observe(String),
    #[error("write artifact: {0}")]
    Artifact(String),
}

impl SupervisorCommand {
    pub fn new(name: &str, stage: ProcessStage, command: Command) -> Self {
        Self {
            process: ProcessSpecification {
                name: name.to_owned(),
                stage,
                artifact_prefix: None,
            },
            command,
        }
    }

    pub fn with_artifact_prefix(mut self, output: &Path) -> Self {
        self.process.artifact_prefix = Some(output.to_path_buf());
        self
    }

    pub(crate) fn into_parts(self) -> (ProcessSpecification, Command) {
        (self.process, self.command)
    }
}

impl Supervisor {
    pub fn spawn(command: Command) -> Result<SupervisedChild, SupervisorError> {
        SupervisedChild::spawn(command).map_err(SupervisorError::Spawn)
    }

    pub fn run(request: SupervisorCommand) -> Result<SupervisorEvidence, SupervisorError> {
        let (specification, command) = request.into_parts();
        let mut child = Self::spawn(command)?;
        let completed = child.wait().map_err(SupervisorError::Observe);
        let cleanup = child.cleanup();
        let process =
            crate::supervisor_runner::process_record(&specification, completed?, cleanup)?;
        Ok(SupervisorEvidence {
            schema_version: SUPERVISOR_SCHEMA_VERSION,
            verdict: process.verdict,
            processes: vec![process],
            blocked_stage: None,
            blocked_reason: None,
        })
    }

    pub fn preflight_capability_blocked(reason: &str) -> SupervisorEvidence {
        let process = ProcessRecord {
            name: "supervisor".to_owned(),
            stage: ProcessStage::Preflight,
            status: ProcessStatus::CapabilityUnavailable,
            panic_observed: false,
            exit_status: None,
            signal: None,
            stdout: OutputArtifact {
                path: None,
                snippet: String::new(),
            },
            stderr: OutputArtifact {
                path: None,
                snippet: reason.to_owned(),
            },
            cleanup: SupervisorCleanupReceipt {
                attempted: false,
                graceful_signal_sent: false,
                force_killed: false,
                child_reaped: false,
            },
            verdict: Verdict::Blocked,
        };
        SupervisorEvidence {
            schema_version: SUPERVISOR_SCHEMA_VERSION,
            verdict: Verdict::Blocked,
            processes: vec![process],
            blocked_stage: Some(BlockedStage::Preflight),
            blocked_reason: Some(reason.to_owned()),
        }
    }
}

impl SupervisorEvidence {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != SUPERVISOR_SCHEMA_VERSION {
            return Err(format!(
                "unsupported supervisor schema {}",
                self.schema_version
            ));
        }
        for process in &self.processes {
            if process.verdict != classify(process.stage, &process.status, process.panic_observed) {
                return Err("process verdict disagrees with observed status".to_owned());
            }
        }
        match self.verdict {
            Verdict::Blocked if self.blocked_stage == Some(BlockedStage::Preflight) => Ok(()),
            Verdict::Blocked => {
                Err("blocked supervisor evidence requires preflight stage".to_owned())
            }
            Verdict::Pass | Verdict::Fail if self.blocked_stage.is_none() => Ok(()),
            Verdict::Pass | Verdict::Fail => {
                Err("non-blocked evidence cannot name a blocked stage".to_owned())
            }
            Verdict::NotComparable => Err("supervisor does not emit NOT_COMPARABLE".to_owned()),
        }
    }
}

pub(crate) fn classify(
    stage: ProcessStage,
    status: &ProcessStatus,
    panic_observed: bool,
) -> Verdict {
    match (stage, status, panic_observed) {
        (ProcessStage::Preflight, ProcessStatus::CapabilityUnavailable, false) => Verdict::Blocked,
        (_, _, true) => Verdict::Fail,
        (_, ProcessStatus::ExitedOk, false) => Verdict::Pass,
        (_, ProcessStatus::ExitedFailed | ProcessStatus::Signaled, false) => Verdict::Fail,
        (ProcessStage::Traffic, ProcessStatus::CapabilityUnavailable, false) => Verdict::Fail,
    }
}
