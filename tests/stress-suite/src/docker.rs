use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const COMMAND_TIMEOUT: Duration = Duration::from_secs(90);
const WAIT_INTERVAL: Duration = Duration::from_millis(20);

pub struct Docker;

pub struct DockerOutput {
    pub stdout: String,
}

#[derive(Debug, thiserror::Error)]
pub enum DockerError {
    #[error("could not start docker {command}: {source}")]
    Spawn {
        command: String,
        #[source]
        source: std::io::Error,
    },
    #[error("docker command timed out: {command}")]
    Timeout { command: String },
    #[error("docker command failed: {command}: {detail}")]
    Failed { command: String, detail: String },
}

impl Docker {
    pub fn run(&self, args: &[String]) -> Result<DockerOutput, DockerError> {
        let command = command_text(args);
        let mut child = Command::new("docker")
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|source| DockerError::Spawn {
                command: command.clone(),
                source,
            })?;
        let deadline = Instant::now() + COMMAND_TIMEOUT;

        loop {
            if child
                .try_wait()
                .map_err(|source| DockerError::Spawn {
                    command: command.clone(),
                    source,
                })?
                .is_some()
            {
                let output = child
                    .wait_with_output()
                    .map_err(|source| DockerError::Spawn {
                        command: command.clone(),
                        source,
                    })?;
                if output.status.success() {
                    return Ok(DockerOutput {
                        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                    });
                }
                return Err(DockerError::Failed {
                    command,
                    detail: output_detail(&output.stderr),
                });
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                return Err(DockerError::Timeout { command });
            }
            thread::sleep(WAIT_INTERVAL);
        }
    }
}

fn command_text(args: &[String]) -> String {
    format!("docker {}", args.join(" "))
}

fn output_detail(stderr: &[u8]) -> String {
    let detail = String::from_utf8_lossy(stderr);
    let trimmed = detail.trim();
    if trimmed.is_empty() {
        "no stderr output".to_owned()
    } else {
        trimmed.chars().take(500).collect()
    }
}
