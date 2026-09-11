use std::io::Read;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::process::{CommandExt as _, ExitStatusExt as _};

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct SupervisorCleanupReceipt {
    pub attempted: bool,
    pub graceful_signal_sent: bool,
    pub force_killed: bool,
    pub child_reaped: bool,
}

pub struct CompletedChild {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

pub struct SupervisedChild {
    child: Child,
    launch: String,
    stdout: Capture,
    stderr: Capture,
    stdout_thread: Option<JoinHandle<()>>,
    stderr_thread: Option<JoinHandle<()>>,
    child_reaped: bool,
}

#[derive(Default)]
struct CapturedOutput {
    bytes: Mutex<Vec<u8>>,
    changed: Condvar,
}

type Capture = Arc<CapturedOutput>;

impl SupervisedChild {
    pub fn spawn(mut command: Command) -> Result<Self, String> {
        let launch = command_diagnostic(&command);
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command.spawn().map_err(|error| error.to_string())?;
        let Some(stdout_pipe) = child.stdout.take() else {
            reap_child(&mut child);
            return Err("spawned child has no stdout pipe".to_owned());
        };
        let Some(stderr_pipe) = child.stderr.take() else {
            reap_child(&mut child);
            return Err("spawned child has no stderr pipe".to_owned());
        };
        let stdout = Arc::new(CapturedOutput::default());
        let stderr = Arc::new(CapturedOutput::default());
        let stdout_thread = capture_thread(stdout_pipe, Arc::clone(&stdout));
        let stderr_thread = capture_thread(stderr_pipe, Arc::clone(&stderr));
        Ok(Self {
            child,
            launch,
            stdout,
            stderr,
            stdout_thread: Some(stdout_thread),
            stderr_thread: Some(stderr_thread),
            child_reaped: false,
        })
    }

    pub fn wait(&mut self) -> Result<CompletedChild, String> {
        let status = self.child.wait().map_err(|error| error.to_string())?;
        self.child_reaped = true;
        let (stdout, stderr) = self.finish_capture();
        Ok(CompletedChild {
            status,
            stdout,
            stderr,
        })
    }

    pub fn cleanup(&mut self) -> SupervisorCleanupReceipt {
        let mut receipt = SupervisorCleanupReceipt {
            attempted: true,
            graceful_signal_sent: false,
            force_killed: false,
            child_reaped: false,
        };
        if self.child_reaped {
            receipt.child_reaped = true;
            self.finish_capture();
            return receipt;
        }
        match self.child.try_wait() {
            Ok(Some(_)) => {
                self.child_reaped = true;
                receipt.child_reaped = true;
            }
            Ok(None) | Err(_) => {
                receipt.graceful_signal_sent = terminate_process_group(self.child.id(), "TERM");
                match self.child.try_wait() {
                    Ok(Some(_)) => {
                        self.child_reaped = true;
                        receipt.child_reaped = true;
                    }
                    Ok(None) | Err(_) => {
                        receipt.force_killed = kill_process_group(&mut self.child);
                        receipt.child_reaped = self.child.wait().is_ok();
                        self.child_reaped = receipt.child_reaped;
                    }
                }
            }
        }
        self.finish_capture();
        receipt
    }

    pub fn readiness_diagnostic(&mut self) -> String {
        let status = self
            .child
            .try_wait()
            .map(|status| format!("{status:?}"))
            .unwrap_or_else(|error| format!("try_wait_error={error}"));
        format!(
            "pid={}; {}; status={status}; stdout={}; stderr={}",
            self.child.id(),
            self.launch,
            String::from_utf8_lossy(&capture_bytes(&self.stdout)).trim(),
            String::from_utf8_lossy(&capture_bytes(&self.stderr)).trim(),
        )
    }

    #[allow(
        dead_code,
        reason = "used by supervisor integration tests that are not linked into every binary target"
    )]
    pub(crate) fn wait_for_stdout(&self, expected: &[u8], timeout: Duration) -> bool {
        let Ok(bytes) = self.stdout.bytes.lock() else {
            return false;
        };
        let Ok((bytes, _)) = self
            .stdout
            .changed
            .wait_timeout_while(bytes, timeout, |bytes| {
                !bytes
                    .windows(expected.len())
                    .any(|window| window == expected)
            })
        else {
            return false;
        };
        bytes
            .windows(expected.len())
            .any(|window| window == expected)
    }

    fn finish_capture(&mut self) -> (Vec<u8>, Vec<u8>) {
        if let Some(thread) = self.stdout_thread.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.stderr_thread.take() {
            let _ = thread.join();
        }
        (capture_bytes(&self.stdout), capture_bytes(&self.stderr))
    }
}

fn command_diagnostic(command: &Command) -> String {
    let args = command
        .get_args()
        .map(|argument| argument.to_string_lossy())
        .collect::<Vec<_>>()
        .join(" ");
    let cwd = command
        .get_current_dir()
        .map(|path| path.display().to_string())
        .or_else(|| {
            std::env::current_dir()
                .ok()
                .map(|path| path.display().to_string())
        })
        .unwrap_or_else(|| "<unknown>".to_owned());
    let overrides = command
        .get_envs()
        .map(|(key, value)| match value {
            Some(_) => key.to_string_lossy().into_owned(),
            None => format!("{}=<removed>", key.to_string_lossy()),
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "program={}; args={args}; cwd={cwd}; env=inherited; env_overrides=[{overrides}]",
        command.get_program().to_string_lossy()
    )
}

impl Drop for SupervisedChild {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

pub fn exit_details(status: ExitStatus) -> (Option<i32>, Option<i32>) {
    #[cfg(unix)]
    {
        (status.code(), status.signal())
    }
    #[cfg(not(unix))]
    {
        (status.code(), None)
    }
}

fn capture_thread<R>(mut pipe: R, capture: Capture) -> JoinHandle<()>
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let mut buffer = [0_u8; 4096];
        loop {
            let read = match pipe.read(&mut buffer) {
                Ok(0) | Err(_) => break,
                Ok(read) => read,
            };
            if let Ok(mut target) = capture.bytes.lock() {
                target.extend_from_slice(&buffer[..read]);
                capture.changed.notify_all();
            }
        }
    })
}

fn capture_bytes(capture: &Capture) -> Vec<u8> {
    capture
        .bytes
        .lock()
        .map(|bytes| bytes.clone())
        .unwrap_or_default()
}

fn reap_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

#[cfg(unix)]
fn terminate_process_group(pid: u32, signal: &str) -> bool {
    Command::new("kill")
        .args([format!("-{signal}"), "--".to_owned(), format!("-{pid}")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

#[cfg(not(unix))]
fn terminate_process_group(_pid: u32, _signal: &str) -> bool {
    false
}

#[cfg(unix)]
fn kill_process_group(child: &mut Child) -> bool {
    terminate_process_group(child.id(), "KILL")
}

#[cfg(not(unix))]
fn kill_process_group(child: &mut Child) -> bool {
    child.kill().is_ok()
}
