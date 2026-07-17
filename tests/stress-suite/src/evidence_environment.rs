use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Deserialize, Serialize)]
pub struct CpuManifest {
    pub count: usize,
    pub model: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ResourceLimits {
    pub cgroup: String,
    pub memory_limit: String,
    pub fd_soft_limit: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct HostLoadSample {
    pub collected_at_unix_ms: u64,
    pub one_minute: f64,
    pub five_minutes: f64,
    pub fifteen_minutes: f64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct EnvironmentManifest {
    pub kernel: String,
    pub tc_version: String,
    pub docker_version: String,
    pub image_digests: BTreeMap<String, String>,
    pub binary_hashes: BTreeMap<String, String>,
    pub git_sha: String,
    pub git_dirty: bool,
    pub cpu: CpuManifest,
    pub limits: ResourceLimits,
    pub host_load_samples: Vec<HostLoadSample>,
}

pub struct EnvironmentCaptureInput<'a> {
    pub repository: &'a Path,
    pub binaries: &'a [PathBuf],
    pub image_digests: &'a BTreeMap<String, String>,
}

impl EnvironmentManifest {
    pub fn synthetic() -> Self {
        Self {
            kernel: "synthetic-kernel".to_owned(),
            tc_version: "iproute2-synthetic".to_owned(),
            docker_version: "docker-synthetic".to_owned(),
            image_digests: BTreeMap::from([("cc-lb".to_owned(), "sha256:synthetic".to_owned())]),
            binary_hashes: BTreeMap::from([("cc-lb".to_owned(), "sha256:synthetic".to_owned())]),
            git_sha: "synthetic".to_owned(),
            git_dirty: false,
            cpu: CpuManifest {
                count: 1,
                model: "synthetic-cpu".to_owned(),
            },
            limits: ResourceLimits {
                cgroup: "synthetic".to_owned(),
                memory_limit: "unlimited".to_owned(),
                fd_soft_limit: "1024".to_owned(),
            },
            host_load_samples: vec![HostLoadSample {
                collected_at_unix_ms: 0,
                one_minute: 0.0,
                five_minutes: 0.0,
                fifteen_minutes: 0.0,
            }],
        }
    }
}

pub fn capture_environment(input: EnvironmentCaptureInput<'_>) -> EnvironmentManifest {
    EnvironmentManifest {
        kernel: command_text("uname", &["-r"]),
        tc_version: command_text("tc", &["-V"]),
        docker_version: command_text("docker", &["--version"]),
        image_digests: input.image_digests.clone(),
        binary_hashes: binary_hashes(input.binaries),
        git_sha: git_text(input.repository, &["rev-parse", "HEAD"]),
        git_dirty: git_dirty(input.repository),
        cpu: CpuManifest {
            count: std::thread::available_parallelism().map_or(0, std::num::NonZero::get),
            model: cpu_model(),
        },
        limits: ResourceLimits {
            cgroup: read_text("/proc/self/cgroup"),
            memory_limit: read_text("/sys/fs/cgroup/memory.max"),
            fd_soft_limit: fd_soft_limit(),
        },
        host_load_samples: vec![host_load_sample()],
    }
}

fn binary_hashes(paths: &[PathBuf]) -> BTreeMap<String, String> {
    paths
        .iter()
        .map(|path| (path.display().to_string(), hash_file(path)))
        .collect()
}

fn hash_file(path: &Path) -> String {
    let Ok(bytes) = std::fs::read(path) else {
        return "unavailable".to_owned();
    };
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

fn command_text(program: &str, args: &[&str]) -> String {
    let mut command = Command::new(program);
    command.args(args);
    run_command(&mut command)
}

fn git_text(repository: &Path, args: &[&str]) -> String {
    let mut command = Command::new("git");
    command.args(args).current_dir(repository);
    run_command(&mut command)
}

fn run_command(command: &mut Command) -> String {
    command
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|text| text.trim().to_owned())
        .filter(|text| !text.is_empty())
        .unwrap_or_else(|| "unavailable".to_owned())
}

fn git_dirty(repository: &Path) -> bool {
    let mut command = Command::new("git");
    command
        .args(["status", "--porcelain"])
        .current_dir(repository);
    match command.output() {
        Ok(output) if output.status.success() => !output.stdout.is_empty(),
        Ok(_) | Err(_) => false,
    }
}

fn read_text(path: &str) -> String {
    std::fs::read_to_string(path)
        .ok()
        .map(|text| text.trim().to_owned())
        .filter(|text| !text.is_empty())
        .unwrap_or_else(|| "unavailable".to_owned())
}

fn cpu_model() -> String {
    std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|text| {
            text.lines().find_map(|line| {
                line.split_once(':').and_then(|(name, value)| {
                    (name.trim() == "model name").then(|| value.trim().to_owned())
                })
            })
        })
        .unwrap_or_else(|| "unavailable".to_owned())
}

fn fd_soft_limit() -> String {
    std::fs::read_to_string("/proc/self/limits")
        .ok()
        .and_then(|text| {
            text.lines().find_map(|line| {
                line.starts_with("Max open files")
                    .then(|| line.split_whitespace().nth(3).map(str::to_owned))
                    .flatten()
            })
        })
        .unwrap_or_else(|| "unavailable".to_owned())
}

fn host_load_sample() -> HostLoadSample {
    let values = std::fs::read_to_string("/proc/loadavg")
        .ok()
        .map(|text| {
            text.split_whitespace()
                .take(3)
                .map(|value| value.parse::<f64>().unwrap_or(0.0))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    HostLoadSample {
        collected_at_unix_ms: unix_millis(),
        one_minute: values.first().copied().unwrap_or(0.0),
        five_minutes: values.get(1).copied().unwrap_or(0.0),
        fifteen_minutes: values.get(2).copied().unwrap_or(0.0),
    }
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| u64::try_from(elapsed.as_millis()).ok())
        .unwrap_or_default()
}
