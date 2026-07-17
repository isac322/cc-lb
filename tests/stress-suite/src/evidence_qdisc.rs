use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct QdiscCommand {
    pub command: String,
    pub started_at_unix_ms: u64,
    pub completed_at_unix_ms: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct QdiscStats {
    pub edge: String,
    pub device: String,
    pub command: QdiscCommand,
    pub scheduled_at_unix_ms: u64,
    pub schedule_drift_ms: u64,
    pub bytes: u64,
    pub packets: u64,
    pub dropped: u64,
    pub overlimits: u64,
    pub requeues: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct NetemEvidence {
    pub qdisc: Vec<QdiscStats>,
}

pub struct QdiscCapture<'a> {
    pub edge: &'a str,
    pub device: &'a str,
    pub output: &'a str,
    pub scheduled_at_unix_ms: u64,
    pub command_started_at_unix_ms: u64,
    pub command_completed_at_unix_ms: u64,
}

pub fn capture_qdisc(
    edge: &str,
    device: &str,
    scheduled_at_unix_ms: u64,
) -> Result<QdiscStats, String> {
    let command_started_at_unix_ms = unix_millis()?;
    let output = Command::new("tc")
        .args(["-s", "qdisc", "show", "dev", device])
        .output()
        .map_err(|error| format!("run tc qdisc stats: {error}"))?;
    let command_completed_at_unix_ms = unix_millis()?;
    if !output.status.success() {
        return Err(format!("tc qdisc stats exited with {}", output.status));
    }
    let text =
        String::from_utf8(output.stdout).map_err(|error| format!("decode tc output: {error}"))?;
    record_qdisc(QdiscCapture {
        edge,
        device,
        output: &text,
        scheduled_at_unix_ms,
        command_started_at_unix_ms,
        command_completed_at_unix_ms,
    })
}

pub fn record_qdisc(capture: QdiscCapture<'_>) -> Result<QdiscStats, String> {
    let sent = capture
        .output
        .lines()
        .map(str::trim_start)
        .find(|line| line.starts_with("Sent "))
        .ok_or_else(|| "tc qdisc output has no Sent counters".to_owned())?;
    let bytes = counter_after(sent, "Sent ").ok_or_else(|| "tc qdisc bytes missing".to_owned())?;
    let packets =
        counter_after(sent, "bytes ").ok_or_else(|| "tc qdisc packets missing".to_owned())?;
    let dropped = counter_after(sent, "dropped ").unwrap_or_default();
    let overlimits = counter_after(sent, "overlimits ").unwrap_or_default();
    let requeues = counter_after(sent, "requeues ").unwrap_or_default();
    Ok(QdiscStats {
        edge: capture.edge.to_owned(),
        device: capture.device.to_owned(),
        command: QdiscCommand {
            command: format!("tc -s qdisc show dev {}", capture.device),
            started_at_unix_ms: capture.command_started_at_unix_ms,
            completed_at_unix_ms: capture.command_completed_at_unix_ms,
        },
        scheduled_at_unix_ms: capture.scheduled_at_unix_ms,
        schedule_drift_ms: capture
            .command_completed_at_unix_ms
            .saturating_sub(capture.scheduled_at_unix_ms),
        bytes,
        packets,
        dropped,
        overlimits,
        requeues,
    })
}

fn counter_after(line: &str, marker: &str) -> Option<u64> {
    line.split_once(marker)?
        .1
        .split(|character: char| !character.is_ascii_digit())
        .next()?
        .parse()
        .ok()
}

fn unix_millis() -> Result<u64, String> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("read clock: {error}"))?;
    u64::try_from(elapsed.as_millis()).map_err(|error| format!("convert clock: {error}"))
}
