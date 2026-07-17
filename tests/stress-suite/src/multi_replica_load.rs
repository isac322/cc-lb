use crate::docker::Docker;
use crate::fabric_state::{ResourceNames, label};
use crate::multi_replica_config::ReplicaPorts;

#[derive(Clone, Copy)]
pub(crate) struct FabricRef<'a> {
    pub docker: &'a Docker,
    pub names: &'a ResourceNames,
    pub run_id: &'a str,
}

pub(crate) const MESSAGES_BODY: &str =
    r#"{"model":"fake-alpha","max_tokens":8,"messages":[{"role":"user","content":"stress"}]}"#;
const TIMED_LOAD_ENV: &str = "CC_LB_STRESS_TIMED_LOAD";
const LOAD_WORKERS_ENV: &str = "CC_LB_STRESS_LOAD_WORKERS";
const LOAD_RAMP_ENV: &str = "CC_LB_STRESS_LOAD_RAMP";

#[derive(Clone, Copy, Debug, Default)]
pub struct BatchResult {
    pub attempted: u64,
    pub completed: u64,
    pub failed: u64,
}

pub fn replica_request_count(total: u64, replica_index: usize, replica_count: usize) -> u64 {
    let replicas = u64::try_from(replica_count).unwrap_or(u64::MAX).max(1);
    let index = u64::try_from(replica_index).unwrap_or(u64::MAX);
    let base = total / replicas;
    let remainder = total % replicas;
    base + u64::from(index < remainder)
}

pub fn request_wave_via_fabric(
    fabric: FabricRef,
    replica_ports: &[ReplicaPorts],
    key: &str,
    target_per_wave: u64,
    duration_secs: u64,
    wave_index: usize,
) -> Result<Vec<(usize, BatchResult)>, String> {
    if timed_load_enabled() {
        return crate::multi_replica_load_timed::request_timed_wave_via_fabric(
            fabric.names,
            fabric.run_id,
            replica_ports,
            key,
            duration_secs,
            load_worker_count_for_wave(wave_index),
        );
    }
    replica_ports
        .iter()
        .enumerate()
        .map(|(index, ports)| {
            let count = replica_request_count(target_per_wave, index, replica_ports.len());
            let result = request_count_batch_via_fabric(fabric, ports.proxy, key, count)?;
            if result.attempted != count {
                return Err(format!(
                    "replica-{index} attempted {} requests, expected {count}",
                    result.attempted
                ));
            }
            Ok((index, result))
        })
        .collect()
}

pub fn timed_load_enabled() -> bool {
    std::env::var(TIMED_LOAD_ENV).as_deref() == Ok("1")
}

pub fn load_worker_count() -> u64 {
    std::env::var(LOAD_WORKERS_ENV)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(16)
}

pub fn load_worker_count_for_wave(wave_index: usize) -> u64 {
    let base = load_worker_count();
    if std::env::var(LOAD_RAMP_ENV).as_deref() != Ok("1") {
        return base;
    }
    let exponent = u32::try_from(wave_index).unwrap_or(u32::MAX).min(10);
    base.saturating_mul(2_u64.saturating_pow(exponent))
        .min(1_024)
}

fn request_count_batch_via_fabric(
    fabric: FabricRef,
    port: u16,
    key: &str,
    count: u64,
) -> Result<BatchResult, String> {
    let output = fabric
        .docker
        .run(&[
            "run".to_owned(),
            "--rm".to_owned(),
            "--label".to_owned(),
            label(fabric.run_id),
            "--network".to_owned(),
            fabric.names.network.clone(),
            "--add-host".to_owned(),
            "host.docker.internal:host-gateway".to_owned(),
            fabric.names.helper_image.clone(),
            "sh".to_owned(),
            "-ec".to_owned(),
            batch_script().to_owned(),
            "ccstress-load".to_owned(),
            count.to_string(),
            format!("http://host.docker.internal:{port}/v1/messages"),
            key.to_owned(),
            MESSAGES_BODY.to_owned(),
        ])
        .map_err(|error| error.to_string())?;
    parse_batch_output(&output.stdout)
}

fn batch_script() -> &'static str {
    r#"
count="$1"
url="$2"
key="$3"
body="$4"
ok=0
fail=0
i=0
while [ "$i" -lt "$count" ]; do
  code=$(curl --max-time 10 -sS -o /dev/null -w '%{http_code}' \
    -H "x-api-key: $key" \
    -H 'anthropic-version: 2023-06-01' \
    -H 'content-type: application/json' \
    --data "$body" "$url" || true)
  if [ "$code" = "200" ]; then
    ok=$((ok + 1))
  else
    fail=$((fail + 1))
  fi
  i=$((i + 1))
done
printf 'attempted=%s completed=%s failed=%s\n' "$count" "$ok" "$fail"
test "$fail" -eq 0
"#
}

pub(crate) fn parse_batch_output(output: &str) -> Result<BatchResult, String> {
    let line = output
        .lines()
        .find(|line| line.starts_with("attempted="))
        .ok_or_else(|| "missing batch result line".to_owned())?;
    let mut attempted = None;
    let mut completed = None;
    let mut failed = None;
    for field in line.split_whitespace() {
        let Some((name, value)) = field.split_once('=') else {
            continue;
        };
        let parsed = value.parse::<u64>().map_err(|error| error.to_string())?;
        match name {
            "attempted" => attempted = Some(parsed),
            "completed" => completed = Some(parsed),
            "failed" => failed = Some(parsed),
            _ => {}
        }
    }
    match (attempted, completed, failed) {
        (Some(attempted), Some(completed), Some(failed)) => Ok(BatchResult {
            attempted,
            completed,
            failed,
        }),
        _ => Err("incomplete batch result line".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::parse_batch_output;

    #[test]
    fn parse_batch_output_records_failed_requests() {
        let result = parse_batch_output("attempted=3 completed=2 failed=1\n");

        let batch = result.expect("failed requests are evidence, not parse errors");
        assert_eq!(batch.attempted, 3);
        assert_eq!(batch.completed, 2);
        assert_eq!(batch.failed, 1);
    }

    #[test]
    fn parse_batch_output_reads_successful_counts() {
        let result = parse_batch_output("attempted=180 completed=180 failed=0\n")
            .expect("batch output should parse");

        assert_eq!(result.attempted, 180);
        assert_eq!(result.completed, 180);
        assert_eq!(result.failed, 0);
    }

    #[test]
    fn replica_request_count_distributes_remainder() {
        assert_eq!(super::replica_request_count(5, 0, 2), 3);
        assert_eq!(super::replica_request_count(5, 1, 2), 2);
    }
}
