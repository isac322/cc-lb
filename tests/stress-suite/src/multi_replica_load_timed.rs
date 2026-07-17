use crate::docker::Docker;
use crate::fabric_state::{ResourceNames, label};
use crate::multi_replica_config::ReplicaPorts;
use crate::multi_replica_load::{BatchResult, MESSAGES_BODY, parse_batch_output};

pub fn request_timed_wave_via_fabric(
    names: &ResourceNames,
    run_id: &str,
    replica_ports: &[ReplicaPorts],
    key: &str,
    duration_secs: u64,
    workers: u64,
) -> Result<Vec<(usize, BatchResult)>, String> {
    let network = names.network.clone();
    let helper_image = names.helper_image.clone();
    let run_label = label(run_id);
    std::thread::scope(|scope| {
        let handles = replica_ports
            .iter()
            .enumerate()
            .map(|(index, ports)| {
                let network = network.clone();
                let helper_image = helper_image.clone();
                let run_label = run_label.clone();
                let key = key.to_owned();
                scope.spawn(move || {
                    request_timed_load_raw(
                        network,
                        helper_image,
                        run_label,
                        ports.proxy,
                        key,
                        duration_secs,
                        workers,
                    )
                    .map(|result| (index, result))
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .map_err(|_| "timed load worker panicked".to_owned())?
            })
            .collect()
    })
}

fn request_timed_load_raw(
    network: String,
    helper_image: String,
    run_label: String,
    port: u16,
    key: String,
    duration_secs: u64,
    workers: u64,
) -> Result<BatchResult, String> {
    let output = Docker
        .run(&[
            "run".to_owned(),
            "--rm".to_owned(),
            "--label".to_owned(),
            run_label,
            "--network".to_owned(),
            network,
            "--add-host".to_owned(),
            "host.docker.internal:host-gateway".to_owned(),
            helper_image,
            "sh".to_owned(),
            "-ec".to_owned(),
            timed_script().to_owned(),
            "ccstress-load".to_owned(),
            duration_secs.to_string(),
            workers.to_string(),
            format!("http://host.docker.internal:{port}/v1/messages"),
            key,
            MESSAGES_BODY.to_owned(),
        ])
        .map_err(|error| error.to_string())?;
    parse_batch_output(&output.stdout)
}

fn timed_script() -> &'static str {
    r#"
duration="$1"
workers="$2"
url="$3"
key="$4"
body="$5"
tmp="$(mktemp -d)"
end=$(( $(date +%s) + duration ))
i=0
while [ "$i" -lt "$workers" ]; do
  (
    attempted=0
    ok=0
    fail=0
    while [ "$(date +%s)" -lt "$end" ]; do
      attempted=$((attempted + 1))
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
    done
    printf '%s %s %s\n' "$attempted" "$ok" "$fail" > "$tmp/worker-$i"
  ) &
  i=$((i + 1))
done
wait
attempted=0
ok=0
fail=0
for file in "$tmp"/worker-*; do
  read -r worker_attempted worker_ok worker_fail < "$file" || true
  attempted=$((attempted + worker_attempted))
  ok=$((ok + worker_ok))
  fail=$((fail + worker_fail))
done
rm -rf "$tmp"
printf 'attempted=%s completed=%s failed=%s\n' "$attempted" "$ok" "$fail"
test "$attempted" -gt 0
"#
}
