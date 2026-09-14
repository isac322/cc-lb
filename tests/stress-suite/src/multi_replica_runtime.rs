use std::process::Command;
use std::time::Instant;

use crate::docker::Docker;
use crate::fabric_runtime::{build_helper_image, create_network};
use crate::fabric_state::{FabricCleanup, ResourceNames};
use crate::multi_replica::RunInput;
use crate::multi_replica_auth::issue_key;
use crate::multi_replica_config::ReplicaPorts;
use crate::multi_replica_evidence::{
    PerReplicaLimit, ReplicaEvidence, RunCleanupEvidence, RunEvidence,
};
use crate::multi_replica_load::{BatchResult, request_wave_via_fabric};
use crate::multi_replica_storage::failure_evidence;
use crate::multi_replica_waves::{Processes, request_via_fabric, seed_runtime, storage_impairment};
use crate::verdict::Verdict;

pub fn execute(input: &RunInput) -> Result<RunEvidence, String> {
    let names = ResourceNames::new(&input.run_id)?;
    let docker = Docker;
    if let Err(error) = docker.run(&[
        "version".to_owned(),
        "--format".to_owned(),
        "{{.Server.Version}}".to_owned(),
    ]) {
        return Ok(failure_evidence(
            input.profile.as_str(),
            &input.run_id,
            input.replicas,
            Verdict::Blocked,
            error.to_string(),
        ));
    }
    let mut cleanup = FabricCleanup::new(&docker, &names, &input.run_id);
    let result = execute_run(input, &docker, &names);
    let _ = cleanup.cleanup();
    match result {
        Ok(mut evidence) => {
            evidence.cleanup = RunCleanupEvidence {
                attempted: true,
                labeled_containers_remaining: crate::multi_replica_storage::labeled_count(
                    &docker,
                    "ps",
                    &input.run_id,
                ),
                labeled_networks_remaining: crate::multi_replica_storage::labeled_count(
                    &docker,
                    "network",
                    &input.run_id,
                ),
            };
            if evidence.cleanup.labeled_containers_remaining != 0
                || evidence.cleanup.labeled_networks_remaining != 0
            {
                evidence.verdict = Verdict::Fail;
            }
            Ok(evidence)
        }
        Err(error) => Ok(failure_evidence(
            input.profile.as_str(),
            &input.run_id,
            input.replicas,
            Verdict::Fail,
            error,
        )),
    }
}

fn execute_run(
    input: &RunInput,
    docker: &Docker,
    names: &ResourceNames,
) -> Result<RunEvidence, String> {
    let setup_started = Instant::now();
    ensure_postgres_server_binary()?;
    build_helper_image(docker, names, &input.run_id)?;
    create_network(docker, names, &input.run_id)?;
    let storage_url = crate::multi_replica_storage::start_postgres(docker, names, &input.run_id)?;
    let ports = ReplicaPorts::allocate_many(input.replicas + 1)?;
    let fake_port = ports
        .last()
        .ok_or_else(|| "missing fake upstream port".to_owned())?
        .proxy;
    let replica_ports = &ports[..input.replicas];
    let runtime_dir = input.output.join("runtime");
    std::fs::create_dir_all(&runtime_dir).map_err(|error| error.to_string())?;
    let mut processes = Processes::start(&runtime_dir, replica_ports, &storage_url, fake_port)?;
    seed_runtime(replica_ports, fake_port)?;
    let setup_ms = elapsed_ms(setup_started);
    let waves_started = Instant::now();
    let latency = crate::multi_replica_http::wait_for_body(
        crate::multi_replica_storage::address(replica_ports[1].admin),
        "/admin/v1/upstreams",
        "stress-upstream",
    )?;
    let key = issue_key(crate::multi_replica_storage::address(
        replica_ports[0].admin,
    ))?;
    let mut request_counts = vec![0_u64; input.replicas];
    let mut wave_counts = vec![BatchResult::default(); input.profile.wave_count()];
    let target_per_wave = input.target_requests_per_wave();
    let wave_duration_secs = input.wave_duration_ms().div_ceil(1_000).max(1);
    let fabric = crate::multi_replica_load::FabricRef {
        docker,
        names,
        run_id: &input.run_id,
    };
    for (wave_index, wave_count) in wave_counts.iter_mut().enumerate() {
        for (replica_index, batch) in request_wave_via_fabric(
            fabric,
            input.load,
            replica_ports,
            &key,
            target_per_wave,
            wave_duration_secs,
            wave_index,
        )? {
            request_counts[replica_index] =
                request_counts[replica_index].saturating_add(batch.completed);
            wave_count.attempted = wave_count.attempted.saturating_add(batch.attempted);
            wave_count.completed = wave_count.completed.saturating_add(batch.completed);
            wave_count.failed = wave_count.failed.saturating_add(batch.failed);
        }
        if wave_count.failed > 0 && !input.load.timed() {
            return Err(format!(
                "wave-{wave_index} had {} failed load requests",
                wave_count.failed
            ));
        }
    }
    if input.only_wave.is_none() {
        processes.restart_first(&runtime_dir, replica_ports, &storage_url)?;
        crate::multi_replica_http::wait_for_healthy(crate::multi_replica_storage::address(
            replica_ports[0].proxy,
        ))?;
        let status =
            request_via_fabric(docker, names, &input.run_id, replica_ports[0].proxy, &key)?;
        if status != 200 {
            return Err(format!("rejoined replica returned {status}"));
        }
        request_counts[0] = request_counts[0].saturating_add(1);
        if let Some(last_wave) = wave_counts.last_mut() {
            last_wave.attempted = last_wave.attempted.saturating_add(1);
            last_wave.completed = last_wave.completed.saturating_add(1);
        }
    }
    let storage = storage_impairment(docker, names, &input.run_id, replica_ports, &key, latency)?;
    pace_profile(input.min_wave_execution_ms, waves_started)?;
    let wave_execution_ms = elapsed_ms(waves_started);
    let total_completed = request_counts.iter().copied().sum::<u64>();
    input.validate_throughput(total_completed, wave_execution_ms)?;
    let replicas = replica_ports
        .iter()
        .enumerate()
        .map(|(index, ports)| ReplicaEvidence {
            name: format!("replica-{index}"),
            proxy_port: ports.proxy,
            admin_port: ports.admin,
            metrics_port: ports.metrics,
            requests: request_counts[index],
        })
        .collect::<Vec<_>>();
    let per_replica_allowed = if input.load.timed() {
        u64::MAX
    } else {
        input.profile.per_replica_allowed()
    };
    let limits = replicas
        .iter()
        .map(|replica| PerReplicaLimit::new(&replica.name, per_replica_allowed, replica.requests))
        .collect::<Vec<_>>();
    PerReplicaLimit::validate_all(&limits)?;
    storage.validate()?;
    let mut evidence = RunEvidence::new(input.profile.as_str(), &input.run_id);
    evidence.verdict = Verdict::Pass;
    evidence.setup_ms = Some(setup_ms);
    evidence.wave_execution_ms = Some(wave_execution_ms);
    evidence.waves = crate::multi_replica_final::wave_evidence(input, &wave_counts, &storage)?;
    evidence.replicas = replicas;
    evidence.replica_process_metrics =
        crate::multi_replica_final::replica_process_metrics(&evidence.replicas);
    evidence.storage = storage;
    evidence.auth_wave.managed_key = true;
    evidence.auth_wave.cross_replica_usable = true;
    evidence.per_replica_limits = limits;
    evidence.per_replica_limit_check =
        crate::multi_replica_final::limit_check(&evidence.per_replica_limits);
    Ok(evidence)
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

fn pace_profile(min_wave_execution_ms: u64, started: Instant) -> Result<(), String> {
    let elapsed_ms = elapsed_ms(started);
    if let Some(remaining_ms) = min_wave_execution_ms.checked_sub(elapsed_ms) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .map_err(|error| format!("build stress pacing runtime: {error}"))?;
        runtime.block_on(tokio::time::sleep(std::time::Duration::from_millis(
            remaining_ms,
        )));
    }
    Ok(())
}

fn ensure_postgres_server_binary() -> Result<(), String> {
    let status = Command::new("cargo")
        .args(["build", "-p", "cc-lb-server", "--features", "postgres"])
        .current_dir(workspace_root())
        .status()
        .map_err(|error| format!("spawn postgres-enabled cc-lb build: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("postgres-enabled cc-lb build exited with {status}"))
    }
}

fn workspace_root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .map(std::path::Path::to_path_buf)
        .unwrap_or_else(|| std::path::PathBuf::from("."))
}
