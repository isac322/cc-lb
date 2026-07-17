use std::net::SocketAddr;
use std::time::{Duration, Instant};

use crate::docker::Docker;
use crate::fabric_state::{POSTGRES_IP, ResourceNames, label, label_filter};
use crate::multi_replica_evidence::{
    RunCleanupEvidence, RunEvidence, StorageError, StorageEvidence,
};
use crate::verdict::Verdict;

pub fn start_postgres(
    docker: &Docker,
    names: &ResourceNames,
    run_id: &str,
) -> Result<String, String> {
    docker
        .run(&[
            "run".to_owned(),
            "-d".to_owned(),
            "--name".to_owned(),
            names.postgres.clone(),
            "--label".to_owned(),
            label(run_id),
            "--network".to_owned(),
            names.network.clone(),
            "--ip".to_owned(),
            POSTGRES_IP.to_owned(),
            "-p".to_owned(),
            "127.0.0.1::5432".to_owned(),
            "-e".to_owned(),
            "POSTGRES_USER=cc_lb".to_owned(),
            "-e".to_owned(),
            "POSTGRES_PASSWORD=stress".to_owned(),
            "-e".to_owned(),
            "POSTGRES_DB=cc_lb".to_owned(),
            "postgres:18".to_owned(),
        ])
        .map_err(|error| error.to_string())?;
    wait_for_postgres(docker, &names.postgres)?;
    let output = docker
        .run(&[
            "inspect".to_owned(),
            "--format".to_owned(),
            "{{(index (index .NetworkSettings.Ports \"5432/tcp\") 0).HostPort}}".to_owned(),
            names.postgres.clone(),
        ])
        .map_err(|error| error.to_string())?;
    let port = output
        .stdout
        .trim()
        .parse::<u16>()
        .map_err(|error| error.to_string())?;
    Ok(format!("postgres://cc_lb:stress@127.0.0.1:{port}/cc_lb"))
}

pub fn wait_for_postgres(docker: &Docker, postgres: &str) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if docker
            .run(&[
                "exec".to_owned(),
                postgres.to_owned(),
                "pg_isready".to_owned(),
                "-U".to_owned(),
                "cc_lb".to_owned(),
                "-d".to_owned(),
                "cc_lb".to_owned(),
            ])
            .is_ok()
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("postgres did not become ready".to_owned());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

pub fn address(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

pub fn labeled_count(docker: &Docker, noun: &str, run_id: &str) -> u64 {
    let args = match noun {
        "ps" => vec![
            "ps".to_owned(),
            "-aq".to_owned(),
            "--filter".to_owned(),
            label_filter(run_id),
        ],
        "network" => vec![
            "network".to_owned(),
            "ls".to_owned(),
            "-q".to_owned(),
            "--filter".to_owned(),
            label_filter(run_id),
        ],
        _ => return u64::MAX,
    };
    docker
        .run(&args)
        .map(|output| output.stdout.lines().count() as u64)
        .unwrap_or(u64::MAX)
}

pub fn failure_evidence(
    profile: &str,
    run_id: &str,
    replicas: usize,
    verdict: Verdict,
    error: String,
) -> RunEvidence {
    let mut storage = StorageEvidence::failure("orchestration");
    storage.plane_effect.errors = vec![StorageError {
        source: "orchestration".to_owned(),
        classification: error.clone(),
    }];
    let mut evidence = RunEvidence::new(profile, run_id);
    evidence.verdict = verdict;
    evidence.replicas = Vec::with_capacity(replicas);
    evidence.storage = storage;
    if verdict == Verdict::Blocked {
        evidence.blocked_stage = Some("preflight".to_owned());
        evidence.blocked_reason = Some(error);
    }
    evidence.cleanup = RunCleanupEvidence {
        attempted: true,
        labeled_containers_remaining: 0,
        labeled_networks_remaining: 0,
    };
    evidence
}
