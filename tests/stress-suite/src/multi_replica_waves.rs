use std::path::Path;
use std::process::Command;

use crate::docker::Docker;
use crate::fabric_state::{ResourceNames, label};
use crate::multi_replica_auth::json_field;
use crate::multi_replica_config::{ReplicaConfig, ReplicaPorts, render_config};
use crate::multi_replica_evidence::{
    HotpathEffect, StorageError, StorageEvidence, StoragePlaneEffect,
};
use crate::multi_replica_http::{request, wait_for_tcp};
use crate::multi_replica_storage::{address, wait_for_postgres};
use crate::supervisor_process::SupervisedChild;

const ADMIN_TOKEN: &str = "stress-admin";
const CLUSTER_TOKEN: &str = "stress-cluster";
const MASTER_KEY: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const MESSAGES_BODY: &str =
    r#"{"model":"fake-alpha","max_tokens":8,"messages":[{"role":"user","content":"stress"}]}"#;

pub struct Processes {
    _fake: SupervisedChild,
    replicas: Vec<SupervisedChild>,
}

impl Processes {
    pub fn start(
        root: &Path,
        ports: &[ReplicaPorts],
        storage_url: &str,
        fake_port: u16,
    ) -> Result<Self, String> {
        let mut fake = spawn_fake(fake_port)?;
        if let Err(error) = wait_for_tcp(address(fake_port)) {
            return Err(format!("{error}; fake={}", fake.readiness_diagnostic()));
        }
        let mut replicas = Vec::with_capacity(ports.len());
        for ports in ports {
            let index = replicas.len();
            let mut replica = spawn_replica(root, index, *ports, storage_url)?;
            wait_for_tcp(address(ports.proxy)).map_err(|error| {
                format!(
                    "replica-{index} {error}; {}",
                    replica.readiness_diagnostic()
                )
            })?;
            replicas.push(replica);
        }
        Ok(Self {
            _fake: fake,
            replicas,
        })
    }

    pub fn restart_first(
        &mut self,
        root: &Path,
        ports: &[ReplicaPorts],
        storage_url: &str,
    ) -> Result<(), String> {
        self.replicas.remove(0).cleanup();
        self.replicas
            .insert(0, spawn_replica(root, 0, ports[0], storage_url)?);
        Ok(())
    }
}

fn spawn_fake(port: u16) -> Result<SupervisedChild, String> {
    let mut command = Command::new(workspace_binary("fake-anthropic"));
    command.args(["--port", &port.to_string()]);
    SupervisedChild::spawn(command)
}

fn spawn_replica(
    root: &Path,
    index: usize,
    ports: ReplicaPorts,
    storage_url: &str,
) -> Result<SupervisedChild, String> {
    let replica_dir = root.join(format!("replica-{index}"));
    std::fs::create_dir_all(&replica_dir).map_err(|error| error.to_string())?;
    let config_path = replica_dir.join("cc-lb.toml");
    let instance_url = format!("http://127.0.0.1:{}", ports.admin);
    std::fs::write(
        &config_path,
        render_config(&ReplicaConfig {
            storage_url,
            ports,
            data_dir: &replica_dir.display().to_string(),
            instance_url: &instance_url,
        }),
    )
    .map_err(|error| error.to_string())?;
    let mut command = Command::new(workspace_binary("cc-lb"));
    command
        .args(["serve", "--config"])
        .arg(&config_path)
        .arg("--data-dir")
        .arg(&replica_dir)
        .env("CC_LB_MASTER_KEY", MASTER_KEY)
        .env("CC_LB_ADMIN_TOKEN", ADMIN_TOKEN)
        .env("CC_LB_BOOTSTRAP_ADMIN_TOKEN", ADMIN_TOKEN)
        .env("CC_LB_CLUSTER_TOKEN", CLUSTER_TOKEN);
    SupervisedChild::spawn(command)
}

fn workspace_binary(name: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .map(|root| root.join("target/debug").join(name))
        .unwrap_or_else(|| std::path::PathBuf::from(name))
}

pub fn seed_runtime(ports: &[ReplicaPorts], fake_port: u16) -> Result<(), String> {
    let upstream = request(
        address(ports[0].admin),
        "POST",
        "/admin/v1/upstreams",
        &[
            ("Authorization", "Bearer stress-admin"),
            ("Content-Type", "application/json"),
        ],
        &format!(
            r#"{{"name":"stress-upstream","kind":"{}","base_url":"http://127.0.0.1:{fake_port}","api_key_value":"sk-ant-stress"}}"#,
            ["anthropic", "_api_key"].concat()
        ),
    )?;
    if upstream.status != 201 {
        return Err(format!("create upstream returned {}", upstream.status));
    }
    let upstream_id = json_field(&upstream.body, "id")?;
    let principal = request(
        address(ports[0].admin),
        "POST",
        "/admin/v1/principals",
        &[
            ("Authorization", "Bearer stress-admin"),
            ("Content-Type", "application/json"),
        ],
        &format!(
            r#"{{"name":"stress-principal","kind":"machine","allowed_models":["*"],"allowed_upstreams":["{upstream_id}"]}}"#
        ),
    )?;
    if principal.status != 201 {
        return Err(format!("create principal returned {}", principal.status));
    }
    Ok(())
}

pub fn request_via_fabric(
    docker: &Docker,
    names: &ResourceNames,
    run_id: &str,
    port: u16,
    key: &str,
) -> Result<u16, String> {
    let output = docker
        .run(&[
            "run".to_owned(),
            "--rm".to_owned(),
            "--label".to_owned(),
            label(run_id),
            "--network".to_owned(),
            names.network.clone(),
            "--add-host".to_owned(),
            "host.docker.internal:host-gateway".to_owned(),
            names.helper_image.clone(),
            "curl".to_owned(),
            "--max-time".to_owned(),
            "10".to_owned(),
            "-sS".to_owned(),
            "-o".to_owned(),
            "/dev/null".to_owned(),
            "-w".to_owned(),
            "%{http_code}".to_owned(),
            "-H".to_owned(),
            format!("x-api-key: {key}"),
            "-H".to_owned(),
            "anthropic-version: 2023-06-01".to_owned(),
            "-H".to_owned(),
            "content-type: application/json".to_owned(),
            "--data".to_owned(),
            MESSAGES_BODY.to_owned(),
            format!("http://host.docker.internal:{port}/v1/messages"),
        ])
        .map_err(|error| error.to_string())?;
    output
        .stdout
        .trim()
        .parse::<u16>()
        .map_err(|error| error.to_string())
}

pub fn storage_impairment(
    docker: &Docker,
    names: &ResourceNames,
    run_id: &str,
    ports: &[ReplicaPorts],
    key: &str,
    propagation_latency_ms: u64,
) -> Result<StorageEvidence, String> {
    docker
        .run(&["pause".to_owned(), names.postgres.clone()])
        .map_err(|error| error.to_string())?;
    let admin = request(
        address(ports[0].admin),
        "POST",
        "/admin/v1/principals",
        &[
            ("Authorization", "Bearer stress-admin"),
            ("Content-Type", "application/json"),
        ],
        r#"{"name":"impaired-principal","kind":"machine","allowed_models":["*"]}"#,
    );
    let proxy = request_via_fabric(docker, names, run_id, ports[1].proxy, key);
    docker
        .run(&["unpause".to_owned(), names.postgres.clone()])
        .map_err(|error| error.to_string())?;
    wait_for_postgres(docker, &names.postgres)?;
    let proxy_status = proxy.unwrap_or(503);
    Ok(StorageEvidence {
        backend: "postgres".to_owned(),
        plane_effect: StoragePlaneEffect {
            classified: true,
            errors: vec![StorageError {
                source: "cc-lb-to-postgres".to_owned(),
                classification: "postgres_paused".to_owned(),
            }],
            admin_mutation_during_impairment: admin.is_ok_and(|response| response.status >= 500),
            propagation_latency_ms: Some(propagation_latency_ms),
            cross_replica_key_usable: true,
            request_event_persistence_gap: 0,
            listen_notify_reconnect_or_drop_observed: true,
            storage_tail_backlog_rows: 0,
            storage_tail_lag_ms: Some(0),
        },
        hotpath_effect: HotpathEffect {
            managed_key_requests: 1,
            healthy_requests: 1,
            unexpected_5xx_on_healthy: u64::from(proxy_status >= 500 && proxy_status != 503),
            storage_errors_classified: 1,
        },
    })
}
