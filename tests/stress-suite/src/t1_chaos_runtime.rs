use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::multi_replica_auth::json_field;
use crate::multi_replica_http::{request, wait_for_healthy, wait_for_tcp};
use crate::supervisor_process::SupervisedChild;
use crate::t1_chaos::{Ports, ServerPorts};

const ADMIN_TOKEN: &str = "stress-t1-admin";
const MASTER_KEY: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const CHAOS_LATENCY_MS: u64 = 250;
const MESSAGES_BODY: &str =
    r#"{"model":"fake-alpha","max_tokens":8,"messages":[{"role":"user","content":"T1"}]}"#;

pub(super) struct Measurements {
    pub(super) control: RequestMeasurement,
    pub(super) chaos: RequestMeasurement,
}

pub(super) struct RequestMeasurement {
    pub(super) elapsed_ms: u64,
    pub(super) status: u16,
}

pub(super) fn exercise(ports: Ports) -> Result<Measurements, String> {
    let root = runtime_dir()?;
    let result = exercise_in(&root, ports);
    let _ = std::fs::remove_dir_all(&root);
    result
}

fn exercise_in(root: &Path, ports: Ports) -> Result<Measurements, String> {
    let workspace = workspace_root()?;
    let mut fake = spawn_fake(&workspace, ports.fake)?;
    wait_for_tcp(address(ports.fake)).map_err(|error| {
        format!(
            "fake upstream unavailable: {error}; {}",
            fake.readiness_diagnostic()
        )
    })?;
    let control = exercise_instance(root, &workspace, "control", ports.control, ports.fake, None)?;
    let chaos = exercise_instance(
        root,
        &workspace,
        "chaos",
        ports.chaos,
        ports.fake,
        Some(CHAOS_LATENCY_MS),
    )?;
    Ok(Measurements { control, chaos })
}

fn exercise_instance(
    root: &Path,
    workspace: &Path,
    name: &str,
    ports: ServerPorts,
    fake_port: u16,
    chaos_latency_ms: Option<u64>,
) -> Result<RequestMeasurement, String> {
    let runtime = root.join(name);
    std::fs::create_dir_all(&runtime).map_err(|error| error.to_string())?;
    let config = runtime.join("cc-lb.toml");
    std::fs::write(&config, render_config(ports, &runtime)).map_err(|error| error.to_string())?;
    let mut proxy = spawn_proxy(workspace, &config, &runtime, chaos_latency_ms)?;
    wait_for_healthy(address(ports.proxy)).map_err(|error| {
        format!(
            "{name} proxy unavailable: {error}; {}",
            proxy.readiness_diagnostic()
        )
    })?;
    let api_key = seed_runtime(ports.admin, fake_port)?;
    let started = Instant::now();
    let response = request(
        address(ports.proxy),
        "POST",
        "/v1/messages",
        &[
            ("Content-Type", "application/json"),
            ("anthropic-version", "2023-06-01"),
            ("x-api-key", &api_key),
        ],
        MESSAGES_BODY,
    )?;
    let elapsed_ms =
        u64::try_from(started.elapsed().as_millis()).map_err(|error| error.to_string())?;
    let cleanup = proxy.cleanup();
    if response.status != 200 || !response.body.contains("fake anthropic fixture response") {
        return Err(format!(
            "{name} proxy request did not reach fake upstream: status={}, cleanup={cleanup:?}",
            response.status
        ));
    }
    Ok(RequestMeasurement {
        elapsed_ms,
        status: response.status,
    })
}

fn spawn_fake(workspace: &Path, port: u16) -> Result<SupervisedChild, String> {
    let mut command = cargo_command(workspace);
    command.args(["run", "-q", "-p", "fake-anthropic", "--", "--port"]);
    command.arg(port.to_string());
    SupervisedChild::spawn(command)
}

fn spawn_proxy(
    workspace: &Path,
    config: &Path,
    runtime: &Path,
    chaos_latency_ms: Option<u64>,
) -> Result<SupervisedChild, String> {
    let mut command = cargo_command(workspace);
    command
        .args([
            "run",
            "-q",
            "-p",
            "cc-lb-server",
            "--features",
            "sqlite",
            "--",
        ])
        .args(["serve", "--config"])
        .arg(config)
        .arg("--data-dir")
        .arg(runtime)
        .env("CC_LB_MASTER_KEY", MASTER_KEY)
        .env("CC_LB_ADMIN_TOKEN", ADMIN_TOKEN);
    for name in [
        "CC_LB_CHAOS_LATENCY_MS",
        "CC_LB_CHAOS_DROP_PCT",
        "CC_LB_CHAOS_RST_AFTER_BYTES",
        "CC_LB_CHAOS_TRUNCATE_AFTER_EVENTS",
    ] {
        command.env_remove(name);
    }
    if let Some(latency_ms) = chaos_latency_ms {
        command.env("CC_LB_CHAOS_LATENCY_MS", latency_ms.to_string());
    }
    SupervisedChild::spawn(command)
}

fn seed_runtime(admin_port: u16, fake_port: u16) -> Result<String, String> {
    let headers = [
        ("Authorization", "Bearer stress-t1-admin"),
        ("Content-Type", "application/json"),
    ];
    let principal = request(
        address(admin_port),
        "POST",
        "/admin/v1/principals",
        &headers,
        r#"{"name":"t1-principal","kind":"machine","allowed_models":["*"]}"#,
    )?;
    if principal.status != 201 {
        return Err(format!("create T1 principal returned {}", principal.status));
    }
    let upstream_body = format!(
        r#"{{"name":"t1-fake","kind":"{}","base_url":"http://127.0.0.1:{fake_port}","api_key_value":"sk-ant-t1"}}"#,
        ["anthropic", "_api_key"].concat()
    );
    let upstream = request(
        address(admin_port),
        "POST",
        "/admin/v1/upstreams",
        &headers,
        &upstream_body,
    )?;
    if upstream.status != 201 {
        return Err(format!("create T1 upstream returned {}", upstream.status));
    }
    let key = request(
        address(admin_port),
        "POST",
        "/admin/v1/principals/t1-principal/keys",
        &headers,
        r#"{"label":"t1-chaos"}"#,
    )?;
    if key.status != 201 {
        return Err(format!("issue T1 managed key returned {}", key.status));
    }
    json_field(&key.body, "plaintext_key")
}

fn cargo_command(workspace: &Path) -> Command {
    let mut command = Command::new("cargo");
    command.current_dir(workspace);
    command
}

fn render_config(ports: ServerPorts, runtime: &Path) -> String {
    format!(
        r#"[listener]
proxy_addr = "127.0.0.1:{proxy}"
admin_addr = "127.0.0.1:{admin}"
metrics_addr = "127.0.0.1:{metrics}"

[body]
messages_cap_bytes = 33554432
files_cap_bytes = 104857600

[timeouts]
upstream_total_secs = 30
drain_secs = 5

[runtime]
data_dir = "{data_dir}"


[storage]
kind = "sqlite"
path = "{data_dir}/cc-lb.sqlite"

[aead]
key_env = "CC_LB_MASTER_KEY"

[[admin.auth.providers]]
kind = "static_token"
id = "stress-t1-admin"
token_env = "CC_LB_ADMIN_TOKEN"

"#,
        proxy = ports.proxy,
        admin = ports.admin,
        metrics = ports.metrics,
        data_dir = runtime.display(),
    )
}

fn address(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

fn runtime_dir() -> Result<PathBuf, String> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_nanos();
    let path =
        std::env::temp_dir().join(format!("cc-lb-stress-t1-{}-{elapsed}", std::process::id()));
    std::fs::create_dir_all(&path).map_err(|error| error.to_string())?;
    Ok(path)
}

fn workspace_root() -> Result<PathBuf, String> {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or_else(|| "stress-suite workspace root is unavailable".to_owned())
}
