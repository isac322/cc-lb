use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::multi_replica_http::{request, wait_for_healthy, wait_for_tcp};
use crate::supervisor_process::SupervisedChild;
use crate::t1_chaos::{Ports, ServerPorts};

const ADMIN_TOKEN: &str = "stress-t1-admin";
const MASTER_KEY: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const CHAOS_LATENCY_MS: u64 = 500;
const DROP_ATTEMPTS: u64 = 200;
const RST_AFTER_BYTES: u64 = 128;
const TRUNCATE_AFTER_EVENTS: u64 = 2;
const MESSAGES_BODY: &str =
    r#"{"model":"fake-alpha","max_tokens":8,"messages":[{"role":"user","content":"T1"}]}"#;
const STREAM_MESSAGES_BODY: &str = r#"{"model":"fake-alpha","max_tokens":8,"messages":[{"role":"user","content":"T1"}],"stream":true}"#;

pub(super) struct Measurements {
    pub(super) control: RequestMeasurement,
    pub(super) latency: RequestMeasurement,
    pub(super) drop: DropMeasurement,
    pub(super) rst: RstMeasurement,
    pub(super) truncate: TruncateMeasurement,
}

pub(super) struct RequestMeasurement {
    pub(super) elapsed_ms: u64,
    pub(super) status: u16,
}

pub(super) struct DropMeasurement {
    pub(super) attempts: u64,
    pub(super) dropped: u64,
    pub(super) passed: u64,
}

pub(super) struct RstMeasurement {
    pub(super) status: Option<u16>,
    pub(super) body_bytes: usize,
    pub(super) terminated_early: bool,
}

pub(super) struct TruncateMeasurement {
    pub(super) status: u16,
    pub(super) data_events: usize,
    pub(super) has_message_stop: bool,
}

#[derive(Clone, Copy, Default)]
struct ChaosSettings {
    latency_ms: u64,
    drop_pct: u8,
    rst_after_bytes: u64,
    truncate_after_events: u64,
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

    let control = with_instance(
        root,
        &workspace,
        "control",
        ports.control,
        ports.fake,
        ChaosSettings::default(),
        measure_request,
    )?;
    let latency = with_instance(
        root,
        &workspace,
        "latency",
        ports.latency,
        ports.fake,
        ChaosSettings {
            latency_ms: CHAOS_LATENCY_MS,
            ..ChaosSettings::default()
        },
        measure_request,
    )?;
    let drop = with_instance(
        root,
        &workspace,
        "drop-pct-50",
        ports.drop,
        ports.fake,
        ChaosSettings {
            drop_pct: 50,
            ..ChaosSettings::default()
        },
        measure_drop,
    )?;
    let rst = with_instance(
        root,
        &workspace,
        "rst-after-bytes",
        ports.rst,
        ports.fake,
        ChaosSettings {
            rst_after_bytes: RST_AFTER_BYTES,
            ..ChaosSettings::default()
        },
        measure_rst,
    )?;
    let truncate = with_instance(
        root,
        &workspace,
        "truncate-mid-stream",
        ports.truncate,
        ports.fake,
        ChaosSettings {
            truncate_after_events: TRUNCATE_AFTER_EVENTS,
            ..ChaosSettings::default()
        },
        measure_truncate,
    )?;

    Ok(Measurements {
        control,
        latency,
        drop,
        rst,
        truncate,
    })
}

fn with_instance<T>(
    root: &Path,
    workspace: &Path,
    name: &str,
    ports: ServerPorts,
    fake_port: u16,
    settings: ChaosSettings,
    exercise: impl FnOnce(u16) -> Result<T, String>,
) -> Result<T, String> {
    let runtime = root.join(name);
    std::fs::create_dir_all(&runtime).map_err(|error| error.to_string())?;
    let config = runtime.join("cc-lb.toml");
    std::fs::write(&config, render_config(ports, &runtime)).map_err(|error| error.to_string())?;
    let mut proxy = spawn_proxy(workspace, &config, &runtime, settings)?;
    wait_for_healthy(address(ports.proxy)).map_err(|error| {
        format!(
            "{name} proxy unavailable: {error}; {}",
            proxy.readiness_diagnostic()
        )
    })?;
    seed_runtime(ports.admin, fake_port)?;

    let result = exercise(ports.proxy);
    let cleanup = proxy.cleanup();
    result.map_err(|error| format!("{name} chaos exercise failed: {error}; cleanup={cleanup:?}"))
}

fn measure_request(proxy_port: u16) -> Result<RequestMeasurement, String> {
    let started = Instant::now();
    let response = request(
        address(proxy_port),
        "POST",
        "/v1/messages",
        &[
            ("Content-Type", "application/json"),
            ("anthropic-version", "2023-06-01"),
        ],
        MESSAGES_BODY,
    )?;
    let elapsed_ms =
        u64::try_from(started.elapsed().as_millis()).map_err(|error| error.to_string())?;
    if response.status != 200 || !response.body.contains("fake anthropic fixture response") {
        return Err(format!(
            "proxy request did not reach fake upstream: status={}",
            response.status
        ));
    }
    Ok(RequestMeasurement {
        elapsed_ms,
        status: response.status,
    })
}

fn measure_drop(proxy_port: u16) -> Result<DropMeasurement, String> {
    let mut dropped = 0_u64;
    let mut passed = 0_u64;
    for _ in 0..DROP_ATTEMPTS {
        match request(address(proxy_port), "GET", "/v1/models", &[], "") {
            Ok(response) if response.status == 502 => dropped += 1,
            Ok(response) if response.status == 200 => passed += 1,
            Ok(response) => {
                return Err(format!(
                    "drop-pct-50 returned unexpected status {}",
                    response.status
                ));
            }
            Err(_) => dropped += 1,
        }
    }
    Ok(DropMeasurement {
        attempts: DROP_ATTEMPTS,
        dropped,
        passed,
    })
}

fn measure_rst(proxy_port: u16) -> Result<RstMeasurement, String> {
    let payload = "x".repeat(4096);
    let body = format!(r#"{{"data":"{payload}"}}"#);
    let response = raw_request(
        address(proxy_port),
        "POST",
        "/v1/files",
        &[
            ("Content-Type", "application/json"),
            ("anthropic-version", "2023-06-01"),
        ],
        &body,
    )?;
    Ok(RstMeasurement {
        status: response.status,
        body_bytes: response.body.len(),
        terminated_early: !response.complete,
    })
}

fn measure_truncate(proxy_port: u16) -> Result<TruncateMeasurement, String> {
    let response = raw_request(
        address(proxy_port),
        "POST",
        "/v1/messages",
        &[
            ("Content-Type", "application/json"),
            ("anthropic-version", "2023-06-01"),
            ("Accept", "text/event-stream"),
        ],
        STREAM_MESSAGES_BODY,
    )?;
    let status = response
        .status
        .ok_or_else(|| "truncated SSE response is missing HTTP status".to_owned())?;
    let body = String::from_utf8_lossy(&response.body);
    Ok(TruncateMeasurement {
        status,
        data_events: body
            .lines()
            .filter(|line| line.starts_with("data:"))
            .count(),
        has_message_stop: body.contains("message_stop"),
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
    settings: ChaosSettings,
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
        .env("CC_LB_ADMIN_TOKEN", ADMIN_TOKEN)
        .env("CC_LB_BOOTSTRAP_ADMIN_TOKEN", ADMIN_TOKEN)
        .env("CC_LB_CHAOS_LATENCY_MS", settings.latency_ms.to_string())
        .env("CC_LB_CHAOS_DROP_PCT", settings.drop_pct.to_string())
        .env(
            "CC_LB_CHAOS_RST_AFTER_BYTES",
            settings.rst_after_bytes.to_string(),
        )
        .env(
            "CC_LB_CHAOS_TRUNCATE_AFTER_EVENTS",
            settings.truncate_after_events.to_string(),
        );
    SupervisedChild::spawn(command)
}

fn seed_runtime(admin_port: u16, fake_port: u16) -> Result<(), String> {
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
    Ok(())
}

fn raw_request(
    address: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> Result<RawResponse, String> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(2))
        .map_err(|error| error.to_string())?;
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .map_err(|error| error.to_string())?;
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    for (name, value) in headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str("\r\n");
    request.push_str(body);
    stream
        .write_all(request.as_bytes())
        .map_err(|error| error.to_string())?;

    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 8192];
    let mut read_error = None;
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => bytes.extend_from_slice(&buffer[..read]),
            Err(error) => {
                read_error = Some(error.to_string());
                break;
            }
        }
    }
    let head_end = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| match read_error.as_deref() {
            Some(error) => format!("response ended before complete HTTP headers: {error}"),
            None => "response ended before complete HTTP headers".to_owned(),
        })?;
    let head = &bytes[..head_end];
    let body = bytes[head_end + 4..].to_vec();
    let head_text = String::from_utf8_lossy(head);
    let status = head_text
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok());
    let complete = read_error.is_none() && response_body_complete(&head_text, &body);
    Ok(RawResponse {
        status,
        body,
        complete,
    })
}

fn response_body_complete(headers: &str, body: &[u8]) -> bool {
    if headers
        .lines()
        .any(|line| line.eq_ignore_ascii_case("transfer-encoding: chunked"))
    {
        return body.ends_with(b"0\r\n\r\n")
            || body.windows(7).any(|window| window == b"\r\n0\r\n\r\n");
    }
    let content_length = headers.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("content-length")
            .then(|| value.trim().parse::<usize>().ok())
            .flatten()
    });
    content_length.is_none_or(|expected| body.len() >= expected)
}

struct RawResponse {
    status: Option<u16>,
    body: Vec<u8>,
    complete: bool,
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
request_header_secs = 10
request_body_chunk_secs = 30
idle_secs = 300
upstream_total_secs = 30
drain_secs = 5

[runtime]
data_dir = "{data_dir}"

[downstream_auth]
mode = "none"

[downstream_auth.none_mode]
principal_id = "t1-principal"
upstream_kind = "anthropic_key"

[storage]
kind = "sqlite"
path = "{data_dir}/cc-lb.sqlite"

[aead]
key_env = "CC_LB_MASTER_KEY"

[admin]
token_env = "CC_LB_ADMIN_TOKEN"

[egress]
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
