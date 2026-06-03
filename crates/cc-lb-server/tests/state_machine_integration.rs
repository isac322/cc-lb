use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::io::{self, ErrorKind};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::Router;
use axum::http::StatusCode;
use cc_lb_aead::AeadService;
use cc_lb_config::{Config, DownstreamAuthMode, NoneModeConfig, NoneModeUpstreamKind};
use cc_lb_plugin_wire::handshake::{HANDSHAKE_SCHEMA_VERSION_V1, HandshakeAccept};
use cc_lb_plugin_wire::identity::{CC_LB_PLUGIN_MAGIC, CC_LB_PLUGIN_SECTION_NAME};
use cc_lb_server::App;
use cc_lb_server::app::build_app_with_storage;
use cc_lb_server::state_machine::ServerStateHandle;
use cc_lb_storage_api::upstream::{UpstreamCreate, UpstreamKind};
use cc_lb_storage_api::{
    ManagedKeyStore, PrincipalCreate, PrincipalKind, PrincipalStore, Storage as StorageTrait,
    UpstreamStore,
};
use cc_lb_storage_redb::{RedbManagedKeyStore, Storage as RedbStorage};
use metrics::{
    Counter, CounterFn, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
};
use serde_json::json;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use url::Url;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const ADMIN_TOKEN: &str = "state-machine-test-token";
const PRINCIPAL_ID: &str = "state-machine-test-principal";

#[tokio::test]
async fn admin_closed_during_startup_rehandshake() -> TestResult<()> {
    let state = Arc::new(ServerStateHandle::new_starting());
    let addr = unused_loopback_addr().await?;
    let (bound_rx, release_tx, task) = spawn_gated_tcp_listener(state.clone(), addr);

    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_connect_refused(addr).await?;

    state.transition_to_ready();
    let bound_addr = timeout(Duration::from_secs(1), bound_rx).await??;
    assert_eq!(bound_addr, addr);
    release_tx.send(()).expect("release listener");
    task.await??;
    Ok(())
}

#[tokio::test]
async fn admin_open_after_ready() -> TestResult<()> {
    let state = Arc::new(ServerStateHandle::new_starting());
    let addr = unused_loopback_addr().await?;
    let (bound_rx, release_tx, task) = spawn_gated_tcp_listener(state.clone(), addr);

    state.transition_to_ready();
    let bound_addr = timeout(Duration::from_secs(1), bound_rx).await??;
    assert_eq!(bound_addr, addr);
    assert_connect_accepted(addr).await?;

    release_tx.send(()).expect("release listener");
    task.await??;
    Ok(())
}

#[tokio::test]
async fn proxy_closed_during_starting() -> TestResult<()> {
    let state = Arc::new(ServerStateHandle::new_starting());
    let addr = unused_loopback_addr().await?;
    let (bound_rx, release_tx, task) = spawn_gated_tcp_listener(state.clone(), addr);

    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_connect_refused(addr).await?;

    state.transition_to_ready();
    let bound_addr = timeout(Duration::from_secs(1), bound_rx).await??;
    assert_eq!(bound_addr, addr);
    release_tx.send(()).expect("release listener");
    task.await??;
    Ok(())
}

#[test]
fn concurrent_registration_same_sha256() -> TestResult<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let metrics = CapturingMetrics::default();

    metrics::with_local_recorder(&metrics, || {
        runtime.block_on(async {
            let server = spawn_ready_admin_server().await?;
            let wasm = plugin_wasm("same-plugin", "1.0.0");
            let (
                first,
                second,
                third,
                fourth,
                fifth,
                sixth,
                seventh,
                eighth,
                ninth,
                tenth,
            ) = tokio::join!(
                post_register_tcp(server.addr, "same-plugin", &wasm),
                post_register_tcp(server.addr, "same-plugin", &wasm),
                post_register_tcp(server.addr, "same-plugin", &wasm),
                post_register_tcp(server.addr, "same-plugin", &wasm),
                post_register_tcp(server.addr, "same-plugin", &wasm),
                post_register_tcp(server.addr, "same-plugin", &wasm),
                post_register_tcp(server.addr, "same-plugin", &wasm),
                post_register_tcp(server.addr, "same-plugin", &wasm),
                post_register_tcp(server.addr, "same-plugin", &wasm),
                post_register_tcp(server.addr, "same-plugin", &wasm),
            );
            let responses = vec![
                first?, second?, third?, fourth?, fifth?, sixth?, seventh?, eighth?, ninth?, tenth?,
            ];
            let mut statuses = responses
                .iter()
                .map(|response| response.status)
                .collect::<Vec<_>>();
            statuses.sort_unstable();

            let mut expected = vec![StatusCode::OK.as_u16(); 9];
            expected.push(StatusCode::CREATED.as_u16());
            expected.sort_unstable();
            assert_eq!(statuses, expected, "responses={responses:?}");
            assert!(
                responses
                    .iter()
                    .all(|response| response.body.contains("same-plugin")),
                "responses={responses:?}"
            );
            assert_eq!(metrics.handshake_total(), 1);
            Ok::<(), Box<dyn Error + Send + Sync>>(())
        })
    })?;

    Ok(())
}

#[test]
fn concurrent_registration_different_sha256() -> TestResult<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let metrics = CapturingMetrics::default();

    metrics::with_local_recorder(&metrics, || {
        runtime.block_on(async {
            let server = spawn_ready_admin_server().await?;
            let first_wasm = plugin_wasm("different-plugin-a", "1.0.0");
            let second_wasm = plugin_wasm("different-plugin-b", "1.0.0");
            let third_wasm = plugin_wasm("different-plugin-c", "1.0.0");
            let fourth_wasm = plugin_wasm("different-plugin-d", "1.0.0");
            let fifth_wasm = plugin_wasm("different-plugin-e", "1.0.0");
            let (first, second, third, fourth, fifth) = tokio::join!(
                post_register_tcp(server.addr, "different-plugin-a", &first_wasm),
                post_register_tcp(server.addr, "different-plugin-b", &second_wasm),
                post_register_tcp(server.addr, "different-plugin-c", &third_wasm),
                post_register_tcp(server.addr, "different-plugin-d", &fourth_wasm),
                post_register_tcp(server.addr, "different-plugin-e", &fifth_wasm),
            );
            let responses = vec![first?, second?, third?, fourth?, fifth?];

            assert!(
                responses
                    .iter()
                    .all(|response| response.status == StatusCode::CREATED.as_u16()),
                "responses={responses:?}"
            );
            assert_eq!(metrics.handshake_total(), 5);
            Ok::<(), Box<dyn Error + Send + Sync>>(())
        })
    })?;

    Ok(())
}

#[tokio::test]
async fn registration_during_starting_refused() -> TestResult<()> {
    let test_app = build_test_app().await?;
    let state = Arc::new(ServerStateHandle::new_starting());
    let addr = unused_loopback_addr().await?;
    let (bound_rx, task) =
        spawn_gated_router(state.clone(), addr, test_app.app.admin_router.clone());
    let wasm = plugin_wasm("starting-plugin", "1.0.0");

    match post_register_tcp(addr, "starting-plugin", &wasm).await {
        Err(error) if error.kind() == ErrorKind::ConnectionRefused => {}
        Err(error) => {
            panic!("expected connection refused for registration during Starting, got {error}")
        }
        Ok(response) => panic!(
            "expected connection refused for registration during Starting, got response {response:?}"
        ),
    }

    state.transition_to_ready();
    let bound_addr = timeout(Duration::from_secs(1), bound_rx).await??;
    assert_eq!(bound_addr, addr);
    task.abort();
    let _ = task.await;
    drop(test_app);
    Ok(())
}

fn spawn_gated_tcp_listener(
    state: Arc<ServerStateHandle>,
    addr: SocketAddr,
) -> (
    oneshot::Receiver<SocketAddr>,
    oneshot::Sender<()>,
    JoinHandle<TestResult<()>>,
) {
    let (bound_tx, bound_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        state.wait_for_ready().await;
        let listener = TcpListener::bind(addr).await?;
        let bound_addr = listener.local_addr()?;
        let _ = bound_tx.send(bound_addr);
        let _ = release_rx.await;
        drop(listener);
        Ok(())
    });
    (bound_rx, release_tx, task)
}

fn spawn_gated_router(
    state: Arc<ServerStateHandle>,
    addr: SocketAddr,
    router: Router,
) -> (oneshot::Receiver<SocketAddr>, JoinHandle<io::Result<()>>) {
    let (bound_tx, bound_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        state.wait_for_ready().await;
        let listener = TcpListener::bind(addr).await?;
        let bound_addr = listener.local_addr()?;
        let _ = bound_tx.send(bound_addr);
        axum::serve(listener, router).await
    });
    (bound_rx, task)
}

async fn assert_connect_refused(addr: SocketAddr) -> TestResult<()> {
    match timeout(Duration::from_secs(1), TcpStream::connect(addr)).await {
        Ok(Err(error)) if error.kind() == ErrorKind::ConnectionRefused => Ok(()),
        Ok(Err(error)) => panic!("expected connection refused for {addr}, got {error}"),
        Ok(Ok(_stream)) => panic!("expected closed port for {addr}, but connect succeeded"),
        Err(_) => panic!("expected connection refused for {addr}, but connect timed out"),
    }
}

async fn assert_connect_accepted(addr: SocketAddr) -> TestResult<()> {
    timeout(Duration::from_secs(1), TcpStream::connect(addr))
        .await
        .map_err(|_| format!("connect to {addr} timed out"))?
        .map(|_stream| ())
        .map_err(|error| format!("expected open port for {addr}, got {error}").into())
}

async fn unused_loopback_addr() -> io::Result<SocketAddr> {
    let listener =
        TcpListener::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))).await?;
    let addr = listener.local_addr()?;
    drop(listener);
    Ok(addr)
}

async fn spawn_ready_admin_server() -> TestResult<RunningAdminServer> {
    let test_app = build_test_app().await?;
    let listener =
        TcpListener::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))).await?;
    let addr = listener.local_addr()?;
    let router = test_app.app.admin_router.clone();
    let task = tokio::spawn(async move { axum::serve(listener, router).await });
    wait_for_status(addr, "/admin/health", StatusCode::OK.as_u16()).await?;
    Ok(RunningAdminServer {
        addr,
        task,
        _test_app: test_app,
    })
}

struct RunningAdminServer {
    addr: SocketAddr,
    task: JoinHandle<io::Result<()>>,
    _test_app: TestApp,
}

impl Drop for RunningAdminServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct TestApp {
    app: App,
    _dir: TempDir,
}

async fn build_test_app() -> TestResult<TestApp> {
    let dir = tempfile::tempdir()?;
    let key = [0_u8; 32];
    let storage_path = dir.path().join("state-machine.redb");
    let storage_arc = Arc::new(RedbStorage::open(&storage_path, key)?);
    seed_storage(storage_arc.as_ref()).await?;
    let managed_store: Arc<dyn ManagedKeyStore> =
        Arc::new(RedbManagedKeyStore::new(storage_arc.clone()));
    let storage: Arc<dyn StorageTrait> = storage_arc;
    let mut config = Config {
        storage: cc_lb_config::StorageConfig::Redb { path: storage_path },
        ..Default::default()
    };
    config.runtime.data_dir = Some(dir.path().to_path_buf());
    config.aead.key_env = "__CC_LB_STATE_MACHINE_TEST_KEY__".to_owned();
    config.admin.token = Some(ADMIN_TOKEN.to_owned());
    config.downstream_auth.mode = DownstreamAuthMode::None;
    config.downstream_auth.none_mode = Some(NoneModeConfig {
        principal_id: PRINCIPAL_ID.to_owned(),
        upstream_kind: NoneModeUpstreamKind::AnthropicKey,
    });
    let app = build_app_with_storage(
        config,
        None,
        managed_store,
        storage,
        Arc::new(AeadService::from_master_key(key)),
    )
    .await?;
    Ok(TestApp { app, _dir: dir })
}

async fn seed_storage(storage: &RedbStorage) -> TestResult<()> {
    UpstreamStore::create(
        storage,
        UpstreamCreate {
            name: "state-machine-upstream".to_owned(),
            kind: UpstreamKind::Custom,
            base_url: Some(Url::parse("http://127.0.0.1:1")?),
            api_key_ciphertext: None,
            shape_plugin: None,
        },
    )
    .await?;
    PrincipalStore::create(
        storage,
        PrincipalCreate {
            name: PRINCIPAL_ID.to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models: Vec::new(),
            allowed_upstreams: Vec::new(),
            default_limits: Vec::new(),
        },
        1,
    )
    .await?;
    Ok(())
}

async fn wait_for_status(addr: SocketAddr, path: &str, status: u16) -> TestResult<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match raw_http(addr, get_request(addr, path)).await {
            Ok(response) if response.status == status => return Ok(()),
            Ok(response) if tokio::time::Instant::now() >= deadline => {
                panic!("server at {addr}{path} did not return {status}: {response:?}")
            }
            Err(error) if tokio::time::Instant::now() >= deadline => {
                panic!("server at {addr}{path} did not return {status}: {error}")
            }
            _ => tokio::time::sleep(Duration::from_millis(25)).await,
        }
    }
}

fn get_request(addr: SocketAddr, path: &str) -> Vec<u8> {
    format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n").into_bytes()
}

async fn post_register_tcp(addr: SocketAddr, name: &str, wasm: &[u8]) -> io::Result<RawResponse> {
    let boundary = format!("state-machine-{name}");
    let body = multipart_body(&boundary, name, "plugin.wasm", wasm);
    let mut request = format!(
        "POST /admin/plugins HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer {ADMIN_TOKEN}\r\nContent-Type: multipart/form-data; boundary={boundary}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    request.extend_from_slice(&body);
    raw_http(addr, request).await
}

async fn raw_http(addr: SocketAddr, request: Vec<u8>) -> io::Result<RawResponse> {
    let mut stream = TcpStream::connect(addr).await?;
    stream.write_all(&request).await?;
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await?;
    Ok(parse_response(&bytes))
}

fn parse_response(bytes: &[u8]) -> RawResponse {
    let text = String::from_utf8_lossy(bytes).to_string();
    let status = text
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .unwrap_or(0);
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_owned())
        .unwrap_or_default();
    RawResponse { status, body }
}

#[derive(Debug)]
struct RawResponse {
    status: u16,
    body: String,
}

fn multipart_body(boundary: &str, name: &str, original_filename: &str, bytes: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    push_text_part(&mut body, boundary, "name", name.as_bytes());
    push_text_part(
        &mut body,
        boundary,
        "original_filename",
        original_filename.as_bytes(),
    );
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        b"Content-Disposition: form-data; name=\"bytes\"; filename=\"plugin.wasm\"\r\n",
    );
    body.extend_from_slice(b"Content-Type: application/wasm\r\n\r\n");
    body.extend_from_slice(bytes);
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    body
}

fn push_text_part(body: &mut Vec<u8>, boundary: &str, name: &str, value: &[u8]) {
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        format!("Content-Disposition: form-data; name=\"{name}\"\r\n\r\n").as_bytes(),
    );
    body.extend_from_slice(value);
    body.extend_from_slice(b"\r\n");
}

fn plugin_wasm(plugin_name: &str, plugin_version: &str) -> Vec<u8> {
    let accept = HandshakeAccept {
        handshake_schema_version: HANDSHAKE_SCHEMA_VERSION_V1,
        envelope_version: 1,
        chosen_versions: BTreeMap::from([("route".to_owned(), 1)]),
        plugin_supported: BTreeMap::from([("route".to_owned(), vec![1])]),
        implemented_functions: BTreeSet::from(["route".to_owned()]),
        required_capabilities: BTreeSet::new(),
    };
    let handshake_output = serde_json::to_string(&accept).expect("accept serializes");
    let self_check_output = json!({
        "status": "success",
        "failures": [],
        "completed_at": 1,
    })
    .to_string();
    let wat = format!(
        r#"
(module
  (import "extism:host/env" "alloc" (func $alloc (param i64) (result i64)))
  (import "extism:host/env" "store_u8" (func $store_u8 (param i64 i32)))
  (import "extism:host/env" "output_set" (func $output_set (param i64 i64)))
  {handshake_helper}
  {self_check_helper}
  (func (export "cc_lb_handshake") (result i32)
    (call $output_set (call $handshake_out) (i64.const {handshake_len}))
    (i32.const 0))
  (func (export "cc_lb_self_check") (result i32)
    (call $output_set (call $self_check_out) (i64.const {self_check_len}))
    (i32.const 0))
  (func (export "route") (result i32)
    (i32.const 0))
)
"#,
        handshake_helper = bytes_helper("handshake_out", handshake_output.as_bytes()),
        self_check_helper = bytes_helper("self_check_out", self_check_output.as_bytes()),
        handshake_len = handshake_output.len(),
        self_check_len = self_check_output.len(),
    );
    let mut wasm = wat::parse_str(&wat).expect("wat parses");
    append_identity_section(&mut wasm, plugin_name, plugin_version);
    wasm
}

fn bytes_helper(name: &str, bytes: &[u8]) -> String {
    let mut stores = String::new();
    for (index, byte) in bytes.iter().enumerate() {
        stores.push_str(&format!(
            "  (call $store_u8 (i64.add (local.get $ptr) (i64.const {index})) (i32.const {byte}))\n"
        ));
    }
    format!(
        r#"
(func ${name} (result i64)
  (local $ptr i64)
  (local.set $ptr (call $alloc (i64.const {len})))
{stores}  (local.get $ptr))
"#,
        len = bytes.len()
    )
}

fn append_identity_section(wasm: &mut Vec<u8>, plugin_name: &str, plugin_version: &str) {
    let payload = json!({
        "magic": CC_LB_PLUGIN_MAGIC,
        "abi_envelope": 1,
        "plugin_name": plugin_name,
        "plugin_version": plugin_version,
    })
    .to_string();
    wasm.push(0);
    let mut section = Vec::new();
    encode_u32(CC_LB_PLUGIN_SECTION_NAME.len() as u32, &mut section);
    section.extend_from_slice(CC_LB_PLUGIN_SECTION_NAME.as_bytes());
    section.extend_from_slice(payload.as_bytes());
    encode_u32(section.len() as u32, wasm);
    wasm.extend_from_slice(&section);
}

fn encode_u32(mut value: u32, output: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        output.push(byte);
        if value == 0 {
            break;
        }
    }
}

#[derive(Clone, Default)]
struct CapturingMetrics {
    handshakes: Arc<AtomicU64>,
}

impl CapturingMetrics {
    fn handshake_total(&self) -> u64 {
        self.handshakes.load(Ordering::SeqCst)
    }
}

impl Recorder for CapturingMetrics {
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        Counter::from_arc(Arc::new(TestCounter {
            handshakes: self.handshakes.clone(),
            enabled: format!("{key:?}").contains("cc_lb_plugin_handshake_total"),
        }))
    }

    fn register_gauge(&self, _key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }

    fn register_histogram(&self, _key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
}

struct TestCounter {
    handshakes: Arc<AtomicU64>,
    enabled: bool,
}

impl CounterFn for TestCounter {
    fn increment(&self, value: u64) {
        if self.enabled {
            self.handshakes.fetch_add(value, Ordering::SeqCst);
        }
    }

    fn absolute(&self, value: u64) {
        if self.enabled {
            self.handshakes.store(value, Ordering::SeqCst);
        }
    }
}
