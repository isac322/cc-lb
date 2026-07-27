#![cfg(feature = "postgres")]

use std::collections::HashSet;
use std::error::Error;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::{Duration, Instant};

use axum::Router;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::any;
use cc_lb_aead::AeadService;
use cc_lb_config::{Config, DownstreamAuthMode, PostgresPoolConfig, StorageConfig};
use cc_lb_server::app::{App, build_app_with_storage, seed_app_testing_storage};
use cc_lb_storage_api::principal::{PrincipalCreate, PrincipalKind};
use cc_lb_storage_api::{
    BackendKind, ManagedKeyStore, PrincipalStore, Storage as StorageTrait, StorageError,
};
use cc_lb_storage_postgres::adapter::retry::RetryPolicy;
use cc_lb_storage_postgres::{PostgresManagedKeyStore, PostgresStorage};
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::{Layer, prelude::*};
use url::Url;

const ADMIN_TOKEN: &str = "test-token";
const PRINCIPAL_ID: &str = "multi-instance-principal";
const TASKS_PER_INSTANCE: usize = 50;
const EXPECTED_ISSUED_KEYS: usize = TASKS_PER_INSTANCE * 2;
const READY_TIMEOUT_DEFAULT: Duration = Duration::from_secs(30);
const READY_POLL_INTERVAL: Duration = Duration::from_millis(50);

fn ready_timeout() -> Duration {
    std::env::var("CC_LB_TEST_READY_TIMEOUT_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(READY_TIMEOUT_DEFAULT)
}
const MESSAGES_BODY: &str = r#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#;

static POSTGRES_TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
static REBIND_COMPLETIONS: LazyLock<tokio::sync::broadcast::Sender<()>> = LazyLock::new(|| {
    let (tx, _) = tokio::sync::broadcast::channel(32);
    let subscriber = tracing_subscriber::registry().with(RebindCompletionLayer { tx: tx.clone() });
    tracing::subscriber::set_global_default(subscriber)
        .expect("managed-key test tracing subscriber installs once");
    tx
});

const REBIND_TERMINAL_MESSAGES: [&str; 3] = [
    "dynamic view rebound after runtime change notification",
    "notify-triggered view rejected: newer generation already resident",
    "dynamic view rebind failed after runtime change notification",
];

#[derive(Default)]
struct EventMessage {
    value: Option<String>,
}

impl Visit for EventMessage {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.value = Some(format!("{value:?}"));
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.value = Some(value.to_owned());
        }
    }
}

struct RebindCompletionLayer {
    tx: tokio::sync::broadcast::Sender<()>,
}

impl<S> Layer<S> for RebindCompletionLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
        if event.metadata().target() != "cc_lb_server::notify_listener" {
            return;
        }
        let mut message = EventMessage::default();
        event.record(&mut message);
        if message.value.as_deref().is_some_and(|message| {
            REBIND_TERMINAL_MESSAGES
                .iter()
                .any(|terminal| message.contains(terminal))
        }) {
            let _ = self.tx.send(());
        }
    }
}

fn rebind_completions() -> &'static tokio::sync::broadcast::Sender<()> {
    &REBIND_COMPLETIONS
}

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_instance_issue_auth_revoke() -> TestResult<()> {
    let Some(database_url) = ci_postgres_url() else {
        eprintln!("skipped: CI_POSTGRES_URL unset");
        return Ok(());
    };
    let rebind_completions = rebind_completions();
    let _serial = postgres_test_lock().lock().await;

    reset_managed_key_tables(&database_url).await?;
    let upstream = spawn_ok_upstream().await?;
    let (instance_a, instance_b) = spawn_two_instances(&database_url, upstream.addr).await?;

    let mut issue_rebinds = rebind_completions.subscribe();
    let issued = issue_key(instance_a.admin_addr, "issued-on-instance-a").await?;
    assert_eq!(issued.principal_id, PRINCIPAL_ID);
    let authenticated = wait_for_proxy_status(
        instance_b.proxy_addr,
        &issued.plaintext_key,
        200,
        401,
        &mut issue_rebinds,
    )
    .await?;
    assert!(
        authenticated.body.contains(r#""type":"message""#),
        "unexpected proxy success body: {}",
        authenticated.body
    );

    let mut revoke_rebinds = rebind_completions.subscribe();
    let revoked = revoke_key(instance_a.admin_addr, &issued.key_id).await?;
    assert_eq!(
        revoked.status, 200,
        "instance A revoke should succeed: body={}",
        revoked.body
    );
    wait_for_proxy_status(
        instance_b.proxy_addr,
        &issued.plaintext_key,
        401,
        200,
        &mut revoke_rebinds,
    )
    .await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_cross_instance_issue() -> TestResult<()> {
    let Some(database_url) = ci_postgres_url() else {
        eprintln!("skipped: CI_POSTGRES_URL unset");
        return Ok(());
    };
    let _ = rebind_completions();
    let _serial = postgres_test_lock().lock().await;

    reset_managed_key_tables(&database_url).await?;
    let upstream = spawn_ok_upstream().await?;
    let (instance_a, instance_b) = spawn_two_instances(&database_url, upstream.addr).await?;

    let mut tasks = Vec::with_capacity(EXPECTED_ISSUED_KEYS);
    for index in 0..TASKS_PER_INSTANCE {
        let admin_addr = instance_a.admin_addr;
        tasks.push(tokio::task::spawn(async move {
            issue_key(admin_addr, &format!("instance-a-{index}")).await
        }));
    }
    for index in 0..TASKS_PER_INSTANCE {
        let admin_addr = instance_b.admin_addr;
        tasks.push(tokio::task::spawn(async move {
            issue_key(admin_addr, &format!("instance-b-{index}")).await
        }));
    }

    let mut key_ids = HashSet::with_capacity(EXPECTED_ISSUED_KEYS);
    for task in tasks {
        let issued = task.await??;
        assert!(
            key_ids.insert(issued.key_id),
            "duplicate key_id issued by concurrent requests"
        );
    }

    assert_eq!(key_ids.len(), EXPECTED_ISSUED_KEYS);
    println!("issued={} unique={}", EXPECTED_ISSUED_KEYS, key_ids.len());

    Ok(())
}

fn ci_postgres_url() -> Option<String> {
    std::env::var("CI_POSTGRES_URL").ok()
}

fn postgres_test_lock() -> &'static tokio::sync::Mutex<()> {
    POSTGRES_TEST_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

async fn spawn_two_instances(
    database_url: &str,
    upstream_addr: SocketAddr,
) -> TestResult<(RunningApp, RunningApp)> {
    let instance_a = tokio::task::spawn(build_running_app(
        database_url.to_owned(),
        upstream_addr,
        "instance-a",
    ));
    let instance_b = tokio::task::spawn(build_running_app(
        database_url.to_owned(),
        upstream_addr,
        "instance-b",
    ));

    let (instance_a, instance_b) = tokio::join!(instance_a, instance_b);
    Ok((instance_a??, instance_b??))
}

async fn build_running_app(
    database_url: String,
    upstream_addr: SocketAddr,
    label: &'static str,
) -> TestResult<RunningApp> {
    let clock: cc_lb_engine::ClockHandle = Arc::new(cc_lb_engine::SystemClock);
    let pool = PgPoolOptions::new()
        .max_connections(16)
        .connect(&database_url)
        .await?;
    let managed_store: Arc<dyn ManagedKeyStore> = Arc::new(PostgresManagedKeyStore::new(
        pool.clone(),
        Arc::new(RetryPolicy::default()),
        clock.clone(),
    ));
    let storage: Arc<dyn StorageTrait> = Arc::new(PostgresStorage::new(pool, clock.clone()));
    storage.initialize(BackendKind::Postgres).await?;
    seed_app_testing_storage(
        storage.as_ref(),
        Some(Url::parse(&format!("http://{upstream_addr}"))?),
        &*clock,
    )
    .await?;
    seed_test_principal(storage.as_ref()).await?;
    let mut app = build_app_with_storage(
        test_config(&database_url),
        None,
        managed_store,
        storage,
        Arc::new(AeadService::from_master_key([0; 32])),
        clock.clone(),
    )
    .await?;

    let proxy_listener = TcpListener::bind("127.0.0.1:0").await?;
    let admin_listener = TcpListener::bind("127.0.0.1:0").await?;
    let proxy_addr = proxy_listener.local_addr()?;
    let admin_addr = admin_listener.local_addr()?;
    app.proxy_addr = proxy_addr;
    app.admin_addr = admin_addr;

    let proxy_router = app.router.clone();
    let admin_router = app.admin_router.clone();
    let proxy_task =
        tokio::task::spawn(async move { axum::serve(proxy_listener, proxy_router).await });
    let admin_task =
        tokio::task::spawn(async move { axum::serve(admin_listener, admin_router).await });

    let running = RunningApp {
        _label: label,
        _app: app,
        proxy_addr,
        admin_addr,
        proxy_task,
        admin_task,
    };
    wait_for_status(running.proxy_addr, "/healthz", 200).await?;
    wait_for_status(running.admin_addr, "/admin/health", 200).await?;
    Ok(running)
}

fn test_config(database_url: &str) -> Config {
    let mut config = Config::default();
    config.listener.proxy_addr = "127.0.0.1:0".parse().expect("valid proxy addr");
    config.listener.admin_addr = "127.0.0.1:0".parse().expect("valid admin addr");
    config.storage = StorageConfig::Postgres {
        url: database_url.to_owned(),
        pool: PostgresPoolConfig::default(),
    };
    config.admin.token = Some(ADMIN_TOKEN.to_owned());
    config.downstream_auth.mode = DownstreamAuthMode::ApiKey;
    config.downstream_auth.none_mode = None;
    config
}

async fn seed_test_principal(storage: &dyn StorageTrait) -> TestResult<()> {
    use cc_lb_engine::Clock as _;

    let clock = cc_lb_engine::SystemClock;
    let now = clock
        .now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    match PrincipalStore::create(
        storage,
        PrincipalCreate {
            name: PRINCIPAL_ID.to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models: vec!["*".to_owned()],
            allowed_upstreams: vec![],
            default_limits: vec![],
            cache_keepalive: None,
        },
        now,
    )
    .await
    {
        Ok(_) | Err(StorageError::Conflict { .. }) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

async fn reset_managed_key_tables(database_url: &str) -> TestResult<()> {
    let clock: cc_lb_engine::ClockHandle = Arc::new(cc_lb_engine::SystemClock);
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await?;
    let storage: Arc<dyn StorageTrait> =
        Arc::new(PostgresStorage::new(pool.clone(), clock.clone()));
    storage.initialize(BackendKind::Postgres).await?;
    sqlx::query("TRUNCATE managed_api_key_index_v1, managed_api_keys_v1")
        .execute(&pool)
        .await?;
    sqlx::query("DELETE FROM upstream_spec_v1 WHERE name = 'test-upstream'")
        .execute(&pool)
        .await?;
    sqlx::query("DROP SCHEMA IF EXISTS apalis CASCADE")
        .execute(&pool)
        .await?;
    sqlx::query("DROP SCHEMA IF EXISTS cc_lb_scheduler CASCADE")
        .execute(&pool)
        .await?;
    pool.close().await;

    Ok(())
}

struct RunningUpstream {
    addr: SocketAddr,
    task: JoinHandle<Result<(), io::Error>>,
}

impl Drop for RunningUpstream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn spawn_ok_upstream() -> TestResult<RunningUpstream> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let router = Router::new().fallback(any(|| async {
        (
            StatusCode::OK,
            [("content-type", "application/json")],
            r#"{"id":"msg_multi_instance","type":"message","role":"assistant","model":"claude-3-5-sonnet-20241022","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":7,"output_tokens":3}}"#,
        )
            .into_response()
    }));
    let task = tokio::task::spawn(async move { axum::serve(listener, router).await });
    Ok(RunningUpstream { addr, task })
}

struct RunningApp {
    _label: &'static str,
    _app: App,
    proxy_addr: SocketAddr,
    admin_addr: SocketAddr,
    proxy_task: JoinHandle<Result<(), io::Error>>,
    admin_task: JoinHandle<Result<(), io::Error>>,
}

impl Drop for RunningApp {
    fn drop(&mut self) {
        self.proxy_task.abort();
        self.admin_task.abort();
    }
}

#[derive(Debug)]
struct IssuedKey {
    principal_id: String,
    key_id: String,
    plaintext_key: String,
}

async fn issue_key(admin_addr: SocketAddr, label: &str) -> TestResult<IssuedKey> {
    let response = admin_post_json(
        admin_addr,
        &format!("/admin/v1/principals/{PRINCIPAL_ID}/keys"),
        json!({
            "label": label,
            "upstream_kind": "anthropic_key",
        }),
    )
    .await?;
    assert_eq!(
        response.status, 201,
        "issue key failed for {label}: body={}",
        response.body
    );

    let payload: Value = serde_json::from_str(&response.body)?;
    Ok(IssuedKey {
        principal_id: json_string(&payload, "principal_id")?,
        key_id: json_string(&payload, "key_id")?,
        plaintext_key: json_string(&payload, "plaintext_key")?,
    })
}

async fn revoke_key(admin_addr: SocketAddr, key_id: &str) -> TestResult<RawResponse> {
    admin_post_body(
        admin_addr,
        &format!("/admin/v1/principals/{PRINCIPAL_ID}/keys/{key_id}/revoke"),
        "",
        "application/octet-stream",
    )
    .await
}

async fn proxy_messages(proxy_addr: SocketAddr, api_key: &str) -> TestResult<RawResponse> {
    http_post_body(
        proxy_addr,
        "/v1/messages",
        MESSAGES_BODY,
        "application/json",
        &[("x-api-key", api_key), ("anthropic-version", "2023-06-01")],
    )
    .await
}

async fn admin_post_json(addr: SocketAddr, path: &str, body: Value) -> TestResult<RawResponse> {
    admin_post_body(addr, path, &body.to_string(), "application/json").await
}

async fn admin_post_body(
    addr: SocketAddr,
    path: &str,
    body: &str,
    content_type: &str,
) -> TestResult<RawResponse> {
    let auth = format!("Bearer {ADMIN_TOKEN}");
    http_post_body(
        addr,
        path,
        body,
        content_type,
        &[("Authorization", auth.as_str())],
    )
    .await
}

async fn wait_for_proxy_status(
    proxy_addr: SocketAddr,
    api_key: &str,
    expected_status: u16,
    transitional_status: u16,
    rebinds: &mut tokio::sync::broadcast::Receiver<()>,
) -> TestResult<RawResponse> {
    let mut observed_rebinds = 0u64;
    let mut last_response = None;
    let result = tokio::time::timeout(ready_timeout(), async {
        loop {
            match rebinds.recv().await {
                Ok(()) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    return Err(error("rebind completion channel closed"));
                }
            }
            observed_rebinds += 1;
            let response = proxy_messages(proxy_addr, api_key).await?;
            if response.status == expected_status {
                return Ok(response);
            }
            if response.status != transitional_status {
                return Err(error(format!(
                    "instance {proxy_addr} returned unexpected status {} after rebind: body={}",
                    response.status, response.body
                )));
            }
            last_response = Some(response);
        }
    })
    .await;
    match result {
        Ok(result) => result,
        Err(_) => Err(error(format!(
            "instance {proxy_addr} did not return status {expected_status} after \
             {observed_rebinds} completed rebinds; last_response={last_response:?}"
        ))),
    }
}

async fn wait_for_status(addr: SocketAddr, path: &str, status: u16) -> TestResult<()> {
    let deadline = Instant::now() + ready_timeout();
    loop {
        let last = match http_get(addr, path).await {
            Ok(response) if response.status == status => return Ok(()),
            Ok(response) => format!("status={} body={}", response.status, response.body),
            Err(source) => source.to_string(),
        };

        if Instant::now() >= deadline {
            return Err(error(format!(
                "server {addr}{path} did not become ready with status {status}: {last}"
            )));
        }

        tokio::time::sleep(READY_POLL_INTERVAL).await;
    }
}

async fn http_get(addr: SocketAddr, path: &str) -> TestResult<RawResponse> {
    raw_http(
        addr,
        &format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"),
    )
    .await
}

async fn http_post_body(
    addr: SocketAddr,
    path: &str,
    body: &str,
    content_type: &str,
    extra_headers: &[(&str, &str)],
) -> TestResult<RawResponse> {
    let mut request = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for (name, value) in extra_headers {
        request.push_str(name);
        request.push_str(": ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    request.push_str(body);
    raw_http(addr, &request).await
}

#[derive(Debug)]
struct RawResponse {
    status: u16,
    body: String,
}

async fn raw_http(addr: SocketAddr, request: &str) -> TestResult<RawResponse> {
    let mut stream = TcpStream::connect(addr).await?;
    stream.write_all(request.as_bytes()).await?;

    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await?;
    let text = String::from_utf8_lossy(&bytes);
    let status = text
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .unwrap_or(0);
    let body = response_body(&text);
    Ok(RawResponse { status, body })
}

fn response_body(text: &str) -> String {
    let Some((headers, body)) = text.split_once("\r\n\r\n") else {
        return String::new();
    };
    if headers
        .lines()
        .any(|line| line.eq_ignore_ascii_case("transfer-encoding: chunked"))
    {
        decode_chunked_body(body).unwrap_or_else(|| body.to_owned())
    } else {
        body.to_owned()
    }
}

fn decode_chunked_body(body: &str) -> Option<String> {
    let mut remaining = body.as_bytes();
    let mut decoded = Vec::new();

    loop {
        let line_end = remaining.windows(2).position(|window| window == b"\r\n")?;
        let size_text = std::str::from_utf8(&remaining[..line_end]).ok()?;
        let size = usize::from_str_radix(size_text.trim(), 16).ok()?;
        remaining = &remaining[line_end + 2..];
        if size == 0 {
            break;
        }
        if remaining.len() < size + 2 {
            return None;
        }
        decoded.extend_from_slice(&remaining[..size]);
        remaining = &remaining[size + 2..];
    }

    String::from_utf8(decoded).ok()
}

fn json_string(payload: &Value, field: &str) -> TestResult<String> {
    payload
        .get(field)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| error(format!("response missing {field}: {payload}")))
}

fn error(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(message.into()))
}
