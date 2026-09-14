#![cfg(feature = "postgres")]

use std::collections::HashSet;
use std::error::Error;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use axum::Router;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::any;
use cc_lb_aead::AeadService;
use cc_lb_config::{Config, DownstreamAuthMode, PostgresPoolConfig, StorageConfig};
use cc_lb_server::app::{BuildError, build_app_with_storage, seed_app_testing_storage};
use cc_lb_server::signal::SignalHandle;
use cc_lb_storage_api::principal::{PrincipalCreate, PrincipalKind};
use cc_lb_storage_api::{
    ManagedKeyStore, PrincipalStore, Storage as StorageTrait, StorageError, UpstreamStore,
};
use cc_lb_storage_conformance::PostgresFixture;
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
    READY_TIMEOUT_DEFAULT
}
const MESSAGES_BODY: &str = r#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#;

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

#[tokio::test]
async fn t3_postgres__cross_instance_issue_auth_revoke() -> TestResult<()> {
    let rebind_completions = rebind_completions();
    let fixture = ManagedKeyFixture::spawn().await?;
    let result = run_cross_instance_issue_auth_revoke(&fixture, rebind_completions).await;
    let cleanup = fixture.shutdown().await;
    combine_result_and_cleanup(result, cleanup)
}

async fn run_cross_instance_issue_auth_revoke(
    fixture: &ManagedKeyFixture,
    rebind_completions: &tokio::sync::broadcast::Sender<()>,
) -> TestResult<()> {
    let mut issue_rebinds = rebind_completions.subscribe();
    let issued = issue_key(fixture.instance_a.admin_addr, "issued-on-instance-a").await?;
    if issued.principal_id != PRINCIPAL_ID {
        return Err(error(format!(
            "issued key principal mismatch: expected={PRINCIPAL_ID} actual={}",
            issued.principal_id
        )));
    }
    let authenticated = wait_for_proxy_status(
        fixture.instance_b.proxy_addr,
        &issued.plaintext_key,
        200,
        401,
        &mut issue_rebinds,
    )
    .await?;
    if !authenticated.body.contains(r#""type":"message""#) {
        return Err(error(format!(
            "unexpected proxy success body: {}",
            authenticated.body
        )));
    }

    let mut revoke_rebinds = rebind_completions.subscribe();
    let revoked = revoke_key(fixture.instance_a.admin_addr, &issued.key_id).await?;
    if revoked.status != 200 {
        return Err(error(format!(
            "instance A revoke should succeed: status={} body={}",
            revoked.status, revoked.body
        )));
    }
    wait_for_proxy_status(
        fixture.instance_b.proxy_addr,
        &issued.plaintext_key,
        401,
        200,
        &mut revoke_rebinds,
    )
    .await?;

    Ok(())
}

#[tokio::test]
async fn t3_postgres__concurrent_cross_instance_issue() -> TestResult<()> {
    let _ = rebind_completions();
    let fixture = ManagedKeyFixture::spawn().await?;
    let result = run_concurrent_cross_instance_issue(&fixture).await;
    let cleanup = fixture.shutdown().await;
    combine_result_and_cleanup(result, cleanup)
}

async fn run_concurrent_cross_instance_issue(fixture: &ManagedKeyFixture) -> TestResult<()> {
    let mut tasks = Vec::with_capacity(EXPECTED_ISSUED_KEYS);
    for index in 0..TASKS_PER_INSTANCE {
        let admin_addr = fixture.instance_a.admin_addr;
        tasks.push(tokio::task::spawn(async move {
            issue_key(admin_addr, &format!("instance-a-{index}")).await
        }));
    }
    for index in 0..TASKS_PER_INSTANCE {
        let admin_addr = fixture.instance_b.admin_addr;
        tasks.push(tokio::task::spawn(async move {
            issue_key(admin_addr, &format!("instance-b-{index}")).await
        }));
    }

    let mut key_ids = HashSet::with_capacity(EXPECTED_ISSUED_KEYS);
    for task in tasks {
        let issued = task.await??;
        if !key_ids.insert(issued.key_id) {
            return Err(error("duplicate key_id issued by concurrent requests"));
        }
    }

    if key_ids.len() != EXPECTED_ISSUED_KEYS {
        return Err(error(format!(
            "issued key count mismatch: expected={EXPECTED_ISSUED_KEYS} actual={}",
            key_ids.len()
        )));
    }
    println!("issued={} unique={}", EXPECTED_ISSUED_KEYS, key_ids.len());

    Ok(())
}

struct ManagedKeyFixture {
    database: PostgresFixture,
    _serial: sqlx::PgConnection,
    upstream: RunningUpstream,
    instance_a: RunningApp,
    instance_b: RunningApp,
}

impl ManagedKeyFixture {
    async fn spawn() -> TestResult<Self> {
        let database = cc_lb_storage_conformance::postgres_fixture().await?;
        let database_url =
            match scoped_database_url(database.database_url(), database.schema_name()) {
                Ok(url) => url,
                Err(source) => {
                    let cleanup = teardown_database(database).await;
                    return Err(with_cleanup_error(source, cleanup));
                }
            };
        let serial = match crate::common::postgres_test_lock(database.database_url()).await {
            Ok(serial) => serial,
            Err(source) => {
                let cleanup = teardown_database(database).await;
                return Err(with_cleanup_error(source.into(), cleanup));
            }
        };
        let upstream = match spawn_ok_upstream().await {
            Ok(upstream) => upstream,
            Err(source) => {
                let cleanup = teardown_database(database).await;
                drop(serial);
                return Err(with_cleanup_error(source, cleanup));
            }
        };
        let clock = cc_lb_testkit::fixed_clock(1_700_000_000);
        let setup = async {
            seed_app_testing_storage(
                database.storage(),
                Some(Url::parse(&format!("http://{}", upstream.addr))?),
                &*clock,
            )
            .await?;
            seed_test_principal(database.storage()).await
        }
        .await;
        if let Err(source) = setup {
            let cleanup = cleanup_partial_fixture(None, None, upstream, database, serial).await;
            return Err(with_cleanup_error(source, cleanup));
        }

        let instance_a = match build_running_app(database_url.clone(), "instance-a").await {
            Ok(instance) => instance,
            Err(source) => {
                let cleanup = cleanup_partial_fixture(None, None, upstream, database, serial).await;
                return Err(with_cleanup_error(source, cleanup));
            }
        };
        let instance_b = match build_running_app(database_url, "instance-b").await {
            Ok(instance) => instance,
            Err(source) => {
                let cleanup =
                    cleanup_partial_fixture(Some(instance_a), None, upstream, database, serial)
                        .await;
                return Err(with_cleanup_error(source, cleanup));
            }
        };

        Ok(Self {
            database,
            _serial: serial,
            upstream,
            instance_a,
            instance_b,
        })
    }

    async fn shutdown(self) -> TestResult<()> {
        cleanup_partial_fixture(
            Some(self.instance_a),
            Some(self.instance_b),
            self.upstream,
            self.database,
            self._serial,
        )
        .await
    }
}

fn scoped_database_url(database_url: &str, schema_name: &str) -> TestResult<String> {
    let mut url = Url::parse(database_url)?;
    url.query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema_name},public"));
    Ok(url.to_string())
}

async fn teardown_database(database: PostgresFixture) -> TestResult<()> {
    database.teardown().await.map_err(|source| source.into())
}

async fn cleanup_partial_fixture(
    instance_a: Option<RunningApp>,
    instance_b: Option<RunningApp>,
    upstream: RunningUpstream,
    database: PostgresFixture,
    _serial: sqlx::PgConnection,
) -> TestResult<()> {
    let mut failures = Vec::new();
    if let Some(instance) = instance_a
        && let Err(source) = instance.shutdown().await
    {
        failures.push(format!("shut down instance A: {source}"));
    }
    if let Some(instance) = instance_b
        && let Err(source) = instance.shutdown().await
    {
        failures.push(format!("shut down instance B: {source}"));
    }
    if let Err(source) = upstream.shutdown().await {
        failures.push(format!("shut down fake upstream: {source}"));
    }
    if let Err(source) = database.teardown().await {
        failures.push(format!("tear down PostgreSQL fixture: {source:#}"));
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(error(failures.join("; ")))
    }
}

fn with_cleanup_error(
    source: Box<dyn Error + Send + Sync>,
    cleanup: TestResult<()>,
) -> Box<dyn Error + Send + Sync> {
    match cleanup {
        Ok(()) => source,
        Err(cleanup_error) => error(format!("{source}; cleanup also failed: {cleanup_error}")),
    }
}

fn combine_result_and_cleanup(result: TestResult<()>, cleanup: TestResult<()>) -> TestResult<()> {
    match (result, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(source), Ok(())) => Err(source),
        (Ok(()), Err(cleanup_error)) => Err(cleanup_error),
        (Err(source), Err(cleanup_error)) => Err(error(format!(
            "{source}; cleanup also failed: {cleanup_error}"
        ))),
    }
}

async fn build_running_app(database_url: String, label: &'static str) -> TestResult<RunningApp> {
    let clock = cc_lb_testkit::fixed_clock(1_700_000_000);
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
    let mut app = build_app_with_storage(
        test_config(&database_url, label),
        None,
        managed_store,
        storage,
        Arc::new(AeadService::from_master_key([0; 32])),
        clock,
    )
    .await?;

    let proxy_listener = TcpListener::bind("127.0.0.1:0").await?;
    let admin_listener = TcpListener::bind("127.0.0.1:0").await?;
    let proxy_addr = proxy_listener.local_addr()?;
    let admin_addr = admin_listener.local_addr()?;
    app.proxy_addr = proxy_addr;
    app.admin_addr = admin_addr;
    let signal = app.signal_handle();
    let task = tokio::task::spawn(app.start_with_listeners(proxy_listener, admin_listener));

    let running = RunningApp {
        _label: label,
        proxy_addr,
        admin_addr,
        signal,
        task: Some(task),
    };
    if let Err(source) = wait_for_status(running.proxy_addr, "/healthz", 200).await {
        let cleanup = running.shutdown().await;
        return Err(with_cleanup_error(source, cleanup));
    }
    if let Err(source) = wait_for_status(running.admin_addr, "/admin/health", 200).await {
        let cleanup = running.shutdown().await;
        return Err(with_cleanup_error(source, cleanup));
    }
    Ok(running)
}

fn test_config(database_url: &str, label: &str) -> Config {
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
    config.cluster.instance_url = Some(format!("http://{label}.example.test"));
    config.cluster.token_env = crate::common::TEST_NONEMPTY_ENV.to_owned();
    config
}

async fn seed_test_principal(storage: &dyn StorageTrait) -> TestResult<()> {
    let now = 1_700_000_000;
    let upstream = UpstreamStore::get_by_name(storage, "test-upstream")
        .await?
        .ok_or_else(|| error("seeded test upstream is missing"))?;
    match PrincipalStore::create(
        storage,
        PrincipalCreate {
            name: PRINCIPAL_ID.to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models: vec!["*".to_owned()],
            allowed_upstreams: vec![upstream.id],
            default_limits: vec![],
            cache_keepalive: None,
        },
        now,
    )
    .await
    {
        Ok(_) | Err(StorageError::Conflict { .. }) => Ok(()),
        Err(source) => Err(source.into()),
    }
}

struct RunningUpstream {
    addr: SocketAddr,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<(), io::Error>>>,
}

impl RunningUpstream {
    async fn shutdown(mut self) -> TestResult<()> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let mut task = self
            .task
            .take()
            .ok_or_else(|| error("fake upstream task missing"))?;
        match tokio::time::timeout(ready_timeout(), &mut task).await {
            Ok(result) => {
                result??;
                Ok(())
            }
            Err(_) => {
                task.abort();
                let _ = task.await;
                Err(error("fake upstream did not shut down"))
            }
        }
    }
}

impl Drop for RunningUpstream {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
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
    let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
    let task = tokio::task::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
    });
    Ok(RunningUpstream {
        addr,
        shutdown: Some(shutdown),
        task: Some(task),
    })
}

struct RunningApp {
    _label: &'static str,
    proxy_addr: SocketAddr,
    admin_addr: SocketAddr,
    signal: SignalHandle,
    task: Option<JoinHandle<Result<(), BuildError>>>,
}

impl RunningApp {
    async fn shutdown(mut self) -> TestResult<()> {
        self.signal.start_shutdown();
        let mut task = self.task.take().ok_or_else(|| error("app task missing"))?;
        match tokio::time::timeout(ready_timeout(), &mut task).await {
            Ok(result) => {
                result??;
                Ok(())
            }
            Err(_) => {
                task.abort();
                let _ = task.await;
                Err(error(format!("{} did not shut down", self._label)))
            }
        }
    }
}

impl Drop for RunningApp {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
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
    if response.status != 201 {
        return Err(error(format!(
            "issue key failed for {label}: status={} body={}",
            response.status, response.body
        )));
    }

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
