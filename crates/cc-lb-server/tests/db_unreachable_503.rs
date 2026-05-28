#![cfg(feature = "postgres")]

use std::error::Error;
use std::io;
use std::net::SocketAddr;
use std::process::{Command, Output};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::http::{Request, Response, StatusCode};
use axum::response::IntoResponse;
use axum::routing::any;
use cc_lb_aead::AeadService;
use cc_lb_config::{
    AuthStrategy, Config, DownstreamAuthMode, Limit, LimitKind, PostgresPoolConfig, PrincipalSpec,
    PrincipalType, StorageConfig, UpstreamKind, UpstreamSpec,
};
use cc_lb_server::app::{App, build_app_for_testing_postgres, build_app_with_storage};
use cc_lb_storage_api::{BackendKind, ManagedKeyStore, Storage as StorageTrait};
use cc_lb_storage_postgres::adapter::retry::RetryPolicy;
use cc_lb_storage_postgres::{PostgresManagedKeyStore, PostgresStorage};
use http::header::RETRY_AFTER;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tower::ServiceExt;
use url::Url;

const POSTGRES_CONTAINER: &str = "cc-lb-postgres";
const DOCKER_HOST: &str = "tcp://localhost:2375";
const ADMIN_TOKEN: &str = "test-token";
const PRINCIPAL_ID: &str = "test-principal";
const FAILURE_LATENCY_CEILING: Duration = Duration::from_millis(1_500);
const READY_TIMEOUT: Duration = Duration::from_secs(30);
const READY_POLL_INTERVAL: Duration = Duration::from_millis(200);
const CONNECT_PROBE_TIMEOUT: Duration = Duration::from_millis(500);

static CHAOS_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
#[ignore]
async fn db_unreachable_returns_503_with_retry_after() -> TestResult<()> {
    let Some(url) = ci_postgres_url() else {
        eprintln!("skipped: CI_POSTGRES_URL unset");
        return Ok(());
    };
    let _serial = chaos_lock().lock().await;

    ensure_postgres_up(&url).await?;
    let _guard = PostgresRestartGuard;
    let (app, _upstream) = build_api_key_app_for_testing_postgres(&url).await?;
    let api_key = issue_key(&app).await?;

    let before = proxy_messages(&app, &api_key).await?;
    assert_eq!(before.status(), StatusCode::OK);

    stop_postgres()?;
    let started = Instant::now();
    let unavailable = proxy_messages(&app, &api_key).await?;
    let elapsed = started.elapsed();

    assert_eq!(unavailable.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_retry_after(&unavailable)?;
    assert!(
        elapsed <= FAILURE_LATENCY_CEILING,
        "DB-down auth latency {elapsed:?} exceeded retry budget ceiling {FAILURE_LATENCY_CEILING:?}"
    );

    Ok(())
}

#[tokio::test]
#[ignore]
async fn db_recovery_after_restart() -> TestResult<()> {
    let Some(url) = ci_postgres_url() else {
        eprintln!("skipped: CI_POSTGRES_URL unset");
        return Ok(());
    };
    let _serial = chaos_lock().lock().await;

    ensure_postgres_up(&url).await?;
    let _guard = PostgresRestartGuard;
    let (app, _upstream) = build_api_key_app_for_testing_postgres(&url).await?;
    let api_key = issue_key(&app).await?;

    let before = proxy_messages(&app, &api_key).await?;
    assert_eq!(before.status(), StatusCode::OK);

    stop_postgres()?;
    start_postgres()?;
    wait_postgres_ready(&url).await?;

    let after = proxy_messages(&app, &api_key).await?;
    assert_eq!(after.status(), StatusCode::OK);

    Ok(())
}

fn ci_postgres_url() -> Option<String> {
    std::env::var("CI_POSTGRES_URL").ok()
}

fn chaos_lock() -> &'static tokio::sync::Mutex<()> {
    CHAOS_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

fn docker(action: &str, container: &str) -> io::Result<Output> {
    Command::new("docker")
        .env("DOCKER_HOST", DOCKER_HOST)
        .args([action, container])
        .output()
}

struct PostgresRestartGuard;

impl Drop for PostgresRestartGuard {
    fn drop(&mut self) {
        let _ = docker("start", POSTGRES_CONTAINER);
    }
}

fn stop_postgres() -> TestResult<()> {
    let output = docker("stop", POSTGRES_CONTAINER)?;
    assert_docker_success("stop", &output)
}

fn start_postgres() -> TestResult<()> {
    let output = docker("start", POSTGRES_CONTAINER)?;
    assert_docker_success("start", &output)
}

fn assert_docker_success(action: &str, output: &Output) -> TestResult<()> {
    if output.status.success() {
        return Ok(());
    }

    Err(error(format!(
        "docker {action} {POSTGRES_CONTAINER} failed: status={:?} stdout={} stderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout).trim(),
        String::from_utf8_lossy(&output.stderr).trim()
    )))
}

async fn ensure_postgres_up(url: &str) -> TestResult<()> {
    if postgres_ready(url).await {
        return Ok(());
    }

    start_postgres()?;
    wait_postgres_ready(url).await
}

async fn wait_postgres_ready(url: &str) -> TestResult<()> {
    let deadline = Instant::now() + READY_TIMEOUT;

    loop {
        let probe_error =
            match tokio::time::timeout(CONNECT_PROBE_TIMEOUT, connect_probe(url)).await {
                Ok(Ok(())) => return Ok(()),
                Ok(Err(source)) => source.to_string(),
                Err(_) => format!("connect probe exceeded {CONNECT_PROBE_TIMEOUT:?}"),
            };

        if Instant::now() >= deadline {
            return Err(error(format!(
                "postgres did not become ready within {READY_TIMEOUT:?}: {probe_error}"
            )));
        }

        tokio::time::sleep(READY_POLL_INTERVAL).await;
    }
}

async fn postgres_ready(url: &str) -> bool {
    matches!(
        tokio::time::timeout(CONNECT_PROBE_TIMEOUT, connect_probe(url)).await,
        Ok(Ok(()))
    )
}

async fn connect_probe(url: &str) -> Result<(), sqlx::Error> {
    let pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
    pool.close().await;
    Ok(())
}

async fn build_api_key_app_for_testing_postgres(
    database_url: &str,
) -> TestResult<(App, RunningUpstream)> {
    let fixture_app = build_app_for_testing_postgres(database_url).await?;
    drop(fixture_app);
    reset_managed_key_tables(database_url).await?;

    let upstream = spawn_ok_upstream().await?;
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_millis(100))
        .connect(database_url)
        .await?;
    let managed_store: Arc<dyn ManagedKeyStore> = Arc::new(PostgresManagedKeyStore::new(
        pool,
        Arc::new(RetryPolicy::default()),
    ));
    let app = build_app_with_storage(
        test_config(database_url, upstream.addr)?,
        None,
        managed_store,
        None,
        Arc::new(AeadService::from_master_key([0; 32])),
    )?;

    Ok((app, upstream))
}

async fn reset_managed_key_tables(database_url: &str) -> TestResult<()> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await?;
    let storage: Arc<dyn StorageTrait> = Arc::new(PostgresStorage::new(pool.clone()));
    storage.initialize(BackendKind::Postgres).await?;
    sqlx::query("TRUNCATE managed_api_key_index_v1, managed_api_keys_v1")
        .execute(&pool)
        .await?;
    pool.close().await;
    Ok(())
}

fn test_config(database_url: &str, upstream_addr: SocketAddr) -> TestResult<Config> {
    let mut config = Config::default();
    config.storage = StorageConfig::Postgres {
        url: database_url.to_owned(),
        pool: PostgresPoolConfig::default(),
    };
    config.admin.token = Some(ADMIN_TOKEN.to_owned());
    config.downstream_auth.mode = DownstreamAuthMode::ApiKey;
    config.downstream_auth.none_mode = None;
    config.principals.insert(
        PRINCIPAL_ID.to_owned(),
        PrincipalSpec {
            principal_type: PrincipalType::Machine,
            default_limits: vec![Limit {
                kind: LimitKind::Requests,
                window: Duration::from_secs(60),
                cap_micros: 1_000,
            }],
            enabled: true,
            allowed_models: vec!["*".to_owned()],
            credentials_ref: None,
            router_plugin: None,
            observability_hooks: None,
        },
    );
    config.upstreams.insert(
        "test-upstream".to_owned(),
        UpstreamSpec {
            kind: UpstreamKind::Custom,
            base_url: Some(Url::parse(&format!("http://{upstream_addr}"))?),
            region: None,
            project: None,
            auth_strategy: AuthStrategy::ApiKey,
            credentials_ref: None,
        },
    );
    Ok(config)
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
            "{\"ok\":true}",
        )
            .into_response()
    }));
    let task = tokio::spawn(async move { axum::serve(listener, router).await });

    Ok(RunningUpstream { addr, task })
}

async fn issue_key(app: &App) -> TestResult<String> {
    let response = app
        .admin_router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/admin/principals/{PRINCIPAL_ID}/keys"))
                .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({
                        "label": "db unreachable chaos key",
                        "upstream_kind": "anthropic_key",
                        "upstream_credential_ref": "test-upstream",
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::CREATED);

    let body = response.into_body().collect().await?.to_bytes();
    let payload: Value = serde_json::from_slice(&body)?;
    let Some(plaintext_key) = payload.get("plaintext_key").and_then(Value::as_str) else {
        return Err(error(format!(
            "issue key response missing plaintext_key: {payload}"
        )));
    };

    Ok(plaintext_key.to_owned())
}

async fn proxy_messages(app: &App, api_key: &str) -> TestResult<Response<Body>> {
    Ok(app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("x-api-key", api_key)
                .header("anthropic-version", "2023-06-01")
                .header("content-type", "application/json")
                .body(Body::from(Bytes::from_static(
                    b"{\"model\":\"claude-3-5-sonnet-20241022\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],\"max_tokens\":1}",
                )))?,
        )
        .await?)
}

fn assert_retry_after(response: &Response<Body>) -> TestResult<()> {
    let retry_after = response
        .headers()
        .get(RETRY_AFTER)
        .ok_or_else(|| error("missing Retry-After header"))?
        .to_str()?;
    assert!(
        retry_after == "1" || retry_after == "2",
        "Retry-After header was {retry_after:?}, expected \"1\" or \"2\""
    );
    Ok(())
}

fn error(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(message.into()))
}
