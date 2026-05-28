#![cfg(feature = "postgres")]

use std::error::Error;
use std::io;
use std::io::Write as _;
use std::net::SocketAddr;
use std::process::Command;
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
const HEALTH_TIMEOUT: Duration = Duration::from_secs(30);
const HEALTH_POLL_INTERVAL: Duration = Duration::from_millis(500);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

static CHAOS_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
#[ignore]
async fn db_unreachable_returns_503_then_recovers() -> TestResult<()> {
    let Some(url) = ci_postgres_url() else {
        log_marker("skip reason=CI_POSTGRES_URL_unset");
        return Ok(());
    };
    let _serial = chaos_lock().lock().await;

    let initial_health = match postgres_health() {
        Ok(status) if status == "healthy" => status,
        Ok(status) => {
            log_marker(format!(
                "skip reason=postgres_not_healthy initial_health={status}"
            ));
            return Ok(());
        }
        Err(source) => {
            log_marker(format!("skip reason=docker_inspect_failed error={source}"));
            return Ok(());
        }
    };
    log_marker(format!("initial_health={initial_health}"));

    let (app, _upstream) = build_api_key_app_for_testing_postgres(&url).await?;
    let api_key = issue_key(&app).await?;

    let before = proxy_messages(&app, &api_key).await?;
    let before_status = before.status();
    let _before_body = before.into_body().collect().await?.to_bytes();
    log_marker(format!("before_status={before_status}"));
    assert_eq!(before_status, StatusCode::OK);

    let mut restart_guard = PostgresRestartGuard::armed();
    stop_postgres()?;
    let started = Instant::now();
    let unavailable = tokio::time::timeout(REQUEST_TIMEOUT, proxy_messages(&app, &api_key))
        .await
        .map_err(|_| error("proxy request timed out while postgres was stopped"))??;
    let elapsed = started.elapsed();
    let unavailable_status = unavailable.status();
    let retry_after = retry_after_value(&unavailable)?;
    let _unavailable_body = unavailable.into_body().collect().await?.to_bytes();
    log_marker(format!(
        "unavailable_status={unavailable_status} retry_after={retry_after} latency_ms={}",
        elapsed.as_millis()
    ));

    assert_eq!(unavailable_status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(retry_after, "1");
    assert!(
        elapsed <= FAILURE_LATENCY_CEILING,
        "DB-down auth latency {elapsed:?} exceeded retry budget ceiling {FAILURE_LATENCY_CEILING:?}"
    );

    let restarted_health = restart_guard.restart_and_disarm()?;
    log_marker(format!("restarted_health={restarted_health}"));

    let after = proxy_messages(&app, &api_key).await?;
    let after_status = after.status();
    let _after_body = after.into_body().collect().await?.to_bytes();
    log_marker(format!("recovery_status={after_status}"));
    assert_eq!(after_status, StatusCode::OK);

    let final_health = wait_postgres_healthy_blocking(HEALTH_TIMEOUT)?;
    log_marker(format!("final_health={final_health}"));
    assert_eq!(final_health, "healthy");

    Ok(())
}

fn ci_postgres_url() -> Option<String> {
    std::env::var("CI_POSTGRES_URL").ok()
}

fn chaos_lock() -> &'static tokio::sync::Mutex<()> {
    CHAOS_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

struct PostgresRestartGuard {
    armed: bool,
}

impl PostgresRestartGuard {
    fn armed() -> Self {
        Self { armed: true }
    }

    fn restart_and_disarm(&mut self) -> TestResult<String> {
        start_postgres()?;
        let health = wait_postgres_healthy_blocking(HEALTH_TIMEOUT)?;
        self.armed = false;
        Ok(health)
    }
}

impl Drop for PostgresRestartGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = start_postgres();
            let _ = wait_postgres_healthy_blocking(HEALTH_TIMEOUT);
        }
    }
}

fn stop_postgres() -> TestResult<()> {
    docker_output(&["stop", POSTGRES_CONTAINER]).map(|_| ())
}

fn start_postgres() -> TestResult<()> {
    docker_output(&["start", POSTGRES_CONTAINER]).map(|_| ())
}

fn postgres_health() -> TestResult<String> {
    docker_output(&[
        "inspect",
        "-f",
        "{{.State.Health.Status}}",
        POSTGRES_CONTAINER,
    ])
}

fn wait_postgres_healthy_blocking(timeout: Duration) -> TestResult<String> {
    let started = Instant::now();
    let mut last_status = String::from("<not checked>");

    loop {
        if started.elapsed() >= timeout {
            return Err(error(format!(
                "postgres did not become healthy within {timeout:?}: last_status={last_status}"
            )));
        }

        match postgres_health() {
            Ok(status) if status == "healthy" => return Ok(status),
            Ok(status) => last_status = status,
            Err(source) => last_status = source.to_string(),
        }

        std::thread::sleep(HEALTH_POLL_INTERVAL);
    }
}

fn docker_output(args: &[&str]) -> TestResult<String> {
    let output = Command::new("docker")
        .env("DOCKER_HOST", DOCKER_HOST)
        .args(args)
        .output()?;

    if output.status.success() {
        return Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned());
    }

    Err(error(format!(
        "docker {} failed: status={:?} stdout={} stderr={}",
        args.join(" "),
        output.status.code(),
        String::from_utf8_lossy(&output.stdout).trim(),
        String::from_utf8_lossy(&output.stderr).trim()
    )))
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
    config.validate()?;
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
            r#"{"ok":true}"#,
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
                    br#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#,
                )))?,
        )
        .await?)
}

fn retry_after_value(response: &Response<Body>) -> TestResult<String> {
    Ok(response
        .headers()
        .get(RETRY_AFTER)
        .ok_or_else(|| error("missing Retry-After header"))?
        .to_str()?
        .to_owned())
}

fn log_marker(message: impl AsRef<str>) {
    let mut stderr = io::stderr().lock();
    let _ = writeln!(stderr, "task17: {}", message.as_ref());
}

fn error(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(message.into()))
}
