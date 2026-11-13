//! This module deliberately uses only APIs available before the retry persistence fix.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::{Router, http::StatusCode, routing::get};
use cc_lb_config::{Config, StorageConfig};
use cc_lb_scheduler::jobs::price_catalog::PriceCatalogRefreshJob;
use cc_lb_scheduler::worker::{CronJob, SchedulerBackend, SchedulerPushTask};
use cc_lb_server::scheduler_factory::open_scheduler_storage;
use sqlx::Row;

const DEADLINE: Duration = Duration::from_secs(30);
const HEALTHY_CATALOG: &str = r#"{"claude-3-5-sonnet-20241022":{"input_cost_per_token":0.000003,"output_cost_per_token":0.000015,"mode":"chat","max_tokens":8192}}"#;

struct CatalogEndpoint {
    url: String,
    requests: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl CatalogEndpoint {
    async fn start(status: StatusCode) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind catalog endpoint");
        let url = format!(
            "http://{}/catalog.json",
            listener.local_addr().expect("catalog address")
        );
        let requests = Arc::new(AtomicUsize::new(0));
        let counter = requests.clone();
        let router = Router::new().route(
            "/catalog.json",
            get(move || {
                let counter = counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    (status, HEALTHY_CATALOG)
                }
            }),
        );
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .expect("serve catalog endpoint");
        });
        Self {
            url,
            requests,
            task,
        }
    }
}

impl Drop for CatalogEndpoint {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct ServerProcess(std::process::Child);

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start_server(config: &Config, directory: &std::path::Path) -> ServerProcess {
    let config_path = directory.join("cc-lb.toml");
    let storage = match &config.storage {
        StorageConfig::Sqlite { path } => format!("kind = \"sqlite\"\npath = {path:?}"),
        StorageConfig::Postgres { url, .. } => format!("kind = \"postgres\"\nurl = {url:?}"),
    };
    let mut text = format!(
        r#"[listener]
proxy_addr = "127.0.0.1:0"
admin_addr = "127.0.0.1:0"
metrics_addr = "127.0.0.1:0"
[runtime]
data_dir = {directory:?}
[storage]
{storage}
[price_catalog]
url = {url:?}
cache_path = {cache_path:?}
[aead]
key_env = "CC_LB_MASTER_KEY"
[cluster]
instance_url = "http://127.0.0.1:0"
token_env = "CC_LB_RETRY_TEST_TOKEN"
[[admin.auth.providers]]
kind = "static_token"
id = "test"
token_env = "CC_LB_RETRY_TEST_TOKEN"
"#,
        url = config.price_catalog.url,
        cache_path = config.price_catalog.cache_path,
    );
    for name in config.scheduler.recurring_jobs.keys() {
        text.push_str(&format!(
            "\n[scheduler.recurring_jobs.{name}]\nenabled = false\n"
        ));
    }
    std::fs::write(&config_path, text).expect("write server config");
    ServerProcess(
        std::process::Command::new(env!("CARGO_BIN_EXE_cc-lb"))
            .arg("serve")
            .arg("--config")
            .arg(config_path)
            .env(
                "CC_LB_MASTER_KEY",
                "0000000000000000000000000000000000000000000000000000000000000000",
            )
            .env("CC_LB_RETRY_TEST_TOKEN", "catalog-retry-test")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .expect("start real server scheduler"),
    )
}

fn server_config(
    storage: StorageConfig,
    directory: &std::path::Path,
    endpoint: &CatalogEndpoint,
) -> Config {
    let mut config = Config {
        storage,
        ..Config::default()
    };
    config.listener.proxy_addr = "127.0.0.1:0".parse().expect("proxy address");
    config.listener.admin_addr = "127.0.0.1:0".parse().expect("admin address");
    config.listener.metrics_addr = "127.0.0.1:0".parse().expect("metrics address");
    config.runtime.data_dir = Some(directory.to_path_buf());
    config.price_catalog.url = endpoint.url.clone();
    config.price_catalog.cache_path = directory.join("catalog.json");
    config.admin.auth.providers = vec![cc_lb_config::AdminAuthProviderConfig::StaticToken {
        id: "test".to_owned(),
        token_env: "CC_LB_RETRY_TEST_TOKEN".to_owned(),
    }];
    config.cluster.instance_url = Some("http://127.0.0.1:0".to_owned());
    config.cluster.token_env = "CC_LB_RETRY_TEST_TOKEN".to_owned();
    for job in config.scheduler.recurring_jobs.values_mut() {
        job.enabled = false;
    }
    config
}

async fn enqueue_catalog(backend: &SchedulerBackend, key: &str) {
    backend
        .push_cron_task(SchedulerPushTask {
            args: CronJob::PriceCatalogRefresh(PriceCatalogRefreshJob::default()),
            idempotency_key: Some(key.to_owned()),
            run_at_unix_secs: Some(0),
            max_attempts: None,
        })
        .await
        .expect("enqueue price catalog refresh with retry policy defaults");
}

#[derive(Debug)]
struct JobRow {
    status: String,
    attempts: i64,
    run_at: i64,
    lock_by: Option<String>,
    done_at: Option<i64>,
    last_result: Option<String>,
}

#[cfg(feature = "sqlite")]
async fn sqlite_clock(pool: &sqlx::SqlitePool) -> i64 {
    sqlx::query_scalar("SELECT CAST(strftime('%s', 'now') AS INTEGER)")
        .fetch_one(pool)
        .await
        .expect("read SQLite clock")
}

#[cfg(feature = "sqlite")]
async fn sqlite_settled_attempt(pool: &sqlx::SqlitePool, key: &str, attempt: i64) -> JobRow {
    tokio::time::timeout(DEADLINE, async {
        loop {
            let row = sqlx::query("SELECT status, attempts, run_at, lock_by, done_at, last_result FROM Jobs WHERE idempotency_key = ?")
                .bind(key).fetch_one(pool).await.expect("read catalog job");
            let state = JobRow {
                status: row.get("status"), attempts: row.get("attempts"),
                run_at: row.get("run_at"), lock_by: row.get("lock_by"),
                done_at: row.get("done_at"), last_result: row.get("last_result"),
            };
            if state.attempts >= attempt
                && !matches!(state.status.as_str(), "Queued" | "Running")
                && (attempt == 1 || state.status != "Pending") {
                return state;
            }
            tokio::task::yield_now().await;
        }
    }).await.expect("catalog worker settles within deadline")
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_price_catalog_503_retries_once_at_30s_then_kills() {
    let directory = tempfile::tempdir().expect("isolated server directory");
    let endpoint = CatalogEndpoint::start(StatusCode::SERVICE_UNAVAILABLE).await;
    let config = server_config(
        StorageConfig::Sqlite {
            path: directory.path().join("app.sqlite"),
        },
        directory.path(),
        &endpoint,
    );
    let opened = open_scheduler_storage(
        &config.storage,
        &config.scheduler,
        Arc::new(cc_lb_clock::SystemClock),
    )
    .await
    .expect("open SQLite scheduler");
    let pool = match &opened.backend {
        SchedulerBackend::Sqlite(sqlite) => sqlite.pool(),
        #[cfg(feature = "postgres")]
        SchedulerBackend::Postgres(_) => panic!("expected SQLite"),
    };
    let _server = start_server(&config, directory.path());
    let before = sqlite_clock(pool).await;
    enqueue_catalog(&opened.backend, "catalog-503").await;
    let first = sqlite_settled_attempt(pool, "catalog-503", 1).await;
    let after = sqlite_clock(pool).await;
    assert_eq!(
        first.status, "Pending",
        "first HTTP 503 must reschedule rather than acknowledge DeadLetter"
    );
    assert_eq!(first.attempts, 1);
    assert!(first.lock_by.is_none());
    assert!(first.done_at.is_none());
    assert_eq!(endpoint.requests.load(Ordering::SeqCst), 1);
    assert!(
        before + 30 <= first.run_at && first.run_at <= after + 30,
        "DB-clock retry bracket: {before} <= {:?} <= {after}",
        first
    );
    sqlx::query("UPDATE Jobs SET run_at = 0 WHERE idempotency_key = ?")
        .bind("catalog-503")
        .execute(pool)
        .await
        .expect("fast-forward retry");
    let final_row = sqlite_settled_attempt(pool, "catalog-503", 2).await;
    assert_eq!(final_row.status, "Killed");
    assert_eq!(final_row.attempts, 2);
    assert_eq!(endpoint.requests.load(Ordering::SeqCst), 2);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(
            final_row.last_result.as_deref().expect("terminal result")
        )
        .expect("result JSON"),
        serde_json::json!({"Ok":"DeadLetter"})
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_price_catalog_200_healthy_control_completes_done() {
    let directory = tempfile::tempdir().expect("isolated server directory");
    let endpoint = CatalogEndpoint::start(StatusCode::OK).await;
    let config = server_config(
        StorageConfig::Sqlite {
            path: directory.path().join("app.sqlite"),
        },
        directory.path(),
        &endpoint,
    );
    let opened = open_scheduler_storage(
        &config.storage,
        &config.scheduler,
        Arc::new(cc_lb_clock::SystemClock),
    )
    .await
    .expect("open SQLite scheduler");
    let pool = match &opened.backend {
        SchedulerBackend::Sqlite(sqlite) => sqlite.pool(),
        #[cfg(feature = "postgres")]
        SchedulerBackend::Postgres(_) => panic!("expected SQLite"),
    };
    let _server = start_server(&config, directory.path());
    enqueue_catalog(&opened.backend, "catalog-healthy").await;
    let row = sqlite_settled_attempt(pool, "catalog-healthy", 1).await;
    assert_eq!(row.status, "Done");
    assert_eq!(row.attempts, 1);
    assert_eq!(endpoint.requests.load(Ordering::SeqCst), 1);
    let fingerprint: String = sqlx::query_scalar(
        "SELECT fingerprint FROM price_catalog_versions WHERE source = 'litellm'",
    )
    .fetch_one(pool)
    .await
    .expect("catalog version persisted");
    assert!(!fingerprint.is_empty());
    assert!(
        config.price_catalog.cache_path.is_file(),
        "catalog snapshot persisted to disk"
    );
}

#[cfg(feature = "postgres")]
async fn postgres_clock(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()))::bigint")
        .fetch_one(pool)
        .await
        .expect("read Postgres clock")
}

#[cfg(feature = "postgres")]
async fn postgres_settled_attempt(pool: &sqlx::PgPool, key: &str, attempt: i64) -> JobRow {
    tokio::time::timeout(DEADLINE, async {
        loop {
            let row = sqlx::query("SELECT status, attempts::bigint AS attempts, floor(extract(epoch FROM run_at))::bigint AS run_at, lock_by, floor(extract(epoch FROM done_at))::bigint AS done_at, last_result::text AS last_result FROM apalis.jobs WHERE idempotency_key = $1")
                .bind(key).fetch_one(pool).await.expect("read catalog job");
            let state = JobRow {
                status: row.get("status"), attempts: row.get("attempts"),
                run_at: row.get("run_at"), lock_by: row.get("lock_by"),
                done_at: row.get("done_at"), last_result: row.get("last_result"),
            };
            if state.attempts >= attempt
                && !matches!(state.status.as_str(), "Queued" | "Running")
                && (attempt == 1 || state.status != "Pending") {
                return state;
            }
            tokio::task::yield_now().await;
        }
    }).await.expect("catalog worker settles within deadline")
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_price_catalog_503_retries_once_at_30s_then_kills() {
    let Some(admin_url) = std::env::var("CI_POSTGRES_URL")
        .or_else(|_| std::env::var("DATABASE_URL_TEST"))
        .or_else(|_| std::env::var("DATABASE_URL"))
        .ok()
    else {
        eprintln!("SKIP: postgres URL unset");
        return;
    };
    if !(admin_url.contains("localhost")
        || admin_url.contains("127.0.0.1")
        || admin_url.contains("cc_lb_test"))
    {
        eprintln!("SKIP: postgres URL is not local/test-like");
        return;
    }
    let name = format!("cc_lb_catalog_retry_{}", uuid::Uuid::new_v4().simple());
    let admin_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&admin_url)
        .await
        .expect("connect database administrator");
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE \"{name}\"")))
        .execute(&admin_pool)
        .await
        .expect("create isolated scheduler database");
    let mut url = url::Url::parse(&admin_url).expect("parse database URL");
    url.set_path(&format!("/{name}"));
    let outcome = futures_util::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(async {
        let directory = tempfile::tempdir().expect("isolated server directory");
        let endpoint = CatalogEndpoint::start(StatusCode::SERVICE_UNAVAILABLE).await;
        let config = server_config(
            StorageConfig::Postgres {
                url: url.to_string(),
                pool: cc_lb_config::PostgresPoolConfig::default(),
            },
            directory.path(),
            &endpoint,
        );
        let opened = open_scheduler_storage(
            &config.storage,
            &config.scheduler,
            Arc::new(cc_lb_clock::SystemClock),
        )
        .await
        .expect("open Postgres scheduler");
        #[allow(clippy::infallible_destructuring_match)]
        let pool = match &opened.backend {
            SchedulerBackend::Postgres(postgres) => postgres.pool(),
            #[cfg(feature = "sqlite")]
            SchedulerBackend::Sqlite(_) => panic!("expected Postgres"),
        };
        let _server = start_server(&config, directory.path());
        let before = postgres_clock(pool).await;
        enqueue_catalog(&opened.backend, "catalog-503").await;
        let first = postgres_settled_attempt(pool, "catalog-503", 1).await;
        let after = postgres_clock(pool).await;
        assert_eq!(first.status, "Pending");
        assert_eq!(first.attempts, 1);
        assert!(first.lock_by.is_none());
        assert!(first.done_at.is_none());
        assert_eq!(endpoint.requests.load(Ordering::SeqCst), 1);
        assert!(before + 30 <= first.run_at && first.run_at <= after + 30);
        sqlx::query("UPDATE apalis.jobs SET run_at = to_timestamp(0) WHERE idempotency_key = $1")
            .bind("catalog-503")
            .execute(pool)
            .await
            .expect("fast-forward retry");
        let final_row = postgres_settled_attempt(pool, "catalog-503", 2).await;
        assert_eq!(final_row.status, "Killed");
        assert_eq!(final_row.attempts, 2);
        assert_eq!(endpoint.requests.load(Ordering::SeqCst), 2);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                final_row.last_result.as_deref().expect("terminal result")
            )
            .expect("result JSON"),
            serde_json::json!({"Ok":"DeadLetter"})
        );
    }))
    .await;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP DATABASE \"{name}\" WITH (FORCE)"
    )))
    .execute(&admin_pool)
    .await
    .expect("drop isolated scheduler database");
    admin_pool.close().await;
    if let Err(payload) = outcome {
        std::panic::resume_unwind(payload);
    }
}
