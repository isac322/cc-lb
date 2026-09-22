use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use cc_lb_aead::AeadService;
use cc_lb_config::Config;
use cc_lb_engine::api_keys::key_store::{CreateParams, KeyStore};
use cc_lb_server::app::build_app_with_storage;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    BackendKind, ManagedKeyStore, MetaStore, PrincipalCreate, PrincipalKind, PrincipalStore,
    RateLimitKind, Storage as StorageTrait, UpstreamCreate, UpstreamRateLimitObservationRecord,
    UpstreamRateLimitStateStore, UpstreamStore,
};
use cc_lb_storage_sqlite::{SqliteStorage, open_sqlite};
use fake_anthropic::{AppConfig, app as fake_anthropic_app};
use http_body_util::BodyExt;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tower::ServiceExt;
use url::Url;
use uuid::Uuid;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn upstream_rate_limit_observations_are_persisted_end_to_end() -> TestResult<()> {
    let clock: cc_lb_engine::ClockHandle = Arc::new(cc_lb_engine::SystemClock);
    let upstream_server = spawn_upstream().await?;
    let dir = tempfile::tempdir()?;
    let aead = Arc::new(AeadService::from_master_key([0; 32]));
    let storage_path = dir.path().join("storage.sqlite");
    let storage_arc = sqlite_storage(&storage_path).await?;
    let upstream = UpstreamStore::create(
        storage_arc.as_ref(),
        UpstreamCreate {
            name: "fake-anthropic".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            base_url: Some(Url::parse(&format!("http://{}", upstream_server.addr))?),
            api_key_ciphertext: None,
            oauth_token_generation: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
        },
    )
    .await?;
    let ciphertext = aead.encrypt(b"sk-ant-fixture-secret", upstream.id.as_bytes())?;
    let upstream =
        UpstreamStore::update_api_key_secret(storage_arc.as_ref(), upstream.id, Some(ciphertext))
            .await?;
    PrincipalStore::create(
        storage_arc.as_ref(),
        PrincipalCreate {
            name: "api-key".to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models: Vec::new(),
            allowed_upstreams: Vec::new(),
            default_limits: Vec::new(),
            cache_keepalive: None,
        },
        1,
    )
    .await?;
    let key_store = KeyStore::new(storage_arc.clone());
    let (_key_record, key_secret) = key_store
        .create(
            "api-key",
            CreateParams {
                label: "rate-limit-persistence".to_owned(),
                description: None,
                expires_at_unix_secs: None,
                limit_overrides: Vec::new(),
            },
        )
        .await?;

    let managed_store: Arc<dyn ManagedKeyStore> = storage_arc.clone();
    let storage: Arc<dyn StorageTrait> = storage_arc.clone();
    let mut config = Config {
        storage: cc_lb_config::StorageConfig::Sqlite { path: storage_path },
        ..Default::default()
    };
    config.runtime.data_dir = Some(dir.path().to_path_buf());
    config.aead.key_env = "__CC_LB_TEST_KEY__".to_owned();
    let app = build_app_with_storage(
        config,
        None,
        managed_store,
        storage.clone(),
        aead.clone(),
        clock.clone(),
    )
    .await?;

    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("x-api-key", key_secret.expose())
                .header("anthropic-version", "2023-06-01")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}],"max_tokens":1}"#,
                ))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await?.to_bytes();
    assert!(String::from_utf8_lossy(&body).contains(r#""type":"message""#));

    let records = wait_for_rate_limit_records(storage_arc.as_ref(), upstream.id).await?;
    assert!(
        records.iter().any(|record| {
            record.upstream_id == upstream.id
                && record.kind == RateLimitKind::Requests
                && record.window == "default"
                && record.remaining == Some(999)
        }),
        "missing persisted requests rate-limit observation: {records:?}"
    );
    assert!(
        records.iter().any(|record| {
            record.upstream_id == upstream.id
                && record.kind == RateLimitKind::Tokens
                && record.window == "default"
                && record.remaining == Some(999_000)
        }),
        "missing persisted tokens rate-limit observation: {records:?}"
    );

    Ok(())
}

async fn sqlite_storage(path: &std::path::Path) -> TestResult<Arc<SqliteStorage>> {
    let database_url = format!("sqlite://{}", path.display());
    let storage = open_sqlite(&database_url, Arc::new(cc_lb_engine::SystemClock)).await?;
    storage.initialize(BackendKind::Sqlite).await?;
    Ok(Arc::new(storage))
}

struct RunningUpstream {
    addr: std::net::SocketAddr,
    task: JoinHandle<std::io::Result<()>>,
}

impl Drop for RunningUpstream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn spawn_upstream() -> TestResult<RunningUpstream> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let task = tokio::spawn(async move {
        axum::serve(listener, fake_anthropic_app(AppConfig::default())).await
    });
    Ok(RunningUpstream { addr, task })
}

async fn wait_for_rate_limit_records(
    storage: &dyn UpstreamRateLimitStateStore,
    upstream_id: Uuid,
) -> TestResult<Vec<UpstreamRateLimitObservationRecord>> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let records = storage.list_for_upstream_ids(&[upstream_id]).await?;
        if records.len() >= 2 {
            return Ok(records);
        }
        if Instant::now() >= deadline {
            return Ok(records);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
