use std::error::Error;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use cc_lb_aead::AeadService;
use cc_lb_config::{Config, DownstreamAuthMode, NoneModeConfig, NoneModeUpstreamKind};
use cc_lb_server::app::{build_app_for_testing, build_app_with_storage};
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    BackendKind, ManagedKeyStore, MetaStore, PrincipalCreate, PrincipalKind, PrincipalStore,
    Storage as StorageTrait, UpstreamCreate, UpstreamStore,
};
use cc_lb_storage_sqlite::{SqliteStorage, open_sqlite};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn readyz_uses_declared_runtime_readiness_without_proxy_traffic() -> TestResult<()> {
    let clock: cc_lb_engine::ClockHandle = Arc::new(cc_lb_engine::SystemClock);
    let dir = tempfile::tempdir()?;
    let storage_path = dir.path().join("storage.sqlite");
    let storage_arc = sqlite_storage(&storage_path).await?;
    UpstreamStore::create(
        storage_arc.as_ref(),
        UpstreamCreate {
            name: "declared-upstream".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            base_url: None,
            api_key_ciphertext: Some(vec![0; 32]),
            oauth_token_generation: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
        },
    )
    .await?;
    PrincipalStore::create(
        storage_arc.as_ref(),
        PrincipalCreate {
            name: "declared-principal".to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models: Vec::new(),
            allowed_upstreams: Vec::new(),
            default_limits: Vec::new(),
            cache_keepalive: None,
        },
        1,
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
    config.downstream_auth.mode = DownstreamAuthMode::None;
    config.downstream_auth.none_mode = Some(NoneModeConfig {
        principal_id: "declared-principal".to_owned(),
        upstream_kind: NoneModeUpstreamKind::AnthropicKey,
    });
    let app = build_app_with_storage(
        config,
        managed_store,
        storage,
        Arc::new(AeadService::from_master_key([0; 32])),
        clock.clone(),
    )
    .await?;

    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/readyz")
                .body(Body::empty())?,
        )
        .await?;
    let status = response.status();
    let body = response.into_body().collect().await?.to_bytes();
    let json: Value = serde_json::from_slice(&body)?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["ready"], true);
    Ok(())
}

#[tokio::test]
async fn proxy_fallbacks_return_anthropic_json_errors() -> TestResult<()> {
    let clock: cc_lb_engine::ClockHandle = Arc::new(cc_lb_engine::SystemClock);
    let app = build_app_for_testing(Config::default(), clock.clone()).await?;

    for (method, path, expected_status, expected_message) in [
        (
            "GET",
            "/not-a-proxy-route",
            StatusCode::NOT_FOUND,
            "requested proxy path was not found",
        ),
        (
            "POST",
            "/v1/models",
            StatusCode::METHOD_NOT_ALLOWED,
            "method is not allowed for this proxy path",
        ),
    ] {
        let response = app
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .body(Body::empty())?,
            )
            .await?;
        let status = response.status();
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        let body = response.into_body().collect().await?.to_bytes();
        let json: Value = serde_json::from_slice(&body)?;

        assert_eq!(status, expected_status);
        assert!(content_type.starts_with("application/json"));
        assert_eq!(json["type"], "error");
        assert_eq!(json["error"]["type"], "not_found");
        assert_eq!(json["error"]["message"], expected_message);
    }

    Ok(())
}

async fn sqlite_storage(path: &std::path::Path) -> TestResult<Arc<SqliteStorage>> {
    let database_url = format!("sqlite://{}", path.display());
    let storage = open_sqlite(&database_url, Arc::new(cc_lb_engine::SystemClock)).await?;
    storage.initialize(BackendKind::Sqlite).await?;
    Ok(Arc::new(storage))
}
