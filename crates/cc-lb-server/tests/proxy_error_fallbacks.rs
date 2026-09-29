use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use cc_lb_aead::AeadService;
use cc_lb_config::Config;
use cc_lb_server::app::build_app_with_storage;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    ApiKeyMutation, IssueParams, ManagedKeyStore, MetaStore, PrincipalCreate, PrincipalKind,
    PrincipalStore, RequestEventStore, Storage as StorageTrait, StorageError, StorageResult,
    StoredApiKeyRecord, UpstreamCreate, UpstreamStore,
};
use cc_lb_storage_sqlite::{SqliteStorage, open_sqlite};
use http_body_util::BodyExt;
use serde_json::Value;
use tokio::time::{Instant, sleep};
use tower::ServiceExt;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn readyz_uses_declared_runtime_readiness_without_proxy_traffic() -> TestResult<()> {
    let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
    let dir = tempfile::tempdir()?;
    let storage_path = dir.path().join("storage.sqlite");
    let storage_arc = sqlite_storage(&storage_path).await?;
    let aead = Arc::new(AeadService::from_master_key([0; 32]));
    let upstream = UpstreamStore::create(
        storage_arc.as_ref(),
        UpstreamCreate {
            name: "declared-upstream".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            base_url: None,
            api_key_ciphertext: None,
            oauth_token_generation: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
        },
    )
    .await?;
    let ciphertext = aead.encrypt(b"sk-ant-fixture-secret", upstream.id.as_bytes())?;
    UpstreamStore::update_api_key_secret(storage_arc.as_ref(), upstream.id, Some(ciphertext))
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
    let app =
        build_app_with_storage(config, None, managed_store, storage, aead, clock.clone()).await?;

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
    let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
    let dir = tempfile::tempdir()?;
    let storage_path = dir.path().join("storage.sqlite");
    let storage_arc = sqlite_storage(&storage_path).await?;
    let aead = Arc::new(AeadService::from_master_key([0; 32]));
    let managed_store: Arc<dyn ManagedKeyStore> = storage_arc.clone();
    let storage: Arc<dyn StorageTrait> = storage_arc.clone();
    let mut config = Config {
        storage: cc_lb_config::StorageConfig::Sqlite {
            path: storage_path.clone(),
        },
        ..Default::default()
    };
    config.runtime.data_dir = Some(dir.path().to_path_buf());
    config.aead.key_env = "__CC_LB_TEST_KEY__".to_owned();
    let app = build_app_with_storage(config, None, managed_store, storage, aead, clock).await?;

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

    // #849: pre-auth router fallbacks are silent — they must persist zero
    // request_events rows. Give the async writer a window to (incorrectly)
    // persist before asserting the table stayed empty.
    sleep(Duration::from_millis(500)).await;
    let rows = stored_request_events(storage_arc.as_ref()).await?;
    assert!(
        rows.is_empty(),
        "pre-auth 404/405 fallbacks must not persist request events, found {} row(s)",
        rows.len()
    );

    Ok(())
}

/// Reads every persisted request event in append order through the cursor API.
async fn stored_request_events(
    storage: &SqliteStorage,
) -> StorageResult<Vec<cc_lb_storage_api::RequestEvent>> {
    let cursor = storage.current_request_event_cursor().await?;
    Ok(storage
        .query_request_events_between_cursors(
            0,
            cursor,
            500,
            &cc_lb_storage_api::RequestEventStreamFilters::default(),
        )
        .await?
        .into_iter()
        .map(|(_, event)| event)
        .collect())
}

/// ManagedKeyStore wrapper whose credential lookup always fails — used to
/// exercise the authn-storage-outage path end to end.
struct FailingManagedKeyStore {
    inner: Arc<SqliteStorage>,
}

#[async_trait::async_trait]
impl ManagedKeyStore for FailingManagedKeyStore {
    async fn issue(
        &self,
        principal_id: &str,
        key_id: &str,
        params: IssueParams,
    ) -> StorageResult<StoredApiKeyRecord> {
        self.inner.issue(principal_id, key_id, params).await
    }

    async fn get(
        &self,
        principal_id: &str,
        key_id: &str,
    ) -> StorageResult<Option<StoredApiKeyRecord>> {
        self.inner.get(principal_id, key_id).await
    }

    async fn lookup_by_index_hash(
        &self,
        _index_hash: &[u8; 32],
    ) -> StorageResult<Option<(String, String, StoredApiKeyRecord)>> {
        Err(StorageError::Unavailable {
            message: "injected key store outage".to_owned(),
        })
    }

    async fn list_by_principal(
        &self,
        principal_id: &str,
    ) -> StorageResult<Vec<StoredApiKeyRecord>> {
        self.inner.list_by_principal(principal_id).await
    }

    async fn list_all(&self) -> StorageResult<Vec<(String, String, StoredApiKeyRecord)>> {
        self.inner.list_all().await
    }

    async fn update(
        &self,
        principal_id: &str,
        key_id: &str,
        mutation: ApiKeyMutation,
    ) -> StorageResult<()> {
        ManagedKeyStore::update(self.inner.as_ref(), principal_id, key_id, mutation).await
    }

    async fn revoke_zero_secrets(&self, principal_id: &str, key_id: &str) -> StorageResult<()> {
        self.inner.revoke_zero_secrets(principal_id, key_id).await
    }
}

#[tokio::test]
async fn key_store_unavailable_persists_typed_authn_reason() -> TestResult<()> {
    let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
    let dir = tempfile::tempdir()?;
    let storage_path = dir.path().join("storage.sqlite");
    let storage_arc = sqlite_storage(&storage_path).await?;
    let aead = Arc::new(AeadService::from_master_key([0; 32]));
    let managed_store: Arc<dyn ManagedKeyStore> = Arc::new(FailingManagedKeyStore {
        inner: storage_arc.clone(),
    });
    let storage: Arc<dyn StorageTrait> = storage_arc.clone();
    let mut config = Config {
        storage: cc_lb_config::StorageConfig::Sqlite {
            path: storage_path.clone(),
        },
        ..Default::default()
    };
    config.runtime.data_dir = Some(dir.path().to_path_buf());
    config.aead.key_env = "__CC_LB_TEST_KEY__".to_owned();
    let app = build_app_with_storage(config, None, managed_store, storage, aead, clock).await?;

    // A well-formed credential whose lookup fails: the caller sees 503 and
    // the persisted row must carry the typed authn/unavailable diagnostic.
    let plaintext = cc_lb_control::api_keys::secret::generate_new().plaintext;
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .header("x-api-key", plaintext.expose())
                .body(Body::from(r#"{"model":"m","messages":[],"max_tokens":1}"#))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response
            .headers()
            .get(http::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok()),
        Some("1")
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    let row = loop {
        let rows = stored_request_events(storage_arc.as_ref()).await?;
        if let Some(row) = rows.into_iter().next() {
            break row;
        }
        if Instant::now() >= deadline {
            return Err("no request_event row persisted within 5s".into());
        }
        sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(row.status, 503);
    assert_eq!(row.error_code.as_deref(), Some("authentication_failed"));
    assert_eq!(
        serde_json::to_value(&row.internal_errors)?,
        serde_json::json!([{
            "stage": "authn",
            "kind": "unavailable",
            "message": "api key storage unavailable"
        }])
    );
    assert!(row.upstream_error_type.is_none());
    assert!(row.upstream_error_message.is_none());

    Ok(())
}

async fn sqlite_storage(path: &std::path::Path) -> TestResult<Arc<SqliteStorage>> {
    let database_url = format!("sqlite://{}", path.display());
    let storage = open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock)).await?;
    storage.initialize().await?;
    Ok(Arc::new(storage))
}
