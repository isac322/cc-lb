#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use axum::body::{Body, to_bytes};
use axum::extract::Request;
use axum::http::{StatusCode, header};
use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_config::{AdminAuthProviderConfig, Config, StorageConfig};
use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
use cc_lb_server::App;
use cc_lb_server::app::build_app_with_storage;
use cc_lb_storage_api::{
    AccountOwnerBinding, AccountOwnerStore, ManagedKeyStore, MetaStore, OAuthTokensCreate,
    PrincipalCreate, PrincipalKind, PrincipalStore, UpstreamCreate, UpstreamKind, UpstreamStore,
    principal::Limit,
};
use serde_json::{Value, json};
use tempfile::TempDir;
use tower::ServiceExt;
use url::Url;
use uuid::Uuid;

pub const PROVIDER_TOKEN: &str = "provider-oauth-access";
pub const BUNDLE_ACCOUNT: &str = "oauth-bundle-account";
pub const ALTERNATE_PROVIDER_TOKEN: &str = "alternate-provider-oauth-access";
pub const ALTERNATE_BUNDLE_ACCOUNT: &str = "alternate-oauth-bundle-account";
pub const LOCAL_WORKSPACE: &str = "local-session-workspace";
pub const VENDOR_WORKSPACE: &str = "selected-vendor-workspace";

pub struct OAuthSessionApp {
    pub app: App,
    pub storage: Arc<cc_lb_storage_sqlite::SqliteStorage>,
    pub dir: TempDir,
    pub access: String,
    pub renewal: String,
    pub parent: String,
    pub principal_id: String,
    pub parent_key_id: String,
    pub owner_binding_id: String,
    pub selected_upstream_id: Uuid,
    pub alternate_upstream_id: Uuid,
}

pub async fn build_oauth_session_app(
    selected_url: Url,
    alternate_url: Url,
    principal_limits: Vec<Limit>,
) -> OAuthSessionApp {
    let dir = tempfile::tempdir().expect("fixture directory");
    let path: PathBuf = dir.path().join("codex-control.sqlite");
    let storage = Arc::new(
        cc_lb_storage_sqlite::open_sqlite(
            &format!("sqlite://{}", path.display()),
            Arc::new(cc_lb_clock::SystemClock),
        )
        .await
        .expect("open SQLite"),
    );
    storage.initialize().await.expect("initialize SQLite");
    let aead = Arc::new(AeadService::from_master_key([0; 32]));

    let upstreams = [
        ("selected-owner", selected_url),
        ("alternate-owner", alternate_url),
    ];
    let mut upstream_ids = Vec::with_capacity(upstreams.len());
    for (name, base_url) in upstreams {
        let id = Uuid::new_v4();
        let (access_token, account_id) = if name == "selected-owner" {
            (PROVIDER_TOKEN, BUNDLE_ACCOUNT)
        } else {
            (ALTERNATE_PROVIDER_TOKEN, ALTERNATE_BUNDLE_ACCOUNT)
        };
        let tokens = OAuthTokenBundle {
            oauth_profile: Default::default(),
            access_token: access_token.to_owned(),
            refresh_token: String::new(),
            expires_at_unix_secs: 4_000_000_000,
            refresh_token_expires_at_unix_secs: None,
            account_id: Some(account_id.to_owned()),
            project_id: None,
            scopes: Vec::new(),
            never_refresh: true,
        };
        let encrypted = EncryptedOAuthTokens::encrypt(aead.as_ref(), &tokens, id.as_bytes())
            .expect("encrypt OAuth tokens");
        UpstreamStore::create(
            storage.as_ref(),
            UpstreamCreate {
                id,
                name: name.to_owned(),
                kind: UpstreamKind::OpenaiOauth,
                base_url: Some(base_url),
                api_key_ciphertext: None,
                oauth_tokens: Some(OAuthTokensCreate {
                    tokens: encrypted,
                    never_refresh: true,
                }),
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
            },
        )
        .await
        .expect("seed OAuth upstream");
        upstream_ids.push(id);
    }

    let now = cc_lb_clock::unix_secs(SystemTime::now());
    let principal = PrincipalStore::create(
        storage.as_ref(),
        PrincipalCreate {
            name: "codex-control-session-parent".to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models: Vec::new(),
            allowed_upstreams: upstream_ids.clone(),
            default_limits: principal_limits,
            cache_keepalive: None,
        },
        now,
    )
    .await
    .expect("seed principal");
    let managed: Arc<dyn ManagedKeyStore> = storage.clone();
    let keys = KeyStore::new(managed.clone());
    let (_, parent) = keys
        .create(
            &principal.id.to_string(),
            CreateParams {
                label: "codex control parent".to_owned(),
                description: None,
                expires_at_unix_secs: None,
                limit_overrides: Vec::new(),
            },
        )
        .await
        .expect("issue parent key");
    let parent_key_id = keys
        .list_all()
        .await
        .expect("list parent key")
        .into_iter()
        .find(|(principal_id, _, _)| principal_id == &principal.id.to_string())
        .map(|(_, key_id, _)| key_id)
        .expect("parent key identity");

    let owner_binding_id = "codex-control-selected-owner".to_owned();
    AccountOwnerStore::create_owner_binding(
        storage.as_ref(),
        AccountOwnerBinding {
            binding_id: owner_binding_id.clone(),
            principal_id: principal.id.to_string(),
            provider: "openai".to_owned(),
            owner_id: "selected-provider-owner".to_owned(),
            upstream_id: upstream_ids[0],
            owner_generation: 1,
            local_user_id: "apps-local-user".to_owned(),
            allowed_workspaces: vec![LOCAL_WORKSPACE.to_owned()],
            provider_workspace_id: VENDOR_WORKSPACE.to_owned(),
            outbound_region: None,
            revision: 1,
            revoked_at_unix_secs: None,
        },
    )
    .await
    .expect("bind selected owner");

    let mut config = Config {
        storage: StorageConfig::Sqlite { path },
        ..Config::default()
    };
    config.runtime.data_dir = Some(dir.path().to_owned());
    config.aead.key_env = "__CC_LB_CODEX_CONTROL_TEST_KEY__".to_owned();
    config.admin.auth.providers = vec![AdminAuthProviderConfig::StaticToken {
        id: "test".to_owned(),
        token_env: crate::common::TEST_NONEMPTY_ENV.to_owned(),
    }];
    let app = build_app_with_storage(
        config,
        None,
        managed,
        storage.clone(),
        aead,
        Arc::new(cc_lb_clock::SystemClock),
    )
    .await
    .expect("build real App");

    let request = Request::builder()
        .method("POST")
        .uri(format!(
            "/admin/v1/principals/{}/codex-sessions",
            principal.id
        ))
        .header(
            header::AUTHORIZATION,
            format!("Bearer {}", env!("CARGO_PKG_NAME")),
        )
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({
                "parent_key_id": parent_key_id,
                "owner_binding_id": owner_binding_id,
                "workspace_id": LOCAL_WORKSPACE,
                "origin": "https://apps.example.test",
                "access_ttl_seconds": 300,
                "session_ttl_seconds": 600,
            })
            .to_string(),
        ))
        .expect("issuance request");
    let response = app
        .admin_router
        .clone()
        .oneshot(request)
        .await
        .expect("issue session");
    assert_eq!(
        response.status(),
        StatusCode::CREATED,
        "session bootstrap must be mounted"
    );
    let grant: Value = serde_json::from_slice(
        &to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("grant bytes"),
    )
    .expect("grant JSON");

    OAuthSessionApp {
        app,
        storage,
        dir,
        access: grant["access_token"]
            .as_str()
            .expect("access token")
            .to_owned(),
        renewal: grant["renewal_token"]
            .as_str()
            .expect("renewal token")
            .to_owned(),
        parent: parent.expose().to_owned(),
        principal_id: principal.id.to_string(),
        parent_key_id,
        owner_binding_id,
        selected_upstream_id: upstream_ids[0],
        alternate_upstream_id: upstream_ids[1],
    }
}
