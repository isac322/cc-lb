use std::sync::Arc;

use cc_lb_aead::AeadService;
use cc_lb_aead::EncryptedOAuthTokens;
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_core::{ApplyStatus, DynamicView};
use cc_lb_runtime_extism::ExtismRuntime;
use cc_lb_server::dynamic_view_builder::{Stores, build_dynamic_view};
use cc_lb_storage_api::{
    BackendKind, MetaStore, PrincipalCreate, PrincipalKind, PrincipalStore, UpstreamCreate,
    UpstreamStore,
};

use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_sqlite::{SqliteStorage, open_sqlite};

fn oauth_config() -> AnthropicOAuthConfig {
    AnthropicOAuthConfig::default()
}

fn stores(storage: Arc<SqliteStorage>) -> Stores {
    Stores {
        upstreams: storage.clone(),
        principals: storage.clone(),
        plugin_registry: storage.clone(),
        upstream_rate_limits: storage.clone(),
        upstream_subscription_quotas: storage.clone(),
        prompt_cache_observations: storage.clone(),
        anthropic_compatibility_kv: storage,
        audit: None,
        plugin_registry_repo: None,
    }
}

async fn storage_fixture() -> (tempfile::TempDir, Arc<SqliteStorage>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!("sqlite://{}", dir.path().join("test.sqlite").display());
    let storage = open_sqlite(&database_url).await.expect("storage");
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("storage initialized");
    let storage = Arc::new(storage);
    (dir, storage)
}

async fn create_principal(
    storage: &SqliteStorage,
    name: &str,
) -> cc_lb_storage_api::PrincipalRecord {
    PrincipalStore::create(
        storage,
        PrincipalCreate {
            name: name.to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models: Vec::new(),
            allowed_upstreams: Vec::new(),
            default_limits: Vec::new(),
        },
        1,
    )
    .await
    .expect("principal created")
}

async fn create_api_key_upstream(
    storage: &SqliteStorage,
    name: &str,
) -> cc_lb_storage_api::UpstreamRecord {
    UpstreamStore::create(
        storage,
        UpstreamCreate {
            name: name.to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            base_url: None,
            api_key_ciphertext: Some(vec![1, 2, 3]),
            oauth_token_generation: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
        },
    )
    .await
    .expect("upstream created")
}

async fn build(
    stores: &Stores,
    current_generation: u64,
    runtime: &ExtismRuntime,
    data_dir: &std::path::Path,
) -> Arc<DynamicView> {
    build_dynamic_view(
        stores,
        &oauth_config(),
        Arc::new(AeadService::from_master_key([1; 32])),
        None,
        current_generation,
        runtime,
        data_dir,
        Arc::new(cc_lb_server::SubscriptionQuotaCache::new()),
        1800,
        &cc_lb_config::Config::default(),
    )
    .await
    .expect("dynamic view builds")
}

#[tokio::test]
async fn principals_delete_rebuild_removes_deleted_and_increments_generation() {
    let (dir, storage) = storage_fixture().await;
    let stores = stores(storage.clone());
    let runtime = ExtismRuntime::new();
    let principal_a = create_principal(&storage, "principal-a").await;
    create_principal(&storage, "principal-b").await;

    let view = build(&stores, 0, &runtime, dir.path()).await;
    assert_eq!(view.generation, 1);
    assert_eq!(
        view.principal_view.principal_status("principal-a"),
        cc_lb_core::api_keys::principal_view::PrincipalStatus::Active
    );
    assert_eq!(
        view.principal_view.principal_status("principal-b"),
        cc_lb_core::api_keys::principal_view::PrincipalStatus::Active
    );

    PrincipalStore::soft_delete(&*storage, principal_a.id, principal_a.revision, 2)
        .await
        .expect("soft delete");
    let view = build(&stores, view.generation, &runtime, dir.path()).await;

    assert_eq!(view.generation, 2);
    assert_eq!(
        view.principal_view.principal_status("principal-a"),
        cc_lb_core::api_keys::principal_view::PrincipalStatus::Missing
    );
    assert_eq!(
        view.principal_view.principal_status("principal-b"),
        cc_lb_core::api_keys::principal_view::PrincipalStatus::Active
    );
}

#[tokio::test]
async fn corrupt_oauth_upstream_is_error_while_other_upstreams_stay_active() {
    let (dir, storage) = storage_fixture().await;
    let stores = stores(storage.clone());
    let runtime = ExtismRuntime::new();
    create_principal(&storage, "principal-a").await;
    create_api_key_upstream(&storage, "healthy").await;
    let corrupt = UpstreamStore::create(
        &*storage,
        UpstreamCreate {
            name: "corrupt".to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: None,
            api_key_ciphertext: None,
            oauth_token_generation: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
        },
    )
    .await
    .expect("oauth upstream created");
    storage
        .store_oauth_tokens(
            corrupt.id,
            corrupt.revision,
            EncryptedOAuthTokens::from_ciphertext(vec![9]),
        )
        .await
        .expect("corrupt oauth stored");

    let view = build(&stores, 10, &runtime, dir.path()).await;
    assert_eq!(view.generation, 11);
    let mut upstream_names = view
        .upstreams_snapshot()
        .iter()
        .map(|record| record.name.as_str())
        .collect::<Vec<_>>();
    upstream_names.sort_unstable();
    assert_eq!(upstream_names, ["corrupt", "healthy"]);

    let healthy = view
        .upstream_status_snapshot
        .entries
        .get("healthy")
        .expect("healthy status");
    assert_eq!(healthy.status, ApplyStatus::Active);
    assert!(healthy.last_apply_error.is_none());

    let corrupt = view
        .upstream_status_snapshot
        .entries
        .get("corrupt")
        .expect("corrupt status");
    assert_eq!(corrupt.status, ApplyStatus::Error);
    assert!(
        corrupt
            .last_apply_error
            .as_ref()
            .is_some_and(|message| !message.is_empty())
    );

    let persisted = UpstreamStore::get_by_name(&*storage, "corrupt")
        .await
        .expect("load corrupt")
        .expect("corrupt exists");
    assert!(persisted.last_apply_error.is_some());
}
