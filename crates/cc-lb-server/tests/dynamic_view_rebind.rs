use std::collections::HashMap;
use std::sync::Arc;

use cc_lb_aead::AeadService;
use cc_lb_aead::{EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_control::{ApplyStatus, DynamicView};
use cc_lb_engine::{RequestKind, build_candidates};
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use cc_lb_server::dynamic_view_builder::{Stores, build_dynamic_view};
use cc_lb_storage_api::{
    MetaStore, OrganizationMetadataRecord, OrganizationMetadataStore, PlanTierStore,
    PrincipalCreate, PrincipalKind, PrincipalStore, TierResolutionSource, UpstreamCreate,
    UpstreamStatusUpdate, UpstreamStore, UpstreamSubscriptionMetadataRecord,
    UpstreamSubscriptionMetadataStore,
};

use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_sqlite::{SqliteStorage, open_sqlite};
use uuid::Uuid;

fn stores(storage: Arc<SqliteStorage>) -> Stores {
    Stores {
        upstreams: storage.clone(),
        principals: storage.clone(),
        plugin_registry: storage.clone(),
        upstream_rate_limits: storage.clone(),
        upstream_subscription_quotas: storage.clone(),
        upstream_subscription_metadata: storage.clone(),
        organization_metadata: storage.clone(),
        plan_tiers: storage.clone(),
        prompt_cache_observations: storage.clone(),
        anthropic_compatibility_kv: storage,
        audit: None,
    }
}

async fn storage_fixture() -> (tempfile::TempDir, Arc<SqliteStorage>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!("sqlite://{}", dir.path().join("test.sqlite").display());
    let storage = open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
        .await
        .expect("storage");
    storage.initialize().await.expect("storage initialized");
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
            cache_keepalive: None,
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
    let created = UpstreamStore::create(
        storage,
        UpstreamCreate {
            id: Uuid::new_v4(),
            name: name.to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            base_url: None,
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        },
    )
    .await
    .expect("upstream created");
    let ciphertext = AeadService::from_master_key([1; 32])
        .encrypt(b"sk-ant-fixture-secret", created.id.as_bytes())
        .expect("api-key ciphertext");
    UpstreamStore::update_api_key_secret(storage, created.id, Some(ciphertext))
        .await
        .expect("upstream api-key secret")
}

async fn attach_plan_metadata(storage: &SqliteStorage, upstream_id: uuid::Uuid) {
    UpstreamSubscriptionMetadataStore::put_upstream_subscription_metadata(
        storage,
        &UpstreamSubscriptionMetadataRecord {
            upstream_id,
            organization_uuid: Some("org-max-5x".to_owned()),
            organization_role: None,
            workspace_role: None,
            observed_at_unix_millis: 1_800_000_000_000,
            last_error: None,
            raw_roles: None,
            raw_bootstrap: None,
        },
    )
    .await
    .expect("subscription metadata stored");
    OrganizationMetadataStore::put_organization_metadata(
        storage,
        &OrganizationMetadataRecord {
            organization_uuid: "org-max-5x".to_owned(),
            organization_name: Some("Max 5x Org".to_owned()),
            organization_type: Some("claude_max".to_owned()),
            rate_limit_tier: Some("default_claude_max_5x".to_owned()),
            seat_tier: None,
            has_extra_usage_enabled: None,
            billing_type: None,
            subscription_created_at_unix_secs: None,
            account_email: None,
            account_display_name: None,
            account_uuid: None,
            overage_credit_amount_minor_units: None,
            overage_credit_currency: None,
            overage_credit_granted: None,
            overage_credit_eligible: None,
            observed_at_unix_millis: 1_800_000_000_000,
            last_error: None,
            raw_profile: None,
            raw_overage_grant: None,
        },
    )
    .await
    .expect("organization metadata stored");
}

async fn build(
    stores: &Stores,
    current_generation: u64,
    runtime: &Arc<WasmtimeRuntime>,
    data_dir: &std::path::Path,
) -> Arc<DynamicView> {
    let (gemini_config, antigravity_config) = catalog_configs();
    build_dynamic_view(
        stores,
        Some(&gemini_config),
        Some(&antigravity_config),
        Arc::new(AeadService::from_master_key([1; 32])),
        None,
        current_generation,
        runtime,
        data_dir,
        Arc::new(cc_lb_server::SubscriptionQuotaCache::new()),
        30,
        Arc::new(cc_lb_control::NoopPromptCacheObservationSink),
        1800,
        Arc::new(cc_lb_clock::SystemClock),
    )
    .await
    .expect("dynamic view builds")
}

pub(super) fn catalog_configs() -> (
    cc_lb_config::GeminiOAuthConfig,
    cc_lb_config::AntigravityOAuthConfig,
) {
    (
        cc_lb_config::GeminiOAuthConfig {
            code_assist_url: "https://cli-metadata.example.test/tenant/cli?tenant=cli#discard"
                .parse()
                .expect("CLI metadata URL"),
            ..Default::default()
        },
        cc_lb_config::AntigravityOAuthConfig {
            code_assist_url: "https://agy-metadata.example.test/tenant/agy?tenant=agy#discard"
                .parse()
                .expect("Antigravity metadata URL"),
            ..Default::default()
        },
    )
}

pub(super) async fn create_catalog_upstreams(
    storage: &SqliteStorage,
    aead: &AeadService,
) -> Vec<(cc_lb_storage_api::UpstreamRecord, &'static str)> {
    use cc_lb_aead::GeminiOAuthProfile::{Antigravity, GeminiCli};

    let mut upstreams = Vec::new();
    for (name, profile, base_url, expected_url) in [
        (
            "catalog-cli-config",
            GeminiCli,
            None,
            "https://cli-metadata.example.test/tenant/cli/v1internal:fetchAvailableModels?tenant=cli",
        ),
        (
            "catalog-agy-config",
            Antigravity,
            None,
            "https://agy-metadata.example.test/tenant/agy/v1internal:fetchAvailableModels?tenant=agy",
        ),
        (
            "catalog-cli-override",
            GeminiCli,
            Some("https://cloudcode-pa.googleapis.com/stored/cli?tenant=stored-cli#discard"),
            "https://cloudcode-pa.googleapis.com/stored/cli:fetchAvailableModels?tenant=stored-cli",
        ),
        (
            "catalog-agy-override",
            Antigravity,
            Some("https://stored-metadata.example.test/stored/agy?tenant=stored-agy#discard"),
            "https://stored-metadata.example.test/stored/agy:fetchAvailableModels?tenant=stored-agy",
        ),
    ] {
        let created = UpstreamStore::create(
            storage,
            UpstreamCreate {
                id: Uuid::new_v4(),
                name: name.to_owned(),
                kind: UpstreamKind::GeminiOauth,
                base_url: base_url.map(|url| url.parse().expect("stored metadata URL")),
                api_key_ciphertext: None,
                oauth_tokens: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
            },
        )
        .await
        .expect("catalog upstream created");
        let tokens = OAuthTokenBundle {
            access_token: "synthetic-catalog-access".to_owned(),
            refresh_token: String::new(),
            account_id: None,
            project_id: Some("synthetic-catalog-project".to_owned()),
            oauth_profile: profile,
            expires_at_unix_secs: u64::MAX,
            refresh_token_expires_at_unix_secs: None,
            scopes: Vec::new(),
            never_refresh: true,
        };
        let encrypted = EncryptedOAuthTokens::encrypt(aead, &tokens, created.id.as_bytes())
            .expect("encrypt synthetic catalog credential");
        let record = UpstreamStore::store_oauth_tokens(
            storage,
            created.id,
            created.revision,
            encrypted,
            false,
        )
        .await
        .expect("catalog credential stored");
        upstreams.push((record, expected_url));
    }
    upstreams
}

pub(super) async fn assert_catalog_urls(
    view: &DynamicView,
    upstreams: &[(cc_lb_storage_api::UpstreamRecord, &str)],
) {
    for (record, expected_url) in upstreams {
        let upstream = cc_lb_domain::Upstream::GeminiDirect {
            base_url: record.base_url.clone(),
        };
        let dialect = cc_lb_engine::lifecycle::native_route_dialect(record);
        let context = cc_lb_upstream::DialectShapeContext {
            request_id: "catalog-rebuild-request".to_owned(),
            downstream_headers: http::HeaderMap::new(),
            method: http::Method::GET,
            path: "/v1beta/models".to_owned(),
            query: None,
            body_bytes: bytes::Bytes::new(),
        };
        let principal = cc_lb_domain::Principal {
            id: "catalog-rebuild-principal".to_owned(),
            kind: cc_lb_domain::PrincipalKind::ApiKey,
        };
        let shaped =
            cc_lb_upstream::shape_request(dialect.as_ref(), &context, &upstream, &principal)
                .expect("shape native catalog request");
        let factory = view.signer_factory.with_router_choice(record.name.clone());
        let signer = factory
            .build(&upstream)
            .await
            .expect("build catalog signer from dynamic view");
        let signed = cc_lb_upstream::sign_request(signer.as_ref(), shaped)
            .await
            .expect("sign native catalog request");
        assert_eq!(signed.url().as_str(), *expected_url, "{}", record.name);
        assert_eq!(signed.method(), &http::Method::POST);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(signed.body()).expect("catalog JSON"),
            serde_json::json!({"project": "synthetic-catalog-project"}),
        );
    }
}

#[tokio::test]
async fn catalog_metadata_endpoints_survive_changed_revision_rebind() {
    let (dir, storage) = storage_fixture().await;
    let upstreams =
        create_catalog_upstreams(&storage, &AeadService::from_master_key([1; 32])).await;
    let stores = stores(storage.clone());
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let initial = build(&stores, 0, &runtime, dir.path()).await;
    assert_catalog_urls(&initial, &upstreams).await;

    create_principal(&storage, "catalog-rebind-revision").await;
    let rebuilt = build(&stores, initial.generation, &runtime, dir.path()).await;
    assert_eq!(rebuilt.generation, initial.generation + 1);
    assert_ne!(
        rebuilt.upstream_status_snapshot.revision_hash,
        initial.upstream_status_snapshot.revision_hash,
    );
    assert_catalog_urls(&rebuilt, &upstreams).await;
}

#[tokio::test]
async fn principals_delete_rebuild_removes_deleted_and_increments_generation() {
    let (dir, storage) = storage_fixture().await;
    let stores = stores(storage.clone());
    let runtime = std::sync::Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let principal_a = create_principal(&storage, "principal-a").await;
    create_principal(&storage, "principal-b").await;

    let view = build(&stores, 0, &runtime, dir.path()).await;
    assert_eq!(view.generation, 1);
    assert_eq!(
        view.principal_view.principal_status("principal-a"),
        cc_lb_control::api_keys::principal_view::PrincipalStatus::Active
    );
    assert_eq!(
        view.principal_view.principal_status("principal-b"),
        cc_lb_control::api_keys::principal_view::PrincipalStatus::Active
    );

    PrincipalStore::soft_delete(&*storage, principal_a.id, principal_a.revision, 2)
        .await
        .expect("soft delete");
    let view = build(&stores, view.generation, &runtime, dir.path()).await;

    assert_eq!(view.generation, 2);
    assert_eq!(
        view.principal_view.principal_status("principal-a"),
        cc_lb_control::api_keys::principal_view::PrincipalStatus::Missing
    );
    assert_eq!(
        view.principal_view.principal_status("principal-b"),
        cc_lb_control::api_keys::principal_view::PrincipalStatus::Active
    );
}

#[tokio::test]
async fn corrupt_oauth_upstream_is_error_while_other_upstreams_stay_active() {
    let (dir, storage) = storage_fixture().await;
    let stores = stores(storage.clone());
    let runtime = std::sync::Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    create_principal(&storage, "principal-a").await;
    create_api_key_upstream(&storage, "healthy").await;
    let corrupt = UpstreamStore::create(
        &*storage,
        UpstreamCreate {
            id: Uuid::new_v4(),
            name: "corrupt".to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: None,
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        },
    )
    .await
    .expect("oauth upstream created");
    storage
        .store_oauth_tokens(
            corrupt.id,
            corrupt.revision,
            EncryptedOAuthTokens::from_ciphertext(vec![9]),
            false,
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

#[tokio::test]
async fn oauth_reconnect_error_survives_active_and_disabled_rebuilds_until_token_replacement() {
    let (dir, storage) = storage_fixture().await;
    let stores = stores(storage.clone());
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let aead = AeadService::from_master_key([1; 32]);
    let bundle = OAuthTokenBundle {
        access_token: "access-before-reconnect".to_owned(),
        refresh_token: "refresh-before-reconnect".to_owned(),
        account_id: None,
        project_id: None,
        oauth_profile: Default::default(),
        expires_at_unix_secs: 1_900_000_000,
        refresh_token_expires_at_unix_secs: None,
        scopes: vec!["messages".to_owned()],
        never_refresh: false,
    };
    let mut upstreams = Vec::new();
    for reason in ["status_400", "status_401", "refresh_token_expired"] {
        let created = UpstreamStore::create(
            storage.as_ref(),
            UpstreamCreate {
                id: Uuid::new_v4(),
                name: format!("reconnect-{reason}"),
                kind: UpstreamKind::AnthropicOauth,
                base_url: None,
                api_key_ciphertext: None,
                oauth_tokens: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
            },
        )
        .await
        .expect("oauth upstream created");
        let encrypted = EncryptedOAuthTokens::encrypt(&aead, &bundle, created.id.as_bytes())
            .expect("encrypt oauth credential");
        let upstream = UpstreamStore::store_oauth_tokens(
            storage.as_ref(),
            created.id,
            created.revision,
            encrypted,
            false,
        )
        .await
        .expect("oauth credential stored");
        UpstreamStore::set_status(
            storage.as_ref(),
            upstream.id,
            UpstreamStatusUpdate {
                last_apply_error: Some(Some(reason.to_owned())),
                expected_oauth_token_generation: Some(upstream.oauth_token_generation),
                ..UpstreamStatusUpdate::default()
            },
        )
        .await
        .expect("terminal renewal error stored");
        upstreams.push((upstream, reason));
    }

    let initial = build(&stores, 0, &runtime, dir.path()).await;
    for (upstream, reason) in &upstreams {
        let status = initial
            .upstream_status_snapshot
            .entries
            .get(&upstream.name)
            .expect("initial oauth status");
        assert_eq!(status.status, ApplyStatus::Active);
        assert_eq!(status.last_apply_error.as_deref(), Some(*reason));
        UpstreamStore::set_enabled(storage.as_ref(), upstream.id, upstream.revision, false)
            .await
            .expect("disable routing without reconnecting");
    }
    let disabled = build(&stores, initial.generation, &runtime, dir.path()).await;
    let rebuilt = build(&stores, disabled.generation, &runtime, dir.path()).await;
    for (upstream, reason) in &upstreams {
        let status = rebuilt
            .upstream_status_snapshot
            .entries
            .get(&upstream.name)
            .expect("disabled oauth status");
        assert_eq!(status.status, ApplyStatus::Disabled);
        assert_eq!(status.last_apply_error.as_deref(), Some(*reason));
        let current = UpstreamStore::get_by_id(storage.as_ref(), upstream.id)
            .await
            .expect("load terminal state")
            .expect("upstream exists");
        assert_eq!(current.last_apply_error.as_deref(), Some(*reason));
        assert_eq!(
            current.oauth_token_generation,
            upstream.oauth_token_generation
        );
        let encrypted = EncryptedOAuthTokens::encrypt(&aead, &bundle, upstream.id.as_bytes())
            .expect("encrypt replacement credential");
        let replaced = UpstreamStore::store_oauth_tokens(
            storage.as_ref(),
            current.id,
            current.revision,
            encrypted,
            false,
        )
        .await
        .expect("actual credential replacement");
        assert_eq!(replaced.last_apply_error, None);
        assert_eq!(
            replaced.oauth_token_generation,
            upstream.oauth_token_generation + 1
        );
        UpstreamStore::set_enabled(storage.as_ref(), replaced.id, replaced.revision, true)
            .await
            .expect("enable reconnected upstream");
    }
    let reconnected = build(&stores, rebuilt.generation, &runtime, dir.path()).await;
    for (upstream, _) in &upstreams {
        let status = reconnected
            .upstream_status_snapshot
            .entries
            .get(&upstream.name)
            .expect("reconnected oauth status");
        assert_eq!(status.status, ApplyStatus::Active);
        assert_eq!(status.last_apply_error, None);
        let current = UpstreamStore::get_by_id(storage.as_ref(), upstream.id)
            .await
            .expect("load reconnected state")
            .expect("upstream exists");
        assert_eq!(current.last_apply_error, None);
    }
}

#[tokio::test]
async fn plan_info_uses_catalog_ratio_and_reconciles_history() {
    let (dir, storage) = storage_fixture().await;
    let stores = stores(storage.clone());
    let runtime = std::sync::Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let upstream = create_api_key_upstream(&storage, "max-5x").await;
    attach_plan_metadata(&storage, upstream.id).await;

    let view = build(&stores, 0, &runtime, dir.path()).await;

    let plan_info = view
        .plan_info_by_upstream
        .get(&upstream.id)
        .expect("upstream plan info");
    assert_eq!(plan_info.organization_type.as_deref(), Some("claude_max"));
    assert_eq!(
        plan_info.rate_limit_tier.as_deref(),
        Some("default_claude_max_5x")
    );
    assert_eq!(plan_info.capacity_ratio, 5.0);

    let history = PlanTierStore::list_current_upstream_plan_tiers(&*storage)
        .await
        .expect("plan tier history listed");
    let resolved = history
        .iter()
        .find(|record| record.upstream_id == upstream.id)
        .expect("upstream tier history");
    assert_eq!(resolved.organization_uuid.as_deref(), Some("org-max-5x"));
    assert_eq!(resolved.organization_type.as_deref(), Some("claude_max"));
    assert_eq!(
        resolved.rate_limit_tier.as_deref(),
        Some("default_claude_max_5x")
    );
    assert_eq!(resolved.seat_tier, None);
    assert_eq!(resolved.tier_key.as_deref(), Some("max_5x"));
    assert_eq!(resolved.resolution_source, TierResolutionSource::Builtin);
    assert_eq!(resolved.resolved_ratio_snapshot, Some(5.0));
    assert_eq!(resolved.effective_to_unix_millis, None);
    assert_eq!(resolved.provenance, "dynamic_view_reconcile");
}

/// An upstream whose credential fails apply validation enters the view with
/// `ApplyStatus::Error` and is excluded from routing candidates. Replacing the
/// credential through the store and rebuilding the view must restore it to
/// `ApplyStatus::Active` and back into `build_candidates` output — no restart
/// or separate eligibility reset involved.
#[tokio::test]
async fn repaired_upstream_leaves_apply_error_and_becomes_routable() {
    let (dir, storage) = storage_fixture().await;
    let stores = stores(storage.clone());
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let principal = create_principal(&storage, "principal-repair").await;
    // No `api_key_ciphertext`: apply validation fails with
    // "anthropic api-key upstream missing api_key_ciphertext".
    let broken = UpstreamStore::create(
        storage.as_ref(),
        UpstreamCreate {
            id: Uuid::new_v4(),
            name: "api-key-broken".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            base_url: None,
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        },
    )
    .await
    .expect("upstream created");

    let candidates_for = |view: &DynamicView, principal_name: &str| {
        build_candidates(
            view,
            principal_name,
            RequestKind::AnthropicMessages,
            &[],
            &HashMap::new(),
            &cc_lb_clock::SystemClock,
        )
        .iter()
        .map(|candidate| candidate.upstream_id)
        .collect::<Vec<_>>()
    };

    let broken_view = build(&stores, 0, &runtime, dir.path()).await;
    let status = broken_view
        .upstream_status_snapshot
        .entries
        .get("api-key-broken")
        .expect("broken upstream status");
    assert_eq!(status.status, ApplyStatus::Error);
    assert!(status.last_apply_error.is_some());
    assert!(
        !candidates_for(&broken_view, &principal.name).contains(&broken.id),
        "apply-error upstream must not be a routing candidate"
    );

    let aead = AeadService::from_master_key([1; 32]);
    let ciphertext = aead
        .encrypt(b"sk-ant-repaired-secret", broken.id.as_bytes())
        .expect("api-key ciphertext");
    UpstreamStore::update_api_key_secret(storage.as_ref(), broken.id, Some(ciphertext))
        .await
        .expect("api-key secret repaired");

    let repaired_view = build(&stores, broken_view.generation, &runtime, dir.path()).await;
    let status = repaired_view
        .upstream_status_snapshot
        .entries
        .get("api-key-broken")
        .expect("repaired upstream status");
    assert_eq!(status.status, ApplyStatus::Active);
    assert_eq!(status.last_apply_error, None);
    assert!(
        candidates_for(&repaired_view, &principal.name).contains(&broken.id),
        "repaired upstream must return to routing candidates"
    );
}
