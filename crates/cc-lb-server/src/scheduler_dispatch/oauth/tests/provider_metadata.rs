use super::super::{
    bound_provider_identity, merge_gemini_account_metadata, merge_openai_usage_identity,
};
use super::*;
use cc_lb_oauth_protocol::{GeminiOAuthProfile, OAuthAccountIdentity};
use cc_lb_scheduler::jobs::oauth_usage_poll::OAuthUsagePollObservation;
use cc_lb_storage_api::{UpstreamSubscriptionMetadataRecord, UpstreamSubscriptionMetadataStore};
use serde_json::{Value, json};
use std::sync::Mutex;

fn bundle(kind: UpstreamKind) -> OAuthTokenBundle {
    OAuthTokenBundle {
        access_token: "synthetic-display-token".to_owned(),
        refresh_token: String::new(),
        expires_at_unix_secs: 1_900_000_000,
        refresh_token_expires_at_unix_secs: None,
        account_id: Some(
            if kind == UpstreamKind::OpenaiOauth {
                "synthetic-workspace"
            } else {
                "member@example.test"
            }
            .to_owned(),
        ),
        project_id: (kind == UpstreamKind::GeminiOauth).then(|| "synthetic-project".to_owned()),
        oauth_profile: GeminiOAuthProfile::Antigravity,
        scopes: Vec::new(),
        never_refresh: true,
    }
}

fn metadata(id: Uuid, identity: &OAuthAccountIdentity) -> UpstreamSubscriptionMetadataRecord {
    UpstreamSubscriptionMetadataRecord {
        upstream_id: id,
        organization_uuid: None,
        organization_role: None,
        workspace_role: None,
        observed_at_unix_millis: 1,
        last_error: None,
        raw_roles: None,
        raw_bootstrap: Some(json!({"oauth_account": identity}).to_string()),
    }
}

fn openai_identity() -> OAuthAccountIdentity {
    OAuthAccountIdentity {
        account_email: Some("member@example.test".to_owned()),
        account_id: Some("synthetic-workspace".to_owned()),
        account_name: Some("Synthetic Member".to_owned()),
        plan: Some("team".to_owned()),
        ..Default::default()
    }
}

#[test]
fn openai_usage_metadata_requires_exact_account_and_preserves_optional_fields() {
    let bundle = bundle(UpstreamKind::OpenaiOauth);
    let mut identity = openai_identity();
    for invalid in [json!({}), json!(null), json!(42), json!("")] {
        let mut usage = json!({"account_id": "synthetic-workspace", "email": null});
        usage["plan_type"] = invalid;
        assert!(merge_openai_usage_identity(&mut identity, &bundle, &usage));
        assert_eq!(identity, openai_identity());
    }
    for account in [
        json!(null),
        json!(""),
        json!("another-workspace"),
        json!(123),
    ] {
        let before = identity.clone();
        assert!(!merge_openai_usage_identity(
            &mut identity,
            &bundle,
            &json!({
                "account_id": account, "plan_type": "pro", "email": "wrong@example.test"
            })
        ));
        assert_eq!(identity, before);
    }
    assert!(merge_openai_usage_identity(
        &mut identity,
        &bundle,
        &json!({
            "account_id": "synthetic-workspace", "plan_type": "prolite", "email": "updated@example.test"
        })
    ));
    assert_eq!(identity.plan.as_deref(), Some("prolite"));
    assert_eq!(
        identity.account_email.as_deref(),
        Some("updated@example.test")
    );
    assert_eq!(identity.account_name.as_deref(), Some("Synthetic Member"));
    assert_eq!(identity.plan_name, None);
}

#[test]
fn stored_display_identity_is_invalidated_on_account_or_project_switch() {
    let mut bundle = bundle(UpstreamKind::OpenaiOauth);
    let record = metadata(Uuid::nil(), &openai_identity());
    assert_eq!(
        bound_provider_identity(UpstreamKind::OpenaiOauth, &bundle, Some(&record), 1),
        openai_identity()
    );
    bundle.account_id = Some("new-workspace".to_owned());
    assert_eq!(
        bound_provider_identity(UpstreamKind::OpenaiOauth, &bundle, Some(&record), 1),
        OAuthAccountIdentity::default()
    );
    let mut bundle = self::bundle(UpstreamKind::GeminiOauth);
    let identity = OAuthAccountIdentity {
        account_email: bundle.account_id.clone(),
        project_id: bundle.project_id.clone(),
        account_name: Some("Synthetic Member".to_owned()),
        plan: Some("free-tier".to_owned()),
        ..Default::default()
    };
    let record = metadata(Uuid::nil(), &identity);
    bundle.account_id = Some("MEMBER@example.test".to_owned());
    assert_eq!(
        bound_provider_identity(UpstreamKind::GeminiOauth, &bundle, Some(&record), 1),
        identity
    );
    bundle.project_id = Some("new-project".to_owned());
    assert_eq!(
        bound_provider_identity(UpstreamKind::GeminiOauth, &bundle, Some(&record), 1),
        OAuthAccountIdentity::default()
    );
    bundle.project_id = identity.project_id;
    bundle.account_id = Some("another@example.test".to_owned());
    assert_eq!(
        bound_provider_identity(UpstreamKind::GeminiOauth, &bundle, Some(&record), 1),
        OAuthAccountIdentity::default()
    );
}

#[test]
fn gemini_partial_metadata_preserves_fields_and_new_plan_clears_old_name() {
    let bundle = bundle(UpstreamKind::GeminiOauth);
    let mut identity = OAuthAccountIdentity {
        account_email: bundle.account_id.clone(),
        project_id: bundle.project_id.clone(),
        account_name: Some("Synthetic Member".to_owned()),
        plan: Some("g1-ultra-tier".to_owned()),
        plan_name: Some("Google AI Ultra".to_owned()),
        ..Default::default()
    };
    let original = identity.clone();
    assert!(!merge_gemini_account_metadata(
        &mut identity,
        &bundle,
        None,
        None
    ));
    assert_eq!(identity, original);
    assert!(merge_gemini_account_metadata(
        &mut identity,
        &bundle,
        None,
        Some(&json!({"email": "MEMBER@example.test"}))
    ));
    assert_eq!(identity.plan_name.as_deref(), Some("Google AI Ultra"));
    assert_eq!(identity.account_name.as_deref(), Some("Synthetic Member"));
    let load = json!({
        "cloudaicompanionProject": "synthetic-project",
        "paidTier": {"id": "future-paid-tier"},
        "currentTier": {"id": "free-tier", "name": "Antigravity"}
    });
    assert!(merge_gemini_account_metadata(
        &mut identity,
        &bundle,
        Some(&load),
        None
    ));
    assert_eq!(identity.plan.as_deref(), Some("future-paid-tier"));
    assert_eq!(identity.plan_name, None);
    let before = identity.clone();
    assert!(!merge_gemini_account_metadata(
        &mut identity,
        &bundle,
        Some(&load),
        Some(&json!({
            "email": "other@example.test", "name": "Other Account"
        }))
    ));
    assert_eq!(identity, before);
    let mut different_project = load;
    different_project["cloudaicompanionProject"] = json!("other-project");
    assert!(!merge_gemini_account_metadata(
        &mut identity,
        &bundle,
        Some(&different_project),
        None
    ));
    assert_eq!(identity, before);
}

pub(super) async fn create_provider(
    fixture: &DispatchFixture,
    kind: UpstreamKind,
    base: Url,
    profile: GeminiOAuthProfile,
) -> UpstreamRecord {
    let id = Uuid::new_v4();
    let mut bundle = bundle(kind);
    bundle.oauth_profile = profile;
    let tokens = EncryptedOAuthTokens::encrypt(fixture.aead.as_ref(), &bundle, id.as_bytes())
        .expect("encrypt synthetic credential");
    UpstreamStore::create(
        fixture.storage.as_ref(),
        UpstreamCreate {
            id,
            name: format!("provider-metadata-{id}"),
            kind,
            base_url: Some(base),
            api_key_ciphertext: None,
            oauth_tokens: Some(cc_lb_storage_api::OAuthTokensCreate {
                tokens,
                never_refresh: true,
            }),
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        },
    )
    .await
    .expect("create provider")
}

async fn read_identity(fixture: &DispatchFixture, id: Uuid) -> OAuthAccountIdentity {
    let record = fixture
        .storage
        .get_upstream_subscription_metadata(id)
        .await
        .expect("read metadata")
        .expect("metadata persisted");
    let raw = record.raw_bootstrap.expect("allowlisted metadata");
    for private in [
        "synthetic-display-token",
        "private-picture",
        "private-user",
        "secret-provider-response",
    ] {
        assert!(
            !raw.contains(private),
            "unexpected provider response field persisted"
        );
    }
    let value: Value = serde_json::from_str(&raw).expect("metadata JSON");
    serde_json::from_value(value["oauth_account"].clone()).expect("identity")
}

#[tokio::test]
async fn openai_existing_poll_backfills_real_plan_and_preserves_name_and_quota() {
    let payload = Arc::new(Mutex::new(json!({
        "account_id": "synthetic-workspace", "email": "updated@example.test", "plan_type": "prolite",
        "user_id": "private-user", "raw": "secret-provider-response",
        "rate_limit": {"primary_window": {"used_percent": 25, "limit_window_seconds": 18000, "reset_at": 1_700_010_000}},
        "credits": {"has_credits": true, "unlimited": false, "balance": "15.0"}
    })));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let request_log = requests.clone();
    let body = payload.clone();
    let app = axum::Router::new().fallback(move |request: axum::extract::Request| {
        request_log
            .lock()
            .expect("log")
            .push(request.uri().path().to_owned());
        let response = body.lock().expect("body").clone();
        async move { axum::Json(response) }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let base = Url::parse(&format!(
        "http://{}/backend-api/codex",
        listener.local_addr().expect("addr")
    ))
    .expect("URL");
    let server = tokio::spawn(async move { axum::serve(listener, app).await.expect("server") });
    let fixture = DispatchFixture::new().await;
    let upstream = create_provider(
        &fixture,
        UpstreamKind::OpenaiOauth,
        base,
        GeminiOAuthProfile::Antigravity,
    )
    .await;
    fixture
        .storage
        .put_upstream_subscription_metadata(&metadata(upstream.id, &openai_identity()))
        .await
        .expect("seed identity");
    for plan in [json!("prolite"), Value::Null, json!(123), json!(" ")] {
        payload.lock().expect("body")["plan_type"] = plan;
        let observation = fixture
            .dispatch
            .poll_usage(upstream.id, None)
            .await
            .expect("poll");
        assert!(matches!(
            observation,
            OAuthUsagePollObservation::Success { .. }
        ));
        let identity = read_identity(&fixture, upstream.id).await;
        assert_eq!(identity.plan.as_deref(), Some("prolite"));
        assert_eq!(identity.account_name.as_deref(), Some("Synthetic Member"));
        assert_eq!(
            identity.account_email.as_deref(),
            Some("updated@example.test")
        );
    }
    let snapshots = fixture
        .dispatch
        .subscription_quota_cache
        .snapshot_for_upstream(upstream.id, 1_700_000_000_000, 60);
    assert!(
        snapshots
            .iter()
            .any(|snapshot| snapshot.window == "5h" && snapshot.utilization == Some(0.25))
    );
    assert!(
        snapshots
            .iter()
            .any(|snapshot| snapshot.window == "overage" && snapshot.credits.is_some())
    );
    let recorded = requests.lock().expect("log");
    assert_eq!(recorded.len(), 8);
    for pair in recorded.as_chunks::<2>().0 {
        assert_eq!(pair[0], "/backend-api/wham/usage");
        assert_eq!(pair[1], "/backend-api/wham/rate-limit-reset-credits");
    }
    drop(recorded);
    assert_eq!(fixture.refresh_entry_calls.load(Ordering::SeqCst), 0);
    server.abort();
}

async fn assert_gemini_poll_metadata(profile: GeminiOAuthProfile, project_only_import: bool) {
    let metadata_status = Arc::new(AtomicUsize::new(if project_only_import {
        503
    } else {
        200
    }));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let request_log = requests.clone();
    let status = metadata_status.clone();
    let app = axum::Router::new().fallback(move |request: axum::extract::Request| {
        let path = request.uri().path().to_owned();
        let user_agent = request.headers().get(http::header::USER_AGENT)
            .and_then(|value| value.to_str().ok()).unwrap_or_default().to_owned();
        request_log.lock().expect("log").push(path.clone());
        let status = status.load(Ordering::SeqCst);
        async move {
            let (status, value) = if path.ends_with(":retrieveUserQuotaSummary") {
                (200, json!({"groups": [
                    {"displayName": "Gemini models", "buckets": [{"bucketId": "gemini-5h", "remainingFraction": 0.75, "resetTime": "2026-11-01T13:00:00Z"}]},
                    {"displayName": "Claude and GPT models", "buckets": [{"bucketId": "3p-5h", "remainingFraction": 0.5, "resetTime": "2026-11-01T13:00:00Z"}]}
                ]}))
            } else if path == "/metadata/v1internal:loadCodeAssist" {
                let body = axum::body::to_bytes(request.into_body(), 4096).await.expect("request body");
                let body: Value = serde_json::from_slice(&body).expect("load JSON");
                if profile == GeminiOAuthProfile::Antigravity {
                    assert_eq!(body, json!({"metadata": {"ideType": "ANTIGRAVITY"}}));
                    assert_eq!(user_agent, cc_lb_oauth_protocol::ANTIGRAVITY_OAUTH_USER_AGENT);
                } else {
                    assert_eq!(body, json!({
                        "cloudaicompanionProject": "synthetic-project",
                        "metadata": {
                            "ideType": "IDE_UNSPECIFIED", "platform": "PLATFORM_UNSPECIFIED",
                            "pluginType": "GEMINI", "duetProject": "synthetic-project"
                        }
                    }));
                    assert_eq!(user_agent, "GeminiCLI/cc-lb");
                }
                let mut load = json!({
                    "cloudaicompanionProject": "synthetic-project", "gcpManaged": false,
                    "currentTier": {"id": "free-tier", "name": "Antigravity"},
                    "paidTier": {"id": "g1-ultra-tier", "name": "Google AI Ultra"},
                    "private": "secret-provider-response"
                });
                if profile == GeminiOAuthProfile::GeminiCli {
                    load.as_object_mut().expect("load").remove("cloudaicompanionProject");
                }
                (status, load)
            } else if path == "/metadata/userinfo" {
                let userinfo_status = if project_only_import && status == 503 { 200 } else { status };
                (userinfo_status, json!({"id": "private-user", "email": "MEMBER@example.test", "name": "Synthetic Member", "picture": "private-picture"}))
            } else {
                (500, json!({"error": "unexpected endpoint"}))
            };
            (http::StatusCode::from_u16(status as u16).expect("status"), axum::Json(value))
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let root = format!("http://{}", listener.local_addr().expect("addr"));
    let server = tokio::spawn(async move { axum::serve(listener, app).await.expect("server") });
    let mut fixture = DispatchFixture::new().await;
    let config = Arc::make_mut(&mut fixture.dispatch.config);
    let antigravity_path = if profile == GeminiOAuthProfile::Antigravity {
        "metadata"
    } else {
        "wrong-profile"
    };
    let cli_path = if profile == GeminiOAuthProfile::GeminiCli {
        "metadata"
    } else {
        "wrong-profile"
    };
    config.oauth.antigravity = Some(cc_lb_config::AntigravityOAuthConfig {
        code_assist_url: format!("{root}/{antigravity_path}").parse().expect("URL"),
        userinfo_url: format!("{root}/{antigravity_path}/userinfo")
            .parse()
            .expect("URL"),
        ..Default::default()
    });
    config.oauth.gemini = Some(cc_lb_config::GeminiOAuthConfig {
        code_assist_url: format!("{root}/{cli_path}/v1internal")
            .parse()
            .expect("URL"),
        userinfo_url: format!("{root}/{cli_path}/userinfo").parse().expect("URL"),
        ..Default::default()
    });
    let upstream = create_provider(
        &fixture,
        UpstreamKind::GeminiOauth,
        format!("{root}/inference").parse().expect("URL"),
        profile,
    )
    .await;
    let upstream = if project_only_import {
        let mut imported = bundle(UpstreamKind::GeminiOauth);
        imported.oauth_profile = profile;
        imported.account_id = None;
        let tokens =
            EncryptedOAuthTokens::encrypt(fixture.aead.as_ref(), &imported, upstream.id.as_bytes())
                .expect("encrypt documented project-only import");
        UpstreamStore::store_oauth_tokens(
            fixture.storage.as_ref(),
            upstream.id,
            upstream.revision,
            tokens,
            true,
        )
        .await
        .expect("store project-only import")
    } else {
        upstream
    };
    let initial_requests = if project_only_import {
        let observation = fixture
            .dispatch
            .poll_usage(upstream.id, None)
            .await
            .expect("poll with failed load");
        assert!(matches!(
            observation,
            OAuthUsagePollObservation::Success { .. }
        ));
        assert!(
            fixture
                .storage
                .get_upstream_subscription_metadata(upstream.id)
                .await
                .expect("read unconfirmed metadata")
                .is_none(),
            "successful userinfo must not bind a project when load returns HTTP 503"
        );
        metadata_status.store(200, Ordering::SeqCst);
        3
    } else {
        0
    };
    for expected_requests in [3, 4] {
        let expected_requests = expected_requests + initial_requests;
        let observation = fixture
            .dispatch
            .poll_usage(upstream.id, None)
            .await
            .expect("poll");
        assert!(matches!(
            observation,
            OAuthUsagePollObservation::Success { .. }
        ));
        let identity = read_identity(&fixture, upstream.id).await;
        assert_eq!(identity.plan.as_deref(), Some("g1-ultra-tier"));
        assert_eq!(identity.plan_name.as_deref(), Some("Google AI Ultra"));
        assert_eq!(identity.account_name.as_deref(), Some("Synthetic Member"));
        assert_eq!(
            requests.lock().expect("log").len(),
            expected_requests,
            "metadata must use slow cadence, not each quota tick"
        );
    }
    let good = read_identity(&fixture, upstream.id).await;
    if project_only_import {
        let persisted = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream.id)
            .await
            .expect("read imported credential")
            .expect("import exists");
        let stored_bundle = persisted
            .oauth_credentials
            .as_ref()
            .expect("credential")
            .decrypt(fixture.aead.as_ref(), upstream.id.as_bytes())
            .expect("decrypt import");
        assert_eq!(
            stored_bundle.account_id, None,
            "display enrichment must not rewrite credential identity"
        );
        let record = fixture
            .storage
            .get_upstream_subscription_metadata(upstream.id)
            .await
            .expect("read binding")
            .expect("metadata");
        let raw: Value = serde_json::from_str(record.raw_bootstrap.as_deref().expect("bootstrap"))
            .expect("allowlisted bootstrap");
        assert_eq!(
            raw["oauth_account_generation"],
            upstream.oauth_token_generation
        );
    }
    if !project_only_import {
        let mut refreshed = bundle(UpstreamKind::GeminiOauth);
        refreshed.oauth_profile = profile;
        refreshed.access_token = "same-account-refreshed-synthetic-token".to_owned();
        let encrypted = EncryptedOAuthTokens::encrypt(
            fixture.aead.as_ref(),
            &refreshed,
            upstream.id.as_bytes(),
        )
        .expect("encrypt refreshed credential");
        let rotated = UpstreamStore::store_oauth_tokens(
            fixture.storage.as_ref(),
            upstream.id,
            upstream.revision,
            encrypted,
            true,
        )
        .await
        .expect("simulate completed same-account refresh");
        assert!(rotated.oauth_token_generation > upstream.oauth_token_generation);
        let before = requests.lock().expect("log").len();
        let observation = fixture
            .dispatch
            .poll_usage(upstream.id, None)
            .await
            .expect("poll after refresh");
        assert!(matches!(
            observation,
            OAuthUsagePollObservation::Success { .. }
        ));
        assert_eq!(read_identity(&fixture, upstream.id).await, good);
        assert_eq!(
            requests.lock().expect("log").len(),
            before + 1,
            "email-bound same-account refresh must retain metadata cadence"
        );
    }
    for failure_status in [401, 403, 500] {
        metadata_status.store(failure_status, Ordering::SeqCst);
        let mut aged = fixture
            .storage
            .get_upstream_subscription_metadata(upstream.id)
            .await
            .expect("load metadata to age")
            .expect("persisted metadata");
        aged.observed_at_unix_millis = 1;
        fixture
            .storage
            .put_upstream_subscription_metadata(&aged)
            .await
            .expect("age metadata");
        let observation = fixture
            .dispatch
            .poll_usage(upstream.id, None)
            .await
            .expect("poll with metadata failure");
        assert!(matches!(
            observation,
            OAuthUsagePollObservation::Success { .. }
        ));
        assert_eq!(read_identity(&fixture, upstream.id).await, good);
    }
    let snapshots = fixture
        .dispatch
        .subscription_quota_cache
        .snapshot_for_upstream(upstream.id, 1_700_000_000_000, 60);
    assert!(
        snapshots
            .iter()
            .any(|snapshot| snapshot.window == "gemini_5h" && snapshot.utilization == Some(0.25))
    );
    assert!(requests.lock().expect("log").iter().all(|path| {
        [
            "/inference:retrieveUserQuotaSummary",
            "/metadata/v1internal:loadCodeAssist",
            "/metadata/userinfo",
        ]
        .contains(&path.as_str())
    }));
    assert_eq!(fixture.refresh_entry_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.claim_calls.load(Ordering::SeqCst), 0);
    server.abort();
}

#[tokio::test]
async fn gemini_existing_poll_enriches_on_configured_hosts_and_failure_keeps_quota() {
    assert_gemini_poll_metadata(GeminiOAuthProfile::Antigravity, false).await;
}

#[tokio::test]
async fn gemini_cli_existing_poll_uses_its_own_config_and_core_metadata() {
    assert_gemini_poll_metadata(GeminiOAuthProfile::GeminiCli, false).await;
}

#[tokio::test]
async fn gemini_project_only_import_backfills_metadata_and_respects_cadence() {
    assert_gemini_poll_metadata(GeminiOAuthProfile::GeminiCli, true).await;
    assert_gemini_poll_metadata(GeminiOAuthProfile::Antigravity, true).await;
}

async fn assert_reconnect_metadata_publication(
    generation_changes_after_snapshot: bool,
    initial_metadata_present: bool,
) {
    let usage = json!({
        "account_id": "synthetic-workspace", "plan_type": "prolite", "email": "member@example.test",
        "rate_limit": {"primary_window": {
            "used_percent": 25, "limit_window_seconds": 18000, "reset_at": 1_700_010_000
        }}
    });
    let app = axum::Router::new().fallback(move || {
        let body = usage.clone();
        async move { axum::Json(body) }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let base = format!(
        "http://{}/backend-api/codex",
        listener.local_addr().expect("address")
    );
    let server = tokio::spawn(async move { axum::serve(listener, app).await.expect("server") });
    let fixture = DispatchFixture::new().await;
    let upstream = create_provider(
        &fixture,
        UpstreamKind::OpenaiOauth,
        Url::parse(&base).expect("URL"),
        GeminiOAuthProfile::GeminiCli,
    )
    .await;
    if initial_metadata_present {
        fixture
            .storage
            .put_upstream_subscription_metadata(&metadata(upstream.id, &openai_identity()))
            .await
            .expect("seed old name");
    }
    // Second publication window: reconnect's token commit precedes the poll,
    // but its metadata publication still happens in the pre-write hook.
    let upstream = if generation_changes_after_snapshot {
        upstream
    } else {
        let mut current_bundle = bundle(UpstreamKind::OpenaiOauth);
        current_bundle.access_token = "same-account-reconnect-token".to_owned();
        let tokens = EncryptedOAuthTokens::encrypt(
            fixture.aead.as_ref(),
            &current_bundle,
            upstream.id.as_bytes(),
        )
        .expect("encrypt reconnect before poll");
        UpstreamStore::store_oauth_tokens(
            fixture.storage.as_ref(),
            upstream.id,
            upstream.revision,
            tokens,
            true,
        )
        .await
        .expect("commit reconnect tokens before metadata")
    };
    let storage = fixture.storage.clone();
    let aead = fixture.aead.clone();
    let original = upstream.clone();
    let hook: super::ProviderMetadataWriteHook = Arc::new(move || {
        let storage = storage.clone();
        let aead = aead.clone();
        let original = original.clone();
        Box::pin(async move {
            if generation_changes_after_snapshot {
                let mut current_bundle = bundle(UpstreamKind::OpenaiOauth);
                current_bundle.access_token = "same-account-reconnect-token".to_owned();
                let tokens = EncryptedOAuthTokens::encrypt(
                    aead.as_ref(),
                    &current_bundle,
                    original.id.as_bytes(),
                )
                .expect("encrypt reconnect");
                let reconnected = UpstreamStore::store_oauth_tokens(
                    storage.as_ref(),
                    original.id,
                    original.revision,
                    tokens,
                    true,
                )
                .await
                .expect("same-account reconnect");
                assert!(reconnected.oauth_token_generation > original.oauth_token_generation);
            }
            let mut fresh = openai_identity();
            fresh.account_name = Some("Fresh Reconnect Name".to_owned());
            fresh.plan = Some("business".to_owned());
            storage
                .put_upstream_subscription_metadata(&metadata(original.id, &fresh))
                .await
                .expect("fresh reconnect metadata");
        })
    });
    let observation = super::BEFORE_PROVIDER_METADATA_WRITE
        .scope(hook, fixture.dispatch.poll_usage(upstream.id, None))
        .await
        .expect("poll with declined stale metadata");
    assert!(matches!(
        observation,
        OAuthUsagePollObservation::Success { .. }
    ));
    let identity = read_identity(&fixture, upstream.id).await;
    assert_eq!(
        identity.account_name.as_deref(),
        Some("Fresh Reconnect Name")
    );
    assert_eq!(identity.plan.as_deref(), Some("business"));
    assert!(
        fixture
            .dispatch
            .subscription_quota_cache
            .snapshot_for_upstream(upstream.id, 1_700_000_000_000, 60)
            .iter()
            .any(|snapshot| snapshot.window == "5h" && snapshot.utilization == Some(0.25))
    );
    let later = fixture
        .dispatch
        .poll_usage(upstream.id, None)
        .await
        .expect("later poll");
    assert!(matches!(later, OAuthUsagePollObservation::Success { .. }));
    assert_eq!(
        read_identity(&fixture, upstream.id)
            .await
            .account_name
            .as_deref(),
        Some("Fresh Reconnect Name"),
        "a later poll must not restore the obsolete name"
    );
    server.abort();
}

#[tokio::test]
async fn stale_poll_cannot_overwrite_same_account_reconnect_metadata() {
    assert_reconnect_metadata_publication(true, true).await;
}

#[tokio::test]
async fn same_generation_poll_cannot_overwrite_reconnect_metadata_publication() {
    assert_reconnect_metadata_publication(false, true).await;
}

#[tokio::test]
async fn initially_missing_metadata_cannot_overwrite_reconnect_publication() {
    assert_reconnect_metadata_publication(false, false).await;
}

#[test]
fn project_only_gemini_merge_requires_successful_project_scoped_load() {
    let load = json!({
        "cloudaicompanionProject": "synthetic-project",
        "paidTier": {"id": "g1-ultra-tier", "name": "Google AI Ultra"}
    });
    let userinfo = json!({"email": "member@example.test", "name": "Synthetic Imported Member"});
    for empty_email in [None, Some("  ".to_owned())] {
        let mut imported = bundle(UpstreamKind::GeminiOauth);
        imported.account_id = empty_email;
        let mut identity = OAuthAccountIdentity::default();
        assert!(
            !merge_gemini_account_metadata(&mut identity, &imported, None, Some(&userinfo)),
            "userinfo alone cannot bind an unconfirmed project"
        );
        assert_eq!(identity, OAuthAccountIdentity::default());
        let mut mismatch = load.clone();
        mismatch["cloudaicompanionProject"] = json!("different-project");
        assert!(!merge_gemini_account_metadata(
            &mut identity,
            &imported,
            Some(&mismatch),
            Some(&userinfo)
        ));
        assert_eq!(identity, OAuthAccountIdentity::default());
        assert!(merge_gemini_account_metadata(
            &mut identity,
            &imported,
            Some(&load),
            Some(&userinfo)
        ));
        assert_eq!(
            identity.account_name.as_deref(),
            Some("Synthetic Imported Member")
        );
        assert_eq!(identity.plan.as_deref(), Some("g1-ultra-tier"));
        assert_eq!(identity.plan_name.as_deref(), Some("Google AI Ultra"));
    }
    let mut imported = bundle(UpstreamKind::GeminiOauth);
    imported.account_id = None;
    imported.project_id = Some("  ".to_owned());
    assert!(!merge_gemini_account_metadata(
        &mut OAuthAccountIdentity::default(),
        &imported,
        Some(&load),
        Some(&userinfo),
    ));
}
