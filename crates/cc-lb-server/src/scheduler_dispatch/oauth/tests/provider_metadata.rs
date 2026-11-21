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
            vertex: None,
            vertex_credential_type: None,
            vertex_quota_project_id: None,
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
            } else if path == "/inference:loadCodeAssist" {
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
            "/inference:loadCodeAssist",
            "/metadata/userinfo",
        ]
        .contains(&path.as_str())
    }));
    assert_eq!(fixture.refresh_entry_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.claim_calls.load(Ordering::SeqCst), 0);
    server.abort();
}

#[tokio::test]
async fn gemini_existing_poll_enriches_on_explicit_host_and_failure_keeps_quota() {
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

#[tokio::test]
async fn gemini_metadata_without_override_preserves_profile_config_prefix_and_userinfo() {
    for profile in [
        GeminiOAuthProfile::Antigravity,
        GeminiOAuthProfile::GeminiCli,
    ] {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let request_log = requests.clone();
        let app = axum::Router::new().fallback(move |request: axum::extract::Request| {
            let path = request.uri().path().to_owned();
            request_log.lock().expect("log").push(path.clone());
            async move {
                let value = match path.as_str() {
                    "/metadata/v1internal:loadCodeAssist" => json!({
                        "cloudaicompanionProject": "synthetic-project",
                        "currentTier": {"id": "free-tier"}
                    }),
                    "/metadata/userinfo" => json!({"email": "member@example.test"}),
                    _ => return (http::StatusCode::NOT_FOUND, axum::Json(Value::Null)),
                };
                (http::StatusCode::OK, axum::Json(value))
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let root = format!("http://{}", listener.local_addr().expect("address"));
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
        let mut credential = bundle(UpstreamKind::GeminiOauth);
        credential.oauth_profile = profile;
        let (load, userinfo) = super::super::super::http::fetch_gemini_account_metadata(
            &fixture.dispatch.http,
            &fixture.dispatch.config,
            None,
            &credential,
            &fixture.dispatch.cancel,
        )
        .await;
        assert_eq!(
            load.expect("load metadata")["currentTier"]["id"],
            "free-tier"
        );
        assert_eq!(userinfo.expect("userinfo")["email"], "member@example.test");
        let mut paths = requests.lock().expect("log").clone();
        paths.sort_unstable();
        assert_eq!(
            paths,
            ["/metadata/userinfo", "/metadata/v1internal:loadCodeAssist"]
        );
        assert_eq!(fixture.refresh_entry_calls.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.claim_calls.load(Ordering::SeqCst), 0);
        server.abort();
    }
}

async fn assert_gemini_explicit_control_destination(
    profile: GeminiOAuthProfile,
    base_path: &str,
    expected_prefix: &str,
    summary_status: http::StatusCode,
) {
    assert_gemini_explicit_destination_matrix(
        profile,
        base_path,
        expected_prefix,
        summary_status,
        false,
    )
    .await;
}

async fn assert_gemini_explicit_destination_matrix(
    profile: GeminiOAuthProfile,
    base_path: &str,
    expected_prefix: &str,
    summary_status: http::StatusCode,
    include_content_and_warmup: bool,
) {
    assert_gemini_explicit_destination_matrix_with_query(
        profile,
        base_path,
        expected_prefix,
        summary_status,
        include_content_and_warmup,
        None,
    )
    .await;
}

async fn assert_gemini_explicit_destination_matrix_with_query(
    profile: GeminiOAuthProfile,
    base_path: &str,
    expected_prefix: &str,
    summary_status: http::StatusCode,
    include_content_and_warmup: bool,
    expected_control_query: Option<&str>,
) {
    let summary_path = format!("{expected_prefix}:retrieveUserQuotaSummary");
    let quota_path = format!("{expected_prefix}:retrieveUserQuota");
    let load_path = format!("{expected_prefix}:loadCodeAssist");
    let requests = Arc::new(Mutex::new(Vec::new()));
    let request_log = requests.clone();
    let expected_summary = summary_path.clone();
    let expected_quota = quota_path.clone();
    let expected_load = load_path.clone();
    let app = axum::Router::new().fallback(move |request: axum::extract::Request| {
        let request_log = request_log.clone();
        let summary_path = expected_summary.clone();
        let quota_path = expected_quota.clone();
        let load_path = expected_load.clone();
        let load_prefix = expected_load
            .strip_suffix(":loadCodeAssist")
            .expect("load action")
            .to_owned();
        async move {
            let method = request.method().clone();
            let path = request.uri().path().to_owned();
            let query = request.uri().query().map(str::to_owned);
            let authority = request
                .headers()
                .get(http::header::HOST)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_owned();
            let user_agent = request
                .headers()
                .get(http::header::USER_AGENT)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_owned();
            let content_type = request
                .headers()
                .get(http::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_owned();
            // Record presence only: never retain or print the bearer credential.
            let bearer_present = request
                .headers()
                .get(http::header::AUTHORIZATION)
                .is_some_and(|value| value.as_bytes().starts_with(b"Bearer "));
            let body = axum::body::to_bytes(request.into_body(), 4096)
                .await
                .expect("request body");
            let body: Value = if body.is_empty() {
                Value::Null
            } else {
                serde_json::from_slice(&body).expect("control request JSON")
            };
            request_log.lock().expect("log").push((
                method,
                authority,
                path.clone(),
                user_agent,
                content_type,
                bearer_present,
                body,
                query,
            ));
            let (status, value) = if [
                format!("{load_prefix}:generateContent"),
                format!("{load_prefix}:streamGenerateContent"),
                format!("{load_prefix}:countTokens"),
            ]
            .contains(&path)
            {
                (
                    http::StatusCode::OK,
                    json!({"response": {"candidates": []}, "totalTokens": 7}),
                )
            } else if path == summary_path {
                (summary_status, json!({"groups": []}))
            } else if path == quota_path {
                (
                    http::StatusCode::OK,
                    json!({"buckets": [
                        {"modelId": "gemini-2.5-pro", "remainingFraction": 0.4},
                        {"modelId": "claude-sonnet", "remainingFraction": 0.3}
                    ]}),
                )
            } else if path == load_path {
                (
                    http::StatusCode::OK,
                    json!({
                        "cloudaicompanionProject": "synthetic-project",
                        "currentTier": {"id": "free-tier", "name": "Synthetic Tier"}
                    }),
                )
            } else if path == "/config-only/userinfo" {
                (
                    http::StatusCode::OK,
                    json!({"email": "member@example.test", "name": "Synthetic Member"}),
                )
            } else {
                (
                    http::StatusCode::NOT_FOUND,
                    json!({"error": "unexpected destination"}),
                )
            };
            (status, axum::Json(value))
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let authority = listener.local_addr().expect("address").to_string();
    let root = format!("http://{authority}");
    let server = tokio::spawn(async move { axum::serve(listener, app).await.expect("server") });
    let mut fixture = DispatchFixture::new().await;
    let config = Arc::make_mut(&mut fixture.dispatch.config);
    let userinfo_query = expected_control_query.map(|_| "identity=own");
    let userinfo_url: Url = format!(
        "{root}/config-only/userinfo{}",
        if userinfo_query.is_some() {
            "?identity=own"
        } else {
            ""
        }
    )
    .parse()
    .expect("userinfo URL");
    config.oauth.antigravity = Some(cc_lb_config::AntigravityOAuthConfig {
        code_assist_url: format!("{root}/config-only").parse().expect("URL"),
        userinfo_url: userinfo_url.clone(),
        ..Default::default()
    });
    config.oauth.gemini = Some(cc_lb_config::GeminiOAuthConfig {
        code_assist_url: format!("{root}/config-only/v1internal")
            .parse()
            .expect("URL"),
        userinfo_url,
        ..Default::default()
    });
    let upstream = create_provider(
        &fixture,
        UpstreamKind::GeminiOauth,
        format!("{root}{base_path}").parse().expect("URL"),
        profile,
    )
    .await;
    let observation = fixture
        .dispatch
        .poll_usage(upstream.id, None)
        .await
        .expect("poll");
    let terminal = matches!(
        summary_status,
        http::StatusCode::UNAUTHORIZED | http::StatusCode::TOO_MANY_REQUESTS
    );
    match summary_status {
        http::StatusCode::UNAUTHORIZED => assert!(matches!(
            observation,
            OAuthUsagePollObservation::StatusFailure { status: 401, .. }
        )),
        http::StatusCode::TOO_MANY_REQUESTS => assert!(matches!(
            observation,
            OAuthUsagePollObservation::Throttled { .. }
        )),
        _ => assert!(matches!(
            observation,
            OAuthUsagePollObservation::Success { .. }
        )),
    }
    // Use the same persisted upstream, signer builder, HTTP client and recorder as quota/load.
    // This exercises signed wire requests, not just resolver or request-builder output.
    let generate_path = format!("{expected_prefix}:generateContent");
    let stream_path = format!("{expected_prefix}:streamGenerateContent");
    let count_path = format!("{expected_prefix}:countTokens");
    if include_content_and_warmup && !terminal {
        let credential = upstream
            .oauth_credentials
            .as_ref()
            .expect("stored OAuth credential")
            .decrypt(fixture.aead.as_ref(), upstream.id.as_bytes())
            .expect("decrypt stored credential");
        let signer = crate::dynamic_view_builder::build_subscription_signer(
            &upstream,
            &credential,
            fixture.clock.clone(),
            fixture.dispatch.config.oauth.gemini.as_ref(),
            fixture.dispatch.config.oauth.antigravity.as_ref(),
        )
        .expect("real content signer");
        let dialect = cc_lb_engine::lifecycle::native_route_dialect(&upstream);
        let api = cc_lb_domain::Upstream::GeminiDirect {
            base_url: upstream.base_url.clone(),
        };
        let principal = cc_lb_domain::Principal {
            id: upstream.id.to_string(),
            kind: cc_lb_domain::PrincipalKind::OAuthSubject,
        };
        for action in ["generateContent", "streamGenerateContent", "countTokens"] {
            let mut headers = http::HeaderMap::new();
            headers.insert(
                http::header::CONTENT_TYPE,
                http::HeaderValue::from_static("application/json"),
            );
            headers.insert(
                http::header::USER_AGENT,
                http::HeaderValue::from_static("GeminiCLI/cc-lb"),
            );
            let context = cc_lb_upstream::DialectShapeContext {
                request_id: Uuid::new_v4().to_string(),
                downstream_headers: headers,
                method: http::Method::POST,
                path: format!("/v1beta/models/gemini-2.5-flash:{action}"),
                query: Some("keep=a%2Bb&trace=1".to_owned()),
                body_bytes: bytes::Bytes::from(
                    serde_json::to_vec(&json!({
                        "contents": [{"role": "user", "parts": [{"text": "recorder content"}]}]
                    }))
                    .expect("native content JSON"),
                ),
            };
            let shaped =
                cc_lb_upstream::shape_request(dialect.as_ref(), &context, &api, &principal)
                    .expect("native content shape");
            let signed = cc_lb_upstream::sign_request(signer.as_ref(), shaped)
                .await
                .expect("real content signing");
            let mut request = http::Request::builder()
                .method(signed.method().clone())
                .uri(signed.url().as_str());
            for (name, value) in signed.headers() {
                request = request.header(name, value);
            }
            let response = fixture
                .dispatch
                .http
                .request(
                    request
                        .body(http_body_util::Full::new(signed.body().clone()))
                        .expect("signed HTTP request"),
                )
                .await
                .expect("content loopback response");
            assert_eq!(response.status(), http::StatusCode::OK, "{action}");
            let response = http_body_util::BodyExt::collect(response.into_body())
                .await
                .expect("content response body")
                .to_bytes();
            let response: Value = serde_json::from_slice(&response).expect("response JSON");
            if action == "countTokens" {
                assert_eq!(response["totalTokens"], 7);
            } else {
                assert_eq!(response["response"]["candidates"], json!([]));
            }
        }
        for lane in [
            cc_lb_storage_api::WarmupLane::Primary,
            cc_lb_storage_api::WarmupLane::ThirdParty,
        ] {
            let warmup = crate::warmup::request::dispatch_provider_warmup_attempt(
                crate::warmup::request::ProviderWarmupDispatchParams {
                    http: &fixture.dispatch.http,
                    upstream: &upstream,
                    lane,
                    aead: fixture.aead.as_ref(),
                    config: &fixture.dispatch.config,
                    clock: fixture.clock.clone(),
                },
            )
            .await;
            assert!(
                matches!(
                    warmup,
                    crate::warmup::request::WarmupRequestAttempt::Response {
                        status: http::StatusCode::OK,
                        ref observations,
                        ref error_detail,
                    } if observations.is_empty() && error_detail.is_none()
                ),
                "real provider warmup response for {lane:?}: {warmup:?}"
            );
        }
    }
    // All destinations must be observed, including the sibling fallback action.
    // The fake deliberately rejects doubled action suffixes rather than accepting ends_with.
    server.abort();
    let records = requests.lock().expect("log");
    let mut paths: Vec<_> = records.iter().map(|record| record.2.as_str()).collect();
    assert_eq!(paths.first().copied(), Some(summary_path.as_str()));
    if !terminal {
        assert_eq!(paths.get(1).copied(), Some(quota_path.as_str()));
    }
    let mut expected = if terminal {
        vec![summary_path.as_str()]
    } else {
        vec![
            summary_path.as_str(),
            quota_path.as_str(),
            load_path.as_str(),
            "/config-only/userinfo",
        ]
    };
    if include_content_and_warmup && !terminal {
        expected.extend([
            generate_path.as_str(),
            stream_path.as_str(),
            count_path.as_str(),
            generate_path.as_str(),
            generate_path.as_str(),
        ]);
    }
    // loadCodeAssist and userinfo run concurrently; their arrival order is immaterial.
    paths.sort_unstable();
    expected.sort_unstable();
    assert_eq!(
        paths, expected,
        "control destinations for {profile:?} and {base_path}"
    );
    if include_content_and_warmup && !terminal {
        let mut warmup_models: Vec<_> = records
            .iter()
            .filter(|record| record.6["request"]["contents"][0]["parts"][0]["text"] == ".")
            .map(|record| record.6["model"].as_str().expect("warmup model"))
            .collect();
        let warmup = &fixture.dispatch.config.warmup;
        let mut expected_models = vec![
            match profile {
                GeminiOAuthProfile::Antigravity => warmup.gemini_antigravity_model.as_str(),
                GeminiOAuthProfile::GeminiCli => warmup.gemini_model.as_str(),
            },
            warmup.gemini_third_party_model.as_str(),
        ];
        warmup_models.sort_unstable();
        expected_models.sort_unstable();
        assert_eq!(warmup_models, expected_models, "both real warmup lanes");
    }
    for (method, host, path, user_agent, content_type, bearer_present, body, query) in
        records.iter()
    {
        assert_eq!(host, &authority, "no alternate-host retry");
        assert!(*bearer_present, "authenticated control request");
        let is_warmup = body["request"]["contents"][0]["parts"][0]["text"] == ".";
        let expected_query = if path == &stream_path {
            Some("keep=a%2Bb&trace=1&alt=sse")
        } else if path == &count_path || (path == &generate_path && !is_warmup) {
            Some("keep=a%2Bb&trace=1")
        } else if path == "/config-only/userinfo" {
            userinfo_query
        } else if path == &summary_path || path == &quota_path || path == &load_path {
            expected_control_query
        } else {
            None
        };
        assert_eq!(
            query.as_deref(),
            expected_query,
            "wire query for {profile:?} and {path}"
        );
        if path == "/config-only/userinfo" {
            assert_eq!(method, http::Method::GET);
            continue;
        }
        assert_eq!(method, http::Method::POST);
        assert_eq!(content_type, "application/json");
        if profile == GeminiOAuthProfile::Antigravity {
            assert_eq!(
                user_agent,
                cc_lb_oauth_protocol::ANTIGRAVITY_OAUTH_USER_AGENT
            );
        }
        if path == &load_path {
            let expected_body = match profile {
                GeminiOAuthProfile::Antigravity => json!({"metadata": {"ideType": "ANTIGRAVITY"}}),
                GeminiOAuthProfile::GeminiCli => json!({
                    "cloudaicompanionProject": "synthetic-project",
                    "metadata": {
                        "ideType": "IDE_UNSPECIFIED", "platform": "PLATFORM_UNSPECIFIED",
                        "pluginType": "GEMINI", "duetProject": "synthetic-project"
                    }
                }),
            };
            assert_eq!(body, &expected_body);
            if profile == GeminiOAuthProfile::GeminiCli {
                assert_eq!(user_agent, "GeminiCLI/cc-lb");
            }
        } else if [
            generate_path.as_str(),
            stream_path.as_str(),
            count_path.as_str(),
        ]
        .contains(&path.as_str())
        {
            let request = &body["request"];
            if path == &count_path {
                assert_eq!(
                    body,
                    &json!({"request": {
                        "model": "models/gemini-2.5-flash",
                        "contents": [{"role": "user", "parts": [{"text": "recorder content"}]}]
                    }}),
                    "count omits project, top-level model and Antigravity envelope fields"
                );
            } else {
                let is_warmup = request["contents"][0]["parts"][0]["text"] == ".";
                assert_eq!(body["project"], "synthetic-project");
                if is_warmup {
                    assert!(
                        [
                            match profile {
                                GeminiOAuthProfile::Antigravity => fixture
                                    .dispatch
                                    .config
                                    .warmup
                                    .gemini_antigravity_model
                                    .as_str(),
                                GeminiOAuthProfile::GeminiCli => {
                                    fixture.dispatch.config.warmup.gemini_model.as_str()
                                }
                            },
                            fixture
                                .dispatch
                                .config
                                .warmup
                                .gemini_third_party_model
                                .as_str(),
                        ]
                        .contains(&body["model"].as_str().expect("warmup model"))
                    );
                } else {
                    assert_eq!(body["model"], "gemini-2.5-flash");
                }
                assert_eq!(
                    request["contents"],
                    json!([{"role": "user", "parts": [{
                        "text": if is_warmup { "." } else { "recorder content" }
                    }]}])
                );
                if is_warmup {
                    assert_eq!(path, &generate_path);
                    assert_eq!(request["generationConfig"]["maxOutputTokens"], 1);
                }
                if profile == GeminiOAuthProfile::Antigravity {
                    assert_eq!(body["userAgent"], "antigravity");
                    assert!(
                        body["requestId"]
                            .as_str()
                            .is_some_and(|id| id.starts_with("agent-"))
                    );
                } else {
                    assert!(body.get("userAgent").is_none());
                    assert!(body.get("requestId").is_none());
                }
            }
            if profile == GeminiOAuthProfile::GeminiCli
                && request["contents"][0]["parts"][0]["text"] != "."
            {
                assert_eq!(user_agent, "GeminiCLI/cc-lb");
            }
        } else {
            assert_eq!(body, &json!({"project": "synthetic-project"}));
        }
    }
    assert_eq!(fixture.refresh_entry_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.claim_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn gemini_explicit_base_overrides_profile_metadata_root_version_and_action() {
    for profile in [
        GeminiOAuthProfile::Antigravity,
        GeminiOAuthProfile::GeminiCli,
    ] {
        for (base, prefix) in [
            ("", "/v1internal"),
            ("/upstream/v1internal", "/upstream/v1internal"),
            (
                "/upstream/v1internal:generateContent",
                "/upstream/v1internal",
            ),
            (
                "/upstream/v1internal:loadCodeAssist",
                "/upstream/v1internal",
            ),
            (
                "/upstream/v1internal:retrieveUserQuotaSummary",
                "/upstream/v1internal",
            ),
        ] {
            assert_gemini_explicit_control_destination(profile, base, prefix, http::StatusCode::OK)
                .await;
        }
    }
}

#[tokio::test]
async fn gemini_explicit_action_fallback_stays_on_host_and_auth_or_rate_limit_stops() {
    for profile in [
        GeminiOAuthProfile::Antigravity,
        GeminiOAuthProfile::GeminiCli,
    ] {
        for status in [
            http::StatusCode::NOT_FOUND,
            http::StatusCode::UNAUTHORIZED,
            http::StatusCode::TOO_MANY_REQUESTS,
        ] {
            assert_gemini_explicit_control_destination(
                profile,
                "/upstream/v1internal:retrieveUserQuotaSummary",
                "/upstream/v1internal",
                status,
            )
            .await;
        }
    }
}

#[tokio::test]
async fn gemini_explicit_base_query_survives_quota_fallback_and_load_without_replacing_userinfo_query()
 {
    for profile in [
        GeminiOAuthProfile::Antigravity,
        GeminiOAuthProfile::GeminiCli,
    ] {
        for (base, prefix) in [
            ("/?tenant=one", "/v1internal"),
            (
                "/upstream/v1internal:retrieveUserQuotaSummary?tenant=one",
                "/upstream/v1internal",
            ),
            (
                "/upstream/v1internal:loadCodeAssist?tenant=one",
                "/upstream/v1internal",
            ),
        ] {
            assert_gemini_explicit_destination_matrix_with_query(
                profile,
                base,
                prefix,
                http::StatusCode::NOT_FOUND,
                false,
                Some("tenant=one"),
            )
            .await;
        }
    }
}

#[tokio::test]
async fn gemini_explicit_override_records_content_quota_metadata_and_warmup_on_one_listener() {
    for profile in [
        GeminiOAuthProfile::Antigravity,
        GeminiOAuthProfile::GeminiCli,
    ] {
        for (base, prefix) in [
            ("", "/v1internal"),
            ("/upstream/v1internal", "/upstream/v1internal"),
            (
                "/upstream/v1internal:generateContent",
                "/upstream/v1internal",
            ),
            (
                "/upstream/v1internal:streamGenerateContent",
                "/upstream/v1internal",
            ),
            ("/upstream/v1internal:countTokens", "/upstream/v1internal"),
            (
                "/upstream/v1internal:loadCodeAssist",
                "/upstream/v1internal",
            ),
            (
                "/upstream/v1internal:retrieveUserQuotaSummary",
                "/upstream/v1internal",
            ),
        ] {
            assert_gemini_explicit_destination_matrix(
                profile,
                base,
                prefix,
                http::StatusCode::OK,
                true,
            )
            .await;
        }
    }
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
