use crate::admin_test_common;

use async_trait::async_trait;
use axum::{
    body::Body,
    http::{HeaderMap, Request, StatusCode},
};
use cc_lb_admin::{
    AdminState,
    auth::{AdminActorKind, AdminAuthProvider, AdminAuthenticator, AdminIdentity, ProviderOutcome},
    router,
};
use cc_lb_config::Config;
use std::sync::Arc;
use tower::ServiceExt;

fn test_state() -> AdminState {
    test_state_with_auth(admin_test_common::static_token_auth("test-token"))
}

fn test_state_with_auth(admin_auth: Arc<AdminAuthenticator>) -> AdminState {
    let config = Config::default();
    AdminState {
        storage: None,
        key_store: None,
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: admin_test_common::limit_engine(),
        lifecycle: None,
        dynamic_view: admin_test_common::dynamic_view_holder(&config),
        config: Arc::new(Config::default()),
        scheduler: None,
        admin_auth,
        lazy_refresher: None,
        runtime: None,
        data_dir: None,
        warmup_dialect_dispatcher: None,
        subscription_metadata_hook: None,
        start_time: std::time::Instant::now(),
        event_bus: None,
        storage_tail: cc_lb_admin::events::storage_tail_channel(),
        clock: Arc::new(cc_lb_clock::SystemClock),
    }
}

#[tokio::test]
async fn test_auth_required() {
    let app = router(test_state());

    let endpoints = vec![
        ("/admin/principals/alice/usage", "GET"),
        ("/admin/principals/alice/limits", "GET"),
        ("/admin/principals/alice/keys/key-1", "GET"),
        ("/admin/principals/alice/keys/key-1/revoke", "POST"),
        ("/admin/principals/alice/keys/key-1/disable", "POST"),
        ("/admin/principals/alice/keys/key-1/enable", "POST"),
        ("/admin/principals/alice/keys/key-1/usage", "GET"),
        ("/admin/audit", "GET"),
        ("/admin/config/current", "GET"),
        ("/admin/config/reload", "POST"),
        ("/admin/scheduler/status", "GET"),
        ("/admin/scheduler/failures", "GET"),
        ("/admin/v1/status", "GET"),
        ("/admin/v1/upstreams", "GET"),
        ("/admin/v1/upstreams", "POST"),
        (
            "/admin/v1/upstreams/00000000-0000-0000-0000-000000000001",
            "GET",
        ),
        (
            "/admin/v1/upstreams/00000000-0000-0000-0000-000000000001/enable",
            "POST",
        ),
        (
            "/admin/v1/upstreams/00000000-0000-0000-0000-000000000001/oauth/start",
            "POST",
        ),
        ("/admin/v1/principals", "GET"),
        ("/admin/v1/principals", "POST"),
        (
            "/admin/v1/principals/00000000-0000-0000-0000-000000000001",
            "GET",
        ),
        (
            "/admin/v1/principals/00000000-0000-0000-0000-000000000001/disable",
            "POST",
        ),
        (
            "/admin/v1/principals/00000000-0000-0000-0000-000000000001/plugin-chain",
            "GET",
        ),
        ("/admin/v1/plugins/registry", "GET"),
        ("/admin/v1/plugins/registry", "POST"),
    ];

    for (path, method) in endpoints {
        let req = Request::builder()
            .method(method)
            .uri(path)
            .body(Body::empty())
            .unwrap();

        let response = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "Endpoint {} should require auth",
            path
        );
    }
}

#[tokio::test]
async fn test_auth_success() {
    let app = router(test_state());

    let req = Request::builder()
        .method("GET")
        .uri("/admin/config/current")
        .header("Authorization", "Bearer test-token")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[derive(Clone, Copy)]
enum FakeProviderMode {
    Always,
    Header,
}

struct FakeVerifiedProvider {
    mode: FakeProviderMode,
}

impl FakeVerifiedProvider {
    fn always() -> Self {
        Self {
            mode: FakeProviderMode::Always,
        }
    }

    fn from_header() -> Self {
        Self {
            mode: FakeProviderMode::Header,
        }
    }

    fn identity(subject: &str) -> AdminIdentity {
        AdminIdentity {
            authority: "https://fake.example".to_owned(),
            subject: subject.to_owned(),
            kind: AdminActorKind::Human,
            provider_id: "fake".to_owned(),
            email: Some(format!("{subject}@example.com")),
            display_name: Some(subject.to_owned()),
            groups: Vec::new(),
            expires_at_unix_secs: None,
        }
    }
}

#[async_trait]
impl AdminAuthProvider for FakeVerifiedProvider {
    fn id(&self) -> &str {
        "fake"
    }

    async fn authenticate(&self, headers: &HeaderMap) -> ProviderOutcome {
        match self.mode {
            FakeProviderMode::Always => ProviderOutcome::Verified(Self::identity("always")),
            FakeProviderMode::Header => headers
                .get("x-test-user")
                .and_then(|value| value.to_str().ok())
                .filter(|subject| !subject.is_empty())
                .map(Self::identity)
                .map(ProviderOutcome::Verified)
                .unwrap_or(ProviderOutcome::NotPresent),
        }
    }
}

#[tokio::test]
async fn authentication_rejection_reports_provider_neutral_auth_mode() {
    let static_server = admin_test_common::spawn_admin_server_with_auth(
        admin_test_common::static_token_auth("test-token"),
    )
    .await;
    let (status, _, body) = static_server
        .client
        .get_without_auth("/admin/v1/auth/session")
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["error"], "unauthorized");
    assert_eq!(body["auth_mode"], "static_token");
    assert!(body.get("provider_id").is_none());

    let mut providers = cc_lb_admin::auth::build_providers(
        &cc_lb_config::AdminAuthConfig::default(),
        Some("test-token".to_owned()),
    )
    .expect("static provider builds");
    providers.push(Arc::new(FakeVerifiedProvider::from_header()));
    let external_server = admin_test_common::spawn_admin_server_with_auth(Arc::new(
        AdminAuthenticator::new(providers),
    ))
    .await;
    let (status, _, body) = external_server
        .client
        .get_without_auth("/admin/v1/auth/session")
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["error"], "unauthorized");
    assert_eq!(body["auth_mode"], "external");
    assert!(body.get("provider_id").is_none());
}

#[tokio::test]
async fn ambiguous_credentials_rejected() {
    let mut providers = cc_lb_admin::auth::build_providers(
        &cc_lb_config::AdminAuthConfig::default(),
        Some("test-token".to_owned()),
    )
    .expect("static provider builds");
    providers.push(Arc::new(FakeVerifiedProvider::always()));
    let app = router(test_state_with_auth(Arc::new(AdminAuthenticator::new(
        providers,
    ))));

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/admin/v1/auth/session")
                .header("Authorization", "Bearer test-token")
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("admin request succeeds");

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn new_provider_plugs_into_audit_without_handler_changes() {
    let admin_auth = Arc::new(AdminAuthenticator::new(vec![Arc::new(
        FakeVerifiedProvider::from_header(),
    )]));
    let server = admin_test_common::spawn_admin_server_with_auth(admin_auth).await;

    let draft =
        serde_json::to_value(cc_lb_config::Config::default()).expect("default config serializes");
    let (status, _, _) = server
        .client
        .json(
            "PUT",
            "/admin/v1/config/draft",
            Some(serde_json::json!({
                "draft": draft,
                "expected_revision": 0,
            })),
            &[("x-test-user", "alice")],
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _, session) = server
        .client
        .json(
            "GET",
            "/admin/v1/auth/session",
            None,
            &[("x-test-user", "alice")],
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(session["authority"], "https://fake.example");
    assert_eq!(session["subject"], "alice");
    assert_eq!(session["kind"], "human");
    assert_eq!(session["auth_mode"], "external");

    let (status, _, audit) = server
        .client
        .json(
            "GET",
            "/admin/v1/audit?actor_authority=https://fake.example&actor_subject=alice",
            None,
            &[("x-test-user", "alice")],
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let config_draft = audit["entries"]
        .as_array()
        .expect("audit entries array")
        .iter()
        .find(|entry| entry["admin_action"] == "config_draft_put")
        .expect("config draft audit entry");
    assert_eq!(config_draft["actor_authority"], "https://fake.example");
    assert_eq!(config_draft["actor_subject"], "alice");
    assert_eq!(config_draft["actor_kind"], "human");
    assert_eq!(config_draft["actor_email"], "alice@example.com");
}

#[test]
fn invalid_provider_header_returns_build_error() {
    let result = cc_lb_admin::auth::build_providers(
        &cc_lb_config::AdminAuthConfig {
            providers: vec![cc_lb_config::AdminAuthProviderConfig::CloudflareAccess {
                id: "cf".to_owned(),
                team_domain: "https://team.cloudflareaccess.com".to_owned(),
                audiences: vec!["admin".to_owned()],
                header: "Cf Access Jwt".to_owned(),
            }],
        },
        None,
    );

    assert!(matches!(
        result,
        Err(cc_lb_admin::auth::AdminAuthBuildError::InvalidHeader {
            provider_id,
            header,
        }) if provider_id == "cf" && header == "Cf Access Jwt"
    ));
}
