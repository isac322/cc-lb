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
use std::time::{Duration, Instant};
use tower::ServiceExt;

fn test_state() -> AdminState {
    test_state_with_auth(admin_test_common::static_token_auth("test-token"))
}

fn test_state_with_auth(admin_auth: Arc<AdminAuthenticator>) -> AdminState {
    let config = Config::default();
    AdminState {
        config_path: None,
        build: admin_test_common::test_build_info(),
        startup_config_overrides: Default::default(),
        storage: None,
        key_store: None,
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        lifecycle: None,
        dynamic_view: admin_test_common::dynamic_view_holder(&config),
        config: Arc::new(config),
        dynamic_view_rebinder: None,
        scheduler: None,
        admin_auth,
        lazy_refresher: None,
        runtime: None,
        data_dir: None,
        warmup_dialect_dispatcher: None,
        warmup_window_verifier: None,
        limit_reset_usage_refresh: None,
        start_time: std::time::Instant::now(),
        event_bus: None,
        storage_tail: cc_lb_admin::events::storage_tail_channel(),
        clock: Arc::new(cc_lb_clock::SystemClock),
        anthropic_org_api: cc_lb_control::anthropic_api_key::AnthropicOrgApi::production()
            .expect("production Anthropic origin parses"),
        post_commit_tracker: Default::default(),
    }
}

#[tokio::test]
async fn test_auth_required() {
    let app = router(test_state());

    let endpoints = vec![
        ("/admin/v1/principals/alice/keys", "GET"),
        ("/admin/v1/principals/alice/keys", "POST"),
        ("/admin/v1/principals/alice/keys/key-1/revoke", "POST"),
        ("/admin/v1/audit", "GET"),
        ("/admin/v1/dashboard/summary?range=1h", "GET"),
        ("/admin/v1/dashboard/usage?range=1h&group_by=model", "GET"),
        ("/admin/v1/config/editor", "GET"),
        ("/admin/v1/config/draft", "PUT"),
        ("/admin/v1/config/draft/validate", "POST"),
        ("/admin/v1/config/save", "POST"),
        ("/admin/v1/config/draft/download", "POST"),
        ("/admin/v1/config/history", "GET"),
        ("/admin/v1/status", "GET"),
        ("/admin/v1/upstreams", "GET"),
        ("/admin/v1/upstreams", "POST"),
        (
            "/admin/v1/upstreams/00000000-0000-0000-0000-000000000001",
            "GET",
        ),
        (
            "/admin/v1/upstreams/00000000-0000-0000-0000-000000000001",
            "PATCH",
        ),
        (
            "/admin/v1/upstreams/00000000-0000-0000-0000-000000000001/oauth/start",
            "POST",
        ),
        ("/admin/v1/principals", "GET"),
        ("/admin/v1/principals", "POST"),
        (
            "/admin/v1/principals/00000000-0000-0000-0000-000000000001/cache-keepalive/detail?id=key",
            "GET",
        ),
        ("/admin/v1/events/detail?id=event", "GET"),
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

    let mut set = tokio::task::JoinSet::new();
    for (path, method) in endpoints {
        let app = app.clone();
        for token in [None, Some("Bearer wrong-token")] {
            let app = app.clone();
            set.spawn(async move {
                let mut req = Request::builder().method(method).uri(path);
                if let Some(token) = token {
                    req = req.header("Authorization", token);
                }
                let req = req.body(Body::empty()).unwrap();
                (path, app.oneshot(req).await.unwrap())
            });
        }
    }
    while let Some(res) = set.join_next().await {
        let (path, response) = res.unwrap();
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
    let dir = tempfile::tempdir().unwrap();
    let storage = admin_test_common::sqlite_storage(dir.path(), "auth-success.sqlite").await;
    let mut state = test_state();
    state.storage = Some(storage);
    let app = router(state);

    let req = Request::builder()
        .method("GET")
        .uri("/admin/v1/config/editor")
        .header("Authorization", "Bearer test-token")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn admin_401_sleeps_100ms() {
    let app = router(test_state());

    let req = Request::builder()
        .method("GET")
        .uri("/admin/v1/config/editor")
        .header("Authorization", "Bearer wrong-token")
        .body(Body::empty())
        .unwrap();

    let started = Instant::now();
    let response = app.oneshot(req).await.unwrap();
    let elapsed = started.elapsed();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(elapsed >= Duration::from_millis(100));
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

    let mut providers = vec![admin_test_common::static_token_provider("test-token")];
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
    let mut providers = vec![admin_test_common::static_token_provider("test-token")];
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
fn empty_provider_config_stays_unconfigured() {
    let providers =
        cc_lb_admin::auth::build_providers(&cc_lb_config::AdminAuthConfig::default()).unwrap();

    assert!(providers.is_empty());
}

#[test]
fn invalid_provider_header_returns_build_error() {
    let result = cc_lb_admin::auth::build_providers(&cc_lb_config::AdminAuthConfig {
        providers: vec![cc_lb_config::AdminAuthProviderConfig::CloudflareAccess {
            id: "cf".to_owned(),
            team_domain: "https://team.cloudflareaccess.com".to_owned(),
            audiences: vec!["admin".to_owned()],
            header: "Cf Access Jwt".to_owned(),
        }],
    });

    assert!(matches!(
        result,
        Err(cc_lb_admin::auth::AdminAuthBuildError::InvalidHeader {
            provider_id,
            header,
        }) if provider_id == "cf" && header == "Cf Access Jwt"
    ));
}

#[cfg(not(feature = "embed-spa"))]
#[tokio::test]
async fn api_only_mode_omits_spa_routes_and_keeps_health_and_auth() {
    let app = router(test_state());

    let root_res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(root_res.status(), StatusCode::NOT_FOUND);

    let fallback_res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(fallback_res.status(), StatusCode::NOT_FOUND);

    let health_res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/admin/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(health_res.status(), StatusCode::OK);

    let unauth_res = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/admin/v1/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauth_res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn unauthenticated_401_flood_bounds_audit_writes_and_sheds_excess_with_429() {
    let dir = tempfile::tempdir().unwrap();
    let storage = admin_test_common::sqlite_storage(dir.path(), "auth-flood.sqlite").await;
    let mut state = test_state();
    state.storage = Some(storage);
    let app = router(state);

    // Send 80 concurrent unauthenticated requests: sleep semaphore (64) bounds concurrent
    // sleeps and sheds excess immediately with 429 Retry-After: 1, while audit writes stay
    // capped at the 10-token burst (no 80-row amplification).
    let mut set = tokio::task::JoinSet::new();
    for _ in 0..80 {
        let app = app.clone();
        set.spawn(async move {
            let req = Request::builder()
                .method("GET")
                .uri("/admin/v1/status")
                .header("Authorization", "Bearer wrong-token")
                .body(Body::empty())
                .unwrap();
            app.oneshot(req).await.unwrap()
        });
    }

    let mut unauthorized_count = 0usize;
    let mut throttled_count = 0usize;
    while let Some(res) = set.join_next().await {
        let response = res.unwrap();
        match response.status() {
            StatusCode::UNAUTHORIZED => unauthorized_count += 1,
            StatusCode::TOO_MANY_REQUESTS => {
                assert_eq!(
                    response
                        .headers()
                        .get("retry-after")
                        .and_then(|v| v.to_str().ok()),
                    Some("1")
                );
                throttled_count += 1;
            }
            other => panic!("unexpected status during 401 flood: {other}"),
        }
    }
    assert!(unauthorized_count <= 64 && unauthorized_count > 0);
    assert_eq!(unauthorized_count + throttled_count, 80);
    assert!(throttled_count >= 16);

    // Query audit table via authenticated admin endpoint and verify audit rows are bounded.
    let req = Request::builder()
        .method("GET")
        .uri("/admin/v1/audit?limit=100")
        .header("Authorization", "Bearer test-token")
        .body(Body::empty())
        .unwrap();
    let audit_res = app.oneshot(req).await.unwrap();
    assert_eq!(audit_res.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(audit_res.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let auth_rejected_rows = body["entries"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| entry["admin_action"] == "auth_rejected")
        .count();
    assert!(
        auth_rejected_rows <= 12,
        "expected audit burst <= ~10 rows during 80-req 401 flood, saw {auth_rejected_rows}"
    );
}
