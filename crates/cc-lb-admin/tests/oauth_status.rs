use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_config::{Config, PrincipalSpec};
use cc_lb_core::DashboardBroadcaster;
use cc_lb_storage_redb::{OAuthCredentials, Storage};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

const PROVIDER: &str = "anthropic_oauth";
const LEAK_MARKER: &str = "LEAK-TOKEN-DO-NOT-RETURN";

#[tokio::test]
async fn storage_missing_returns_unobserved_empty_credentials() {
    let app = router(test_state(oauth_config("alice"), None));
    let (status, json, _) = authorized_json(app, "/admin/oauth/status").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["credentials"].as_array().unwrap().len(), 0);
    assert_eq!(json["observed"], false);
}

#[tokio::test]
async fn valid_credential_reports_valid_status_and_refresh_presence() {
    let (_dir, storage) = storage_with_credential(
        "alice",
        OAuthCredentials {
            access_token: "access-token".to_string(),
            refresh_token: "refresh-token".to_string(),
            expires_at: unix_now_secs() + 3_600,
            scopes: vec!["scope-a".to_string()],
        },
    );
    let app = router(test_state(oauth_config("alice"), Some(storage)));
    let (status, json, _) = authorized_json(app, "/admin/oauth/status").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["observed"], true);
    let credential = only_credential(&json);
    assert_eq!(credential["principal_id"], "alice");
    assert_eq!(credential["provider"], PROVIDER);
    assert_eq!(credential["has_credentials"], true);
    assert_eq!(credential["refresh_token_present"], true);
    assert_eq!(credential["status"], "valid");
    assert_eq!(credential["scopes"], serde_json::json!(["scope-a"]));
    assert_eq!(credential["last_updated_unix_secs"], Value::Null);
}

#[tokio::test]
async fn expired_credential_reports_expired_status() {
    let (_dir, storage) = storage_with_credential(
        "alice",
        OAuthCredentials {
            access_token: "access-token".to_string(),
            refresh_token: String::new(),
            expires_at: unix_now_secs().saturating_sub(1),
            scopes: Vec::new(),
        },
    );
    let app = router(test_state(oauth_config("alice"), Some(storage)));
    let (status, json, _) = authorized_json(app, "/admin/oauth/status").await;

    assert_eq!(status, StatusCode::OK);
    let credential = only_credential(&json);
    assert_eq!(credential["status"], "expired");
    assert_eq!(credential["refresh_token_present"], false);
}

#[tokio::test]
async fn soon_expiring_credential_reports_expiring_soon_status() {
    let (_dir, storage) = storage_with_credential(
        "alice",
        OAuthCredentials {
            access_token: "access-token".to_string(),
            refresh_token: "refresh-token".to_string(),
            expires_at: unix_now_secs() + 60,
            scopes: Vec::new(),
        },
    );
    let app = router(test_state(oauth_config("alice"), Some(storage)));
    let (status, json, _) = authorized_json(app, "/admin/oauth/status").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(only_credential(&json)["status"], "expiring_soon");
}

#[tokio::test]
async fn oauth_status_requires_admin_auth() {
    let app = router(test_state(oauth_config("alice"), None));
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/admin/oauth/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn oauth_status_response_excludes_raw_token_material() {
    let (_dir, storage) = storage_with_credential(
        "alice",
        OAuthCredentials {
            access_token: format!("sk-ant-{LEAK_MARKER}"),
            refresh_token: format!("refresh_token={LEAK_MARKER}"),
            expires_at: unix_now_secs() + 3_600,
            scopes: vec!["safe-scope".to_string()],
        },
    );
    let app = router(test_state(oauth_config("alice"), Some(storage)));
    let (status, _, body) = authorized_json(app, "/admin/oauth/status").await;

    assert_eq!(status, StatusCode::OK);
    for forbidden in forbidden_terms() {
        assert!(
            !body
                .windows(forbidden.len())
                .any(|window| window == forbidden.as_bytes()),
            "forbidden term present: {forbidden}"
        );
    }
}

fn test_state(config: Config, storage: Option<Arc<Storage>>) -> AdminState {
    AdminState {
        storage,
        quota_manager: None,
        lifecycle: None,
        breaker_registry: None,
        drain_controller: None,
        bulkhead_registry: None,
        plugin_runtime_status: None,
        dashboard_broadcaster: Arc::new(DashboardBroadcaster::new()),
        config: Arc::new(config),
        config_path: None,
        config_watcher: None,
        config_started_at_unix_secs: 0,
        admin_token: Some("test-token".to_string()),
        start_time: std::time::Instant::now(),
    }
}

fn oauth_config(principal_id: &str) -> Config {
    let mut config = Config::default();
    config.principals.insert(
        principal_id.to_string(),
        PrincipalSpec {
            credentials_ref: Some(PROVIDER.to_string()),
            ..PrincipalSpec::default()
        },
    );
    config
}

fn storage_with_credential(
    principal_id: &str,
    creds: OAuthCredentials,
) -> (tempfile::TempDir, Arc<Storage>) {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(&dir.path().join("oauth.redb"), [12; 32]).unwrap();
    storage.put_oauth(principal_id, PROVIDER, &creds).unwrap();
    (dir, Arc::new(storage))
}

async fn authorized_json(app: axum::Router, uri: &str) -> (StatusCode, Value, Vec<u8>) {
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .header("Authorization", "Bearer test-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&body).unwrap();
    (status, json, body.to_vec())
}

fn only_credential(json: &Value) -> &Value {
    let credentials = json["credentials"].as_array().unwrap();
    assert_eq!(credentials.len(), 1);
    &credentials[0]
}

fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn forbidden_terms() -> Vec<String> {
    vec![
        LEAK_MARKER.to_string(),
        ["sk", "-ant"].concat(),
        ["Bearer", " "].concat(),
        ["refresh", "_token="].concat(),
    ]
}
