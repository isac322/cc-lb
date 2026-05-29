mod admin_test_common;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_config::Config;
use cc_lb_core::api_keys::key_store::{CreateParams, KeyStore};
use cc_lb_storage_api::types::{PrincipalKindLite, UpstreamKind};
use cc_lb_storage_redb::{OAuthCredentials, RedbManagedKeyStore, Storage};
use http_body_util::BodyExt;
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tower::ServiceExt;

fn test_state(storage: Arc<Storage>) -> AdminState {
    let config = Config::default();
    AdminState {
        storage: Some(storage.clone()),
        key_store: Some(Arc::new(KeyStore::new(Arc::new(RedbManagedKeyStore::new(
            storage.clone(),
        ))))),
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: admin_test_common::limit_engine(),
        lifecycle: None,
        audit_sink: None,
        dynamic_view: admin_test_common::dynamic_view_holder(&config),
        config: Arc::new(Config::default()),
        admin_token: Some("test-token".to_string()),
        start_time: Instant::now(),
    }
}

fn new_storage() -> (tempfile::TempDir, Arc<Storage>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("oauth_no_authn_fallback.redb");
    let storage = Arc::new(Storage::open(&path, [7u8; 32]).unwrap());
    (dir, storage)
}

fn managed_key_params(oauth_credential_id: &str) -> CreateParams {
    CreateParams {
        upstream_kind: UpstreamKind::AnthropicOAuth,
        upstream_credential_ref: oauth_credential_id.to_string(),
        label: "managed oauth key".to_string(),
        description: Some("managed oauth key".to_string()),
        expires_at_unix_secs: None,
        limit_overrides: vec![],
        principal_kind: PrincipalKindLite::Machine,
    }
}

fn oauth_credentials() -> OAuthCredentials {
    OAuthCredentials {
        access_token: "access-token".to_string(),
        refresh_token: "refresh-token".to_string(),
        expires_at: 1_800_000_000,
        scopes: vec!["scope".to_string()],
    }
}

async fn get_json(app: axum::Router, path: &str) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("GET")
        .uri(path)
        .header("Authorization", "Bearer test-token")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body).unwrap()
    };
    (status, json)
}

#[tokio::test]
async fn oauth_status_derives_from_managed_key() {
    let (_dir, storage) = new_storage();
    storage
        .put_oauth("alice", "anthropic_oauth", &oauth_credentials())
        .unwrap();
    let key_store = KeyStore::new(Arc::new(RedbManagedKeyStore::new(storage.clone())));
    key_store
        .create("alice", managed_key_params("anthropic_oauth"))
        .await
        .unwrap();

    let app = router(test_state(storage));
    let (status, json) = get_json(app, "/admin/oauth/anthropic_oauth").await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(json, Value::Null);
}

#[tokio::test]
async fn oauth_status_no_managed_key_returns_not_enrolled() {
    let (_dir, storage) = new_storage();
    storage
        .put_oauth("alice", "anthropic_oauth", &oauth_credentials())
        .unwrap();

    let app = router(test_state(storage));
    let (status, json) = get_json(app, "/admin/oauth/anthropic_oauth").await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(json, Value::Null);
}

#[tokio::test]
async fn admin_401_sleeps_100ms() {
    let (_dir, storage) = new_storage();
    let app = router(test_state(storage));

    let req = Request::builder()
        .method("GET")
        .uri("/admin/config/current")
        .header("Authorization", "Bearer wrong-token")
        .body(Body::empty())
        .unwrap();

    let started = Instant::now();
    let response = app.oneshot(req).await.unwrap();
    let elapsed = started.elapsed();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(elapsed >= Duration::from_millis(100));
}
