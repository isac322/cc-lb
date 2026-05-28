use std::{sync::Arc, time::Duration};

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_config::{Config, Limit, LimitKind, PrincipalSpec, PrincipalType};
use cc_lb_core::api_keys::principal_view::PrincipalView;
use cc_lb_storage_redb::Storage;
use serde_json::{Value, json};
use tower::ServiceExt;

fn test_state(storage: Arc<Storage>) -> AdminState {
    let config = test_config();
    AdminState {
        storage: Some(storage),
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: cc_lb_core::api_keys::limit_engine::LimitEngine::new(
            Arc::new(cc_lb_core::api_keys::concurrent_guard::KeyConcurrencyManager::new()),
            Arc::new(
                arc_swap::ArcSwap::from(
                    cc_lb_core::api_keys::principal_view::PrincipalView::from_config(
                        &Config::default(),
                    )
                    .expect("principal view builds"),
                ),
            ),
        ),
        lifecycle: None,
        audit_sink: None,
        principal_view: Arc::new(arc_swap::ArcSwap::from(
            PrincipalView::from_config(&config).expect("principal view builds"),
        )),
        config: Arc::new(config),
        admin_token: Some("test-token".to_owned()),
        start_time: std::time::Instant::now(),
    }
}

fn test_config() -> Config {
    let mut config = Config::default();
    config.principals.insert(
        "u1".to_owned(),
        PrincipalSpec {
            principal_type: PrincipalType::Machine,
            default_limits: vec![Limit {
                kind: LimitKind::Requests,
                window: Duration::from_secs(60),
                cap_micros: 100,
            }],
            enabled: true,
            allowed_models: vec!["claude-3.5-sonnet".to_owned()],
            credentials_ref: None,
        },
    );
    config
}

fn new_store() -> (tempfile::TempDir, Arc<Storage>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("disable_enable_key.redb");
    let storage = Storage::open(&path, [31; 32]).unwrap();
    (dir, Arc::new(storage))
}

async fn send_json(
    app: axum::Router,
    method: &str,
    path: &str,
    body: Value,
) -> axum::http::Response<Body> {
    let req = Request::builder()
        .method(method)
        .uri(path)
        .header("Authorization", "Bearer test-token")
        .header("Content-Type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    app.oneshot(req).await.unwrap()
}

async fn response_json(response: axum::http::Response<Body>) -> Value {
    serde_json::from_slice(
        &http_body_util::BodyExt::collect(response.into_body())
            .await
            .unwrap()
            .to_bytes(),
    )
    .unwrap()
}

async fn response_text(response: axum::http::Response<Body>) -> String {
    String::from_utf8(
        http_body_util::BodyExt::collect(response.into_body())
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap()
}

async fn issue_key(app: axum::Router) -> Value {
    let response = send_json(
        app,
        "POST",
        "/admin/principals/u1/keys",
        json!({
            "label": "managed key",
            "upstream_kind": "anthropic_key",
            "upstream_credential_ref": "anthropic-prod",
        }),
    )
    .await;

    assert_eq!(response.status(), StatusCode::CREATED);
    response_json(response).await
}

async fn list_keys(app: axum::Router) -> Value {
    let request = Request::builder()
        .method("GET")
        .uri("/admin/principals/u1/keys")
        .header("Authorization", "Bearer test-token")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    response_json(response).await
}

async fn post_transition(
    app: axum::Router,
    key_id: &str,
    transition: &str,
) -> axum::http::Response<Body> {
    let request = Request::builder()
        .method("POST")
        .uri(format!("/admin/principals/u1/keys/{key_id}/{transition}"))
        .header("Authorization", "Bearer test-token")
        .body(Body::empty())
        .unwrap();
    app.oneshot(request).await.unwrap()
}

#[tokio::test]
async fn disable_then_list_shows_disabled() {
    let (_dir, storage) = new_store();
    let app = router(test_state(storage));
    let issued = issue_key(app.clone()).await;
    let key_id = issued["key_id"].as_str().unwrap().to_owned();

    let response = post_transition(app.clone(), &key_id, "disable").await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = list_keys(app).await;
    assert!(
        body["keys"].as_array().unwrap().iter().any(
            |record| record["key_id"] == json!(key_id) && record["status"] == json!("disabled")
        )
    );
}

#[tokio::test]
async fn disable_then_enable_restores_active() {
    let (_dir, storage) = new_store();
    let app = router(test_state(storage));
    let issued = issue_key(app.clone()).await;
    let key_id = issued["key_id"].as_str().unwrap().to_owned();

    let disable = post_transition(app.clone(), &key_id, "disable").await;
    assert_eq!(disable.status(), StatusCode::OK);

    let enable = post_transition(app.clone(), &key_id, "enable").await;
    assert_eq!(enable.status(), StatusCode::OK);

    let body = list_keys(app).await;
    assert!(
        body["keys"]
            .as_array()
            .unwrap()
            .iter()
            .any(|record| record["key_id"] == json!(key_id) && record["status"] == json!("active"))
    );
}

#[tokio::test]
async fn enable_revoked_returns_409() {
    let (_dir, storage) = new_store();
    let app = router(test_state(storage));
    let issued = issue_key(app.clone()).await;
    let key_id = issued["key_id"].as_str().unwrap().to_owned();

    let revoke = post_transition(app.clone(), &key_id, "revoke").await;
    assert_eq!(revoke.status(), StatusCode::OK);

    let enable = post_transition(app.clone(), &key_id, "enable").await;
    assert_eq!(enable.status(), StatusCode::CONFLICT);
    assert!(response_text(enable).await.contains("revoked"));
}

#[tokio::test]
async fn disable_on_already_disabled_idempotent() {
    let (_dir, storage) = new_store();
    let app = router(test_state(storage));
    let issued = issue_key(app.clone()).await;
    let key_id = issued["key_id"].as_str().unwrap().to_owned();

    let first = post_transition(app.clone(), &key_id, "disable").await;
    assert_eq!(first.status(), StatusCode::OK);

    let second = post_transition(app.clone(), &key_id, "disable").await;
    assert_eq!(second.status(), StatusCode::OK);

    let body = list_keys(app).await;
    assert!(
        body["keys"].as_array().unwrap().iter().any(
            |record| record["key_id"] == json!(key_id) && record["status"] == json!("disabled")
        )
    );
}

#[tokio::test]
async fn disable_missing_key_returns_404() {
    let (_dir, storage) = new_store();
    let app = router(test_state(storage));

    let response = post_transition(app, "missing-key", "disable").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn enable_missing_key_returns_404() {
    let (_dir, storage) = new_store();
    let app = router(test_state(storage));

    let response = post_transition(app, "missing-key", "enable").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
