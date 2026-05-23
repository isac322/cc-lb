mod config_admin_common;

use axum::http::StatusCode;
use cc_lb_config::{Config, PrincipalSpec};
use config_admin_common::{app, authed_json, temp_storage, test_state, unauthenticated_status};
use serde_json::json;

#[tokio::test]
async fn create_new_principal_bumps_draft_revision() {
    let (_dir, storage) = temp_storage();
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, _, body, _) = authed_json(
        app,
        "POST",
        "/admin/principals",
        Some(json!({
            "id": "alice",
            "spec": { "allowed_models": ["claude-3-5-sonnet"] }
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["revision"], 1);
    assert_eq!(body["principal_id"], "alice");
}

#[tokio::test]
async fn create_duplicate_returns_conflict() {
    let (_dir, storage) = temp_storage();
    let app = app(test_state(Config::default(), Some(storage)));
    let body = json!({ "id": "alice", "spec": { "allowed_models": [] } });

    let (status, _, _, _) =
        authed_json(app.clone(), "POST", "/admin/principals", Some(body.clone())).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, body, _) = authed_json(app, "POST", "/admin/principals", Some(body)).await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "principal_exists");
}

#[tokio::test]
async fn update_existing_principal_bumps_draft_revision() {
    let (_dir, storage) = temp_storage();
    let app = app(test_state(config_with_principal("alice"), Some(storage)));

    let (status, _, body, _) = authed_json(
        app,
        "PUT",
        "/admin/principals/alice",
        Some(json!({ "spec": { "allowed_models": ["claude-opus"] } })),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["revision"], 1);
    assert_eq!(body["principal_id"], "alice");
}

#[tokio::test]
async fn update_unknown_principal_returns_not_found() {
    let (_dir, storage) = temp_storage();
    let app = app(test_state(Config::default(), Some(storage)));

    let (status, _, body, _) = authed_json(
        app,
        "PUT",
        "/admin/principals/missing",
        Some(json!({ "spec": { "allowed_models": [] } })),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "unknown_principal");
}

#[tokio::test]
async fn disable_sets_disabled_field_in_draft() {
    let (_dir, storage) = temp_storage();
    let app = app(test_state(config_with_principal("alice"), Some(storage)));

    let (status, _, body, _) =
        authed_json(app.clone(), "POST", "/admin/principals/alice/disable", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["revision"], 1);

    let (_, _, draft, _) = authed_json(app, "GET", "/admin/config/draft", None).await;
    assert_eq!(draft["draft"]["principals"]["alice"]["disabled"], true);
}

#[tokio::test]
async fn enable_after_disable_flips_disabled_field() {
    let (_dir, storage) = temp_storage();
    let app = app(test_state(config_with_principal("alice"), Some(storage)));

    let _ = authed_json(app.clone(), "POST", "/admin/principals/alice/disable", None).await;
    let (status, _, body, _) =
        authed_json(app.clone(), "POST", "/admin/principals/alice/enable", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["revision"], 2);

    let (_, _, draft, _) = authed_json(app, "GET", "/admin/config/draft", None).await;
    assert_eq!(draft["draft"]["principals"]["alice"]["disabled"], false);
}

#[tokio::test]
async fn allowed_models_update_is_reflected_in_draft() {
    let (_dir, storage) = temp_storage();
    let app = app(test_state(config_with_principal("alice"), Some(storage)));

    let (status, _, body, _) = authed_json(
        app.clone(),
        "PUT",
        "/admin/principals/alice/allowed_models",
        Some(json!({ "allowed_models": ["claude-sonnet", "claude-haiku"] })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["revision"], 1);

    let (_, _, draft, _) = authed_json(app, "GET", "/admin/config/draft", None).await;
    assert_eq!(
        draft["draft"]["principals"]["alice"]["allowed_models"],
        json!(["claude-sonnet", "claude-haiku"])
    );
}

#[tokio::test]
async fn principal_create_requires_admin_auth() {
    let (_dir, storage) = temp_storage();
    let app = app(test_state(Config::default(), Some(storage)));

    let status = unauthenticated_status(app, "POST", "/admin/principals").await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

fn config_with_principal(principal_id: &str) -> Config {
    let mut config = config_admin_common::minimal_config();
    config.principals.insert(
        principal_id.to_owned(),
        PrincipalSpec {
            allowed_models: vec!["claude-*".to_owned()],
            ..PrincipalSpec::default()
        },
    );
    config
}
