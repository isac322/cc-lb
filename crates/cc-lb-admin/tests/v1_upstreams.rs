//! Upstream CRUD admin endpoint smoke tests (list, create, get, update, delete, enable/disable).

use crate::admin_test_common;

use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_config::Config;
use cc_lb_storage_api::AuditStore;
use cc_lb_storage_sqlite::SqliteStorage as Storage;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

struct TestResponse {
    status: StatusCode,
    headers: axum::http::HeaderMap,
    body: Option<Value>,
}

fn test_state(storage: Arc<Storage>) -> AdminState {
    let config = Config::default();
    AdminState {
        storage: Some(storage.clone()),
        key_store: Some(admin_test_common::key_store(storage)),
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: admin_test_common::limit_engine(),
        lifecycle: None,
        dynamic_view: admin_test_common::dynamic_view_holder(&config),
        config: Arc::new(config),
        scheduler: None,
        admin_auth: crate::admin_test_common::static_token_auth("test-token"),
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

async fn new_store() -> (tempfile::TempDir, Arc<Storage>) {
    let dir = tempfile::tempdir().unwrap();
    let storage = admin_test_common::sqlite_storage(dir.path(), "v1_upstreams.sqlite").await;
    (dir, storage)
}

async fn request(
    app: axum::Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
    if_match: Option<&str>,
) -> TestResponse {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, "Bearer test-token");
    if body.is_some() {
        builder = builder.header(header::CONTENT_TYPE, "application/json");
    }
    if let Some(if_match) = if_match {
        builder = builder.header(header::IF_MATCH, if_match);
    }
    let request = builder
        .body(match body {
            Some(body) => Body::from(body.to_string()),
            None => Body::empty(),
        })
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body = if body_bytes.is_empty() {
        None
    } else {
        Some(serde_json::from_slice(&body_bytes).unwrap())
    };
    TestResponse {
        status,
        headers,
        body,
    }
}

async fn create(app: axum::Router, name: &str) -> TestResponse {
    request(
        app,
        "POST",
        "/admin/v1/upstreams",
        Some(json!({ "name": name, "kind": "anthropic_oauth" })),
        None,
    )
    .await
}

fn body(response: &TestResponse) -> &Value {
    response.body.as_ref().unwrap()
}

#[tokio::test]
async fn create_returns_201_with_body_and_location_header() {
    let (_dir, storage) = new_store().await;
    let app = router(test_state(storage));

    let response = create(app, "primary").await;

    assert_eq!(response.status, StatusCode::CREATED);
    assert_eq!(body(&response)["name"], "primary");
    assert_eq!(body(&response)["kind"], "anthropic_oauth");
    assert_eq!(body(&response)["enabled"], true);
    assert_eq!(body(&response)["spec_revision"], 1);
    assert!(body(&response)["base_url"].is_null());
    assert!(body(&response)["revision"].is_null());
    assert!(body(&response)["status"].is_object());
    let id = body(&response)["id"].as_str().unwrap();
    assert_eq!(
        response.headers.get(header::LOCATION).unwrap(),
        &format!("/admin/v1/upstreams/{id}")
    );
}

#[tokio::test]
async fn create_with_invalid_name_returns_structured_bad_request() {
    let (_dir, storage) = new_store().await;
    let app = router(test_state(storage));

    let response = request(
        app,
        "POST",
        "/admin/v1/upstreams",
        Some(json!({
            "name": "system.blocked",
            "kind": "anthropic_api_key",
            "api_key_value": "sk-ant-api03-test"
        })),
        None,
    )
    .await;

    assert_eq!(response.status, StatusCode::BAD_REQUEST);
    assert_eq!(body(&response)["error"], "invalid_input");
    assert_eq!(body(&response)["field"], "upstream.name");
    assert!(
        body(&response)["reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("system"))
    );
}

#[tokio::test]
async fn get_after_create_returns_etag_with_revision() {
    let (_dir, storage) = new_store().await;
    let app = router(test_state(storage));
    let created = create(app.clone(), "primary").await;
    let id = body(&created)["id"].as_str().unwrap();

    let response = request(app, "GET", &format!("/admin/v1/upstreams/{id}"), None, None).await;

    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(body(&response)["id"], id);
    assert_eq!(response.headers.get(header::ETAG).unwrap(), "W/\"1\"");
}

#[tokio::test]
async fn base_url_roundtrips_across_create_list_get_and_update_without_exposing_secret() {
    let (_dir, storage) = new_store().await;
    let app = router(test_state(storage));
    let secret = "sk-ant-api03-base-url-roundtrip";
    let created = request(
        app.clone(),
        "POST",
        "/admin/v1/upstreams",
        Some(json!({
            "name": "base-url-roundtrip",
            "kind": "anthropic_api_key",
            "base_url": "https://gateway.example.com/v1/",
            "api_key_value": secret
        })),
        None,
    )
    .await;

    assert_eq!(created.status, StatusCode::CREATED);
    assert_eq!(
        body(&created)["base_url"],
        "https://gateway.example.com/v1/"
    );
    assert!(!body(&created).to_string().contains(secret));
    assert!(body(&created).get("api_key_value").is_none());
    assert!(body(&created).get("api_key_env").is_none());
    let id = body(&created)["id"].as_str().unwrap();

    let detail = request(
        app.clone(),
        "GET",
        &format!("/admin/v1/upstreams/{id}"),
        None,
        None,
    )
    .await;
    assert_eq!(detail.status, StatusCode::OK);
    assert_eq!(body(&detail)["base_url"], "https://gateway.example.com/v1/");
    assert!(!body(&detail).to_string().contains(secret));

    let list = request(app.clone(), "GET", "/admin/v1/upstreams", None, None).await;
    assert_eq!(list.status, StatusCode::OK);
    let listed = body(&list)["upstreams"]
        .as_array()
        .unwrap()
        .iter()
        .find(|upstream| upstream["id"] == id)
        .unwrap();
    assert_eq!(listed["base_url"], "https://gateway.example.com/v1/");
    assert!(!listed.to_string().contains(secret));

    let updated = request(
        app.clone(),
        "PUT",
        &format!("/admin/v1/upstreams/{id}"),
        Some(json!({ "base_url": "https://replacement.example.com/api/" })),
        Some("W/\"1\""),
    )
    .await;
    assert_eq!(updated.status, StatusCode::OK);
    assert_eq!(
        body(&updated)["base_url"],
        "https://replacement.example.com/api/"
    );
    assert!(!body(&updated).to_string().contains(secret));

    let reloaded = request(
        app.clone(),
        "GET",
        &format!("/admin/v1/upstreams/{id}"),
        None,
        None,
    )
    .await;
    assert_eq!(reloaded.status, StatusCode::OK);
    assert_eq!(
        body(&reloaded)["base_url"],
        "https://replacement.example.com/api/"
    );
    assert!(!body(&reloaded).to_string().contains(secret));

    let reloaded_list = request(app.clone(), "GET", "/admin/v1/upstreams", None, None).await;
    assert_eq!(reloaded_list.status, StatusCode::OK);
    let reloaded_listed = body(&reloaded_list)["upstreams"]
        .as_array()
        .unwrap()
        .iter()
        .find(|upstream| upstream["id"] == id)
        .unwrap();
    assert_eq!(
        reloaded_listed["base_url"],
        "https://replacement.example.com/api/"
    );
    assert!(!reloaded_listed.to_string().contains(secret));

    let renamed = request(
        app.clone(),
        "PATCH",
        &format!("/admin/v1/upstreams/{id}"),
        Some(json!({ "name": "base-url-renamed" })),
        Some("W/\"2\""),
    )
    .await;
    assert_eq!(renamed.status, StatusCode::OK);
    assert_eq!(
        body(&renamed)["base_url"],
        "https://replacement.example.com/api/"
    );

    // A stale precondition still conflicts even when the body only
    // preserves the override.
    let stale_preserve = request(
        app.clone(),
        "PUT",
        &format!("/admin/v1/upstreams/{id}"),
        Some(json!({ "base_url": null })),
        Some("W/\"2\""),
    )
    .await;
    assert_eq!(stale_preserve.status, StatusCode::CONFLICT);

    // Legacy clients send `base_url: null` on every save; it must
    // preserve the override rather than clear it. A preserve-only
    // update does not bump the spec revision.
    let preserved = request(
        app.clone(),
        "PATCH",
        &format!("/admin/v1/upstreams/{id}"),
        Some(json!({ "base_url": null })),
        Some("W/\"3\""),
    )
    .await;
    assert_eq!(preserved.status, StatusCode::OK);
    assert_eq!(
        body(&preserved)["base_url"],
        "https://replacement.example.com/api/"
    );
    assert!(!body(&preserved).to_string().contains(secret));

    // A non-null base_url combined with clear_base_url is rejected
    // before any mutation.
    let contradictory = request(
        app.clone(),
        "PUT",
        &format!("/admin/v1/upstreams/{id}"),
        Some(json!({
            "base_url": "https://contradiction.example.com/",
            "clear_base_url": true
        })),
        Some("W/\"3\""),
    )
    .await;
    assert_eq!(contradictory.status, StatusCode::BAD_REQUEST);
    assert_eq!(body(&contradictory)["error"], "conflicting_base_url");

    // A stale precondition wins over body validation: the same
    // contradictory body with an old If-Match conflicts and leaves
    // the record untouched.
    let stale_contradictory = request(
        app.clone(),
        "PUT",
        &format!("/admin/v1/upstreams/{id}"),
        Some(json!({
            "base_url": "https://contradiction.example.com/",
            "clear_base_url": true
        })),
        Some("W/\"2\""),
    )
    .await;
    assert_eq!(stale_contradictory.status, StatusCode::CONFLICT);

    let after_reject = request(
        app.clone(),
        "GET",
        &format!("/admin/v1/upstreams/{id}"),
        None,
        None,
    )
    .await;
    assert_eq!(after_reject.status, StatusCode::OK);
    assert_eq!(
        body(&after_reject)["base_url"],
        "https://replacement.example.com/api/"
    );

    // Clearing requires the explicit flag.
    let cleared = request(
        app.clone(),
        "PATCH",
        &format!("/admin/v1/upstreams/{id}"),
        Some(json!({ "clear_base_url": true })),
        Some("W/\"3\""),
    )
    .await;
    assert_eq!(cleared.status, StatusCode::OK);
    assert_eq!(body(&cleared)["base_url"], Value::Null);
    assert!(!body(&cleared).to_string().contains(secret));

    let cleared_detail = request(
        app.clone(),
        "GET",
        &format!("/admin/v1/upstreams/{id}"),
        None,
        None,
    )
    .await;
    assert_eq!(cleared_detail.status, StatusCode::OK);
    assert_eq!(body(&cleared_detail)["base_url"], Value::Null);

    let restored = request(
        app,
        "PUT",
        &format!("/admin/v1/upstreams/{id}"),
        Some(json!({ "base_url": "https://restored.example.com/" })),
        Some("W/\"4\""),
    )
    .await;
    assert_eq!(restored.status, StatusCode::OK);
    assert_eq!(body(&restored)["base_url"], "https://restored.example.com/");
}

#[tokio::test]
async fn list_paginates_with_x_total_count() {
    let (_dir, storage) = new_store().await;
    let app = router(test_state(storage));
    create(app.clone(), "u-a").await;
    create(app.clone(), "u-b").await;
    create(app.clone(), "u-c").await;

    let first = request(
        app.clone(),
        "GET",
        "/admin/v1/upstreams?limit=2",
        None,
        None,
    )
    .await;
    assert_eq!(first.status, StatusCode::OK);
    assert_eq!(first.headers.get("X-Total-Count").unwrap(), "3");
    let first_page = body(&first)["upstreams"].as_array().unwrap();
    assert_eq!(first_page.len(), 2);

    let after = first_page.last().unwrap()["id"].as_str().unwrap();
    let second = request(
        app,
        "GET",
        &format!("/admin/v1/upstreams?after={after}&limit=2"),
        None,
        None,
    )
    .await;
    assert_eq!(second.status, StatusCode::OK);
    assert_eq!(second.headers.get("X-Total-Count").unwrap(), "3");
    assert_eq!(body(&second)["upstreams"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn update_with_correct_if_match_returns_200_and_bumps_revision() {
    let (_dir, storage) = new_store().await;
    let app = router(test_state(storage));
    let created = create(app.clone(), "primary").await;
    let id = body(&created)["id"].as_str().unwrap();

    let response = request(
        app,
        "PUT",
        &format!("/admin/v1/upstreams/{id}"),
        Some(json!({ "name": "primary-renamed", "base_url": "https://example.com" })),
        Some("W/\"1\""),
    )
    .await;

    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(body(&response)["name"], "primary-renamed");
    assert_eq!(body(&response)["spec_revision"], 2);
}

#[tokio::test]
async fn patch_flips_enabled_and_warmup_enabled_atomically() {
    let (_dir, storage) = new_store().await;
    let app = router(test_state(storage));
    let created = create(app.clone(), "primary").await;
    let id = body(&created)["id"].as_str().unwrap();
    assert_eq!(body(&created)["enabled"], true);

    let response = request(
        app,
        "PATCH",
        &format!("/admin/v1/upstreams/{id}"),
        Some(json!({ "enabled": false, "warmup_enabled": false })),
        Some("W/\"1\""),
    )
    .await;

    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(body(&response)["enabled"], false);
    assert_eq!(body(&response)["warmup_enabled"], false);
    assert_eq!(body(&response)["spec_revision"], 2);
}

#[tokio::test]
async fn update_with_stale_if_match_returns_409_with_current_revision() {
    let (_dir, storage) = new_store().await;
    let app = router(test_state(storage));
    let created = create(app.clone(), "primary").await;
    let id = body(&created)["id"].as_str().unwrap();

    let response = request(
        app,
        "PUT",
        &format!("/admin/v1/upstreams/{id}"),
        Some(json!({ "base_url": "https://example.com" })),
        Some("0"),
    )
    .await;

    assert_eq!(response.status, StatusCode::CONFLICT);
    assert_eq!(body(&response)["error"], "stale_revision");
    assert_eq!(body(&response)["current_revision"], 1);
}

#[tokio::test]
async fn update_without_if_match_returns_428_precondition_required() {
    let (_dir, storage) = new_store().await;
    let app = router(test_state(storage));
    let created = create(app.clone(), "primary").await;
    let id = body(&created)["id"].as_str().unwrap();

    let response = request(
        app,
        "PUT",
        &format!("/admin/v1/upstreams/{id}"),
        Some(json!({ "base_url": "https://example.com" })),
        None,
    )
    .await;

    assert_eq!(response.status, StatusCode::PRECONDITION_REQUIRED);
    assert_eq!(body(&response)["error"], "if_match_required");
}

#[tokio::test]
async fn enable_disable_emits_audit_and_persists_state() {
    let (_dir, storage) = new_store().await;
    let app = router(test_state(storage.clone()));
    let created = create(app.clone(), "primary").await;
    let id = body(&created)["id"].as_str().unwrap();

    let disabled = request(
        app.clone(),
        "POST",
        &format!("/admin/v1/upstreams/{id}/disable"),
        None,
        Some("W/\"1\""),
    )
    .await;
    assert_eq!(disabled.status, StatusCode::OK);
    assert_eq!(body(&disabled)["enabled"], false);
    assert_eq!(body(&disabled)["spec_revision"], 2);

    let enabled = request(
        app,
        "POST",
        &format!("/admin/v1/upstreams/{id}/enable"),
        None,
        Some("W/\"2\""),
    )
    .await;
    assert_eq!(enabled.status, StatusCode::OK);
    assert_eq!(body(&enabled)["enabled"], true);
    assert_eq!(body(&enabled)["spec_revision"], 3);

    let entries = storage.query_audit(None, 0, u64::MAX, 10).await.unwrap();
    let disable_route = format!("/admin/v1/upstreams/{id}/disable");
    let enable_route = format!("/admin/v1/upstreams/{id}/enable");
    assert!(entries.iter().any(|entry| {
        entry.route == disable_route
            && entry
                .admin_action
                .as_deref()
                .is_some_and(|action| action.starts_with("upstream_disable"))
    }));
    assert!(entries.iter().any(|entry| {
        entry.route == enable_route
            && entry
                .admin_action
                .as_deref()
                .is_some_and(|action| action.starts_with("upstream_enable"))
    }));
}

#[tokio::test]
async fn delete_soft_deletes_upstream() {
    let (_dir, storage) = new_store().await;
    let app = router(test_state(storage));
    let created = create(app.clone(), "primary").await;
    let id = body(&created)["id"].as_str().unwrap();

    let deleted = request(
        app.clone(),
        "DELETE",
        &format!("/admin/v1/upstreams/{id}"),
        None,
        Some("W/\"1\""),
    )
    .await;
    assert_eq!(deleted.status, StatusCode::NO_CONTENT);
    assert!(deleted.body.is_none());

    let get_deleted = request(
        app.clone(),
        "GET",
        &format!("/admin/v1/upstreams/{id}"),
        None,
        None,
    )
    .await;
    assert_eq!(get_deleted.status, StatusCode::NOT_FOUND);

    let list = request(app, "GET", "/admin/v1/upstreams", None, None).await;
    assert_eq!(body(&list)["upstreams"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn recreate_after_delete_returns_created_then_name_conflict() {
    let (_dir, storage) = new_store().await;
    let app = router(test_state(storage));
    let first = create(app.clone(), "primary").await;
    assert_eq!(first.status, StatusCode::CREATED);
    let first_id = body(&first)["id"].as_str().unwrap().to_owned();
    let current = request(
        app.clone(),
        "GET",
        &format!("/admin/v1/upstreams/{first_id}"),
        None,
        None,
    )
    .await;
    assert_eq!(current.status, StatusCode::OK);
    let first_etag = current
        .headers
        .get(header::ETAG)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();

    let deleted = request(
        app.clone(),
        "DELETE",
        &format!("/admin/v1/upstreams/{first_id}"),
        None,
        Some(&first_etag),
    )
    .await;
    assert_eq!(deleted.status, StatusCode::NO_CONTENT);

    let list = request(app.clone(), "GET", "/admin/v1/upstreams", None, None).await;
    assert_eq!(list.status, StatusCode::OK);
    assert!(
        body(&list)["upstreams"]
            .as_array()
            .unwrap()
            .iter()
            .all(|upstream| upstream["id"] != first_id)
    );

    let second = create(app.clone(), "primary").await;
    assert_eq!(second.status, StatusCode::CREATED);
    let second_id = body(&second)["id"].as_str().unwrap().to_owned();
    assert_ne!(second_id, first_id);

    let conflict = create(app, "primary").await;
    assert_eq!(conflict.status, StatusCode::CONFLICT);
    assert_eq!(body(&conflict)["error"], "upstream_name_conflict");
    assert_eq!(body(&conflict)["existing_upstream_id"], second_id);
}

#[tokio::test]
async fn concurrent_same_name_create_has_one_winner() {
    let (_dir, storage) = new_store().await;
    let app = router(test_state(storage));
    let (left, right) = tokio::join!(
        create(app.clone(), "concurrent-name"),
        create(app, "concurrent-name")
    );
    let (created, conflict) = if left.status == StatusCode::CREATED {
        (&left, &right)
    } else {
        (&right, &left)
    };

    assert_eq!(created.status, StatusCode::CREATED);
    assert_eq!(conflict.status, StatusCode::CONFLICT);
    assert_eq!(body(conflict)["error"], "upstream_name_conflict");
    assert_eq!(body(conflict)["existing_upstream_id"], body(created)["id"]);
}
