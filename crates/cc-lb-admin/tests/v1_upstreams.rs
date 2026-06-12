//! Upstream CRUD admin endpoint smoke tests (list, create, get, update, delete, enable/disable).

mod admin_test_common;

use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_config::Config;
use cc_lb_core::AuditWriterSink;
use cc_lb_storage_redb::Storage;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

struct TestResponse {
    status: StatusCode,
    headers: axum::http::HeaderMap,
    body: Option<Value>,
}

fn test_state(storage: Arc<Storage>, audit_sink: Option<AuditWriterSink>) -> AdminState {
    let config = Config::default();
    AdminState {
        storage: Some(storage.clone()),
        key_store: Some(admin_test_common::key_store(storage)),
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: admin_test_common::limit_engine(),
        lifecycle: None,
        audit_sink: audit_sink.map(Arc::new),
        dynamic_view: admin_test_common::dynamic_view_holder(&config),
        config: Arc::new(config),
        admin_token: Some("test-token".to_owned()),
        lazy_refresher: None,
        runtime: None,
        data_dir: None,
        warmup_dialect_dispatcher: None,
        subscription_metadata_hook: None,
        start_time: std::time::Instant::now(),
    }
}

fn new_store() -> (tempfile::TempDir, Arc<Storage>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("v1_upstreams.redb");
    let storage = Storage::open(&path, [41; 32]).unwrap();
    (dir, Arc::new(storage))
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
    let (_dir, storage) = new_store();
    let app = router(test_state(storage, None));

    let response = create(app, "primary").await;

    assert_eq!(response.status, StatusCode::CREATED);
    assert_eq!(body(&response)["name"], "primary");
    assert_eq!(body(&response)["kind"], "anthropic_oauth");
    assert_eq!(body(&response)["enabled"], true);
    assert_eq!(body(&response)["revision"], 1);
    let id = body(&response)["id"].as_str().unwrap();
    assert_eq!(
        response.headers.get(header::LOCATION).unwrap(),
        &format!("/admin/v1/upstreams/{id}")
    );
}

#[tokio::test]
async fn get_after_create_returns_etag_with_revision() {
    let (_dir, storage) = new_store();
    let app = router(test_state(storage, None));
    let created = create(app.clone(), "primary").await;
    let id = body(&created)["id"].as_str().unwrap();

    let response = request(app, "GET", &format!("/admin/v1/upstreams/{id}"), None, None).await;

    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(body(&response)["id"], id);
    assert_eq!(response.headers.get(header::ETAG).unwrap(), "W/\"1\"");
}

#[tokio::test]
async fn list_paginates_with_x_total_count() {
    let (_dir, storage) = new_store();
    let app = router(test_state(storage, None));
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
    let (_dir, storage) = new_store();
    let app = router(test_state(storage, None));
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
    assert_eq!(body(&response)["revision"], 2);
}

#[tokio::test]
async fn update_with_stale_if_match_returns_409_with_current_revision() {
    let (_dir, storage) = new_store();
    let app = router(test_state(storage, None));
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
    let (_dir, storage) = new_store();
    let app = router(test_state(storage, None));
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
    let (_dir, storage) = new_store();
    let (audit_sink, audit_writer) = cc_lb_core::spawn_audit_writer(storage.clone(), 64);
    let app = router(test_state(storage.clone(), Some(audit_sink)));
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
    assert_eq!(body(&disabled)["revision"], 2);

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
    assert_eq!(body(&enabled)["revision"], 3);

    // Poll the audit log instead of relying on a fixed sleep. The audit sink
    // batches writes to its background writer; on slower CI runners the 250 ms
    // window we used to assume was sometimes too short and the second of the
    // two expected entries (`upstream_enable`) had not been flushed yet,
    // producing a flaky assertion. Poll for both actions with a generous
    // deadline before reporting failure.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let entries = storage.query_audit(Some("admin"), 0, u64::MAX, 10).unwrap();
        let has_disable = entries.iter().any(|entry| {
            entry
                .admin_action
                .as_deref()
                .is_some_and(|action| action.starts_with("upstream_disable"))
        });
        let has_enable = entries.iter().any(|entry| {
            entry
                .admin_action
                .as_deref()
                .is_some_and(|action| action.starts_with("upstream_enable"))
        });
        if has_disable && has_enable {
            break;
        }
        if std::time::Instant::now() >= deadline {
            assert!(
                has_disable,
                "upstream_disable audit entry not observed within 5s"
            );
            assert!(
                has_enable,
                "upstream_enable audit entry not observed within 5s"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    audit_writer.abort();
}

#[tokio::test]
async fn delete_soft_deletes_upstream() {
    let (_dir, storage) = new_store();
    let app = router(test_state(storage, None));
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
