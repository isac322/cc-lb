//! Upstream CRUD admin endpoint smoke tests (list, create, get, update, delete, enable/disable).

use crate::admin_test_common;

use std::sync::{Arc, Mutex};

use axum::{
    body::Body,
    extract::State as AxumState,
    http::{Request, StatusCode, header},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_aead::AeadService;
use cc_lb_config::Config;
use cc_lb_storage_api::{
    AuditQueryScope, AuditStore, UpstreamCreate, UpstreamStore, upstream::UpstreamKind,
};
use cc_lb_storage_sqlite::SqliteStorage as Storage;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

type RecordedProbes = Arc<Mutex<Vec<(String, Option<String>, Option<String>)>>>;

async fn provider_probe_server() -> (url::Url, RecordedProbes) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = axum::Router::new()
        .fallback(provider_probe_handler)
        .with_state(Arc::clone(&seen));
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (
        url::Url::parse(&format!("http://{address}/")).unwrap(),
        seen,
    )
}

async fn provider_probe_handler(
    AxumState(seen): AxumState<RecordedProbes>,
    request: Request<Body>,
) -> (StatusCode, &'static str) {
    let path = request.uri().path().to_owned();
    let authorization = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let google_key = request
        .headers()
        .get("x-goog-api-key")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    seen.lock().unwrap().push((path, authorization, google_key));
    (StatusCode::OK, "")
}

struct TestResponse {
    status: StatusCode,
    headers: axum::http::HeaderMap,
    body: Option<Value>,
}

fn test_state(storage: Arc<Storage>) -> AdminState {
    let config = Config::default();
    AdminState {
        config_path: None,
        startup_config_overrides: Default::default(),
        storage: Some(storage.clone()),
        key_store: Some(admin_test_common::key_store(storage)),
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: admin_test_common::limit_engine(),
        lifecycle: None,
        dynamic_view: admin_test_common::dynamic_view_holder(&config),
        dynamic_view_rebinder: None,
        config: Arc::new(config),
        scheduler: None,
        admin_auth: crate::admin_test_common::static_token_auth("test-token"),
        lazy_refresher: None,
        runtime: None,
        data_dir: None,
        warmup_dialect_dispatcher: None,
        start_time: std::time::Instant::now(),
        event_bus: None,
        storage_tail: cc_lb_admin::events::storage_tail_channel(),
        clock: Arc::new(cc_lb_clock::SystemClock),
        anthropic_org_api: cc_lb_control::anthropic_api_key::AnthropicOrgApi::production()
            .expect("production Anthropic origin parses"),
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
            "api_key_value": "sk-ant-api03-test",
            "skip_preflight": true
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
            "api_key_value": secret,
            "skip_preflight": true
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
    let id = body(&created)["id"].as_str().unwrap();
    // Storing the key is itself a spec write, so the created revision is the
    // baseline rather than a fixed 1.
    let base = body(&created)["spec_revision"].as_u64().unwrap();
    let rev = |n: u64| format!("W/\"{}\"", base + n - 1);

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
        Some(json!({ "base_url": "https://replacement.example.com/api/", "skip_preflight": true })),
        Some(rev(1).as_str()),
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
        Some(rev(2).as_str()),
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
        Some(rev(2).as_str()),
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
        Some(rev(3).as_str()),
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
        Some(rev(3).as_str()),
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
        Some(rev(2).as_str()),
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
        Some(json!({ "clear_base_url": true, "skip_preflight": true })),
        Some(rev(3).as_str()),
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
        Some(json!({ "base_url": "https://restored.example.com/", "skip_preflight": true })),
        Some(rev(4).as_str()),
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

    let entries = storage
        .query_recent_audit(AuditQueryScope::All, 0, u64::MAX, 10, false)
        .await
        .unwrap();
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

fn oauth_bundle_json(
    access_token: &str,
    refresh_token: &str,
    account_id: Option<&str>,
    project_id: Option<&str>,
    never_refresh: bool,
) -> Value {
    json!({
        "access_token": access_token,
        "refresh_token": refresh_token,
        "expires_at_unix_secs": 1_900_000_000u64,
        "refresh_token_expires_at_unix_secs": null,
        "account_id": account_id,
        "project_id": project_id,
        "scopes": ["openid", "profile"],
        "never_refresh": never_refresh
    })
}

#[tokio::test]
async fn access_only_oauth_create_persists_no_refresh_secret()
-> Result<(), Box<dyn std::error::Error>> {
    for kind in ["openai_oauth", "gemini_oauth"] {
        let (_dir, storage) = new_store().await;
        let response = request(
            router(test_state(storage.clone())),
            "POST",
            "/admin/v1/upstreams",
            Some(json!({
                "name": format!("{kind}-access-only"),
                "kind": kind,
                "oauth_tokens": oauth_bundle_json(
                    "access-only-test-token",
                    "",
                    None,
                    (kind == "gemini_oauth").then_some("test-project"),
                    true
                )
            })),
            None,
        )
        .await;
        assert_eq!(
            response.status,
            StatusCode::CREATED,
            "{kind}: {}",
            body(&response)
        );
        let id = Uuid::parse_str(
            body(&response)
                .get("id")
                .and_then(Value::as_str)
                .ok_or("missing upstream id")?,
        )?;
        let record = UpstreamStore::get_by_id(storage.as_ref(), id)
            .await?
            .ok_or("created upstream is missing")?;
        assert!(record.oauth_never_refresh);
        let bundle = record
            .oauth_credentials
            .ok_or("missing stored credentials")?
            .decrypt(&AeadService::from_master_key([0; 32]), id.as_bytes())?;
        assert_eq!(bundle.access_token, "access-only-test-token");
        assert_eq!(bundle.refresh_token, "");
        assert_eq!(bundle.refresh_token_expires_at_unix_secs, None);
        assert!(bundle.never_refresh);
        assert_eq!(bundle.expires_at_unix_secs, 1_900_000_000);
        let rendered = body(&response).to_string();
        assert!(!rendered.contains("access-only-test-token"));
        assert!(body(&response).get("oauth_tokens").is_none());
    }
    Ok(())
}

#[tokio::test]
async fn access_only_oauth_update_replaces_previous_refresh_secret()
-> Result<(), Box<dyn std::error::Error>> {
    for kind in [UpstreamKind::OpenaiOauth, UpstreamKind::GeminiOauth] {
        let (_dir, storage) = new_store().await;
        let id = Uuid::new_v4();
        let previous = cc_lb_aead::OAuthTokenBundle {
            access_token: "previous-access-test-token".to_owned(),
            refresh_token: "previous-refresh-test-token".to_owned(),
            expires_at_unix_secs: 1_900_000_000,
            refresh_token_expires_at_unix_secs: None,
            account_id: None,
            project_id: (kind == UpstreamKind::GeminiOauth).then(|| "test-project".to_owned()),
            scopes: Vec::new(),
            never_refresh: false,
        };
        let encrypted = cc_lb_aead::EncryptedOAuthTokens::encrypt(
            &AeadService::from_master_key([0; 32]),
            &previous,
            id.as_bytes(),
        )?;
        let original = UpstreamStore::create(
            storage.as_ref(),
            UpstreamCreate {
                id,
                name: format!("{}-access-update", kind.as_str()),
                kind,
                base_url: None,
                api_key_ciphertext: None,
                oauth_tokens: Some(cc_lb_storage_api::OAuthTokensCreate {
                    tokens: encrypted,
                    never_refresh: false,
                }),
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
            },
        )
        .await?;
        let response = request(
            router(test_state(storage.clone())),
            "PUT",
            &format!("/admin/v1/upstreams/{id}"),
            Some(json!({
                "oauth_tokens": oauth_bundle_json(
                    "replacement-access-test-token",
                    "",
                    None,
                    (kind == UpstreamKind::GeminiOauth).then_some("test-project"),
                    true
                )
            })),
            Some(&format!("W/\"{}\"", original.revision)),
        )
        .await;
        assert_eq!(
            response.status,
            StatusCode::OK,
            "{}: {}",
            kind.as_str(),
            body(&response)
        );
        let stored = UpstreamStore::get_by_id(storage.as_ref(), id)
            .await?
            .ok_or("updated upstream is missing")?;
        assert!(stored.oauth_never_refresh);
        assert_eq!(
            stored.oauth_token_generation,
            original.oauth_token_generation + 1
        );
        let bundle = stored
            .oauth_credentials
            .as_ref()
            .ok_or("missing stored credentials")?
            .decrypt(&AeadService::from_master_key([0; 32]), id.as_bytes())?;
        assert_eq!(bundle.access_token, "replacement-access-test-token");
        assert_eq!(bundle.refresh_token, "");
        assert!(bundle.never_refresh);
        assert_eq!(bundle.refresh_token_expires_at_unix_secs, None);
        assert!(
            !body(&response)
                .to_string()
                .contains("replacement-access-test-token")
        );
        for (refresh_token, refresh_expiry) in [
            ("unexpected-refresh-test-token", None),
            ("", Some(1_950_000_000u64)),
        ] {
            let mut invalid = oauth_bundle_json(
                "rejected-access-test-token",
                refresh_token,
                None,
                (kind == UpstreamKind::GeminiOauth).then_some("test-project"),
                true,
            );
            invalid
                .as_object_mut()
                .ok_or("bundle is not an object")?
                .insert(
                    "refresh_token_expires_at_unix_secs".to_owned(),
                    json!(refresh_expiry),
                );
            let rejected = request(
                router(test_state(storage.clone())),
                "PUT",
                &format!("/admin/v1/upstreams/{id}"),
                Some(json!({ "oauth_tokens": invalid })),
                Some(&format!("W/\"{}\"", stored.revision)),
            )
            .await;
            assert_eq!(rejected.status, StatusCode::BAD_REQUEST);
            let unchanged = UpstreamStore::get_by_id(storage.as_ref(), id)
                .await?
                .ok_or("upstream disappeared after rejected update")?;
            assert_eq!(
                unchanged.oauth_token_generation,
                stored.oauth_token_generation
            );
            assert_eq!(unchanged.revision, stored.revision);
            assert_eq!(
                unchanged
                    .oauth_credentials
                    .as_ref()
                    .map(cc_lb_aead::EncryptedOAuthTokens::ciphertext),
                stored
                    .oauth_credentials
                    .as_ref()
                    .map(cc_lb_aead::EncryptedOAuthTokens::ciphertext)
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn access_only_oauth_rejects_mixed_refresh_credentials_without_write()
-> Result<(), Box<dyn std::error::Error>> {
    for kind in ["openai_oauth", "gemini_oauth"] {
        for (refresh_token, refresh_expiry) in [
            ("unexpected-refresh-test-token", None),
            ("", Some(1_950_000_000u64)),
        ] {
            let (_dir, storage) = new_store().await;
            let mut bundle = oauth_bundle_json(
                "access-only-test-token",
                refresh_token,
                None,
                (kind == "gemini_oauth").then_some("test-project"),
                true,
            );
            bundle
                .as_object_mut()
                .ok_or("bundle is not an object")?
                .insert(
                    "refresh_token_expires_at_unix_secs".to_owned(),
                    json!(refresh_expiry),
                );
            let app = router(test_state(storage));
            let response = request(
                app.clone(),
                "POST",
                "/admin/v1/upstreams",
                Some(json!({
                    "name": format!("{kind}-mixed"),
                    "kind": kind,
                    "oauth_tokens": bundle
                })),
                None,
            )
            .await;
            assert_eq!(response.status, StatusCode::BAD_REQUEST);
            assert!(
                !body(&response)
                    .to_string()
                    .contains("unexpected-refresh-test-token")
            );
            let list = request(app, "GET", "/admin/v1/upstreams", None, None).await;
            assert_eq!(list.status, StatusCode::OK);
            assert_eq!(
                body(&list)
                    .get("upstreams")
                    .and_then(Value::as_array)
                    .ok_or("missing upstreams")?
                    .len(),
                0
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn openai_oauth_create_derives_account_id_and_does_not_disclose_tokens() {
    let (_dir, storage) = new_store().await;
    let app = router(test_state(storage.clone()));
    let access_token = "eyJhbGciOiJub25lIn0.eyJodHRwczovL2FwaS5vcGVuYWkuY29tL2F1dGgiOnsiY2hhdGdwdF9hY2NvdW50X2lkIjoiand0LWFjY291bnQifX0.signature";
    let refresh_token = "openai-refresh-secret";

    let response = request(
        app,
        "POST",
        "/admin/v1/upstreams",
        Some(json!({
            "name": "openai-import",
            "kind": "openai_oauth",
            "oauth_tokens": oauth_bundle_json(
                access_token,
                refresh_token,
                None,
                None,
                false
            )
        })),
        None,
    )
    .await;

    assert_eq!(response.status, StatusCode::CREATED);
    assert_eq!(body(&response)["kind"], "openai_oauth");
    let response_text = body(&response).to_string();
    assert!(!response_text.contains(access_token));
    assert!(!response_text.contains(refresh_token));
    assert!(body(&response).get("oauth_tokens").is_none());
    assert!(body(&response).get("access_token").is_none());
    assert!(body(&response).get("refresh_token").is_none());

    let id = Uuid::parse_str(body(&response)["id"].as_str().unwrap()).unwrap();
    let stored = UpstreamStore::get_by_id(storage.as_ref(), id)
        .await
        .unwrap()
        .unwrap();
    let bundle = stored
        .oauth_credentials
        .unwrap()
        .decrypt(&AeadService::from_master_key([0; 32]), id.as_bytes())
        .unwrap();
    assert_eq!(bundle.account_id.as_deref(), Some("jwt-account"));
    assert_eq!(bundle.access_token, access_token);
    assert_eq!(bundle.refresh_token, refresh_token);
    assert!(!bundle.never_refresh);
}

#[tokio::test]
async fn openai_oauth_update_accepts_bundle_and_does_not_disclose_tokens() {
    let (_dir, storage) = new_store().await;
    let upstream = UpstreamStore::create(
        storage.as_ref(),
        UpstreamCreate {
            id: Uuid::new_v4(),
            name: "openai-update".to_owned(),
            kind: UpstreamKind::OpenaiOauth,
            base_url: None,
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        },
    )
    .await
    .unwrap();
    let app = router(test_state(storage.clone()));
    let access_token = "openai-update-access-secret";
    let refresh_token = "openai-update-refresh-secret";

    let response = request(
        app,
        "PUT",
        &format!("/admin/v1/upstreams/{}", upstream.id),
        Some(json!({
            "oauth_tokens": oauth_bundle_json(
                access_token,
                refresh_token,
                Some("explicit-account"),
                None,
                false
            )
        })),
        Some("W/\"1\""),
    )
    .await;

    assert_eq!(response.status, StatusCode::OK);
    let response_text = body(&response).to_string();
    assert!(!response_text.contains(access_token));
    assert!(!response_text.contains(refresh_token));
    assert!(body(&response).get("oauth_tokens").is_none());
    assert!(body(&response).get("access_token").is_none());
    assert!(body(&response).get("refresh_token").is_none());

    let stored = UpstreamStore::get_by_id(storage.as_ref(), upstream.id)
        .await
        .unwrap()
        .unwrap();
    let bundle = stored
        .oauth_credentials
        .unwrap()
        .decrypt(
            &AeadService::from_master_key([0; 32]),
            upstream.id.as_bytes(),
        )
        .unwrap();
    assert_eq!(bundle.account_id.as_deref(), Some("explicit-account"));
    assert_eq!(bundle.access_token, access_token);
    assert_eq!(bundle.refresh_token, refresh_token);
}

#[tokio::test]
async fn openai_oauth_update_rejects_api_key_value() {
    let (_dir, storage) = new_store().await;
    let upstream = UpstreamStore::create(
        storage.as_ref(),
        UpstreamCreate {
            id: Uuid::new_v4(),
            name: "openai-update-api-key".to_owned(),
            kind: UpstreamKind::OpenaiOauth,
            base_url: None,
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        },
    )
    .await
    .unwrap();

    let response = request(
        router(test_state(storage)),
        "PUT",
        &format!("/admin/v1/upstreams/{}", upstream.id),
        Some(json!({ "api_key_value": "sk-openai-secret" })),
        Some("W/\"1\""),
    )
    .await;

    assert_eq!(response.status, StatusCode::BAD_REQUEST);
    assert_eq!(body(&response)["error"], "unexpected_api_key");
}

#[tokio::test]
async fn openai_oauth_import_rejects_refresh_token_in_access_only_mode() {
    let (_dir, storage) = new_store().await;
    let app = router(test_state(storage.clone()));

    let response = request(
        app,
        "POST",
        "/admin/v1/upstreams",
        Some(json!({
            "name": "openai-long-lived",
            "kind": "openai_oauth",
            "oauth_tokens": oauth_bundle_json(
                "openai-access",
                "openai-refresh",
                Some("account"),
                None,
                true
            )
        })),
        None,
    )
    .await;

    assert_eq!(response.status, StatusCode::BAD_REQUEST);
    assert_eq!(body(&response)["error"], "invalid_oauth_tokens");
    assert!(!body(&response).to_string().contains("openai-refresh"));
    let list = request(
        router(test_state(storage)),
        "GET",
        "/admin/v1/upstreams",
        None,
        None,
    )
    .await;
    assert_eq!(list.status, StatusCode::OK);
    assert!(body(&list)["upstreams"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn gemini_oauth_requires_project_id_and_persists_body_project_id_without_secrets() {
    let (_dir, storage) = new_store().await;
    let app = router(test_state(storage.clone()));
    let missing_project = request(
        app.clone(),
        "POST",
        "/admin/v1/upstreams",
        Some(json!({
            "name": "gemini-missing-project",
            "kind": "gemini_oauth",
            "oauth_tokens": oauth_bundle_json(
                "gemini-access",
                "gemini-refresh",
                None,
                None,
                false
            )
        })),
        None,
    )
    .await;
    assert_eq!(missing_project.status, StatusCode::BAD_REQUEST);
    assert_eq!(body(&missing_project)["error"], "missing_project_id");

    let access_token = "gemini-access-secret";
    let refresh_token = "gemini-refresh-secret";
    let created = request(
        app,
        "POST",
        "/admin/v1/upstreams",
        Some(json!({
            "name": "gemini-import",
            "kind": "gemini_oauth",
            "project_id": "project-from-request",
            "oauth_tokens": oauth_bundle_json(
                access_token,
                refresh_token,
                None,
                None,
                false
            )
        })),
        None,
    )
    .await;
    assert_eq!(created.status, StatusCode::CREATED);
    let response_text = body(&created).to_string();
    assert!(!response_text.contains(access_token));
    assert!(!response_text.contains(refresh_token));

    let id = Uuid::parse_str(body(&created)["id"].as_str().unwrap()).unwrap();
    let stored = UpstreamStore::get_by_id(storage.as_ref(), id)
        .await
        .unwrap()
        .unwrap();
    let bundle = stored
        .oauth_credentials
        .unwrap()
        .decrypt(&AeadService::from_master_key([0; 32]), id.as_bytes())
        .unwrap();
    assert_eq!(bundle.project_id.as_deref(), Some("project-from-request"));
}

#[tokio::test]
async fn gemini_oauth_rejects_api_key_import() {
    let (_dir, storage) = new_store().await;
    let response = request(
        router(test_state(storage)),
        "POST",
        "/admin/v1/upstreams",
        Some(json!({
            "name": "gemini-api-key-mismatch",
            "kind": "gemini_oauth",
            "api_key_value": "AIza-secret",
            "project_id": "project",
            "oauth_tokens": oauth_bundle_json(
                "gemini-access",
                "gemini-refresh",
                None,
                Some("project"),
                false
            )
        })),
        None,
    )
    .await;

    assert_eq!(response.status, StatusCode::BAD_REQUEST);
    assert_eq!(body(&response)["error"], "invalid_gemini_oauth_credential");
}

#[tokio::test]
async fn anthropic_and_api_key_oauth_imports_remain_rejected() {
    for (index, kind) in ["anthropic_oauth", "openai_api_key"]
        .into_iter()
        .enumerate()
    {
        let (_dir, storage) = new_store().await;
        let response = request(
            router(test_state(storage)),
            "POST",
            "/admin/v1/upstreams",
            Some(json!({
                "name": format!("oauth-rejected-{index}"),
                "kind": kind,
                "oauth_tokens": oauth_bundle_json(
                    "access-secret",
                    "refresh-secret",
                    Some("account"),
                    None,
                    false
                ),
                "skip_preflight": true
            })),
            None,
        )
        .await;

        assert_eq!(response.status, StatusCode::BAD_REQUEST);
        assert_eq!(body(&response)["error"], "unexpected_oauth_tokens");
    }
}

#[tokio::test]
async fn openai_oauth_rejects_api_key_value_on_create() {
    let (_dir, storage) = new_store().await;
    let response = request(
        router(test_state(storage)),
        "POST",
        "/admin/v1/upstreams",
        Some(json!({
            "name": "openai-api-key-mismatch",
            "kind": "openai_oauth",
            "api_key_value": "sk-openai-secret"
        })),
        None,
    )
    .await;

    assert_eq!(response.status, StatusCode::BAD_REQUEST);
    assert_eq!(body(&response)["error"], "unexpected_api_key");
}

#[tokio::test]
async fn openai_and_gemini_api_key_creates_bypass_anthropic_preflight_by_default() {
    for (index, kind) in ["openai_api_key", "gemini_api_key"].into_iter().enumerate() {
        let (_dir, storage) = new_store().await;
        let response = request(
            router(test_state(storage)),
            "POST",
            "/admin/v1/upstreams",
            Some(json!({
                "name": format!("non-anthropic-default-preflight-{index}"),
                "kind": kind,
                "api_key_value": "provider-secret-key"
            })),
            None,
        )
        .await;

        assert_eq!(response.status, StatusCode::CREATED);
        assert_eq!(body(&response)["kind"], kind);
    }
}

#[tokio::test]
async fn provider_api_key_preflight_and_recheck_use_native_models_probe() {
    for (index, (kind, expected_path, expected_auth, expected_google_key)) in [
        (
            "openai_api_key",
            "/v1/models",
            Some("Bearer provider-secret-key"),
            None,
        ),
        (
            "gemini_api_key",
            "/v1beta/models",
            None,
            Some("provider-secret-key"),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let (base_url, seen) = provider_probe_server().await;
        let (_dir, storage) = new_store().await;
        let app = router(test_state(storage));
        let preflight = request(
            app.clone(),
            "POST",
            "/admin/v1/upstreams/preflight",
            Some(json!({
                "kind": kind,
                "api_key_value": "provider-secret-key",
                "base_url": base_url.as_str()
            })),
            None,
        )
        .await;
        assert_eq!(preflight.status, StatusCode::OK, "{kind}");
        assert_eq!(body(&preflight)["verdict"], "ok", "{kind}");
        assert_eq!(
            body(&preflight)["capabilities"]["workspace_list"],
            "not_applicable",
            "{kind}"
        );
        assert!(body(&preflight)["classification"]["workspace"]["id"].is_null());

        let created = request(
            app.clone(),
            "POST",
            "/admin/v1/upstreams",
            Some(json!({
                "name": format!("provider-native-{index}"),
                "kind": kind,
                "api_key_value": "provider-secret-key",
                "base_url": base_url.as_str(),
                "skip_preflight": true
            })),
            None,
        )
        .await;
        assert_eq!(created.status, StatusCode::CREATED, "{kind}");
        let id = body(&created)["id"].as_str().unwrap().to_owned();

        let recheck = request(
            app.clone(),
            "POST",
            &format!("/admin/v1/upstreams/{id}/preflight"),
            None,
            None,
        )
        .await;
        assert_eq!(recheck.status, StatusCode::OK, "{kind}");
        assert_eq!(body(&recheck)["verdict"], "ok", "{kind}");

        let detail = request(app, "GET", &format!("/admin/v1/upstreams/{id}"), None, None).await;
        assert_eq!(detail.status, StatusCode::OK, "{kind}");
        assert_eq!(body(&detail)["api_key_access"]["verified"], true);
        assert!(body(&detail)["api_key_access"]["workspace"]["id"].is_null());
        let requests = seen.lock().unwrap();
        assert_eq!(requests.len(), 2, "{kind}");
        for (path, authorization, google_key) in requests.iter() {
            assert_eq!(path, expected_path, "{kind}");
            assert_eq!(authorization.as_deref(), expected_auth, "{kind}");
            assert_eq!(google_key.as_deref(), expected_google_key, "{kind}");
        }
    }
}

#[tokio::test]
async fn openai_and_gemini_oauth_patch_supports_metadata_only_and_combined_updates() {
    let (_dir, storage) = new_store().await;
    let state = test_state(storage);
    let created = request(
        router(state.clone()),
        "POST",
        "/admin/v1/upstreams",
        Some(json!({
            "name": "openai-oauth-patch",
            "kind": "openai_oauth",
            "oauth_tokens": oauth_bundle_json(
                "access-1",
                "refresh-1",
                Some("acct-1"),
                None,
                false
            )
        })),
        None,
    )
    .await;
    assert_eq!(created.status, StatusCode::CREATED);
    let id = body(&created)["id"].as_str().unwrap().to_owned();

    let disabled = request(
        router(state.clone()),
        "PATCH",
        &format!("/admin/v1/upstreams/{id}"),
        Some(json!({ "enabled": false })),
        Some("1"),
    )
    .await;
    assert_eq!(disabled.status, StatusCode::OK);
    assert_eq!(body(&disabled)["enabled"], false);
    assert_eq!(
        disabled.headers.get("etag").and_then(|v| v.to_str().ok()),
        Some("W/\"2\"")
    );

    let other = request(
        router(state.clone()),
        "POST",
        "/admin/v1/upstreams",
        Some(json!({
            "name": "taken-oauth-name",
            "kind": "openai_oauth",
            "oauth_tokens": oauth_bundle_json(
                "other-access",
                "other-refresh",
                Some("other-acct"),
                None,
                false
            )
        })),
        None,
    )
    .await;
    assert_eq!(other.status, StatusCode::CREATED);

    let conflicting = request(
        router(state.clone()),
        "PATCH",
        &format!("/admin/v1/upstreams/{id}"),
        Some(json!({
            "name": "taken-oauth-name",
            "enabled": true,
            "oauth_tokens": oauth_bundle_json(
                "should-not-commit",
                "should-not-commit-refresh",
                Some("acct-conflict"),
                None,
                false
            )
        })),
        Some("\"2\""),
    )
    .await;
    assert_eq!(conflicting.status, StatusCode::CONFLICT);
    let uuid = Uuid::parse_str(&id).unwrap();
    let unchanged = UpstreamStore::get_by_id(state.storage.as_ref().unwrap().as_ref(), uuid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(unchanged.revision, 2);
    assert_eq!(unchanged.oauth_token_generation, 1);

    let combined = request(
        router(state),
        "PATCH",
        &format!("/admin/v1/upstreams/{id}"),
        Some(json!({
            "name": "openai-oauth-renamed",
            "enabled": true,
            "oauth_tokens": oauth_bundle_json(
                "access-2",
                "refresh-2",
                Some("acct-2"),
                None,
                false
            )
        })),
        Some("\"2\""),
    )
    .await;
    assert_eq!(combined.status, StatusCode::OK);
    assert_eq!(body(&combined)["name"], "openai-oauth-renamed");
    assert_eq!(body(&combined)["enabled"], true);
    assert_eq!(
        combined.headers.get("etag").and_then(|v| v.to_str().ok()),
        Some("W/\"3\"")
    );
}
