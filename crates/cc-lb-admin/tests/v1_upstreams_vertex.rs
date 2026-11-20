use std::sync::{
    Arc,
    atomic::{AtomicU16, Ordering},
};

use axum::{
    Router,
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    routing::any,
};
use cc_lb_storage_api::{UpstreamStatusUpdate, UpstreamStore};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::api_key_support::{ADMIN_TOKEN, spawn_api_key_harness};

async fn request(
    app: &Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
    etag: Option<&str>,
) -> (StatusCode, Value, axum::http::HeaderMap) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", format!("Bearer {ADMIN_TOKEN}"));
    if body.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    if let Some(etag) = etag {
        builder = builder.header("if-match", etag);
    }
    let response = app
        .clone()
        .oneshot(
            builder
                .body(
                    body.map(|v| Body::from(v.to_string()))
                        .unwrap_or_else(Body::empty),
                )
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        headers,
    )
}
async fn success_probe_server() -> String {
    async fn handler() -> (StatusCode, &'static str) {
        (
            StatusCode::OK,
            r#"{"models":[{"name":"models/gemini-test"}],"candidates":[{"content":{"parts":[{"text":"ok"}]}}],"totalTokens":1}"#,
        )
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(
        axum::serve(listener, Router::new().route("/{*path}", any(handler))).into_future(),
    );
    format!("http://{address}/")
}

#[tokio::test]
async fn qa_adm_1_vertex_valid_create_types_return_metadata_only() {
    let harness = spawn_api_key_harness().await;
    let app = harness.app;
    for (name, kind, fields) in [
        (
            "express",
            "vertex_api_key",
            json!({"express": true, "api_key_value": "AIza-qa-adm-1"}),
        ),
        (
            "regional-key",
            "vertex_api_key",
            json!({"project_id": "project-12345", "location": "us-central1", "api_key_value": "AIza-qa-adm-1"}),
        ),
        (
            "service",
            "vertex_credential",
            json!({"project_id": "project-12345", "location": "us-central1", "google_credential": format!(r#"{{"type":"service_account","client_email":"vertex-test@project.iam.gserviceaccount.com","private_key":{:?},"private_key_id":"kid-1","project_id":"my-project"}}"#, include_str!("../../cc-lb-server/tests/fixtures/tls/key-a.pem"))}),
        ),
        (
            "authorized",
            "vertex_credential",
            json!({"project_id": "project-12345", "location": "us-central1", "google_credential": "{\"type\":\"authorized_user\",\"client_id\":\"client-id\",\"client_secret\":\"CLIENT-SECRET-VALUE\",\"refresh_token\":\"REFRESH-TOKEN-VALUE\",\"quota_project_id\":\"my-project\"}"}),
        ),
        (
            "impersonated",
            "vertex_credential",
            json!({"project_id": "project-12345", "location": "us-central1", "google_credential": r#"{"type":"impersonated_service_account","service_account_impersonation_url":"https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/target@project.iam.gserviceaccount.com:generateAccessToken","source_credentials":{"type":"authorized_user","client_id":"client-id","client_secret":"CLIENT-SECRET-VALUE","refresh_token":"REFRESH-TOKEN-VALUE"},"delegates":["source@project.iam.gserviceaccount.com"],"quota_project_id":"target-project"}"#}),
        ),
    ] {
        let mut payload =
            json!({"name": format!("qa-adm-1-{name}"), "kind": kind, "skip_preflight": true});
        payload
            .as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        let (status, value, _) =
            request(&app, "POST", "/admin/v1/upstreams", Some(payload), None).await;
        assert_eq!(status, StatusCode::CREATED, "{name}: {value}");
        assert!(value.get("api_key_value").is_none());
        assert!(value.get("google_credential").is_none());
    }
}

#[tokio::test]
async fn qa_adm_2_vertex_invalid_combinations_do_not_persist() {
    let harness = spawn_api_key_harness().await;
    let (status, _, _) = request(&harness.app, "POST", "/admin/v1/upstreams", Some(json!({"name":"qa-adm-2","kind":"vertex_credential","express":true,"google_credential":"{}"})), None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (_, list, _) = request(&harness.app, "GET", "/admin/v1/upstreams", None, None).await;
    assert!(!list.to_string().contains("qa-adm-2"));
}

#[tokio::test]
async fn qa_adm_3_list_and_get_never_decrypt_credentials() {
    let harness = spawn_api_key_harness().await;
    let (status, created, _) = request(&harness.app, "POST", "/admin/v1/upstreams", Some(json!({"name":"qa-adm-3","kind":"vertex_api_key","express":true,"api_key_value":"AIza-qa-adm-3","skip_preflight":true})), None).await;
    assert_eq!(status, StatusCode::CREATED);
    let id = created["id"].as_str().unwrap();
    let (_, list, _) = request(&harness.app, "GET", "/admin/v1/upstreams", None, None).await;
    let (_, detail, _) = request(
        &harness.app,
        "GET",
        &format!("/admin/v1/upstreams/{id}"),
        None,
        None,
    )
    .await;
    for value in [list, detail] {
        assert!(!value.to_string().contains("AIza-qa-adm-3"));
        assert!(value.to_string().contains("vertex"));
    }
}

#[tokio::test]
async fn qa_adm_4_update_without_replacement_rechecks_and_skip_is_honored() {
    let harness = spawn_api_key_harness().await;
    let (status, created, _) = request(&harness.app, "POST", "/admin/v1/upstreams", Some(json!({"name":"qa-adm-4","kind":"vertex_api_key","express":true,"api_key_value":"AIza-qa-adm-4","skip_preflight":true})), None).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let id = created["id"].as_str().unwrap();
    let (status, _, headers) = request(
        &harness.app,
        "GET",
        &format!("/admin/v1/upstreams/{id}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let etag = headers.get("etag").unwrap().to_str().unwrap().to_owned();
    let probe_url = success_probe_server().await;
    let (status, updated, _) = request(
        &harness.app,
        "PATCH",
        &format!("/admin/v1/upstreams/{id}"),
        Some(json!({"base_url":probe_url,"skip_preflight":true})),
        Some(&etag),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{updated}");
    assert_eq!(updated["base_url"], probe_url);
    assert_eq!(updated["spec_revision"], 2);
    let (status, persisted, _) = request(
        &harness.app,
        "GET",
        &format!("/admin/v1/upstreams/{id}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(persisted["base_url"], probe_url);
    assert_eq!(persisted["spec_revision"], 2);
}

#[tokio::test]
async fn qa_adm_7_vertex_api_key_update_rejects_credential_json() {
    let harness = spawn_api_key_harness().await;
    let (status, created, _) = request(
        &harness.app,
        "POST",
        "/admin/v1/upstreams",
        Some(json!({
            "name": "qa-adm-7",
            "kind": "vertex_api_key",
            "express": true,
            "api_key_value": "AIza-qa-adm-7",
            "skip_preflight": true
        })),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let id = created["id"].as_str().unwrap();
    let (_, _, headers) = request(
        &harness.app,
        "GET",
        &format!("/admin/v1/upstreams/{id}"),
        None,
        None,
    )
    .await;
    let etag = headers.get("etag").unwrap().to_str().unwrap();
    let (status, value, _) = request(
        &harness.app,
        "PATCH",
        &format!("/admin/v1/upstreams/{id}"),
        Some(json!({"api_key_value": " {\"type\":\"service_account\"}"})),
        Some(etag),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
    assert_eq!(value["error"], "invalid_input");
}

#[tokio::test]
async fn qa_adm_5_replacement_clears_terminal_and_stale_if_match_conflicts() {
    let harness = spawn_api_key_harness().await;
    let (status, created, _) = request(&harness.app, "POST", "/admin/v1/upstreams", Some(json!({"name":"qa-adm-5","kind":"vertex_credential","project_id":"project-qa-adm-5","location":"us-central1","google_credential":"{\"type\":\"authorized_user\",\"client_id\":\"client-id\",\"client_secret\":\"CLIENT-SECRET-VALUE\",\"refresh_token\":\"REFRESH-TOKEN-VALUE\"}","skip_preflight":true})), None).await;
    assert_eq!(status, StatusCode::CREATED);
    let id = created["id"].as_str().unwrap();
    UpstreamStore::set_status(
        &*harness.storage,
        id.parse().unwrap(),
        UpstreamStatusUpdate {
            last_apply_error: Some(Some("invalid_grant".to_owned())),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let (status, _, headers) = request(
        &harness.app,
        "GET",
        &format!("/admin/v1/upstreams/{id}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let etag = headers.get("etag").unwrap().to_str().unwrap().to_owned();
    let probe_url = success_probe_server().await;
    let (status, stale, _) = request(
        &harness.app,
        "PATCH",
        &format!("/admin/v1/upstreams/{id}"),
        Some(json!({"api_key_value":"AIza-qa-adm-5-new"})),
        Some("W/\"0\""),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(stale["error"], "stale_revision");
    let (status, replacement, _) = request(
        &harness.app,
        "PATCH",
        &format!("/admin/v1/upstreams/{id}"),
        Some(json!({"google_credential":"{\"type\":\"authorized_user\",\"client_id\":\"client-id-new\",\"client_secret\":\"CLIENT-SECRET-VALUE\",\"refresh_token\":\"REFRESH-TOKEN-VALUE\"}","base_url":probe_url,"skip_preflight":true})),
        Some(&etag),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{replacement}");
    assert_eq!(replacement["base_url"], probe_url);
    assert_eq!(replacement["spec_revision"], 2);
    let (status, persisted, _) = request(
        &harness.app,
        "GET",
        &format!("/admin/v1/upstreams/{id}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(persisted["base_url"], probe_url);
    assert_eq!(persisted["spec_revision"], 2);
    assert_eq!(persisted["last_apply_error"], Value::Null);
}

#[tokio::test]
async fn qa_adm_6_workspace_and_access_routes_are_allowlisted() {
    let harness = spawn_api_key_harness().await;
    let (status, created, _) = request(&harness.app, "POST", "/admin/v1/upstreams", Some(json!({"name":"qa-adm-6","kind":"vertex_api_key","express":true,"api_key_value":"AIza-qa-adm-6","skip_preflight":true})), None).await;
    assert_eq!(status, StatusCode::CREATED);
    let id = created["id"].as_str().unwrap();
    let (workspaces, _, _) = request(
        &harness.app,
        "GET",
        &format!("/admin/v1/upstreams/{id}/workspaces"),
        None,
        None,
    )
    .await;
    assert!(matches!(
        workspaces,
        StatusCode::BAD_REQUEST | StatusCode::NOT_FOUND
    ));
    let (access, _, _) = request(
        &harness.app,
        "GET",
        &format!("/admin/v1/upstreams/{id}/api-key-access"),
        None,
        None,
    )
    .await;
    assert_ne!(access, StatusCode::METHOD_NOT_ALLOWED);
}

#[derive(Clone)]
struct ProbeState(Arc<AtomicU16>);
async fn probe(State(state): State<ProbeState>) -> (StatusCode, &'static str) {
    match state.0.load(Ordering::Relaxed) {
        400 => (
            StatusCode::BAD_REQUEST,
            r#"{"error":{"reason":"PERMISSION_DENIED"}}"#,
        ),
        401 => (StatusCode::UNAUTHORIZED, ""),
        403 => (StatusCode::FORBIDDEN, ""),
        404 => (StatusCode::NOT_FOUND, ""),
        429 => (StatusCode::TOO_MANY_REQUESTS, ""),
        500 => (StatusCode::INTERNAL_SERVER_ERROR, ""),
        _ => (
            StatusCode::OK,
            r#"{"models":[{"name":"models/gemini-test"}]}"#,
        ),
    }
}

#[tokio::test]
async fn qa_err_4_vertex_preflight_maps_provider_statuses() {
    let harness = spawn_api_key_harness().await;
    let status = Arc::new(AtomicU16::new(400));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(
        axum::serve(
            listener,
            Router::new()
                .route("/{*path}", any(probe))
                .with_state(ProbeState(Arc::clone(&status))),
        )
        .into_future(),
    );
    let base = format!("http://{address}/");
    for (code, expected) in [
        (400, "ok"),
        (401, "rejected"),
        (403, "rejected"),
        (404, "ok"),
        (429, "ok"),
        (500, "unreachable"),
    ] {
        status.store(code, Ordering::Relaxed);
        let (_, value, _) = request(&harness.app, "POST", "/admin/v1/upstreams/preflight", Some(json!({"kind":"vertex_api_key","api_key_value":"AIza-qa-err-4","express":true,"base_url":base,"skip_preflight":true})), None).await;
        assert_eq!(value["verdict"], expected, "status {code}: {value}");
        if matches!(code, 400 | 404) {
            assert_eq!(
                value["capabilities"]["inference"], "unverified",
                "status {code}: {value}"
            );
        }
    }
    status.store(400, Ordering::Relaxed);
    let (status, created, _) = request(
        &harness.app,
        "POST",
        "/admin/v1/upstreams",
        Some(json!({
            "name": "qa-err-4-unverified-create",
            "kind": "vertex_api_key",
            "express": true,
            "api_key_value": "AIza-qa-err-4-create",
            "base_url": base,
        })),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
}

#[tokio::test]
async fn qa_adm_vertex_unreachable_create_uses_probe_remediation() {
    let harness = spawn_api_key_harness().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let (status, value, _) = request(
        &harness.app,
        "POST",
        "/admin/v1/upstreams",
        Some(json!({
            "name": "qa-adm-vertex-unreachable",
            "kind": "vertex_api_key",
            "express": true,
            "api_key_value": "AIza-qa-adm-vertex-unreachable",
            "base_url": format!("http://{address}/"),
        })),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{value}");
    assert_eq!(value["error"]["code"], "probe_unreachable");
}
